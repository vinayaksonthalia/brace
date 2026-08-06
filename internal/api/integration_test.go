//go:build integration

// Integration tests for the HTTP layer. They run against a REAL engine on a
// REAL PostgreSQL, because everything worth asserting about this package is
// about the seam between the two: that a rejected statement is a 400 on the
// submitting request, that a run in flight is a 409, that the SSE stream
// carries stats at 1 Hz and then run_started and run_finished in that order.
// A mocked engine would test the mock.
//
//	docker run --rm -d --name brace-dev-pg -p 5433:5432 \
//	  -e POSTGRES_HOST_AUTH_METHOD=trust postgres:16
//	TEST_ADMIN_URL=postgres://postgres@localhost:5433/postgres \
//	  go test -tags integration -race ./internal/api/
package api

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/vinayaksonthalia/brace/internal/engine"
	"github.com/vinayaksonthalia/brace/internal/twins"
)

const (
	// itNamespace is the prefix every database this file creates shares, so one
	// sweep leaves the server as it was found.
	itNamespace  = "brace_api_"
	templateName = itNamespace + "template"
	appDBName    = itNamespace + "app"

	// migratorRole is deliberately NOT engine.DefaultMigratorRole: the engine
	// package's own suite drops that role at teardown, and the two packages are
	// compiled and run in parallel by `go test ./...`.
	migratorRole = "brace_api_migrator"

	// templateRows is enough that a plain CREATE INDEX is measurable and small
	// enough that cloning it is not the slowest part of the suite.
	templateRows = 300_000
)

var (
	adminURL  string
	appURL    string
	dbSkipMsg string
)

func TestMain(m *testing.M) {
	adminURL = os.Getenv("TEST_ADMIN_URL")
	if adminURL == "" {
		adminURL = "postgres://postgres@localhost:5433/postgres"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	conn, err := waitForPostgres(ctx, adminURL, 30*time.Second)
	cancel()
	if err != nil {
		dbSkipMsg = fmt.Sprintf("PostgreSQL at TEST_ADMIN_URL=%s is unreachable: %v", adminURL, err)
		os.Exit(m.Run())
	}
	bg := context.Background()
	if err := sweep(bg, conn); err != nil {
		fmt.Fprintf(os.Stderr, "pre-test sweep failed: %v\n", err)
		os.Exit(1)
	}
	if err := buildTemplate(bg, conn); err != nil {
		fmt.Fprintf(os.Stderr, "template build failed: %v\n", err)
		os.Exit(1)
	}
	conn.Close(bg)
	appURL = dsnForDB(adminURL, appDBName)

	code := m.Run()

	if c, err := pgx.Connect(bg, adminURL); err == nil {
		if err := sweep(bg, c); err != nil {
			fmt.Fprintf(os.Stderr, "post-test sweep failed: %v\n", err)
		}
		_, _ = c.Exec(bg, `DROP ROLE IF EXISTS `+migratorRole)
		c.Close(bg)
	}
	os.Exit(code)
}

func waitForPostgres(ctx context.Context, dsn string, limit time.Duration) (*pgx.Conn, error) {
	deadline := time.Now().Add(limit)
	var lastErr error
	for {
		attemptCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		conn, err := pgx.Connect(attemptCtx, dsn)
		if err == nil {
			if err = conn.Ping(attemptCtx); err == nil {
				cancel()
				return conn, nil
			}
			conn.Close(attemptCtx)
		}
		cancel()
		lastErr = err
		if time.Now().After(deadline) || ctx.Err() != nil {
			return nil, lastErr
		}
		time.Sleep(time.Second)
	}
}

func sweep(ctx context.Context, conn *pgx.Conn) error {
	rows, err := conn.Query(ctx, `SELECT datname FROM pg_database WHERE starts_with(datname, $1)`, itNamespace)
	if err != nil {
		return err
	}
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			rows.Close()
			return err
		}
		names = append(names, n)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, n := range names {
		if _, err := conn.Exec(ctx, `DROP DATABASE IF EXISTS "`+n+`" WITH (FORCE)`); err != nil {
			return fmt.Errorf("drop %q: %w", n, err)
		}
	}
	return nil
}

func buildTemplate(ctx context.Context, admin *pgx.Conn) error {
	if _, err := admin.Exec(ctx, `CREATE DATABASE "`+appDBName+`"`); err != nil {
		return fmt.Errorf("create app db: %w", err)
	}
	if _, err := admin.Exec(ctx, `CREATE DATABASE "`+templateName+`"`); err != nil {
		return fmt.Errorf("create template: %w", err)
	}
	conn, err := pgx.Connect(ctx, dsnForDB(adminURL, templateName))
	if err != nil {
		return fmt.Errorf("connect template: %w", err)
	}
	defer conn.Close(ctx)

	if _, err := conn.Exec(ctx, `CREATE TABLE orders (
		id           bigserial   PRIMARY KEY,
		customer_id  bigint      NOT NULL,
		amount_cents bigint      NOT NULL,
		status       text        NOT NULL DEFAULT 'pending',
		created_at   timestamptz NOT NULL DEFAULT now()
	)`); err != nil {
		return fmt.Errorf("create orders: %w", err)
	}
	if _, err := conn.Exec(ctx, `
		INSERT INTO orders (customer_id, amount_cents)
		SELECT (random()*100000)::bigint + 1, (random()*50000)::bigint + 1
		FROM generate_series(1, $1)`, templateRows); err != nil {
		return fmt.Errorf("seed orders: %w", err)
	}
	if _, err := conn.Exec(ctx, `ANALYZE orders`); err != nil {
		return fmt.Errorf("analyze: %w", err)
	}
	return nil
}

func dsnForDB(base, db string) string {
	u, err := url.Parse(base)
	if err != nil {
		panic(err)
	}
	u.Path = "/" + db
	return u.String()
}

func requireDB(t *testing.T) {
	t.Helper()
	if dbSkipMsg != "" {
		t.Skip(dbSkipMsg)
	}
}

// ---------------------------------------------------------------- harness

type lab struct {
	srv  *Server
	http *httptest.Server
	eng  *engine.Engine
	mgr  *twins.Manager
}

// newLab builds a private twin pool, an engine on it, an api.Server and a real
// HTTP listener. A real listener rather than httptest.ResponseRecorder because
// half of what is under test — SSE — does not exist without a socket.
func newLab(t *testing.T, prefix string, poolSize int) *lab {
	t.Helper()
	requireDB(t)
	ctx := context.Background()

	mgr, err := twins.NewManager(ctx, twins.Config{
		AdminURL:     appURL,
		TemplateName: templateName,
		PoolSize:     poolSize,
		NamePrefix:   itNamespace + prefix + "_",
		Logger:       quiet(),
	})
	if err != nil {
		t.Fatalf("twins.NewManager: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := mgr.Close(closeCtx); err != nil {
			t.Errorf("twins.Close: %v", err)
		}
	})

	eng, err := engine.New(ctx, engine.Config{
		AdminURL:         appURL,
		Twins:            mgr,
		Workers:          12,
		MigratorRole:     migratorRole,
		LockTimeout:      200 * time.Millisecond,
		SettleDuration:   1500 * time.Millisecond,
		RecoveryDuration: time.Second,
		SampleInterval:   100 * time.Millisecond,
		Logger:           quiet(),
	})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })

	pool, err := pgxpool.New(ctx, appURL)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)

	// The abuse guard is deliberately opened up here: it has its own test, and
	// leaving it at 3/min would make every case below depend on the order the
	// others ran in.
	srv := New(Config{
		Logger:       quiet(),
		TickInterval: time.Second,
		RunPerMinute: 6000,
		RunBurst:     500,
	})
	t.Cleanup(srv.Close)
	srv.Ready(eng, pool, TemplateInfo{Rows: templateRows, Size: "42 MB"})

	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	waitWarm(t, mgr, poolSize)
	return &lab{srv: srv, http: ts, eng: eng, mgr: mgr}
}

func waitWarm(t *testing.T, mgr *twins.Manager, want int) {
	t.Helper()
	deadline := time.Now().Add(120 * time.Second)
	for {
		if mgr.Status().Warm >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("twin pool never reached %d warm: %+v", want, mgr.Status())
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (l *lab) get(t *testing.T, path string) (int, []byte) {
	t.Helper()
	resp, err := http.Get(l.http.URL + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	buf := make([]byte, 0, 4096)
	tmp := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(tmp)
		buf = append(buf, tmp[:n]...)
		if err != nil {
			break
		}
	}
	return resp.StatusCode, buf
}

func (l *lab) run(t *testing.T, sql, mode string) (int, map[string]any) {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"sql": sql, "mode": mode})
	resp, err := http.Post(l.http.URL+"/api/run", "application/json", strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("POST /api/run: %v", err)
	}
	defer resp.Body.Close()
	var decoded map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&decoded)
	return resp.StatusCode, decoded
}

// ---------------------------------------------------------------- SSE client

type sseEvent struct {
	name string
	data map[string]any
	at   time.Time
}

// sseClient reads a stream until it is closed, collecting every event.
type sseClient struct {
	mu     sync.Mutex
	events []sseEvent
	cancel context.CancelFunc
	done   chan struct{}
	hdr    http.Header
}

func openStream(t *testing.T, base string) *sseClient {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/api/stream", nil)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		t.Fatalf("open stream: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		cancel()
		t.Fatalf("stream status = %d, want 200", resp.StatusCode)
	}

	c := &sseClient{cancel: cancel, done: make(chan struct{}), hdr: resp.Header}
	go func() {
		defer close(c.done)
		defer resp.Body.Close()
		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
		var name, data string
		for sc.Scan() {
			line := sc.Text()
			switch {
			case strings.HasPrefix(line, "event: "):
				name = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: "):
				data = strings.TrimPrefix(line, "data: ")
			case line == "":
				if name != "" && data != "" {
					var payload map[string]any
					if err := json.Unmarshal([]byte(data), &payload); err == nil {
						c.mu.Lock()
						c.events = append(c.events, sseEvent{name: name, data: payload, at: time.Now()})
						c.mu.Unlock()
					}
				}
				name, data = "", ""
			}
		}
	}()
	t.Cleanup(func() { c.close() })
	return c
}

func (c *sseClient) close() {
	c.cancel()
	select {
	case <-c.done:
	case <-time.After(5 * time.Second):
	}
}

func (c *sseClient) snapshot() []sseEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]sseEvent, len(c.events))
	copy(out, c.events)
	return out
}

// waitFor blocks until an event of the given name arrives, or the timeout.
func (c *sseClient) waitFor(t *testing.T, name string, timeout time.Duration) sseEvent {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, ev := range c.snapshot() {
			if ev.name == name {
				return ev
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("no %q event within %v (saw %v)", name, timeout, names(c.snapshot()))
	return sseEvent{}
}

func names(evs []sseEvent) []string {
	out := make([]string, 0, len(evs))
	for _, e := range evs {
		out = append(out, e.name)
	}
	return out
}

// ---------------------------------------------------------------- tests

func TestStatusAndStatsShape(t *testing.T) {
	l := newLab(t, "status", 1)

	code, body := l.get(t, "/api/status")
	if code != http.StatusOK {
		t.Fatalf("GET /api/status = %d: %s", code, body)
	}
	var status map[string]any
	if err := json.Unmarshal(body, &status); err != nil {
		t.Fatalf("decode status: %v (%s)", err, body)
	}
	for _, key := range []string{"pool", "running", "baseline_writes_per_sec", "migrator_role", "template"} {
		if _, ok := status[key]; !ok {
			t.Fatalf("status is missing %q: %s", key, body)
		}
	}
	tmpl, ok := status["template"].(map[string]any)
	if !ok || tmpl["rows"] == nil || tmpl["size"] == nil {
		t.Fatalf("template block = %v", status["template"])
	}
	if pool, ok := status["pool"].(map[string]any); !ok || pool["warm"] == nil {
		t.Fatalf("pool block = %v", status["pool"])
	}

	code, body = l.get(t, "/api/stats")
	if code != http.StatusOK {
		t.Fatalf("GET /api/stats = %d: %s", code, body)
	}
	var stats map[string]any
	if err := json.Unmarshal(body, &stats); err != nil {
		t.Fatalf("decode stats: %v (%s)", err, body)
	}
	for _, key := range []string{"writes_per_sec", "casualties_total", "by_label", "blocked", "run_id"} {
		if _, ok := stats[key]; !ok {
			t.Fatalf("stats is missing %q: %s", key, body)
		}
	}
	if stats["run_id"] != nil {
		t.Fatalf("run_id = %v with no run in flight, want null", stats["run_id"])
	}
}

func TestStreamHeadersAndSeedFrame(t *testing.T) {
	l := newLab(t, "stream", 1)
	c := openStream(t, l.http.URL)

	// These three are the difference between a live dashboard and a dead one
	// behind a buffering proxy.
	if got := c.hdr.Get("Content-Type"); !strings.HasPrefix(got, "text/event-stream") {
		t.Fatalf("Content-Type = %q", got)
	}
	if got := c.hdr.Get("Cache-Control"); got != "no-cache" {
		t.Fatalf("Cache-Control = %q, want no-cache", got)
	}
	if got := c.hdr.Get("X-Accel-Buffering"); got != "no" {
		t.Fatalf("X-Accel-Buffering = %q, want no", got)
	}

	first := c.waitFor(t, "stats", 5*time.Second)
	if _, ok := first.data["by_label"]; !ok {
		t.Fatalf("first stats frame missing by_label: %v", first.data)
	}
	if first.data["run_id"] != nil {
		t.Fatalf("run_id = %v while idle, want null", first.data["run_id"])
	}

	// Cadence: the dashboard's scope assumes one frame per second.
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if len(c.snapshot()) >= 4 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	evs := c.snapshot()
	if len(evs) < 4 {
		t.Fatalf("expected at least 4 stats frames in 6s, got %d", len(evs))
	}
	for i := 2; i < len(evs); i++ {
		gap := evs[i].at.Sub(evs[i-1].at)
		if gap < 500*time.Millisecond || gap > 2500*time.Millisecond {
			t.Fatalf("stats gap %d->%d = %v, want ~1s", i-1, i, gap)
		}
	}

	// Only the FIRST frame carries the seed history.
	if len(evs) > 1 {
		if _, ok := evs[1].data["history"]; ok {
			t.Fatalf("history repeated on frame 2: %v", evs[1].data)
		}
	}

	// A stream opened once the ring has samples must seed the scope, so the
	// dashboard opens full instead of drawing itself in from the right edge.
	c2 := openStream(t, l.http.URL)
	seed := c2.waitFor(t, "stats", 5*time.Second)
	hist, ok := seed.data["history"].([]any)
	if !ok || len(hist) == 0 {
		t.Fatalf("second stream's first frame carried no history: %v", seed.data)
	}
	point, ok := hist[0].([]any)
	if !ok || len(point) != 2 {
		t.Fatalf("history point = %v, want [msEpoch, wps]", hist[0])
	}
	if ms, ok := point[0].(float64); !ok || ms < 1e12 {
		t.Fatalf("history timestamp = %v, want epoch milliseconds", point[0])
	}
}

func TestRunAcceptedThenStreamedToVerdict(t *testing.T) {
	l := newLab(t, "run", 1)
	c := openStream(t, l.http.URL)
	c.waitFor(t, "stats", 5*time.Second)

	code, body := l.run(t, "CREATE INDEX idx_orders_customer_id ON orders (customer_id)", "naive")
	if code != http.StatusAccepted {
		t.Fatalf("POST /api/run = %d: %v", code, body)
	}
	runID, _ := body["run_id"].(string)
	if runID == "" {
		t.Fatalf("202 body has no run_id: %v", body)
	}

	started := c.waitFor(t, "run_started", 10*time.Second)
	if started.data["run_id"] != runID {
		t.Fatalf("run_started run_id = %v, want %s", started.data["run_id"], runID)
	}
	if started.data["mode"] != "naive" {
		t.Fatalf("run_started mode = %v, want naive", started.data["mode"])
	}
	if !strings.Contains(fmt.Sprint(started.data["sql"]), "idx_orders_customer_id") {
		t.Fatalf("run_started sql = %v", started.data["sql"])
	}

	finished := c.waitFor(t, "run_finished", 90*time.Second)
	for _, key := range []string{"run_id", "verdict", "blocked_seconds", "casualties", "duration_seconds", "timeline"} {
		if _, ok := finished.data[key]; !ok {
			t.Fatalf("run_finished missing %q: %v", key, finished.data)
		}
	}
	if finished.data["run_id"] != runID {
		t.Fatalf("run_finished run_id = %v, want %s", finished.data["run_id"], runID)
	}
	verdict := fmt.Sprint(finished.data["verdict"])
	if verdict != engine.VerdictSafe && verdict != engine.VerdictUnsafe {
		t.Fatalf("verdict = %q", verdict)
	}
	cas, ok := finished.data["casualties"].(map[string]any)
	if !ok || cas["total"] == nil || cas["by_label"] == nil {
		t.Fatalf("casualties = %v", finished.data["casualties"])
	}

	// Ordering: run_started must precede run_finished, with stats in between.
	evs := c.snapshot()
	startIdx, finishIdx := -1, -1
	for i, e := range evs {
		if e.name == "run_started" && startIdx < 0 {
			startIdx = i
		}
		if e.name == "run_finished" && finishIdx < 0 {
			finishIdx = i
		}
	}
	if startIdx < 0 || finishIdx < 0 || startIdx > finishIdx {
		t.Fatalf("event order wrong: %v", names(evs))
	}
	statsBetween := 0
	for _, e := range evs[startIdx:finishIdx] {
		if e.name == "stats" {
			statsBetween++
		}
	}
	if statsBetween < 2 {
		t.Fatalf("only %d stats frames during the run, want the live trace", statsBetween)
	}

	// /api/stats now offers the completed run to the polling fallback.
	_, statsBody := l.get(t, "/api/stats")
	var polled map[string]any
	if err := json.Unmarshal(statsBody, &polled); err != nil {
		t.Fatal(err)
	}
	last, ok := polled["last_run"].(map[string]any)
	if !ok || last["run_id"] != runID {
		t.Fatalf("last_run = %v, want run %s", polled["last_run"], runID)
	}

	// …and the ledger has it, by id.
	testReportRoundtrip(t, l, runID)
}

func testReportRoundtrip(t *testing.T, l *lab, runID string) {
	t.Helper()
	code, body := l.get(t, "/api/history?limit=5")
	if code != http.StatusOK {
		t.Fatalf("GET /api/history = %d: %s", code, body)
	}
	var history []map[string]any
	if err := json.Unmarshal(body, &history); err != nil {
		t.Fatalf("decode history: %v (%s)", err, body)
	}
	if len(history) == 0 {
		t.Fatal("history is empty after a completed run")
	}
	id, ok := history[0]["id"].(float64)
	if !ok || id <= 0 {
		t.Fatalf("history[0].id = %v", history[0]["id"])
	}

	code, body = l.get(t, fmt.Sprintf("/api/report/%d", int64(id)))
	if code != http.StatusOK {
		t.Fatalf("GET /api/report/%d = %d: %s", int64(id), code, body)
	}
	var rec map[string]any
	if err := json.Unmarshal(body, &rec); err != nil {
		t.Fatalf("decode report: %v (%s)", err, body)
	}
	if rec["id"] != id {
		t.Fatalf("report id = %v, want %v", rec["id"], id)
	}
	for _, key := range []string{"sql", "verdict", "blocked_seconds", "casualties_total", "twin"} {
		if _, ok := rec[key]; !ok {
			t.Fatalf("report missing %q: %s", key, body)
		}
	}

	// A report that does not exist is a 404 with a JSON body, not an HTML page.
	code, body = l.get(t, "/api/report/99999999")
	if code != http.StatusNotFound {
		t.Fatalf("GET /api/report/99999999 = %d, want 404: %s", code, body)
	}
	var missing map[string]any
	if err := json.Unmarshal(body, &missing); err != nil || missing["error"] != "not_found" {
		t.Fatalf("404 body = %s (err %v)", body, err)
	}
	code, body = l.get(t, "/api/report/not-a-number")
	if code != http.StatusNotFound {
		t.Fatalf("GET /api/report/not-a-number = %d, want 404: %s", code, body)
	}
}

func TestRejectedStatementsAre400(t *testing.T) {
	l := newLab(t, "reject", 1)

	// (a) not a migration Brace will run at all.
	code, body := l.run(t, "DROP TABLE orders", "")
	if code != http.StatusBadRequest {
		t.Fatalf("DROP TABLE = %d, want 400: %v", code, body)
	}
	if body["error"] != "rejected" || body["kind"] != string(engine.RejectRejected) {
		t.Fatalf("rejected body = %v", body)
	}
	if fmt.Sprint(body["reason"]) == "" {
		t.Fatalf("a 400 without a reason is useless: %v", body)
	}

	// (b) Brace could run it, but it is catalog-only: the rehearsal would draw
	// a flat line, so it is turned away with the lesson and a suggestion.
	code, body = l.run(t, "ANALYZE orders", "")
	if code != http.StatusBadRequest {
		t.Fatalf("ANALYZE = %d, want 400: %v", code, body)
	}
	if body["kind"] != string(engine.RejectAlreadySafe) {
		t.Fatalf("ANALYZE kind = %v, want already_safe: %v", body["kind"], body)
	}
	if fmt.Sprint(body["suggestion"]) == "" {
		t.Fatalf("already_safe must suggest something to run instead: %v", body)
	}

	// (c) an unknown mode is a 400 too, and no twin is spent finding out.
	code, body = l.run(t, "CREATE INDEX i ON orders (id)", "sideways")
	if code != http.StatusBadRequest {
		t.Fatalf("bad mode = %d, want 400: %v", code, body)
	}

	// The pre-check must not have touched the engine.
	if l.eng.Status().Running {
		t.Fatal("a rejected statement started a run")
	}
}

func TestSecondRunDuringARunIs409(t *testing.T) {
	l := newLab(t, "busy", 1)

	code, body := l.run(t, "CREATE INDEX idx_orders_amount ON orders (amount_cents)", "naive")
	if code != http.StatusAccepted {
		t.Fatalf("first run = %d: %v", code, body)
	}

	code, body = l.run(t, "CREATE INDEX idx_orders_status ON orders (status)", "naive")
	if code != http.StatusConflict {
		t.Fatalf("second run = %d, want 409: %v", code, body)
	}
	if body["error"] != "run_in_progress" {
		t.Fatalf("409 body = %v", body)
	}

	// Let the first run finish so the twin pool is returned intact.
	deadline := time.Now().Add(90 * time.Second)
	for l.eng.Status().Running && time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
	}
	if l.eng.Status().Running {
		t.Fatal("the first run never finished")
	}
}

func TestRunIsRateLimitedEndToEnd(t *testing.T) {
	requireDB(t)
	// A server with the shipped limits and no engine behind it: the guard runs
	// before the engine is consulted, which is the point — abuse must be
	// cheap to refuse.
	s := New(Config{Logger: quiet(), TickInterval: time.Hour, RunPerMinute: 3, RunBurst: 2})
	t.Cleanup(s.Close)
	s.Ready(nil, nil, TemplateInfo{})
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)

	post := func() int {
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/run",
			strings.NewReader(`{"sql":"CREATE INDEX i ON orders (id)"}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Forwarded-For", "203.0.113.55")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST: %v", err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	_ = post()
	_ = post()
	if code := post(); code != http.StatusTooManyRequests {
		t.Fatalf("third run in a burst = %d, want 429", code)
	}
}
