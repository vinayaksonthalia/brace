//go:build integration

// Integration tests for the labeled load generator. They need a real
// PostgreSQL 16 superuser: the whole point of this package is what the server
// does to a write that is blocked on a lock, and no fake can testify to that.
//
//	docker run --rm -d --name brace-dev-pg -p 5433:5432 \
//	  -e POSTGRES_HOST_AUTH_METHOD=trust postgres:16
//	TEST_ADMIN_URL=postgres://postgres@localhost:5433/postgres \
//	  go test -tags integration ./internal/loadgen/
package loadgen

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	itPrefix   = "brace_lg_test_"
	readyDelay = 60 * time.Second
)

var (
	adminURL  string
	dbSkipMsg string
)

func TestMain(m *testing.M) {
	adminURL = os.Getenv("TEST_ADMIN_URL")
	if adminURL == "" {
		adminURL = "postgres://postgres@localhost:5433/postgres"
	}

	ctx, cancel := context.WithTimeout(context.Background(), readyDelay+10*time.Second)
	conn, err := waitForPostgres(ctx, adminURL, readyDelay)
	cancel()
	if err != nil {
		dbSkipMsg = fmt.Sprintf("PostgreSQL at TEST_ADMIN_URL=%s is unreachable after %s: %v", adminURL, readyDelay, err)
		os.Exit(m.Run())
	}

	bg := context.Background()
	if err := dropScratchDatabases(bg, conn); err != nil {
		fmt.Fprintf(os.Stderr, "integration cleanup failed: %v\n", err)
		conn.Close(bg)
		os.Exit(1)
	}
	conn.Close(bg)

	code := m.Run()

	if c, err := pgx.Connect(bg, adminURL); err == nil {
		_ = dropScratchDatabases(bg, c)
		c.Close(bg)
	}
	os.Exit(code)
}

// waitForPostgres polls the way pg_isready would: the container is often still
// booting when `go test` starts.
func waitForPostgres(ctx context.Context, dsn string, limit time.Duration) (*pgx.Conn, error) {
	deadline := time.Now().Add(limit)
	var lastErr error
	for {
		attemptCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		conn, err := pgx.Connect(attemptCtx, dsn)
		if err == nil {
			err = conn.Ping(attemptCtx)
			if err == nil {
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

func requireDB(t *testing.T) {
	t.Helper()
	if dbSkipMsg != "" {
		t.Skip(dbSkipMsg)
	}
}

func dropScratchDatabases(ctx context.Context, conn *pgx.Conn) error {
	rows, err := conn.Query(ctx,
		`SELECT datname FROM pg_database WHERE starts_with(datname, $1)`, itPrefix)
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

// dsnForDB rewrites the admin URL to point at another database on the same
// server.
func dsnForDB(base, db string) string {
	u, err := url.Parse(base)
	if err != nil {
		panic(err)
	}
	u.Path = "/" + db
	return u.String()
}

// newScratchDB creates a database with the template's orders table and returns
// its DSN. Everything it makes is torn down by t.Cleanup.
func newScratchDB(t *testing.T) string {
	t.Helper()
	requireDB(t)

	name := fmt.Sprintf("%s%d", itPrefix, time.Now().UnixNano())
	ctx := context.Background()

	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	defer admin.Close(ctx)

	if _, err := admin.Exec(ctx, `CREATE DATABASE "`+name+`"`); err != nil {
		t.Fatalf("create scratch db: %v", err)
	}
	t.Cleanup(func() {
		c, err := pgx.Connect(context.Background(), adminURL)
		if err != nil {
			t.Logf("cleanup: connect admin: %v", err)
			return
		}
		defer c.Close(context.Background())
		if _, err := c.Exec(context.Background(), `DROP DATABASE IF EXISTS "`+name+`" WITH (FORCE)`); err != nil {
			t.Logf("cleanup: drop %q: %v", name, err)
		}
	})

	dsn := dsnForDB(adminURL, name)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect scratch db: %v", err)
	}
	defer conn.Close(ctx)
	// Mirrors the template schema the default labels write to.
	if _, err := conn.Exec(ctx, `CREATE TABLE orders (
		id           bigserial primary key,
		customer_id  bigint not null,
		amount_cents bigint not null,
		created_at   timestamptz not null default now()
	)`); err != nil {
		t.Fatalf("create orders: %v", err)
	}
	return dsn
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestObservedLockTimeoutSQLSTATE is the empirical anchor for the whole
// package: it records, against the real server, which SQLSTATE a write killed
// by lock_timeout reports. If PostgreSQL ever changes this, this test fails
// first and Classify is wrong until it is updated.
func TestObservedLockTimeoutSQLSTATE(t *testing.T) {
	dsn := newScratchDB(t)
	ctx := context.Background()

	g, err := New(ctx, Config{DSN: dsn, Workers: 1, LockTimeout: 250 * time.Millisecond, Logger: quietLogger()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Stop(ctx)

	holder := beginLockHolder(t, dsn)
	defer holder.rollback(t)

	_, err = g.pool.Exec(ctx, `INSERT INTO orders (customer_id, amount_cents) VALUES ($1,$2)`, int64(1), int64(2))
	if err == nil {
		t.Fatal("expected the blocked INSERT to be killed by lock_timeout")
	}

	var pge *pgconn.PgError
	if !errors.As(err, &pge) {
		t.Fatalf("expected a *pgconn.PgError, got %T: %v", err, err)
	}
	t.Logf("OBSERVED on this server: SQLSTATE=%s message=%q", pge.Code, pge.Message)
	if pge.Code != "55P03" {
		t.Fatalf("lock_timeout reported SQLSTATE %s (%q); this package counts casualties on 55P03", pge.Code, pge.Message)
	}
	if !strings.Contains(pge.Message, "lock timeout") {
		t.Errorf("unexpected message for 55P03: %q", pge.Message)
	}
	if got := Classify(err); got != OutcomeDied {
		t.Fatalf("Classify of a real lock timeout = %s, want died", got)
	}
}

// TestLockTimeoutAppliesToPoolOnly proves the RuntimeParam landed on the load
// pool and, just as importantly, that statement_timeout was NOT set: a write
// that is slow but unblocked must survive.
func TestLockTimeoutAppliesToPoolOnly(t *testing.T) {
	dsn := newScratchDB(t)
	ctx := context.Background()

	g, err := New(ctx, Config{DSN: dsn, Workers: 2, LockTimeout: 900 * time.Millisecond, Logger: quietLogger()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Stop(ctx)

	var lockTimeout, stmtTimeout string
	if err := g.pool.QueryRow(ctx, `SELECT current_setting('lock_timeout'), current_setting('statement_timeout')`).
		Scan(&lockTimeout, &stmtTimeout); err != nil {
		t.Fatalf("read settings: %v", err)
	}
	if lockTimeout != "900ms" {
		t.Errorf("lock_timeout = %q, want 900ms", lockTimeout)
	}
	if stmtTimeout != "0" {
		t.Fatalf("statement_timeout = %q, want 0 — a slow write must never be scored as a casualty", stmtTimeout)
	}

	// A statement far slower than lock_timeout, but blocked on nothing, must
	// succeed. This is the attribution-integrity invariant, executed.
	if _, err := g.pool.Exec(ctx, `SELECT pg_sleep(2)`); err != nil {
		t.Fatalf("slow-but-unblocked statement failed: %v", err)
	}

	// A separate connection to the same database must be untouched.
	other, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect other: %v", err)
	}
	defer other.Close(ctx)
	var otherLock string
	if err := other.QueryRow(ctx, `SELECT current_setting('lock_timeout')`).Scan(&otherLock); err != nil {
		t.Fatalf("read other lock_timeout: %v", err)
	}
	if otherLock != "0" {
		t.Errorf("non-loadgen connection has lock_timeout %q; it must stay unset", otherLock)
	}
}

func TestSustainsWrites(t *testing.T) {
	dsn := newScratchDB(t)
	ctx := context.Background()

	g, err := New(ctx, Config{DSN: dsn, Workers: 8, LockTimeout: 500 * time.Millisecond, Logger: quietLogger()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := g.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	var samples []Stats
	for i := 0; i < 3; i++ {
		time.Sleep(time.Second)
		samples = append(samples, g.Snapshot())
	}
	stopCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := g.Stop(stopCtx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	for i := 1; i < len(samples); i++ {
		if totalOK(samples[i]) <= totalOK(samples[i-1]) {
			t.Fatalf("writes did not rise between samples %d (%d) and %d (%d)",
				i-1, totalOK(samples[i-1]), i, totalOK(samples[i]))
		}
	}
	last := samples[len(samples)-1]
	if last.CasualtiesTotal != 0 {
		t.Errorf("casualties = %d with no migration running; must be 0", last.CasualtiesTotal)
	}
	if last.ErroredTotal != 0 {
		t.Errorf("errored = %d on a healthy database; must be 0", last.ErroredTotal)
	}
	if last.WritesPerSec <= 0 {
		t.Errorf("WritesPerSec = %d, want > 0", last.WritesPerSec)
	}
	for _, name := range []string{"checkout", "login", "add_to_cart"} {
		if last.ByLabel[name].OK == 0 {
			t.Errorf("label %q produced no writes", name)
		}
	}

	// Every counted OK is a committed row.
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	var rows int64
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM orders`).Scan(&rows); err != nil {
		t.Fatalf("count: %v", err)
	}
	final := g.Snapshot()
	// The generator may UNDER-count by at most one write per worker: a write
	// that commits on the server while Stop's cancellation is in flight comes
	// back as context.Canceled and is scored `stopped`, not `ok`. That bias is
	// deliberate — the alternative is guessing, and guessing is how a headline
	// number stops being trustworthy. It must never OVER-count.
	if rows < totalOK(final) {
		t.Errorf("generator counted %d successes but only %d rows exist — successes are being over-counted", totalOK(final), rows)
	}
	if slack := rows - totalOK(final); slack > int64(8) {
		t.Errorf("uncounted committed rows = %d, expected at most one per worker (8)", slack)
	}
	t.Logf("3s of load: %d writes, %.0f/s average, casualties=%d errored=%d",
		totalOK(final), float64(totalOK(final))/3.0, final.CasualtiesTotal, final.ErroredTotal)
}

// TestCasualtiesUnderMigrationLock is THE money test: it reproduces what a
// careless migration does to live traffic and checks that the number we put on
// screen is the number the database actually inflicted.
func TestCasualtiesUnderMigrationLock(t *testing.T) {
	dsn := newScratchDB(t)
	ctx := context.Background()

	g, err := New(ctx, Config{DSN: dsn, Workers: 8, LockTimeout: 300 * time.Millisecond, Logger: quietLogger()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := g.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := g.Stop(stopCtx); err != nil {
			t.Errorf("Stop: %v", err)
		}
	}()

	// Baseline: healthy traffic, nobody dying.
	time.Sleep(1500 * time.Millisecond)
	before := g.Snapshot()
	if before.CasualtiesTotal != 0 {
		t.Fatalf("casualties before the lock = %d, want 0", before.CasualtiesTotal)
	}
	if before.WritesPerSec <= 0 {
		t.Fatalf("no traffic before the lock (WritesPerSec=%d)", before.WritesPerSec)
	}

	// The migration: a lock that conflicts with INSERT's ROW EXCLUSIVE, held
	// for 3 seconds.
	holder := beginLockHolder(t, dsn)
	time.Sleep(3 * time.Second)
	during := g.Snapshot()
	holder.rollback(t)

	if during.CasualtiesTotal <= before.CasualtiesTotal {
		t.Fatalf("casualties did not climb during the lock: before=%d during=%d",
			before.CasualtiesTotal, during.CasualtiesTotal)
	}
	if during.ErroredTotal != 0 {
		t.Errorf("errored = %d during a plain lock hold; lock deaths must not leak into the error bucket", during.ErroredTotal)
	}

	// Casualties must be attributable: the by-label numbers are what the UI
	// breaks down, and they must add up to the headline exactly.
	var sum int64
	labelled := 0
	for name, ls := range during.ByLabel {
		sum += ls.Died
		if ls.Died > 0 {
			labelled++
		}
		t.Logf("label %-12s ok=%-6d died=%-4d errored=%d", name, ls.OK, ls.Died, ls.Errored)
	}
	if sum != during.CasualtiesTotal {
		t.Fatalf("by-label casualties sum to %d, headline says %d", sum, during.CasualtiesTotal)
	}
	if labelled == 0 {
		t.Fatal("casualties were counted but no label owns them")
	}
	t.Logf("writes that still got through during the hold: %d", totalOK(during)-totalOK(before))
	t.Logf("3s lock hold: %d casualties, %d errored", during.CasualtiesTotal, during.ErroredTotal)

	// Recovery: with the lock released, traffic resumes and the death count
	// stops moving. This is the "and then it healed" half of the demo.
	time.Sleep(2500 * time.Millisecond)
	after := g.Snapshot()
	if totalOK(after) <= totalOK(during) {
		t.Fatalf("writes did not resume after ROLLBACK: during=%d after=%d", totalOK(during), totalOK(after))
	}
	if after.WritesPerSec <= 0 {
		t.Errorf("rate did not recover after ROLLBACK (WritesPerSec=%d)", after.WritesPerSec)
	}
	if after.CasualtiesTotal != during.CasualtiesTotal {
		t.Errorf("casualties kept climbing after the lock was released: during=%d after=%d",
			during.CasualtiesTotal, after.CasualtiesTotal)
	}
	if after.ErroredTotal != 0 {
		t.Errorf("errored = %d after recovery, want 0", after.ErroredTotal)
	}
}

// TestSurvivesDatabaseDisappearing covers the twin being dropped mid-run. The
// generator must keep trying and keep counting into the error bucket — never
// into casualties — and must still stop cleanly.
func TestSurvivesDatabaseDisappearing(t *testing.T) {
	dsn := newScratchDB(t)
	ctx := context.Background()

	g, err := New(ctx, Config{DSN: dsn, Workers: 4, LockTimeout: 300 * time.Millisecond, Logger: quietLogger()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := g.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	time.Sleep(time.Second)
	before := g.Snapshot()
	if totalOK(before) == 0 {
		t.Fatal("no writes before the twin was dropped")
	}

	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	dbName := strings.TrimPrefix(mustPath(dsn), "/")
	if _, err := admin.Exec(ctx, `DROP DATABASE IF EXISTS "`+dbName+`" WITH (FORCE)`); err != nil {
		t.Fatalf("drop twin: %v", err)
	}
	admin.Close(ctx)

	time.Sleep(2 * time.Second)
	after := g.Snapshot()
	if after.ErroredTotal == 0 {
		t.Error("twin vanished but nothing was counted as errored")
	}
	if after.CasualtiesTotal != 0 {
		t.Errorf("casualties = %d after the twin vanished; a missing database is not a lock death", after.CasualtiesTotal)
	}

	stopCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := g.Stop(stopCtx); err != nil {
		t.Fatalf("Stop after the twin vanished: %v", err)
	}
	t.Logf("after the twin was dropped: ok=%d errored=%d casualties=%d",
		totalOK(after), after.ErroredTotal, after.CasualtiesTotal)
}

// TestGracefulStopNoLeaksNoMisclassification checks the two things that make
// Stop trustworthy: nothing is left running, and the cancellation storm at
// shutdown is not scored as customer deaths.
func TestGracefulStopNoLeaksNoMisclassification(t *testing.T) {
	dsn := newScratchDB(t)
	ctx := context.Background()

	runtime.GC()
	baseline := runtime.NumGoroutine()

	g, err := New(ctx, Config{DSN: dsn, Workers: 16, LockTimeout: 500 * time.Millisecond, Logger: quietLogger()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := g.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	time.Sleep(1500 * time.Millisecond)

	// Stop mid-flight: every worker is inside or about to enter an Exec.
	stopCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := g.Stop(stopCtx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := g.Stop(stopCtx); err != nil {
		t.Fatalf("second Stop must be a no-op, got: %v", err)
	}

	s := g.Snapshot()
	if s.CasualtiesTotal != 0 {
		t.Errorf("graceful stop produced %d casualties; cancellation is not a lock death", s.CasualtiesTotal)
	}
	if s.ErroredTotal != 0 {
		t.Errorf("graceful stop produced %d errors; cancellation must be classified as stopped", s.ErroredTotal)
	}
	if totalOK(s) == 0 {
		t.Error("no successful writes recorded before stop")
	}

	// Counters must be frozen once Stop returns.
	first := g.Snapshot()
	time.Sleep(500 * time.Millisecond)
	if second := g.Snapshot(); totalOK(second) != totalOK(first) {
		t.Errorf("counters still moving after Stop: %d -> %d", totalOK(first), totalOK(second))
	}

	// pgx tears its own goroutines down asynchronously, so poll rather than
	// demanding the delta be zero on the first look.
	deadline := time.Now().Add(5 * time.Second)
	var delta int
	for {
		runtime.GC()
		delta = runtime.NumGoroutine() - baseline
		if delta <= 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if delta > 0 {
		buf := make([]byte, 1<<16)
		n := runtime.Stack(buf, true)
		t.Fatalf("goroutine leak: %d goroutine(s) still running after Stop\n%s", delta, buf[:n])
	}
}

func totalOK(s Stats) int64 {
	var n int64
	for _, ls := range s.ByLabel {
		n += ls.OK
	}
	return n
}

func mustPath(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		panic(err)
	}
	return u.Path
}

// lockHolder is the stand-in for a migration: a session sitting on a lock that
// conflicts with INSERT's ROW EXCLUSIVE.
type lockHolder struct {
	conn *pgx.Conn
	tx   pgx.Tx
}

func beginLockHolder(t *testing.T, dsn string) *lockHolder {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("lock holder connect: %v", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("lock holder begin: %v", err)
	}
	if _, err := tx.Exec(ctx, `LOCK TABLE orders IN SHARE MODE`); err != nil {
		t.Fatalf("lock holder LOCK TABLE: %v", err)
	}
	return &lockHolder{conn: conn, tx: tx}
}

func (h *lockHolder) rollback(t *testing.T) {
	t.Helper()
	if h.tx == nil {
		return
	}
	ctx := context.Background()
	if err := h.tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		t.Logf("lock holder rollback: %v", err)
	}
	h.tx = nil
	h.conn.Close(ctx)
}
