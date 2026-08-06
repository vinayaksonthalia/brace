//go:build integration

// Integration tests for the twin lifecycle manager. They need a real
// PostgreSQL 16 superuser, because everything interesting here (CREATE
// DATABASE ... TEMPLATE, DROP ... WITH (FORCE), pg_terminate_backend) is
// impossible to fake convincingly.
//
//	docker run --rm -d --name brace-dev-pg -p 5433:5432 \
//	  -e POSTGRES_HOST_AUTH_METHOD=trust postgres:16
//	TEST_ADMIN_URL=postgres://postgres@localhost:5433/postgres \
//	  go test -tags integration ./internal/twins/
package twins

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	itTemplate = "brace_it_template"
	itPrefix   = "brace_run_"
	itRows     = 300
)

var (
	adminURL   string
	dbSkipMsg  string
	readyDelay = 60 * time.Second
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
		// The container may simply not be running on this machine; say so
		// loudly rather than failing and pretending the code is broken.
		dbSkipMsg = fmt.Sprintf("PostgreSQL at TEST_ADMIN_URL=%s is unreachable after %s: %v", adminURL, readyDelay, err)
		os.Exit(m.Run())
	}

	cleanCtx := context.Background()
	if err := resetFixtures(cleanCtx, conn); err != nil {
		fmt.Fprintf(os.Stderr, "integration fixture setup failed: %v\n", err)
		conn.Close(cleanCtx)
		os.Exit(1)
	}
	conn.Close(cleanCtx)

	code := m.Run()

	if c, err := pgx.Connect(cleanCtx, adminURL); err == nil {
		_ = dropAllTestDatabases(cleanCtx, c)
		c.Close(cleanCtx)
	}
	os.Exit(code)
}

// waitForPostgres polls the way pg_isready would: the container is often still
// booting when `go test` starts.
func waitForPostgres(ctx context.Context, url string, limit time.Duration) (*pgx.Conn, error) {
	deadline := time.Now().Add(limit)
	var lastErr error
	for {
		attemptCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		conn, err := pgx.Connect(attemptCtx, url)
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

// resetFixtures builds a small template from scratch so the suite is
// self-contained and repeatable even after a crashed previous run.
func resetFixtures(ctx context.Context, conn *pgx.Conn) error {
	if err := dropAllTestDatabases(ctx, conn); err != nil {
		return err
	}
	if _, err := conn.Exec(ctx, `CREATE DATABASE `+quoteIdent(itTemplate)); err != nil {
		return fmt.Errorf("create template: %w", err)
	}

	tplConn, err := pgx.Connect(ctx, dsnForDB(adminURL, conn.Config().Config.Copy(), itTemplate))
	if err != nil {
		return fmt.Errorf("connect to template: %w", err)
	}
	defer tplConn.Close(ctx)

	if _, err := tplConn.Exec(ctx, `CREATE TABLE ledger (id bigserial PRIMARY KEY, note text NOT NULL)`); err != nil {
		return fmt.Errorf("create fixture table: %w", err)
	}
	if _, err := tplConn.Exec(ctx,
		`INSERT INTO ledger (note) SELECT 'row ' || g FROM generate_series(1, $1) g`, itRows); err != nil {
		return fmt.Errorf("seed fixture rows: %w", err)
	}
	return nil
}

func dropAllTestDatabases(ctx context.Context, conn *pgx.Conn) error {
	names, err := listPrefixed(ctx, conn, itPrefix)
	if err != nil {
		return err
	}
	names = append(names, itTemplate)
	for _, n := range names {
		if _, err := conn.Exec(ctx, `DROP DATABASE IF EXISTS `+quoteIdent(n)+` WITH (FORCE)`); err != nil {
			return fmt.Errorf("drop %q: %w", n, err)
		}
	}
	return nil
}

func listPrefixed(ctx context.Context, conn *pgx.Conn, prefix string) ([]string, error) {
	rows, err := conn.Query(ctx,
		`SELECT datname FROM pg_database WHERE starts_with(datname, $1) ORDER BY datname`, prefix)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

func adminConnect(t *testing.T) *pgx.Conn {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatalf("admin connect: %v", err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}

// unlockTemplate re-opens the template for connections. Only the tests do this;
// the Manager only ever locks.
func unlockTemplate(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	if _, err := conn.Exec(context.Background(),
		`ALTER DATABASE `+quoteIdent(itTemplate)+` WITH ALLOW_CONNECTIONS true`); err != nil {
		t.Fatalf("unlock template: %v", err)
	}
}

func newIntegrationManager(t *testing.T, poolSize int) *Manager {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	m, err := NewManager(ctx, Config{
		AdminURL:     adminURL,
		TemplateName: itTemplate,
		PoolSize:     poolSize,
		NamePrefix:   itPrefix,
		Logger:       testLogger(t),
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	return m
}

// tWriter routes the manager's slog output into the test log, so a failing run
// shows the twin.warming/twin.ready/twin.dropped trail that explains it. After
// the test finishes it falls back to stderr: the background worker can still be
// draining when t is no longer usable.
type tWriter struct {
	t    *testing.T
	done *atomic.Bool
}

func (w tWriter) Write(p []byte) (int, error) {
	if w.done.Load() {
		return os.Stderr.Write(p)
	}
	w.t.Logf("%s", bytes.TrimRight(p, "\n"))
	return len(p), nil
}

func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	done := &atomic.Bool{}
	t.Cleanup(func() { done.Store(true) })
	return slog.New(slog.NewTextHandler(tWriter{t: t, done: done}, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func waitForWarm(t *testing.T, m *Manager, want int, limit time.Duration) PoolStatus {
	t.Helper()
	deadline := time.Now().Add(limit)
	var s PoolStatus
	for time.Now().Before(deadline) {
		s = m.Status()
		if s.Warm == want {
			return s
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for warm=%d; last status %+v", want, s)
	return s
}

func databaseExists(t *testing.T, conn *pgx.Conn, name string) bool {
	t.Helper()
	var exists bool
	err := conn.QueryRow(context.Background(),
		`SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)`, name).Scan(&exists)
	if err != nil {
		t.Fatalf("check database %q: %v", name, err)
	}
	return exists
}

// TestFullLifecycle walks fill -> acquire -> use -> release -> refill, which is
// exactly the loop a rehearsal run drives.
func TestFullLifecycle(t *testing.T) {
	requireDB(t)
	admin := adminConnect(t)

	m := newIntegrationManager(t, 2)
	defer m.Close(context.Background())

	s := waitForWarm(t, m, 2, 60*time.Second)
	if s.Target != 2 || s.CheckedOut != 0 {
		t.Fatalf("unexpected status after fill: %+v", s)
	}
	for _, n := range s.WarmNames {
		if !databaseExists(t, admin, n) {
			t.Fatalf("warm twin %q is not in pg_database", n)
		}
	}

	// The template must have been pinned shut by startup.
	var allowConn bool
	if err := admin.QueryRow(context.Background(),
		`SELECT datallowconn FROM pg_database WHERE datname = $1`, itTemplate).Scan(&allowConn); err != nil {
		t.Fatalf("read datallowconn: %v", err)
	}
	if allowConn {
		t.Error("template should be ALLOW_CONNECTIONS false after startup")
	}

	start := time.Now()
	tw, err := m.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Acquire took %s; it must never wait on a clone", elapsed)
	}

	// The DSN handed back must actually work, and carry the template's data.
	twinConn, err := pgx.Connect(context.Background(), tw.DSN)
	if err != nil {
		t.Fatalf("connect to twin via DSN %q: %v", tw.DSN, err)
	}
	var n int
	if err := twinConn.QueryRow(context.Background(), `SELECT count(*) FROM ledger`).Scan(&n); err != nil {
		t.Fatalf("query twin: %v", err)
	}
	if n != itRows {
		t.Errorf("twin has %d rows, want %d from the template", n, itRows)
	}
	// Writing to a twin must not be able to touch the template.
	if _, err := twinConn.Exec(context.Background(), `INSERT INTO ledger (note) VALUES ('written by test')`); err != nil {
		t.Fatalf("write to twin: %v", err)
	}
	twinConn.Close(context.Background())

	if got := m.Status(); got.CheckedOut != 1 || got.Warm != 1 {
		t.Fatalf("status after acquire: %+v", got)
	}

	// Release with a live connection still open, to prove WITH (FORCE) works.
	straggler, err := pgx.Connect(context.Background(), tw.DSN)
	if err != nil {
		t.Fatalf("open straggler connection: %v", err)
	}
	defer straggler.Close(context.Background())

	if err := m.Release(context.Background(), tw); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if databaseExists(t, admin, tw.Name) {
		t.Errorf("twin %q still exists after Release", tw.Name)
	}
	if got := m.Status(); got.CheckedOut != 0 {
		t.Fatalf("status after release: %+v", got)
	}

	// Release must trigger a refill back to target.
	waitForWarm(t, m, 2, 60*time.Second)

	// Releasing again is tolerated: the database is already gone.
	if err := m.Release(context.Background(), tw); err != nil {
		t.Errorf("second Release should tolerate a missing database, got %v", err)
	}
}

// TestOrphanSweep covers crash recovery: a previous process died mid-run and
// left twins behind that nothing is tracking.
func TestOrphanSweep(t *testing.T) {
	requireDB(t)
	admin := adminConnect(t)

	const orphan = itPrefix + "orphan"
	if _, err := admin.Exec(context.Background(), `CREATE DATABASE `+quoteIdent(orphan)); err != nil {
		t.Fatalf("create fake orphan: %v", err)
	}
	if !databaseExists(t, admin, orphan) {
		t.Fatal("fake orphan was not created")
	}

	m := newIntegrationManager(t, 1)
	defer m.Close(context.Background())

	if databaseExists(t, admin, orphan) {
		t.Errorf("orphan %q survived startup sweep", orphan)
	}
	// The sweep must not have eaten the template along with the orphans.
	if !databaseExists(t, admin, itTemplate) {
		t.Fatal("sweep destroyed the template")
	}
	waitForWarm(t, m, 1, 60*time.Second)
}

// TestCloneWithLingeringTemplateConnection is the failure this package exists
// to prevent: a stale session on the template makes CREATE DATABASE fail with
// 55006. ALLOW_CONNECTIONS false does not evict sessions that already exist,
// so the manager has to terminate them.
func TestCloneWithLingeringTemplateConnection(t *testing.T) {
	requireDB(t)
	admin := adminConnect(t)

	unlockTemplate(t, admin)
	lingering, err := pgx.Connect(context.Background(), dsnForDB(adminURL, admin.Config().Config.Copy(), itTemplate))
	if err != nil {
		t.Fatalf("open lingering template connection: %v", err)
	}
	defer lingering.Close(context.Background())

	// Hold a real session, not just an idle handshake.
	var one int
	if err := lingering.QueryRow(context.Background(), `SELECT 1`).Scan(&one); err != nil {
		t.Fatalf("prime lingering connection: %v", err)
	}

	var before int
	if err := admin.QueryRow(context.Background(),
		`SELECT count(*) FROM pg_stat_activity WHERE datname = $1`, itTemplate).Scan(&before); err != nil {
		t.Fatalf("count template backends: %v", err)
	}
	if before == 0 {
		t.Fatal("expected at least one session on the template before starting the manager")
	}

	m := newIntegrationManager(t, 1)
	defer m.Close(context.Background())

	// If the terminate step were missing, this would never reach warm=1.
	waitForWarm(t, m, 1, 60*time.Second)

	var after int
	if err := admin.QueryRow(context.Background(),
		`SELECT count(*) FROM pg_stat_activity WHERE datname = $1`, itTemplate).Scan(&after); err != nil {
		t.Fatalf("count template backends after clone: %v", err)
	}
	if after != 0 {
		t.Errorf("%d sessions still on the template after clone", after)
	}

	// And the lingering connection is genuinely dead.
	if err := lingering.QueryRow(context.Background(), `SELECT 1`).Scan(&one); err == nil {
		t.Error("lingering template connection should have been terminated")
	}
}

// TestCloseLeavesNothingBehind is the guarantee an operator cares about: after
// Close, no twin database and no goroutine survives.
func TestCloseLeavesNothingBehind(t *testing.T) {
	requireDB(t)
	admin := adminConnect(t)

	goroutinesBefore := runtime.NumGoroutine()

	m := newIntegrationManager(t, 2)
	waitForWarm(t, m, 2, 60*time.Second)

	// Hold one twin checked out: Close must drop those too, not just warm ones.
	tw, err := m.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	held, err := pgx.Connect(context.Background(), tw.DSN)
	if err != nil {
		t.Fatalf("connect to held twin: %v", err)
	}
	defer held.Close(context.Background())

	if err := m.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := m.Close(context.Background()); err != nil {
		t.Fatalf("Close must be idempotent, second call gave %v", err)
	}

	remaining, err := listPrefixed(context.Background(), admin, itPrefix)
	if err != nil {
		t.Fatalf("list remaining twins: %v", err)
	}
	if len(remaining) != 0 {
		t.Errorf("Close left %d twin databases behind: %v", len(remaining), remaining)
	}

	if s := m.Status(); !s.Closed || s.Warm != 0 || s.CheckedOut != 0 {
		t.Errorf("status after Close: %+v", s)
	}
	if _, err := m.Acquire(context.Background()); !errors.Is(err, ErrClosed) {
		t.Errorf("Acquire after Close = %v, want ErrClosed", err)
	}

	// Let pgxpool's own teardown settle before counting.
	var goroutinesAfter int
	for i := 0; i < 40; i++ {
		time.Sleep(50 * time.Millisecond)
		goroutinesAfter = runtime.NumGoroutine()
		if goroutinesAfter <= goroutinesBefore+1 {
			break
		}
	}
	if goroutinesAfter > goroutinesBefore+1 {
		buf := make([]byte, 1<<16)
		buf = buf[:runtime.Stack(buf, true)]
		t.Errorf("goroutine leak: %d before, %d after\n%s", goroutinesBefore, goroutinesAfter, buf)
	}
}

// TestStartupRejectsMissingTemplate and friends: startup must fail fast and
// loudly rather than run a lab with no template.
func TestStartupValidation(t *testing.T) {
	requireDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	t.Run("missing template", func(t *testing.T) {
		_, err := NewManager(ctx, Config{
			AdminURL:     adminURL,
			TemplateName: "brace_no_such_template",
			NamePrefix:   itPrefix,
			Logger:       testLogger(t),
		})
		if err == nil {
			t.Fatal("expected an error for a nonexistent template")
		}
	})

	t.Run("unreachable admin url", func(t *testing.T) {
		_, err := NewManager(ctx, Config{
			AdminURL:     "postgres://postgres@127.0.0.1:1/postgres?connect_timeout=2",
			TemplateName: itTemplate,
			NamePrefix:   itPrefix,
			Logger:       testLogger(t),
		})
		if err == nil {
			t.Fatal("expected an error for an unreachable admin URL")
		}
		var pgErr *pgconn.PgError
		_ = errors.As(err, &pgErr) // just documenting that a driver error is fine here
	})

	t.Run("admin url pointing at a twin namespace", func(t *testing.T) {
		_, err := NewManager(ctx, Config{
			AdminURL:     dsnForDB(adminURL, mustParse(t, adminURL), itPrefix+"whatever"),
			TemplateName: itTemplate,
			NamePrefix:   itPrefix,
			Logger:       testLogger(t),
		})
		if err == nil {
			t.Fatal("expected an error when the admin URL is not the maintenance database")
		}
	})
}

func mustParse(t *testing.T, url string) *pgconn.Config {
	t.Helper()
	cfg, err := pgconn.ParseConfig(url)
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	return cfg
}
