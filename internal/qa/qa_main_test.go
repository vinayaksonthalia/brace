//go:build integration

// Package qa holds hostile, black-box attack tests against the committed
// packages. Nothing in here is imported by production code; it exists only to
// try to break internal/twins and internal/loadgen from the outside.
//
//	docker run --rm -d --name brace-dev-pg -p 5433:5432 \
//	  -e POSTGRES_HOST_AUTH_METHOD=trust postgres:16
//	go test -race -tags integration ./internal/qa/
package qa

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	// qaTemplate is a tiny template: clones finish in milliseconds.
	qaTemplate = "brace_qa_template"
	// qaFat is a deliberately chunky template so a clone stays in flight long
	// enough to be interrupted by Close.
	qaFat = "brace_qa_fat"
	// qaPrefix is the twin namespace these attacks own.
	qaPrefix = "brace_qa_run_"
	// qaScratchPrefix is for standalone loadgen target databases.
	qaScratchPrefix = "brace_qa_lg_"
)

var (
	adminURL  string
	dbSkipMsg string
	fatClone  time.Duration
)

func TestMain(m *testing.M) {
	adminURL = os.Getenv("TEST_ADMIN_URL")
	if adminURL == "" {
		adminURL = "postgres://postgres@localhost:5433/postgres"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Second)
	conn, err := waitForPostgres(ctx, adminURL, 60*time.Second)
	cancel()
	if err != nil {
		dbSkipMsg = fmt.Sprintf("PostgreSQL at %s unreachable: %v", adminURL, err)
		os.Exit(m.Run())
	}

	bg := context.Background()

	// A re-exec'd child (used to observe a crash without taking the parent
	// suite down with it) must not touch the shared fixtures.
	if os.Getenv("BRACE_QA_CHILD") == "1" {
		conn.Close(bg)
		os.Exit(m.Run())
	}

	if err := sweepQA(bg, conn); err != nil {
		fmt.Fprintf(os.Stderr, "qa cleanup failed: %v\n", err)
		os.Exit(1)
	}
	if err := buildTemplates(bg, conn); err != nil {
		fmt.Fprintf(os.Stderr, "qa fixture setup failed: %v\n", err)
		os.Exit(1)
	}
	conn.Close(bg)

	code := m.Run()

	if c, err := pgx.Connect(bg, adminURL); err == nil {
		if err := sweepQA(bg, c); err != nil {
			fmt.Fprintf(os.Stderr, "qa teardown failed: %v\n", err)
		}
		c.Close(bg)
	}
	os.Exit(code)
}

func waitForPostgres(ctx context.Context, dsn string, limit time.Duration) (*pgx.Conn, error) {
	deadline := time.Now().Add(limit)
	var lastErr error
	for {
		attempt, cancel := context.WithTimeout(ctx, 3*time.Second)
		conn, err := pgx.Connect(attempt, dsn)
		if err == nil {
			if err = conn.Ping(attempt); err == nil {
				cancel()
				return conn, nil
			}
			conn.Close(attempt)
		}
		cancel()
		lastErr = err
		if time.Now().After(deadline) || ctx.Err() != nil {
			return nil, lastErr
		}
		time.Sleep(time.Second)
	}
}

// sweepQA removes every database this suite could have created. It is scoped to
// the "brace_qa" namespace plus the "pg_2"-prefixed twins the malicious-prefix
// attack generates; it never touches template0/template1/postgres.
func sweepQA(ctx context.Context, conn *pgx.Conn) error {
	for _, p := range []string{"brace_qa", "pg_2"} {
		names, err := listPrefixed(ctx, conn, p)
		if err != nil {
			return err
		}
		for _, n := range names {
			if n == "postgres" || n == "template0" || n == "template1" {
				continue
			}
			if _, err := conn.Exec(ctx, `DROP DATABASE IF EXISTS `+quoteIdent(n)+` WITH (FORCE)`); err != nil {
				return fmt.Errorf("drop %q: %w", n, err)
			}
		}
	}
	return nil
}

func buildTemplates(ctx context.Context, conn *pgx.Conn) error {
	if _, err := conn.Exec(ctx, `CREATE DATABASE `+quoteIdent(qaTemplate)); err != nil {
		return err
	}
	tpl, err := pgx.Connect(ctx, dsnFor(qaTemplate))
	if err != nil {
		return err
	}
	if err := seedOrderSchema(ctx, tpl); err != nil {
		tpl.Close(ctx)
		return err
	}
	tpl.Close(ctx)

	// Fat template: ~90MB of heap so a clone takes long enough to interrupt.
	if _, err := conn.Exec(ctx, `CREATE DATABASE `+quoteIdent(qaFat)); err != nil {
		return err
	}
	fat, err := pgx.Connect(ctx, dsnFor(qaFat))
	if err != nil {
		return err
	}
	if _, err := fat.Exec(ctx,
		`CREATE TABLE ballast AS SELECT g AS id, repeat('x', 280) AS pad FROM generate_series(1, 300000) g`); err != nil {
		fat.Close(ctx)
		return err
	}
	fat.Close(ctx)

	// Measure a clone of the fat template once so the tests can size their
	// interrupt windows against reality instead of a guess.
	start := time.Now()
	if _, err := conn.Exec(ctx, `CREATE DATABASE brace_qa_probe TEMPLATE `+quoteIdent(qaFat)); err != nil {
		return err
	}
	fatClone = time.Since(start)
	if _, err := conn.Exec(ctx, `DROP DATABASE brace_qa_probe WITH (FORCE)`); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "qa: fat template clone takes %s\n", fatClone)
	return nil
}

// seedOrderSchema creates the two tables the loadgen labels write to: `orders`
// is the one migrations lock, `slow_orders` is deliberately never locked so a
// slow-but-unblocked write can be told apart from a lock casualty.
func seedOrderSchema(ctx context.Context, conn *pgx.Conn) error {
	stmts := []string{
		`CREATE TABLE orders (id bigserial PRIMARY KEY, customer_id bigint NOT NULL, amount_cents bigint NOT NULL, created_at timestamptz NOT NULL DEFAULT now())`,
		`CREATE TABLE slow_orders (id bigserial PRIMARY KEY, customer_id bigint NOT NULL, amount_cents bigint NOT NULL)`,
		`INSERT INTO orders (customer_id, amount_cents) SELECT g, g*7 FROM generate_series(1, 500) g`,
	}
	for _, s := range stmts {
		if _, err := conn.Exec(ctx, s); err != nil {
			return fmt.Errorf("seed %q: %w", s, err)
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

func quoteIdent(name string) string { return pgx.Identifier{name}.Sanitize() }

func dsnFor(db string) string {
	u, err := url.Parse(adminURL)
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

func adminConn(t *testing.T) *pgx.Conn {
	t.Helper()
	c, err := pgx.Connect(context.Background(), adminURL)
	if err != nil {
		t.Fatalf("admin connect: %v", err)
	}
	t.Cleanup(func() { c.Close(context.Background()) })
	return c
}

func dbExists(t *testing.T, c *pgx.Conn, name string) bool {
	t.Helper()
	var n int
	if err := c.QueryRow(context.Background(),
		`SELECT count(*) FROM pg_database WHERE datname = $1`, name).Scan(&n); err != nil {
		t.Fatalf("dbExists(%q): %v", name, err)
	}
	return n > 0
}

// createScratchDB makes a standalone loadgen target and returns its DSN.
func createScratchDB(t *testing.T, name string) string {
	t.Helper()
	ctx := context.Background()
	c := adminConn(t)
	if _, err := c.Exec(ctx, `DROP DATABASE IF EXISTS `+quoteIdent(name)+` WITH (FORCE)`); err != nil {
		t.Fatalf("pre-drop %q: %v", name, err)
	}
	if _, err := c.Exec(ctx, `CREATE DATABASE `+quoteIdent(name)); err != nil {
		t.Fatalf("create %q: %v", name, err)
	}
	t.Cleanup(func() {
		cc, err := pgx.Connect(context.Background(), adminURL)
		if err != nil {
			return
		}
		defer cc.Close(context.Background())
		_, _ = cc.Exec(context.Background(), `DROP DATABASE IF EXISTS `+quoteIdent(name)+` WITH (FORCE)`)
	})

	target, err := pgx.Connect(ctx, dsnFor(name))
	if err != nil {
		t.Fatalf("connect %q: %v", name, err)
	}
	defer target.Close(ctx)
	if err := seedOrderSchema(ctx, target); err != nil {
		t.Fatalf("seed %q: %v", name, err)
	}
	return dsnFor(name)
}

// countBackends reports how many server connections are open to a database.
// It is how we prove a pool really was (or was not) closed.
func countBackends(t *testing.T, c *pgx.Conn, db string) int {
	t.Helper()
	var n int
	if err := c.QueryRow(context.Background(),
		`SELECT count(*) FROM pg_stat_activity WHERE datname = $1`, db).Scan(&n); err != nil {
		t.Fatalf("countBackends: %v", err)
	}
	return n
}

func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	// Discard: these attacks intentionally generate error storms.
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// goroutineGate snapshots the goroutine count and returns a checker that waits
// for it to come back down. Every attack ends with one.
func goroutineGate(t *testing.T) func() {
	t.Helper()
	runtime.GC()
	before := runtime.NumGoroutine()
	return func() {
		t.Helper()
		const tolerance = 3
		deadline := time.Now().Add(10 * time.Second)
		var after int
		for {
			runtime.GC()
			after = runtime.NumGoroutine()
			if after <= before+tolerance || time.Now().After(deadline) {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if after > before+tolerance {
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			t.Errorf("goroutine leak: %d before, %d after (tolerance %d)\n%s",
				before, after, tolerance, buf[:n])
		}
	}
}
