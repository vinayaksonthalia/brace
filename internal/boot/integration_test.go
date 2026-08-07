//go:build integration

// Integration tests for the bring-your-own-data boot path. They need a real
// PostgreSQL 16 superuser, because the claim under test is not "the parser
// parses" — it is that a lab pointed at a foreign schema, with traffic nobody
// wrote Go code for, still measures real casualties on that schema.
//
//	docker run --rm -d --name brace-dev-pg -p 5433:5432 \
//	  -e POSTGRES_HOST_AUTH_METHOD=trust postgres:16
//	TEST_ADMIN_URL=postgres://postgres@localhost:5433/postgres \
//	  go test -tags integration -race ./internal/boot/
//
// NOTE: this file drives the production boot path, so it uses the production
// names — it CREATES AND DROPS the database `brace_template` and everything
// matching `brace_run_` on the target server. Point it at a scratch container,
// never at anything you care about.
package boot

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/vinayaksonthalia/brace/internal/engine"
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
	conn.Close(bg)

	code := m.Run()

	if c, err := pgx.Connect(bg, adminURL); err == nil {
		if err := sweep(bg, c); err != nil {
			fmt.Fprintf(os.Stderr, "post-test sweep failed: %v\n", err)
		}
		c.Close(bg)
	}
	os.Exit(code)
}

func requireDB(t *testing.T) {
	t.Helper()
	if dbSkipMsg != "" {
		t.Skip(dbSkipMsg)
	}
}

// ---------------------------------------------------------------- the schema
//
// invoices is deliberately NOT the shipped orders table: the whole point of the
// exercise is that nothing in the Go code knows this schema exists.
//
// slow_key is how this suite gets a multi-second index build out of a table
// small enough to clone in a couple of seconds. Marking it IMMUTABLE is a lie
// PostgreSQL cannot check and is required for an expression index; it buys a
// deterministic lock hold instead of a five-million-row template and a flaky
// timing assumption.
const (
	// Measured on PG16: the expression index costs ~4.5ms per row (pg_sleep's
	// granularity dominates the requested 0.4ms), so 2000 rows buys a ~9s lock
	// hold — comfortably past the 1.5s lock_timeout that makes casualties, and
	// comfortably inside the engine's 90s run budget.
	byodRows = 2_000

	byodSchema = `
CREATE TABLE invoices (
	id          bigserial PRIMARY KEY,
	customer_id bigint    NOT NULL,
	cents       bigint    NOT NULL,
	memo        text      NOT NULL DEFAULT 'x'
);

CREATE FUNCTION slow_key(v bigint) RETURNS bigint
	LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
	PERFORM pg_sleep(0.0004);
	RETURN v;
END $$;
`

	// naiveIndex holds SHARE on invoices for as long as the expression takes to
	// evaluate over every row — a few seconds — which is far past the load
	// generator's lock_timeout. That is the outage being rehearsed.
	naiveIndex = `CREATE INDEX idx_invoices_slow ON invoices (slow_key(customer_id))`

	byodLabels = `checkout:3:INSERT INTO invoices (customer_id, cents) VALUES ($1, $2);` +
		`topup:1:UPDATE invoices SET cents = cents + $2 WHERE customer_id = $1`
)

// TestBYODFullPath is the feature, end to end: a template nobody seeded, a
// traffic mix nobody compiled in, and a real measurement of a real freeze on
// somebody else's tables.
func TestBYODFullPath(t *testing.T) {
	requireDB(t)
	ctx := context.Background()
	buildCustomTemplate(t, ctx)

	cfg := byodConfig(t, byodLabels)
	lab := openLab(t, ctx, cfg)

	// The template was verified, not seeded: the row estimate has to come from
	// the restored invoices table, and no orders table may have appeared.
	if lab.Template.Rows <= 0 {
		t.Errorf("Template.Rows = %d, want the restored table's estimate", lab.Template.Rows)
	}
	assertNoOrdersTable(t, ctx)

	res := rehearse(t, ctx, lab.Engine, naiveIndex)

	if res.Verdict == "" {
		t.Fatal("no verdict")
	}
	t.Logf("verdict=%s blocked=%.2fs casualties=%d errored=%d baseline=%d by_label=%v",
		res.Verdict, res.BlockedSeconds, res.Casualties.Total, res.ErroredTotal,
		res.BaselineWPS, res.Casualties.ByLabel)

	if res.Error != "" {
		t.Fatalf("the migration itself failed: %s", res.Error)
	}
	// The load ran against invoices, so a non-zero baseline is proof the custom
	// statements executed at all.
	if res.BaselineWPS <= 0 {
		t.Fatalf("baseline was %d writes/sec — the custom labels never wrote anything", res.BaselineWPS)
	}
	// The measurement that matters: writes to the user's own table were killed
	// waiting for the user's own migration's lock.
	if res.Casualties.Total <= 0 {
		t.Fatalf("casualties = 0 on a naive CREATE INDEX under custom load; blocked=%.2fs monitor_blocked=%.2fs",
			res.BlockedSeconds, res.MonitorBlockedSeconds)
	}
	if res.BlockedSeconds <= 0 {
		t.Errorf("blocked_seconds = %.2f, want a measured freeze", res.BlockedSeconds)
	}
	// Attribution: the casualties are reported under the names from the
	// environment variable, not under the shipped ones.
	for _, want := range []string{"checkout", "topup"} {
		if _, ok := res.Casualties.ByLabel[want]; !ok {
			t.Errorf("casualties.by_label has no %q; got %v", want, res.Casualties.ByLabel)
		}
	}
	for _, unwanted := range []string{"add_to_cart"} {
		if _, ok := res.Casualties.ByLabel[unwanted]; ok {
			t.Errorf("stock label %q leaked into a custom run: %v", unwanted, res.Casualties.ByLabel)
		}
	}
	// Errors of unknown origin would mean the statements were half-broken and
	// the casualty count is not what it appears to be.
	if res.ErroredTotal > 0 {
		t.Errorf("errored = %d on custom labels: the self-check should have caught whatever this is", res.ErroredTotal)
	}
}

// TestBYODBadLabelFailsBoot is the reason the self-check exists: without it,
// this configuration boots happily and every run reports zero casualties.
func TestBYODBadLabelFailsBoot(t *testing.T) {
	requireDB(t)
	ctx := context.Background()
	buildCustomTemplate(t, ctx)

	cfg := byodConfig(t, `checkout:3:INSERT INTO invoces (customer_id, cents) VALUES ($1, $2)`)
	lab, err := Open(ctx, cfg, nil)
	if lab != nil {
		lab.Close(ctx)
	}
	if err == nil {
		t.Fatal("Open accepted a label pointing at a table that does not exist")
	}
	msg := err.Error()
	for _, want := range []string{"checkout", "invoces", "BRACE_LOAD_LABELS"} {
		if !strings.Contains(msg, want) {
			t.Errorf("boot error does not mention %q: %s", want, msg)
		}
	}
}

// TestBYODStockLabelsAgainstForeignTemplate: BRACE_SEED=off with the traffic
// mix left alone is the same trap wearing a different hat — the built-in labels
// write to `orders`, which a restored dump almost certainly does not have, and
// every run would report zero casualties out of a flood of errors. The
// self-check has to catch our own statements too, and point at the fix.
func TestBYODStockLabelsAgainstForeignTemplate(t *testing.T) {
	requireDB(t)
	ctx := context.Background()
	buildCustomTemplate(t, ctx)

	cfg := byodConfig(t, "") // seed off, no custom labels
	lab, err := Open(ctx, cfg, nil)
	if lab != nil {
		lab.Close(ctx)
	}
	if err == nil {
		t.Fatal("Open accepted the stock orders traffic against a template with no orders table")
	}
	if !strings.Contains(err.Error(), "BRACE_LOAD_LABELS") {
		t.Errorf("error does not point at the fix: %v", err)
	}
	if !strings.Contains(err.Error(), "orders") {
		t.Errorf("error does not name the missing table: %v", err)
	}
}

// TestBYODMissingTemplate: the first thing a self-hoster gets wrong, and the
// error has to contain the recipe rather than a bare "database does not exist".
func TestBYODMissingTemplate(t *testing.T) {
	requireDB(t)
	ctx := context.Background()
	dropTemplate(t, ctx)

	cfg := byodConfig(t, "")
	lab, err := Open(ctx, cfg, nil)
	if lab != nil {
		lab.Close(ctx)
	}
	if err == nil {
		t.Fatal("Open succeeded with no template and BRACE_SEED=off")
	}
	if !errors.Is(err, ErrTemplateMissing) {
		t.Errorf("error is not ErrTemplateMissing: %v", err)
	}
	for _, want := range []string{"createdb", "pg_restore", TemplateName} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %q: %v", want, err)
		}
	}
	// The twins manager sets ALLOW_CONNECTIONS itself; telling the user to do it
	// too would be one more step that is wrong to follow.
	if strings.Contains(err.Error(), "ALLOW_CONNECTIONS false;") {
		t.Errorf("error instructs the user to lock the template, which Brace does itself: %v", err)
	}
}

// TestBYODEmptyTemplate: a template that exists but has no tables would clone
// fine and then fail every write. Fail at boot instead.
func TestBYODEmptyTemplate(t *testing.T) {
	requireDB(t)
	ctx := context.Background()
	dropTemplate(t, ctx)
	adminExec(t, ctx, `CREATE DATABASE `+quoteIdent(TemplateName))

	cfg := byodConfig(t, "")
	lab, err := Open(ctx, cfg, nil)
	if lab != nil {
		lab.Close(ctx)
	}
	if err == nil {
		t.Fatal("Open succeeded against a template with no user tables")
	}
	if !strings.Contains(err.Error(), "no user tables") {
		t.Errorf("unhelpful error: %v", err)
	}
}

// TestSeedStillWorks guards the other half of the switch: BRACE_SEED unset must
// still build the synthetic orders store exactly as before.
func TestSeedStillWorks(t *testing.T) {
	requireDB(t)
	ctx := context.Background()
	dropTemplate(t, ctx)

	cfg := byodConfig(t, "")
	cfg.Seed = true
	cfg.SeedRows = 1000
	lab := openLab(t, ctx, cfg)
	if lab.Template.Rows < 1000 {
		t.Errorf("Template.Rows = %d, want at least 1000 seeded rows", lab.Template.Rows)
	}

	// The manager keeps the template connection-free; reopen it to look.
	adminExec(t, ctx, `ALTER DATABASE `+quoteIdent(TemplateName)+` WITH ALLOW_CONNECTIONS true`)
	defer adminExec(t, ctx, `ALTER DATABASE `+quoteIdent(TemplateName)+` WITH ALLOW_CONNECTIONS false`)

	conn := connectTo(t, ctx, dsnForDB(adminURL, TemplateName))
	defer conn.Close(ctx)
	var n int64
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM orders`).Scan(&n); err != nil {
		t.Fatalf("orders table missing after a seeded boot: %v", err)
	}
	if n < 1000 {
		t.Errorf("orders has %d rows, want at least 1000", n)
	}
}

// ---------------------------------------------------------------- helpers

// byodConfig is what FromEnv would produce for the documented BYOD environment.
// It goes through FromEnv on purpose: the environment surface is the feature,
// and a test that hand-built a Config would not exercise it.
func byodConfig(t *testing.T, labels string) Config {
	t.Helper()
	t.Setenv("BRACE_ADMIN_URL", adminURL)
	t.Setenv("BRACE_SEED", "off")
	t.Setenv("SEED_ROWS", "")
	t.Setenv("BRACE_LOAD_LABELS", labels)
	t.Setenv("POOL_SIZE", "2")
	t.Setenv("WORKERS", "8")

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv: %v", err)
	}
	if cfg.Seed {
		t.Fatal("FromEnv did not honour BRACE_SEED=off")
	}
	cfg.Logger = testLogger(t)
	return cfg
}

func openLab(t *testing.T, ctx context.Context, cfg Config) *Lab {
	t.Helper()
	lab, err := Open(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("boot.Open: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		lab.Close(closeCtx)
	})
	return lab
}

// rehearse retries past ErrNoWarmTwin: the validation twin was consumed at boot
// and the background worker needs a moment to clone its replacement.
func rehearse(t *testing.T, ctx context.Context, eng *engine.Engine, sql string) *engine.RunResult {
	t.Helper()
	deadline := time.Now().Add(3 * time.Minute)
	for {
		runCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
		res, err := eng.Rehearse(runCtx, sql, engine.ModeNaive)
		cancel()
		if err == nil {
			return res
		}
		var noTwin *engine.NoWarmTwinError
		if !errors.As(err, &noTwin) || time.Now().After(deadline) {
			t.Fatalf("Rehearse: %v", err)
		}
		time.Sleep(time.Second)
	}
}

// buildCustomTemplate restores "the user's dump" — a schema Brace has never
// heard of — into the template database.
func buildCustomTemplate(t *testing.T, ctx context.Context) {
	t.Helper()
	dropTemplate(t, ctx)
	adminExec(t, ctx, `CREATE DATABASE `+quoteIdent(TemplateName))

	conn := connectTo(t, ctx, dsnForDB(adminURL, TemplateName))
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, byodSchema); err != nil {
		t.Fatalf("create custom schema: %v", err)
	}
	if _, err := conn.Exec(ctx, `
		INSERT INTO invoices (customer_id, cents)
		SELECT (random()*1000)::bigint + 1, (random()*5000)::bigint + 1
		FROM generate_series(1, $1)`, byodRows); err != nil {
		t.Fatalf("fill invoices: %v", err)
	}
	if _, err := conn.Exec(ctx, `ANALYZE invoices`); err != nil {
		t.Fatalf("analyze invoices: %v", err)
	}
}

func assertNoOrdersTable(t *testing.T, ctx context.Context) {
	t.Helper()
	// Reopening the template is safe here: the manager only needs it
	// connection-free while it clones, and this asserts against the template
	// rather than a twin because the template is what seeding would have
	// written to.
	adminExec(t, ctx, `ALTER DATABASE `+quoteIdent(TemplateName)+` WITH ALLOW_CONNECTIONS true`)
	defer adminExec(t, ctx, `ALTER DATABASE `+quoteIdent(TemplateName)+` WITH ALLOW_CONNECTIONS false`)

	conn := connectTo(t, ctx, dsnForDB(adminURL, TemplateName))
	defer conn.Close(ctx)
	var exists bool
	if err := conn.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='orders')`).Scan(&exists); err != nil {
		t.Fatalf("check for orders: %v", err)
	}
	if exists {
		t.Error("BRACE_SEED=off still created the synthetic orders table in the user's template")
	}
}

func dropTemplate(t *testing.T, ctx context.Context) {
	t.Helper()
	adminExec(t, ctx, `DROP DATABASE IF EXISTS `+quoteIdent(TemplateName)+` WITH (FORCE)`)
}

func adminExec(t *testing.T, ctx context.Context, sql string) {
	t.Helper()
	conn := connectTo(t, ctx, adminURL)
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, sql); err != nil {
		t.Fatalf("admin exec %q: %v", sql, err)
	}
}

func connectTo(t *testing.T, ctx context.Context, dsn string) *pgx.Conn {
	t.Helper()
	connCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(connCtx, dsn)
	if err != nil {
		t.Fatalf("connect %s: %v", dsn, err)
	}
	return conn
}

// sweep removes everything this suite (and the boot path it drives) can leave
// behind, so a crashed run does not poison the next one.
func sweep(ctx context.Context, conn *pgx.Conn) error {
	rows, err := conn.Query(ctx,
		`SELECT datname FROM pg_database WHERE starts_with(datname, 'brace_run_') OR datname = $1`,
		TemplateName)
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
		if _, err := conn.Exec(ctx, `DROP DATABASE IF EXISTS `+quoteIdent(n)+` WITH (FORCE)`); err != nil {
			return fmt.Errorf("drop %q: %w", n, err)
		}
	}
	return nil
}

func waitForPostgres(ctx context.Context, dsn string, budget time.Duration) (*pgx.Conn, error) {
	deadline := time.Now().Add(budget)
	var lastErr error
	for {
		conn, err := pgx.Connect(ctx, dsn)
		if err == nil {
			return conn, nil
		}
		lastErr = err
		if time.Now().After(deadline) || ctx.Err() != nil {
			return nil, lastErr
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func testLogger(t *testing.T) *slog.Logger {
	if testing.Verbose() {
		return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
