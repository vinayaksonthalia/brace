//go:build integration

// Integration tests for the orchestrator. They need a real PostgreSQL 16
// superuser, because everything worth asserting here — that a naive CREATE
// INDEX freezes live writes, that CONCURRENTLY does not, that an unprivileged
// role is genuinely unable to reach the filesystem — is a claim about what the
// server does, and no fake can testify to that.
//
//	docker run --rm -d --name brace-dev-pg -p 5433:5432 \
//	  -e POSTGRES_HOST_AUTH_METHOD=trust postgres:16
//	TEST_ADMIN_URL=postgres://postgres@localhost:5433/postgres \
//	  go test -tags integration -race ./internal/engine/
package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/vinayaksonthalia/brace/internal/twins"
)

const (
	// itNamespace is the prefix EVERYTHING this file creates shares, so a single
	// sweep at start and end leaves the server exactly as it was found.
	itNamespace  = "brace_eng_"
	templateName = itNamespace + "template"
	appDBName    = itNamespace + "app"

	// templateRows is chosen so that a plain CREATE INDEX takes long enough to
	// be visible against 100ms sampling, while a clone of the template stays
	// fast enough that the suite is not dominated by CREATE DATABASE.
	templateRows = 500_000
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
		_, _ = c.Exec(bg, `DROP ROLE IF EXISTS `+DefaultMigratorRole)
		_, _ = c.Exec(bg, `DROP ROLE IF EXISTS `+DefaultTrafficRole)
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

// sweep drops every database in the test namespace. Databases are the only
// thing these tests leave on disk, and leaving one behind would poison the next
// run's template clone.
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
		// A template is held at ALLOW_CONNECTIONS false; DROP does not care,
		// but WITH (FORCE) is what handles a load generator still winding down.
		if _, err := conn.Exec(ctx, `DROP DATABASE IF EXISTS "`+n+`" WITH (FORCE)`); err != nil {
			return fmt.Errorf("drop %q: %w", n, err)
		}
	}
	return nil
}

// buildTemplate creates the application database and the seeded template every
// twin is cloned from.
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
		id           bigserial primary key,
		customer_id  bigint not null,
		amount_cents bigint not null,
		created_at   timestamptz not null default now()
	)`); err != nil {
		return fmt.Errorf("create orders: %w", err)
	}
	start := time.Now()
	if _, err := conn.Exec(ctx, `
		INSERT INTO orders (customer_id, amount_cents)
		SELECT (random()*100000)::bigint + 1, (random()*50000)::bigint + 1
		FROM generate_series(1, $1)`, templateRows); err != nil {
		return fmt.Errorf("seed orders: %w", err)
	}
	if _, err := conn.Exec(ctx, `ANALYZE orders`); err != nil {
		return fmt.Errorf("analyze: %w", err)
	}
	fmt.Fprintf(os.Stderr, "template seeded: %d rows in %v\n", templateRows, time.Since(start).Round(time.Millisecond))
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

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func requireDB(t *testing.T) {
	t.Helper()
	if dbSkipMsg != "" {
		t.Skip(dbSkipMsg)
	}
}

// newLab builds a private twin pool plus an Engine on it. Each test gets its
// own NamePrefix, because twins.NewManager sweeps its whole namespace at
// startup and would otherwise destroy another lab's twins.
func newLab(t *testing.T, prefix string, poolSize int) (*twins.Manager, *Engine) {
	t.Helper()
	requireDB(t)
	ctx := context.Background()

	mgr, err := twins.NewManager(ctx, twins.Config{
		AdminURL:     appURL,
		TemplateName: templateName,
		PoolSize:     poolSize,
		NamePrefix:   itNamespace + prefix + "_",
		Logger:       quietLogger(),
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

	eng, err := New(ctx, Config{
		AdminURL: appURL,
		Twins:    mgr,
		Workers:  12,
		// The twin's real size, which is the denominator of every scale
		// projection. Without it the engine correctly refuses to project.
		TwinRows: templateRows,
		// A tight request deadline, so a freeze of a few hundred milliseconds
		// produces the casualties a production stack would suffer.
		LockTimeout:      200 * time.Millisecond,
		SettleDuration:   1500 * time.Millisecond,
		RecoveryDuration: time.Second,
		SampleInterval:   100 * time.Millisecond,
		Logger:           quietLogger(),
	})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })

	waitWarm(t, mgr, poolSize)
	return mgr, eng
}

func waitWarm(t *testing.T, mgr *twins.Manager, want int) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for {
		s := mgr.Status()
		if s.Warm >= want {
			return
		}
		if s.LastError != "" {
			t.Fatalf("twin pool failed to warm: %s", s.LastError)
		}
		if time.Now().After(deadline) {
			t.Fatalf("twin pool did not reach %d warm twins (status %+v)", want, s)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func countDatabases(t *testing.T, prefix string) int {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	defer conn.Close(ctx)
	var n int
	if err := conn.QueryRow(ctx,
		`SELECT count(*) FROM pg_database WHERE starts_with(datname, $1)`, prefix).Scan(&n); err != nil {
		t.Fatalf("count databases: %v", err)
	}
	return n
}

// ---------------------------------------------------------------------------
// The crown: a full rehearsal of the dangerous form.
// ---------------------------------------------------------------------------

func TestRehearseNaiveCreateIndex(t *testing.T) {
	mgr, eng := newLab(t, "naive", 1)
	ctx := context.Background()

	res, err := eng.Rehearse(ctx, `CREATE INDEX idx_orders_customer_id ON orders (customer_id)`, ModeNaive)
	if err != nil {
		t.Fatalf("Rehearse: %v", err)
	}

	t.Logf("verdict=%s blocked=%.3fs monitor_blocked=%.3fs duration=%.3fs baseline=%d/s casualties=%d errored=%d locks=%v",
		res.Verdict, res.BlockedSeconds, res.MonitorBlockedSeconds, res.DurationSeconds,
		res.BaselineWPS, res.Casualties.Total, res.ErroredTotal, res.LockModes)

	// --- the pipeline ---
	if res.Error != "" {
		t.Fatalf("the migration itself failed: %s", res.Error)
	}
	if res.RunID == "" || res.Twin == "" {
		t.Errorf("run identity missing: id=%q twin=%q", res.RunID, res.Twin)
	}
	if res.BaselineWPS <= 0 {
		t.Fatalf("no baseline was measured (%d/s) — every downstream number depends on it", res.BaselineWPS)
	}
	if len(res.Timeline) == 0 {
		t.Fatal("the timeline is empty; the UI has nothing to replay")
	}
	if res.DDLStartT <= 0 || res.DDLEndT < res.DDLStartT {
		t.Errorf("DDL window is nonsense: %.3f..%.3f", res.DDLStartT, res.DDLEndT)
	}
	if res.DurationSeconds <= 0 {
		t.Errorf("duration = %v", res.DurationSeconds)
	}
	if res.Classification != ClassCreateIndex {
		t.Errorf("classification = %q, want %q", res.Classification, ClassCreateIndex)
	}
	if res.Explanation == "" {
		t.Error("a verdict shipped with no teach text")
	}
	if res.Suggestion == "" {
		t.Error("the dangerous form must suggest the safe rewrite")
	}
	if res.Verdict != VerdictSafe && res.Verdict != VerdictUnsafe {
		t.Errorf("verdict = %q", res.Verdict)
	}

	// --- attribution integrity ---
	var sum int64
	for _, n := range res.Casualties.ByLabel {
		sum += n
	}
	if sum != res.Casualties.Total {
		t.Errorf("by_label sums to %d but total says %d", sum, res.Casualties.Total)
	}
	if res.ErroredTotal != 0 {
		t.Errorf("errored = %d during a plain index build; unattributable failures mean something else broke",
			res.ErroredTotal)
	}
	if len(res.InvalidIndexes) != 0 {
		t.Errorf("a non-concurrent build left invalid indexes: %v", res.InvalidIndexes)
	}

	// --- the finding ---
	// A SHARE lock over live INSERT traffic must show up as SOMETHING: a
	// measured freeze, dead writes, or the server itself reporting waiters.
	// Which of the three dominates depends on how long the build takes at this
	// table size, so the assertion is the disjunction rather than a number the
	// hardware would have to hit.
	if res.BlockedSeconds == 0 && res.Casualties.Total == 0 && res.Monitor.BlockedSeconds == 0 {
		t.Errorf("a naive CREATE INDEX over live writes produced no harm at all "+
			"(blocked=%v casualties=%v monitor=%+v) — either the load never landed or the measurement is broken",
			res.BlockedSeconds, res.Casualties.Total, res.Monitor)
	}
	if res.Monitor.Samples == 0 {
		t.Errorf("the lock monitor took no samples: %+v", res.Monitor)
	}
	if mode := res.Monitor.StrongestLockMode; mode != "" && mode != "ShareLock" && mode != "AccessExclusiveLock" {
		t.Errorf("CREATE INDEX was seen holding %q; expected ShareLock", mode)
	}

	// --- the ledger ---
	if res.LedgerID == 0 {
		t.Fatal("no ledger row was written")
	}
	runs, err := eng.History(ctx, 10)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(runs) == 0 {
		t.Fatal("History is empty after a completed run")
	}
	rec := runs[0]
	if rec.ID != res.LedgerID {
		t.Errorf("newest ledger row is %d, run reported %d", rec.ID, res.LedgerID)
	}
	if rec.Verdict != res.Verdict || rec.CasualtiesTotal != res.Casualties.Total ||
		rec.BlockedSeconds != res.BlockedSeconds || rec.BaselineWPS != res.BaselineWPS {
		t.Errorf("ledger row disagrees with the result: %+v vs %+v", rec, res)
	}
	if rec.Mode != ModeNaive {
		t.Errorf("ledger mode = %q, want naive", rec.Mode)
	}
	if !strings.Contains(rec.SQL, "CREATE INDEX") {
		t.Errorf("ledger did not record the SQL: %q", rec.SQL)
	}
	if rec.CasualtiesByLabel == nil {
		t.Error("ledger lost the per-label breakdown")
	}

	// --- the twin is gone ---
	if s := mgr.Status(); s.CheckedOut != 0 {
		t.Errorf("twin still checked out after the run: %+v", s)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if !databaseExists(t, res.Twin) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("twin %q still exists after the run", res.Twin)
		}
		time.Sleep(100 * time.Millisecond)
	}

	// --- the engine is idle again ---
	st := eng.Status()
	if st.Running || st.CurrentRun != nil {
		t.Errorf("engine still reports a run in progress: %+v", st)
	}
	if st.LastBaselineWPS != res.BaselineWPS {
		t.Errorf("Status baseline = %d, run measured %d", st.LastBaselineWPS, res.BaselineWPS)
	}
}

func databaseExists(t *testing.T, name string) bool {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	defer conn.Close(ctx)
	var exists bool
	if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)`, name).Scan(&exists); err != nil {
		t.Fatalf("check database: %v", err)
	}
	return exists
}

// TestRehearseConcurrentlyIsSafe is the other half of the demo: the same index,
// built the safe way, must not freeze anything and must not leave debris.
func TestRehearseConcurrentlyIsSafe(t *testing.T) {
	_, eng := newLab(t, "conc", 1)
	ctx := context.Background()

	res, err := eng.Rehearse(ctx, `CREATE INDEX CONCURRENTLY idx_orders_customer_id ON orders (customer_id)`, ModeSafe)
	if err != nil {
		t.Fatalf("Rehearse: %v", err)
	}
	t.Logf("verdict=%s blocked=%.3fs monitor_blocked=%.3fs duration=%.3fs baseline=%d/s casualties=%d locks=%v",
		res.Verdict, res.BlockedSeconds, res.MonitorBlockedSeconds, res.DurationSeconds,
		res.BaselineWPS, res.Casualties.Total, res.LockModes)

	if res.Error != "" {
		// This is also the proof that the DDL runs in autocommit: CREATE INDEX
		// CONCURRENTLY refuses to run inside a transaction block.
		t.Fatalf("CONCURRENTLY failed — is the statement being wrapped in a transaction? %s", res.Error)
	}
	if res.Classification != ClassCreateIndexConcurrently {
		t.Errorf("classification = %q", res.Classification)
	}
	if !res.PredictedSafe {
		t.Error("CONCURRENTLY should be predicted safe")
	}
	if res.Verdict != VerdictSafe {
		t.Errorf("verdict = %s (blocked %.3fs); the safe rewrite must not freeze writes", res.Verdict, res.BlockedSeconds)
	}
	if res.Casualties.Total != 0 {
		t.Errorf("casualties = %d; CONCURRENTLY must kill nobody", res.Casualties.Total)
	}
	if len(res.InvalidIndexes) != 0 {
		t.Errorf("a successful CONCURRENTLY build left INVALID indexes: %v", res.InvalidIndexes)
	}
	if res.BaselineWPS <= 0 {
		t.Errorf("no baseline measured")
	}
	if len(res.Timeline) == 0 {
		t.Error("empty timeline")
	}
	if res.LedgerID == 0 {
		t.Error("no ledger row")
	}
}

// TestInvalidIndexIsDetected: a CONCURRENTLY build that fails leaves an index
// that is invisible to queries but still costs every write. Surfacing it is the
// point.
func TestInvalidIndexIsDetected(t *testing.T) {
	_, eng := newLab(t, "invalid", 1)
	ctx := context.Background()

	// customer_id is seeded with duplicates, so a UNIQUE build cannot succeed
	// and leaves an INVALID index behind.
	res, err := eng.Rehearse(ctx,
		`CREATE UNIQUE INDEX CONCURRENTLY idx_orders_unique_customer ON orders (customer_id)`, ModeCustom)
	if err != nil {
		t.Fatalf("Rehearse: %v", err)
	}
	t.Logf("error=%q invalid=%v note=%q", res.Error, res.InvalidIndexes, res.Note)

	if res.Error == "" {
		t.Fatal("a UNIQUE build over duplicate values should have failed")
	}
	if len(res.InvalidIndexes) == 0 {
		t.Error("the failed CONCURRENTLY build left no INVALID index — or we failed to look")
	}
	if res.Note == "" {
		t.Error("a failed run must explain itself")
	}
	// A failed migration is a finding, not an engine error: it still gets a
	// verdict, a ledger row and a released twin.
	if res.LedgerID == 0 {
		t.Error("a failed migration must still be recorded")
	}
}

// ---------------------------------------------------------------------------
// The security boundary. These are the assertions that matter most: the
// endpoint is public and the pre-filter is explicitly NOT the defence.
// ---------------------------------------------------------------------------

func TestMigratorRoleIsBoxedIn(t *testing.T) {
	mgr, eng := newLab(t, "sec", 1)
	ctx := context.Background()

	twin, err := mgr.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer func() {
		if err := mgr.Release(context.Background(), twin); err != nil {
			t.Errorf("Release: %v", err)
		}
	}()

	if err := grantOnTwin(ctx, twin.DSN, twin.Name, eng.cfg.MigratorRole); err != nil {
		t.Fatalf("grantOnTwin: %v", err)
	}
	conn, err := connectAsMigrator(ctx, twin.DSN, eng.cfg.MigratorRole, eng.migratorPassword)
	if err != nil {
		t.Fatalf("connect as migrator: %v", err)
	}
	defer conn.Close(context.Background())

	// The privileges it MUST have, or Brace cannot rehearse anything.
	for _, sql := range []string{
		`CREATE INDEX idx_sec ON orders (amount_cents)`,
		`DROP INDEX idx_sec`,
		`ALTER TABLE orders ADD COLUMN probe int`,
		`ALTER TABLE orders DROP COLUMN probe`,
		`ANALYZE orders`,
	} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Errorf("migrator cannot run %q: %v", sql, err)
		}
	}

	// The powers it MUST NOT have. Every one of these is denied by PostgreSQL
	// itself because the role is NOSUPERUSER and owns nothing outside the twin
	// — not by a regex we wrote.
	denied := []struct {
		name string
		sql  string
	}{
		{"COPY TO PROGRAM", `COPY orders TO PROGRAM 'touch /tmp/brace_pwned'`},
		{"COPY FROM PROGRAM", `COPY orders FROM PROGRAM 'echo 1,2'`},
		{"COPY to a server file", `COPY orders TO '/tmp/brace_pwned.csv'`},
		{"COPY from a server file", `COPY orders FROM '/etc/passwd'`},
		{"read a server file", `SELECT pg_read_file('/etc/passwd')`},
		{"list the data directory", `SELECT pg_ls_dir('.')`},
		{"DROP DATABASE", `DROP DATABASE "` + appDBName + `"`},
		{"DROP the twin it is inside", `DROP DATABASE "` + twin.Name + `"`},
		{"CREATE DATABASE", `CREATE DATABASE brace_eng_pwned`},
		{"CREATE ROLE", `CREATE ROLE pwned SUPERUSER LOGIN`},
		{"escalate itself", `ALTER ROLE ` + DefaultMigratorRole + ` SUPERUSER`},
		{"read the run ledger", `SELECT * FROM dblink('` + appURL + `', 'SELECT 1') AS t(x int)`},
	}
	for _, c := range denied {
		t.Run(c.name, func(t *testing.T) {
			if _, err := conn.Exec(ctx, c.sql); err == nil {
				t.Fatalf("SECURITY: the migrator role was permitted to %s (%q)", c.name, c.sql)
			} else {
				t.Logf("denied as expected: %v", err)
			}
		})
	}

	// The role's attributes, asserted directly rather than inferred.
	var super, createdb, createrole, replication, bypassrls bool
	if err := conn.QueryRow(ctx, `
		SELECT rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls
		FROM pg_roles WHERE rolname = $1`, eng.cfg.MigratorRole).
		Scan(&super, &createdb, &createrole, &replication, &bypassrls); err != nil {
		t.Fatalf("read role attributes: %v", err)
	}
	if super || createdb || createrole || replication || bypassrls {
		t.Errorf("SECURITY: migrator role attributes are too wide: super=%v createdb=%v createrole=%v repl=%v bypassrls=%v",
			super, createdb, createrole, replication, bypassrls)
	}

	// And it must not be able to reach the run ledger in the application
	// database, even though PUBLIC can connect there.
	appCfg, err := pgx.ParseConfig(appURL)
	if err != nil {
		t.Fatalf("parse app url: %v", err)
	}
	appCfg.User = eng.cfg.MigratorRole
	appCfg.Password = eng.migratorPassword
	appConn, err := pgx.ConnectConfig(ctx, appCfg)
	if err != nil {
		t.Logf("migrator cannot even connect to the app database: %v", err)
		return
	}
	defer appConn.Close(context.Background())
	if _, err := appConn.Exec(ctx, `SELECT * FROM brace_runs`); err == nil {
		t.Error("SECURITY: the migrator role can read the run ledger")
	} else {
		t.Logf("ledger read denied as expected: %v", err)
	}
	if _, err := appConn.Exec(ctx, `DROP TABLE brace_runs`); err == nil {
		t.Error("SECURITY: the migrator role can drop the run ledger")
	}
}

// TestMigratorPasswordIsRotated: every startup mints a new password, so a role
// left behind by a previous deploy cannot be used by whoever knew the old one.
func TestMigratorPasswordIsRotated(t *testing.T) {
	_, eng1 := newLab(t, "rot1", 1)
	first := eng1.migratorPassword
	if len(first) < 40 {
		t.Errorf("migrator password is only %d chars", len(first))
	}

	ctx := context.Background()
	eng2, err := New(ctx, Config{AdminURL: appURL, Twins: eng1.cfg.Twins, Logger: quietLogger()})
	if err != nil {
		t.Fatalf("second engine: %v", err)
	}
	defer eng2.Close()
	if eng2.migratorPassword == first {
		t.Error("the migrator password was not rotated on startup")
	}
}

// ---------------------------------------------------------------------------
// The error paths the API maps to status codes.
// ---------------------------------------------------------------------------

// TestRejectedStatementConsumesNoTwin: pre-filtering happens BEFORE Acquire,
// so a bad paste never costs an 80-second clone.
func TestRejectedStatementConsumesNoTwin(t *testing.T) {
	mgr, eng := newLab(t, "reject", 1)
	ctx := context.Background()

	before := mgr.Status()
	beforeDBs := countDatabases(t, itNamespace)

	for _, sql := range []string{
		`CREATE INDEX idx ON orders (customer_id); DROP TABLE orders`,
		`COPY orders TO PROGRAM 'sh -c id'`,
		`DROP DATABASE ` + appDBName,
		``,
		`ALTER TABLE orders ADD COLUMN flag boolean DEFAULT false`, // already_safe
	} {
		res, err := eng.Rehearse(ctx, sql, ModeCustom)
		if res != nil {
			t.Errorf("Rehearse(%q) returned a result for a rejected statement", sql)
		}
		var rej *RejectedError
		if !errors.As(err, &rej) {
			t.Fatalf("Rehearse(%q) returned %T (%v), want *RejectedError", sql, err, err)
		}
		if rej.Reason == "" {
			t.Errorf("Rehearse(%q) rejected with no reason", sql)
		}
	}

	after := mgr.Status()
	if after.CheckedOut != before.CheckedOut || after.Warm != before.Warm {
		t.Errorf("a rejected statement disturbed the pool: %+v -> %+v", before, after)
	}
	if got := countDatabases(t, itNamespace); got != beforeDBs {
		t.Errorf("database count changed from %d to %d on rejected statements", beforeDBs, got)
	}
	if eng.Status().Running {
		t.Error("engine is stuck reporting a run after a rejection")
	}
}

// TestNoWarmTwin: with the pool drained, a run must fail fast with an ETA
// rather than block for the length of a clone.
func TestNoWarmTwin(t *testing.T) {
	mgr, eng := newLab(t, "nowarm", 1)
	ctx := context.Background()

	// Drain the pool, holding everything we take.
	var held []*twins.Twin
	defer func() {
		for _, tw := range held {
			_ = mgr.Release(context.Background(), tw)
		}
	}()

	var runErr error
	for attempt := 0; attempt < 4; attempt++ {
		for {
			tw, err := mgr.Acquire(ctx)
			if errors.Is(err, twins.ErrNoWarmTwin) {
				break
			}
			if err != nil {
				t.Fatalf("Acquire: %v", err)
			}
			held = append(held, tw)
		}
		// The pool is empty as of this instant; the refill worker needs at
		// least a CREATE DATABASE to change that, and Rehearse reaches Acquire
		// in microseconds.
		_, runErr = eng.Rehearse(ctx, `CREATE INDEX idx_nowarm ON orders (customer_id)`, ModeNaive)
		var noWarm *NoWarmTwinError
		if errors.As(runErr, &noWarm) {
			if noWarm.ETASeconds <= 0 {
				t.Errorf("503 carried no usable ETA: %+v", noWarm)
			}
			if !errors.Is(runErr, twins.ErrNoWarmTwin) {
				t.Error("NoWarmTwinError must unwrap to twins.ErrNoWarmTwin")
			}
			t.Logf("no-warm-twin answer: %v (pool %+v)", noWarm, noWarm.Pool)
			return
		}
		t.Logf("attempt %d raced the refill worker (%v); retrying", attempt, runErr)
	}
	t.Fatalf("never observed the empty-pool path; last result: %v", runErr)
}

// TestOnlyOneRunAtATime: two rehearsals on one server would each measure the
// other's I/O as their own migration's cost.
func TestOnlyOneRunAtATime(t *testing.T) {
	_, eng := newLab(t, "solo", 2)
	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Add(1)
	firstDone := make(chan struct{})
	var firstErr error
	go func() {
		defer wg.Done()
		defer close(firstDone)
		_, firstErr = eng.Rehearse(ctx, `CREATE INDEX idx_solo ON orders (customer_id)`, ModeNaive)
	}()

	// Wait until the first run is genuinely in flight.
	deadline := time.Now().Add(20 * time.Second)
	for !eng.Status().Running {
		if time.Now().After(deadline) {
			t.Fatal("the first run never started")
		}
		select {
		case <-firstDone:
			t.Fatalf("the first run finished before the second could start: %v", firstErr)
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}

	_, err := eng.Rehearse(ctx, `CREATE INDEX idx_solo2 ON orders (amount_cents)`, ModeNaive)
	if !errors.Is(err, ErrRunInProgress) {
		t.Errorf("second concurrent Rehearse returned %v, want ErrRunInProgress", err)
	}

	// Live views must work while a run is in flight — the SSE layer depends on
	// them. LiveStats only exists once the load generator does, which is a few
	// phases in, so wait for the load rather than for the slot.
	if st := eng.Status(); st.CurrentRun == nil {
		t.Error("Status reported no current run while one was in flight")
	} else if st.CurrentRun.Phase == "" {
		t.Error("current run has no phase")
	}
	sawStats, sawTimeline := false, false
	for !(sawStats && sawTimeline) {
		if _, ok := eng.LiveStats(); ok {
			sawStats = true
		}
		if len(eng.LiveTimeline()) > 0 {
			sawTimeline = true
		}
		select {
		case <-firstDone:
			// The run is over; one last look, then judge.
			if _, ok := eng.LiveStats(); ok {
				sawStats = true
			}
			if len(eng.LiveTimeline()) > 0 {
				sawTimeline = true
			}
			goto judge
		case <-time.After(20 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			break
		}
	}
judge:
	if !sawStats {
		t.Error("LiveStats never reported anything while a run was in flight")
	}
	if !sawTimeline {
		t.Error("LiveTimeline never reported anything while a run was in flight")
	}

	wg.Wait()
	if firstErr != nil {
		t.Fatalf("first run: %v", firstErr)
	}
	// And the slot is released afterwards.
	if _, err := eng.Rehearse(ctx, ``, ModeCustom); errors.Is(err, ErrRunInProgress) {
		t.Error("the run slot was never released")
	}
}

// TestLedgerSurvivesRestart: the ledger lives in the application database, not
// in a twin, so history outlives every run. This is the assertion that would
// fail if somebody "simplified" it onto the twin.
func TestLedgerSurvivesRestart(t *testing.T) {
	requireDB(t)
	ctx := context.Background()

	_, eng := newLab(t, "ledger", 1)
	if _, err := eng.Rehearse(ctx, `ANALYZE orders`, ModeCustom); err == nil {
		t.Fatal("ANALYZE should be answered already_safe without a run")
	}
	res, err := eng.Rehearse(ctx, `VACUUM orders`, ModeCustom)
	if err != nil {
		t.Fatalf("Rehearse: %v", err)
	}
	if res.LedgerID == 0 {
		t.Fatal("no ledger row")
	}

	// A brand-new Ledger over a brand-new pool sees the same rows.
	eng2, err := New(ctx, Config{AdminURL: appURL, Twins: eng.cfg.Twins, Logger: quietLogger()})
	if err != nil {
		t.Fatalf("second engine: %v", err)
	}
	defer eng2.Close()

	runs, err := eng2.History(ctx, 50)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	found := false
	for _, r := range runs {
		if r.ID == res.LedgerID {
			found = true
			if r.Classification != ClassVacuum {
				t.Errorf("ledger classification = %q, want vacuum", r.Classification)
			}
		}
	}
	if !found {
		t.Errorf("run %d is missing from a freshly opened ledger", res.LedgerID)
	}
}

// ---------------------------------------------------------------------------
// The false-safe, end to end.
// ---------------------------------------------------------------------------

// TestScaleProjectionCatchesFalseSafe is the whole scale layer proved against a
// real server rather than against arithmetic.
//
// The twin here holds 500,000 rows. A statement that finishes inside the
// sampler's resolution on a table that size is reported — correctly — as SAFE:
// nothing froze, nobody died, and that IS what happened here. The point is that
// "here" is a sample. Told that the operator's real table is a thousand times
// larger, the same measurement projects to a multi-second freeze, and the run
// must come back SAFE-but-EffectiveVerdict-UNSAFE with a warning that says so
// in numbers.
//
// Without this, Brace's answer to "is this migration safe?" would be "it was
// safe on a table one thousandth the size of yours", presented as a green tick.
func TestScaleProjectionCatchesFalseSafe(t *testing.T) {
	_, eng := newLab(t, "scale", 1)
	ctx := context.Background()

	// SET NOT NULL on a column that is already NOT NULL: PostgreSQL takes
	// ACCESS EXCLUSIVE and finishes immediately, because there is no scan left
	// to do. Fast enough to measure SAFE on this twin, and classified
	// set_not_null — a shape whose cost IS O(rows) in the general case, which is
	// exactly the combination that produces a false safe.
	const sql = `ALTER TABLE orders ALTER COLUMN customer_id SET NOT NULL`

	// 1000x the twin. A statement that cost even one sample here costs minutes
	// there.
	const targetRows = int64(templateRows) * 1000

	res, err := eng.Rehearse(ctx, sql, ModeCustom, WithTargetRows(targetRows))
	if err != nil {
		t.Fatalf("Rehearse: %v", err)
	}
	t.Logf("verdict=%s effective=%s blocked=%.3fs projected=%.3fs at_floor=%v twin_rows=%d target_rows=%d",
		res.Verdict, res.EffectiveVerdict, res.BlockedSeconds,
		res.ScaleProjection.ProjectedBlockedSeconds, res.ScaleProjection.MeasuredAtFloor,
		res.ScaleProjection.TwinRows, res.ScaleProjection.TargetRows)

	if res.Error != "" {
		t.Fatalf("the migration itself failed: %s", res.Error)
	}

	// --- the measurement is untouched ---
	if res.Verdict != VerdictSafe {
		t.Fatalf("measured verdict = %s (blocked %.3fs), want SAFE — this statement does no work on this twin",
			res.Verdict, res.BlockedSeconds)
	}
	if res.BlockedSeconds > unsafeThresholdSeconds {
		t.Errorf("blocked_seconds = %.3f, want <= %.1f", res.BlockedSeconds, unsafeThresholdSeconds)
	}

	// --- the projection vetoes it ---
	p := res.ScaleProjection
	if !p.Known() {
		t.Fatalf("no projection was made despite both row counts being known: %+v", p)
	}
	if p.TwinRows != templateRows {
		t.Errorf("projection twin_rows = %d, want %d", p.TwinRows, int64(templateRows))
	}
	if p.TargetRows != targetRows {
		t.Errorf("projection target_rows = %d, want %d", p.TargetRows, targetRows)
	}
	if p.Model == "" {
		t.Error("the projection does not name its model")
	}
	if p.Verdict != VerdictUnsafe {
		t.Errorf("projected verdict = %s (%.3fs), want UNSAFE at 1000x", p.Verdict, p.ProjectedBlockedSeconds)
	}
	if p.ProjectedBlockedSeconds <= res.BlockedSeconds {
		t.Errorf("projection %.3fs is not larger than the measurement %.3fs at 1000x",
			p.ProjectedBlockedSeconds, res.BlockedSeconds)
	}

	if res.EffectiveVerdict != VerdictUnsafe {
		t.Errorf("effective_verdict = %s, want UNSAFE — a CI gate reading this would have shipped an outage",
			res.EffectiveVerdict)
	}
	if res.ScaleWarning == "" {
		t.Fatal("no scale warning on a SAFE-measured, UNSAFE-projected run")
	}
	for _, want := range []string{"SAFE on this", "Rehearse against a larger sample"} {
		if !strings.Contains(res.ScaleWarning, want) {
			t.Errorf("scale warning missing %q:\n%s", want, res.ScaleWarning)
		}
	}
	if res.Note != res.ScaleWarning {
		t.Errorf("the run's note should BE the scale warning when one exists;\nnote:    %s\nwarning: %s",
			res.Note, res.ScaleWarning)
	}

	// --- and without a stated size, we say nothing rather than something false ---
	res2, err := eng.Rehearse(ctx, sql, ModeCustom)
	if err != nil {
		t.Fatalf("Rehearse (no target): %v", err)
	}
	if res2.ScaleProjection.Known() {
		t.Errorf("a projection was made with no target size: %+v", res2.ScaleProjection)
	}
	if res2.EffectiveVerdict != res2.Verdict {
		t.Errorf("effective_verdict = %s with no projection, want the measured %s",
			res2.EffectiveVerdict, res2.Verdict)
	}
	if res2.ScaleWarning != "" {
		t.Errorf("scale warning with no target size: %s", res2.ScaleWarning)
	}
}
