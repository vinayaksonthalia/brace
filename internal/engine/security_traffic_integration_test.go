//go:build integration

// The privilege-escalation regression suite.
//
// A live hostile audit proved a CRITICAL hole: the load generator connected to
// each twin with the twin's ADMIN (superuser) DSN, so every INSERT/UPDATE the
// traffic workers ran executed as a PostgreSQL SUPERUSER. Because a CHECK
// constraint / trigger / column DEFAULT / index expression is evaluated in the
// context of the session that performs the write, an allow-listed migration
// could install an expression that fired AS SUPERUSER the instant live traffic
// touched a row. The auditor's proof came back SAFE with 0 errored:
//
//	ALTER TABLE orders ADD CONSTRAINT c CHECK (current_setting('is_superuser') = 'on') NOT VALID;
//
// never failed an INSERT ⇒ the traffic session WAS superuser.
//
// The fix runs the load as brace_traffic, an unprivileged NOSUPERUSER role.
// These tests reproduce the exploit and prove it is dead: the same CHECK now
// fails EVERY write (the writer is not superuser), pg_read_file() is denied to
// the writer, and brace_traffic's rolsuper is false. They also prove the traffic
// role can still do its legitimate job and cannot do the dangerous things.
//
//	TEST_ADMIN_URL=postgres://postgres@localhost:5433/postgres \
//	  go test -tags integration -race -run Traffic ./internal/engine/
package engine

import (
	"context"
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/vinayaksonthalia/brace/internal/loadgen"
)

// totalOK sums successful writes across every label in a loadgen snapshot.
func totalOK(s loadgen.Stats) int64 {
	var n int64
	for _, ls := range s.ByLabel {
		n += ls.OK
	}
	return n
}

// grantedTwin acquires a twin and applies BOTH per-twin grants (migrator and
// traffic) exactly as a real run does, returning the twin. The caller releases
// it via the returned cleanup.
func grantedTwin(t *testing.T, eng *Engine) (twinName, twinDSN string, cleanup func()) {
	t.Helper()
	ctx := context.Background()
	twin, err := eng.cfg.Twins.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if err := grantOnTwin(ctx, twin.DSN, twin.Name, eng.cfg.MigratorRole); err != nil {
		_ = eng.cfg.Twins.Release(context.Background(), twin)
		t.Fatalf("grantOnTwin: %v", err)
	}
	if err := grantTrafficOnTwin(ctx, twin.DSN, twin.Name, eng.cfg.TrafficRole); err != nil {
		_ = eng.cfg.Twins.Release(context.Background(), twin)
		t.Fatalf("grantTrafficOnTwin: %v", err)
	}
	return twin.Name, twin.DSN, func() {
		if err := eng.cfg.Twins.Release(context.Background(), twin); err != nil {
			t.Errorf("Release: %v", err)
		}
	}
}

// connectAsTraffic opens a connection to a twin authenticating as the traffic
// role, the same way loadgen does.
func connectAsTraffic(t *testing.T, eng *Engine, twinDSN string) *pgx.Conn {
	t.Helper()
	dsn, err := trafficDSN(twinDSN, eng.cfg.TrafficRole, eng.trafficPassword)
	if err != nil {
		t.Fatalf("trafficDSN: %v", err)
	}
	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect as traffic role: %v", err)
	}
	return conn
}

// TestTrafficRoleExploitIsDead is the headline: it installs the auditor's exact
// escalation gadget as the migrator, then drives the LOAD GENERATOR — with the
// real traffic DSN — against the poisoned table, and proves the writes now FAIL
// because the traffic session is not a superuser. Before the fix every INSERT
// succeeded (0 errored); after it, every INSERT is refused by the CHECK.
func TestTrafficRoleExploitIsDead(t *testing.T) {
	_, eng := newLab(t, "exploit", 1)
	ctx := context.Background()

	twinName, twinDSN, cleanup := grantedTwin(t, eng)
	defer cleanup()

	// The migrator installs the gadget, exactly as an allow-listed ALTER TABLE
	// would. NOT VALID so existing seeded rows are not scanned — the constraint
	// only bites NEW writes, which is precisely the load generator's traffic.
	mig, err := connectAsMigrator(ctx, twinDSN, eng.cfg.MigratorRole, eng.migratorPassword)
	if err != nil {
		t.Fatalf("connect as migrator: %v", err)
	}
	if _, err := mig.Exec(ctx,
		`ALTER TABLE orders ADD CONSTRAINT c_super CHECK (current_setting('is_superuser') = 'on') NOT VALID`); err != nil {
		mig.Close(ctx)
		t.Fatalf("install exploit constraint: %v", err)
	}
	mig.Close(ctx)

	// Now run the LOAD the way the engine does: the traffic DSN, not the admin
	// DSN. Every INSERT the workers attempt must be refused by the CHECK, because
	// is_superuser is 'off' for brace_traffic.
	dsn, err := trafficDSN(twinDSN, eng.cfg.TrafficRole, eng.trafficPassword)
	if err != nil {
		t.Fatalf("trafficDSN: %v", err)
	}
	gen, err := loadgen.New(ctx, loadgen.Config{
		DSN: dsn, Workers: 6, LockTimeout: 300 * time.Millisecond, Logger: quietLogger(),
	})
	if err != nil {
		t.Fatalf("loadgen.New: %v", err)
	}
	if err := gen.Start(ctx); err != nil {
		t.Fatalf("loadgen.Start: %v", err)
	}
	time.Sleep(1500 * time.Millisecond)
	stopCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	if err := gen.Stop(stopCtx); err != nil {
		t.Errorf("loadgen.Stop: %v", err)
	}
	cancel()

	s := gen.Snapshot()
	t.Logf("under the exploit constraint: ok=%d errored=%d casualties=%d", totalOK(s), s.ErroredTotal, s.CasualtiesTotal)

	// THE PROOF. Pre-fix, is_superuser='on' ⇒ every INSERT committed and errored
	// would be 0. Post-fix the writer is not superuser, the CHECK is false, and
	// every INSERT is refused: successes must be zero and the failures land in the
	// (visible, never-casualty) errored bucket.
	if totalOK(s) != 0 {
		t.Fatalf("SECURITY: %d writes SUCCEEDED under CHECK(is_superuser='on') — the traffic session is STILL a superuser",
			totalOK(s))
	}
	if s.ErroredTotal == 0 {
		t.Fatal("no writes were even attempted; the exploit test proved nothing")
	}
	if s.CasualtiesTotal != 0 {
		t.Errorf("a check violation is not a lock death, but %d casualties were counted", s.CasualtiesTotal)
	}

	// Corroborate directly: a plain connection as the traffic role sees itself as
	// non-superuser, and one INSERT bounces off the same constraint.
	conn := connectAsTraffic(t, eng, twinDSN)
	defer conn.Close(context.Background())
	var isSuper string
	if err := conn.QueryRow(ctx, `SELECT current_setting('is_superuser')`).Scan(&isSuper); err != nil {
		t.Fatalf("read is_superuser: %v", err)
	}
	if isSuper != "off" {
		t.Fatalf("SECURITY: traffic session reports is_superuser=%q, want off", isSuper)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO orders (customer_id, amount_cents) VALUES (1, 1)`); err == nil {
		t.Fatal("SECURITY: an INSERT as the traffic role passed CHECK(is_superuser='on')")
	} else {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23514" {
			t.Logf("exploit dead: INSERT refused by check constraint (SQLSTATE 23514) as expected")
		} else {
			t.Logf("exploit dead: INSERT refused: %v", err)
		}
	}
	_ = twinName
}

// TestTrafficRoleCannotReadFilesViaExpression is the second half of the auditor's
// finding: `CHECK (pg_read_file(...) IS NOT NULL)` passed, i.e. arbitrary file
// read was reachable from a write. With the writer unprivileged, PostgreSQL
// denies pg_read_file() itself.
func TestTrafficRoleCannotReadFilesViaExpression(t *testing.T) {
	_, eng := newLab(t, "fileread", 1)
	ctx := context.Background()

	_, twinDSN, cleanup := grantedTwin(t, eng)
	defer cleanup()

	conn := connectAsTraffic(t, eng, twinDSN)
	defer conn.Close(context.Background())

	// Directly: the writer cannot read a server file at all.
	if _, err := conn.Exec(ctx, `SELECT pg_read_file('/etc/hostname')`); err == nil {
		t.Fatal("SECURITY: the traffic role could pg_read_file a server file")
	} else {
		t.Logf("pg_read_file denied to traffic role as expected: %v", err)
	}

	// And via a write-triggered expression, which is the actual attack shape: the
	// migrator installs a DEFAULT that reads a file; a traffic INSERT would have to
	// evaluate it. The DEFAULT is evaluated as the writer, so the read is denied.
	mig, err := connectAsMigrator(ctx, twinDSN, eng.cfg.MigratorRole, eng.migratorPassword)
	if err != nil {
		t.Fatalf("connect as migrator: %v", err)
	}
	// A DEFAULT that calls pg_read_file. Adding the column is fine; the read only
	// happens when a row is inserted without supplying the column.
	if _, err := mig.Exec(ctx,
		`ALTER TABLE orders ADD COLUMN leak text DEFAULT pg_read_file('/etc/hostname')`); err != nil {
		mig.Close(ctx)
		// Some builds evaluate the default eagerly against existing rows; either way
		// the security property is that the traffic role's write cannot read the
		// file, which the direct check above already proved. Log and move on.
		t.Logf("note: installing the pg_read_file DEFAULT as migrator failed (%v); direct denial already proven", err)
		return
	}
	mig.Close(ctx)

	_, err = conn.Exec(ctx, `INSERT INTO orders (customer_id, amount_cents) VALUES (1, 1)`)
	if err == nil {
		t.Fatal("SECURITY: a traffic INSERT evaluated a pg_read_file() DEFAULT and succeeded")
	}
	t.Logf("write-triggered file read denied as expected: %v", err)
}

// TestTrafficRoleAttributesAndDenials asserts the role is boxed in exactly the
// way the fix promises: NOSUPERUSER, and unable to DROP/TRUNCATE/DELETE, create
// objects, read files, or reach another database — while still able to do the
// one job it has (SELECT/INSERT/UPDATE the load target).
func TestTrafficRoleAttributesAndDenials(t *testing.T) {
	_, eng := newLab(t, "trafbox", 1)
	ctx := context.Background()

	_, twinDSN, cleanup := grantedTwin(t, eng)
	defer cleanup()

	conn := connectAsTraffic(t, eng, twinDSN)
	defer conn.Close(context.Background())

	// Attributes, asserted directly. rolsuper=false is the load-bearing one.
	var super, createdb, createrole, replication, bypassrls bool
	if err := conn.QueryRow(ctx, `
		SELECT rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls
		FROM pg_roles WHERE rolname = $1`, eng.cfg.TrafficRole).
		Scan(&super, &createdb, &createrole, &replication, &bypassrls); err != nil {
		t.Fatalf("read role attributes: %v", err)
	}
	if super {
		t.Fatal("SECURITY: the traffic role is a SUPERUSER")
	}
	if createdb || createrole || replication || bypassrls {
		t.Errorf("SECURITY: traffic role attributes too wide: createdb=%v createrole=%v repl=%v bypassrls=%v",
			createdb, createrole, replication, bypassrls)
	}

	// The privileges it MUST have, or the load generator cannot measure anything.
	for _, sql := range []string{
		`INSERT INTO orders (customer_id, amount_cents) VALUES (1, 1)`,
		`UPDATE orders SET amount_cents = amount_cents WHERE id = (SELECT id FROM orders LIMIT 1)`,
		`SELECT count(*) FROM orders`,
	} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Errorf("traffic role cannot run its own workload %q: %v", sql, err)
		}
	}

	// The powers it MUST NOT have. Every denial is PostgreSQL's, not a regex.
	denied := []struct {
		name string
		sql  string
	}{
		{"DELETE rows", `DELETE FROM orders`},
		{"TRUNCATE the table", `TRUNCATE orders`},
		{"DROP the table", `DROP TABLE orders`},
		{"ALTER the table", `ALTER TABLE orders ADD COLUMN pwned int`},
		{"create a table", `CREATE TABLE traffic_pwned (x int)`},
		{"read a server file", `SELECT pg_read_file('/etc/passwd')`},
		{"list the data directory", `SELECT pg_ls_dir('.')`},
		{"COPY TO PROGRAM", `COPY orders TO PROGRAM 'touch /tmp/brace_traffic_pwned'`},
		{"COPY from a server file", `COPY orders FROM '/etc/passwd'`},
		{"CREATE ROLE", `CREATE ROLE traffic_pwned SUPERUSER LOGIN`},
		{"escalate itself", `ALTER ROLE ` + eng.cfg.TrafficRole + ` SUPERUSER`},
	}
	for _, c := range denied {
		t.Run(c.name, func(t *testing.T) {
			if _, err := conn.Exec(ctx, c.sql); err == nil {
				t.Fatalf("SECURITY: the traffic role was permitted to %s (%q)", c.name, c.sql)
			} else {
				t.Logf("denied as expected: %v", err)
			}
		})
	}

	// It must not be able to reach ANOTHER database (the run ledger lives in the
	// application database). PUBLIC can connect there, but the ledger must be out
	// of reach.
	appCfg, err := pgx.ParseConfig(appURL)
	if err != nil {
		t.Fatalf("parse app url: %v", err)
	}
	appCfg.User = eng.cfg.TrafficRole
	appCfg.Password = eng.trafficPassword
	appConn, err := pgx.ConnectConfig(ctx, appCfg)
	if err != nil {
		t.Logf("traffic role cannot even connect to the app database: %v", err)
		return
	}
	defer appConn.Close(context.Background())
	if _, err := appConn.Exec(ctx, `SELECT * FROM brace_runs`); err == nil {
		t.Error("SECURITY: the traffic role can read the run ledger")
	} else {
		t.Logf("ledger read denied as expected: %v", err)
	}
}

// TestTrafficDSNConstruction is the cheap unit-level guard on the DSN rewrite:
// only the userinfo changes, everything else (host, port, database, query
// options) survives verbatim.
func TestTrafficDSNConstruction(t *testing.T) {
	const pw = "abcdEFGH1234_-abcdEFGH1234" // matches safePassword's shape
	got, err := trafficDSN("postgres://postgres@db.example:5433/brace_run_x?sslmode=disable", "brace_traffic", pw)
	if err != nil {
		t.Fatalf("trafficDSN: %v", err)
	}
	u, err := url.Parse(got)
	if err != nil {
		t.Fatalf("parse result: %v", err)
	}
	if u.User.Username() != "brace_traffic" {
		t.Errorf("user = %q, want brace_traffic", u.User.Username())
	}
	if p, _ := u.User.Password(); p != pw {
		t.Errorf("password not carried through: %q", p)
	}
	if u.Host != "db.example:5433" {
		t.Errorf("host = %q, want db.example:5433", u.Host)
	}
	if u.Path != "/brace_run_x" {
		t.Errorf("database = %q, want /brace_run_x", u.Path)
	}
	if u.Query().Get("sslmode") != "disable" {
		t.Errorf("sslmode option was dropped: %q", u.RawQuery)
	}

	// A bad role name or a non-postgres URL is refused rather than silently
	// producing a broken DSN.
	if _, err := trafficDSN("postgres://h/db", "Robert'); DROP", pw); err == nil {
		t.Error("trafficDSN accepted an unsafe role name")
	}
	if _, err := trafficDSN("mysql://h/db", "brace_traffic", pw); err == nil {
		t.Error("trafficDSN accepted a non-postgres URL")
	}
}
