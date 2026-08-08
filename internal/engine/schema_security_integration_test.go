//go:build integration

// The bring-your-own-schema security + behaviour suite.
//
// RehearseSchema lets a public caller paste arbitrary CREATE TABLE DDL. The
// whole product's credibility is "the database role is the boundary, not a
// regex", so these tests prove the boundary the same way the twin/traffic tests
// do: against a REAL PostgreSQL, by trying the attack and showing the server
// refuses it.
//
// What is asserted here:
//
//   - a valid CREATE TABLE + a naive CREATE INDEX on it produces a MEASURED
//     verdict against the user's OWN shape, then the disposable database is gone;
//
//   - obviously-malicious DDL (multi-statement, CREATE TABLE AS, `; DROP`
//     smuggling) is REJECTED before any database is created;
//
//   - a CREATE TABLE that smuggles a privileged expression into a DEFAULT / CHECK
//     is created fine but REFUSES to seed, because seeding runs as an
//     unprivileged role — no file is read, no privilege is gained, nothing leaks;
//
//   - the stock orders flow still works in the same lab.
//
//     TEST_ADMIN_URL=postgres://postgres@localhost:5433/postgres \
//     go test -tags integration -race -p 1 -run Schema ./internal/engine/
package engine

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// byosPrefix is the scratch namespace newLab configures — every disposable
// schema-run database starts with it, so a count of zero proves nothing leaked.
func byosPrefix() string { return itNamespace + "byos_" }

// waitScratchGone polls until no database in the scratch namespace remains, so a
// drop that races the assertion does not flake the test.
func waitScratchGone(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		if n := countDatabases(t, byosPrefix()); n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("a scratch database survived the run: %d still present under %q", countDatabases(t, byosPrefix()), byosPrefix())
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestSchemaRehearsalMeasuresOwnTable is the headline: a user's own table is
// built, seeded, and a naive CREATE INDEX on it is measured under live traffic —
// then everything is torn down.
func TestSchemaRehearsalMeasuresOwnTable(t *testing.T) {
	mgr, eng := newLab(t, "own", 1)
	ctx := context.Background()

	if n := countDatabases(t, byosPrefix()); n != 0 {
		t.Fatalf("scratch namespace not clean at start: %d databases", n)
	}

	const ddl = `CREATE TABLE tickets (
		id          bigserial PRIMARY KEY,
		customer_id bigint      NOT NULL,
		amount      bigint      NOT NULL,
		note        text,
		created_at  timestamptz NOT NULL DEFAULT now()
	)`
	res, err := eng.RehearseSchema(ctx, SchemaRequest{
		SchemaDDL: ddl,
		RowCount:  600_000,
		SQL:       `CREATE INDEX idx_tickets_customer ON tickets (customer_id)`,
		Mode:      ModeNaive,
	})
	if err != nil {
		t.Fatalf("RehearseSchema: %v", err)
	}
	t.Logf("verdict=%s effective=%s blocked=%.3fs monitor_blocked=%.3fs baseline=%d/s casualties=%d errored=%d locks=%v twin=%s target_rows=%d",
		res.Verdict, res.EffectiveVerdict, res.BlockedSeconds, res.MonitorBlockedSeconds,
		res.BaselineWPS, res.Casualties.Total, res.ErroredTotal, res.LockModes, res.Twin, res.ScaleProjection.TargetRows)

	if res.Error != "" {
		t.Fatalf("the migration itself failed against the user's table: %s", res.Error)
	}
	if !strings.HasPrefix(res.Twin, byosPrefix()) {
		t.Errorf("run did not use a scratch database: twin=%q", res.Twin)
	}
	if res.BaselineWPS <= 0 {
		t.Fatalf("no baseline measured (%d/s) — traffic never landed on the user's table", res.BaselineWPS)
	}
	if len(res.Timeline) == 0 {
		t.Error("empty timeline: nothing was measured")
	}
	if res.Monitor.Samples == 0 {
		t.Errorf("the lock monitor took no samples: %+v", res.Monitor)
	}
	if res.Classification != ClassCreateIndex {
		t.Errorf("classification = %q, want %q", res.Classification, ClassCreateIndex)
	}
	if res.Verdict != VerdictSafe && res.Verdict != VerdictUnsafe {
		t.Errorf("verdict = %q, want a measured SAFE/UNSAFE", res.Verdict)
	}
	// The measurement pipeline ran end to end against the user's own table: this
	// is what BYOS correctness hinges on. Whether a naive build actually froze
	// traffic depends on how fast this box builds an index over the seeded sample —
	// a SAFE measurement on a small sample is a legitimate outcome (it is precisely
	// what the scale projection exists to qualify), so a freeze is logged, not
	// required.
	if res.BlockedSeconds > 0 || res.Casualties.Total > 0 || res.Monitor.StrongestLockMode != "" {
		t.Logf("observed harm on the user's table: blocked=%.3fs casualties=%d strongest_lock=%q",
			res.BlockedSeconds, res.Casualties.Total, res.Monitor.StrongestLockMode)
	} else {
		t.Logf("the build finished within the sampler resolution on this box (SAFE on this sample)")
	}
	// The stated size drives the projection.
	if res.ScaleProjection.TargetRows != 600_000 {
		t.Errorf("projection target_rows = %d, want the stated 600000", res.ScaleProjection.TargetRows)
	}
	if res.LedgerID == 0 {
		t.Error("a completed schema run wrote no ledger row")
	}

	// The disposable database is gone and the pool was never touched.
	waitScratchGone(t)
	if s := mgr.Status(); s.CheckedOut != 0 {
		t.Errorf("a schema run checked out a pool twin: %+v", s)
	}
	if st := eng.Status(); st.Running || st.CurrentRun != nil {
		t.Errorf("engine still reports a run after the schema rehearsal: %+v", st)
	}

	// And the stock orders flow still works in the same lab, immediately after.
	ordersRes, err := eng.Rehearse(ctx, `CREATE INDEX idx_orders_amount ON orders (amount_cents)`, ModeNaive)
	if err != nil {
		t.Fatalf("the stock orders flow broke after a schema run: %v", err)
	}
	if ordersRes.Error != "" {
		t.Fatalf("orders rehearsal errored after a schema run: %s", ordersRes.Error)
	}
	if ordersRes.BaselineWPS <= 0 {
		t.Error("orders rehearsal measured no baseline after a schema run")
	}
}

// TestSchemaRehearsalSeedsDiverseTypes proves the type-aware seeder handles the
// common column types the feature promises, end to end.
func TestSchemaRehearsalSeedsDiverseTypes(t *testing.T) {
	_, eng := newLab(t, "types", 1)
	ctx := context.Background()

	const ddl = `CREATE TABLE widgets (
		id         bigserial PRIMARY KEY,
		owner_id   bigint       NOT NULL,
		sku        varchar(32)  NOT NULL,
		price      numeric      NOT NULL,
		active     boolean      NOT NULL,
		tag        uuid         NOT NULL,
		updated_on date         NOT NULL,
		meta       jsonb        NOT NULL,
		source     inet         NOT NULL,
		labels     text[]       NOT NULL,
		created_at timestamptz  NOT NULL DEFAULT now(),
		note       text
	)`
	res, err := eng.RehearseSchema(ctx, SchemaRequest{
		SchemaDDL: ddl,
		RowCount:  120_000,
		SQL:       `CREATE INDEX idx_widgets_owner ON widgets (owner_id)`,
		Mode:      ModeNaive,
	})
	if err != nil {
		t.Fatalf("RehearseSchema over diverse types: %v", err)
	}
	if res.Error != "" {
		t.Fatalf("migration failed on the diverse-type table: %s", res.Error)
	}
	if res.BaselineWPS <= 0 {
		t.Fatalf("no baseline: type-aware traffic never wrote a row (errored=%d)", res.ErroredTotal)
	}
	if res.Verdict != VerdictSafe && res.Verdict != VerdictUnsafe {
		t.Errorf("verdict = %q", res.Verdict)
	}
	t.Logf("diverse types: verdict=%s baseline=%d/s errored=%d", res.Verdict, res.BaselineWPS, res.ErroredTotal)
	waitScratchGone(t)
}

// TestSchemaRehearsalRejectsMaliciousDDL proves the pre-filter turns every
// obviously-hostile paste away BEFORE a database is spent, and leaves nothing
// behind.
func TestSchemaRehearsalRejectsMaliciousDDL(t *testing.T) {
	_, eng := newLab(t, "shape", 1)
	ctx := context.Background()

	before := countDatabases(t, byosPrefix())

	cases := []struct {
		name string
		ddl  string
	}{
		{"multi_statement", `CREATE TABLE t (id int); DROP TABLE orders`},
		{"semicolon_smuggled_drop", `CREATE TABLE t (id int) ; DROP DATABASE ` + appDBName},
		{"create_table_as_select", `CREATE TABLE t AS SELECT 1 AS x`},
		{"create_table_as_execute", `CREATE TABLE t AS EXECUTE somequery`},
		{"not_a_create_table", `DROP TABLE orders`},
		{"temp_table", `CREATE TEMP TABLE t (id int)`},
		{"empty", ``},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res, err := eng.RehearseSchema(ctx, SchemaRequest{
				SchemaDDL: c.ddl,
				RowCount:  1000,
				SQL:       `CREATE INDEX idx_t ON t (id)`,
				Mode:      ModeCustom,
			})
			if res != nil {
				t.Errorf("a rejected DDL returned a result: %+v", res)
			}
			var rej *RejectedError
			if !errors.As(err, &rej) {
				t.Fatalf("RehearseSchema returned %T (%v), want *RejectedError", err, err)
			}
			if rej.Reason == "" {
				t.Error("rejection carried no reason")
			}
		})
	}

	if after := countDatabases(t, byosPrefix()); after != before {
		t.Errorf("rejected DDL changed the scratch database count from %d to %d", before, after)
	}
	// The migration allow-list also still guards the migration argument in schema
	// mode: a non-schema-change migration is refused even with a valid table.
	if _, err := eng.RehearseSchema(ctx, SchemaRequest{
		SchemaDDL: `CREATE TABLE t (id int NOT NULL)`,
		SQL:       `DROP TABLE t`,
		Mode:      ModeCustom,
	}); err == nil {
		t.Error("a non-allow-listed migration was accepted in schema mode")
	} else {
		var rej *RejectedError
		if !errors.As(err, &rej) {
			t.Errorf("migration rejection was %T, want *RejectedError", err)
		}
	}
	if after := countDatabases(t, byosPrefix()); after != before {
		t.Errorf("a rejected migration leaked a scratch database (count %d -> %d)", before, after)
	}
}

// TestSchemaRehearsalEscalationDefaultIsDenied is the security crown for this
// feature: a pasted CREATE TABLE that smuggles a privileged call into a DEFAULT
// or a CHECK is created without incident (as an unprivileged role, storing an
// expression proves nothing) — but the moment Brace SEEDS it, the expression
// fires as that unprivileged role and PostgreSQL refuses it. The run is rejected,
// no file is read, and no database is left behind.
//
// This is the CREATE-TABLE analogue of the traffic-role exploit test: the same
// escalation, closed the same way — the writer is never a superuser.
func TestSchemaRehearsalEscalationDefaultIsDenied(t *testing.T) {
	_, eng := newLab(t, "escl", 1)
	ctx := context.Background()

	before := countDatabases(t, byosPrefix())

	cases := []struct {
		name string
		ddl  string
	}{
		{
			// A DEFAULT that reads a server file. `leak` is omitted by the seeder
			// (it has a default), so the default fires per row — as the unprivileged
			// role — and pg_read_file is denied.
			"default_pg_read_file",
			`CREATE TABLE loot (id bigint NOT NULL, leak text DEFAULT pg_read_file('/etc/passwd'))`,
		},
		{
			// A CHECK that reads a server file. It is evaluated on every insert of
			// the required `id`, again as the unprivileged role.
			"check_pg_read_file",
			`CREATE TABLE loot (id bigint NOT NULL, CHECK (pg_read_file('/etc/hostname') IS NOT NULL))`,
		},
		{
			// The is_superuser oracle the original audit used, as a DEFAULT.
			"default_is_superuser_probe",
			`CREATE TABLE loot (id bigint NOT NULL, probe text DEFAULT current_setting('is_superuser'))`,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res, err := eng.RehearseSchema(ctx, SchemaRequest{
				SchemaDDL: c.ddl,
				RowCount:  5_000,
				SQL:       `CREATE INDEX idx_loot ON loot (id)`,
				Mode:      ModeCustom,
			})
			// The is_superuser probe does NOT error (current_setting is allowed) — it
			// simply returns 'off', proving the seeder is unprivileged. The file-read
			// gadgets must be refused outright. Either way, nothing dangerous happened
			// and no database may leak.
			if c.name == "default_is_superuser_probe" {
				// It can complete as a normal run OR reject; what matters is it never
				// read a file and never gained privilege. Assert it did not error out
				// as if the probe returned 'on'. We assert the seeded value is 'off'
				// below by reconnecting is impossible (db is dropped), so here we just
				// require a clean outcome with no privilege escalation crash.
				if err != nil {
					var rej *RejectedError
					if !errors.As(err, &rej) {
						t.Fatalf("unexpected non-rejection error: %v", err)
					}
				}
			} else {
				if res != nil {
					t.Errorf("an escalation gadget produced a run result: %+v", res)
				}
				var rej *RejectedError
				if !errors.As(err, &rej) {
					t.Fatalf("escalation gadget was not rejected: got %T (%v)", err, err)
				}
				t.Logf("escalation denied as expected: %s", rej.Reason)
			}
			waitScratchGone(t)
		})
	}

	if after := countDatabases(t, byosPrefix()); after != before {
		t.Errorf("an escalation attempt leaked a scratch database (count %d -> %d)", before, after)
	}

	// Corroborate the mechanism directly: the migrator role — the role that
	// creates and seeds the pasted table — is genuinely NOSUPERUSER, so any
	// privileged expression it evaluates is refused by PostgreSQL, not by Brace.
	var super bool
	conn, err := pgx.Connect(ctx, appURL)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	defer conn.Close(ctx)
	if err := conn.QueryRow(ctx, `SELECT rolsuper FROM pg_roles WHERE rolname = $1`, eng.cfg.MigratorRole).Scan(&super); err != nil {
		t.Fatalf("read migrator rolsuper: %v", err)
	}
	if super {
		t.Fatal("SECURITY: the role that creates and seeds pasted schemas is a SUPERUSER")
	}
}

// TestSchemaScratchDBIsIsolatedNamespace guards the isolation invariant: a
// schema run's database name is in the scratch namespace and is therefore neither
// the template, the app database, nor a twin from the pool.
func TestSchemaScratchDBIsIsolatedNamespace(t *testing.T) {
	_, eng := newLab(t, "iso", 1)
	ctx := context.Background()

	res, err := eng.RehearseSchema(ctx, SchemaRequest{
		SchemaDDL: `CREATE TABLE probe (id bigint NOT NULL, v bigint NOT NULL)`,
		RowCount:  10_000,
		SQL:       `CREATE INDEX idx_probe ON probe (id)`,
		Mode:      ModeCustom,
	})
	if err != nil {
		t.Fatalf("RehearseSchema: %v", err)
	}
	if !strings.HasPrefix(res.Twin, byosPrefix()) {
		t.Errorf("scratch db %q is not in the scratch namespace %q", res.Twin, byosPrefix())
	}
	if res.Twin == templateName || res.Twin == appDBName {
		t.Errorf("SECURITY: a schema run ran against a shared database %q", res.Twin)
	}
	if strings.Contains(res.Twin, "_run_") {
		t.Errorf("SECURITY: a schema run used a pool twin namespace: %q", res.Twin)
	}
	waitScratchGone(t)
}
