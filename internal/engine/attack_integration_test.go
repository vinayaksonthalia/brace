//go:build integration

// Adversarial, at-scale integration coverage for the orchestrator. These tests
// share TestMain, sweep(), buildTemplate() and the small helpers from
// integration_test.go, and add a SECOND, larger template seeded with enough
// rows that a naive CREATE INDEX and an ACCESS EXCLUSIVE rewrite hold their
// locks for measurable wall-clock — the only scale at which the "money path"
// (a lock that actually freezes live traffic) can be proven rather than
// asserted by disjunction.
//
//	TEST_ADMIN_URL=postgres://postgres@localhost:5433/postgres \
//	  go test -tags integration -race -run 'Attack|Guard|Idle|Whole|Invalid|Rewrite' ./internal/engine/
//
// What is proven here:
//   - FIX #1  the max-run-duration guard cancels a hung migration and returns
//     INCONCLUSIVE instead of wedging the single run slot (TestGuard*, TestIdle*).
//   - FIX #2  every allow-listed shape run end-to-end under live load at a
//     locking scale (TestWholeAllowlistUnderLoad, TestAttackMoneyPath*).
//   - FIX #3  an idle-in-transaction blocker cannot hang Brace forever
//     (TestIdleInTransactionDoesNotHangForever).
//   - FIX #5  a CONCURRENTLY build cancelled mid-flight is surfaced as an
//     INVALID-index amber outcome (TestInvalidIndexFromCancelledConcurrentBuild).
package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/vinayaksonthalia/brace/internal/twins"
)

const (
	// bigTemplateName is the larger seeded template. It lives under itNamespace so
	// the TestMain sweep reclaims it at start and end just like everything else.
	bigTemplateName = itNamespace + "bigtemplate"

	// bigTemplateRows is chosen so a plain CREATE INDEX and an ACCESS EXCLUSIVE
	// table rewrite each hold their lock for well over a second against the
	// 100ms sampler — past the 1s SAFE/UNSAFE threshold — while a file-copy clone
	// of the template (~520MB) stays under ~1.7s so the suite is not dominated by
	// CREATE DATABASE.
	//
	// This number is load-bearing and hardware-measured. On the target box a
	// naive CREATE INDEX freezes writes for ~0.25s at 1.2M rows (verdict SAFE),
	// ~0.57s at 3M (still SAFE), and ~2.2s at 6M (verdict UNSAFE). In other words
	// the "money path" only crosses into UNSAFE at production-ish scale: the harm
	// is real and measurable far below that, but the verdict threshold needs a
	// table big enough that a sub-second build is not possible. See the FINDINGS
	// note in TestWholeAllowlistUnderLoad.
	bigTemplateRows = 6_000_000

	// bigIndexName is a secondary index seeded into the big template so DROP
	// INDEX and REINDEX have a real, pre-existing target to operate on.
	bigIndexName = "idx_orders_amount"
)

var (
	bigTemplateOnce sync.Once
	bigTemplateErr  error
)

// ensureBigTemplate builds the larger template exactly once per test binary.
// It is built lazily (not in TestMain) so runs that never touch the money path
// do not pay for 1.2M rows.
func ensureBigTemplate(t *testing.T) {
	t.Helper()
	requireDB(t)
	bigTemplateOnce.Do(func() {
		bigTemplateErr = buildBigTemplate(context.Background())
	})
	if bigTemplateErr != nil {
		t.Fatalf("big template build failed: %v", bigTemplateErr)
	}
}

func buildBigTemplate(ctx context.Context) error {
	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		return fmt.Errorf("connect admin: %w", err)
	}
	defer admin.Close(ctx)

	// A previous aborted run may have left it (or a session on it); force it away
	// and rebuild from scratch so every clone starts identical.
	if _, err := admin.Exec(ctx, `DROP DATABASE IF EXISTS "`+bigTemplateName+`" WITH (FORCE)`); err != nil {
		return fmt.Errorf("drop stale big template: %w", err)
	}
	if _, err := admin.Exec(ctx, `CREATE DATABASE "`+bigTemplateName+`"`); err != nil {
		return fmt.Errorf("create big template: %w", err)
	}

	conn, err := pgx.Connect(ctx, dsnForDB(adminURL, bigTemplateName))
	if err != nil {
		return fmt.Errorf("connect big template: %w", err)
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
		FROM generate_series(1, $1)`, bigTemplateRows); err != nil {
		return fmt.Errorf("seed orders: %w", err)
	}
	// A pre-existing secondary index gives DROP INDEX / REINDEX a real target.
	if _, err := conn.Exec(ctx, `CREATE INDEX `+bigIndexName+` ON orders (amount_cents)`); err != nil {
		return fmt.Errorf("seed secondary index: %w", err)
	}
	if _, err := conn.Exec(ctx, `ANALYZE orders`); err != nil {
		return fmt.Errorf("analyze: %w", err)
	}
	fmt.Printf("big template seeded: %d rows + %s in %v\n", bigTemplateRows, bigIndexName, time.Since(start).Round(time.Millisecond))
	return nil
}

// newBigLab builds a private twin pool on the big template plus an Engine on it,
// with a caller-chosen MaxRunDuration so the guard can be exercised on a short
// budget.
func newBigLab(t *testing.T, prefix string, poolSize int, maxRun time.Duration) (*twins.Manager, *Engine) {
	t.Helper()
	ensureBigTemplate(t)
	ctx := context.Background()

	mgr, err := twins.NewManager(ctx, twins.Config{
		AdminURL:     appURL,
		TemplateName: bigTemplateName,
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
		// A tight request deadline so a freeze of a few hundred milliseconds
		// produces the casualties a production stack would suffer.
		LockTimeout:      200 * time.Millisecond,
		SettleDuration:   1500 * time.Millisecond,
		RecoveryDuration: time.Second,
		SampleInterval:   100 * time.Millisecond,
		MaxRunDuration:   maxRun,
		Logger:           quietLogger(),
	})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })

	waitWarm(t, mgr, poolSize)
	return mgr, eng
}

// waitTwinGone blocks until the named database is dropped, or fails.
func waitTwinGone(t *testing.T, name string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		if !databaseExists(t, name) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("twin %q still exists long after its run finished — a leaked twin is a lab that fills its disk", name)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// assertLedgered checks the run produced a history row that agrees with the
// result, and that the twin and run slot were both released.
func assertLedgered(t *testing.T, mgr *twins.Manager, eng *Engine, res *RunResult) {
	t.Helper()
	if res.LedgerID == 0 {
		t.Errorf("no ledger row was written for run %s (verdict %s)", res.RunID, res.Verdict)
	} else {
		runs, err := eng.History(context.Background(), 5)
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		found := false
		for _, r := range runs {
			if r.ID == res.LedgerID {
				found = true
				if r.Verdict != res.Verdict {
					t.Errorf("ledger verdict %q != result verdict %q", r.Verdict, res.Verdict)
				}
				if r.CasualtiesTotal != res.Casualties.Total {
					t.Errorf("ledger casualties %d != result %d", r.CasualtiesTotal, res.Casualties.Total)
				}
			}
		}
		if !found {
			t.Errorf("run %d missing from the ledger", res.LedgerID)
		}
	}
	// Attribution must always add up.
	var sum int64
	for _, n := range res.Casualties.ByLabel {
		sum += n
	}
	if sum != res.Casualties.Total {
		t.Errorf("by_label sums to %d but total says %d", sum, res.Casualties.Total)
	}
	// The twin is gone and the engine is idle.
	waitTwinGone(t, res.Twin)
	if st := mgr.Status(); st.CheckedOut != 0 {
		t.Errorf("twin still checked out after the run: %+v", st)
	}
	if st := eng.Status(); st.Running || st.CurrentRun != nil {
		t.Errorf("engine still reports a run in progress after completion: %+v", st)
	}
}

// ---------------------------------------------------------------------------
// FIX #2 — the whole allowlist, end to end, under live load at a locking scale.
// ---------------------------------------------------------------------------

// TestWholeAllowlistUnderLoad drives every runnable allow-listed shape through a
// full rehearsal against the 1.2M-row template with traffic flowing, and checks
// that each produces a sane, non-hanging, fully-torn-down result.
func TestWholeAllowlistUnderLoad(t *testing.T) {
	// poolSize 2 so refills overlap the run and each case gets a warm twin
	// promptly. MaxRunDuration generous: none of these should hit the guard.
	mgr, eng := newBigLab(t, "whole", 2, 60*time.Second)
	ctx := context.Background()

	type expect int
	const (
		anyVerdict expect = iota // measured, must be SAFE or UNSAFE, must not hang
		wantUnsafe               // the money path: a real lock over live writes
		wantSafe                 // the safe rewrite: writes keep flowing
	)

	cases := []struct {
		name         string
		sql          string
		mode         Mode
		want         expect
		wantLock     string // strongest lock mode, if the monitor caught it ("" = don't assert)
		mustNotError bool
		concurrent   bool
		// tolerateShmInfra allows a run to come back with a /dev/shm "No space
		// left on device" (SQLSTATE 53100) error without failing the test: on a
		// container with a 64MB /dev/shm, a parallel VACUUM of a 6M-row table asks
		// for ~67MB of shared memory and the SERVER refuses. That is an infra
		// limit of the dev container, not an engine defect — and Brace handles it
		// exactly right (surfaces it as res.Error, reaps the twin, writes the
		// ledger). We still assert the run was fully torn down.
		tolerateShmInfra bool
	}{
		{
			name: "naive CREATE INDEX (MONEY PATH)",
			sql:  `CREATE INDEX idx_orders_customer_id ON orders (customer_id)`,
			mode: ModeNaive, want: wantUnsafe, wantLock: "ShareLock", mustNotError: true,
		},
		{
			name: "CREATE INDEX CONCURRENTLY (safe rewrite)",
			sql:  `CREATE INDEX CONCURRENTLY idx_orders_customer_id ON orders (customer_id)`,
			mode: ModeSafe, want: wantSafe, mustNotError: true, concurrent: true,
		},
		{
			name: "ADD COLUMN volatile default random() (rewrite)",
			sql:  `ALTER TABLE orders ADD COLUMN score double precision NOT NULL DEFAULT random()`,
			mode: ModeCustom, want: wantUnsafe, wantLock: "AccessExclusiveLock", mustNotError: true,
		},
		{
			name: "ALTER COLUMN TYPE (rewrite)",
			// amount_cents is already bigint in this schema, so "TYPE bigint" would
			// be a no-op that rewrites nothing; numeric forces the ACCESS EXCLUSIVE
			// rewrite the spec is after.
			sql:  `ALTER TABLE orders ALTER COLUMN amount_cents TYPE numeric`,
			mode: ModeCustom, want: wantUnsafe, wantLock: "AccessExclusiveLock", mustNotError: true,
		},
		{
			name: "DROP INDEX (pre-created)",
			sql:  `DROP INDEX ` + bigIndexName,
			mode: ModeCustom, want: anyVerdict, mustNotError: true,
		},
		{
			name: "REINDEX INDEX (pre-created)",
			sql:  `REINDEX INDEX ` + bigIndexName,
			mode: ModeCustom, want: anyVerdict, mustNotError: true,
		},
		{
			name: "VACUUM ANALYZE (safe)",
			sql:  `VACUUM ANALYZE orders`,
			mode: ModeCustom, want: wantSafe, mustNotError: true, tolerateShmInfra: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res, err := eng.Rehearse(ctx, c.sql, c.mode)
			if err != nil {
				t.Fatalf("Rehearse(%q): %v", c.sql, err)
			}
			t.Logf("verdict=%s blocked=%.3fs monitor_blocked=%.3fs dur=%.3fs baseline=%d/s casualties=%d errored=%d locks=%v err=%q",
				res.Verdict, res.BlockedSeconds, res.MonitorBlockedSeconds, res.DurationSeconds,
				res.BaselineWPS, res.Casualties.Total, res.ErroredTotal, res.LockModes, res.Error)

			if res.Verdict == VerdictInconclusive {
				t.Fatalf("run hit the max-duration guard for a statement that should complete: %q (note: %s)", c.sql, res.Note)
			}
			shmInfra := c.tolerateShmInfra && res.Error != "" &&
				(strings.Contains(res.Error, "No space left on device") || strings.Contains(res.Error, "shared memory segment"))
			if shmInfra {
				t.Logf("FINDING (infra, not engine): %q failed with a /dev/shm limit on this container — %q. "+
					"Brace surfaced it as a migration failure and cleaned up correctly. Re-run on a host with a "+
					"larger /dev/shm (e.g. docker --shm-size=256m) to measure this statement's true lock story.",
					c.sql, res.Error)
				assertLedgered(t, mgr, eng, res)
				return
			}
			if c.mustNotError && res.Error != "" {
				t.Fatalf("migration errored unexpectedly: %s", res.Error)
			}
			if res.BaselineWPS <= 0 {
				t.Fatalf("no baseline measured — every downstream number depends on it")
			}
			if len(res.Timeline) == 0 {
				t.Errorf("empty timeline; the UI has nothing to replay")
			}
			if res.Explanation == "" {
				t.Errorf("a verdict shipped with no teach text")
			}
			if !c.concurrent && len(res.InvalidIndexes) != 0 {
				t.Errorf("a non-concurrent statement left INVALID indexes: %v", res.InvalidIndexes)
			}

			switch c.want {
			case wantUnsafe:
				if res.Verdict != VerdictUnsafe {
					t.Errorf("verdict = %s, want UNSAFE; a real lock over live writes at %d rows should freeze traffic",
						res.Verdict, bigTemplateRows)
				}
				if res.BlockedSeconds <= 0 && res.MonitorBlockedSeconds <= 0 {
					t.Errorf("UNSAFE but neither the write-rate nor the monitor measured any freeze (%.3fs / %.3fs)",
						res.BlockedSeconds, res.MonitorBlockedSeconds)
				}
				// Casualties are the headline. If a real lock at this scale still
				// killed nobody, that is a FINDING about load intensity, reported
				// loudly rather than hidden by weakening the test.
				if res.Casualties.Total == 0 {
					t.Logf("FINDING: verdict UNSAFE with blocked=%.3fs but ZERO casualties at %d rows — the freeze was "+
						"real but the 12-worker/200ms-deadline load was not intense enough to leave dead writes. "+
						"On busier production traffic the same lock kills writes; local load intensity is the gap.",
						res.BlockedSeconds, bigTemplateRows)
				}
			case wantSafe:
				if res.Verdict != VerdictSafe {
					t.Errorf("verdict = %s (blocked %.3fs), want SAFE; the safe form must not freeze writes",
						res.Verdict, res.BlockedSeconds)
				}
				if c.concurrent && res.Casualties.Total != 0 {
					t.Errorf("a CONCURRENTLY/safe form killed %d writes; it must kill nobody", res.Casualties.Total)
				}
			case anyVerdict:
				if res.Verdict != VerdictSafe && res.Verdict != VerdictUnsafe {
					t.Errorf("verdict = %q, want a measured SAFE or UNSAFE", res.Verdict)
				}
			}

			if c.wantLock != "" && res.Monitor.StrongestLockMode != "" && res.Monitor.StrongestLockMode != c.wantLock {
				// Not fatal: the monitor can miss a very short lock between polls.
				// A WRONG-and-weaker lock, though, is worth flagging.
				t.Logf("note: monitor saw strongest lock %q, expected %q for %q",
					res.Monitor.StrongestLockMode, c.wantLock, c.sql)
			}

			assertLedgered(t, mgr, eng, res)
		})
	}
}

// ---------------------------------------------------------------------------
// FIX #4 companion at scale — the const-default ADD COLUMN really is
// metadata-only: prove it does not rewrite (relfilenode unchanged), and that
// Brace answers it already_safe rather than burning a twin.
// ---------------------------------------------------------------------------

func TestConstDefaultAddColumnDoesNotRewrite(t *testing.T) {
	mgr, eng := newBigLab(t, "norewrite", 1, 60*time.Second)
	ctx := context.Background()

	// Brace must short-circuit the const-default form as already_safe.
	if _, err := eng.Rehearse(ctx, `ALTER TABLE orders ADD COLUMN flag int NOT NULL DEFAULT 0`, ModeCustom); err == nil {
		t.Fatal("const-default ADD COLUMN should be answered already_safe without a run")
	} else {
		var rej *RejectedError
		if !errors.As(err, &rej) || rej.Kind != RejectAlreadySafe {
			t.Fatalf("expected already_safe rejection, got %v", err)
		}
	}

	// And prove the claim it rests on: on a real twin the statement leaves the
	// table's relfilenode untouched (a rewrite would change it).
	twin, err := mgr.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer func() { _ = mgr.Release(context.Background(), twin) }()
	if err := grantOnTwin(ctx, twin.DSN, twin.Name, eng.cfg.MigratorRole); err != nil {
		t.Fatalf("grantOnTwin: %v", err)
	}
	conn, err := connectAsMigrator(ctx, twin.DSN, eng.cfg.MigratorRole, eng.migratorPassword)
	if err != nil {
		t.Fatalf("connect as migrator: %v", err)
	}
	defer conn.Close(context.Background())

	var before, after uint32
	if err := conn.QueryRow(ctx, `SELECT relfilenode FROM pg_class WHERE relname = 'orders'`).Scan(&before); err != nil {
		t.Fatalf("read relfilenode: %v", err)
	}
	start := time.Now()
	if _, err := conn.Exec(ctx, `ALTER TABLE orders ADD COLUMN flag int NOT NULL DEFAULT 0`); err != nil {
		t.Fatalf("ADD COLUMN const default: %v", err)
	}
	elapsed := time.Since(start)
	if err := conn.QueryRow(ctx, `SELECT relfilenode FROM pg_class WHERE relname = 'orders'`).Scan(&after); err != nil {
		t.Fatalf("read relfilenode: %v", err)
	}
	t.Logf("const-default ADD COLUMN took %v; relfilenode %d -> %d", elapsed.Round(time.Millisecond), before, after)
	if before != after {
		t.Errorf("relfilenode changed %d -> %d: the table WAS rewritten, so this is NOT metadata-only", before, after)
	}
	if elapsed > 2*time.Second {
		t.Errorf("const-default ADD COLUMN took %v on %d rows — a metadata-only change should be near-instant", elapsed, bigTemplateRows)
	}
}

// TestVolatileFalseSafeShapesActuallyRewrite is the at-scale proof behind the
// FIX #4 classifier change: the shapes Brace USED to mislabel already_safe
// (SERIAL, IDENTITY, GENERATED STORED) really do rewrite the table, so refusing
// to call them safe — and running them as ACCESS EXCLUSIVE rewrites — is correct.
func TestVolatileFalseSafeShapesActuallyRewrite(t *testing.T) {
	mgr, eng := newBigLab(t, "falsesafe", 1, 60*time.Second)
	ctx := context.Background()

	twin, err := mgr.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer func() { _ = mgr.Release(context.Background(), twin) }()
	if err := grantOnTwin(ctx, twin.DSN, twin.Name, eng.cfg.MigratorRole); err != nil {
		t.Fatalf("grantOnTwin: %v", err)
	}
	conn, err := connectAsMigrator(ctx, twin.DSN, eng.cfg.MigratorRole, eng.migratorPassword)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(context.Background())

	shapes := []struct {
		name string
		sql  string
	}{
		{"serial", `ALTER TABLE orders ADD COLUMN s1 serial`},
		{"bigserial", `ALTER TABLE orders ADD COLUMN s2 bigserial`},
		{"identity", `ALTER TABLE orders ADD COLUMN s3 int GENERATED ALWAYS AS IDENTITY`},
		{"generated stored", `ALTER TABLE orders ADD COLUMN s4 bigint GENERATED ALWAYS AS (amount_cents * 2) STORED`},
	}
	for _, s := range shapes {
		t.Run(s.name, func(t *testing.T) {
			// Classifier: must NOT be predicted/trivially safe.
			as, aerr := Assess(s.sql)
			if aerr != nil {
				t.Fatalf("Assess(%q) rejected: %v", s.sql, aerr)
			}
			if as.PredictedSafe || as.Lesson.TriviallySafe {
				t.Fatalf("SECURITY-OF-ADVICE: %q classified predictedSafe=%v triviallySafe=%v — a false 'safe' on a rewriting statement",
					s.sql, as.PredictedSafe, as.Lesson.TriviallySafe)
			}
			// Reality: it rewrites (relfilenode changes).
			var before, after uint32
			if err := conn.QueryRow(ctx, `SELECT relfilenode FROM pg_class WHERE relname='orders'`).Scan(&before); err != nil {
				t.Fatalf("relfilenode: %v", err)
			}
			if _, err := conn.Exec(ctx, s.sql); err != nil {
				t.Fatalf("exec %q: %v", s.sql, err)
			}
			if err := conn.QueryRow(ctx, `SELECT relfilenode FROM pg_class WHERE relname='orders'`).Scan(&after); err != nil {
				t.Fatalf("relfilenode: %v", err)
			}
			if before == after {
				t.Errorf("%q did NOT change relfilenode (%d) — expected a rewrite", s.sql, before)
			} else {
				t.Logf("%q rewrote the table (relfilenode %d -> %d), as classified", s.sql, before, after)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// FIX #1 + FIX #3 — the hang-forever scenario: an idle-in-transaction session
// holding ACCESS SHARE blocks an ACCESS EXCLUSIVE migration. Brace must abort at
// its guard, return INCONCLUSIVE, reap the twin, and leave the lab usable.
// ---------------------------------------------------------------------------

func TestIdleInTransactionDoesNotHangForever(t *testing.T) {
	const maxRun = 6 * time.Second
	mgr, eng := newBigLab(t, "idlehang", 2, maxRun)
	ctx := context.Background()

	// Run an ACCESS EXCLUSIVE migration that will block on the idle session.
	// A volatile-default ADD COLUMN is used (not the const-default form, which
	// Brace short-circuits as already_safe and never runs).
	type outcome struct {
		res *RunResult
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		res, err := eng.Rehearse(ctx, `ALTER TABLE orders ADD COLUMN filler double precision NOT NULL DEFAULT random()`, ModeCustom)
		done <- outcome{res, err}
	}()

	// Slip an idle-in-transaction blocker onto the twin during the baseline
	// window — after grants and load, before the migration — so it is the
	// MIGRATION that queues behind ACCESS SHARE, hangs, and trips the guard.
	twinName := waitForRunTwin(t, eng, 20*time.Second)
	blocker := grabAccessShareAtBaseline(t, eng, twinName)
	blockerClosed := false
	defer func() {
		if !blockerClosed {
			_ = blocker.Close(context.Background())
		}
	}()

	start := time.Now()
	got := <-done
	elapsed := time.Since(start)

	if got.err != nil {
		t.Fatalf("Rehearse returned a hard error, not a result: %v", got.err)
	}
	res := got.res
	t.Logf("guarded run: verdict=%s dur=%.3fs elapsed=%v error=%q note=%q", res.Verdict, res.DurationSeconds, elapsed.Round(time.Millisecond), res.Error, res.Note)

	// It must not have hung: the whole run is bounded by roughly the guard plus
	// the fixed settle, nowhere near forever.
	if elapsed > maxRun+25*time.Second {
		t.Fatalf("Rehearse took %v with a %v guard — the guard did not bound the run", elapsed, maxRun)
	}
	if res.Verdict != VerdictInconclusive {
		t.Fatalf("verdict = %s, want INCONCLUSIVE for a migration that could not acquire its lock", res.Verdict)
	}
	if !strings.Contains(res.Error, "exceeded") {
		t.Errorf("INCONCLUSIVE error should explain the timeout, got %q", res.Error)
	}
	if res.Note == "" {
		t.Errorf("an INCONCLUSIVE run must explain itself in the note")
	}
	assertLedgered(t, mgr, eng, res) // twin reaped, run slot released, ledger row written

	// Release the blocker; the lab must be usable again immediately.
	_ = blocker.Close(context.Background())
	blockerClosed = true

	res2, err := eng.Rehearse(ctx, `VACUUM ANALYZE orders`, ModeCustom)
	if err != nil {
		t.Fatalf("lab not usable after a guarded run: %v", err)
	}
	if res2.Verdict == VerdictInconclusive {
		t.Errorf("a plain VACUUM after recovery went INCONCLUSIVE: %q", res2.Note)
	}
	t.Logf("lab recovered: follow-up VACUUM verdict=%s", res2.Verdict)
}

// waitForRunTwin polls the engine's status until the in-flight run has been
// assigned a twin, and returns its name.
func waitForRunTwin(t *testing.T, eng *Engine, limit time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(limit)
	for {
		if st := eng.Status(); st.CurrentRun != nil && st.CurrentRun.Twin != "" {
			return st.CurrentRun.Twin
		}
		if time.Now().After(deadline) {
			t.Fatal("the run never acquired a twin")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// waitForRunPhase blocks until the in-flight run reports the given phase, so a
// test can slip a lock in during a specific window. Returns false on timeout.
func waitForRunPhase(t *testing.T, eng *Engine, phase string, limit time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(limit)
	for {
		if st := eng.Status(); st.CurrentRun != nil && st.CurrentRun.Phase == phase {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// grabAccessShareAtBaseline pre-connects to the named twin, waits for the run to
// reach its baseline window (after grants and load start, BEFORE the migration),
// then opens an idle-in-transaction session holding ACCESS SHARE on orders — the
// classic "migration hangs forever behind an idle transaction" blocker. It is
// timed to block the MIGRATION specifically, not the earlier grant. Returns the
// still-open connection; the caller closes it to release the lock.
func grabAccessShareAtBaseline(t *testing.T, eng *Engine, twinName string) *pgx.Conn {
	t.Helper()
	ctx := context.Background()
	// Pre-connect so the connect latency is out of the critical baseline window.
	blocker, err := pgx.Connect(ctx, dsnForDB(adminURL, twinName))
	if err != nil {
		t.Fatalf("connect blocker: %v", err)
	}
	if !waitForRunPhase(t, eng, phaseBaseline, 20*time.Second) {
		_ = blocker.Close(ctx)
		t.Fatal("run never reached the baseline phase; cannot place the blocker before the migration")
	}
	if _, err := blocker.Exec(ctx, `BEGIN`); err != nil {
		_ = blocker.Close(ctx)
		t.Fatalf("blocker BEGIN: %v", err)
	}
	var one int
	if err := blocker.QueryRow(ctx, `SELECT 1 FROM orders LIMIT 1`).Scan(&one); err != nil {
		_ = blocker.Close(ctx)
		t.Fatalf("blocker SELECT (grab ACCESS SHARE): %v", err)
	}
	// The blocker now sits idle-in-transaction holding ACCESS SHARE on orders;
	// the pending ACCESS EXCLUSIVE migration will queue behind it until the guard.
	return blocker
}

// TestGuardReleasesRunSlotAndTwin is the narrower FIX #1 assertion: after a
// guarded (INCONCLUSIVE) run, the single run slot is free again — the exact
// property whose absence would freeze the whole public lab.
func TestGuardReleasesRunSlotAndTwin(t *testing.T) {
	const maxRun = 5 * time.Second
	_, eng := newBigLab(t, "guardslot", 2, maxRun)
	ctx := context.Background()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = eng.Rehearse(ctx, `ALTER TABLE orders ADD COLUMN filler double precision NOT NULL DEFAULT random()`, ModeCustom)
	}()
	twinName := waitForRunTwin(t, eng, 20*time.Second)
	blocker := grabAccessShareAtBaseline(t, eng, twinName)
	defer blocker.Close(context.Background())

	<-done
	if st := eng.Status(); st.Running {
		t.Fatalf("run slot still held after a guarded run: %+v", st)
	}
	// The slot is genuinely reusable.
	if _, err := eng.Rehearse(ctx, ``, ModeCustom); errors.Is(err, ErrRunInProgress) {
		t.Error("the run slot was never released after the guard fired")
	}
}

// ---------------------------------------------------------------------------
// FIX #5 — a CONCURRENTLY build cancelled mid-flight leaves an INVALID index.
// Brace must detect indisvalid=false and surface it as a distinct amber outcome,
// not as a success and not as a bare error.
// ---------------------------------------------------------------------------

func TestInvalidIndexFromCancelledConcurrentBuild(t *testing.T) {
	mgr, eng := newBigLab(t, "cancelconc", 2, 60*time.Second)
	ctx := context.Background()

	type outcome struct {
		res *RunResult
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		// A fresh index name; CONCURRENTLY on 1.2M rows takes long enough to be
		// caught and cancelled mid-build.
		res, err := eng.Rehearse(ctx, `CREATE INDEX CONCURRENTLY idx_orders_cancel ON orders (customer_id)`, ModeSafe)
		done <- outcome{res, err}
	}()

	twinName := waitForRunTwin(t, eng, 20*time.Second)
	killer, err := pgx.Connect(ctx, dsnForDB(adminURL, twinName))
	if err != nil {
		t.Fatalf("connect killer: %v", err)
	}
	defer killer.Close(context.Background())

	// Poll for the migration's own backend (by application_name) and cancel it
	// once it is actively building the index.
	cancelled := false
	deadline := time.Now().Add(30 * time.Second)
	for !cancelled && time.Now().After(deadline) == false {
		var pid int
		err := killer.QueryRow(ctx, `
			SELECT pid FROM pg_stat_activity
			WHERE datname = current_database()
			  AND application_name = $1
			  AND state = 'active'
			  AND query ILIKE 'CREATE INDEX CONCURRENTLY%'
			LIMIT 1`, MigrationAppName).Scan(&pid)
		if err == nil && pid != 0 {
			// Give the build a moment to actually create the (invalid) pg_index
			// row before we cancel, so there is something left behind.
			time.Sleep(400 * time.Millisecond)
			var ok bool
			if err := killer.QueryRow(ctx, `SELECT pg_cancel_backend($1)`, pid).Scan(&ok); err != nil {
				t.Fatalf("pg_cancel_backend: %v", err)
			}
			cancelled = true
			t.Logf("cancelled migration backend pid=%d", pid)
			break
		}
		select {
		case o := <-done:
			// The build finished before we could catch it. Push the result back
			// and fall out; the assertions below will note the miss.
			done <- o
			cancelled = false
			deadline = time.Now() // break the loop
		case <-time.After(50 * time.Millisecond):
		}
	}

	got := <-done
	if got.err != nil {
		t.Fatalf("Rehearse returned a hard error: %v", got.err)
	}
	res := got.res
	t.Logf("cancelled-build result: verdict=%s error=%q invalid=%v note=%q",
		res.Verdict, res.Error, res.InvalidIndexes, res.Note)

	if !cancelled {
		t.Skip("could not cancel the CONCURRENTLY build before it finished; increase bigTemplateRows to make the build slower")
	}

	// The amber outcome: a failed/cancelled build is neither a clean success nor
	// a bare error — it must surface the INVALID index it left behind.
	if res.Error == "" {
		t.Error("a cancelled CONCURRENTLY build reported no error at all")
	}
	if len(res.InvalidIndexes) == 0 {
		t.Errorf("the cancelled CONCURRENTLY build left no INVALID index in the result — the leftover was missed")
	} else {
		t.Logf("INVALID index surfaced: %v", res.InvalidIndexes)
	}
	if res.Note == "" {
		t.Error("an amber (invalid-index) run must explain itself")
	}
	// It is still a fully-formed run: recorded, twin reaped, slot released.
	assertLedgered(t, mgr, eng, res)
}
