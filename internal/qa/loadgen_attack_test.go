//go:build integration

package qa

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/vinayaksonthalia/brace/internal/loadgen"
	"github.com/vinayaksonthalia/brace/internal/twins"
)

const insertOrders = `INSERT INTO orders (customer_id, amount_cents) VALUES ($1, $2)`

// slowUnblocked is a write that takes far LONGER than lock_timeout but never
// waits on a lock. If the pool ever grew a statement_timeout, this is the label
// that would start reporting phantom casualties.
const slowUnblocked = `INSERT INTO slow_orders (customer_id, amount_cents) SELECT $1::bigint, $2::bigint FROM pg_sleep(0.6)`

func newGen(t *testing.T, cfg loadgen.Config) *loadgen.Generator {
	t.Helper()
	cfg.Logger = testLogger(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	g, err := loadgen.New(ctx, cfg)
	if err != nil {
		t.Fatalf("loadgen.New: %v", err)
	}
	return g
}

func stopGen(t *testing.T, g *loadgen.Generator) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := g.Stop(ctx); err != nil {
		t.Errorf("Stop: %v", err)
	}
}

// ---------------------------------------------------------------------------
// ATTACK L1: lifecycle abuse — Start twice, Start after Stop, Stop twice.
// ---------------------------------------------------------------------------

func TestAttackLoadgenLifecycleAbuse(t *testing.T) {
	requireDB(t)
	gate := goroutineGate(t)
	ctx := context.Background()
	dsn := createScratchDB(t, qaScratchPrefix+"life")

	g := newGen(t, loadgen.Config{DSN: dsn, Workers: 4, LockTimeout: 300 * time.Millisecond})

	if err := g.Start(ctx); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	if err := g.Start(ctx); err == nil {
		t.Error("FINDING: second Start returned nil; workers would be doubled silently")
	} else {
		t.Logf("second Start rejected: %v", err)
	}
	// Hammer Start from many goroutines at once: exactly one must win.
	var wins int64
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := g.Start(ctx); err == nil {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if wins != 0 {
		t.Errorf("FINDING: %d concurrent Start calls succeeded after the generator was already running", wins)
	}

	time.Sleep(500 * time.Millisecond)
	stopGen(t, g)
	stopGen(t, g) // idempotent

	if err := g.Start(ctx); !errors.Is(err, loadgen.ErrClosed) {
		t.Errorf("Start after Stop returned %v, want ErrClosed", err)
	}
	// Snapshot must remain callable and stable after Stop.
	a := g.Snapshot()
	time.Sleep(200 * time.Millisecond)
	b := g.Snapshot()
	if a.CasualtiesTotal != b.CasualtiesTotal || a.ErroredTotal != b.ErroredTotal {
		t.Errorf("counters still moving after Stop: %+v then %+v", a, b)
	}
	t.Logf("post-stop snapshot: %+v", b)
	gate()
}

// ---------------------------------------------------------------------------
// ATTACK L2: Stop before Start, and Snapshot before Start.
// ---------------------------------------------------------------------------

func TestAttackLoadgenStopBeforeStart(t *testing.T) {
	requireDB(t)
	gate := goroutineGate(t)
	ctx := context.Background()
	dsn := createScratchDB(t, qaScratchPrefix+"nostart")

	g := newGen(t, loadgen.Config{DSN: dsn, Workers: 3})
	s := g.Snapshot()
	if s.CasualtiesTotal != 0 || s.ErroredTotal != 0 || s.WritesPerSec != 0 {
		t.Errorf("fresh generator has nonzero counters: %+v", s)
	}
	if len(s.ByLabel) != len(loadgen.DefaultLabels()) {
		t.Errorf("fresh Snapshot has %d labels, want %d", len(s.ByLabel), len(loadgen.DefaultLabels()))
	}
	if err := g.Stop(ctx); err != nil {
		t.Errorf("Stop before Start: %v", err)
	}
	if err := g.Stop(ctx); err != nil {
		t.Errorf("second Stop before Start: %v", err)
	}
	if err := g.Start(ctx); !errors.Is(err, loadgen.ErrClosed) {
		t.Errorf("Start after Stop-before-Start returned %v, want ErrClosed", err)
	}

	// The pool must actually be gone.
	c := adminConn(t)
	deadline := time.Now().Add(5 * time.Second)
	for countBackends(t, c, qaScratchPrefix+"nostart") > 0 && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	if n := countBackends(t, c, qaScratchPrefix+"nostart"); n > 0 {
		t.Errorf("Stop-before-Start left %d backend connections open", n)
	}
	gate()
}

// ---------------------------------------------------------------------------
// ATTACK L3: 50 goroutines hammering Snapshot during live load (race detector).
// ---------------------------------------------------------------------------

func TestAttackLoadgenSnapshotStorm(t *testing.T) {
	requireDB(t)
	gate := goroutineGate(t)
	ctx := context.Background()
	dsn := createScratchDB(t, qaScratchPrefix+"snap")

	g := newGen(t, loadgen.Config{DSN: dsn, Workers: 12, LockTimeout: 300 * time.Millisecond})
	if err := g.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var mono sync.Mutex
	prev := map[string]loadgen.LabelStats{}
	var regressions []string

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				s := g.Snapshot()
				// Mutating the returned map must not corrupt the generator.
				s.ByLabel["injected"] = loadgen.LabelStats{OK: -1}
				var sum int64
				for _, ls := range s.ByLabel {
					if ls.OK < 0 && ls.Died == 0 && ls.Errored == 0 {
						continue // our own injected entry
					}
					if ls.OK < 0 || ls.Died < 0 || ls.Errored < 0 {
						mono.Lock()
						regressions = append(regressions, fmt.Sprintf("negative counter: %+v", ls))
						mono.Unlock()
					}
					sum += ls.Died
				}
				if sum != s.CasualtiesTotal {
					mono.Lock()
					regressions = append(regressions,
						fmt.Sprintf("CasualtiesTotal %d != sum of per-label Died %d", s.CasualtiesTotal, sum))
					mono.Unlock()
				}
			}
		}()
	}
	// One monotonicity auditor: counters must never go backwards.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			s := g.Snapshot()
			mono.Lock()
			for name, ls := range s.ByLabel {
				if name == "injected" {
					continue
				}
				if p, ok := prev[name]; ok {
					if ls.OK < p.OK || ls.Died < p.Died || ls.Errored < p.Errored {
						regressions = append(regressions,
							fmt.Sprintf("label %s went backwards: %+v -> %+v", name, p, ls))
					}
				}
				prev[name] = ls
			}
			mono.Unlock()
			time.Sleep(time.Millisecond)
		}
	}()

	time.Sleep(3 * time.Second)
	close(stop)
	wg.Wait()

	final := g.Snapshot()
	if _, poisoned := final.ByLabel["injected"]; poisoned {
		t.Error("FINDING: Snapshot returns a map aliased to generator state; a caller poisoned it")
	}
	for _, r := range regressions {
		t.Error(r)
	}
	t.Logf("snapshot storm final: wps=%d by_label=%+v", final.WritesPerSec, final.ByLabel)
	if final.ByLabel["checkout"].OK == 0 {
		t.Error("no writes landed during the snapshot storm; test proved nothing")
	}
	stopGen(t, g)
	gate()
}

// ---------------------------------------------------------------------------
// ATTACK L4: unreachable DSNs. New must fail fast, never hang.
// ---------------------------------------------------------------------------

func TestAttackLoadgenUnreachableDSN(t *testing.T) {
	gate := goroutineGate(t)

	cases := []struct {
		name string
		dsn  string
	}{
		{"closed port", "postgres://postgres@127.0.0.1:1/postgres?sslmode=disable&connect_timeout=3"},
		{"nonexistent host", "postgres://postgres@brace-qa-no-such-host.invalid:5432/postgres?sslmode=disable&connect_timeout=3"},
		{"blackholed ip", "postgres://postgres@192.0.2.1:5432/postgres?sslmode=disable&connect_timeout=3"},
		{"garbage dsn", "this is not a dsn at all"},
	}
	if dbSkipMsg == "" {
		cases = append(cases, struct {
			name string
			dsn  string
		}{"nonexistent database", dsnFor("brace_qa_no_such_database") + "?sslmode=disable&connect_timeout=3"})
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			done := make(chan error, 1)
			start := time.Now()
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
				defer cancel()
				g, err := loadgen.New(ctx, loadgen.Config{DSN: tc.dsn, Workers: 4, Logger: testLogger(t)})
				if err == nil {
					_ = g.Stop(context.Background())
				}
				done <- err
			}()
			select {
			case err := <-done:
				elapsed := time.Since(start)
				t.Logf("New failed after %s: %v", elapsed, err)
				if err == nil {
					t.Errorf("FINDING: New succeeded against an unreachable DSN %q", tc.dsn)
				}
				if elapsed > 20*time.Second {
					t.Errorf("FINDING: New took %s to give up on %q; a UI calling this would hang", elapsed, tc.dsn)
				}
			case <-time.After(30 * time.Second):
				t.Fatalf("FINDING: loadgen.New HUNG for >30s on DSN %q", tc.dsn)
			}
		})
	}
	gate()
}

// ---------------------------------------------------------------------------
// ATTACK L5: a label whose SQL is invalid, or whose arity is wrong.
// It must land in `errored` and NEVER in `died`.
// ---------------------------------------------------------------------------

func TestAttackLoadgenBrokenStatementsNeverCountAsCasualties(t *testing.T) {
	requireDB(t)
	gate := goroutineGate(t)
	ctx := context.Background()
	dsn := createScratchDB(t, qaScratchPrefix+"broken")

	g := newGen(t, loadgen.Config{
		DSN:         dsn,
		Workers:     6,
		LockTimeout: 200 * time.Millisecond,
		Labels: []loadgen.LabelSpec{
			{Name: "good", Weight: 1, Statement: insertOrders},
			{Name: "syntax_error", Weight: 1, Statement: `INSRT INTO orders (customer_id, amount_cents) VALUES ($1, $2)`},
			{Name: "missing_table", Weight: 1, Statement: `INSERT INTO no_such_table (customer_id, amount_cents) VALUES ($1, $2)`},
			{Name: "wrong_arity", Weight: 1, Statement: `INSERT INTO orders (customer_id, amount_cents) VALUES ($1, $1)`},
			{Name: "type_error", Weight: 1, Statement: `INSERT INTO orders (customer_id, amount_cents) VALUES ($1, $2::uuid)`},
			{Name: "constraint_violation", Weight: 1, Statement: `INSERT INTO orders (id, customer_id, amount_cents) VALUES (1, $1, $2)`},
		},
	})
	if err := g.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	time.Sleep(2500 * time.Millisecond)
	s := g.Snapshot()
	stopGen(t, g)

	t.Logf("broken-label snapshot: %+v", s)
	if s.CasualtiesTotal != 0 {
		t.Errorf("SACRED INVARIANT VIOLATED: %d casualties recorded with no lock anywhere in the test: %+v",
			s.CasualtiesTotal, s.ByLabel)
	}
	for _, name := range []string{"syntax_error", "missing_table", "type_error", "constraint_violation"} {
		ls := s.ByLabel[name]
		if ls.Errored == 0 {
			t.Errorf("label %q never registered an error (%+v); it should be visible in the errored bucket", name, ls)
		}
		if ls.Died != 0 {
			t.Errorf("label %q recorded %d casualties from a non-lock failure", name, ls.Died)
		}
		if ls.OK != 0 {
			t.Errorf("label %q recorded %d successes despite being unrunnable", name, ls.OK)
		}
	}
	// wrong_arity uses $1 twice, so pgx sends 2 args for 1 placeholder.
	wa := s.ByLabel["wrong_arity"]
	t.Logf("wrong_arity (extra bind parameter): %+v", wa)
	if wa.Died != 0 {
		t.Errorf("wrong_arity recorded %d casualties", wa.Died)
	}
	if s.ByLabel["good"].OK == 0 {
		t.Error("the healthy label never succeeded; the error storm starved real traffic")
	}
	gate()
}

// ---------------------------------------------------------------------------
// ATTACK L6: hostile label configuration.
// ---------------------------------------------------------------------------

func TestAttackLoadgenHostileConfig(t *testing.T) {
	requireDB(t)
	ctx := context.Background()
	dsn := createScratchDB(t, qaScratchPrefix+"cfg")

	bad := []struct {
		name   string
		labels []loadgen.LabelSpec
	}{
		{"zero weight", []loadgen.LabelSpec{{Name: "a", Weight: 0, Statement: insertOrders}}},
		{"all zero weights", []loadgen.LabelSpec{
			{Name: "a", Weight: 0, Statement: insertOrders},
			{Name: "b", Weight: 0, Statement: insertOrders},
		}},
		{"negative weight", []loadgen.LabelSpec{{Name: "a", Weight: -5, Statement: insertOrders}}},
		{"one zero among many", []loadgen.LabelSpec{
			{Name: "a", Weight: 3, Statement: insertOrders},
			{Name: "b", Weight: 0, Statement: insertOrders},
		}},
		{"empty name", []loadgen.LabelSpec{{Name: "", Weight: 1, Statement: insertOrders}}},
		{"whitespace name", []loadgen.LabelSpec{{Name: "  ", Weight: 1, Statement: insertOrders}}},
		{"duplicate names", []loadgen.LabelSpec{
			{Name: "a", Weight: 1, Statement: insertOrders},
			{Name: "a", Weight: 1, Statement: insertOrders},
		}},
		{"empty statement", []loadgen.LabelSpec{{Name: "a", Weight: 1, Statement: "   "}}},
	}
	for _, tc := range bad {
		t.Run("reject/"+tc.name, func(t *testing.T) {
			g, err := loadgen.New(ctx, loadgen.Config{DSN: dsn, Workers: 2, Labels: tc.labels, Logger: testLogger(t)})
			if err == nil {
				_ = g.Stop(ctx)
				t.Errorf("FINDING: New ACCEPTED hostile labels %+v", tc.labels)
				return
			}
			t.Logf("rejected: %v", err)
		})
	}

	t.Run("zero and negative workers default", func(t *testing.T) {
		for _, w := range []int{0, -1, -1000} {
			g := newGen(t, loadgen.Config{DSN: dsn, Workers: w})
			c := adminConn(t)
			if err := g.Start(ctx); err != nil {
				t.Fatalf("Start with Workers=%d: %v", w, err)
			}
			deadline := time.Now().Add(10 * time.Second)
			n := 0
			for time.Now().Before(deadline) {
				n = countBackends(t, c, qaScratchPrefix+"cfg")
				if n >= 24 {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			t.Logf("Workers=%d produced %d backends (documented default is 24)", w, n)
			if n < 20 {
				t.Errorf("Workers=%d produced only %d backends; expected the documented default of 24", w, n)
			}
			stopGen(t, g)
		}
	})

	t.Run("sub-millisecond lock timeout", func(t *testing.T) {
		g := newGen(t, loadgen.Config{DSN: dsn, Workers: 2, LockTimeout: 100 * time.Microsecond})
		if err := g.Start(ctx); err != nil {
			t.Fatalf("Start: %v", err)
		}
		time.Sleep(1500 * time.Millisecond)
		s := g.Snapshot()
		stopGen(t, g)
		t.Logf("100us lock timeout (clamped to 1ms): %+v", s)
		if s.CasualtiesTotal > 0 {
			t.Logf("note: %d casualties from ordinary uncontended traffic at a 1ms lock timeout", s.CasualtiesTotal)
		}
		var ok int64
		for _, ls := range s.ByLabel {
			ok += ls.OK
		}
		if ok == 0 {
			t.Errorf("FINDING: a sub-millisecond LockTimeout was clamped to 0 (no timeout) or to something "+
				"that blocks all traffic; zero successful writes: %+v", s)
		}
	})
}

// ---------------------------------------------------------------------------
// ATTACK L7: more workers than the server has connection slots.
// ---------------------------------------------------------------------------

func TestAttackLoadgenWorkersExceedMaxConnections(t *testing.T) {
	requireDB(t)
	gate := goroutineGate(t)
	ctx := context.Background()
	name := qaScratchPrefix + "storm"
	dsn := createScratchDB(t, name)
	c := adminConn(t)

	var maxConns int
	if err := c.QueryRow(ctx, `SELECT setting::int FROM pg_settings WHERE name='max_connections'`).Scan(&maxConns); err != nil {
		t.Fatalf("read max_connections: %v", err)
	}
	workers := maxConns + 50
	t.Logf("server max_connections=%d; launching %d workers", maxConns, workers)

	newCtx, newCancel := context.WithTimeout(ctx, 30*time.Second)
	g, err := loadgen.New(newCtx, loadgen.Config{
		DSN: dsn, Workers: workers, LockTimeout: 300 * time.Millisecond, Logger: testLogger(t),
	})
	newCancel()
	if err != nil {
		t.Logf("New refused an oversubscribed pool: %v", err)
		gate()
		return
	}

	if err := g.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	time.Sleep(4 * time.Second)
	s := g.Snapshot()
	t.Logf("oversubscribed: wps=%d ok/died/errored per label=%+v casualties=%d errored=%d",
		s.WritesPerSec, s.ByLabel, s.CasualtiesTotal, s.ErroredTotal)

	stopCtx, stopCancel := context.WithTimeout(ctx, 60*time.Second)
	stopErr := g.Stop(stopCtx)
	stopCancel()
	if stopErr != nil {
		t.Errorf("FINDING: Stop failed after connection oversubscription: %v", stopErr)
	}

	if s.CasualtiesTotal > 0 {
		t.Errorf("SACRED INVARIANT VIOLATED: %d casualties recorded with no migration lock in play; "+
			"connection exhaustion must never be attributed to a lock wait", s.CasualtiesTotal)
	}
	var ok int64
	for _, ls := range s.ByLabel {
		ok += ls.OK
	}
	if ok == 0 {
		t.Errorf("FINDING: no writes at all succeeded when Workers > max_connections; "+
			"the generator did not degrade, it stalled completely (%+v)", s.ByLabel)
	}
	if s.ErroredTotal == 0 {
		t.Logf("note: connection pressure produced no errored writes (pool queued instead of failing)")
	}

	// Connections must all be handed back.
	deadline := time.Now().Add(20 * time.Second)
	for countBackends(t, c, name) > 0 && time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
	}
	if n := countBackends(t, c, name); n > 0 {
		t.Errorf("FINDING: %d backends still connected to %q after Stop", n, name)
	}
	gate()
}

// ---------------------------------------------------------------------------
// ATTACK L8: THE SACRED INVARIANT, under a real ACCESS EXCLUSIVE lock, with a
// slow-but-unblocked workload running at the same time.
//
// Only lock-blocked writes may be counted as casualties. Writes that merely
// take longer than lock_timeout must be counted as successes.
// ---------------------------------------------------------------------------

func TestAttackLoadgenSacredInvariantSlowVersusBlocked(t *testing.T) {
	requireDB(t)
	defer goroutineGate(t)() // registered first, so it runs LAST (after stopGen)
	ctx := context.Background()
	name := qaScratchPrefix + "sacred"
	dsn := createScratchDB(t, name)

	const lockTimeout = 250 * time.Millisecond
	g := newGen(t, loadgen.Config{
		DSN:         dsn,
		Workers:     10,
		LockTimeout: lockTimeout,
		Labels: []loadgen.LabelSpec{
			// Writes to the table the "migration" will lock.
			{Name: "blocked_traffic", Weight: 1, Statement: insertOrders},
			// Writes that take 600ms (>> lock_timeout) but never wait on a lock.
			{Name: "slow_traffic", Weight: 1, Statement: slowUnblocked},
		},
	})
	if err := g.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer stopGen(t, g)

	// Baseline: both labels healthy with no lock held.
	time.Sleep(2 * time.Second)
	base := g.Snapshot()
	t.Logf("baseline (no lock): %+v", base.ByLabel)
	if base.ByLabel["slow_traffic"].OK == 0 {
		t.Fatalf("slow label never completed a write in 2s; adjust the fixture (%+v)", base.ByLabel)
	}
	if base.CasualtiesTotal != 0 {
		t.Errorf("SACRED INVARIANT VIOLATED: %d casualties before any lock was taken (%+v)",
			base.CasualtiesTotal, base.ByLabel)
	}

	// Take the lock a migration would take, and hold it well past lock_timeout.
	lockConn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("lock connection: %v", err)
	}
	tx, err := lockConn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec(ctx, `LOCK TABLE orders IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatalf("LOCK TABLE: %v", err)
	}
	t.Log("ACCESS EXCLUSIVE lock on orders held for 3s")
	lockStart := time.Now()
	time.Sleep(3 * time.Second)
	during := g.Snapshot()
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	lockConn.Close(ctx)
	t.Logf("lock released after %s", time.Since(lockStart))

	blockedDied := during.ByLabel["blocked_traffic"].Died - base.ByLabel["blocked_traffic"].Died
	blockedOK := during.ByLabel["blocked_traffic"].OK - base.ByLabel["blocked_traffic"].OK
	slowDied := during.ByLabel["slow_traffic"].Died - base.ByLabel["slow_traffic"].Died
	slowOK := during.ByLabel["slow_traffic"].OK - base.ByLabel["slow_traffic"].OK
	slowErr := during.ByLabel["slow_traffic"].Errored - base.ByLabel["slow_traffic"].Errored
	blockedErr := during.ByLabel["blocked_traffic"].Errored - base.ByLabel["blocked_traffic"].Errored

	t.Logf("during lock window: blocked_traffic ok=%d died=%d errored=%d | slow_traffic ok=%d died=%d errored=%d",
		blockedOK, blockedDied, blockedErr, slowOK, slowDied, slowErr)

	if blockedDied == 0 {
		t.Errorf("FINDING: a 3s ACCESS EXCLUSIVE lock with a %s lock_timeout produced ZERO casualties; "+
			"the headline number does not detect the harm it exists to measure", lockTimeout)
	}
	if slowDied != 0 {
		t.Errorf("SACRED INVARIANT VIOLATED: %d casualties attributed to slow_traffic, which never "+
			"waited on a lock. Slow writes are being miscounted as migration casualties.", slowDied)
	}
	if slowOK == 0 {
		t.Errorf("FINDING: slow-but-unblocked writes stopped succeeding while an unrelated table was " +
			"locked (ok delta 0); cross-contamination between workloads")
	}
	if slowErr != 0 {
		t.Errorf("FINDING: slow_traffic recorded %d errors during the lock window; a write on an "+
			"unlocked table should be unaffected", slowErr)
	}
	if blockedOK != 0 {
		t.Logf("note: %d writes to the locked table still committed during the lock window", blockedOK)
	}

	// After the lock is released, casualties must stop accruing.
	time.Sleep(2 * time.Second)
	after := g.Snapshot()
	postDied := after.CasualtiesTotal - during.CasualtiesTotal
	postOK := after.ByLabel["blocked_traffic"].OK - during.ByLabel["blocked_traffic"].OK
	t.Logf("after lock release: casualties+%d ok+%d", postDied, postOK)
	if postOK == 0 {
		t.Error("FINDING: traffic to the locked table did not recover after the lock was released")
	}
	if postDied > 0 {
		t.Errorf("FINDING: %d casualties accrued %s AFTER the lock was released", postDied, 2*time.Second)
	}
}

// ---------------------------------------------------------------------------
// ATTACK L9: Stop whose context expires before the workers finish.
// The package documents that it leaves the pool open. What it does not
// document is whether the caller can ever get it back.
// ---------------------------------------------------------------------------

func TestAttackLoadgenStopTimeoutStrandsThePool(t *testing.T) {
	requireDB(t)
	ctx := context.Background()
	name := qaScratchPrefix + "stranded"
	dsn := createScratchDB(t, name)
	c := adminConn(t)

	g := newGen(t, loadgen.Config{DSN: dsn, Workers: 4, LockTimeout: 300 * time.Millisecond})
	if err := g.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	time.Sleep(1 * time.Second)
	if n := countBackends(t, c, name); n == 0 {
		t.Fatal("no backends connected; test cannot prove anything")
	}

	dead, cancel := context.WithCancel(ctx)
	cancel()
	err := g.Stop(dead)
	if err == nil {
		t.Skip("Stop beat the cancelled context; attack does not apply on this run")
	}
	t.Logf("Stop with an expired context returned: %v", err)

	// Workers do wind down (cancel was still issued). Give them time.
	time.Sleep(3 * time.Second)

	// Retry: the API's only lever.
	retryCtx, retryCancel := context.WithTimeout(ctx, 10*time.Second)
	retryErr := g.Stop(retryCtx)
	retryCancel()
	t.Logf("retried Stop with a generous context returned: %v", retryErr)

	deadline := time.Now().Add(10 * time.Second)
	n := 0
	for time.Now().Before(deadline) {
		n = countBackends(t, c, name)
		if n == 0 {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if n > 0 {
		t.Errorf("FINDING: after a Stop that timed out, the pool is unreclaimable. "+
			"%d server connections to %q remain open, the workers have exited, and the "+
			"idempotent second Stop returned %v without closing the pool. There is no API "+
			"to recover it — the connections leak for the life of the process.", n, name, retryErr)
	} else {
		t.Logf("pool was reclaimed by the retried Stop; %d backends remain", n)
	}
}

// ---------------------------------------------------------------------------
// ATTACK L10 (CROSS): twin FORCE-dropped out from under a running generator.
// ---------------------------------------------------------------------------

func TestAttackCrossReleaseTwinUnderLiveLoad(t *testing.T) {
	requireDB(t)
	defer goroutineGate(t)() // registered first, so it runs LAST
	ctx := context.Background()

	m := newManager(t, twins.Config{PoolSize: 1, NamePrefix: "brace_qa_run_cross_"})
	defer closeManager(t, m)
	waitWarm(t, m, 1, 60*time.Second)

	tw, err := m.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	t.Logf("acquired twin %s", tw.Name)

	g := newGen(t, loadgen.Config{DSN: tw.DSN, Workers: 8, LockTimeout: 300 * time.Millisecond})
	if err := g.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	time.Sleep(1500 * time.Millisecond)
	before := g.Snapshot()
	if before.ByLabel["checkout"].OK+before.ByLabel["login"].OK+before.ByLabel["add_to_cart"].OK == 0 {
		t.Fatalf("no writes landed on the twin before the drop: %+v", before)
	}
	t.Logf("before drop: %+v", before.ByLabel)

	// The orchestrator should never do this. Defense in depth.
	dropStart := time.Now()
	if err := m.Release(ctx, tw); err != nil {
		t.Fatalf("Release under load: %v", err)
	}
	t.Logf("twin FORCE-dropped in %s while %d workers were writing to it", time.Since(dropStart), 8)

	time.Sleep(2500 * time.Millisecond)
	after := g.Snapshot()
	t.Logf("after drop: casualties=%d errored=%d by_label=%+v",
		after.CasualtiesTotal, after.ErroredTotal, after.ByLabel)

	if after.CasualtiesTotal != before.CasualtiesTotal {
		t.Errorf("SACRED INVARIANT VIOLATED: dropping the database added %d casualties. "+
			"Our own teardown must never be counted as migration harm.",
			after.CasualtiesTotal-before.CasualtiesTotal)
	}
	if after.ErroredTotal <= before.ErroredTotal {
		t.Errorf("FINDING: the database vanished and the errored bucket did not move (%d -> %d); "+
			"the failure is invisible", before.ErroredTotal, after.ErroredTotal)
	}

	// Stop must still be clean, and quick — a hung Stop here would wedge a demo.
	stopStart := time.Now()
	stopCtx, stopCancel := context.WithTimeout(ctx, 30*time.Second)
	stopErr := g.Stop(stopCtx)
	stopCancel()
	t.Logf("Stop after the database vanished returned %v in %s", stopErr, time.Since(stopStart))
	if stopErr != nil {
		t.Errorf("FINDING: Stop failed after the target database was dropped: %v", stopErr)
	}
	if time.Since(stopStart) > 10*time.Second {
		t.Errorf("FINDING: Stop took %s after the database vanished", time.Since(stopStart))
	}
}

// ---------------------------------------------------------------------------
// ATTACK L11: integer overflow in the weighted label picker.
// Weight is a plain int and validate() only checks Weight > 0, so weights that
// sum past MaxInt wrap negative. Run in a child process: if it panics, it takes
// the whole process with it, which is exactly the point.
// ---------------------------------------------------------------------------

func TestAttackLoadgenWeightOverflowPanics(t *testing.T) {
	requireDB(t)

	if target := os.Getenv("BRACE_QA_OVERFLOW_DSN"); target != "" {
		g, err := loadgen.New(context.Background(), loadgen.Config{
			DSN:     target,
			Workers: 2,
			Labels: []loadgen.LabelSpec{
				{Name: "a", Weight: math.MaxInt/2 + 1, Statement: insertOrders},
				{Name: "b", Weight: math.MaxInt/2 + 1, Statement: insertOrders},
			},
			Logger: testLogger(t),
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "CHILD: New rejected overflowing weights: %v\n", err)
			return
		}
		fmt.Fprintln(os.Stderr, "CHILD: New ACCEPTED overflowing weights; starting workers")
		if err := g.Start(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "CHILD: Start rejected: %v\n", err)
			return
		}
		time.Sleep(2 * time.Second)
		_ = g.Stop(context.Background())
		fmt.Fprintln(os.Stderr, "CHILD: survived")
		return
	}

	dsn := createScratchDB(t, qaScratchPrefix+"overflow")
	cmd := exec.Command(os.Args[0], "-test.run=TestAttackLoadgenWeightOverflowPanics", "-test.v", "-test.timeout=90s")
	cmd.Env = append(os.Environ(), "BRACE_QA_CHILD=1", "BRACE_QA_OVERFLOW_DSN="+dsn)
	out, err := cmd.CombinedOutput()
	text := string(out)
	t.Logf("child exit: %v", err)
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "CHILD:") || strings.Contains(line, "panic") ||
			strings.Contains(line, "invalid argument to Intn") {
			t.Logf("  %s", line)
		}
	}

	switch {
	case strings.Contains(text, "CHILD: New rejected overflowing weights"):
		t.Log("PASS: New rejects weights that overflow the cumulative total")
	case strings.Contains(text, "invalid argument to Intn"), strings.Contains(text, "panic:"):
		t.Errorf("FINDING: label weights summing past MaxInt wrap negative, and the weighted " +
			"picker panics (rand.Intn on a non-positive bound). The panic is inside a worker " +
			"goroutine, so it is unrecoverable and kills the whole process. Config.validate() " +
			"checks each Weight > 0 but never checks the running total.")
	case strings.Contains(text, "CHILD: survived"):
		t.Log("PASS: overflowing weights did not crash the generator")
	default:
		t.Errorf("child produced an unclassifiable result (exit %v):\n%s", err, text)
	}
}
