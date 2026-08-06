//go:build integration

package qa

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

func newManager(t *testing.T, cfg twins.Config) *twins.Manager {
	t.Helper()
	if cfg.AdminURL == "" {
		cfg.AdminURL = adminURL
	}
	if cfg.TemplateName == "" {
		cfg.TemplateName = qaTemplate
	}
	if cfg.NamePrefix == "" {
		cfg.NamePrefix = qaPrefix
	}
	cfg.Logger = testLogger(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	m, err := twins.NewManager(ctx, cfg)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	return m
}

func closeManager(t *testing.T, m *twins.Manager) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := m.Close(ctx); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// waitWarm blocks until the pool has at least n warm twins.
func waitWarm(t *testing.T, m *twins.Manager, n int, limit time.Duration) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for {
		if m.Status().Warm >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("pool never reached %d warm twins; status=%+v", n, m.Status())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// ATTACK T1: 20-goroutine Acquire/Release storm.
// Invariant under test: a twin is never handed to two callers at once, and no
// database survives the storm.
// ---------------------------------------------------------------------------

func TestAttackTwinsAcquireReleaseStorm(t *testing.T) {
	requireDB(t)
	gate := goroutineGate(t)

	m := newManager(t, twins.Config{PoolSize: 3})
	waitWarm(t, m, 1, 60*time.Second)

	var (
		mu       sync.Mutex
		live     = map[string]int{} // twin name -> concurrent holders
		everSeen = map[string]bool{}
		dupes    []string
		acquired int
		released int
		noWarm   int
		otherErr []error
	)

	deadline := time.Now().Add(4 * time.Second)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx := context.Background()
			for time.Now().Before(deadline) {
				tw, err := m.Acquire(ctx)
				if err != nil {
					if errors.Is(err, twins.ErrNoWarmTwin) {
						mu.Lock()
						noWarm++
						mu.Unlock()
						time.Sleep(5 * time.Millisecond)
						continue
					}
					mu.Lock()
					otherErr = append(otherErr, err)
					mu.Unlock()
					return
				}

				mu.Lock()
				live[tw.Name]++
				if live[tw.Name] > 1 {
					dupes = append(dupes, fmt.Sprintf("%s held by %d callers at once", tw.Name, live[tw.Name]))
				}
				if everSeen[tw.Name] {
					dupes = append(dupes, fmt.Sprintf("%s re-issued after a previous full lifecycle", tw.Name))
				}
				everSeen[tw.Name] = true
				acquired++
				mu.Unlock()

				time.Sleep(time.Duration(2+id%5) * time.Millisecond)

				mu.Lock()
				live[tw.Name]--
				if live[tw.Name] == 0 {
					delete(live, tw.Name)
				}
				mu.Unlock()

				if err := m.Release(ctx, tw); err != nil {
					mu.Lock()
					otherErr = append(otherErr, err)
					mu.Unlock()
					return
				}
				mu.Lock()
				released++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	t.Logf("storm: acquired=%d released=%d no_warm=%d", acquired, released, noWarm)
	if acquired == 0 {
		t.Fatal("storm never acquired a twin; test proved nothing")
	}
	if len(dupes) > 0 {
		t.Errorf("EXCLUSIVITY VIOLATED:\n  %s", strings.Join(dupes, "\n  "))
	}
	for _, err := range otherErr {
		t.Errorf("unexpected error during storm: %v", err)
	}

	st := m.Status()
	if st.CheckedOut != 0 {
		t.Errorf("after storm CheckedOut=%d, want 0 (names=%v)", st.CheckedOut, st.CheckedOutNames)
	}

	closeManager(t, m)

	c := adminConn(t)
	leftover, err := listPrefixed(context.Background(), c, qaPrefix)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(leftover) != 0 {
		t.Errorf("storm leaked databases: %v", leftover)
	}
	gate()
}

// ---------------------------------------------------------------------------
// ATTACK T2: Acquire once, Release twice.
// ---------------------------------------------------------------------------

func TestAttackTwinsDoubleRelease(t *testing.T) {
	requireDB(t)
	gate := goroutineGate(t)
	ctx := context.Background()

	m := newManager(t, twins.Config{PoolSize: 2})
	waitWarm(t, m, 1, 60*time.Second)

	tw, err := m.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if err := m.Release(ctx, tw); err != nil {
		t.Fatalf("first Release: %v", err)
	}
	err2 := m.Release(ctx, tw)
	t.Logf("second Release of %s returned: %v", tw.Name, err2)
	if err2 != nil {
		t.Errorf("second Release should be a harmless no-op, got %v", err2)
	}

	st := m.Status()
	if st.CheckedOut != 0 {
		t.Errorf("CheckedOut=%d after double release, want 0", st.CheckedOut)
	}
	closeManager(t, m)
	gate()
}

// ---------------------------------------------------------------------------
// ATTACK T3: Release a Twin the Manager never created.
// Release takes a caller-supplied *Twin and issues DROP DATABASE on its Name.
// Does it verify ownership first?
// ---------------------------------------------------------------------------

func TestAttackTwinsReleaseForeignDatabase(t *testing.T) {
	requireDB(t)
	gate := goroutineGate(t)
	ctx := context.Background()
	c := adminConn(t)

	const bystander = "brace_qa_bystander"
	if _, err := c.Exec(ctx, `DROP DATABASE IF EXISTS `+quoteIdent(bystander)+` WITH (FORCE)`); err != nil {
		t.Fatalf("pre-drop: %v", err)
	}
	if _, err := c.Exec(ctx, `CREATE DATABASE `+quoteIdent(bystander)); err != nil {
		t.Fatalf("create bystander: %v", err)
	}
	t.Cleanup(func() {
		cc, err := pgx.Connect(context.Background(), adminURL)
		if err != nil {
			return
		}
		defer cc.Close(context.Background())
		_, _ = cc.Exec(context.Background(), `DROP DATABASE IF EXISTS `+quoteIdent(bystander)+` WITH (FORCE)`)
	})

	m := newManager(t, twins.Config{PoolSize: 1})
	waitWarm(t, m, 1, 60*time.Second)

	// A Twin this Manager never minted, outside its own namespace entirely.
	forged := &twins.Twin{Name: bystander, DSN: dsnFor(bystander), CreatedAt: time.Now()}
	err := m.Release(ctx, forged)
	t.Logf("Release(forged twin %q) returned: %v", bystander, err)

	stillThere := dbExists(t, c, bystander)
	t.Logf("bystander database still exists after Release: %v", stillThere)
	if !stillThere {
		t.Errorf("FINDING: Release destroyed database %q, which this Manager never created "+
			"and which is outside its NamePrefix namespace. Release performs no ownership check.",
			bystander)
	}

	closeManager(t, m)
	gate()
}

// ---------------------------------------------------------------------------
// ATTACK T4: make the DROP inside Release fail, then Close.
// Release deletes from bookkeeping BEFORE the drop succeeds. If the drop fails,
// does anything still remember the database?
// ---------------------------------------------------------------------------

func TestAttackTwinsFailedReleaseLeaksDatabase(t *testing.T) {
	requireDB(t)
	gate := goroutineGate(t)
	c := adminConn(t)

	m := newManager(t, twins.Config{PoolSize: 1})
	waitWarm(t, m, 1, 60*time.Second)

	tw, err := m.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if !dbExists(t, c, tw.Name) {
		t.Fatalf("acquired twin %q does not exist", tw.Name)
	}

	// A caller whose request was cancelled — an entirely routine event.
	dead, cancel := context.WithCancel(context.Background())
	cancel()
	relErr := m.Release(dead, tw)
	t.Logf("Release with cancelled ctx returned: %v", relErr)
	if relErr == nil {
		t.Skip("Release unexpectedly succeeded with a cancelled context; attack does not apply")
	}

	st := m.Status()
	t.Logf("after failed Release: checked_out=%d names=%v", st.CheckedOut, st.CheckedOutNames)
	tracked := false
	for _, n := range st.CheckedOutNames {
		if n == tw.Name {
			tracked = true
		}
	}

	closeManager(t, m)

	orphan := dbExists(t, c, tw.Name)
	t.Logf("twin %q still exists after Manager.Close: %v (was still tracked: %v)", tw.Name, orphan, tracked)
	if orphan {
		// Clean up so later tests are not confused by the debris.
		if _, err := c.Exec(context.Background(),
			`DROP DATABASE IF EXISTS `+quoteIdent(tw.Name)+` WITH (FORCE)`); err != nil {
			t.Logf("manual cleanup failed: %v", err)
		}
		t.Errorf("FINDING: Release removed %q from bookkeeping before its DROP succeeded. "+
			"The DROP failed, so the database survived Close() untracked and leaked.", tw.Name)
	}
	gate()
}

// ---------------------------------------------------------------------------
// ATTACK T5: destroy the template while the refill worker is running.
// Expected: no spin, no crash, and Status().LastError explains the outage.
// ---------------------------------------------------------------------------

func TestAttackTwinsTemplateDestroyedMidRefill(t *testing.T) {
	requireDB(t)
	gate := goroutineGate(t)
	ctx := context.Background()
	c := adminConn(t)

	const doomedTpl = "brace_qa_tpl_doomed"
	const doomedPrefix = "brace_qa_run_doomed_"
	if _, err := c.Exec(ctx, `DROP DATABASE IF EXISTS `+quoteIdent(doomedTpl)+` WITH (FORCE)`); err != nil {
		t.Fatalf("pre-drop template: %v", err)
	}
	if _, err := c.Exec(ctx, `CREATE DATABASE `+quoteIdent(doomedTpl)+` TEMPLATE `+quoteIdent(qaTemplate)); err != nil {
		t.Fatalf("create doomed template: %v", err)
	}

	m := newManager(t, twins.Config{
		TemplateName: doomedTpl,
		NamePrefix:   doomedPrefix,
		PoolSize:     2,
	})
	waitWarm(t, m, 2, 60*time.Second)

	// Yank the template out from under the running worker, then drain whatever
	// is still warm so the worker is forced to attempt a refill it cannot do.
	if _, err := c.Exec(ctx, `DROP DATABASE IF EXISTS `+quoteIdent(doomedTpl)+` WITH (FORCE)`); err != nil {
		t.Fatalf("drop template: %v", err)
	}
	for i := 0; i < 32; i++ {
		tw, err := m.Acquire(ctx)
		if err != nil {
			break
		}
		if err := m.Release(ctx, tw); err != nil {
			t.Fatalf("Release: %v", err)
		}
	}

	// Give the worker time to fail several refill cycles.
	var st twins.PoolStatus
	deadline := time.Now().Add(20 * time.Second)
	for {
		st = m.Status()
		if st.LastError != "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Status().LastError never populated after template destruction; status=%+v", st)
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Logf("degraded status: warm=%d warming=%d last_error=%q at=%s",
		st.Warm, st.Warming, st.LastError, st.LastErrorAt.Format(time.RFC3339Nano))

	if !strings.Contains(st.LastError, doomedTpl) {
		t.Errorf("LastError %q does not name the broken template %q", st.LastError, doomedTpl)
	}
	if st.LastErrorAt.IsZero() {
		t.Errorf("LastErrorAt is zero despite LastError being set")
	}

	// Acquire must still be well-behaved (routine ErrNoWarmTwin, not a hang).
	acqCtx, acqCancel := context.WithTimeout(ctx, 3*time.Second)
	_, err := m.Acquire(acqCtx)
	acqCancel()
	if !errors.Is(err, twins.ErrNoWarmTwin) {
		t.Errorf("Acquire on a broken pool returned %v, want ErrNoWarmTwin", err)
	}

	// Confirm it backs off rather than hot-looping: count refill attempts over
	// a window by watching LastErrorAt advance.
	first := m.Status().LastErrorAt
	time.Sleep(5 * time.Second)
	second := m.Status().LastErrorAt
	t.Logf("LastErrorAt advanced by %s over a 5s observation window", second.Sub(first))
	if second.Sub(first) > 0 && second.Sub(first) < 500*time.Millisecond {
		t.Errorf("refill appears to be hot-looping: LastErrorAt moved only %s", second.Sub(first))
	}

	closeManager(t, m)
	leftover, err := listPrefixed(ctx, c, doomedPrefix)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(leftover) != 0 {
		t.Errorf("template destruction leaked databases: %v", leftover)
		for _, n := range leftover {
			_, _ = c.Exec(ctx, `DROP DATABASE IF EXISTS `+quoteIdent(n)+` WITH (FORCE)`)
		}
	}
	gate()
}

// ---------------------------------------------------------------------------
// ATTACK T6: split brain. Two Managers, same NamePrefix, same server.
// Manager B's startup orphan sweep drops "everything in the namespace" — but
// Manager A is alive and owns some of it.
// ---------------------------------------------------------------------------

func TestAttackTwinsSplitBrainSamePrefix(t *testing.T) {
	requireDB(t)
	gate := goroutineGate(t)
	ctx := context.Background()
	c := adminConn(t)

	const splitPrefix = "brace_qa_run_split_"

	a := newManager(t, twins.Config{PoolSize: 2, NamePrefix: splitPrefix})
	waitWarm(t, a, 2, 60*time.Second)

	held, err := a.Acquire(ctx)
	if err != nil {
		t.Fatalf("A.Acquire: %v", err)
	}
	aStatus := a.Status()
	t.Logf("Manager A before B starts: warm=%v checked_out=%v", aStatus.WarmNames, aStatus.CheckedOutNames)
	if !dbExists(t, c, held.Name) {
		t.Fatalf("A's checked-out twin %q missing before B starts", held.Name)
	}

	// A second process boots with the same configuration. SECURE behavior
	// (post-fix): it must be REFUSED via the namespace advisory lock.
	b, berr := twins.NewManager(ctx, twins.Config{
		AdminURL:     adminURL,
		TemplateName: qaTemplate,
		PoolSize:     1,
		NamePrefix:   splitPrefix,
		Logger:       testLogger(t),
	})
	if berr == nil {
		t.Errorf("FINDING (split brain): a second Manager booted on a namespace another Manager holds")
		defer closeManager(t, b)
	} else if !errors.Is(berr, twins.ErrNamespaceHeld) {
		t.Fatalf("second NewManager = %v, want ErrNamespaceHeld", berr)
	}

	heldSurvived := dbExists(t, c, held.Name)
	var warmSurvived, warmChecked int
	for _, n := range aStatus.WarmNames {
		warmChecked++
		if dbExists(t, c, n) {
			warmSurvived++
		}
	}
	t.Logf("after B starts: A's checked-out twin alive=%v; A's warm twins alive=%d/%d",
		heldSurvived, warmSurvived, warmChecked)

	if !heldSurvived {
		t.Errorf("FINDING (split brain): Manager B's startup orphan sweep DROPPED %q, "+
			"a database Manager A had already handed to a caller. A live migration would "+
			"have been destroyed underneath its user.", held.Name)
	}
	if warmChecked > 0 && warmSurvived < warmChecked {
		t.Errorf("FINDING (split brain): Manager B's startup sweep dropped %d of Manager A's %d warm twins",
			warmChecked-warmSurvived, warmChecked)
	}

	// What does A think? It still reports the twin as checked out.
	after := a.Status()
	t.Logf("Manager A status after B's sweep: warm=%d checked_out=%d names=%v (A is unaware)",
		after.Warm, after.CheckedOut, after.CheckedOutNames)

	_ = a.Release(ctx, held)
	closeManager(t, a)

	leftover, err := listPrefixed(ctx, c, splitPrefix)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	for _, n := range leftover {
		_, _ = c.Exec(ctx, `DROP DATABASE IF EXISTS `+quoteIdent(n)+` WITH (FORCE)`)
	}
	if len(leftover) != 0 {
		t.Logf("split-brain left %d databases behind: %v", len(leftover), leftover)
	}
	gate()
}

// ---------------------------------------------------------------------------
// ATTACK T7: Close() while a clone is in flight (fat template).
// ---------------------------------------------------------------------------

func TestAttackTwinsCloseDuringInFlightClone(t *testing.T) {
	requireDB(t)
	gate := goroutineGate(t)
	ctx := context.Background()
	c := adminConn(t)

	const fatPrefix = "brace_qa_run_fat_"
	m := newManager(t, twins.Config{
		TemplateName: qaFat,
		NamePrefix:   fatPrefix,
		PoolSize:     4,
	})

	// Wait until the worker is demonstrably mid-clone, then interrupt it.
	deadline := time.Now().Add(30 * time.Second)
	for m.Status().Warming == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if m.Status().Warming == 0 {
		t.Fatal("worker never reported Warming; cannot interrupt a clone")
	}
	// Land squarely inside the CREATE DATABASE.
	time.Sleep(fatClone / 2)
	st := m.Status()
	t.Logf("interrupting with status warm=%d warming=%d (fat clone takes %s)", st.Warm, st.Warming, fatClone)

	start := time.Now()
	closeManager(t, m)
	t.Logf("Close during in-flight clone returned in %s", time.Since(start))

	// Post-close contract.
	if _, err := m.Acquire(ctx); !errors.Is(err, twins.ErrClosed) {
		t.Errorf("Acquire after Close returned %v, want ErrClosed", err)
	}
	if err := m.Release(ctx, &twins.Twin{Name: "nope"}); !errors.Is(err, twins.ErrClosed) {
		t.Errorf("Release after Close returned %v, want ErrClosed", err)
	}
	if err := m.Close(ctx); err != nil {
		t.Errorf("second Close returned %v, want nil (idempotent)", err)
	}
	if !m.Status().Closed {
		t.Error("Status().Closed is false after Close")
	}

	// The interrupted CREATE DATABASE must not have left debris. Give the
	// server a moment: a cancelled CREATE DATABASE unwinds asynchronously.
	var leftover []string
	waitTill := time.Now().Add(20 * time.Second)
	for {
		var err error
		leftover, err = listPrefixed(ctx, c, fatPrefix)
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		if len(leftover) == 0 || time.Now().After(waitTill) {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if len(leftover) != 0 {
		for _, n := range leftover {
			_, _ = c.Exec(ctx, `DROP DATABASE IF EXISTS `+quoteIdent(n)+` WITH (FORCE)`)
		}
		t.Errorf("FINDING: Close during an in-flight clone leaked databases: %v", leftover)
	}
	gate()
}

// ---------------------------------------------------------------------------
// ATTACK T8: hostile configuration.
// ---------------------------------------------------------------------------

func TestAttackTwinsHostileConfig(t *testing.T) {
	requireDB(t)
	ctx := context.Background()

	rejects := []struct {
		name string
		cfg  twins.Config
	}{
		{"empty template name", twins.Config{AdminURL: adminURL, TemplateName: "", NamePrefix: qaPrefix}},
		{"whitespace template name", twins.Config{AdminURL: adminURL, TemplateName: "   ", NamePrefix: qaPrefix}},
		{"empty admin url", twins.Config{AdminURL: "", TemplateName: qaTemplate, NamePrefix: qaPrefix}},
		{"prefix with dash", twins.Config{AdminURL: adminURL, TemplateName: qaTemplate, NamePrefix: "brace-run-"}},
		{"prefix with quote", twins.Config{AdminURL: adminURL, TemplateName: qaTemplate, NamePrefix: `a"; DROP DATABASE postgres; --`}},
		{"prefix starting with digit", twins.Config{AdminURL: adminURL, TemplateName: qaTemplate, NamePrefix: "1run_"}},
		{"template inside namespace", twins.Config{AdminURL: adminURL, TemplateName: qaPrefix + "tpl", NamePrefix: qaPrefix}},
		{"admin url points at template", twins.Config{AdminURL: dsnFor(qaTemplate), TemplateName: qaTemplate, NamePrefix: qaPrefix}},
		{"nonexistent template", twins.Config{AdminURL: adminURL, TemplateName: "brace_qa_no_such_template", NamePrefix: qaPrefix}},
		{"63-char-overflowing prefix", twins.Config{AdminURL: adminURL, TemplateName: qaTemplate, NamePrefix: strings.Repeat("z", 60) + "_"}},
	}
	for _, tc := range rejects {
		t.Run("reject/"+tc.name, func(t *testing.T) {
			c := tc.cfg
			c.Logger = testLogger(t)
			cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
			defer cancel()
			m, err := twins.NewManager(cctx, c)
			if err == nil {
				t.Errorf("NewManager ACCEPTED hostile config %+v", tc.cfg)
				_ = m.Close(context.Background())
				return
			}
			t.Logf("rejected: %v", err)
		})
	}

	// PoolSize 0 and negative must fall back to the documented default of 2.
	for _, ps := range []int{0, -1, -100} {
		t.Run(fmt.Sprintf("poolsize/%d", ps), func(t *testing.T) {
			m := newManager(t, twins.Config{PoolSize: ps, NamePrefix: "brace_qa_run_ps_"})
			defer closeManager(t, m)
			if got := m.Status().Target; got != 2 {
				t.Errorf("PoolSize %d produced Target=%d, want the documented default 2", ps, got)
			}
		})
	}

	// A reserved-looking prefix. Postgres has no databases named pg_*, so this
	// is safe to actually run — the question is whether the Manager guards it.
	t.Run("prefix/pg_", func(t *testing.T) {
		cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		m, err := twins.NewManager(cctx, twins.Config{
			AdminURL:     adminURL,
			TemplateName: qaTemplate,
			NamePrefix:   "pg_",
			PoolSize:     1,
			Logger:       testLogger(t),
		})
		if err != nil {
			t.Logf("NewManager rejected the reserved prefix pg_: %v", err)
			return
		}
		defer closeManager(t, m)
		waitWarm(t, m, 1, 60*time.Second)
		st := m.Status()
		t.Errorf("FINDING: NewManager ACCEPTED NamePrefix %q and created databases in the "+
			"reserved pg_ namespace: %v. The orphan sweep will DROP anything in pg_database "+
			"matching the prefix, so any prefix that shadows a real database name is a "+
			"data-destruction footgun (validation only checks the identifier charset).",
			"pg_", st.WarmNames)
	})

	// Large pool: does the worker cope, and does Close clean up everything it
	// managed to create?
	t.Run("poolsize/100", func(t *testing.T) {
		const bigPrefix = "brace_qa_run_big_"
		m := newManager(t, twins.Config{PoolSize: 100, NamePrefix: bigPrefix})
		time.Sleep(3 * time.Second)
		st := m.Status()
		t.Logf("PoolSize=100 after 3s: target=%d warm=%d warming=%d last_error=%q",
			st.Target, st.Warm, st.Warming, st.LastError)
		if st.Warming > 1 {
			t.Errorf("FINDING: %d clones in flight at once; the package documents clones as "+
				"strictly serialized on a single worker", st.Warming)
		}
		closeManager(t, m)
		c := adminConn(t)
		leftover, err := listPrefixed(ctx, c, bigPrefix)
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		for _, n := range leftover {
			_, _ = c.Exec(ctx, `DROP DATABASE IF EXISTS `+quoteIdent(n)+` WITH (FORCE)`)
		}
		if len(leftover) != 0 {
			t.Errorf("PoolSize=100 Close leaked %d databases: %v", len(leftover), leftover)
		}
	})
}

// ---------------------------------------------------------------------------
// ATTACK T10: break the template, let the pool recover, and see whether the
// status endpoint ever stops shouting about an outage that is over.
// ---------------------------------------------------------------------------

func TestAttackTwinsLastErrorIsSticky(t *testing.T) {
	requireDB(t)
	gate := goroutineGate(t)
	ctx := context.Background()
	c := adminConn(t)

	const flapTpl = "brace_qa_tpl_flap"
	const flapPrefix = "brace_qa_run_flap_"
	mk := func() {
		if _, err := c.Exec(ctx, `CREATE DATABASE `+quoteIdent(flapTpl)+` TEMPLATE `+quoteIdent(qaTemplate)); err != nil {
			t.Fatalf("create flap template: %v", err)
		}
	}
	if _, err := c.Exec(ctx, `DROP DATABASE IF EXISTS `+quoteIdent(flapTpl)+` WITH (FORCE)`); err != nil {
		t.Fatalf("pre-drop: %v", err)
	}
	mk()

	m := newManager(t, twins.Config{TemplateName: flapTpl, NamePrefix: flapPrefix, PoolSize: 1})
	waitWarm(t, m, 1, 60*time.Second)

	// Outage.
	if _, err := c.Exec(ctx, `DROP DATABASE IF EXISTS `+quoteIdent(flapTpl)+` WITH (FORCE)`); err != nil {
		t.Fatalf("drop template: %v", err)
	}
	for i := 0; i < 32; i++ {
		tw, err := m.Acquire(ctx)
		if err != nil {
			break
		}
		_ = m.Release(ctx, tw)
	}
	deadline := time.Now().Add(30 * time.Second)
	for m.Status().LastError == "" {
		if time.Now().After(deadline) {
			t.Fatalf("LastError never set; status=%+v", m.Status())
		}
		time.Sleep(200 * time.Millisecond)
	}
	outage := m.Status()
	t.Logf("during outage: warm=%d last_error=%q", outage.Warm, outage.LastError)

	// Recovery.
	mk()
	deadline = time.Now().Add(60 * time.Second)
	for m.Status().Warm == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("pool never recovered after the template was restored; status=%+v", m.Status())
		}
		time.Sleep(200 * time.Millisecond)
	}
	// Let a few more successful clone cycles go by.
	time.Sleep(3 * time.Second)
	recovered := m.Status()
	t.Logf("after recovery: warm=%d warming=%d last_error=%q last_error_at=%s",
		recovered.Warm, recovered.Warming, recovered.LastError, recovered.LastErrorAt.Format(time.RFC3339))

	if recovered.LastError != "" {
		t.Errorf("FINDING: the pool has fully recovered (warm=%d) but Status().LastError still "+
			"reports the resolved outage %q from %s. Nothing ever clears lastErr on a successful "+
			"clone, so a status endpoint or health check reading LastError will show a permanently "+
			"broken system after any single transient failure.",
			recovered.Warm, recovered.LastError, recovered.LastErrorAt.Format(time.RFC3339))
	}

	closeManager(t, m)
	_, _ = c.Exec(ctx, `DROP DATABASE IF EXISTS `+quoteIdent(flapTpl)+` WITH (FORCE)`)
	gate()
}

// ---------------------------------------------------------------------------
// ATTACK T11: the package documents that clones must be serialized because two
// concurrent CREATE DATABASE from the same template conflict. That serialization
// is per-process. Two Managers sharing a template break the assumption.
// ---------------------------------------------------------------------------

func TestAttackTwinsTwoManagersShareOneTemplate(t *testing.T) {
	requireDB(t)
	gate := goroutineGate(t)
	ctx := context.Background()
	c := adminConn(t)

	const pa, pb = "brace_qa_run_ta_", "brace_qa_run_tb_"
	a := newManager(t, twins.Config{TemplateName: qaFat, NamePrefix: pa, PoolSize: 3})
	b := newManager(t, twins.Config{TemplateName: qaFat, NamePrefix: pb, PoolSize: 3})

	time.Sleep(8 * time.Second)
	sa, sb := a.Status(), b.Status()
	t.Logf("A: warm=%d warming=%d last_error=%q", sa.Warm, sa.Warming, sa.LastError)
	t.Logf("B: warm=%d warming=%d last_error=%q", sb.Warm, sb.Warming, sb.LastError)

	if sa.LastError != "" || sb.LastError != "" {
		t.Errorf("FINDING: two Managers cloning from the same template collide. "+
			"clone serialization is enforced only within one process (a single worker "+
			"goroutine), so a second Brace instance sharing the template produces clone "+
			"failures. A=%q B=%q", sa.LastError, sb.LastError)
	}
	if sa.Warm == 0 || sb.Warm == 0 {
		t.Errorf("FINDING: sharing a template starved a pool (A warm=%d, B warm=%d)", sa.Warm, sb.Warm)
	}

	closeManager(t, a)
	closeManager(t, b)
	for _, p := range []string{pa, pb} {
		left, err := listPrefixed(ctx, c, p)
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		for _, n := range left {
			_, _ = c.Exec(ctx, `DROP DATABASE IF EXISTS `+quoteIdent(n)+` WITH (FORCE)`)
		}
		if len(left) != 0 {
			t.Errorf("Close left %v behind under %q", left, p)
		}
	}
	gate()
}

// ---------------------------------------------------------------------------
// ATTACK T9: nil twin, and Status snapshot aliasing under concurrent mutation.
// ---------------------------------------------------------------------------

func TestAttackTwinsStatusUnderStorm(t *testing.T) {
	requireDB(t)
	gate := goroutineGate(t)
	ctx := context.Background()

	m := newManager(t, twins.Config{PoolSize: 2, NamePrefix: "brace_qa_run_stat_"})
	waitWarm(t, m, 1, 60*time.Second)

	if err := m.Release(ctx, nil); err == nil {
		t.Error("Release(nil) returned nil error")
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				s := m.Status()
				// Mutate the snapshot: it must be a private copy.
				s.WarmNames = append(s.WarmNames, "poison")
				s.CheckedOutNames = append(s.CheckedOutNames, "poison")
				if s.Warm < 0 || s.CheckedOut < 0 || s.Warming < 0 {
					t.Errorf("negative counter in status: %+v", s)
					return
				}
			}
		}()
	}
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				tw, err := m.Acquire(ctx)
				if err != nil {
					time.Sleep(2 * time.Millisecond)
					continue
				}
				_ = m.Release(ctx, tw)
			}
		}()
	}
	time.Sleep(3 * time.Second)
	close(stop)
	wg.Wait()

	final := m.Status()
	for _, n := range append(append([]string{}, final.WarmNames...), final.CheckedOutNames...) {
		if n == "poison" {
			t.Error("FINDING: Status() returns slices aliased to Manager internals; a caller mutated them")
		}
	}
	closeManager(t, m)
	gate()
}
