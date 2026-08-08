package engine

import (
	"strings"
	"testing"

	"github.com/vinayaksonthalia/brace/internal/monitor"
)

// The credibility bug, pinned as pure logic so it cannot regress on a flaky rig.
//
// CREATE INDEX CONCURRENTLY holds ShareUpdateExclusiveLock, which does NOT
// conflict with the ROW EXCLUSIVE lock writes take — so it never freezes writes.
// But its two heap scans compete for I/O on a shared box, and that occasionally
// dips the measured write rate, producing a non-zero blocked_seconds from NOISE.
// A tool that lets that noise flip a genuinely safe migration to UNSAFE — either
// directly or by projecting it onto a bigger table — is broken. These tests hold
// the line with a synthetic monitor report, because the live I/O dip is
// nondeterministic and cannot be relied on to reproduce.

func monReport(modes ...string) monitor.Report {
	r := monitor.Report{Samples: 10, LockModes: modes}
	if len(modes) > 0 {
		r.StrongestLockMode = monitor.Strongest(modes)
	}
	return r
}

// TestVerdictForObserved: a write-rate freeze only makes a run UNSAFE when a
// write-blocking lock was actually held. Otherwise the dip is I/O contention, and
// only real dead writes (casualties) can still fail the run.
func TestVerdictForObserved(t *testing.T) {
	cases := []struct {
		name       string
		blocked    float64
		casualties int64
		heldLock   bool
		want       string
	}{
		{"blocking lock + long freeze is UNSAFE", 2.0, 0, true, VerdictUnsafe},
		{"blocking lock + sub-second is SAFE", 0.5, 0, true, VerdictSafe},
		// The bug: a deep dip under a NON-blocking lock is I/O noise, not a freeze.
		{"no blocking lock + long dip is SAFE (the CONCURRENTLY bug)", 2.0, 0, false, VerdictSafe},
		{"no blocking lock + enormous dip is still SAFE", 30.0, 0, false, VerdictSafe},
		// Casualties are real dead writes and survive the suppression, whichever way
		// the lock went — a write that died is a customer that saw an error.
		{"casualties are honoured even with no blocking lock", 0, 3, false, VerdictUnsafe},
		{"casualties are honoured with a blocking lock", 0, 3, true, VerdictUnsafe},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := verdictForObserved(tc.blocked, tc.casualties, tc.heldLock); got != tc.want {
				t.Errorf("verdictForObserved(%v, %d, %v) = %s, want %s",
					tc.blocked, tc.casualties, tc.heldLock, got, tc.want)
			}
		})
	}
}

// TestMigrationHeldWriteBlockingLock: the monitor's observed lock is the arbiter,
// with the static shape prediction as a fallback only when the monitor saw
// nothing.
func TestMigrationHeldWriteBlockingLock(t *testing.T) {
	concurrently, err := Assess(`CREATE INDEX CONCURRENTLY idx ON orders (customer_id)`)
	if err != nil {
		t.Fatalf("Assess concurrently: %v", err)
	}
	naive, err := Assess(`CREATE INDEX idx ON orders (customer_id)`)
	if err != nil {
		t.Fatalf("Assess naive: %v", err)
	}

	cases := []struct {
		name string
		as   *Assessment
		mon  monitor.Report
		want bool
	}{
		// The monitor saw the real lock: it is the arbiter, over the prediction.
		{"CONCURRENTLY seen holding SHARE UPDATE EXCLUSIVE is non-blocking",
			concurrently, monReport("ShareUpdateExclusiveLock"), false},
		{"naive CREATE INDEX seen holding SHARE is write-blocking",
			naive, monReport("ShareLock"), true},
		{"a rewrite seen holding ACCESS EXCLUSIVE is write-blocking",
			naive, monReport("AccessExclusiveLock"), true},
		// Mixed modes: any write-blocking mode in the set is decisive.
		{"a write-blocking mode anywhere in the set counts",
			concurrently, monReport("ShareUpdateExclusiveLock", "ShareLock"), true},
		// An unrecognised (future) mode is treated conservatively as blocking, so a
		// freeze is never suppressed on evidence we cannot read.
		{"an unknown observed mode is treated as blocking",
			concurrently, monReport("SomeFuturePgLock"), true},
		// No monitor data: fall back to the shape prediction.
		{"no monitor data falls back to the non-blocking prediction",
			concurrently, monitor.Report{Samples: 0}, false},
		{"no monitor data falls back to the blocking prediction",
			naive, monitor.Report{Samples: 0}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := migrationHeldWriteBlockingLock(tc.as, tc.mon); got != tc.want {
				t.Errorf("migrationHeldWriteBlockingLock = %v, want %v (modes %v, predictsBlocks %v)",
					got, tc.want, tc.mon.LockModes, tc.as.Lesson.BlocksWrites)
			}
		})
	}
}

// TestConcurrentlyNeverFrozenByIODip is the end-to-end-of-the-logic proof the
// task asks for: given a synthetic run shaped exactly like the flaky case — lock
// ShareUpdateExclusiveLock, a blocked_seconds well over the one-second threshold,
// and a large target_rows — the verdict is SAFE and NO projection is made. The
// naive contrast (ShareLock, write-blocking) must stay UNSAFE and still project.
func TestConcurrentlyNeverFrozenByIODip(t *testing.T) {
	const twinRows, targetRows = int64(500_000), int64(500_000_000) // 1000x

	concurrently, err := Assess(`CREATE INDEX CONCURRENTLY idx ON orders (customer_id)`)
	if err != nil {
		t.Fatalf("Assess concurrently: %v", err)
	}

	// The flaky measurement: 1.2s of "blocked" from an I/O dip, and the monitor
	// confirms only a non-write-blocking lock was ever held.
	mon := monReport("ShareUpdateExclusiveLock")
	held := migrationHeldWriteBlockingLock(concurrently, mon)
	if held {
		t.Fatal("a ShareUpdateExclusiveLock must not be read as a write-blocking lock")
	}

	verdict := verdictForObserved(1.2, 0, held)
	if verdict != VerdictSafe {
		t.Fatalf("verdict = %s, want SAFE — an I/O dip on a non-write-blocking lock is not a freeze", verdict)
	}

	proj := scaleProjectionFor(verdict, scaleInputs{
		Measured: 1.2, TwinRows: twinRows, TargetRows: targetRows,
		ScalesWithRows:        concurrently.Lesson.ScalesWithRows, // true — the build IS O(rows)
		BlocksWrites:          concurrently.Lesson.BlocksWrites,   // false
		HeldWriteBlockingLock: held,                               // false
		Verdict:               verdict,
	})
	if proj.Known() {
		t.Fatalf("a projection was made for a non-write-blocking shape at 1000x: %+v", proj)
	}
	if got := effectiveVerdict(verdict, proj); got != VerdictSafe {
		t.Fatalf("effective_verdict = %s, want SAFE — a non-blocking shape must never be flipped by scale", got)
	}
	if w := scaleWarning(verdict, proj); w != "" {
		t.Fatalf("a scale warning was emitted for a non-blocking shape: %q", w)
	}

	// The contrast: a naive CREATE INDEX holds SHARE (write-blocking). The same
	// numbers must stay UNSAFE and MUST still project to UNSAFE at 1000x.
	naive, err := Assess(`CREATE INDEX idx ON orders (customer_id)`)
	if err != nil {
		t.Fatalf("Assess naive: %v", err)
	}
	naiveMon := monReport("ShareLock")
	naiveHeld := migrationHeldWriteBlockingLock(naive, naiveMon)
	if !naiveHeld {
		t.Fatal("a ShareLock must be read as a write-blocking lock")
	}
	naiveVerdict := verdictForObserved(1.2, 0, naiveHeld)
	if naiveVerdict != VerdictUnsafe {
		t.Fatalf("naive verdict = %s, want UNSAFE — a real SHARE-lock freeze is an outage", naiveVerdict)
	}
	naiveProj := scaleProjectionFor(naiveVerdict, scaleInputs{
		Measured: 1.2, TwinRows: twinRows, TargetRows: targetRows,
		ScalesWithRows: naive.Lesson.ScalesWithRows, BlocksWrites: naive.Lesson.BlocksWrites,
		HeldWriteBlockingLock: naiveHeld, Verdict: naiveVerdict,
	})
	if naiveProj.Verdict != VerdictUnsafe {
		t.Fatalf("naive projection = %s, want UNSAFE at 1000x — the false-safe defence must still fire", naiveProj.Verdict)
	}
}

// TestNaiveCreateIndexProjectsFromFloorWithoutMonitor guards the other direction
// of the asymmetry: a KNOWN write-blocking scaling shape (CREATE INDEX, SET NOT
// NULL) projects from a floor measurement even when the monitor never confirmed
// the lock — its build can finish sub-sample on a tiny twin, and the false-safe
// defence must not depend on catching the lock live.
func TestNaiveCreateIndexProjectsFromFloorWithoutMonitor(t *testing.T) {
	const twinRows, targetRows = int64(500_000), int64(500_000_000)
	naive, err := Assess(`CREATE INDEX idx ON orders (customer_id)`)
	if err != nil {
		t.Fatalf("Assess: %v", err)
	}
	// Floor measurement, and HeldWriteBlockingLock=false (monitor missed the brief
	// lock). It must STILL project, because BlocksWrites && !ScaleUncertain.
	proj := scaleProjectionFor(VerdictSafe, scaleInputs{
		Measured: 0, TwinRows: twinRows, TargetRows: targetRows,
		ScalesWithRows: naive.Lesson.ScalesWithRows, BlocksWrites: naive.Lesson.BlocksWrites,
		ScaleUncertain: naive.Lesson.ScaleUncertain, HeldWriteBlockingLock: false, Verdict: VerdictSafe,
	})
	if proj.Verdict != VerdictUnsafe {
		t.Fatalf("known-blocking scaling shape did not project from the floor: %+v", proj)
	}
}

// TestScaleProjectionForSuppressesErroredAndInconclusive pins BUG B: a run that
// measured nothing must carry no projection at all — not even a struct populated
// with twin/target sizes and a model name, which would read as measured.
func TestScaleProjectionForSuppressesErroredAndInconclusive(t *testing.T) {
	in := scaleInputs{
		Measured: 0, TwinRows: 500_000, TargetRows: 500_000_000,
		ScalesWithRows: true, BlocksWrites: true, HeldWriteBlockingLock: true,
	}
	for _, verdict := range []string{VerdictErrored, VerdictInconclusive} {
		t.Run(verdict, func(t *testing.T) {
			got := scaleProjectionFor(verdict, in)
			if got.Known() {
				t.Errorf("%s produced a projection: %+v", verdict, got)
			}
			if got != (ScaleProjection{}) {
				t.Errorf("%s produced a non-empty projection struct (measured-sounding): %+v", verdict, got)
			}
		})
	}
	// A completed run still projects normally.
	if got := scaleProjectionFor(VerdictSafe, in); !got.Known() {
		t.Errorf("a SAFE run must still project: %+v", got)
	}
}

// TestErroredExplanationIsMarkedGeneral pins the other half of BUG B: an ERRORED
// or INCONCLUSIVE run's explanation must be framed as the shape's general
// behaviour, never as a measurement of a statement that never ran.
func TestErroredExplanationIsMarkedGeneral(t *testing.T) {
	as, err := Assess(`CREATE INDEX idx ON orders (customer_id)`)
	if err != nil {
		t.Fatalf("Assess: %v", err)
	}
	const marker = "did not run to a measured result"
	for _, verdict := range []string{VerdictErrored, VerdictInconclusive} {
		res := &RunResult{Verdict: verdict}
		got := explanationFor(res, as)
		if !strings.Contains(got, marker) {
			t.Errorf("%s explanation not marked as general behaviour:\n%s", verdict, got)
		}
	}
	// A measured run keeps the plain lesson.
	measured := &RunResult{Verdict: VerdictSafe}
	if got := explanationFor(measured, as); strings.Contains(got, marker) {
		t.Errorf("a measured run's explanation was wrongly marked as unrun:\n%s", got)
	}
}
