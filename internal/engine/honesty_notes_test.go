package engine

import (
	"strings"
	"testing"

	"github.com/vinayaksonthalia/brace/internal/monitor"
)

// The seed-cap note must fire, loudly, only when the caller's stated size exceeds
// what was seeded — and must carry both row counts and push the reader to
// effective_verdict.
func TestAppendSeedCapNote(t *testing.T) {
	capped := appendSeedCapNote("", &RunResult{TwinSeededRows: 5_000_000, TwinRequestedRows: 500_000_000})
	for _, want := range []string{"5,000,000", "500,000,000", "PROJECTION", "effective_verdict"} {
		if !strings.Contains(capped, want) {
			t.Errorf("seed-cap note missing %q: %q", want, capped)
		}
	}
	if got := appendSeedCapNote("base.", &RunResult{}); got != "base." {
		t.Errorf("no-cap run should be untouched, got %q", got)
	}
	if got := appendSeedCapNote("base.", &RunResult{TwinSeededRows: 1000, TwinRequestedRows: 1000}); got != "base." {
		t.Errorf("requested==seeded should be untouched, got %q", got)
	}
}

func TestNonMeasuredExplanation(t *testing.T) {
	for _, v := range []string{VerdictErrored, VerdictInconclusive, VerdictSkipped} {
		got := nonMeasuredExplanation(v)
		if got == "" || !strings.Contains(got, "shape_note") {
			t.Errorf("nonMeasuredExplanation(%s) = %q, want non-empty pointing at shape_note", v, got)
		}
	}
	if got := nonMeasuredExplanation(VerdictSafe); got != "" {
		t.Errorf("nonMeasuredExplanation(SAFE) = %q, want empty", got)
	}
}

// An ERRORED run must not carry the shape lesson in explanation, where an agent
// could quote it as a finding; it belongs in shape_note, labelled not-a-measurement.
func TestExplanationForMovesShapeIntoShapeNote(t *testing.T) {
	const shape = "SENTINEL shape lesson: this shape takes ACCESS EXCLUSIVE."
	as := &Assessment{Lesson: Lesson{Explanation: shape}}

	res := &RunResult{Verdict: VerdictErrored}
	got := explanationFor(res, as)
	if strings.Contains(got, "SENTINEL") {
		t.Errorf("explanation for an ERRORED run must not carry the shape lesson: %q", got)
	}
	if !strings.Contains(res.ShapeNote, "SENTINEL") {
		t.Errorf("shape_note must carry the shape lesson: %q", res.ShapeNote)
	}
	if !strings.Contains(res.ShapeNote, "NOT a measurement") {
		t.Errorf("shape_note must be labelled not-a-measurement: %q", res.ShapeNote)
	}

	// A measured run is untouched: explanation is the lesson, shape_note stays empty.
	res2 := &RunResult{Verdict: VerdictSafe}
	if got := explanationFor(res2, as); got != shape {
		t.Errorf("measured run explanation = %q, want the lesson", got)
	}
	if res2.ShapeNote != "" {
		t.Errorf("measured run must not set shape_note, got %q", res2.ShapeNote)
	}
}

func TestLockModeNoteLine(t *testing.T) {
	// Evidence of a lock plus a captured requested mode: name it, and correct the
	// "empty means no lock" misreading.
	ln := lockModeNoteLine(2.0, 0, VerdictUnsafe, monitor.Report{MigrationLockModeRequested: "AccessExclusiveLock"})
	if !strings.Contains(ln, "AccessExclusiveLock") || !strings.Contains(ln, "does NOT mean no lock") {
		t.Errorf("lock-mode note should name the mode and correct the misreading: %q", ln)
	}
	// No evidence at all: no note (a genuinely lock-free ANALYZE needs no caveat).
	if ln := lockModeNoteLine(0, 0, VerdictSafe, monitor.Report{}); ln != "" {
		t.Errorf("no-evidence lock-mode note should be empty, got %q", ln)
	}
	// Evidence but no captured mode: still correct the misreading generically.
	if ln := lockModeNoteLine(0, 0, VerdictInconclusive, monitor.Report{}); !strings.Contains(ln, "does NOT mean no lock") {
		t.Errorf("inconclusive run with no captured mode should still get the generic caveat: %q", ln)
	}
}
