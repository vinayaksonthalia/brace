//go:build integration

// Integration tests for the credibility fix — CREATE INDEX CONCURRENTLY (the
// SAFE form) must never be flipped UNSAFE by an I/O dip or a phantom scale
// projection — and for BUG B, an ERRORED run that must carry nothing
// measured-sounding. They run against a real PostgreSQL 16; see integration_test.go
// for the harness (TEST_ADMIN_URL, newLab, templateRows).
package engine

import (
	"context"
	"strings"
	"testing"
)

// TestConcurrentlyNeverProjectsPhantomFreeze is the whole fix proved against a
// real server: CREATE INDEX CONCURRENTLY holds SHARE UPDATE EXCLUSIVE, which does
// not conflict with writes, so EVEN when told the operator's real table is 1000x
// the twin — the exact input that turns a genuinely-blocking SET NOT NULL into an
// effective-UNSAFE — it must stay SAFE and effective-SAFE with NO projection and
// NO warning. A dip from the build's two heap scans competing for I/O is not a
// freeze, and a non-write-blocking shape does not scale a write-freeze with rows.
func TestConcurrentlyNeverProjectsPhantomFreeze(t *testing.T) {
	_, eng := newLab(t, "concnoproj", 1)
	ctx := context.Background()

	// 1000x the twin — the multiplier that makes an O(rows) write-blocking shape
	// project to a multi-second outage. A non-write-blocking shape must ignore it.
	const targetRows = int64(templateRows) * 1000

	res, err := eng.Rehearse(ctx,
		`CREATE INDEX CONCURRENTLY idx_conc_noproj ON orders (customer_id)`, ModeSafe, WithTargetRows(targetRows))
	if err != nil {
		t.Fatalf("Rehearse: %v", err)
	}
	t.Logf("verdict=%s effective=%s blocked=%.3fs monitor_blocked=%.3fs casualties=%d locks=%v projection_known=%v warning=%q note=%q",
		res.Verdict, res.EffectiveVerdict, res.BlockedSeconds, res.MonitorBlockedSeconds,
		res.Casualties.Total, res.LockModes, res.ScaleProjection.Known(), res.ScaleWarning, res.Note)

	if res.Error != "" {
		t.Fatalf("the CONCURRENTLY build itself failed: %s", res.Error)
	}
	if res.Classification != ClassCreateIndexConcurrently {
		t.Fatalf("classification = %q, want %q", res.Classification, ClassCreateIndexConcurrently)
	}

	// The measured verdict: SAFE. Any I/O dip must NOT have driven it UNSAFE.
	if res.Verdict != VerdictSafe {
		t.Errorf("measured verdict = %s (blocked %.3fs, locks %v) — a non-write-blocking lock never freezes writes",
			res.Verdict, res.BlockedSeconds, res.LockModes)
	}
	// The heart of the fix: no phantom projection at 1000x, so the gate stays SAFE.
	if res.EffectiveVerdict != VerdictSafe {
		t.Errorf("effective_verdict = %s at 1000x, want SAFE — a non-write-blocking shape must not project a freeze",
			res.EffectiveVerdict)
	}
	if res.ScaleProjection.Known() {
		t.Errorf("a scale projection was made for CREATE INDEX CONCURRENTLY: %+v", res.ScaleProjection)
	}
	if res.ScaleWarning != "" {
		t.Errorf("a scale warning was emitted for CREATE INDEX CONCURRENTLY: %s", res.ScaleWarning)
	}
	if res.Casualties.Total != 0 {
		t.Errorf("casualties = %d; CONCURRENTLY kills nobody", res.Casualties.Total)
	}
	// The monitor must corroborate: the strongest lock it saw held on a
	// pre-existing relation is NOT one that conflicts with writes.
	if mode := res.Monitor.StrongestLockMode; conflictsWithWriteHold(mode) && mode != "" {
		t.Errorf("monitor saw write-blocking lock %q for CONCURRENTLY; expected only ShareUpdateExclusiveLock", mode)
	}
}

// TestNaiveCreateIndexStillProjectsToUnsafe is the regression guard the fix must
// not break: a NAIVE CREATE INDEX holds SHARE (write-blocking), so at 1000x it
// must still come back effective-UNSAFE with a real projection — the false-safe
// defence is intact, and only the non-blocking case was carved out.
func TestNaiveCreateIndexStillProjectsToUnsafe(t *testing.T) {
	_, eng := newLab(t, "naiveproj", 1)
	ctx := context.Background()

	const targetRows = int64(templateRows) * 1000

	res, err := eng.Rehearse(ctx,
		`CREATE INDEX idx_naive_proj ON orders (customer_id)`, ModeNaive, WithTargetRows(targetRows))
	if err != nil {
		t.Fatalf("Rehearse: %v", err)
	}
	t.Logf("verdict=%s effective=%s blocked=%.3fs projected=%.3fs locks=%v projection_known=%v",
		res.Verdict, res.EffectiveVerdict, res.BlockedSeconds,
		res.ScaleProjection.ProjectedBlockedSeconds, res.LockModes, res.ScaleProjection.Known())

	if res.Error != "" {
		t.Fatalf("the build itself failed: %s", res.Error)
	}
	if res.Classification != ClassCreateIndex {
		t.Fatalf("classification = %q, want %q", res.Classification, ClassCreateIndex)
	}
	// The projection must fire (a KNOWN write-blocking scaling shape projects even
	// from a floor measurement), and it must veto SAFE at 1000x.
	if !res.ScaleProjection.Known() {
		t.Fatalf("no projection for a naive CREATE INDEX at 1000x — the false-safe defence would be gone: %+v",
			res.ScaleProjection)
	}
	if res.ScaleProjection.Verdict != VerdictUnsafe {
		t.Errorf("projected verdict = %s (%.3fs) at 1000x, want UNSAFE",
			res.ScaleProjection.Verdict, res.ScaleProjection.ProjectedBlockedSeconds)
	}
	if res.EffectiveVerdict != VerdictUnsafe {
		t.Errorf("effective_verdict = %s, want UNSAFE — a naive CREATE INDEX at 1000x is an outage", res.EffectiveVerdict)
	}
}

// TestErroredRunCarriesNothingMeasured pins BUG B: a statement that never
// executed (a bad column reference) comes back ERRORED and must carry NO scale
// projection and an explanation clearly framed as general shape behaviour, not a
// measurement of a run that did not happen.
func TestErroredRunCarriesNothingMeasured(t *testing.T) {
	_, eng := newLab(t, "erroredproj", 1)
	ctx := context.Background()

	// A syntactically valid CREATE INDEX that references a column that does not
	// exist: Postgres refuses it (42703), so it ERRORS before measuring anything.
	res, err := eng.Rehearse(ctx,
		`CREATE INDEX idx_oops ON orders (nonexistent_column)`, ModeCustom, WithTargetRows(int64(templateRows)*1000))
	if err != nil {
		t.Fatalf("Rehearse: %v", err)
	}
	t.Logf("verdict=%s effective=%s error=%q projection_known=%v warning=%q explanation=%q",
		res.Verdict, res.EffectiveVerdict, res.Error, res.ScaleProjection.Known(), res.ScaleWarning, res.Explanation)

	if res.Verdict != VerdictErrored {
		t.Fatalf("verdict = %s, want ERRORED — the statement referenced a missing column", res.Verdict)
	}
	if res.EffectiveVerdict != VerdictErrored {
		t.Errorf("effective_verdict = %s, want ERRORED", res.EffectiveVerdict)
	}
	// The heart of BUG B: nothing measured-sounding on a run that never ran.
	if res.ScaleProjection.Known() {
		t.Errorf("an ERRORED run carried a scale projection: %+v", res.ScaleProjection)
	}
	if res.ScaleProjection != (ScaleProjection{}) {
		t.Errorf("an ERRORED run carried a populated (measured-sounding) projection struct: %+v", res.ScaleProjection)
	}
	if res.ScaleWarning != "" {
		t.Errorf("an ERRORED run carried a scale warning: %s", res.ScaleWarning)
	}
	// New contract: an ERRORED run's explanation states only that nothing was
	// measured and points at shape_note; the general shape behaviour lives in
	// shape_note, labelled not-a-measurement, so it can never be quoted as this
	// run's finding.
	if res.Explanation != "" && !strings.Contains(res.Explanation, "shape_note") {
		t.Errorf("an ERRORED run's explanation is not framed as non-measurement (no shape_note pointer):\n%s", res.Explanation)
	}
	if res.ShapeNote != "" && !strings.Contains(res.ShapeNote, "NOT a measurement") {
		t.Errorf("an ERRORED run's shape_note is not labelled not-a-measurement:\n%s", res.ShapeNote)
	}
	// -5 target_rows must be ignored (clamped to 0 → no projection), independent of
	// the ERRORED path.
	res2, err := eng.Rehearse(ctx,
		`CREATE INDEX CONCURRENTLY idx_negtarget ON orders (amount_cents)`, ModeSafe, WithTargetRows(-5))
	if err != nil {
		t.Fatalf("Rehearse (negative target): %v", err)
	}
	if res2.ScaleProjection.Known() {
		t.Errorf("a negative target_rows produced a projection: %+v", res2.ScaleProjection)
	}
}
