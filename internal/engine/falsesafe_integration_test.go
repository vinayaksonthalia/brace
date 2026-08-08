//go:build integration

// Integration tests for the two false-verdict fixes that can only be proved
// against a real server: the flagship volatile-default false-SAFE (a
// user-defined-function default must be REHEARSED and measured, never waved
// through as already_safe), and the constant-time ALTER TABLE false-UNSAFE (a
// catalog-only DROP COLUMN must not have a phantom multi-second freeze projected
// onto it from the resolution floor). See integration_test.go for the harness
// (TEST_ADMIN_URL, the seeded template with brace_udf_default(), and newLab).
package engine

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// TestAlreadySafeNonexistentTableIsRejected pins the flagship false-SAFE: an
// already_safe-SHAPED statement (ALTER TABLE ... ADD COLUMN with a constant
// default) whose target relation does NOT exist must never come back SAFE. The
// static fast-path classified it catalog-only and short-circuited to already_safe
// without ever checking the table exists, so a typo'd or absent table was blessed
// SAFE — the worst verdict this tool can give, because the statement cannot even
// run. The existence gate must turn it into a clear REJECTED that names the
// missing object and lists what the lab has, while the SAME shape against the
// real orders table still returns already_safe SAFE with the honest
// acquisition_not_simulated qualifier.
func TestAlreadySafeNonexistentTableIsRejected(t *testing.T) {
	mgr, eng := newLab(t, "ghost", 1)
	ctx := context.Background()

	// The check must cost no twin: the whole point of the fast-path is that it
	// never clones. Capture the pool and database count to prove it.
	before := mgr.Status()
	beforeDBs := countDatabases(t, itNamespace)

	// 1. The bug. A nonexistent target must NOT be SAFE.
	const ghost = "ghost_table_xyz"
	_, err := eng.Rehearse(ctx, `ALTER TABLE `+ghost+` ADD COLUMN x int`, ModeCustom)
	var rej *RejectedError
	if !errors.As(err, &rej) {
		t.Fatalf("a nonexistent-table ADD COLUMN returned %T (%v), want *RejectedError", err, err)
	}
	if rej.Kind == RejectAlreadySafe {
		t.Fatalf("FALSE-SAFE: ALTER TABLE %s ADD COLUMN was answered already_safe — a statement that "+
			"cannot even run was blessed SAFE (kind=%s reason=%q)", ghost, rej.Kind, rej.Reason)
	}
	if rej.Kind != RejectRejected {
		t.Errorf("kind = %q, want %q (a clearly not-safe rejection)", rej.Kind, RejectRejected)
	}
	// The reason must name the missing object and, ideally, list what the lab has.
	if !strings.Contains(rej.Reason, ghost) {
		t.Errorf("reason does not name the missing relation %q: %s", ghost, rej.Reason)
	}
	if !strings.Contains(rej.Reason, "orders") {
		t.Errorf("reason does not list the lab's real table (orders): %s", rej.Reason)
	}
	t.Logf("nonexistent-table rejection: code=%s reason=%q suggestion=%q", rej.Code, rej.Reason, rej.Suggestion)

	// A bare ANALYZE of a nonexistent table is the same defect in another shape.
	_, err = eng.Rehearse(ctx, `ANALYZE `+ghost, ModeCustom)
	if !errors.As(err, &rej) || rej.Kind == RejectAlreadySafe {
		t.Errorf("ANALYZE of a nonexistent table was not rejected as not-safe: %v", err)
	}

	// 2. The control. The SAME shape against the REAL table stays already_safe —
	// the gate must not turn a genuine catalog-only change into a rehearsal or a
	// rejection.
	_, err = eng.Rehearse(ctx, `ALTER TABLE orders ADD COLUMN gate_probe int`, ModeCustom)
	if !errors.As(err, &rej) {
		t.Fatalf("ALTER TABLE orders ADD COLUMN returned %T (%v), want an already_safe *RejectedError", err, err)
	}
	if rej.Kind != RejectAlreadySafe {
		t.Errorf("kind = %q for a real-table constant-default ADD COLUMN, want %q", rej.Kind, RejectAlreadySafe)
	}
	// The recently-added honesty qualifier must still travel with the SAFE: ADD
	// COLUMN takes ACCESS EXCLUSIVE, whose acquisition wait a default run does not
	// simulate.
	if rej.Qualifier != QualifierAcquisitionNotSimulated {
		t.Errorf("qualifier = %q on a real-table ADD COLUMN, want %q",
			rej.Qualifier, QualifierAcquisitionNotSimulated)
	}
	if rej.Note == "" {
		t.Error("an already_safe ACCESS EXCLUSIVE shape must carry the acquisition-wait note")
	}

	// 3. No twin was consumed for any of it — the gate is a peek, not a clone.
	after := mgr.Status()
	if after.CheckedOut != before.CheckedOut || after.Warm != before.Warm {
		t.Errorf("the existence gate disturbed the pool: %+v -> %+v", before, after)
	}
	if got := countDatabases(t, itNamespace); got != beforeDBs {
		t.Errorf("database count changed from %d to %d — the gate cloned a twin", beforeDBs, got)
	}
}

// TestAddColumnUDFDefaultIsRehearsedNotWavedThrough pins the CRITICAL false-SAFE.
// ALTER TABLE ... ADD COLUMN x DEFAULT some_udf() is NOT a constant default: on
// PG16 a non-constant default forces a full ACCESS EXCLUSIVE table rewrite, a
// real outage. The old denylist of built-in volatile functions did not recognise
// a user-defined function, so it fell through to already_safe and a CI gate
// exited 0 on a minutes-long outage. It must now be classified volatile, RUN, and
// measure the rewrite — while a genuine constant default stays already_safe.
func TestAddColumnUDFDefaultIsRehearsedNotWavedThrough(t *testing.T) {
	_, eng := newLab(t, "udf_default", 1)
	ctx := context.Background()

	// brace_udf_default() is the VOLATILE SQL function the template defines.
	const sql = `ALTER TABLE orders ADD COLUMN probe double precision NOT NULL DEFAULT brace_udf_default()`

	// 1. It must NOT be short-circuited already_safe — that is the false-SAFE.
	res, err := eng.Rehearse(ctx, sql, ModeCustom)
	if err != nil {
		var rej *RejectedError
		if errors.As(err, &rej) && rej.Kind == RejectAlreadySafe {
			t.Fatalf("FALSE-SAFE: a UDF default was answered already_safe (class %s) — a full ACCESS EXCLUSIVE "+
				"rewrite would have been waved through without running", rej.Classification)
		}
		t.Fatalf("Rehearse: %v", err)
	}
	t.Logf("verdict=%s effective=%s blocked=%.3fs monitor_blocked=%.3fs casualties=%d errored=%d locks=%v class=%s",
		res.Verdict, res.EffectiveVerdict, res.BlockedSeconds, res.Monitor.BlockedSeconds,
		res.Casualties.Total, res.ErroredTotal, res.LockModes, res.Classification)

	if res.Error != "" {
		t.Fatalf("the migration itself failed (does brace_udf_default() exist in the twin?): %s", res.Error)
	}
	if res.Classification != ClassAddColumnVolatile {
		t.Errorf("classification = %q, want %q — a non-constant default is a rewrite", res.Classification, ClassAddColumnVolatile)
	}

	// 2. It must MEASURE the rewrite: a full-table rewrite under ACCESS EXCLUSIVE
	// over live writes shows up as SOMETHING — a freeze, dead writes, or the
	// server reporting waiters. Which dominates depends on the hardware, so the
	// assertion is the disjunction.
	if res.BlockedSeconds == 0 && res.Casualties.Total == 0 && res.Monitor.BlockedSeconds == 0 {
		t.Errorf("a table-rewriting ADD COLUMN over live writes produced no measurable harm at all "+
			"(blocked=%v casualties=%v monitor=%+v) — the rewrite was not measured",
			res.BlockedSeconds, res.Casualties.Total, res.Monitor)
	}
	if mode := res.Monitor.StrongestLockMode; mode != "" && mode != "AccessExclusiveLock" {
		t.Errorf("rewrite was seen holding %q; a full rewrite takes AccessExclusiveLock", mode)
	}

	// 3. The gate must not read SAFE. brace-check exits 0 only on effective SAFE /
	// already_safe; asserting effective_verdict is UNSAFE proves it would exit 1.
	if res.EffectiveVerdict == VerdictSafe {
		t.Errorf("effective_verdict = SAFE on a UDF-default rewrite — brace-check would exit 0 on an outage")
	}
	if res.Verdict != VerdictUnsafe {
		t.Errorf("verdict = %s, want UNSAFE — the rewrite froze live writes", res.Verdict)
	}

	// 4. The control: a genuine constant default STAYS already_safe (no run). The
	// inversion must not turn every default into a rehearsal.
	for _, constSQL := range []string{
		`ALTER TABLE orders ADD COLUMN c1 int NOT NULL DEFAULT 0`,
		`ALTER TABLE orders ADD COLUMN c2 text DEFAULT 'x'`,
	} {
		_, err := eng.Rehearse(ctx, constSQL, ModeCustom)
		var rej *RejectedError
		if !errors.As(err, &rej) || rej.Kind != RejectAlreadySafe {
			t.Errorf("a constant default should stay already_safe, got err=%v for %q", err, constSQL)
		}
	}
}

// TestDropColumnConstantTimeStaysSafe pins the constant-time ALTER TABLE
// false-UNSAFE. A catalog-only DROP COLUMN does no O(rows) work, so even told the
// operator's real table is 1000x the twin, the scale layer must NOT project a
// freeze: the run stays SAFE and effective-SAFE with no scale warning. The
// contrast is TestScaleProjectionCatchesFalseSafe, where a genuinely O(rows)
// SET NOT NULL at the same 1000x DOES project to UNSAFE.
func TestDropColumnConstantTimeStaysSafe(t *testing.T) {
	_, eng := newLab(t, "dropcol", 1)
	ctx := context.Background()

	// scratch_col lives in the seeded template (buildTemplate), so every cloned
	// twin has it — a Rehearse gets a fresh twin, so the column being dropped must
	// pre-exist in the template rather than be added in an earlier (throwaway) run.
	const sql = `ALTER TABLE orders DROP COLUMN scratch_col`
	// 1000x the twin — the same multiplier that turns SET NOT NULL UNSAFE.
	const targetRows = int64(templateRows) * 1000

	res, err := eng.Rehearse(ctx, sql, ModeCustom, WithTargetRows(targetRows))
	if err != nil {
		t.Fatalf("Rehearse: %v", err)
	}
	t.Logf("verdict=%s effective=%s blocked=%.3fs class=%s projection_known=%v warning=%q qualifier=%q",
		res.Verdict, res.EffectiveVerdict, res.BlockedSeconds, res.Classification,
		res.ScaleProjection.Known(), res.ScaleWarning, res.Qualifier)

	if res.Error != "" {
		t.Fatalf("the drop itself failed: %s", res.Error)
	}
	if res.Classification != ClassAlterTableConstantTime {
		t.Errorf("classification = %q, want %q", res.Classification, ClassAlterTableConstantTime)
	}
	if res.Verdict != VerdictSafe {
		t.Errorf("measured verdict = %s, want SAFE — a catalog-only drop froze nothing", res.Verdict)
	}
	// The heart of the fix: no phantom projection, so the gate stays SAFE.
	if res.EffectiveVerdict != VerdictSafe {
		t.Errorf("effective_verdict = %s, want SAFE — a constant-time drop must not be scaled into a false outage",
			res.EffectiveVerdict)
	}
	if res.ScaleProjection.Known() {
		t.Errorf("a scale projection was made for a catalog-only shape: %+v", res.ScaleProjection)
	}
	if res.ScaleWarning != "" {
		t.Errorf("a scale warning was emitted for a constant-time drop: %s", res.ScaleWarning)
	}
}
