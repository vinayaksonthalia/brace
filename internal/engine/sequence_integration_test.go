//go:build integration

// Integration tests for the multi-statement sequence runner (#8) and the per-run
// deadline (#7). They need a real PostgreSQL 16 superuser, because the whole point
// of a sequence — that statement 2 sees the schema statement 1 produced, on ONE
// persistent twin — is a claim about what the server does across two statements,
// and no fake can testify to it.
//
//	docker run --rm -d --name brace-dev-pg -p 5433:5432 \
//	  -e POSTGRES_HOST_AUTH_METHOD=trust postgres:16
//	TEST_ADMIN_URL=postgres://postgres@localhost:5433/postgres \
//	  go test -p 1 -tags integration ./internal/engine/
package engine

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestSequenceDependentStatements is the crown of the feature: a two-step
// migration where statement 2 depends on statement 1. `CREATE INDEX ON orders
// (promo)` can only succeed because `ALTER TABLE orders ADD COLUMN promo int` ran
// first, ON THE SAME TWIN — as two single runs the index build would ERROR "column
// promo does not exist", because each single run gets a fresh clone.
func TestSequenceDependentStatements(t *testing.T) {
	mgr, eng := newLab(t, "seqdep", 1)
	ctx := context.Background()

	res, err := eng.RehearseSequence(ctx, []string{
		"ALTER TABLE orders ADD COLUMN promo int",
		"CREATE INDEX idx_orders_promo ON orders (promo)",
	}, ModeCustom)
	if err != nil {
		t.Fatalf("RehearseSequence: %v", err)
	}
	t.Logf("overall verdict=%s effective=%s blocked=%.3fs casualties=%d statements=%d",
		res.Verdict, res.EffectiveVerdict, res.BlockedSeconds, res.Casualties.Total, len(res.Statements))
	for _, s := range res.Statements {
		t.Logf("  [%d] %-55q verdict=%s blocked=%.3fs casualties=%d err=%q",
			s.Index, s.SQL, s.Verdict, s.BlockedSeconds, s.Casualties.Total, s.Error)
	}

	if len(res.Statements) != 2 {
		t.Fatalf("got %d per-statement results, want 2", len(res.Statements))
	}

	// The whole reason the feature exists: statement 2 did NOT fail with "column
	// does not exist", because it ran on the twin statement 1 had already altered.
	idx := res.Statements[1]
	if idx.Error != "" {
		t.Fatalf("statement 2 (CREATE INDEX on the new column) errored: %q — the sequence did not persist statement 1's effect", idx.Error)
	}
	if idx.Verdict == VerdictErrored || idx.Verdict == VerdictSkipped {
		t.Fatalf("statement 2 verdict = %s, want a measured verdict", idx.Verdict)
	}
	if idx.Classification != ClassCreateIndex {
		t.Errorf("statement 2 classified %q, want create_index", idx.Classification)
	}

	// Statement 1 is the catalog-only ADD COLUMN: it ran (not skipped) and measured
	// a verdict of its own.
	add := res.Statements[0]
	if add.Classification != ClassAddColumnConstDefault {
		t.Errorf("statement 1 classified %q, want add_column_const_default", add.Classification)
	}
	if add.Verdict == VerdictSkipped || add.Verdict == VerdictErrored {
		t.Errorf("statement 1 verdict = %s; a catalog-only ADD COLUMN should run and measure", add.Verdict)
	}

	// The overall roll-up is a real measured verdict and was recorded.
	if res.Verdict != VerdictSafe && res.Verdict != VerdictUnsafe {
		t.Errorf("overall verdict = %s, want SAFE or UNSAFE", res.Verdict)
	}
	if res.EffectiveVerdict == "" {
		t.Error("overall effective_verdict is empty")
	}
	if res.BaselineWPS <= 0 {
		t.Errorf("no baseline measured (%d/s) for the sequence", res.BaselineWPS)
	}
	if len(res.Timeline) == 0 {
		t.Error("the sequence produced no timeline for the UI to replay")
	}
	if res.LedgerID == 0 {
		t.Error("the sequence run was not recorded in the ledger")
	}

	// The twin is released and the engine is idle again.
	if s := mgr.Status(); s.CheckedOut != 0 {
		t.Errorf("twin still checked out after the sequence: %+v", s)
	}
	if st := eng.Status(); st.Running {
		t.Error("engine still reports a run after the sequence finished")
	}
}

// TestSequenceCarriesAcquisitionQualifier proves the honesty-parity fix: a
// metadata-only ACCESS EXCLUSIVE statement (ADD COLUMN … DEFAULT <const>) measures
// SAFE on an idle twin because it gets its lock instantly — but the single path
// attaches acquisition_not_simulated to exactly this shape, and a sequence must do
// the same rather than emit a bare green light. The per-statement result, and the
// SAFE top-level roll-up, both carry the qualifier and a note.
func TestSequenceCarriesAcquisitionQualifier(t *testing.T) {
	_, eng := newLab(t, "seqacq", 1)
	ctx := context.Background()

	res, err := eng.RehearseSequence(ctx, []string{
		"ALTER TABLE orders ADD COLUMN flag boolean NOT NULL DEFAULT false",
	}, ModeCustom)
	if err != nil {
		t.Fatalf("RehearseSequence: %v", err)
	}
	if len(res.Statements) != 1 {
		t.Fatalf("got %d per-statement results, want 1", len(res.Statements))
	}
	s := res.Statements[0]
	t.Logf("stmt verdict=%s class=%s qualifier=%q blocked=%.3f cas=%d",
		s.Verdict, s.Classification, s.Qualifier, s.BlockedSeconds, s.Casualties.Total)

	if s.Verdict != VerdictSafe {
		t.Fatalf("catalog-only ADD COLUMN measured %s, want SAFE — cannot assert the acquisition qualifier otherwise", s.Verdict)
	}
	if s.Classification != ClassAddColumnConstDefault {
		t.Errorf("classified %q, want add_column_const_default", s.Classification)
	}
	if s.Qualifier != QualifierAcquisitionNotSimulated {
		t.Errorf("per-statement qualifier = %q, want %q — a sequence bare-SAFE'd an ACCESS EXCLUSIVE shape the single path is careful about",
			s.Qualifier, QualifierAcquisitionNotSimulated)
	}
	if s.Note == "" {
		t.Error("per-statement acquisition qualifier carries no note")
	}
	// And the SAFE top-level roll-up surfaces it, so a gate reading only the overall
	// verdict gets the same caveat.
	if res.Verdict == VerdictSafe && res.Qualifier != QualifierAcquisitionNotSimulated {
		t.Errorf("overall SAFE sequence dropped the acquisition qualifier: %q", res.Qualifier)
	}
	if res.Verdict == VerdictSafe && res.Note == "" {
		t.Error("overall SAFE sequence with an acquisition qualifier carries no note")
	}
}

// TestSequenceRejectsNonAllowlistedMidway proves the security guard is NOT relaxed
// by the sequence path: a DROP TABLE hidden between two legitimate statements is
// refused as a *RejectedError, and — because the refusal happens before Acquire —
// it costs no twin.
func TestSequenceRejectsNonAllowlistedMidway(t *testing.T) {
	mgr, eng := newLab(t, "seqrej", 1)
	ctx := context.Background()

	before := mgr.Status()
	beforeDBs := countDatabases(t, itNamespace)

	res, err := eng.RehearseSequence(ctx, []string{
		"CREATE INDEX idx_seq ON orders (customer_id)",
		"DROP TABLE orders",
		"ANALYZE orders",
	}, ModeCustom)
	if res != nil {
		t.Errorf("a rejected sequence returned a result")
	}
	var rej *RejectedError
	if !errors.As(err, &rej) {
		t.Fatalf("RehearseSequence returned %T (%v), want *RejectedError", err, err)
	}

	// A too-long/too-many sequence is refused the same way, also without a twin.
	tooMany := make([]string, maxSequenceStatements+1)
	for i := range tooMany {
		tooMany[i] = "ANALYZE orders"
	}
	if _, err := eng.RehearseSequence(ctx, tooMany, ModeCustom); !errors.As(err, &rej) {
		t.Errorf("an over-count sequence returned %v, want *RejectedError", err)
	}

	after := mgr.Status()
	if after.CheckedOut != before.CheckedOut || after.Warm != before.Warm {
		t.Errorf("a rejected sequence disturbed the pool: %+v -> %+v", before, after)
	}
	if got := countDatabases(t, itNamespace); got != beforeDBs {
		t.Errorf("database count changed from %d to %d on a rejected sequence", beforeDBs, got)
	}
	if eng.Status().Running {
		t.Error("engine is stuck reporting a run after a sequence rejection")
	}
}

// TestSingleStatementPathUnchanged: the default single-statement flow is additive
// and carries no per-statement array — the sequence field is nil (omitted) for it.
func TestSingleStatementPathUnchanged(t *testing.T) {
	_, eng := newLab(t, "seqsingle", 1)
	ctx := context.Background()

	res, err := eng.Rehearse(ctx, `CREATE INDEX idx_single ON orders (customer_id)`, ModeCustom)
	if err != nil {
		t.Fatalf("Rehearse: %v", err)
	}
	if res.Statements != nil {
		t.Errorf("a single-statement run carried a per-statement array of %d: %+v", len(res.Statements), res.Statements)
	}
}

// TestDeadlineFlowsToCasualties (#7) proves the per-run deadline reaches
// casualties.deadline_ms — the value the MCP now exposes as deadline_ms and the API
// as lock_timeout_ms. It is the definition of a casualty, so it must be reported
// back verbatim on both the single and the sequence paths.
func TestDeadlineFlowsToCasualties(t *testing.T) {
	_, eng := newLab(t, "deadline", 1)
	ctx := context.Background()

	const want = 500 * time.Millisecond

	res, err := eng.Rehearse(ctx, `CREATE INDEX idx_deadline ON orders (customer_id)`, ModeNaive, WithLockTimeout(want))
	if err != nil {
		t.Fatalf("Rehearse: %v", err)
	}
	if got := res.Casualties.DeadlineMS; got != want.Milliseconds() {
		t.Errorf("single-run casualties.deadline_ms = %d, want %d", got, want.Milliseconds())
	}

	seq, err := eng.RehearseSequence(ctx, []string{
		"CREATE INDEX idx_deadline_seq ON orders (amount_cents)",
	}, ModeCustom, WithLockTimeout(want))
	if err != nil {
		t.Fatalf("RehearseSequence: %v", err)
	}
	if got := seq.Casualties.DeadlineMS; got != want.Milliseconds() {
		t.Errorf("sequence casualties.deadline_ms = %d, want %d", got, want.Milliseconds())
	}
	if len(seq.Statements) == 1 {
		if got := seq.Statements[0].Casualties.DeadlineMS; got != want.Milliseconds() {
			t.Errorf("per-statement casualties.deadline_ms = %d, want %d", got, want.Milliseconds())
		}
	}
}
