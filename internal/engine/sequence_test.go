package engine

import (
	"errors"
	"strings"
	"testing"
)

// TestAssessSequenceCaps: the count and total-byte caps are enforced, and an
// empty list is refused — all UX/blast-radius guards, before any twin is spent.
func TestAssessSequenceCaps(t *testing.T) {
	if _, err := assessSequence(nil, runOptions{}); err == nil {
		t.Error("an empty sequence was admitted")
	}

	tooMany := make([]string, maxSequenceStatements+1)
	for i := range tooMany {
		tooMany[i] = "ANALYZE orders"
	}
	_, err := assessSequence(tooMany, runOptions{})
	var rej *RejectedError
	if !errors.As(err, &rej) || rej.Code != "sequence_too_many" {
		t.Errorf("a %d-statement sequence returned %v, want sequence_too_many", len(tooMany), err)
	}

	// One giant statement blows the total-byte cap even though the count is fine.
	big := "CREATE INDEX idx ON orders (" + strings.Repeat("a", maxSequenceBytes) + ")"
	_, err = assessSequence([]string{big}, runOptions{})
	if !errors.As(err, &rej) || rej.Code != "sequence_too_long" {
		t.Errorf("an over-long sequence returned %v, want sequence_too_long", err)
	}
}

// TestAssessSequenceRejectsNonAllowlistedInMiddle: a sequence must never be a way
// to smuggle a non-allow-listed verb past the guard. A DROP TABLE hidden between
// two legitimate statements rejects the WHOLE sequence, naming the offender.
func TestAssessSequenceRejectsNonAllowlistedInMiddle(t *testing.T) {
	stmts := []string{
		"CREATE INDEX idx_orders_customer ON orders (customer_id)",
		"DROP TABLE orders",
		"ANALYZE orders",
	}
	_, err := assessSequence(stmts, runOptions{})
	var rej *RejectedError
	if !errors.As(err, &rej) {
		t.Fatalf("assessSequence returned %T (%v), want *RejectedError", err, err)
	}
	if !strings.Contains(rej.Reason, "Statement 2") {
		t.Errorf("rejection did not name the offending statement: %q", rej.Reason)
	}
}

// TestAssessSequenceRejectsSmuggle: a `;`-smuggled second statement inside ONE
// element is caught by Assess's own splitter, exactly as on the single path.
func TestAssessSequenceRejectsSmuggle(t *testing.T) {
	stmts := []string{"CREATE INDEX idx ON orders (customer_id); DROP TABLE orders"}
	_, err := assessSequence(stmts, runOptions{})
	var rej *RejectedError
	if !errors.As(err, &rej) {
		t.Fatalf("a semicolon-smuggled element was admitted: %v", err)
	}
}

// TestAssessSequenceAdmitsAlreadySafe: a catalog-only statement (ADD COLUMN with a
// constant default is answered already_safe on the single path) is ADMITTED in a
// sequence and marked staticSafe — it must still execute so a later statement sees
// its effect. This is the divergence the whole feature turns on.
func TestAssessSequenceAdmitsAlreadySafe(t *testing.T) {
	stmts := []string{
		"ALTER TABLE orders ADD COLUMN promo int",
		"CREATE INDEX idx_orders_promo ON orders (promo)",
	}
	plans, err := assessSequence(stmts, runOptions{})
	if err != nil {
		t.Fatalf("a dependent ADD COLUMN + CREATE INDEX sequence was rejected: %v", err)
	}
	if len(plans) != 2 {
		t.Fatalf("got %d plans, want 2", len(plans))
	}
	if !plans[0].staticSafe {
		t.Error("the catalog-only ADD COLUMN was not marked staticSafe")
	}
	if plans[0].as.Classification != ClassAddColumnConstDefault {
		t.Errorf("statement 1 classified %q, want add_column_const_default", plans[0].as.Classification)
	}
	if plans[1].as.Classification != ClassCreateIndex {
		t.Errorf("statement 2 classified %q, want create_index", plans[1].as.Classification)
	}
}

// TestBlockedSecondsBetween: the windowed freeze measurement counts a collapse
// inside the statement's window and ignores one that belongs to a different window.
func TestBlockedSecondsBetween(t *testing.T) {
	const baseline = 100.0
	// A freeze (WPS 0) from T=1.5 to T=2.0, high before and after.
	points := []RatePoint{
		{T: 0.5, WPS: 100},
		{T: 1.0, WPS: 100},
		{T: 1.5, WPS: 0},
		{T: 2.0, WPS: 0},
		{T: 2.5, WPS: 100},
		{T: 3.0, WPS: 100},
		{T: 3.5, WPS: 100},
	}
	if in := blockedSecondsBetween(points, 1.4, 2.2, baseline); in <= 0 {
		t.Errorf("blocked-seconds in the freeze window = %.3f, want > 0", in)
	}
	if out := blockedSecondsBetween(points, 2.6, 3.5, baseline); out != 0 {
		t.Errorf("blocked-seconds in a clean window = %.3f, want 0", out)
	}
	if none := blockedSecondsBetween(points, 1.4, 2.2, 0); none != 0 {
		t.Errorf("with no baseline the window must report 0, got %.3f", none)
	}
}

// TestVerdictRankWorstOf: the worst-of roll-up orders the verdicts so a gate
// blocks on any UNSAFE step, and the rank/verdict mapping round-trips.
func TestVerdictRankWorstOf(t *testing.T) {
	if verdictRank(VerdictUnsafe) <= verdictRank(VerdictSafe) {
		t.Error("UNSAFE must outrank SAFE")
	}
	if verdictRank(VerdictInconclusive) <= verdictRank(VerdictErrored) {
		t.Error("INCONCLUSIVE must outrank ERRORED")
	}
	if verdictRank(VerdictSkipped) != 0 {
		t.Error("SKIPPED must rank 0 so it never dominates the roll-up")
	}
	// A sequence of SAFE, SAFE, UNSAFE rolls up to UNSAFE.
	worst := 0
	for _, v := range []string{VerdictSafe, VerdictSafe, VerdictUnsafe} {
		if r := verdictRank(v); r > worst {
			worst = r
		}
	}
	if verdictForRank(worst) != VerdictUnsafe {
		t.Errorf("worst-of {SAFE,SAFE,UNSAFE} = %q, want UNSAFE", verdictForRank(worst))
	}
	if verdictForRank(0) != "" {
		t.Errorf("rank 0 must map to the empty verdict, got %q", verdictForRank(0))
	}
}

// TestJoinSequenceSQL: the ledger/stream rendering is one terminated statement per
// line, in order, with no doubled semicolons.
func TestJoinSequenceSQL(t *testing.T) {
	plans := []stmtPlan{
		{as: &Assessment{SQL: "ALTER TABLE orders ADD COLUMN promo int;"}},
		{as: &Assessment{SQL: "CREATE INDEX ON orders (promo)"}},
	}
	got := joinSequenceSQL(plans)
	want := "ALTER TABLE orders ADD COLUMN promo int;\nCREATE INDEX ON orders (promo);"
	if got != want {
		t.Errorf("joinSequenceSQL =\n%q\nwant\n%q", got, want)
	}
	if strings.Contains(got, ";;") {
		t.Error("joined SQL has a doubled semicolon")
	}
}
