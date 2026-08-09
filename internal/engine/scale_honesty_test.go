package engine

import (
	"strings"
	"testing"
)

// modeled_beyond_measurement: a projection whose target/twin ratio exceeds the
// high-extrapolation threshold must carry the distinct qualifier, and one within
// range must not. The qualifier never changes the verdict — it only labels the
// number as an order-of-magnitude floor.
func TestProjectScaleModeledBeyondMeasurement(t *testing.T) {
	const twin = int64(10_000_000)

	// 50x, measured above the floor: a real projection, flagged beyond measurement.
	far := projectScale(scaleInputs{
		Measured: 1.0, TwinRows: twin, TargetRows: 500_000_000,
		ScalesWithRows: true, BlocksWrites: true, Verdict: VerdictSafe,
		TargetSource: TargetSourceRequest,
	})
	if !far.Known() {
		t.Fatalf("expected a projection at 50x, got none")
	}
	if far.Qualifier != QualifierModeledBeyondMeasurement {
		t.Errorf("50x projection: qualifier = %q, want %q", far.Qualifier, QualifierModeledBeyondMeasurement)
	}
	if far.Verdict != VerdictUnsafe {
		t.Errorf("50x projection: verdict = %q, want UNSAFE", far.Verdict)
	}

	// 2x: still a projection, but within range — no beyond-measurement flag.
	near := projectScale(scaleInputs{
		Measured: 2.0, TwinRows: twin, TargetRows: 20_000_000,
		ScalesWithRows: true, BlocksWrites: true, Verdict: VerdictSafe,
		TargetSource: TargetSourceRequest,
	})
	if !near.Known() {
		t.Fatalf("expected a projection at 2x, got none")
	}
	if near.Qualifier != "" {
		t.Errorf("2x projection: qualifier = %q, want empty", near.Qualifier)
	}
}

// The scale warning must name WHERE the target size came from: a size the caller
// passed for this run reads differently from the deployment's standing default or
// a pure Brace assumption, and conflating them would let an agent read "your
// table" about a number it never supplied.
func TestScaleWarningNamesTargetSource(t *testing.T) {
	base := func(src string, assumed bool) ScaleProjection {
		return ScaleProjection{
			TwinRows: 10_000_000, TargetRows: 500_000_000,
			TargetRowsSource: src, TargetRowsAssumed: assumed,
			MeasuredBlockedSeconds: 0.1, ProjectedBlockedSeconds: 5.0,
			MeasuredAtFloor: true, Model: ScaleModelLinear, Verdict: VerdictUnsafe,
			ScalesWithRows: true,
		}
	}

	dep := scaleWarning(VerdictSafe, base(TargetSourceDeployment, false))
	if !strings.Contains(dep, "BRACE_TARGET_ROWS") || !strings.Contains(dep, "not a size passed for this run") {
		t.Errorf("deployment-source warning did not name the deployment default: %q", dep)
	}

	asm := scaleWarning(VerdictSafe, base(TargetSourceAssumed, true))
	if !strings.Contains(asm, "ASSUMED") {
		t.Errorf("assumed-source warning did not flag the assumption: %q", asm)
	}

	req := scaleWarning(VerdictSafe, base(TargetSourceRequest, false))
	if !strings.Contains(req, "your") {
		t.Errorf("request-source warning should speak of your table: %q", req)
	}
}
