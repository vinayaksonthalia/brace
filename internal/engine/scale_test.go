package engine

import (
	"strings"
	"testing"
)

// The projection is a MODEL, and the only thing standing between it and
// snake-oil is that every branch of it is pinned here: what it refuses to
// project, what it does with a measurement it could not resolve, and where it
// is allowed to override a SAFE.

func TestProjectScale(t *testing.T) {
	const tenM, fiveHundredM = int64(10_000_000), int64(500_000_000)

	cases := []struct {
		name string
		in   scaleInputs

		wantVerdict   string  // "" = no projection made
		wantProjected float64 // only checked when wantVerdict != ""
	}{
		{
			// The case this whole file exists for: a statement that measures
			// SAFE on a 10M-row twin and is a ten-second outage on a 500M-row
			// production table.
			//
			// 0.2s is BELOW one 250ms sample, so it is floored before scaling:
			// 0.25 x 50 = 12.5s "at most", not 0.2 x 50 = 10s. The tool refuses
			// to report a projection more precise than the instrument that fed
			// it, and rounds toward the outage rather than away from it.
			name: "the false safe: SAFE measurement, UNSAFE at 50x",
			in: scaleInputs{
				Measured: 0.2, TwinRows: tenM, TargetRows: fiveHundredM,
				ScalesWithRows: true, BlocksWrites: true, Verdict: VerdictSafe,
			},
			wantVerdict: VerdictUnsafe, wantProjected: 12.5,
		},
		{
			name: "measured above the floor scales from the measurement itself",
			in: scaleInputs{
				Measured: 0.8, TwinRows: tenM, TargetRows: 20_000_000,
				ScalesWithRows: true, BlocksWrites: true, Verdict: VerdictSafe,
			},
			wantVerdict: VerdictUnsafe, wantProjected: 1.6,
		},
		{
			// 0.0s does NOT mean zero — it means "shorter than one 250ms sample".
			// Scaling 0 by 100 would give 0, which is the false safe wearing a
			// different hat.
			name: "zero measured is projected from the resolution floor, not from zero",
			in: scaleInputs{
				Measured: 0, TwinRows: tenM, TargetRows: 1_000_000_000,
				ScalesWithRows: true, BlocksWrites: true, Verdict: VerdictSafe,
			},
			wantVerdict: VerdictUnsafe, wantProjected: 25,
		},
		{
			name: "exactly at the floor is still at the floor",
			in: scaleInputs{
				Measured: scaleFloorSeconds, TwinRows: tenM, TargetRows: 20_000_000,
				ScalesWithRows: true, BlocksWrites: true, Verdict: VerdictSafe,
			},
			wantVerdict: VerdictSafe, wantProjected: 0.5,
		},
		{
			name: "a small target still projects, and can stay SAFE",
			in: scaleInputs{
				Measured: 0.4, TwinRows: tenM, TargetRows: 20_000_000,
				ScalesWithRows: true, BlocksWrites: true, Verdict: VerdictSafe,
			},
			wantVerdict: VerdictSafe, wantProjected: 0.8,
		},
		{
			// A production table SMALLER than the twin: the model runs in both
			// directions, and shrinking a measured outage is as legitimate as
			// growing one — but it can never soften the measured verdict, which
			// TestEffectiveVerdict pins.
			name: "target smaller than twin projects downward",
			in: scaleInputs{
				Measured: 4, TwinRows: tenM, TargetRows: 1_000_000,
				ScalesWithRows: true, BlocksWrites: true, Verdict: VerdictUnsafe,
			},
			wantVerdict: VerdictSafe, wantProjected: 0.4,
		},
		{
			name: "no target rows: no projection, and no pretending",
			in: scaleInputs{
				Measured: 0.2, TwinRows: tenM, TargetRows: 0,
				ScalesWithRows: true, BlocksWrites: true, Verdict: VerdictSafe,
			},
		},
		{
			name: "no twin rows: no denominator, no projection",
			in: scaleInputs{
				Measured: 0.2, TwinRows: 0, TargetRows: fiveHundredM,
				ScalesWithRows: true, BlocksWrites: true, Verdict: VerdictSafe,
			},
		},
		{
			name: "both unknown",
			in: scaleInputs{
				Measured: 0.2, TwinRows: 0, TargetRows: 0,
				ScalesWithRows: true, BlocksWrites: true, Verdict: VerdictSafe,
			},
		},
		{
			name: "negative target rows is treated as unstated",
			in: scaleInputs{
				Measured: 0.2, TwinRows: tenM, TargetRows: -5,
				ScalesWithRows: true, BlocksWrites: true, Verdict: VerdictSafe,
			},
		},
		{
			name: "absurd target rows is treated as unstated",
			in: scaleInputs{
				Measured: 0.2, TwinRows: tenM, TargetRows: int64(maxTargetRows) * 10,
				ScalesWithRows: true, BlocksWrites: true, Verdict: VerdictSafe,
			},
		},
		{
			// Catalog-only work is the same microsecond at any size. Scaling it
			// would invent an outage that cannot physically happen, which is as
			// dishonest as missing one that can.
			name: "catalog-only shape is never projected",
			in: scaleInputs{
				Measured: 0.1, TwinRows: tenM, TargetRows: fiveHundredM,
				ScalesWithRows: false, BlocksWrites: true, Verdict: VerdictSafe,
			},
		},
		{
			// CREATE INDEX CONCURRENTLY takes 50x longer on a 50x table — without
			// blocking anybody. Its freeze is zero at every size.
			name: "scaling work that does not block writes is not projected",
			in: scaleInputs{
				Measured: 0, TwinRows: tenM, TargetRows: fiveHundredM,
				ScalesWithRows: true, BlocksWrites: false, Verdict: VerdictSafe,
			},
		},
		{
			// The credibility bug this fix exists for: CREATE INDEX CONCURRENTLY held
			// only SHARE UPDATE EXCLUSIVE (no write-blocking lock observed), and its
			// two heap scans dipped the write rate below the threshold from I/O
			// contention on a shared box — NOT a freeze. A dip on its own must not be
			// scaled into a phantom outage: the same non-blocking lock is merely held
			// for longer on a bigger table, freezing nobody at any size.
			name: "a write-rate dip with no write-blocking lock is not projected (the CONCURRENTLY bug)",
			in: scaleInputs{
				Measured: 0.85, TwinRows: tenM, TargetRows: fiveHundredM,
				ScalesWithRows: true, BlocksWrites: false, HeldWriteBlockingLock: false, Verdict: VerdictSafe,
			},
		},
		{
			// Even a large I/O dip that crossed the measured threshold is not scaled
			// when no write-blocking lock was held — noise is noise at any magnitude.
			name: "a large I/O dip with no write-blocking lock is still not projected",
			in: scaleInputs{
				Measured: 3, TwinRows: tenM, TargetRows: fiveHundredM,
				ScalesWithRows: true, BlocksWrites: false, HeldWriteBlockingLock: false, Verdict: VerdictSafe,
			},
		},
		{
			// ...unless the monitor CORROBORATED the freeze with a write-blocking lock
			// actually held. A shape predicted non-blocking that surprised us by taking
			// a stronger lock is a genuine freeze — measurement outranks prediction,
			// which is the entire premise of the product — so it IS projected.
			name: "a surprise freeze corroborated by a write-blocking lock is projected",
			in: scaleInputs{
				Measured: 0.6, TwinRows: tenM, TargetRows: fiveHundredM,
				ScalesWithRows: true, BlocksWrites: false, HeldWriteBlockingLock: true, Verdict: VerdictSafe,
			},
			wantVerdict: VerdictUnsafe, wantProjected: 30,
		},
		{
			name: "ERRORED measured nothing, so there is nothing to scale",
			in: scaleInputs{
				Measured: 0, TwinRows: tenM, TargetRows: fiveHundredM,
				ScalesWithRows: true, BlocksWrites: true, Verdict: VerdictErrored,
			},
		},
		{
			name: "INCONCLUSIVE is not a completed measurement",
			in: scaleInputs{
				Measured: 12, TwinRows: tenM, TargetRows: fiveHundredM,
				ScalesWithRows: true, BlocksWrites: true, Verdict: VerdictInconclusive,
			},
		},
		{
			// A catch-all ALTER TABLE (scaling merely UNCERTAIN) that measured
			// nothing above the floor must NOT be projected: manufacturing a freeze
			// from the 250ms floor here is a false-UNSAFE on a statement that did no
			// measurable work — the DROP COLUMN family lived on this bug.
			name: "uncertain-scaling shape at the floor is not projected",
			in: scaleInputs{
				Measured: 0.1, TwinRows: tenM, TargetRows: fiveHundredM,
				ScalesWithRows: true, BlocksWrites: true, ScaleUncertain: true, Verdict: VerdictSafe,
			},
		},
		{
			// ...but if that same uncertain shape actually MEASURED a freeze above
			// the floor AND the monitor saw a write-blocking lock (the rewrite's
			// ACCESS EXCLUSIVE), a real measurement outranks the uncertainty, so it is
			// projected.
			name: "uncertain-scaling shape with a real measured freeze is projected",
			in: scaleInputs{
				Measured: 0.6, TwinRows: tenM, TargetRows: fiveHundredM,
				ScalesWithRows: true, BlocksWrites: true, ScaleUncertain: true,
				HeldWriteBlockingLock: true, Verdict: VerdictSafe,
			},
			wantVerdict: VerdictUnsafe, wantProjected: 30,
		},
		{
			// The same uncertain shape whose dip was I/O contention (no write-blocking
			// lock observed) must NOT be projected — this is the ClassAlterTableOther
			// / generic analogue of the CONCURRENTLY bug.
			name: "uncertain-scaling shape with an I/O dip and no write-blocking lock is not projected",
			in: scaleInputs{
				Measured: 0.6, TwinRows: tenM, TargetRows: fiveHundredM,
				ScalesWithRows: true, BlocksWrites: true, ScaleUncertain: true,
				HeldWriteBlockingLock: false, Verdict: VerdictSafe,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := projectScale(tc.in)

			if got.Verdict != tc.wantVerdict {
				t.Fatalf("Verdict = %q, want %q (projection %+v)", got.Verdict, tc.wantVerdict, got)
			}
			if got.Known() != (tc.wantVerdict != "") {
				t.Errorf("Known() = %v, want %v", got.Known(), tc.wantVerdict != "")
			}
			if tc.wantVerdict == "" {
				if got.ProjectedBlockedSeconds != 0 {
					t.Errorf("ProjectedBlockedSeconds = %v with no projection; want 0", got.ProjectedBlockedSeconds)
				}
			} else if got.ProjectedBlockedSeconds != tc.wantProjected {
				t.Errorf("ProjectedBlockedSeconds = %v, want %v", got.ProjectedBlockedSeconds, tc.wantProjected)
			}
			// MeasuredAtFloor describes the MEASUREMENT, so it is set whether or
			// not a projection was made — a reader of a run with no target size
			// still deserves to know the freeze was below the instrument's
			// resolution.
			if want := tc.in.Measured <= scaleFloorSeconds; got.MeasuredAtFloor != want {
				t.Errorf("MeasuredAtFloor = %v, want %v (measured %.3fs, floor %.3fs)",
					got.MeasuredAtFloor, want, tc.in.Measured, scaleFloorSeconds)
			}
			if got.MeasuredBlockedSeconds != tc.in.Measured {
				t.Errorf("MeasuredBlockedSeconds = %v, want the measurement %v unchanged",
					got.MeasuredBlockedSeconds, tc.in.Measured)
			}
			if got.Model == "" {
				t.Error("Model is empty: a projection must always name its assumption")
			}
			if got.TargetRows < 0 {
				t.Errorf("TargetRows = %d, want a normalised non-negative value", got.TargetRows)
			}
		})
	}
}

// The projection is never allowed to make a measured finding look better. It is
// a veto on SAFE, not a second opinion on UNSAFE.
func TestEffectiveVerdict(t *testing.T) {
	cases := []struct {
		name      string
		measured  string
		projected string
		want      string
	}{
		{"safe and no projection stays safe", VerdictSafe, "", VerdictSafe},
		{"safe and projected safe stays safe", VerdictSafe, VerdictSafe, VerdictSafe},
		{"safe but projected unsafe becomes unsafe", VerdictSafe, VerdictUnsafe, VerdictUnsafe},
		{"unsafe stays unsafe when projection is safe", VerdictUnsafe, VerdictSafe, VerdictUnsafe},
		{"unsafe stays unsafe with no projection", VerdictUnsafe, "", VerdictUnsafe},
		{"errored is never rewritten by a projection", VerdictErrored, VerdictUnsafe, VerdictErrored},
		{"inconclusive is never rewritten by a projection", VerdictInconclusive, VerdictUnsafe, VerdictInconclusive},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := ScaleProjection{Verdict: tc.projected}
			if got := effectiveVerdict(tc.measured, p); got != tc.want {
				t.Errorf("effectiveVerdict(%q, %q) = %q, want %q", tc.measured, tc.projected, got, tc.want)
			}
		})
	}
}

// The warning is the product: the sentence a reader sees INSTEAD of shipping.
func TestScaleWarning(t *testing.T) {
	t.Run("fires only on the false-safe transition", func(t *testing.T) {
		cases := []struct {
			measured, projected string
			want                bool
		}{
			{VerdictSafe, VerdictUnsafe, true},
			{VerdictSafe, VerdictSafe, false},
			{VerdictSafe, "", false},
			{VerdictUnsafe, VerdictUnsafe, false},
			{VerdictUnsafe, VerdictSafe, false},
			{VerdictErrored, VerdictUnsafe, false},
			{VerdictInconclusive, VerdictUnsafe, false},
		}
		for _, tc := range cases {
			p := ScaleProjection{
				Verdict: tc.projected, TwinRows: 10_000_000, TargetRows: 500_000_000,
				ProjectedBlockedSeconds: 10, Model: ScaleModelLinear,
			}
			got := scaleWarning(tc.measured, p)
			if (got != "") != tc.want {
				t.Errorf("scaleWarning(%q, projected=%q) = %q; want non-empty=%v",
					tc.measured, tc.projected, got, tc.want)
			}
		}
	})

	t.Run("names the numbers a reader needs", func(t *testing.T) {
		p := projectScale(scaleInputs{
			Measured: 0.6, TwinRows: 10_000_000, TargetRows: 500_000_000,
			ScalesWithRows: true, BlocksWrites: true, Verdict: VerdictSafe,
		})
		got := scaleWarning(VerdictSafe, p)
		for _, want := range []string{"10,000,000", "500,000,000", "50x", "30s", ScaleModelLinear} {
			if !strings.Contains(got, want) {
				t.Errorf("warning missing %q:\n%s", want, got)
			}
		}
	})

	t.Run("a floor measurement is reported as an upper bound, never as a precise claim", func(t *testing.T) {
		p := projectScale(scaleInputs{
			Measured: 0, TwinRows: 10_000_000, TargetRows: 500_000_000,
			ScalesWithRows: true, BlocksWrites: true, Verdict: VerdictSafe,
		})
		got := scaleWarning(VerdictSafe, p)
		if !strings.Contains(got, "at most") {
			t.Errorf("a projection from the resolution floor must say 'at most':\n%s", got)
		}
		if !strings.Contains(got, "250ms") {
			t.Errorf("the reader must be told why the measurement was floored:\n%s", got)
		}
	})
}

func TestValidTargetRows(t *testing.T) {
	cases := []struct{ in, want int64 }{
		{0, 0},
		{-1, 0},
		{1, 1},
		{500_000_000, 500_000_000},
		{int64(maxTargetRows), int64(maxTargetRows)},
		{int64(maxTargetRows) + 1, 0},
	}
	for _, tc := range cases {
		if got := validTargetRows(tc.in); got != tc.want {
			t.Errorf("validTargetRows(%d) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

// The teach layer's ScalesWithRows is an input to the projection, so a wrong
// entry is a wrong verdict. These are the ones the projection's honesty hangs
// on: the catalog-only shapes must never scale, the rewriting ones must.
func TestLessonsScaleWithRows(t *testing.T) {
	scaling := []Classification{
		ClassCreateIndex, ClassAlterColumnType, ClassAddColumnVolatile,
		ClassReindex, ClassVacuumFull, ClassAddConstraint, ClassSetNotNull,
		ClassValidateConstraint, ClassAlterIndexRewrite,
	}
	flat := []Classification{
		ClassAddColumnConstDefault, ClassComment, ClassAnalyze,
		ClassAlterIndex, ClassAddConstraintNotValid, ClassAlterTableConstantTime,
	}
	for _, c := range scaling {
		if !Explain(c).ScalesWithRows {
			t.Errorf("%s: work is O(rows) but ScalesWithRows is false — its measurement would never be projected", c)
		}
	}
	for _, c := range flat {
		if Explain(c).ScalesWithRows {
			t.Errorf("%s: catalog-only shape marked ScalesWithRows — projecting it would invent an outage", c)
		}
	}
}

func TestCommas(t *testing.T) {
	cases := []struct {
		in   int64
		want string
	}{
		{0, "0"}, {7, "7"}, {999, "999"}, {1000, "1,000"},
		{10_000_000, "10,000,000"}, {500_000_000, "500,000,000"}, {-1234, "-1,234"},
	}
	for _, tc := range cases {
		if got := commas(tc.in); got != tc.want {
			t.Errorf("commas(%d) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
