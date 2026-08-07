package engine

// The scale projection.
//
// This file exists because of a false-safe that the duration-based verdict has
// on its own. Lock PRESENCE is scale-free: a plain CREATE INDEX takes SHARE on
// a thousand-row table exactly as it does on a billion-row one. Lock DURATION
// is not: the index build, the table rewrite, the constraint validation scan are
// all O(rows), so the same statement that holds the lock for 0.2s against a
// ten-million-row twin holds it for ten seconds against a five-hundred-million
// row production table.
//
// A verdict of "UNSAFE when blocked > 1s", measured on a SAMPLE, therefore says
// SAFE about a statement that is an outage in production. That is the single
// failure mode this product exists to prevent, so the measured number is not
// allowed to be the last word: when the operator tells us how big the real table
// is, we scale the measurement to it and let the projection veto SAFE.
//
// What is measured and what is modelled stays separated on purpose. Verdict and
// BlockedSeconds remain exactly what the instrument saw. EffectiveVerdict is the
// one a CI gate should read, and it is the worse of measured and projected.

import "fmt"

// scaleFloorSeconds is the sampler's resolution floor: one 250ms sample. A
// blocked window measured at or below it is not "0.2 seconds of freeze", it is
// "shorter than this instrument can resolve" — see MeasuredAtFloor.
const scaleFloorSeconds = 0.25

// maxTargetRows bounds the operator-supplied production row count. A trillion
// rows is already far beyond any single unpartitioned Postgres table; past it
// the caller has fat-fingered a number and a projection built on it would be
// theatre.
const maxTargetRows = 1e12

// ScaleModelLinear names the assumption the projection makes, in one line, so
// nobody has to read this file to know what they are trusting.
const ScaleModelLinear = "linear in row count (index build and table rewrite are both O(rows))"

// ScaleProjection is the modelled answer to "what does this migration cost on MY
// table?", as distinct from the measured answer to "what did it cost on this
// twin?".
//
// It is only ever as good as its inputs: TargetRows is what an operator TOLD us
// (BRACE_TARGET_ROWS, or target_rows on the run request), never something Brace
// discovered. With no target there is no projection, and the struct says so via
// an empty Verdict.
type ScaleProjection struct {
	// TwinRows is how many rows the rehearsal actually ran against — the seeded
	// template's size. 0 when the deployment did not tell the engine.
	TwinRows int64 `json:"twin_rows"`
	// TargetRows is the operator's production table size. 0 = unset.
	TargetRows int64 `json:"target_rows"`
	// MeasuredBlockedSeconds is the freeze the instrument saw on the twin. It is
	// a copy of RunResult.BlockedSeconds, kept here so the projection is
	// self-contained in a report.
	MeasuredBlockedSeconds float64 `json:"measured_blocked_seconds"`
	// ProjectedBlockedSeconds is MeasuredBlockedSeconds scaled by
	// TargetRows/TwinRows — or, when the measurement sat at the resolution
	// floor, the FLOOR scaled by that ratio, which makes it an upper bound on
	// something we could not resolve rather than a precise claim. 0 when unknown.
	ProjectedBlockedSeconds float64 `json:"projected_blocked_seconds"`
	// MeasuredAtFloor says the twin measurement was at or below one sample
	// (250ms). The projection is then an "at most" figure: a freeze reported as
	// 0.0s cannot honestly be scaled to 0.0s at 100x, because 0.0s only ever
	// meant "under 0.25s".
	MeasuredAtFloor bool `json:"measured_at_floor"`
	// Model names the assumption in human words.
	Model string `json:"model"`
	// Verdict is SAFE/UNSAFE applied to ProjectedBlockedSeconds using the same
	// one-second threshold as the measured verdict. Empty when no projection
	// could be made — which is itself the honest answer, not a SAFE.
	Verdict string `json:"verdict,omitempty"`
	// ScalesWithRows carries the teach layer's answer to "does this shape's cost
	// grow with the table?". False for a catalog-only change, where the linear
	// model does not apply at all and the projection is suppressed.
	ScalesWithRows bool `json:"scales_with_rows"`
}

// Known reports whether a projection was actually computed.
func (p ScaleProjection) Known() bool { return p.Verdict != "" }

// validTargetRows normalises an operator-supplied production row count.
// Anything absent, non-positive or absurd is treated as "not stated" rather
// than as an error: a projection is an optional extra, and refusing to run a
// rehearsal because somebody typed -1 would be the wrong trade.
func validTargetRows(n int64) int64 {
	if n <= 0 || float64(n) > maxTargetRows {
		return 0
	}
	return n
}

// scaleInputs is everything projectScale needs, gathered in one place so the
// arithmetic can be tested without a database.
type scaleInputs struct {
	// Measured is the run's BlockedSeconds.
	Measured float64
	// TwinRows / TargetRows are the sample size and the operator's real size.
	TwinRows, TargetRows int64
	// ScalesWithRows is the teach layer's fact: this shape's work is O(rows).
	ScalesWithRows bool
	// BlocksWrites is the teach layer's prediction: the O(rows) work is done
	// under a lock that stops writes. Both must hold before a freeze measured
	// here can be scaled — CREATE INDEX CONCURRENTLY takes twice as long on a
	// table twice the size, but it takes twice as long WITHOUT blocking anybody,
	// so scaling its (zero) freeze would invent an outage.
	BlocksWrites bool
	// Verdict is the measured verdict, used to refuse projecting from a run that
	// measured nothing.
	Verdict string
}

// projectScale computes the projection for one run.
//
// It deliberately refuses rather than guessing when:
//   - either row count is unknown — there is no ratio;
//   - the statement's cost is not O(rows) (a catalog-only ADD COLUMN with a
//     constant default is the same microsecond at any size), or its O(rows) work
//     does not block writes — scaling either would manufacture an outage that
//     physics does not support;
//   - the run did not complete a freeze measurement (ERRORED, INCONCLUSIVE).
//
// The one case where a non-blocking prediction is overridden is when the
// rehearsal MEASURED a freeze longer than the resolution floor anyway. The
// measurement outranks the prediction — that is the whole premise of the
// product — so a surprise freeze on a scaling shape is projected too.
func projectScale(in scaleInputs) ScaleProjection {
	p := ScaleProjection{
		TwinRows:               in.TwinRows,
		TargetRows:             validTargetRows(in.TargetRows),
		MeasuredBlockedSeconds: in.Measured,
		Model:                  ScaleModelLinear,
		ScalesWithRows:         in.ScalesWithRows,
		MeasuredAtFloor:        in.Measured <= scaleFloorSeconds,
	}
	measured := in.Measured
	if in.TwinRows <= 0 || p.TargetRows <= 0 {
		return p
	}
	if !in.ScalesWithRows || !(in.BlocksWrites || measured > scaleFloorSeconds) {
		return p
	}
	if in.Verdict != VerdictSafe && in.Verdict != VerdictUnsafe {
		// ERRORED / INCONCLUSIVE: blocked_seconds is not a completed
		// measurement, so there is nothing honest to scale.
		return p
	}
	base := measured
	if p.MeasuredAtFloor {
		// Project from the floor, not from the number. Below 250ms the
		// instrument's answer is "shorter than one sample", and the only honest
		// extrapolation is an upper bound built on the smallest thing we could
		// have seen.
		base = scaleFloorSeconds
	}
	ratio := float64(p.TargetRows) / float64(in.TwinRows)
	p.ProjectedBlockedSeconds = roundSeconds(base * ratio)
	// A projection has no observed casualties of its own: it is a duration
	// scaled onto a bigger table. Judge it on the projected freeze alone.
	p.Verdict = verdictFor(p.ProjectedBlockedSeconds, 0)
	return p
}

// effectiveVerdict is the verdict a CI gate should act on: the worse of what was
// measured and what is projected. It never softens a measured finding — a
// projection can turn SAFE into UNSAFE, and never the other way round.
func effectiveVerdict(measured string, p ScaleProjection) string {
	if measured == VerdictSafe && p.Verdict == VerdictUnsafe {
		return VerdictUnsafe
	}
	return measured
}

// scaleWarning is the sentence a reader needs when the twin said SAFE and the
// operator's real table says otherwise. Empty in every other case, including
// when both agree on UNSAFE (the measured note already covers it).
func scaleWarning(measured string, p ScaleProjection) string {
	if measured != VerdictSafe || p.Verdict != VerdictUnsafe {
		return ""
	}
	measuredText := fmt.Sprintf("%.1fs", p.MeasuredBlockedSeconds)
	projectedText := fmt.Sprintf("~%.0fs", p.ProjectedBlockedSeconds)
	if p.MeasuredAtFloor {
		measuredText = fmt.Sprintf("under %.2fs — below what 250ms sampling can resolve", scaleFloorSeconds)
		projectedText = fmt.Sprintf("at most ~%.0fs", p.ProjectedBlockedSeconds)
	}
	return fmt.Sprintf(
		"SAFE on this %s-row twin (%s), but your %s-row table is %s larger: expect %s of blocked writes. "+
			"The model is %s. Rehearse against a larger sample before shipping.",
		commas(p.TwinRows), measuredText, commas(p.TargetRows), ratioText(p.TwinRows, p.TargetRows),
		projectedText, p.Model)
}

// ratioText renders the size multiple the way a person would say it.
func ratioText(twinRows, targetRows int64) string {
	r := float64(targetRows) / float64(twinRows)
	if r < 10 {
		return fmt.Sprintf("%.1fx", r)
	}
	return fmt.Sprintf("%.0fx", r)
}

// commas groups a row count with thousands separators. Ten million rows and one
// million rows are one character apart otherwise, and this number is the entire
// point of the sentence it appears in.
func commas(n int64) string {
	s := fmt.Sprintf("%d", n)
	neg := ""
	if len(s) > 0 && s[0] == '-' {
		neg, s = "-", s[1:]
	}
	var out []byte
	for i, c := range []byte(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, c)
	}
	return neg + string(out)
}
