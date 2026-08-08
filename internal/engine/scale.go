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

// Where TargetRows came from. It is published because the three cases carry
// very different weight: a number the caller sent for THIS run is a fact about
// their table, a number the deployment was configured with is an operator's
// standing statement, and the built-in default is Brace guessing on behalf of
// somebody who has not told us anything. The last one must never read as
// measured, or as knowledge about the reader's database.
const (
	// TargetSourceRequest: target_rows on the run request.
	TargetSourceRequest = "request"
	// TargetSourceDeployment: the operator's BRACE_TARGET_ROWS.
	TargetSourceDeployment = "deployment"
	// TargetSourceAssumed: nobody said, so the public lab's stock assumption was
	// used to make the false-safe defence work by default.
	TargetSourceAssumed = "brace_default_assumption"
)

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
	// TargetRowsSource says who supplied TargetRows — see TargetSource*. Empty
	// when there is no target at all.
	TargetRowsSource string `json:"target_rows_source,omitempty"`
	// TargetRowsAssumed is true when nobody supplied a size and Brace used its
	// stock assumption. Every rendering of this projection has to say so: it is
	// an assumption ABOUT THE READER'S TABLE, and presenting it as anything
	// firmer would be inventing a fact.
	TargetRowsAssumed bool `json:"target_rows_assumed"`
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
	// ScaleUncertain is the teach layer's flag that ScalesWithRows is a
	// can't-rule-it-out default, not a known fact (the ALTER TABLE / generic
	// catch-alls). For these we may project a MEASURED freeze above the floor, but
	// must not manufacture one by projecting the resolution floor itself — that
	// would be a false-UNSAFE on a statement that did no measurable work.
	ScaleUncertain bool
	// HeldWriteBlockingLock is the MONITOR's empirical answer to "did the
	// migration actually hold a lock that conflicts with writes on a pre-existing
	// relation?". It is what turns a measured write-rate dip into a scalable
	// write-freeze: a dip under a NON-write-blocking lock (CREATE INDEX
	// CONCURRENTLY's SHARE UPDATE EXCLUSIVE, VACUUM) is I/O/CPU contention on a
	// shared box, not a freeze, so scaling it onto a bigger table would
	// manufacture a phantom outage the lock physics cannot produce. Only the
	// KNOWN-blocking scaling shapes (BlocksWrites && !ScaleUncertain — CREATE
	// INDEX, SET NOT NULL, ALTER COLUMN TYPE) project from a floor measurement
	// without needing this corroboration; every other shape's measured dip must be
	// backed by an observed write-blocking lock before it is projected.
	HeldWriteBlockingLock bool
	// Verdict is the measured verdict, used to refuse projecting from a run that
	// measured nothing.
	Verdict string
	// TargetSource is one of the TargetSource* constants.
	TargetSource string
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
	if p.TargetRows > 0 {
		p.TargetRowsSource = in.TargetSource
		p.TargetRowsAssumed = in.TargetSource == TargetSourceAssumed
	}
	measured := in.Measured
	if in.TwinRows <= 0 || p.TargetRows <= 0 {
		return p
	}
	if !in.ScalesWithRows {
		// Catalog-only work is the same microsecond at any size; scaling it invents
		// an outage that cannot physically happen.
		return p
	}
	// A projection needs a legitimate base to scale. Two independent sources
	// qualify:
	//   - the shape is KNOWN to do O(rows) work under a write-blocking lock, so
	//     even a floor measurement is a defensible upper bound (CREATE INDEX, SET
	//     NOT NULL, ALTER COLUMN TYPE); or
	//   - the rehearsal MEASURED a freeze above the resolution floor AND the
	//     monitor corroborated it with a write-blocking lock actually held. The
	//     measurement outranks the shape prediction — a shape that took a stronger
	//     lock than predicted, or a catch-all whose scaling we could not predict,
	//     is projected — but a write-rate dip ON ITS OWN is not a freeze. Under a
	//     NON-write-blocking lock (CREATE INDEX CONCURRENTLY's SHARE UPDATE
	//     EXCLUSIVE, VACUUM) the dip is I/O/CPU contention on a shared box, and
	//     scaling it would invent a phantom outage: the same non-blocking lock is
	//     merely held for longer on a bigger table, freezing nobody at any size.
	// A merely-uncertain scaling shape (ClassAlterTableOther / generic) that
	// measured nothing above the floor gets NO projection: manufacturing a freeze
	// from the 250ms floor there would be a false-UNSAFE on a statement that did no
	// measurable work.
	measuredFreeze := measured > scaleFloorSeconds
	scalableMeasuredFreeze := measuredFreeze && in.HeldWriteBlockingLock
	knownBlockingScale := in.BlocksWrites && !in.ScaleUncertain
	if !knownBlockingScale && !scalableMeasuredFreeze {
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

// scaleProjectionFor is projectScale gated on the run having actually produced a
// measurement. A run that ERRORED (the statement never executed — a bad column,
// a permission error) or came back INCONCLUSIVE (the guard aborted it before it
// finished) measured nothing, so there is no freeze to scale. Emitting a
// projection struct populated with twin/target sizes and a model name for a
// statement that never ran would dress a non-run as a measurement, which is
// exactly the kind of measured-sounding output an errored run must not carry. For
// those terminal states the projection is empty (Known() == false).
func scaleProjectionFor(verdict string, in scaleInputs) ScaleProjection {
	if verdict != VerdictSafe && verdict != VerdictUnsafe {
		return ScaleProjection{}
	}
	// This is the single entry point, so make the verdict it was given
	// authoritative for the projection too — the two can never disagree.
	in.Verdict = verdict
	return projectScale(in)
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
	target := fmt.Sprintf("your %s-row table", commas(p.TargetRows))
	tail := "Rehearse against a larger sample before shipping."
	if p.TargetRowsAssumed {
		// Nobody told us how big their table is. The projection is still the
		// useful thing to show — but it is arithmetic on an assumption, and
		// saying "your table" about a number Brace invented would be a lie.
		target = fmt.Sprintf("a %s-row table (ASSUMED — nobody told Brace your real size)", commas(p.TargetRows))
		tail = "Pass target_rows (or set BRACE_TARGET_ROWS) to project onto your actual table instead of this assumption."
	}
	return fmt.Sprintf(
		"SAFE on this %s-row twin (%s), but %s is %s larger: expect %s of blocked writes. "+
			"The model is %s. %s",
		commas(p.TwinRows), measuredText, target, ratioText(p.TwinRows, p.TargetRows),
		projectedText, p.Model, tail)
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
