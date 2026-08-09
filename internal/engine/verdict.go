package engine

// The verdict arithmetic, kept pure and separate so it can be tested against
// synthetic timelines rather than against a database.
//
// The primary measurement of "the migration froze the application" is the one
// the application would make: the write rate collapsed. Not "a lock was held" —
// plenty of locks are held harmlessly — and not "pg_stat_activity showed
// waiters", which is corroboration from the server's point of view. What ships
// as the headline is what a user would have felt.

const (
	// collapseFraction is the share of baseline throughput below which we call
	// the application frozen. 15% is chosen to be unambiguous: normal jitter on
	// this rig is a few percent, an index build that competes for I/O costs
	// maybe 5-10%, and a table that is actually lock-blocked goes to ~0. A
	// sample under 15% of baseline is not a slow database, it is a stopped one.
	collapseFraction = 0.15

	// unsafeThresholdSeconds is where SAFE becomes UNSAFE. A sub-second freeze
	// is the cost of taking any lock at all on a busy table and is survivable;
	// anything past a second is an outage that a user, a health check and a
	// pager all notice.
	unsafeThresholdSeconds = 1.0

	// VerdictSafe / VerdictUnsafe are the two measured verdicts.
	VerdictSafe   = "SAFE"
	VerdictUnsafe = "UNSAFE"
	// VerdictInconclusive is the terminal state for a run the max-duration guard
	// had to abort: the migration could not complete its work (typically because
	// it never acquired its lock) inside the run budget. It is NOT a measurement
	// of "safe" or "unsafe" — it is the finding that the change cannot finish
	// under live traffic, which in production is itself a red flag.
	VerdictInconclusive = "INCONCLUSIVE"

	// QualifierAcquisitionNotSimulated marks a SAFE that is only SAFE about half
	// the question. The statement takes AccessExclusiveLock — which conflicts
	// with everything, including SELECT — but on an idle-ish twin it got that
	// lock instantly and held it for microseconds, so nothing was measurably
	// blocked. What decides the real freeze is how long the migration WAITS for
	// the lock behind the longest transaction already on the table, and a default
	// run does not simulate one. See hostile mode in loadgen/contend.go.
	QualifierAcquisitionNotSimulated = "acquisition_not_simulated"

	// QualifierContentionNotEngaged marks a SAFE hostile run where contention was
	// REQUESTED but never actually exercised the acquisition wait: either no hold
	// was held within the wait budget, or the migration's lock level did not
	// conflict with the hold so the DDL never had to queue. The numbers are a
	// default (uncontended) run's, and reading them as "tested under contention
	// and passed" would be the false green light hostile mode exists to prevent.
	QualifierContentionNotEngaged = "contention_not_engaged"

	// QualifierModeledBeyondMeasurement marks a scale PROJECTION whose target/twin
	// ratio exceeds highExtrapolationRatio (~20x). It is a property of the
	// projection (it lives on ScaleProjection.Qualifier), not of the run: past that
	// ratio a single measurement scaled linearly is an order-of-magnitude estimate,
	// and because real index-build and table-rewrite cost is super-linear the
	// projected freeze is a LOWER bound. It never softens a verdict — the
	// EffectiveVerdict escalation still stands; it only says "treat the projected
	// number as a floor, not a point estimate".
	QualifierModeledBeyondMeasurement = "modeled_beyond_measurement"

	// FailureModeWritesErrored says the write rate collapsed because the traffic
	// statements themselves started failing — not because a lock blocked them.
	// The canonical case is a rename: after ALTER TABLE orders RENAME TO ...,
	// every INSERT INTO orders fails 42P01 and the write rate goes to zero while
	// no lock is held by anybody. Narrating that as a lock outage is wrong and,
	// worse, hides the real finding.
	FailureModeWritesErrored = "writes_errored"

	// VerdictErrored is for a migration Postgres refused to run at all (bad
	// syntax, permission denied, non-IMMUTABLE index expression, ...). Such a
	// run measures nothing: it never held a lock, so blocked_seconds is
	// trivially 0 — and calling that "SAFE" would be the single most misleading
	// thing this tool could say. A migration that cannot execute is not a safe
	// migration; it is a migration that needs fixing before it can be judged.
	VerdictErrored = "ERRORED"
)

// RatePoint is one sample of the run's timeline. The API replays these to draw
// the scope trace.
type RatePoint struct {
	// T is seconds since load started. The DDL window is delimited by
	// RunResult.DDLStartT and DDLEndT, so a point before DDLStartT is baseline.
	T float64 `json:"t"`
	// WPS is the DERIVED write rate for the interval ending at T: committed
	// writes in that interval divided by its length. Derived rather than taken
	// from loadgen's own WritesPerSec because that counter closes a window once
	// a second, which is four times coarser than we sample and would smear a
	// freeze across the seconds either side of it.
	WPS int64 `json:"wps"`
	// Casualties is the cumulative count of writes killed waiting for a lock.
	Casualties int64 `json:"casualties"`
}

// longestBlockedWindow returns the length in seconds of the longest contiguous
// freeze, given the run's baseline write rate.
//
// Contiguous, not cumulative: two separate 0.6s dips are not a 1.2s outage, and
// reporting them as one would be the kind of exaggeration that makes the whole
// number untrustworthy. Each sample accounts for the interval since the
// previous sample, so the first sample — which bounds no interval — is skipped.
//
// The subtlety is the EDGES. A freeze almost never begins on a sample boundary,
// so the interval it starts in carries part of a second's worth of writes and
// lands above the collapse threshold — as does the interval the freeze ends in.
// Counting whole sub-threshold intervals only, as this did originally, discards
// both and under-reports every freeze by up to two sample intervals (500ms at
// the default cadence): a genuine 1.2s outage measured 0.75s and shipped SAFE.
//
// Those two edge intervals are attributed proportionally instead. Within one
// interval the writers are either flowing at roughly baseline or stopped dead,
// so an observed rate of r over an interval of length dt means it was frozen for
// dt * (1 - r/baseline). That estimate is applied ONLY to the interval either
// side of a sub-threshold run — never to ordinary intervals, where the same
// arithmetic would turn normal jitter into fictional freeze time.
func longestBlockedWindow(points []RatePoint, baselineWPS float64) float64 {
	if baselineWPS <= 0 || len(points) < 2 {
		return 0
	}
	threshold := baselineWPS * collapseFraction
	below := func(i int) bool {
		return points[i].T-points[i-1].T > 0 && float64(points[i].WPS) < threshold
	}

	var best float64
	for i := 1; i < len(points); {
		if !below(i) {
			i++
			continue
		}
		run := edgeShare(points, i-1, baselineWPS)
		for ; i < len(points) && below(i); i++ {
			run += points[i].T - points[i-1].T
		}
		if i < len(points) {
			run += edgeShare(points, i, baselineWPS)
		}
		if run > best {
			best = run
		}
	}
	return best
}

// edgeShare estimates how much of the interval ending at points[k] was frozen,
// from how far its averaged rate fell short of baseline. Zero for the interval
// before the first sample, which has no length to divide.
func edgeShare(points []RatePoint, k int, baselineWPS float64) float64 {
	if k < 1 || k >= len(points) {
		return 0
	}
	dt := points[k].T - points[k-1].T
	if dt <= 0 {
		return 0
	}
	frac := 1 - float64(points[k].WPS)/baselineWPS
	if frac <= 0 {
		return 0
	}
	if frac > 1 {
		frac = 1
	}
	return dt * frac
}

// blockedSecondsFrom computes the freeze window over the migration's own
// timeline. Points before the DDL started are baseline and are excluded: a dip
// while the load generator was still spinning up is not the migration's doing.
func blockedSecondsFrom(points []RatePoint, ddlStartT, baselineWPS float64) float64 {
	if baselineWPS <= 0 {
		// With no baseline there is nothing to be below, so there is no honest
		// way to call anything a collapse. Report zero and let the monitor's
		// corroboration and the casualty count speak instead.
		return 0
	}
	window := make([]RatePoint, 0, len(points))
	for i, p := range points {
		if p.T >= ddlStartT {
			// Include the sample immediately before the DDL so the first
			// in-window sample has an interval to measure against.
			if len(window) == 0 && i > 0 {
				window = append(window, points[i-1])
			}
			window = append(window, p)
		}
	}
	return roundSeconds(longestBlockedWindow(window, baselineWPS))
}

// verdictFor is the single place SAFE and UNSAFE are decided.
//
// Two independent grounds for UNSAFE, because either one alone can miss a real
// outage:
//
//   - The freeze exceeded the threshold. This is the headline measurement.
//   - Real writes DIED. A write that waited past its lock_timeout and was
//     killed is not a statistic about the twin; it is the same event that, in
//     production, is a customer seeing an error. A statement that kills writes
//     cannot be SAFE no matter how the freeze rounds — and a freeze landing a
//     hair under the threshold (1.000s against a 1s bar) is exactly where a
//     duration-only rule would call a genuine outage safe.
func verdictFor(blockedSeconds float64, casualties int64) string {
	if blockedSeconds > unsafeThresholdSeconds || casualties > 0 {
		return VerdictUnsafe
	}
	return VerdictSafe
}

// verdictForObserved is verdictFor with the monitor's observed lock folded in as
// the arbiter of whether a write-rate collapse was a LOCK freeze at all.
//
// The write-rate `blocked_seconds` is the primary signal that "the application
// froze", but a collapsing write rate has two possible causes, and only one of
// them is an outage the migration is responsible for:
//
//   - the migration held a lock that conflicts with writes (SHARE, SHARE ROW
//     EXCLUSIVE, EXCLUSIVE, ACCESS EXCLUSIVE), so every INSERT/UPDATE/DELETE
//     queued behind it — a real freeze; or
//   - the migration held only a NON-write-blocking lock (ACCESS SHARE, ROW
//     SHARE, ROW EXCLUSIVE, SHARE UPDATE EXCLUSIVE), and the dip is I/O or CPU
//     contention from the work competing for the disk on a shared box. CREATE
//     INDEX CONCURRENTLY's two heap scans are the canonical case: they cost
//     throughput without ever blocking a writer.
//
// When no write-blocking lock was held, the write-rate dip is not a freeze and
// must not flip the verdict to UNSAFE — a tool that cries UNSAFE at a genuinely
// safe migration on measurement noise is broken. Real casualties (writes killed
// waiting for a lock) are still honoured: they are dead writes, not a rate dip,
// and cannot happen without something to wait for — so if any are counted they
// remain grounds for UNSAFE.
func verdictForObserved(blockedSeconds float64, casualties int64, heldWriteBlockingLock bool) string {
	if !heldWriteBlockingLock {
		// The dip, however deep, was not a lock freeze. Only dead writes can make
		// this run UNSAFE now.
		return verdictFor(0, casualties)
	}
	return verdictFor(blockedSeconds, casualties)
}

// roundSeconds trims to milliseconds. The extra digits are measurement noise
// and printing them implies a precision we do not have.
func roundSeconds(v float64) float64 {
	return float64(int64(v*1000+0.5)) / 1000
}
