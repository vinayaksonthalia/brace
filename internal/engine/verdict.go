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
// stretch of samples whose rate is below threshold.
//
// Contiguous, not cumulative: two separate 0.6s dips are not a 1.2s outage, and
// reporting them as one would be the kind of exaggeration that makes the whole
// number untrustworthy. Each sample accounts for the interval since the
// previous sample, so the first sample — which bounds no interval — is skipped.
func longestBlockedWindow(points []RatePoint, threshold float64) float64 {
	if threshold <= 0 || len(points) < 2 {
		return 0
	}
	var best, cur float64
	for i := 1; i < len(points); i++ {
		dt := points[i].T - points[i-1].T
		if dt <= 0 {
			continue
		}
		if float64(points[i].WPS) < threshold {
			cur += dt
			if cur > best {
				best = cur
			}
		} else {
			cur = 0
		}
	}
	return best
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
	return roundSeconds(longestBlockedWindow(window, baselineWPS*collapseFraction))
}

// verdictFor is the single place SAFE and UNSAFE are decided.
func verdictFor(blockedSeconds float64) string {
	if blockedSeconds > unsafeThresholdSeconds {
		return VerdictUnsafe
	}
	return VerdictSafe
}

// roundSeconds trims to milliseconds. The extra digits are measurement noise
// and printing them implies a precision we do not have.
func roundSeconds(v float64) float64 {
	return float64(int64(v*1000+0.5)) / 1000
}
