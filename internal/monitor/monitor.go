// Package monitor watches a twin database from the inside while a migration
// runs against it, and reports two things the load generator cannot see:
//
//   - how many live writers were parked in a lock wait, and for how long;
//   - which lock modes the migration itself actually held.
//
// It is deliberately CORROBORATION, not evidence. The primary signal for "the
// migration froze the application" is the load generator's write rate
// collapsing — that is what a user would experience, measured the way a user
// would experience it. This package explains WHY the rate collapsed by naming
// the lock. If every query in here fails, a run must still produce a verdict;
// so every failure is counted, logged, and swallowed. The monitor never returns
// an error into the run path after Start.
package monitor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	// DefaultInterval is the poll cadence. 250ms is fast enough that a
	// one-second freeze is four samples wide, and slow enough that the polling
	// itself is invisible next to 24 writer connections.
	DefaultInterval = 250 * time.Millisecond

	// DefaultAppName is the application_name the migration connection sets. It
	// is how we pick the migration's backend out of pg_stat_activity without
	// having to plumb a PID around.
	DefaultAppName = "brace-migration"

	// monitorAppName labels our own backend so it is easy to spot in psql and
	// so we can exclude ourselves from the blocked-writer count.
	monitorAppName = "brace-monitor"

	// connectTimeout bounds the one connect we do at Start. If the twin will
	// not take a connection promptly the run proceeds without corroboration
	// rather than stalling.
	connectTimeout = 5 * time.Second

	// maxSampleGap caps how much wall-clock a single sample may attribute to
	// the blocked state. Without it, a GC pause or a laptop suspend between two
	// samples would be booked as seconds of freeze that nobody observed.
	maxSampleGapFactor = 5
)

// lockStrength ranks PostgreSQL's table-level lock modes weakest to strongest,
// using the exact spellings pg_locks.mode reports. Used to pick the strongest
// mode the migration held, which is the one that decides who got blocked.
var lockStrength = map[string]int{
	"AccessShareLock":          1,
	"RowShareLock":             2,
	"RowExclusiveLock":         3,
	"ShareUpdateExclusiveLock": 4,
	"ShareLock":                5,
	"ShareRowExclusiveLock":    6,
	"ExclusiveLock":            7,
	"AccessExclusiveLock":      8,
}

// Report is what one run's observation adds up to.
//
// It is a report ABOUT a monitored window, so zero values are meaningful:
// BlockedSeconds == 0 with Samples > 0 is a positive statement that nothing was
// ever seen waiting. BlockedSeconds == 0 with Samples == 0 says only that we
// never got to look.
type Report struct {
	// BlockedPeakCount is the highest number of live writers seen waiting on a
	// lock in a single sample.
	BlockedPeakCount int `json:"blocked_peak_count"`
	// BlockedSeconds is wall-clock time across samples where at least one
	// writer was waiting on a lock.
	BlockedSeconds float64 `json:"blocked_seconds"`
	// LockModes are the distinct relation-level lock modes the migration held,
	// strongest first.
	LockModes []string `json:"lock_modes"`
	// StrongestLockMode is LockModes[0], hoisted out because it is the single
	// fact that explains who was blocked.
	StrongestLockMode string `json:"strongest_lock_mode,omitempty"`
	// MigrationLockWaited is true if the migration's own backend was ever seen
	// parked in a lock wait — it had to QUEUE for its lock behind another session.
	// It is the server-side proof that a hostile hold actually engaged: a shape
	// whose lock does not conflict with the hold acquires instantly and is never
	// caught here. Unlike BlockedPeakCount this counts the migration itself, which
	// the blocked-writer count deliberately excludes.
	MigrationLockWaited bool `json:"migration_lock_waited"`
	// MigrationLockModeRequested is the strongest relation-level lock mode the
	// migration's OWN backend was seen holding OR waiting for on a pre-existing
	// relation, regardless of grant status. LockModes above is granted-only — the
	// locks that actually blocked somebody — so a run that spent its whole window
	// QUEUED for an ungranted lock (a hostile ADD COLUMN behind a held transaction),
	// or whose sub-250ms granted window the sampler missed, reports LockModes empty
	// even though a lock was central to the freeze. This field catches that case: it
	// is evidence of the migration's lock INTENT, not proof of a held lock, and must
	// never be folded into LockModes. Empty when nothing was seen.
	MigrationLockModeRequested string `json:"migration_lock_mode_requested,omitempty"`
	// Samples is how many polls completed successfully.
	Samples int `json:"samples"`
	// FailedSamples is how many polls failed. Non-zero means the numbers above
	// are a floor, not a measurement, and the caller should say so.
	FailedSamples int `json:"failed_samples"`
	// LastError is the most recent failure, for the operator. Never surfaced as
	// a run failure.
	LastError string `json:"last_error,omitempty"`
}

// MonitorReport is an alias for Report. The type is named Report because it is
// already package-qualified as monitor.Report; the longer spelling exists for
// callers that prefer to be explicit.
type MonitorReport = Report

// Degraded reports whether any poll failed, i.e. whether the numbers are a
// floor rather than a measurement.
func (r Report) Degraded() bool { return r.FailedSamples > 0 }

// Config configures a Monitor.
type Config struct {
	// DSN points at the twin being watched. It should be an admin/superuser
	// DSN: pg_stat_activity hides other users' query text and, for
	// non-superusers, some rows entirely.
	DSN string
	// AppName is the migration connection's application_name.
	// Defaults to DefaultAppName.
	AppName string
	// Interval is the poll cadence. Defaults to DefaultInterval.
	Interval time.Duration
	// Logger receives failures. Defaults to slog.Default().
	Logger *slog.Logger
}

func (c *Config) applyDefaults() {
	if c.AppName == "" {
		c.AppName = DefaultAppName
	}
	if c.Interval <= 0 {
		c.Interval = DefaultInterval
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// blockedTracker accumulates "time spent with somebody blocked" from a series
// of point samples. Split out from Monitor so the arithmetic — which is what
// the headline BlockedSeconds is made of — can be tested without a database.
type blockedTracker struct {
	interval time.Duration
	last     time.Time
	blocked  time.Duration
	peak     int
}

// observe books the interval that just elapsed. A sample that finds somebody
// waiting is taken to mean they were waiting for the whole gap since the
// previous sample — the same rectangle rule any sampling profiler uses — with
// the gap capped so a scheduling stall cannot manufacture freeze time.
func (t *blockedTracker) observe(now time.Time, blocked int) {
	if blocked > t.peak {
		t.peak = blocked
	}
	if t.last.IsZero() {
		t.last = now
		return
	}
	d := now.Sub(t.last)
	t.last = now
	if d < 0 {
		return // a clock that went backwards buys no freeze time
	}
	if max := time.Duration(maxSampleGapFactor) * t.interval; d > max {
		d = max
	}
	if blocked > 0 {
		t.blocked += d
	}
}

// Monitor polls one twin for the duration of one run. Single-use.
type Monitor struct {
	cfg Config
	log *slog.Logger

	mu      sync.Mutex
	conn    *pgx.Conn
	started bool
	stopped bool
	cancel  context.CancelFunc
	done    chan struct{}

	// preexisting holds the OIDs of every relation that existed when the run
	// began. Locks are only reported for these: a lock on a relation the
	// migration created during the run (a new index, its TOAST table) blocks
	// nobody, while a lock on a pre-existing relation is exactly what does.
	preexisting []uint32

	tracker   blockedTracker
	modes     map[string]struct{}
	migWaited bool
	reqMode   string
	samples   int
	failed    int
	lastErr   error
}

// New builds an unstarted Monitor.
func New(cfg Config) *Monitor {
	cfg.applyDefaults()
	return &Monitor{
		cfg:     cfg,
		log:     cfg.Logger.With("component", "monitor"),
		modes:   make(map[string]struct{}),
		tracker: blockedTracker{interval: cfg.Interval},
	}
}

// Start opens the monitor's own connection and begins polling.
//
// An error here means "no corroboration this run" — the caller should log it
// and carry on. StopAndReport remains safe to call either way, and will report
// zero samples plus the failure.
func (m *Monitor) Start(ctx context.Context) error {
	m.mu.Lock()
	if m.started || m.stopped {
		m.mu.Unlock()
		return errors.New("monitor: Start called more than once")
	}
	m.started = true
	m.mu.Unlock()

	if strings.TrimSpace(m.cfg.DSN) == "" {
		return m.fail(errors.New("monitor: Config.DSN is required"))
	}

	connCfg, err := pgx.ParseConfig(m.cfg.DSN)
	if err != nil {
		return m.fail(fmt.Errorf("monitor: parse DSN: %w", err))
	}
	connCfg.RuntimeParams["application_name"] = monitorAppName
	// The monitor only reads catalogs, which no user-table lock can block. The
	// timeouts are belt-and-braces so that a pathological server can never turn
	// an observer into a participant that stalls the run.
	connCfg.RuntimeParams["statement_timeout"] = "1000"
	connCfg.RuntimeParams["lock_timeout"] = "1000"

	connCtx, cancelConn := context.WithTimeout(ctx, connectTimeout)
	conn, err := pgx.ConnectConfig(connCtx, connCfg)
	cancelConn()
	if err != nil {
		return m.fail(fmt.Errorf("monitor: connect: %w", err))
	}

	// Snapshot the relations that exist right now. Anything the migration
	// creates after this point (a new index, its TOAST table) is excluded from
	// the reported lock modes, because a lock on an uncommitted new relation
	// blocks nobody. See queryLockModes.
	pre, preErr := snapshotRelations(ctx, conn)
	if preErr != nil {
		m.log.Warn("monitor.relation_snapshot_failed", "err", preErr)
	}

	pollCtx, cancel := context.WithCancel(ctx)
	m.mu.Lock()
	m.preexisting = pre
	m.conn = conn
	m.cancel = cancel
	m.done = make(chan struct{})
	m.tracker.last = time.Now()
	m.mu.Unlock()

	go m.loop(pollCtx)
	return nil
}

// fail records a startup failure so it shows up in the Report, and returns it.
func (m *Monitor) fail(err error) error {
	m.mu.Lock()
	m.failed++
	m.lastErr = err
	m.mu.Unlock()
	m.log.Warn("monitor.degraded", "err", err)
	return err
}

func (m *Monitor) loop(ctx context.Context) {
	defer close(m.done)
	t := time.NewTicker(m.cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.sample(ctx)
		}
	}
}

func (m *Monitor) sample(ctx context.Context) {
	// Bound each poll to under two intervals: a poll that outlives its own
	// cadence is a poll that has stopped being a sample.
	qctx, cancel := context.WithTimeout(ctx, 2*m.cfg.Interval)
	defer cancel()

	blocked, err := m.queryBlocked(qctx)
	if err != nil {
		m.record(err)
		return
	}
	// The blocked count is the sample's spine. The two enrichment queries below —
	// the migration's held lock modes and whether it is itself waiting for a lock
	// — each contribute independently and each is allowed to fail without
	// discarding the blocked count already in hand.
	modes, modesErr := m.queryLockModes(qctx)
	migWaiting, waitErr := m.queryMigrationWaiting(qctx)
	reqMode, reqErr := m.queryMigrationLockMode(qctx)

	m.mu.Lock()
	m.tracker.observe(time.Now(), blocked)
	for _, mode := range modes {
		m.modes[mode] = struct{}{}
	}
	if migWaiting {
		m.migWaited = true
	}
	if reqMode != "" && Strength(reqMode) > Strength(m.reqMode) {
		// Keep the strongest lock the migration's backend was ever seen holding or
		// waiting for. Strength(unknown)==0, so an unrecognised spelling only wins
		// over "nothing seen yet".
		m.reqMode = reqMode
	}
	if modesErr == nil && waitErr == nil {
		// A poll counts as a completed sample only when the whole poll succeeded,
		// so FailedSamples stays an honest measure of how much we could not see.
		m.samples++
	}
	m.mu.Unlock()

	if modesErr != nil {
		m.record(modesErr)
	}
	if waitErr != nil {
		m.record(waitErr)
	}
	if reqErr != nil {
		m.record(reqErr)
	}
}

func (m *Monitor) record(err error) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		// Our own shutdown, or a poll we abandoned on purpose. Not a fault.
		return
	}
	m.mu.Lock()
	m.failed++
	m.lastErr = err
	m.mu.Unlock()
	m.log.Debug("monitor.sample_failed", "err", err)
}

// queryBlocked counts live sessions parked in a lock wait.
//
// Two exclusions on top of the obvious predicate, both deliberate: our own
// backend can never be the thing we are measuring, and the MIGRATION itself
// frequently waits for a lock (a naive CREATE INDEX queues behind in-flight
// writers). Counting the migration would make "blocked writers" read 1 during
// the exact window where the truthful answer is 0.
func (m *Monitor) queryBlocked(ctx context.Context) (int, error) {
	var n int
	err := m.conn.QueryRow(ctx, `
		SELECT count(*) FROM pg_stat_activity
		WHERE datname = current_database()
		  AND wait_event_type = 'Lock'
		  AND state = 'active'
		  AND pid <> pg_backend_pid()
		  AND coalesce(application_name, '') <> $1`, m.cfg.AppName).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("monitor: blocked count: %w", err)
	}
	return n, nil
}

// queryMigrationWaiting reports whether the migration's OWN backend is, right
// now, parked waiting for a lock. queryBlocked deliberately excludes the
// migration from the writer count; this query is the opposite — it looks for
// exactly the migration, because its wait is the evidence a hostile hold
// engaged: the DDL had to queue behind the contention transaction to get its
// lock. A migration whose lock does not conflict with the hold acquires at once
// and never appears here.
func (m *Monitor) queryMigrationWaiting(ctx context.Context) (bool, error) {
	var waiting bool
	err := m.conn.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_stat_activity
			WHERE datname = current_database()
			  AND application_name = $1
			  AND wait_event_type = 'Lock'
			  AND state = 'active'
			  AND pid <> pg_backend_pid())`, m.cfg.AppName).Scan(&waiting)
	if err != nil {
		return false, fmt.Errorf("monitor: migration wait: %w", err)
	}
	return waiting, nil
}

// queryLockModes returns the relation-level lock modes the migration currently
// HOLDS on relations that existed before it started — i.e. the locks that can
// actually block another session.
//
// Two exclusions matter, and both were wrong before:
//
//   - Ungranted rows: a lock the migration is still queued for has hurt nobody.
//   - Locks on relations the migration itself CREATED during this run.
//     Postgres takes AccessExclusiveLock on every new relation's own OID (the
//     index being built, its TOAST table), which blocks nobody — nothing else
//     can reference an uncommitted relation. Reporting it made a plain
//     CREATE INDEX read as "AccessExclusiveLock", contradicting both the lesson
//     text and Postgres itself, which takes only ShareLock on the TABLE.
//
// The exclusion is by OID, not by age: a table REWRITE (ALTER COLUMN TYPE,
// volatile DEFAULT) keeps the original relation and its AccessExclusiveLock —
// which really does block writers and must still be reported. Only OIDs that
// did not exist when the run began are dropped.
func (m *Monitor) queryLockModes(ctx context.Context) ([]string, error) {
	rows, err := m.conn.Query(ctx, `
		SELECT DISTINCT l.mode
		FROM pg_locks l
		JOIN pg_stat_activity a ON a.pid = l.pid
		WHERE a.datname = current_database()
		  AND a.application_name = $1
		  AND l.granted
		  AND l.locktype = 'relation'
		  AND l.relation = ANY($2::oid[])`, m.cfg.AppName, m.preexisting)
	if err != nil {
		return nil, fmt.Errorf("monitor: lock modes: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var mode string
		if err := rows.Scan(&mode); err != nil {
			return nil, fmt.Errorf("monitor: scan lock mode: %w", err)
		}
		out = append(out, mode)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("monitor: lock modes: %w", err)
	}
	return out, nil
}

// queryMigrationLockMode returns the strongest relation-level lock mode the
// migration's OWN backend currently holds OR is waiting for on a pre-existing
// relation, IGNORING grant status. It is the deliberate complement to
// queryLockModes (granted-only): the wait phase of a hostile run, and the tiny
// granted window of a sub-250ms metadata change, are exactly what queryLockModes
// excludes, which leaves LockModes empty on runs where a lock was nonetheless the
// whole story. This names that lock as INTENT, never as a held lock. New relations
// the migration created are excluded by the same pre-existing-OID filter.
func (m *Monitor) queryMigrationLockMode(ctx context.Context) (string, error) {
	rows, err := m.conn.Query(ctx, `
		SELECT l.mode
		FROM pg_locks l
		JOIN pg_stat_activity a ON a.pid = l.pid
		WHERE a.datname = current_database()
		  AND a.application_name = $1
		  AND l.locktype = 'relation'
		  AND l.relation = ANY($2::oid[])`, m.cfg.AppName, m.preexisting)
	if err != nil {
		return "", fmt.Errorf("monitor: migration lock mode: %w", err)
	}
	defer rows.Close()
	var strongest string
	for rows.Next() {
		var mode string
		if err := rows.Scan(&mode); err != nil {
			return "", fmt.Errorf("monitor: scan migration lock mode: %w", err)
		}
		if Strength(mode) > Strength(strongest) {
			strongest = mode
		}
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("monitor: migration lock mode: %w", err)
	}
	return strongest, nil
}

// StopAndReport halts polling, closes the connection, and returns what was
// seen. Idempotent; safe on a Monitor whose Start failed or was never called.
func (m *Monitor) StopAndReport() Report {
	m.mu.Lock()
	if !m.stopped {
		m.stopped = true
		cancel := m.cancel
		m.mu.Unlock()
		if cancel != nil {
			cancel()
			<-m.done
		}
		m.mu.Lock()
		if m.conn != nil {
			// A fresh context: the run's context is usually already cancelled
			// by the time we tear down, and Close would then leak the socket.
			ctx, c := context.WithTimeout(context.Background(), 2*time.Second)
			_ = m.conn.Close(ctx)
			c()
			m.conn = nil
		}
	}
	defer m.mu.Unlock()

	rep := Report{
		BlockedPeakCount:           m.tracker.peak,
		BlockedSeconds:             round1ms(m.tracker.blocked),
		MigrationLockWaited:        m.migWaited,
		MigrationLockModeRequested: m.reqMode,
		Samples:                    m.samples,
		FailedSamples:              m.failed,
		LockModes:                  sortModes(m.modes),
	}
	if len(rep.LockModes) > 0 {
		rep.StrongestLockMode = rep.LockModes[0]
	}
	if m.lastErr != nil {
		rep.LastError = m.lastErr.Error()
	}
	return rep
}

// sortModes orders the observed modes strongest first, so LockModes[0] is
// always the mode that decided who waited. Unknown spellings (a future
// PostgreSQL, or a lock type we did not anticipate) sort last but are never
// dropped — an unrecognised lock is still evidence.
func sortModes(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for m := range set {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool {
		si, sj := lockStrength[out[i]], lockStrength[out[j]]
		if si != sj {
			return si > sj
		}
		return out[i] < out[j]
	})
	return out
}

// Strength ranks a pg_locks mode string. 0 means unrecognised. Exported because
// the orchestrator merges monitor modes with its own expectations.
func Strength(mode string) int { return lockStrength[mode] }

// Strongest returns the strongest of the given pg_locks mode strings.
func Strongest(modes []string) string {
	best, bestN := "", -1
	for _, m := range modes {
		if n := lockStrength[m]; n > bestN {
			best, bestN = m, n
		}
	}
	return best
}

func round1ms(d time.Duration) float64 {
	return float64(d.Round(time.Millisecond)) / float64(time.Second)
}

// snapshotRelations lists every relation OID currently visible in the twin.
func snapshotRelations(ctx context.Context, conn *pgx.Conn) ([]uint32, error) {
	rows, err := conn.Query(ctx, `SELECT oid::oid FROM pg_class`)
	if err != nil {
		return nil, fmt.Errorf("monitor: snapshot relations: %w", err)
	}
	defer rows.Close()
	var oids []uint32
	for rows.Next() {
		var oid uint32
		if err := rows.Scan(&oid); err != nil {
			return nil, fmt.Errorf("monitor: snapshot relations: %w", err)
		}
		oids = append(oids, oid)
	}
	return oids, rows.Err()
}
