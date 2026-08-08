// Package engine is Brace's orchestrator: it turns one submitted statement
// into one measured rehearsal.
//
// A run is a fixed sequence, and the order is the product:
//
//	assess  -> acquire a warm twin -> grant the migrator its privileges
//	        -> start labeled write load, let it settle, MEASURE THE BASELINE
//	        -> start the lock monitor
//	        -> run the DDL as brace_migrator, sampling the write rate every 250ms
//	        -> keep the load running past the DDL so recovery is visible
//	        -> stop everything, compute the verdict, write the ledger row
//	        -> destroy the twin, always
//
// Two invariants hold no matter what fails in the middle: the twin is always
// released, and a run that produced measurements always returns them. A
// migration that ERRORS is not an engine error — it is a result with an error
// field, because "your migration does not even apply" is a finding.
package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/vinayaksonthalia/brace/internal/loadgen"
	"github.com/vinayaksonthalia/brace/internal/monitor"
	"github.com/vinayaksonthalia/brace/internal/twins"
)

// Mode records how the user asked for this run, purely for the ledger and the
// UI's framing. It has no effect on execution: the same statement is run the
// same way whatever it is labeled.
type Mode string

const (
	// ModeNaive: the user ran the deliberately dangerous form.
	ModeNaive Mode = "naive"
	// ModeSafe: the user ran the safe rewrite.
	ModeSafe Mode = "safe"
	// ModeCustom: the user's own SQL. The default.
	ModeCustom Mode = "custom"
)

func (m Mode) normalize() (Mode, error) {
	switch m {
	case "":
		return ModeCustom, nil
	case ModeNaive, ModeSafe, ModeCustom:
		return m, nil
	}
	return "", &RejectedError{
		Kind:   RejectRejected,
		Code:   "bad_mode",
		Reason: fmt.Sprintf("Unknown run mode %q. Expected one of: naive, safe, custom.", string(m)),
	}
}

// ErrRunInProgress is returned when a rehearsal is already running. Exactly one
// runs at a time, because two concurrent runs would contend for the same
// server's I/O and each would measure the other's interference as its own
// migration's cost. The API maps this to 409.
var ErrRunInProgress = errors.New("engine: a rehearsal is already in progress")

// ErrClosed is returned after Close.
var ErrClosed = errors.New("engine: closed")

// NoWarmTwinError says the pool is empty. The API maps it to 503 with the ETA.
type NoWarmTwinError struct {
	ETASeconds int              `json:"eta_seconds"`
	Pool       twins.PoolStatus `json:"pool"`
}

func (e *NoWarmTwinError) Error() string {
	return fmt.Sprintf("engine: no warm twin available (eta ~%ds)", e.ETASeconds)
}

// Unwrap lets callers test with errors.Is(err, twins.ErrNoWarmTwin).
func (e *NoWarmTwinError) Unwrap() error { return twins.ErrNoWarmTwin }

const (
	defaultSettle      = 2 * time.Second
	defaultRecovery    = 1500 * time.Millisecond
	defaultSampleEvery = 250 * time.Millisecond
	defaultWorkers     = 24
	defaultLockTimeout = 1500 * time.Millisecond
	defaultCloneETA    = 45 * time.Second
	// defaultMaxRunDuration bounds the ENTIRE measured window of one rehearsal —
	// grants, load, baseline, the migration itself, and the recovery observation
	// — everything after a warm twin is in hand. It exists because the migration
	// connection runs with statement_timeout=0 (so a real DDL is never
	// self-killed) and only one run may be in flight at a time: without this
	// guard, a single migration that queues forever behind live traffic or an
	// idle-in-transaction session would wedge the one run slot and freeze the
	// whole public lab until the process is restarted.
	defaultMaxRunDuration = 90 * time.Second
	teardownTimeout       = 30 * time.Second
	ledgerInsertTimeout   = 10 * time.Second
	invalidIndexTimeout   = 10 * time.Second
	grantTimeout          = 15 * time.Second
	migratorConnTimeout   = 15 * time.Second
	defaultHistoryLimit   = 20
	maxLedgerErrorLength  = 4000

	// maxHostileHold bounds a per-run contention hold. A hold approaching the run
	// budget would starve the migration of any chance to complete and turn every
	// hostile run into an INCONCLUSIVE, which measures nothing.
	maxHostileHold = 15 * time.Second
	// maxLockTimeout bounds a per-run simulated deadline. Past this the traffic
	// stops being a model of an application: nothing with a request behind it
	// waits half a minute for a row lock.
	maxLockTimeout = 30 * time.Second
	// contentionWait bounds how long the run waits for the contention source to
	// actually be holding a lock before starting the DDL. Reaching it means
	// hostile mode did not engage, and the report says so rather than claiming a
	// contended run it did not get.
	contentionWait = 10 * time.Second
)

// Contention is what Brace did to make the migration compete for its lock.
type Contention struct {
	// Enabled is whether hostile mode ran, achieved a hold, AND the migration was
	// observed queuing behind that hold for its lock — a MEASURED overlap, never
	// an assumed one. False means the migration was measured against an
	// effectively uncontended table (the hold never engaged, or the shape's lock
	// level does not conflict with it, so the DDL never had to wait), which is
	// what Qualifier warns about.
	Enabled bool `json:"enabled"`
	// Requested is whether hostile mode was asked for. Requested && !Enabled is
	// the honest "you asked and we could not deliver" case.
	Requested bool `json:"requested"`
	// HoldSeconds is how long each cycle held its transaction open. Every number
	// in a hostile run is conditional on it.
	HoldSeconds float64 `json:"hold_seconds,omitempty"`
	// Cycles is how many hold/commit cycles completed.
	Cycles int64 `json:"cycles,omitempty"`
	// Note explains, in one line, what the reader is looking at.
	Note string `json:"note,omitempty"`
}

// Config configures an Engine.
type Config struct {
	// AdminURL is the superuser DSN for the APPLICATION database — the same
	// maintenance database twins.Manager uses. The run ledger lives here.
	AdminURL string
	// Twins is the warm-twin pool. Required; the Engine does not own it and
	// will not close it.
	Twins *twins.Manager

	// Workers is the load generator's concurrency. Defaults to 24.
	Workers int
	// Labels is the traffic mix run against every twin. Nil means
	// loadgen.DefaultLabels() — the synthetic orders store. A self-hoster who
	// restored their own dump supplies their own here (see internal/boot's
	// BRACE_LOAD_LABELS), which is the only way the casualty counts describe
	// their application rather than ours.
	Labels []loadgen.LabelSpec
	// LockTimeout is the simulated request deadline on write traffic: a write
	// that waits longer than this for its lock is counted as a casualty.
	// Defaults to 1500ms, and every casualty count is reported alongside the
	// value that produced it, because it IS the definition of a casualty.
	// Overridable per run with WithLockTimeout.
	LockTimeout time.Duration
	// Hostile turns contention on by default for every run: while the migration
	// executes, Brace holds a transaction open against the load target so that a
	// fast DDL has to QUEUE for its lock instead of taking it instantly. OFF by
	// default — it makes the run answer a harsher question, and a number that
	// changed because of a Brace flag must be something the caller asked for.
	Hostile bool
	// HostileHold is how long each contention cycle holds its lock.
	// Defaults to loadgen.DefaultHold (2s).
	HostileHold time.Duration
	// SettleDuration is how long load runs before the baseline is taken.
	// Defaults to 2s.
	SettleDuration time.Duration
	// RecoveryDuration is how long load keeps running after the DDL returns, so
	// the recovery is visible on the trace. Defaults to 1.5s.
	RecoveryDuration time.Duration
	// SampleInterval is the timeline resolution. Defaults to 250ms.
	SampleInterval time.Duration
	// MigratorRole is the unprivileged role migrations run as.
	// Defaults to brace_migrator.
	MigratorRole string
	// TrafficRole is the unprivileged role the load generator's write traffic
	// runs as. It is what keeps a migration-installed CHECK/trigger/DEFAULT/index
	// expression from firing as a superuser when live traffic writes a row.
	// Defaults to brace_traffic.
	TrafficRole string
	// CloneETA is how long a twin clone takes on this deployment; it is only
	// used to give a 503 an honest-ish "come back in N seconds". Defaults to 45s.
	CloneETA time.Duration
	// ScratchPrefix is the database-name namespace for bring-your-own-schema runs
	// — the disposable, per-request databases RehearseSchema creates from a user's
	// pasted CREATE TABLE, seeds, measures, and drops. It is deliberately DISTINCT
	// from the twin pool's namespace (brace_run_) and from brace_template so a
	// scratch database can never be confused with, or swept alongside, a warm twin
	// or the real template. Defaults to brace_byos_. Every scratch database is
	// dropped at the end of its run; any left by a crash are swept at startup.
	ScratchPrefix string
	// TwinRows is how many rows the seeded template — and therefore every twin —
	// actually holds. It is not used to run anything; it is the denominator of
	// the scale projection, which is the only thing standing between a
	// measurement taken on a sample and a false SAFE about a table fifty times
	// bigger. 0 means the deployment did not tell us, and no projection is made.
	TwinRows int64
	// DefaultTargetRows is the operator's production row count from
	// BRACE_TARGET_ROWS: "my real orders table has 500,000,000 rows". Every run
	// is projected onto it unless the caller overrides it per-run. 0 = unset.
	DefaultTargetRows int64
	// DefaultTargetSource says where DefaultTargetRows came from — one of the
	// TargetSource* constants. It is carried all the way into the report so an
	// assumed size is never rendered as a fact about the caller's database.
	// Empty is read as TargetSourceDeployment.
	DefaultTargetSource string
	// MaxRunDuration bounds the whole measured rehearsal (everything after the
	// twin is acquired, up to and including the recovery observation). A run that
	// exceeds it is aborted — the migration is cancelled, load is stopped, the
	// twin is reaped, and the result comes back with verdict INCONCLUSIVE rather
	// than hanging the single run slot forever. Defaults to 90s.
	MaxRunDuration time.Duration
	// Logger defaults to slog.Default().
	Logger *slog.Logger
}

func (c *Config) applyDefaults() {
	if c.Workers <= 0 {
		c.Workers = defaultWorkers
	}
	if c.LockTimeout <= 0 {
		c.LockTimeout = defaultLockTimeout
	}
	if c.SettleDuration <= 0 {
		c.SettleDuration = defaultSettle
	}
	if c.RecoveryDuration <= 0 {
		c.RecoveryDuration = defaultRecovery
	}
	if c.SampleInterval <= 0 {
		c.SampleInterval = defaultSampleEvery
	}
	if c.MigratorRole == "" {
		c.MigratorRole = DefaultMigratorRole
	}
	if c.TrafficRole == "" {
		c.TrafficRole = DefaultTrafficRole
	}
	if c.CloneETA <= 0 {
		c.CloneETA = defaultCloneETA
	}
	if c.ScratchPrefix == "" {
		c.ScratchPrefix = DefaultScratchPrefix
	}
	if c.MaxRunDuration <= 0 {
		c.MaxRunDuration = defaultMaxRunDuration
	}
	if c.HostileHold <= 0 {
		c.HostileHold = loadgen.DefaultHold
	}
	if c.DefaultTargetSource == "" {
		c.DefaultTargetSource = TargetSourceDeployment
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// Casualties is the frozen dashboard shape for killed writes.
type Casualties struct {
	Total   int64            `json:"total"`
	ByLabel map[string]int64 `json:"by_label"`
	// DeadlineMS is the simulated request deadline these deaths are relative to
	// — the lock_timeout the load pool ran with. It is part of the number, not
	// metadata about it: the same freeze kills nobody in an application that
	// sets no deadline and kills several times as many in one that allows 200ms.
	// Every rendering of Total has to carry it.
	DeadlineMS int64 `json:"deadline_ms"`
}

// RunResult is one rehearsal's finding. The JSON tags are the frozen contract
// the dashboard's run_finished event is built from — verdict, blocked_seconds,
// casualties{total,by_label}, duration_seconds, error — plus everything else
// the lab learned.
type RunResult struct {
	RunID     string    `json:"run_id"`
	StartedAt time.Time `json:"started_at"`
	Twin      string    `json:"twin"`
	Mode      Mode      `json:"mode"`
	SQL       string    `json:"sql"`

	// --- the frozen contract ---
	// Verdict is the MEASURED verdict and nothing else: what this twin, at this
	// size, actually did. It is deliberately never adjusted by the projection —
	// the dashboard is coded against it, and more importantly a measurement that
	// silently absorbs a model stops being a measurement.
	Verdict         string     `json:"verdict"`
	BlockedSeconds  float64    `json:"blocked_seconds"`
	Casualties      Casualties `json:"casualties"`
	DurationSeconds float64    `json:"duration_seconds"`
	// Error is the migration's own failure, if it failed. A non-empty Error
	// with a nil Go error means "the run completed and the migration did not".
	Error string `json:"error,omitempty"`

	// --- the scale layer (additive; see scale.go) ---
	// EffectiveVerdict is the verdict to gate a deploy on: the worse of the
	// measured verdict and the projected one. When no target size was supplied
	// it is identical to Verdict. It can turn SAFE into UNSAFE and never the
	// reverse.
	EffectiveVerdict string `json:"effective_verdict"`
	// ScaleProjection is what this migration is modelled to cost at the
	// operator's real table size. Modelled, not measured — the distinction is
	// the point, and Model names the assumption.
	ScaleProjection ScaleProjection `json:"scale_projection"`
	// ScaleWarning is non-empty exactly when the twin said SAFE and the
	// projection says UNSAFE: the false-safe this whole layer exists to catch.
	ScaleWarning string `json:"scale_warning,omitempty"`

	// --- the teach layer ---
	Classification Classification `json:"classification"`
	Explanation    string         `json:"explanation"`
	Suggestion     string         `json:"suggestion,omitempty"`
	PredictedSafe  bool           `json:"predicted_safe"`
	Note           string         `json:"note,omitempty"`
	// Qualifier names what this run did NOT establish, when a bare verdict would
	// overclaim. Today there is one: QualifierAcquisitionNotSimulated, the SAFE
	// that only means "nothing was holding the table when we asked". Empty when
	// the verdict stands on its own.
	Qualifier string `json:"qualifier,omitempty"`
	// FailureMode names why traffic stopped when the cause was not a lock — see
	// FailureModeWritesErrored. Empty in the ordinary case.
	FailureMode string `json:"failure_mode,omitempty"`
	// Contention describes the concurrent transaction Brace held open against
	// the load target during the DDL, if any. Enabled=false is the default and
	// means the run measured the migration against an otherwise-uncontended
	// table — see Qualifier.
	Contention Contention `json:"contention"`

	// --- the evidence ---
	BaselineWPS int64 `json:"baseline_writes_per_sec"`
	// LockModes are the modes the migration was actually seen holding, from the
	// monitor, strongest first.
	LockModes []string `json:"lock_modes"`
	// MonitorBlockedSeconds is the server-side corroboration of
	// BlockedSeconds. The two are measured completely differently — one from
	// the write rate, one from pg_stat_activity — and both are published so a
	// disagreement is visible rather than resolved silently.
	MonitorBlockedSeconds float64        `json:"monitor_blocked_seconds"`
	Monitor               monitor.Report `json:"monitor"`
	// ErroredTotal is writes that failed for reasons we could NOT attribute to
	// a lock, across the whole run. Never folded into casualties.
	ErroredTotal int64 `json:"errored_total"`
	// ErroredDuringDDL is the same count windowed onto the migration, which is
	// what distinguishes "your migration broke the application's SQL" from
	// "something was already failing before it started".
	ErroredDuringDDL int64 `json:"errored_during_ddl"`
	// CasualtiesAllPhases is the run's grand total of lock-killed writes,
	// including the settle and recovery phases. Casualties above is windowed to
	// the DDL; this is here so the difference is visible rather than lost.
	CasualtiesAllPhases int64 `json:"casualties_all_phases"`
	// InvalidIndexes are indexes left INVALID by a failed CONCURRENTLY build.
	InvalidIndexes []string `json:"invalid_indexes,omitempty"`
	// Timeline is the 250ms trace; DDLStartT and DDLEndT delimit the migration
	// within it, both in seconds since load start.
	Timeline  []RatePoint `json:"timeline"`
	DDLStartT float64     `json:"ddl_start_t"`
	DDLEndT   float64     `json:"ddl_end_t"`
	// LedgerID is the brace_runs row id, or 0 if the insert failed.
	LedgerID int64 `json:"ledger_id,omitempty"`
}

// RunState is the live view of an in-flight run, for the status endpoint.
type RunState struct {
	RunID       string    `json:"run_id"`
	Twin        string    `json:"twin"`
	Mode        Mode      `json:"mode"`
	SQL         string    `json:"sql"`
	Phase       string    `json:"phase"`
	StartedAt   time.Time `json:"started_at"`
	BaselineWPS int64     `json:"baseline_writes_per_sec"`
}

// Status is what the API's /status serves.
type Status struct {
	Pool       twins.PoolStatus `json:"pool"`
	Running    bool             `json:"running"`
	CurrentRun *RunState        `json:"current_run,omitempty"`
	// LastBaselineWPS is the most recent measured baseline, so the UI has a
	// scale to draw before the first run of a session finishes.
	LastBaselineWPS int64  `json:"baseline_writes_per_sec"`
	MigratorRole    string `json:"migrator_role"`
	// TrafficRole is the unprivileged role the load traffic writes as, surfaced
	// for observability so an operator can confirm the writes are not superuser.
	TrafficRole string `json:"traffic_role"`
}

// run phases, in order.
const (
	phaseAcquiring = "acquiring_twin"
	// phaseProvisioning and phaseSeeding are the two extra phases a
	// bring-your-own-schema run passes through before it joins the normal
	// pipeline at phaseGranting: creating the disposable database + the user's
	// table, then seeding it with synthetic rows.
	phaseProvisioning = "provisioning_schema"
	phaseSeeding      = "seeding_schema"
	phaseGranting     = "granting"
	phaseWarming      = "starting_load"
	phaseBaseline     = "measuring_baseline"
	phaseMigrating    = "running_migration"
	phaseRecovery     = "observing_recovery"
	phaseReporting    = "reporting"
)

// Engine orchestrates rehearsals. Safe for concurrent use; at most one
// rehearsal runs at a time.
type Engine struct {
	cfg    Config
	log    *slog.Logger
	pool   *pgxpool.Pool
	ledger *Ledger

	// migratorPassword exists only in this process's memory. It is never
	// logged, never stored, and is rotated on every startup.
	migratorPassword string
	// trafficPassword is the load-traffic role's password. Same discipline as
	// migratorPassword: memory-only, never logged, rotated on every startup.
	trafficPassword string

	mu           sync.Mutex
	running      bool
	cur          *RunState
	curSampler   *sampler
	curGen       *loadgen.Generator
	lastBaseline int64
	closed       bool
}

// New prepares the engine: it opens its admin pool, installs the run ledger,
// and creates or re-secures the unprivileged migrator role.
func New(ctx context.Context, cfg Config) (*Engine, error) {
	cfg.applyDefaults()
	if cfg.AdminURL == "" {
		return nil, errors.New("engine: Config.AdminURL is required")
	}
	if cfg.Twins == nil {
		return nil, errors.New("engine: Config.Twins is required")
	}

	poolCfg, err := pgxpool.ParseConfig(cfg.AdminURL)
	if err != nil {
		return nil, fmt.Errorf("engine: parse AdminURL: %w", err)
	}
	// A handful of connections: the ledger and the per-run grants are all this
	// pool ever does.
	poolCfg.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("engine: connect admin pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("engine: ping admin database: %w", err)
	}

	password, err := generatePassword()
	if err != nil {
		pool.Close()
		return nil, err
	}
	trafficPassword, err := generatePassword()
	if err != nil {
		pool.Close()
		return nil, err
	}

	e := &Engine{
		cfg:              cfg,
		log:              cfg.Logger.With("component", "engine"),
		pool:             pool,
		ledger:           NewLedger(pool),
		migratorPassword: password,
		trafficPassword:  trafficPassword,
	}
	// Both unprivileged roles are (re-)secured on every startup with a fresh
	// password. The migrator runs the DDL under test; the traffic role runs the
	// load generator's writes. NEITHER is superuser, which is what makes any
	// expression a migration installs (CHECK/trigger/DEFAULT/index) harmless when
	// live traffic fires it. See migrate.go's package boundary note.
	if err := ensureRole(ctx, pool, cfg.MigratorRole, password); err != nil {
		pool.Close()
		return nil, err
	}
	if err := ensureRole(ctx, pool, cfg.TrafficRole, trafficPassword); err != nil {
		pool.Close()
		return nil, err
	}
	if err := e.ledger.CreateSchema(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	// The scratch namespace must be specific enough that a sweep of it cannot
	// touch anything else on the server — it is DROP DATABASE with a prefix.
	if !safeIdent.MatchString(cfg.ScratchPrefix) || len(cfg.ScratchPrefix) < 6 {
		pool.Close()
		return nil, fmt.Errorf("engine: ScratchPrefix %q must match %s and be at least 6 bytes", cfg.ScratchPrefix, safeIdent)
	}
	// Reclaim any scratch database a previous crash left behind. Best-effort: a
	// bring-your-own-schema run is additive, and a sweep that cannot complete must
	// not stop the lab from serving the stock orders flow. Brace is single-tenant
	// per server (the twin pool and the rotated roles already assume that), so a
	// fresh process owns the whole namespace and anything matching the prefix is
	// debris.
	if err := e.sweepScratchOrphans(ctx); err != nil {
		e.log.Warn("engine.scratch_sweep_failed", "prefix", cfg.ScratchPrefix, "err", err)
	}
	labelSource := "default"
	if len(cfg.Labels) > 0 {
		labelSource = "custom"
	}
	e.log.Info("engine.ready", "migrator_role", cfg.MigratorRole, "traffic_role", cfg.TrafficRole,
		"workers", cfg.Workers, "labels", labelSource, "label_count", len(cfg.Labels))
	return e, nil
}

// Close releases the engine's own resources. It does NOT close the twins
// Manager, which the caller owns.
func (e *Engine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	e.mu.Unlock()
	e.pool.Close()
	return nil
}

// Status reports the pool and the current run.
func (e *Engine) Status() Status {
	e.mu.Lock()
	s := Status{
		Running:         e.running,
		LastBaselineWPS: e.lastBaseline,
		MigratorRole:    e.cfg.MigratorRole,
		TrafficRole:     e.cfg.TrafficRole,
	}
	if e.cur != nil {
		cp := *e.cur
		s.CurrentRun = &cp
	}
	e.mu.Unlock()
	s.Pool = e.cfg.Twins.Status()
	return s
}

// LiveStats returns the in-flight run's load statistics, for the SSE layer.
// The bool is false when no run is active.
func (e *Engine) LiveStats() (loadgen.Stats, bool) {
	e.mu.Lock()
	gen := e.curGen
	e.mu.Unlock()
	if gen == nil {
		return loadgen.Stats{}, false
	}
	return gen.Snapshot(), true
}

// LiveTimeline returns the timeline sampled so far for the in-flight run.
func (e *Engine) LiveTimeline() []RatePoint {
	e.mu.Lock()
	s := e.curSampler
	e.mu.Unlock()
	if s == nil {
		return nil
	}
	return s.points()
}

// History returns recent runs from the ledger, newest first.
func (e *Engine) History(ctx context.Context, limit int) ([]RunRecord, error) {
	if limit <= 0 {
		limit = defaultHistoryLimit
	}
	return e.ledger.List(ctx, limit)
}

// begin claims the single run slot. Returns false if a run is already active —
// it never waits, because a queued HTTP request that silently blocks for two
// minutes is worse than an honest 409.
func (e *Engine) begin(state *RunState) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrClosed
	}
	if e.running {
		return ErrRunInProgress
	}
	e.running = true
	e.cur = state
	return nil
}

func (e *Engine) finish() {
	e.mu.Lock()
	e.running = false
	e.cur = nil
	e.curGen = nil
	e.curSampler = nil
	e.mu.Unlock()
}

func (e *Engine) setPhase(phase string) {
	e.mu.Lock()
	if e.cur != nil {
		e.cur.Phase = phase
	}
	e.mu.Unlock()
}

// Rehearse crash-tests one statement against a disposable twin.
//
// Error contract, which the API maps directly onto status codes:
//
//	*RejectedError    -> 400 (including kind "already_safe")
//	ErrRunInProgress  -> 409
//	*NoWarmTwinError  -> 503 with an ETA
//	anything else     -> 500
//
// A migration that fails on the server is NOT one of these: it comes back as a
// (*RunResult, nil) with RunResult.Error set.
//
// Options are the per-run overrides — today only WithTargetRows. They are
// variadic so that adding one never breaks a caller, and so the common case
// (Rehearse(ctx, sql, mode)) still reads as one line.
func (e *Engine) Rehearse(ctx context.Context, sql string, mode Mode, opts ...RunOption) (*RunResult, error) {
	ro := e.resolveOptions(opts)
	mode, err := mode.normalize()
	if err != nil {
		return nil, err
	}
	as, err := assessForRun(sql, ro)
	if err != nil {
		// Includes the "already safe, don't waste a twin" case. No twin has
		// been touched at this point, which is the whole point of pre-filtering
		// before acquiring. The one carve-out lives in assessForRun: a hostile
		// run on an already_safe shape that still takes ACCESS EXCLUSIVE is let
		// through to a real rehearsal, because MEASURING that acquisition freeze
		// is the entire point of hostile mode.
		//
		// gateAlreadySafe is the second carve-out, in the other direction: an
		// already_safe answer for a statement whose target relation does not exist
		// is downgraded to a clear REJECTED, so a typo'd or absent table can never
		// short-circuit to SAFE without ever running. It is a no-op for every other
		// rejection.
		return nil, e.gateAlreadySafe(ctx, as, err)
	}

	runID, err := newRunID()
	if err != nil {
		return nil, err
	}
	state := &RunState{
		RunID:     runID,
		Mode:      mode,
		SQL:       as.SQL,
		Phase:     phaseAcquiring,
		StartedAt: time.Now(),
	}
	if err := e.begin(state); err != nil {
		return nil, err
	}
	defer e.finish()

	twin, err := e.cfg.Twins.Acquire(ctx)
	if err != nil {
		if errors.Is(err, twins.ErrNoWarmTwin) {
			status := e.cfg.Twins.Status()
			return nil, &NoWarmTwinError{ETASeconds: int(e.cfg.CloneETA.Seconds()), Pool: status}
		}
		return nil, fmt.Errorf("engine: acquire twin: %w", err)
	}
	// The twin is released no matter how this function leaves — panic, error,
	// or cancelled context. The context is deliberately detached: teardown must
	// work precisely when the caller's request has already been abandoned, and
	// a leaked twin is a database that stays on disk until the next restart.
	defer func() {
		relCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), teardownTimeout)
		defer cancel()
		if err := e.cfg.Twins.Release(relCtx, twin); err != nil && !errors.Is(err, twins.ErrClosed) {
			e.log.Error("engine.twin_release_failed", "run", runID, "twin", twin.Name, "err", err)
		}
	}()

	e.mu.Lock()
	if e.cur != nil {
		e.cur.Twin = twin.Name
	}
	e.mu.Unlock()
	e.log.Info("engine.run_started", "run", runID, "twin", twin.Name, "mode", mode,
		"classification", as.Classification)

	return e.runOn(ctx, twin, as, mode, state, ro)
}

// resolveOptions folds the caller's per-run overrides onto the deployment's
// defaults, producing the effective options one run will use. Rehearse and the
// hostile-aware pre-check (AssessRun) share it so both reach the same admit
// decision from the same hostile value — including BRACE_HOSTILE, which lives
// only in the engine's config and is invisible to the HTTP layer.
func (e *Engine) resolveOptions(opts []RunOption) runOptions {
	ro := runOptions{
		TargetRows:   e.cfg.DefaultTargetRows,
		TargetSource: e.cfg.DefaultTargetSource,
		Hostile:      e.cfg.Hostile,
		HostileHold:  e.cfg.HostileHold,
		LockTimeout:  e.cfg.LockTimeout,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&ro)
		}
	}
	return ro
}

// AssessRun pre-filters a statement exactly as Rehearse will, honouring this
// run's options. The HTTP layer runs it synchronously so a bad statement is a
// 400 on the submitting request — but it MUST make the same decision the engine
// would, or the two disagree. The decisive case is hostile mode: without it, a
// hostile run on an already_safe ACCESS EXCLUSIVE shape would be refused here
// before it could measure the very freeze hostile mode exists to expose.
//
// It takes a context because it now runs the already_safe existence gate (a
// cheap catalog peek at a warm twin), so an already_safe answer for a
// nonexistent relation becomes a REJECTED on the very request that submitted it,
// exactly as Rehearse would decide.
func (e *Engine) AssessRun(ctx context.Context, sql string, opts ...RunOption) (*Assessment, error) {
	as, err := assessForRun(sql, e.resolveOptions(opts))
	if err != nil {
		return as, e.gateAlreadySafe(ctx, as, err)
	}
	return as, nil
}

// assessForRun is Assess plus the hostile carve-out. Assess turns away the
// catalog-only shapes with an already_safe rejection so a twin is not spent
// proving a flat line — the right call for a default run. But hostile mode
// exists to measure the one risk that fast-path assumes away: the wait to
// ACQUIRE the lock behind an already-open transaction. So a hostile run on an
// already_safe shape that still takes ACCESS EXCLUSIVE (the metadata-only ADD
// COLUMN class) is allowed through to a real rehearsal. Everything else keeps
// the fast-path: every non-hostile run, and any shape that takes no ACCESS
// EXCLUSIVE lock, where a hold on the load target has nothing to conflict with.
func assessForRun(sql string, ro runOptions) (*Assessment, error) {
	as, err := Assess(sql)
	if err == nil {
		return as, nil
	}
	if ro.Hostile && as != nil && isAlreadySafeAccessExclusive(err, as) {
		return as, nil
	}
	return as, err
}

// isAlreadySafeAccessExclusive reports whether err is the already_safe rejection
// for a shape whose strongest lock is ACCESS EXCLUSIVE — the "fast but ACCESS
// EXCLUSIVE" class whose only danger is the acquisition wait, which is exactly
// what hostile mode reproduces.
func isAlreadySafeAccessExclusive(err error, as *Assessment) bool {
	var rej *RejectedError
	if !errors.As(err, &rej) || rej.Kind != RejectAlreadySafe {
		return false
	}
	return as.Lesson.ExpectedLock == "AccessExclusiveLock"
}

// RunOption is a per-run override.
type RunOption func(*runOptions)

type runOptions struct {
	// TargetRows is the operator's production row count for THIS run, defaulting
	// to Config.DefaultTargetRows. An invalid value is dropped rather than
	// rejected: the projection is an extra, and refusing to rehearse a migration
	// because a row count was mistyped would be the wrong trade.
	TargetRows int64
	// TargetSource records who supplied TargetRows; see the TargetSource*
	// constants.
	TargetSource string
	// Hostile runs this rehearsal against a deliberately contended table.
	Hostile bool
	// HostileHold is how long each contention cycle holds its lock.
	HostileHold time.Duration
	// LockTimeout is the simulated request deadline for this run's traffic — the
	// definition of a casualty. Zero means the deployment's default.
	LockTimeout time.Duration
	// Labels overrides the traffic mix for THIS run. It is how a
	// bring-your-own-schema run drives generic INSERT traffic at the user's own
	// table instead of the stock orders mix. Nil means the deployment's default
	// (Config.Labels), so the stock orders flow is untouched.
	Labels []loadgen.LabelSpec
	// TwinRows overrides the scale projection's denominator for THIS run — the
	// number of rows the disposable database actually holds. A schema run seeds a
	// bounded sample and sets this to the seeded count; the stock flow leaves it 0
	// and Config.TwinRows (the template's size) stands.
	TwinRows int64
}

// WithTargetRows answers "what about MY table?" for one run. Non-positive or
// absurd values (> 1e12) are ignored, leaving whatever the deployment's
// BRACE_TARGET_ROWS said.
func WithTargetRows(n int64) RunOption {
	return func(o *runOptions) {
		if v := validTargetRows(n); v > 0 {
			o.TargetRows = v
			o.TargetSource = TargetSourceRequest
		}
	}
}

// WithHostile turns the contention source on or off for one run, overriding the
// deployment's default either way. Hostile mode is what makes a fast DDL have to
// WAIT for its lock — see loadgen.Contender and the Qualifier field.
func WithHostile(on bool) RunOption {
	return func(o *runOptions) { o.Hostile = on }
}

// WithHostileHold sets how long each contention cycle holds its lock. Ignored
// when non-positive. Values are clamped to maxHostileHold, because a hold longer
// than the run budget would guarantee an INCONCLUSIVE and measure nothing.
func WithHostileHold(d time.Duration) RunOption {
	return func(o *runOptions) {
		if d <= 0 {
			return
		}
		if d > maxHostileHold {
			d = maxHostileHold
		}
		o.HostileHold = d
	}
}

// WithLockTimeout sets the simulated request deadline for one run: the wait
// after which a blocked write is counted dead. It is the single largest lever on
// the casualty count, which is why it is both configurable and reported.
// Non-positive values leave the deployment's default; absurd ones are clamped.
func WithLockTimeout(d time.Duration) RunOption {
	return func(o *runOptions) {
		if d <= 0 {
			return
		}
		if d > maxLockTimeout {
			d = maxLockTimeout
		}
		o.LockTimeout = d
	}
}

// runOn is the measured part of a run: everything after the twin exists and
// before it is destroyed.
func (e *Engine) runOn(ctx context.Context, twin *twins.Twin, as *Assessment, mode Mode, state *RunState, ro runOptions) (*RunResult, error) {
	// The traffic mix and the projection denominator are per-run: a
	// bring-your-own-schema run supplies both (generic INSERTs at the user's table,
	// and the seeded row count), while the stock orders flow leaves them unset and
	// the deployment's Config values stand. Everything else below is identical for
	// both paths — the whole point is that a schema run is measured exactly like a
	// twin run.
	labels := e.cfg.Labels
	if len(ro.Labels) > 0 {
		labels = ro.Labels
	}
	twinRows := e.cfg.TwinRows
	if ro.TwinRows > 0 {
		twinRows = ro.TwinRows
	}

	res := &RunResult{
		RunID:          state.RunID,
		StartedAt:      state.StartedAt,
		Twin:           twin.Name,
		Mode:           mode,
		SQL:            as.SQL,
		Classification: as.Classification,
		Explanation:    as.Lesson.Explanation,
		Suggestion:     as.Lesson.Suggestion,
		PredictedSafe:  as.PredictedSafe,
		Casualties:     Casualties{ByLabel: map[string]int64{}},
		LockModes:      []string{},
	}

	// The max-duration guard. Everything measured from here on runs under runCtx,
	// which fires after MaxRunDuration no matter what the migration is doing. When
	// it fires while the caller's own context is still alive, that is the guard —
	// not an abandoned request — and the run is reported INCONCLUSIVE rather than
	// left to wedge the single run slot forever. Teardown paths below deliberately
	// use context.WithoutCancel(ctx) so a fired guard cannot also prevent cleanup.
	runCtx, cancelRun := context.WithTimeout(ctx, e.cfg.MaxRunDuration)
	defer cancelRun()
	guardFired := func() bool {
		return ctx.Err() == nil && errors.Is(runCtx.Err(), context.DeadlineExceeded)
	}

	// 3. Twin-local privileges for the two unprivileged roles: the migrator (which
	// runs the DDL) and the traffic role (which runs the load writes). Both are
	// granted here, from the twin's admin DSN, because object-level grants must be
	// issued inside the database that owns the objects.
	e.setPhase(phaseGranting)
	grantCtx, cancelGrant := context.WithTimeout(runCtx, grantTimeout)
	err := grantOnTwin(grantCtx, twin.DSN, twin.Name, e.cfg.MigratorRole)
	if err == nil {
		err = grantTrafficOnTwin(grantCtx, twin.DSN, twin.Name, e.cfg.TrafficRole)
	}
	cancelGrant()
	if err != nil {
		return nil, err
	}

	// 4. Live traffic — run as the UNPRIVILEGED traffic role, never the admin DSN.
	// This is the privilege-escalation fix: the writes fire any CHECK/trigger/
	// DEFAULT/index expression the migration installs, and running them as a
	// NOSUPERUSER role is what makes PostgreSQL deny pg_read_file() and report
	// is_superuser='off' inside those expressions. The monitor and the migrator
	// connection keep using twin.DSN — they legitimately need it.
	trafDSN, err := trafficDSN(twin.DSN, e.cfg.TrafficRole, e.trafficPassword)
	if err != nil {
		return nil, fmt.Errorf("engine: derive traffic DSN: %w", err)
	}
	e.setPhase(phaseWarming)
	// The simulated request deadline. It defines what a casualty IS, so it is
	// pinned on the result before anything is counted against it.
	lockTimeout := ro.LockTimeout
	if lockTimeout <= 0 {
		lockTimeout = e.cfg.LockTimeout
	}
	res.Casualties.DeadlineMS = lockTimeout.Milliseconds()
	gen, err := loadgen.New(runCtx, loadgen.Config{
		DSN:         trafDSN,
		Workers:     e.cfg.Workers,
		LockTimeout: lockTimeout,
		Labels:      labels,
		Logger:      e.cfg.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("engine: start load generator: %w", err)
	}
	loadStopped := false
	stopLoad := func() {
		if loadStopped {
			return
		}
		loadStopped = true
		stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), teardownTimeout)
		defer cancel()
		if err := gen.Stop(stopCtx); err != nil {
			e.log.Error("engine.loadgen_stop_failed", "run", state.RunID, "err", err)
		}
	}
	defer stopLoad()

	if err := gen.Start(runCtx); err != nil {
		return nil, fmt.Errorf("engine: start load generator: %w", err)
	}

	// 6. The timeline. Sampling starts with the load, not with the DDL, so the
	// trace carries its own baseline and the UI can draw the "before".
	smp := newSampler(gen, e.cfg.SampleInterval)
	smp.start()
	defer smp.stop()

	e.mu.Lock()
	e.curGen = gen
	e.curSampler = smp
	e.mu.Unlock()

	// Let the pool warm and the rate stabilise before believing any number.
	e.setPhase(phaseBaseline)
	baselineFrom := time.Now()
	if !sleepCtx(runCtx, e.cfg.SettleDuration) {
		// The guard cannot realistically fire this early (the settle is a couple
		// of seconds and the budget is tens), but if MaxRunDuration was set
		// absurdly low we still refuse to return a nil error the caller cannot
		// interpret. A caller cancel returns its own error.
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, runCtx.Err()
	}
	baselineTo := time.Now()
	baseline := smp.rateBetween(baselineFrom, baselineTo)
	if baseline <= 0 {
		// Fall back to loadgen's own closed-window counter before giving up on
		// having a baseline at all.
		baseline = float64(gen.Snapshot().WritesPerSec)
	}
	res.BaselineWPS = int64(baseline + 0.5)
	e.mu.Lock()
	e.lastBaseline = res.BaselineWPS
	if e.cur != nil {
		e.cur.BaselineWPS = res.BaselineWPS
	}
	e.mu.Unlock()
	e.log.Info("engine.baseline", "run", state.RunID, "wps", res.BaselineWPS)

	// 4b. Hostile mode. The baseline is deliberately already measured above:
	// contention costs throughput, and folding that into the baseline would make
	// the freeze look smaller than it was.
	//
	// The set-up below is ORDERED so that it straddles the dispatch. A fast DDL's
	// danger is the wait to acquire its lock behind an already-open transaction,
	// and a hold that is not active at the instant the migration requests its lock
	// measures nothing — the DDL slips through the ~gap between two cycles and
	// reports a freeze of zero. So the slow parts (the monitor, the migrator
	// connection) are done first, then the hold is confirmed genuinely held, and
	// only then is the migration dispatched into it.
	res.Contention = Contention{Requested: ro.Hostile}
	stopContention := func() {}
	defer func() { stopContention() }()

	// 5. The monitor. Started before the contender and the DDL so it is already
	// sampling when the migration queues for its lock: the wait it records is the
	// proof the DDL actually contended, which is what earns the hostile verdict.
	mon := monitor.New(monitor.Config{
		DSN:      twin.DSN,
		AppName:  MigrationAppName,
		Interval: e.cfg.SampleInterval,
		Logger:   e.cfg.Logger,
	})
	if err := mon.Start(runCtx); err != nil {
		// Corroboration is optional. The run continues without it.
		e.log.Warn("engine.monitor_unavailable", "run", state.RunID, "err", err)
	}

	// 6. The migrator connection, established BEFORE the hold is confirmed. This
	// connect can be slow; doing it here means it cannot burn the hold window and
	// leave the DDL requesting its lock after the cycle has already committed.
	migCtx, cancelMig := context.WithTimeout(runCtx, migratorConnTimeout)
	migConn, err := connectAsMigrator(migCtx, twin.DSN, e.cfg.MigratorRole, e.migratorPassword)
	cancelMig()
	if err != nil {
		_ = mon.StopAndReport()
		return nil, err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		_ = migConn.Close(closeCtx)
		cancel()
	}()

	// 7. The contention hold, confirmed active immediately before dispatch. Start
	// the contender and BLOCK until a transaction is provably holding its lock on
	// the load target — WaitHolding does not return until the INSERT inside the
	// held transaction has taken its row lock, so the hold is fresh with nearly a
	// full cycle left. holdConfirmed gates every hostile claim below: with no
	// confirmed hold there was nothing for the migration to queue behind, and the
	// numbers are a default run's.
	holdConfirmed := false
	if ro.Hostile {
		hold := ro.HostileHold
		if hold <= 0 {
			hold = e.cfg.HostileHold
		}
		res.Contention.HoldSeconds = hold.Seconds()
		con := loadgen.NewContender(loadgen.ContendConfig{
			DSN:       trafDSN,
			Statement: loadgen.ContentionStatement(labels),
			Hold:      hold,
			Logger:    e.cfg.Logger,
		})
		if startErr := con.Start(runCtx); startErr != nil {
			// Contention is the run's premise, not an optional extra: continuing
			// without it would produce a number labelled hostile that is not.
			e.log.Warn("engine.contender_unavailable", "run", state.RunID, "err", startErr)
			res.Contention.Note = "the contention session could not be started"
		} else {
			stopContention = func() {
				stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), teardownTimeout)
				defer cancel()
				st := con.Stop(stopCtx)
				res.Contention.Cycles = st.Cycles
				res.Contention.HoldSeconds = st.HoldSeconds
			}
			waitCtx, cancelWait := context.WithTimeout(runCtx, contentionWait)
			holdConfirmed = con.WaitHolding(waitCtx)
			cancelWait()
			if !holdConfirmed {
				res.Contention.Note = "no lock was ever held within the wait budget"
			}
		}
	}

	// 8. The crash — dispatched now, while the hold (if any) is confirmed active,
	// so the migration requests its lock INTO live contention rather than into a
	// race with the cycle timer.
	e.setPhase(phaseMigrating)
	t0 := time.Now()
	res.DDLStartT = smp.since(t0)
	// Autocommit, never a transaction block: CREATE INDEX CONCURRENTLY, DROP
	// INDEX CONCURRENTLY, REINDEX CONCURRENTLY and VACUUM all refuse to run
	// inside one, and wrapping the statement would silently change what is
	// being measured for everything else.
	//
	// runCtx bounds this Exec. If the DDL cannot acquire its lock inside the run
	// budget, runCtx fires; pgx then sends a cancel request to the server (the
	// programmatic equivalent of pg_cancel_backend against the migration's own
	// backend) and Exec returns. That is what stops one hung migration from
	// pinning the single run slot forever.
	_, ddlErr := migConn.Exec(runCtx, as.SQL)
	t1 := time.Now()
	res.DDLEndT = smp.since(t1)
	// Contention ends with the DDL. The recovery observation is meant to show
	// traffic coming back, and a session still taking locks would smear that.
	stopContention()
	res.DurationSeconds = roundSeconds(t1.Sub(t0).Seconds())
	guarded := ddlErr != nil && guardFired()
	if ddlErr != nil {
		res.Error = truncate(ddlErr.Error(), maxLedgerErrorLength)
		e.log.Warn("engine.migration_failed", "run", state.RunID, "err", ddlErr, "guard_fired", guarded)
	}

	// 7. Keep the load running so recovery is visible on the trace — but not when
	// the guard already fired: the budget is spent, and the only thing left to do
	// is report what we have and reap the twin.
	if !guarded {
		e.setPhase(phaseRecovery)
		if !sleepCtx(runCtx, e.cfg.RecoveryDuration) && ctx.Err() != nil {
			e.log.Warn("engine.recovery_cut_short", "run", state.RunID, "err", ctx.Err())
		}
	}

	e.setPhase(phaseReporting)
	monReport := mon.StopAndReport()
	smp.stop()
	stopLoad()
	final := gen.Snapshot()

	// Hostile mode's Enabled flag is EARNED, never latched from "a hold started
	// once". It is true only when a hold was confirmed active at dispatch AND the
	// migration was then observed waiting for its lock — i.e. it actually queued
	// behind the hold. A shape whose lock does not conflict with the hold (an
	// ALTER INDEX ... RENAME takes only SHARE UPDATE EXCLUSIVE on PG12+) acquires
	// without ever waiting: it did not contend, so it must not be reported as
	// though it did, and the confident "strongest green light" note stays out of
	// reach. If the monitor never got to look, we cannot claim the overlap either
	// — Enabled stays false, which is the honest direction.
	if ro.Hostile && holdConfirmed {
		res.Contention.Enabled = monReport.MigrationLockWaited
		if !res.Contention.Enabled && res.Contention.Note == "" {
			// The DDL did not queue behind the hold. There are two very different
			// reasons, and only one is safe to assert. If its lock level is KNOWN
			// not to conflict with the writers' ROW EXCLUSIVE hold (a RENAME's SHARE
			// UPDATE EXCLUSIVE, say), non-conflict is a fact. But a shape that DOES
			// conflict (ACCESS EXCLUSIVE) yet was never seen waiting means the 250ms
			// monitor simply missed a short wait or failed to sample — claiming
			// non-conflict there would be inventing a reason. Only say what we know.
			if conflictsWithWriteHold(as.Lesson.ExpectedLock) {
				res.Contention.Note = "the migration was never observed waiting for its lock within the sampling resolution, so the acquisition wait was not measured"
			} else {
				res.Contention.Note = "the migration never had to wait for its lock — its lock level does not conflict with the hold, so the acquisition wait was not exercised"
			}
		}
	}

	// 8. An INVALID index is the CONCURRENTLY failure mode. Check whenever the
	// statement was concurrent — a build can be left invalid even on paths
	// where the client saw no error, e.g. a cancelled second pass.
	if as.Concurrent {
		idxCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), invalidIndexTimeout)
		invalid, err := findInvalidIndexes(idxCtx, twin.DSN)
		cancel()
		if err != nil {
			e.log.Warn("engine.invalid_index_check_failed", "run", state.RunID, "err", err)
		} else if len(invalid) > 0 {
			res.InvalidIndexes = invalid
		}
	}

	// 9. The verdict.
	res.Timeline = smp.points()
	res.BlockedSeconds = blockedSecondsFrom(res.Timeline, res.DDLStartT, baseline)
	// Casualties are windowed onto the migration, exactly like blocked_seconds:
	// the counters as they stood when the DDL began, subtracted from the final
	// ones. The run's grand total is kept alongside rather than thrown away.
	atStart := smp.atDDLStart(res.DDLStartT)
	res.Casualties = windowedCasualties(final, atStart, lockTimeout)
	res.CasualtiesAllPhases = final.CasualtiesTotal
	res.ErroredTotal = final.ErroredTotal
	res.ErroredDuringDDL = final.ErroredTotal - atStart.erred
	// The migration's OBSERVED lock is the arbiter of whether the write rate
	// collapsing was a lock freeze at all. A write-rate dip under a lock that does
	// not conflict with writes — CREATE INDEX CONCURRENTLY's SHARE UPDATE
	// EXCLUSIVE, VACUUM, ANALYZE — is I/O and CPU contention from the work
	// competing for the disk on a shared box, not a freeze, and must not produce
	// an UNSAFE verdict. A tool that cries UNSAFE at a genuinely safe migration on
	// measurement noise is broken. See migrationHeldWriteBlockingLock and
	// verdictForObserved; real casualties (dead writes) still count.
	heldWriteBlockingLock := migrationHeldWriteBlockingLock(as, monReport)
	res.Verdict = verdictForObserved(res.BlockedSeconds, res.Casualties.Total, heldWriteBlockingLock)
	if res.Error != "" && !guarded {
		// Postgres refused the statement: nothing was measured, so no SAFE/UNSAFE
		// claim is honest here. See VerdictErrored.
		res.Verdict = VerdictErrored
	}
	if guarded {
		// The guard fired: the migration never finished inside the budget, so the
		// blocked-seconds number is only "how long it had frozen traffic SO FAR",
		// not a completed measurement. Report the distinct terminal state and
		// replace the raw context-deadline string with a message that says what
		// actually happened and why it matters.
		res.Verdict = VerdictInconclusive
		res.Error = fmt.Sprintf("migration exceeded %ds — could not acquire its lock while traffic ran; "+
			"in production this is itself a red flag: the change cannot complete under load",
			int(e.cfg.MaxRunDuration.Seconds()+0.5))
	}
	res.MonitorBlockedSeconds = monReport.BlockedSeconds
	res.Monitor = monReport
	if len(monReport.LockModes) > 0 {
		res.LockModes = monReport.LockModes
	}
	// 10. The scale layer. The twin is a SAMPLE, and a freeze measured on a
	// sample is not the freeze production gets: lock presence is scale-free, lock
	// duration is not. Verdict above stays exactly what was measured;
	// EffectiveVerdict is what a deploy gate should read.
	//
	// Two gates keep a projection honest here: it fires only for a run that
	// actually MEASURED something (scaleProjectionFor drops it for ERRORED /
	// INCONCLUSIVE), and only when the freeze it would scale was a real
	// write-freeze — a dip under a non-write-blocking lock (CONCURRENTLY, VACUUM)
	// carries HeldWriteBlockingLock=false and is never projected, because the same
	// lock is merely held longer on a bigger table without freezing anybody.
	res.ScaleProjection = scaleProjectionFor(res.Verdict, scaleInputs{
		Measured:              res.BlockedSeconds,
		TwinRows:              twinRows,
		TargetRows:            ro.TargetRows,
		ScalesWithRows:        as.Lesson.ScalesWithRows,
		BlocksWrites:          as.Lesson.BlocksWrites,
		ScaleUncertain:        as.Lesson.ScaleUncertain,
		HeldWriteBlockingLock: heldWriteBlockingLock,
		Verdict:               res.Verdict,
		TargetSource:          ro.TargetSource,
	})
	res.EffectiveVerdict = effectiveVerdict(res.Verdict, res.ScaleProjection)
	res.ScaleWarning = scaleWarning(res.Verdict, res.ScaleProjection)

	// 11. The narrative. FailureMode and Qualifier are decided before the note,
	// because both change what the note is allowed to claim.
	res.FailureMode = failureModeFor(res, monReport)
	res.Qualifier = qualifierFor(res, as, monReport)
	res.Note = noteFor(res, as, monReport)
	// The explanation was seeded with the static per-class lesson at
	// result-construction time. Revise it now, once the verdict, contention and
	// freeze are final: a shape whose lesson promises "nothing here to crash" but
	// that hostile mode actually made block must not keep contradicting the UNSAFE
	// verdict on the same card. explanationFor leaves every other case untouched.
	res.Explanation = explanationFor(res, as)

	e.log.Info("engine.run_finished",
		"run", state.RunID, "twin", twin.Name, "verdict", res.Verdict,
		"effective_verdict", res.EffectiveVerdict,
		"projected_blocked_s", res.ScaleProjection.ProjectedBlockedSeconds,
		"target_rows", res.ScaleProjection.TargetRows,
		"blocked_s", res.BlockedSeconds, "monitor_blocked_s", res.MonitorBlockedSeconds,
		"casualties", res.Casualties.Total, "casualties_all_phases", res.CasualtiesAllPhases,
		"deadline_ms", res.Casualties.DeadlineMS,
		"errored", res.ErroredTotal, "errored_during_ddl", res.ErroredDuringDDL,
		"hostile", res.Contention.Enabled, "qualifier", res.Qualifier,
		"failure_mode", res.FailureMode,
		"duration_s", res.DurationSeconds, "lock_modes", res.LockModes,
		"migration_error", res.Error)

	// The ledger is best-effort by design: the user waited for this
	// measurement, and losing the history row is a smaller harm than losing the
	// result.
	ledCtx, cancelLed := context.WithTimeout(context.WithoutCancel(ctx), ledgerInsertTimeout)
	id, err := e.ledger.Insert(ledCtx, res)
	cancelLed()
	if err != nil {
		e.log.Error("engine.ledger_insert_failed", "run", state.RunID, "err", err)
	} else {
		res.LedgerID = id
	}
	return res, nil
}

// casualtiesFrom projects loadgen's per-label stats onto the frozen dashboard
// shape. Only Died is carried across: casualties are writes killed waiting for
// a lock, and nothing else is ever folded in.
func casualtiesFrom(s loadgen.Stats) Casualties {
	c := Casualties{Total: s.CasualtiesTotal, ByLabel: make(map[string]int64, len(s.ByLabel))}
	for name, ls := range s.ByLabel {
		c.ByLabel[name] = ls.Died
	}
	return c
}

// windowedCasualties subtracts the counters as they stood when the migration
// began, so the reported deaths are the ones the migration caused. Negative
// differences are impossible from monotonic counters but are clamped anyway: a
// negative casualty count is the kind of number that destroys trust in every
// other number on the page.
func windowedCasualties(final loadgen.Stats, atStart sample, deadline time.Duration) Casualties {
	c := casualtiesFrom(final)
	c.DeadlineMS = deadline.Milliseconds()
	c.Total = nonNegative(c.Total - atStart.died)
	for name := range c.ByLabel {
		c.ByLabel[name] = nonNegative(c.ByLabel[name] - atStart.diedBy[name])
	}
	return c
}

func nonNegative(n int64) int64 {
	if n < 0 {
		return 0
	}
	return n
}

// takesAccessExclusive reports whether this run has grounds to believe the
// statement takes PostgreSQL's strongest lock — either the teach layer says so
// from the shape, or the monitor watched it happen.
func takesAccessExclusive(as *Assessment, mon monitor.Report) bool {
	if as != nil && as.Lesson.ExpectedLock == "AccessExclusiveLock" {
		return true
	}
	for _, m := range mon.LockModes {
		if m == "AccessExclusiveLock" {
			return true
		}
	}
	return false
}

// conflictsWithWriteHold reports whether a DDL holding expectedLock would have to
// queue behind the contender's hold. The contender holds the table with a write
// statement, i.e. a ROW EXCLUSIVE lock. In PostgreSQL's conflict table ROW
// EXCLUSIVE conflicts with SHARE, SHARE ROW EXCLUSIVE, EXCLUSIVE and ACCESS
// EXCLUSIVE; it does NOT conflict with ACCESS SHARE, ROW SHARE, ROW EXCLUSIVE or
// SHARE UPDATE EXCLUSIVE. An empty/unknown lock is treated as conflicting, so an
// unrecognised shape that was never seen waiting is reported as "not measured"
// rather than as "does not conflict" — the safe, non-overclaiming direction.
func conflictsWithWriteHold(expectedLock string) bool {
	switch expectedLock {
	case "ShareLock", "ShareRowExclusiveLock", "ExclusiveLock", "AccessExclusiveLock":
		return true
	case "AccessShareLock", "RowShareLock", "RowExclusiveLock", "ShareUpdateExclusiveLock":
		return false
	default:
		return true
	}
}

// migrationHeldWriteBlockingLock reports whether the migration held a lock that
// conflicts with writes (ROW EXCLUSIVE) on a relation that existed BEFORE the run
// — the only kind of lock that can actually freeze the application's writes.
//
// The MONITOR is the arbiter when it has data. monitor.LockModes are precisely
// the modes the migration held on PRE-EXISTING relations: a CREATE INDEX
// CONCURRENTLY build takes ACCESS EXCLUSIVE on the NEW index it is creating, but
// that OID did not exist when the run began and is excluded, so what remains is
// the SHARE UPDATE EXCLUSIVE it holds on the table — which does not conflict with
// writes. An observed set of modes is therefore empirical ground truth, and is
// preferred over the static shape prediction. When the monitor never got to see a
// held mode (it was degraded, or the lock was too brief to sample), fall back to
// the shape prediction, which errs toward "blocks" — so a freeze is never
// suppressed on a shape we could not observe and cannot vouch for.
func migrationHeldWriteBlockingLock(as *Assessment, mon monitor.Report) bool {
	if len(mon.LockModes) > 0 {
		for _, m := range mon.LockModes {
			if conflictsWithWriteHold(m) {
				return true
			}
		}
		return false
	}
	return as != nil && as.Lesson.BlocksWrites
}

// qualifierFor decides whether this run's SAFE is the whole answer.
//
// It is not, for the entire class of statements whose danger is the WAIT rather
// than the work: DROP INDEX, ALTER INDEX ... RENAME, a metadata-only ALTER
// TABLE. Each takes AccessExclusiveLock, which conflicts with every other mode
// including plain SELECT. On a twin where nothing holds a transaction open they
// get that lock instantly, do a catalog write, and release — nobody waits, no
// interval collapses, verdict SAFE. In production the same statement queues
// behind the longest open transaction on the table, and every statement that
// arrives after it queues behind IT.
//
// A default run does not simulate that transaction, so it cannot say anything
// about it. Saying nothing would leave a bare green light on the most
// misunderstood class of migration there is; this says which half was measured.
func qualifierFor(res *RunResult, as *Assessment, mon monitor.Report) string {
	if res.Verdict != VerdictSafe {
		return ""
	}
	if res.Contention.Enabled {
		// Contention was real and the statement still did not freeze traffic.
		// That is a measurement, not an assumption.
		return ""
	}
	if res.Contention.Requested {
		// Hostile was asked for but the DDL never actually contended — no hold was
		// held in time, or its lock did not conflict with the hold. The SAFE
		// verdict is a default run's, and it must say the acquisition wait was not
		// exercised rather than read as a passed contention test.
		return QualifierContentionNotEngaged
	}
	if !takesAccessExclusive(as, mon) {
		return ""
	}
	if res.BlockedSeconds > 0 || res.Casualties.Total > 0 {
		// A hold WAS measured; the verdict is about something that happened.
		return ""
	}
	return QualifierAcquisitionNotSimulated
}

// failureModeFor separates "a lock stopped the writes" from "the writes stopped
// working". They look identical on the write-rate trace and mean opposite
// things.
//
// The canonical case is a rename. After ALTER TABLE orders RENAME TO
// orders_renamed, every INSERT INTO orders fails with 42P01; the rate goes to
// zero, no lock is held by anybody, and the casualty count stays at zero because
// nothing waited on a lock. Reading that as an outage caused by ACCESS EXCLUSIVE
// is wrong and buries the real finding.
func failureModeFor(res *RunResult, mon monitor.Report) string {
	if res.ErroredDuringDDL <= 0 || res.Casualties.Total > 0 {
		return ""
	}
	if res.BlockedSeconds <= 0 {
		// Nothing to misattribute.
		return ""
	}
	// Only when the server itself says nobody was ever waiting. A degraded or
	// never-started monitor is not evidence of absence, and in that case the
	// conservative lock narrative stands.
	if mon.Samples == 0 || mon.BlockedPeakCount > 0 || mon.BlockedSeconds > 0 {
		return ""
	}
	return FailureModeWritesErrored
}

// noteFor adds the one line of context the numbers cannot carry themselves.
// Every branch here is about being honest when the evidence is thin or the
// measurement disagrees with the prediction.
//
// Order is precedence: the first true branch wins, so the branches that correct
// a WRONG reading of the numbers come before the ones that merely add colour.
func noteFor(res *RunResult, as *Assessment, mon monitor.Report) string {
	return appendDeadlineNote(baseNote(res, as, mon), res)
}

// explanationFor keeps the static per-class lesson by default, but rewrites its
// tail in the one case where the static text and the measurement contradict each
// other on the same card.
//
// The metadata-only shapes (Lesson.TriviallySafe / BlocksWrites==false) close on
// the same teaching move: the change is catalog-only, so "the risk is not the
// lock itself, it is waiting to GET the lock behind a long-running query. There
// is nothing here for Brace to crash." That conclusion is correct in isolation
// and on the default path — but hostile mode holds exactly such a transaction
// open, and when it actually engaged and froze traffic the card would otherwise
// read UNSAFE / N.Ns frozen AND "nothing here to crash" at once. So in that one
// case we strip the reassuring conclusion and land the measured wait instead —
// the mechanism half of the text is right and stays put; only the false
// all-clear is replaced, turning the contradiction into the lesson.
//
// Every other path (default / uncontended, or a shape whose lesson already
// claims it blocks) is returned unchanged: there the "nothing to crash" tail
// plus the acquisition_not_simulated qualifier is exactly the honest reading.
func explanationFor(res *RunResult, as *Assessment) string {
	base := as.Lesson.Explanation
	if res.Verdict == VerdictErrored || res.Verdict == VerdictInconclusive {
		// The statement never produced a measurement — it failed to execute, or the
		// guard aborted it before it finished. The per-class lesson still describes
		// the SHAPE, but nothing here was measured, so it must be framed as general
		// behaviour rather than a finding about this run. Anything measured-sounding
		// on a run that never ran (a freeze story, a scale projection) is a lie.
		return "This statement did not run to a measured result, so the following is the general behaviour of this " +
			"shape, not a measurement of this run. " + base
	}
	claimsSafe := !as.Lesson.BlocksWrites || as.Lesson.TriviallySafe
	froze := res.Verdict == VerdictUnsafe || res.BlockedSeconds > unsafeThresholdSeconds
	if !res.Contention.Enabled || !claimsSafe || !froze {
		return base
	}
	// Drop the reassuring conclusion if the lesson carries it, keeping the
	// (correct) catalog-only mechanism that precedes it.
	const reassurance = "There is nothing here for Brace to crash."
	base = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(base), reassurance))
	// Complement the note rather than repeat it: the note explains the hold and
	// the pile-up; here we retire the class-level all-clear with this run's number.
	return base + fmt.Sprintf(" In isolation that wait is nothing. Hostile mode held exactly such a "+
		"transaction open — and this run measured the wait: writes froze for %.1fs while the statement "+
		"queued for its lock, and everything behind it queued too.", res.BlockedSeconds)
}

// appendDeadlineNote labels the casualty count with the deadline that defines
// it. A casualty is a write that waited longer than this run's simulated
// lock_timeout — an application that sets none suffers the same freeze with zero
// casualties, and one with a 200ms budget suffers many times more. The number is
// meaningless without the assumption, so they travel together.
func appendDeadlineNote(note string, res *RunResult) string {
	if res.Casualties.Total <= 0 || res.Casualties.DeadlineMS <= 0 {
		return note
	}
	line := fmt.Sprintf("The %d killed writes are writes that waited longer than the %dms request deadline "+
		"this run simulated; with a different deadline the freeze is the same and this count is not.",
		res.Casualties.Total, res.Casualties.DeadlineMS)
	if note == "" {
		return line
	}
	return note + " " + line
}

func baseNote(res *RunResult, as *Assessment, mon monitor.Report) string {
	switch {
	case res.Verdict == VerdictInconclusive:
		return "Brace stopped the run at its time limit: the migration never acquired its lock while traffic " +
			"ran, so it could not complete under load. That is not a Brace failure — it is the finding. A change " +
			"that cannot finish behind live writes here will not finish behind them in production either; look for " +
			"a long-running query or an idle-in-transaction session holding the table, or rewrite it to take a " +
			"weaker lock (CONCURRENTLY, or the NOT VALID two-step)."
	case len(res.InvalidIndexes) > 0:
		// Checked BEFORE the generic error branch: a cancelled or failed
		// CONCURRENTLY build usually ALSO sets res.Error, but the actionable
		// signal is the INVALID leftover it left behind, not merely "it failed".
		return "A CONCURRENTLY build left an INVALID index behind. It costs write overhead but will never be used " +
			"by a query — drop it and retry."
	case res.FailureMode == FailureModeWritesErrored:
		// The write rate collapsed and no lock was held by anyone: the traffic's
		// own statements stopped working. Narrating a lock outage here would be
		// false, and it would bury the finding that actually matters.
		return fmt.Sprintf("Writes stopped, but NOT because of a lock: the monitor saw nobody waiting, and no write "+
			"was killed by the request deadline. %d writes failed outright — the statements the application sends no "+
			"longer match the schema this migration produced (a renamed or dropped object is the usual cause). That is "+
			"exactly what a live application still referencing the old name would suffer, so the code that uses the new "+
			"name has to be deployed and able to serve traffic BEFORE this migration runs. The lock itself was cheap; "+
			"the incompatibility is the outage.", res.ErroredDuringDDL)
	case res.Error != "":
		return "The migration itself failed, so the freeze measurement covers only the time before it gave up."
	case res.BaselineWPS == 0:
		return "No baseline traffic was measured, so the freeze window could not be computed from the write rate. " +
			"Treat the monitor's figures as the only evidence for this run."
	case res.Contention.Enabled && res.Verdict == VerdictUnsafe:
		// Placed ABOVE the "blocked though its shape usually does not" branch below:
		// hostile mode's headline case is precisely an engaged run on a shape that
		// normally does not block writes (!BlocksWrites), so that branch would
		// otherwise mask this one and wrongly imply the statement surprised us,
		// hiding that Brace's own hold created the queue.
		return fmt.Sprintf("This is a HOSTILE-mode measurement: Brace held a transaction open on the load target for "+
			"%.1fs at a time, so the migration had to queue for its lock — and every write that arrived after that "+
			"queued request waited behind it. That pile-up is the %.2fs freeze above. It is a measurement of this "+
			"migration behind a %.1fs transaction, not a property of the migration alone: if nothing on your table "+
			"ever stays open that long, the freeze is shorter, and if something stays open longer, it is longer.",
			res.Contention.HoldSeconds, res.BlockedSeconds, res.Contention.HoldSeconds)
	case res.Contention.Enabled && res.Verdict == VerdictSafe:
		return fmt.Sprintf("Measured with contention: Brace held a transaction open on the load target for %.1fs at a "+
			"time while the migration ran, and writes still kept flowing. That is the acquisition risk tested rather "+
			"than assumed away — the strongest green light this rig can give.", res.Contention.HoldSeconds)
	case res.Verdict == VerdictSafe && !as.Lesson.BlocksWrites && res.Casualties.Total == 0 &&
		res.BlockedSeconds > scaleFloorSeconds && len(mon.LockModes) > 0 &&
		!migrationHeldWriteBlockingLock(as, mon):
		// The write rate dipped, but the monitor confirms the migration held only a
		// non-write-blocking lock — the CREATE INDEX CONCURRENTLY / VACUUM case. The
		// dip is I/O and CPU contention on a shared box, not a lock freeze, so the
		// verdict stays SAFE and no freeze is projected. Say so, honestly, and name
		// the lock the monitor actually saw.
		return fmt.Sprintf("The write rate dipped while the statement ran, but the monitor never saw a write-blocking "+
			"lock: the migration held only %s, which does not conflict with the ROW EXCLUSIVE lock every INSERT, UPDATE "+
			"and DELETE takes. That dip is I/O and CPU contention from the build competing for the disk on a shared box, "+
			"not a lock freeze — writes kept committing and nobody was blocked. On a bigger table the same non-blocking "+
			"lock is simply held for longer, so there is no freeze to project.", mon.StrongestLockMode)
	case res.Verdict == VerdictUnsafe && !as.Lesson.BlocksWrites:
		return "This statement blocked writes even though its shape usually does not. Worth reading the timeline " +
			"rather than trusting the pattern."
	case res.Contention.Requested && !res.Contention.Enabled:
		// Asked for hostile, did not get it. The numbers are a default run's and
		// must not be read as anything harsher.
		return "Hostile mode was requested but did not engage (" + res.Contention.Note + "), so these numbers " +
			"describe an UNCONTENDED table — read them as a default run, not as a contended one."
	case res.Qualifier == QualifierAcquisitionNotSimulated && res.ScaleWarning == "":
		// The false-SAFE this rig used to ship. It wins over the acquisition note
		// ONLY when there is no scale warning: for a constant-time ACCESS EXCLUSIVE
		// shape the projection is suppressed and the acquisition wait is the whole
		// story. But an O(rows) shape (SET NOT NULL, ALTER COLUMN TYPE) that
		// measured SAFE on the twin still gets a quantified scale veto — an
		// EffectiveVerdict of UNSAFE with a number — and that veto must not be
		// hidden behind the softer "you didn't test acquisition" line.
		return fmt.Sprintf("No freeze was measured — and this run did not test the thing that actually makes %s "+
			"dangerous. It takes ACCESS EXCLUSIVE, which conflicts with every other lock mode including plain SELECT. "+
			"Here it got that lock instantly because nothing was holding the table. In production it waits behind your "+
			"longest open transaction, and because PostgreSQL grants locks in arrival order, every query that arrives "+
			"after it waits too — the table stops until that transaction ends. The freeze you would see is the length "+
			"of that transaction, which this run did not simulate. Re-run with hostile mode (hostile: true, or "+
			"BRACE_HOSTILE=on) to measure it.", strings.TrimSpace(as.Lesson.Title))
	case res.ScaleWarning != "":
		// Placed above the generic "on a larger table it is held for longer"
		// hand-wave below: when the operator has told us their real size we can
		// say how much longer, with a number, and that sentence is the note.
		return res.ScaleWarning
	case res.Verdict == VerdictSafe && as.Lesson.BlocksWrites && res.Casualties.Total == 0 && as.Lesson.ScalesWithRows:
		return "No freeze was measured, but this shape does take a blocking lock — on this twin the statement " +
			"finished faster than the write traffic could notice. Its work is O(rows), so on a larger table the same " +
			"lock is held for longer. Set BRACE_TARGET_ROWS (or pass target_rows) and Brace will project this " +
			"measurement onto your real size."
	case res.Verdict == VerdictSafe && as.Lesson.BlocksWrites && res.Casualties.Total == 0:
		// Constant-time shapes. Telling this reader that a bigger table makes it
		// worse would be false — and the advice that follows it (set
		// BRACE_TARGET_ROWS) yields a suppressed projection anyway, because
		// scaling a catalog write by row count models nothing.
		return "No freeze was measured, and this shape's work does not grow with the table: it is a catalog change, " +
			"the same microseconds at ten thousand rows and at a billion. A bigger table does not make it slower, so " +
			"there is no scale projection to make. The remaining risk is the wait to ACQUIRE the lock behind whatever " +
			"is already running — re-run with hostile mode to measure that."
	case mon.Degraded():
		return "The lock monitor lost some samples, so its figures are a floor. The write-rate measurement is " +
			"unaffected."
	case res.Verdict == VerdictUnsafe:
		return "Writes stopped for longer than a second. At production traffic that is the same shape of outage, " +
			"scaled by however much busier production is than this twin."
	}
	return ""
}

func newRunID() (string, error) {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("engine: generate run id: %w", err)
	}
	return "r" + hex.EncodeToString(b[:]), nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
