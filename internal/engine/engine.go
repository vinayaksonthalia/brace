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
)

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
	// LockTimeout is the simulated request deadline on write traffic.
	// Defaults to 1500ms.
	LockTimeout time.Duration
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
	// CloneETA is how long a twin clone takes on this deployment; it is only
	// used to give a 503 an honest-ish "come back in N seconds". Defaults to 45s.
	CloneETA time.Duration
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
	if c.CloneETA <= 0 {
		c.CloneETA = defaultCloneETA
	}
	if c.MaxRunDuration <= 0 {
		c.MaxRunDuration = defaultMaxRunDuration
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// Casualties is the frozen dashboard shape for killed writes.
type Casualties struct {
	Total   int64            `json:"total"`
	ByLabel map[string]int64 `json:"by_label"`
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
	// a lock. Never folded into casualties.
	ErroredTotal int64 `json:"errored_total"`
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
}

// run phases, in order.
const (
	phaseAcquiring = "acquiring_twin"
	phaseGranting  = "granting"
	phaseWarming   = "starting_load"
	phaseBaseline  = "measuring_baseline"
	phaseMigrating = "running_migration"
	phaseRecovery  = "observing_recovery"
	phaseReporting = "reporting"
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

	e := &Engine{
		cfg:              cfg,
		log:              cfg.Logger.With("component", "engine"),
		pool:             pool,
		ledger:           NewLedger(pool),
		migratorPassword: password,
	}
	if err := ensureMigratorRole(ctx, pool, cfg.MigratorRole, password); err != nil {
		pool.Close()
		return nil, err
	}
	if err := e.ledger.CreateSchema(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	labelSource := "default"
	if len(cfg.Labels) > 0 {
		labelSource = "custom"
	}
	e.log.Info("engine.ready", "migrator_role", cfg.MigratorRole, "workers", cfg.Workers,
		"labels", labelSource, "label_count", len(cfg.Labels))
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
	ro := runOptions{TargetRows: e.cfg.DefaultTargetRows}
	for _, opt := range opts {
		if opt != nil {
			opt(&ro)
		}
	}
	mode, err := mode.normalize()
	if err != nil {
		return nil, err
	}
	as, err := Assess(sql)
	if err != nil {
		// Includes the "already safe, don't waste a twin" case. No twin has
		// been touched at this point, which is the whole point of pre-filtering
		// before acquiring.
		return nil, err
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

// RunOption is a per-run override.
type RunOption func(*runOptions)

type runOptions struct {
	// TargetRows is the operator's production row count for THIS run, defaulting
	// to Config.DefaultTargetRows. An invalid value is dropped rather than
	// rejected: the projection is an extra, and refusing to rehearse a migration
	// because a row count was mistyped would be the wrong trade.
	TargetRows int64
}

// WithTargetRows answers "what about MY table?" for one run. Non-positive or
// absurd values (> 1e12) are ignored, leaving whatever the deployment's
// BRACE_TARGET_ROWS said.
func WithTargetRows(n int64) RunOption {
	return func(o *runOptions) {
		if v := validTargetRows(n); v > 0 {
			o.TargetRows = v
		}
	}
}

// runOn is the measured part of a run: everything after the twin exists and
// before it is destroyed.
func (e *Engine) runOn(ctx context.Context, twin *twins.Twin, as *Assessment, mode Mode, state *RunState, ro runOptions) (*RunResult, error) {
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

	// 3. Twin-local privileges for the unprivileged migrator role.
	e.setPhase(phaseGranting)
	grantCtx, cancelGrant := context.WithTimeout(runCtx, grantTimeout)
	err := grantOnTwin(grantCtx, twin.DSN, twin.Name, e.cfg.MigratorRole)
	cancelGrant()
	if err != nil {
		return nil, err
	}

	// 4. Live traffic.
	e.setPhase(phaseWarming)
	gen, err := loadgen.New(runCtx, loadgen.Config{
		DSN:         twin.DSN,
		Workers:     e.cfg.Workers,
		LockTimeout: e.cfg.LockTimeout,
		Labels:      e.cfg.Labels,
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

	// 5. The monitor, then the crash.
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
	res.Verdict = verdictFor(res.BlockedSeconds, res.Casualties.Total)
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
	res.Casualties = casualtiesFrom(final)
	res.ErroredTotal = final.ErroredTotal

	// 10. The scale layer. The twin is a SAMPLE, and a freeze measured on a
	// sample is not the freeze production gets: lock presence is scale-free, lock
	// duration is not. Verdict above stays exactly what was measured;
	// EffectiveVerdict is what a deploy gate should read.
	res.ScaleProjection = projectScale(scaleInputs{
		Measured:       res.BlockedSeconds,
		TwinRows:       e.cfg.TwinRows,
		TargetRows:     ro.TargetRows,
		ScalesWithRows: as.Lesson.ScalesWithRows,
		BlocksWrites:   as.Lesson.BlocksWrites,
		Verdict:        res.Verdict,
	})
	res.EffectiveVerdict = effectiveVerdict(res.Verdict, res.ScaleProjection)
	res.ScaleWarning = scaleWarning(res.Verdict, res.ScaleProjection)

	res.Note = noteFor(res, as, monReport)

	e.log.Info("engine.run_finished",
		"run", state.RunID, "twin", twin.Name, "verdict", res.Verdict,
		"effective_verdict", res.EffectiveVerdict,
		"projected_blocked_s", res.ScaleProjection.ProjectedBlockedSeconds,
		"target_rows", res.ScaleProjection.TargetRows,
		"blocked_s", res.BlockedSeconds, "monitor_blocked_s", res.MonitorBlockedSeconds,
		"casualties", res.Casualties.Total, "errored", res.ErroredTotal,
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

// noteFor adds the one line of context the numbers cannot carry themselves.
// Every branch here is about being honest when the evidence is thin or the
// measurement disagrees with the prediction.
func noteFor(res *RunResult, as *Assessment, mon monitor.Report) string {
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
	case res.Error != "":
		return "The migration itself failed, so the freeze measurement covers only the time before it gave up."
	case res.BaselineWPS == 0:
		return "No baseline traffic was measured, so the freeze window could not be computed from the write rate. " +
			"Treat the monitor's figures as the only evidence for this run."
	case res.Verdict == VerdictUnsafe && !as.Lesson.BlocksWrites:
		return "This statement blocked writes even though its shape usually does not. Worth reading the timeline " +
			"rather than trusting the pattern."
	case res.ScaleWarning != "":
		// Placed above the generic "on a larger table it is held for longer"
		// hand-wave below: when the operator has told us their real size we can
		// say how much longer, with a number, and that sentence is the note.
		return res.ScaleWarning
	case res.Verdict == VerdictSafe && as.Lesson.BlocksWrites && res.Casualties.Total == 0:
		return "No freeze was measured, but this shape does take a blocking lock — on this twin the statement " +
			"finished faster than the write traffic could notice. On a larger table the same lock is held for longer. " +
			"Set BRACE_TARGET_ROWS (or pass target_rows) and Brace will project this measurement onto your real size."
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
