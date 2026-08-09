package engine

// Multi-statement migration sequences.
//
// A real migration is a FILE, not one statement: ADD COLUMN, then backfill,
// then CREATE INDEX on the new column, then a constraint. The single-statement
// path deliberately rejects that — every headline number it produces (the freeze
// window, the casualty count) is attributed to ONE statement, and it acquires a
// fresh twin per run, so statement 2 would never see statement 1's effect (the
// canonical failure: `CREATE INDEX ON orders (promo)` ERRORs "column does not
// exist" because the `ADD COLUMN promo` ran against a different, discarded twin).
//
// This file adds an OPT-IN sequence mode: N allow-listed statements run IN ORDER
// on a SINGLE acquired twin, under the SAME load generator, each measured in its
// own window. Between statements the twin persists, so statement 2 sees statement
// 1's committed effect — which is the whole point, and what finally lets Brace
// rehearse (and verify) its own suggested safe rewrite as the multi-step script
// it actually is.
//
// TWO invariants are load-bearing and are the reason this is a separate, additive
// runner rather than a relaxation of the existing one:
//
//  1. The single-statement flow is UNCHANGED. runOn is not touched; the sequence
//     runner REUSES the same measurement primitives (the sampler, the monitor, the
//     verdict arithmetic in verdict.go, the scale projection in scale.go, the
//     grants and roles in migrate.go) instead of forking them.
//
//  2. The security model is UNCHANGED and is NOT relaxed. A sequence is not a way
//     to smuggle a non-allow-listed verb past the guard: EVERY statement is put
//     through the SAME Assess() the single path uses, so the allow-list (CREATE
//     INDEX, ALTER TABLE, REINDEX, VACUUM, ANALYZE, ADD CONSTRAINT, ...) holds for
//     every step, and Assess's own splitter still rejects a `;`-smuggled second
//     statement inside any one element. The statements still run as the
//     unprivileged brace_migrator (NOSUPERUSER); traffic still runs as
//     brace_traffic. No new privilege is introduced anywhere.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/vinayaksonthalia/brace/internal/loadgen"
	"github.com/vinayaksonthalia/brace/internal/monitor"
	"github.com/vinayaksonthalia/brace/internal/twins"
)

const (
	// maxSequenceStatements caps how many statements one sequence may carry. A
	// migration file with more than ten interesting DDL statements is past what a
	// single rehearsal can measure legibly, and — because the whole sequence runs
	// under one MaxRunDuration budget on one held twin — an unbounded count is also
	// a way to monopolise the single run slot. The cap is UX and blast-radius
	// hygiene, exactly like assess.go's single-statement guards; it is not the
	// security boundary.
	maxSequenceStatements = 10

	// maxSequenceBytes caps the TOTAL size of the sequence. Each individual
	// statement is still separately bounded by maxSQLBytes inside Assess; this is
	// the ceiling around the whole script so nobody can post a novel at the
	// endpoint by splitting it across ten fields.
	maxSequenceBytes = 16 << 10

	// VerdictSkipped marks a sequence statement that was NOT run because an earlier
	// statement failed — a real migration halts at the first error, so the steps
	// after it never execute. It appears only inside the per-statement array, never
	// as a top-level verdict, and it is not a measurement: treat it as "unknown,
	// because the run stopped before reaching it".
	VerdictSkipped = "SKIPPED"
)

// StatementResult is one statement's finding within a sequence. It is the
// per-statement slice of the frozen contract: the same verdict / freeze /
// casualty / lock-mode fields a single run reports, windowed onto this one
// statement's execution on the shared twin.
type StatementResult struct {
	// Index is the statement's position in the submitted sequence, 0-based.
	Index int `json:"index"`
	// SQL is the statement as submitted, trimmed.
	SQL string `json:"sql"`
	// Classification is the recognised shape, exactly as the single path assigns
	// it.
	Classification Classification `json:"classification"`
	// Verdict is this statement's MEASURED verdict on the twin — SAFE / UNSAFE /
	// ERRORED / INCONCLUSIVE, or SKIPPED if an earlier statement halted the run.
	Verdict string `json:"verdict"`
	// EffectiveVerdict is Verdict escalated by this statement's own scale
	// projection, so a step that is SAFE on the twin but UNSAFE at the operator's
	// real size reads UNSAFE here.
	EffectiveVerdict string `json:"effective_verdict"`
	// BlockedSeconds is the longest write-freeze measured inside THIS statement's
	// window on the shared timeline.
	BlockedSeconds float64 `json:"blocked_seconds"`
	// Casualties are the writes killed waiting for a lock during this statement's
	// window — the counters diffed across exactly its execution, so a death caused
	// by an earlier or later statement is never charged to it.
	Casualties Casualties `json:"casualties"`
	// LockModes are the modes the monitor actually saw THIS statement hold,
	// strongest first.
	LockModes []string `json:"lock_modes"`
	// DurationSeconds is how long this statement's Exec took.
	DurationSeconds float64 `json:"duration_seconds"`
	// Explanation is the teach text for this statement's shape.
	Explanation string `json:"explanation"`
	// ShapeNote carries this statement's GENERAL shape behaviour when the step
	// produced no measurement (ERRORED / INCONCLUSIVE / SKIPPED). Explanation then
	// states only that the step measured nothing; the shape lesson lives here,
	// explicitly not-a-measurement, so it is never quoted as this step's finding.
	ShapeNote string `json:"shape_note,omitempty"`
	// Suggestion is the safe rewrite for this statement, when one exists.
	Suggestion string `json:"suggestion,omitempty"`
	// Error is this statement's own failure, if PostgreSQL refused it. A non-empty
	// Error on a statement is what stops the ones after it from running.
	Error string `json:"error,omitempty"`
	// ScaleProjection is this statement's projection onto the operator's real
	// table size; empty (Known()==false) when nothing could be projected.
	ScaleProjection ScaleProjection `json:"scale_projection"`
	// ScaleWarning is non-empty exactly when this statement measured SAFE but
	// projects UNSAFE.
	ScaleWarning string `json:"scale_warning,omitempty"`
	// MonitorBlockedSeconds is the server-side corroboration of BlockedSeconds for
	// this statement's window.
	MonitorBlockedSeconds float64 `json:"monitor_blocked_seconds"`
	// DDLStartT / DDLEndT delimit this statement within the shared timeline, both
	// in seconds since load start, so the UI can shade each statement's window.
	DDLStartT float64 `json:"ddl_start_t"`
	DDLEndT   float64 `json:"ddl_end_t"`
	// Qualifier names what this statement's SAFE did NOT establish, mirroring the
	// single path. In a sequence the only one that can apply is
	// acquisition_not_simulated: the statement took ACCESS EXCLUSIVE but, on a twin
	// nobody was holding, got the lock instantly, so the production wait to ACQUIRE
	// it was never exercised. Empty for a genuinely unconditional SAFE.
	Qualifier string `json:"qualifier,omitempty"`
	// Note is the one-line caveat that travels with Qualifier.
	Note string `json:"note,omitempty"`
}

// stmtPlan is a validated statement plus what Assess learned about it. staticSafe
// records that Assess would have short-circuited this shape to already_safe on the
// single path — in a sequence it must still EXECUTE (so a later statement sees its
// effect), but it is expected to draw a flat line, and marking it keeps that
// intent explicit.
type stmtPlan struct {
	as         *Assessment
	staticSafe bool
}

// assessSequence puts every statement through the SAME pre-filter the single path
// uses, and enforces the count and total-byte caps. It returns the validated plan
// or a *RejectedError naming the offending statement.
//
// The one deliberate difference from the single path: an already_safe answer is
// ADMITTED, not refused. On the single path Assess turns a catalog-only shape away
// with an already_safe rejection to avoid spending a twin proving a flat line. In
// a sequence the twin is already in hand and a later statement DEPENDS on this one
// having run (ADD COLUMN with a constant default is already_safe, yet the CREATE
// INDEX after it needs the column to exist), so it is kept and executed. Every
// OTHER rejection — a non-allow-listed verb, a `;`-smuggled second statement, an
// empty or over-long element — rejects the WHOLE sequence, because a sequence must
// never be a way to relax the allow-list.
func assessSequence(stmts []string, ro runOptions) ([]stmtPlan, error) {
	if len(stmts) == 0 {
		return nil, &RejectedError{
			Kind:   RejectRejected,
			Code:   "sequence_empty",
			Reason: "No statements were provided. Submit a sequence as a list of one or more migration statements to run in order.",
		}
	}
	if len(stmts) > maxSequenceStatements {
		return nil, &RejectedError{
			Kind: RejectRejected,
			Code: "sequence_too_many",
			Reason: fmt.Sprintf("A sequence may contain at most %d statements; this one has %d. Brace rehearses a bounded "+
				"migration script on one twin — split a larger migration and rehearse the interesting stretch.",
				maxSequenceStatements, len(stmts)),
		}
	}
	total := 0
	for _, s := range stmts {
		total += len(s)
	}
	if total > maxSequenceBytes {
		return nil, &RejectedError{
			Kind: RejectRejected,
			Code: "sequence_too_long",
			Reason: fmt.Sprintf("The sequence is larger than Brace accepts (limit %d KB across all statements). Trim it to "+
				"the statements you want to rehearse.", maxSequenceBytes>>10),
		}
	}

	plans := make([]stmtPlan, 0, len(stmts))
	for i, raw := range stmts {
		as, err := Assess(raw)
		if err != nil {
			var rej *RejectedError
			if errors.As(err, &rej) && rej.Kind == RejectAlreadySafe {
				// Catalog-only, but it must still run so the statements after it see
				// its effect. Keep it and mark it.
				plans = append(plans, stmtPlan{as: as, staticSafe: true})
				continue
			}
			// Anything else — not allow-listed, multi-statement, empty, too long —
			// fails the whole sequence, with the offending position named so the
			// caller can fix exactly that statement.
			return nil, sequenceStatementRejected(i, err)
		}
		// The hostile carve-out (an already_safe ACCESS EXCLUSIVE shape let through
		// for hostile mode) is irrelevant here: assessForRun would only matter for
		// the already_safe path, which we admit anyway. Assess's plain result is
		// exactly what we want to run and measure.
		_ = ro
		plans = append(plans, stmtPlan{as: as})
	}
	return plans, nil
}

// sequenceStatementRejected wraps a per-statement Assess rejection so the caller
// learns WHICH statement in the sequence was refused, preserving the original
// reason and suggestion.
func sequenceStatementRejected(index int, err error) error {
	var rej *RejectedError
	if !errors.As(err, &rej) {
		return err
	}
	return &RejectedError{
		Kind:           rej.Kind,
		Code:           rej.Code,
		Reason:         fmt.Sprintf("Statement %d of the sequence was rejected: %s", index+1, rej.Reason),
		Suggestion:     rej.Suggestion,
		Classification: rej.Classification,
	}
}

// AssessSequence is the synchronous pre-check the HTTP layer runs so a bad
// sequence is a 400 on the submitting request rather than an ERRORED run seconds
// later. It validates the caps and every statement's shape; unlike AssessRun it
// does NOT run the already_safe existence gate, because a sequence's later
// statements legitimately reference relations (a just-added column) that do not
// exist until an earlier statement runs.
func (e *Engine) AssessSequence(_ context.Context, stmts []string, opts ...RunOption) ([]stmtPlan, error) {
	return assessSequence(stmts, e.resolveOptions(opts))
}

// RehearseSequence crash-tests an ordered sequence of statements against ONE
// disposable twin, measuring each statement in its own window.
//
// The error contract matches Rehearse (so the API maps it identically): a
// *RejectedError for anything the user can fix, ErrRunInProgress / *NoWarmTwinError
// for the transport states, and a (*RunResult, nil) whose per-statement array
// carries any statement PostgreSQL refused — a statement that ERRORs is a finding,
// not an engine error, exactly as on the single path.
func (e *Engine) RehearseSequence(ctx context.Context, stmts []string, mode Mode, opts ...RunOption) (*RunResult, error) {
	ro := e.resolveOptions(opts)
	mode, err := mode.normalize()
	if err != nil {
		return nil, err
	}
	plans, err := assessSequence(stmts, ro)
	if err != nil {
		return nil, err
	}

	runID, err := newRunID()
	if err != nil {
		return nil, err
	}
	joined := joinSequenceSQL(plans)
	state := &RunState{
		RunID:     runID,
		Mode:      mode,
		SQL:       joined,
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
	// The twin is released no matter how this function leaves — the same detached
	// teardown the single path uses, for the same reason: a leaked twin stays on
	// disk until the next restart.
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
	e.log.Info("engine.sequence_started", "run", runID, "twin", twin.Name, "mode", mode, "statements", len(plans))

	return e.runSequenceOn(ctx, twin, plans, mode, state, ro)
}

// runSequenceOn is the measured part of a sequence run: shared infrastructure
// (grants, load, sampler, baseline) is stood up ONCE, then each statement is run
// and measured in turn on the persistent twin.
//
// It is deliberately parallel to runOn and shares its every primitive. The only
// structural difference is the loop: where runOn measures one DDL window with a
// trailing recovery, this measures many windows on one continuous timeline.
func (e *Engine) runSequenceOn(ctx context.Context, twin *twins.Twin, plans []stmtPlan, mode Mode, state *RunState, ro runOptions) (*RunResult, error) {
	labels := e.cfg.Labels
	if len(ro.Labels) > 0 {
		labels = ro.Labels
	}
	twinRows := e.cfg.TwinRows
	if ro.TwinRows > 0 {
		twinRows = ro.TwinRows
	}

	res := &RunResult{
		RunID:      state.RunID,
		StartedAt:  state.StartedAt,
		Twin:       twin.Name,
		Mode:       mode,
		SQL:        state.SQL,
		Casualties: Casualties{ByLabel: map[string]int64{}},
		LockModes:  []string{},
		Statements: make([]StatementResult, 0, len(plans)),
		// Contention is not applied per-statement in a sequence — a sequence answers
		// "does this script freeze writes as it runs", which the default (uncontended)
		// measurement already answers per statement. Report it plainly as not enabled.
		Contention: Contention{},
	}

	// The whole sequence runs under one MaxRunDuration budget, exactly as one DDL
	// does: everything measured from here runs under runCtx, which fires after the
	// budget no matter which statement is executing. A sequence that cannot finish
	// under load is itself the finding.
	runCtx, cancelRun := context.WithTimeout(ctx, e.cfg.MaxRunDuration)
	defer cancelRun()
	guardFired := func() bool {
		return ctx.Err() == nil && errors.Is(runCtx.Err(), context.DeadlineExceeded)
	}

	// Grants for both unprivileged roles, once — the same calls runOn makes.
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

	// Live traffic as the unprivileged traffic role, once.
	trafDSN, err := trafficDSN(twin.DSN, e.cfg.TrafficRole, e.trafficPassword)
	if err != nil {
		return nil, fmt.Errorf("engine: derive traffic DSN: %w", err)
	}
	e.setPhase(phaseWarming)
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

	smp := newSampler(gen, e.cfg.SampleInterval)
	smp.start()
	defer smp.stop()
	e.mu.Lock()
	e.curGen = gen
	e.curSampler = smp
	e.mu.Unlock()

	// Baseline, once — identical to the single path.
	e.setPhase(phaseBaseline)
	baselineFrom := time.Now()
	if !sleepCtx(runCtx, e.cfg.SettleDuration) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, runCtx.Err()
	}
	baselineTo := time.Now()
	baseline := smp.rateBetween(baselineFrom, baselineTo)
	if baseline <= 0 {
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

	// One migrator connection for the WHOLE sequence, opened once: a migration
	// file runs its statements on a single session, and reusing the connection is
	// what makes statement 2 see statement 1's committed effect (each Exec is
	// autocommit, exactly as on the single path).
	migCtx, cancelMig := context.WithTimeout(runCtx, migratorConnTimeout)
	migConn, err := connectAsMigrator(migCtx, twin.DSN, e.cfg.MigratorRole, e.migratorPassword)
	cancelMig()
	if err != nil {
		return nil, err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		_ = migConn.Close(closeCtx)
		cancel()
	}()

	// Per-statement execution. Each statement gets its OWN monitor (started before
	// it, stopped after) so the lock modes and the server-side blocked-seconds are
	// attributed to exactly that statement — the same monitor the single path uses,
	// just cycled per statement. The heavy verdict/scale arithmetic is deferred to a
	// second pass once the shared timeline is complete, so each statement's freeze
	// can be read off the window it actually occupies.
	type measured struct {
		plan     stmtPlan
		report   monitor.Report
		startT   float64
		endT     float64
		duration float64
		casualty Casualties
		execErr  error
		guarded  bool
	}
	runsMeasured := make([]measured, 0, len(plans))
	anyConcurrent := false
	halted := false
	var haltedAt int

	for i := range plans {
		p := plans[i]
		if p.as.Concurrent {
			anyConcurrent = true
		}
		e.setPhase(phaseMigrating)

		mon := monitor.New(monitor.Config{
			DSN:      twin.DSN,
			AppName:  MigrationAppName,
			Interval: e.cfg.SampleInterval,
			Logger:   e.cfg.Logger,
		})
		if err := mon.Start(runCtx); err != nil {
			e.log.Warn("engine.monitor_unavailable", "run", state.RunID, "statement", i, "err", err)
		}

		startStats := gen.Snapshot()
		t0 := time.Now()
		startT := smp.since(t0)
		_, execErr := migConn.Exec(runCtx, p.as.SQL)
		t1 := time.Now()
		endT := smp.since(t1)
		monReport := mon.StopAndReport()
		endStats := gen.Snapshot()

		guarded := execErr != nil && guardFired()
		m := measured{
			plan:     p,
			report:   monReport,
			startT:   startT,
			endT:     endT,
			duration: roundSeconds(t1.Sub(t0).Seconds()),
			casualty: sequenceCasualties(startStats, endStats, lockTimeout),
			execErr:  execErr,
			guarded:  guarded,
		}
		runsMeasured = append(runsMeasured, m)

		if execErr != nil {
			// A real migration halts at the first failing statement; the ones after
			// it never run. Record this one and stop the loop.
			e.log.Warn("engine.sequence_statement_failed", "run", state.RunID, "statement", i, "err", execErr, "guard_fired", guarded)
			halted = true
			haltedAt = i
			break
		}

		// A brief settle between statements so each freeze recovers before the next
		// window opens — it keeps the per-statement windows cleanly separable on the
		// shared timeline. Skipped after the last statement (the trailing recovery
		// below covers it) and when the guard has already fired.
		if i < len(plans)-1 && !guardFired() {
			if !sleepCtx(runCtx, sequenceGap(e.cfg.RecoveryDuration)) && ctx.Err() != nil {
				break
			}
		}
	}

	// A trailing recovery so the last statement's freeze is followed by visible
	// recovery on the trace — but not when the guard fired, where the budget is
	// spent and the only thing left is to report and reap.
	if !guardFired() {
		e.setPhase(phaseRecovery)
		if !sleepCtx(runCtx, e.cfg.RecoveryDuration) && ctx.Err() != nil {
			e.log.Warn("engine.recovery_cut_short", "run", state.RunID, "err", ctx.Err())
		}
	}

	e.setPhase(phaseReporting)
	smp.stop()
	stopLoad()
	res.Timeline = smp.points()

	// Second pass: with the timeline complete, compute each statement's verdict off
	// the window it occupied, reusing verdict.go and scale.go exactly as the single
	// path does.
	maxRank := 0    // worst measured verdict across the sequence
	maxEffRank := 0 // worst effective verdict across the sequence
	var worstClass Classification
	casTotal := int64(0)
	casByLabel := map[string]int64{}
	durTotal := 0.0
	var maxBlocked float64
	var anyAcqQualifier bool
	modeSet := map[string]struct{}{}
	// Worst-of scale projection across the steps, for the top-level roll-up. worstProj
	// is the highest-ranked KNOWN projection; anyProjPopulated is the first projection
	// with real sizes (Known() or not), used as an honest fallback when nothing could
	// be projected so the top level never emits an all-zeros stub.
	worstProjRank := -1
	var worstProj ScaleProjection
	var worstProjWarn string
	var anyProjPopulated ScaleProjection

	for i, m := range runsMeasured {
		blocked := blockedSecondsBetween(res.Timeline, m.startT, m.endT, baseline)
		heldWriteBlockingLock := migrationHeldWriteBlockingLock(m.plan.as, m.report)
		verdict := verdictForObserved(blocked, m.casualty.Total, heldWriteBlockingLock)
		if m.execErr != nil && !m.guarded {
			verdict = VerdictErrored
		}
		if m.guarded {
			verdict = VerdictInconclusive
		}

		sr := StatementResult{
			Index:                 i,
			SQL:                   m.plan.as.SQL,
			Classification:        m.plan.as.Classification,
			Verdict:               verdict,
			BlockedSeconds:        blocked,
			Casualties:            m.casualty,
			LockModes:             []string{},
			DurationSeconds:       m.duration,
			Explanation:           m.plan.as.Lesson.Explanation,
			Suggestion:            m.plan.as.Lesson.Suggestion,
			MonitorBlockedSeconds: m.report.BlockedSeconds,
			DDLStartT:             m.startT,
			DDLEndT:               m.endT,
		}
		if len(m.report.LockModes) > 0 {
			sr.LockModes = m.report.LockModes
			for _, mode := range m.report.LockModes {
				modeSet[mode] = struct{}{}
			}
		}
		if m.execErr != nil {
			sr.Error = truncate(m.execErr.Error(), maxLedgerErrorLength)
		}
		if m.guarded {
			sr.Error = fmt.Sprintf("statement exceeded the %ds sequence budget — it could not acquire its lock while "+
				"traffic ran; in production this is itself a red flag.", int(e.cfg.MaxRunDuration.Seconds()+0.5))
		}

		// Per-statement scale projection, gated exactly like the single path.
		sr.ScaleProjection = scaleProjectionFor(verdict, scaleInputs{
			Measured:              blocked,
			TwinRows:              twinRows,
			TargetRows:            ro.TargetRows,
			ScalesWithRows:        m.plan.as.Lesson.ScalesWithRows,
			BlocksWrites:          m.plan.as.Lesson.BlocksWrites,
			ScaleUncertain:        m.plan.as.Lesson.ScaleUncertain,
			HeldWriteBlockingLock: heldWriteBlockingLock,
			Verdict:               verdict,
			TargetSource:          ro.TargetSource,
		})
		sr.EffectiveVerdict = effectiveVerdict(verdict, sr.ScaleProjection)
		sr.ScaleWarning = scaleWarning(verdict, sr.ScaleProjection)

		// Parity with the single path (qualifierFor): a statement that measured SAFE
		// only because nothing was holding the table — a metadata-only ACCESS
		// EXCLUSIVE shape that got its lock instantly — must carry the same
		// acquisition_not_simulated caveat here that AssessRun attaches on the single
		// path. Without it a sequence would quietly report a bare green light on
		// exactly the shape Brace is most careful about. Sequences never run
		// contention (hostile+statements is refused upstream), so this is the only
		// qualifier they can carry, and it applies only when no freeze was measured
		// and the projection did not veto the SAFE.
		if verdict == VerdictSafe && sr.ScaleWarning == "" &&
			takesAccessExclusive(m.plan.as, m.report) &&
			blocked == 0 && m.casualty.Total == 0 {
			sr.Qualifier = QualifierAcquisitionNotSimulated
			sr.Note = acquisitionNoteSeq(m.plan.as)
			anyAcqQualifier = true
		}

		// shape_note separation: a step that produced no measurement must not carry
		// the general per-shape lesson in `explanation`, where it could be quoted as
		// this step's finding. Move it to shape_note.
		if verdict != VerdictSafe && verdict != VerdictUnsafe {
			sr.ShapeNote = "General behaviour of this SHAPE — NOT a measurement of this run: " + m.plan.as.Lesson.Explanation
			sr.Explanation = nonMeasuredExplanation(verdict)
		}
		// Empty held-lock list with lock evidence: name the mode the monitor saw the
		// statement hold-or-await, so an empty lock_modes is not misread as "no lock".
		if len(sr.LockModes) == 0 {
			sr.Note = appendSentence(sr.Note, lockModeNoteLine(sr.BlockedSeconds, sr.Casualties.Total, verdict, m.report))
		}
		// Track the worst-of projection for the top-level roll-up.
		if sr.ScaleProjection.TwinRows > 0 && anyProjPopulated.TwinRows == 0 {
			anyProjPopulated = sr.ScaleProjection
		}
		if sr.ScaleProjection.Known() {
			if r := verdictRank(sr.ScaleProjection.Verdict); r > worstProjRank ||
				(r == worstProjRank && sr.ScaleProjection.ProjectedBlockedSeconds > worstProj.ProjectedBlockedSeconds) {
				worstProjRank = r
				worstProj = sr.ScaleProjection
				worstProjWarn = sr.ScaleWarning
			}
		}

		res.Statements = append(res.Statements, sr)

		// Roll the worst-of and the totals.
		if r := verdictRank(verdict); r > maxRank {
			maxRank = r
			worstClass = m.plan.as.Classification
		}
		if r := verdictRank(sr.EffectiveVerdict); r > maxEffRank {
			maxEffRank = r
		}
		casTotal += m.casualty.Total
		for name, n := range m.casualty.ByLabel {
			casByLabel[name] += n
		}
		durTotal += m.duration
		if blocked > maxBlocked {
			maxBlocked = blocked
		}
	}

	// The statements that never ran because an earlier one failed. Recorded so the
	// caller sees the whole plan and exactly where it stopped.
	if halted {
		for i := haltedAt + 1; i < len(plans); i++ {
			res.Statements = append(res.Statements, StatementResult{
				Index:          i,
				SQL:            plans[i].as.SQL,
				Classification: plans[i].as.Classification,
				Verdict:        VerdictSkipped,
				LockModes:      []string{},
				Explanation:    nonMeasuredExplanation(VerdictSkipped),
				ShapeNote:      "General behaviour of this SHAPE — NOT a measurement (this step never ran): " + plans[i].as.Lesson.Explanation,
				Suggestion:     plans[i].as.Lesson.Suggestion,
				Error:          "not run: an earlier statement in the sequence failed, so the migration halted before this one.",
			})
		}
	}

	// One invalid-index sweep for the whole twin, if any statement was concurrent.
	if anyConcurrent {
		idxCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), invalidIndexTimeout)
		invalid, err := findInvalidIndexes(idxCtx, twin.DSN)
		cancel()
		if err != nil {
			e.log.Warn("engine.invalid_index_check_failed", "run", state.RunID, "err", err)
		} else if len(invalid) > 0 {
			res.InvalidIndexes = invalid
		}
	}

	// Top-level worst-of. Verdict / EffectiveVerdict are the most alarming across
	// the sequence, so a gate reading either blocks the deploy if ANY step would.
	res.Verdict = verdictForRank(maxRank)
	res.EffectiveVerdict = verdictForRank(maxEffRank)
	if res.EffectiveVerdict == "" {
		res.EffectiveVerdict = res.Verdict
	}
	// Top-level scale projection: the worst-of the per-statement projections, so it
	// matches the top-level EffectiveVerdict instead of emitting an all-zeros stub
	// that would read as "no projection / nothing to worry about". When no step
	// projected, fall back to a populated-but-unknown projection (real sizes + model,
	// empty verdict → Known()==false) rather than a bare zero.
	if worstProjRank >= 0 {
		res.ScaleProjection = worstProj
		res.ScaleWarning = worstProjWarn
	} else {
		res.ScaleProjection = anyProjPopulated
	}
	res.Classification = worstClass
	res.BlockedSeconds = maxBlocked
	res.Casualties.Total = casTotal
	res.Casualties.ByLabel = casByLabel
	res.CasualtiesAllPhases = gen.Snapshot().CasualtiesTotal
	res.ErroredTotal = gen.Snapshot().ErroredTotal
	res.DurationSeconds = roundSeconds(durTotal)
	res.LockModes = sortLockModes(modeSet)
	if len(runsMeasured) > 0 {
		res.DDLStartT = runsMeasured[0].startT
		res.DDLEndT = runsMeasured[len(runsMeasured)-1].endT
	}
	// If the whole sequence is SAFE but at least one step's SAFE hinged on an
	// unsimulated lock acquisition, surface that at the top level too, so a gate or
	// agent reading only the overall verdict gets the same caveat the single path
	// would have given for that statement on its own.
	if res.Verdict == VerdictSafe && anyAcqQualifier {
		res.Qualifier = QualifierAcquisitionNotSimulated
	}
	res.Explanation = sequenceExplanation(res, len(plans))
	res.Note = sequenceNote(res, halted, haltedAt, len(plans))

	e.log.Info("engine.sequence_finished",
		"run", state.RunID, "twin", twin.Name,
		"verdict", res.Verdict, "effective_verdict", res.EffectiveVerdict,
		"statements", len(plans), "ran", len(runsMeasured), "halted", halted,
		"blocked_s", res.BlockedSeconds, "casualties", res.Casualties.Total,
		"deadline_ms", res.Casualties.DeadlineMS)

	// The ledger, best-effort, exactly as the single path.
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

// sequenceGap is how long the run pauses between two statements so each freeze
// recovers before the next begins. Bounded to a fraction of the recovery duration
// so a ten-statement sequence does not spend most of the run budget idling.
func sequenceGap(recovery time.Duration) time.Duration {
	g := recovery / 2
	if g > 750*time.Millisecond {
		g = 750 * time.Millisecond
	}
	if g <= 0 {
		g = 250 * time.Millisecond
	}
	return g
}

// blockedSecondsBetween is blockedSecondsFrom windowed to a single statement's
// [startT, endT] slice of a shared sequence timeline. It REUSES longestBlockedWindow
// unchanged — only the point selection differs, because a sequence measures many
// DDL windows on one continuous timeline instead of one DDL followed by a recovery.
func blockedSecondsBetween(points []RatePoint, startT, endT, baselineWPS float64) float64 {
	if baselineWPS <= 0 {
		return 0
	}
	window := make([]RatePoint, 0, len(points))
	for i, p := range points {
		if p.T < startT {
			continue
		}
		// Include the sample immediately before the window opens so the first
		// in-window sample has an interval to measure against — the same edge-handling
		// blockedSecondsFrom relies on.
		if len(window) == 0 && i > 0 {
			window = append(window, points[i-1])
		}
		window = append(window, p)
		if p.T >= endT {
			// Include the first sample at or after the window closes (the interval the
			// freeze recovers in) and stop — beyond it belongs to the next statement.
			break
		}
	}
	return roundSeconds(longestBlockedWindow(window, baselineWPS))
}

// sequenceCasualties diffs the load generator's cumulative counters across exactly
// one statement's execution, so a write killed by an earlier or later statement is
// never charged to this one. It mirrors windowedCasualties, differing only in that
// a sequence captures its subtrahend as a live Snapshot at the window's start
// rather than reading it back off the sampler.
func sequenceCasualties(start, end loadgen.Stats, deadline time.Duration) Casualties {
	c := Casualties{ByLabel: map[string]int64{}, DeadlineMS: deadline.Milliseconds()}
	c.Total = nonNegative(end.CasualtiesTotal - start.CasualtiesTotal)
	for name, ls := range end.ByLabel {
		c.ByLabel[name] = nonNegative(ls.Died - start.ByLabel[name].Died)
	}
	return c
}

// verdictRank orders the verdicts for the worst-of roll-up. Higher is more
// alarming, so a gate blocks on the max. UNSAFE outranks the two "did not measure
// a pass" terminal states, which in turn outrank SAFE; SKIPPED and unknown rank 0
// so they never dominate.
func verdictRank(v string) int {
	switch v {
	case VerdictUnsafe:
		return 4
	case VerdictInconclusive:
		return 3
	case VerdictErrored:
		return 2
	case VerdictSafe:
		return 1
	default:
		return 0
	}
}

// verdictForRank is the inverse of verdictRank for the roll-up. Rank 0 (nothing
// ran, or only skipped) yields an empty verdict, which the caller resolves.
func verdictForRank(r int) string {
	switch r {
	case 4:
		return VerdictUnsafe
	case 3:
		return VerdictInconclusive
	case 2:
		return VerdictErrored
	case 1:
		return VerdictSafe
	default:
		return ""
	}
}

// sortLockModes orders a set of pg_locks mode strings strongest first, using the
// monitor's own strength ranking, so the top-level LockModes reads like a single
// run's. Unknown spellings sort last but are never dropped.
func sortLockModes(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for m := range set {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool {
		si, sj := monitor.Strength(out[i]), monitor.Strength(out[j])
		if si != sj {
			return si > sj
		}
		return out[i] < out[j]
	})
	return out
}

// joinSequenceSQL renders the sequence as one script for the ledger, the stream
// and the UI — one statement per line, terminated, in order.
func joinSequenceSQL(plans []stmtPlan) string {
	var b strings.Builder
	for i, p := range plans {
		if i > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(strings.TrimSuffix(strings.TrimSpace(p.as.SQL), ";"))
		b.WriteByte(';')
	}
	return b.String()
}

// sequenceExplanation is the one-paragraph framing for the overall card: what a
// sequence run is and how to read its worst-of verdict.
func sequenceExplanation(res *RunResult, total int) string {
	return fmt.Sprintf("This is a SEQUENCE of %d statements run in order on one twin, each measured in its own window — "+
		"so a statement sees the schema the statements before it produced. The overall verdict is %s, the worst of the "+
		"per-statement verdicts: a sequence is only as safe as its most dangerous step. Read the per-statement breakdown "+
		"to see which step carries the risk.", total, res.EffectiveVerdict)
}

// sequenceNote adds the honest caveat when the run did not reach every statement.
func sequenceNote(res *RunResult, halted bool, haltedAt, total int) string {
	if halted {
		return fmt.Sprintf("The migration halted at statement %d: it failed to apply, so the %d statement(s) after it were "+
			"NOT run — exactly as a real migration would stop. Fix that statement and rehearse the sequence again.",
			haltedAt+1, total-haltedAt-1)
	}
	if res.Casualties.Total > 0 && res.Casualties.DeadlineMS > 0 {
		return fmt.Sprintf("Across the sequence, %d writes waited longer than the %dms request deadline this run simulated "+
			"and were killed; with a different deadline the freezes are the same and this count is not.",
			res.Casualties.Total, res.Casualties.DeadlineMS)
	}
	if res.Qualifier == QualifierAcquisitionNotSimulated {
		return "One or more steps measured SAFE only because nothing was holding the table: they take ACCESS EXCLUSIVE and got " +
			"their lock instantly. In production each waits behind your longest open transaction, and because PostgreSQL grants " +
			"locks in arrival order, every query that arrives after it waits too — the table stops until that transaction ends. " +
			"This run did not simulate that wait; see the per-statement notes for which steps, and re-run any one of them on its " +
			"own with hostile:true to measure it."
	}
	return ""
}

// acquisitionNoteSeq is the per-statement caveat for a sequence step that measured
// SAFE but takes ACCESS EXCLUSIVE — the sequence-mode sibling of the single path's
// acquisition note. It differs only in the remediation pointer: a sequence cannot
// run contention (hostile+statements is refused), so it points the operator at
// re-running that one statement on its own with hostile:true.
func acquisitionNoteSeq(as *Assessment) string {
	return fmt.Sprintf("This step measured SAFE, but it takes ACCESS EXCLUSIVE — the lock a bare %s takes — and got it instantly "+
		"only because nothing was holding the table. In production it waits behind your longest open transaction, and every query "+
		"that arrives after it waits too, until that transaction ends. This run did not simulate that wait. To measure it, run this "+
		"one statement on its own with hostile:true.", strings.TrimSpace(as.Lesson.Title))
}
