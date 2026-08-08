package loadgen

// The contention source — "hostile mode".
//
// Everything else in this package issues short autocommit writes, which is an
// honest model of application traffic and a DISHONEST model of a production
// database, because it means nothing ever holds a lock for long. A migration
// that only takes a fast catalog lock (DROP INDEX, ALTER INDEX ... RENAME, a
// metadata-only ALTER TABLE) therefore acquires in microseconds, and the rig
// reports that nobody was harmed.
//
// That is not what those statements do in production. Their danger is the WAIT:
// the AccessExclusiveLock request queues behind whatever transaction is already
// holding the table, and because PostgreSQL grants lock requests in arrival
// order, every statement that arrives after the queued AccessExclusive request
// waits for it too. One open transaction plus one instant DDL is a full stop on
// the table until that transaction commits.
//
// Contender is the missing half of that picture: a session that opens a
// transaction, takes a real lock on the load target, holds it for a configured
// time, commits, and repeats. It is OFF by default — with it on, the run
// measures "this migration behind a transaction I held open for N seconds",
// which is a different (and much harsher) question than the default run asks,
// and the report has to say which question it answered.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	// DefaultHold is how long one contention cycle keeps its transaction open.
	// Two seconds is deliberately longer than the default 1500ms simulated
	// request deadline: a hold shorter than the deadline would queue writes
	// without killing any, and the casualty count is the number people read.
	DefaultHold = 2 * time.Second

	// defaultGap is the pause between cycles. Short, because the run only has a
	// few seconds of DDL window to be hostile during.
	defaultGap = 250 * time.Millisecond

	// contenderAppName labels the contention session in pg_stat_activity. The
	// monitor counts blocked writers by application_name, and this one is not a
	// writer whose suffering we are measuring — it is the cause.
	contenderAppName = "brace-contender"

	// contenderLockTimeout bounds the contender's own statement. It must be
	// generous: once the migration holds AccessExclusive, the contender's next
	// INSERT queues like everything else, and a contender that gave up instantly
	// would stop being a source of contention exactly when it matters.
	contenderLockTimeout = "10000"
)

// ContendConfig configures a Contender.
type ContendConfig struct {
	// DSN points at the twin. Required. It should be the same unprivileged
	// traffic DSN the writers use: the contention must be something the
	// application could plausibly be doing.
	DSN string
	// Statement is run once per cycle inside the held transaction, with the same
	// two bound bigints every label statement takes. Defaults to the first of
	// DefaultLabels, i.e. a write to the load target.
	Statement string
	// Hold is how long the transaction stays open after Statement returns.
	// Defaults to DefaultHold.
	Hold time.Duration
	// Gap is the pause between cycles. Defaults to 250ms.
	Gap time.Duration
	// Logger defaults to slog.Default().
	Logger *slog.Logger
}

func (c *ContendConfig) applyDefaults() {
	if strings.TrimSpace(c.Statement) == "" {
		c.Statement = DefaultLabels()[0].Statement
	}
	if c.Hold <= 0 {
		c.Hold = DefaultHold
	}
	if c.Gap <= 0 {
		c.Gap = defaultGap
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// ContendStats is what one run's contention adds up to. It is reported next to
// the measurement precisely so nobody reads a hostile-mode freeze as the freeze
// an idle database would have seen.
type ContendStats struct {
	// Cycles is how many transactions were opened, held and committed.
	Cycles int64 `json:"cycles"`
	// HoldSeconds is the configured hold per cycle — the assumption the whole
	// run is conditional on.
	HoldSeconds float64 `json:"hold_seconds"`
	// Failures is how many cycles could not take their lock (typically because
	// the migration was already holding AccessExclusive). Not an error: a
	// contender the migration managed to block is still evidence.
	Failures int64 `json:"failures"`
	// LastError is the most recent cycle failure, for the operator.
	LastError string `json:"last_error,omitempty"`
}

// ContentionStatement picks the statement a Contender should hold a lock with,
// given the run's traffic mix. It is the first label's statement, so the lock is
// taken on the table the writers actually write to — including in
// bring-your-own-data deployments, where Brace has no idea what that table is
// called.
func ContentionStatement(labels []LabelSpec) string {
	for _, l := range labels {
		if strings.TrimSpace(l.Statement) != "" {
			return l.Statement
		}
	}
	return DefaultLabels()[0].Statement
}

// Contender holds a lock on the load target, on a cycle, for as long as it is
// running. Single-use: Start once, Stop once.
type Contender struct {
	cfg ContendConfig
	log *slog.Logger

	cycles   atomic.Int64
	failures atomic.Int64

	// holding closes the first time a transaction is actually holding its lock,
	// so the orchestrator can fire the DDL into contention that provably exists
	// rather than into a race with the cycle timer.
	holding     chan struct{}
	holdingOnce sync.Once

	mu      sync.Mutex
	started bool
	stopped bool
	lastErr error
	cancel  context.CancelFunc
	done    chan struct{}
}

// NewContender builds an unstarted Contender.
func NewContender(cfg ContendConfig) *Contender {
	cfg.applyDefaults()
	return &Contender{
		cfg:     cfg,
		log:     cfg.Logger.With("component", "contender"),
		holding: make(chan struct{}),
		done:    make(chan struct{}),
	}
}

// Start begins the hold/commit cycle. It returns as soon as the goroutine is
// running; use WaitHolding to know when a lock is actually held.
func (c *Contender) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started || c.stopped {
		return errors.New("loadgen: Contender.Start called more than once")
	}
	if strings.TrimSpace(c.cfg.DSN) == "" {
		return errors.New("loadgen: ContendConfig.DSN is required")
	}
	c.started = true
	loopCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	go c.loop(loopCtx)
	c.log.Info("contender.started", "hold_ms", c.cfg.Hold.Milliseconds())
	return nil
}

// WaitHolding blocks until a transaction is holding its lock, the context ends,
// or the contender stops. It reports whether contention was actually achieved —
// a false here means the run is NOT hostile, whatever the caller asked for, and
// must not be reported as such.
func (c *Contender) WaitHolding(ctx context.Context) bool {
	select {
	case <-c.holding:
		return true
	case <-c.done:
		return false
	case <-ctx.Done():
		return false
	}
}

func (c *Contender) loop(ctx context.Context) {
	defer close(c.done)
	var conn *pgx.Conn
	defer func() {
		if conn != nil {
			closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
			_ = conn.Close(closeCtx)
			cancel()
		}
	}()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for ctx.Err() == nil {
		if conn == nil {
			var err error
			conn, err = c.connect(ctx)
			if err != nil {
				c.record(err)
				if !sleepCtx(ctx, erroredBackoff) {
					return
				}
				continue
			}
		}
		if broken := c.cycle(ctx, conn, rng); broken {
			// The connection is unusable (the twin went away, the transaction
			// was aborted). Drop it; the next iteration reconnects.
			closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
			_ = conn.Close(closeCtx)
			cancel()
			conn = nil
		}
		if !sleepCtx(ctx, c.cfg.Gap) {
			return
		}
	}
}

func (c *Contender) connect(ctx context.Context) (*pgx.Conn, error) {
	connCfg, err := pgx.ParseConfig(c.cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("loadgen: contender parse DSN: %w", err)
	}
	connCfg.RuntimeParams["application_name"] = contenderAppName
	connCfg.RuntimeParams["lock_timeout"] = contenderLockTimeout
	// Deliberately no idle_in_transaction_session_timeout: holding the
	// transaction open is the entire job.
	connCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	conn, err := pgx.ConnectConfig(connCtx, connCfg)
	if err != nil {
		return nil, fmt.Errorf("loadgen: contender connect: %w", err)
	}
	return conn, nil
}

// cycle runs one BEGIN / write / hold / COMMIT. It reports whether the
// connection should be discarded.
func (c *Contender) cycle(ctx context.Context, conn *pgx.Conn, rng *rand.Rand) bool {
	tx, err := conn.Begin(ctx)
	if err != nil {
		c.record(err)
		return true
	}
	customerID := int64(rng.Intn(100_000) + 1)
	amountCents := int64(rng.Intn(50_000) + 1)
	if _, err := tx.Exec(ctx, c.cfg.Statement, customerID, amountCents); err != nil {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		c.record(err)
		// A lock timeout or a cancelled statement leaves the connection usable;
		// anything else is safer to replace.
		return !isRecoverable(err)
	}

	// The lock is held from here until Commit. Announce it once: the
	// orchestrator waits for this before starting the DDL, which is what makes
	// the queueing deterministic instead of a race against the cycle timer.
	c.holdingOnce.Do(func() { close(c.holding) })

	held := sleepCtx(ctx, c.cfg.Hold)
	// Commit even when the run is winding down — an abandoned transaction would
	// hold its lock until the connection is reaped, which is exactly the
	// production incident this simulates and not something to leave behind in a
	// teardown path.
	commitCtx := ctx
	if !held {
		var cancel context.CancelFunc
		commitCtx, cancel = context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
	}
	if err := tx.Commit(commitCtx); err != nil {
		c.record(err)
		return true
	}
	c.cycles.Add(1)
	return false
}

// isRecoverable reports whether a failed statement left the session in a state
// the next cycle can reuse.
func isRecoverable(err error) bool {
	switch Classify(err) {
	case OutcomeDied, OutcomeStopped:
		return true
	}
	return false
}

func (c *Contender) record(err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	c.failures.Add(1)
	c.mu.Lock()
	c.lastErr = err
	c.mu.Unlock()
	c.log.Debug("contender.cycle_failed", "err", err)
}

// Stop halts the cycle and waits for the in-flight transaction to be committed
// or rolled back. Idempotent, and safe on a Contender that was never started.
func (c *Contender) Stop(ctx context.Context) ContendStats {
	c.mu.Lock()
	started, cancel := c.started, c.cancel
	c.stopped = true
	c.mu.Unlock()

	if started && cancel != nil {
		cancel()
		select {
		case <-c.done:
		case <-ctx.Done():
			c.log.Warn("contender.stop_timeout", "err", ctx.Err())
		}
	}
	return c.Stats()
}

// Stats reports the cycle tally. Safe at any time.
func (c *Contender) Stats() ContendStats {
	s := ContendStats{
		Cycles:      c.cycles.Load(),
		HoldSeconds: c.cfg.Hold.Seconds(),
		Failures:    c.failures.Load(),
	}
	c.mu.Lock()
	if c.lastErr != nil {
		s.LastError = c.lastErr.Error()
	}
	c.mu.Unlock()
	return s
}

// ParseHold reads a hold duration in milliseconds from a string, returning the
// default for anything absent or nonsensical. Shared by the boot layer and the
// API so "0" means the same thing in both.
func ParseHold(ms string) time.Duration {
	v := strings.TrimSpace(ms)
	if v == "" {
		return DefaultHold
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return DefaultHold
	}
	return time.Duration(n) * time.Millisecond
}
