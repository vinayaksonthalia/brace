// Package loadgen sustains labeled write traffic against a twin database and
// counts, per label, the writes that succeed versus the writes that DIE
// because a migration's lock blocked them past a timeout.
//
// Those casualty numbers are the product's headline, so attribution integrity
// outranks everything else here. Two rules follow from that and are enforced
// throughout:
//
//  1. The pool sets lock_timeout and NEVER statement_timeout. A write that is
//     merely slow — an unlucky checkpoint, a cold cache, a big index scan — is
//     not a casualty of the migration, and statement_timeout cannot tell the
//     difference between "blocked on a lock" and "slow". lock_timeout only
//     fires while a statement is waiting to ACQUIRE a lock, which is exactly
//     and only the harm a migration inflicts on live traffic.
//
//  2. Anything we cannot positively attribute to a lock wait is counted in a
//     separate, visible `errored` bucket. Casualties are never padded with
//     errors of unknown origin.
//
// Measured on the target platform (PostgreSQL 16.14, Debian aarch64):
//
//	SET lock_timeout='300ms'; INSERT ...   -- blocked by a conflicting lock
//	ERROR:  55P03: canceling statement due to lock timeout
//
//	SET statement_timeout='200ms'; SELECT pg_sleep(2);
//	ERROR:  57014: canceling statement due to statement timeout
//
// So on PG16 a lock timeout surfaces as SQLSTATE 55P03 (lock_not_available),
// despite the message reading "canceling statement due to ... timeout" like its
// 57014 cousin. 55P03 is the ONLY code this package counts as a casualty.
package loadgen

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

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultWorkers     = 24
	defaultLockTimeout = 1500 * time.Millisecond

	// sqlstateLockNotAvailable is 55P03. VERIFIED against PostgreSQL 16.14:
	// this is what a statement killed by lock_timeout reports. It is the single
	// code that increments the casualty counter.
	sqlstateLockNotAvailable = "55P03"

	// sqlstateQueryCanceled is 57014. It is what statement_timeout and an
	// explicit pg_cancel_backend / client cancel report. Because this pool
	// never sets statement_timeout, 57014 CANNOT have come from a lock wait
	// here, so it must never be counted as a casualty. See Classify.
	sqlstateQueryCanceled = "57014"

	// erroredBackoff paces a worker after an unclassifiable failure. If the
	// twin is dropped out from under us, errors return instantly and an
	// unpaced worker would burn a core spinning. We keep trying — deciding
	// when to stop belongs to the orchestrator, not to the load generator.
	erroredBackoff = 50 * time.Millisecond
)

// ErrClosed is returned by Start after Stop: a Generator is single-use.
var ErrClosed = errors.New("loadgen: generator is stopped")

// LabelSpec is one class of simulated customer traffic.
//
// Statement MUST be parameterized with exactly two placeholders, in this order:
//
//	$1 customer_id  bigint
//	$2 amount_cents bigint
//
// Both are filled from the worker's seeded rand. Keeping the arity fixed is
// what lets a label be pure configuration (and eventually user-supplied SQL)
// without handing every label its own Go callback.
type LabelSpec struct {
	Name      string
	Weight    int
	Statement string
}

// DefaultLabels is the stock traffic mix: a storefront where people browse and
// add to cart far more often than they pay. Every statement is a small INSERT
// into the template schema's orders table
// (orders(customer_id bigint, amount_cents bigint, ...)).
func DefaultLabels() []LabelSpec {
	const stmt = `INSERT INTO orders (customer_id, amount_cents) VALUES ($1, $2)`
	return []LabelSpec{
		{Name: "checkout", Weight: 3, Statement: stmt},
		{Name: "login", Weight: 2, Statement: stmt},
		{Name: "add_to_cart", Weight: 5, Statement: stmt},
	}
}

// Config configures a Generator.
type Config struct {
	// DSN points at the twin database to hammer. Required.
	DSN string
	// Workers is the number of concurrent writers. Defaults to 24.
	Workers int
	// LockTimeout is how long a write waits for a lock before it is declared a
	// casualty. Defaults to 1500ms. Applied to this pool's connections only.
	LockTimeout time.Duration
	// Labels is the traffic mix. Defaults to DefaultLabels().
	Labels []LabelSpec
	// Logger receives lifecycle events. Defaults to slog.Default().
	Logger *slog.Logger
}

func (c *Config) applyDefaults() {
	if c.Workers <= 0 {
		c.Workers = defaultWorkers
	}
	if c.LockTimeout <= 0 {
		c.LockTimeout = defaultLockTimeout
	}
	if len(c.Labels) == 0 {
		c.Labels = DefaultLabels()
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

func (c Config) validate() error {
	if strings.TrimSpace(c.DSN) == "" {
		return errors.New("loadgen: Config.DSN is required")
	}
	seen := make(map[string]bool, len(c.Labels))
	for i, l := range c.Labels {
		if strings.TrimSpace(l.Name) == "" {
			return fmt.Errorf("loadgen: Labels[%d].Name is empty", i)
		}
		if seen[l.Name] {
			return fmt.Errorf("loadgen: duplicate label %q", l.Name)
		}
		seen[l.Name] = true
		if l.Weight <= 0 {
			return fmt.Errorf("loadgen: label %q has non-positive Weight %d", l.Name, l.Weight)
		}
		if strings.TrimSpace(l.Statement) == "" {
			return fmt.Errorf("loadgen: label %q has an empty Statement", l.Name)
		}
	}
	return nil
}

// LabelStats is the per-label tally. Died is sacred: it counts only writes
// killed while waiting for a lock.
type LabelStats struct {
	OK      int64 `json:"ok"`
	Died    int64 `json:"died"`
	Errored int64 `json:"errored"`
}

// Stats is a point-in-time snapshot for the SSE layer.
type Stats struct {
	// WritesPerSec is successful writes observed in the last closed 1s window.
	WritesPerSec    int64                 `json:"writes_per_sec"`
	ByLabel         map[string]LabelStats `json:"by_label"`
	CasualtiesTotal int64                 `json:"casualties_total"`
	ErroredTotal    int64                 `json:"errored_total"`
}

// Outcome is the classification of a single write attempt.
type Outcome int

const (
	// OutcomeOK: the write committed.
	OutcomeOK Outcome = iota
	// OutcomeDied: the write was killed waiting for a lock. A casualty.
	OutcomeDied
	// OutcomeStopped: the write was abandoned because we are shutting down.
	// Counted nowhere — it is our own doing, not the migration's.
	OutcomeStopped
	// OutcomeErrored: anything else. Visible, and never merged into casualties.
	OutcomeErrored
)

func (o Outcome) String() string {
	switch o {
	case OutcomeOK:
		return "ok"
	case OutcomeDied:
		return "died"
	case OutcomeStopped:
		return "stopped"
	case OutcomeErrored:
		return "errored"
	}
	return "unknown(" + strconv.Itoa(int(o)) + ")"
}

// Classify turns the result of one write into a counter bucket. This is the
// function the headline number is made of; change it only with evidence.
//
// Order matters. The 55P03 check comes FIRST, before the shutdown checks: a
// write really was killed by a lock even if Stop happened to fire in the same
// instant, and dropping it would undercount the harm.
func Classify(err error) Outcome {
	if err == nil {
		return OutcomeOK
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case sqlstateLockNotAvailable:
			// 55P03 — "canceling statement due to lock timeout" on PG16. The
			// only way to get this on our pool is to have exceeded
			// lock_timeout waiting on a lock. A casualty, definitively.
			return OutcomeDied
		case sqlstateQueryCanceled:
			// 57014 — query_canceled. On this pool it can NOT be a lock wait:
			// we never set statement_timeout, and PG16 reports lock waits as
			// 55P03 (verified, see package doc). So 57014 means someone
			// cancelled us — our own Stop, or an operator/pg_cancel_backend.
			// Counting it as a casualty would inflate the headline with a
			// death we caused, so it is deliberately counted nowhere.
			return OutcomeStopped
		}
		return OutcomeErrored
	}

	// Graceful stop: pgx surfaces a cancelled query context wrapped around
	// context.Canceled. Neither a success nor a casualty.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return OutcomeStopped
	}
	return OutcomeErrored
}

// labelCounter holds one label's atomics. Allocated once at construction and
// never rehashed, so workers touch it lock-free.
type labelCounter struct {
	spec    LabelSpec
	ok      atomic.Int64
	died    atomic.Int64
	errored atomic.Int64
}

// Generator sustains labeled write load against one database.
type Generator struct {
	cfg  Config
	log  *slog.Logger
	pool *pgxpool.Pool

	labels []*labelCounter
	// cumWeights[i] is the running total of weights through labels[i]; a pick
	// is a single rand below totalWeight plus a scan. Precomputed once.
	cumWeights  []int
	totalWeight int

	// window accumulates successful writes for the current second;
	// writesPerSec holds the last closed window. The rate goroutine Swaps one
	// into the other, so Snapshot is a couple of atomic loads.
	window       atomic.Int64
	writesPerSec atomic.Int64

	mu      sync.Mutex
	started bool
	stopped bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// New validates the config and opens the load pool. The pool is separate from
// every other pool in the process precisely so that lock_timeout applies here
// and nowhere else — the migration runner must NOT inherit it.
func New(ctx context.Context, cfg Config) (*Generator, error) {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("loadgen: parse DSN: %w", err)
	}

	// THE CRITICAL SETTING. RuntimeParams are sent in the startup packet, so
	// every connection this pool ever opens — including ones it opens later to
	// replace a broken one — carries the timeout. Doing it with a per-checkout
	// `SET` would leave a window where a fresh connection has no lock_timeout
	// and a blocked write would hang forever instead of being counted.
	//
	// Postgres reads a bare integer here as milliseconds.
	ms := cfg.LockTimeout.Milliseconds()
	if ms < 1 {
		ms = 1 // sub-millisecond would round to 0, which means "no timeout"
	}
	poolCfg.ConnConfig.RuntimeParams["lock_timeout"] = strconv.FormatInt(ms, 10)
	// Deliberately absent: statement_timeout. A slow-but-unblocked write is
	// not a casualty, and setting it would make that distinction unknowable.

	// One connection per worker: a worker parked in a 1.5s lock wait must not
	// stall the others by holding the only connection.
	poolCfg.MaxConns = int32(cfg.Workers)
	poolCfg.MinConns = int32(cfg.Workers)

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("loadgen: connect load pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("loadgen: ping %s: %w", poolCfg.ConnConfig.Database, err)
	}

	g := &Generator{
		cfg:  cfg,
		log:  cfg.Logger.With("component", "loadgen"),
		pool: pool,
	}
	for _, spec := range cfg.Labels {
		g.labels = append(g.labels, &labelCounter{spec: spec})
		g.totalWeight += spec.Weight
		g.cumWeights = append(g.cumWeights, g.totalWeight)
	}
	return g, nil
}

// pickLabel chooses a label with probability proportional to its weight.
func (g *Generator) pickLabel(rng *rand.Rand) *labelCounter {
	n := rng.Intn(g.totalWeight)
	for i, cum := range g.cumWeights {
		if n < cum {
			return g.labels[i]
		}
	}
	// Unreachable: the last cumulative weight equals totalWeight.
	return g.labels[len(g.labels)-1]
}

// Start spins the workers and the rate-window goroutine. It returns as soon as
// they are running; load continues until Stop.
func (g *Generator) Start(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stopped {
		return ErrClosed
	}
	if g.started {
		return errors.New("loadgen: already started")
	}
	g.started = true

	// Workers outlive the caller's ctx if it is a startup context, but still
	// die with it if the caller cancels: Stop cancels this derived context.
	workerCtx, cancel := context.WithCancel(ctx)
	g.cancel = cancel

	seed := time.Now().UnixNano()
	for i := 0; i < g.cfg.Workers; i++ {
		// Per-worker rand: math/rand's global source is mutex-guarded and 24
		// workers contending on it would be a measurable share of our loop.
		rng := rand.New(rand.NewSource(seed + int64(i)*7919))
		g.wg.Add(1)
		go g.worker(workerCtx, rng)
	}
	g.wg.Add(1)
	go g.rateLoop(workerCtx)

	g.log.Info("loadgen.started",
		"workers", g.cfg.Workers,
		"lock_timeout_ms", g.cfg.LockTimeout.Milliseconds(),
		"labels", len(g.labels))
	return nil
}

func (g *Generator) worker(ctx context.Context, rng *rand.Rand) {
	defer g.wg.Done()
	for {
		if ctx.Err() != nil {
			return
		}
		lc := g.pickLabel(rng)

		// Values are deliberately unremarkable: the point is contention on the
		// table, not an interesting data distribution.
		customerID := int64(rng.Intn(100_000) + 1)
		amountCents := int64(rng.Intn(50_000) + 1)

		_, err := g.pool.Exec(ctx, lc.spec.Statement, customerID, amountCents)

		switch Classify(err) {
		case OutcomeOK:
			lc.ok.Add(1)
			g.window.Add(1)
		case OutcomeDied:
			lc.died.Add(1)
		case OutcomeStopped:
			// Ours, not the migration's. Counted nowhere.
		case OutcomeErrored:
			lc.errored.Add(1)
			g.log.Debug("loadgen.write_errored", "label", lc.spec.Name, "err", err)
			// The twin may have been dropped mid-run. Keep trying anyway —
			// the orchestrator owns the decision to stop.
			if !sleepCtx(ctx, erroredBackoff) {
				return
			}
		}
	}
}

// rateLoop closes one rate window per second by swapping the accumulator out.
func (g *Generator) rateLoop(ctx context.Context) {
	defer g.wg.Done()
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			g.writesPerSec.Store(g.window.Swap(0))
		}
	}
}

// Snapshot returns the current tallies. Cheap by construction — a handful of
// atomic loads and one small map — because the SSE layer calls it every second.
func (g *Generator) Snapshot() Stats {
	s := Stats{
		WritesPerSec: g.writesPerSec.Load(),
		ByLabel:      make(map[string]LabelStats, len(g.labels)),
	}
	for _, lc := range g.labels {
		ls := LabelStats{
			OK:      lc.ok.Load(),
			Died:    lc.died.Load(),
			Errored: lc.errored.Load(),
		}
		s.ByLabel[lc.spec.Name] = ls
		s.CasualtiesTotal += ls.Died
		s.ErroredTotal += ls.Errored
	}
	return s
}

// Stop halts load and releases the pool. Idempotent.
//
// Measured behaviour worth knowing: Stop can under-count successes by at most
// one write per worker. A write already committed on the server when the
// cancellation lands comes back to us as context.Canceled, and we score it
// `stopped` rather than guess. Under-counting successes is harmless; inventing
// them, or inventing casualties, is not.
//
// The order is load-bearing: cancel the workers, WAIT for them, and only then
// close the pool. pgxpool.Close on a pool with in-flight acquisitions destroys
// connections out from under the goroutines using them ("conn busy" panics
// inside pgx). The wait is what makes the close safe.
//
// If ctx expires before the workers finish, Stop gives up waiting and returns —
// but deliberately leaves the pool OPEN, because closing it under live workers
// is the exact crash this ordering exists to prevent. A leaked pool is a much
// cheaper bug than a panic in a demo.
func (g *Generator) Stop(ctx context.Context) error {
	g.mu.Lock()
	if g.stopped {
		g.mu.Unlock()
		return nil
	}
	g.stopped = true
	cancel := g.cancel
	started := g.started
	g.mu.Unlock()

	if !started {
		// Never started: no workers to wait for, just the pool.
		g.closePool()
		return nil
	}
	cancel()

	done := make(chan struct{})
	go func() {
		g.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		g.log.Error("loadgen.stop_timeout", "err", ctx.Err(),
			"note", "workers still running; pool left open to avoid a busy-conn panic")
		return fmt.Errorf("loadgen: stop: workers did not finish: %w", ctx.Err())
	}

	g.closePool()
	s := g.Snapshot()
	g.log.Info("loadgen.stopped",
		"casualties", s.CasualtiesTotal, "errored", s.ErroredTotal)
	return nil
}

// closePool tolerates a Generator assembled without a pool (unit tests build
// one directly to exercise selection and counting without a database).
func (g *Generator) closePool() {
	if g.pool != nil {
		g.pool.Close()
	}
}

// sleepCtx waits for d, reporting false if the context ended first.
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
