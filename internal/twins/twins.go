// Package twins manages the lifecycle of throwaway "twin" databases: clones of
// a template database that Brace runs migrations against and then destroys.
//
// Three platform facts, measured on real hardware, shape everything here:
//
//  1. CREATE DATABASE ... TEMPLATE fails outright if any session is connected
//     to the template ("source database is being accessed by other users",
//     SQLSTATE 55006). So the template is held at ALLOW_CONNECTIONS false, and
//     every clone defensively terminates whatever is still camped on it.
//  2. Cloning an ~873MB template takes about 80 seconds. That is far too long
//     to happen inside a request, so clones only ever run on a background
//     worker and consumers take pre-warmed twins. Acquire never clones.
//  3. Two concurrent CREATE DATABASE from the same template conflict, so the
//     worker is deliberately a single goroutine — clones are serialized.
package twins

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNoWarmTwin is returned by Acquire when the pool has no ready twin. It is
// an expected, routine condition — the caller should surface something like
// "the lab is preparing the next dummy" and retry later, never block.
var ErrNoWarmTwin = errors.New("twins: no warm twin available")

// ErrClosed is returned by every public method after Close.
var ErrClosed = errors.New("twins: manager is closed")

const (
	defaultPoolSize   = 2
	defaultNamePrefix = "brace_run_"

	// maxCloneAttempts bounds retries of a single clone. A 55006 means someone
	// reconnected to the template between our terminate and the CREATE; that
	// races out within a couple of tries or it is a real bug.
	maxCloneAttempts = 3

	// sqlstateObjectInUse is 55006, "source database is being accessed by
	// other users" — the one error we retry rather than give up on.
	sqlstateObjectInUse = "55006"

	// cleanupTimeout bounds best-effort DROPs issued during shutdown, where
	// the caller's context is usually already cancelled.
	cleanupTimeout = 30 * time.Second

	// refillBackoff paces the worker after a failed clone so a permanently
	// broken template cannot turn into a hot loop of 80-second clones.
	refillBackoff = 2 * time.Second
)

// Twin is a live throwaway database. It is owned by the caller between Acquire
// and Release; after Release the database no longer exists.
type Twin struct {
	Name      string
	DSN       string
	CreatedAt time.Time
}

// Config configures a Manager.
type Config struct {
	// AdminURL is a superuser connection string. It MUST point at the postgres
	// maintenance database: DROP DATABASE cannot be issued from inside the
	// database being dropped, and the admin pool must survive every twin.
	AdminURL string
	// TemplateName is the template database to clone, e.g. "brace_template".
	TemplateName string
	// PoolSize is how many warm twins to keep ready. Defaults to 2.
	PoolSize int
	// NamePrefix prefixes every generated twin name and defines the namespace
	// the orphan sweep reclaims. Defaults to "brace_run_".
	NamePrefix string
	// Logger receives lifecycle events. Defaults to slog.Default().
	Logger *slog.Logger
}

// PoolStatus is a point-in-time snapshot for the HTTP status endpoint.
type PoolStatus struct {
	Target          int       `json:"target"`
	Warm            int       `json:"warm"`
	Warming         int       `json:"warming"`
	CheckedOut      int       `json:"checked_out"`
	WarmNames       []string  `json:"warm_names"`
	CheckedOutNames []string  `json:"checked_out_names"`
	Closed          bool      `json:"closed"`
	LastError       string    `json:"last_error,omitempty"`
	LastErrorAt     time.Time `json:"last_error_at,omitempty"`
}

// Manager keeps a pool of warm twins topped up and hands them out.
type Manager struct {
	cfg       Config
	log       *slog.Logger
	pool      *pgxpool.Pool
	adminConn *pgconn.Config
	adminURL  string

	// quotedTemplate is precomputed: the template name comes from config once,
	// and re-sanitizing it on every clone would just be noise.
	quotedTemplate string

	mu          sync.Mutex
	warm        []*Twin
	checkedOut  map[string]*Twin
	warming     int
	closed      bool
	lastErr     error
	lastErrTime time.Time

	refill chan struct{}
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func (c *Config) applyDefaults() {
	if c.PoolSize <= 0 {
		c.PoolSize = defaultPoolSize
	}
	if c.NamePrefix == "" {
		c.NamePrefix = defaultNamePrefix
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

func (c Config) validate() error {
	if strings.TrimSpace(c.AdminURL) == "" {
		return errors.New("twins: Config.AdminURL is required")
	}
	if strings.TrimSpace(c.TemplateName) == "" {
		return errors.New("twins: Config.TemplateName is required")
	}
	if !validPrefix.MatchString(c.NamePrefix) {
		return fmt.Errorf("twins: Config.NamePrefix %q must match %s", c.NamePrefix, validPrefix)
	}
	if strings.HasPrefix(c.TemplateName, c.NamePrefix) {
		// Otherwise the orphan sweep would cheerfully drop the template.
		return fmt.Errorf("twins: TemplateName %q must not start with NamePrefix %q", c.TemplateName, c.NamePrefix)
	}
	return nil
}

// NewManager verifies the environment, reclaims orphaned twins left by a
// previous crash, and starts the background refill worker.
func NewManager(ctx context.Context, cfg Config) (*Manager, error) {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	poolCfg, err := pgxpool.ParseConfig(cfg.AdminURL)
	if err != nil {
		return nil, fmt.Errorf("twins: parse AdminURL: %w", err)
	}
	adminDB := poolCfg.ConnConfig.Database
	// A pool pinned to a twin (or to the template) would deadlock teardown:
	// you cannot DROP the database you are connected to, and the template must
	// stay connection-free.
	if adminDB == cfg.TemplateName || strings.HasPrefix(adminDB, cfg.NamePrefix) {
		return nil, fmt.Errorf("twins: AdminURL must point at the maintenance database, not %q", adminDB)
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("twins: connect admin pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("twins: ping admin database %q: %w", adminDB, err)
	}

	m := &Manager{
		cfg:            cfg,
		log:            cfg.Logger.With("component", "twins"),
		pool:           pool,
		adminConn:      poolCfg.ConnConfig.Config.Copy(),
		adminURL:       cfg.AdminURL,
		quotedTemplate: quoteIdent(cfg.TemplateName),
		checkedOut:     make(map[string]*Twin),
		refill:         make(chan struct{}, 1),
	}

	if err := m.lockTemplate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	if err := m.sweepOrphans(ctx); err != nil {
		pool.Close()
		return nil, err
	}

	// The worker outlives the caller's ctx on purpose: NewManager's ctx is a
	// startup context, and the pool must keep refilling after it returns.
	workerCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	m.cancel = cancel
	m.wg.Add(1)
	go m.worker(workerCtx)
	m.signalRefill()

	return m, nil
}

// lockTemplate asserts the template exists and pins it to ALLOW_CONNECTIONS
// false. Idempotent: this runs on every startup, and re-running ALTER DATABASE
// with the same value is a no-op.
func (m *Manager) lockTemplate(ctx context.Context) error {
	var allowConn bool
	err := m.pool.QueryRow(ctx,
		`SELECT datallowconn FROM pg_database WHERE datname = $1`,
		m.cfg.TemplateName).Scan(&allowConn)
	if err != nil {
		return fmt.Errorf("twins: template database %q not found or unreadable: %w", m.cfg.TemplateName, err)
	}
	if !allowConn {
		return nil
	}
	if _, err := m.pool.Exec(ctx, `ALTER DATABASE `+m.quotedTemplate+` WITH ALLOW_CONNECTIONS false`); err != nil {
		return fmt.Errorf("twins: lock template %q: %w", m.cfg.TemplateName, err)
	}
	m.log.Info("twin.template_locked", "template", m.cfg.TemplateName)
	return nil
}

// sweepOrphans drops every database in our namespace at startup. A fresh
// Manager tracks nothing, so anything matching the prefix is by definition
// debris from a crashed previous run.
func (m *Manager) sweepOrphans(ctx context.Context) error {
	// starts_with rather than LIKE: the default prefix "brace_run_" is full of
	// underscores, which LIKE would treat as single-character wildcards.
	rows, err := m.pool.Query(ctx,
		`SELECT datname FROM pg_database WHERE starts_with(datname, $1) ORDER BY datname`,
		m.cfg.NamePrefix)
	if err != nil {
		return fmt.Errorf("twins: list orphan twins: %w", err)
	}
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			rows.Close()
			return fmt.Errorf("twins: scan orphan twin name: %w", err)
		}
		names = append(names, n)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("twins: list orphan twins: %w", err)
	}

	for _, n := range names {
		start := time.Now()
		if err := m.dropDatabase(ctx, n); err != nil {
			return fmt.Errorf("twins: sweep orphan %q: %w", n, err)
		}
		m.log.Info("twin.orphan_swept", "twin", n, "duration_ms", time.Since(start).Milliseconds())
	}
	return nil
}

// dropDatabase removes a database, forcing off any stragglers. WITH (FORCE) is
// PG13+ and terminates remaining backends itself, which is what makes teardown
// reliable while load generators are still winding down.
func (m *Manager) dropDatabase(ctx context.Context, name string) error {
	_, err := m.pool.Exec(ctx, `DROP DATABASE IF EXISTS `+quoteIdent(name)+` WITH (FORCE)`)
	if err != nil {
		// IF EXISTS covers the normal race, but a concurrent drop can still
		// surface 3D000 between the existence check and the drop.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "3D000" {
			return nil
		}
		return fmt.Errorf("twins: drop database %q: %w", name, err)
	}
	return nil
}

// terminateTemplateBackends evicts anything connected to the template.
// ALLOW_CONNECTIONS false stops *new* sessions but does nothing about sessions
// that were already open when it was set, and a single one of those is enough
// to fail an 80-second clone.
func (m *Manager) terminateTemplateBackends(ctx context.Context) (int, error) {
	rows, err := m.pool.Query(ctx,
		`SELECT pg_terminate_backend(pid) FROM pg_stat_activity
		 WHERE datname = $1 AND pid <> pg_backend_pid()`,
		m.cfg.TemplateName)
	if err != nil {
		return 0, fmt.Errorf("twins: terminate template backends: %w", err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		n++
	}
	if err := rows.Err(); err != nil {
		return n, fmt.Errorf("twins: terminate template backends: %w", err)
	}
	return n, nil
}

// clone creates one twin. Serialized by the single worker goroutine.
func (m *Manager) clone(ctx context.Context) (*Twin, error) {
	name, err := newTwinName(m.cfg.NamePrefix)
	if err != nil {
		return nil, err
	}
	m.log.Info("twin.warming", "twin", name, "template", m.cfg.TemplateName)
	start := time.Now()

	var lastErr error
	for attempt := 1; attempt <= maxCloneAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		killed, err := m.terminateTemplateBackends(ctx)
		if err != nil {
			return nil, err
		}
		if killed > 0 {
			m.log.Warn("twin.template_backends_terminated", "template", m.cfg.TemplateName, "count", killed)
		}

		// ALLOW_CONNECTIONS is stated explicitly rather than inherited: the
		// template is locked, and a twin nobody can connect to is useless.
		_, err = m.pool.Exec(ctx,
			`CREATE DATABASE `+quoteIdent(name)+` TEMPLATE `+m.quotedTemplate+` ALLOW_CONNECTIONS true`)
		if err == nil {
			t := &Twin{
				Name:      name,
				DSN:       dsnForDB(m.adminURL, m.adminConn, name),
				CreatedAt: time.Now(),
			}
			m.log.Info("twin.ready", "twin", name, "duration_ms", time.Since(start).Milliseconds(), "attempts", attempt)
			return t, nil
		}
		lastErr = err

		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != sqlstateObjectInUse {
			break
		}
		m.log.Warn("twin.clone_retry", "twin", name, "attempt", attempt, "sqlstate", pgErr.Code, "err", pgErr.Message)
		if !sleepCtx(ctx, time.Duration(attempt)*250*time.Millisecond) {
			return nil, ctx.Err()
		}
	}

	// A cancelled or half-failed CREATE DATABASE can still leave the target
	// behind; drop it so it never becomes an orphan the next run has to sweep.
	m.bestEffortDrop(name)
	return nil, fmt.Errorf("twins: clone %q from template %q: %w", name, m.cfg.TemplateName, lastErr)
}

// bestEffortDrop cleans up with a fresh context, because it is usually called
// on a path where the caller's context has already been cancelled.
func (m *Manager) bestEffortDrop(name string) {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	if err := m.dropDatabase(ctx, name); err != nil {
		m.log.Error("twin.drop_failed", "twin", name, "err", err)
	}
}

func (m *Manager) signalRefill() {
	select {
	case m.refill <- struct{}{}:
	default: // a refill pass is already pending; one wake-up is enough
	}
}

// worker is the only goroutine that clones. Keeping it single ensures two
// CREATE DATABASE ... TEMPLATE statements never overlap on the same template.
func (m *Manager) worker(ctx context.Context) {
	defer m.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-m.refill:
			m.fill(ctx)
		}
	}
}

func (m *Manager) fill(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			return
		}
		need := m.cfg.PoolSize - (len(m.warm) + m.warming)
		if need > 0 {
			m.warming++
		}
		m.mu.Unlock()
		if need <= 0 {
			return
		}

		t, err := m.clone(ctx)

		m.mu.Lock()
		m.warming--
		if err == nil {
			if m.closed {
				// Raced with Close: nobody will ever take this one.
				m.mu.Unlock()
				m.bestEffortDrop(t.Name)
				return
			}
			m.warm = append(m.warm, t)
		} else {
			m.lastErr = err
			m.lastErrTime = time.Now()
		}
		m.mu.Unlock()

		if err != nil {
			if ctx.Err() != nil {
				return
			}
			m.log.Error("twin.clone_failed", "err", err)
			// Back off, then ask for another pass so a transient failure heals
			// itself without spinning.
			if !sleepCtx(ctx, refillBackoff) {
				return
			}
			m.signalRefill()
			return
		}
	}
}

// Acquire hands back a warm twin, or ErrNoWarmTwin. It never clones: an
// 80-second CREATE DATABASE must never happen inside a user's request.
func (m *Manager) Acquire(ctx context.Context) (*Twin, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, ErrClosed
	}
	if len(m.warm) == 0 {
		m.mu.Unlock()
		m.signalRefill()
		return nil, ErrNoWarmTwin
	}
	t := m.warm[0]
	m.warm = append(m.warm[:0:0], m.warm[1:]...)
	m.checkedOut[t.Name] = t
	m.mu.Unlock()

	m.log.Info("twin.acquired", "twin", t.Name, "age_ms", time.Since(t.CreatedAt).Milliseconds())
	m.signalRefill()
	return t, nil
}

// Release destroys the twin and triggers a refill. Safe to call for a twin that
// has already vanished.
func (m *Manager) Release(ctx context.Context, t *Twin) error {
	if t == nil {
		return errors.New("twins: Release called with nil twin")
	}
	m.mu.Lock()
	if m.closed {
		// Close already owns teardown of everything it snapshotted; dropping
		// the twin here too would just race it.
		m.mu.Unlock()
		return ErrClosed
	}
	delete(m.checkedOut, t.Name)
	m.mu.Unlock()

	start := time.Now()
	if err := m.dropDatabase(ctx, t.Name); err != nil {
		return err
	}
	m.log.Info("twin.dropped", "twin", t.Name,
		"lifetime_ms", time.Since(t.CreatedAt).Milliseconds(),
		"duration_ms", time.Since(start).Milliseconds())
	m.signalRefill()
	return nil
}

// Status returns a snapshot for the UI. Safe to call concurrently.
func (m *Manager) Status() PoolStatus {
	m.mu.Lock()
	defer m.mu.Unlock()

	s := PoolStatus{
		Target:          m.cfg.PoolSize,
		Warm:            len(m.warm),
		Warming:         m.warming,
		CheckedOut:      len(m.checkedOut),
		Closed:          m.closed,
		WarmNames:       make([]string, 0, len(m.warm)),
		CheckedOutNames: make([]string, 0, len(m.checkedOut)),
	}
	for _, t := range m.warm {
		s.WarmNames = append(s.WarmNames, t.Name)
	}
	for name := range m.checkedOut {
		s.CheckedOutNames = append(s.CheckedOutNames, name)
	}
	sort.Strings(s.CheckedOutNames)
	if m.lastErr != nil {
		s.LastError = m.lastErr.Error()
		s.LastErrorAt = m.lastErrTime
	}
	return s
}

// Close stops refilling, drops every twin this Manager knows about (warm and
// still checked out), and closes the admin pool. Idempotent.
func (m *Manager) Close(ctx context.Context) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	m.mu.Unlock()

	// Stop the worker before dropping anything, so it cannot append a twin we
	// have already walked past.
	m.cancel()
	m.wg.Wait()

	m.mu.Lock()
	doomed := make([]*Twin, 0, len(m.warm)+len(m.checkedOut))
	doomed = append(doomed, m.warm...)
	for _, t := range m.checkedOut {
		doomed = append(doomed, t)
	}
	m.warm = nil
	m.checkedOut = make(map[string]*Twin)
	m.mu.Unlock()

	var errs []error
	for _, t := range doomed {
		if err := m.dropDatabase(ctx, t.Name); err != nil {
			errs = append(errs, err)
			continue
		}
		m.log.Info("twin.dropped", "twin", t.Name, "reason", "close")
	}
	m.pool.Close()
	m.log.Info("twin.manager_closed", "dropped", len(doomed), "errors", len(errs))
	return errors.Join(errs...)
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
