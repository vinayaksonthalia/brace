package twins

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newTestManager builds a Manager with no database behind it. Every method
// exercised here is pure bookkeeping, which is exactly what we want to pin
// down without paying for an 80-second clone.
func newTestManager(poolSize int, warm ...*Twin) *Manager {
	return &Manager{
		cfg:         Config{PoolSize: poolSize, NamePrefix: defaultNamePrefix, TemplateName: "brace_template"},
		log:         discardLogger(),
		warm:        warm,
		checkedOut:  make(map[string]*Twin),
		pendingDrop: make(map[string]*Twin),
		released:    make(map[string]struct{}),
		refill:      make(chan struct{}, 1),
	}
}

func twin(name string) *Twin {
	return &Twin{Name: name, DSN: "postgres://localhost/" + name, CreatedAt: time.Now()}
}

func TestNewTwinNameIsUniqueAndWellFormed(t *testing.T) {
	ident := regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)
	seen := make(map[string]bool)
	for i := 0; i < 500; i++ {
		name, err := newTwinName(defaultNamePrefix)
		if err != nil {
			t.Fatalf("newTwinName: %v", err)
		}
		if !strings.HasPrefix(name, defaultNamePrefix) {
			t.Fatalf("name %q missing prefix %q", name, defaultNamePrefix)
		}
		if !ident.MatchString(name) {
			t.Fatalf("name %q is not a bare identifier", name)
		}
		if len(name) > maxIdentLen {
			t.Fatalf("name %q is %d bytes, over the %d-byte limit", name, len(name), maxIdentLen)
		}
		if seen[name] {
			t.Fatalf("duplicate name generated: %q", name)
		}
		seen[name] = true
	}
}

func TestNewTwinNameRejectsOverlongPrefix(t *testing.T) {
	if _, err := newTwinName(strings.Repeat("x", 60)); err == nil {
		t.Fatal("expected an error for a prefix that cannot fit in 63 bytes")
	}
}

func TestQuoteIdent(t *testing.T) {
	cases := map[string]string{
		"brace_run_1":     `"brace_run_1"`,
		"Weird Name":      `"Weird Name"`,
		`quote"injection`: `"quote""injection"`,
		"drop; --":        `"drop; --"`,
	}
	for in, want := range cases {
		if got := quoteIdent(in); got != want {
			t.Errorf("quoteIdent(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestDsnForDB(t *testing.T) {
	cases := []struct {
		name string
		in   string
		db   string
		want string
	}{
		{
			name: "trust auth, no password",
			in:   "postgres://postgres@localhost:5433/postgres",
			db:   "brace_run_x",
			want: "postgres://postgres@localhost:5433/brace_run_x",
		},
		{
			// Options other than the database name must survive verbatim --
			// dropping sslmode would silently downgrade the twin connection.
			name: "options preserved",
			in:   "postgres://u:p@h:5432/postgres?sslmode=require&application_name=brace",
			db:   "brace_run_w",
			want: "postgres://u:p@h:5432/brace_run_w?sslmode=require&application_name=brace",
		},
		{
			name: "with password",
			in:   "postgres://admin:s3cret@db.internal:5432/postgres?sslmode=disable",
			db:   "brace_run_y",
			want: "postgres://admin:s3cret@db.internal:5432/brace_run_y?sslmode=disable",
		},
		{
			name: "keyword value dsn",
			in:   "host=localhost port=5433 user=postgres dbname=postgres sslmode=disable",
			db:   "brace_run_z",
			want: "postgres://postgres@localhost:5433/brace_run_z?sslmode=disable",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := pgconn.ParseConfig(tc.in)
			if err != nil {
				t.Fatalf("ParseConfig: %v", err)
			}
			if got := dsnForDB(tc.in, cfg, tc.db); got != tc.want {
				t.Errorf("dsnForDB = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestConfigDefaults(t *testing.T) {
	c := Config{AdminURL: "postgres://localhost/postgres", TemplateName: "brace_template"}
	c.applyDefaults()
	if c.PoolSize != defaultPoolSize {
		t.Errorf("PoolSize = %d, want %d", c.PoolSize, defaultPoolSize)
	}
	if c.NamePrefix != defaultNamePrefix {
		t.Errorf("NamePrefix = %q, want %q", c.NamePrefix, defaultNamePrefix)
	}
	if c.Logger == nil {
		t.Error("Logger should default to slog.Default()")
	}
	if err := c.validate(); err != nil {
		t.Errorf("validate: %v", err)
	}
}

func TestConfigValidate(t *testing.T) {
	base := func() Config {
		c := Config{AdminURL: "postgres://localhost/postgres", TemplateName: "brace_template"}
		c.applyDefaults()
		return c
	}
	cases := []struct {
		name  string
		mut   func(*Config)
		valid bool
	}{
		{"ok", func(*Config) {}, true},
		{"missing admin url", func(c *Config) { c.AdminURL = "  " }, false},
		{"missing template", func(c *Config) { c.TemplateName = "" }, false},
		{"prefix with quote", func(c *Config) { c.NamePrefix = `bad"prefix_` }, false},
		{"prefix starting with digit", func(c *Config) { c.NamePrefix = "1bad_" }, false},
		{"prefix with uppercase", func(c *Config) { c.NamePrefix = "Brace_" }, false},
		// The sweep drops everything matching the prefix, so a template inside
		// that namespace would delete itself on the next startup.
		{"template inside sweep namespace", func(c *Config) { c.TemplateName = "brace_run_template" }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := base()
			tc.mut(&c)
			err := c.validate()
			if tc.valid && err != nil {
				t.Errorf("expected valid, got %v", err)
			}
			if !tc.valid && err == nil {
				t.Error("expected an error, got nil")
			}
		})
	}
}

func TestAcquireEmptyPoolReturnsErrNoWarmTwin(t *testing.T) {
	m := newTestManager(2)
	got, err := m.Acquire(context.Background())
	if !errors.Is(err, ErrNoWarmTwin) {
		t.Fatalf("err = %v, want ErrNoWarmTwin", err)
	}
	if got != nil {
		t.Fatalf("twin = %v, want nil", got)
	}
	// An empty Acquire must nudge the worker, or the pool never recovers.
	select {
	case <-m.refill:
	default:
		t.Error("Acquire on an empty pool should have signalled a refill")
	}
}

func TestAcquireIsFIFOAndMovesToCheckedOut(t *testing.T) {
	a, b := twin("brace_run_a"), twin("brace_run_b")
	m := newTestManager(2, a, b)

	got, err := m.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if got.Name != "brace_run_a" {
		t.Fatalf("got %q, want the oldest twin brace_run_a", got.Name)
	}

	s := m.Status()
	if s.Warm != 1 || s.CheckedOut != 1 {
		t.Fatalf("status warm=%d checkedOut=%d, want 1/1", s.Warm, s.CheckedOut)
	}
	if len(s.WarmNames) != 1 || s.WarmNames[0] != "brace_run_b" {
		t.Fatalf("WarmNames = %v, want [brace_run_b]", s.WarmNames)
	}
	if len(s.CheckedOutNames) != 1 || s.CheckedOutNames[0] != "brace_run_a" {
		t.Fatalf("CheckedOutNames = %v, want [brace_run_a]", s.CheckedOutNames)
	}

	if _, err := m.Acquire(context.Background()); err != nil {
		t.Fatalf("second Acquire: %v", err)
	}
	if _, err := m.Acquire(context.Background()); !errors.Is(err, ErrNoWarmTwin) {
		t.Fatalf("third Acquire err = %v, want ErrNoWarmTwin", err)
	}
}

func TestAcquireAfterCloseReturnsErrClosed(t *testing.T) {
	m := newTestManager(1, twin("brace_run_a"))
	m.closed = true
	if _, err := m.Acquire(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
}

func TestAcquireHonoursCancelledContext(t *testing.T) {
	m := newTestManager(1, twin("brace_run_a"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := m.Acquire(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if m.Status().Warm != 1 {
		t.Error("a cancelled Acquire must not consume a warm twin")
	}
}

func TestReleaseNilTwin(t *testing.T) {
	m := newTestManager(1)
	if err := m.Release(context.Background(), nil); err == nil {
		t.Fatal("expected an error for a nil twin")
	}
}

func TestReleaseAfterCloseReturnsErrClosedAndKeepsBookkeeping(t *testing.T) {
	tw := twin("brace_run_a")
	m := newTestManager(1)
	m.checkedOut[tw.Name] = tw
	m.closed = true

	if err := m.Release(context.Background(), tw); !errors.Is(err, ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
	// Close owns teardown of what it snapshotted; Release must not quietly
	// remove the twin from the tracking map and orphan the database.
	if _, ok := m.checkedOut[tw.Name]; !ok {
		t.Error("Release after Close must leave the twin tracked for Close to drop")
	}
}

func TestStatusSnapshotIsIndependent(t *testing.T) {
	m := newTestManager(3, twin("brace_run_a"))
	m.warming = 2

	s := m.Status()
	if s.Target != 3 || s.Warm != 1 || s.Warming != 2 || s.Closed {
		t.Fatalf("unexpected status: %+v", s)
	}
	s.WarmNames[0] = "mutated"
	if m.Status().WarmNames[0] != "brace_run_a" {
		t.Error("Status must return a copy, not a live view of internal state")
	}
}

func TestStatusReportsLastCloneError(t *testing.T) {
	m := newTestManager(1)
	m.lastErr = errors.New("boom")
	m.lastErrTime = time.Now()
	s := m.Status()
	if s.LastError != "boom" || s.LastErrorAt.IsZero() {
		t.Fatalf("last error not surfaced: %+v", s)
	}
}

func TestSignalRefillCoalesces(t *testing.T) {
	m := newTestManager(1)
	// Must never block, however many times it is called: the channel is a
	// wake-up flag, not a queue of work items.
	for i := 0; i < 100; i++ {
		m.signalRefill()
	}
	if len(m.refill) != 1 {
		t.Fatalf("refill channel holds %d items, want 1", len(m.refill))
	}
}

func TestConcurrentAcquireHandsOutEachTwinOnce(t *testing.T) {
	const n = 50
	warm := make([]*Twin, n)
	for i := range warm {
		warm[i] = twin("brace_run_" + string(rune('a'+i%26)) + string(rune('a'+i/26)))
	}
	m := newTestManager(n, warm...)

	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		got   = make(map[string]int)
		empty int
	)
	for i := 0; i < n*2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tw, err := m.Acquire(context.Background())
			mu.Lock()
			defer mu.Unlock()
			if errors.Is(err, ErrNoWarmTwin) {
				empty++
				return
			}
			if err != nil {
				t.Errorf("Acquire: %v", err)
				return
			}
			got[tw.Name]++
		}()
	}
	wg.Wait()

	if len(got) != n {
		t.Fatalf("handed out %d distinct twins, want %d", len(got), n)
	}
	for name, count := range got {
		if count != 1 {
			t.Errorf("twin %q handed out %d times", name, count)
		}
	}
	if empty != n {
		t.Errorf("%d callers got ErrNoWarmTwin, want %d", empty, n)
	}
}

func TestSleepCtxReturnsFalseOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if sleepCtx(ctx, time.Hour) {
		t.Fatal("sleepCtx should report false when the context is done")
	}
	if !sleepCtx(context.Background(), time.Millisecond) {
		t.Fatal("sleepCtx should report true when the timer fires")
	}
}
