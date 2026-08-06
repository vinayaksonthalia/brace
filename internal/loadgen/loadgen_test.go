package loadgen

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// pgErr fabricates the error pgx hands back for a given SQLSTATE. The messages
// are copied verbatim from PostgreSQL 16.14 so the test reads like the log.
func pgErr(code, msg string) error {
	return &pgconn.PgError{Code: code, Message: msg, Severity: "ERROR"}
}

// TestClassify is the guard on the headline number. Every case here is a claim
// about what may and may not be reported as a dead customer.
func TestClassify(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want Outcome
	}{
		{
			name: "success",
			err:  nil,
			want: OutcomeOK,
		},
		{
			// The money case. Observed on PG 16.14 with lock_timeout set and a
			// conflicting ACCESS EXCLUSIVE lock held by another session.
			name: "lock_timeout is 55P03 and is a casualty",
			err:  pgErr("55P03", "canceling statement due to lock timeout"),
			want: OutcomeDied,
		},
		{
			name: "lock_timeout wrapped by a caller is still a casualty",
			err:  fmt.Errorf("insert orders: %w", pgErr("55P03", "canceling statement due to lock timeout")),
			want: OutcomeDied,
		},
		{
			// 57014 cannot come from a lock wait on this pool: we never set
			// statement_timeout and PG16 reports lock waits as 55P03.
			name: "query_canceled is never a casualty",
			err:  pgErr("57014", "canceling statement due to user request"),
			want: OutcomeStopped,
		},
		{
			name: "statement_timeout is never a casualty",
			err:  pgErr("57014", "canceling statement due to statement timeout"),
			want: OutcomeStopped,
		},
		{
			name: "graceful stop",
			err:  context.Canceled,
			want: OutcomeStopped,
		},
		{
			name: "graceful stop wrapped by pgx",
			err:  fmt.Errorf("timeout: context already done: %w", context.Canceled),
			want: OutcomeStopped,
		},
		{
			name: "deadline exceeded",
			err:  context.DeadlineExceeded,
			want: OutcomeStopped,
		},
		{
			name: "deadlock is an error, not a casualty",
			err:  pgErr("40P01", "deadlock detected"),
			want: OutcomeErrored,
		},
		{
			name: "serialization failure is an error, not a casualty",
			err:  pgErr("40001", "could not serialize access due to concurrent update"),
			want: OutcomeErrored,
		},
		{
			// The migration dropped the table: real damage, but not a lock
			// death. It must stay visible in its own bucket.
			name: "undefined table is an error, not a casualty",
			err:  pgErr("42P01", `relation "orders" does not exist`),
			want: OutcomeErrored,
		},
		{
			name: "admin shutdown (twin dropped) is an error",
			err:  pgErr("57P01", "terminating connection due to administrator command"),
			want: OutcomeErrored,
		},
		{
			name: "network failure is an error",
			err:  errors.New("failed to connect to `host=localhost`: dial error"),
			want: OutcomeErrored,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Classify(tc.err); got != tc.want {
				t.Fatalf("Classify(%v) = %s, want %s", tc.err, got, tc.want)
			}
		})
	}
}

// newTestGenerator builds a Generator without touching a database, so label
// selection and counting can be tested in isolation.
func newTestGenerator(t *testing.T, labels []LabelSpec) *Generator {
	t.Helper()
	cfg := Config{DSN: "postgres://x/y", Labels: labels}
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	g := &Generator{cfg: cfg, log: cfg.Logger}
	for _, spec := range cfg.Labels {
		g.labels = append(g.labels, &labelCounter{spec: spec})
		g.totalWeight += spec.Weight
		g.cumWeights = append(g.cumWeights, g.totalWeight)
	}
	return g
}

func TestPickLabelDistribution(t *testing.T) {
	g := newTestGenerator(t, nil) // defaults: checkout 3, login 2, add_to_cart 5
	rng := rand.New(rand.NewSource(1))

	const n = 200_000
	counts := map[string]int{}
	for i := 0; i < n; i++ {
		counts[g.pickLabel(rng).spec.Name]++
	}

	want := map[string]float64{"checkout": 0.3, "login": 0.2, "add_to_cart": 0.5}
	for name, frac := range want {
		got := float64(counts[name]) / n
		if math.Abs(got-frac) > 0.01 {
			t.Errorf("label %q share = %.4f, want %.2f (+/-0.01)", name, got, frac)
		}
	}
	if total := counts["checkout"] + counts["login"] + counts["add_to_cart"]; total != n {
		t.Fatalf("picks accounted for %d of %d — a label was never returned", total, n)
	}
}

func TestPickLabelSingleLabelAlwaysWins(t *testing.T) {
	g := newTestGenerator(t, []LabelSpec{{Name: "solo", Weight: 1, Statement: "INSERT INTO orders VALUES ($1,$2)"}})
	rng := rand.New(rand.NewSource(7))
	for i := 0; i < 1000; i++ {
		if got := g.pickLabel(rng).spec.Name; got != "solo" {
			t.Fatalf("pickLabel = %q, want solo", got)
		}
	}
}

func TestSnapshotAggregates(t *testing.T) {
	g := newTestGenerator(t, nil)
	g.labels[0].ok.Store(10) // checkout
	g.labels[0].died.Store(3)
	g.labels[1].died.Store(4) // login
	g.labels[2].ok.Store(20)  // add_to_cart
	g.labels[2].errored.Store(5)
	g.writesPerSec.Store(30)

	s := g.Snapshot()
	if s.WritesPerSec != 30 {
		t.Errorf("WritesPerSec = %d, want 30", s.WritesPerSec)
	}
	if s.CasualtiesTotal != 7 {
		t.Errorf("CasualtiesTotal = %d, want 7", s.CasualtiesTotal)
	}
	if s.ErroredTotal != 5 {
		t.Errorf("ErroredTotal = %d, want 5", s.ErroredTotal)
	}
	if len(s.ByLabel) != 3 {
		t.Fatalf("ByLabel has %d entries, want 3", len(s.ByLabel))
	}
	if got := s.ByLabel["checkout"]; got != (LabelStats{OK: 10, Died: 3}) {
		t.Errorf("ByLabel[checkout] = %+v", got)
	}
	// The by-label casualties must sum to the headline; the UI shows both.
	var sum int64
	for _, ls := range s.ByLabel {
		sum += ls.Died
	}
	if sum != s.CasualtiesTotal {
		t.Errorf("by-label casualties sum to %d, headline says %d", sum, s.CasualtiesTotal)
	}
}

func TestRateWindowing(t *testing.T) {
	if testing.Short() {
		t.Skip("windowing test spans real ticker seconds")
	}
	g := newTestGenerator(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	g.wg.Add(1)
	go g.rateLoop(ctx)

	// Before the first tick closes a window, the reported rate is still zero.
	if got := g.Snapshot().WritesPerSec; got != 0 {
		t.Fatalf("WritesPerSec before first window = %d, want 0", got)
	}

	for i := 0; i < 42; i++ {
		g.window.Add(1)
	}
	time.Sleep(1300 * time.Millisecond)
	if got := g.Snapshot().WritesPerSec; got != 42 {
		t.Fatalf("WritesPerSec after first window = %d, want 42", got)
	}

	// The window is a window, not a running total: an idle second reports 0.
	time.Sleep(1100 * time.Millisecond)
	if got := g.Snapshot().WritesPerSec; got != 0 {
		t.Fatalf("WritesPerSec after idle window = %d, want 0", got)
	}

	cancel()
	g.wg.Wait()
}

func TestConfigDefaults(t *testing.T) {
	var c Config
	c.DSN = "postgres://x/y"
	c.applyDefaults()

	if c.Workers != 24 {
		t.Errorf("Workers = %d, want 24", c.Workers)
	}
	if c.LockTimeout != 1500*time.Millisecond {
		t.Errorf("LockTimeout = %s, want 1.5s", c.LockTimeout)
	}
	if len(c.Labels) != 3 {
		t.Fatalf("Labels = %d, want 3 defaults", len(c.Labels))
	}
	if c.Logger == nil {
		t.Error("Logger not defaulted")
	}
	want := map[string]int{"checkout": 3, "login": 2, "add_to_cart": 5}
	for _, l := range c.Labels {
		if want[l.Name] != l.Weight {
			t.Errorf("label %q weight = %d, want %d", l.Name, l.Weight, want[l.Name])
		}
	}
}

func TestConfigValidate(t *testing.T) {
	good := LabelSpec{Name: "a", Weight: 1, Statement: "INSERT INTO orders VALUES ($1,$2)"}
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"ok", Config{DSN: "postgres://x/y", Labels: []LabelSpec{good}}, false},
		{"missing dsn", Config{Labels: []LabelSpec{good}}, true},
		{"blank dsn", Config{DSN: "   ", Labels: []LabelSpec{good}}, true},
		{"empty label name", Config{DSN: "d", Labels: []LabelSpec{{Weight: 1, Statement: "x"}}}, true},
		{"zero weight", Config{DSN: "d", Labels: []LabelSpec{{Name: "a", Statement: "x"}}}, true},
		{"negative weight", Config{DSN: "d", Labels: []LabelSpec{{Name: "a", Weight: -1, Statement: "x"}}}, true},
		{"empty statement", Config{DSN: "d", Labels: []LabelSpec{{Name: "a", Weight: 1}}}, true},
		{"duplicate label", Config{DSN: "d", Labels: []LabelSpec{good, good}}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.validate()
			if (err != nil) != tc.wantErr {
				t.Fatalf("validate() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestNewRejectsBadConfig(t *testing.T) {
	if _, err := New(context.Background(), Config{}); err == nil {
		t.Fatal("New with empty DSN should fail")
	}
	if _, err := New(context.Background(), Config{DSN: "://nonsense"}); err == nil {
		t.Fatal("New with unparseable DSN should fail")
	}
}

func TestStopWithoutStartIsSafeAndIdempotent(t *testing.T) {
	g := newTestGenerator(t, nil)
	ctx := context.Background()
	if err := g.Stop(ctx); err != nil {
		t.Fatalf("first Stop: %v", err)
	}
	if err := g.Stop(ctx); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
	if err := g.Start(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("Start after Stop = %v, want ErrClosed", err)
	}
}

func TestOutcomeString(t *testing.T) {
	for _, tc := range []struct {
		o    Outcome
		want string
	}{
		{OutcomeOK, "ok"},
		{OutcomeDied, "died"},
		{OutcomeStopped, "stopped"},
		{OutcomeErrored, "errored"},
		{Outcome(9), "unknown(9)"},
	} {
		if got := tc.o.String(); got != tc.want {
			t.Errorf("Outcome(%d).String() = %q, want %q", tc.o, got, tc.want)
		}
	}
}
