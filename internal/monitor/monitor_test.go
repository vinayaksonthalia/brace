package monitor

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// TestBlockedTrackerRectangleRule pins the arithmetic behind BlockedSeconds:
// each sample books the gap that just elapsed, and only if somebody was
// waiting when we looked.
func TestBlockedTrackerRectangleRule(t *testing.T) {
	base := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	tr := blockedTracker{interval: 250 * time.Millisecond, last: base}

	// 0.25s clear, 1.00s blocked (4 samples), 0.25s clear.
	steps := []struct {
		at      time.Duration
		blocked int
	}{
		{250 * time.Millisecond, 0},
		{500 * time.Millisecond, 3},
		{750 * time.Millisecond, 7},
		{1000 * time.Millisecond, 5},
		{1250 * time.Millisecond, 2},
		{1500 * time.Millisecond, 0},
	}
	for _, s := range steps {
		tr.observe(base.Add(s.at), s.blocked)
	}

	if got := tr.blocked; got != time.Second {
		t.Errorf("blocked = %v, want 1s", got)
	}
	if tr.peak != 7 {
		t.Errorf("peak = %d, want 7", tr.peak)
	}
}

// TestBlockedTrackerFirstSampleOpensNoWindow: the sample taken at Start has no
// previous sample to bound a rectangle with, so it must book nothing.
func TestBlockedTrackerFirstSampleOpensNoWindow(t *testing.T) {
	base := time.Now()
	tr := blockedTracker{interval: 250 * time.Millisecond}
	tr.observe(base, 4)
	if tr.blocked != 0 {
		t.Errorf("first sample booked %v, want 0", tr.blocked)
	}
	if tr.peak != 4 {
		t.Errorf("peak = %d, want 4 (the count is still real)", tr.peak)
	}
	tr.observe(base.Add(250*time.Millisecond), 4)
	if tr.blocked != 250*time.Millisecond {
		t.Errorf("second sample booked %v, want 250ms", tr.blocked)
	}
}

// TestBlockedTrackerCapsPathologicalGaps: a stalled poller (GC, suspended
// laptop, an overloaded box) must not be able to invent freeze time it never
// observed.
func TestBlockedTrackerCapsPathologicalGaps(t *testing.T) {
	base := time.Now()
	tr := blockedTracker{interval: 250 * time.Millisecond, last: base}
	tr.observe(base.Add(30*time.Second), 1)
	want := time.Duration(maxSampleGapFactor) * 250 * time.Millisecond
	if tr.blocked != want {
		t.Errorf("30s gap booked %v, want it capped at %v", tr.blocked, want)
	}
}

func TestBlockedTrackerIgnoresBackwardsClock(t *testing.T) {
	base := time.Now()
	tr := blockedTracker{interval: 250 * time.Millisecond, last: base}
	tr.observe(base.Add(-time.Second), 3)
	if tr.blocked != 0 {
		t.Errorf("a backwards clock booked %v, want 0", tr.blocked)
	}
}

func TestSortModesStrongestFirst(t *testing.T) {
	set := map[string]struct{}{
		"RowExclusiveLock":    {},
		"AccessExclusiveLock": {},
		"ShareLock":           {},
		"SomeFutureLock":      {},
	}
	got := sortModes(set)
	want := []string{"AccessExclusiveLock", "ShareLock", "RowExclusiveLock", "SomeFutureLock"}
	if len(got) != len(want) {
		t.Fatalf("sortModes returned %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sortModes = %v, want %v", got, want)
		}
	}
}

func TestStrongest(t *testing.T) {
	cases := []struct {
		in   []string
		want string
	}{
		{nil, ""},
		{[]string{"ShareLock"}, "ShareLock"},
		{[]string{"RowExclusiveLock", "AccessExclusiveLock", "ShareLock"}, "AccessExclusiveLock"},
		{[]string{"ShareUpdateExclusiveLock", "AccessShareLock"}, "ShareUpdateExclusiveLock"},
	}
	for _, c := range cases {
		if got := Strongest(c.in); got != c.want {
			t.Errorf("Strongest(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestStopAndReportWithoutStart is the degradation contract: a monitor that
// never connected must still hand back a usable, honest report rather than
// panic or block the run.
func TestStopAndReportWithoutStart(t *testing.T) {
	m := New(Config{DSN: "", Logger: quietLogger()})
	rep := m.StopAndReport()
	if rep.Samples != 0 || rep.BlockedSeconds != 0 || len(rep.LockModes) != 0 {
		t.Errorf("unstarted monitor reported data: %+v", rep)
	}
	if rep.Degraded() {
		t.Errorf("never-started monitor should not claim failed samples: %+v", rep)
	}
}

// TestStartWithoutDSNDegrades: a misconfigured monitor reports the failure to
// its caller AND remembers it in the report, but is otherwise inert.
func TestStartWithoutDSNDegrades(t *testing.T) {
	m := New(Config{Logger: quietLogger()})
	if err := m.Start(context.Background()); err == nil {
		t.Fatal("Start with an empty DSN must return an error")
	}
	rep := m.StopAndReport()
	if !rep.Degraded() {
		t.Errorf("failed Start must mark the report degraded: %+v", rep)
	}
	if rep.LastError == "" {
		t.Error("failed Start must record LastError")
	}
	// And a second Stop is still safe.
	_ = m.StopAndReport()
}

func TestStartTwiceIsRefused(t *testing.T) {
	m := New(Config{DSN: "postgres://nobody@127.0.0.1:1/none", Logger: quietLogger()})
	// The first Start fails to connect (nothing listens on port 1) but still
	// marks the monitor used.
	_ = m.Start(context.Background())
	if err := m.Start(context.Background()); err == nil {
		t.Fatal("second Start must be refused")
	}
	_ = m.StopAndReport()
}

func TestReportRounding(t *testing.T) {
	if got := round1ms(1234 * time.Millisecond); got != 1.234 {
		t.Errorf("round1ms(1.234s) = %v", got)
	}
	if got := round1ms(0); got != 0 {
		t.Errorf("round1ms(0) = %v", got)
	}
}
