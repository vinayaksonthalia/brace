package engine

import (
	"sync"
	"time"

	"github.com/vinayaksonthalia/brace/internal/loadgen"
)

// The timeline sampler.
//
// It records cumulative counters, not rates, and derives the rate afterwards.
// That matters: loadgen's own WritesPerSec closes a window once a second, which
// is four times coarser than we sample, so a 400ms freeze would be smeared
// across the seconds either side of it and could vanish entirely. Differencing
// the cumulative success count between two samples gives an honest rate at
// whatever resolution we choose to look.

type sample struct {
	at   time.Time
	ok   int64
	died int64
}

type sampler struct {
	gen      *loadgen.Generator
	interval time.Duration
	begin    time.Time

	mu      sync.Mutex
	samples []sample

	once sync.Once
	quit chan struct{}
	done chan struct{}
}

func newSampler(gen *loadgen.Generator, interval time.Duration) *sampler {
	s := &sampler{
		gen:      gen,
		interval: interval,
		begin:    time.Now(),
		quit:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	s.take()
	return s
}

func (s *sampler) start() {
	go func() {
		defer close(s.done)
		t := time.NewTicker(s.interval)
		defer t.Stop()
		for {
			select {
			case <-s.quit:
				return
			case <-t.C:
				s.take()
			}
		}
	}()
}

// stop halts sampling and waits for the goroutine, so that points() after stop
// is stable. Idempotent, and safe on a sampler that was never started — start
// is always called immediately after construction in this package, but a future
// caller that forgets should get a no-op rather than a hang.
func (s *sampler) stop() {
	s.once.Do(func() {
		close(s.quit)
		select {
		case <-s.done:
		case <-time.After(2 * s.interval):
			// The sampler goroutine only ever does a Snapshot and an append; if
			// it has not returned in two intervals it was never started.
		}
	})
}

func (s *sampler) take() {
	st := s.gen.Snapshot()
	var ok int64
	for _, ls := range st.ByLabel {
		ok += ls.OK
	}
	s.mu.Lock()
	s.samples = append(s.samples, sample{at: time.Now(), ok: ok, died: st.CasualtiesTotal})
	s.mu.Unlock()
}

// since converts an absolute instant to the timeline's seconds-since-load-start.
func (s *sampler) since(t time.Time) float64 {
	return roundSeconds(t.Sub(s.begin).Seconds())
}

// points derives the rate trace. The first sample bounds no interval, so the
// timeline begins at the second one; emitting a leading zero would draw a
// freeze that never happened.
func (s *sampler) points() []RatePoint {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.samples) < 2 {
		return []RatePoint{}
	}
	out := make([]RatePoint, 0, len(s.samples)-1)
	for i := 1; i < len(s.samples); i++ {
		dt := s.samples[i].at.Sub(s.samples[i-1].at).Seconds()
		var wps int64
		if dt > 0 {
			wps = int64(float64(s.samples[i].ok-s.samples[i-1].ok)/dt + 0.5)
		}
		out = append(out, RatePoint{
			T:          roundSeconds(s.samples[i].at.Sub(s.begin).Seconds()),
			WPS:        wps,
			Casualties: s.samples[i].died,
		})
	}
	return out
}

// rateBetween is the derived write rate across a wall-clock window, used to
// measure the baseline. It takes the first sample at or after `from` and the
// last at or before `to`, so a window with fewer than two samples in it yields
// 0 rather than a fabricated rate.
func (s *sampler) rateBetween(from, to time.Time) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	first, last := -1, -1
	for i, sm := range s.samples {
		if sm.at.Before(from) {
			continue
		}
		if sm.at.After(to) {
			break
		}
		if first < 0 {
			first = i
		}
		last = i
	}
	if first < 0 || last <= first {
		return 0
	}
	dt := s.samples[last].at.Sub(s.samples[first].at).Seconds()
	if dt <= 0 {
		return 0
	}
	return float64(s.samples[last].ok-s.samples[first].ok) / dt
}
