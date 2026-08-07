package api

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// A per-IP token bucket for POST /api/run.
//
// This is an abuse guard, not a security control. A run costs a warm twin and
// ~10 seconds of a dedicated CPU, so the cost of someone holding down the run
// button is real; the cost of them spoofing X-Forwarded-For to get around this
// is a second run, which the engine's single-run lock already refuses. That
// asymmetry is why an in-memory map is the right amount of machinery.

type bucket struct {
	tokens float64
	last   time.Time
}

type limiter struct {
	// ratePerSec is the sustained refill rate; burst is the bucket ceiling.
	ratePerSec float64
	burst      float64
	// now is injectable so the tests do not have to sleep for a minute.
	now func() time.Time

	mu      sync.Mutex
	buckets map[string]*bucket
	swept   time.Time
}

func newLimiter(perMinute float64, burst int) *limiter {
	if perMinute <= 0 {
		perMinute = 3
	}
	if burst <= 0 {
		burst = 2
	}
	return &limiter{
		ratePerSec: perMinute / 60,
		burst:      float64(burst),
		now:        time.Now,
		buckets:    make(map[string]*bucket),
	}
}

// allow consumes a token for key, returning false when the bucket is empty.
func (l *limiter) allow(key string) bool {
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()

	l.sweepLocked(now)

	b := l.buckets[key]
	if b == nil {
		b = &bucket{tokens: l.burst, last: now}
		l.buckets[key] = b
	}
	if elapsed := now.Sub(b.last).Seconds(); elapsed > 0 {
		b.tokens += elapsed * l.ratePerSec
		if b.tokens > l.burst {
			b.tokens = l.burst
		}
		b.last = now
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// sweepLocked drops buckets that have been full (i.e. idle) long enough that
// forgetting them changes nothing. Without it the map is an unbounded leak
// keyed by attacker-controlled strings.
func (l *limiter) sweepLocked(now time.Time) {
	if now.Sub(l.swept) < time.Minute {
		return
	}
	l.swept = now
	refillFull := time.Duration(float64(time.Second) * l.burst / l.ratePerSec)
	for k, b := range l.buckets {
		if now.Sub(b.last) > refillFull {
			delete(l.buckets, k)
		}
	}
}

// clientIP identifies the caller for rate-limiting.
//
// X-Forwarded-For is caller-supplied and therefore forgeable: trusting its
// first entry lets anyone reset their bucket by inventing a new value per
// request, which defeats the one control bounding a scarce resource (a warm
// twin plus dedicated CPU for up to MaxRunDuration per run). So we only honour
// it when the request actually arrived from a private-network hop — which is
// what Zerops' balancer is — and we take the LAST entry, the one appended by
// that trusted hop rather than any value the client pre-seeded. Otherwise the
// socket address wins.
func clientIP(r *http.Request) string {
	host := r.RemoteAddr
	if h, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		host = h
	}
	if trustedHop(host) {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			parts := strings.Split(xff, ",")
			last := strings.TrimSpace(parts[len(parts)-1])
			if last != "" {
				return last
			}
		}
	}
	return host
}

// trustedHop reports whether addr is a loopback or private-range address, i.e.
// an in-project balancer rather than the open internet.
func trustedHop(addr string) bool {
	ip := net.ParseIP(addr)
	if ip == nil {
		return false
	}
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast()
}
