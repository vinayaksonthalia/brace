package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newTestServer(t *testing.T) *Server {
	t.Helper()
	s := New(Config{Logger: quiet(), TickInterval: 50 * time.Millisecond})
	t.Cleanup(s.Close)
	return s
}

// --------------------------------------------------------------- readiness

func TestHealthzAnswersWhileStarting(t *testing.T) {
	s := newTestServer(t)
	s.SetPhase("seeding")
	h := s.Handler()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("healthz = %d, want 200", rec.Code)
	}
	if got := rec.Body.String(); got != "ok" {
		t.Fatalf("healthz body = %q, want %q", got, "ok")
	}
}

func TestAPIIsStartingUntilReady(t *testing.T) {
	s := newTestServer(t)
	s.SetPhase("seeding")
	h := s.Handler()

	for _, path := range []string{"/api/status", "/api/stats", "/api/history", "/api/stream", "/api/report/1"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("GET %s while starting = %d, want 503", path, rec.Code)
		}
		var body map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("GET %s: decode: %v", path, err)
		}
		if body["error"] != "starting" || body["phase"] != "seeding" {
			t.Fatalf("GET %s body = %v, want error=starting phase=seeding", path, body)
		}
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/run", strings.NewReader(`{"sql":"VACUUM orders"}`)))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("POST /api/run while starting = %d, want 503", rec.Code)
	}
}

// --------------------------------------------------------------- CORS

func TestCORSHeadersAndPreflight(t *testing.T) {
	s := newTestServer(t)
	h := s.Handler()

	req := httptest.NewRequest(http.MethodOptions, "/api/run", nil)
	req.Header.Set("Origin", "https://web.example.com")
	req.Header.Set("Access-Control-Request-Method", "POST")
	req.Header.Set("Access-Control-Request-Headers", "content-type")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("preflight = %d, want 204", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("Allow-Origin = %q, want *", got)
	}
	for _, want := range []string{"GET", "POST", "OPTIONS"} {
		if !strings.Contains(rec.Header().Get("Access-Control-Allow-Methods"), want) {
			t.Fatalf("Allow-Methods = %q, missing %s", rec.Header().Get("Access-Control-Allow-Methods"), want)
		}
	}
	if !strings.Contains(strings.ToLower(rec.Header().Get("Access-Control-Allow-Headers")), "content-type") {
		t.Fatalf("Allow-Headers = %q, missing content-type", rec.Header().Get("Access-Control-Allow-Headers"))
	}

	// The header must be present on real responses too, not only preflight —
	// the browser checks it on the response that carries the data.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/status", nil))
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("Allow-Origin on GET = %q, want *", got)
	}
}

// --------------------------------------------------------------- rate limit

func TestLimiterBurstThenRefill(t *testing.T) {
	now := time.Now()
	l := newLimiter(3, 2)
	l.now = func() time.Time { return now }

	if !l.allow("a") || !l.allow("a") {
		t.Fatal("burst of 2 must be allowed")
	}
	if l.allow("a") {
		t.Fatal("third immediate request must be refused")
	}
	// A different caller has its own bucket.
	if !l.allow("b") {
		t.Fatal("second IP must not inherit the first IP's bucket")
	}
	// 3/min is one token every 20 seconds.
	now = now.Add(19 * time.Second)
	if l.allow("a") {
		t.Fatal("19s is not yet a token")
	}
	now = now.Add(2 * time.Second)
	if !l.allow("a") {
		t.Fatal("21s must have refilled one token")
	}
}

func TestRunIsRateLimited(t *testing.T) {
	s := New(Config{Logger: quiet(), TickInterval: time.Hour, RunPerMinute: 3, RunBurst: 2})
	t.Cleanup(s.Close)
	// Ready with a nil engine: the limiter runs before anything touches the
	// engine, which is exactly what this test is isolating.
	s.Ready(nil, nil, TemplateInfo{})
	h := s.Handler()

	post := func() int {
		req := httptest.NewRequest(http.MethodPost, "/api/run", strings.NewReader(`{"sql":"CREATE INDEX x ON orders (id)"}`))
		req.RemoteAddr = "10.9.9.9:1111" // private hop, as the balancer would be
		req.Header.Set("X-Forwarded-For", "203.0.113.7, 10.0.0.1")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}

	if code := post(); code == http.StatusTooManyRequests {
		t.Fatalf("first request was rate limited (%d)", code)
	}
	if code := post(); code == http.StatusTooManyRequests {
		t.Fatalf("second request was rate limited (%d)", code)
	}
	if code := post(); code != http.StatusTooManyRequests {
		t.Fatalf("third request = %d, want 429", code)
	}

	// A different forwarded client is unaffected.
	req := httptest.NewRequest(http.MethodPost, "/api/run", strings.NewReader(`{"sql":"CREATE INDEX x ON orders (id)"}`))
	req.Header.Set("X-Forwarded-For", "198.51.100.4")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code == http.StatusTooManyRequests {
		t.Fatal("a different IP must not be limited by another IP's bucket")
	}
}

// TestOversizedBodyIsTooLargeNotBadJSON pins BUG 3: a well-formed JSON body that
// merely exceeds the size cap must be reported as too large (413), not as
// invalid JSON — the latter sends the caller hunting for a syntax error that is
// not there.
func TestOversizedBodyIsTooLargeNotBadJSON(t *testing.T) {
	s := New(Config{Logger: quiet(), TickInterval: time.Hour})
	t.Cleanup(s.Close)
	// Ready with a nil engine: the size check runs in the decode step, before
	// anything touches the engine.
	s.Ready(nil, nil, TemplateInfo{})
	h := s.Handler()

	// Syntactically valid JSON, simply larger than the 64KB transport cap.
	body := `{"sql":"` + strings.Repeat("A", 128<<10) + `"}`
	req := httptest.NewRequest(http.MethodPost, "/api/run", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body = %d, want 413", rec.Code)
	}
	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	reason, _ := resp["reason"].(string)
	if !strings.Contains(strings.ToLower(reason), "too large") {
		t.Errorf("reason = %q, want it to say the body is too large", reason)
	}
	if strings.Contains(reason, "not valid JSON") {
		t.Errorf("an oversized-but-valid body must not be reported as invalid JSON: %q", reason)
	}
}

func TestClientIPPrefersFirstForwarded(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "10.1.2.3:5555"
	if got := clientIP(req); got != "10.1.2.3" {
		t.Fatalf("clientIP without XFF = %q, want 10.1.2.3", got)
	}
	// X-Forwarded-For is forgeable, so it is only honoured when the request
	// arrived from a private hop (the in-project balancer), and then only its
	// LAST entry — the one that hop appended. A client-seeded prefix must not
	// be able to mint a fresh rate-limit bucket.
	req.Header.Set("X-Forwarded-For", " 203.0.113.9 , 10.0.0.1 ")
	if got := clientIP(req); got != "10.0.0.1" {
		t.Fatalf("clientIP behind private hop = %q, want the last hop 10.0.0.1", got)
	}
	// Same header, but the request came straight off the internet: ignore it
	// entirely and key on the socket.
	pub := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	pub.RemoteAddr = "203.0.113.50:4444"
	pub.Header.Set("X-Forwarded-For", "1.2.3.4")
	if got := clientIP(pub); got != "203.0.113.50" {
		t.Fatalf("clientIP from public peer = %q, want the socket address", got)
	}
}

// --------------------------------------------------------------- hub

func TestHubBroadcastsToEverySubscriber(t *testing.T) {
	h := newHub()
	a, b := h.subscribe(), h.subscribe()
	if h.count() != 2 {
		t.Fatalf("count = %d, want 2", h.count())
	}
	h.broadcast([]byte("frame"))
	for i, ch := range []chan []byte{a, b} {
		select {
		case got := <-ch:
			if string(got) != "frame" {
				t.Fatalf("sub %d got %q", i, got)
			}
		default:
			t.Fatalf("sub %d received nothing", i)
		}
	}
	h.unsubscribe(a)
	h.unsubscribe(a) // idempotent
	if h.count() != 1 {
		t.Fatalf("count after unsubscribe = %d, want 1", h.count())
	}
}

func TestHubDropsForSlowSubscriberWithoutBlocking(t *testing.T) {
	h := newHub()
	ch := h.subscribe()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < subBuffer*10; i++ {
			h.broadcast([]byte{byte(i)})
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("broadcast blocked on a subscriber that never read")
	}

	if got := len(ch); got != subBuffer {
		t.Fatalf("buffered frames = %d, want %d", got, subBuffer)
	}
	if h.droppedFrames() == 0 {
		t.Fatal("expected dropped frames to be counted")
	}
	// The frames retained must be the NEWEST ones: a dashboard that fell
	// behind should resume at now, not replay a stale minute.
	first := <-ch
	if want := byte(subBuffer*10 - subBuffer); first[0] != want {
		t.Fatalf("oldest retained frame = %d, want %d (drop-oldest policy)", first[0], want)
	}
}

func TestSSEFrameFormat(t *testing.T) {
	buf, err := sseFrame("stats", map[string]any{"writes_per_sec": 12})
	if err != nil {
		t.Fatal(err)
	}
	got := string(buf)
	if !strings.HasPrefix(got, "event: stats\ndata: ") {
		t.Fatalf("frame = %q", got)
	}
	if !strings.HasSuffix(got, "\n\n") {
		t.Fatalf("frame must end with a blank line, got %q", got)
	}
	if strings.Count(strings.TrimSuffix(got, "\n\n"), "\n") != 1 {
		t.Fatalf("frame body must be single-line, got %q", got)
	}
}

// --------------------------------------------------------------- stats shape

func TestLiveFrameWithoutEngineIsQuietButValid(t *testing.T) {
	s := newTestServer(t)
	f := s.liveFrame()
	buf, err := json.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(buf, &decoded); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"writes_per_sec", "casualties_total", "by_label", "blocked", "run_id"} {
		if _, ok := decoded[key]; !ok {
			t.Fatalf("stats frame is missing %q: %s", key, buf)
		}
	}
	if decoded["run_id"] != nil {
		t.Fatalf("run_id = %v, want null when idle", decoded["run_id"])
	}
	if _, ok := decoded["history"]; ok {
		t.Fatal("history must appear on the first stream frame only")
	}
}

func TestRingKeepsOnlyTheConfiguredWindow(t *testing.T) {
	s := New(Config{Logger: quiet(), TickInterval: time.Hour, HistorySeconds: 5})
	t.Cleanup(s.Close)
	base := time.Now()
	for i := 0; i < 20; i++ {
		s.sample(base.Add(time.Duration(i) * time.Second))
	}
	hist := s.historyRing()
	if len(hist) != 5 {
		t.Fatalf("history length = %d, want 5", len(hist))
	}
	if hist[0][0] != base.Add(15*time.Second).UnixMilli() {
		t.Fatalf("history did not keep the newest samples: %v", hist)
	}
}
