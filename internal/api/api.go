// Package api is Brace's HTTP surface: the endpoints the dashboard is coded
// against, and nothing else.
//
// Three things shape this package.
//
// First, the dashboard is already written. Every JSON key here is a contract
// with web/app.js, not a design decision still open for discussion.
//
// Second, a run takes ten seconds and the browser must not hold a request open
// for it. POST /api/run therefore does the whole of its error reporting up
// front — reject, busy, no-warm-twin — and then answers 202 with a run id while
// the rehearsal proceeds on the stream. Everything a user can get wrong is
// still a synchronous status code; only the measurement is asynchronous.
//
// Third, the stream must never be hostage to one slow reader. The stats ticker
// broadcasts into per-subscriber buffers and drops frames rather than waiting.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/vinayaksonthalia/brace/internal/engine"
)

const (
	defaultTick        = time.Second
	defaultRunPerMin   = 3
	defaultRunBurst    = 2
	defaultCloneETA    = 45
	defaultHistorySecs = 60

	// maxBodyBytes caps a POST /api/run body. The engine's own limit on the
	// statement is 4KB; this is the transport-level ceiling around it.
	maxBodyBytes = 64 << 10

	// runIDWait bounds how long the POST handler waits for the engine to
	// publish the run id it just started. The engine assigns one within a
	// millisecond of Rehearse being called; this is purely a deadlock guard.
	runIDWait = 5 * time.Second

	// blockedFraction mirrors the engine's collapse threshold: a live write
	// rate under 15% of this run's measured baseline is a frozen application,
	// not a slow one. It is duplicated rather than imported because the engine
	// keeps it unexported; if that constant ever moves, this one moves with it,
	// or the live red band and the final verdict start disagreeing on screen.
	blockedFraction = 0.15
)

// TemplateInfo is the cached description of the seed template, queried once at
// boot because pg_database_size on every /api/status would be a pointless
// round trip.
type TemplateInfo struct {
	Rows int64  `json:"rows"`
	Size string `json:"size"`
}

// Config configures a Server.
type Config struct {
	Logger *slog.Logger
	// TickInterval is the stats cadence. Defaults to 1s; the dashboard's
	// scope assumes one frame per second.
	TickInterval time.Duration
	// RunPerMinute and RunBurst configure the per-IP guard on POST /api/run.
	// Default 3/min with a burst of 2.
	RunPerMinute float64
	RunBurst     int
	// CloneETASeconds is what a 503 tells the user to wait. Default 45.
	CloneETASeconds int
	// HistorySeconds is how much of the stats ring the first SSE frame carries
	// so the scope opens full. Default 60.
	HistorySeconds int
	// WebDir, if set, serves the static dashboard from this process. On Zerops
	// the web service serves it instead; this exists so `./app` is a complete
	// local demo.
	WebDir string
}

func (c *Config) applyDefaults() {
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.TickInterval <= 0 {
		c.TickInterval = defaultTick
	}
	if c.RunPerMinute <= 0 {
		c.RunPerMinute = defaultRunPerMin
	}
	if c.RunBurst <= 0 {
		c.RunBurst = defaultRunBurst
	}
	if c.CloneETASeconds <= 0 {
		c.CloneETASeconds = defaultCloneETA
	}
	if c.HistorySeconds <= 0 {
		c.HistorySeconds = defaultHistorySecs
	}
}

// histPoint is one entry of the stats ring: epoch milliseconds and the write
// rate observed in that second. The dashboard's Chart.seed takes exactly
// [[msEpoch, wps], …].
type histPoint struct {
	AtMS int64
	WPS  int64
}

// labelFrame is the per-label slice of the stats frame. loadgen.LabelStats also
// carries Errored; the dashboard reads ok and died, and the stats frame stays
// exactly the shape it was specified as.
type labelFrame struct {
	OK   int64 `json:"ok"`
	Died int64 `json:"died"`
}

// statsFrame is the `stats` SSE event and the body of GET /api/stats.
type statsFrame struct {
	WritesPerSec    int64                 `json:"writes_per_sec"`
	CasualtiesTotal int64                 `json:"casualties_total"`
	ByLabel         map[string]labelFrame `json:"by_label"`
	Blocked         bool                  `json:"blocked"`
	// RunID is null between runs, which is how the polling fallback detects
	// that a run ended.
	RunID *string `json:"run_id"`
	Twin  string  `json:"twin,omitempty"`
	Mode  string  `json:"mode,omitempty"`
	// History is present on the FIRST frame of a stream only.
	History [][2]int64 `json:"history,omitempty"`
}

// statsResponse is the polling body: the same frame plus the last completed
// run, from which the polling client synthesises run_finished.
type statsResponse struct {
	statsFrame
	LastRun *engine.RunResult `json:"last_run,omitempty"`
}

// statusResponse is engine.Status plus the cached template facts.
type statusResponse struct {
	engine.Status
	Template TemplateInfo `json:"template"`
}

// runStarted is the `run_started` event.
type runStarted struct {
	RunID string `json:"run_id"`
	SQL   string `json:"sql"`
	Twin  string `json:"twin,omitempty"`
	Mode  string `json:"mode"`
}

// Server is the HTTP layer. It is created before the database is ready, on
// purpose: /healthz must answer while the template is still seeding, or the
// platform kills the container before it ever finishes.
type Server struct {
	cfg Config
	log *slog.Logger
	hub *hub
	lim *limiter

	// dispatchMu serialises POST /api/run admission. Holding it across the
	// engine's status check and the run id handshake is what makes "check
	// Running, then start" atomic from this layer's point of view — without it
	// two simultaneous posts could each read Running=false and one would then
	// see the other's run id as its own.
	dispatchMu sync.Mutex

	mu      sync.RWMutex
	eng     *engine.Engine
	pool    *pgxpool.Pool
	ready   bool
	phase   string
	tmpl    TemplateInfo
	lastRun *engine.RunResult
	ring    []histPoint

	runCtx    context.Context
	runCancel context.CancelFunc
	stop      chan struct{}
	stopOnce  sync.Once
	wg        sync.WaitGroup
}

// New builds the server and starts the stats ticker. The ticker runs from this
// moment, not from the first run, so a dashboard that connects to an idle lab
// still gets a live (flat) trace and knows the rig is alive.
func New(cfg Config) *Server {
	cfg.applyDefaults()
	runCtx, runCancel := context.WithCancel(context.Background())
	s := &Server{
		cfg:       cfg,
		log:       cfg.Logger.With("component", "api"),
		hub:       newHub(),
		lim:       newLimiter(cfg.RunPerMinute, cfg.RunBurst),
		phase:     "starting",
		runCtx:    runCtx,
		runCancel: runCancel,
		stop:      make(chan struct{}),
	}
	s.wg.Add(1)
	go s.tickLoop()
	return s
}

// SetPhase records what boot is currently doing. It is what a caller hitting
// /api/* during startup is told.
func (s *Server) SetPhase(phase string) {
	s.mu.Lock()
	s.phase = phase
	s.mu.Unlock()
}

// Ready hands the server its engine and flips /api/* live. Until it is called
// every /api/* route answers 503 {"error":"starting"}.
func (s *Server) Ready(eng *engine.Engine, pool *pgxpool.Pool, tmpl TemplateInfo) {
	s.mu.Lock()
	s.eng = eng
	s.pool = pool
	s.tmpl = tmpl
	s.ready = true
	s.phase = "ready"
	s.mu.Unlock()
}

// Close stops the ticker and abandons any in-flight rehearsal's context. It
// does NOT close the engine; the caller owns that, and ordering matters
// (HTTP down, then engine).
func (s *Server) Close() {
	s.stopOnce.Do(func() {
		close(s.stop)
		s.runCancel()
	})
	s.wg.Wait()
}

func (s *Server) engine() *engine.Engine {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.eng
}

// ---------------------------------------------------------------- routing

// Handler returns the complete mux: /healthz, /api/*, and optionally the
// static dashboard.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.Handle("/api/", cors(http.HandlerFunc(s.routeAPI)))

	if s.cfg.WebDir != "" {
		if st, err := os.Stat(s.cfg.WebDir); err == nil && st.IsDir() {
			mux.Handle("/", http.FileServer(http.Dir(s.cfg.WebDir)))
		} else {
			s.log.Warn("api.web_dir_unusable", "dir", s.cfg.WebDir, "err", err)
		}
	}
	return mux
}

// cors opens the API to any origin. This is a read-mostly public demo with no
// cookies, no auth and no credentials: the only state a caller can change is
// starting a run, which the rate limiter bounds and the engine serialises.
// Allow-Origin * and Allow-Credentials are mutually exclusive by spec, and we
// deliberately take the former.
func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Access-Control-Allow-Origin", "*")
		h.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		h.Set("Access-Control-Allow-Headers", "Content-Type")
		h.Set("Access-Control-Max-Age", "600")
		h.Add("Vary", "Origin")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) routeAPI(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	ready, phase := s.ready, s.phase
	s.mu.RUnlock()
	if !ready {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error": "starting",
			"phase": phase,
		})
		return
	}

	path := r.URL.Path
	switch {
	case path == "/api/stream":
		s.requireGET(w, r, s.handleStream)
	case path == "/api/stats":
		s.requireGET(w, r, s.handleStats)
	case path == "/api/status":
		s.requireGET(w, r, s.handleStatus)
	case path == "/api/history":
		s.requireGET(w, r, s.handleHistory)
	case path == "/api/run":
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method_not_allowed"})
			return
		}
		s.handleRun(w, r)
	case strings.HasPrefix(path, "/api/report/"):
		s.requireGET(w, r, s.handleReport)
	default:
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "not_found"})
	}
}

func (s *Server) requireGET(w http.ResponseWriter, r *http.Request, h http.HandlerFunc) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method_not_allowed"})
		return
	}
	h(w, r)
}

// handleHealthz is the readiness probe, and it is deliberately dumber than
// readiness: it says yes the moment the listener is up, including while the
// template is seeding. A platform that kills the container during a ten-minute
// seed can never finish the seed.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "ok")
}

// ---------------------------------------------------------------- stats

// tickLoop is the single producer of `stats` frames.
func (s *Server) tickLoop() {
	defer s.wg.Done()
	t := time.NewTicker(s.cfg.TickInterval)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case now := <-t.C:
			frame := s.sample(now)
			buf, err := sseFrame("stats", frame)
			if err != nil {
				s.log.Error("api.stats_encode_failed", "err", err)
				continue
			}
			s.hub.broadcast(buf)
		}
	}
}

// sample takes one live reading, records it in the ring, and returns the frame.
func (s *Server) sample(now time.Time) statsFrame {
	f := s.liveFrame()
	s.mu.Lock()
	s.ring = append(s.ring, histPoint{AtMS: now.UnixMilli(), WPS: f.WritesPerSec})
	if max := s.cfg.HistorySeconds; len(s.ring) > max {
		s.ring = append(s.ring[:0], s.ring[len(s.ring)-max:]...)
	}
	s.mu.Unlock()
	return f
}

// liveFrame reads the engine without touching the ring.
func (s *Server) liveFrame() statsFrame {
	f := statsFrame{ByLabel: map[string]labelFrame{}}
	eng := s.engine()
	if eng == nil {
		return f
	}
	st := eng.Status()
	stats, live := eng.LiveStats()
	if live {
		f.WritesPerSec = stats.WritesPerSec
		f.CasualtiesTotal = stats.CasualtiesTotal
		for name, ls := range stats.ByLabel {
			f.ByLabel[name] = labelFrame{OK: ls.OK, Died: ls.Died}
		}
	}
	if st.Running && st.CurrentRun != nil {
		id := st.CurrentRun.RunID
		f.RunID = &id
		f.Twin = st.CurrentRun.Twin
		f.Mode = string(st.CurrentRun.Mode)
		// The baseline is this run's own, measured two seconds in. Before it
		// exists there is nothing to be 15% of, and calling the warm-up
		// "blocked" would paint a freeze across every run's first seconds.
		if base := st.CurrentRun.BaselineWPS; base > 0 && live {
			f.Blocked = float64(stats.WritesPerSec) < blockedFraction*float64(base)
		}
	}
	return f
}

// historyRing returns the recent trace for the first frame of a stream. It
// prefers the ticker's own ring; on a stream opened into a run that started
// before this process had 60 seconds of samples, it falls back to the engine's
// live 250ms timeline, mapped onto wall clock from the run's start.
func (s *Server) historyRing() [][2]int64 {
	s.mu.RLock()
	ring := make([]histPoint, len(s.ring))
	copy(ring, s.ring)
	s.mu.RUnlock()

	if len(ring) > 0 {
		out := make([][2]int64, 0, len(ring))
		for _, p := range ring {
			out = append(out, [2]int64{p.AtMS, p.WPS})
		}
		return out
	}

	eng := s.engine()
	if eng == nil {
		return nil
	}
	st := eng.Status()
	if st.CurrentRun == nil {
		return nil
	}
	pts := eng.LiveTimeline()
	if len(pts) == 0 {
		return nil
	}
	origin := st.CurrentRun.StartedAt
	out := make([][2]int64, 0, len(pts))
	for _, p := range pts {
		out = append(out, [2]int64{origin.Add(time.Duration(p.T * float64(time.Second))).UnixMilli(), p.WPS})
	}
	return out
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	resp := statsResponse{statsFrame: s.liveFrame()}
	s.mu.RLock()
	resp.LastRun = s.lastRun
	s.mu.RUnlock()
	writeJSON(w, http.StatusOK, resp)
}

// handleStream is the SSE endpoint.
//
// The three headers below are not decoration. text/event-stream and
// Cache-Control: no-cache are the protocol; X-Accel-Buffering: no is what stops
// the reverse proxy in front of this service from buffering the stream into
// silence, which is the difference between a live dashboard and one that
// updates every 30 seconds in a single burst.
func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "streaming_unsupported"})
		return
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	// Subscribe before writing the seed frame: a run_started published in
	// between must land in the buffer, not in the gap.
	ch := s.hub.subscribe()
	if ch == nil {
		// At subscriber capacity: say so in-band. The headers are already
		// written, so this is an SSE-shaped refusal rather than a 503.
		fmt.Fprint(w, "event: error\ndata: {\"error\":\"stream_capacity\"}\n\n")
		flusher.Flush()
		return
	}
	defer s.hub.unsubscribe(ch)

	first := s.liveFrame()
	first.History = s.historyRing()
	if buf, err := sseFrame("stats", first); err == nil {
		if _, err := w.Write(append([]byte("retry: 2000\n\n"), buf...)); err != nil {
			return
		}
		flusher.Flush()
	}

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stop:
			return
		case buf := <-ch:
			if _, err := w.Write(buf); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// ---------------------------------------------------------------- status, history, report

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	eng := s.engine()
	if eng == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "starting", "phase": "engine"})
		return
	}
	s.mu.RLock()
	tmpl := s.tmpl
	s.mu.RUnlock()
	writeJSON(w, http.StatusOK, statusResponse{Status: eng.Status(), Template: tmpl})
}

func (s *Server) handleHistory(w http.ResponseWriter, r *http.Request) {
	limit := 20
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	rows, err := s.engine().History(ctx, limit)
	if err != nil {
		s.log.Error("api.history_failed", "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "internal", "reason": "history unavailable"})
		return
	}
	if rows == nil {
		rows = []engine.RunRecord{}
	}
	writeJSON(w, http.StatusOK, rows)
}

// reportColumns is the ledger projection, kept identical to engine.Ledger.List
// so /api/report/{id} and /api/history return the same shape.
const reportColumns = `id, created_at, "sql", mode, classification, verdict,
	blocked_seconds, duration_seconds, casualties_total,
	casualties_by_label, baseline_wps, lock_modes, twin, coalesce(error,''),
	casualty_deadline_ms`

func (s *Server) handleReport(w http.ResponseWriter, r *http.Request) {
	idStr := strings.TrimPrefix(r.URL.Path, "/api/report/")
	idStr = strings.Trim(idStr, "/")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil || id <= 0 {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "not_found", "reason": "no such run"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	rec, err := s.reportByID(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "not_found", "reason": "no such run"})
		return
	}
	if err != nil {
		s.log.Error("api.report_failed", "id", id, "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "internal", "reason": "report unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

func (s *Server) reportByID(ctx context.Context, id int64) (*engine.RunRecord, error) {
	s.mu.RLock()
	pool := s.pool
	s.mu.RUnlock()

	if pool == nil {
		// Without an admin pool the ledger is still reachable through the
		// engine, just not by primary key. Recent history is the useful part of
		// a shareable report anyway.
		rows, err := s.engine().History(ctx, 200)
		if err != nil {
			return nil, err
		}
		for i := range rows {
			if rows[i].ID == id {
				return &rows[i], nil
			}
		}
		return nil, pgx.ErrNoRows
	}

	var (
		rec     engine.RunRecord
		mode    string
		class   string
		byLabel []byte
	)
	err := pool.QueryRow(ctx, `SELECT `+reportColumns+` FROM brace_runs WHERE id = $1`, id).
		Scan(&rec.ID, &rec.CreatedAt, &rec.SQL, &mode, &class, &rec.Verdict,
			&rec.BlockedSeconds, &rec.DurationSeconds, &rec.CasualtiesTotal,
			&byLabel, &rec.BaselineWPS, &rec.LockModes, &rec.Twin, &rec.Error,
			&rec.CasualtyDeadlineMS)
	if err != nil {
		return nil, err
	}
	rec.Mode = engine.Mode(mode)
	rec.Classification = engine.Classification(class)
	if len(byLabel) > 0 {
		_ = json.Unmarshal(byLabel, &rec.CasualtiesByLabel)
	}
	return &rec, nil
}

// ---------------------------------------------------------------- run

type runRequest struct {
	SQL  string `json:"sql"`
	Mode string `json:"mode"`
	// TargetRows lets a caller ask "what about MY table?": the run is measured
	// on the twin as always, and the result additionally carries a projection
	// onto this many rows. Absent, zero, negative or absurd (> 1e12) values are
	// ignored silently and the deployment's BRACE_TARGET_ROWS stands — a
	// mistyped row count must never cost somebody a rehearsal.
	TargetRows int64 `json:"target_rows,omitempty"`
	// Hostile asks for a contended rehearsal: while the migration runs, the lab
	// holds a transaction open on the load target so a fast DDL has to QUEUE for
	// its lock instead of taking it instantly. A pointer so that absent means
	// "whatever the deployment defaults to" (off, unless BRACE_HOSTILE=on) while
	// an explicit false can turn it off again.
	Hostile *bool `json:"hostile,omitempty"`
	// HostileHoldMS is how long each contention cycle holds its lock. Ignored
	// unless hostile mode is on. 0 = the deployment's default (2000ms).
	HostileHoldMS int64 `json:"hostile_hold_ms,omitempty"`
	// LockTimeoutMS is the simulated request deadline on the write traffic — the
	// wait after which a blocked write counts as a casualty. It IS the definition
	// of the casualty number, so it is per-run configurable and reported back as
	// casualties.deadline_ms. 0 = the deployment's default (1500ms).
	LockTimeoutMS int64 `json:"lock_timeout_ms,omitempty"`
}

// runOptions turns the request's per-run knobs into engine options. Every one of
// them is forgiving: an out-of-range value falls back to the deployment default
// rather than costing the caller a rehearsal.
func (r runRequest) runOptions() []engine.RunOption {
	opts := []engine.RunOption{engine.WithTargetRows(r.TargetRows)}
	if r.Hostile != nil {
		opts = append(opts, engine.WithHostile(*r.Hostile))
	}
	if r.HostileHoldMS > 0 {
		opts = append(opts, engine.WithHostileHold(time.Duration(r.HostileHoldMS)*time.Millisecond))
	}
	if r.LockTimeoutMS > 0 {
		opts = append(opts, engine.WithLockTimeout(time.Duration(r.LockTimeoutMS)*time.Millisecond))
	}
	return opts
}

type outcome struct {
	res *engine.RunResult
	err error
}

func (s *Server) handleRun(w http.ResponseWriter, r *http.Request) {
	if !s.lim.allow(clientIP(r)) {
		w.Header().Set("Retry-After", "20")
		writeJSON(w, http.StatusTooManyRequests, map[string]any{"error": "rate_limited"})
		return
	}

	var req runRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err := dec.Decode(&req); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			// The body was valid JSON syntax as far as we could tell — it was
			// simply larger than the cap. Saying "not valid JSON" here sends the
			// caller hunting for a syntax error that is not there; name the real
			// problem, with the 413 that fits it.
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]any{
				"error": "rejected",
				"kind":  string(engine.RejectRejected),
				"reason": fmt.Sprintf("The request body is too large (limit %d KB). Brace rehearses one migration "+
					"statement at a time — send just the statement.", maxBodyBytes>>10),
			})
			return
		}
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error":  "rejected",
			"kind":   string(engine.RejectRejected),
			"reason": "The request body was not valid JSON. Expected {\"sql\": \"…\"}.",
		})
		return
	}

	mode := engine.Mode(strings.ToLower(strings.TrimSpace(req.Mode)))
	switch mode {
	case "", engine.ModeCustom:
		mode = engine.ModeCustom
	case engine.ModeNaive, engine.ModeSafe:
	default:
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error":  "rejected",
			"kind":   string(engine.RejectRejected),
			"reason": fmt.Sprintf("Unknown run mode %q. Expected one of: naive, safe, custom.", req.Mode),
		})
		return
	}

	eng := s.engine()
	if eng == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "starting", "phase": "engine"})
		return
	}

	// The pre-check. AssessRun is pure and cheap, and running it here rather than
	// only inside the goroutine is what lets a bad statement come back as a 400
	// on the very request that submitted it. It must be AssessRun, not Assess, so
	// this synchronous check honours the run's options — above all hostile mode,
	// which is allowed past the already_safe fast-path for an ACCESS EXCLUSIVE
	// shape precisely so it can measure the acquisition freeze.
	if _, err := eng.AssessRun(req.SQL, req.runOptions()...); err != nil {
		s.writeRunError(w, err)
		return
	}

	s.dispatchMu.Lock()
	defer s.dispatchMu.Unlock()

	st := eng.Status()
	if st.Running {
		writeJSON(w, http.StatusConflict, map[string]any{"error": "run_in_progress"})
		return
	}
	if st.Pool.Warm == 0 {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error":       "no_warm_twin",
			"eta_seconds": s.cfg.CloneETASeconds,
		})
		return
	}
	prevRunID := ""
	if st.CurrentRun != nil {
		prevRunID = st.CurrentRun.RunID
	}

	// The run's context is the server's, not the request's: the browser is
	// about to be answered and its context cancelled, and a rehearsal that dies
	// with the request would leave a twin under load.
	resCh := make(chan outcome, 1)
	sql := req.SQL
	opts := req.runOptions()
	go func() {
		res, err := eng.Rehearse(s.runCtx, sql, mode, opts...)
		resCh <- outcome{res: res, err: err}
	}()

	runID, done := s.awaitRunID(eng, prevRunID, resCh)
	switch {
	case done != nil && done.err != nil:
		// A typed failure raced past the pre-check — the pool emptied, the
		// engine closed. Report it on this request; nothing was published.
		s.writeRunError(w, done.err)
		return
	case done != nil && done.res != nil:
		runID = done.res.RunID
		s.publishStarted(runID, sql, done.res.Twin, mode)
		s.finishRun(runID, sql, mode, *done)
	case runID != "":
		s.publishStarted(runID, sql, s.twinFor(eng, runID), mode)
		go func() {
			s.finishRun(runID, sql, mode, <-resCh)
		}()
	default:
		s.log.Error("api.run_id_timeout")
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"error":  "internal",
			"reason": "the engine did not acknowledge the run",
		})
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]any{"run_id": runID})
}

// awaitRunID waits for the engine to publish the id of the run we just started,
// or for that run to fail before it ever got one. Returns ("", nil) on timeout.
func (s *Server) awaitRunID(eng *engine.Engine, prevRunID string, resCh <-chan outcome) (string, *outcome) {
	deadline := time.Now().Add(runIDWait)
	t := time.NewTicker(time.Millisecond)
	defer t.Stop()
	for {
		select {
		case out := <-resCh:
			return "", &out
		default:
		}
		if st := eng.Status(); st.CurrentRun != nil && st.CurrentRun.RunID != prevRunID {
			return st.CurrentRun.RunID, nil
		}
		if time.Now().After(deadline) {
			return "", nil
		}
		select {
		case out := <-resCh:
			return "", &out
		case <-t.C:
		case <-s.stop:
			return "", nil
		}
	}
}

func (s *Server) twinFor(eng *engine.Engine, runID string) string {
	if st := eng.Status(); st.CurrentRun != nil && st.CurrentRun.RunID == runID {
		return st.CurrentRun.Twin
	}
	return ""
}

func (s *Server) publishStarted(runID, sql, twin string, mode engine.Mode) {
	buf, err := sseFrame("run_started", runStarted{
		RunID: runID, SQL: sql, Twin: twin, Mode: string(mode),
	})
	if err != nil {
		s.log.Error("api.run_started_encode_failed", "err", err)
		return
	}
	s.hub.broadcast(buf)
}

// finishRun publishes run_finished and records the result for the polling
// fallback. A run that failed with a typed error after admission still gets a
// run_finished — a dashboard left waiting forever is the one outcome worse than
// a bad verdict.
func (s *Server) finishRun(runID, sql string, mode engine.Mode, out outcome) {
	res := out.res
	if res == nil {
		res = &engine.RunResult{
			RunID:     runID,
			StartedAt: time.Now(),
			Mode:      mode,
			SQL:       sql,
			// A rehearsal that never completed measured NOTHING, so it must never
			// read as SAFE — a CI gate keying on this would wave an unmeasured
			// migration into production. ERRORED is the honest terminal state and
			// the gate treats it as non-safe (exit 2).
			Verdict:          engine.VerdictErrored,
			EffectiveVerdict: engine.VerdictErrored,
			Casualties:       engine.Casualties{ByLabel: map[string]int64{}},
			LockModes:        []string{},
			Note:             "The rehearsal could not be completed, so nothing was measured. Treat as not-yet-verified, never as safe.",
		}
		// Never surface the raw engine error (a wrapped pgx error can carry the
		// admin host/user/db) to the public stream; log it, show a static line.
		if out.err != nil {
			res.Error = "the rehearsal failed before it could be measured"
		}
		s.log.Error("api.run_failed", "run", runID, "err", out.err)
	} else {
		s.mu.Lock()
		s.lastRun = res
		s.mu.Unlock()
	}
	buf, err := sseFrame("run_finished", res)
	if err != nil {
		s.log.Error("api.run_finished_encode_failed", "run", runID, "err", err)
		return
	}
	s.hub.broadcast(buf)
}

// writeRunError is the single place the engine's error contract becomes HTTP.
func (s *Server) writeRunError(w http.ResponseWriter, err error) {
	var rej *engine.RejectedError
	if errors.As(err, &rej) {
		body := map[string]any{
			"error":  "rejected",
			"kind":   string(rej.Kind),
			"reason": rej.Reason,
		}
		if rej.Suggestion != "" {
			body["suggestion"] = rej.Suggestion
		}
		if rej.Classification != "" {
			body["classification"] = string(rej.Classification)
		}
		// An already_safe shape that still takes ACCESS EXCLUSIVE carries these:
		// the acquisition wait it did NOT simulate. Without them a caller reads the
		// already_safe as an unconditional SAFE and ships a change that freezes
		// writes the moment it queues behind a long transaction.
		if rej.Qualifier != "" {
			body["qualifier"] = rej.Qualifier
		}
		if rej.Note != "" {
			body["note"] = rej.Note
		}
		writeJSON(w, http.StatusBadRequest, body)
		return
	}
	if errors.Is(err, engine.ErrRunInProgress) {
		writeJSON(w, http.StatusConflict, map[string]any{"error": "run_in_progress"})
		return
	}
	var warm *engine.NoWarmTwinError
	if errors.As(err, &warm) {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error":       "no_warm_twin",
			"eta_seconds": warm.ETASeconds,
		})
		return
	}
	s.log.Error("api.run_error", "err", err)
	writeJSON(w, http.StatusInternalServerError, map[string]any{
		"error":  "internal",
		"reason": "the lab could not start this run",
	})
}

// ---------------------------------------------------------------- plumbing

func writeJSON(w http.ResponseWriter, status int, body any) {
	buf, err := json.Marshal(body)
	if err != nil {
		http.Error(w, `{"error":"internal"}`, http.StatusInternalServerError)
		return
	}
	h := w.Header()
	h.Set("Content-Type", "application/json; charset=utf-8")
	h.Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(buf)
}
