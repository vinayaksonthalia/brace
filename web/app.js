/* ═══════════════════════════════════════════════════════════════
   BRACE — dashboard
   No framework, no build, no network beyond /api/*.

   Layers, top to bottom:
     1. BraceApi      — the one transport abstraction. SSE first;
                        after two stream errors it silently swaps to
                        1 Hz polling of /api/stats. Same callbacks
                        either way, so nothing above knows or cares.
     2. Chart         — 60s canvas scope, rAF, DPR-aware.
     3. UI            — a small state machine: idle | running | crash.
   ═══════════════════════════════════════════════════════════════ */
(function () {
  'use strict';
const API = window.BRACE_API_BASE || (/^web-[^.]+\./.test(location.host)
  ? location.protocol + '//' + location.host.replace(/^web-([^.]+)\./, 'api-$1-8080.')
  : '');

  var $ = function (s, r) { return (r || document).querySelector(s); };
  var qs = new URLSearchParams(location.search);

  var fmt = new Intl.NumberFormat('en-US');
  var num = function (n) { return fmt.format(Math.round(n || 0)); };
  var secs = function (s) { return (Math.round((s || 0) * 10) / 10).toFixed(1) + 's'; };

  /* ═══ 1. TRANSPORT ══════════════════════════════════════════
     The contract every consumer codes against:
       openStream({onStats,onRunStarted,onRunFinished,onTransport})
       status()  run(sql)
     BraceApi (below) and BraceMock both implement it, so the whole
     UI is swappable between the real rig and the simulator.
  ─────────────────────────────────────────────────────────────── */
  var BraceApi = {
    isMock: false,

    status: function () {
      return fetch(API + '/api/status', { headers: { accept: 'application/json' } })
        .then(function (r) {
          if (!r.ok) throw new Error('status ' + r.status);
          return r.json();
        });
    },

    // The last completed run, so a page reload rehydrates the crash report
    // instead of discarding it. GET /api/stats carries last_run.
    hydrate: function () {
      return fetch(API + '/api/stats', { headers: { accept: 'application/json' } })
        .then(function (r) { return r.json(); })
        .then(function (s) { return (s && s.last_run) || null; })
        .catch(function () { return null; });
    },

    run: function (sql, opts) {
      opts = opts || {};
      var body = { sql: sql };
      if (opts.hostile) body.hostile = true;
      if (opts.target_rows != null) body.target_rows = opts.target_rows;
      return fetch(API + '/api/run', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body)
      }).then(function (r) {
        return r.json().catch(function () { return {}; }).then(function (body) {
          if (r.status === 202) return body;
          throw { status: r.status, body: body };
        });
      }, function (e) {
        throw { status: 0, body: { error: 'unreachable', reason: String(e && e.message || e) } };
      });
    },

    openStream: function (h) {
      var closed = false;
      var es = null;
      var poller = null;
      var errors = 0;
      var mode = null;

      function setMode(kind, label) {
        if (mode === kind) return;
        mode = kind;
        h.onTransport({ kind: kind, label: label });
      }

      function dispatch(name, data) {
        var payload;
        try { payload = JSON.parse(data); } catch (e) { return; }
        if (name === 'stats') h.onStats(payload);
        else if (name === 'run_started') h.onRunStarted(payload);
        else if (name === 'run_finished') h.onRunFinished(payload);
      }

      function startSse() {
        if (closed) return;
        es = new EventSource(API + '/api/stream');
        es.addEventListener('open', function () { errors = 0; setMode('sse', 'live · sse'); });
        ['stats', 'run_started', 'run_finished'].forEach(function (n) {
          es.addEventListener(n, function (ev) { dispatch(n, ev.data); });
        });
        es.addEventListener('error', function () {
          // EventSource retries on its own; we only count hard drops.
          if (closed) return;
          if (es.readyState === EventSource.CLOSED) {
            errors += 1;
            setMode('down', 'reconnecting');
            if (errors >= 2) { fallbackToPolling(); return; }
            setTimeout(startSse, 1200);
          } else {
            errors += 1;
            setMode('down', 'reconnecting');
            if (errors >= 2) { es.close(); fallbackToPolling(); }
          }
        });
      }

      // Polling can't carry run_started / run_finished events, so we
      // synthesise both from the stats stream's run_id + blocked flags.
      var seenRun = null;
      var lastBlocked = false;
      var freezeStart = 0;

      function fallbackToPolling() {
        if (closed || poller) return;
        if (es) { try { es.close(); } catch (e) {} es = null; }
        setMode('poll', 'live · polling');
        var pull = function () {
          if (closed) return;
          fetch(API + '/api/stats', { headers: { accept: 'application/json' } })
            .then(function (r) { return r.json(); })
            .then(function (s) {
              if (closed) return;
              setMode('poll', 'live · polling');
              if (s.run_id && s.run_id !== seenRun) {
                seenRun = s.run_id;
                freezeStart = 0;
                h.onRunStarted({ run_id: s.run_id, twin: s.twin || null, mode: s.mode || 'custom', sql: null });
              }
              if (s.blocked && !lastBlocked) freezeStart = Date.now();
              if (!s.run_id && seenRun) {
                var blockedSeconds = freezeStart ? (Date.now() - freezeStart) / 1000 : 0;
                h.onRunFinished({
                  run_id: seenRun,
                  verdict: blockedSeconds > 0.4 ? 'UNSAFE' : 'SAFE',
                  blocked_seconds: Math.round(blockedSeconds * 10) / 10,
                  casualties: { total: s.casualties_total || 0, by_label: null },
                  duration_seconds: null,
                  error: null,
                  synthesised: true
                });
                seenRun = null;
              }
              lastBlocked = !!s.blocked;
              h.onStats(s);
            })
            .catch(function () { setMode('down', 'lab offline'); });
        };
        pull();
        poller = setInterval(pull, 1000);
      }

      startSse();

      return {
        close: function () {
          closed = true;
          if (es) { try { es.close(); } catch (e) {} }
          if (poller) clearInterval(poller);
        }
      };
    }
  };

  /* ═══ 2. THE SCOPE ══════════════════════════════════════════ */
  function Chart(canvas) {
    var ctx = canvas.getContext('2d');
    var WINDOW_MS = 60000;
    var pts = [];               // {t, v, blocked}
    var bands = [];             // {from, to|0}  blocked regions
    var marks = [];             // {t}  impact markers
    var baseline = 0;
    var w = 0, h = 0, dpr = 1;

    // Canvas has no cascade, so the palette is read out of the CSS
    // custom properties and re-read whenever the theme flips.
    var T = {};
    function readTheme() {
      var cs = getComputedStyle(document.documentElement);
      var v = function (n, d) { return (cs.getPropertyValue(n) || d).trim(); };
      var rgb = function (n, d) { return v(n, d).split(',').map(Number); };
      T = {
        green: rgb('--chart-green-rgb', '47,224,138'),
        red: rgb('--chart-red-rgb', '255,59,48'),
        idle: rgb('--chart-idle-rgb', '132,146,168'),
        areaAlpha: parseFloat(v('--chart-area-alpha', '.20')) || 0.2,
        grid: v('--chart-grid', 'rgba(120,160,200,.07)'),
        axis: v('--chart-axis', 'rgba(142,160,181,.32)'),
        axisText: v('--chart-axis-text', 'rgba(142,160,181,.6)'),
        mark: v('--chart-mark', 'rgba(255,176,32,.55)'),
        markText: v('--chart-mark-text', 'rgba(255,176,32,.85)'),
        bandTop: v('--chart-band-top', 'rgba(255,59,48,.24)'),
        bandBot: v('--chart-band-bot', 'rgba(255,59,48,.04)')
      };
    }
    readTheme();
    window.addEventListener('brace:theme', readTheme);
    var reduce = matchMedia('(prefers-reduced-motion: reduce)');

    function resize() {
      dpr = Math.min(global_dpr(), 2);
      var r = canvas.getBoundingClientRect();
      w = Math.max(1, r.width); h = Math.max(1, r.height);
      canvas.width = Math.round(w * dpr);
      canvas.height = Math.round(h * dpr);
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    }
    function global_dpr() { return window.devicePixelRatio || 1; }

    new ResizeObserver(resize).observe(canvas);
    resize();

    function push(v, blocked) {
      var t = Date.now();
      pts.push({ t: t, v: Math.max(0, v || 0), blocked: !!blocked });
      var cut = t - WINDOW_MS - 4000;
      while (pts.length && pts[0].t < cut) pts.shift();

      var open = bands.length && !bands[bands.length - 1].to ? bands[bands.length - 1] : null;
      if (blocked && !open) bands.push({ from: t, to: 0 });
      if (!blocked && open) open.to = t;
      bands = bands.filter(function (b) { return (b.to || t) > cut; });
      marks = marks.filter(function (m) { return m.t > cut; });
    }

    function closeBand() {
      var open = bands.length && !bands[bands.length - 1].to ? bands[bands.length - 1] : null;
      if (open) open.to = Date.now();
    }

    function mark() { marks.push({ t: Date.now() }); }

    function reset() { pts = []; bands = []; marks = []; }

    // Seed prior history so the scope opens full instead of drawing
    // itself in from the right edge. Accepts [[msEpoch, wps], …].
    function seed(rows) {
      if (!rows || !rows.length) return;
      var cut = Date.now() - WINDOW_MS - 2000;
      rows.forEach(function (r) {
        var t = r[0], v = r[1];
        if (t >= cut) pts.push({ t: t, v: Math.max(0, v || 0), blocked: false });
      });
      pts.sort(function (a, b) { return a.t - b.t; });
    }

    // A soft-limit ceiling. An axis that re-fits itself to the window's
    // peak every frame makes a steady trace look like it is breathing —
    // the reading changes when the data has not. So the ceiling is pinned
    // to the baseline, and only lifts if traffic genuinely overshoots it.
    var ceiling = 0;
    function scaleY() {
      var peak = 0;
      for (var i = 0; i < pts.length; i++) peak = Math.max(peak, pts[i].v);
      var want = Math.max((baseline || peak || 1) * 1.35, peak * 1.08);
      // ratchet up immediately, ease down slowly, never chatter
      ceiling = ceiling ? (want > ceiling ? want : ceiling + (want - ceiling) * 0.02) : want;
      return ceiling;
    }

    function draw() {
      var now = Date.now();
      var t0 = now - WINDOW_MS;
      var max = scaleY();
      var padT = 12, padB = 16;
      var plotH = h - padT - padB;
      var X = function (t) { return (t - t0) / WINDOW_MS * w; };
      var Y = function (v) { return padT + plotH - (v / max) * plotH; };

      ctx.clearRect(0, 0, w, h);

      // horizontal reference grid
      ctx.strokeStyle = T.grid;
      ctx.lineWidth = 1;
      for (var g = 1; g <= 4; g++) {
        var y = padT + (plotH / 4) * g;
        ctx.beginPath(); ctx.moveTo(0, y + .5); ctx.lineTo(w, y + .5); ctx.stroke();
      }

      // blocked regions — the crater floods red
      bands.forEach(function (b) {
        var x1 = Math.max(0, X(b.from));
        var x2 = Math.min(w, X(b.to || now));
        if (x2 <= x1) return;
        var g2 = ctx.createLinearGradient(0, padT, 0, padT + plotH);
        g2.addColorStop(0, T.bandTop);
        g2.addColorStop(1, T.bandBot);
        ctx.fillStyle = g2;
        ctx.fillRect(x1, padT, x2 - x1, plotH);
      });

      // baseline
      if (baseline > 0) {
        var by = Y(baseline);
        ctx.save();
        ctx.setLineDash([3, 5]);
        ctx.strokeStyle = T.axis;
        ctx.beginPath(); ctx.moveTo(0, by + .5); ctx.lineTo(w, by + .5); ctx.stroke();
        ctx.restore();
        ctx.fillStyle = T.axisText;
        ctx.font = '9px ui-monospace, SFMono-Regular, Menlo, monospace';
        ctx.fillText('BASELINE', 4, by - 5);
      }

      // impact markers
      marks.forEach(function (m) {
        var x = X(m.t);
        if (x < 0 || x > w) return;
        ctx.strokeStyle = T.mark;
        ctx.setLineDash([2, 3]);
        ctx.beginPath(); ctx.moveTo(x + .5, padT); ctx.lineTo(x + .5, padT + plotH); ctx.stroke();
        ctx.setLineDash([]);
        ctx.fillStyle = T.markText;
        ctx.font = '9px ui-monospace, SFMono-Regular, Menlo, monospace';
        ctx.fillText('MIGRATION', Math.min(x + 5, w - 62), padT + 10);
      });

      if (pts.length < 2) return;

      // ── SIGNATURE: the trace does not switch colour, it ramps.
      // Health = writes/sec against baseline. Full green above 90%,
      // ramping through to full red at a flatline, and back on recovery.
      var series = pts.slice();
      var last = series[series.length - 1];
      series.push({ t: now, v: last.v, blocked: last.blocked });

      var GREEN = T.green, RED = T.red, IDLE = T.idle;

      // Red is reserved for a live migration hurting writes. At idle no
      // migration is running, so a low or zero trace is RESTING, not a
      // crash — it must never render in crash-red. A genuinely quiet
      // engine (near-zero traffic) reads neutral grey and says so.
      var stateNow = document.documentElement.dataset.state || 'idle';
      var armed = stateNow === 'running' || stateNow === 'crash';
      var restingQuiet = !armed && baseline > 0 && last.v < baseline * 0.12;

      function hurtOf(p) {
        if (p.blocked) return 1;
        if (!armed) return 0;            // idle never alarms
        if (!baseline) return 0;
        var ratio = p.v / baseline;
        return Math.min(1, Math.max(0, (0.9 - ratio) / 0.9));
      }
      function mix(t) {
        if (restingQuiet) return IDLE;   // resting engine: neutral, not a crash
        var e = Math.pow(t, 0.72);   // reddens early, so the dip reads
        return [
          Math.round(GREEN[0] + (RED[0] - GREEN[0]) * e),
          Math.round(GREEN[1] + (RED[1] - GREEN[1]) * e),
          Math.round(GREEN[2] + (RED[2] - GREEN[2]) * e)
        ];
      }
      function rgba(c, a) { return 'rgba(' + c[0] + ',' + c[1] + ',' + c[2] + ',' + a + ')'; }

      var headHurt = hurtOf(last);
      var headCol = mix(headHurt);

      // area — one path, tinted along x by the same ramp
      var xa = X(series[0].t), xb = X(series[series.length - 1].t);
      if (xb > xa) {
        var area = ctx.createLinearGradient(xa, 0, xb, 0);
        series.forEach(function (p, i) {
          var stop = Math.min(1, Math.max(0, (X(p.t) - xa) / (xb - xa)));
          area.addColorStop(stop, rgba(mix(hurtOf(p)), T.areaAlpha));
        });
        ctx.beginPath();
        ctx.moveTo(xa, padT + plotH);
        series.forEach(function (p) { ctx.lineTo(X(p.t), Y(p.v)); });
        ctx.lineTo(xb, padT + plotH);
        ctx.closePath();
        ctx.save();
        ctx.clip();
        ctx.fillStyle = area;
        ctx.fillRect(xa, padT, xb - xa, plotH);
        // vertical fade, applied inside the clipped area only
        var fade = ctx.createLinearGradient(0, padT, 0, padT + plotH);
        fade.addColorStop(0, 'rgba(0,0,0,0)');
        fade.addColorStop(1, 'rgba(0,0,0,.92)');
        ctx.globalCompositeOperation = 'destination-out';
        ctx.fillStyle = fade;
        ctx.fillRect(xa, padT, xb - xa, plotH);
        ctx.restore();

        // the trace itself
        var line = ctx.createLinearGradient(xa, 0, xb, 0);
        series.forEach(function (p) {
          var stop = Math.min(1, Math.max(0, (X(p.t) - xa) / (xb - xa)));
          line.addColorStop(stop, rgba(mix(hurtOf(p)), 1));
        });
        ctx.beginPath();
        series.forEach(function (p, i) {
          i ? ctx.lineTo(X(p.t), Y(p.v)) : ctx.moveTo(X(p.t), Y(p.v));
        });
        ctx.lineJoin = 'round'; ctx.lineCap = 'round';
        ctx.lineWidth = 2;
        ctx.strokeStyle = line;
        ctx.shadowColor = rgba(headCol, .55);
        ctx.shadowBlur = 14;
        ctx.stroke();
        ctx.shadowBlur = 0;
      }

      var stroke = rgba(headCol, 1);
      var crashing = headHurt > 0.5;

      // live head
      var hx = X(now), hy = Y(last.v);
      ctx.fillStyle = stroke;
      ctx.beginPath(); ctx.arc(hx, hy, 3, 0, Math.PI * 2); ctx.fill();
      if (!reduce.matches) {
        var pulse = 3 + 4 * (0.5 + 0.5 * Math.sin(now / 260));
        ctx.strokeStyle = rgba(headCol, .5);
        ctx.lineWidth = 1;
        ctx.beginPath(); ctx.arc(hx, hy, pulse, 0, Math.PI * 2); ctx.stroke();
      }

      // a quiet engine says so plainly, so a flat resting line never
      // gets mistaken for a flatlined one
      if (restingQuiet) {
        ctx.save();
        ctx.fillStyle = T.axisText;
        ctx.font = '10px ui-monospace, SFMono-Regular, Menlo, monospace';
        ctx.textAlign = 'center';
        ctx.fillText('ENGINE IDLE — RUN A MIGRATION TO START TRAFFIC', w / 2, padT + plotH * 0.42);
        ctx.restore();
      }
    }

    (function loop() { draw(); requestAnimationFrame(loop); })();

    return {
      push: push, mark: mark, reset: reset, closeBand: closeBand, seed: seed,
      setBaseline: function (b) { baseline = b || 0; }
    };
  }

  /* ═══ 3. UI ═════════════════════════════════════════════════ */
  var el = {
    root: document.documentElement,
    wps: $('#wps'),
    baseline: $('#baseline'),
    casualties: $('#casualties'),
    impactTimer: $('#impactTimer'),
    chamberState: $('#chamberState'),
    labels: $('#labels'),
    twin: $('#twinName'),
    pool: $('#poolMeta'),
    template: $('#templateMeta'),
    transportChip: $('#transportChip'),
    transportLabel: $('#transportLabel'),
    mockChip: $('#mockChip'),
    sql: $('#sql'),
    runBtn: $('#runBtn'),
    hostile: $('#hostileToggle'),
    cmdChip: $('#cmdChip'),
    runText: $('.run__text'),
    runHint: $('#runHint'),
    report: $('#reportBody'),
    reportNotice: $('#reportNotice'),
    reportNote: $('#reportNote'),
    presets: Array.prototype.slice.call(document.querySelectorAll('.preset'))
  };

  var PRESETS = {
    naive: 'CREATE INDEX idx_orders_customer_id\n  ON orders (customer_id);',
    safe: 'CREATE INDEX CONCURRENTLY idx_orders_customer_id\n  ON orders (customer_id);'
  };

  /* ── motion helpers ─────────────────────────────────────
     Every animated affordance goes through here so reduced-motion
     is one check, not fifteen. */
  var reduceMotion = matchMedia('(prefers-reduced-motion: reduce)');

  // Crossfade the report panel instead of swapping content under the eye.
  function swapReport(build) {
    var render = function () {
      el.report.innerHTML = '';
      var node = build();
      if (node) el.report.appendChild(node);
      el.report.classList.remove('is-swapping');
    };
    if (reduceMotion.matches || !el.report.firstElementChild) { render(); return; }
    el.report.classList.add('is-swapping');
    setTimeout(render, 150);
  }

  // Transient states (rejected SQL, rig busy, rate-limited, warming a twin)
  // mount HERE rather than in the report body, so a fumbled run never evicts
  // the last real crash report. The notice is dismissible.
  function clearNotice() {
    if (!el.reportNotice) return;
    el.reportNotice.innerHTML = '';
    el.reportNotice.hidden = true;
  }
  function mountNotice(node) {
    if (!el.reportNotice) { swapReport(function () { return node; }); return; }
    clearNotice();
    var close = document.createElement('button');
    close.type = 'button';
    close.className = 'notice__close';
    close.setAttribute('aria-label', 'Dismiss this notice');
    close.innerHTML = '&times;';
    close.addEventListener('click', clearNotice);
    el.reportNotice.appendChild(close);
    el.reportNotice.appendChild(node);
    el.reportNotice.hidden = false;
  }

  // Count a number up on first paint. Measured figures deserve the beat.
  function tickTo(node, value, format, ms) {
    if (!node) return;
    if (reduceMotion.matches) { node.textContent = format(value); return; }
    var dur = ms || 700, t0 = 0;
    function step(t) {
      if (!t0) t0 = t;
      var p = Math.min(1, (t - t0) / dur);
      var e = 1 - Math.pow(1 - p, 4);            // easeOutQuart, matches --ease
      node.textContent = format(value * e);
      if (p < 1) requestAnimationFrame(step);
    }
    requestAnimationFrame(step);
  }

  var chart = Chart($('#chart'));
  var state = 'idle';
  var api = null;
  var baselineWps = 0;
  var freezeStartedAt = 0;
  var freezeTimer = null;
  var displayedWps = 0;
  var targetWps = 0;
  var currentRun = null;
  var seeded = false;

  /* One place decides how every control and label reads in each state,
     so the arc can never half-apply itself. */
  var STATES = {
    idle:    { run: 'Run the crash test',   hint: 'Ctrl↵ to run',            chamber: 'chamber nominal',
               desc: 'Nominal: no migration is running — the twin\'s write traffic is steady.' },
    running: { run: 'Crash in progress',    hint: 'the twin is under load',  chamber: 'rig armed',
               desc: 'Armed: the migration is running against the twin — the crash-test dummy database, not production.' },
    crash:   { run: 'Crash in progress',    hint: 'writes are frozen',       chamber: 'writes frozen',
               desc: 'The migration has taken a blocking lock: writes to the twin are frozen right now.' },
    // No warm twin yet — say that, not "run", so the button never
    // contradicts the "preparing the next dummy" card underneath it.
    prep:    { run: 'Waiting for a dummy…', hint: 'a twin is warming',       chamber: 'chamber nominal',
               desc: 'Nominal: no migration is running — the twin\'s write traffic is steady.' }
  };

  function setState(s) {
    state = s;
    el.root.dataset.state = s;
    var busy = (s === 'running' || s === 'crash' || s === 'prep');
    var copy = STATES[s] || STATES.idle;
    el.runBtn.disabled = busy;
    el.sql.disabled = busy;
    if (el.hostile) el.hostile.disabled = busy;
    el.presets.forEach(function (b) { b.disabled = busy; });
    el.runText.textContent = copy.run;
    el.runHint.textContent = copy.hint;
    el.chamberState.textContent = copy.chamber;
    el.chamberState.title = copy.desc;
  }

  /* ── the big number eases toward each 1 Hz sample ───────── */
  (function easeNumber() {
    displayedWps += (targetWps - displayedWps) * 0.22;
    if (Math.abs(targetWps - displayedWps) < 1) displayedWps = targetWps;
    el.wps.textContent = num(displayedWps);
    requestAnimationFrame(easeNumber);
  })();

  /* ── stats ──────────────────────────────────────────────── */
  function onStats(s) {
    // The first frame is what turns the awaiting labels into numbers.
    // Before it lands, every readout says what it is waiting for rather
    // than reading a zero we never measured.
    if (el.root.dataset.feed !== 'live') el.root.dataset.feed = 'live';
    // Optional: a backend may attach 60s of history to the first frame
    // so the scope opens full. Purely additive to the contract.
    if (!seeded) { seeded = true; if (s.history) chart.seed(s.history); }
    targetWps = s.writes_per_sec || 0;
    chart.push(s.writes_per_sec, s.blocked);
    el.casualties.textContent = num(s.casualties_total);

    var by = s.by_label || {};
    Array.prototype.forEach.call(el.labels.querySelectorAll('.lab[data-label]'), function (node) {
      var d = by[node.dataset.label] || { ok: 0, died: 0 };
      $('.lab__ok', node).textContent = num(d.ok);
      $('.lab__died', node).textContent = num(d.died) + ' died';
      node.dataset.hurt = d.died > 0 ? '1' : '0';
    });

    if (s.blocked && state !== 'crash') beginFreeze();
    if (!s.blocked && state === 'crash') endFreeze();
  }

  function beginFreeze() {
    setState('crash');
    var chamber = document.getElementById('chamber');
    if (chamber && !reduceMotion.matches) {
      chamber.classList.remove('is-impact');
      void chamber.offsetWidth;                  // restart the keyframes
      chamber.classList.add('is-impact');
      setTimeout(function () { chamber.classList.remove('is-impact'); }, 620);
    }
    freezeStartedAt = Date.now();
    clearInterval(freezeTimer);
    freezeTimer = setInterval(function () {
      var t = (Date.now() - freezeStartedAt) / 1000;
      el.impactTimer.innerHTML = t.toFixed(1) + '<i>s</i>';
    }, 100);
  }

  function endFreeze() {
    clearInterval(freezeTimer); freezeTimer = null;
    chart.closeBand();
    setState(currentRun ? 'running' : 'idle');
  }

  function onRunStarted(ev) {
    currentRun = ev;
    clearNotice();
    chart.mark();
    setState('running');
    el.twin.textContent = ev.twin || 'cloning…';
    el.twin.dataset.live = '1';
    el.reportNote.textContent = 'run ' + (ev.run_id || '') + ' · ' + (ev.mode || 'custom');
    swapReport(function () { return runningCard(ev); });
  }

  function onRunFinished(ev) {
    currentRun = null;
    clearNotice();
    clearInterval(freezeTimer); freezeTimer = null;
    chart.closeBand();
    setState('idle');
    el.twin.dataset.live = '0';
    el.twin.textContent = (ev.twin || el.twin.textContent) + ' · discarded';
    el.reportNote.textContent = 'run ' + (ev.run_id || '') + ' · complete';
    renderReport(ev);
    refreshStatus();
  }

  /* The one place the connection lamp is decided. Four states, each
     with its own colour and its own short mono label:
       init  amber   the transport has not answered yet
       sse   green   live, one frame per second, pushed
       poll  amber   the stream died; same numbers, pulled at 1 Hz
       down  red     nothing is answering */
  var TRANSPORT_TITLE = {
    init: 'Opening the stats stream.',
    sse: 'Server-sent events, one stats frame per second.',
    poll: 'The SSE stream dropped twice. Stats are now polled once a second — same data, same numbers.',
    down: 'The stats feed is not answering. The numbers below are the last ones measured.'
  };
  function onTransport(t) {
    el.transportChip.dataset.t = t.kind;
    el.transportLabel.textContent = api && api.isMock ? 'mock · sse' : t.label;
    el.transportChip.title = TRANSPORT_TITLE[t.kind] || TRANSPORT_TITLE.init;
  }

  /* ── report cards ───────────────────────────────────────── */
  function tpl(id) { return document.getElementById(id).content.firstElementChild.cloneNode(true); }
  function slot(root, name) { return root.querySelector('[data-slot="' + name + '"]'); }

  function runningCard(ev) {
    var n = document.createElement('div');
    n.className = 'working';
    n.innerHTML = '<span class="working__tag"></span><p></p>';
    n.querySelector('.working__tag').textContent = ev.mode === 'safe'
      ? 'building without the lock' : 'migration on the rig';
    n.querySelector('p').textContent = ev.mode === 'safe'
      ? 'Watch the line. A safe migration never dips, and this card fills in when the build finishes.'
      : 'Watch the line. The moment it takes a blocking lock, writes stop and the timer starts.';
    return n;
  }

  /* A failed CREATE INDEX CONCURRENTLY leaves an INVALID index in the
     schema. Nothing measures it and nothing cleans it up for you, so
     the card says so plainly and hands you the statement to run. */
  function renderWreckage(card, ev) {
    var wrap = slot(card, 'wreckWrap');
    if (!wrap) return;
    var idx = ev.invalid_indexes;
    if (!idx || !idx.length) return;
    var names = idx.map(function (i) {
      return '<code>' + String(typeof i === 'string' ? i : (i && i.name) || i).replace(/[<>&]/g, '') + '</code>';
    });
    wrap.hidden = false;
    var one = names.length === 1;
    slot(card, 'wreckText').innerHTML = one
      ? 'The build did not finish, so ' + names[0] + ' is still in the schema and marked INVALID. ' +
        'Postgres will not use it, and the next build will not replace it. Drop it, then run the migration again.'
      : 'The build did not finish, so ' + names.length + ' indexes are still in the schema and marked INVALID: ' +
        names.join(' ') + '. Postgres will not use them, and the next build will not replace them. ' +
        'Drop each one, then run the migration again.';
  }

  /* ── THE DIVERGENCE PROOF ─────────────────────────────────────
     The single most useful picture the run can draw: what a healthy
     run holds (the reference — flat at baseline), overlaid with what
     this run actually did, and one mark at the instant the two part.

     Everything here comes from the run's own evidence: ev.timeline is
     the 250ms trace [{t, wps, casualties}], ev.ddl_start_t / ddl_end_t
     delimit the migration inside it, and ev.baseline_writes_per_sec is
     the reference height. No new API surface — this is the payload the
     contract already carries, finally drawn.

     Divergence is defined as the first sample inside the DDL window
     where writes actually started dying; if nothing died, the first
     sample that fell below 40% of baseline. If neither happened, the
     two traces never parted and we say so instead of inventing a mark. */
  var DV_W = 640, DV_H = 72, DV_PAD = 6;

  function buildDivergence(ev, safe) {
    var tl = ev.timeline;
    if (!tl || tl.length < 4) return null;

    var base = ev.baseline_writes_per_sec || baselineWps || 0;
    if (base <= 0) {
      for (var b = 0; b < tl.length; b++) base = Math.max(base, tl[b].wps || 0);
    }
    if (base <= 0) return null;

    var t0 = tl[0].t, t1 = tl[tl.length - 1].t;
    if (!(t1 > t0)) return null;
    var ddl = typeof ev.ddl_start_t === 'number' ? ev.ddl_start_t : t0;

    var top = base * 1.25;
    var X = function (t) { return (t - t0) / (t1 - t0) * DV_W; };
    var Y = function (v) {
      return DV_PAD + (DV_H - DV_PAD * 2) * (1 - Math.min(1, Math.max(0, v / top)));
    };

    // where the two traces part
    var cut = null, cutKind = null;
    var casAtDdl = null;
    for (var i = 0; i < tl.length; i++) {
      var p = tl[i];
      if (p.t < ddl) { casAtDdl = p.casualties || 0; continue; }
      if (casAtDdl === null) casAtDdl = p.casualties || 0;
      if ((p.casualties || 0) > casAtDdl) { cut = p; cutKind = 'died'; break; }
      if (cut === null && (p.wps || 0) < base * 0.4) { cut = p; cutKind = 'stalled'; }
    }

    var wrap = document.createElement('div');
    wrap.className = 'diverge';

    var runPts = tl.map(function (p) { return X(p.t) + ',' + Y(p.wps || 0); }).join(' ');
    var refY = Y(base);

    // the shaded delta between the reference and the trace, so the
    // missing writes read as an area and not just a dropped line
    var fill = 'M0,' + refY + ' L' + runPts.split(' ').join(' L') + ' L' + DV_W + ',' + refY + ' Z';

    var svgNs = 'http://www.w3.org/2000/svg';
    var svg = document.createElementNS(svgNs, 'svg');
    svg.setAttribute('viewBox', '0 0 ' + DV_W + ' ' + DV_H);
    svg.setAttribute('preserveAspectRatio', 'none');
    svg.setAttribute('focusable', 'false');
    svg.setAttribute('aria-hidden', 'true');

    function node(name, attrs) {
      var n = document.createElementNS(svgNs, name);
      Object.keys(attrs).forEach(function (k) { n.setAttribute(k, attrs[k]); });
      // strokes must not stretch with the non-uniform viewBox
      if (name !== 'path' || attrs.class !== 'dv-fill') n.setAttribute('vector-effect', 'non-scaling-stroke');
      return n;
    }

    if (!safe) svg.appendChild(node('path', { 'class': 'dv-fill', d: fill }));
    svg.appendChild(node('line', { 'class': 'dv-ref', x1: 0, y1: refY, x2: DV_W, y2: refY }));
    svg.appendChild(node('polyline', { 'class': 'dv-run', points: runPts }));

    var pillText, pillSafe = false;
    if (cut && !safe) {
      var cx = X(cut.t);
      svg.appendChild(node('line', { 'class': 'dv-tick', x1: cx, y1: 0, x2: cx, y2: DV_H }));
      pillText = (cutKind === 'died' ? 'writes died' : 'writes stalled') +
        ' · t+' + (Math.round((cut.t - ddl) * 10) / 10).toFixed(1) + 's';
    } else {
      pillSafe = true;
      pillText = 'no divergence';
    }

    var head = document.createElement('div');
    head.className = 'diverge__head';
    head.innerHTML =
      '<span>divergence — measured</span>' +
      '<span class="diverge__key"><i class="is-ref">safe shape</i><i>this run</i></span>';

    var frame = document.createElement('div');
    frame.className = 'diverge__frame';
    frame.appendChild(svg);

    var pill = document.createElement('span');
    pill.className = 'diverge__pill' + (pillSafe ? ' diverge__pill--safe' : '');
    pill.textContent = pillText;
    // clamped so the pill never hangs off either end of the frame
    pill.style.left = (cut && !safe
      ? Math.min(88, Math.max(12, (X(cut.t) / DV_W) * 100))
      : 50) + '%';
    frame.appendChild(pill);

    var foot = document.createElement('p');
    foot.className = 'diverge__foot';
    foot.textContent = 'Dashed: ' + num(base) + ' writes/sec, the rate this table held before the migration. ' +
      'Solid: what it actually did, sampled every 250ms.';

    wrap.appendChild(head);
    wrap.appendChild(frame);
    wrap.appendChild(foot);
    return wrap;
  }

  function renderDivergence(card, ev, safe) {
    var host = slot(card, 'diverge');
    if (!host) return;
    var built;
    try { built = buildDivergence(ev, safe); } catch (e) { built = null; }
    if (!built) return;                      // no timeline, no claim
    host.hidden = false;
    host.replaceWith(built);
  }

  var LABEL_NAMES = { checkout: 'checkouts', login: 'logins', add_to_cart: 'cart adds' };
  var MODEL_NAMES = {
    linear_by_rows: 'linear by row count',
    access_exclusive_wait: 'lock-acquisition wait'
  };

  /* ── verdict header: the honest headline ──────────────────────
     The tag reads the EFFECTIVE verdict — the one after scale
     projection, which is the number a judge should act on. When the
     measured verdict differs (SAFE on the twin, UNSAFE once projected
     or once the lock has to wait) we say both, so the flip is legible
     rather than hidden. The qualifier chip fires when a result only
     looked safe because contention was never simulated — that case
     must never read as an unqualified green light. */
  function renderVerdictExtras(card, ev, measured, effective, acqNotSim) {
    var tagEl = card.querySelector('.card__verdict .tag');
    if (tagEl && effective) tagEl.textContent = effective.toLowerCase();

    var chip = slot(card, 'qualChip');
    if (chip && acqNotSim) chip.hidden = false;

    var flip = slot(card, 'flip');
    if (!flip) return;
    if (measured && effective && measured !== effective) {
      flip.textContent = acqNotSim
        ? measured.toLowerCase() + ' as measured on the idle twin → ' +
          effective.toLowerCase() + ' once the lock has to wait for a live transaction'
        : measured.toLowerCase() + ' as measured on the 10M twin → ' +
          effective.toLowerCase() + ' projected onto your table at scale';
      flip.hidden = false;
    }
  }

  /* ── two instruments agree ────────────────────────────────────
     The freeze is measured two independent ways: by the drop in write
     throughput, and by reading the lock straight from pg_locks. When
     both landed and a freeze actually happened, showing them side by
     side is a trust signal — two instruments corroborating, not noise. */
  function renderCorroborate(card, ev) {
    var host = slot(card, 'corroborate');
    if (!host) return;
    var b = ev.blocked_seconds, m = ev.monitor_blocked_seconds;
    if (b == null || m == null || b <= 0.05 || m <= 0.05) return;
    slot(card, 'corrWps').textContent = secs(b);
    slot(card, 'corrLocks').textContent = secs(m);
    host.hidden = false;
  }

  /* ── the teach layer ──────────────────────────────────────────
     The richest thing the engine returns, and what makes a non-expert
     trust the verdict: a plain-English paragraph on what lock this
     shape takes (explanation) and what THIS run means and what to do
     next (note). Rendered as first-class prose, never raw JSON. */
  function renderTeach(card, ev, acqNotSim) {
    var host = slot(card, 'teach');
    if (!host) return;
    var any = false;

    var exRow = slot(card, 'explainRow');
    if (ev.explanation && exRow) {
      slot(card, 'explanation').textContent = ev.explanation;
      var lm = slot(card, 'lockModes');
      if (lm) {
        var modes = (ev.lock_modes || []).join(' · ');
        var label = modes || ev.classification || '';
        lm.textContent = label;
        lm.hidden = !label;
      }
      exRow.hidden = false;
      any = true;
    }

    var noteRow = slot(card, 'noteRow');
    if (ev.note && noteRow) {
      slot(card, 'note').textContent = ev.note;
      noteRow.hidden = false;
      // when the verdict carries a caveat, the note IS the guidance —
      // lead with it rather than let it read as a footnote
      if (acqNotSim) noteRow.classList.add('teach__row--lead');
      any = true;
    }

    if (any) host.hidden = false;
  }

  /* ── projected impact — the server's number, never ours ───────
     The old build multiplied a client-side rate by the freeze and
     presented the product as fact. It is gone. This shows only the
     engine's own scale_projection, clearly labelled a projection, and
     shows nothing at all when the engine did not make one. */
  function renderProjection(card, ev) {
    var wrap = slot(card, 'projWrap');
    if (!wrap) return;
    var sp = ev.scale_projection;
    if (!sp || sp.projected_blocked_seconds == null || !sp.target_rows) return;

    var rows = sp.target_rows >= 1e6
      ? (Math.round(sp.target_rows / 1e5) / 10) + 'M'
      : num(sp.target_rows);
    var proj = slot(card, 'projected');
    proj.innerHTML = '~<b data-n="p"></b> freeze projected at ' + rows + ' rows';
    tickTo(proj.querySelector('[data-n="p"]'), sp.projected_blocked_seconds,
      function (v) { return (Math.round(v * 10) / 10).toFixed(1) + 's'; }, 900);

    var f = slot(card, 'formula');
    if (f) {
      var model = MODEL_NAMES[sp.model] || sp.model || 'model';
      f.innerHTML = 'A projection, not a measurement. ' +
        (ev.scale_warning ? ev.scale_warning.replace(/[<>&]/g, '') + ' ' : '') +
        'Model: <code>' + model.replace(/[<>&]/g, '') + '</code>' +
        (sp.measured_at_floor
          ? '. The measured freeze was at the floor (nothing was contending), so the figure above is modelled, not scaled from a real freeze.'
          : '.');
    }
    var btn = slot(card, 'whyBtn');
    if (btn && f) {
      btn.addEventListener('click', function () {
        var open = btn.getAttribute('aria-expanded') === 'true';
        btn.setAttribute('aria-expanded', String(!open));
        f.hidden = open;
      });
    }
    wrap.hidden = false;
  }

  /* ── hostile-mode contention ──────────────────────────────────
     When a run held a transaction open to force the freeze to be
     measured (not assumed), say so plainly: the freeze above was
     caused by a real held lock. This is the "watch it die" beat. */
  function renderContention(card, ev) {
    var host = slot(card, 'contention');
    if (!host) return;
    var c = ev.contention;
    if (!c || !(c.enabled || c.requested)) return;
    var hold = c.hold_seconds != null ? c.hold_seconds : 0;
    var cycles = c.cycles != null ? c.cycles : 0;
    slot(card, 'contendText').innerHTML =
      'A transaction was held open for <b>' + num(hold) + 's</b> across <b>' + num(cycles) + '</b> ' +
      (cycles === 1 ? 'cycle' : 'cycles') +
      ', so the freeze above was forced by a real held lock — measured, not assumed.';
    host.hidden = false;
  }

  /* ── errored / inconclusive — never a red UNSAFE badge ────────
     An errored run never executed, so it has no freeze and no
     casualties. Rendering it as UNSAFE (red, all-zero evidence) while
     hiding the Postgres error is, per the engine's own comment, the
     single most misleading thing the tool could say. These get their
     own honest cards, in the annotation colour, carrying the real
     error string verbatim. */
  function renderErrored(ev) {
    var card = tpl('tpl-errored');
    card.querySelector('.card__id').textContent = ev.run_id ? 'run ' + ev.run_id : '';
    var box = slot(card, 'errBox');
    if (box) box.textContent = ev.error || 'The database returned no error text.';
    renderTeach(card, ev, false);
    slot(card, 'duration').textContent = ev.duration_seconds != null
      ? 'migration ran ' + secs(ev.duration_seconds) + ' · twin discarded'
      : 'twin discarded';
    swapReport(function () { return card; });
  }

  function renderInconclusive(ev) {
    var card = tpl('tpl-inconclusive');
    card.querySelector('.card__id').textContent = ev.run_id ? 'run ' + ev.run_id : '';
    renderTeach(card, ev, false);
    slot(card, 'duration').textContent = ev.duration_seconds != null
      ? 'migration ran ' + secs(ev.duration_seconds) + ' · twin discarded'
      : 'twin discarded';
    swapReport(function () { return card; });
  }

  function renderReport(ev) {
    var measured = String(ev.verdict || '').toUpperCase();
    var effective = String(ev.effective_verdict || ev.verdict || '').toUpperCase();

    if (effective === 'ERRORED') { renderErrored(ev); return; }
    if (effective === 'INCONCLUSIVE') { renderInconclusive(ev); return; }

    // the headline is the EFFECTIVE verdict — the one after scale projection
    var safe = effective === 'SAFE';
    var acqNotSim = ev.qualifier === 'acquisition_not_simulated';

    var card = tpl(safe ? 'tpl-safe' : 'tpl-unsafe');
    card.querySelector('.card__id').textContent = ev.run_id ? 'run ' + ev.run_id : '';

    var total = (ev.casualties && ev.casualties.total) || 0;
    var byLabel = (ev.casualties && ev.casualties.by_label) || null;
    var asSecs = function (v) { return (Math.round(v * 10) / 10).toFixed(1) + 's'; };

    renderVerdictExtras(card, ev, measured, effective, acqNotSim);

    if (safe) {
      slot(card, 'freeze').textContent = secs(ev.blocked_seconds || 0);
      slot(card, 'died').textContent = num(total);
    } else if (acqNotSim) {
      // A red "0.0s" would read as an all-clear. It was never simulated —
      // say that, and let the caveat + note carry the finding.
      var fz = slot(card, 'freeze');
      fz.textContent = 'not simulated';
      var hero = fz.closest ? fz.closest('.metric') : null;
      if (hero) hero.classList.add('metric--muted');
      slot(card, 'died').textContent = num(total);
      var mixA = slot(card, 'mix');
      if (mixA) mixA.remove();
    } else {
      tickTo(slot(card, 'freeze'), ev.blocked_seconds || 0, asSecs, 900);
      tickTo(slot(card, 'died'), total, function (v) { return num(v) + ' writes'; }, 900);

      var mix = slot(card, 'mix');
      if (byLabel && total > 0) {
        Object.keys(byLabel).forEach(function (k, i) {
          if (!byLabel[k]) return;
          var li = document.createElement('li');
          li.innerHTML = '<b>' + num(byLabel[k]) + '</b> ' + (LABEL_NAMES[k] || k);
          li.style.setProperty('--i', String(i));
          mix.appendChild(li);
        });
        if (!mix.children.length) mix.remove();
      } else if (mix) {
        mix.remove();
      }
    }

    renderCorroborate(card, ev);
    renderTeach(card, ev, acqNotSim);
    renderDivergence(card, ev, safe);
    renderWreckage(card, ev);
    renderProjection(card, ev);
    renderContention(card, ev);

    slot(card, 'duration').textContent = ev.duration_seconds != null
      ? 'migration ran ' + secs(ev.duration_seconds) + ' · twin discarded'
      : 'twin discarded';

    swapReport(function () { return card; });
  }

  function showRejected(body) {
    var card = tpl('tpl-rejected');
    var alreadySafe = body.kind === 'already_safe' ||
      (!body.kind && /\bsafe\b/i.test(body.reason || ''));

    slot(card, 'title').textContent = alreadySafe
      ? "This one's actually safe on PG16"
      : "Brace won't run that";
    slot(card, 'reason').textContent = body.reason || 'No reason given.';

    if (body.suggestion) {
      slot(card, 'suggestWrap').hidden = false;
      slot(card, 'suggestLead').textContent = alreadySafe
        ? 'Want to see a real crash? Try this instead'
        : 'Try this instead';
      slot(card, 'suggestSql').textContent = body.suggestion;
      slot(card, 'loadBtn').addEventListener('click', function () {
        el.sql.value = body.suggestion;
        syncPresets();
        el.sql.focus();
      });
    }
    el.reportNote.textContent = 'not run';
    mountNotice(card);
  }

  function showPreparing(eta) {
    var card = tpl('tpl-prep');
    var left = Math.max(1, eta || 45);
    var span = slot(card, 'eta');
    var bar = slot(card, 'bar');
    span.textContent = left;
    el.reportNote.textContent = 'warming a twin';
    mountNotice(card);

    var total = left;
    var t = setInterval(function () {
      left -= 1;
      if (!card.isConnected) { clearInterval(t); return; }
      span.textContent = Math.max(0, left);
      bar.style.width = Math.min(100, ((total - left) / total) * 100) + '%';
      if (left <= 0) {
        clearInterval(t);
        span.textContent = '0';
        card.querySelector('.card__title').textContent = 'Twin ready — run it again';
        if (state === 'prep') setState('idle');
      }
      refreshStatus();
    }, 1000);
  }

  function showSimple(id, title, reason) {
    var card = tpl(id);
    if (title && slot(card, 'title')) slot(card, 'title').textContent = title;
    if (reason && slot(card, 'reason')) slot(card, 'reason').textContent = reason;
    mountNotice(card);
  }

  /* ── console ────────────────────────────────────────────── */
  function syncPresets() {
    var v = el.sql.value.trim();
    el.presets.forEach(function (b) {
      b.setAttribute('aria-pressed', String(v === PRESETS[b.dataset.preset].trim()));
    });
  }

  el.presets.forEach(function (b) {
    b.addEventListener('click', function () {
      el.sql.value = PRESETS[b.dataset.preset];
      syncPresets();
    });
  });
  el.sql.addEventListener('input', syncPresets);
  el.sql.addEventListener('keydown', function (e) {
    if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') { e.preventDefault(); doRun(); }
  });
  el.runBtn.addEventListener('click', doRun);

  /* ── the command chip ───────────────────────────────────────
     The RUN button is one HTTP call and the chip names it. Clicking
     copies the curl for the SQL currently in the console, so the lab
     is reachable from a terminal without reverse-engineering it. */
  if (el.cmdChip) {
    var cmdResetTimer = null;
    var curlFor = function () {
      var origin = API || location.origin;
      var body = { sql: el.sql.value };
      if (el.hostile && el.hostile.checked) body.hostile = true;
      return "curl -sX POST " + origin + "/api/run \\\n" +
        "  -H 'content-type: application/json' \\\n" +
        "  -d " + JSON.stringify(JSON.stringify(body)).replace(/'/g, "'\\''");
    };
    var flash = function (label, ok) {
      var path = $('.cmd__path', el.cmdChip);
      var verb = $('.cmd__verb', el.cmdChip);
      el.cmdChip.classList.add(ok ? 'is-copied' : 'is-failed');
      verb.textContent = label;
      path.textContent = '';
      clearTimeout(cmdResetTimer);
      cmdResetTimer = setTimeout(function () {
        el.cmdChip.classList.remove('is-copied', 'is-failed');
        verb.textContent = 'POST';
        path.textContent = '/api/run';
      }, 1800);
    };

    /* Clipboard permission is not guaranteed — an insecure origin or a
       locked-down browser will refuse. The old execCommand path still
       works in all of them, so the chip tries that before giving up. */
    var legacyCopy = function (text) {
      var ta = document.createElement('textarea');
      ta.value = text;
      ta.setAttribute('readonly', '');
      ta.style.cssText = 'position:fixed;top:0;left:-9999px;opacity:0';
      document.body.appendChild(ta);
      var done = false;
      try {
        ta.select();
        ta.setSelectionRange(0, text.length);
        done = document.execCommand('copy');
      } catch (e) { done = false; }
      document.body.removeChild(ta);
      return done;
    };

    el.cmdChip.addEventListener('click', function () {
      var text = curlFor();
      var no = function () {
        var done = legacyCopy(text);
        flash(done ? 'copied ✓' : 'copy blocked', done);
      };
      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(text).then(function () { flash('copied ✓', true); }, no);
      } else {
        no();
      }
    });
    el.cmdChip.setAttribute('aria-label', 'Copy the curl command for this run');
  }

  function doRun() {
    if (state === 'running' || state === 'crash') return;
    var sql = el.sql.value;
    var hostile = !!(el.hostile && el.hostile.checked);
    clearNotice();
    el.reportNote.textContent = 'dispatching';
    el.chamberState.textContent = 'loading the rig';   // pre-accept; setState takes over on run_started

    api.run(sql, { hostile: hostile }).then(function (res) {
      // run_started arrives on the stream; this only confirms acceptance.
      el.reportNote.textContent = 'run ' + (res.run_id || '') + ' accepted';
    }).catch(function (err) {
      var body = (err && err.body) || {};
      setState('idle');
      if (err.status === 400) showRejected(body);
      else if (err.status === 409) { showSimple('tpl-busy'); el.reportNote.textContent = 'rig busy'; }
      else if (err.status === 503) { setState('prep'); showPreparing(body.eta_seconds); }
      else if (err.status === 429) {
        // Rate-limited, not unreachable: the rig answered. Honest
        // diagnostics means this never wears the "lab offline" card.
        showSimple('tpl-ratelimited', null, body.reason ? body.reason : null);
        el.reportNote.textContent = 'rate limited';
      }
      else {
        showSimple('tpl-error', 'Can\'t reach the rig',
          'The API did not answer' + (body.reason ? ' (' + body.reason + ')' : '') +
          '. The dashboard keeps streaming; try the run again in a moment.');
        el.reportNote.textContent = 'unreachable';
      }
    });
  }

  /* ── status polling for the topology strip ──────────────── */
  function refreshStatus() {
    return api.status().then(function (s) {
      baselineWps = s.baseline_writes_per_sec || baselineWps;
      chart.setBaseline(baselineWps);
      el.baseline.textContent = num(baselineWps) + '/s';
      var p = (s.pool || {});
      el.pool.innerHTML = '<b>' + (p.warm || 0) + '</b> warm · <b>' + (p.warming || 0) + '</b> warming';
      if (s.template) {
        var rows = s.template.rows ? (s.template.rows >= 1e6
          ? Math.round(s.template.rows / 1e5) / 10 + 'M rows' : num(s.template.rows) + ' rows') : '';
        el.template.textContent = [rows, s.template.size].filter(Boolean).join(' · ') || 'postgres 16';
      }
    }).catch(function () { /* strip keeps its last known values */ });
  }

  /* ── boot: pick the rig ─────────────────────────────────── */
  function boot(chosen) {
    api = chosen;
    if (api.isMock) {
      el.mockChip.hidden = false;
      document.title = 'Brace (mock lab) — crash-test lab for Postgres migrations';
    }
    el.sql.value = PRESETS.naive;
    syncPresets();
    setState('idle');
    if (api.history) { chart.seed(api.history()); seeded = true; }
    refreshStatus();
    setInterval(refreshStatus, 10000);
    api.openStream({
      onStats: onStats,
      onRunStarted: onRunStarted,
      onRunFinished: onRunFinished,
      onTransport: onTransport
    });

    // A reload must not wipe the crash report. If the engine remembers the
    // last completed run, render it — but never step on a run that has
    // already started streaming, and only fill the empty state.
    if (api.hydrate) {
      api.hydrate().then(function (lr) {
        if (lr && !currentRun && state === 'idle' && el.report.querySelector('.blank')) {
          renderReport(lr);
          el.reportNote.textContent = 'run ' + (lr.run_id || '') + ' · last run';
        }
      }).catch(function () {});
    }
  }

  if (qs.get('mock') === '1') {
    boot(window.BraceMock.create());
  } else {
    var probeOnce = function (ms) {
      return Promise.race([
        BraceApi.status(),
        new Promise(function (_, rej) { setTimeout(rej, ms); })
      ]);
    };
    // First visit pays cross-origin TLS setup; give the rig a real chance
    // (and one retry) before surrendering to the simulator.
    probeOnce(6000)
      .then(function () { boot(BraceApi); })
      .catch(function () {
        probeOnce(6000)
          .then(function () { boot(BraceApi); })
          .catch(function () {
            // No rig reachable — fall back to the simulator so the page is
            // never a dead dashboard.
            boot(window.BraceMock.create());
          });
      });
  }
})();
