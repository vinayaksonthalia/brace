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
      return fetch('/api/status', { headers: { accept: 'application/json' } })
        .then(function (r) {
          if (!r.ok) throw new Error('status ' + r.status);
          return r.json();
        });
    },

    run: function (sql) {
      return fetch('/api/run', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ sql: sql })
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
        es = new EventSource('/api/stream');
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
          fetch('/api/stats', { headers: { accept: 'application/json' } })
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

    function scaleY() {
      var peak = baseline || 1;
      for (var i = 0; i < pts.length; i++) peak = Math.max(peak, pts[i].v);
      return peak * 1.22;
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

      var GREEN = T.green, RED = T.red;

      function hurtOf(p) {
        if (p.blocked) return 1;
        if (!baseline) return 0;
        var ratio = p.v / baseline;
        return Math.min(1, Math.max(0, (0.9 - ratio) / 0.9));
      }
      function mix(t) {
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
    runText: $('.run__text'),
    runHint: $('#runHint'),
    report: $('#reportBody'),
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

  function setState(s) {
    state = s;
    el.root.dataset.state = s;
    var busy = (s === 'running' || s === 'crash');
    el.runBtn.disabled = busy;
    el.sql.disabled = busy;
    el.presets.forEach(function (b) { b.disabled = busy; });
    el.runText.textContent = busy ? 'Crash in progress' : 'Run the crash test';
    el.runHint.textContent = busy ? 'the twin is under load' : 'Ctrl↵ to run';
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
      setTimeout(function () { chamber.classList.remove('is-impact'); }, 500);
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
    chart.mark();
    setState('running');
    el.twin.textContent = ev.twin || 'cloning…';
    el.twin.dataset.live = '1';
    el.reportNote.textContent = 'run ' + (ev.run_id || '') + ' · ' + (ev.mode || 'custom');
    swapReport(function () { return runningCard(ev); });
  }

  function onRunFinished(ev) {
    currentRun = null;
    clearInterval(freezeTimer); freezeTimer = null;
    chart.closeBand();
    setState('idle');
    el.twin.dataset.live = '0';
    el.twin.textContent = (ev.twin || el.twin.textContent) + ' · discarded';
    el.chamberState.textContent = 'chamber nominal';
    el.reportNote.textContent = 'run ' + (ev.run_id || '') + ' · complete';
    renderReport(ev);
    refreshStatus();
  }

  function onTransport(t) {
    el.transportChip.dataset.t = t.kind;
    el.transportLabel.textContent = api && api.isMock ? 'mock · sse' : t.label;
    el.transportChip.title = t.kind === 'poll'
      ? 'The SSE stream dropped twice. Stats are now polled once a second — same data, same numbers.'
      : 'Server-sent events, one stats frame per second.';
  }

  /* ── report cards ───────────────────────────────────────── */
  function tpl(id) { return document.getElementById(id).content.firstElementChild.cloneNode(true); }
  function slot(root, name) { return root.querySelector('[data-slot="' + name + '"]'); }

  function runningCard(ev) {
    var n = document.createElement('div');
    n.className = 'empty';
    n.innerHTML = '<p></p>';
    n.querySelector('p').textContent = ev.mode === 'safe'
      ? 'Building the index without the lock. Watch the line — a safe migration never dips.'
      : 'The migration is on the rig. Watch the line.';
    return n;
  }

  var LABEL_NAMES = { checkout: 'checkouts', login: 'logins', add_to_cart: 'cart adds' };

  function renderReport(ev) {
    var safe = String(ev.verdict).toUpperCase() === 'SAFE';
    var card = tpl(safe ? 'tpl-safe' : 'tpl-unsafe');
    card.querySelector('.card__id').textContent = ev.run_id ? 'run ' + ev.run_id : '';

    var total = (ev.casualties && ev.casualties.total) || 0;
    var byLabel = (ev.casualties && ev.casualties.by_label) || null;

    var asSecs = function (v) { return (Math.round(v * 10) / 10).toFixed(1) + 's'; };

    if (safe) {
      slot(card, 'freeze').textContent = secs(ev.blocked_seconds || 0);
      slot(card, 'died').textContent = num(total);
    } else {
      tickTo(slot(card, 'freeze'), ev.blocked_seconds || 0, asSecs, 900);
      tickTo(slot(card, 'died'), total, function (v) { return num(v) + ' writes'; }, 900);

      var mix = slot(card, 'mix');
      if (byLabel) {
        Object.keys(byLabel).forEach(function (k, i) {
          var li = document.createElement('li');
          li.innerHTML = '<b>' + num(byLabel[k]) + '</b> ' + (LABEL_NAMES[k] || k);
          li.style.setProperty('--i', String(i));
          mix.appendChild(li);
        });
      } else {
        mix.remove();
      }

      // projection — always second, always labelled, formula on demand
      var rate = baselineWps || 0;
      var denied = Math.round(rate * (ev.blocked_seconds || 0));
      var share = { checkout: 0.22, login: 0.31 };
      var proj = slot(card, 'projected');
      proj.innerHTML = '~<span data-n="c"></span> checkouts + <span data-n="l"></span> logins denied';
      tickTo(proj.querySelector('[data-n="c"]'), Math.round(denied * share.checkout), num, 900);
      tickTo(proj.querySelector('[data-n="l"]'), Math.round(denied * share.login), num, 900);
      slot(card, 'formula').innerHTML =
        'Projection, not a measurement. <code>' + num(rate) + ' writes/sec × ' +
        secs(ev.blocked_seconds) + ' = ' + num(denied) +
        '</code> writes that would find the table locked, split by the traffic mix above. ' +
        'The measured numbers are the two figures above this one.';

      var btn = slot(card, 'whyBtn');
      var f = slot(card, 'formula');
      btn.addEventListener('click', function () {
        var open = btn.getAttribute('aria-expanded') === 'true';
        btn.setAttribute('aria-expanded', String(!open));
        f.hidden = open;
      });
    }

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
    swapReport(function () { return card; });
  }

  function showPreparing(eta) {
    var card = tpl('tpl-prep');
    var left = Math.max(1, eta || 45);
    var span = slot(card, 'eta');
    var bar = slot(card, 'bar');
    span.textContent = left;
    el.reportNote.textContent = 'warming a twin';
    swapReport(function () { return card; });

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
      }
      refreshStatus();
    }, 1000);
  }

  function showSimple(id, title, reason) {
    var card = tpl(id);
    if (title && slot(card, 'title')) slot(card, 'title').textContent = title;
    if (reason && slot(card, 'reason')) slot(card, 'reason').textContent = reason;
    swapReport(function () { return card; });
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

  function doRun() {
    if (state === 'running' || state === 'crash') return;
    var sql = el.sql.value;
    el.reportNote.textContent = 'dispatching';
    el.chamberState.textContent = 'loading the rig';

    api.run(sql).then(function (res) {
      // run_started arrives on the stream; this only confirms acceptance.
      el.reportNote.textContent = 'run ' + (res.run_id || '') + ' accepted';
    }).catch(function (err) {
      var body = (err && err.body) || {};
      el.chamberState.textContent = 'chamber nominal';
      if (err.status === 400) showRejected(body);
      else if (err.status === 409) { showSimple('tpl-busy'); el.reportNote.textContent = 'rig busy'; }
      else if (err.status === 503) showPreparing(body.eta_seconds);
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
  }

  if (qs.get('mock') === '1') {
    boot(window.BraceMock.create());
  } else {
    var timeout = new Promise(function (_, rej) { setTimeout(rej, 2500); });
    Promise.race([BraceApi.status(), timeout])
      .then(function () { boot(BraceApi); })
      .catch(function () {
        // No rig reachable — fall back to the simulator so the page is
        // never a dead dashboard.
        boot(window.BraceMock.create());
      });
  }
})();
