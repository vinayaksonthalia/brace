/* ═══════════════════════════════════════════════════════════════
   BRACE — landing behaviour.
   Two things only: the pulse teaser (the brand gesture, same
   green→red ramp as the lab's scope) and scroll reveals.
   ═══════════════════════════════════════════════════════════════ */
(function () {
  'use strict';

  var reduce = matchMedia('(prefers-reduced-motion: reduce)');

  /* ── scroll reveals ─────────────────────────────────────── */
  var items = document.querySelectorAll('.reveal');
  if (!('IntersectionObserver' in window) || reduce.matches) {
    items.forEach(function (n) { n.classList.add('in'); });
  } else {
    // Siblings that enter together are staggered by their index within
    // the group, so a row of cards deals in rather than popping at once.
    var io = new IntersectionObserver(function (entries) {
      var batch = entries.filter(function (e) { return e.isIntersecting; });
      batch.forEach(function (e, i) {
        e.target.style.setProperty('--i', String(Math.min(i, 5)));
        e.target.classList.add('in');
        io.unobserve(e.target);
      });
    }, { rootMargin: '0px 0px -12% 0px', threshold: 0.08 });
    items.forEach(function (n) { io.observe(n); });
  }

  /* ── the section rail ────────────────────────────────────
     [03/06] › THE CLAIM, updated as you scroll, so the page reads
     as a numbered report rather than a scroll. Purely decorative
     (aria-hidden) — the headings are the real structure — and it
     stays hidden until the reader has actually started reading. */
  (function () {
    var rail = document.getElementById('secrail');
    var secs = Array.prototype.slice.call(document.querySelectorAll('[data-sec]'));
    if (!rail || !secs.length || !('IntersectionObserver' in window)) return;

    var noEl = rail.querySelector('.secrail__no');
    var nameEl = rail.querySelector('.secrail__name');
    var total = String(secs.length).padStart(2, '0');
    var seen = new Array(secs.length).fill(false);
    var shown = -1;

    function pad(n) { return String(n + 1).padStart(2, '0'); }

    function paint() {
      // the deepest section currently on screen wins, so scrolling
      // forward never flickers back to the one above it
      var idx = -1;
      for (var i = 0; i < secs.length; i++) if (seen[i]) idx = i;
      if (idx < 0 || idx === shown) return;
      shown = idx;
      noEl.textContent = '[' + pad(idx) + '/' + total + ']';
      nameEl.textContent = '› ' + secs[idx].dataset.sec;
      rail.classList.toggle('in', idx > 0);
    }

    rail.hidden = false;
    var io = new IntersectionObserver(function (entries) {
      entries.forEach(function (e) { seen[secs.indexOf(e.target)] = e.isIntersecting; });
      paint();
    }, { rootMargin: '-12% 0px -55% 0px' });
    secs.forEach(function (s) { io.observe(s); });
  })();

  /* ── the ambient heartbeat ───────────────────────────────
     A fixed trace at the bottom edge that follows you down the page:
     barely there at the top, a little stronger as you scroll, then
     handing its opacity to the sign-off pulse as the finale rises —
     so the two read as one heartbeat merging into the close.
     rAF-throttled, passive listener, static under reduced motion. */
  (function () {
    var amb = document.getElementById('ambient');
    if (!amb) return;
    if (reduce.matches) { amb.style.opacity = '.08'; return; }

    var finale = document.querySelector('.signoff');
    var queued = false;

    function apply() {
      queued = false;
      var vh = window.innerHeight || 1;
      var progress = Math.min(1, window.scrollY / (vh * 1.1));
      var op = 0.09 + progress * 0.16;
      if (finale) {
        var top = finale.getBoundingClientRect().top;
        // the handoff: fade out exactly as the real pulse comes up
        op *= Math.max(0, Math.min(1, (top - vh * 0.35) / (vh * 0.4)));
      }
      amb.style.opacity = op.toFixed(3);
    }

    addEventListener('scroll', function () {
      if (queued) return;
      queued = true;
      requestAnimationFrame(apply);
    }, { passive: true });
    addEventListener('resize', apply, { passive: true });
    apply();
  })();

  document.querySelectorAll('a[href^="#"]').forEach(function (a) {
    a.addEventListener('click', function (e) {
      var href = a.getAttribute('href');
      if (!href || href === '#') return;          // placeholder links stay inert
      var t;
      try { t = document.querySelector(href); } catch (err) { return; }
      if (!t) return;
      e.preventDefault();
      t.scrollIntoView({ behavior: reduce.matches ? 'auto' : 'smooth', block: 'start' });
    });
  });

  /* ── the pulse ──────────────────────────────────────────── */
  var heroMark = document.getElementById('heroMark');
  var canvas = document.getElementById('teaser');
  if (!canvas) return;
  var stateEl = document.getElementById('teaserState');
  var ctx = canvas.getContext('2d');

  var T = {};
  function readTheme() {
    var cs = getComputedStyle(document.documentElement);
    var v = function (n, d) { return (cs.getPropertyValue(n) || d).trim(); };
    var rgb = function (n, d) { return v(n, d).split(',').map(Number); };
    T = {
      green: rgb('--chart-green-rgb', '47,224,138'),
      red: rgb('--chart-red-rgb', '255,59,48'),
      grid: v('--chart-grid', 'rgba(120,160,200,.09)'),
      axisText: v('--chart-axis-text', 'rgba(148,165,185,.62)'),
      mark: v('--chart-mark', 'rgba(232,163,63,.6)'),
      markText: v('--chart-mark-text', 'rgba(232,163,63,.9)'),
      fade: rgb('--panel-fade-rgb', '13,19,27')
    };
  }
  readTheme();
  var BASE = 1;                 // normalised traffic
  var CYCLE = 8000;             // one flatline every 8s
  var FREEZE_AT = 5600;         // when the lock lands
  var FREEZE_FOR = 1500;        // how long writes stay dead

  var pts = [];                 // {t, v}
  var w = 0, h = 0;
  var last = 0;
  var visible = true;

  function size() {
    var dpr = Math.min(window.devicePixelRatio || 1, 2);
    var r = canvas.getBoundingClientRect();
    w = Math.max(1, r.width); h = Math.max(1, r.height);
    canvas.width = Math.round(w * dpr);
    canvas.height = Math.round(h * dpr);
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  }
  new ResizeObserver(size).observe(canvas);
  size();

  document.addEventListener('visibilitychange', function () { visible = !document.hidden; });

  function valueAt(t) {
    var p = t % CYCLE;
    if (p < FREEZE_AT) {
      // healthy: a small, irregular pulse — never a sine wave
      return BASE * (0.94 + 0.05 * Math.sin(t / 210) + 0.03 * Math.sin(t / 97));
    }
    var into = p - FREEZE_AT;
    if (into < 180) return BASE * (1 - into / 180) * 0.5;   // the crater edge
    if (into < FREEZE_FOR) return 0;                        // flatlined
    var back = (into - FREEZE_FOR) / 700;                   // recovery
    return back >= 1 ? BASE * 0.96 : BASE * Math.min(0.96, back * 1.05);
  }

  function hurt(v) { return Math.min(1, Math.max(0, (0.9 - v / BASE) / 0.9)); }
  function mix(t) {
    var e = Math.pow(t, 0.72);
    return [
      Math.round(T.green[0] + (T.red[0] - T.green[0]) * e),
      Math.round(T.green[1] + (T.red[1] - T.green[1]) * e),
      Math.round(T.green[2] + (T.red[2] - T.green[2]) * e)
    ];
  }
  function rgba(c, a) { return 'rgba(' + c[0] + ',' + c[1] + ',' + c[2] + ',' + a + ')'; }

  var WINDOW = 6000;
  var MONO = 'ui-monospace, SFMono-Regular, Menlo, monospace';
  var hurtState = 0;
  var loop = !reduce.matches;
  var staticAt = 0;

  function frame(now) {
    if (loop) requestAnimationFrame(frame);
    if (!visible) { last = now; return; }
    if (loop && now - last < 16) return;
    last = now;

    if (loop) pts.push({ t: now, v: valueAt(now) });
    while (pts.length && pts[0].t < now - WINDOW - 200) pts.shift();
    if (pts.length < 2) return;

    var pad = 14, plotH = h - pad * 2;
    var X = function (t) { return (t - (now - WINDOW)) / WINDOW * w; };
    var Y = function (v) { return pad + plotH - (v / (BASE * 1.15)) * plotH; };

    ctx.clearRect(0, 0, w, h);

    // baseline hairline
    ctx.strokeStyle = T.grid;
    ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(0, Y(BASE) + .5); ctx.lineTo(w, Y(BASE) + .5); ctx.stroke();

    // the annotation: an amber leader at the exact instant the lock lands.
    // Same marker vocabulary the lab's scope uses, so the two pages agree.
    if (loop) {
      for (var k = Math.floor((now - WINDOW) / CYCLE); k <= Math.ceil(now / CYCLE); k++) {
        var mt = k * CYCLE + FREEZE_AT;
        if (mt < now - WINDOW || mt > now) continue;
        var mx = Math.round(X(mt)) + .5;
        ctx.save();
        ctx.strokeStyle = T.mark;
        ctx.setLineDash([2, 3]);
        ctx.lineWidth = 1;
        ctx.beginPath(); ctx.moveTo(mx, pad - 6); ctx.lineTo(mx, pad + plotH); ctx.stroke();
        ctx.restore();
        ctx.fillStyle = T.markText;
        ctx.font = '9px ' + MONO;
        ctx.fillText('MIGRATION', Math.min(mx + 5, w - 66), pad - 1);
      }
    }

    var xa = X(pts[0].t), xb = X(pts[pts.length - 1].t);
    var head = pts[pts.length - 1];
    var headHurt = hurt(head.v);

    if (xb > xa) {
      // area first, tinted along x by the same ramp as the trace and faded
      // downward, so the space under the line is the distance it can fall
      var area = ctx.createLinearGradient(xa, 0, xb, 0);
      pts.forEach(function (p) {
        area.addColorStop(Math.min(1, Math.max(0, (X(p.t) - xa) / (xb - xa))), rgba(mix(hurt(p.v)), .17));
      });
      ctx.save();
      ctx.beginPath();
      ctx.moveTo(xa, pad + plotH);
      pts.forEach(function (p) { ctx.lineTo(X(p.t), Y(p.v)); });
      ctx.lineTo(xb, pad + plotH);
      ctx.closePath();
      ctx.clip();
      ctx.fillStyle = area;
      ctx.fillRect(xa, pad, xb - xa, plotH);
      var vfade = ctx.createLinearGradient(0, pad, 0, pad + plotH);
      vfade.addColorStop(0, 'rgba(0,0,0,0)');
      vfade.addColorStop(1, 'rgba(0,0,0,.94)');
      ctx.globalCompositeOperation = 'destination-out';
      ctx.fillStyle = vfade;
      ctx.fillRect(xa, pad, xb - xa, plotH);
      ctx.restore();

      var grad = ctx.createLinearGradient(xa, 0, xb, 0);
      pts.forEach(function (p) {
        grad.addColorStop(Math.min(1, Math.max(0, (X(p.t) - xa) / (xb - xa))), rgba(mix(hurt(p.v)), 1));
      });
      ctx.beginPath();
      pts.forEach(function (p, i) { i ? ctx.lineTo(X(p.t), Y(p.v)) : ctx.moveTo(X(p.t), Y(p.v)); });
      ctx.lineJoin = 'round'; ctx.lineCap = 'round';
      ctx.lineWidth = 2;
      ctx.strokeStyle = grad;
      ctx.shadowColor = rgba(mix(headHurt), .5);
      ctx.shadowBlur = 12;
      ctx.stroke();
      ctx.shadowBlur = 0;

      ctx.fillStyle = rgba(mix(headHurt), 1);
      ctx.beginPath(); ctx.arc(X(head.t), Y(head.v), 2.6, 0, Math.PI * 2); ctx.fill();
    }

    // fade the trailing edge into the panel — short and soft, so it reads
    // as the trace running off the left of the scope, not as a swatch
    var fw = Math.min(64, w * 0.12);
    var fade = ctx.createLinearGradient(0, 0, fw, 0);
    fade.addColorStop(0, 'rgba(' + T.fade.join(',') + ',1)');
    fade.addColorStop(.45, 'rgba(' + T.fade.join(',') + ',.65)');
    fade.addColorStop(1, 'rgba(' + T.fade.join(',') + ',0)');
    ctx.fillStyle = fade;
    ctx.fillRect(0, 0, fw, h);

    if ((headHurt > .5) !== (hurtState > .5)) {
      hurtState = headHurt;
      var hurting = headHurt > .5;
      if (stateEl) {
        stateEl.dataset.hurt = hurting ? '1' : '0';
        stateEl.textContent = hurting ? 'blocked' : 'nominal';
      }
      // the mark blooms red on the same frame the trace flatlines
      if (heroMark) heroMark.classList.toggle('is-bloom', hurting);
    }
  }

  window.addEventListener('brace:theme', function () {
    readTheme();
    if (!loop) requestAnimationFrame(function () { frame(staticAt); });
  });

  if (loop) {
    requestAnimationFrame(frame);
  } else {
    // Reduced motion: one static, healthy trace. No loop, no flatline.
    staticAt = performance.now();
    for (var i = 0; i < 200; i++) pts.push({ t: staticAt - (200 - i) * 30, v: valueAt(i * 20) });
    requestAnimationFrame(function () { frame(staticAt); });
    window.addEventListener('resize', function () { frame(staticAt); });
  }
})();
