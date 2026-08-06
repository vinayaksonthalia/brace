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

    var pad = 10, plotH = h - pad * 2;
    var X = function (t) { return (t - (now - WINDOW)) / WINDOW * w; };
    var Y = function (v) { return pad + plotH - (v / (BASE * 1.15)) * plotH; };

    ctx.clearRect(0, 0, w, h);

    // baseline hairline
    ctx.strokeStyle = T.grid;
    ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(0, Y(BASE) + .5); ctx.lineTo(w, Y(BASE) + .5); ctx.stroke();

    var xa = X(pts[0].t), xb = X(pts[pts.length - 1].t);
    var head = pts[pts.length - 1];
    var headHurt = hurt(head.v);

    if (xb > xa) {
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

    // fade the trailing edge into the panel
    var fade = ctx.createLinearGradient(0, 0, Math.min(90, w * 0.2), 0);
    fade.addColorStop(0, 'rgba(' + T.fade.join(',') + ',1)');
    fade.addColorStop(1, 'rgba(' + T.fade.join(',') + ',0)');
    ctx.fillStyle = fade;
    ctx.fillRect(0, 0, Math.min(90, w * 0.2), h);

    if (stateEl && (headHurt > .5) !== (hurtState > .5)) {
      hurtState = headHurt;
      stateEl.dataset.hurt = headHurt > .5 ? '1' : '0';
      stateEl.textContent = headHurt > .5 ? 'blocked' : 'nominal';
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
