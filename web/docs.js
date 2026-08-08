/* ═══════════════════════════════════════════════════════════════
   docs.js — progressive enhancement for the manual.

   The page is fully readable with this file absent: every section is
   present in the markup, the chapter selector is a plain <details>
   disclosure whose anchors work on their own, and .reveal blocks are
   shown by default (`html:not(.js) .reveal` in docs.css). This only
   adds the niceties — fade-in on scroll, a chapter label that tracks
   where you are, and closing the menu once you pick a chapter.
   ═══════════════════════════════════════════════════════════════ */
(function () {
  'use strict';

  var reduce = window.matchMedia &&
    window.matchMedia('(prefers-reduced-motion: reduce)').matches;

  // ── reveal on scroll ─────────────────────────────────────────
  var reveals = [].slice.call(document.querySelectorAll('.reveal'));
  if (reduce || !('IntersectionObserver' in window)) {
    reveals.forEach(function (el) { el.classList.add('in'); });
  } else {
    var io = new IntersectionObserver(function (entries) {
      entries.forEach(function (e) {
        if (e.isIntersecting) { e.target.classList.add('in'); io.unobserve(e.target); }
      });
    }, { rootMargin: '0px 0px -12% 0px', threshold: 0.06 });
    reveals.forEach(function (el) { io.observe(el); });
  }

  // ── chapter selector ─────────────────────────────────────────
  var menu = document.getElementById('chapMenu');
  var cur = document.getElementById('chapCur');
  var secs = [].slice.call(document.querySelectorAll('.doc-sec[data-chapter]'));
  if (!menu || !cur || !secs.length) return;

  var links = [].slice.call(menu.querySelectorAll('.chapmenu__panel a'));

  function pad(n) { return (n < 10 ? '0' : '') + n; }

  function setCurrent(idx) {
    var sec = secs[idx];
    if (!sec) return;
    cur.innerHTML = '<i>' + pad(idx + 1) + '</i>';
    cur.appendChild(document.createTextNode(sec.getAttribute('data-chapter')));
    var id = sec.id;
    links.forEach(function (a) {
      var on = a.getAttribute('href') === '#' + id;
      if (on) a.setAttribute('aria-current', 'true');
      else a.removeAttribute('aria-current');
    });
  }

  // close the disclosure once a chapter is chosen
  links.forEach(function (a) {
    a.addEventListener('click', function () { menu.removeAttribute('open'); });
  });

  // close on outside click / Escape, so it behaves like a menu
  document.addEventListener('click', function (e) {
    if (menu.hasAttribute('open') && !menu.contains(e.target)) menu.removeAttribute('open');
  });
  document.addEventListener('keydown', function (e) {
    if (e.key === 'Escape' && menu.hasAttribute('open')) menu.removeAttribute('open');
  });

  // scrollspy: label follows the section nearest the top of the viewport
  var current = -1;
  function onScroll() {
    var line = 140, best = 0, bestTop = -Infinity;
    for (var i = 0; i < secs.length; i++) {
      var top = secs[i].getBoundingClientRect().top;
      if (top - line <= 0 && top > bestTop) { bestTop = top; best = i; }
    }
    if (best !== current) { current = best; setCurrent(best); }
  }

  var ticking = false;
  window.addEventListener('scroll', function () {
    if (ticking) return;
    ticking = true;
    window.requestAnimationFrame(function () { onScroll(); ticking = false; });
  }, { passive: true });

  setCurrent(0);
  onScroll();
})();
