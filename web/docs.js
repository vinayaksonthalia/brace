/* ═══════════════════════════════════════════════════════════════
   docs.js — progressive enhancement for the manual, as a docs site.

   The page is fully readable and navigable with this file absent:
   every section is present, the sidebar is real anchor links, and
   .reveal blocks are shown by default (`html:not(.js) .reveal`). The
   on-this-page rail is JS-only (hidden without JS). This file adds:
     · reveal-on-scroll fade-ins
     · scrollspy that highlights the active sidebar + TOC item
     · a working quick-filter over the sidebar (type to narrow/jump)
     · the mobile sidebar drawer (hamburger + backdrop + Esc)
     · a dynamic "on this page" rail built from the active section
   ═══════════════════════════════════════════════════════════════ */
(function () {
  'use strict';

  var reduce = window.matchMedia &&
    window.matchMedia('(prefers-reduced-motion: reduce)').matches;

  var secs = [].slice.call(document.querySelectorAll('.doc-sec[data-title]'));
  var navLinks = [].slice.call(document.querySelectorAll('.navlink[data-nav]'));

  // ── reveal on scroll ─────────────────────────────────────────
  var reveals = [].slice.call(document.querySelectorAll('.reveal'));
  if (reduce || !('IntersectionObserver' in window)) {
    reveals.forEach(function (el) { el.classList.add('in'); });
  } else {
    var io = new IntersectionObserver(function (entries) {
      entries.forEach(function (e) {
        if (e.isIntersecting) { e.target.classList.add('in'); io.unobserve(e.target); }
      });
    }, { rootMargin: '0px 0px -10% 0px', threshold: 0.04 });
    reveals.forEach(function (el) { io.observe(el); });
  }

  // ── the on-this-page rail (built from the active section) ────
  var tocList = document.getElementById('tocList');
  var tocLinks = [];

  function buildToc(sec) {
    if (!tocList || !sec) return;
    tocList.innerHTML = '';
    tocLinks = [];
    var frag = document.createDocumentFragment();

    // the section title itself
    var li0 = document.createElement('li');
    var a0 = document.createElement('a');
    a0.href = '#' + sec.id;
    a0.textContent = sec.getAttribute('data-title');
    a0.setAttribute('aria-current', 'true');
    li0.appendChild(a0);
    frag.appendChild(li0);
    tocLinks.push({ a: a0, target: sec });

    // any h3 subheadings within it get ids and nested links
    var subs = [].slice.call(sec.querySelectorAll('h3'));
    subs.forEach(function (h, i) {
      if (!h.id) {
        h.id = sec.id + '-h' + (i + 1) + '-' +
          h.textContent.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '').slice(0, 32);
      }
      var li = document.createElement('li');
      var a = document.createElement('a');
      a.href = '#' + h.id;
      a.className = 'dxtoc__sub';
      a.textContent = h.textContent;
      li.appendChild(a);
      frag.appendChild(li);
      tocLinks.push({ a: a, target: h });
    });
    tocList.appendChild(frag);
  }

  function markToc() {
    if (!tocLinks.length) return;
    var line = 185, best = 0, bestTop = -Infinity;
    for (var i = 0; i < tocLinks.length; i++) {
      var top = tocLinks[i].target.getBoundingClientRect().top;
      if (top - line <= 0 && top > bestTop) { bestTop = top; best = i; }
    }
    tocLinks.forEach(function (t, i) {
      if (i === best) t.a.setAttribute('aria-current', 'true');
      else t.a.removeAttribute('aria-current');
    });
  }

  // ── scrollspy: highlight the active section in the sidebar ───
  var currentSec = -1;
  function spy() {
    if (!secs.length) return;
    var line = 185, best = 0, bestTop = -Infinity;
    for (var i = 0; i < secs.length; i++) {
      var top = secs[i].getBoundingClientRect().top;
      if (top - line <= 0 && top > bestTop) { bestTop = top; best = i; }
    }
    if (best !== currentSec) {
      currentSec = best;
      var id = secs[best].id;
      navLinks.forEach(function (a) {
        if (a.getAttribute('href') === '#' + id) a.setAttribute('aria-current', 'true');
        else a.removeAttribute('aria-current');
      });
      buildToc(secs[best]);
    }
    markToc();
  }

  var ticking = false;
  window.addEventListener('scroll', function () {
    if (ticking) return;
    ticking = true;
    window.requestAnimationFrame(function () { spy(); ticking = false; });
  }, { passive: true });
  window.addEventListener('resize', function () { spy(); }, { passive: true });

  // ── the quick filter over the sidebar ────────────────────────
  var side = document.getElementById('dxside');
  var groups = [].slice.call(document.querySelectorAll('.dxnav .navgroup'));
  var inputs = [
    document.getElementById('docSearch'),
    document.getElementById('docSearchM')
  ].filter(Boolean);

  function filter(qRaw) {
    // Normalise before matching: fold to lower case, drop everything that is
    // not a letter, digit or space, then collapse runs of space. A stray "/"
    // (or any punctuation-only input — the exact keystroke that used to nuke the
    // whole nav) reduces to "" and is treated as no filter at all, so the nav is
    // never hidden by a character that could not match a section label anyway.
    var q = (qRaw || '').toLowerCase().replace(/[^a-z0-9 ]+/g, ' ').replace(/\s+/g, ' ').trim();
    if (!side) return;
    if (!q) {
      side.classList.remove('is-filtered', 'no-match');
      navLinks.forEach(function (a) { a.classList.remove('is-hidden'); });
      groups.forEach(function (g) { g.classList.remove('is-hidden'); });
      return;
    }
    side.classList.add('is-filtered');
    var any = false;
    groups.forEach(function (g) {
      var links = [].slice.call(g.querySelectorAll('.navlink'));
      var shown = 0;
      links.forEach(function (a) {
        var label = (a.textContent || '').toLowerCase();
        var match = label.indexOf(q) !== -1;
        a.classList.toggle('is-hidden', !match);
        if (match) { shown++; any = true; }
      });
      g.classList.toggle('is-hidden', shown === 0);
    });
    side.classList.toggle('no-match', !any);
  }

  function firstVisible() {
    for (var i = 0; i < navLinks.length; i++) {
      if (!navLinks[i].classList.contains('is-hidden')) return navLinks[i];
    }
    return null;
  }

  inputs.forEach(function (inp) {
    inp.addEventListener('input', function () {
      filter(inp.value);
      inputs.forEach(function (o) { if (o !== inp) o.value = inp.value; });
    });
    inp.addEventListener('keydown', function (e) {
      if (e.key === 'Enter') {
        var t = firstVisible();
        if (t) { t.click(); inp.blur(); closeNav(); }
      } else if (e.key === 'Escape') {
        inp.value = ''; filter(''); inputs.forEach(function (o) { o.value = ''; });
      }
    });
  });

  // press "/" to jump to the filter (unless already typing). preventDefault must
  // fire BEFORE focus moves, so the "/" that triggered the shortcut is cancelled
  // and never lands in the box (the keydown default — the character input — is
  // what we cancel; moving focus afterwards cannot revive it). We also guard
  // against modified chords ("/" under Ctrl/Meta/Alt is a browser shortcut, e.g.
  // Quick Find — leave it alone) and only act on the bare key. Even if a stray
  // "/" somehow reached the box, filter() now normalises it away, so the nav
  // stays intact either way — this is belt-and-suspenders.
  document.addEventListener('keydown', function (e) {
    if (e.key !== '/' || e.ctrlKey || e.metaKey || e.altKey) return;
    var tag = (e.target && e.target.tagName) || '';
    if (/^(INPUT|TEXTAREA|SELECT)$/.test(tag) || (e.target && e.target.isContentEditable)) return;
    if (!inputs.length) return;
    e.preventDefault();
    var visible = inputs.filter(function (i) { return i.offsetParent !== null; })[0] || inputs[0];
    visible.focus();
    // Some browsers place the caret without selecting; make a subsequent real
    // keystroke replace nothing unexpected by putting the caret at the end.
    try { var v = visible.value; visible.setSelectionRange(v.length, v.length); } catch (_) {}
  });

  // ── the mobile drawer ────────────────────────────────────────
  var toggle = document.getElementById('navToggle');
  var backdrop = document.getElementById('navBackdrop');

  function openNav() {
    document.body.classList.add('nav-open');
    if (backdrop) backdrop.hidden = false;
    if (toggle) { toggle.setAttribute('aria-expanded', 'true'); toggle.setAttribute('aria-label', 'Close navigation'); }
  }
  function closeNav() {
    if (!document.body.classList.contains('nav-open')) return;
    document.body.classList.remove('nav-open');
    if (backdrop) backdrop.hidden = true;
    if (toggle) { toggle.setAttribute('aria-expanded', 'false'); toggle.setAttribute('aria-label', 'Open navigation'); }
  }

  if (toggle) {
    toggle.addEventListener('click', function () {
      if (document.body.classList.contains('nav-open')) closeNav(); else openNav();
    });
  }
  if (backdrop) backdrop.addEventListener('click', closeNav);
  document.addEventListener('keydown', function (e) { if (e.key === 'Escape') closeNav(); });
  // close the drawer after choosing a destination
  navLinks.forEach(function (a) { a.addEventListener('click', closeNav); });

  // initial paint
  if (secs.length) { buildToc(secs[0]); spy(); }
})();
