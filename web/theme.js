/* ═══════════════════════════════════════════════════════════════
   theme.js — light / dark for both pages.

   The chosen theme is already on <html data-theme> before this file
   runs (a two-line script in <head> does that, so there is no flash).
   This only owns the toggle, persistence, and telling the canvases to
   repaint: they read their colours from CSS custom properties and
   re-read them on the `brace:theme` event.
   ═══════════════════════════════════════════════════════════════ */
(function () {
  'use strict';

  var KEY = 'brace:theme';
  var root = document.documentElement;

  function current() { return root.dataset.theme === 'light' ? 'light' : 'dark'; }

  function apply(theme) {
    root.dataset.theme = theme;
    try { localStorage.setItem(KEY, theme); } catch (e) { /* private mode */ }
    sync();
    window.dispatchEvent(new CustomEvent('brace:theme', { detail: { theme: theme } }));
  }

  var btn = document.getElementById('themeToggle');

  function sync() {
    if (!btn) return;
    var dark = current() === 'dark';
    var next = dark ? 'light' : 'dark';
    btn.setAttribute('aria-label', 'Switch to ' + next + ' theme');
    btn.setAttribute('title', 'Switch to ' + next + ' theme');
    btn.setAttribute('aria-pressed', String(dark));
    var label = btn.querySelector('.chip__t');
    if (label) label.textContent = next;
  }

  if (btn) {
    sync();
    btn.addEventListener('click', function () {
      apply(current() === 'dark' ? 'light' : 'dark');
    });
  }

  // Follow the OS only while the visitor has never chosen for themselves.
  var mq = window.matchMedia('(prefers-color-scheme: light)');
  var listen = function (e) {
    var stored = null;
    try { stored = localStorage.getItem(KEY); } catch (err) {}
    if (stored) return;
    root.dataset.theme = e.matches ? 'light' : 'dark';
    sync();
    window.dispatchEvent(new CustomEvent('brace:theme', { detail: { theme: current() } }));
  };
  if (mq.addEventListener) mq.addEventListener('change', listen);
  else if (mq.addListener) mq.addListener(listen);
})();
