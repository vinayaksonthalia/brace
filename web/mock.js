/* ═══════════════════════════════════════════════════════════════
   mock.js — the simulated rig.
   Implements the exact same interface app.js expects from the real
   backend (see BraceApi in app.js):

     openStream({onStats,onRunStarted,onRunFinished,onTransport}) -> {close()}
     status()      -> Promise<statusJson>
     run(sql)      -> Promise<{run_id}>  |  rejects with {status, body}

   Enabled by ?mock=1, or automatically when GET /api/status is
   unreachable. Also the harness we develop the real backend's UI
   states against.

   Knobs (query string, mock only):
     ?mock=1            steady state, naive crash, safe run
     &nowarm=1          next run answers 503 no_warm_twin
     &speed=2           run the whole lifecycle at 2x
     &invalid=1         the next run leaves an INVALID index behind
   ═══════════════════════════════════════════════════════════════ */
(function (global) {
  'use strict';

  var LABELS = ['checkout', 'login', 'add_to_cart'];
  var MIX = { checkout: 0.22, login: 0.31, add_to_cart: 0.47 };
  var BASELINE = 8000;

  function jitter(base, pct) {
    return Math.max(0, Math.round(base * (1 + (Math.random() - 0.5) * 2 * pct)));
  }

  function classify(sql) {
    var s = String(sql || '').trim();
    var flat = s.replace(/\s+/g, ' ').toUpperCase();

    if (!s) {
      return { kind: 'rejected', reason: 'The console is empty. Paste a migration, or load one of the examples above.' };
    }
    // Not SQL we can run at all.
    if (!/^(CREATE|ALTER|DROP|REINDEX|VACUUM|CLUSTER|TRUNCATE|BEGIN|SET|COMMENT|UPDATE|INSERT|DELETE)\b/.test(flat)) {
      return {
        kind: 'rejected',
        reason: 'That does not start with a statement Brace can run against a twin. Brace accepts DDL and DML — the things migrations are made of.'
      };
    }
    // Safe in PG16 — worth saying so rather than wasting a twin.
    if (/^ALTER TABLE .+ ADD COLUMN /.test(flat) &&
        /DEFAULT/.test(flat) &&
        !/DEFAULT (NOW\(\)|CURRENT_|GEN_RANDOM|RANDOM\(\)|NEXTVAL)/.test(flat) &&
        !/\bUNIQUE\b|\bPRIMARY KEY\b|\bREFERENCES\b/.test(flat)) {
      return {
        kind: 'already_safe',
        reason: 'Adding a column with a constant default has not rewritten the table since Postgres 11. On PG16 this takes an ACCESS EXCLUSIVE lock for microseconds and touches no rows — there is nothing here to crash.',
        suggestion: 'CREATE INDEX idx_orders_customer_id\n  ON orders (customer_id);'
      };
    }
    if (/CONCURRENTLY/.test(flat)) return { kind: 'run', mode: 'safe' };
    if (/^CREATE (UNIQUE )?INDEX /.test(flat)) return { kind: 'run', mode: 'naive' };
    if (/^ALTER TABLE/.test(flat)) return { kind: 'run', mode: 'custom', unsafe: true };
    return { kind: 'run', mode: 'custom', unsafe: false };
  }

  function twinName() {
    var w = ['sled', 'barrier', 'dummy', 'anvil', 'kerb', 'pylon', 'strut', 'girder'];
    return 'brace_run_' + w[Math.floor(Math.random() * w.length)] +
      '_' + Math.random().toString(36).slice(2, 6);
  }

  function createMockApi(opts) {
    opts = opts || {};
    var qs = new URLSearchParams(global.location.search);
    var speed = Math.max(0.25, parseFloat(qs.get('speed')) || 1);
    var forceNoWarm = qs.get('nowarm') === '1';
    var forceInvalid = qs.get('invalid') === '1';

    var handlers = null;
    var tick = null;
    var runSeq = 0;

    var pool = { warm: 2, warming: 1 };
    var active = null;          // {run_id, mode, twin, startedAt, blockedFrom, blockedUntil, endsAt, casualties, byLabel}
    var totals = { checkout: { ok: 0, died: 0 }, login: { ok: 0, died: 0 }, add_to_cart: { ok: 0, died: 0 } };
    var casualties = 0;
    var currentTwin = null;
    var poolRefillAt = 0;

    function now() { return Date.now(); }

    function blocked() {
      return !!(active && active.blockedFrom && now() >= active.blockedFrom && now() < active.blockedUntil);
    }

    function emitStats() {
      var isBlocked = blocked();
      var wps;

      if (isBlocked) {
        // craters to zero within the first ~600ms, stays dead
        var since = now() - active.blockedFrom;
        wps = since < 700 ? jitter(BASELINE * 0.06, 0.8) : 0;
        // ~430 writes/sec hit their lock_timeout and die
        var died = Math.round(430 * (1 + (Math.random() - 0.5) * 0.16));
        // split first, then total the split — so the headline count always
        // equals the sum of its labels, exactly as the report card claims
        LABELS.forEach(function (l) {
          var n = Math.round(died * MIX[l]);
          totals[l].died += n;
          casualties += n;
          if (active) active.byLabel[l].died += n;
        });
      } else {
        // safe runs cost a shallow, honest dip while the build scans
        var drag = (active && active.mode === 'safe') ? 0.965 : 1;
        wps = jitter(BASELINE * drag, 0.035);
        LABELS.forEach(function (l) {
          var n = Math.round(wps * MIX[l]);
          totals[l].ok += n;
          if (active) active.byLabel[l].ok += n;
        });
      }

      if (poolRefillAt && now() >= poolRefillAt) {
        pool = { warm: 2, warming: 1 };
        poolRefillAt = 0;
      }

      handlers.onStats({
        writes_per_sec: wps,
        casualties_total: casualties,
        by_label: {
          checkout: { ok: totals.checkout.ok, died: totals.checkout.died },
          login: { ok: totals.login.ok, died: totals.login.died },
          add_to_cart: { ok: totals.add_to_cart.ok, died: totals.add_to_cart.died }
        },
        blocked: isBlocked,
        run_id: active ? active.run_id : null
      });

      if (active && now() >= active.endsAt) finish();
    }

    function finish() {
      var r = active;
      active = null;
      currentTwin = null;
      pool = { warm: 0, warming: 1 };
      poolRefillAt = now() + 18000 / speed;

      var blockedSeconds = r.blockedUntil > r.blockedFrom
        ? Math.round((r.blockedUntil - r.blockedFrom) / 100 * speed) / 10
        : 0;

      handlers.onRunFinished({
        run_id: r.run_id,
        verdict: blockedSeconds > 0.4 ? 'UNSAFE' : 'SAFE',
        blocked_seconds: blockedSeconds,
        casualties: {
          total: r.byLabel.checkout.died + r.byLabel.login.died + r.byLabel.add_to_cart.died,
          by_label: {
            checkout: r.byLabel.checkout.died,
            login: r.byLabel.login.died,
            add_to_cart: r.byLabel.add_to_cart.died
          }
        },
        duration_seconds: Math.round((now() - r.startedAt) / 100 * speed) / 10,
        // A CONCURRENTLY build that does not finish leaves the index in
        // the schema, marked INVALID. The API reports them; so do we.
        invalid_indexes: r.invalid ? ['idx_orders_customer_id'] : [],
        error: null
      });
    }

    return {
      isMock: true,

      status: function () {
        return Promise.resolve({
          pool: { warm: pool.warm, warming: pool.warming },
          baseline_writes_per_sec: BASELINE,
          template: { rows: 10000000, size: '873 MB' }
        });
      },

      // Optional capability: 58s of prior traffic so the scope opens
      // full instead of drawing itself in from the right edge.
      history: function () {
        var out = [];
        var t = now() - 58000;
        for (var i = 0; i < 58; i++) out.push([t + i * 1000, jitter(BASELINE, 0.035)]);
        return out;
      },

      openStream: function (h) {
        handlers = h;
        h.onTransport({ kind: 'sse', label: 'live · sse' });
        emitStats();
        tick = setInterval(emitStats, 1000);
        return {
          close: function () { clearInterval(tick); tick = null; }
        };
      },

      run: function (sql) {
        if (active) {
          return Promise.reject({ status: 409, body: { error: 'run_in_progress' } });
        }
        var verdict = classify(sql);

        if (verdict.kind === 'rejected' || verdict.kind === 'already_safe') {
          return Promise.reject({
            status: 400,
            body: {
              error: 'rejected',
              kind: verdict.kind,
              reason: verdict.reason,
              suggestion: verdict.suggestion || null
            }
          });
        }

        if (forceNoWarm || pool.warm === 0) {
          var eta = poolRefillAt ? Math.max(3, Math.ceil((poolRefillAt - now()) / 1000)) : 45;
          forceNoWarm = false; // one-shot, so the retry succeeds
          return Promise.reject({ status: 503, body: { error: 'no_warm_twin', eta_seconds: eta } });
        }

        var id = 'r' + (++runSeq);
        var mode = verdict.mode;
        var twin = twinName();
        currentTwin = twin;
        pool.warm -= 1;

        var t0 = now();
        var lead = 1000 / speed;        // traffic runs clean for a beat
        var freezeMs = 12400 / speed;   // the lock holds the table
        var tail = 2600 / speed;        // index finishes, writes resume
        var isUnsafe = mode === 'naive' || (mode === 'custom' && verdict.unsafe);

        active = {
          run_id: id,
          mode: mode,
          twin: twin,
          startedAt: t0,
          blockedFrom: isUnsafe ? t0 + lead : 0,
          blockedUntil: isUnsafe ? t0 + lead + freezeMs : 0,
          endsAt: isUnsafe ? t0 + lead + freezeMs + tail : t0 + 8000 / speed,
          invalid: forceInvalid,
          byLabel: {
            checkout: { ok: 0, died: 0 },
            login: { ok: 0, died: 0 },
            add_to_cart: { ok: 0, died: 0 }
          }
        };

        setTimeout(function () {
          if (handlers) handlers.onRunStarted({ run_id: id, sql: sql, twin: twin, mode: mode });
        }, 60);

        return Promise.resolve({ run_id: id });
      }
    };
  }

  global.BraceMock = { create: createMockApi, classify: classify };
})(window);
