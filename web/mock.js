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
    if (/CONCURRENTLY/.test(flat)) return { kind: 'run', mode: 'safe', shape: 'share_update_exclusive' };
    if (/^CREATE (UNIQUE )?INDEX /.test(flat)) return { kind: 'run', mode: 'naive', shape: 'share' };
    // A catalog-only rename. Its lock is momentary and does not conflict with a
    // held write transaction, so under hostile mode the acquisition wait is
    // REQUESTED but never engaged — the honest "you asked, it did not exercise
    // the wait" case the report card must not dress up as a forced freeze.
    if (/^ALTER INDEX .+ RENAME\b/.test(flat) || /^ALTER TABLE .+ RENAME\b/.test(flat)) {
      return { kind: 'run', mode: 'custom', shape: 'catalog_only' };
    }
    // Passes the allowlist, but Postgres rejects it at execution — an
    // ERRORED run, not a rejected one. The engine mislabelling this as
    // UNSAFE is the single most misleading thing the tool could say, so
    // the mock produces it to prove the errored card renders honestly.
    if (/^ALTER TABLE\b/.test(flat) && /\bWHERE\b/.test(flat)) {
      return {
        kind: 'run', mode: 'custom', shape: null, errored: true,
        error: 'ERROR: syntax error at or near "WHERE" (SQLSTATE 42601)'
      };
    }
    if (/^ALTER TABLE/.test(flat)) return { kind: 'run', mode: 'custom', shape: 'access_exclusive', unsafe: true };
    return { kind: 'run', mode: 'custom', shape: null, unsafe: false };
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
    var forceErrored = qs.get('errored') === '1';
    var forceInconclusive = qs.get('inconclusive') === '1';
    var quietIdle = qs.get('quiet') === '1';   // near-zero idle traffic, to see the resting state

    var handlers = null;
    var tick = null;
    var runSeq = 0;

    var pool = { warm: 2, warming: 1 };
    var active = null;          // {run_id, mode, twin, startedAt, blockedFrom, blockedUntil, endsAt, casualties, byLabel}
    var totals = { checkout: { ok: 0, died: 0 }, login: { ok: 0, died: 0 }, add_to_cart: { ok: 0, died: 0 } };
    var casualties = 0;
    var currentTwin = null;
    var poolRefillAt = 0;
    var lastRun = null;
    try {
      var saved = global.sessionStorage.getItem('brace:mock:lastrun');
      if (saved) lastRun = JSON.parse(saved);
    } catch (e) {}

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
      } else if (quietIdle && !active) {
        // a deliberately quiet engine, to exercise the neutral resting
        // trace (a flat low line must never read as a crash-red flatline)
        wps = jitter(BASELINE * 0.02, 0.6);
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

    /* The 250ms trace the real engine publishes as RunResult.Timeline.
       Synthesised here from the same lifecycle the stats stream already
       animates, so the mock's crash report can draw the divergence proof
       exactly as the rig's does. Shape is the frozen contract:
       [{t, wps, casualties}] in seconds since load start. */
    function timelineFor(r) {
      var out = [];
      var lead = r.blockedFrom ? r.blockedFrom - r.startedAt : 1000 / speed;
      var pre = 3000 / speed;                            // baseline before the run
      var span = (now() - r.startedAt) + 1000 / speed;   // a beat of tail
      var step = 250 / speed;
      var cas = 0;
      var perSample = 430 * (step / 1000) * speed;
      for (var ms = -pre; ms <= span; ms += step) {
        var t = r.startedAt + ms;
        var isBlocked = r.blockedFrom && t >= r.blockedFrom && t < r.blockedUntil;
        var wps;
        if (isBlocked) {
          var into = t - r.blockedFrom;
          wps = into < 500 ? Math.round(BASELINE * (1 - into / 500) * 0.5) : 0;
          cas += perSample;
        } else if (r.blockedUntil && t >= r.blockedUntil && t - r.blockedUntil < 700 / speed) {
          wps = Math.round(BASELINE * ((t - r.blockedUntil) / (700 / speed)) * 0.98);
        } else {
          wps = jitter(BASELINE * (r.mode === 'safe' ? 0.965 : 1), 0.03);
        }
        out.push({
          // seconds since load start, on the un-sped-up clock the real
          // engine reports in — so ?speed=3 changes the wall time, not
          // the numbers on the report card
          t: Math.round((ms + pre) / 1000 * speed * 1000) / 1000,
          wps: wps,
          casualties: Math.round(cas)
        });
      }
      // The DDL is ISSUED before writes die: it has to wait for its own
      // lock first. Half a second of that gap is what makes the two traces
      // visibly run together and then part, which is the whole point.
      var ddl = (pre + lead) / 1000 * speed - 0.5;
      return { points: out, ddlStartT: Math.round(Math.max(0, ddl) * 1000) / 1000 };
    }

    /* The teach layer the real engine attaches to every RunResult:
       what lock the statement takes and why (explanation), what THIS
       run means and what to do next (note), the honest verdict after
       scale projection (effective_verdict), and the caveat that fires
       when an ACCESS EXCLUSIVE shape only looked safe because nothing
       was holding a lock (qualifier). Mocked here per lock shape so the
       report renders representative — never misleading — prose without
       a backend. The real path (internal/*) fills the same field names,
       so app.js reads one shape either way. */
    function describe(r, blockedSeconds) {
      var r1 = function (n) { return Math.round(n * 10) / 10; };
      var hostile = !!r.hostile;
      var out = {
        verdict: blockedSeconds > 0.4 ? 'UNSAFE' : 'SAFE',
        effective_verdict: null,
        qualifier: null,
        classification: 'statement',
        lock_modes: [],
        explanation: 'This statement did not take a lock that blocks writes on the twin.',
        note: 'No blocking lock and no freeze were measured on the twin.',
        monitor_blocked_seconds: null,
        scale_projection: null,
        scale_warning: null,
        contention: hostile ? { enabled: true, requested: true, hold_seconds: 8, cycles: 3 } : null,
        hostile: hostile
      };

      if (r.errored) {
        out.classification = 'errored';
        out.verdict = 'ERRORED';
        out.effective_verdict = 'ERRORED';
        out.explanation = 'The statement passed the safety allowlist but Postgres rejected it during execution. No lock was taken and no writes were touched, so there is nothing to measure.';
        out.note = 'This is the database\'s own error, verbatim — fix the statement and run it again. An errored run is not a safe one and not an unsafe one; it simply never executed.';
        return out;
      }
      if (r.inconclusive) {
        out.classification = 'inconclusive';
        out.verdict = 'INCONCLUSIVE';
        out.effective_verdict = 'INCONCLUSIVE';
        out.explanation = 'The run started but the measurement could not be trusted — the twin\'s traffic generator stalled before a clean baseline was captured.';
        out.note = 'No verdict was reached. This is reported honestly rather than guessed: re-run it, ideally with hostile mode, to get a measurement worth acting on.';
        return out;
      }
      if (r.shape === 'share') {
        out.classification = 'create_index';
        out.lock_modes = ['ShareLock'];
        out.verdict = 'UNSAFE';
        out.effective_verdict = 'UNSAFE';
        out.monitor_blocked_seconds = r1(blockedSeconds * 0.99);
        out.explanation = 'CREATE INDEX takes a SHARE lock on the whole table and holds it for the entire build. SHARE blocks every INSERT, UPDATE and DELETE, so writes stay frozen from the moment the build starts until the index is finished.';
        out.note = 'Writes froze for ' + r1(blockedSeconds) + 's while the index built on a 10M-row twin. The fix is CREATE INDEX CONCURRENTLY, which builds the same index in two passes without ever taking a blocking lock, so traffic keeps flowing. A bare CREATE INDEX should never run against a live table.';
        out.scale_projection = {
          target_rows: 100000000,
          projected_blocked_seconds: r1(blockedSeconds * 10),
          verdict: 'UNSAFE',
          model: 'linear_by_rows',
          measured_at_floor: false
        };
        out.scale_warning = 'Projected linearly by row count from the measured freeze (10M to 100M rows). Real build time also grows with index width and disk throughput, so treat this as a lower bound.';
      } else if (r.shape === 'share_update_exclusive') {
        out.classification = 'create_index_concurrently';
        out.lock_modes = ['ShareUpdateExclusiveLock'];
        out.verdict = 'SAFE';
        out.effective_verdict = 'SAFE';
        out.monitor_blocked_seconds = 0;
        out.explanation = 'CREATE INDEX CONCURRENTLY takes only a SHARE UPDATE EXCLUSIVE lock, which does not block reads or writes — it only blocks other schema changes on the same table. The index is built in two passes while normal traffic continues.';
        out.note = 'No freeze, and write throughput never dipped: this is the safe way to add an index to a live table. One caveat — if a CONCURRENTLY build is interrupted it leaves an INVALID index behind that Postgres will not use and will not overwrite, so drop it before retrying.';
      } else if (r.shape === 'access_exclusive') {
        out.classification = 'alter_table';
        out.lock_modes = ['AccessExclusiveLock'];
        out.explanation = 'ALTER TABLE takes an ACCESS EXCLUSIVE lock, the strongest lock Postgres has: it blocks reads and writes alike. The statement itself is near-instant on an idle table, so in production the freeze comes almost entirely from how long this lock waits to be granted behind transactions that are already open.';
        if (hostile) {
          out.verdict = 'UNSAFE';
          out.effective_verdict = 'UNSAFE';
          out.monitor_blocked_seconds = r1(blockedSeconds * 0.98);
          out.note = 'With a transaction deliberately held open (8s across 3 cycles), the ACCESS EXCLUSIVE lock queued behind it and every write blocked for ' + r1(blockedSeconds) + 's. That is the real freeze this migration causes any time a transaction is in flight — which, on a live database, is always. Gate it behind a short lock_timeout with a retry, or avoid the table rewrite entirely.';
          // The freeze here came from the HELD LOCK, not from row count, so the
          // engine refuses to scale it: target_rows is set (the deployment always
          // sets it) but the projection is Known()=false — projected 0, verdict "".
          // The report must render NOTHING for this, never "~0.0s projected".
          out.scale_projection = {
            target_rows: 100000000,
            projected_blocked_seconds: 0,
            verdict: '',
            model: 'access_exclusive_wait',
            measured_at_floor: false,
            scales_with_rows: false
          };
        } else {
          out.verdict = 'SAFE';
          out.qualifier = 'acquisition_not_simulated';
          out.effective_verdict = 'UNSAFE';
          out.monitor_blocked_seconds = 0;
          out.note = 'Measured SAFE only because the twin had no competing transactions, so the ACCESS EXCLUSIVE lock was granted instantly and the freeze you would feel in production was never simulated. Re-run with hostile mode on to hold a transaction open and measure the real lock-acquisition freeze before trusting this result.';
          out.scale_projection = {
            target_rows: 100000000,
            projected_blocked_seconds: 9.0,
            verdict: 'UNSAFE',
            model: 'access_exclusive_wait',
            measured_at_floor: true
          };
          out.scale_warning = 'The twin measured a zero-length freeze because nothing was holding a lock. On a busy table this statement queues behind open transactions and blocks every new one until they commit — modelled here as a multi-second freeze. Measure it directly with hostile mode.';
        }
      } else if (r.shape === 'catalog_only') {
        out.classification = 'alter_rename';
        out.lock_modes = ['AccessExclusiveLock'];
        out.verdict = 'SAFE';
        out.effective_verdict = 'SAFE';
        out.monitor_blocked_seconds = 0;
        out.explanation = 'A rename only updates a catalog entry. It takes its ACCESS EXCLUSIVE lock for microseconds and scans or rewrites nothing, so there is no freeze to measure regardless of table size.';
        if (hostile) {
          // Requested contention that never engaged: the rename\'s lock does not
          // conflict with the held write transaction, so it never had to queue.
          // The verdict stays honestly SAFE, flagged so nobody reads these
          // default numbers as "tested under contention and passed".
          out.qualifier = 'contention_not_engaged';
          out.contention = {
            enabled: false,
            requested: true,
            hold_seconds: 8,
            cycles: 3,
            note: 'the migration never had to wait for its lock — its lock level does not conflict with the hold, so the acquisition wait was not exercised'
          };
          out.note = 'Hostile mode held a transaction open, but this statement never had to queue behind it for its lock, so the freeze you would feel under contention was not exercised. These numbers are a default, uncontended run — not proof this is safe under load.';
        } else {
          out.note = 'A catalog-only rename: no rows are scanned or rewritten, so there is no freeze on a twin of any size.';
        }
      }

      if (out.effective_verdict === null) out.effective_verdict = out.verdict;
      return out;
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

      var tl = timelineFor(r);
      var d = describe(r, blockedSeconds);

      var payload = {
        run_id: r.run_id,
        verdict: d.verdict,
        // the verdict AFTER scale projection — this is the headline the
        // report leads with. It can be UNSAFE while the measured verdict
        // is SAFE (e.g. an ACCESS EXCLUSIVE that only acquired instantly
        // because the idle twin held nothing).
        effective_verdict: d.effective_verdict,
        qualifier: d.qualifier,
        blocked_seconds: blockedSeconds,
        // the same freeze, measured a second, independent way (pg_locks).
        // When both are present and agree, the report shows them as two
        // instruments corroborating rather than as duplicate noise.
        monitor_blocked_seconds: d.monitor_blocked_seconds,
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
        // the plain-English teach layer + machine-readable classification
        classification: d.classification,
        lock_modes: d.lock_modes,
        explanation: d.explanation,
        note: d.note,
        // the real projection (never a client-computed number)
        scale_projection: d.scale_projection,
        scale_warning: d.scale_warning,
        // present when hostile mode held a transaction open to force the
        // lock-acquisition freeze to be measured rather than assumed
        contention: d.contention,
        hostile: d.hostile,
        // the evidence the divergence proof is drawn from
        baseline_writes_per_sec: BASELINE,
        timeline: tl.points,
        ddl_start_t: tl.ddlStartT,
        ddl_end_t: Math.round((now() - r.startedAt + 3000 / speed) / 1000 * speed * 1000) / 1000,
        // the database's own error string, verbatim, on an errored run
        error: r.errorText || null
      };

      // Remember the last completed run so a page reload can rehydrate the
      // crash report instead of showing "no run on record". This mirrors
      // the real backend's GET /api/stats -> last_run; here it is the
      // visitor's own last run, scoped to the tab session.
      lastRun = payload;
      try { global.sessionStorage.setItem('brace:mock:lastrun', JSON.stringify(payload)); } catch (e) {}

      handlers.onRunFinished(payload);
    }

    return {
      isMock: true,

      status: function () {
        return Promise.resolve({
          pool: { warm: pool.warm, warming: pool.warming },
          baseline_writes_per_sec: BASELINE,
          template: { rows: 10000000, size: '873 MB' },
          last_run: lastRun
        });
      },

      // Rehydrate the last completed run on load, so a refresh does not
      // wipe the crash report. Mirrors the real backend contract.
      hydrate: function () { return Promise.resolve(lastRun); },

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

      run: function (sql, opts) {
        opts = opts || {};
        var hostile = !!opts.hostile;
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
        var shape = verdict.shape || null;
        var errored = !!verdict.errored || forceErrored;
        var inconclusive = forceInconclusive;
        var errorText = verdict.error || (forceErrored
          ? 'ERROR: canceling statement due to lock timeout (SQLSTATE 55P03)' : null);
        var twin = twinName();
        currentTwin = twin;
        pool.warm -= 1;
        // one-shot knobs, so a retry behaves normally
        forceErrored = false;
        forceInconclusive = false;

        var t0 = now();
        var lead = 1000 / speed;        // traffic runs clean for a beat
        var tail = 2600 / speed;        // the build finishes, writes resume

        // Whether the twin actually freezes, and for how long, is decided
        // by the lock shape — not a blanket "unsafe" flag:
        //   SHARE (bare CREATE INDEX)      always freezes; the whole build.
        //   SHARE UPDATE EXCLUSIVE         never freezes; CONCURRENTLY.
        //   ACCESS EXCLUSIVE (ALTER TABLE) freezes only when hostile mode
        //     holds a transaction open — otherwise the idle twin grants
        //     the lock instantly and we flag acquisition_not_simulated.
        var freezes = false;
        var freezeMs = 0;
        // an errored or inconclusive run never took a real measurement
        if (!errored && !inconclusive) {
          if (shape === 'share') { freezes = true; freezeMs = 12400 / speed; }
          else if (shape === 'access_exclusive') { freezes = hostile; freezeMs = 8000 / speed; }
        }
        // errored runs die fast — Postgres rejected the statement
        var runMs = errored ? 1600 / speed : 8000 / speed;

        active = {
          run_id: id,
          mode: mode,
          shape: shape,
          hostile: hostile,
          errored: errored,
          inconclusive: inconclusive,
          errorText: errorText,
          twin: twin,
          startedAt: t0,
          blockedFrom: freezes ? t0 + lead : 0,
          blockedUntil: freezes ? t0 + lead + freezeMs : 0,
          endsAt: freezes ? t0 + lead + freezeMs + tail : t0 + runMs,
          invalid: forceInvalid,
          byLabel: {
            checkout: { ok: 0, died: 0 },
            login: { ok: 0, died: 0 },
            add_to_cart: { ok: 0, died: 0 }
          }
        };

        setTimeout(function () {
          if (handlers) handlers.onRunStarted({ run_id: id, sql: sql, twin: twin, mode: mode, hostile: hostile });
        }, 60);

        return Promise.resolve({ run_id: id });
      }
    };
  }

  global.BraceMock = { create: createMockApi, classify: classify };
})(window);
