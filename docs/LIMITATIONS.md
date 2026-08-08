# Honest limits

Brace is a rehearsal, not a guarantee. The trust mechanic is naming exactly
where the rehearsal stops matching reality.

## The twin is a sample, not your database

The stock lab seeds a synthetic ten-million-row `orders` table with a made-up
label mix (checkout / login / add_to_cart). That is a good demo and a weak
rehearsal — the migration you're actually afraid of runs against **your**
schema, **your** row widths, **your** indexes, **your** traffic. Two
environment variables (`BRACE_SEED=off`, `BRACE_LOAD_LABELS=...`) point a
self-hosted deployment at a restored production dump instead. See
[docs/BYOD.md](BYOD.md).

## Lock presence is scale-free; lock duration is not

Whether a statement takes an `ACCESS EXCLUSIVE` lock does not depend on table
size — a `CREATE INDEX` without `CONCURRENTLY` blocks writers whether the
table has ten rows or ten billion. **How long** it blocks them does depend on
size, because the work under the lock (a table rewrite, a full index build)
scales with row count. A 10M-row twin can measure a freeze too short to be
called UNSAFE while the same statement against a 500M-row production table
would be an outage.

Brace's answer is the scale projection, not a bigger twin: `BRACE_TARGET_ROWS`
(web lab), `-target-rows` (`brace-check`), or the `target_rows` field (MCP)
project the *measured* freeze onto the row count you supply, and the verdict
follows `effective_verdict` — measured **or** projected. A `CREATE INDEX` that
measures 7.5s on 10M rows and projects to 375s at 500M rows is reported
UNSAFE even though nothing bad happened in the rehearsal itself. The
projection is a model, not a second measurement — it has not been validated
across arbitrary index types, fill factors, or hardware, and it will be wrong
in ways a real crash test at your actual scale would not be.

## The traffic model is a caricature of production

24 workers running three labeled write statements under a 1.5s
`lock_timeout` is a stand-in for "an application doing writes," not a replay
of your actual query mix, connection pooling behavior, or retry logic. It
proves the *mechanism* — a blocking lock kills waiting writers — without
claiming to reproduce your specific traffic shape. `BRACE_LOAD_LABELS` lets
you describe your own mix, but it is still a fixed set of parameterized
statements, not a captured production workload.

## The default run has no long-running transaction — hostile mode adds one

A plain rehearsal runs the migration against writers only; nothing else holds
a lock. That correctly measures a statement that freezes writes by *holding* a
lock for a long time (a table rewrite, a `SHARE`-locked index build), but it
under-states a statement that is only dangerous because it has to *wait* for a
lock behind a long-running transaction — and then makes every write queue
behind it. A metadata-only `ADD COLUMN` is instant on an idle twin, so the
default run (and every linter) calls it safe, and Brace says so with the
qualifier `acquisition_not_simulated` rather than an unqualified green light.
**Hostile mode** (`BRACE_HOSTILE`, or `hostile: true` per run) holds a real
transaction open on the twin while the migration runs, so that acquisition
freeze is measured, not assumed. It is still a caricature of *one* held
transaction — not your real concurrency — and if the migration's lock never
actually conflicts with the hold, the run reports `contention_not_engaged`
rather than inventing a freeze.

## Sequences run; rollout coordination doesn't

Brace measures a single DDL statement **or** an ordered multi-statement
sequence (pass `statements=[...]`, up to 10) on one twin — each step runs in
order so a later step sees the schema the earlier steps produced, and each is
measured in its own window with a per-statement verdict plus a worst-of overall.
What stays out of scope is the application-level coordination *around* a
migration — dual writes, batched backfill jobs, feature-flag cutover — and long
transactional DDL blocks. Brace tells you what each DDL step does to live
writes; it does not orchestrate the rollout around it. Hostile mode cannot be
combined with a sequence (a sequence can't hold one contended lock across the
whole script); rehearse the single statement whose acquisition wait you care
about on its own with `hostile=true`.

## `CREATE INDEX CONCURRENTLY` has its own failure mode

Brace reports `CONCURRENTLY` builds as SAFE for write-freeze purposes because
they do not take the blocking lock — that is measured, not assumed. What it
does not simulate is a `CONCURRENTLY` build failing partway through and
leaving an `INVALID` index behind, which happens in production (usually from
a concurrent constraint violation) and requires a manual `DROP INDEX` and
retry. The lab surfaces `invalid_indexes` when it sees one during a run, but
it does not go looking for that failure mode on your behalf.

## The demo data is synthetic

Row values, timestamps, and label statement text in the stock seed are
generated, not sampled from any real dataset. Query planner behavior
(selectivity estimates, index choice) can differ from a production table
with real data skew even at the same row count. Bring your own dump
([docs/BYOD.md](BYOD.md)) if planner behavior is what you're rehearsing.

## Numbers are measured on one hardware profile

The measured-numbers table in the README reflects one run of the lab on the
Zerops hardware it was built and deployed on (2–3 dedicated vCPU, PostgreSQL
16, `NON_HA`). Absolute seconds will differ on different hardware; the
*relative* ordering (naive `ADD COLUMN ... DEFAULT random()` is far worse
than `CREATE INDEX CONCURRENTLY`) is the part expected to generalize.
