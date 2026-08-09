# Honest limits

Brace is a rehearsal, not a guarantee. The trust mechanic is naming exactly
where the rehearsal stops matching reality.

Each limit below carries a **disposition**: *fixed* (closed by a shipped
change), *partially owned* (mitigated, with a named gap still open), or *owned*
(out of scope today, with a roadmap direction). A tool that names its twenty
edges precisely earns more trust than one that names none — so none of these is
hidden, and not one of them is allowed to turn an unmeasured change into a green
light. When an edge means Brace can't be sure, the verdict degrades to a
labelled qualifier, never to a bare SAFE.

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
UNSAFE even though nothing bad happened in the rehearsal itself.

The projection is a model, not a second measurement, and it is deliberately a
**linear floor**: `projected = measured × target_rows ÷ twin_rows`. Real
index-build and table-rewrite cost is **super-linear** (≈ O(*n* log *n*) or
worse), so the projected freeze is a **lower bound** — at least this long, and
usually longer, never shorter. Past ~20× the twin size the number is an
order-of-magnitude flag rather than a figure, and the projection says so with
`scale_projection.qualifier = "modeled_beyond_measurement"`. It has not been
validated across arbitrary index types, fill factors, or hardware, and it will
be wrong in ways a real crash test at your actual scale would not be.

The projection also reports **whose row count it used**, in
`scale_projection.target_rows_source`: `request` (you passed `target_rows` for
this run), `deployment` (the deployment's `BRACE_TARGET_ROWS`), or
`brace_default_assumption` (nobody supplied one, so Brace assumed a size —
~100M rows — to keep the false-safe defence on by default). Over MCP, leaving
`target_rows` at 0 does **not** skip the projection: it falls back to the
deployment default or that assumption, and the source field plus the note say
which, loudly, so an assumed size is never read as a fact about your table.

## The traffic model is a caricature of production

24 workers running a labeled write mix under a 1.5s `lock_timeout` is a
stand-in for "an application doing writes," not a replay of your actual query
mix, connection pooling behavior, or retry logic. It proves the *mechanism* —
a blocking lock kills waiting writers — without claiming to reproduce your
specific traffic shape.

The stock labels are **simulated**, and worth being precise about: `checkout`,
`login` and `add_to_cart` (weights 3 / 2 / 5) all run the **identical**
statement — `INSERT INTO orders (customer_id, amount_cents) VALUES ($1, $2)`.
The persona names are cosmetic tags applied by weight, not three distinct
operations, so `casualties.by_label` is a weighted split of one synthetic write
— a realistic *shape* of failure, not a count of real user actions.
`BRACE_LOAD_LABELS` lets you describe your own mix (each label its own
statement), but it is still a fixed set of parameterized statements, not a
captured production workload.

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

## Bring-your-own-schema is one synthetic table with generic writes

Pasting a `CREATE TABLE` (`schema_ddl`) rehearses the migration against **your
table's shape** on the public lab, without self-hosting — but it is a weaker
rehearsal than a restored dump in three named ways. It takes **exactly one**
plain `CREATE TABLE` (not multiple related tables, not `CREATE TABLE … AS`, not
temp/unlogged). Its rows are **type-aware but synthetic**: required columns are
filled from a fixed type whitelist, a required column of an unsupported type is
**rejected by name** rather than filled wrong, and the distribution is uniform,
so planner selectivity can differ from production. And the load against it is a
**single generic `write` label**, not the persona split — `casualties.by_label`
has one key, `write`. The seed is capped at **5,000,000 rows** (default 1M)
regardless of the `row_count` you state; `row_count` still drives the scale
projection, so the honest reading is "measured on ≤5M synthetic rows, projected
onto your real size." When you need real values, real distribution, or a real
multi-table workload, restore a dump — see [docs/BYOD.md](BYOD.md).

## Numbers are measured on one hardware profile

The measured-numbers table in the README reflects one run of the lab on the
Zerops hardware it was built and deployed on (2–3 dedicated vCPU, PostgreSQL
16, `NON_HA`). Absolute seconds will differ on different hardware; the
*relative* ordering (naive `ADD COLUMN ... DEFAULT random()` is far worse
than `CREATE INDEX CONCURRENTLY`) is the part expected to generalize.

## One Postgres version, and the report card doesn't say which

Every rehearsal runs against a single PostgreSQL major version — 16 on the
hosted lab — and the report card does not name it. That matters because several
of Brace's verdicts are version-gated: `ADD COLUMN ... DEFAULT <constant>` is
catalog-only only since PG 11 (it rewrote the whole table on 10), and
`ALTER INDEX ... RENAME` takes the light `SHARE UPDATE EXCLUSIVE` lock only
since PG 12. There is no PG 11/12/13/14/15/16 matrix; a caller cannot see which
assumptions a given result leans on, or that a SAFE about a version-sensitive
shape would flip on an older engine. **Disposition: partially owned.** The
per-shape version assumptions are documented in `teach.go`, but the running
twin's `server_version` is never surfaced. Roadmap: report the twin's exact
`server_version` / `server_version_num` in every report card, and let a
self-hosted deployment pin the twin to the same major it runs in production so
the rehearsal matches the engine it is judging. Until then, read every verdict
as "on PostgreSQL 16" and re-check version-gated shapes against your own major.

## No replication or HA dimension

A twin is one standalone primary. Brace never stands up a replica, so it is
blind to the three hazards that exist only in a replicated topology: an
`ACCESS EXCLUSIVE` lock taken on a primary stalling read replicas through
recovery conflict and `max_standby_streaming_delay`; replication lag opening up
under a long index build or table rewrite; and a schema change a
logical-replication subscriber cannot apply (a column type change or a rewrite
that breaks a publication/subscription). Brace measures what the change does to
writers on the primary and says nothing about what it does downstream — a SAFE
is "safe on the primary only." **Disposition: owned, out of scope for the
current engine.** Roadmap: an optional streaming replica in the twin topology so
recovery-conflict pauses and lag under the migration become measured numbers,
plus a logical-replication smoke check that applies the DDL through a real
subscriber. If you run replicas or logical replication, treat today's verdict as
primary-only.

## No disk or memory dimension

The twin is not sized against your production disk or `maintenance_work_mem`, so
Brace cannot catch the failures that are about resources rather than locks: a
`VACUUM FULL` or table rewrite needs roughly **2x** the table's size in free
disk and fails part-way if it isn't there, and an index build spills to disk —
or slows to a crawl — when `maintenance_work_mem` is too small for the table.
The freeze Brace measures is a lock freeze, not a disk-full or out-of-memory
freeze, and a rewrite that fits comfortably on the twin can still run your
production primary out of disk. **Disposition: owned, out of scope.** Roadmap:
report the twin's free disk and `maintenance_work_mem`, and add a disk-headroom
pre-check for the rewrite classes (`VACUUM FULL`, `SET LOGGED` / `SET UNLOGGED`,
`ALTER COLUMN ... TYPE`) that fails loudly when the twin has more headroom than
your target rather than silently rehearsing a rewrite that would abort in
production.

## Statement coverage is an allowlist, and its edges are named

Brace only rehearses the shapes it can attribute a freeze window and casualty
count to: `CREATE INDEX`, `DROP INDEX`, `ALTER TABLE`, `ALTER INDEX`, `REINDEX`,
`VACUUM`, `ANALYZE`, `COMMENT ON`. Anything else is **rejected** as
`not_allowlisted` rather than guessed at — the honest failure — but it means
these real migration hazards are simply not modeled: `TRUNCATE` (takes
`ACCESS EXCLUSIVE`), `CREATE TABLE` / `DROP TABLE`, `CREATE MATERIALIZED VIEW`
and a non-`CONCURRENTLY` `REFRESH MATERIALIZED VIEW` (which locks the matview
`ACCESS EXCLUSIVE`), a standalone `CLUSTER` (rewrites the table under
`ACCESS EXCLUSIVE`), `ALTER TYPE ... ADD VALUE` on an enum (cannot run inside a
transaction block), and large DML backfills (`UPDATE` / `INSERT ... SELECT` over
millions of rows). `RENAME` **is** in scope, classified constant-time on
PG 12+. **The self-inconsistency worth naming out loud:** Brace's own advice for
several unsafe shapes is "add the column, then **backfill in batches**" — yet a
bare backfill `UPDATE` is exactly the DML the allowlist rejects, so Brace
prescribes a remediation step it cannot itself crash-test. **Disposition: owned;
the allowlist is deliberate** (a shape with no measurable freeze model is
rejected, never assumed safe). Roadmap: extend the allowlist one lock-taking DDL
at a time, each with its own measured lock lesson, and add a batched-backfill
rehearsal mode so "backfill in batches" becomes something Brace can actually run
and time instead of only recommend.

## The `INVALID` index aftermath is surfaced, not rehearsed

A `CREATE INDEX CONCURRENTLY` is reported SAFE for write-freeze because it takes
no blocking lock — measured, not assumed — and a run surfaces `invalid_indexes`
if it *observes* one. What Brace still does not do is deliberately induce the
canonical `CONCURRENTLY`-build-fails-partway failure (usually a concurrent
constraint violation) that leaves an `INVALID` index behind, nor rehearse the
manual `DROP INDEX` + retry recovery that failure forces. **Disposition:
partially owned** — detection ships, induction does not. Roadmap: an optional
fault-injection variant that drives a constraint violation during the concurrent
build so the invalid-index cleanup path is measured, not merely detected when it
happens to occur.

## No auth or tenancy model

The hosted lab is a shared, unauthenticated pool: one global rate limit, one run
slot, one result ledger, no per-caller isolation. That is right for a public
demo but means there is no tenant boundary — one caller's runs contend with
everyone's, and the ledger is common. The boundary Brace **does** enforce is the
role-and-network one (`brace_migrator` / `brace_traffic` are `NOSUPERUSER`, the
`db` is never publicly exposed), not a tenancy one. **Disposition: owned, out of
scope for the public lab.** Roadmap: API keys with per-key rate limits and
private result slots for the self-hosted deployment, so a team can run Brace in
CI without sharing a queue or a ledger with the public instance.

## No CI / gate-mode / webhook story yet

Brace exposes a synchronous HTTP + MCP API and a `brace-check` CLI, and the
verdict enum is built so a gate can treat anything but a clean measured `SAFE`
as "do not ship" (`brace-check` exits non-zero on it). What does not exist yet is
first-class CI packaging: no migration-file submission (you paste SQL; you do
not point Brace at a `migrations/` directory), no PR status-check or webhook, no
hosted gate mode. **Disposition: owned, roadmap** — the hard part (a clean
four-value measured enum plus a non-zero exit) is done; the missing piece is
integration. Roadmap: a GitHub Action wrapping `brace-check` that reads the
changed migration files, runs each, and posts the report card as a status check,
plus an optional webhook so a failing gate can block a merge.

## Degenerate no-op measurements are honest but uninformative

Brace measures what the statement actually did on the twin, so when the
statement is a no-op on the twin's data the measurement is a truthful near-zero.
`SET NOT NULL` on a column the stock twin already stores as `NOT NULL` scans
nothing and measures ~0.03s → SAFE; the same statement against a production
column with real nulls to prove would scan every row under `ACCESS EXCLUSIVE`.
The number is not wrong — it answers a question about the twin's data, not
yours. **Disposition: owned. The moat holds** because the verdict is still a
real measurement, never a fabricated SAFE — but it can be a real measurement of
the wrong table. Roadmap: detect the degenerate case (a shape classified
O(rows) that measures under the resolution floor with a trivial row-touch count)
and attach a `degenerate_measurement` qualifier — "measured near-zero because
the twin had no work to do here; bring your own data to rehearse the real
scan." This is the single strongest argument for [BYOD](BYOD.md), and the lab
should say so at the moment it happens rather than only in this document.

## A bring-your-own-schema twin is a capped sample

`schema_ddl` runs against a disposable copy of your table, but the seed is
capped at **5,000,000 rows** for the shared lab no matter how large a
`row_count` you state. When your stated size exceeds the cap, the twin is built
with the capped sample and everything above it is **projection, not
measurement** — the run note says the twin was built with N rows and the rest
is projection, and a SAFE there is a SAFE *on the sample*. Gate on
`effective_verdict`, which carries the projection onto your stated size.

## `explanation` vs `shape_note` on a run that measured nothing

An `ERRORED`, `INCONCLUSIVE`, or `SKIPPED` result measured nothing — the
statement never executed, was aborted before it finished, or never ran because
an earlier step failed. For those, `explanation` states only that fact, and the
general behaviour of the shape is moved to a separate **`shape_note`** field.
The two are kept apart on purpose: the shape text reads like a finding, and an
agent scanning `explanation` must never be able to quote it as *this run's*
measurement.

## Empty `lock_modes` does not mean "no lock"

Brace samples `pg_locks` every 250ms and reports in `lock_modes` only the
**granted, write-blocking** locks it caught on **pre-existing** tables. Two real
cases leave `lock_modes` empty even though a lock was central to the freeze: a
metadata change that holds `ACCESS EXCLUSIVE` for less than one sample interval,
and a run (a hostile `ADD COLUMN`) that spends its whole window **waiting** for
an ungranted lock — which the granted-only filter deliberately excludes. The
monitor separately captures the mode the migration was seen holding *or*
awaiting (`monitor.migration_lock_mode_requested`), and the run note names it so
an empty `lock_modes` is never misread as "took no locks". It is evidence of the
migration's lock intent, not proof of a held lock, and is never folded into
`lock_modes`.

## A sequence's top-level projection is the worst of its steps

For a multi-statement sequence, the top-level `scale_projection` is the
**worst-of** the per-statement projections — the one driving the top-level
`effective_verdict` — rather than an all-zeros stub. When no step could be
projected (every step catalog-only or `CONCURRENTLY`), the top-level projection
carries the real twin/target sizes and model with an empty `verdict`
(`Known() == false`), which is the honest "no projection made", never a
fabricated zero.
