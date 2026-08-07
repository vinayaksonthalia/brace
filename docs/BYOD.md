# Bring your own data

Out of the box Brace seeds a synthetic ten-million-row `orders` table and hammers
it with three INSERT labels. That is a good demo and a weak rehearsal. The
migration you are actually afraid of runs against **your** schema, with **your**
indexes, **your** row widths, under **your** traffic mix — and none of those are
things a generated table can stand in for.

A self-hosted Brace can be pointed at a restored production dump instead, with
two environment variables and no code changes.

---

## The five steps

### 1. Self-host the lab

Import the project (`import.yml`) or deploy the `api` setup from `zerops.yml`.
Brace needs a PostgreSQL 16 service it **owns**: it creates and drops databases
on every run, and it rotates the `brace_migrator` and `brace_traffic` role
passwords on every start. Do not point it at a server anything else depends on.

### 2. Restore your dump as `brace_template`

The template is the thing every disposable twin is cloned from. Its name is not
configurable — the twin pool, the orphan sweep and the boot check all agree on
`brace_template`.

```sh
createdb  -h <db-host> -U <superuser> brace_template
pg_restore -h <db-host> -U <superuser> -d brace_template your-dump.dump
```

You do **not** need to set `ALLOW_CONNECTIONS false`. Brace pins the template to
it on every start, and reopens it only for the moments it needs to look inside.

Trim before you restore if you can. Every warm twin is a full physical copy of
the template, so the template size is both your disk multiplier (`POOL_SIZE`
copies of it, plus the one being used) and your clone time — roughly 80 seconds
per gigabyte on the hardware Brace was measured on. The tables named by your
migration and by your load statements are the ones that have to be realistic;
the rest can be truncated.

### 3. Turn seeding off and describe your traffic

```yaml
envVariables:
  BRACE_SEED: "off"
  BRACE_LOAD_LABELS: >-
    checkout:3:INSERT INTO invoices (customer_id, cents) VALUES ($1, $2);
    login:2:UPDATE sessions SET last_seen = now() WHERE id = $1 % 50 AND $2 > 0
```

**`BRACE_SEED`** — `off` (also `0`, `no`, `false`, `none`, `skip`) skips seeding
entirely; `SEED_ROWS: "0"` means the same thing. Anything Brace does not
recognise is a startup error rather than a silent default, because quietly
appending ten million synthetic orders to a restored production dump is the worst
thing this switch could do. With seeding off, boot **verifies** the template
instead of writing to it: it must exist and it must contain at least one user
table, or the process refuses to start and prints the recipe from step 2.

**`BRACE_LOAD_LABELS`** — semicolon-separated `name:weight:statement` triples.

| Field | Rule |
| --- | --- |
| `name` | Non-empty, no whitespace, unique. It becomes a JSON key in `casualties.by_label` and a series on the dashboard. |
| `weight` | Integer > 0. Relative share of the write traffic. |
| `statement` | One statement. Must use **both `$1` and `$2`**, and nothing higher. May contain colons; may **not** contain a semicolon. |

Only the **first two colons** in an entry are structural, so
`SET seen = now()::timestamptz` survives parsing intact.

The `$1`/`$2` rule is the wire protocol, not a style preference. Every write the
load generator issues binds exactly two bigints — `$1` a customer-id-shaped value
in 1…100000, `$2` an amount-shaped value in 1…50000 — and PostgreSQL rejects a
bind that supplies more parameters than the statement declares:

```
PREPARE p AS INSERT INTO t (id) VALUES ($1); EXECUTE p(1, 2);
ERROR:  wrong number of parameters for prepared statement "p"
DETAIL:  Expected 1 parameters but got 2.
```

So a statement mentioning only `$1` would fail on *every* write. If your second
value is genuinely unused, give it a harmless home: `... WHERE id = $1 AND $2 > 0`.

Unset the variable and you get the built-in `orders` mix back — but note that
with `BRACE_SEED=off` the built-in mix is almost certainly wrong, because it
writes to an `orders` table your dump probably does not have. Brace treats that
as the same mistake and refuses to start, naming the fix. In practice the two
variables always travel together.

### 4. Redeploy the api service

```sh
zcli push --serviceId <api service>
```

Watch the log for the three lines that say BYOD is live:

```
byod.seed_disabled       template=brace_template note="BRACE_SEED=off — using the restored template as-is"
byod.template_verified   template=brace_template tables=37 estimated_rows=41283110 size="3184 MB"
byod.labels_validated    count=2 twin=brace_run_20260807t111139_b169f344
```

That last line is the self-check earning its keep. Before the engine is
published, Brace clones a twin, runs **each** statement of the effective traffic
mix against it exactly once, and throws the twin away. (Effective, not custom:
with `BRACE_SEED=off` the stock mix is checked the same way.) A typo'd table name
fails the boot with the label name, the SQLSTATE and the statement:

```
boot: BRACE_LOAD_LABELS: label "checkout" does not run against this template:
ERROR: relation "invoces" does not exist (SQLSTATE 42P01)
  statement: INSERT INTO invoces (customer_id, cents) VALUES ($1, $2)
```

Without it, that configuration boots perfectly happily, and every rehearsal
reports **zero casualties** — not because the migration is safe, but because no
write ever landed. The number would be a lie, and it would look exactly like good
news. Failing at boot is the only honest option.

### 5. Crash-test your real migrations

Submit the statement you were about to run in production. The casualty counts
now describe writes to your tables, killed waiting for your migration's lock,
under a traffic mix you named.

```
verdict=UNSAFE blocked=9.4s casualties=1841 by_label=map[checkout:1402 topup:439] errored=0
```

Read `errored` every time. It is the bucket for failures Brace could **not**
attribute to a lock, and it is never folded into casualties. On a BYOD run a
climbing `errored` count means a statement is failing for a reason the boot
self-check could not see — a unique violation against real data, a NOT NULL
column your INSERT does not fill, a foreign key with no matching row. Fix the
label; casualties measured alongside a flood of errors are not measuring your
migration.

---

## What runs as what

Brace has exactly one security boundary and it is worth knowing where it sits
before you hand it your production schema.

- **The migration under test** runs as `brace_migrator`: a LOGIN role that is
  `NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS`, with a random
  password generated at startup and held only in process memory. That is what
  makes PostgreSQL itself refuse `COPY … FROM PROGRAM`, server-side file reads
  and `DROP DATABASE`. Inside the twin — and only inside the twin — it is given
  ownership of the public schema's tables, because `ALTER TABLE`, `DROP INDEX`,
  `REINDEX` and `VACUUM` are owner-only in PostgreSQL. The twin is destroyed
  minutes later. It runs with `statement_timeout = 0` and `lock_timeout = 0`, so
  a slow migration is measured rather than truncated.
- **Your `BRACE_LOAD_LABELS` statements** run on the load generator's pool as
  `brace_traffic`: a second LOGIN role that is `NOSUPERUSER NOCREATEDB
  NOCREATEROLE NOREPLICATION NOBYPASSRLS`, with its own random password generated
  at startup and held only in process memory. Inside each twin it is granted the
  least privilege the load needs and nothing more — `USAGE` on schema `public`
  and `SELECT, INSERT, UPDATE` on the existing public tables (never `DELETE`,
  `TRUNCATE`, `CREATE`, or ownership), plus the sequence `USAGE` an INSERT into a
  serial column needs. The pool sets `lock_timeout` (default 1500ms) and
  deliberately no `statement_timeout` — that is what lets a blocked write be
  counted as a casualty (SQLSTATE 55P03) while a merely slow one is not.

  This is a security boundary, not a convenience. A CHECK constraint, trigger,
  column DEFAULT or index expression is evaluated as the session that performs
  the **write**, not the one that installed it — so if the writes ran as a
  superuser (as they once did, over the admin DSN), an allow-listed migration
  that smuggled `pg_read_file()` or `current_setting('is_superuser')` into such
  an expression would fire it as superuser the moment traffic touched a row.
  Running the writes as unprivileged `brace_traffic` makes PostgreSQL itself deny
  those expressions. `brace_migrator` (which owns objects on the twin) is also
  `NOSUPERUSER`, so even a `SECURITY DEFINER` function — which runs as its owner
  — cannot reach superuser either. Both roles being non-superuser is what closes
  both paths. The role name is reported as `traffic_role` on `/status`.

  Your label statements are configuration, not user input: anyone who can set
  `BRACE_LOAD_LABELS` already controls the deployment, and nothing on the public
  HTTP surface can change them.
- **Everything happens on a disposable clone.** Your restored `brace_template` is
  never written to — with `BRACE_SEED=off`, boot only reads its catalog — and
  every run gets a fresh twin that is dropped when the run ends.

Two operational facts that follow from the design: exactly one Brace process may
run against a given PostgreSQL server (a second would rotate the `brace_migrator`
and `brace_traffic` passwords out from under the first), and one rehearsal runs
at a time (two would measure each other's I/O as their own migration's cost).

---

## Troubleshooting

| Symptom | Cause |
| --- | --- |
| `boot: template database is missing` | `BRACE_SEED=off` and no `brace_template`. Step 2. |
| `template … exists but has no user tables` | `pg_restore` ran against a different database, or restored only a schema-less globals file. |
| `BRACE_LOAD_LABELS: bad entry …: the statement uses $1 but not $2` | See step 3 — both parameters are always bound. |
| `bad entry "…": expected three colon-separated fields` | A statement containing a semicolon split one entry into two. Remove it. |
| `label "x" does not run against this template` | The self-check ran the statement and PostgreSQL refused it. The error carries the SQLSTATE. |
| `the default traffic mix: label "checkout" does not run …` | `BRACE_SEED=off` without `BRACE_LOAD_LABELS`: the built-in mix writes to `orders`. Step 3. |
| `no twin became available within 10m to validate BRACE_LOAD_LABELS` | The first clone of a very large template is still running, or it is failing — the message carries the pool's last error. |
| Runs report 0 casualties and a large `errored` | A label fails against real data (constraint, NOT NULL, FK). Boot only proves the statement runs once. |
| Runs are slow to start | Every twin is a physical copy of the template. Trim it, or lower `POOL_SIZE`. |
