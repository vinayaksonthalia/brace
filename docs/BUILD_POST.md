# Brace — a crash-test lab for Postgres migrations

*Built for the WeMakeDevs × Zerops hackathon.*

> Every other tool tells you a migration **takes a lock.**
> Brace tells you **who it would have killed.**

**Live lab:** https://web-2a61.prg1.zerops.app · **Repo:** https://github.com/vinayaksonthalia/brace · **Docs:** https://web-2a61.prg1.zerops.app/docs.html

---

## The problem

A schema migration passes review. It passes staging. Staging has no traffic, so the migration looks instant. You ship it at 2pm.

In production the same statement takes a lock that conflicts with every `INSERT`, `UPDATE`, and `DELETE`. For as long as the index builds, **every write queues.** Reads still work, so your dashboards stay green — but nothing saves. Checkout spins. Logins hang. Twelve seconds later the index is ready and the incident channel is on fire.

The knowledge to prevent this exists — it's in a dozen blog posts about `lock_timeout` and `CREATE INDEX CONCURRENTLY`. But knowing *"this takes a ShareLock"* is not the same as *feeling* that this takes checkout down for 28 seconds and kills 432 orders. **Lock theory doesn't reach people. A body count does.**

## What Brace does

Brace is a lab that runs your migration against a **real, disposable clone of a database under real write load**, and reports the blast radius in human terms.

1. You paste a migration — `CREATE INDEX`, `ALTER TABLE`, or a multi-step script.
2. Brace clones a throwaway twin database, points a labeled load generator at it (`checkout`, `add_to_cart`, `login` writes at thousands/sec), and runs your migration on the twin.
3. A lock monitor watches `pg_locks` and times exactly how long writes were frozen.
4. You get a verdict: **SAFE** or **UNSAFE**, the total **write-freeze duration**, and the **casualties** — how many real user actions were denied, by type.
5. The twin is destroyed. Your real database was never touched.

Then you add one word — `CONCURRENTLY` — and run it again. **0s blocked. 0 denied.** You *feel* the fix.

The end state of both runs is identical (the same index exists), so the result can't be dismissed as "you wrote it wrong." The only difference is the lock — which is the entire point.

## A real result, from the live lab

`CREATE INDEX idx_orders_promo ON orders (promo)` against a twin under ~3,300 writes/sec:

| | Value |
|---|---|
| Verdict | **UNSAFE** |
| Lock taken | `ShareLock` (blocks writes) |
| Total write freeze | **28.0 s** |
| Writes denied | **432** — 206 add-to-cart, 144 checkout, 82 login |

Add `CONCURRENTLY`: `ShareUpdateExclusiveLock`, **0 s blocked, 0 denied.** Same index. Same data. One word.

## What makes it trustworthy, not a toy

I built this to be something a real DB engineer would believe, so the honest engineering got as much attention as the demo:

- **Measured floor, honest projection.** The casualty count is *measured* on the rig. A separate, explicitly-labeled projection scales the measured freeze onto production row-counts. The script never says "failed" when it means "would be denied."
- **The role is the boundary, not a regex.** Your SQL runs as `brace_migrator` — a `NOSUPERUSER NOCREATEDB` role. Load runs as `brace_traffic`. An allowlist is a convenience on top; the privilege floor is what actually contains a hostile migration. `pg_read_file()` and friends fail with *permission denied*, by construction.
- **It refuses to lie.** A migration against a table that doesn't exist returns **ERRORED**, never SAFE. A statement that fails mid-run reports honestly and the twin is still destroyed.
- **Non-blocking shapes don't get a phantom freeze.** The monitor's observed lock mode is the arbiter — a catalog-only `ADD COLUMN … DEFAULT <const>` is correctly called SAFE, not scared-up into a fake outage.
- **Hostile mode.** A fast DDL can still cause an outage in production if it *queues behind a long transaction*. Toggle hostile mode and Brace holds a competing transaction so the migration has to wait for its lock — measuring the acquisition stall the happy path can't see.

## Beyond the demo — it's a product

- **Multi-statement sequences.** Paste an ordered script (`ADD COLUMN`, then `CREATE INDEX` on the new column). Brace runs it on one twin so each step sees the last step's schema, and reports a per-statement verdict plus the worst-of overall — pinpointing *which* step is the killer.
- **Bring your own data (self-host).** Point Brace at a clone of your own database with one Zerops import YAML. Your data never leaves your infrastructure.
- **Bring your own schema (hosted).** Not ready to self-host? Paste a `CREATE TABLE` into the public lab; Brace provisions an isolated throwaway table, seeds it type-aware, and rehearses against it.
- **Remote MCP — connect any coding agent.** Brace is a Model Context Protocol server. Your agent can gate its own migrations:
  ```bash
  claude mcp add --transport http brace https://mcp-2a61-8080.prg1.zerops.app/mcp
  ```
  Tools: `check_migration` (sql or a `statements[]` script, with `hostile`, `target_rows`, `deadline_ms`), `get_result` for long runs, `lab_status`. It degrades gracefully — a rate-limited or still-warming lab returns a typed status with a retry hint, not a raw 500.

## Why Zerops

Brace *is* a Zerops-shaped application — it needs cheap, disposable, real Postgres, a private network, and one-command deploys, and Zerops gives all three:

- **Four services from one import YAML:** `api` (Go engine), `web` (static dashboard), `mcp` (Python MCP endpoint), `db` (**PostgreSQL 16, dedicated vCPU**). The dedicated core is what keeps the safe run *flat* under the same load that craters the naive run — the whole contrast depends on it.
- **The private network is the security model.** The `mcp` and `web` services reach the engine at `http://api:8080` over the internal network; the database is never publicly exposed.
- **Twin databases are the core primitive, and they're free.** `CREATE DATABASE … TEMPLATE` clones a seeded twin in under a second; `DROP DATABASE WITH (FORCE)` throws it away. Brace keeps a warm pool and sweeps orphans on startup.
- **`zcli push --serviceId` per service** made the fix→deploy→verify loop tight enough to iterate all night.

The entire thing is one YAML anyone can `import` to run their own private instance.

## What was hard

- **Making the number honest AND punchy.** A blocked connection waits `lock_timeout`, fails, retries — so the raw hard-fail count is capped by the connection pool, not by the outage. The fix was to lead with the *measured freeze duration* (undeniable) and label the production-scale number as an explicit projection.
- **The proxy downgraded my MCP POSTs.** The platform proxy turned `POST /mcp/` into a `307 → GET /mcp` and broke the handshake. An ASGI path-normalizing wrapper serving both `/mcp` and `/mcp/` directly fixed it.
- **Verifying through the endpoint judges use.** A fix to the engine isn't live for an agent until *both* the `api` and the separate `mcp` service are redeployed. Every fix this weekend was verified through the public MCP and the live URL — not just against the internal API.

## Try it

- **Lab:** https://web-2a61.prg1.zerops.app — a naive migration is prefilled. Run it, watch the crater, add `CONCURRENTLY`, run it again.
- **Type your own.** Any locking `CREATE INDEX` / `ALTER TABLE` craters; it isn't hardcoded.
- **Connect your agent:** `claude mcp add --transport http brace https://mcp-2a61-8080.prg1.zerops.app/mcp`
- **Docs:** https://web-2a61.prg1.zerops.app/docs.html
