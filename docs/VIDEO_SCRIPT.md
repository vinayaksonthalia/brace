# Brace — narrated video script (v3, full spoken script + delivery cues)

**This is a talk-to-camera-via-screen script.** Everything you SAY is in **bold quotes**. `[CUES]` are what's on screen or how to deliver. `[PAUSE]` = stop talking, let the picture breathe. Target **2:15–2:45**. Record the voice-over LAST, to the locked picture, in one calm take.

**Setup before recording**
- Live lab open: https://web-2a61.prg1.zerops.app — **pre-warm a twin** (run one throwaway migration ~1 min before) so nothing says "WARMING" on camera.
- Dark mode, zoomed so the write counter + casualty number read on a phone.
- A terminal with Claude Code connected to the remote MCP:
  `claude mcp add --transport http brace https://mcp-2a61-8080.prg1.zerops.app/mcp`
- Everything else closed. Full-screen.

---

## THE SCRIPT

### 0:00–0:09 · Cold open — the hook (curiosity, not a clock)
`[CUE: the live lab, write counter ticking up ~3,000/sec, chart green and calm. No logo, no title.]`

> **"You can't see a database migration hurt your users."**

`[PAUSE — 1 beat]`

> **"It happens inside a lock you can't see — and the writes it kills don't throw an error you'll notice. They just… wait. And then they're gone."**

`[deliver this low and even, like you're telling a secret]`

### 0:09–0:16 · Who you are + what this is
`[CUE: stay on the live counter.]`

> **"Hey — I'm Vinayak Sonthalia, and this is Brace: a crash-test lab for Postgres migrations. Instead of guessing whether a migration is safe, it lets you watch one happen — to a crash-test dummy, before it touches your users."**

### 0:16–0:24 · The setup for the kill
`[CUE: cursor rests on the prefilled `CREATE INDEX` statement. Don't click yet.]`

> **"This is a real database under real write load. Every tick is a user — a checkout, a login, an add-to-cart. And this migration"** `[hover the statement]` **"passed review, and passed staging. Watch what it does."**

### 0:24–0:44 · The kill — HOLD on the crater
`[CUE: click RUN. Counter craters to 0, the line turns RED, the freeze timer counts up, the red "denied" area fills.]`

> **"A plain CREATE INDEX takes a lock that blocks every single write. Reads still work — so your dashboards stay green…"**

`[PAUSE — let the red crater sit. 2 full beats. Do NOT cut away. This silence is the most important 3 seconds in the video.]`

> **"…while nothing saves."**

### 0:44–0:57 · The number — measured first, checkout last
`[CUE: the casualty card resolves. Reveal one line at a time.]`

> **"Twenty-eight seconds of total write freeze — measured, not guessed."** `[beat]` **"Four hundred and thirty-two user actions denied."** `[beat]` **"Two hundred add-to-carts. A hundred and forty-four… checkouts."**

`[PAUSE — 1 beat of silence on "checkouts." Let it sting.]`

### 0:57–1:15 · The save — one word
`[CUE: paste `CONCURRENTLY` into the same statement. Highlight that ONE word is the only change. Click RUN. Hard cut from the red crater straight to a FLAT GREEN line.]`

> **"Now — one word. CONCURRENTLY."** `[beat]` **"Same index. Same data. But this lock blocks nothing."**

`[CUE: text on screen: `0s blocked · 0 denied`]`

> **"Zero seconds blocked. Zero writes killed. Same result — one word apart. That contrast is the whole product."**

### 1:15–1:31 · It refuses to lie
`[CUE: type `ALTER TABLE orders ADD COLUMN promo int` → SAFE, but with the "acquisition not simulated" chip. Then a migration against a table that doesn't exist → ERRORED.]`

> **"And here's the part I care about most: it refuses to lie. A change that only looks safe because nothing happened to be holding the table — it tells you that."** `[beat]` **"A migration against a table that doesn't exist? That's an error. Never a green light. Brace never says 'safe' without an actual measurement behind it."**

### 1:31–1:46 · Real migrations are scripts (sequences)
`[CUE: open the "run a multi-step migration" panel; paste `ADD COLUMN` then `CREATE INDEX` on that new column. Run. Per-statement card: step 1 SAFE, step 2 UNSAFE, worst-of overall.]`

> **"Real migrations aren't one statement — they're a script. So Brace runs the whole thing on one database, in order, and tells you exactly which step is the killer."** `[point at step 2]` **"Step one's fine. Step two is the one that freezes everything."**

### 1:46–2:00 · Hostile mode + your own schema
`[CUE: tick HOSTILE MODE on the fast `ADD COLUMN` → it now measures the ~2s acquisition freeze. Then briefly show the "rehearse your own schema" panel with a pasted CREATE TABLE.]`

> **"Some migrations only hurt under pressure. Hostile mode holds a real transaction open, so the wait to grab the lock gets measured — not assumed. And you can crash-test your own table shape, not just my demo's."**

### 2:00–2:22 · Connect it to your coding agent (the Zerops beat)
`[CUE: cut to the terminal. In Claude Code, type: "use brace to check `CREATE INDEX idx_orders_email ON orders (email)` before I ship it." The agent calls the MCP, returns UNSAFE + casualties + the CONCURRENTLY rewrite, and refuses.]`

> **"And this whole thing is a Model Context Protocol server, hosted on Zerops. So any coding agent — Claude Code, Cursor — connects by URL and gates its own migrations."** `[the agent responds UNSAFE]` **"The agent that wrote the migration just asked 'will this hurt?' — and refused to ship it."**

### 2:22–2:40 · Why Zerops — show the dashboard
`[CUE: cut to your Zerops project dashboard — the services list (web · api · mcp · db · zcp) and the topology. Let them see it's a real, running project.]`

> **"And this only works because of Zerops. Brace's whole trick is cloning a real Postgres on every run and throwing it away — and almost no managed database lets you do that. Zerops gives me a Postgres I actually own."** `[gesture at the services]` **"Four services on a private network — the Go engine, the static dashboard, the Python MCP endpoint, and the database on its own dedicated cores — stood up from one import file. The database never touches the public internet; the whole thing runs for pennies a day."**

### 2:36–end · Close on the reframe
`[CUE: back to the calm, green, healthy lab. Brace wordmark fades in.]`

> **"Every other tool tells you a migration takes a lock."** `[beat]` **"Brace tells you who it would have killed."**

`[PAUSE. Hold on the wordmark 1.5s. Cut to black.]`

---

## Delivery notes
- **Pace slow.** You're explaining something scary, not selling. The crater and the number carry the energy — you don't have to.
- **The three silences are non-negotiable:** on the red crater, after "checkouts," and on the final wordmark. They're where it lands.
- Real screen capture only — no slides. One motion per cut. The red→green cut is the only effect.
- Record room-tone-free VO to the locked picture, one calm take.

## The four lines to nail (rehearse until reflexive)
1. **"…while nothing saves."** (after the 2-beat silence on the crater)
2. **"A hundred and forty-four… checkouts."** (then silence)
3. **"Brace never says 'safe' without an actual measurement behind it."**
4. **"Every other tool tells you a migration takes a lock. Brace tells you who it would have killed."**

## Optional B-roll (only if you have time; never the climax)
- The scale projection escalating a twin-SAFE to UNSAFE at 500M rows (labeled a projection).
- The docs' "what we got wrong — and fixed" / honest-limits section (shows a judge you know exactly where the tool stops).

## How I used Zerops — talking-points bank (all TRUE; pull what you need)
Say these in your own words over the dashboard/topology. Every one is accurate:

- **A Postgres I own, not a managed slice.** Brace clones a database on *every run* with `CREATE DATABASE … TEMPLATE` and drops it with `DROP DATABASE … WITH (FORCE)`. That needs superuser + native template cloning + direct `pg_locks` reads — which managed Postgres forbids. Zerops hands you a real PostgreSQL 16 you own. **This is the one thing that makes the twin mechanism possible.**
- **Four services from one `import.yml`, on a private network.** `web` (static dashboard) · `api` (Go engine) · `mcp` (Python MCP endpoint) · `db` (PostgreSQL 16). They talk over Zerops' private network — the `mcp` service reaches the engine at `http://api:8080`, and the database's superuser DSN never leaves the project. **The private network is the security model**, not just convenience.
- **Dedicated vCPU on the database.** The one place I deliberately spend: 2–3 dedicated cores on `db`, so `CREATE INDEX CONCURRENTLY`'s two heap scans don't starve the write load — that's what keeps the "safe" run *flat* under the same traffic that craters the naive one. The whole naive-vs-safe contrast depends on it.
- **`zcli push --serviceId … --setup <name>` per service** made the fix → deploy → verify loop tight enough to iterate all weekend. One trap I hit and can mention: the engine and the MCP are *separate* services, so an engine fix isn't live for agents until both `api` and `mcp` redeploy.
- **The remote MCP is hosted on Zerops** with subdomain access — any coding agent connects by URL (`claude mcp add --transport http brace https://…/mcp`), nothing cloned or installed. That's the "infrastructure for coding agents" story, made real.
- **Cheap, disposable, real infra.** The project runs for a couple of dollars a day (the dashboard shows ~$0.58 / 24h at times) — real Postgres you can throw away thousands of times, which is exactly what a crash-test lab needs.
- **(Optional to mention)** a `zcp` (Zerops Control Plane) service is in the project for control-plane/CLI automation — don't overclaim it if it's idle; the four above are the load-bearing ones.
- **Not lock-in:** it's standard Go + Python + PostgreSQL 16 with no vendor APIs, so it runs anywhere those do. Zerops is where it's *easy*, not where it's *trapped* — one YAML anyone can `import`.
