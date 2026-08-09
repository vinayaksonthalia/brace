# Brace — video script & shot list (v2, full feature showcase)

**Target: 2:00–2:40.** Real screen capture of the live lab + a live agent call. No slideware. One motion per cut. The red→green transition is the only "effect." Record the voice-over **last**, to the locked picture.

**Before recording — set the stage**
- Lab open at https://web-2a61.prg1.zerops.app — **pre-warm a twin** (run one throwaway migration ~1 min before) so no "WARMING" shows on camera.
- Dark mode, browser zoomed so the counter + casualty number are readable on a phone.
- A terminal ready with Claude Code (or Cursor) connected to the remote MCP:
  `claude mcp add --transport http brace https://mcp-2a61-8080.prg1.zerops.app/mcp`
- Close every other tab, notification, dock badge. Full-screen.

---

## Shot list

### 0:00–0:07 — Cold open. No logo, no title.
**On screen:** the live lab, write counter ticking up (~3,000/sec), chart green.
**Text overlay:** `This database is handling thousands of writes a second.`
**VO:** *"This is a database under real load — every tick is a user. A checkout, a login, an add-to-cart."*

### 0:07–0:14 — The hook.
**On screen:** cursor on the prefilled `CREATE INDEX`.
**VO:** *"This migration passed review. It passed staging. And it's about to take checkout down for twenty-eight seconds. Watch."*

### 0:14–0:34 — The kill. Hold on the crater.
**On screen:** click **Run**. Counter craters to 0, line turns **red**, freeze timer counts up, red "denied" area fills.
**VO (then silence):** *"A plain CREATE INDEX takes a lock that blocks every write. Reads still work — so your dashboards stay green while nothing saves."*
**⚠️ Do NOT cut away.** Let the red crater sit half a beat too long.

### 0:34–0:46 — The number. Measured first.
**On screen:** casualty card resolves, one line at a time, **checkout last**.
**VO:** *"Twenty-eight seconds of total write freeze — measured. Four hundred and thirty-two user actions denied. Two hundred add-to-carts. A hundred and forty-four checkouts."* *(beat of silence)*

### 0:46–1:05 — The save. The visual rhyme.
**On screen:** paste `CONCURRENTLY` into the same statement — highlight the one-word change. Run. Hard cut / split-screen from red crater to **flat green line**.
**Text overlay:** `0s blocked · 0 denied.`
**VO:** *"Now add one word. CONCURRENTLY. Same index, same data — this lock blocks nothing."*

### 1:05–1:20 — It won't lie (the honesty beat).
**On screen:** type `ALTER TABLE orders ADD COLUMN promo int` → SAFE, but with the **`acquisition not simulated`** chip. Then a migration on a table that doesn't exist → **ERRORED**, not SAFE.
**VO:** *"And it refuses to lie. A change that only looks safe because nothing was holding the table says so. A migration against a table that doesn't exist is an error — never a green light. It never says 'safe' without a measurement."*

### 1:20–1:35 — Multi-step migrations (sequences).
**On screen:** open the "run a multi-step migration" panel; paste `ADD COLUMN` then `CREATE INDEX` on that new column. Run. The per-statement card: step 1 SAFE, step 2 UNSAFE, **worst-of overall**.
**VO:** *"Real migrations are scripts, not single statements. Brace runs the whole thing on one database and tells you exactly which step is the killer."*

### 1:35–1:50 — Hostile mode + your own schema (fast).
**On screen:** tick **hostile mode** on a fast `ADD COLUMN` → it now measures the ~2s acquisition freeze. Then the "rehearse your own schema" panel with a pasted `CREATE TABLE`.
**VO:** *"Some migrations only hurt under contention — hostile mode holds a transaction open and measures that wait. And you can crash-test your own table shape, not just the demo's."*

### 1:50–2:12 — Connect it to your coding agent (the Zerops beat).
**On screen:** cut to the terminal. In Claude Code: *"use brace to check `CREATE INDEX idx_orders_email ON orders (email)` before I ship it."* The agent calls the MCP, gets back **UNSAFE + casualties + the CONCURRENTLY rewrite**, and refuses to ship it.
**VO:** *"It's a Model Context Protocol server, hosted on Zerops. Any coding agent — Claude Code, Cursor — connects by URL and gates its own migrations. The agent that wrote the migration asks 'will this hurt?' before it ships."*

### 2:12–2:28 — Why Zerops. One breath, topology on screen.
**On screen:** the 4-service diagram / Zerops project — twin cloned from a template over the private network, then dropped.
**VO:** *"Every twin is a real Postgres, cloned in under a second on Zerops and thrown away after. Four services, one import file — anyone can run their own private instance."*

### 2:28–end — Close on the reframe, not the tech.
**On screen:** back to the healthy green lab; Brace wordmark fades in.
**VO:** *"Every other tool tells you a migration takes a lock. Brace tells you who it would have killed."*

---

## Craft rules
- Real screen capture, no slide-ware. One motion per cut. Counter font readable on a phone.
- The red→green transition is the only effect. Record room-tone-free VO last, to the locked picture.
- If a live run risks WARMING/rate-limit on camera, record beats separately and cut together — never show a spinner as the climax.

## The lines that must land (rehearse until reflexive)
1. *"…about to take checkout down for twenty-eight seconds. Watch."*
2. *"Four hundred and thirty-two user actions denied."* (checkout last, then silence)
3. *"It never says 'safe' without a measurement."*
4. *"Every other tool tells you a migration takes a lock. Brace tells you who it would have killed."*

## Optional B-roll (only if time; never the climax)
- The scale projection escalating a twin-SAFE to UNSAFE at 500M rows, labeled a projection.
- The docs site's honest-limits / roadmap section (shows maturity to a judge who clicks through).
