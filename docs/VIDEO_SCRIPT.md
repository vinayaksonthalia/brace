# Brace — video script & shot list

**Target: 90–110s.** Real screen capture only, no slideware. One motion per cut. The red→green transition is the only "effect." Record the voice-over **last**, to the locked picture.

**Before recording — set the stage:**
- Lab open at https://web-2a61.prg1.zerops.app, twin pre-warmed (run one throwaway migration a minute before so the pool is hot — no "WARMING" on camera).
- Browser in dark mode, zoom so the live counter and the casualty number are readable **on a phone**.
- Have the naive `CREATE INDEX` prefilled; have `CONCURRENTLY` ready to paste for the save.
- Close every other tab, notification, and dock badge. Full-screen the browser.

---

## Shot list

### 0:00–0:06 — Cold open. No logo, no title card.
**On screen:** the live lab, write counter already ticking up fast (~3,000/sec), chart green and healthy.
**Text overlay (one line):** `This database is handling thousands of writes a second.`
**VO:** *"This is a database under real load. Every one of these is a user — a checkout, a login, an add-to-cart."*
*(Establish the height it falls from. The drop only lands if the height is felt.)*

### 0:06–0:13 — The hook.
**On screen:** cursor rests on the prefilled `CREATE INDEX` statement; hand hovers before running.
**VO:** *"This migration passed review. It passed staging. And it's about to take checkout down for twenty-eight seconds. Watch."*

### 0:13–0:32 — The kill. **Hold on the crater.**
**On screen:** click **Run**. The write counter craters to 0. The chart line drops and turns **red**. A timer counts the blocked seconds upward. The red "denied" area fills as it goes.
**VO (then silence):** *"That's a plain CREATE INDEX. It takes a lock that blocks every write. Reads still work — so your dashboards stay green while nothing saves."*
**⚠️ Do NOT cut away.** Let the red crater sit half a beat too long. This is the single most important 3 seconds in the video.

### 0:32–0:44 — The number reveal. Measured first.
**On screen:** the casualty card resolves. Reveal one line at a time, **checkout last**.
**VO:** *"Twenty-eight seconds of total write freeze — measured. Four hundred and thirty-two user actions denied. Two hundred add-to-carts. A hundred and forty-four checkouts."*
*(Emotional apex. Give the last number a beat of silence. The freeze duration and casualty count are measured on the rig; if you show the production-scale number, say the word "projected.")*

### 0:44–1:05 — The save. The visual rhyme.
**On screen:** paste `CONCURRENTLY` into the same statement — highlight that **one word** is the only change. Click Run. Hard cut (or split-screen) from the red crater to a **flat green line**. Counter never dips.
**VO:** *"Now add one word. CONCURRENTLY. Same index, same data — but this lock blocks nothing."*
**Text overlay:** `0s blocked · 0 denied.`
*(The red→green cut makes the point wordlessly. Let it breathe.)*

### 1:05–1:18 — Un-fakeable + it's a real tool.
**On screen:** clear the box, type a *different* locking migration live (e.g. `ALTER TABLE orders ADD COLUMN big text NOT NULL DEFAULT repeat('x',100)` or another `CREATE INDEX`). It craters too.
**VO:** *"It's not a canned demo. Type your own migration — if it locks, Brace catches it. Your SQL runs as an unprivileged role against a throwaway clone. Your real database is never touched."*

### 1:18–1:32 — Why Zerops. One breath, topology on screen.
**On screen:** a simple diagram or the Zerops project view — 4 services (api, web, mcp, db), the twin spun from a template over the private network, then thrown away.
**VO:** *"Every twin is a real Postgres database, cloned in under a second on Zerops and destroyed after. Four services, one import file — anyone can run their own private instance."*

### 1:32–end — Close on the reframe, not the tech.
**On screen:** back to the lab, healthy green counter; Brace wordmark fades in.
**VO:** *"Every other tool tells you a migration takes a lock. Brace tells you who it would have killed."*

---

## Craft rules
- Real screen capture, no slide-ware. One motion per cut.
- The counter font is big enough to read on a phone.
- The red→green transition is the only visual effect.
- Record room-tone-free VO **last**, to the locked picture.
- If a live run risks WARMING/rate-limit on camera, record the two runs separately and cut together — never show a spinner as the climax.

## The three lines that must land (rehearse these until reflexive)
1. *"…about to take checkout down for twenty-eight seconds. Watch."*
2. *"Four hundred and thirty-two user actions denied."* (checkout last, then silence)
3. *"Every other tool tells you a migration takes a lock. Brace tells you who it would have killed."*

## Optional B-roll (only if time; never as the climax)
- An agent calling the MCP: `claude mcp add --transport http brace https://mcp-2a61-8080.prg1.zerops.app/mcp`, then "check this migration" → SAFE/UNSAFE inline. Shows the same verdict reaching any coding agent.
- A multi-statement sequence resolving to a per-statement verdict, pinpointing the one dangerous step.
