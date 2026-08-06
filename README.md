# Brace

**The crash-test lab for Postgres migrations.**

Cars stopped killing people when the industry started crashing them into walls on purpose — full-scale replicas, instrumented, expendable — before a single customer sat in the seat. Software still ships database migrations the old way: straight into live traffic, with your users in the passenger seat.

Brace builds a production-scale twin of your database, puts real write traffic in the seats, slams your migration into it, and shows you — live — how long everything froze and how many orders died. Then it proves the safe rewrite of the same migration hurts nobody.

> *Linters read the car's blueprint and guess. Brace crashes the car.*

## Status

🚧 Building — Zerops Challenge, Aug 2026.

## How it works (the 60-second version)

1. **The dummy** — a full-size clone of the database, created in seconds from a seeded template (`CREATE DATABASE … TEMPLATE`), pre-warmed in the background so you never wait.
2. **The passengers** — a load generator fires labeled write traffic (checkouts, logins, cart-adds) at the twin, with the request timeouts every real production stack has.
3. **The crash** — your migration runs against the twin *while the traffic is live*. If it takes a blocking lock, writes freeze — and the ones that wait too long **die**, exactly as they would in production.
4. **The report** — measured freeze duration, real failed-write count, and the projected user damage at your traffic level (formula shown, nothing hidden).
5. **The save** — run the safe rewrite; same end state, zero casualties. That contrast is the whole product.

Everything is thrown away after each run. Nothing real is ever at risk.

## Deployed on Zerops

Brace is only possible on infrastructure that hands you a real Postgres superuser, native template cloning, a private network, and one-file self-hosting — which is why it runs on [Zerops](https://zerops.io). Full architecture, self-host recipe, and honest prior-art comparison coming with the build.

## License

MIT
