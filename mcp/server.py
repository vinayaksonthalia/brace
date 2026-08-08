"""Brace MCP server — the crash-test verb for coding agents.

Agents write migrations in seconds; this tool lets them find out — before
deploying — whether that migration would freeze production. It calls a Brace
lab (https://github.com/vinayaksonthalia/brace), which rehearses the SQL on a
production-scale Postgres twin under live labeled write traffic and measures
the damage.

Two ways to run it:

  Local (stdio) — clone the repo, then point any MCP client at this file:
      python mcp/server.py
      claude mcp add brace -- python /path/to/mcp/server.py

  Remote (HTTP) — no clone required; an agent connects to a hosted URL:
      MCP_TRANSPORT=http PORT=8080 python mcp/server.py
      claude mcp add --transport http brace https://<host>/mcp/

Config (env):
    BRACE_API      — lab base URL (defaults to the public lab)
    MCP_TRANSPORT  — "stdio" (default) or "http"
    PORT           — HTTP port when MCP_TRANSPORT=http (default 8080)
"""

import os
import time

import requests
from fastmcp import FastMCP

API = os.environ.get("BRACE_API", "https://api-2a61-8080.prg1.zerops.app")

mcp = FastMCP(
    "brace",
    instructions=(
        "Crash-test lab for Postgres migrations. Before deploying any schema "
        "migration (CREATE INDEX, ALTER TABLE, ...), call check_migration to "
        "learn whether it would block production writes, for how long, and "
        "how many writes would die. UNSAFE verdicts include the safe rewrite."
    ),
)


@mcp.tool
def check_migration(
    sql: str, target_rows: int = 0, hostile: bool = False, wait_seconds: int = 240
) -> dict:
    """Crash-test a Postgres migration on a production-scale twin under live
    write traffic. Returns effective_verdict (SAFE / UNSAFE / INCONCLUSIVE /
    ERRORED) — the field to gate on — plus the measured write-freeze duration,
    the count of writes that actually died (by label), the lock modes taken, an
    explanation, and, when UNSAFE, the safe rewrite to use instead. Nothing real
    is at risk: the twin is disposable.

    target_rows: the row count of the table you will actually run this against.
    The twin is a sample, and a freeze scales with table size — pass your real
    number and the projection escalates a SAFE measurement to UNSAFE when it
    would not be safe at your scale. Leave it 0 to skip projection.

    hostile: run the migration against a deliberately contended table — while it
    runs, the lab holds a transaction open on the target so the migration has to
    QUEUE for its lock. Use this to measure the acquisition-wait risk of a shape
    that a default run reports SAFE with qualifier "acquisition_not_simulated"
    (DROP INDEX, DROP COLUMN, and other fast ACCESS EXCLUSIVE changes get that
    lock instantly on an idle twin, so a plain run never tests the wait that
    actually causes their production outages).

    ALWAYS gate on effective_verdict, never on verdict: verdict is only what was
    measured on the twin, while effective_verdict also accounts for the
    projection onto target_rows. A migration can be SAFE here and UNSAFE for you.

    Read the qualifier and note too: a SAFE with qualifier
    "acquisition_not_simulated" or "contention_not_engaged" is only half the
    answer — re-run with hostile=true to measure the rest. contention reports
    whether hostile mode actually engaged.
    """
    payload = {"sql": sql, "mode": "custom"}
    if target_rows > 0:
        payload["target_rows"] = target_rows
    if hostile:
        payload["hostile"] = True
    r = requests.post(f"{API}/api/run", json=payload, timeout=30)
    body = r.json()
    if r.status_code == 400:
        if body.get("kind") == "already_safe":
            result = {
                "effective_verdict": "SAFE",
                "verdict": "SAFE",
                "statically_safe": True,
                "reason": body.get("reason"),
            }
            # A metadata-only shape can still take ACCESS EXCLUSIVE (ADD COLUMN with a
            # constant default). It is answered already_safe WITHOUT running, but its
            # acquisition wait — behind a long-running transaction, every query queues
            # behind it — was never simulated. Carry the qualifier + note through so an
            # agent gating on `acquisition_not_simulated` does not read this as an
            # unconditional clean SAFE and ship a change that freezes writes on acquire.
            if body.get("qualifier"):
                result["qualifier"] = body["qualifier"]
            if body.get("note"):
                result["note"] = body["note"]
            return result
        return {"verdict": "REJECTED", "reason": body.get("reason"), "suggestion": body.get("suggestion")}
    if r.status_code == 409:
        return {"verdict": "BUSY", "reason": "another crash test is running; retry shortly"}
    if r.status_code == 503:
        return {"verdict": "WARMING", "reason": f"no warm twin; next in ~{body.get('eta_seconds', 60):.0f}s"}
    r.raise_for_status()
    run_id = body["run_id"]

    deadline = time.time() + wait_seconds
    while time.time() < deadline:
        time.sleep(3)
        stats = requests.get(f"{API}/api/stats", timeout=15).json()
        lr = stats.get("last_run")
        if lr and lr.get("run_id") == run_id:
            result = {
                k: lr.get(k)
                for k in (
                    # effective_verdict comes first because it is the one to gate
                    # on: it is verdict escalated by the projection onto
                    # target_rows, so it can be UNSAFE where verdict is SAFE.
                    "effective_verdict", "verdict", "scale_projection",
                    "scale_warning", "blocked_seconds", "duration_seconds",
                    "casualties", "errored_total", "baseline_writes_per_sec",
                    "lock_modes", "explanation", "suggestion", "error",
                    "invalid_indexes",
                    # The honesty layer: what this run did NOT establish (qualifier),
                    # the one-line caveat (note), and whether hostile contention
                    # actually engaged (contention). Without these an agent reads an
                    # unqualified SAFE for a shape whose acquisition wait was never
                    # simulated.
                    "qualifier", "note", "contention",
                )
            }
            # A run that ERRORED (the statement never executed) or came back
            # INCONCLUSIVE (aborted before it finished) measured nothing. It must not
            # carry a scale_projection or a scale_warning — those would be a
            # measured-sounding claim about a statement that never ran. The engine
            # already frames the explanation as "general behaviour, not a measurement";
            # drop the projection fields here so nothing downstream reads them as real.
            if result.get("verdict") in ("ERRORED", "INCONCLUSIVE") or \
                    result.get("effective_verdict") in ("ERRORED", "INCONCLUSIVE"):
                result.pop("scale_projection", None)
                result.pop("scale_warning", None)
            return result
    return {"verdict": "TIMEOUT", "reason": f"crash test still running after {wait_seconds}s"}


@mcp.tool
def lab_status() -> dict:
    """Current lab status: warm crash-test twins, template size, whether a run
    is in progress."""
    return requests.get(f"{API}/api/status", timeout=15).json()


if __name__ == "__main__":
    # stdio by default (local Claude Code / any MCP client). When deployed as a
    # hosted service, MCP_TRANSPORT=http serves streamable HTTP at /mcp/ so an
    # agent can connect by URL with nothing installed.
    if os.environ.get("MCP_TRANSPORT", "stdio").lower() == "http":
        import uvicorn

        app = mcp.http_app(path="/mcp")

        # Serve /mcp AND /mcp/ identically. Starlette would 307-redirect the
        # trailing-slash form, and through the Zerops proxy that redirect
        # downgrades the POST to a GET (→ 400), so an agent that adds a slash
        # would fail confusingly. Normalising the path in front of the app
        # removes the redirect entirely — both forms just work. lifespan and
        # every other scope pass straight through untouched.
        async def normalize_slash(scope, receive, send):
            if scope.get("type") == "http" and scope.get("path") == "/mcp/":
                scope = dict(scope, path="/mcp", raw_path=b"/mcp")
            await app(scope, receive, send)

        uvicorn.run(normalize_slash, host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
    else:
        mcp.run()
