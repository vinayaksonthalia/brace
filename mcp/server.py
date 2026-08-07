"""Brace MCP server — the crash-test verb for coding agents.

Agents write migrations in seconds; this tool lets them find out — before
deploying — whether that migration would freeze production. It calls a Brace
lab (https://github.com/vinayaksonthalia/brace), which rehearses the SQL on a
production-scale Postgres twin under live labeled write traffic and measures
the damage.

Run (stdio, for Claude Code / any MCP client):
    fastmcp run mcp/server.py            # or: python mcp/server.py
Config:
    BRACE_API — lab base URL (defaults to the public lab)
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
def check_migration(sql: str, wait_seconds: int = 240) -> dict:
    """Crash-test a Postgres migration on a production-scale twin under live
    write traffic. Returns verdict (SAFE / UNSAFE / INCONCLUSIVE), the measured
    write-freeze duration, the count of writes that actually died (by label),
    the lock modes taken, an explanation, and — when UNSAFE — the safe rewrite
    to use instead. Nothing real is at risk: the twin is disposable."""
    r = requests.post(f"{API}/api/run", json={"sql": sql, "mode": "custom"}, timeout=30)
    body = r.json()
    if r.status_code == 400:
        if body.get("kind") == "already_safe":
            return {"verdict": "SAFE", "statically_safe": True, "reason": body.get("reason")}
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
            return {
                k: lr.get(k)
                for k in (
                    "verdict", "blocked_seconds", "duration_seconds", "casualties",
                    "baseline_writes_per_sec", "lock_modes", "explanation",
                    "suggestion", "error", "invalid_indexes",
                )
            }
    return {"verdict": "TIMEOUT", "reason": f"crash test still running after {wait_seconds}s"}


@mcp.tool
def lab_status() -> dict:
    """Current lab status: warm crash-test twins, template size, whether a run
    is in progress."""
    return requests.get(f"{API}/api/status", timeout=15).json()


if __name__ == "__main__":
    mcp.run()
