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


# The rich fields a finished run carries. Both check_migration and get_result
# return exactly this shape, so an agent never has to learn two schemas.
_RESULT_FIELDS = (
    # effective_verdict comes first because it is the one to gate on: it is
    # verdict escalated by the projection onto target_rows, so it can be UNSAFE
    # where verdict is SAFE.
    "effective_verdict", "verdict", "scale_projection",
    "scale_warning", "blocked_seconds", "duration_seconds",
    "casualties", "errored_total", "baseline_writes_per_sec",
    "lock_modes", "explanation", "shape_note", "suggestion", "error",
    "invalid_indexes",
    # The honesty layer: what this run did NOT establish (qualifier), the
    # one-line caveat (note), and whether hostile contention actually engaged
    # (contention). Without these an agent reads an unqualified SAFE for a shape
    # whose acquisition wait was never simulated.
    "qualifier", "note", "contention",
    # The per-statement breakdown of a SEQUENCE run. Present only when the run was
    # submitted with `statements=[...]`; each element carries that statement's own
    # verdict/blocked_seconds/casualties/lock_modes/explanation, and the top-level
    # verdict/effective_verdict are the worst-of across the sequence.
    "statements",
)

# Default wait budgets are kept BELOW the ~60s tool timeout most MCP clients
# impose, so check_migration always returns something usable — a verdict, or a
# pollable run_id — before the client gives up and the result is lost.
_DEFAULT_WAIT_SECONDS = 45


def _safe_get_json(url: str, timeout: int = 15):
    """GET url and decode JSON, returning None instead of raising on a network
    error or a non-JSON body. Every polling call goes through here so a blip in
    the lab never turns into an unhandled exception out of a tool."""
    try:
        r = requests.get(url, timeout=timeout)
    except requests.RequestException:
        return None
    try:
        return r.json()
    except ValueError:
        return None


def _retry_after_seconds(r, default: int = 20) -> int:
    """Best-effort parse of a Retry-After header (the lab sends a plain integer
    number of seconds), falling back to a body field and then to default."""
    raw = r.headers.get("Retry-After")
    if raw is None:
        try:
            raw = r.json().get("retry_after")
        except ValueError:
            raw = None
    try:
        return max(0, int(float(raw)))
    except (TypeError, ValueError):
        return default


def _result_from_last_run(lr: dict) -> dict:
    """Project a finished run's stats.last_run onto the rich result shape."""
    result = {k: lr.get(k) for k in _RESULT_FIELDS}
    # A run that ERRORED (the statement never executed) or came back INCONCLUSIVE
    # (aborted before it finished) measured nothing. It must not carry a
    # scale_projection or a scale_warning — those would be a measured-sounding
    # claim about a statement that never ran. The engine already frames the
    # explanation as "general behaviour, not a measurement"; drop the projection
    # fields here so nothing downstream reads them as real.
    if result.get("verdict") in ("ERRORED", "INCONCLUSIVE") or \
            result.get("effective_verdict") in ("ERRORED", "INCONCLUSIVE"):
        result.pop("scale_projection", None)
        result.pop("scale_warning", None)
    return result


def _finished_result(run_id: str):
    """Return the rich result for run_id if the lab's most-recent completed run
    IS run_id, else None. Runs are serialised, so a run that just finished is the
    lab's last_run until the next one starts — which is exactly the window in
    which an agent polls for a slow migration."""
    stats = _safe_get_json(f"{API}/api/stats")
    lr = (stats or {}).get("last_run")
    if lr and lr.get("run_id") == run_id:
        return _result_from_last_run(lr)
    return None


def _still_running(run_id: str) -> bool:
    """Whether run_id is the run currently in flight on the lab."""
    status = _safe_get_json(f"{API}/api/status")
    cur = (status or {}).get("current_run") or {}
    return bool((status or {}).get("running")) and cur.get("run_id") == run_id


def _pending(run_id: str, waited: int) -> dict:
    """The recoverable timeout status. It carries the run_id so the result is
    pollable, never lost — the whole point of the get_result companion tool."""
    return {
        "verdict": "PENDING",
        "status": "running",
        "run_id": run_id,
        "reason": (
            f"the crash test did not finish within {waited}s. Slow migrations — "
            "CREATE INDEX CONCURRENTLY, table rewrites, ALTER COLUMN TYPE — "
            "routinely run 40-90s. The run was NOT lost."
        ),
        "next": f'call get_result("{run_id}") in a few seconds to fetch the finished verdict.',
    }


@mcp.tool
def check_migration(
    sql: str = "", target_rows: int = 0, hostile: bool = False,
    schema_ddl: str = "", row_count: int = 0, deadline_ms: int = 0,
    statements: list[str] | None = None,
    wait_seconds: int = _DEFAULT_WAIT_SECONDS,
) -> dict:
    """Crash-test a Postgres migration on a production-scale twin under live
    write traffic, then return the finding. Nothing real is at risk: the twin is
    disposable.

    ALWAYS gate on effective_verdict, never on verdict: verdict is only what was
    measured on the twin, while effective_verdict also accounts for the
    projection onto target_rows. A migration can be SAFE here and UNSAFE for you.

    target_rows: the row count of the table you will actually run this against.
    The twin is a sample, and a freeze scales with table size — pass your real
    number and the projection escalates a SAFE measurement to UNSAFE when it
    would not be safe at your scale. Leaving it 0 does NOT skip the projection:
    the lab falls back to its deployment-configured BRACE_TARGET_ROWS, or — if
    the operator set none — a built-in default ASSUMPTION about your table
    (~100M rows). The projection and whose number it used are always reported in
    scale_projection.target_rows_source ("request" | "deployment" |
    "brace_default_assumption"); a projection built on the default assumption is
    loudly marked in the note so you never read it as a fact about your table.
    Pass your real target_rows to project onto YOUR table instead of the lab's
    default.

    hostile: run the migration against a deliberately contended table — while it
    runs, the lab holds a transaction open on the target so the migration has to
    QUEUE for its lock. Use this to measure the acquisition-wait risk of a shape
    that a default run reports SAFE with qualifier "acquisition_not_simulated"
    (DROP INDEX, DROP COLUMN, and other fast ACCESS EXCLUSIVE changes get that
    lock instantly on an idle twin, so a plain run never tests the wait that
    actually causes their production outages).

    Read the qualifier and note too: a SAFE with qualifier
    "acquisition_not_simulated" or "contention_not_engaged" is only half the
    answer — re-run with hostile=true to measure the rest. contention reports
    whether hostile mode actually engaged.

    scale_projection: the modelled cost at your table size, ALWAYS present when a
    projection was made (see target_rows above). Read target_rows_source to know
    whose number it used, and note its model is a LINEAR-in-rows floor: real
    index-build and table-rewrite cost is super-linear, so the projected freeze is
    a LOWER bound, never an over-estimate. When the target is more than ~20x the
    twin, scale_projection.qualifier is "modeled_beyond_measurement" — treat the
    number as an order of magnitude, not a precise figure. For a SEQUENCE, the
    top-level scale_projection is the worst-of the per-statement projections.

    lock_modes vs shape_note: an EMPTY lock_modes does NOT mean the migration took
    no lock. Brace samples pg_locks every 250ms and reports only GRANTED,
    write-blocking locks on pre-existing tables; a sub-250ms metadata change, or a
    run that spent its window WAITING for its lock (a hostile ADD COLUMN), can
    freeze writes with lock_modes still empty — the note then names the mode the
    monitor saw held-or-awaited. And on an ERRORED / INCONCLUSIVE / SKIPPED result,
    "explanation" states only that this run measured nothing; the general behaviour
    of the shape is in "shape_note", kept separate precisely so it is never quoted
    as this run's finding.

    RETURN CONTRACT — every value this tool (and get_result) can put in the
    top-level "verdict"/"effective_verdict"/"status" fields, so a strict gate is
    never surprised by an undocumented one:

      Measured/derived outcomes (in effective_verdict AND verdict):
        SAFE          writes kept flowing; the change is safe at the tested size.
        UNSAFE        writes froze >1s and/or real writes died. Block the deploy.
                      The safe rewrite is in "suggestion".
        INCONCLUSIVE  the run was aborted before it could finish (e.g. it never
                      acquired its lock within the budget). Not a pass.
        ERRORED       the statement could not execute at all (bad SQL, a
                      NONEXISTENT target relation, permission denied). NEVER safe.

      Static fast-path (no run performed):
        SAFE with "statically_safe": true — a catalog-only shape (ADD COLUMN with
                      a constant default, ANALYZE, COMMENT) proven safe by shape.
                      It still carries "qualifier"/"note" when it takes ACCESS
                      EXCLUSIVE, because its acquisition wait was not simulated.

      Per-statement only (inside the sequence "statements" array, never top-level):
        SKIPPED       a sequence statement that did not run because an earlier
                      statement failed and the migration halted before it. Not a
                      measurement; treat as not-verified.

      Submission/transport statuses (in "verdict" only; NOT a measurement):
        REJECTED      the lab will not run this statement (not a migration, multi
                      statement, too long, or a NONEXISTENT target on a static
                      shape). See "reason"/"suggestion". Treat as not-safe.
        RATE_LIMITED  too many runs; retry after "retry_after_seconds".
        BUSY          another crash test is in progress; retry shortly.
        WARMING       no warm twin yet; retry after "reason"'s ETA.
        PENDING       the run did not finish within wait_seconds but was NOT lost.
                      Carries "run_id"; call get_result(run_id) to fetch it.

    A slow migration therefore never disappears: on PENDING, poll get_result with
    the returned run_id.

    schema_ddl: rehearse against YOUR OWN table instead of the demo's. Pass a
    single CREATE TABLE statement and Brace builds a disposable, isolated database
    from it, seeds a bounded sample of synthetic rows (type-aware), runs sql
    against THAT shape, and drops everything afterwards. row_count is your
    approximate real table size: it drives the scale projection and (clamped) how
    many rows are seeded. Leave schema_ddl empty to use the stock orders table.

    deadline_ms: the per-request lock deadline of YOUR application — the wait after
    which one of your write requests would give up. A casualty is a write that
    waited longer than this for its lock, so this single number IS the definition
    of the casualty count. Pass your real client/HTTP timeout and the casualty
    count reflects YOUR environment, not the lab's; leave it 0 for the lab default
    (1500ms), and note the deadline that produced any count comes back on
    casualties.deadline_ms. Bounded to 30s.

    statements: run a MULTI-STATEMENT migration as an ordered SEQUENCE on ONE twin
    instead of a single statement. Pass a list like
        ["ALTER TABLE orders ADD COLUMN promo int",
         "CREATE INDEX ON orders (promo)"]
    and Brace runs them IN ORDER on one fresh clone, so a later statement sees the
    schema the earlier ones produced (the second statement here would ERROR "column
    does not exist" as a single run, because each single run gets a fresh twin).
    Every statement passes the SAME allow-list a single run does — a sequence cannot
    smuggle a non-allow-listed verb (max 10 statements). The result carries a
    per-statement array ("statements") with each step's own verdict, blocked_seconds,
    casualties, lock_modes and explanation; the top-level verdict/effective_verdict
    are the worst-of, so gating on effective_verdict blocks the deploy if ANY step
    would. When statements is set, sql is ignored. This is how you verify a
    multi-step safe rewrite (ADD COLUMN; add a NOT VALID constraint; VALIDATE; ...)
    as the script it actually is. A SAFE step that takes ACCESS EXCLUSIVE only
    because nothing was holding the table still carries acquisition_not_simulated in
    its per-statement qualifier/note, exactly as a single run would. hostile cannot
    combine with statements (a sequence can't hold one contended lock across the
    whole script) — that request is refused; to measure a step's acquisition wait,
    run that one statement on its own with hostile=true.
    """
    payload: dict = {"mode": "custom"}
    if statements:
        payload["statements"] = statements
    else:
        payload["sql"] = sql
    if target_rows > 0:
        payload["target_rows"] = target_rows
    if hostile:
        payload["hostile"] = True
    if deadline_ms > 0:
        # The API field is lock_timeout_ms (see internal/api/api.go runRequest); the
        # MCP name deadline_ms is the agent-facing framing of the same value.
        payload["lock_timeout_ms"] = deadline_ms
    if schema_ddl.strip():
        payload["schema_ddl"] = schema_ddl
        if row_count > 0:
            payload["row_count"] = row_count

    # One wait budget covers BOTH the warm-up wait and the run itself, so
    # wait_seconds means what it says: "block up to this long for a result."
    overall_deadline = time.time() + max(0, wait_seconds)
    while True:
        try:
            r = requests.post(f"{API}/api/run", json=payload, timeout=30)
        except requests.RequestException as e:
            return {"verdict": "ERRORED", "reason": f"could not reach the lab: {e}"}
        try:
            body = r.json()
        except ValueError:
            body = {}

        # No warm twin yet (503) or another run in progress (409) are transient, not
        # failures. Rather than bail instantly, wait within wait_seconds and resubmit
        # — so a cold or busy lab blocks for a real result instead of making the
        # agent hand-poll. Only give up (returning the typed status) if the budget
        # runs out.
        if r.status_code in (503, 409):
            remaining = overall_deadline - time.time()
            if remaining <= 1:
                if r.status_code == 409:
                    return {"verdict": "BUSY", "reason": "another crash test is running; retry shortly"}
                eta = body.get("eta_seconds", 45) or 45
                return {"verdict": "WARMING", "reason": f"no warm twin; next in ~{float(eta):.0f}s"}
            if r.status_code == 409:
                nap = 5.0
            else:
                nap = min(float(body.get("eta_seconds", 8) or 8) + 1, 15.0)
            time.sleep(min(nap, remaining))
            continue
        break

    if r.status_code == 429:
        return {
            "verdict": "RATE_LIMITED",
            "reason": "the lab is rate-limiting run submissions; wait and retry.",
            "retry_after_seconds": _retry_after_seconds(r),
        }
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
        # Any other 400 is a refusal, INCLUDING a static shape whose target
        # relation does not exist — which the lab now rejects instead of blessing
        # SAFE. Not a measurement, and clearly not safe. A refusal has no "fix", so
        # suggestion is only present when the engine genuinely has an example.
        rej = {"verdict": "REJECTED", "reason": body.get("reason")}
        if body.get("suggestion"):
            rej["suggestion"] = body["suggestion"]
        return rej
    if r.status_code >= 400 or "run_id" not in body:
        # Never let an unexpected status raise out of the tool; report it plainly.
        return {
            "verdict": "ERRORED",
            "reason": body.get("reason") or f"unexpected lab response (HTTP {r.status_code}).",
        }

    run_id = body["run_id"]
    while time.time() < overall_deadline:
        time.sleep(3)
        result = _finished_result(run_id)
        if result is not None:
            return result
    # The wait budget elapsed with no result. The run is still going, not lost:
    # hand back the run_id so it can be polled via get_result.
    return _pending(run_id, wait_seconds)


@mcp.tool
def get_result(run_id: str, wait_seconds: int = _DEFAULT_WAIT_SECONDS) -> dict:
    """Fetch a finished crash test by its run_id — the recovery path for a
    migration too slow to complete inside check_migration's wait budget (it
    returned PENDING with this run_id).

    Returns the SAME rich fields check_migration returns (effective_verdict,
    verdict, blocked_seconds, casualties, explanation, qualifier, note,
    contention, scale_projection, ...) and the same RETURN CONTRACT of verdict
    values. If the run is still in flight it returns PENDING (poll again); if it
    finished it returns the full result; if no run with this run_id is currently
    retrievable it returns INCONCLUSIVE with a reason.

    Note on retention: the lab keeps the full rich result only for the MOST
    RECENT completed run (served from /api/stats). Because runs are serialised,
    that is the run you just submitted — poll promptly. Its append-only ledger
    (/api/history, /api/report/{ledger_id}) persists a REDUCED record for older
    runs, but it is keyed by an integer ledger id, not this run_id string, so the
    rich fields above are not recoverable once a newer run has superseded it.
    """
    if not isinstance(run_id, str) or not run_id.strip():
        return {"verdict": "ERRORED", "reason": "get_result requires the run_id string from check_migration."}
    run_id = run_id.strip()

    deadline = time.time() + max(0, wait_seconds)
    while True:
        result = _finished_result(run_id)
        if result is not None:
            return result
        if not _still_running(run_id):
            break
        if time.time() >= deadline:
            return _pending(run_id, wait_seconds)
        time.sleep(3)

    # One last look, to cover the brief window between "no longer running" and
    # last_run being published.
    result = _finished_result(run_id)
    if result is not None:
        return result

    # If the caller happened to pass an integer ledger id, the persistent report
    # endpoint can still return the REDUCED (non-rich) record for it.
    if run_id.isdigit():
        report = _safe_get_json(f"{API}/api/report/{run_id}")
        if isinstance(report, dict) and not report.get("error") and report.get("verdict"):
            report["note"] = (
                "from the persistent ledger (/api/report): this is the recorded subset "
                "(verdict, blocked_seconds, casualties_total, lock_modes, ...), not the live "
                "rich fields (effective_verdict, scale_projection, qualifier, contention)."
            )
            return report

    return {
        "verdict": "INCONCLUSIVE",
        "status": "not_found",
        "run_id": run_id,
        "reason": (
            "no finished run with this run_id is currently retrievable. The lab retains the "
            "full result only for the most recent completed run, and a newer run has superseded "
            "it. Re-run check_migration to measure again."
        ),
    }


@mcp.tool
def lab_status() -> dict:
    """Current lab status: warm crash-test twins, template size, whether a run
    is in progress."""
    status = _safe_get_json(f"{API}/api/status")
    if status is None:
        return {"verdict": "ERRORED", "reason": "could not reach the lab."}
    return status


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
