#!/usr/bin/env bash
# Guards the documentation-completeness invariants this package establishes.
# Run from repo root: bash scripts/docs_consistency_test.sh
set -euo pipefail
DOCS=web/docs.html
fail=0

# 1. Every scrollspy navlink (href="#x") must have a matching doc-sec id with a
#    data-title, or docs.js scrollspy silently drops the section from the rail.
nav_ids=$(grep -oE 'class="navlink" href="#[a-z0-9-]+"' "$DOCS" | sed -E 's/.*#([a-z0-9-]+)".*/\1/' | sort -u)
for id in $nav_ids; do
  if ! grep -qE "class=\"doc-sec[^\"]*\" id=\"$id\" data-title=\"" "$DOCS"; then
    echo "FAIL: navlink #$id has no matching <section class=doc-sec id=$id data-title=...>"; fail=1
  fi
done

# 2. Every doc-sec[data-title] must have a navlink pointing at it (no orphan section).
sec_ids=$(grep -oE 'class="doc-sec[^"]*" id="[a-z0-9-]+" data-title=' "$DOCS" | sed -E 's/.*id="([a-z0-9-]+)".*/\1/' | sort -u)
for id in $sec_ids; do
  if ! grep -qE "class=\"navlink\" href=\"#$id\"" "$DOCS"; then
    echo "FAIL: section #$id has no navlink"; fail=1
  fi
done

# 3. The new BYO-schema section must exist.
grep -q 'id="schema" data-title="Bring your own schema"' "$DOCS" || { echo 'FAIL: #schema section missing'; fail=1; }

# 4. Honest-edge phrases must be present where this package placed them.
grep -q 'identical' docs/LIMITATIONS.md || { echo 'FAIL: LIMITATIONS missing identical-statement note'; fail=1; }
grep -qi '5,000,000\|5M' docs/LIMITATIONS.md || { echo 'FAIL: LIMITATIONS missing 5M seed cap'; fail=1; }
grep -q 'single generic .write. label\|generic `write` label\|generic write label' docs/LIMITATIONS.md || { echo 'FAIL: LIMITATIONS missing generic write label note'; fail=1; }
grep -q 'simulated' "$DOCS" || { echo 'FAIL: docs.html missing simulated-labels callout'; fail=1; }
grep -q 'hold_seconds' "$DOCS" || { echo 'FAIL: docs.html missing contention.hold_seconds explanation'; fail=1; }
grep -q 'target_rows_assumed' "$DOCS" || { echo 'FAIL: docs.html missing assumed-target flag note'; fail=1; }
grep -q 'checkout/login/add_to_cart\|checkout, login\|add_to_cart' mcp/server.py || { echo 'FAIL: server.py missing by_label persona note'; fail=1; }
grep -q 'clamped to 5,000,000' mcp/server.py || { echo 'FAIL: server.py missing 5M seed clamp note'; fail=1; }

# 5. MOAT guard: no doc may print a bare unqualified SAFE for an unmeasured shape.
#    The strings we add near 'statically_safe' / dry-run must keep a qualifier nearby.
if grep -qiE 'returns? SAFE without' "$DOCS"; then echo 'WARN: verify no unmeasured-SAFE phrasing'; fi

if [ "$fail" -ne 0 ]; then echo 'docs consistency: FAILED'; exit 1; fi
echo 'docs consistency: OK'
