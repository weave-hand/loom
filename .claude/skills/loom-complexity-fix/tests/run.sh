#!/usr/bin/env bash
# Golden test for the complexity triage ranker. Run from repo root:
#   bash .claude/skills/loom-complexity-fix/tests/run.sh
set -euo pipefail
D="$(cd "$(dirname "$0")" && pwd)"
# All paths are absolute, so buck2 run's cwd does not matter. -v0 keeps buck2's
# progress chatter off stderr; jq's rendered JSON is the only stdout. -f (not -rf)
# because triage emits JSON, not raw text.
GOT="$(LC_ALL=C buck2 run -v0 //tools:jq -- -f "$D/../triage.jq" --arg root "/workspace/" "$D/fixture.json")"
if diff -u "$D/golden.json" <(printf '%s\n' "$GOT"); then
  echo "PASS: complexity triage matches golden"
else
  echo "FAIL: complexity triage drifted from golden"; exit 1
fi
