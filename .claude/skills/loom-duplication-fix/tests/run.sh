#!/usr/bin/env bash
# Golden test for the duplication triage ranker. Run from repo root:
#   bash .claude/skills/loom-duplication-fix/tests/run.sh
set -euo pipefail
D="$(cd "$(dirname "$0")" && pwd)"
GOT="$(LC_ALL=C buck2 run -v0 //tools:jq -- -f "$D/../triage.jq" "$D/fixture.json")"
if diff -u "$D/golden.json" <(printf '%s\n' "$GOT"); then
  echo "PASS: duplication triage matches golden"
else
  echo "FAIL: duplication triage drifted from golden"; exit 1
fi
