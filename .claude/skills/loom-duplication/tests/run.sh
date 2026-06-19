#!/usr/bin/env bash
# Golden test for the duplication census renderer. Run from repo root:
#   bash .claude/skills/loom-duplication/tests/run.sh
# Covers both the empty baseline (no Accepted section) and a populated baseline
# (accepted pairs listed) so the suppression-disclosure path stays tested.
set -euo pipefail
D="$(cd "$(dirname "$0")" && pwd)"

check() { # <baseline.json> <golden.md> <label>
  local got
  got="$(LC_ALL=C buck2 run -v0 //tools:jq -- -rf "$D/../render.jq" --slurpfile baseline "$1" "$D/dup.json")"
  if diff -u "$2" <(printf '%s\n' "$got"); then
    echo "PASS: duplication renderer matches golden ($3)"
  else
    echo "FAIL: duplication renderer drifted from golden ($3)"; exit 1
  fi
}

check "$D/baseline.json"           "$D/golden.md"            "empty baseline"
check "$D/baseline-populated.json" "$D/golden-populated.md"  "populated baseline"
