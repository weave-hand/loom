#!/usr/bin/env bash
# Golden test for the complexity census renderer. Run from repo root:
#   bash .claude/skills/loom-complexity/tests/run.sh
set -euo pipefail
D="$(cd "$(dirname "$0")" && pwd)"
# All paths are absolute, so buck2 run's cwd does not matter. -v0 keeps buck2's
# progress chatter off stderr; jq's rendered text is the only stdout.
GOT="$(LC_ALL=C buck2 run -v0 //tools:jq -- -rf "$D/../render.jq" --arg root "/workspace/" --slurpfile waivers "$D/waivers.json" "$D/fixture.json")"
if diff -u "$D/golden.md" <(printf '%s\n' "$GOT"); then
  echo "PASS: complexity renderer matches golden"
else
  echo "FAIL: complexity renderer drifted from golden"; exit 1
fi
