#!/usr/bin/env bash
set -euo pipefail
D="$(cd "$(dirname "$0")" && pwd)"
GOT="$(LC_ALL=C buck2 run -v0 //tools:jq -- -rf "$D/../render.jq" --slurpfile baseline "$D/baseline.json" "$D/dup.json")"
if diff -u "$D/golden.md" <(printf '%s\n' "$GOT"); then
  echo "PASS: duplication renderer matches golden"
else
  echo "FAIL: duplication renderer drifted from golden"; exit 1
fi
