#!/usr/bin/env bash
# Black-box tests for tools/docs.sh. Run: bash tools/tests/docs_test.sh
set -u
DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$DIR/../.." && pwd)"
DOCS="$ROOT/tools/docs.sh"
FIX="$DIR/fixtures"
fail=0
rc(){ "$@" >/dev/null 2>&1; echo $?; }
check(){ # desc want_rc got_rc
  if [ "$2" = "$3" ]; then echo "ok   - $1"; else echo "FAIL - $1 (want rc=$2 got rc=$3)"; fail=1; fi
}

check "valid registers pass" 0 "$(rc bash "$DOCS" validate "$FIX/good-ROADMAP.md" "$FIX/good-FUTURE.md" "$FIX/good-ISSUES.md")"
check "malformed tag block fails" 1 "$(rc bash "$DOCS" validate "$FIX/bad-grammar-ROADMAP.md")"
check "duplicate id fails" 1 "$(rc bash "$DOCS" validate "$FIX/bad-dupid-FUTURE.md")"
check "bad area fails" 1 "$(rc bash "$DOCS" validate "$FIX/bad-area-ISSUES.md")"
check "wrong status for register fails" 1 "$(rc bash "$DOCS" validate "$FIX/bad-status-ROADMAP.md")"
check "unresolvable link fails" 1 "$(rc bash "$DOCS" validate "$FIX/bad-link-FUTURE.md")"

# Uppercase [X] must be parsed, not silently skipped: a bad area in an [X] item must still fail.
check "uppercase [X] item is validated not skipped" 1 "$(rc bash "$DOCS" validate "$FIX/bad-uppercase-ISSUES.md")"
# A missing area: key must report "missing area" (not a misleading shifted-column message).
miss="$(bash "$DOCS" validate "$FIX/bad-missing-area-FUTURE.md" 2>&1 | grep -c 'missing area' || true)"
check "missing area reported clearly" 1 "$miss"

exit $fail
