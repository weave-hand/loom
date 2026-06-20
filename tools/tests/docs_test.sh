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

exit $fail
