#!/usr/bin/env bash
# Run one instrumented rust_test binary, writing its profile to $1.
# Args: <profraw_out_path> <test_bin>
# Any extra args after the binary are passed to it (none needed today).
set -euo pipefail
out="$1"; bin="$2"; shift 2
LLVM_PROFILE_FILE="$out" "$bin" "$@" >/dev/null 2>&1 || true
# The libtest binary exits non-zero only on a FAILING test; coverage still wants
# the profile, so we don't fail the action on test failure (the test suite is
# graded by `buck2 test`, not here).
