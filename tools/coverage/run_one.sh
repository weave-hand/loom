#!/usr/bin/env bash
# Run one instrumented rust_test binary, writing its profile to $1.
# Args: <profraw_out_path> <test_bin>
# Any extra args after the binary are passed to it (none needed today).
set -euo pipefail
out="$1"; bin="$2"; shift 2
# A FAILING test exits non-zero but STILL writes its profile, so tolerate a
# non-zero exit here (the suite is graded by `buck2 test`, not coverage).
LLVM_PROFILE_FILE="$out" "$bin" "$@" >/dev/null 2>&1 || true
# But a binary that CRASHES (segfault/OOM — e.g. a heavy instrumented binary on
# RE, or a fixture whose postgres won't boot) writes NO profile. Require it:
# without this, buck sees an exit-0 action with a missing declared output and can
# cache that profile-less "success" (a stale RE failure then poisons later local
# runs under the same digest). Exiting non-zero makes it a real, uncached failure
# that re-runs — so the crash surfaces loudly instead of silently reporting 0%.
test -s "$out" || { echo "coverage: $bin wrote no profile (crashed before flush?)" >&2; exit 1; }
