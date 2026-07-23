#!/usr/bin/env bash
# Slice-1 boundary guard: query-api's LIBRARY target must not directly depend
# on the control-plane/postgres crate (direct deps, depth=1). Transitive postgres
# via //src/services/runtime is accepted and deferred to slice 2. See
# git history: 2026-06-29-engine-serving-write-relocation-design.
set -euo pipefail
out="$(buck2 cquery 'deps(//src/services/query-api:query-api, 1)' 2>/dev/null)"
if grep -q '//src/control-plane/postgres:postgres' <<<"$out"; then
  echo "FAIL: //src/services/query-api:query-api still directly depends on control-plane/postgres" >&2
  exit 1
fi
echo "OK: query-api library has no direct dependency on control-plane/postgres"
