#!/usr/bin/env bash
# Wrapper around `reindeer buckify` that regenerates third-party/BUCK from the
# workspace Cargo.lock. It does NO post-processing of reindeer's output — everything
# is handled natively now:
#   - archive-before-buildscript ordering  → reindeer's rule-priority sort (PR #102);
#   - native-lib link forwarding           → per-crate `[buildscript.run]
#     rustc_link_lib/search = true` fixups (libduckdb-sys/ring/zstd-sys);
#   - two public majors of one crate        → a Cargo dep rename, honoured by loom's
#     reindeer fork (weave-hand/reindeer#1) — see src/control-plane/postgres/Cargo.toml.
#
# Usage: ./tools/buckify.sh   (run from anywhere; resolves the repo root itself)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

cd "$REPO_ROOT"

# reindeer shells out to `cargo metadata`, so put loom's hermetic Rust toolchain
# (cargo + rustc + sysroot, assembled by //tools:rust-host-toolchain) ahead of
# anything on PATH. This keeps buckify reproducible and means no host rustup is
# needed — locally or in CI.
TOOLCHAIN="$REPO_ROOT/$(buck2 build root//tools:rust-host-toolchain --show-output 2>/dev/null | awk '{print $2}')"
export PATH="$TOOLCHAIN/bin:$PATH"
export LD_LIBRARY_PATH="$TOOLCHAIN/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

buck2 run root//tools:reindeer -- buckify "$@"

echo "buckify complete"
