#!/usr/bin/env bash
# Regenerate third-party/python/ from muntjac.toml + src/sdk/python/uv.lock:
# `uv lock` → `muntjac vendor` → `muntjac buckify`. The Python analog of
# tools/buckify.sh.
#
# --frozen skips the `uv lock` step and passes --frozen to muntjac (which
# forbids its own when-stale re-lock) — the deterministic, network-free form
# the muntjac-check prek hook and CI use. muntjac's staleness gate is a raw
# mtime comparison, and fresh CI checkouts have arbitrary mtimes; --frozen
# keeps the lint job from ever resolving against PyPI.
#
# vendor runs in prebake-only mode ([buck] vendor = false): with an all-wheels
# dependency set it only (re)writes third-party/python/prebake/'s manifest —
# it becomes load-bearing the day a dependency ships sdist-only.
#
# Usage: ./tools/pybuckify.sh [--frozen]   (run from anywhere)
set -euo pipefail

FROZEN=""
if [ "${1:-}" = "--frozen" ]; then FROZEN="--frozen"; fi

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

# muntjac invokes `uv lock` when pyproject.toml is newer than uv.lock, so the
# vendored uv goes on PATH ahead of anything else — no host uv anywhere. Resolve
# the concrete per-arch genrule, NOT the :uv command_alias: alias trampolines
# resolve siblings via $0 and break when symlinked (same reason env.sh points at
# genrules directly).
case "$(uname -m)" in
  x86_64) UV_TARGET="root//tools:uv-x86_64-linux" ;;
  aarch64) UV_TARGET="root//tools:uv-aarch64-linux" ;;
  *) echo "unsupported arch: $(uname -m)" >&2; exit 1 ;;
esac
UV_BIN="$REPO_ROOT/$(buck2 build "$UV_TARGET" --show-output 2>/dev/null | awk '{print $2}')"
UV_DIR="$(mktemp -d)"
trap 'rm -rf "$UV_DIR"' EXIT
ln -s "$UV_BIN" "$UV_DIR/uv"
export PATH="$UV_DIR:$PATH"

if [ -z "$FROZEN" ]; then
    uv lock --project src/sdk/python
fi
buck2 run --console none root//tools:muntjac -- $FROZEN vendor
buck2 run --console none root//tools:muntjac -- $FROZEN buckify

echo "pybuckify complete"
