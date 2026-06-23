#!/bin/bash
# Shared per-action setup for loom's BuildBuddy Workflows CI (buildbuddy.yaml).
#
# Runs at the top of every action. BuildBuddy snapshots/reuses workflow VMs, so
# every install is existence-guarded — warm runs are near-instant. This is the DRY
# replacement for the old .github/actions/{setup-buck2,install-bsdtar} composite
# actions plus prelude submodule init: the runner checks out neither submodules nor
# the buck2 binary nor the apt packages we need.
#
# Mirrors the buck2-install logic in tools/cloud-setup.sh. Keep BUCK2_RELEASE
# aligned with the prelude submodule pin (see CLAUDE.md / .gitmodules).
set -euo pipefail

BUCK2_RELEASE="2026-05-18"   # keep aligned with the prelude submodule pin
BUCK2_DIR="$HOME/.cache/loom-buck2/$BUCK2_RELEASE"
BUCK2_BIN="$BUCK2_DIR/buck2"

# 1. Packages. zstd decompresses the buck2 release; bsdtar (libarchive-tools) is
#    used by the control-plane-postgres libxml2 genrule (runs locally on the
#    runner); jq parses btd output in the affected action. Guarded — a snapshotted
#    VM that already has them skips apt entirely.
need_pkg=()
command -v zstd   >/dev/null 2>&1 || need_pkg+=(zstd)
command -v bsdtar >/dev/null 2>&1 || need_pkg+=(libarchive-tools)
command -v jq     >/dev/null 2>&1 || need_pkg+=(jq)
if [ "${#need_pkg[@]}" -gt 0 ]; then
  sudo apt-get update -y
  sudo apt-get install -y --no-install-recommends "${need_pkg[@]}"
fi

# 2. buck2 -> a version-stamped cache dir, symlinked onto the default PATH. The
#    filesystem persists across an action's steps (only the shell env does not), and
#    /usr/local/bin is on the default PATH, so later steps find buck2 without re-
#    exporting PATH. A BUCK2_RELEASE bump lands in a fresh dir; the snapshot keeps
#    prior versions warm.
if [ ! -x "$BUCK2_BIN" ]; then
  mkdir -p "$BUCK2_DIR"
  url="https://github.com/facebook/buck2/releases/download/$BUCK2_RELEASE/buck2-x86_64-unknown-linux-gnu.zst"
  curl -fsSL "$url" -o "$BUCK2_DIR/buck2.zst"
  zstd -d -f "$BUCK2_DIR/buck2.zst" -o "$BUCK2_BIN"
  chmod +x "$BUCK2_BIN"
fi
sudo ln -sf "$BUCK2_BIN" /usr/local/bin/buck2

# 3. Prelude submodule (the runner does not check it out). Idempotent.
git submodule update --init --recursive

# 4. Kill any stale buck2 daemon. BuildBuddy snapshots/reuses workflow VMs and
#    restores running processes, but its repo-sync runs `git clean -x` which DELETES
#    buck-out/ — so a restored daemon still holds the now-removed buck-out/v2 and the
#    next `buck2 build` dies with "Error validating working directory: Failed to stat
#    .../buck-out/v2: ENOENT". killall clears it (no valid buck-out needed); the next
#    buck2 invocation spawns a fresh daemon and recreates buck-out. The real cache is
#    BuildBuddy RE, not the local buck-out, so nothing of value is lost.
buck2 killall 2>/dev/null || true

buck2 --version
