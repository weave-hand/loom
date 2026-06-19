#!/bin/bash
# Cloud-session SETUP script for running loom's code-health routines.
#
# Runs ONCE as root on Ubuntu 24.04 before Claude Code launches; the resulting
# filesystem is snapshotted and reused by later sessions (the script is then
# skipped). So heavy, one-time installs belong here. Keep total runtime under
# ~5 min or the snapshot can't build.
#
# Paste this into the environment settings "Setup script" field (it is committed
# here for version control / review). The per-session secrets (BUILDBUDDY_API_KEY,
# GITHUB_TOKEN) are NOT available at setup time — they are wired by the
# SessionStart hook (tools/cloud-session-start.sh).
#
# NETWORK: needs access to github.com and *.githubusercontent.com (buck2 release
# downloads redirect to release-assets.githubusercontent.com; the //tools:* release
# binaries come from GitHub releases too). The default "Trusted" network set only
# covers npm/PyPI/etc — add github.com + *.githubusercontent.com to allowed hosts.

set -u  # not -e: non-critical steps use `|| true` so an intermittent failure
        # does not block the whole snapshot build.

REPO="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO"

# Keep aligned with .github/workflows/ci.yml and the prelude submodule pin.
BUCK2_RELEASE="2026-05-18"

echo "loom cloud setup: repo=$REPO buck2_release=$BUCK2_RELEASE"

# 1. System packages: gh (PR landing) + zstd (buck2 .zst decompress). Run in the
#    background while we fetch the prelude in parallel.
( apt-get update -y && apt-get install -y gh zstd ) >/tmp/loom-apt.log 2>&1 &
APT_PID=$!

# 2. Prelude submodule (pin-aligned with buck2; required for any buck2 build).
git submodule update --init --recursive >/tmp/loom-submodule.log 2>&1 || \
  { echo "WARN: submodule init failed:"; tail -20 /tmp/loom-submodule.log; }

wait "$APT_PID" || { echo "WARN: apt install failed:"; tail -20 /tmp/loom-apt.log; }

# 3. Pinned buck2 -> /usr/local/bin (root-owned, on the default PATH, so it
#    survives in the snapshot and needs no per-session PATH wiring). CRITICAL:
#    nothing works without buck2, so fail the snapshot build if this fails.
if ! command -v buck2 >/dev/null 2>&1; then
  url="https://github.com/facebook/buck2/releases/download/${BUCK2_RELEASE}/buck2-x86_64-unknown-linux-gnu.zst"
  curl -fsSL "$url" -o /tmp/buck2.zst
  zstd -d -f /tmp/buck2.zst -o /usr/local/bin/buck2
  chmod +x /usr/local/bin/buck2
fi
buck2 --version || { echo "FATAL: buck2 install failed"; exit 1; }

# 4. Pre-warm the routines' hermetic tools into buck-out (captured by the
#    snapshot), so sessions skip these downloads/extractions. Forced LOCAL via
#    `--config project.remote_enabled=`: the BuildBuddy key is not available at
#    setup time, and .buckconfig sets remote_enabled=true (which would need it).
buck2 build --config project.remote_enabled= \
  //tools:jq //tools:rust-code-analysis //tools:lucidshark-duplo \
  >/tmp/loom-prewarm.log 2>&1 || \
  { echo "WARN: tool pre-warm failed (non-fatal; sessions will fetch on demand):"; tail -20 /tmp/loom-prewarm.log; }

echo "loom cloud setup complete: $(buck2 --version 2>/dev/null) | gh $(gh --version 2>/dev/null | head -1)"
