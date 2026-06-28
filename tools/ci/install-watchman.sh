#!/bin/bash
# Install watchman on a CI runner — buck2's file_watcher (see .buckconfig).
#
# .buckconfig sets `[buck2] file_watcher = watchman`, so EVERY buck2 invocation
# (build/test/lint here, image builds in release.yml) needs the watchman binary or
# the daemon refuses to start. watchman is not in the Ubuntu repos, so install it
# from the facebook/watchman release zip into /usr/local/{bin,lib} (it links
# against libfolly/libglog/... shipped in the zip's lib/), plus the world-writable
# state dir watchman needs, then ldconfig so the binary resolves its libs.
#
# Idempotent + existence-guarded so a snapshotted/cached runner skips it. Shared by
# tools/ci/buildbuddy-setup.sh and .github/actions/setup-buck2 (both run with the
# repo checked out). tools/cloud-setup.sh inlines an equivalent block instead — it
# runs before the repo is guaranteed to exist, so it cannot source this file; keep
# WATCHMAN_VERSION aligned between the two.
set -euo pipefail

WATCHMAN_VERSION="v2026.06.21.00"   # keep aligned with tools/cloud-setup.sh

if command -v watchman >/dev/null 2>&1; then
  echo "watchman present: $(watchman version 2>/dev/null | tr -d '\n ' | head -c 60)"
  exit 0
fi

# Use sudo only when not already root (cloud=root, GH/BuildBuddy runners=sudo user).
SUDO=""
[ "$(id -u)" -ne 0 ] && SUDO="sudo"

# unzip unpacks the release; install it if the runner lacks it.
if ! command -v unzip >/dev/null 2>&1; then
  $SUDO apt-get update -y
  $SUDO apt-get install -y --no-install-recommends unzip
fi

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
url="https://github.com/facebook/watchman/releases/download/${WATCHMAN_VERSION}/watchman-${WATCHMAN_VERSION}-linux.zip"
curl -fsSL "$url" -o "$tmp/watchman.zip"
unzip -q "$tmp/watchman.zip" -d "$tmp"
wm_dir="$(find "$tmp" -maxdepth 1 -type d -name 'watchman-*' | head -1)"
if [ -z "$wm_dir" ]; then
  echo "ERROR: watchman zip did not contain the expected watchman-* directory" >&2
  exit 1
fi

$SUDO mkdir -p /usr/local/bin /usr/local/lib /usr/local/var/run/watchman
$SUDO cp -a "$wm_dir"/bin/* /usr/local/bin/
$SUDO cp -a "$wm_dir"/lib/* /usr/local/lib/
$SUDO chmod 755 /usr/local/bin/watchman
$SUDO chmod 2777 /usr/local/var/run/watchman
$SUDO ldconfig

watchman version
