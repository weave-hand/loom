#!/bin/bash
# Cloud-session SETUP script for running loom's code-health routines.
#
# Runs ONCE as root on Ubuntu 24.04 before Claude Code launches; the resulting
# filesystem is snapshotted and reused by later sessions (the script is then
# skipped). Heavy, one-time installs belong here. Keep total runtime under ~5 min.
#
# IMPORTANT: the platform copies this script to /tmp and runs it from there, so
# `$0` is NOT in the repo, and the repo may not even be checked out yet at setup
# time. So this script does only the REPO-INDEPENDENT installs reliably (apt
# packages + the buck2 binary), and pre-warms the //tools:* builds ONLY if it can
# locate the repo. The SessionStart hook (tools/cloud-session-start.sh) inits the
# prelude submodule and builds per session, so nothing here is load-bearing for
# correctness — it only warms the cache. It always exits 0 so the snapshot builds;
# real problems surface as WARN lines.
#
# NETWORK: needs github.com + *.githubusercontent.com in the env's allowed hosts
# (buck2 + gh + the //tools:* release binaries download from GitHub; the buck2
# asset redirects to release-assets.githubusercontent.com).

set -u
START_PWD="$PWD"
BUCK2_RELEASE="2026-05-18"   # keep aligned with tools/ci/buildbuddy-setup.sh + prelude pin
GH_VERSION="2.62.0"          # fallback gh (apt's gh is unreliable on a base image)
echo "loom cloud setup starting (pwd=$START_PWD, buck2=$BUCK2_RELEASE)"

# 1. Packages. The base image carries broken third-party PPAs (deadsnakes/ondrej)
#    that 403, so `apt-get update` exits non-zero — do NOT let that abort us; the
#    main Ubuntu archives still refresh, which is all we need. zstd is CRITICAL
#    (buck2 ships as .zst); install it on its own line so a gh failure can't take
#    it down.
apt-get update -y || echo "WARN: apt-get update reported errors (continuing; main archives still refresh)"
apt-get install -y --no-install-recommends zstd >/tmp/loom-apt-zstd.log 2>&1 || \
  { echo "WARN: apt install zstd failed:"; tail -5 /tmp/loom-apt-zstd.log; }
# libarchive-tools provides `bsdtar`, required by the :libxml2 fixture genrule (one
# of the hermetic-Postgres test inputs). Baking it into the snapshot also makes it
# available per session in case the libxml2 action ever cache-misses.
apt-get install -y --no-install-recommends libarchive-tools >/tmp/loom-apt-bsdtar.log 2>&1 || \
  { echo "WARN: apt install libarchive-tools (bsdtar) failed:"; tail -5 /tmp/loom-apt-bsdtar.log; }
apt-get install -y --no-install-recommends gh >/tmp/loom-apt-gh.log 2>&1 || true

# gh fallback: install the official release tarball to /usr/local/bin if apt didn't
# provide it (gh is not in the default Ubuntu repos).
if ! command -v gh >/dev/null 2>&1; then
  if curl -fsSL "https://github.com/cli/cli/releases/download/v${GH_VERSION}/gh_${GH_VERSION}_linux_amd64.tar.gz" -o /tmp/gh.tgz; then
    tar -xzf /tmp/gh.tgz -C /tmp && install -m755 "/tmp/gh_${GH_VERSION}_linux_amd64/bin/gh" /usr/local/bin/gh
  fi
fi
command -v gh >/dev/null 2>&1 && echo "gh ok: $(gh --version | head -1)" || echo "WARN: gh unavailable (PR landing will fail)"

# 2. buck2 -> /usr/local/bin (root-owned, on the default PATH, survives the
#    snapshot). Needs zstd to decompress.
if ! command -v buck2 >/dev/null 2>&1; then
  if command -v zstd >/dev/null 2>&1; then
    if curl -fsSL "https://github.com/facebook/buck2/releases/download/${BUCK2_RELEASE}/buck2-x86_64-unknown-linux-gnu.zst" -o /tmp/buck2.zst; then
      zstd -d -f /tmp/buck2.zst -o /usr/local/bin/buck2 && chmod +x /usr/local/bin/buck2
    fi
  else
    echo "WARN: zstd missing — cannot install buck2"
  fi
fi
command -v buck2 >/dev/null 2>&1 && echo "buck2 ok: $(buck2 --version)" || echo "WARN: buck2 unavailable (routines cannot run)"

# 3. Locate the repo (NOT derivable from $0 here). Try the initial cwd, then common
#    checkout paths. If found, init the prelude submodule and pre-warm the routines'
#    tools into buck-out (captured by the snapshot). If NOT found, skip quietly —
#    the SessionStart hook does both per session.
REPO=""
for c in "$START_PWD" "${OLDPWD:-}" /workspace /root/loom "$HOME/loom" /home/*/loom /app /code; do
  [ -n "$c" ] && git -C "$c" rev-parse --show-toplevel >/dev/null 2>&1 && \
    { REPO="$(git -C "$c" rev-parse --show-toplevel)"; break; }
done

if [ -n "$REPO" ]; then
  echo "repo found at $REPO — initializing prelude + pre-warming tools"
  git -C "$REPO" submodule update --init --recursive >/tmp/loom-submodule.log 2>&1 || \
    { echo "WARN: submodule init failed:"; tail -5 /tmp/loom-submodule.log; }
  if command -v buck2 >/dev/null 2>&1; then
    # Forced LOCAL (--config project.remote_enabled=): the BuildBuddy key is not
    # available at setup time and .buckconfig sets remote_enabled=true.
    #
    # Two groups get pre-warmed into the snapshot's CAS:
    #  - //tools:*            — the code-health routines' binaries.
    #  - the fixture/toolchain DOWNLOADS — postgres-bin, duckdb-cli, libxml2, and
    #    the CPython toolchain archive. These are `download_file`/genrule inputs the
    #    hermetic-Postgres/DuckDB tests need to even BUILD. At session time their
    #    fetches go through the agent proxy, whose HEAD-request handling redirects
    #    GitHub release HEADs to objects.githubusercontent.com (401) while the GET
    #    succeeds — so buck2's pre-flight http_head aborts download_file before it
    #    ever issues the working GET (see anthropics/claude-code#70588).
    #    Fetching them here (setup runs outside that proxy) bakes them into the
    #    snapshot so the session build hits cache instead of re-downloading.
    ( cd "$REPO" && buck2 build --config project.remote_enabled= \
        //tools:jq //tools:rust-code-analysis //tools:lucidshark-duplo \
        //tools:rust-analyzer //tools:rust-project \
        //src/control-plane/postgres:postgres-bin \
        //src/control-plane/postgres:duckdb-cli \
        //src/control-plane/postgres:libxml2 \
        toolchains//:cpython_archive ) \
      >/tmp/loom-prewarm.log 2>&1 || \
      { echo "WARN: tool pre-warm failed (non-fatal):"; tail -10 /tmp/loom-prewarm.log; }
  fi
else
  echo "WARN: loom repo not found at setup time — skipping submodule + pre-warm (the SessionStart hook handles them per session)."
fi

echo "loom cloud setup complete."
exit 0
