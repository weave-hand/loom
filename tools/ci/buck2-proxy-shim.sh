#!/usr/bin/env bash
# buck2 proxy shim for loom cloud sessions.
#
# Installed at /usr/local/sbin/buck2 (which precedes /usr/local/bin on the injected
# PATH, so it shadows the real binary) by tools/cloud-session-start.sh. Its single
# job: guarantee the buck2 DAEMON inherits the GitHub egress-proxy bypass.
#
# Why a shim and not ~/.bashrc: buck2's http_archive lowers to a `download_file`
# action that ALWAYS runs on the local daemon (never RE), and the daemon inherits its
# environment from whichever `buck2` invocation first spawns it. Cloud tool calls are
# non-interactive `bash -c` shells that do not source ~/.bashrc (BASH_ENV unset), and
# the LOOM_CLOUD_ENV block there sits after the `[ -z "$PS1" ] && return` guard
# anyway — so a profile-based NO_PROXY can never reach the daemon. Exporting it here,
# on the path every `buck2` invocation takes, does. Through the proxy GitHub serves
# release-asset HEADs via a 401 backend (objects.githubusercontent.com) while GET
# succeeds (release-assets.githubusercontent.com); bypassing the proxy for these
# public, sha256-pinned assets makes buck2's http_head preflight + GET both work.
#
# See docs/superpowers/specs/2026-06-25-cloud-session-cold-build-reliability-design.md.
set -u

export NO_PROXY="${NO_PROXY:+$NO_PROXY,}github.com,objects.githubusercontent.com,release-assets.githubusercontent.com,codeload.github.com,.githubusercontent.com"
export no_proxy="$NO_PROXY"

# The real binary. tools/cloud-setup.sh always installs it at /usr/local/bin/buck2;
# the shim lives at /usr/local/sbin/buck2, so the two paths never collide (no exec
# loop). LOOM_REAL_BUCK2 overrides for testing.
REAL_BUCK2="${LOOM_REAL_BUCK2:-/usr/local/bin/buck2}"
if [ ! -x "$REAL_BUCK2" ]; then
  echo "buck2-proxy-shim: real buck2 not found at $REAL_BUCK2 (set LOOM_REAL_BUCK2)" >&2
  exit 127
fi
exec "$REAL_BUCK2" "$@"
