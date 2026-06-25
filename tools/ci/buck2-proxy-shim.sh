#!/usr/bin/env bash
# buck2 shim for loom cloud sessions.
#
# Installed at /usr/local/sbin/buck2 (which precedes /usr/local/bin on the injected
# PATH, so it shadows the real binary) by tools/cloud-session-start.sh. Two jobs:
#
# 1. GitHub egress-proxy bypass for the buck2 DAEMON. buck2's http_archive lowers to a
#    `download_file` action that ALWAYS runs on the local daemon (never RE), and the
#    daemon inherits its environment from whichever `buck2` invocation first spawns it.
#    Cloud tool calls are non-interactive `bash -c` shells that do not source ~/.bashrc
#    (BASH_ENV unset), and the LOOM_CLOUD_ENV block there sits after the
#    `[ -z "$PS1" ] && return` guard anyway — so a profile-based NO_PROXY can never
#    reach the daemon. Exporting it here, on the path every `buck2` invocation takes,
#    does. Through the proxy GitHub serves release-asset HEADs via a 401 backend
#    (objects.githubusercontent.com) while GET succeeds (release-assets.…); bypassing
#    the proxy for these public, sha256-pinned assets makes http_head + GET both work.
#
# 2. Route `buck2 test` execution to RE. Cloud sessions run as root, and buck2/tpx runs
#    test-run actions on the LOCAL executor by default (it only goes to RE when told).
#    The fixture tests boot initdb/postgres/duckdb, which refuse to run as root — so a
#    plain `buck2 test //src/...` here is 88 pass / 82 fail (local-as-root), while the
#    same run with --unstable-allow-all-tests-on-re is 170/170 (RE runs them as the
#    non-root `buildbuddy` worker). RE itself is fully working in cloud sessions; this
#    is purely test-EXECUTION PLACEMENT. There is NO buckconfig key for it and NO remote
#    test-result cache — the flag is the only lever, so we inject it for `test`
#    invocations. Gated on BUILDBUDDY_API_KEY so it degrades to local if RE is absent;
#    cloud-only, so local dev / a developer's `buck2 test` is unaffected. (The flag is
#    `--unstable-*`; it is stable for the pinned buck2 and should be re-checked on bump.)
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

# Inject the RE test-routing flag for `test` invocations (job 2 above). Locate the
# subcommand as the first non-option argument, skipping the one global option that
# takes a separate-token value (--isolation-dir); insert the flag right after it (so it
# lands before any targets or a `--` test-arg separator). No-op unless RE is configured
# and the flag isn't already present.
args=("$@")
if [ -n "${BUILDBUDDY_API_KEY:-}" ]; then
  sub=""; subi=-1; skip=0
  for i in "${!args[@]}"; do
    a="${args[$i]}"
    if [ "$skip" = 1 ]; then skip=0; continue; fi
    case "$a" in
      --isolation-dir) skip=1 ;;
      -*) ;;
      *) sub="$a"; subi="$i"; break ;;
    esac
  done
  if [ "$sub" = "test" ] && [[ " $* " != *" --unstable-allow-all-tests-on-re "* ]]; then
    args=("${args[@]:0:$((subi + 1))}" --unstable-allow-all-tests-on-re "${args[@]:$((subi + 1))}")
  fi
fi

exec "$REAL_BUCK2" "${args[@]}"
