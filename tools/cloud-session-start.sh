#!/bin/bash
# SessionStart hook for loom cloud code-health routines.
#
# Runs at the start of every cloud session (after the cached snapshot is restored,
# before work begins). Wired via .claude/settings.json. It is a NO-OP unless
# REMOTE_ENV=true, so it stays inert for local interactive development.
#
# The cloud session injects two secrets into this hook's environment:
#   BUILDBUDDY_API_KEY  — remote execution (.buckconfig reads $BUILDBUDDY_API_KEY)
#   GITHUB_TOKEN        — gh auth for PR landing (gh honors GH_TOKEN/GITHUB_TOKEN)
# plus REMOTE_ENV=true as the "this is a cloud routine" marker.
#
# Each Bash tool call starts a fresh shell from the user's profile, so the session
# env does not automatically reach those subshells. We therefore persist what
# buck2/gh need into ~/.bashrc (idempotently). buck2 is already on PATH from the
# setup script (/usr/local/bin); we re-assert it for safety.

set -u
# Gate on the platform-set CLAUDE_CODE_REMOTE (always "true" in any cloud session),
# falling back to the user-supplied REMOTE_ENV marker. Keying solely on REMOTE_ENV was
# brittle: it lives in the environment's "Environment variables" field, so editing that
# field (rename/remove) could silently knock out the entire cloud bootstrap — no buck2
# PATH, no remote execution, no submodule init, no warning. CLAUDE_CODE_REMOTE is owned
# by the platform and cannot be clobbered that way.
[ "${CLAUDE_CODE_REMOTE:-}" = "true" ] || [ "${REMOTE_ENV:-}" = "true" ] || exit 0   # inert outside cloud routines

# The hook runs from within the repo (invoked as $CLAUDE_PROJECT_DIR/tools/...), so
# $0 resolves correctly here (unlike the setup script, which runs from /tmp). Prefer
# the explicit project dir when the harness provides it.
REPO="${CLAUDE_PROJECT_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
PROFILE="$HOME/.bashrc"
touch "$PROFILE"

if ! grep -q 'LOOM_CLOUD_ENV' "$PROFILE" 2>/dev/null; then
  {
    echo '# --- LOOM_CLOUD_ENV (added by tools/cloud-session-start.sh) ---'
    echo 'export PATH="/usr/local/bin:$PATH"'   # buck2 (and gh, both in standard dirs)
    echo 'export REMOTE_ENV=true'
    # Secrets: written in plaintext to the profile of an ephemeral cloud session.
    [ -n "${BUILDBUDDY_API_KEY:-}" ] && printf 'export BUILDBUDDY_API_KEY=%q\n' "$BUILDBUDDY_API_KEY"
    [ -n "${GITHUB_TOKEN:-}" ]       && printf 'export GITHUB_TOKEN=%q\n' "$GITHUB_TOKEN"
    [ -n "${GITHUB_TOKEN:-}" ]       && printf 'export GH_TOKEN=%q\n' "$GITHUB_TOKEN"
    # Bypass the egress proxy for github asset hosts so buck2 can fetch toolchains.
    # buck2's http_archive lowers to a `download_file` action that ALWAYS runs on the
    # local daemon (never on RE — it can't be offloaded), and toolchains/BUCK pulls the
    # hermetic LLVM (:78) and CPython (:115) toolchains from github releases. The cloud
    # proxy now brokers all authenticated github access (the Claude GitHub App layer):
    # github.com 302s to release-assets.githubusercontent.com, where buck2's redirect
    # gets a 401, so every native `buck2 build //src/...` fails on the toolchain fetch.
    # Direct egress to github works for these public, sha256-pinned tarballs, so route
    # them around the proxy. The same applies to the //tools:* github-release binaries.
    # This profile block is for INTERACTIVE shells (a human's terminal). It does NOT
    # reach non-interactive `bash -c` tool-call shells (they skip ~/.bashrc past the
    # `[ -z "$PS1" ] && return` guard) nor the buck2 daemon they spawn — the buck2
    # shim installed below is what carries this bypass to the daemon, which is the
    # process that actually runs `download_file`. Written single-quoted so each shell
    # APPENDS to the live NO_PROXY rather than baking a stale snapshot of it.
    echo 'export NO_PROXY="${NO_PROXY:+$NO_PROXY,}github.com,objects.githubusercontent.com,release-assets.githubusercontent.com,codeload.github.com,.githubusercontent.com"'
    echo 'export no_proxy="$NO_PROXY"'
    echo '# --- end LOOM_CLOUD_ENV ---'
  } >> "$PROFILE"
fi

# --- Cold-build hardening -----------------------------------------------------
# A fresh cloud session builds //src/... from cold, which trips three blockers the
# snapshot/profile alone do not cover. See
# docs/superpowers/specs/2026-06-25-cloud-session-cold-build-reliability-design.md.

# (1) buck2 shim. /usr/local/sbin precedes /usr/local/bin on PATH, so this shim shadows
# the real binary. It does two things before exec-ing it: (a) exports the github
# NO_PROXY bypass so the daemon (which runs `download_file`) can fetch toolchains
# regardless of how the harness injects env into non-interactive tool shells — the
# authoritative fix for the "head issue"; and (b) injects --unstable-allow-all-tests-on-re
# for `buck2 test`, so fixture test RUNS go to RE (non-root `buildbuddy`) instead of the
# local executor, which here is root and would fail their initdb. Idempotent copy.
SHIM_SRC="$REPO/tools/ci/buck2-proxy-shim.sh"
SHIM_DST="/usr/local/sbin/buck2"
if [ -f "$SHIM_SRC" ]; then
  if ! cmp -s "$SHIM_SRC" "$SHIM_DST" 2>/dev/null; then
    install -m 0755 "$SHIM_SRC" "$SHIM_DST" 2>/dev/null \
      || echo "WARN: could not install buck2 proxy shim to $SHIM_DST (github fetches may 401)"
  fi
else
  echo "WARN: buck2 proxy shim source missing at $SHIM_SRC"
fi

# (2) bsdtar (libarchive-tools) — required by the :libxml2 fixture genrule. The setup
# snapshot may not include it; install per-session if absent.
if ! command -v bsdtar >/dev/null 2>&1; then
  if [ "$(id -u)" = 0 ]; then _apt="apt-get"; else _apt="sudo apt-get"; fi
  $_apt install -y --no-install-recommends libarchive-tools >/tmp/loom-bsdtar.log 2>&1 \
    || echo "WARN: bsdtar install failed (libxml2 fixture genrule may fail); see /tmp/loom-bsdtar.log"
fi

# (3) Third-party GitHub git sources (the prelude submodule + Cargo git deps like
# apache/iceberg-rust). ~/.gitconfig rewrites ALL https://github.com/ to the scoped
# git proxy, which 403s any repo outside this session's scope — so these public,
# sha-pinned sources are denied. We don't hardcode the orgs: derive them from the
# repo's OWN declarations (.gitmodules + `git = "https://github.com/…"` in Cargo
# manifests), so a new submodule or git-dep is covered automatically. For each, a
# longer (more specific) self-referential insteadOf wins by longest-match and keeps
# github.com/<org>/* on its direct URL instead of the scoped proxy. The actual
# egress-proxy bypass is per-consumer: the shim's NO_PROXY for the buck2 daemon's
# git_fetch, and `-c http.proxy=` on the submodule init below.
{
  git -C "$REPO" config -f "$REPO/.gitmodules" --get-regexp 'submodule\..*\.url' 2>/dev/null | awk '{print $2}'
  grep -rhoE 'git = "https://github\.com/[^"]+"' "$REPO/src" 2>/dev/null | sed -E 's/git = "//; s/"$//'
} | while read -r _url; do
  case "$_url" in
    https://github.com/*/*)
      _rest="${_url#https://github.com/}"; _org="https://github.com/${_rest%%/*}/"
      git config --global url."$_org".insteadOf "$_org" 2>/dev/null \
        || echo "WARN: could not set git insteadOf override for $_org (fetch may 403)"
      ;;
  esac
done
# --- end cold-build hardening -------------------------------------------------

# Sanity: warn (don't fail) if a required tool/secret is missing — the routine can
# still partially run, and a clear message beats a cryptic failure later.
command -v buck2 >/dev/null 2>&1 || echo "WARN: buck2 not found on PATH"
command -v gh    >/dev/null 2>&1 || echo "WARN: gh not found on PATH (PR landing will fail)"
[ -n "${BUILDBUDDY_API_KEY:-}" ] || echo "WARN: BUILDBUDDY_API_KEY unset (remote execution disabled)"
[ -n "${GITHUB_TOKEN:-}" ]       || echo "WARN: GITHUB_TOKEN unset (gh PR landing will fail)"

# Ensure the prelude submodule is present — any buck2 build needs it, and the setup
# script may not have located the repo to init it. Idempotent / fast if already done.
# `-c http.proxy=` disables the egress proxy for this invocation (it propagates to the
# per-submodule child clones), so the public github submodule clones direct instead of
# 403-ing through the scoped git proxy; the insteadOf overrides above keep its URL on
# github.com. The only submodule is the prelude (github) — loom-hosted submodules, if
# any were added, use 127.0.0.1 (localhost, never proxied), so this is safe for them too.
if [ -f "$REPO/.gitmodules" ]; then
  git -C "$REPO" -c http.proxy= submodule update --init --recursive >/tmp/loom-submodule.log 2>&1 || \
    echo "WARN: submodule init failed (see /tmp/loom-submodule.log)"
fi

# Activate the loom dev env (cargo/rustc/clippy + dev tools on PATH) and persist
# its exports to the profile. With BUILDBUDDY_API_KEY now present, its buck2 builds
# go over remote execution (fast, action-cache backed). Non-fatal: the code-health
# routines themselves only need `buck2 run //tools:...`, which works without this —
# so a failure here does not block the session. Remove this block (or background
# it) if session startup is too slow for a census-only routine.
if [ -x "$REPO/tools/env.sh" ]; then
  ( cd "$REPO" && ./tools/env.sh ) >> "$PROFILE" 2>/tmp/loom-env.log || \
    echo "WARN: tools/env.sh activation failed (non-fatal); see /tmp/loom-env.log"
fi

echo "loom cloud session ready (REMOTE_ENV=true): buck2 + gh + remote execution configured"
