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
[ "${REMOTE_ENV:-}" = "true" ] || exit 0   # inert outside cloud routines

REPO="$(cd "$(dirname "$0")/.." && pwd)"
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
    echo '# --- end LOOM_CLOUD_ENV ---'
  } >> "$PROFILE"
fi

# Sanity: warn (don't fail) if a required tool/secret is missing — the routine can
# still partially run, and a clear message beats a cryptic failure later.
command -v buck2 >/dev/null 2>&1 || echo "WARN: buck2 not found on PATH"
command -v gh    >/dev/null 2>&1 || echo "WARN: gh not found on PATH (PR landing will fail)"
[ -n "${BUILDBUDDY_API_KEY:-}" ] || echo "WARN: BUILDBUDDY_API_KEY unset (remote execution disabled)"
[ -n "${GITHUB_TOKEN:-}" ]       || echo "WARN: GITHUB_TOKEN unset (gh PR landing will fail)"

# Activate the loom dev env (cargo/rustc/clippy + dev tools on PATH) and persist
# its exports to the profile. With BUILDBUDDY_API_KEY now present, its buck2 builds
# go over remote execution (fast, action-cache backed). Non-fatal: the code-health
# routines themselves only need `buck2 run //tools:...`, which works without this —
# so a failure here does not block the session. Remove this block (or background
# it) if session startup is too slow for a census-only routine.
if [ -d "$REPO/tools" ]; then
  ( cd "$REPO" && ./tools/env.sh ) >> "$PROFILE" 2>/tmp/loom-env.log || \
    echo "WARN: tools/env.sh activation failed (non-fatal); see /tmp/loom-env.log"
fi

echo "loom cloud session ready (REMOTE_ENV=true): buck2 + gh + remote execution configured"
