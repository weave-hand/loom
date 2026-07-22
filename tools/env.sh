#!/usr/bin/env bash
# Print `export` lines that activate loom's hermetic toolchain + dev tools in the
# current shell:
#
#     eval "$(./tools/env.sh)"      # manual
#
# or let direnv run it via .envrc. Side effects (builds + symlink management)
# print to stderr; ONLY `export …` lines go to stdout so `eval` stays clean.
#
# What it does: resolves and builds //tools:rust-host-toolchain and the dev-tool
# binaries (one-time, cached), (re)points symlinks under .loom/bin, and emits
# PATH/LD_LIBRARY_PATH. It does NOT build first-party //src — run `loom-refresh`
# for those (their symlinks persist in .loom/bin between shells).
set -euo pipefail

# BASH_SOURCE[0] (not $0) so root resolution also works if the script is sourced.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
BIN="$REPO_ROOT/.loom/bin"
mkdir -p "$BIN"

log() { printf 'loom-env: %s\n' "$*" >&2; }

# Resolve a target's default output to an absolute path, building if needed.
# `|| true` keeps `set -e` from killing us on a build error — we check $rel.
resolve() {
    local rel
    rel="$( { buck2 build "$1" --show-output 2>/dev/null || true; } | awk 'NR==1{print $2}')"
    if [ -z "$rel" ]; then log "failed to resolve $1"; return 1; fi
    printf '%s/%s\n' "$REPO_ROOT" "$rel"
}

log "building toolchain + dev tools (cached after first run)…"
TOOLCHAIN="$(resolve root//tools:rust-host-toolchain)"

# Dev tools: concrete per-arch genrule outputs (real, symlink-safe binaries).
# The command_alias trampolines resolve a sibling via $0 and break when
# symlinked, so we point at the genrules directly. rustfmt is its wrapper genrule.
# (Arch is hardcoded x86_64-linux — matches the toolchain's x86_64-only scope;
# add aarch64 variants here if that scope ever widens.)
declare -A TOOLS=(
    [reindeer]="root//tools:reindeer-x86_64-linux"
    [prek]="root//tools:prek-x86_64-linux"
    [btd]="root//tools:btd-bin-x86_64"
    [supertd]="root//tools:supertd-bin-x86_64"
    [rustfmt]="root//tools:rustfmt"
    [rust-analyzer]="root//tools:rust-analyzer"
    [rust-project]="root//tools:rust-project-x86_64-linux"
    [jq]="root//tools:jq-x86_64-linux"
    [rust-code-analysis-cli]="root//tools:rust-code-analysis-x86_64-linux"
    [lucidshark-duplo]="root//tools:lucidshark-duplo-x86_64-linux"
    [muntjac]="root//tools:muntjac-x86_64-linux"
    [uv]="root//tools:uv-x86_64-linux"
)
for name in "${!TOOLS[@]}"; do
    if p="$(resolve "${TOOLS[$name]}")"; then ln -sfn "$p" "$BIN/$name"; fi
done

# Hermetic python (python-build-standalone) for buckify + ad-hoc dev use. The
# http_archive output is the extracted dist dir; its relocatable bin/python3
# finds its sibling lib/. Symlink both python3 and python so either name works.
if py="$(resolve toolchains//:cpython_archive)"; then
    ln -sfn "$py/bin/python3" "$BIN/python3"
    ln -sfn "$py/bin/python3" "$BIN/python"
fi

# loom-refresh helper (manages first-party binary symlinks).
ln -sfn "$REPO_ROOT/tools/loom-refresh" "$BIN/loom-refresh"

# Drop any prior loom entries from a `:`-separated path var, so re-activation
# neither grows it nor leaves stale (post-`buck2 clean`) toolchain dirs behind.
_loom_strip() {
    printf '%s' "$1" | awk -v RS=: '
        $0 != "" && $0 !~ /\/\.loom\/bin$/ && $0 !~ /\/buck-out\/.*__rust-host-toolchain__\// {
            printf "%s%s", sep, $0; sep=":"
        }'
}

CLEAN_PATH="$(_loom_strip "${PATH}")"
CLEAN_LDLP="$(_loom_strip "${LD_LIBRARY_PATH:-}")"

# Emit ONLY export lines on stdout (%q-quoted so eval is safe).
printf 'export PATH=%q\n' "$BIN:$TOOLCHAIN/bin${CLEAN_PATH:+:$CLEAN_PATH}"
printf 'export LD_LIBRARY_PATH=%q\n' "$TOOLCHAIN/lib${CLEAN_LDLP:+:$CLEAN_LDLP}"
# Marker for shell-prompt / tooling detection ("are we in the loom env?"). Not a
# re-activation guard — activation is always safe to re-run (it re-points symlinks).
printf 'export LOOM_ENV_ACTIVE=%q\n' "1"
log "ready — cargo/rustc/reindeer/prek/btd/supertd/rustfmt/rust-analyzer/rust-project on PATH; 'cargo clippy' for clippy; run loom-refresh for //src binaries"
