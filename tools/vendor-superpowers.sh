#!/usr/bin/env bash
# Vendor the superpowers skills from a fork into .claude/skills/ as committed
# project skills.
#
# Why vendor instead of installing the marketplace plugin: in Claude Code
# web/cloud sessions the ephemeral container does NOT install marketplace
# plugins (the `claude-plugins-official` catalog is never cloned, so
# `enabledPlugins` silently no-ops), and the cloud git proxy 403s `git clone`
# of any repo other than this one. Skill discovery is also exactly one level
# deep with no symlink support, so the skill folders must sit physically as
# direct children of .claude/skills/. Vendoring committed files is the only
# approach that loads reliably in cloud.
#
# Re-run to refresh: downloads the fork tarball over plain HTTPS (works in
# cloud, unlike `git clone`), then syncs skills/* into .claude/skills/, pruning
# any it previously vendored that upstream removed. It NEVER touches loom's own
# skills — only directories tracked in .claude/skills/.superpowers.manifest.
#
# Usage:
#   tools/vendor-superpowers.sh [--repo owner/name] [--ref branch-or-tag]
# Defaults: --repo weave-hand/superpowers --ref main

set -euo pipefail

REPO="weave-hand/superpowers"
REF="main"
while [ $# -gt 0 ]; do
  case "$1" in
    --repo) REPO="$2"; shift 2 ;;
    --ref)  REF="$2";  shift 2 ;;
    -h|--help) sed -n '2,21p' "$0"; exit 0 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SKILLS_DIR="$ROOT/.claude/skills"
MANIFEST="$SKILLS_DIR/.superpowers.manifest"

# Normalize a vendored text file so loom's repo-wide `trailing-whitespace` and
# `end-of-file-fixer` prek hooks (run by CI's lint job) stay green: strip
# per-line trailing whitespace and end with exactly one newline. Deterministic,
# so refreshes produce stable diffs; binary/empty files are left untouched.
normalize() {
  local f="$1" content
  grep -Iq . "$f" 2>/dev/null || return 0
  content="$(sed 's/[[:space:]]*$//' "$f")"
  printf '%s\n' "$content" > "$f"
}

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

echo "→ downloading $REPO@$REF …"
curl -fsSL --max-time 120 -o "$tmp/sp.tgz" \
  "https://github.com/$REPO/archive/refs/heads/$REF.tar.gz" \
  || curl -fsSL --max-time 120 -o "$tmp/sp.tgz" \
       "https://api.github.com/repos/$REPO/tarball/$REF"

mkdir -p "$tmp/sp"
tar xzf "$tmp/sp.tgz" -C "$tmp/sp" --strip-components=1
[ -d "$tmp/sp/skills" ] || { echo "no skills/ dir in $REPO@$REF" >&2; exit 1; }

# best-effort commit sha for provenance
sha="$(curl -fsSL --max-time 30 "https://api.github.com/repos/$REPO/commits/$REF" 2>/dev/null \
  | grep -m1 '"sha"' | sed -E 's/.*"sha"[[:space:]]*:[[:space:]]*"([0-9a-f]+)".*/\1/' || true)"

mkdir -p "$SKILLS_DIR"

# previously-vendored skill names (manifest, ignoring comment/blank lines)
prev=()
if [ -f "$MANIFEST" ]; then
  while IFS= read -r line; do
    case "$line" in ''|\#*) continue ;; esac
    prev+=("$line")
  done < "$MANIFEST"
fi
was_vendored() { local n="$1" p; for p in "${prev[@]:-}"; do [ "$p" = "$n" ] && return 0; done; return 1; }

# prune previously-vendored skills that upstream removed
for name in "${prev[@]:-}"; do
  [ -n "$name" ] || continue
  if [ ! -d "$tmp/sp/skills/$name" ] && [ -d "$SKILLS_DIR/$name" ]; then
    echo "  - pruning removed skill: $name"
    rm -rf "${SKILLS_DIR:?}/$name"
  fi
done

# copy current skills, never clobbering a loom-owned (non-vendored) directory
current=()
for d in "$tmp"/sp/skills/*/; do
  name="$(basename "$d")"
  if [ -d "$SKILLS_DIR/$name" ] && ! was_vendored "$name"; then
    echo "  ! refusing to overwrite loom-owned skill: $name (skipped)" >&2
    continue
  fi
  rm -rf "${SKILLS_DIR:?}/$name"
  cp -a "$d" "$SKILLS_DIR/$name"
  while IFS= read -r f; do normalize "$f"; done < <(find "$SKILLS_DIR/$name" -type f)
  current+=("$name")
  echo "  + vendored: $name"
done

# rewrite manifest
{
  echo "# superpowers skills vendored into .claude/skills/ — managed by tools/vendor-superpowers.sh"
  echo "# source: $REPO@$REF${sha:+ ($sha)}"
  echo "# refreshed: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf '%s\n' "${current[@]}" | sort
} > "$MANIFEST"

echo "✓ vendored ${#current[@]} superpowers skills into .claude/skills/ (manifest: ${MANIFEST#"$ROOT"/})"
