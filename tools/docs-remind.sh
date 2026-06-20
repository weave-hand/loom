#!/usr/bin/env bash
# Stop hook: NO-OP unless the current branch added/changed a spec or plan but did
# NOT touch any of the three registers — then print a one-line advisory nudge to
# run loom-docs-update. Advisory only; always exits 0 (never blocks a stop).
# Mirrors tools/cloud-session-start.sh's "inert unless" shape.
set -euo pipefail

branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo HEAD)"
if [ "$branch" = "main" ] || [ "$branch" = "HEAD" ]; then exit 0; fi

# Files changed on this branch relative to main (committed + working tree).
base="$(git merge-base HEAD main 2>/dev/null || true)"
[ -n "$base" ] || exit 0
changed="$( { git diff --name-only "$base"...HEAD; git diff --name-only; } | sort -u )"

# if-forms (not `&& exit` / `|| exit` chains) so the "should nudge" path isn't
# killed by set -e when the register grep finds no match.
if ! printf '%s\n' "$changed" | grep -qE '^docs/superpowers/(specs|plans)/'; then exit 0; fi
if printf '%s\n' "$changed" | grep -qE '^docs/(ROADMAP|FUTURE|ISSUES)\.md$'; then exit 0; fi

echo "📋 This branch touched a spec/plan but no register. Run the loom-docs-update skill to reconcile docs/{ROADMAP,FUTURE,ISSUES}.md before finishing." >&2
exit 0
