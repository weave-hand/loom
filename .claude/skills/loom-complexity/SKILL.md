---
name: loom-complexity
description: Generate or refresh the code-complexity register at docs/code-health/complexity.md using the hermetic rust-code-analysis-cli, landing any change as a PR against main that merges on green CI. Use when asked to refresh the complexity register, audit complexity hotspots, run the complexity routine, or on a schedule. The census table is deterministic (tool metrics → jq render); a bounded agent preamble notes notable changes. Pass `diff` to analyze only the current branch's changed files and print to the terminal without committing.
---

Refresh the complexity register at `docs/code-health/complexity.md` and land any
change as a PR against `main` that you merge once CI is green. The census table is
a pure function of `rust-code-analysis-cli` metrics rendered by `render.jq` — do
NOT hand-edit the region between the `census` markers. Modes: `full` (default —
whole `src/` tree, render + PR) and `diff` (changed `.rs` only, print, no commit).

## BLOCK A — run + render + detect change (run verbatim)

```bash
set -euo pipefail
SKILL=.claude/skills/loom-complexity
REG=docs/code-health/complexity.md
WAIVERS=docs/code-health/complexity-waivers.json
MODE="${1:-full}"
OUT="$(mktemp -d)"
ROOT="$PWD/"

rca() { buck2 run -v0 //tools:rust-code-analysis -- "$@"; }
jqh() { buck2 run -v0 //tools:jq -- "$@"; }

if [ "$MODE" = diff ]; then
  # Tolerate a missing `main` ref (detached HEAD / renamed default) — fall back
  # to working-tree changes only rather than aborting under `set -e`.
  BASE="$(git merge-base HEAD main 2>/dev/null || true)"
  PATHS="$( { [ -n "$BASE" ] && git diff --name-only "$BASE"...HEAD -- 'src/**/*.rs'; git diff --name-only -- 'src/**/*.rs'; } | sort -u )"
  [ -n "$PATHS" ] || { echo "no changed .rs files"; exit 0; }
else
  PATHS="src"
fi

# shellcheck disable=SC2086
rca -m -O json -p $PATHS -o "$OUT"

# Render the census region from every per-file JSON (slurp with -s).
FILES="$(find "$OUT" -name '*.json')"
# Guard the empty case: `jq -s` with zero file args reads stdin and would hang.
[ -n "$FILES" ] || { echo "rust-code-analysis produced no JSON output" >&2; exit 1; }
# shellcheck disable=SC2086
jqh -rf "$ROOT$SKILL/render.jq" --arg root "$ROOT" --slurpfile waivers "$ROOT$WAIVERS" -s $FILES > /tmp/cx-census.md

if [ "$MODE" = diff ]; then
  cat /tmp/cx-census.md
  echo "COMPLEXITY_RESULT=diff-mode"
  exit 0
fi

# Compare ONLY the census region against the committed register.
extract() { sed -n '/<!-- census:begin -->/,/<!-- census:end -->/p' "$1" 2>/dev/null || true; }
if [ -f "$REG" ] && diff -q <(extract "$REG") /tmp/cx-census.md >/dev/null; then
  echo "COMPLEXITY_RESULT=nochange"
else
  echo "COMPLEXITY_RESULT=changed"
fi
```

## Steps

1. Run BLOCK A with the requested `MODE` (`full` unless the user said `diff`).
2. If `COMPLEXITY_RESULT=diff-mode`: the table is already printed; summarize the
   top 3 hotspots in one line each and STOP (no commit).
3. If `COMPLEXITY_RESULT=nochange`: report "complexity register already current"
   and STOP. Do not open a PR.
4. If `COMPLEXITY_RESULT=changed`: assemble the new register:
   - Read the prior census region (`git show HEAD:docs/code-health/complexity.md`,
     if it exists) and `/tmp/cx-census.md`.
   - Write a **≤10-bullet** "Notable changes this run" preamble naming hotspots
     **added / resolved / worsened** by `file::function` — a summary of the delta
     only; introduce no findings not in the table. First run: one line.
   - Assemble `docs/code-health/complexity.md` as, in order: an H1
     `# Code complexity register`, a one-line `_As of <short-sha>._` (run
     `git rev-parse --short HEAD`), a `<!-- preamble:begin -->` … `<!-- preamble:end -->`
     region holding the bullets, a blank line, then `/tmp/cx-census.md` verbatim.
     End the file with exactly ONE trailing newline and no trailing whitespace.
   - The commit SHA lives ONLY in the preamble region, never in the census region,
     so identical findings always re-detect as `nochange`.
5. Run `buck2 run //tools:prek -- run --all-files` — it may fix trailing
   whitespace / EOF in `complexity.md` in place. Leave those fixes staged (do NOT
   make a separate commit); BLOCK B's `git add` + `git commit` captures them.
   THEN run BLOCK B.

Before running BLOCK B, write `/tmp/cx-title.txt` — one line,
`docs(code-health): <what changed in complexity>`, ≤72 chars — and
`/tmp/cx-body.md` — 2-6 bullets, the same delta as the preamble.

## BLOCK B — commit, PR, watch CI, merge on green (run verbatim; ONLY when COMPLEXITY_RESULT=changed)

```bash
set -euo pipefail
BRANCH=bot/code-health-complexity
git config --get user.email >/dev/null 2>&1 || git config user.email "code-health-bot@users.noreply.github.com"
git config --get user.name  >/dev/null 2>&1 || git config user.name  "code-health-bot"
git switch -C "$BRANCH"
git add docs/code-health/complexity.md
# --no-verify: skip loom's local commit-msg/pre-push hooks (buck2-build/test would
# stall the routine); conventional style is carried by the PR title -> squash commit.
git commit --no-verify -m "$(cat /tmp/cx-title.txt)" -m "$(cat /tmp/cx-body.md)"
git push --no-verify -f -u origin "$BRANCH"
PR_STATE="$(gh pr view "$BRANCH" --json state -q .state 2>/dev/null || echo NONE)"
if [ "$PR_STATE" != "OPEN" ]; then
  gh pr create --base main --head "$BRANCH" --title "$(cat /tmp/cx-title.txt)" --body-file /tmp/cx-body.md
fi
URL="$(gh pr view "$BRANCH" --json url -q .url)"
echo "PR: $URL"
if gh pr checks "$BRANCH" --watch --fail-fast; then
  gh pr merge "$BRANCH" --squash --delete-branch && echo "merged (CI green): $URL"
else
  echo "NOTE: CI not green — PR left open for review: $URL"
fi
```
