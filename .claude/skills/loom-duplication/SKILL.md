---
name: loom-duplication
description: Generate or refresh the duplication register at docs/code-health/duplication.md using the hermetic lucidshark-duplo, landing any change as a PR against main that merges on green CI. Use when asked to refresh the duplication register, find duplicate code / abstraction opportunities, run the duplication routine, or on a schedule. The census table is deterministic (duplo JSON → jq render); accepted pairs in duplication-baseline.json are suppressed. Pass `diff` to analyze only the current branch's changed files and print to the terminal without committing.
---

Refresh the duplication register at `docs/code-health/duplication.md` and land any
change as a PR against `main` merged on green CI. The census is a pure function of
`lucidshark-duplo --json` rendered by `render.jq` — do NOT hand-edit between the
`census` markers. Accepted duplication is recorded in
`docs/code-health/duplication-baseline.json` and suppressed at render time via
`render.jq`'s `--slurpfile baseline`.

## BLOCK A — run + render + detect change (run verbatim)

```bash
set -euo pipefail
SKILL=.claude/skills/loom-duplication
REG=docs/code-health/duplication.md
BASELINE=docs/code-health/duplication-baseline.json
MODE="${1:-full}"
ROOT="$PWD/"

duplo() { buck2 run -v0 //tools:lucidshark-duplo -- "$@"; }
jqh()   { buck2 run -v0 //tools:jq -- "$@"; }

if [ "$MODE" = diff ]; then
  duplo --git --changed-only --json -m 20 --baseline "$ROOT$BASELINE" > /tmp/dup.json 2>/tmp/dup-err.txt || true
  [ -s /tmp/dup.json ] || echo '{"duplicates":[]}' > /tmp/dup.json
else
  git ls-files 'src/**/*.rs' > /tmp/dup-files.txt
  duplo /tmp/dup-files.txt --json -m 20 --baseline "$ROOT$BASELINE" > /tmp/dup.json 2>/tmp/dup-err.txt || true
  [ -s /tmp/dup.json ] || { cat /tmp/dup-err.txt >&2; exit 1; }
fi

jqh -rf "$ROOT$SKILL/render.jq" --slurpfile baseline "$ROOT$BASELINE" /tmp/dup.json > /tmp/dup-census.md

if [ "$MODE" = diff ]; then
  cat /tmp/dup-census.md
  echo "DUPLICATION_RESULT=diff-mode"
  exit 0
fi

extract() { sed -n '/<!-- census:begin -->/,/<!-- census:end -->/p' "$1" 2>/dev/null || true; }
if [ -f "$REG" ] && diff -q <(extract "$REG") /tmp/dup-census.md >/dev/null; then
  echo "DUPLICATION_RESULT=nochange"
else
  echo "DUPLICATION_RESULT=changed"
fi
```

## Steps

1. Run BLOCK A with the requested `MODE` (`full` unless the user said `diff`).
2. `DUPLICATION_RESULT=diff-mode`: table printed; summarize the largest new pair
   and STOP.
3. `DUPLICATION_RESULT=nochange`: report "duplication register already current"
   and STOP.
4. `DUPLICATION_RESULT=changed`: assemble `docs/code-health/duplication.md` —
   H1 `# Code duplication register`, `_As of <short-sha>._`, a
   `<!-- preamble:begin -->`…`<!-- preamble:end -->` region with a ≤10-bullet
   "Notable changes this run" (pairs added/resolved by file, the delta only),
   a blank line, then `/tmp/dup-census.md` verbatim. One trailing newline, no
   trailing whitespace. Commit SHA lives ONLY in the preamble.
5. Write `/tmp/dup-title.txt` (`docs(code-health): <duplication change>`, ≤72 chars)
   and `/tmp/dup-body.md` (2-6 bullets). Run
   `buck2 run //tools:prek -- run --all-files`, leave any hook fixes staged for
   BLOCK B's commit, then run BLOCK B.

## BLOCK B — commit, PR, watch CI, merge on green (run verbatim; only when DUPLICATION_RESULT=changed)

```bash
set -euo pipefail
BRANCH=bot/code-health-duplication
git config --get user.email >/dev/null 2>&1 || git config user.email "code-health-bot@users.noreply.github.com"
git config --get user.name  >/dev/null 2>&1 || git config user.name  "code-health-bot"
git switch -C "$BRANCH"
git add docs/code-health/duplication.md
git commit --no-verify -m "$(cat /tmp/dup-title.txt)" -m "$(cat /tmp/dup-body.md)"
git push --no-verify -f -u origin "$BRANCH"
PR_STATE="$(gh pr view "$BRANCH" --json state -q .state 2>/dev/null || echo NONE)"
if [ "$PR_STATE" != "OPEN" ]; then
  gh pr create --base main --head "$BRANCH" --title "$(cat /tmp/dup-title.txt)" --body-file /tmp/dup-body.md
fi
URL="$(gh pr view "$BRANCH" --json url -q .url)"
echo "PR: $URL"
if gh pr checks "$BRANCH" --watch --fail-fast; then
  gh pr merge "$BRANCH" --squash --delete-branch && echo "merged (CI green): $URL"
else
  echo "NOTE: CI not green — PR left open for review: $URL"
fi
```
