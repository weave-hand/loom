---
name: loom-duplication-fix
description: Remediate a duplication family from the loom-duplication census by extracting a shared abstraction (helper/fixture/test harness) under TDD and opening a PR for human review (NOT auto-merged). Use when asked to fix/dedupe duplication, extract a shared helper, or act on the duplication register. Auto-triages the highest-value hub (preferring lower-risk test-only families), unifies the duplicated blocks behavior-preservingly, proves buck2 tests stay green AND the duplication dropped, then opens a review PR and captures feedback.
---

Remediate ONE duplication family (a hub file + its duplication partners) by
extracting a shared abstraction, and open a PR **for human review — never
auto-merged**. Gated workflow: triage → characterize → extract under TDD → prove
behavior unchanged AND duplication gone → open review PR. If any gate fails, STOP
and report. Use `superpowers:test-driven-development` discipline.

## BLOCK A — triage (run verbatim)

```bash
set -euo pipefail
SKILL=.claude/skills/loom-duplication-fix
BASELINE=docs/code-health/duplication-baseline.json
ROOT="$PWD/"
git ls-files 'src/**/*.rs' > /tmp/dupfix-files.txt
buck2 run -v0 //tools:lucidshark-duplo -- /tmp/dupfix-files.txt --json -m 20 --baseline "$ROOT$BASELINE" > /tmp/dupfix.json 2>/tmp/dupfix-err.txt || true
[ -s /tmp/dupfix.json ] || echo '{"duplicates":[]}' > /tmp/dupfix.json
buck2 run -v0 //tools:jq -- -f "$ROOT$SKILL/triage.jq" /tmp/dupfix.json > /tmp/dupfix-themes.json
echo "Top duplication hubs (value-ranked; test_only families are lower risk):"
buck2 run -v0 //tools:jq -- -r '.[0:8][] | "- \(.file)  (\(.total_lines) dup-lines, \(.pair_count) pairs, test_only=\(.test_only))\n    partners: \((.partners|length))  largest pair: \(.pairs[0].lines) lines"' /tmp/dupfix-themes.json
```

## Steps

1. **Triage.** Run BLOCK A. It writes ranked hubs to `/tmp/dupfix-themes.json` and
   prints the top 8 (the committed baseline already suppresses accepted pairs). If
   the user named a target file/family, use it.
2. **Select with RISK judgment.** Pick the highest-value hub whose sites can be
   unified without changing observable behavior. **Prefer `test_only:true` families
   first** — they're the safest big wins (the hub + all partners are test files).
   Treat the hub plus its `partners` (and, by inspection of `/tmp/dupfix-themes.json`,
   transitively connected files) as ONE family. Announce the family + one sentence why.
3. **Characterize (guardrail).** Run the family's `rust_test` targets green first
   (`buck2 test <targets> > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`).
   If coverage is thin (rare for test files, possible for prod code), add
   characterization tests first. **If the family can't be safely covered, STOP and report.**
4. **Extract under TDD.** Pull the duplicated block into ONE well-named unit and call
   it from each former site:
   - Test families → a shared support module (e.g. `tests/common/mod.rs` or a fixture
     builder) wired per CLAUDE.md's `rust_test` conventions (NO inline tests; fixture
     tests use `loom_fixture_test`).
   - Production code → a private helper or a small shared function/trait.
   Keep observable behavior identical. Re-run the family's tests green as you go. One family only.
5. **Double-verify (hard gates).**
   - Behavior: `buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log` — green, or revert + STOP.
   - Improvement: re-run duplo on the changed files
     (`buck2 run -v0 //tools:lucidshark-duplo -- <changed files> --json -m 20 --baseline docs/code-health/duplication-baseline.json`)
     and confirm the family's pairs are GONE (or materially fewer lines). If not improved → do NOT open a PR; report.
6. **Capture feedback** (may produce nothing — do not invent):
   - **Baseline/waiver:** if some remaining duplication is intentional/accepted, refresh
     `docs/code-health/duplication-baseline.json` via duplo `--save-baseline` (native format) so the census stops flagging it.
   - **CLAUDE.md:** if the extraction established a shared harness/convention, add a concise note pointing future tests/code at it.
   - **Skill suggestion:** if a recurring need surfaced, note it for the PR body + run-end report. Never create/edit a skill here.
7. **Prepare PR text, then land.** Write `/tmp/dupfix-title.txt`
   (`refactor(<area>): extract shared <thing>, dedupe <family>`, ≤72 chars) and
   `/tmp/dupfix-pr.md` (the family, before/after dup-lines from step 5, the green
   `buck2 test //src/...` result, any baseline/CLAUDE.md changes, any skill
   suggestions). Run BLOCK B with a slug. Report the PR URL as OPEN for review (NOT merged).

## BLOCK B — open a review PR (run verbatim; NEVER merges)

```bash
set -euo pipefail
SLUG="${1:?usage: BLOCK B <slug>, e.g. graph-e2e-dedup}"
BRANCH="fix/code-health-$SLUG"
git config --get user.email >/dev/null 2>&1 || git config user.email "code-health-bot@users.noreply.github.com"
git config --get user.name  >/dev/null 2>&1 || git config user.name  "code-health-bot"
git switch -C "$BRANCH"
git add -A
git commit --no-verify -m "$(cat /tmp/dupfix-title.txt)" -m "$(cat /tmp/dupfix-pr.md)"
git push --no-verify -f -u origin "$BRANCH"
PR_STATE="$(gh pr view "$BRANCH" --json state -q .state 2>/dev/null || echo NONE)"
if [ "$PR_STATE" != "OPEN" ]; then
  gh pr create --base main --head "$BRANCH" --title "$(cat /tmp/dupfix-title.txt)" --body-file /tmp/dupfix-pr.md
fi
URL="$(gh pr view "$BRANCH" --json url -q .url)"
echo "PR (open for review, NOT merged): $URL"
gh pr checks "$BRANCH" --watch || echo "NOTE: CI not green — see $URL"
echo "Leaving PR open for human review: $URL"
```
