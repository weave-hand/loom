---
name: loom-complexity-fix
description: Remediate a high-complexity hotspot theme from the loom-complexity census by refactoring it under TDD and opening a PR for human review (NOT auto-merged). Use when asked to fix/reduce complexity, refactor a hotspot, or act on the complexity register. Auto-triages the worst tractable theme (one module's hotspots), characterizes it with tests, decomposes it behavior-preservingly, proves buck2 tests stay green AND the metric dropped, then opens a review PR and captures feedback (CLAUDE.md notes, waivers, skill suggestions).
---

Remediate ONE complexity theme (a module's hotspots) and open a PR **for human
review — never auto-merged** (code changes are judgment calls). The workflow is
deliberate and gated: triage → characterize → refactor under TDD → prove behavior
unchanged AND metric improved → open review PR. If any gate fails, STOP and report
rather than landing a questionable refactor. Use `superpowers:test-driven-development`
discipline throughout.

## BLOCK A — triage (run verbatim)

```bash
set -euo pipefail
SKILL=.claude/skills/loom-complexity-fix
ROOT="$PWD/"
OUT="$(mktemp -d)"
buck2 run -v0 //tools:rust-code-analysis -- -m -O json -p src -o "$OUT" >/dev/null
FILES="$(find "$OUT" -name '*.json')"
[ -n "$FILES" ] || { echo "no metrics produced" >&2; exit 1; }
# shellcheck disable=SC2086
buck2 run -v0 //tools:jq -- -f "$ROOT$SKILL/triage.jq" --arg root "$ROOT" -s $FILES > /tmp/cxfix-themes.json
echo "Top complexity themes (value-ranked; apply risk judgment in Steps):"
buck2 run -v0 //tools:jq -- -r '.[0:8][] | "- \(.file)  (theme_value \(.theme_value), \(.members|length) fn)\n" + (.members[0:3] | map("    \(.function)  cc=\(.cc) cog=\(.cog) mi=\(.mi) sloc=\(.sloc)") | join("\n"))' /tmp/cxfix-themes.json
```

## Steps

1. **Triage.** Run BLOCK A. It writes value-ranked themes to `/tmp/cxfix-themes.json`
   and prints the top 8. If the user named a target function/file, use it.
2. **Select with RISK judgment.** The ranking is value-only; YOU weigh risk:
   - Prefer a theme whose functions already have passing test coverage and a clear
     decomposition. Prefer leaf/utility/serialization code for early fixes.
   - Be cautious with core hot paths (commit/transaction/ACL/lineage) — only take
     them when coverage is strong. When in doubt, pick a safer lower-ranked theme.
   - Announce the chosen theme (file + functions) and one sentence of why.
   - Note: triage is waiver-unaware — already-waived functions still appear in the
     ranking; skip them (a fix won't remove them from the census).
3. **Characterize (guardrail).** Identify the `rust_test` targets covering the
   theme and run them green (`buck2 test <targets> > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`).
   If coverage is thin, ADD characterization tests first — a sibling `tests/<name>.rs`
   wired as a `rust_test` (per CLAUDE.md: NO inline `#[test]`; fixture tests use
   `loom_fixture_test`) capturing current behavior — and get them green. **If the
   theme cannot be safely covered, STOP and report** — do not refactor blind.
4. **Refactor under TDD.** Decompose: extract cohesive private helpers, replace
   nested conditionals with early returns / guard clauses / small dispatch, pull
   independent concerns into focused functions. Keep public signatures and observable
   behavior unchanged. Re-run the theme's tests green after each move. One theme only.
5. **Double-verify (hard gates).**
   - Behavior: `buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log` — must be green. If RED → revert the refactor and STOP/report.
   - Improvement: re-run the metric on the changed files — `O="$(mktemp -d)"; buck2 run -v0 //tools:rust-code-analysis -- -m -O json -p <changed files> -o "$O"` then feed its JSON through triage.jq with the SAME `-s` slurp pattern as BLOCK A (`buck2 run -v0 //tools:jq -- -f "$ROOT$SKILL/triage.jq" --arg root "$ROOT" -s $(find "$O" -name '*.json')`) — confirm the theme's functions are now BELOW threshold (gone from the ranked output). If they did NOT improve → do NOT open a PR; report what was tried.
6. **Capture feedback** (may legitimately produce nothing — do not invent output):
   - **Waiver:** if part of the theme is genuinely-irreducible complexity (a parser,
     a generated dispatch), add `{"file","function","reason"}` to
     `docs/code-health/complexity-waivers.json` so the census stops flagging it.
   - **CLAUDE.md:** if the refactor introduced a reusable pattern, add a concise note
     to the relevant CLAUDE.md section.
   - **Skill suggestion:** if a recurring automatable need surfaced, note it (you will
     put it in the PR body and the run-end report). Never create/edit a skill here.
7. **Prepare PR text, then land.** Write `/tmp/cxfix-title.txt`
   (`refactor(<area>): reduce complexity in <module>`, ≤72 chars) and
   `/tmp/cxfix-pr.md` (the theme, before/after metrics from step 5, the green
   `buck2 test //src/...` result, any CLAUDE.md/waiver changes, any skill
   suggestions). Then run BLOCK B with a short slug. Finally, report the PR URL and
   that it is OPEN for review (NOT merged), plus any skill suggestions.

## BLOCK B — open a review PR (run verbatim; NEVER merges)

```bash
set -euo pipefail
SLUG="${1:?usage: BLOCK B <slug>, e.g. handler-rs}"
BRANCH="fix/code-health-$SLUG"
git config --get user.email >/dev/null 2>&1 || git config user.email "code-health-bot@users.noreply.github.com"
git config --get user.name  >/dev/null 2>&1 || git config user.name  "code-health-bot"
git switch -C "$BRANCH"
# The branch should contain ONLY this theme's refactor (+ tests/waiver/CLAUDE.md).
# Caution: -A stages the whole tree — ensure no unrelated changes are present.
git add -A
# --no-verify: skip loom's local commit-msg/pre-push hooks (buck2-build/test would
# stall the routine); conventional style is carried by the PR title. `-m`+`-m`, NOT
# `-m`+`-F` (git rejects mixing those); a `-m` arg accepts the multi-line body fine.
git commit --no-verify -m "$(cat /tmp/cxfix-title.txt)" -m "$(cat /tmp/cxfix-pr.md)"
git push --no-verify -f -u origin "$BRANCH"
PR_STATE="$(gh pr view "$BRANCH" --json state -q .state 2>/dev/null || echo NONE)"
if [ "$PR_STATE" != "OPEN" ]; then
  gh pr create --base main --head "$BRANCH" --title "$(cat /tmp/cxfix-title.txt)" --body-file /tmp/cxfix-pr.md
fi
URL="$(gh pr view "$BRANCH" --json url -q .url)"
echo "PR (open for review, NOT merged): $URL"
# Report CI status but DO NOT merge — a human reviews code changes.
gh pr checks "$BRANCH" --watch || echo "NOTE: CI not green — see $URL"
echo "Leaving PR open for human review: $URL"
```
