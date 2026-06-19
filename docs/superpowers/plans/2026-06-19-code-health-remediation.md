# Code-health remediation skills — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build two on-demand guided-refactoring skills — `loom-complexity-fix` and `loom-duplication-fix` — that auto-triage the code-health census, refactor a batched theme under TDD, prove behavior is unchanged and the metric dropped, and open a PR **for human review (no auto-merge)**, capturing feedback (CLAUDE.md edits, waiver updates, skill suggestions) along the way.

**Architecture:** Each skill is a `SKILL.md` routine with three parts: a deterministic **BLOCK A (triage)** — runs the hermetic tool, scores/ranks candidate *themes* via a committed `triage.jq` (golden-tested) — judgment-heavy **Steps** prose for the characterize→refactor→double-verify→capture workflow (leaning on `superpowers:test-driven-development`), and a **BLOCK B (land-for-review)** bash that opens but does NOT merge a PR. The improvement gate reuses the census skills' `diff` mode.

**Tech Stack:** buck2, the already-vendored `//tools:jq` / `//tools:rust-code-analysis` / `//tools:lucidshark-duplo`, the `loom-complexity` / `loom-duplication` census skills, `gh` CLI, bash.

**Spec:** `docs/superpowers/specs/2026-06-19-code-health-remediation-design.md`

**Pre-resolved facts (validated during planning — do not re-derive):**
- The two `triage.jq` scripts below were run against loom's real `src/` and produce sensible rankings: complexity surfaces `query-api/handler.rs` (cc=42) as the top theme; duplication surfaces the query-api `*_e2e.rs` family (`test_only:true`, ~529 dup-lines) as the top hub. Both fixture goldens below are byte-exact captured output.
- `buck2 build -v0 --show-simple-output //tools:jq` prints a runnable jq path; the golden tests use `buck2 run -v0 //tools:jq` for robustness (paths absolute, so cwd is irrelevant). Never pipe `buck2 test`/`bxl` through tail/head — redirect to a file and grep.
- The fix skills DEFER the first real remediation run to post-merge (running the skill is "using" it and produces a human-reviewed code PR). This plan builds + unit-validates the skills; it does NOT land a refactor PR.

---

## File structure

| Path | Responsibility |
|---|---|
| `.claude/skills/loom-complexity-fix/triage.jq` (create) | Deterministic complexity theme ranker. |
| `.claude/skills/loom-complexity-fix/tests/` (create) | `fixture.json`, `golden.json`, `run.sh`. |
| `.claude/skills/loom-complexity-fix/SKILL.md` (create) | Triage + refactor workflow + land-for-review. |
| `.claude/skills/loom-duplication-fix/triage.jq` (create) | Deterministic duplication hub ranker. |
| `.claude/skills/loom-duplication-fix/tests/` (create) | `fixture.json`, `golden.json`, `run.sh`. |
| `.claude/skills/loom-duplication-fix/SKILL.md` (create) | Triage + refactor workflow + land-for-review. |

All work on branch `feat/code-health-remediation` (already created; the spec is committed there).

---

## Task 1: loom-complexity-fix triage + golden test

**Files:**
- Create: `.claude/skills/loom-complexity-fix/triage.jq`
- Create: `.claude/skills/loom-complexity-fix/tests/fixture.json`, `tests/golden.json`, `tests/run.sh`

- [ ] **Step 1 (test first): create `tests/fixture.json`** (two over-threshold functions in two files):

```json
[
  {"name":"src/a.rs","kind":"unit","spaces":[
    {"name":"simple","kind":"function","start_line":10,"metrics":{"cyclomatic":{"sum":3.0},"cognitive":{"sum":1.0},"mi":{"mi_visual_studio":75.0},"loc":{"sloc":12.0}}},
    {"name":"gnarly","kind":"function","start_line":40,"metrics":{"cyclomatic":{"sum":23.0},"cognitive":{"sum":31.0},"mi":{"mi_visual_studio":14.2},"loc":{"sloc":180.0}}}
  ]},
  {"name":"src/b.rs","kind":"unit","spaces":[
    {"name":"waived_big","kind":"function","start_line":5,"metrics":{"cyclomatic":{"sum":40.0},"cognitive":{"sum":50.0},"mi":{"mi_visual_studio":5.0},"loc":{"sloc":300.0}}}
  ]}
]
```

- [ ] **Step 2: create `tests/golden.json`** (byte-exact validated output — note `src/b.rs` ranks first by `theme_value`):

```json
[
  {
    "file": "src/b.rs",
    "theme_value": 79,
    "members": [
      {
        "function": "waived_big",
        "line": 5,
        "cc": 40.0,
        "cog": 50.0,
        "mi": 5,
        "sloc": 300.0,
        "value": 79
      }
    ]
  },
  {
    "file": "src/a.rs",
    "theme_value": 31.4,
    "members": [
      {
        "function": "gnarly",
        "line": 40,
        "cc": 23.0,
        "cog": 31.0,
        "mi": 14.2,
        "sloc": 180.0,
        "value": 31.4
      }
    ]
  }
]
```

- [ ] **Step 3: create `tests/run.sh`:**

```bash
#!/usr/bin/env bash
# Golden test for the complexity triage ranker. Run from repo root:
#   bash .claude/skills/loom-complexity-fix/tests/run.sh
set -euo pipefail
D="$(cd "$(dirname "$0")" && pwd)"
GOT="$(LC_ALL=C buck2 run -v0 //tools:jq -- -f "$D/../triage.jq" --arg root "/workspace/" "$D/fixture.json")"
if diff -u "$D/golden.json" <(printf '%s\n' "$GOT"); then
  echo "PASS: complexity triage matches golden"
else
  echo "FAIL: complexity triage drifted from golden"; exit 1
fi
```

- [ ] **Step 4: run the test to verify it FAILS** (no triage.jq yet)

Run: `bash .claude/skills/loom-complexity-fix/tests/run.sh`
Expected: FAIL — jq cannot open `triage.jq`.

- [ ] **Step 5: create `triage.jq`** (validated during planning — reproduce EXACTLY):

```jq
def t_cc: 15;
def t_cog: 15;
def t_mi: 20;
def t_sloc: 100;
def r2: (.*100|round)/100;
[ .[]
  | (.name | ltrimstr($root)) as $file
  | .. | objects | select(.kind=="function")
  | { file:$file, function:(.name // "<anon>"), line:(.start_line // 0),
      cc:(.metrics.cyclomatic.sum // 0), cog:(.metrics.cognitive.sum // 0),
      mi:(.metrics.mi.mi_visual_studio // 100), sloc:(.metrics.loc.sloc // 0) }
  | . + { value: (
        (if .cc  > t_cc   then .cc  - t_cc          else 0 end)
      + (if .cog > t_cog  then .cog - t_cog          else 0 end)
      + (if .sloc > t_sloc then (.sloc - t_sloc) / 50 else 0 end)
      + (if .mi  < t_mi   then (t_mi - .mi)          else 0 end) ) }
]
| map(select(.value > 0))
| group_by(.file)
| map({ file: .[0].file,
        theme_value: ((map(.value) | add) | r2),
        members: ( sort_by(-.value)
                   | map({ function, line, cc, cog, mi:(.mi|r2), sloc, value:(.value|r2) }) ) })
| sort_by(-.theme_value)
```

- [ ] **Step 6: run the test to verify it PASSES**

Run: `bash .claude/skills/loom-complexity-fix/tests/run.sh`
Expected: `PASS: complexity triage matches golden`

- [ ] **Step 7: sanity-check on real src** (not a committed test — just confirm ranking is sensible)

Run:
```bash
OUT=$(mktemp -d); ROOT="$PWD/"
buck2 run -v0 //tools:rust-code-analysis -- -m -O json -p src -o "$OUT" >/dev/null 2>&1
F=$(find "$OUT" -name '*.json')
buck2 run -v0 //tools:jq -- -f .claude/skills/loom-complexity-fix/triage.jq --arg root "$ROOT" -s $F \
  | buck2 run -v0 //tools:jq -- -c '.[0:3][] | {file, theme_value, top:(.members[0].function)}'
```
Expected: `src/services/query-api/src/handler.rs` ranks at or near the top (`resolve_chain`).

- [ ] **Step 8: commit**

```bash
git add .claude/skills/loom-complexity-fix/triage.jq .claude/skills/loom-complexity-fix/tests/
git commit --no-verify -m "feat(loom-complexity-fix): deterministic complexity triage ranker + golden test"
```

---

## Task 2: loom-complexity-fix SKILL.md

**Files:**
- Create: `.claude/skills/loom-complexity-fix/SKILL.md`

- [ ] **Step 1: write frontmatter + overview**

```markdown
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
```

- [ ] **Step 2: append BLOCK A (triage)**

````markdown
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
````

- [ ] **Step 3: append the Steps prose**

```markdown
## Steps

1. **Triage.** Run BLOCK A. It writes value-ranked themes to `/tmp/cxfix-themes.json`
   and prints the top 8. If the user named a target function/file, use it.
2. **Select with RISK judgment.** The ranking is value-only; YOU weigh risk:
   - Prefer a theme whose functions already have passing test coverage and a clear
     decomposition. Prefer leaf/utility/serialization code for early fixes.
   - Be cautious with core hot paths (commit/transaction/ACL/lineage) — only take
     them when coverage is strong. When in doubt, pick a safer lower-ranked theme.
   - Announce the chosen theme (file + functions) and one sentence of why.
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
   - Improvement: re-run the metric on the changed files —
     `buck2 run -v0 //tools:rust-code-analysis -- -m -O json -p <changed files> -o "$(mktemp -d)"` then triage.jq — and confirm the theme's functions are now BELOW threshold (gone from the ranked output). If they did NOT improve → do NOT open a PR; report what was tried.
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
```

- [ ] **Step 4: append BLOCK B (land for review — NO merge)**

````markdown
## BLOCK B — open a review PR (run verbatim; NEVER merges)

```bash
set -euo pipefail
SLUG="${1:?usage: BLOCK B <slug>, e.g. handler-rs}"
BRANCH="fix/code-health-$SLUG"
git config --get user.email >/dev/null 2>&1 || git config user.email "code-health-bot@users.noreply.github.com"
git config --get user.name  >/dev/null 2>&1 || git config user.name  "code-health-bot"
git switch -C "$BRANCH"
# The branch should contain ONLY this theme's refactor (+ tests/waiver/CLAUDE.md).
git add -A
git commit --no-verify -m "$(cat /tmp/cxfix-title.txt)" -F /tmp/cxfix-pr.md
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
````

The skill must be markdown-lint-clean (one trailing newline, no trailing whitespace).

- [ ] **Step 5: validate BLOCK A on real src (dry — no code change)**

Copy BLOCK A's bash into `/tmp/cxa.sh` and run `bash /tmp/cxa.sh`.
Expected: prints a ranked theme list with `query-api/src/handler.rs` near the top.
Then `git status --porcelain` → empty (triage writes only `/tmp` + a temp dir; no repo change, no branch switch).

- [ ] **Step 6: confirm the skill is registered & does NOT auto-merge**

Run: `grep -c 'gh pr merge' .claude/skills/loom-complexity-fix/SKILL.md`
Expected: `0` (BLOCK B never merges).

- [ ] **Step 7: commit**

```bash
git add .claude/skills/loom-complexity-fix/SKILL.md
git commit --no-verify -m "feat(loom-complexity-fix): triage + TDD refactor workflow + review-PR landing"
```

---

## Task 3: loom-duplication-fix triage + golden test

**Files:**
- Create: `.claude/skills/loom-duplication-fix/triage.jq`
- Create: `.claude/skills/loom-duplication-fix/tests/fixture.json`, `tests/golden.json`, `tests/run.sh`

- [ ] **Step 1 (test first): create `tests/fixture.json`:**

```json
{"duplicates":[
  {"line_count":40,"file1":{"path":"src/x/tests/a_e2e.rs","start_line":10,"end_line":50},"file2":{"path":"src/x/tests/b_e2e.rs","start_line":20,"end_line":60}},
  {"line_count":25,"file1":{"path":"src/x/tests/a_e2e.rs","start_line":70,"end_line":95},"file2":{"path":"src/x/tests/c_e2e.rs","start_line":5,"end_line":30}},
  {"line_count":22,"file1":{"path":"src/y/src/prod.rs","start_line":1,"end_line":23},"file2":{"path":"src/y/src/prod2.rs","start_line":1,"end_line":23}}
]}
```

- [ ] **Step 2: create `tests/golden.json`** (byte-exact validated output — `a_e2e.rs` hub ranks first; `test_only` flags correctly):

```json
[
  {
    "file": "src/x/tests/a_e2e.rs",
    "total_lines": 65,
    "pair_count": 2,
    "partners": [
      "src/x/tests/b_e2e.rs",
      "src/x/tests/c_e2e.rs"
    ],
    "test_only": true,
    "pairs": [
      {
        "lines": 40,
        "a": "src/x/tests/a_e2e.rs:10-50",
        "b": "src/x/tests/b_e2e.rs:20-60"
      },
      {
        "lines": 25,
        "a": "src/x/tests/a_e2e.rs:70-95",
        "b": "src/x/tests/c_e2e.rs:5-30"
      }
    ]
  },
  {
    "file": "src/x/tests/b_e2e.rs",
    "total_lines": 40,
    "pair_count": 1,
    "partners": [
      "src/x/tests/a_e2e.rs"
    ],
    "test_only": true,
    "pairs": [
      {
        "lines": 40,
        "a": "src/x/tests/b_e2e.rs:20-60",
        "b": "src/x/tests/a_e2e.rs:10-50"
      }
    ]
  },
  {
    "file": "src/x/tests/c_e2e.rs",
    "total_lines": 25,
    "pair_count": 1,
    "partners": [
      "src/x/tests/a_e2e.rs"
    ],
    "test_only": true,
    "pairs": [
      {
        "lines": 25,
        "a": "src/x/tests/c_e2e.rs:5-30",
        "b": "src/x/tests/a_e2e.rs:70-95"
      }
    ]
  },
  {
    "file": "src/y/src/prod.rs",
    "total_lines": 22,
    "pair_count": 1,
    "partners": [
      "src/y/src/prod2.rs"
    ],
    "test_only": false,
    "pairs": [
      {
        "lines": 22,
        "a": "src/y/src/prod.rs:1-23",
        "b": "src/y/src/prod2.rs:1-23"
      }
    ]
  },
  {
    "file": "src/y/src/prod2.rs",
    "total_lines": 22,
    "pair_count": 1,
    "partners": [
      "src/y/src/prod.rs"
    ],
    "test_only": false,
    "pairs": [
      {
        "lines": 22,
        "a": "src/y/src/prod2.rs:1-23",
        "b": "src/y/src/prod.rs:1-23"
      }
    ]
  }
]
```

- [ ] **Step 3: create `tests/run.sh`:**

```bash
#!/usr/bin/env bash
# Golden test for the duplication triage ranker. Run from repo root:
#   bash .claude/skills/loom-duplication-fix/tests/run.sh
set -euo pipefail
D="$(cd "$(dirname "$0")" && pwd)"
GOT="$(LC_ALL=C buck2 run -v0 //tools:jq -- -f "$D/../triage.jq" "$D/fixture.json")"
if diff -u "$D/golden.json" <(printf '%s\n' "$GOT"); then
  echo "PASS: duplication triage matches golden"
else
  echo "FAIL: duplication triage drifted from golden"; exit 1
fi
```

- [ ] **Step 4: run the test to verify it FAILS** (no triage.jq yet)

Run: `bash .claude/skills/loom-duplication-fix/tests/run.sh`
Expected: FAIL — jq cannot open `triage.jq`.

- [ ] **Step 5: create `triage.jq`** (validated during planning — reproduce EXACTLY):

```jq
def loc(s): (s.path + ":" + (s.start_line|tostring) + "-" + (s.end_line|tostring));
def is_test(p): (p | test("(^|/)tests?/")) ;
[ .duplicates[]?
  | { hub:.file1.path, partner:.file2.path, lines:.line_count, a:.file1, b:.file2 },
    { hub:.file2.path, partner:.file1.path, lines:.line_count, a:.file2, b:.file1 } ]
| group_by(.hub)
| map({ file: .[0].hub,
        total_lines: (map(.lines) | add),
        pair_count: length,
        partners: (map(.partner) | unique),
        test_only: ( ([ .[0].hub ] + (map(.partner))) | unique | all(is_test(.)) ),
        pairs: ( sort_by(-.lines)
                 | map({ lines, a: loc(.a), b: loc(.b) }) ) })
| sort_by(-.total_lines)
```

- [ ] **Step 6: run the test to verify it PASSES**

Run: `bash .claude/skills/loom-duplication-fix/tests/run.sh`
Expected: `PASS: duplication triage matches golden`

- [ ] **Step 7: sanity-check on real src**

Run:
```bash
git ls-files 'src/**/*.rs' > /tmp/dupf.txt
buck2 run -v0 //tools:lucidshark-duplo -- /tmp/dupf.txt --json -m 20 > /tmp/dupf.json 2>/dev/null || true
buck2 run -v0 //tools:jq -- -f .claude/skills/loom-duplication-fix/triage.jq /tmp/dupf.json \
  | buck2 run -v0 //tools:jq -- -c '.[0:3][] | {file,total_lines,pair_count,test_only}'
```
Expected: query-api `*_e2e.rs` files rank on top, `test_only:true`.

- [ ] **Step 8: commit**

```bash
git add .claude/skills/loom-duplication-fix/triage.jq .claude/skills/loom-duplication-fix/tests/
git commit --no-verify -m "feat(loom-duplication-fix): deterministic duplication triage ranker + golden test"
```

---

## Task 4: loom-duplication-fix SKILL.md

**Files:**
- Create: `.claude/skills/loom-duplication-fix/SKILL.md`

- [ ] **Step 1: write frontmatter + overview**

```markdown
---
name: loom-duplication-fix
description: Remediate a duplication family from the loom-duplication census by extracting a shared abstraction (helper/fixture/test harness) under TDD and opening a PR for human review (NOT auto-merged). Use when asked to fix/dedupe duplication, extract a shared helper, or act on the duplication register. Auto-triages the highest-value hub (preferring lower-risk test-only families), unifies the duplicated blocks behavior-preservingly, proves buck2 tests stay green AND the duplication dropped, then opens a review PR and captures feedback.
---

Remediate ONE duplication family (a hub file + its duplication partners) by
extracting a shared abstraction, and open a PR **for human review — never
auto-merged**. Gated workflow: triage → characterize → extract under TDD → prove
behavior unchanged AND duplication gone → open review PR. If any gate fails, STOP
and report. Use `superpowers:test-driven-development` discipline.
```

- [ ] **Step 2: append BLOCK A (triage)**

````markdown
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
````

- [ ] **Step 3: append the Steps prose**

```markdown
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
```

- [ ] **Step 4: append BLOCK B (land for review — NO merge)**

````markdown
## BLOCK B — open a review PR (run verbatim; NEVER merges)

```bash
set -euo pipefail
SLUG="${1:?usage: BLOCK B <slug>, e.g. graph-e2e-dedup}"
BRANCH="fix/code-health-$SLUG"
git config --get user.email >/dev/null 2>&1 || git config user.email "code-health-bot@users.noreply.github.com"
git config --get user.name  >/dev/null 2>&1 || git config user.name  "code-health-bot"
git switch -C "$BRANCH"
git add -A
git commit --no-verify -m "$(cat /tmp/dupfix-title.txt)" -F /tmp/dupfix-pr.md
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
````

Lint-clean markdown (one trailing newline, no trailing whitespace).

- [ ] **Step 5: validate BLOCK A on real src (dry — no code change)**

Copy BLOCK A's bash into `/tmp/dupa.sh` and run `bash /tmp/dupa.sh`.
Expected: prints ranked hubs with query-api `*_e2e.rs` files near the top, `test_only=true`.
Then `git status --porcelain` → empty.

- [ ] **Step 6: confirm no auto-merge**

Run: `grep -c 'gh pr merge' .claude/skills/loom-duplication-fix/SKILL.md`
Expected: `0`.

- [ ] **Step 7: commit**

```bash
git add .claude/skills/loom-duplication-fix/SKILL.md
git commit --no-verify -m "feat(loom-duplication-fix): triage + TDD extraction workflow + review-PR landing"
```

---

## Final verification

- [ ] Both triage golden tests pass: `bash .claude/skills/loom-complexity-fix/tests/run.sh && bash .claude/skills/loom-duplication-fix/tests/run.sh`.
- [ ] Both skills' BLOCK A run clean on real `src/` (dry) and leave `git status --porcelain` empty.
- [ ] Neither SKILL.md contains `gh pr merge` (review-only landing): `grep -L 'gh pr merge' .claude/skills/loom-*-fix/SKILL.md` lists both.
- [ ] All new `.md` pass markdown lint: `buck2 run //tools:prek -- run --all-files` (commit any hook fixes).
- [ ] Open a PR for `feat/code-health-remediation` → `main`.

## Notes for the implementer

- **The first REAL remediation run is deferred to post-merge.** This plan builds and unit-validates the skills (triage golden tests + dry BLOCK A runs). Actually refactoring loom code is *using* the skill and produces a human-reviewed PR — do that after this lands, by invoking `/loom-complexity-fix` or `/loom-duplication-fix`.
- **Never** add `gh pr merge` to these skills — the human-review gate is the whole point.
- Tool calls use `buck2 run -v0 //tools:<t> -- …` (paths passed to jq are absolute, so cwd is irrelevant). Never pipe `buck2 test`/`bxl` through tail/head; redirect to a file and grep.
- The skills depend on the `loom-complexity` / `loom-duplication` census skills' tools and the committed allowlists, all already on `main`.
