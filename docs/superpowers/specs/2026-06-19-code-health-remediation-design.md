# Code-health remediation skills — design

**Date:** 2026-06-19
**Status:** approved (brainstorming), ready for planning
**Builds on:** [`2026-06-19-code-health-routines-design.md`](2026-06-19-code-health-routines-design.md)
(the census skills `loom-complexity` / `loom-duplication` and their registers/allowlists).
**Scope of this spec:** the two remediation *fix* skills + their embedded feedback
capture. The periodic **reflection routine** is sketched in
[§8](#8-reflection-routine--out-of-scope-follow-up-spec) but specified separately.

## 1. Goal

Stand up two on-demand routines that *act on* the code-health registers by safely
**fixing** technical debt — the judgment-heavy half of the system, complementing
the deterministic census skills:

- **`loom-complexity-fix`** — decompose a high-complexity hotspot (or a cluster
  of them in one module) so its metrics drop below threshold.
- **`loom-duplication-fix`** — extract a shared abstraction (helper, fixture, or
  test harness) to collapse a family of duplicate blocks.

Each refactors under TDD, **proves behavior is unchanged and the metric actually
improved**, opens a PR **for human review (no auto-merge)**, and captures durable
feedback (CLAUDE.md edits, waiver updates, skill suggestions) along the way.

## 2. Decisions (from brainstorming)

| Decision | Choice |
|---|---|
| Autonomy | Refactor under TDD + open a PR for **human review**. **No auto-merge** — code changes are judgment calls. |
| Target selection | **Agent auto-triages** the register/census by value-to-risk and picks the item(s) to fix. |
| Scope per PR | **Batch related items** (one module's hotspots; one duplication family) into a single focused PR. |
| Packaging | **Two skills** — complexity vs duplication need different refactor strategies. |
| Feedback trigger | **Both** per-run (this spec) and a periodic reflection routine (follow-up). |
| Feedback channels | **CLAUDE.md edits** (in PR), **skill suggestions** (advisory), **waiver/baseline updates** (in PR). **No auto-memories** — durable knowledge lives in the repo, reviewed. |
| Input | Always a **fresh census** (run the tool live); never depends on a committed register existing. |

## 3. Shared skeleton

Both skills are `SKILL.md` routines, but unlike the deterministic census skills
these are **judgment-heavy guided-refactoring workflows** — mostly prose
discipline plus a few mechanical helpers. They lean on the existing
**`superpowers:test-driven-development`** and **`superpowers:systematic-debugging`**
skills rather than reinventing them. The pipeline (each phase detailed below):

1. **Fresh census** — run the underlying tool (`//tools:rust-code-analysis` or
   `//tools:lucidshark-duplo`) to get *current* structured JSON. Do not rely on a
   committed register; it may be stale or absent. (Cite the register for context
   if present.)
2. **Auto-triage** — pick one high-value/low-risk *theme* (§5).
3. **Characterize** — ensure the target is covered by passing tests *before*
   touching it; add characterization tests if coverage is thin. If it cannot be
   safely covered, **STOP and report** — never refactor blind (§7).
4. **Refactor** — behavior-preserving change under TDD discipline (§6).
5. **Double-verify** — (a) `buck2 test //src/...` stays green (behavior
   unchanged) AND (b) re-run our own tool in `diff` mode (`/loom-complexity diff`
   / `/loom-duplication diff`) to **prove the targeted debt actually dropped**.
   Both gates must pass or the PR is not opened (§7).
6. **Capture feedback** — waiver/baseline updates, CLAUDE.md edits, skill
   suggestions (§9).
7. **Land for review** — push a `fix/code-health-<slug>` branch, open a PR with
   before/after metrics + green-test evidence, and **stop. No auto-merge.**

## 4. Components

- `.claude/skills/loom-complexity-fix/SKILL.md`
- `.claude/skills/loom-duplication-fix/SKILL.md`

Each contains: frontmatter + overview; a small **BLOCK A (triage)** bash that runs
the tool and emits a ranked candidate list as JSON; prose **Steps** for the
characterize→refactor→verify→capture workflow; and a **BLOCK B (land-for-review)**
bash that opens the PR (a variant of the census skills' BLOCK B that omits the
`gh pr merge` step).

## 5. Auto-triage (value-to-risk)

Triage runs the tool, scores candidates, groups them into themes, and picks the
top theme. Scoring is a documented heuristic (tunable), surfaced to the user so
the choice is explainable.

### 5.1 Complexity
- **Value** ∝ how far over threshold: a normalized blend of `cyclomatic`,
  `cognitive`, and `sloc` excess (worse = higher).
- **Risk** ↑ for core hot-path logic (e.g. commit/transaction/ACL paths), ↑ when
  the function lacks test coverage, ↓ for leaf/utility/serialization code.
- **Theme = one module/file.** Group a file's hotspots so shared helpers extracted
  during the refactor are reused (e.g. `query-api/handler.rs`'s
  `resolve_chain` / `read_object` / `read_graph_reach`).
- Pick the highest-value, tractable (coverable, non-rippling) theme.

### 5.2 Duplication
- **Value** ∝ `duplicated_lines × sites` across the family.
- **Risk** is generally **lower for test code** (no production behavior at stake)
  — which aligns with treating test duplication as real debt while making the
  big, safe wins (the e2e-test families) the natural first targets.
- **Theme = a connected family** of pairs that share files (transitively), so one
  extraction collapses many pairs (e.g. the `graph_* / association / object_set`
  e2e cluster → one shared test fixture/harness).
- Pick the highest-value family whose sites can be unified without changing
  observable behavior.

## 6. Refactor phase (behavior-preserving, under TDD)

- **Complexity:** standard decomposition — extract cohesive helpers, replace
  nested conditionals with early returns / guard clauses / small dispatch tables,
  pull independent concerns into private functions. Public signatures unchanged.
- **Duplication:** extract the shared block into one well-named unit (a private
  helper, a `tests/common/` module, or a fixture builder) and call it from each
  former site. For loom's test-heavy duplication, this means a shared e2e harness
  wired as the crate's test support — following the repo's `rust_test` conventions
  (per `CLAUDE.md`: tests are `rust_test` targets, not inline modules).
- The change stays **one theme, one focused diff**. No drive-by refactoring.

## 7. Guardrails (this is the risky half — make them prominent)

- **No untested refactors.** If the target lacks passing test coverage, add
  characterization tests first (themselves a valuable PR). If safe coverage is not
  achievable, **STOP and report** — do not refactor blind.
- **No API ripple.** Don't change public signatures/behavior in ways that force
  edits beyond the target theme.
- **Behavior gate.** If `buck2 test //src/...` fails after the refactor →
  **revert and report**; never paper over a failure. (Fixture tests run local via
  `loom_fixture_test`; honor the repo's test placement rules.)
- **Improvement gate.** If the `diff`-mode re-census shows the metric did *not*
  drop (hotspot still over threshold / pair still reported) → the refactor didn't
  achieve its goal; **do not open the PR** — report what was tried.
- **One theme per PR.** Keep diffs reviewable and revertible.
- **No auto-merge.** The routine ends at an open PR; a human approves.

## 8. Reflection routine — out of scope (follow-up spec)

A future `loom-codehealth-reflect` skill periodically mines accumulated
remediation-PR outcomes (merged / edited-then-merged / rejected) and register
trends for higher-level patterns, then proposes batched knowledge updates
(CLAUDE.md, skill suggestions, waiver tuning). It is sequenced **after** the fix
skills because it has nothing to reflect on until they have produced PR history.
Specified separately.

## 9. Embedded feedback capture

Run as a phase of each fix run (§3 step 6). Three channels; **no auto-memory
writes** — durable knowledge goes into the repo where it is reviewed.

- **Waiver/baseline update (in a PR).** If triage or the refactor concludes that
  part of an item is *accepted debt* — legitimately complex code (a parser, a
  generated table) or intentional duplication — add it to
  `docs/code-health/complexity-waivers.json` (a `{file,function,reason}` entry) or
  `docs/code-health/duplication-baseline.json` (refresh via duplo's
  `--save-baseline`, native format). This may ride in the fix PR or, when no code
  changed, a small dedicated waiver PR. Closes the loop: the census stops
  re-flagging it.
- **CLAUDE.md edit (in the fix PR).** If the refactor codifies a reusable pattern
  (a new shared test harness, a helper module, a decomposition convention), add a
  concise note to the relevant `CLAUDE.md` section so future work follows it.
- **Skill suggestion (advisory).** If a recurring, automatable need surfaces
  (e.g. "the same e2e setup keeps getting extracted — a test-scaffolding skill
  would help"), emit a written suggestion in the **PR body** and the **run-end
  report**. Never auto-creates or edits a skill.

The capture phase is judgment-driven and **may legitimately produce nothing** — it
must not invent a waiver/CLAUDE.md edit/suggestion just to have output.

## 10. Landing (BLOCK B — for review, no merge)

A variant of the census skills' BLOCK B:

- Branch `fix/code-health-<slug>` (e.g. `fix/code-health-handler-rs`,
  `fix/code-health-graph-e2e-dedup`).
- Commit the refactor (+ any characterization tests, CLAUDE.md note, waiver
  change). `--no-verify` (local hooks would stall; CI is the real gate).
- Push; create-or-reuse an **open** PR against `main`.
- PR body: the theme, **before/after metrics** (from the fresh census and the
  `diff`-mode re-census), the `buck2 test //src/...` result, any CLAUDE.md /
  waiver changes, and any skill suggestions.
- **Watch CI** (`gh pr checks --watch`) and report status, but **do NOT
  `gh pr merge`.** Leave the PR open for human review.

## 11. Cross-cutting

- **Determinism:** triage scoring is deterministic given a census; the refactor is
  inherently judgment-driven (that's the point) and gated by the objective
  behavior + improvement checks.
- **Reuses existing assets:** the hermetic tools, the `diff`-mode census of the
  census skills (as the improvement gate), and the committed allowlists (as the
  waiver channel). No new tooling.
- **No CI gate / no schedule in this spec** — on-demand skills; scheduling is a
  later option once they've proven out.
- **Markdown lint:** any generated/edited `.md` (SKILL.md, CLAUDE.md, PR bodies
  written to files) must end with one trailing newline and no trailing whitespace
  (`.claude/rules/markdown-lint.md`).

## 12. Verification (of the skills themselves)

- Each skill's **BLOCK A (triage)** runs against loom's real `src/` and emits a
  sensible ranked theme list (dry, no code changes).
- A full dry run of each skill on a **low-risk real target** (ideally a test-only
  duplication family for `loom-duplication-fix`; a leaf utility hotspot for
  `loom-complexity-fix`) produces: green `buck2 test //src/...`, a `diff`-mode
  re-census showing the metric dropped, and an opened-but-unmerged PR.
- Confirm the guardrails fire: point each skill at an untested or
  no-improvement case and confirm it STOPs rather than opening a PR.

## 13. Implementation order

1. `loom-complexity-fix` — BLOCK A triage, the workflow Steps, guardrails,
   feedback capture, BLOCK B (no-merge); validate on a leaf hotspot.
2. `loom-duplication-fix` — same shape, duplication triage/refactor; validate on a
   test-only duplication family.
3. (Follow-up spec) `loom-codehealth-reflect`.
