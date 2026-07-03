---
name: loom-docs-update
description: Update the documentation registers (docs/ROADMAP.md, docs/FUTURE.md, docs/ISSUES.md) at the moment a spec or plan is completed — close the items the work resolved, record any newly-deferred items it introduced, and stage the edits alongside the work. Use when finishing a development branch, completing a plan or spec, after merging a PR that resolves a tracked item, or when the completion-reminder hook nudges you. For a full rebuild/reconcile use loom-docs-organise instead.
---

Keep the registers live as work lands. This is the lightweight single-item
counterpart to `loom-docs-organise` (no agentic mining). Grammar and registers
are defined in
`docs/superpowers/specs/2026-06-20-docs-registers-consolidation-design.md`.

Run this when a spec/plan is completed (or the `Stop` reminder fires). Inputs:
the spec/plan just finished and the PR number(s) for the work.

## Steps

1. Identify the completed spec/plan (the one just implemented on this branch) and
   the PR number(s). If unsure of the PR, use `gh pr view --json number -q .number`
   for the current branch.
2. **Close resolved items — by REMOVING them.** The registers carry open work
   only (see CLAUDE.md → *Documentation registers*); git history is the
   item-by-item record. Find register items whose `spec:` references this
   spec/plan, or whose description the work satisfies:
   `bash tools/docs.sh query open | grep -i <keyword>` and
   `grep -n <spec-slug> docs/ROADMAP.md docs/FUTURE.md docs/ISSUES.md`.
   For each resolved item:
   - Delete its whole entry (the `- [ ]` line **and** the indented prose below it).
     Do NOT flip to `- [x]` — closed entries no longer live in the registers.
   - **Document the landed capability** in the matching
     `docs/system-capabilities/<subsystem>.md` (or `docs/deploy.md` for deploy):
     fold what shipped into the right themed section with the PR ref inline
     `(#N)`, and remove the item's id from that doc's `## Known gaps` if listed.
   - Rewrite any surviving `[[<id>]]` reference to the removed item as a plain
     `` `#<id>` `` code span (the `[[…]]` link would dangle and fail validate).
   - Name the closed id(s) + PR in the commit/PR body — that is the id↔PR record.
3. **Record new deferrals.** Read the completed spec's "deferred" / "out of
   scope" / "non-goals" section. For each genuinely-deferred follow-up, add a new
   item to FUTURE (an idea) or ISSUES (a defect/gap) with `status: deferred`/`open`,
   `from:<spec-slug>` (the originating spec it was deferred within), **`spec:-`**,
   a fresh prefixed `#id`, and a one-paragraph prose note. Add `[[id]]` links to
   related items. **Never put the originating spec in `spec:`** — `spec:` means a
   *dedicated* design exists (the claim/readiness signal); a deferral has none
   until `loom-work-plan` authors one, so it stays `spec:-`.
4. **Promote if applicable.** If the work began a `deferred` FUTURE item rather
   than finishing it, remove the FUTURE entry and add the matching `road-…` item
   (mention the removed `#fut-…` id in the new item's prose as a code span).
5. **Validate:** `bash tools/docs.sh validate` — fix every reported error.
6. **Stage** the register + capability-doc edits so they ride the current feature
   branch's commit (do not open a separate PR): `git add docs/ROADMAP.md
   docs/FUTURE.md docs/ISSUES.md docs/system-capabilities/`.
   Mention in the commit/PR body which items were closed/added.
