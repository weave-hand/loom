---
name: loom-work-plan
description: Triage the documentation registers and get ONE item ready to build — author a spec on disk and land it in main, then STOP. Use to decide what to work next, promote a FUTURE idea to ROADMAP, retire dead work, or batch-prepare items for checkout. PLANS ONLY — it never claims, writes the implementation plan, or implements; a separate work agent does that via loom-work-checkout. For a full register rebuild use loom-docs-organise; to close items at completion use loom-docs-update.
---

This is a **planning loop, not a build.** Per item you produce exactly one thing —
a committed spec on disk, landed in `main`, with the work-item's `spec:` tag set —
then you **stop and hand back to the operator.** The operator runs this repeatedly
to prepare a *batch* of to-be-planned items quickly; a separate **work agent**
later checks one out, writes the implementation plan *from the spec*, and builds
it. Your job is direction (a landed spec), never completion.

## Boundary — do NOT cross it

Landing the spec is the finish line for this item. After it, do NOT:

- `bash tools/docs.sh claim <id>` — claiming is the **work agent's** first step, not yours;
- invoke `superpowers:writing-plans` or write any implementation plan;
- start a `work/<id>` branch, implement, or touch code;
- offer to "go ahead and build it" — that is the single most common failure here.

Completing the item during planning defeats the loop and steps on the work agent.
Plan it, land the spec, return to the operator.

Composition-only (no new tooling): `tools/docs.sh` reads, `loom-docs-update` edit
mechanics, `superpowers:brainstorming` for spec authoring, and the
`loom-docs-organise` PR-on-green pattern to land. Registers + grammar:
`docs/superpowers/specs/2026-06-21-work-item-planning-checkout-design.md`.

## Per-item steps (one item per pass)

1. **Triage.** Survey open work and recommend what's next:
   - `bash tools/docs.sh query open` (optionally `--area <a>`), `query by-area` for
     the distribution, `shipped-open` / `shipped-open --stale` to reconcile.
   - Flag the un-actionable / in-flight: items with `spec:-` (no direction yet),
     FUTURE ideas with no `road-` promotion, items blocked by `[[links]]` to
     still-open items, and anything in `bash tools/docs.sh claims` (already
     checked out — don't plan over it).
   - Output a short ranked shortlist with one-line reasons (unblocks others, area
     balance, quick win).
2. **Promote / retire.** For a FUTURE idea being committed to: set it
   `- [x] status:promoted`, and mint a ROADMAP item `road-<slug>`,
   `- [ ] status:planned`, carrying the same `area:`, the `spec:` slug (or `-`),
   and a `[[fut-…]]` link back. The reverse is the same mechanics: drop a dead
   FUTURE idea (`- [x] status:dropped`) or demote an abandoned ROADMAP `planned`
   item back to a FUTURE `deferred` idea, recording why in the prose. (ISSUES
   defects are orthogonal — they stay in ISSUES, `open` until `fixed`/`wontfix`;
   do not move them to ROADMAP.) Validate edits: `bash tools/docs.sh validate`.
3. **Ready (direction gate).** Checkout requires `docs/superpowers/specs/<spec>.md`
   to exist. If the chosen item has `spec:-` or the file is missing, invoke
   `superpowers:brainstorming` to author the spec (a human sets direction). **Stop
   at the committed spec — do NOT transition to writing-plans.** Record the produced
   slug on the item's `spec:` tag.
4. **Land.** Bundle the promotion/retirement register edits and the new spec into a
   small `plan/<slug>` PR to `main`, merged on green (the `loom-docs-organise`
   PR-on-green pattern, scoped to one item). After merge the item's direction is
   visible and it is claimable — by a **work agent**, not by you.

## Then loop or stop — never build

The item is now ready for a work agent (`loom-work-checkout`) to claim, plan, and
build. That is **not this session's job.** Return to the operator: if they want to
prepare more, go back to **Triage** for the next item; otherwise stop. Do not claim
and do not build, no matter how ready the item looks.
