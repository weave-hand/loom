---
name: loom-work-plan
description: Triage the documentation registers, promote a deferred idea into committed roadmap work, and get an item ready to build (a spec exists on disk) — the on-ramp to loom-work-checkout. Use when deciding what to work next, promoting a FUTURE idea to ROADMAP, retiring dead work, or preparing an item for checkout. For a full register rebuild use loom-docs-organise; to close items at completion use loom-docs-update.
---

Turn the backlog into a checkout-ready item — open, actionable, with a spec on
disk — then hand off to `loom-work-checkout`. This skill is composition-only (no
new tooling): it uses `tools/docs.sh` reads, `loom-docs-update` edit mechanics,
`superpowers:brainstorming` for spec authoring, and the `loom-docs-organise`
PR-on-green pattern to land. Registers + grammar:
`docs/superpowers/specs/2026-06-21-work-item-planning-checkout-design.md`.

## Steps

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
   item back to a FUTURE `deferred` idea, recording why in the prose.
   Validate edits: `bash tools/docs.sh validate`.
3. **Ready (direction gate).** Checkout requires `docs/superpowers/specs/<spec>.md`
   to exist. If the chosen item has `spec:-` or the file is missing, invoke
   `superpowers:brainstorming` to author the spec (a human sets direction); stop at
   the committed spec (do not require the writing-plans transition here). Record the
   produced slug on the item's `spec:` tag.
4. **Land.** Bundle the promotion/retirement register edits and the new spec into a
   small `plan/<slug>` PR to `main`, merged on green (the `loom-docs-organise`
   PR-on-green pattern, scoped to one item). After merge the checkout-ready item
   and its direction are visible to every worker.

The item is now claimable: `bash tools/docs.sh claim <id>` (see
`loom-work-checkout`).
