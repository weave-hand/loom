---
name: loom-work-checkout
description: Claim a documentation-register work item before building it, so two sessions/agents never work the same item. Uses an atomic refs/claim/<id> git ref as a distributed mutex. Use when starting work on a ROADMAP/FUTURE/ISSUES item, when picking up the next planned item, or in a scheduled session that builds register items. The item must already reference an on-disk spec (use loom-work-plan to get an item to that state).
---

Claim a register item, work it on a conventional branch, and let the claim
self-release when the PR lands. The claim is a server-side git mutex
(`refs/claim/<id>`); the registers and grammar are defined in
`docs/superpowers/specs/2026-06-21-work-item-planning-checkout-design.md`.

## Steps

1. **Pick** an open, ready item: `bash tools/docs.sh query open`. Ready means it
   has a `spec:` set (the claim gate rejects `spec:-`); if nothing is ready, use
   `loom-work-plan` first. Avoid items already listed by
   `bash tools/docs.sh claims`.
2. **Claim** it: `bash tools/docs.sh claim <id>`. On success it prints the
   `work/<id>` branch to use. If the claim is lost or already held, pick another.
3. **Work** it on `work/<id>` (`git switch -c work/<id>`). The spec exists by the
   gate's precondition; write a plan if needed (writing-plans), then implement
   (subagent-driven-development / executing-plans).
4. **Open a PR** whose head branch is `work/<id>` — this is what binds the claim to
   the PR. In that PR, close the register item via `loom-docs-update`
   (`- [ ]`→`- [x]`, terminal status, add `pr:#N`).
5. **Release** is automatic: once the PR merges/closes, the claim is reaped by
   `bash tools/docs.sh claims --reap` (run by routines). If you abandon before a
   PR, release explicitly: `bash tools/docs.sh release <id>`.

## Notes

- A claim with no PR older than the grace window (default 60 min,
  `LOOM_CLAIM_GRACE_MIN`) is reapable — open the PR promptly, or re-run
  `claim <id>` to refresh it.
- `claim` refuses items that are closed, non-actionable, already claimed, or whose
  `spec:` is `-` / missing on disk. A direction-less item is not claimable; give it
  a spec first via `loom-work-plan`.
