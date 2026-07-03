---
name: loom-work-checkout
description: Claim a documentation-register work item before building it, so two sessions/agents never work the same item. Claiming atomically creates the work/<id> branch (the branch the PR is opened from) as a distributed mutex. Use when starting work on a ROADMAP/FUTURE/ISSUES item, when picking up the next planned item, or in a scheduled session that builds register items. The item must already reference an on-disk spec (use loom-work-plan to get an item to that state).
---

Claim a register item, work it on a conventional branch, and let the claim
self-release when the PR lands. The claim is a server-side git mutex: it
atomically creates the `work/<id>` branch (`refs/heads/work/<id>`) — the very
branch the eventual PR is opened from — so claiming and starting the branch are
one step. The registers and grammar are defined in
`docs/superpowers/specs/2026-06-21-work-item-planning-checkout-design.md`.

## Steps

1. **Pick** an open, ready item: `bash tools/docs.sh query open`. Ready means it
   has a `spec:` set (the claim gate rejects `spec:-`); if nothing is ready, use
   `loom-work-plan` first. Avoid items already listed by
   `bash tools/docs.sh claims`.
2. **Claim** it: `bash tools/docs.sh claim <id>`. On success it has created the
   `work/<id>` branch on `origin`. If the claim is lost or already held, pick
   another.
3. **Work it through the rigid pipeline.** Check out the branch the claim created
   (`git fetch origin work/<id> && git switch work/<id>`), then exercise
   these four superpowers skills **in order — none is optional, even for a
   one-line fix** (the spec already exists by the gate's precondition, so start at
   the plan):
   1. **Write the plan** — `superpowers:writing-plans`: turn the spec into a
      task-by-task implementation plan under `docs/superpowers/plans/`. (It runs
      its own self-review at the end.)
   2. **Review the plan** — before any code, gate the plan against the spec:
      dispatch a fresh reviewer subagent (`superpowers:dispatching-parallel-agents`)
      to check spec coverage, no placeholders, and type/signature consistency. Fix
      every gap and do not start implementing until the plan passes.
   3. **Implement with subagents** — `superpowers:subagent-driven-development`: one
      fresh subagent per task, each followed by the mandatory two-stage review
      (spec-compliance, then code-quality), looping until both pass. This skill
      uses `superpowers:requesting-code-review` / `receiving-code-review` and has
      the subagents follow `superpowers:test-driven-development`.
   4. **Final review** — after all tasks, the whole-implementation review that
      `superpowers:subagent-driven-development` ends with (a final code-reviewer
      subagent) before finishing. **The final review MUST include the metric
      gate**: run `loom-complexity diff` and `loom-duplication diff` (both
      skills take a `diff` argument — changed files only, print, no commit)
      and report as findings any NEW hotspot over the census thresholds
      (cc > 15, cognitive > 15, MI < 20, SLOC > 100) or any NEW cross-file
      duplication pair ≥ 20 lines introduced by the branch. Findings are
      advisory, not auto-blocking — some legitimate changes trip the detectors
      (e.g. an intentionally-local test seed) — but each one must be either
      fixed or explicitly justified in the PR description. When the item's
      whole point is a metric improvement, quote the before/after numbers in
      the register close prose.
4. **Finish** — `superpowers:finishing-a-development-branch`: open a PR whose head
   branch is `work/<id>` (this is what binds the claim to the PR). In that PR,
   close the register item via `loom-docs-update` — remove its entry and fold
   the landed capability into `docs/system-capabilities/`, naming the id + PR
   in the PR body (registers carry open work only).
5. **Release** is automatic: once the PR merges/closes, the claim is reaped by
   `bash tools/docs.sh claims --reap` (run by routines). If you abandon before a
   PR, release explicitly: `bash tools/docs.sh release <id>`.

## Notes

- A claim with no PR older than the grace window (default 240 min,
  `LOOM_CLAIM_GRACE_MIN`) is reapable — open the PR promptly, or re-run
  `claim <id>` to refresh it. The default is sized to a full
  plan → plan-review → implement → final-review arc; refresh the claim at the
  start of each long phase anyway if the arc may exceed it.
- **Lease-check before every push to `work/<id>`.** A reaped-and-reclaimed
  branch means another session may hold it now: before pushing, run
  `git ls-remote origin work/<id>` and verify the remote tip is an ancestor of
  your local branch (i.e. your history contains it). If it is not — someone
  else's commits are on the branch — STOP and surface the collision to the
  user rather than force-pushing over live work. (This rule exists because two
  sessions once built the same item after a stale reap; see PR #324.)
- Cloud sessions can **acquire** claims (a `refs/heads/*` create, which the web
  git proxy allows) but cannot `release`/`claims --reap` (the proxy forbids ref
  deletion). That is fine: a cloud session only needs to claim; the branch is
  deleted when its PR merges (reap-on-merge) or by a local `release`/`--reap`.
- `claim` refuses items that are closed, non-actionable, already claimed, or whose
  `spec:` is `-` / missing on disk. A direction-less item is not claimable; give it
  a spec first via `loom-work-plan`.
