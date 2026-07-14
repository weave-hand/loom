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
      gate** (below).
4. **Finish** — `superpowers:finishing-a-development-branch`: open a PR whose head
   branch is `work/<id>` (this is what binds the claim to the PR). In that PR,
   close the register item via `loom-docs-update` — remove its entry and fold
   the landed capability into `docs/system-capabilities/`, naming the id + PR
   in the PR body (registers carry open work only).
5. **Release** is automatic: once the PR merges/closes, the claim is reaped by
   `bash tools/docs.sh claims --reap` (run by routines). If you abandon before a
   PR, release explicitly: `bash tools/docs.sh release <id>`.

## The metric gate is a FIX step, not a reporting step

Run `loom-complexity diff` and `loom-duplication diff` (both take a `diff`
argument — changed files only, print, no commit) before the PR opens.

**Compare against the MERGE-BASE, not the committed register** — the register can
be many commits stale, and a stale register makes new debt look pre-existing.
Measure the functions/files your branch touched at `git merge-base HEAD main`, and
again at `HEAD`. That diff is the finding.

**If your branch worsened any hotspot on any axis (cc, cognitive, MI, SLOC), or
introduced any cross-file duplication pair ≥ 20 lines: FIX IT IN THIS PR.** Not in
a follow-up. Not in an issue. Not in the PR description.

**"It was already a hotspot" is not a licence to make it worse.** A function over
threshold before your branch and further over it after your branch is debt *you*
added, and the gate exists to tell you so while the code is still in your head.

**Do not defer to `loom-complexity-fix` / `loom-duplication-fix`.** Those routines
exist for hotspots nobody is currently touching. Debt you created an hour ago is
not their job.

The gates usually point at the *same seam* — when they do, one extraction fixes
both (and the shared extraction is normally the thing that should have existed
already). Re-run both gates after the fix and put the before/after numbers in the
PR body.

**The only acceptable non-fix** is that the detector is measuring something that is
not debt — and you must name the shared abstraction you checked for and say why it
cannot serve. "A test seed must stay local" is only true if you looked at
`//src/testing:seed` (and the crate's `*-support` test library) and it genuinely
cannot express the case; the shared helper often exists already and you just did
not look.

| Rationalization | Reality |
|---|---|
| "It was already over the threshold before my branch" | You made it worse. Fix the delta you added, at minimum. |
| "This is a correctness fix on a hot path — refactoring adds risk" | You have just written the tests that make the refactor safe. Run them. |
| "loom's convention is that remediation lands as its own PR" | That convention is for *untouched* hotspots. Not for debt from this diff. |
| "The MI crossing is marginal (19.93 vs 20)" | Thresholds are not negotiable by proximity. Fix it. |
| "It's a good `loom-complexity-fix` candidate" | It is a good candidate for you, now, with the context loaded. |
| "The duplication is just test seeding" | Then use the seed library. That is what it is for (CLAUDE.md says so). |
| "I'll note it in the PR description" | A note is not a fix. The gate is signal, not paperwork. |

**Red flags — you are rationalizing, go fix it:**

- You are writing a paragraph explaining why you are *not* fixing a finding.
- The words "justified", "acceptable", "pre-existing", "out of scope", or
  "follow-up" are appearing near a metric finding.
- You are about to close the PR body with a "Known debt" section describing
  something you could have fixed in the time it took to write the section.

## Problems you find while implementing: FIX them, do not FILE them

A register item is closed by *fixing* things. If your branch ends with more open
issues than it started with, you have grown the backlog, not shrunk it.

**Default: if you find a defect while building the item and you can reach it, test
it, and fix it — fix it in this PR.** This includes bugs you find in code your item
does not name (a second instance of the same bug is the commonest case, and the
cheapest possible fix: you have the pattern, the test idiom, and the context).

**Filing is the exception.** You may file a new item ONLY when one of these holds,
and you must say which in the item's prose:

1. The fix needs a **design decision a human must make** (competing approaches with
   different blast radii — the kind of thing `loom-work-plan` writes a spec for), or
2. The fix lands in a **different subsystem** and would need its own spec and its own
   test surface — i.e. it is a genuinely separate item, not a second call site.

**Before you file anything, VERIFY the defect is real.** Read the code path end to
end and convince yourself it can actually be reached. A filed issue that cannot
happen is worse than no issue: it is a permanent, confident lie in the register that
some future session will spend a day "fixing". (A real session filed a
trigger-latches-forever issue whose only trigger path — job abandonment — the worker
cannot even produce, because every RPC failure retries with no attempt cap.)

**Filing a bug does not preserve it — it rots it.** The description you write is a
guess at the fix. It is often wrong: a real session filed "the same one-line guard is
needed in `collect_vectors`", and the actual fix turned out to need a second change
(`write_sidecar` also loaded the Iceberg table) that the note never mentioned. Fixing
it forces you to find that out; filing it does not.

When you do fix a found defect, it rides the same rules as the item itself: a failing
test first (watch it fail for the right reason), then the fix, then the register close
if it had an entry — and the capability recorded in `docs/system-capabilities/`.

| Rationalization | Reality |
|---|---|
| "Filed rather than folded in, to keep this fix scoped" | Scope discipline is about *design*, not about ducking a 5-line fix. |
| "It's the same bug elsewhere — I'll file it as its own item" | Same bug + same fix shape = fix it now. You will never be cheaper. |
| "It's out of scope for this item" | The item is the excuse, not the boundary. Can you test it? Then fix it. |
| "The spec says 'out of scope — file it as its own item'" | The spec was written **before** anyone knew the bug's real shape. That line is a hypothesis, not an exemption — and it is routinely wrong (the same spec also predicted a one-line fix that turned out to need two). Once you can see the fix, the spec's guess does not bind you. |
| "I'll write a really good issue for it" | The best issue is a merged fix. |
| "A future session can pick it up" | It will be stale, mis-described, or never picked up. |

**Red flags — stop and fix instead:**

- You are adding an entry to `docs/ISSUES.md` for something you found *while your
  own branch was open*, and you have not tried to fix it.
- Your PR body says "files N new issues" and N > 0 while "closes" is 1.
- You are describing a **fix shape** in an issue you could just apply.

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
