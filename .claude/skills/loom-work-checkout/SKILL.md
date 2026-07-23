---
name: loom-work-checkout
description: Claim a ready GitHub issue before building it, so two sessions/agents never work the same item. Claiming = assigning the issue plus a timestamped Claimed comment; work lands via a PR that says Closes #N. Use when starting work on a roadmap/bug issue, when picking up the next ready item, or in a scheduled session that builds tracker items. The issue must already carry a spec in its body and the ready label (use loom-work-plan to get it there).
---

Claim a `ready` issue, work it on a conventional branch, and let the PR close
it. The claim is the **issue assignment plus a `Claimed:` comment** — every
session authenticates as the same GitHub account, so the comment timestamp is
the true claim record and the assignment is the visible flag.

Environment: use `gh` locally; in cloud sessions the git proxy 403s `gh` API
calls to the org repo — use the GitHub MCP tools there instead.

## Steps

1. **Pick** an open issue labeled `ready`, unassigned, and not `meta`:
   `gh issue list --label ready --state open --search "no:assignee"` (the
   `--no-assignee` flag does not exist on current `gh` — it errors; filter via
   the search query instead). If one is
   assigned but its newest `Claimed:` comment is older than the grace window
   (240 min) **and** no open PR references it, the claim is stale — you may
   take it over (say so in your comment). If nothing is ready, use
   `loom-work-plan` first. Never claim a `meta` issue — build its children.
2. **Claim** it:
   `gh issue edit <N> --add-assignee @me && gh issue comment <N> --body "Claimed: $(date -u +%Y-%m-%dT%H:%M:%SZ)"`.
   Re-read the issue after claiming; if someone else's fresher `Claimed:`
   comment appears, back off and pick another. Then set the `loom v1` board
   Status → In progress: `tools/board-status.sh <N> 47fc9ee4` (the helper owns
   the ProjectV2 mutation and its cloud/local auth; a cloud session uses
   `$LOOM_PROJECT_TOKEN` and skips softly if it is unset — assignment + comment
   remain the authoritative claim regardless).
3. **Work it through the rigid pipeline.** Create the branch with
   **`gh issue develop <N> --name <N>-<slug> --checkout`** (from up-to-date
   `main`) — NOT a bare `git switch -c`. `gh issue develop` registers the
   branch in the issue's **Development** section, so GitHub links it natively
   and a merged PR from it closes the issue **even if the `Closes #<N>` keyword
   fails to parse** (it silently does — see step 4). A branch name alone never
   links anything; only this native link (or the keyword) drives the
   automation. Cloud sessions (no `gh` API) create the linked branch with the
   GitHub MCP `create_branch` + issue-link tooling, or fall back to a local
   `git switch -c <N>-<slug>` and lean on the keyword + the step-4 verify.
   Then exercise these four superpowers skills **in order — none is optional,
   even for a one-line fix** (the spec is in the issue body by the gate's
   precondition, so start at the plan):
   1. **Write the plan** — `superpowers:writing-plans`: turn the issue's
      `## Spec` into a task-by-task implementation plan **in the session
      scratchpad — ephemeral, never committed**. (It runs its own self-review
      at the end.)
   2. **Review the plan** — before any code, gate the plan against the spec:
      dispatch a fresh reviewer subagent (`superpowers:dispatching-parallel-agents`)
      to check spec coverage, no placeholders, and type/signature consistency.
      Fix every gap and do not start implementing until the plan passes.
   3. **Implement with subagents** — `superpowers:subagent-driven-development`:
      one fresh subagent per task, each followed by the mandatory two-stage
      review (spec-compliance, then code-quality), looping until both pass.
   4. **Final review** — the whole-implementation review that
      `superpowers:subagent-driven-development` ends with. **The final review
      MUST include the metric gate** (below).
4. **Finish** — `superpowers:finishing-a-development-branch`: open a PR from
   the `<N>-<slug>` branch whose body contains `Closes #<N>` (base `main`, the
   default branch — the keyword only auto-closes against the default branch).
   **Then verify the link took — this is a required step, not a courtesy.**
   `gh pr create` silently fails to register the closing keyword often enough
   that you must confirm it every time:

   ```bash
   gh api graphql -f query='{repository(owner:"weave-hand",name:"loom"){
     pullRequest(number:<PR>){closingIssuesReferences(first:5){nodes{number}}}}}' \
     -q '.data.repository.pullRequest.closingIssuesReferences.nodes'
   ```

   If that prints `[]`, the automation will NOT fire on merge. Repair it by
   re-saving the body (`gh pr edit <PR> --body-file <file>`) and re-running the
   query until it returns `[{"number":<N>}]`. (The native `gh issue develop`
   link from step 3 is the belt to this suspenders — with it, merge still
   closes the issue even if this stays empty, but confirm at least one of the
   two is in place before calling the PR done.) Cloud sessions run the same
   check via the GitHub MCP PR tooling. Once the PR is open and the closing
   link is confirmed, move the `loom v1` board Status → **In review**:
   `tools/board-status.sh <N> df73e18b` — so the item visibly parks in that
   column until merge. **Nothing auto-populates it** (the project's only board
   automation is merge/close → Done, so without this step the item jumps In
   progress → Done and never shows In review). The helper is fail-soft: a cloud
   session uses `$LOOM_PROJECT_TOKEN` and skips softly if it is unset (next local
   session reconciles). Record the landed capability in `docs/system-capabilities/`
   in the same PR. If the work deferred anything new, file it as a labeled issue
   (`idea` or `bug` + `area:<a>`) — but read the filing discipline below first.
5. **Release** is automatic: merge closes the issue; confirm the board shows
   Done (`tools/board-status.sh <N> 98236657` if it didn't move). If you abandon
   before a PR, unassign and say so:
   `gh issue edit <N> --remove-assignee @me && gh issue comment <N> --body "Released"`
   (and `tools/board-status.sh <N> 61e4505c` to set the board back to Ready).

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

**Filing is the exception.** You may open a new issue with the right `area:`/kind
labels ONLY when one of these holds, and you must say which in the item's prose:

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
test first (watch it fail for the right reason), then the fix, then close its issue
if it had one (`Closes #N` in the PR body) — and the capability recorded in
`docs/system-capabilities/`.

| Rationalization | Reality |
|---|---|
| "Filed rather than folded in, to keep this fix scoped" | Scope discipline is about *design*, not about ducking a 5-line fix. |
| "It's the same bug elsewhere — I'll file it as its own item" | Same bug + same fix shape = fix it now. You will never be cheaper. |
| "It's out of scope for this item" | The item is the excuse, not the boundary. Can you test it? Then fix it. |
| "The spec says 'out of scope — file it as its own item'" | The spec was written **before** anyone knew the bug's real shape. That line is a hypothesis, not an exemption — and it is routinely wrong (the same spec also predicted a one-line fix that turned out to need two). Once you can see the fix, the spec's guess does not bind you. |
| "I'll write a really good issue for it" | The best issue is a merged fix. |
| "A future session can pick it up" | It will be stale, mis-described, or never picked up. |

**Red flags — stop and fix instead:**

- You are opening a new `bug` issue for something you found *while your
  own branch was open*, and you have not tried to fix it.
- Your PR body says "files N new issues" and N > 0 while "closes" is 1.
- You are describing a **fix shape** in an issue you could just apply.

## Notes

- Refresh a long-running claim by posting a fresh `Claimed:` comment at the
  start of each long phase (plan → review → implement → final review can
  exceed the 240-min grace window).
- **Before every push**, check no *other* open PR references the issue
  (`gh pr list --search "<N> in:body" --state open`). If one exists — someone
  else built it after a stale takeover — STOP and surface the collision
  rather than racing the PR. (Two sessions once built the same item after a
  stale reap; see PR #324.)
- Cloud sessions claim via the GitHub MCP tools (assign + comment); the `gh`
  CLI cannot reach the org repo's API through the git proxy.
