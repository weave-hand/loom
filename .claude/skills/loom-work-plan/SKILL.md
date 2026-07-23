---
name: loom-work-plan
description: Triage the GitHub issue tracker and get ONE work item ready to build — write the spec into the issue body, label it ready, then STOP. Use to decide what to work next, promote an idea to roadmap, retire dead work, mint a meta for a large slice, or batch-prepare issues for checkout. PLANS ONLY — it never claims, writes the implementation plan, or implements; a separate work agent does that via loom-work-checkout.
---

This is a **planning loop, not a build.** Per item you produce exactly one
thing — a spec written into the issue body under `## Spec`, with the `ready`
label set — then you **stop and hand back to the operator.** A separate
**work agent** later claims the issue, writes the implementation plan *from
the spec* (ephemeral, never committed), and builds it. Your job is direction,
never completion. Planning touches **zero repo files**.

Environment: use `gh` locally; in cloud sessions the git proxy 403s `gh` API
calls to the org repo — use the GitHub MCP tools there instead (same
operations: list/view/edit/label/comment).

## Boundary — do NOT cross it

Setting `ready` is the finish line for this item. After it, do NOT:

- assign the issue or post a `Claimed:` comment — claiming is the **work
  agent's** first step, not yours;
- invoke `superpowers:writing-plans` or write any implementation plan;
- start a `work/<N>-<slug>` branch, implement, or touch code;
- offer to "go ahead and build it" — that is the single most common failure here.

## Per-item steps (one item per pass)

1. **Triage.** Survey open work and recommend what's next:
   - `gh issue list --label roadmap --state open` and
     `gh issue list --label bug --state open` — committed work and defects;
     items **without** `ready` need direction, items **with** `ready` and no
     assignee are waiting for a work agent (don't re-plan them).
   - `gh issue list --label idea --state open --limit 200` for the deferred
     pile (add `--label area:<a>` to focus an area).
   - Skip issues that are assigned (claimed — check for staleness only if
     picking them matters) and `meta` issues (they are never planned directly;
     plan their children).
   - Output a short ranked shortlist with one-line reasons (unblocks others,
     area balance, quick win).
2. **Promote / retire / decompose.**
   - Promote an idea being committed to: `gh issue edit <N> --remove-label idea --add-label roadmap`.
   - Retire dead work: `gh issue close <N> --comment "<why>"`.
   - Demote an abandoned roadmap item back to an idea: swap the labels the
     other way, comment why.
   - **Too big for one PR → make it a meta:** relabel the issue `meta`, write
     the workstream goal + a `- [ ] #N` child checklist into its body, file
     each child as its own labeled issue ("Part of #<meta>" in the body), and
     attach them as native sub-issues
     (`gh api -X POST repos/{owner}/{repo}/issues/<meta>/sub_issues -F sub_issue_id=$(gh api repos/{owner}/{repo}/issues/<child> --jq .id)`).
     Metas are never `ready` and never claimed; plan each child separately.
3. **Ready (direction gate).** For the chosen issue, invoke
   `superpowers:brainstorming` to author the design (a human sets direction).
   Write the approved design into the issue body under a `## Spec` heading
   (`gh issue view <N> --json body`, append, `gh issue edit <N> --body-file`),
   then `gh issue edit <N> --add-label ready`, and mirror it on the `loom v1`
   board — Status → Ready (option `61e4505c`):

   ```bash
   tools/board-status.sh <N> 61e4505c
   ```

   (`tools/board-status.sh` is the single home for the ProjectV2 mutation and is
   fail-soft: a cloud session reaches Projects GraphQL with the ambient gh token
   via an `api.github.com` proxy bypass, and if that egress is blocked it skips
   the board update non-fatally — the `ready` label stays authoritative and a
   local session reconciles.) **Stop at the committed spec — do NOT transition to
   writing-plans.**

## Then loop or stop — never build

The issue is now ready for a work agent (`loom-work-checkout`) to claim, plan,
and build. That is **not this session's job.** Return to the operator: if they
want to prepare more, go back to **Triage** for the next item; otherwise stop.
Do not claim and do not build, no matter how ready the issue looks.
