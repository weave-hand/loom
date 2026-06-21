# Work-item checkout — claiming register items (design)

> Design doc. Adds a distributed-mutex "checkout" over the documentation
> registers so two sessions/agents never work the same item, plus a skill that
> documents the checkout → branch → PR → auto-release flow. Also retires the
> now-meaningless `in-progress` ROADMAP status. Next step is an implementation
> plan (writing-plans).

## Problem

The three registers (`docs/ROADMAP.md`, `docs/FUTURE.md`, `docs/ISSUES.md`,
grammar in [`2026-06-20-docs-registers-consolidation-design.md`](2026-06-20-docs-registers-consolidation-design.md))
say *what* work exists, but nothing says *who is working on what right now*. With
multiple sessions — interactive plus scheduled cloud routines — two workers can
independently pick the same open item and duplicate (or collide on) the work.

We want a lightweight way to **claim** an item: a real "only one winner" lock that
works across machines (separate cloud sessions, not just one host), self-releases
when the work lands, and doesn't strand an item if the holder crashes. The claim
must also refuse items whose *direction* a human hasn't set — an agent should not
grab an item and invent an approach.

## Decisions (from brainstorming)

1. **Purpose is collision-avoidance** — a claim/mutex primitive, not a workflow
   orchestrator. Checkout claims; the existing brainstorm → plan → build skills do
   the work.
2. **Medium is an atomic git ref** — claiming pushes `refs/claim/<id>`; `git push`
   create-if-absent is a server-side mutex, so exactly one worker wins. No GitHub
   dependency for the lock itself, no separate state store.
3. **Release is tied to the PR lifecycle** — a claim is held while its PR is open
   and auto-released (reaped) once the PR merges/closes; a claim with no PR past a
   grace window (crashed before opening one) is reapable.
4. **Eligible items span all three registers** but only if a human has set
   direction: the item must reference an existing **spec**.
5. **Surface is script primitives + a skill** — `tools/docs.sh` gains
   `claim`/`release`/`claims`; a `loom-work-checkout` skill documents the flow.
6. **`in-progress` is retired** — see below.

## Retiring `in-progress`

ROADMAP's `status:` vocabulary is currently `planned · in-progress · done`. But
`in-progress` only ever exists on a work branch — `main` never sees it, since an
item goes `planned` → (work happens on a branch) → `done` at PR merge. It conveys
nothing the claim ref doesn't already convey, and a markdown status is neither
live nor self-releasing. **The claim ref is the sole "being worked right now"
signal; `docs.sh claims` is how you view it.**

Therefore ROADMAP's allowed statuses become **`planned · done`** (terminal stays
`done`). FUTURE (`deferred · promoted · dropped`) and ISSUES
(`open · fixed · wontfix`) are unchanged. Item flow becomes: an idea sits in
FUTURE (`deferred`); when committed it is `promoted` and a matching ROADMAP item
appears (`planned` → `done`).

Migration is clean: no register item currently uses `in-progress` (only
`road-iceberg-flush-consumer` is `planned`; the rest are `done`), so nothing needs
rewriting — only the validator and the docs that describe the vocabulary change.

## The claim primitive

A claim is the ref `refs/claim/<id>` on `origin`, pointing at a tiny **orphan
commit** whose commit message is a metadata trailer:

```
claim: <id>

id: <id>
claimant: <name-or-email>
since: <ISO-8601 UTC>
register: roadmap|future|issues
spec: <spec-slug>
branch: work/<id>
```

The orphan commit carries the metadata reaping needs (who, when, which branch)
inside the ref itself — no second store. Two workers racing both build unrelated
orphan commits; the push is create-if-absent, so the loser is rejected.

**Atomic create-if-absent push:**

```
git push --force-with-lease=refs/claim/<id>: origin <sha>:refs/claim/<id>
```

`--force-with-lease=<ref>:` with an empty expected value means "succeed only if
the remote ref does not exist." The first worker creates the ref; the second's
lease check fails because the ref now exists — a reliable server-side mutex. (A
plain non-force push of an unrelated orphan commit is also rejected as
non-fast-forward; the explicit empty lease makes the intent unambiguous and
fails even on the fast-forward edge case.)

`<id>` is used verbatim in the ref path. Register ids match `[a-z0-9-]+`
(`road-`/`fut-`/`iss-` prefixed), which is a safe ref component — `claim`
validates the id against that pattern before constructing the ref.

## Eligibility gate

`claim <id>` runs these checks **before** the push and refuses with a clear
message on any failure:

1. **Exists & open** — `<id>` resolves to exactly one item across the three
   registers and its checkbox is `- [ ]` (open). (Reuses `docs.sh`'s existing
   `_extract` TSV.)
2. **Actionable status** — ROADMAP `planned` · ISSUES `open` · FUTURE `deferred`.
   (Terminal/closed statuses are not claimable.)
3. **Human-set direction** — `spec:` is not `-` **and**
   `docs/superpowers/specs/<spec>.md` exists on disk. A direction-less item is not
   claimable; the path to claim it is to brainstorm a spec for it first.
4. **Not already claimed** — no live `refs/claim/<id>` on `origin`
   (`git ls-remote origin refs/claim/<id>` empty). On a live claim, print the
   holder and `since` from the existing ref.

## Lifecycle & release

Claim ↔ PR bind by **convention**, not by recording a PR number: the work branch
is `work/<id>` and the item's PR is the open PR whose head branch is `work/<id>`.

A claim is:

- **Active** while an open PR with head `work/<id>` exists, **or** (no such PR
  yet) the claim is younger than the **pre-PR grace window** (default 60 minutes,
  measured from `since`).
- **Reapable** when there is no open PR with head `work/<id>` **and** the claim is
  older than the grace window (holder crashed before opening a PR), **or** the
  PR has merged/closed (work done or abandoned).

Release paths:

- **Auto** — `claims --reap` deletes every reapable claim ref. Routines run it;
  the typical "PR merged" release happens here, lazily.
- **Explicit give-up** — `release <id>` deletes `refs/claim/<id>` (local + origin)
  immediately, for a worker that abandons before a PR.

Register `status` is **not** flipped at claim time (that would be a second racy
push to the registers). The `planned → done` flip lands with the work's own PR via
the existing `loom-docs-update` flow — atomic with the work, on `main` only.

## Surface

### `tools/docs.sh` subcommands

- **`docs.sh claim <id>`** — run the eligibility gate; on pass, build the orphan
  commit and push it create-if-absent. On success print the `work/<id>` branch to
  create and the next steps; on a lost race / existing claim, print the current
  holder and `since` and exit non-zero.
- **`docs.sh release <id>`** — delete `refs/claim/<id>` from `origin` and locally.
  No-op-with-notice if the ref is absent.
- **`docs.sh claims [--reap]`** — list live claims, one per line:
  `id  claimant  age  (PR #N open | PR pending <Nm left> | stale)`. `--reap`
  deletes the stale ones. Determining PR state needs `gh` (the same dependency the
  cloud routines already carry); the PR-state probe is isolated behind one helper
  (see Testing) so the listing degrades gracefully and tests can stub it.

These follow the existing `main()` dispatch and `cmd_*` structure in `docs.sh`.

### `loom-work-checkout` skill

A skill (`.claude/skills/loom-work-checkout/SKILL.md`) documenting the flow:

1. **Pick** an item: `bash tools/docs.sh query open` — choose one whose `spec:`
   is set (the gate will reject the rest).
2. **Claim** it: `bash tools/docs.sh claim <id>`. If the claim is lost, pick
   another.
3. **Work** it on `work/<id>` using the existing skills (the spec already exists;
   write a plan if needed, then implement). The id flows into the PR.
4. **Open the PR** with head `work/<id>`; close the register item via
   `loom-docs-update` in that PR (`planned → done`, add `pr:`).
5. **Release** happens automatically when the PR merges (`claims --reap`), or run
   `bash tools/docs.sh release <id>` if abandoning.

## Validator changes

In `cmd_validate` (`tools/docs.sh`), the roadmap branch's allowed statuses change
from `" planned in-progress done "` to `" planned done "`; the terminal set
(`" done "`) is unchanged. No other validation logic changes. The
`docs.sh validate` freshness gate continues to fail any register that uses a
status outside its register's set, so a stray `in-progress` would now be caught.

## Documentation updates

Coupled to the `in-progress` retirement (each must stop advertising the old
vocabulary):

- `docs/superpowers/specs/2026-06-20-docs-registers-consolidation-design.md` — the
  register table and item-flow prose (`planned · in-progress · done` → `planned ·
  done`).
- `.claude/skills/loom-docs-organise/SKILL.md` and
  `.claude/skills/loom-docs-update/SKILL.md` — the `status:` vocab in their
  descriptions/steps.
- `CLAUDE.md` "Documentation registers" section
  (`status: planned|in-progress|done` → `planned|done`).
- `docs/ROADMAP.md` header prose (drops the `in-progress` mention).

Coupled to the new capability:

- `CLAUDE.md` "Documentation registers" section — a sentence on checkout
  (`docs.sh claim/release/claims`) and the `loom-work-checkout` skill.

## Testing

Extend `tools/tests/docs_test.sh` (black-box, per its existing style). The git-ref
mechanics are exercised against a **local bare repo used as `origin`** — no
GitHub:

- **Atomic race** — two `claim <id>` pushes to the same bare origin; exactly one
  succeeds (exit 0), the other is rejected (non-zero) and names the holder.
- **Eligibility gates** — each refusal: unknown id; closed item (`- [x]`);
  non-actionable status; `spec:-`; `spec:` set but file missing; already-claimed.
- **release** — `release <id>` removes the ref; `claims` no longer lists it.
- **Reaping** — with the PR-state probe stubbed (env hook) to report "no open PR":
  a fresh claim is listed `pending` and survives `--reap`; a claim with `since`
  backdated past the grace window is `stale` and `--reap` deletes it.
- **Validator** — a register item with `status:in-progress` now fails
  `docs.sh validate` (regression guard for the retirement).

The PR-state probe is one shell function reading an env override
(e.g. `LOOM_CLAIM_PR_PROBE`) so tests inject a stub instead of calling `gh`; in
normal use the function shells out to `gh pr list --head work/<id> --state open`.

## Non-goals

- **No workflow orchestration** — checkout does not brainstorm, plan, or
  implement; it only claims. (Out of scope: an autonomous "pick the next item and
  build it" dispatcher.)
- **No register `status:in-progress` replacement** — the claim ref *is* the live
  state; we are not adding an `owner:`/`since:` field to the tag block.
- **No heartbeat/TTL renewal** — release is PR-driven; the only time-based rule is
  the fixed pre-PR grace window. (A long pre-PR phase that exceeds grace can be
  re-claimed; in practice a PR opens well inside 60 minutes, and a worker can
  re-`claim` to refresh `since` if needed.)
- **No GitHub-issue mirror** — claims live only as git refs; we do not create an
  issue or label per claim.
