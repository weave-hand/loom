# Documentation registers — consolidation + maintenance skills (design)

> Design doc. Consolidates loom's scattered "what's deferred / planned / broken"
> docs into three parsable registers and the skills that keep them live. Next
> step is an implementation plan (writing-plans).

## Problem

Deferred and planned work is spread across overlapping, drifting docs with no
single parsable source:

- `docs/TO_BE_PLANNED.md` — a flat unplanned checklist (`- [ ]` items).
- `docs/FUTURE.md` — ~30 KB of deferred decisions / tech debt as prose grouped
  by concern, cross-referencing specs.
- `docs/superpowers/specs/2026-06-06-loom-roadmap.md` — the status-of-record
  roadmap (steps, ✅ markers, `PR #NN` refs).
- `docs/spike/ICEBERG_ROADMAP.md` — a separate spike roadmap with its own
  done/deferred tracking.
- ~60 specs + ~60 plans, many with their own "deferred"/"out of scope"/"not
  built" sections, plus known-gap markers in code (`TODO`/`FIXME`, panics-as-
  known-gaps like `quote_ident`).

The result: deferred items get lost, completed items are never marked done in the
deferral docs, and nothing is machine-filterable ("show me everything still open
in acl"). We want to (1) coalesce on a small set of register primitives, (2) make
the markdown programmatically parsable so shell tools can filter completed items,
(3) maintain links between items and to PRs, and (4) keep the registers live as
specs/plans complete.

## The three registers (split by commitment level)

Three top-level files in `docs/`, carved by **commitment level** — is this
committed work, a parked idea, or a known defect?

| File | Holds | `status:` values |
|---|---|---|
| `docs/ROADMAP.md` | Committed / sequenced work — the build plan, what's next | `planned` · `in-progress` · `done` |
| `docs/FUTURE.md` | Deliberately-deferred ideas ("later, if a consumer needs it") | `deferred` · `promoted` · `dropped` |
| `docs/ISSUES.md` | Known defects / gaps / footguns in shipped code | `open` · `fixed` · `wontfix` |

Item flow: an idea sits in FUTURE (`deferred`); when committed it is `promoted`
and a matching ROADMAP item appears (`planned` → `in-progress` → `done`); ISSUES
is orthogonal (defects in shipped code), each `open` until `fixed`/`wontfix`.

## The tagged item grammar

Markdown is the source of truth (these are human-authored judgment with rich
"why deferred" prose — not tool-generated like the code-health registers). Each
item is one markdown list entry with a backtick-wrapped tag block on the title
line and prose indented below:

```
- [ ] **Transitive provenance closure** `{#fut-lineage-closure area:lineage status:deferred from:phase-5 pr:- spec:2026-06-05-control-plane-lineage}`
  `upstream`/`downstream` return one hop. Full closure needs a cycle guard and a
  depth/visited bound. Kept out of P5 so the one-hop queries stay flat.
  Related: [[fut-lineage-stitching]]

- [x] **File supersession / compaction** `{#fut-catalog-compaction area:catalog status:done pr:#... spec:2026-06-17-compaction}`
  Delivered via `Tx::compact_files`…
```

### Fields (in the `{…}` tag block)

| Field | Meaning | Format |
|---|---|---|
| `#id` | Unique slug | prefixed `road-` / `fut-` / `iss-`, kebab-case |
| `area:` | Subsystem | controlled vocab (below) |
| `status:` | Lifecycle state | controlled per register (table above) |
| `from:` | Provenance — what it arose from | freeform token, e.g. `phase-5`, `critical-review`, a spec slug |
| `pr:` | PR refs | comma-joined `#32,#78`, or `-` |
| `spec:` | Spec/plan basenames | comma-joined slugs (no dir, no `.md`), or `-` |

**`area:` controlled vocabulary:** `lineage`, `catalog`, `ontology`, `acl`,
`ingest`, `query`, `transform`, `iceberg`, `build`, `deploy`, `cross-cutting`.
(Extend deliberately; the validator enforces the set.)

**Cross-links:** `[[id]]` references another item by `#id`, in any of the three
files. The validator checks they resolve.

### Why this shape

- The `[ ]` / `[x]` checkbox gives the trivial "filter out completed" filter:
  `grep '^- \[ \]'` = everything unfinished. `[x]` mirrors a terminal
  `status:` (`done`/`fixed`/`wontfix`/`promoted`/`dropped`).
- `status:` / `area:` give finer slicing for shell tools.
- The markdown stays human-browsable and the narrative prose survives — these
  docs are read by people, not just parsed.

### File layout

Each register file:

```
# <Roadmap | Future work | Issues> register

_As of <short-sha>._

<!-- grammar: docs/superpowers/specs/2026-06-20-docs-registers-consolidation-design.md -->

## <Area>

- [ ] **Title** `{#id area:… status:… from:… pr:… spec:…}`
  prose…
```

Items grouped under `## <Area>` headers. One trailing newline, no trailing
whitespace (the `lint` CI job's `end-of-file-fixer` / trailing-whitespace hooks
police `.md` — see `.claude/rules/markdown-lint.md`).

## Tooling: `tools/docs.sh`

One script — validation, reading, and reconciliation helpers. Callable by the
skills, by a prek hook, and directly at the terminal. Pure shell + the hermetic
`gh` (no new build deps).

- **`docs.sh validate [file…]`** — parses every item across the three registers
  and fails (nonzero exit) on: malformed tag block, duplicate `#id`,
  out-of-vocab `area:` or `status:`, unresolvable `[[link]]`, or malformed
  `pr:` / `spec:`. Wired as a **local prek hook** (regex/script style, like
  `tools/check-commit-msg.sh`) so the registers cannot drift into an unparsable
  shape. Also called by both skills before they commit.
- **`docs.sh query <kind> [--area X] [--status Y]`** — the "reading" operation:
  - `open` / `done` — items by checkbox state (optionally filtered).
  - `by-area` — group counts / listing per area.
  - `stale` — `open` items with `pr:-` older than N days (heuristic from git
    blame / file mtime of the `from:` spec).
  - `links <id>` — every item that references `<id>` via `[[…]]`.
- **`docs.sh shipped-open`** — reconciliation helper: for each open item with a
  `pr:` or `spec:` ref, checks whether that PR is merged (`gh pr view`) or the
  spec marked delivered, and prints **candidates to close**. Never auto-closes —
  `loom-docs-organise` reviews the candidates.

## Skills

Two operation-oriented skills under `.claude/skills/`, mirroring the existing
loom-* skill conventions (frontmatter `name`/`description`, companion scripts,
the BLOCK-B PR-on-green machinery copied from `loom-complexity`).

### `loom-docs-organise` (bulk; agentic)

Bootstrap on first run, bulk-reconcile after. Steps:

1. **Mine** — dispatch parallel agents (via `superpowers:dispatching-parallel-agents`)
   across: `FUTURE.md`, `TO_BE_PLANNED.md`, the roadmap, `ICEBERG_ROADMAP.md`,
   all `docs/superpowers/{specs,plans}/` ("deferred" / "out of scope" / "not
   built" sections), code (`TODO`/`FIXME`/known-gap markers), and `gh` PRs
   (merged + open). Each agent returns structured candidate items
   `{title, area, status, from, prs, specs, prose}`.
2. **Dedupe / merge** — collapse the same item surfaced from multiple sources;
   assign stable `#id`s (reuse existing ids on re-run — match on title/area).
3. **Classify** into ROADMAP / FUTURE / ISSUES by commitment level.
4. **Reconcile** — flip previously-open items whose work shipped to
   `done`/`fixed` (+`pr:`), using `docs.sh shipped-open` candidates.
5. **Render** the three files in the grammar, preserving prose →
   `docs.sh validate` → `prek run --all-files` → **PR against `main`, merge on
   green CI** (BLOCK B copied from `loom-complexity`).
6. **`diff` arg** — report drift (what would change) to the terminal, no commit.

First run also performs the one-time **migration** (below).

### `loom-docs-update` (single-item; lightweight; at completion)

Given the just-completed spec/plan (or a free-form description):

1. Read the completed spec/plan.
2. **Close** the items it resolved: set `[x]`, terminal `status:`, add the `pr:`.
3. **Add** any newly-deferred items the spec introduced (its "deferred" / "out
   of scope" section) as new FUTURE/ISSUES entries with `from:<spec-slug>`.
4. If it satisfied a committed ROADMAP item, mark that `done`.
5. `docs.sh validate`, then stage the register edits alongside the work (no
   separate PR — rides the feature branch). No agentic fan-out.

## Completion reminder hook

`tools/docs-remind.sh`, wired as a `Stop` hook in `.claude/settings.json`
(joining the existing `SessionStart` hook). NO-OP — mirroring
`cloud-session-start.sh`'s "inert unless" pattern — **unless** all hold:

- the branch is not `main`, and
- the branch's commits touch `docs/superpowers/{plans,specs}/`, and
- none of its commits touch `docs/ROADMAP.md` / `FUTURE.md` / `ISSUES.md`.

Then it emits a one-line advisory nudge to run `loom-docs-update`. **Advisory,
non-blocking** (exit 0 with message) — never blocks a stop.

## One-time migration (inside the first `loom-docs-organise` run)

- `docs/TO_BE_PLANNED.md` — items → ROADMAP (`planned`) / FUTURE (`deferred`);
  file deleted.
- `docs/superpowers/specs/2026-06-06-loom-roadmap.md` — content → `docs/ROADMAP.md`;
  original archived as `…-loom-roadmap.md.old` (as the prior roadmap was).
- `docs/spike/ICEBERG_ROADMAP.md` — its tracked done/deferred items → registers
  under `area:iceberg`; the "what it is" narrative kept as prose (registers
  become the single source for *items*, spike docs stay pure prose).
- `docs/FUTURE.md` — existing prose restructured into tagged items in place
  (same path, new grammar).

## CLAUDE.md updates

- Add a **"Documentation registers"** section documenting the grammar, the two
  skills, `tools/docs.sh`, and the prek validate hook.
- Repoint existing references from the old roadmap / `TO_BE_PLANNED` paths to
  `docs/ROADMAP.md` (the roadmap "status of record" pointer in the project-status
  paragraph) and note `FUTURE.md` / `ISSUES.md` as the deferral/defect registers.

## Non-goals

- No JSON source-of-truth / jq renderer (markdown IS the source — unlike the
  code-health registers). No new build target or third-party dep.
- No auto-close on reconciliation — `shipped-open` only proposes candidates.
- No blocking hook — the completion reminder is advisory.
- Not a general issue tracker / GitHub Issues replacement — these registers track
  loom's own deferred/planned/defect items, linked *to* PRs, not a workflow tool.

## Testing

- `docs.sh validate` is exercised by a small fixture register (valid + each
  failure mode) so the parser's rejections are pinned.
- `docs.sh query` outputs checked against a fixture register for each kind.
- The grammar is self-validating: after the migration, `docs.sh validate` over
  the three real registers must pass (and runs as a prek hook thereafter).
- Skills are validated by the existing loom convention (run on the repo, inspect
  the PR diff) — no unit harness for the agentic mining pass.
