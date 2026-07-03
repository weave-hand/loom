# Docs clearout — system-capabilities consolidation

**Date:** 2026-07-03
**Status:** approved
**Driver:** the docs tree carries 346 spec/plan files (9.3 MB) under
`docs/superpowers/`, and the registers carry 234 closed items against 146 open.
Completed work's knowledge is scattered across per-slice specs; nothing describes
what the system can do *today*. This clearout consolidates landed capability into
durable per-subsystem docs, then deletes the per-slice artifacts they supersede.

## Goals

1. **`docs/system-capabilities/<subsystem>.md`** — synthesized prose per subsystem:
   what it can do today (behaviour, endpoints/APIs, guarantees, key design
   decisions), distilled from that subsystem's closed register items and their
   specs, with PR numbers referenced inline. Readable standalone; the deleted
   specs become genuinely disposable (git history is the archive). Plus a short
   `README.md` index.
2. **Leaner registers** — ROADMAP and ISSUES keep open items only; FUTURE drops
   terminal (`promoted`/`dropped`) entries *and* gets a stale-cull triage of its
   135 open deferrals (culled list quoted verbatim in the PR body for review).
3. **Orphaned spec/plan deletion** — everything under `docs/superpowers/{specs,plans}/`
   goes except the keep-list below.

## Subsystem split

| Doc | Covers (register `area:` mapping) |
|---|---|
| `control-plane.md` | queue, catalog + Iceberg mirror, ontology, ACL, lineage, auth (`catalog`, `ontology`, `acl`, `lineage`) |
| `ingest.md` | landing, materializer, dataset→model binding, model inference, constraints (`ingest`) |
| `query-api.md` | governed object reads, link traversal, graph queries, filters, actions incl. update/delete, object sets, search (`query`) |
| `engine.md` | engine service, internal Flight SQL wire, DataFusion serving, inline writes/flush, compaction, COW (`iceberg` + engine-side `query`) |
| `transform.md` | transform workers, typed transforms (`transform`) |
| `vector-search.md` | vector column type, HNSW/IVF indexes, search endpoint, rebuild triggers (vector items across areas) |
| *(merge into existing `docs/deploy.md`)* | OCI images, Helm chart, standalone binary, embedded Postgres (`deploy`) |
| `build-and-test.md` | buck2/RE/coverage/clippy/CI, code-health routines, docs-register tooling, testkit/e2e harness/property tests (`devx`, `quality`, `test`) |
| `ui.md` | the Yew/WASM UI experiment (`ui`) |

Items whose area straddles docs (e.g. `cross-cutting`) go where their behaviour
lives; the drafting agent for each subsystem receives the closed items assigned
to it explicitly, so no item is silently unassigned.

## Capability-doc content contract

Each doc follows the same shape:

- H1 `# <Subsystem> capabilities`, then a one-paragraph scope statement.
- `_As of <main short-sha>._` provenance line.
- Prose sections grouped by capability theme (not by PR): what exists, how it
  behaves, its guarantees/limits, and the key design decisions that shaped it.
  PR numbers inline as `(#N)` at the claim they support.
- A closing `## Known gaps` section linking the subsystem's open ISSUES/ROADMAP
  ids (by `#id` tag, since specs for open items survive).
- No task lists, no status tags, no register grammar — these are narrative docs,
  not registers. `tools/docs.sh validate` does not parse them.

## Register slimming

- **ROADMAP.md / ISSUES.md**: delete all `- [x]` entries; keep the header prose
  (updated to mention that landed capability is documented in
  `docs/system-capabilities/`) and the open items verbatim.
- **FUTURE.md**: delete terminal entries; triage the open deferrals and drop the
  stale ones (superseded by landed work, absorbed, or no longer plausible) by
  deleting them — the PR body quotes every culled item verbatim so the review
  can restore any. Survivors keep their grammar untouched.
- **Cross-links**: after slimming, strip or retarget any `[[id]]` reference to a
  removed item so `tools/docs.sh validate` stays green.

## Spec/plan keep-list

Delete everything under `docs/superpowers/{specs,plans}/` **except**:

1. Specs referenced by surviving open register items (post-triage) — currently:
   `2026-06-28-embedded-postgres-lifecycle-design`,
   `2026-07-01-action-computed-assignments-design`,
   `2026-07-01-action-enqueue-downstream-design`,
   `2026-07-01-action-multi-object-design`,
   `2026-07-01-auth-comprehensive-adopt-design`,
   `2026-07-02-pillar-idioms-audit-design`, plus any spec named by a FUTURE
   survivor.
2. Specs referenced from outside `docs/superpowers/` (CLAUDE.md, `.claude/skills/`,
   `tools/`, `src/` code comments, README, spikes):
   `2026-06-06-loom-roadmap`, `2026-06-06-control-plane-critical-review`,
   `2026-06-09-ducklake-single-catalog-write-recipe`,
   `2026-06-10-query-governed-object-read-slice-design`,
   `2026-06-12-qualified-dataset-identity-design`,
   `2026-06-15-whole-codebase-coverage-bxl-design`,
   `2026-06-17-iceberg-datafusion-serving-engine-design`,
   `2026-06-20-docs-registers-consolidation-design`,
   `2026-06-21-work-item-planning-checkout-design`,
   `2026-06-22-iceberg-inline-pg-tableprovider-design`,
   `2026-06-24-engine-serving-execution-wire-design`,
   `2026-06-25-cloud-session-cold-build-reliability-design`,
   `2026-06-26-governed-flight-export-design`,
   `2026-06-26-stricter-clippy-config-design`,
   `2026-06-29-engine-serving-write-relocation-design`,
   `2026-06-29-sqlcatalog-execute-commit-error-design`,
   `2026-07-01-dataset-naming-bridge-design`,
   `2026-07-01-lineage-acl-filtering-design`.
   (The keep-list is recomputed mechanically at execution time by re-running the
   reference grep, so drift since this spec is caught.)
3. Plans: only `2026-06-15-whole-codebase-coverage-bxl` (CLAUDE.md references the
   spec+plan pair by glob) and any plan whose filename pairs a kept open-item
   spec. All other plans are execution artifacts of merged work — deleted.
4. This spec itself.

Source-code comments referencing kept specs are untouched (that is why those
specs are kept — no `src/` churn in this PR).

## Execution shape

- One drafting agent per subsystem, fanned out in parallel; each receives its
  closed-item list (title + prose + PR#s) and the paths of the relevant specs,
  and returns the capability doc body. The orchestrator reviews each draft
  against the content contract, assembles the files, then performs the register
  slimming, FUTURE triage, and keep-list deletion mechanically.
- Gates before commit: `bash tools/docs.sh validate` green; recompute the
  reference grep to prove no dangling `docs/superpowers/` path anywhere outside
  `docs/superpowers/` itself; `buck2 run //tools:prek -- run --all-files` clean.
- CLAUDE.md: add a pointer that landed capability is documented under
  `docs/system-capabilities/` (registers stay the source of truth for
  planned/deferred/broken).
- Lands as **one PR** from `docs/system-capabilities-clearout`; the FUTURE cull
  list rides in the PR body. Not auto-merged — user reviews.

## Out of scope

`docs/spike*/`, `docs/stpa/`, `docs/build-execution.md`,
`docs/error-handling-debt.md`, `docs/grimoire-kg-agenda.md`, `docs/deploy.md`
(except the deploy-capability merge-in), and all `.claude/skills/` content.
No source-code edits.
