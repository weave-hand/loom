# MV delta over inline-only sources Design

> **Status:** design (direction). This spec makes
> `iss-mv-delta-inline-source-unflushed` build-ready. The item stays in
> ISSUES (a defect in shipped code); a separate work agent writes the
> implementation plan from it and builds it.

## Problem

A micro-batch MV **chained** onto a fresh upstream MV output cannot run until
the upstream flushes. Every `commit_micro_batch` write lands on the inline
tier (`inline_append_mv`), which never creates an Iceberg SQL-catalog row —
that row only appears via the Parquet-write path (`ensure_iceberg_table`).
But `mv_delta_scan`'s file leg calls `read_files_as_batches`
(`engine-serving/src/mv_delta.rs:131`), which **unconditionally** calls
`catalog.load_table(&ident)` (`postgres/src/iceberg_read.rs:32`) — even when
the file list is empty — purely to derive an Arrow schema from
`tbl.metadata().current_schema()` (`iceberg_read.rs:38-40`). For an
inline-only table, `load_table` errors and the MV run fails at the source
read.

The in-tree confession: `worker/tests/stream_mv_join_triggers.rs:594-612`
carries an explicit `flush_table` workaround with a comment describing this
exact defect ("an MV output that has ONLY ever been inline-appended … has no
such row yet").

Note the asymmetry that makes this a plain bug rather than a design gap:
`mv_delta_locked` already guards the *mirror* snapshot's absence
(`mv_delta.rs:101-108`) — but `current_snapshot` reads `iceberg_mirror.*`,
which the inline path DOES populate, while `load_table` reads the vendored
`iceberg_tables` catalog, which it does not. And the serving path already
solved the same problem mirror-authoritatively: `build_serving_provider`
never calls `load_table` — schema from `arrow_schema_from_mirror`, file tier
skipped when empty, zero-row provider when both tiers are absent (the #421
fix, `engine-serving/src/serving.rs:168-253`).

## Design

Minimal, mirror-authoritative, matching the #421 pattern:

1. In `mv_delta_locked` (`mv_delta.rs:93-201`): when `paths.is_empty()`,
   **skip `read_files_as_batches` entirely** — the file tier contributes
   nothing. The union input is then just `inline_live_batch_full`'s batch
   (already fetched, `mv_delta.rs:140-143`).
2. Derive the schema the union/predicate stage needs from the **mirror**
   via the existing `framed_schema(&user_cols)` helper
   (`mv_delta.rs:206-216` — user columns + `framing_column_specs`), instead
   of from the loaded Iceberg table. If the non-empty-files leg still uses
   `load_table`'s schema today, leave that leg untouched (it has a catalog
   row by construction — files exist).
3. Delete the `flush_table` workaround + its comment from
   `stream_mv_join_triggers.rs` — the chained-MV e2e then runs against an
   unflushed upstream output and becomes the regression test.

Explicitly **not** in scope: making `read_files_as_batches` itself
mirror-schema-authoritative (accept a schema argument / drop `load_table`).
It has other callers whose tables always have catalog rows; that refactor is
a behavior-preserving cleanup to take opportunistically, not a prerequisite.
(Related: `#fut-iceberg-schema-cache`, `#iss-serving-empty-table-not-found`'s
closed pattern.)

## Non-regression

- Sources with at least one flushed file take the existing path
  byte-identically.
- The empty-file leg produces the same framed schema the flushed leg would
  (both derive from the same mirror columns + framing specs) — the union and
  offset predicate see no difference.
- No migration, no new SQL expected (mirror schema reads exist).

## Testing

- **Chained MV without flush** (the workaround deletion) —
  `stream_mv_join_triggers.rs` runs green with the explicit `flush_table`
  removed: upstream MV commits inline-only output; downstream MV reads it as
  a delta source and produces correct rows.
- **Inline-only source unit** (engine-serving fixture) — `mv_delta_scan`
  over a declared log table with live inline rows and zero files: full
  delta, correct framing/order; then flush and re-scan from a later
  watermark: file+inline union still correct (the transition case).
- **Empty source** — inline-only table with zero rows at/above the
  watermark: empty delta, no error.

## Global constraints (loom-specific, carry into the plan)

- Strict clippy (pedantic + restriction); `#[expect(lint, reason)]` locally.
- `buck2 run //tools:prek -- run --all-files` before every commit.
- Tests are `rust_test`/`loom_fixture_test` targets only.
- If SQL changes, `tools/sqlx-prepare.sh` + commit `.sqlx`.

## Acceptance

1. A downstream MV consumes an unflushed upstream MV output with no
   explicit flush anywhere.
2. The `stream_mv_join_triggers.rs` workaround is gone.
3. Existing suites green (`buck2 test //src/...`).

## Interfaces (names the plan consumes)

- Consumes: `mv_delta_locked` / `framed_schema`
  (`engine-serving/src/mv_delta.rs:93-216`); `read_files_as_batches` /
  `load_table` (`postgres/src/iceberg_read.rs:25-60`);
  `inline_live_batch_full`; `build_serving_provider`'s skip-empty pattern
  (`engine-serving/src/serving.rs:168-253`).
- Produces: the empty-files fast path in `mv_delta_locked` (mirror-derived
  schema, no `load_table`); the un-workaround'd chained-MV e2e.
