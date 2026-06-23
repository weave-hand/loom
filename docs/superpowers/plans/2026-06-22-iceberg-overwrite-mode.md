# Iceberg overwrite/replace write mode — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Spec:** [`docs/superpowers/specs/2026-06-22-iceberg-overwrite-mode-design.md`](../specs/2026-06-22-iceberg-overwrite-mode-design.md) (`road-iceberg-overwrite-mode`).

**Goal:** Add an `overwrite_parquet_snapshot` primitive — the Iceberg twin of DuckLake's `Tx::replace_files`. In one Postgres transaction it makes a new set of files the table's sole live set while the prior files stay reachable by time travel: end-cap every currently-live `iceberg_mirror.data_file` row at the new snapshot, project the new files at that snapshot, append them on the Iceberg metadata side (`fast_append`, as today), and emit lineage — all atomic. This unblocks `road-iceberg-transform-writes` (the next slice). It wires no caller and flips no default.

**Architecture:** loom-governed reads resolve through the `iceberg_mirror.*` rows, so *overwrite semantics live entirely in a mirror end-cap*, not in a real Iceberg overwrite action (see spec §"Why mirror-faithful"). The Iceberg metadata side is the unchanged `fast_append`. The only delta from the existing append path is: inside the one commit transaction (`SqlCatalog::do_update_table` → `write_mirror`), end-cap all live data files for the table at the freshly-allocated snapshot **before** projecting the new files. This is threaded as a new `overwrite: bool` flag through the existing commit-extras seam (`CommitExtras` → `CommitExtrasCatalog` → `append_batches_with_extras` → `append_parquet_snapshot`), so the atomicity, retry, and lineage guarantees of the append path are inherited verbatim.

**Tech Stack:** Rust, iceberg 0.9, arrow/parquet 57 (renamed `parquet57`/`arrow_*57` in this crate), sqlx 0.9 compile-time macros, Postgres, buck2.

## Global Constraints

- **Tests are `rust_test` integration targets only** — never inline `#[cfg(test)]`/`#[test]` in `src/**.rs` (the `no-inline-tests` prek hook fails otherwise). Each test is a sibling `tests/<name>.rs` wired as its own target in the crate's `BUCK`.
- **Fixture-backed tests use the `loom_fixture_test` macro** (`src/control-plane/postgres/defs.bzl`), not a bare `rust_test` — they boot Postgres/object-store which refuse to run as root on RE.
- **Compile-time SQL.** Any new/changed `sqlx::query!`/`query_scalar!` in the postgres crate requires regenerating the committed `.sqlx` cache via `tools/sqlx-prepare.sh` and committing it. The `//src/control-plane/postgres:sqlx-cache-check` test enforces freshness. **Note:** the end-cap SQL this plan extracts is byte-identical to the statement already in `mark_dropped` (`update iceberg_mirror.data_file set end_snapshot = $2 where table_id = $1 and end_snapshot is null`); sqlx caches by query text, so a byte-identical extraction reuses the existing cache entry. Still run `tools/sqlx-prepare.sh` and commit any diff (zero diff is the expected, valid outcome).
- **Append-only behavior is unchanged.** Overwrite is a *new, separately-invoked* path. The existing `append_parquet_snapshot` callers (`land_parquet`, `iceberg_flush`) must keep identical behavior — they pass `overwrite = false`. No default flips, no caller rewiring.
- **Mirror-faithful only.** Do NOT drive a real Iceberg overwrite/replace action or rewrite raw Iceberg manifest metadata. The Iceberg side stays `fast_append`; replaced files remaining in the raw metadata until GC is the accepted gap class of `iss-iceberg-inline-visibility` (spec §"Why mirror-faithful"), tracked for `fut-iceberg-gc`, NOT closed here.
- **Lineage is the loom `LineageEvent`, not DuckLake change-segments.** The spec's "`deleted_from_table` + `inserted_into_table` change segments" describe DuckLake's `ducklake_snapshot_changes.changes_made` text; the Iceberg mirror has **no** `snapshot_changes` analog. For Iceberg the overwrite emits the caller-provided `LineageEvent` atomically via the existing `pg_emit` in `do_update_table` — exactly as the append path does. The lineage test asserts the event is recorded and readable through `cp.events_for(run)` (the shape `iceberg_landing.rs` tests already assert), not change-segment text.
- **Don't pipe `buck2 test` through `tail`/`head`.** Redirect: `buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`.
- Markdown files (incl. this plan) end with exactly one trailing newline and no trailing whitespace (`end-of-file-fixer` / `trim trailing whitespace` hooks).

---

## File Structure

- **Modify** `src/control-plane/postgres/src/iceberg_mirror.rs` — add `pub async fn end_cap_live_data_files(conn, table_id, at)`; refactor `mark_dropped`'s data-file leg to call it.
- **Modify** `src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs` — add `overwrite: bool` to `CommitExtras`; thread it into `write_mirror` (end-cap live data files after `ensure_table`, before `project_files`).
- **Modify** `src/control-plane/postgres/src/iceberg_writer.rs` — add `overwrite` to `CommitExtrasCatalog` and to `append_batches_with_extras`; forward it into the `CommitExtras` the decorator builds.
- **Modify** `src/control-plane/postgres/src/iceberg_landing.rs` — add `overwrite: bool` to `append_parquet_snapshot`; add `pub async fn overwrite_parquet_snapshot(...)` wrapper; handle the zero-file truncate branch (see Task 3).
- **Create** `src/control-plane/postgres/tests/iceberg_overwrite.rs` — the replace-contract / stats / truncate / lineage / atomicity fixture tests (`loom_fixture_test`).
- **Modify** `src/control-plane/postgres/BUCK` — wire the new test target.
- **Refresh** `src/control-plane/postgres/.sqlx/` (expected zero diff; commit if any).

---

## Task 1: Extract `end_cap_live_data_files`, refactor `mark_dropped`

Pure, behavior-preserving refactor that produces the shared end-cap helper both `mark_dropped` (table drop) and the overwrite path use. The data-file leg of `mark_dropped` already *is* the end-cap a replace needs (spec §"Current state").

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_mirror.rs`

**Interface:**
```rust
/// End-cap (set `end_snapshot = at`) every currently-live `iceberg_mirror.data_file`
/// row for `table_id`, leaving its `table`/`column` rows untouched. The data-file half
/// of a drop, and the whole of a replace's "expire old files" step.
pub async fn end_cap_live_data_files(
    conn: &mut PgConnection,
    table_id: i64,
    at: SnapshotId,
) -> Result<()>
```

**Implementation:**
- Move the exact `update iceberg_mirror.data_file set end_snapshot = $2 where table_id = $1 and end_snapshot is null` statement (currently `iceberg_mirror.rs:215-222`) into this helper, keeping the SQL text byte-identical (so the `.sqlx` cache entry is reused).
- Rewrite `mark_dropped`'s data-file leg to call `end_cap_live_data_files(conn, tid, at)`. The `table` and `column` end-caps in `mark_dropped` stay inline (a drop ends those too; a replace does not).

**Verification:** `buck2 test //src/control-plane/postgres:...` — the existing drop/delete-contract tests (`iceberg_live_tables`, `iceberg_catalog`) that exercise `mark_dropped` stay green, proving the refactor is behavior-preserving. No new test in this task.

- [ ] `end_cap_live_data_files` added; `mark_dropped` calls it; SQL byte-identical
- [ ] `tools/sqlx-prepare.sh` run; `.sqlx` committed (expect zero diff)
- [ ] postgres-crate tests green

---

## Task 2: Thread `overwrite` through the commit-extras seam

Carry an `overwrite` flag from the writer entrypoint into `write_mirror`, where it triggers the live-file end-cap **at the same snapshot** the new files are projected, **before** `project_files` (so only pre-existing files are end-capped, never the just-projected ones).

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs`
- Modify: `src/control-plane/postgres/src/iceberg_writer.rs`

**Interfaces & changes:**
- `CommitExtras` (catalog.rs:204) — add `pub overwrite: bool` (its `#[derive(Default)]` gives `false`, so `CommitExtras::default()` — used by the plain `update_table` impl — is unchanged).
- `SqlCatalog::write_mirror` (catalog.rs:360) — add an `overwrite: bool` parameter. After `ensure_table` returns `tid` and before `project_files`:
  ```rust
  if overwrite {
      crate::iceberg_mirror::end_cap_live_data_files(conn, tid, at).await?;
  }
  ```
  Ordering is load-bearing: `next_snapshot` → `ensure_table` → (overwrite end-cap) → `project_columns` (if absent) → `project_files`. End-capping before projecting means the new rows (also `end_snapshot is null`, also `table_id = tid`) are written *after* the end-cap and stay live; the old rows get `end_snapshot = at`. Old rows keep `begin_snapshot < at`, so prior snapshots still time-travel to them.
- `do_update_table` (catalog.rs:391) — pass `extras.overwrite` to `write_mirror`.
- `CommitExtrasCatalog` (iceberg_writer.rs:71) — add `overwrite: bool` field; include it in the `CommitExtras { lineage, end_cap, overwrite }` it constructs in `update_table` (iceberg_writer.rs:90). Update its `Debug` impl to show it.
- `append_batches_with_extras` (iceberg_writer.rs:166) — add an `overwrite: bool` parameter; set it on the `CommitExtrasCatalog`. Update both existing callers:
  - `append_batches_with_lineage` (iceberg_writer.rs:201) → pass `false`.
  - `append_parquet_snapshot` (iceberg_landing.rs:175) → forwards its own new `overwrite` param (Task 3).

**Verification:** crate builds; existing append/flush/landing tests green (they now pass `overwrite = false` and must behave identically).

- [ ] `CommitExtras.overwrite` added; `Default` still yields append behavior
- [ ] `write_mirror` end-caps live files before projecting **iff** `overwrite`
- [ ] `overwrite` threaded through `CommitExtrasCatalog` + `append_batches_with_extras`; both existing callers pass `false`
- [ ] postgres-crate tests green (append path unchanged)

---

## Task 3: `overwrite_parquet_snapshot` surface + truncate branch

Add the public primitive the spec specifies, plus the zero-file truncate handling.

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_landing.rs`

**Interfaces & changes:**
- `append_parquet_snapshot` (iceberg_landing.rs:134) — add a trailing `overwrite: bool` parameter; forward it to `append_batches_with_extras`. Update its callers: `land_parquet` (iceberg_landing.rs:197) passes `false`; `iceberg_flush.rs:102` passes `false`.
- New public wrapper (the spec's surface):
  ```rust
  /// Replace `table`'s live data with `batches` in one Postgres transaction —
  /// the Iceberg twin of DuckLake `Tx::replace_files`. End-caps every currently-live
  /// data file at the new snapshot, projects the new files (with per-column stats),
  /// appends them on the Iceberg side (`fast_append`), and emits `lineage` atomically.
  /// Prior files remain reachable by time travel. Insert side mirrors
  /// `append_parquet_snapshot`; only the live-file end-cap differs.
  pub async fn overwrite_parquet_snapshot(
      pool: &PgPool,
      catalog: &SqlCatalog,
      table: &TableRef,
      columns: &[ColumnSpec],
      batches: Vec<RecordBatch>,
      lineage: Option<&LineageEvent>,
  ) -> Result<SnapshotId>
  ```
  - **Non-empty `batches`:** delegate to `append_parquet_snapshot(pool, catalog, table, columns, batches, lineage, /*end_cap*/ None, /*overwrite*/ true)`.
  - **Empty `batches` (truncate):** the Iceberg writer cannot produce a snapshot from zero data files (`fast_append` over an empty file set may no-op the commit, so `do_update_table`/`write_mirror` would never run and no end-cap would happen). Handle truncate with a **mirror-only** transaction that does not touch the Iceberg writer/catalog: open a PG tx on `pool` and run, in order, `next_snapshot` → `ensure_table(ns, name, at)` (so an absent table still records the overwrite event) → `end_cap_live_data_files(conn, tid, at)` (matches zero rows if the table had none) → `pg_emit(lineage)` if `Some` → commit; then read back the mirror current snapshot via `IcebergCatalog::new(pool).current_snapshot(table)`. This makes the new (empty) set the sole live set and preserves time travel to prior files — identical to DuckLake's empty-replace, whose change-segment code guards the empty-insert case (spec §"Error handling").
    - `pg_emit` is already `pub(crate)` in `crate::lineage` (verified: `src/control-plane/postgres/src/lineage.rs:6`, signature `pg_emit<'e, E: sqlx::PgExecutor<'e>>(exec: E, ev: &LineageEvent)`); call it directly from `iceberg_landing` via `use crate::lineage::pg_emit` — no re-export needed.
    - **Verified**: `IcebergCatalog::current_snapshot` (iceberg_catalog.rs:158) reads from `iceberg_mirror.snapshot` joined to the table's begin/end window — it is mirror-sourced, so a mirror-only snapshot allocated by `next_snapshot`/`ensure_table` IS visible to it without any Iceberg-metadata advance.

> **TDD note on the empty branch:** write the truncate test first (Task 4). If, on `fast_append` over zero files, iceberg 0.9 *does* commit a snapshot (so the unified `overwrite = true` path already end-caps and projects nothing correctly), prefer that single path and delete the mirror-only branch — simpler is better. The branch above is the defensive default for the likely no-op case; let the test decide which survives. Either way the truncate test must pass.

- [ ] `append_parquet_snapshot` takes `overwrite`; `land_parquet` + `iceberg_flush` pass `false`
- [ ] `overwrite_parquet_snapshot` public, non-empty path delegates with `overwrite = true`
- [ ] truncate (empty `batches`) path proven by test (unified or mirror-only, per Task 4)

---

## Task 4: Fixture tests (the contract)

Mirror `snapshot_replace.rs` for Iceberg. New file `tests/iceberg_overwrite.rs`, wired with `loom_fixture_test`. Reuse the harness shape from `tests/iceberg_landing.rs`: `PgFixture::start`, `make_catalog` (LocalFs warehouse via `tempfile::tempdir`), `fx.pool_for(&db)`, `IcebergCatalog`, `cp.events_for(run)`. Seed initial data by either `land(... , /*limit*/ 0, ...)` (forces real Parquet) or a direct `append_parquet_snapshot(..., overwrite=false)`; build replacement `RecordBatch`es directly (arrow-57 `arrow_array::RecordBatch`, same type the landing module uses) and call `overwrite_parquet_snapshot`.

**Files:**
- Create: `src/control-plane/postgres/tests/iceberg_overwrite.rs`
- Modify: `src/control-plane/postgres/BUCK` (new `loom_fixture_test` target; mirror the `iceberg_landing` target's deps)

**Tests (one `#[tokio::test(flavor = "multi_thread", worker_threads = 2)]` each):**
1. **`overwrite_expires_old_and_preserves_time_travel`** (core — the Iceberg twin of `snapshot_replace.rs`): append `a` (N rows) → snapshot `s1`; overwrite with `b` (M rows) → snapshot `s2`. **Verified**: `IcebergCatalog::files_with_stats(table, at)` (iceberg_catalog.rs:67) is snapshot-scoped (`f.begin_snapshot <= at and (f.end_snapshot is null or f.end_snapshot > at)`), so assert directly: `files_with_stats(t, s2)` lists only `b` (M rows); `files_with_stats(t, s1)` lists only `a` (N rows); `IcebergCatalog::current_snapshot(t)` returns `s2`. No raw-sqlx fallback needed — the snapshot-scoped read proves time travel.
2. **`replaced_files_carry_per_column_stats`**: after overwrite, the replacement file's rows in `data_file_column_stat` are present and correct (assert via `files_with_stats`), so the serving pruner operates on replaced data immediately (reuse the assertion style from `iceberg_files_with_stats.rs`).
3. **`truncate_overwrite_with_zero_files`**: append `a` → overwrite with **zero** files → current snapshot lists no live files; prior snapshot still time-travels to `a`. This is the test that decides Task 3's truncate branch.
4. **`overwrite_emits_lineage`**: the overwrite emits the caller-provided `LineageEvent`; assert `cp.events_for(run)` returns it with the expected output dataset (same shape as `iceberg_landing.rs`'s lineage assertions).
5. **`overwrite_atomicity_leaves_prior_set_intact`**: induce a failure inside the overwrite tx (e.g. a lineage event that violates a constraint, or a deliberately bad column/table input that errors after the end-cap stage) and assert the original live set is unchanged — no orphaned end-caps (the `a` data-file row still has `end_snapshot is null`), no half-projected new files. Pick the cheapest reliable failure injection that exercises the post-end-cap rollback; if none is clean without a test hook, assert atomicity via the natural CAS-conflict path or document the chosen injection in the test's doc comment.

> If `IcebergCatalog` lacks a snapshot-scoped file read (only "current"), assert time travel by reading the mirror rows' `begin_snapshot`/`end_snapshot` directly with a small `sqlx` query in the test (tests may use runtime `sqlx::query`), matching how `iceberg_*` fixture tests inspect mirror state.

- [ ] `tests/iceberg_overwrite.rs` created with the five tests
- [ ] BUCK target wired (`loom_fixture_test`)
- [ ] all five pass under `buck2 test`

---

## Task 5: Full-suite green + register close

**Files:**
- Modify: `docs/ROADMAP.md` (via `loom-docs-update` at PR time)

**Steps:**
- Run the **full** `buck2 test //src/...` (not just the postgres crate) — append-only behavior and all defaults unchanged; the `sqlx-cache-check` test green.
- Run `buck2 run //tools:prek -- run --all-files` and commit any hook fixes (markdown/whitespace).
- At PR time, close `road-iceberg-overwrite-mode` in `docs/ROADMAP.md` via `loom-docs-update`: `- [ ]`→`- [x]`, `status:planned`→`status:done`, add `pr:#N`.

- [ ] `buck2 test //src/...` fully green
- [ ] prek clean
- [ ] register item closed in the PR

---

## Acceptance criteria (from the spec)

1. An Iceberg table can be overwritten: the new files become the sole live set; prior files remain reachable by time travel — identical contract to DuckLake `replace_files` (`snapshot_replace.rs`), now passing for Iceberg. *(Task 4 test 1)*
2. End-cap + project + Iceberg metadata + lineage commit in one Postgres transaction; failure leaves the prior live set intact. *(Tasks 2–3; Task 4 test 5)*
3. Replaced files carry per-column stats; the serving pruner operates on them. *(Task 4 test 2)*
4. Truncate (overwrite with zero files) and overwrite-of-empty both behave per the DuckLake contract. *(Task 3; Task 4 test 3)*
5. `buck2 test //src/...` green; append-only behavior and all defaults unchanged. *(Task 5)*
