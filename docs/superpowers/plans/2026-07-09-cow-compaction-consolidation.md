# Scalable COW Slice 2 — Tombstone-Aware Compaction Consolidation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A background consolidation for shadow-bearing non-CDC identity tables: fold the inline tier (appends + versions + tombstones) with the base Parquet into a new base snapshot (latest-per-identity wins, tombstoned identities dropped), retire exactly the consumed inline rows in the same commit, and re-enable the flush slice 1 suppressed — closing `road-cow-compaction-consolidation` (the unbounded-inline-growth gap).

**Architecture:** The fold is `build_merge_view`'s `Precedence::Snapshot` materialized, run engine-side under `lock_table`, committed by a new **consuming** overwrite (`overwrite: true` + targeted `InlineEndCap` — the blanket inline cap is skipped, so a mutation landing mid-consolidation survives live and keeps shadowing; no lock touches the hot mutation path). Enqueue reuses the existing `stream_consolidate` job / `ConsolidateStream` RPC / `consolidate_trigger` seam — the engine entry becomes a per-table-kind dispatch and the trigger's `cdc &&` gate is dropped. `has_shadow` clears only when quiescent; the flush backstop is hardened to derive safety from its own read set. Spec: `docs/superpowers/specs/2026-07-09-cow-compaction-consolidation-design.md`.

**Tech Stack:** Rust, sqlx runtime `AssertSqlSafe` (dynamic `inline_<tid>` SQL), DataFusion, Iceberg, buck2 `loom_fixture_test`/`rust_test`, tonic (no proto changes).

## Global Constraints

Carried from the spec; every task implicitly includes these:

- **Tests are `rust_test` / `loom_fixture_test` integration targets only — never inline `#[cfg(test)]`.** New fixture tests MUST use `loom_fixture_test` (`src/control-plane/postgres/defs.bzl`) and wire their own BUCK target mirroring a named sibling. The `no-inline-tests` prek hook fails on any inline `#[test]`.
- **Clippy is strict (pedantic + restriction):** no `unwrap`/`expect`/`indexing_slicing`/`panic`/`todo`/`map_err_ignore` in production lib/bin code; `#[expect(lint, reason = "...")]` for justified local exceptions. Test code is exempt via the test macros.
- **All new SQL is runtime `AssertSqlSafe`** (dynamic `inline_<tid>` identifiers / the standalone `shadow_flag` table — the slice-1 precedent, see `set_has_shadow`). No `query!` changes are planned, so no `tools/sqlx-prepare.sh` run should be needed; if one sneaks in, regenerate and commit `.sqlx/`.
- **Run prek before every commit:** `buck2 run //tools:prek -- run --all-files` (stage new files with `git add` first — prek skips untracked files). Markdown: no trailing whitespace, exactly one trailing newline.
- **Non-regression is part of every task's definition of done.** These existing targets must stay green throughout: `//src/control-plane/postgres:flush-suppression`, `:overwrite-end-caps-inline` (the blanket cap on the *plain* overwrite is asserted there and must not change), `:inline-delta-cas`, `:inline-tombstone`, `//src/services/engine-serving:merge-on-read`, `:consolidate-lock`, `//src/services/query-api:cow-inline-shadow-e2e`, `:cow-inline-shadow-gov-e2e`, `:stream-cdc-consolidate`, `//src/services/worker:stream-consolidate-job`.
- **Build/test commands:** build `buck2 build -v0 --console none //src/...`; test `buck2 test --console none <targets>` (cloud: `-M none` on builds, scope tests, `buck2 clean` between heavy phases; a full local suite needs `-j 8`).

---

## File Structure

**Create:**
- `src/control-plane/postgres/tests/overwrite_consuming.rs` — consuming-overwrite primitive test.
- `src/control-plane/postgres/tests/inline_shadow_read.rs` — `inline_live_batch_shadow` + `clear_has_shadow_if_quiescent`.
- `src/control-plane/postgres/tests/cow_consolidate_trigger.rs` — non-CDC delta accrual → `stream_consolidate` enqueue.
- `src/services/engine-serving/tests/cow_consolidate.rs` — the COW fold end-to-end at the engine layer.
- `src/services/query-api/tests/cow_consolidate_e2e.rs` — governed mutate → consolidate → governed read e2e.

**Modify (production):**
- `src/control-plane/postgres/src/iceberg_sql_catalog/commit_mirror.rs` — `write_mirror` blanket-cap rule (`overwrite && end_cap.is_none()`).
- `src/control-plane/postgres/src/iceberg_landing.rs` — `overwrite_parquet_snapshot_consuming`; `overwrite_truncate` gains `Option<InlineEndCap>`.
- `src/control-plane/postgres/src/iceberg_inline.rs` — `inline_live_batch_shadow`; `clear_has_shadow_if_quiescent`; relaxed consolidate-trigger gate in `write_inline_delta`.
- `src/control-plane/postgres/src/iceberg_flush.rs` — hardened read-set shadow guard in `flush_locked`.
- `src/services/engine-serving/src/consolidate.rs` — `consolidate_table` dispatch + `consolidate_cow_locked`; CDC arm switches to the consuming cap.
- `src/services/engine-serving/src/lib.rs` — re-export `consolidate_table` (replaces the `consolidate_stream` export).
- `src/services/engine/src/service.rs:260-274` — the `ConsolidateStream` handler calls `consolidate_table`.

**Modify (docs, final task):** `docs/ROADMAP.md`, `docs/system-capabilities/engine.md`, `docs/system-capabilities/query-api.md`, `docs/system-capabilities/stream.md`.

---

## Task 1: The consuming overwrite primitive

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_sql_catalog/commit_mirror.rs:145-190` (`write_mirror`), `:75-99` (doc note on `apply_commit_extras` ordering — unchanged code)
- Modify: `src/control-plane/postgres/src/iceberg_landing.rs:1117-1196` (`overwrite_parquet_snapshot` body extraction, `overwrite_truncate`)
- Test: `src/control-plane/postgres/tests/overwrite_consuming.rs` (new) + `loom_fixture_test` target `overwrite-consuming` in `src/control-plane/postgres/BUCK`

**Interfaces:**
- Consumes: `CommitExtras`/`InlineEndCap` (`commit_mirror.rs:27/:59`), `end_cap_inline_rows_by_id` (`iceberg_inline.rs:106`), `append_parquet_snapshot`.
- Produces (later tasks rely on these EXACT names/types):
  - `pub async fn overwrite_parquet_snapshot_consuming(pool: &PgPool, catalog: &SqlCatalog, table: &TableRef, columns: &[ColumnSpec], batches: Vec<RecordBatch>, lineage: Option<&LineageEvent>, consumed: InlineEndCap<'_>) -> Result<SnapshotId>` in `iceberg_landing`.
  - The blanket-cap rule: in an overwrite commit, `end_cap_live_inline_rows` runs only when `extras.end_cap.is_none()`.

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/postgres/tests/overwrite_consuming.rs`, mirroring the harness of `src/control-plane/postgres/tests/overwrite_end_caps_inline.rs` (the existing blanket-cap test — reuse its fixture/seed/inline helpers verbatim; `PgFixture::shared()`, a seeded file-backed table, `inline_append` rows). Cases:

1. **Targeted cap retires exactly the named rows:** seed a file row + TWO live inline rows; read their `loom_row_id`s (via `IcebergCatalog::inline_live_batch`); call `overwrite_parquet_snapshot_consuming` with a one-row folded batch and `InlineEndCap { table_id, row_ids: &[first_id] }`. Assert: the first inline row has `end_snapshot = <new snap>`; the second is **still live** (`end_snapshot is null`); the prior data files are end-capped; the new file set is live.
2. **The survivor still serves:** `inline_live_batch` at the new snapshot returns only the surviving row.
3. **Zero-file consuming overwrite (truncate branch):** call the consuming variant with `batches = vec![]` and both row ids; assert both inline rows and all files end-capped at the truncate snapshot (no blanket assumptions — assert by id).
4. **Plain overwrite unchanged:** `overwrite_parquet_snapshot` still blanket-caps every live inline row (a duplicate of `overwrite-end-caps-inline`'s core assertion, kept here as the contrast case).

Wire a `loom_fixture_test` target `overwrite-consuming` in `src/control-plane/postgres/BUCK`, mirroring the `overwrite-end-caps-inline` target's `srcs`/`deps`.

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test --console none //src/control-plane/postgres:overwrite-consuming`
Expected: FAIL — `overwrite_parquet_snapshot_consuming` not found (compile error).

- [ ] **Step 3: Implement the blanket-cap rule in `write_mirror`**

`write_mirror` (`commit_mirror.rs:145`) does not see `extras`; its callers pass `extras.overwrite`. Change its signature's `overwrite: bool` to carry the decision explicitly: add a `blanket_inline_cap: bool` parameter (computed by the single caller `commit_mirror_in_tx` at `:392-403` as `extras.overwrite && extras.end_cap.is_none()`), and gate line `:184`:

```rust
        if overwrite {
            end_cap_live_data_files(conn, tid, at).await?;
            // A targeted InlineEndCap riding this same commit supersedes the
            // blanket cap: the caller consumed a known inline row set (the
            // consolidation fold) and everything else must SURVIVE — a delta
            // committed mid-consolidation keeps shadowing the new base.
            if blanket_inline_cap {
                crate::iceberg_inline::end_cap_live_inline_rows(conn, tid, at).await?;
            }
        }
```

Document on `CommitExtras.end_cap` (`commit_mirror.rs:31`) that in overwrite mode the targeted cap replaces the blanket inline cap.

- [ ] **Step 4: Add `overwrite_parquet_snapshot_consuming` + extend `overwrite_truncate`**

In `iceberg_landing.rs`, refactor `overwrite_parquet_snapshot` (`:1117`) into a private `overwrite_with_cap(pool, catalog, table, columns, batches, lineage, consumed: Option<InlineEndCap<'_>>)`:
- zero-row branch → `overwrite_truncate(pool, table, lineage, &rebuild_jobs, consumed)` — extend `overwrite_truncate` (`:1165`) with `consumed: Option<InlineEndCap<'_>>`: when `Some`, call `end_cap_inline_rows_by_id` instead of `end_cap_live_inline_rows` at `:1179`.
- non-zero branch → `append_parquet_snapshot(…, CommitExtras { lineage, overwrite: true, end_cap: consumed, jobs: &rebuild_jobs, data_trigger_tables: …, ..Default::default() }, include_framing)`.

Public surface: `overwrite_parquet_snapshot` delegates with `None` (byte-identical); new `pub async fn overwrite_parquet_snapshot_consuming(…, consumed: InlineEndCap<'_>)` delegates with `Some(consumed)`. Doc-comment the consuming variant with the survival semantics (spec §2).

- [ ] **Step 5: Run the tests**

Run: `buck2 test --console none //src/control-plane/postgres:overwrite-consuming //src/control-plane/postgres:overwrite-end-caps-inline //src/control-plane/postgres:iceberg-overwrite //src/control-plane/postgres:iceberg-flush`
Expected: PASS (new + all blanket/flush behavior unchanged).

- [ ] **Step 6: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(cow): consuming overwrite — targeted inline cap supersedes the blanket cap

An overwrite commit carrying an InlineEndCap now retires exactly the rows
the caller consumed and leaves every other live inline row alone, so a
mutation landing mid-consolidation survives and keeps shadowing the new
base. Plain overwrites are byte-identical (blanket cap unchanged). Zero-file
(truncate) branch carries the same rule."
```

---

## Task 2: Shadow-aware inline read + quiescent flag clear

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_inline.rs` — `inline_live_batch_shadow` next to `inline_live_batch_impl` (`:1423`); `clear_has_shadow_if_quiescent` next to `clear_has_shadow` (`:797`)
- Test: `src/control-plane/postgres/tests/inline_shadow_read.rs` (new) + `loom_fixture_test` target `inline-shadow-read` in `src/control-plane/postgres/BUCK`

**Interfaces:**
- Consumes: `inline_live_batch_impl`'s column/array machinery (`column_array`, `arrow_field`, `mvcc_live_pred`), `inline_table_name`, `has_shadow`/`set_has_shadow`.
- Produces:
  - `IcebergCatalog::inline_live_batch_shadow(&self, table: &TableRef, at: SnapshotId) -> Result<Option<(i64, Vec<i64>, RecordBatch)>>` — ALL live rows (appends, versions, tombstones; non-CDC tables never hold `-U`), user columns in mirror order then `begin_snapshot` (Int64, non-null) then `loom_tombstone` (Boolean, non-null), matching `build_inline_provider`'s naming (`serving.rs:593-594`).
  - `pub async fn clear_has_shadow_if_quiescent(conn: &mut PgConnection, tid: i64) -> Result<bool>` — one `AssertSqlSafe` statement: `delete from iceberg_mirror.shadow_flag where table_id = $1 and not exists (select 1 from inline_<tid> where end_snapshot is null and (loom_tombstone or loom_change_kind in ('+U','-D')))`; returns `rows_affected() > 0`. Guard with `inline_relation_exists` (a shadowed table always has one, but stay panic-free); when the relation is absent, fall back to `clear_has_shadow`.

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/postgres/tests/inline_shadow_read.rs`, mirroring `src/control-plane/postgres/tests/inline_tombstone.rs`'s harness (fixture, seeded table, `write_inline_delta` version + tombstone writes, `inline_append` for a plain append). Cases:

1. **Projection + row ids:** after one append (id=1), one version (id=2, via `write_inline_delta` `tombstone=false`), one tombstone (id=3): `inline_live_batch_shadow` returns 3 rows; schema ends `[.., begin_snapshot: Int64 non-null, loom_tombstone: Boolean non-null]`; the tombstone row has `loom_tombstone=true`; `row_ids.len() == 3`.
2. **MVCC bound:** at a snapshot before the tombstone's `begin_snapshot`, only the earlier rows return.
3. **Quiescent clear:** with the version + tombstone live, `clear_has_shadow_if_quiescent` returns `false` and `has_shadow` stays `true`; after `end_cap_inline_rows_by_id` on the two delta rows (the append may stay live — `'+I'` rows are not shadow deltas), it returns `true` and `has_shadow` reads `false`.
4. **Idempotent:** a second quiescent clear returns `false` (nothing to delete) without error.

Wire `loom_fixture_test` target `inline-shadow-read` mirroring `inline-tombstone`.

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test --console none //src/control-plane/postgres:inline-shadow-read`
Expected: FAIL — the two functions don't exist.

- [ ] **Step 3: Implement**

`inline_live_batch_shadow`: follow `inline_live_batch_impl`'s body shape but select `loom_row_id, begin_snapshot, loom_tombstone, {user col_list}` (user columns from the mirror's *logical* schema — for a non-CDC table `physical_columns` equals it, but resolve via the same `physical_columns` call for symmetry), no `-U` predicate, `order by loom_row_id`. Assemble the arrow batch with the two framing arrays appended after the user columns (`Int64Builder`/`BooleanBuilder` over `try_get`), reusing `column_array` for user columns. Keep it a separate method — do NOT thread more flags through `inline_live_batch_impl` (three modes on one bool-parameterized body is where clippy and readers both suffer).

`clear_has_shadow_if_quiescent`: as specified in Interfaces. Doc-comment WHY conditional (mid-consolidation survivors, spec §3) and why the residual write-skew is acceptable (the Task 3 flush guard is the safety net).

- [ ] **Step 4: Run the tests**

Run: `buck2 test --console none //src/control-plane/postgres:inline-shadow-read //src/control-plane/postgres:inline-tombstone //src/control-plane/postgres:stream-inline-full-read`
Expected: PASS.

- [ ] **Step 5: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(cow): shadow-aware inline read + quiescent has_shadow clear

inline_live_batch_shadow reads every live inline row with begin_snapshot +
loom_tombstone appended (the Snapshot-precedence fold's input);
clear_has_shadow_if_quiescent clears the flag only when no live shadow
delta remains, so a mid-consolidation mutation keeps the flush suppressed."
```

---

## Task 3: Harden the flush backstop — safety from the read set

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_flush.rs:98-122` (`flush_locked`'s non-CDC arm, after `inline_live_batch` returns)
- Modify: `src/control-plane/postgres/src/iceberg_inline.rs` — small helper `shadow_rows_among(conn, tid, row_ids) -> Result<bool>` (an `AssertSqlSafe` `exists(... where loom_row_id = any($1) and (loom_tombstone or loom_change_kind in ('+U','-D')))`)
- Test: extend `src/control-plane/postgres/tests/flush_suppression.rs` (target `flush-suppression`, already wired)

**Interfaces:**
- Consumes: `inline_live_batch`'s `(tid, row_ids, batch)`, `set_has_shadow`, `reset_inline_trigger`.
- Produces: `flush_locked` no-ops, restores `has_shadow`, and resets the trigger when its read set contains a shadow row — even if the flag was (wrongly) clear.

- [ ] **Step 1: Write the failing test**

In `flush_suppression.rs`, add a case `flush_refuses_shadow_rows_in_its_read_set_even_without_the_flag`:
seed a file-backed table, land a `write_inline_delta` version row (sets `has_shadow`), then **manually clear the flag** (`iceberg_inline::clear_has_shadow`) to simulate the conditional-clear write-skew; run `flush_table`. Assert: returns `Ok(None)`; the delta row is still live (never end-capped); NO new data file was projected (file set unchanged); `has_shadow` reads `true` again (self-healed).

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test --console none //src/control-plane/postgres:flush-suppression`
Expected: FAIL — today the flag-cleared flush drains the delta into Parquet.

- [ ] **Step 3: Implement**

In `flush_locked`, after the `inline_live_batch` destructure (`:114`), insert the read-set guard before any Parquet work:

```rust
    // Safety derives from the DATA, not the flag: a flush can only corrupt by
    // appending rows it READ, so checking the read set is race-free — a delta
    // committing after this read is not in `row_ids` and is not written. This
    // closes the conditional-clear write-skew (spec §3): if a shadow row is in
    // the set, the flag was cleared wrongly; restore it and no-op.
    let mut conn = pool.acquire().await.map_err(backend)?;
    if crate::iceberg_inline::shadow_rows_among(&mut conn, tid, &row_ids).await? {
        crate::iceberg_inline::set_has_shadow(&mut conn, tid).await?;
        reset_inline_trigger(&mut conn, tid).await?;
        return Ok(None);
    }
    drop(conn);
```

Keep the existing `has_shadow` early-out (`:104-110`) as the cheap fast path (its doc comment gains "fast path; the authoritative guard is the read-set check below").

- [ ] **Step 4: Run the tests**

Run: `buck2 test --console none //src/control-plane/postgres:flush-suppression //src/control-plane/postgres:iceberg-flush //src/control-plane/postgres:inline-flush-trigger //src/control-plane/postgres:stream-cdc-dual-flush`
Expected: PASS (CDC flush takes `flush_locked_cdc` before this guard and is untouched).

- [ ] **Step 5: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "fix(cow): flush guard derives from the read set, not the flag

flush_locked now refuses (and self-heals has_shadow) when the rows it just
read contain a version/tombstone — race-free because a flush can only
corrupt by writing rows it read. The flag check remains as the fast path."
```

---

## Task 4: The COW fold — `consolidate_table` dispatch + `consolidate_cow_locked`

**Files:**
- Modify: `src/services/engine-serving/src/consolidate.rs` — rename the public entry to `consolidate_table` (dispatch); add `consolidate_cow_locked`; keep the CDC arm as-is this task
- Modify: `src/services/engine-serving/src/lib.rs:15` — re-export `consolidate_table`
- Modify: `src/services/engine/src/service.rs:260-274` — handler calls `consolidate_table`
- Modify: `src/services/engine-serving/tests/consolidate_lock.rs:23,42` — call-site rename
- Test: `src/services/engine-serving/tests/cow_consolidate.rs` (new) + `loom_fixture_test` target `cow-consolidate` in `src/services/engine-serving/BUCK`

**Interfaces:**
- Consumes: `overwrite_parquet_snapshot_consuming` (T1), `inline_live_batch_shadow` + `clear_has_shadow_if_quiescent` (T2), `lock_table`, `read_files_as_batches`, `identity_for_table`, `register_batches`, `reset_inline_trigger`, `clear_consolidate_trigger`, `has_shadow`.
- Produces: `pub async fn consolidate_table(cp: &PgControlPlane, catalog: &SqlCatalog, pool: &PgPool, table: &TableRef) -> Result<i64, EngineServingError>` — CDC → existing fold; non-CDC + identity + `has_shadow` → COW fold; else `Ok(0)`.

- [ ] **Step 1: Write the failing test**

Create `src/services/engine-serving/tests/cow_consolidate.rs`, reusing the harness style of `src/services/query-api/tests/cow_inline_shadow_e2e.rs`'s seed (or `engine-serving/tests/merge_on_read.rs`'s, whichever seeds file-tier rows + an ontology identity type most directly — mirror its fixture/catalog/store setup). Seed: identity type over `("wh","items")` with ids 1..4 landed as Parquet; then `write_inline_delta` UPDATE id=2 (new value), DELETE id=3, and `inline_append` a new id=5. Cases (one `#[tokio::test]` each or staged in one, matching the sibling file's style):

1. **Fold correctness:** `consolidate_table(..)` returns a snapshot id > 0. Read the NEW base's files directly (`files_with_stats` at the new snapshot → `read_files_as_batches`): rows are exactly {1 original, 2 updated image, 4 original, 5 appended} — id=3 absent (tombstone-aware), no duplicates, user columns only (no `loom_*` in the Parquet schema).
2. **Inline consumed:** the three inline rows are end-capped at the consolidation snapshot; `inline_live_batch` at the new snapshot returns `None`.
3. **Flags:** `has_shadow` reads `false`; the consolidate trigger row (if any) is disarmed.
4. **Merge-view equivalence:** a `build_serving_provider` read (or `execute_query` helper the sibling tests use) collected BEFORE consolidation equals the same read AFTER, row-set-wise.
5. **Time travel:** an as-of read at the pre-consolidation snapshot still returns the ORIGINAL id=2 and id=3 rows.
6. **No-op arms:** a non-identity table and a CDC-declared table return through their existing paths (CDC covered by non-regression; assert the identity-less table returns `0`).
7. **Residual delta keeps the flag:** after the first consolidation, write another delta for id=1 and consolidate again mid-way is not simulable here — instead assert the conditional clear directly: write a delta, consolidate, then BEFORE checking, write another delta; `has_shadow` is `true` again and a second `consolidate_table` quiesces it.

Wire `loom_fixture_test` target `cow-consolidate` in `src/services/engine-serving/BUCK` mirroring `consolidate-lock` (deps: `:engine-serving`, `//src/control-plane/postgres:postgres`, `//src/control-plane/core:core`, tokio, arrow, datafusion — copy the sibling's list).

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test --console none //src/services/engine-serving:cow-consolidate`
Expected: FAIL — `consolidate_table` not found.

- [ ] **Step 3: Implement the dispatch**

In `consolidate.rs`, rename `consolidate_stream` → `consolidate_table` and restructure its preamble:

```rust
pub async fn consolidate_table(
    cp: &PgControlPlane,
    catalog: &SqlCatalog,
    pool: &PgPool,
    table: &TableRef,
) -> Result<i64, EngineServingError> {
    // live table id (as today, :63-70) …
    let meta = cp.stream_meta(tid).await.map_err(to_serving)?;
    match meta {
        Some(m) if m.kind == StreamKind::Cdc => { /* existing CDC arm, :78-130, unchanged */ }
        _ => {
            // Non-CDC: the COW arm — identity-bearing + shadow-bearing only.
            let Some(identity) =
                control_plane_postgres::ontology::identity_for_table(pool, table)
                    .await.map_err(to_serving)?
            else { return Ok(0); };
            let mut conn = pool.acquire().await.map_err(to_serving)?;
            let shadowed = control_plane_postgres::iceberg_inline::has_shadow(&mut conn, tid)
                .await.map_err(to_serving)?;
            drop(conn);
            if !shadowed { return Ok(0); }
            let lock = lock_table(pool, table).await.map_err(to_serving)?;
            let result = consolidate_cow_locked(pool, catalog, table, tid, &identity).await;
            lock.release().await;
            result
        }
    }
}
```

`consolidate_cow_locked` mirrors `consolidate_locked`'s shape (`:133-271`):

1. `current_snapshot` (NotFound → `Ok(0)`), `user_cols` from `ice.schema` (as `:151-164`).
2. Files → `read_files_as_batches` (as `:166-174`); inline → `ice.inline_live_batch_shadow(table, current.id)` (T2). If inline is `None`: a stale flag with nothing live — `clear_has_shadow_if_quiescent` + `reset_inline_trigger` + `clear_consolidate_trigger`, return `Ok(0)` (the flush-style self-heal).
3. Register `base_files` / `base_inline` (as `:186-199`; the files-empty case registers only inline — a shadowed table CAN be inline-only).
4. The fold SQL — the spec §1 reference SQL verbatim, with `quote_ident` on the identity and user columns (mirror `:201-247`'s construction):

```rust
    let fold_sql = format!(
        "select {col_list} from ( \
             select {col_list}, _loom_tomb, row_number() over ( \
                 partition by {id_quoted} order by _loom_prec desc \
             ) as _rn from ({union_sql}) base_input \
         ) t where _rn = 1 and _loom_tomb = false"
    );
```

where the file tier selects `{col_list}, 0 as _loom_prec, false as _loom_tomb` and the inline tier `{col_list}, begin_snapshot as _loom_prec, loom_tombstone as _loom_tomb`. Comment: "the `Precedence::Snapshot` merge (`build_merge_view`, serving.rs) materialized — reads before/after are identical by construction."

5. Collect; lineage `consolidate_event`-style with payload `{"source": "consolidate_cow"}` (factor a `source: &str` param into `consolidate_event` at `:41`); commit via `overwrite_parquet_snapshot_consuming(pool, catalog, table, &user_cols, folded, Some(&lineage), InlineEndCap { table_id: tid, row_ids: &row_ids })`.
6. Post-commit clears: `clear_has_shadow_if_quiescent`, `reset_inline_trigger`, `clear_consolidate_trigger` (each idempotent; comment the crash-heal ordering, spec §3).

Update `lib.rs:15` (`pub use consolidate::consolidate_table;`), `service.rs:270` (call + doc), and `consolidate_lock.rs`'s two references. Update the module doc (`consolidate.rs:1-13`) to describe both arms.

- [ ] **Step 4: Run the tests**

Run: `buck2 test --console none //src/services/engine-serving:cow-consolidate //src/services/engine-serving:consolidate-lock //src/services/engine-serving:merge-on-read //src/services/query-api:stream-cdc-consolidate //src/services/worker:stream-consolidate-job`
Expected: PASS — the CDC arm and the lock behavior are unchanged; the renamed entry compiles everywhere.

- [ ] **Step 5: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(cow): tombstone-aware consolidation — the Snapshot-precedence fold

consolidate_table dispatches per table kind: CDC keeps the Offset fold;
a shadow-bearing non-CDC identity table now folds files ∪ inline (appends,
versions, tombstones) by greatest begin_snapshot per identity, drops
tombstoned winners, commits via the consuming overwrite (survivors keep
shadowing), and re-enables the flush via the quiescent flag clear."
```

---

## Task 5: CDC consolidate switches to the consuming cap (latent-race fix)

**Files:**
- Modify: `src/services/engine-serving/src/consolidate.rs` — the CDC arm (`consolidate_locked`) passes its `row_ids` to `overwrite_parquet_snapshot_consuming`
- Test: non-regression only (`stream-cdc-consolidate`, `consolidate-lock`); the survival semantics are pinned deterministically by Task 1's primitive test.

- [ ] **Step 1: Implement**

`consolidate_locked` already destructures `inline_live_batch_full`'s result but discards the ids (`:188` binds `(_, _, inline_batch)`). Bind `(_, row_ids, inline_batch)` and switch the commit at `:256` to `overwrite_parquet_snapshot_consuming(…, InlineEndCap { table_id: tid, row_ids: &row_ids })` when inline rows were read; keep plain `overwrite_parquet_snapshot` when `inline` was `None` (nothing to consume — blanket cap is then vacuous). Comment: "a CDC inline row committing mid-consolidation was previously blanket-capped WITHOUT being folded or changelog-flushed — silent loss; the targeted cap lets it survive to the next flush/consolidate."

- [ ] **Step 2: Run the tests**

Run: `buck2 test --console none //src/services/query-api:stream-cdc-consolidate //src/services/engine-serving:consolidate-lock //src/services/query-api:stream-cdc-e2e`
Expected: PASS, unchanged results (the fix only changes the raced window).

- [ ] **Step 3: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "fix(stream): CDC consolidate retires only the rows it folded

Switch consolidate_locked from the blanket inline cap to the consuming
overwrite so an inline write committing mid-consolidation survives instead
of being end-capped unfolded (lost from base and changelog)."
```

---

## Task 6: Auto-enqueue — relax the consolidate-trigger gate to non-CDC deltas

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_inline.rs:1104-1230` (`write_inline_delta` — `emitted_rows` on the non-CDC branches; the trigger gate at `:1218`)
- Test: `src/control-plane/postgres/tests/cow_consolidate_trigger.rs` (new) + `loom_fixture_test` target `cow-consolidate-trigger` in `src/control-plane/postgres/BUCK`, mirroring `stream-cdc-consolidate-trigger`

**Interfaces:**
- Consumes: `bump_consolidate_trigger`/`arm_consolidate_trigger` (`iceberg_mirror.rs:606/:637`), `STREAM_CONSOLIDATE_JOB_KIND`.
- Produces: a non-CDC `write_inline_delta` accrues 1 delta row per call and enqueues one deduped `stream_consolidate` job on crossing the threshold.

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/postgres/tests/cow_consolidate_trigger.rs`, mirroring `src/control-plane/postgres/tests/stream_cdc_consolidate_trigger.rs`'s harness (it drives `write_inline_delta` with a `consolidate_threshold` and inspects the queue) but on a NON-CDC identity table. Cases:

1. **Below threshold:** with `consolidate_threshold = Some(3)`, two version writes enqueue nothing (`queue` has no `stream_consolidate` job).
2. **Crossing enqueues once:** a third delta enqueues exactly one `stream_consolidate` job with payload `{schema, name}`; a fourth (armed) enqueues nothing more.
3. **Disabled:** `consolidate_threshold = None` never enqueues.
4. **Re-arm:** after `clear_consolidate_trigger`, further deltas accrue and enqueue again.

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test --console none //src/control-plane/postgres:cow-consolidate-trigger`
Expected: FAIL — the `cdc &&` gate leaves non-CDC accrual at zero.

- [ ] **Step 3: Implement**

In `write_inline_delta`: set `emitted_rows = 1;` on both non-CDC branches (`:1167-1179` tombstone, `:1180-1208` version — the variable already exists, `:1108`), and change the gate at `:1218` from `if cdc && let Some(threshold) = consolidate_threshold` to `if let Some(threshold) = consolidate_threshold`. Update the block comment: the counter is per-table, kind-agnostic; a non-CDC table's job lands in the same `stream_consolidate` kind and is dispatched to the COW arm by `consolidate_table` (Task 4).

- [ ] **Step 4: Run the tests**

Run: `buck2 test --console none //src/control-plane/postgres:cow-consolidate-trigger //src/control-plane/postgres:stream-cdc-consolidate-trigger //src/control-plane/postgres:inline-delta-cas`
Expected: PASS.

- [ ] **Step 5: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(cow): non-CDC mutations accrue the consolidate trigger

Drop the cdc-only gate in write_inline_delta so a shadow-bearing table
auto-enqueues the (deduped, armed-once) stream_consolidate job on crossing
LOOM_CONSOLIDATE_DELTA_THRESHOLD — the trigger the COW consolidation
re-arms when it clears."
```

---

## Task 7: End-to-end — governed mutate → consolidate → governed read

**Files:**
- Test: `src/services/query-api/tests/cow_consolidate_e2e.rs` (new) + `loom_fixture_test` target `cow-consolidate-e2e` in `src/services/query-api/BUCK`, mirroring `cow-inline-shadow-e2e`'s target and reusing `//src/services/query-api:e2e-support`

**Interfaces:** consumes everything above through the public seams (action router → engine writer → `write_inline_delta`; `consolidate_table`; `build_serving_provider`).

- [ ] **Step 1: Write the test**

Mirror `cow_inline_shadow_e2e.rs`'s setup (seeded identity type, governed action UPDATE + DELETE through the router). Then:

1. Drive `engine_serving::consolidate_table` (directly, as the engine RPC handler would — the worker↔RPC plumbing is already covered by `stream-consolidate-job`).
2. **Governed read identical:** the `GET /objects/{type}` result after consolidation equals the pre-consolidation result (updated values served, deleted id absent); a restricted subject's masked columns stay masked (reuse the gov harness from `cow_inline_shadow_gov_e2e.rs` if the assertion is cheap, else keep coarse).
3. **Flush lifecycle restored:** land a fresh append via the normal ingest/land helper, call `flush_table` — it drains (returns `Some(snap)`), proving the suppression lifted.
4. **CAS after consolidation:** a mutation built against a pre-consolidation `expected_version` gets `Conflict`; the retry against version `0` succeeds (pin the spec §2 CAS interaction at the user-visible layer).

- [ ] **Step 2: Run the test**

Run: `buck2 test --console none //src/services/query-api:cow-consolidate-e2e //src/services/query-api:cow-inline-shadow-e2e //src/services/query-api:cow-inline-shadow-gov-e2e`
Expected: PASS.

- [ ] **Step 3: Full-suite sweep + prek + commit**

Run: `buck2 build -v0 --console none //src/...` then `buck2 test --console none -j 8 //src/...` (locally; cloud sessions scope to the touched targets + btd-affected instead).
Expected: green.

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "test(cow): consolidation e2e — governed mutate, fold, read, flush restored"
```

---

## Task 8: Close the register + system-capabilities (loom-docs-update)

**Files:**
- Modify: `docs/ROADMAP.md` — remove the `#road-cow-compaction-consolidation` entry (items carry open work only; git history keeps the record). `#fut-cow-identity-change` (slice 3) STAYS in `docs/FUTURE.md`, untouched.
- Modify: `docs/system-capabilities/engine.md` — extend the "Inline shadow writes and the flush vertical" / COW sections: the consolidation fold, the consuming overwrite semantics, the quiescent clear + hardened flush guard, the shared `stream_consolidate` dispatch.
- Modify: `docs/system-capabilities/query-api.md` — the #400 note ("durable cold-entry removal stays with slice-2 compaction, `fut-cow-inline-shadow`") now closes: consolidation + `rebuild_jobs_for` durably remove stale cold index entries.
- Modify: `docs/system-capabilities/stream.md` — the "Compaction: `consolidate_stream`" section: entry renamed `consolidate_table` (dispatching), CDC arm now uses the targeted cap.

- [ ] **Step 1: Run the loom-docs-update skill** (it stages exactly these closes/edits alongside the work; if running manually, apply the edits above).

- [ ] **Step 2: Validate + lint**

```bash
bash tools/docs.sh validate
buck2 run //tools:prek -- run --all-files
```

Expected: validate passes (no dangling `[[road-cow-compaction-consolidation]]` references — grep the registers); prek clean (markdown EOF/trailing-whitespace hooks).

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "docs(cow): close road-cow-compaction-consolidation; record consolidation in system-capabilities

Slice 2 shipped: tombstone-aware consolidation folds the inline shadow tier
into a new base, retires exactly the consumed rows, and re-enables the
flush. Slice 3 (identity-change / upsert) stays deferred as
fut-cow-identity-change."
```

---

## Verification sweep (definition of done)

- `buck2 build -v0 --console none //src/...` — silent success.
- `buck2 test --console none -j 8 //src/...` — full suite green (locally).
- New targets green: `overwrite-consuming`, `inline-shadow-read`, `cow-consolidate`, `cow-consolidate-trigger`, `cow-consolidate-e2e`, extended `flush-suppression`.
- Non-regression set green (Global Constraints list).
- `bash tools/docs.sh validate` passes; `buck2 run //tools:prek -- run --all-files` clean.
- Register state: `road-cow-compaction-consolidation` gone from ROADMAP; `fut-cow-identity-change` still present in FUTURE.
