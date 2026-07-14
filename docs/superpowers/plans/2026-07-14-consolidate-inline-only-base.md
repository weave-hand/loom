# Consolidate over an inline-only base — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `consolidate_locked`'s CDC arm survive — and correctly fold — a base table that has only ever been inline-appended (no Parquet file, no `iceberg_tables` catalog row), and stop a no-op consolidate from latching the table's consolidate trigger forever.

**Architecture:** `consolidate_locked` (`src/services/engine-serving/src/consolidate.rs`) reads the base's file tier *unconditionally* via `read_files_as_batches`, which calls `catalog.load_table` before it ever looks at the (possibly empty) path list — so it dies for a table with no Iceberg catalog row even when there are zero files. The fix lifts the shape its own COW sibling in the same file already uses (`consolidate_cow_locked`, `consolidate.rs:421-426`): a `has_files` bool, a **conditional** `register_batches("base_files", …)`, and a `union_sql` assembled from only the tiers that exist. On top of that, the neither-tier case becomes an explicit trigger-clearing no-op instead of falling through to an empty SQL body. No new SQL, no migration, no `.sqlx` refresh.

**Tech Stack:** Rust 2024, DataFusion (`SessionContext` + `register_batches`), sqlx/Postgres control plane, Apache Iceberg (`SqlCatalog`), buck2 (`loom_fixture_test`).

**Spec:** `docs/superpowers/specs/2026-07-14-consolidate-inline-only-base-design.md`
**Register item:** `iss-consolidate-inline-only-base` (docs/ISSUES.md)
**Precedent to copy:** #436 (`iss-mv-delta-inline-source-unflushed`) — the identical defect fixed in `mv_delta_locked`; its regression test `src/services/engine-serving/tests/mv_delta_inline_source.rs` is the canonical inline-only test idiom.

## Global Constraints

- **Tests are `rust_test` / `loom_fixture_test` targets only** — never an inline `#[cfg(test)] mod tests`. buck2 builds inline test modules but never runs them; the `no-inline-tests` prek hook fails the commit. Every new test is a sibling file under `tests/` wired as its own target in the crate's `BUCK`.
- **Fixture tests must use the `loom_fixture_test` macro**, not a bare `rust_test`, or they run without the Postgres/MinIO fixture env and fail to boot.
- **Strict clippy** (whole `pedantic` + `restriction` groups on production lib/bin code). Enforced high-signal lints include `unwrap_used`, `expect_used`, `indexing_slicing`, `panic`, `todo`, `map_err_ignore`. In `src/**` production code use `?` and real error values — never `unwrap`/`expect`/`unreachable!`. To silence a lint locally use `#[expect(lint, reason = "...")]` (a bare `#[allow]` without a `reason` is itself a lint error). Test code is exempted from the panic-safety lints by the `loom_rust_test` / `loom_fixture_test` wrappers, so `.expect("...")` in tests is fine and idiomatic.
- **Run the hooks before every commit:** `buck2 run //tools:prek -- run --all-files` (rustfmt + clippy + file hygiene + conventional-commit message). Commit whatever the hooks rewrite.
- **Conventional Commits** are enforced on the commit message (`tools/check-commit-msg.sh`).
- **Build/test invocations use `--console none`** (and `-v0` for builds) — never pipe buck2 through `tail`/`head`, it stalls on the unconsumed pipe.
- **No SQL change is expected** in this item (every control-plane helper already exists). If that turns out false, `tools/sqlx-prepare.sh` must be run and the resulting `src/control-plane/postgres/.sqlx/` change committed.
- Work happens on the already-checked-out branch **`work/iss-consolidate-inline-only-base`**.

---

## File Structure

| File | Responsibility | Change |
| --- | --- | --- |
| `src/services/engine-serving/src/consolidate.rs` | The two consolidation arms. Only `consolidate_locked` (the CDC arm, lines 180-349) changes. | Modify |
| `src/services/engine-serving/tests/cdc_consolidate_inline_source.rs` | New fixture test: the inline-only CDC fold, the neither-tier no-op + trigger clear, and the files-only invariant. | Create |
| `src/services/engine-serving/tests/cow_consolidate.rs` | Existing COW fixture test. Gains the missing inline-only COW case (pins the already-guarded branch at `consolidate.rs:421`). | Modify |
| `src/services/engine-serving/BUCK` | Target wiring. | Modify (one new `loom_fixture_test` stanza) |
| `docs/ISSUES.md`, `docs/system-capabilities/engine.md` | Register close + capability record + the spec's register corrections and the newly-found `collect_vectors` issue. | Modify |

---

### Task 1: The CDC arm folds an inline-only base

The defect itself. A CDC base that has never been flushed has **no Iceberg SQL-catalog row**, so `read_files_as_batches` → `catalog.load_table` errors — even though `paths` is empty and there is nothing to read. Guard the file tier and assemble `union_sql` from only the tiers that exist, exactly as `consolidate_cow_locked` already does.

**Files:**
- Modify: `src/services/engine-serving/src/consolidate.rs:213-262` (inside `consolidate_locked`)
- Create: `src/services/engine-serving/tests/cdc_consolidate_inline_source.rs`
- Modify: `src/services/engine-serving/BUCK` (new `loom_fixture_test` target `cdc-consolidate-inline-source`)

**Interfaces:**
- Consumes (all already public, no signature changes anywhere):
  - `engine_serving::consolidate_table(cp: &PgControlPlane, catalog: &SqlCatalog, pool: &PgPool, table: &TableRef) -> Result<i64, EngineServingError>`
  - `control_plane_postgres::iceberg_catalog::IcebergCatalog::new(pool: PgPool)`, `.files_with_stats(&TableRef, SnapshotId) -> Result<Vec<FileWithStats>>` (each has a `.path: String`), `.inline_live_batch_full(&TableRef, SnapshotId) -> Result<Option<(i64, Vec<i64>, RecordBatch)>>` (an inherent method on `IcebergCatalog`, defined in `iceberg_inline.rs` — no trait import needed)
  - `control_plane_postgres::read_files_as_batches(&SqlCatalog, &TableRef, &[String]) -> Result<(SchemaRef, Vec<RecordBatch>)>` (re-exported at the crate root)
  - `control_plane_postgres::iceberg_mirror::{ensure_table, next_snapshot}`
  - `control_plane_postgres::iceberg_inline::{inline_append, write_inline_delta, current_inline_version}`
  - `PgControlPlane::declare_cdc(tid: i64, buckets: i32, key: &str, engine: MergeEngine)` (via the `StreamTables` trait — it must be in scope)
  - `loom_test_seed::local_sql_catalog(dsn: String, warehouse: &str) -> SqlCatalog` (buck dep `//src/testing:seed`)
- Produces: no new public API. `consolidate_locked` keeps its exact signature; the change is internal to its body.

- [ ] **Step 1: Write the failing test**

Create `src/services/engine-serving/tests/cdc_consolidate_inline_source.rs`. This file is the whole test module for the item — Task 2 and Task 3 append cases to it, so write the shared helpers now.

The seeding idiom is `postgres/tests/stream_cdc_consolidate_trigger.rs` (`next_snapshot` + `ensure_table` + `declare_cdc` + `inline_append` + `write_inline_delta`) combined with the `SqlCatalog` construction from `engine-serving/tests/mv_delta_inline_source.rs` (`tempfile::tempdir()` + `local_sql_catalog`). Note a `LastRow` CDC table needs **no ontology type** — the identity comes from `meta.bucket_key`, set by `declare_cdc`.

```rust
//! `consolidate_table`'s CDC arm over a base that has ONLY ever been inline-appended.
//!
//! A CDC declare pre-creates only the CHANGELOG Iceberg table (`land_cdc` ->
//! `ensure_iceberg_table(changelog_table_ref(table))`); the BASE gets its
//! `iceberg_tables` row at its first Parquet write, i.e. at flush. But the
//! consolidate job is enqueued by the inline delta-row trigger
//! (`bump_consolidate_trigger`, from `write_inline_delta`), which needs no flush
//! at all — so `consolidate_locked` can be handed a base with zero Parquet files
//! and no catalog row. It read the file tier unconditionally
//! (`read_files_as_batches` -> `catalog.load_table`), which errors even for an
//! EMPTY path list: `iss-consolidate-inline-only-base`, the same defect
//! `iss-mv-delta-inline-source-unflushed` (#436) fixed in `mv_delta_locked`.
//!
//! loom_fixture_test (Postgres + a local `tempfile` warehouse).
//! Spec: docs/superpowers/specs/2026-07-14-consolidate-inline-only-base-design.md

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{
    ColumnSpec, EventType, LineageEvent, MergeEngine, RunId, SnapshotId, StreamTables, TableRef,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_inline::{
    current_inline_version, inline_append, write_inline_delta,
};
use control_plane_postgres::iceberg_mirror::{ensure_table, next_snapshot};
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use control_plane_postgres::read_files_as_batches;
use loom_test_seed::local_sql_catalog;
use sqlx::PgPool;

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef {
        schema: schema.into(),
        name: name.into(),
    }
}

/// The `(id long, val long)` logical (framing-free) schema every case uses.
fn cols() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        },
        ColumnSpec {
            name: "val".into(),
            ty: "long".into(),
            nullable: false,
        },
    ]
}

fn id_spec() -> Vec<ColumnSpec> {
    vec![ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: false,
    }]
}

fn arrow_cols() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
    ]))
}

/// A one-row `(id, val)` batch.
fn row(id: i64, val: i64) -> RecordBatch {
    RecordBatch::try_new(
        arrow_cols(),
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(Int64Array::from(vec![val])),
        ],
    )
    .expect("row batch")
}

/// A one-cell `(id)` batch — the CAS-witness probe for `current_inline_version`.
fn id_batch(id: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![id]))]).expect("id batch")
}

fn lin() -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![],
        payload: serde_json::json!({ "source": "cdc-consolidate-inline-source-test" }),
    }
}

/// Create `table` in the mirror and declare it CDC (2 buckets, keyed on `id`,
/// `LastRow`) BEFORE any write — the `stream_cdc_consolidate_trigger.rs` idiom.
/// Declaring CDC pre-creates ONLY the changelog Iceberg table; the base gets no
/// `iceberg_tables` row until it is flushed, which is the whole point here.
/// Returns the live mirror table id.
async fn declare_cdc_table(cp: &PgControlPlane, pool: &PgPool, table: &TableRef) -> i64 {
    let mut tx = pool.begin().await.expect("begin");
    let at0 = next_snapshot(&mut tx, None).await.expect("next_snapshot");
    let tid = ensure_table(&mut tx, &table.schema, &table.name, at0)
        .await
        .expect("ensure_table");
    tx.commit().await.expect("commit");
    cp.declare_cdc(tid, 2, "id", MergeEngine::LastRow)
        .await
        .expect("declare_cdc");
    tid
}

/// A CDC `+U` update of `id` to `val` through the inline delta path — the ONLY
/// production caller of `bump_consolidate_trigger`. `stream_buckets: None` on the
/// seeding append means "don't change the declaration" (the table is already CDC).
async fn update(pool: &PgPool, table: &TableRef, id: i64, from: i64, to: i64) {
    let witness = current_inline_version(pool, table, &id_spec(), "id", &id_batch(id))
        .await
        .expect("current_inline_version");
    write_inline_delta(
        pool,
        table,
        &cols(),
        "id",
        false,
        &row(id, to),
        Some((&cols(), &row(id, from))),
        lin(),
        witness,
        None,
        &[],
    )
    .await
    .expect("cdc update delta");
}

/// Flatten batches into sorted `(id, val)` pairs, by column NAME (the folded base
/// carries framing columns after the user columns, so positional access is wrong).
fn rows_sorted(batches: &[RecordBatch]) -> Vec<(i64, i64)> {
    let mut out = Vec::new();
    for b in batches {
        let ids = b
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id is Int64");
        let vals = b
            .column_by_name("val")
            .expect("val column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("val is Int64");
        for i in 0..b.num_rows() {
            out.push((ids.value(i), vals.value(i)));
        }
    }
    out.sort_unstable();
    out
}

/// A fresh fixture db + pool + a local-filesystem `SqlCatalog` over a temp warehouse.
async fn setup(
    fx: &'static PgFixture,
) -> (PgControlPlane, PgPool, SqlCatalog, tempfile::TempDir) {
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("warehouse tempdir");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    (cp, pool, catalog, wh)
}

/// THE DEFECT: a CDC base that has only ever been inline-appended — no Parquet
/// file, no `iceberg_tables` row — must consolidate. Pre-fix this dies in
/// `read_files_as_batches`'s unconditional `catalog.load_table` ("No such table:
/// cdc.orders"), even though the file list is empty. Post-fix the fold runs over
/// the inline tier alone and the CONSUMING overwrite creates the base's Iceberg
/// table on the spot (`append_parquet_snapshot` -> `ensure_iceberg_table`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_only_cdc_base_consolidates_without_a_flush() {
    let fx = PgFixture::shared();
    let (cp, pool, catalog, _wh) = setup(fx).await;
    let table = tref("cdc", "orders");

    declare_cdc_table(&cp, &pool, &table).await;
    // Seed +I id=1 val=100, then two CDC updates — inline only, no flush anywhere.
    inline_append(&pool, &table, &cols(), &row(1, 100), lin(), None, None)
        .await
        .expect("seed inline append");
    update(&pool, &table, 1, 100, 200).await;
    update(&pool, &table, 1, 200, 300).await;

    let snap = engine_serving::consolidate_table(&cp, &catalog, &pool, &table)
        .await
        .expect("an inline-only CDC base must consolidate without a flush");
    assert!(snap > 0, "the fold committed a real new snapshot, got {snap}");

    // The fold WROTE the base: its Iceberg table now exists and holds one Parquet
    // file with the merged row (greatest loom_offset per identity wins).
    let ice = IcebergCatalog::new(pool.clone());
    let files = ice
        .files_with_stats(&table, SnapshotId(snap))
        .await
        .expect("files_with_stats");
    let paths: Vec<String> = files.into_iter().map(|f| f.path).collect();
    assert!(
        !paths.is_empty(),
        "the fold materialized the inline-only base as Parquet"
    );
    let (_schema, batches) = read_files_as_batches(&catalog, &table, &paths)
        .await
        .expect("the base's Iceberg table was created by the fold");
    assert_eq!(
        rows_sorted(&batches),
        vec![(1, 300)],
        "one row per identity, the greatest-offset image wins"
    );

    // The folded inline rows were consumed in the same commit.
    let live = ice
        .inline_live_batch_full(&table, SnapshotId(snap))
        .await
        .expect("inline_live_batch_full");
    assert!(
        live.is_none(),
        "every folded inline row is end-capped, got {live:?}"
    );
}
```

Wire the target in `src/services/engine-serving/BUCK` — mirror the `mv-delta-inline-source` stanza (same dep set; it is the closest sibling):

```python
loom_fixture_test(
    name = "cdc-consolidate-inline-source",
    crate = "cdc_consolidate_inline_source",
    srcs = ["tests/cdc_consolidate_inline_source.rs"],
    crate_root = "tests/cdc_consolidate_inline_source.rs",
    deps = [
        ":engine-serving",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//src/testing:seed",
        "//third-party:arrow",
        "//third-party:serde_json",
        "//third-party:sqlx",
        "//third-party:tempfile",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```

- [ ] **Step 2: Run the test and watch it fail for the RIGHT reason**

Run: `buck2 test --console none //src/services/engine-serving:cdc-consolidate-inline-source`

Expected: **FAIL**. The failure must come from the `.expect("an inline-only CDC base must consolidate without a flush")` with an inner error naming a missing table (`No such table: cdc.orders` / an iceberg `TableNotFound`-shaped `Backend` error) raised inside `catalog.load_table`. That is the defect reproducing.

If it instead fails in the seeding (`declare_cdc`, `inline_append`, `write_inline_delta`) the test is wrong, not the code — fix the test before touching `consolidate.rs`.

- [ ] **Step 3: Guard the file tier and assemble the union from the tiers that exist**

In `src/services/engine-serving/src/consolidate.rs`, inside `consolidate_locked`, replace the unconditional file read + registration + two-armed `union_sql` (currently lines 213-262) with:

```rust
    // The base's physical framed rows: live Parquet files ...
    let files = ice
        .files_with_stats(table, current.id)
        .await
        .map_err(to_serving)?;
    let paths: Vec<String> = files.into_iter().map(|f| f.path).collect();
    let has_files = !paths.is_empty();

    // ... UNION any still-live inline tail (un-flushed changes), so a consolidate
    // that runs without a preceding flush still folds correctly. `_full` keeps
    // `-U` before-images in the read, but they never win the fold (the adjacent
    // `+U` always carries a greater `loom_offset`) and are excluded from the
    // output projection like every other non-winning row.
    let inline = ice
        .inline_live_batch_full(table, current.id)
        .await
        .map_err(to_serving)?;

    // Register only the tiers that exist. The file tier is read LAZILY, behind
    // `has_files`: `read_files_as_batches` calls `catalog.load_table` before it
    // looks at the path list, and a CDC base that has never been flushed has no
    // `iceberg_tables` row at all (the CDC declare pre-creates only the CHANGELOG
    // table) — so reading an EMPTY file list still errored. Same guard the COW arm
    // below already carries (`iss-consolidate-inline-only-base`, the sibling of the
    // `mv_delta` fix in #436).
    let df_ctx = SessionContext::new();
    if has_files {
        let (file_schema, file_batches) = read_files_as_batches(catalog, table, &paths)
            .await
            .map_err(to_serving)?;
        register_batches(&df_ctx, "base_files", file_schema, file_batches).map_err(to_serving)?;
    }
    let has_inline = if let Some((_, _, inline_batch)) = &inline {
        register_batches(
            &df_ctx,
            "base_inline",
            inline_batch.schema(),
            vec![inline_batch.clone()],
        )
        .map_err(to_serving)?;
        true
    } else {
        false
    };

    let col_list = user_cols
        .iter()
        .map(|c| quote_ident(&c.name))
        .collect::<Vec<_>>()
        .join(", ");
    let id_quoted = quote_ident(identity);
    // One leg per registered tier — a base can legitimately be files-only (the
    // post-flush fold), inline-only (never flushed), or both.
    let mut legs: Vec<String> = Vec::new();
    if has_files {
        legs.push(format!(
            "select {col_list}, loom_change_kind, loom_bucket, loom_offset from base_files"
        ));
    }
    if has_inline {
        legs.push(format!(
            "select {col_list}, loom_change_kind, loom_bucket, loom_offset from base_inline"
        ));
    }
    let union_sql = legs.join(" union all ");
```

Notes for the implementer (do NOT add these as code comments):
- **No schema plumbing is needed.** `file_schema` was only ever used to register `base_files`; with the file tier skipped, the fold reads the inline batch's own schema, which `inline_live_batch_full` already returns fully framed. Unlike the `mv_delta` fix, no `framed_schema` helper is required and `mv_delta.rs`'s private `fn framed_schema` does **not** need its visibility changed.
- Everything downstream of `union_sql` (the `order_clause`, `fold_sql`, the `overwrite_parquet_snapshot_consuming` / `overwrite_parquet_snapshot` split, the flag clearing) is **unchanged**. Do not touch it.
- `has_inline` is deliberately still derived from the same `if let Some(…) = &inline` as before, because `inline` is consumed by value further down (the `match inline { Some((_, row_ids, _)) => … }`).
- With this step alone, a table with **neither** tier produces an empty `union_sql`, so `fold_sql` contains `from () base_input` and `df_ctx.sql()` fails with `EngineServingError::Plan(SQL(ParserError(...)))`. That is expected and is Task 2's job — do not fix it here, and do **not** write a comment claiming `legs` cannot be empty, because at this commit it can.

- [ ] **Step 4: Run the test to verify it passes**

Run: `buck2 test --console none //src/services/engine-serving:cdc-consolidate-inline-source`
Expected: `Tests finished: Pass 1. Fail 0.`

- [ ] **Step 5: Prove the fold's existing arms still work**

Run: `buck2 test --console none //src/services/engine-serving/... //src/services/query-api:stream-cdc-consolidate`
Expected: all pass — in particular `cow-consolidate`, `consolidate-lock`, and `mv-delta-inline-source`. The existing CDC e2e (`query-api/tests/stream_cdc_consolidate.rs`) carries an explicit `gov.flush_table(...)`; **leave it** — it exercises the files-present arm on purpose.

- [ ] **Step 6: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/services/engine-serving/src/consolidate.rs src/services/engine-serving/tests/cdc_consolidate_inline_source.rs src/services/engine-serving/BUCK
git commit -m "fix(engine-serving): consolidate a CDC base that was never flushed

The CDC arm read the file tier unconditionally, so read_files_as_batches'
catalog.load_table errored for a base with no iceberg_tables row even when
the file list was empty. Guard the file tier and build the fold's union from
the tiers that actually exist, as the COW arm already does.

Refs iss-consolidate-inline-only-base"
```

---

### Task 2: The neither-tier no-op clears the consolidate trigger

Without this, a base with no files and no live inline rows now hits an empty `union_sql` and fails in the SQL parser. Worse — and this is the durable half of the bug — **`arm_consolidate_trigger` sets `enqueued = true` and only a *successful* consolidate clears it** (`clear_consolidate_trigger`). The enqueue condition is `delta_count >= effective && !st.enqueued`, so once a consolidate job fails and abandons, the trigger stays latched **forever**: that table can never enqueue another `stream_consolidate`, even after a later flush would have made it succeed. The no-op must clear the trigger, mirroring the COW arm's stale-flag self-heal (`consolidate.rs:399-415`).

**Files:**
- Modify: `src/services/engine-serving/src/consolidate.rs` (in `consolidate_locked`, immediately after the `inline` read added in Task 1)
- Modify: `src/services/engine-serving/tests/cdc_consolidate_inline_source.rs` (append two cases)

**Interfaces:**
- Consumes: `control_plane_postgres::iceberg_inline::clear_has_shadow(&mut PgConnection, i64) -> Result<()>` and `control_plane_postgres::iceberg_mirror::clear_consolidate_trigger(&mut PgConnection, i64) -> Result<()>` — both **already imported** at the top of `consolidate.rs` and already called on the function's success path (lines 336-346). No new imports in `src/`.
- Consumes (test only): `control_plane_postgres::iceberg_mirror::{bump_consolidate_trigger, arm_consolidate_trigger, ConsolidateTriggerState}`. `bump_consolidate_trigger(conn, table_id, add_deltas, global_threshold) -> Result<ConsolidateTriggerState>` returns `{ delta_count: i64, effective: i64, enqueued: bool }` — the enqueue decision the production caller makes is `delta_count >= effective && !enqueued`, so asserting on the returned `enqueued` is asserting on the wedge itself.
- Produces: no new public API.

- [ ] **Step 1: Write the failing tests**

Append to `src/services/engine-serving/tests/cdc_consolidate_inline_source.rs`. Add the two mirror-trigger imports to the existing `iceberg_mirror` use statement:

```rust
use control_plane_postgres::iceberg_mirror::{
    arm_consolidate_trigger, bump_consolidate_trigger, ensure_table, next_snapshot,
};
```

Add the zero-row batch helper alongside the other helpers (it belongs to *this* task: a helper introduced in a commit that does not yet use it is dead code, and dead code makes `[clippy.txt]` non-empty, which is how `tools/clippy-all.sh` and the prek `clippy` hook define failure):

```rust
/// A ZERO-row `(id, val)` batch — `inline_append_decl` mints the snapshot and
/// projects the mirror columns BEFORE its per-row insert loop, so appending this
/// leaves a live, schema-bearing table with NO file tier and NO live inline row:
/// the one shape that reaches the neither-tier arm.
fn empty_batch() -> RecordBatch {
    RecordBatch::try_new(
        arrow_cols(),
        vec![
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
        ],
    )
    .expect("empty batch")
}
```

```rust
/// A CDC base with NEITHER tier — declared and snapshotted, but no Parquet file
/// and no live inline row — is a clean no-op. The regression that matters is the
/// SECOND half: the no-op must CLEAR the consolidate trigger. `arm_consolidate_trigger`
/// sets `enqueued = true` and only a successful consolidate clears it, and the
/// enqueue condition is `delta_count >= effective && !enqueued` — so a consolidate
/// that fails and abandons latches the trigger FOREVER and the table can never
/// enqueue another `stream_consolidate`, even after a flush would have made it work.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn neither_tier_is_a_noop_that_unlatches_the_trigger() {
    let fx = PgFixture::shared();
    let (cp, pool, catalog, _wh) = setup(fx).await;
    let table = tref("cdc", "empty");

    let tid = declare_cdc_table(&cp, &pool, &table).await;
    // Zero-row append: mints the snapshot and projects the mirror columns, but
    // leaves NO file tier and NO live inline row.
    inline_append(&pool, &table, &cols(), &empty_batch(), lin(), None, None)
        .await
        .expect("declare-only zero-row append");

    // Put the table in exactly the state a real enqueue leaves it in: deltas
    // accrued past the threshold, job armed.
    let mut conn = pool.acquire().await.expect("acquire");
    let armed = bump_consolidate_trigger(&mut conn, tid, 128, 128)
        .await
        .expect("bump");
    assert!(
        armed.delta_count >= armed.effective,
        "sanity: the trigger is over threshold"
    );
    arm_consolidate_trigger(&mut conn, tid).await.expect("arm");
    drop(conn);

    let snap = engine_serving::consolidate_table(&cp, &catalog, &pool, &table)
        .await
        .expect("a base with neither tier is a no-op, not an error");
    assert_eq!(snap, 0, "nothing to fold: no new snapshot");

    // The wedge is gone: the trigger is disarmed AND its counter reset, so the
    // next accrual can enqueue again.
    let mut conn = pool.acquire().await.expect("acquire");
    let after = bump_consolidate_trigger(&mut conn, tid, 1, 128)
        .await
        .expect("bump after the no-op");
    assert!(
        !after.enqueued,
        "the no-op disarmed the trigger — a later consolidate can be enqueued again"
    );
    assert_eq!(
        after.delta_count, 1,
        "the no-op reset the delta counter; only the fresh bump is counted"
    );
}

/// The preserved invariant: the CDC arm does NOT early-return when the inline tier
/// is empty. A post-flush base is files-only, and folding it (re-writing the
/// coalesced survivors) is legitimate work — Task 1's guard must not have turned
/// that into a no-op.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn files_only_base_still_folds() {
    let fx = PgFixture::shared();
    let (cp, pool, catalog, _wh) = setup(fx).await;
    let table = tref("cdc", "flushed");

    declare_cdc_table(&cp, &pool, &table).await;
    inline_append(&pool, &table, &cols(), &row(1, 100), lin(), None, None)
        .await
        .expect("seed inline append");
    update(&pool, &table, 1, 100, 200).await;

    // Flush moves the whole inline tier (the +I, the -U/+U pair) into Parquet, so
    // the base is now files-only with nothing live inline.
    control_plane_postgres::iceberg_flush::flush_table(
        &catalog,
        &pool,
        &table,
        RunId(uuid::Uuid::new_v4()),
    )
    .await
    .expect("flush")
    .expect("the inline rows were flushed to Parquet");

    let snap = engine_serving::consolidate_table(&cp, &catalog, &pool, &table)
        .await
        .expect("a files-only CDC base still folds");
    assert!(snap > 0, "the files-only fold committed a snapshot, got {snap}");

    let ice = IcebergCatalog::new(pool.clone());
    let files = ice
        .files_with_stats(&table, SnapshotId(snap))
        .await
        .expect("files_with_stats");
    let paths: Vec<String> = files.into_iter().map(|f| f.path).collect();
    let (_schema, batches) = read_files_as_batches(&catalog, &table, &paths)
        .await
        .expect("read the folded base");
    assert_eq!(
        rows_sorted(&batches),
        vec![(1, 200)],
        "the flushed change subset folds to one row per identity"
    );
}

/// The spec's second preserved invariant: a fold whose every identity's winner is
/// a `-D` yields ZERO rows, and the overwrite short-circuits to `overwrite_truncate`
/// — which is mirror-only and therefore safe with no `iceberg_tables` row. This is
/// the inline-only shape most likely to still reach the Iceberg catalog, so pin it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_only_all_tombstoned_fold_truncates() {
    let fx = PgFixture::shared();
    let (cp, pool, catalog, _wh) = setup(fx).await;
    let table = tref("cdc", "all_deleted");

    declare_cdc_table(&cp, &pool, &table).await;
    inline_append(&pool, &table, &cols(), &row(1, 100), lin(), None, None)
        .await
        .expect("seed inline append");

    // DELETE id=1 — a `-D` tombstone, the greatest-offset image for that identity,
    // so the fold drops it and yields an EMPTY row set. No flush anywhere.
    let witness = current_inline_version(&pool, &table, &id_spec(), "id", &id_batch(1))
        .await
        .expect("current_inline_version");
    write_inline_delta(
        &pool,
        &table,
        &id_spec(),
        "id",
        true,
        &id_batch(1),
        None,
        lin(),
        witness,
        None,
        &[],
    )
    .await
    .expect("cdc delete delta");

    let snap = engine_serving::consolidate_table(&cp, &catalog, &pool, &table)
        .await
        .expect("an all-tombstoned inline-only fold truncates, it does not error");
    assert!(snap > 0, "the truncate committed a snapshot, got {snap}");

    let ice = IcebergCatalog::new(pool.clone());
    let files = ice
        .files_with_stats(&table, SnapshotId(snap))
        .await
        .expect("files_with_stats");
    assert!(
        files.is_empty(),
        "every identity was tombstoned: the folded base holds no data file"
    );
    let live = ice
        .inline_live_batch_full(&table, SnapshotId(snap))
        .await
        .expect("inline_live_batch_full");
    assert!(
        live.is_none(),
        "the truncate end-capped the folded inline rows, got {live:?}"
    );
}
```

- [ ] **Step 2: Run the tests to verify they fail for the right reason**

Run: `buck2 test --console none //src/services/engine-serving:cdc-consolidate-inline-source`

Expected: `neither_tier_is_a_noop_that_unlatches_the_trigger` **FAILS** — after Task 1 the empty `union_sql` yields a DataFusion `Plan(SQL(ParserError(...)))` at the `.expect("a base with neither tier is a no-op, not an error")`.

`files_only_base_still_folds` and `inline_only_all_tombstoned_fold_truncates` should already **PASS** — they are characterization tests pinning invariants Task 1 must not have broken (the CDC arm does not early-return on an empty inline tier; a zero-row fold truncates). If either fails, Task 1's guard is wrong — fix that before adding the early return.

- [ ] **Step 3: Add the trigger-clearing early return**

In `consolidate_locked`, immediately after the `let inline = ice.inline_live_batch_full(…)` read and **before** the `SessionContext::new()`:

```rust
    // Neither tier: nothing to fold. NOT a bare `return Ok(0)` — the consolidate
    // trigger is armed (`enqueued = true`) by the enqueue that scheduled this job
    // and only a completed consolidate clears it, so an early return that skipped
    // the clear would latch the trigger forever and this table could never enqueue
    // another `stream_consolidate`. Mirrors the COW arm's stale-flag self-heal.
    if !has_files && inline.is_none() {
        let mut conn = pool.acquire().await.map_err(to_serving)?;
        clear_has_shadow(&mut conn, tid).await.map_err(to_serving)?;
        clear_consolidate_trigger(&mut conn, tid)
            .await
            .map_err(to_serving)?;
        return Ok(0);
    }
```

Now that the early return exists, extend Task 1's `legs` comment with the fact it establishes (it would have been a lie in Task 1's commit, which is why it lands here):

```rust
    // One leg per registered tier — a base can legitimately be files-only (the
    // post-flush fold), inline-only (never flushed), or both. The neither-tier case
    // returned above, so `legs` is never empty here.
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `buck2 test --console none //src/services/engine-serving:cdc-consolidate-inline-source`
Expected: `Tests finished: Pass 4. Fail 0.`

- [ ] **Step 5: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/services/engine-serving/src/consolidate.rs src/services/engine-serving/tests/cdc_consolidate_inline_source.rs
git commit -m "fix(engine-serving): a no-op consolidate must unlatch its trigger

A CDC base with neither tier now returns 0 after clearing has_shadow and the
consolidate trigger. The trigger is armed by the enqueue and only cleared by a
completed consolidate, so a consolidate that failed or bailed early left it
latched and the table could never enqueue another stream_consolidate.

Refs iss-consolidate-inline-only-base"
```

---

### Task 3: Pin the COW arm's already-guarded inline-only branch

`consolidate_cow_locked` has carried the `if has_files` guard since it was written (`consolidate.rs:421`), but `cow_consolidate.rs` has **no inline-only case** — every existing case seeds Parquet via `IcebergWriter::seed_arrays` first, so that branch is untested. This is the branch Task 1 copied; a test costs one case and closes it.

**Files:**
- Modify: `src/services/engine-serving/tests/cow_consolidate.rs` (append Case 8; no BUCK change — the `cow-consolidate` target already exists)

**Interfaces:**
- Consumes: the file's existing local helpers — `define_type(&PgControlPlane, schema, name, Option<&str>)`, `cols() -> Vec<(String, String, bool)>`, `full_specs() -> Vec<ColumnSpec>`, `id_specs() -> Vec<ColumnSpec>`, `version_batch(id, name) -> RecordBatch`, `id_batch(id) -> RecordBatch`, `lineage() -> LineageEvent`, `rows_sorted(&[RecordBatch]) -> Vec<(i64, String)>` — plus `IcebergWriter::{new, inline, sql_catalog}` (`inline(&self, ns, name, columns: &[(String, String, bool)], rows: &[(i64, &str)], run: Uuid) -> i64`; it calls `inline_append`, which `ensure_table`s the mirror row itself, so it works on a table with **no** prior `seed_arrays` and no `iceberg_tables` row).
- **New import required.** `cow_consolidate.rs:26` currently reads `use control_plane_postgres::iceberg_inline::{has_shadow, write_inline_delta};`. This task needs `current_inline_version` too:
  ```rust
  use control_plane_postgres::iceberg_inline::{current_inline_version, has_shadow, write_inline_delta};
  ```
- Produces: nothing consumed by other tasks.

- [ ] **Step 1: Write the test**

Append to `src/services/engine-serving/tests/cow_consolidate.rs`.

**The CAS witness matters here and it is NOT `0`.** `write_inline_delta` CASes on `current_inline_version` (= `max(begin_snapshot)` over the identity's live non-`-U` inline rows). The existing `seed()` in this file passes a hardcoded `0` and gets away with it *only because its rows are file-only* — its own comment says so ("Each id has no prior inline row, so the CAS witness is version 0"). In this case id=2 is created by `writer.inline(...)`, i.e. it **has** a live inline row with `begin_snapshot > 0`, so a hardcoded `0` returns `ControlPlaneError::Conflict` and the test fails. Read the witness first.

```rust
// ---------------------------------------------------------------------------
// Case 8 — inline-only COW base: a shadow-bearing identity table that has never
//          been flushed (no Parquet file, no `iceberg_tables` row) folds through
//          the `has_files` guard. The CDC arm's sibling defect
//          (`iss-consolidate-inline-only-base`) was exactly this branch missing.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_only_cow_base_folds() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let writer = IcebergWriter::new(pool.clone(), dsn);
    let table = TableRef {
        schema: "wh".into(),
        name: "inline_only".into(),
    };

    // NO `seed_arrays` — the table is minted by an inline append alone, so it has
    // no Parquet file and no Iceberg SQL-catalog row.
    writer
        .inline("wh", "inline_only", &cols(), &[(1, "one"), (2, "two")], Uuid::new_v4())
        .await;
    define_type(&cp, "wh", "inline_only", Some("id")).await;

    // A `+U` on id=2 sets `has_shadow`, which is what arms the COW arm. id=2 was
    // created INLINE above, so it already has a live inline row: its CAS witness is
    // its `begin_snapshot`, not 0 (unlike this file's file-seeded `seed()`).
    let witness = current_inline_version(&pool, &table, &id_specs(), "id", &id_batch(2))
        .await
        .expect("CAS witness for the live inline id=2");
    write_inline_delta(
        &pool,
        &table,
        &full_specs(),
        "id",
        false,
        &version_batch(2, "two-v2"),
        None,
        lineage(),
        witness,
        None,
        &[],
    )
    .await
    .expect("update id=2");

    let sql_catalog = writer.sql_catalog().await;
    let catalog = IcebergCatalog::new(pool.clone());

    let snap = engine_serving::consolidate_table(&cp, &sql_catalog, &pool, &table)
        .await
        .expect("an inline-only COW base folds through the has_files guard");
    assert!(snap > 0, "the fold committed a real snapshot, got {snap}");

    let files = catalog
        .files_with_stats(&table, SnapshotId(snap))
        .await
        .expect("files_with_stats");
    let paths: Vec<String> = files.into_iter().map(|f| f.path).collect();
    let (_schema, batches) = read_files_as_batches(&sql_catalog, &table, &paths)
        .await
        .expect("the fold created the base's Iceberg table");
    assert_eq!(
        rows_sorted(&batches),
        vec![(1, "one".to_string()), (2, "two-v2".to_string())],
        "the inline-only fold materializes the merge view: id=2 takes its +U image"
    );
}
```

- [ ] **Step 2: Run it**

Run: `buck2 test --console none //src/services/engine-serving:cow-consolidate`
Expected: `Tests finished: Pass 8. Fail 0.` — this case passes **as written**: `consolidate_cow_locked`'s `has_files` guard already exists, and `IcebergWriter::inline` mints the mirror row itself (verified), so no `seed_arrays` and no Iceberg catalog row is needed. It is a characterization test, not a red-then-green one — it exists to pin a branch that shipped untested and that Task 1 copied.

- [ ] **Step 3: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/services/engine-serving/tests/cow_consolidate.rs
git commit -m "test(engine-serving): pin the COW arm's inline-only fold

The has_files guard in consolidate_cow_locked had no test — every case seeded
Parquet first. It is the branch the CDC-arm fix copies.

Refs iss-consolidate-inline-only-base"
```

---

### Task 4: Close the register item and record what the spec found

The registers carry **open work only**: the closing PR removes the item's entry and folds the landed behavior into `docs/system-capabilities/`. This item's spec also (a) corrects two things the register entry got wrong and (b) found a **second, unreported instance of the same bug**, which must be filed rather than fixed here.

**Files:**
- Modify: `docs/ISSUES.md` (remove the `iss-consolidate-inline-only-base` entry; add the new `collect_vectors` item)
- Modify: `docs/system-capabilities/engine.md` (record the landed behavior)

**Interfaces:**
- Consumes: the register grammar — one markdown list entry whose title line carries a backtick-wrapped tag block: `` - [ ] **Title** `{#id area:<a> status:<s> from:<f> pr:<p> spec:<sp>}` ``. `#id` is prefixed `iss-` for ISSUES; `area:` comes from the controlled vocab (use `area:query` — `vector_index.rs` serves the vector-search read path; check the vocab in `tools/docs.sh` and pick the closest existing value rather than inventing one).
- Produces: a green `bash tools/docs.sh validate`.

- [ ] **Step 1: Use the loom-docs-update skill**

Invoke the `loom-docs-update` skill. It is the routine that closes resolved register items and records new deferrals at spec/plan completion, and the repo's `Stop` hook nudges for it whenever a branch touched a spec/plan without touching a register. Follow it rather than hand-editing if the two disagree.

- [ ] **Step 2: Remove the closed item from `docs/ISSUES.md`**

Delete the whole `- [ ] **`consolidate_stream`'s CDC arm can't read an inline-only base…** {#iss-consolidate-inline-only-base …}` entry and its prose paragraph. Do **not** mark it `[x]` and leave it — the registers hold open work only; git history is the item-by-item record.

- [ ] **Step 3: File the out-of-scope bug the spec found**

`collect_vectors` (`src/control-plane/postgres/src/vector_index.rs:464`, called by `build_vector_index` at `:652`) has the **identical** defect: it passes `paths` straight from `files_with_stats` into `read_files_as_batches` and then unions the hot inline tier — i.e. it explicitly supports inline-only data, yet dies in `load_table` when the table has no Parquet file. It is reachable via the engine's `BuildVectorIndex` RPC (`engine/src/service.rs:477`), and note `overwrite_truncate` enqueues rebuild jobs *without* creating an Iceberg table. Add to `docs/ISSUES.md` under the matching area heading:

```markdown
- [ ] **`collect_vectors` can't read an inline-only table — the third instance of the unflushed-`load_table` bug** `{#iss-collect-vectors-inline-only area:query status:open from:2026-07-14-consolidate-inline-only-base-design pr:- spec:-}`
  `collect_vectors` (`control-plane/postgres/src/vector_index.rs:464`, called by `build_vector_index`, `:652`) passes `paths` straight from `files_with_stats` into `read_files_as_batches` and then unions the hot inline tier — so it explicitly supports inline-only data, yet dies in `catalog.load_table` for a table with no Parquet file and no `iceberg_tables` row, exactly as `mv_delta_locked` did (`#iss-mv-delta-inline-source-unflushed`, closed by #436) and `consolidate_locked`'s CDC arm did (`#iss-consolidate-inline-only-base`, closed by this PR). Reachable via the engine's `BuildVectorIndex` RPC (`engine/src/service.rs:477`); note `overwrite_truncate` enqueues rebuild jobs *without* creating an Iceberg table. **Fix shape:** the same `if !paths.is_empty()` guard around the file leg, with the inline tier's own schema carrying the empty-file case. Found while spec'ing `#iss-consolidate-inline-only-base`; filed rather than folded in to keep that fix scoped. For completeness, the remaining `read_files_as_batches` call site — `engine/src/flight.rs:306` (Flight `DoGet`) — is non-empty by construction at its only in-tree producer (`worker/src/compact.rs:40-50` returns early unless `small.len() >= 2`), though it is not defensively guarded either.
```

- [ ] **Step 4: Carry the spec's corrections into the PR body (not the register)**

The spec lists two corrections to the register entry's prose. Since the entry is **deleted** on close, they have nowhere to land in the register — put them in the **PR description** so the record is accurate in git history:

1. The entry said the reachable shape is "a small first `land` plus `write_delta` **inserts**". Wrong: `write_delta` is the UPDATE/DELETE path and is the *only* thing that bumps the consolidate trigger; inserts (`write_object` → `land_cdc` → `inline_append_decl`) never bump it. The reachable shape is **inline insert(s) + ≥128 delta-rows of governed UPDATE/DELETE** (the production `consolidate_threshold` default, `ingest/src/config.rs:67`).
2. The entry called the fix "the same one-line `if !paths.is_empty()` guard plus an empty-tier early return; cheap" — that **under-specifies** it. `mv_delta`'s early-return-an-empty-delta is not the analogue, because consolidate's job is to **write**: the inline-only case is real work (the fold materializes the base and creates its Iceberg table), and the neither-tier case must clear the consolidate trigger or the table stays wedged forever.

- [ ] **Step 5: Record the landed capability**

Add to `docs/system-capabilities/engine.md`, in the consolidation section (match the file's existing prose style — read the surrounding sections first and follow them; do not invent a new heading level):

> `consolidate_table` folds a CDC base whether its rows live in Parquet, in the inline tier, or both: the file tier is read only when the table actually has files, so a base that has never been flushed (a CDC declare pre-creates only the *changelog* Iceberg table) consolidates on the inline tier alone, and the fold's own overwrite creates the base's Iceberg table. A base with neither tier is a no-op that still clears `has_shadow` and the consolidate trigger, so the table can enqueue another `stream_consolidate` later.

- [ ] **Step 6: Validate and commit**

```bash
bash tools/docs.sh validate
buck2 run //tools:prek -- run --all-files
git add docs/ISSUES.md docs/system-capabilities/engine.md
git commit -m "docs: close iss-consolidate-inline-only-base, file iss-collect-vectors-inline-only

Refs iss-consolidate-inline-only-base"
```

Expected from `validate`: no errors (it checks grammar, ids, the `area:` vocab, cross-links, and that every `spec:` slug resolves to a file on disk).

---

## Final Review (after all tasks)

Run the whole-implementation review that `superpowers:subagent-driven-development` ends with, and **include the metric gate**:

- [ ] `buck2 test --console none //src/...` — the full sweep, green. (Fixture tests run locally on this non-root host; no `--unstable-allow-all-tests-on-re` needed.)
- [ ] `buck2 run //tools:prek -- run --all-files` — clean, nothing rewritten.
- [ ] `loom-complexity diff` — report any NEW hotspot over the census thresholds (cc > 15, cognitive > 15, MI < 20, SLOC > 100). `consolidate_locked` grows by ~15 lines; if it crosses a threshold, either decompose it or justify it explicitly in the PR description.
- [ ] `loom-duplication diff` — report any NEW cross-file duplication pair ≥ 20 lines. The likely hit is the new test file's helpers against `mv_delta_inline_source.rs` / `stream_cdc_consolidate_trigger.rs` seeding helpers; a per-file test seed is legitimate divergence, but say so in the PR body rather than leaving it unexplained.
- [ ] Findings are advisory, not auto-blocking — each must be **either fixed or explicitly justified in the PR description**.

## Acceptance (from the spec — verify each before opening the PR)

1. A CDC base that has only ever been inline-appended consolidates successfully, with **no flush anywhere**, and its Iceberg base table is created by the fold. → `inline_only_cdc_base_consolidates_without_a_flush`
2. The neither-tier case is a clean no-op that **clears** the consolidate trigger (the table is not wedged). → `neither_tier_is_a_noop_that_unlatches_the_trigger`
3. Existing suites green (`buck2 test //src/...`).

Plus the two spec invariants, pinned as characterization tests (they must pass without any src change beyond Task 1): the CDC arm does **not** early-return on an empty inline tier → `files_only_base_still_folds`; and a fold yielding zero rows short-circuits to `overwrite_truncate`, which is mirror-only and safe with no `iceberg_tables` row → `inline_only_all_tombstoned_fold_truncates`. And the COW arm's inline-only branch, which shipped untested → `inline_only_cow_base_folds`.
