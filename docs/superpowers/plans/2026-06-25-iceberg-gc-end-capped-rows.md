# Physical GC of end-capped Iceberg rows (slice 1) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an operator-triggered, per-table `gc_table` that physically deletes the Iceberg-mirror rows (and their Parquet objects) that have aged out of the time-travel retention window, without breaking any in-window time-travel read.

**Architecture:** A new `iceberg_gc::gc_table` reclaim primitive (postgres adapter) resolves an age-based horizon `H` from the existing `iceberg_mirror.snapshot.snapshot_time` column, deletes end-capped `data_file`/`data_file_column_stat`/inline rows at or below `H` in one transaction, then deletes their Parquet objects via a new `SqlCatalog::delete_file` FileIO seam (commit-then-delete ordering). The primitive is reached over the engine-wire as a new `GcTable` RPC (engine owns Postgres + object store), enqueued by a zero-pool worker `handle_gc`, fronted by an operator HTTP endpoint on query-api. The work is split into **Layer 1** (the tested reclaim primitive — a complete, independently shippable slice) and **Layer 2** (the execution-model wiring), implemented and verified in that order.

**Tech Stack:** Rust 2024, buck2, sqlx 0.9 compile-time `query!` (+ committed `.sqlx` cache), the `iceberg` crate's `FileIO`, tonic/prost (engine-wire), axum (query-api), `loom_fixture_test` (hermetic Postgres + `file://` object store).

## Global Constraints

- **Tests are `rust_test` integration targets only** — never inline `#[cfg(test)]`/`#[test]` in `src/**.rs`. The `no-inline-tests` prek hook enforces this.
- **Fixture tests use the `loom_fixture_test` macro** (`src/control-plane/postgres/defs.bzl`), never a bare `rust_test`. In this cloud (root) session, `buck2 test` routes fixture tests to the non-root RE worker automatically.
- **SQL strategy — runtime queries, NO new `query!` macros (recorded deviation).** `cargo sqlx prepare` (the `.sqlx` regen tool) currently FAILS to compile the postgres crate in cargo-mode: `iceberg_sql_catalog/s3_storage.rs` uses `serde::{Deserialize,Serialize}` + `#[typetag::serde]` but `serde` is not a direct dependency in `src/control-plane/postgres/Cargo.toml` (buck2 builds it fine via reindeer's graph-wide feature unification; cargo does not). Fixing it means adding `serde` to the manifest + a reindeer re-lock, which risks the documented duckdb-downgrade footgun (CLAUDE.md). So `iceberg_gc.rs` adds **zero** compile-time `query!`/`query_scalar!` macros and instead uses **runtime** `sqlx::query`/`sqlx::query_scalar` (static `&'static str` literals need no `AssertSqlSafe`; the dynamic `inline_<table_id>` delete uses `AssertSqlSafe(format!(...))`). Consequence: **no `.sqlx` regen is needed** (Task 3 is dropped) and the existing `sqlx-cache-check` test is unaffected. Every query is exercised against a real schema by the Task-4 fixture tests. The cargo-mode breakage is filed as an ISSUES item (Task 10 step 4).
- **Reuse `crate::backend`** (the crate-root `fn backend(e: sqlx::Error) -> ControlPlaneError`, visible to all submodules) — do NOT define a local copy.
- **No new third-party dependency.** Reuse the `iceberg`, `sqlx`, `time`, `tonic` crates already in the tree (avoids a reindeer re-lock).
- **Spec deviation (recorded):** the spec's migration `0017_iceberg_snapshot_committed_at.sql` adding a `committed_at` column is **redundant and is NOT created** — `iceberg_mirror.snapshot.snapshot_time timestamptz not null default now()` already exists (migration `0012`, lines 5–12) and is inserted inside the committing transaction by `next_snapshot` (`iceberg_mirror.rs:56-63`), giving exactly the commit-time→snapshot mapping the spec wanted. The horizon query and the test's age-injection use `snapshot_time`.
- **Markdown lint:** any `.md` file ends with exactly one trailing newline, no trailing whitespace (the `lint` CI job checks all files).
- **Conventional Commits** on every commit message (`feat(...)`, `test(...)`, etc.) — the commit-msg hook enforces it.
- Reuse existing helpers: `crate::iceberg_flush::lock_key`, `crate::iceberg_mirror::live_table_id`, `crate::iceberg_inline::inline_table_name`, the `backend(e: sqlx::Error)` and `iceberg_err(e: iceberg::Error)` error mappers.

---

# Layer 1 — the tested reclaim primitive

## Task 1: `SqlCatalog::delete_file` object-store delete seam

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs` (the `impl SqlCatalog` block; struct is at ~205–215, `fileio: FileIO` field)
- Test: `src/control-plane/postgres/tests/iceberg_gc.rs` (new — first test added here; BUCK target added in Task 4)

**Interfaces:**
- Consumes: `self.fileio: FileIO` (private field of `SqlCatalog`); `FileIO::delete(path: impl AsRef<str>) -> iceberg::Result<()>` (async, idempotent — no error if the object is absent).
- Produces: `pub async fn delete_file(&self, path: &str) -> control_plane_core::Result<()>` on `SqlCatalog`.

- [ ] **Step 1: Write the failing test** — append to `src/control-plane/postgres/tests/iceberg_gc.rs`:

```rust
use std::collections::HashMap;
use std::sync::Arc;

use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;

async fn make_catalog(dsn: String, warehouse: &str) -> SqlCatalog {
    let mut props = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), dsn);
    props.insert(
        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
        format!("file://{warehouse}"),
    );
    SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("loom", props)
        .await
        .expect("catalog")
}

/// Strip a `file://` URL to a local filesystem path.
fn local_path(file_url: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(file_url.strip_prefix("file://").unwrap_or(file_url))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_file_removes_object_and_is_idempotent() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;

    // Write a real object under the warehouse and form its file:// URL.
    let obj = wh.path().join("victim.parquet");
    std::fs::write(&obj, b"bytes").expect("write object");
    let url = format!("file://{}", obj.display());
    assert!(obj.exists(), "object exists before delete");

    catalog.delete_file(&url).await.expect("delete");
    assert!(!obj.exists(), "object gone after delete");

    // Idempotent: deleting an absent object is not an error.
    catalog.delete_file(&url).await.expect("second delete is a no-op");
}
```

- [ ] **Step 2: Run test to verify it fails to compile**

Run: `buck2 build //src/control-plane/postgres:iceberg-gc 2>&1 | tail -20` (target added in Task 4; if not yet present, this fails at target resolution — acceptable, proceed). Expected: `no method named delete_file found for struct SqlCatalog`.

- [ ] **Step 3: Add the seam** — in `catalog.rs`, inside `impl SqlCatalog`, add:

```rust
/// Physically delete an object-store file by its absolute URL (e.g. a `file://`
/// or `s3://` Parquet path). Idempotent: a missing object is not an error. The
/// only object-store *delete* capability on the catalog — used by GC to reclaim
/// the Parquet of end-capped data files. Read/write paths are untouched.
pub async fn delete_file(&self, path: &str) -> control_plane_core::Result<()> {
    self.fileio
        .delete(path)
        .await
        .map_err(|e| control_plane_core::ControlPlaneError::Backend(Box::new(e)))
}
```

(If `catalog.rs` already has a private `iceberg_err`/`be` mapper in scope, call that instead of the inline closure — match the file's existing style.)

- [ ] **Step 4: Defer running** — this test runs in Task 4's suite (the BUCK target is created there). Do not commit yet; Task 2 adds more to the same module/test file.

---

## Task 2: `gc_table` reclaim primitive + `GcSummary`

**Files:**
- Create: `src/control-plane/postgres/src/iceberg_gc.rs`
- Modify: `src/control-plane/postgres/src/lib.rs` (add `pub mod iceberg_gc;` in the iceberg module group, ~line 29)
- Modify: `src/control-plane/postgres/src/iceberg_flush.rs` (change `fn lock_key` → `pub(crate) fn lock_key`, ~line 136)

**Interfaces:**
- Consumes: `crate::iceberg_flush::lock_key(schema, name) -> i64`; `crate::iceberg_mirror::live_table_id(conn, ns, name) -> Result<Option<i64>>`; `crate::iceberg_inline::inline_table_name(table_id: i64) -> String`; `SqlCatalog::delete_file` (Task 1); `control_plane_core::{TableRef, Result, ControlPlaneError}`; `sqlx::{PgPool, AssertSqlSafe}`; `time::OffsetDateTime`.
- Produces:
  - `pub struct GcSummary { pub data_file_rows: u64, pub inline_rows: u64, pub objects_deleted: u64 }` (derive `Debug, Clone, Default, PartialEq, Eq`).
  - `pub async fn gc_table(catalog: &SqlCatalog, pool: &PgPool, table: &TableRef, retention: std::time::Duration) -> Result<GcSummary>`.

- [ ] **Step 1: Write the new module** — create `src/control-plane/postgres/src/iceberg_gc.rs`:

```rust
//! Physical GC of end-capped Iceberg-mirror rows (slice 1).
//!
//! Every Iceberg retirement is an *end-cap*, not a delete: a row's `end_snapshot`
//! is set so older time-travel reads still see it, while the bytes (and, for
//! `data_file` rows, the Parquet object) live on. `gc_table` reclaims the
//! mirror-driven dead bytes — end-capped `data_file` rows + their Parquet, and
//! end-capped inline rows — under an age-based retention horizon, without ever
//! breaking an in-window time-travel read.
//!
//! ## Safety invariant
//! A row is reclaimable iff `end_snapshot IS NOT NULL AND end_snapshot <= H`,
//! where `H = max(snapshot_id) WHERE snapshot_time < now() - retention` (the
//! youngest snapshot fully aged out of the window). The MVCC read predicate is
//! `begin_snapshot <= at AND (end_snapshot IS NULL OR end_snapshot > at)`; an
//! end-capped row with `end_snapshot = E` is visible only for `at < E`. The
//! oldest `at` any in-window reader may supply is a snapshot `> H`, so if
//! `E <= H` the row is invisible to every guaranteed read. Live rows
//! (`end_snapshot IS NULL`) are never touched.
//!
//! ## Ordering: commit-then-delete
//! The mirror is the source of truth. The transaction deletes the rows first;
//! the Parquet objects are deleted *after* commit. A failure between the two
//! degrades a file to the already-deferred orphaned-Parquet class — never data
//! loss, never a dangling mirror→file reference.

use std::time::Duration;

use control_plane_core::{Result, TableRef};
use sqlx::{AssertSqlSafe, PgPool};
use time::OffsetDateTime;

use crate::backend;
use crate::iceberg_flush::lock_key;
use crate::iceberg_inline::inline_table_name;
use crate::iceberg_mirror::live_table_id;
use crate::iceberg_sql_catalog::SqlCatalog;

/// Counts of what a `gc_table` run reclaimed, for observability and tests.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GcSummary {
    pub data_file_rows: u64,
    pub inline_rows: u64,
    pub objects_deleted: u64,
}

/// Reclaim aged-out end-capped rows + their Parquet for one table.
///
/// Serializes per-table against flush/overwrite via the shared advisory lock
/// (`lock_key`). Returns an empty summary (a no-op, not an error) when the table
/// has no live mirror row or when no snapshot has aged out.
pub async fn gc_table(
    catalog: &SqlCatalog,
    pool: &PgPool,
    table: &TableRef,
    retention: Duration,
) -> Result<GcSummary> {
    // Transaction-scoped advisory lock on the table's stable key — the SAME key
    // flush/overwrite take, so GC never races a concurrent flush on this table.
    let mut lock_tx = pool.begin().await.map_err(backend)?;
    let key = lock_key(&table.schema, &table.name);
    sqlx::query("select pg_advisory_xact_lock($1)")
        .bind(key)
        .execute(&mut *lock_tx)
        .await
        .map_err(backend)?;

    let result = gc_locked(catalog, pool, table, retention).await;

    // Rolling back the lock-holding tx releases the advisory lock.
    let _ = lock_tx.rollback().await;
    result
}

async fn gc_locked(
    catalog: &SqlCatalog,
    pool: &PgPool,
    table: &TableRef,
    retention: Duration,
) -> Result<GcSummary> {
    // 1. Resolve the live table id. A table with no live mirror row (never
    //    created, or dropped) is out of scope for slice 1 — a clean no-op.
    let mut conn = pool.acquire().await.map_err(backend)?;
    let tid = match live_table_id(&mut conn, &table.schema, &table.name).await? {
        Some(t) => t,
        None => return Ok(GcSummary::default()),
    };
    drop(conn);

    // 2. Resolve the horizon H = youngest snapshot fully aged out of the window.
    //    `now()` is taken in Rust; sub-second precision is irrelevant at GC scale.
    //    Runtime query (static literal — no AssertSqlSafe needed); `max()` over
    //    zero matching rows yields NULL → None → a clean no-op.
    let cutoff = OffsetDateTime::now_utc() - time::Duration::seconds(retention.as_secs() as i64);
    let horizon: Option<i64> = sqlx::query_scalar(
        "select max(snapshot_id) from iceberg_mirror.snapshot where snapshot_time < $1",
    )
    .bind(cutoff)
    .fetch_one(pool)
    .await
    .map_err(backend)?;
    let h = match horizon {
        Some(h) => h,
        None => return Ok(GcSummary::default()),
    };

    // 3. Collect the Parquet paths of reclaimable data files (before deleting
    //    the rows that name them).
    let paths: Vec<String> = sqlx::query_scalar(
        "select path from iceberg_mirror.data_file \
         where table_id = $1 and end_snapshot is not null and end_snapshot <= $2",
    )
    .bind(tid)
    .bind(h)
    .fetch_all(pool)
    .await
    .map_err(backend)?;

    // 4. Delete mirror rows in one transaction: stats first (FK child), then the
    //    data_file rows, then end-capped inline rows.
    let mut tx = pool.begin().await.map_err(backend)?;
    sqlx::query(
        "delete from iceberg_mirror.data_file_column_stat \
         where data_file_id in ( \
             select data_file_id from iceberg_mirror.data_file \
             where table_id = $1 and end_snapshot is not null and end_snapshot <= $2)",
    )
    .bind(tid)
    .bind(h)
    .execute(&mut *tx)
    .await
    .map_err(backend)?;

    let data_file_rows = sqlx::query(
        "delete from iceberg_mirror.data_file \
         where table_id = $1 and end_snapshot is not null and end_snapshot <= $2",
    )
    .bind(tid)
    .bind(h)
    .execute(&mut *tx)
    .await
    .map_err(backend)?
    .rows_affected();

    // Inline storage is a per-table physical table that may not exist. Guard with
    // to_regclass; the dynamic table name forces a runtime AssertSqlSafe query.
    let inline = inline_table_name(tid);
    let exists: Option<String> = sqlx::query_scalar(AssertSqlSafe("select to_regclass($1)::text"))
        .bind(&inline)
        .fetch_one(&mut *tx)
        .await
        .map_err(backend)?;
    let inline_rows = if exists.is_some() {
        sqlx::query(AssertSqlSafe(format!(
            "delete from {inline} where end_snapshot is not null and end_snapshot <= $1"
        )))
        .bind(h)
        .execute(&mut *tx)
        .await
        .map_err(backend)?
        .rows_affected()
    } else {
        0
    };

    tx.commit().await.map_err(backend)?;

    // 5. Commit-then-delete: now reclaim the Parquet objects. A failed delete is
    //    logged and left as an orphan (never re-raised into a hard error).
    let mut objects_deleted = 0u64;
    for path in &paths {
        match catalog.delete_file(path).await {
            Ok(()) => objects_deleted += 1,
            Err(e) => tracing::warn!(
                error = %e,
                path = %path,
                "gc: failed to delete Parquet object; leaving as orphan"
            ),
        }
    }

    Ok(GcSummary {
        data_file_rows,
        inline_rows,
        objects_deleted,
    })
}
```

- [ ] **Step 2: Wire the module** — in `src/control-plane/postgres/src/lib.rs`, add `pub mod iceberg_gc;` alongside the other `pub mod iceberg_*;` lines (keep alphabetical: after `pub mod iceberg_flush;`).

- [ ] **Step 3: Expose `lock_key`** — in `src/control-plane/postgres/src/iceberg_flush.rs`, change `fn lock_key(` to `pub(crate) fn lock_key(`.

- [ ] **Step 4: Build** — `buck2 build //src/control-plane/postgres:postgres 2>&1 | tail -15`. Expected `BUILD SUCCEEDED`. No `.sqlx` regen is required (runtime queries only — see Global Constraints). Commit happens after Task 4's tests pass.

---

## Task 3: ~~Regenerate the committed `.sqlx` cache~~ — ELIMINATED

**Dropped.** `iceberg_gc.rs` adds no compile-time `query!`/`query_scalar!` macros (runtime queries only — see Global Constraints), so there is nothing to regenerate; the committed `.sqlx` cache is untouched and `sqlx-cache-check` is unaffected. The `cargo sqlx prepare` pipeline is in fact currently broken in cargo-mode (the `serde`-not-a-direct-dep issue), which is *why* this module avoids the macros. Commit Layer-1 progress after Task 4's tests pass.

---

## Task 4: Fixture tests for `gc_table`

**Files:**
- Modify: `src/control-plane/postgres/tests/iceberg_gc.rs` (add the GC behavior tests to the Task-1 file)
- Modify: `src/control-plane/postgres/BUCK` (add the `iceberg-gc` `loom_fixture_test` target)

**Interfaces:**
- Consumes: `control_plane_postgres::iceberg_gc::{gc_table, GcSummary}`; `iceberg_landing::{land, overwrite_parquet_snapshot}`; `iceberg_inline::inline_append`; `iceberg_flush::flush_table`; `iceberg_catalog::IcebergCatalog` (`.current_snapshot`, `.files_with_stats`); the `make_catalog`/`local_path` helpers from Task 1.

- [ ] **Step 1: Add the BUCK target** — in `src/control-plane/postgres/BUCK`, mirror the `iceberg-overwrite` stanza:

```python
loom_fixture_test(
    name = "iceberg-gc",
    crate = "iceberg_gc",
    srcs = ["tests/iceberg_gc.rs"],
    crate_root = "tests/iceberg_gc.rs",
    deps = [
        "//third-party:arrow-array",
        ":postgres",
        "//src/control-plane/core:core",
        "//third-party:arrow-ipc",
        "//third-party:arrow-schema",
        "//third-party:iceberg",
        "//third-party:serde_json",
        "//third-party:sqlx",
        "//third-party:tempfile",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```

- [ ] **Step 2: Write the GC behavior tests** — append to `tests/iceberg_gc.rs`. Add these imports at the top of the file (next to Task 1's imports):

```rust
use arrow_array::{Int64Array, RecordBatch};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use std::time::Duration;

use control_plane_core::{
    Catalog, ColumnSpec, DatasetId, EventType, LineageEvent, PageReq, RunId, TableRef,
};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_flush::flush_table;
use control_plane_postgres::iceberg_gc::{GcSummary, gc_table};
use control_plane_postgres::iceberg_inline::inline_append;
use control_plane_postgres::iceberg_landing::{land, overwrite_parquet_snapshot};
use sqlx::AssertSqlSafe;
use time::OffsetDateTime;
```

Then the helpers + tests:

```rust
fn columns() -> Vec<ColumnSpec> {
    vec![ColumnSpec { name: "id".into(), ty: "long".into(), nullable: false }]
}

fn ipc_body(rows: i64) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>()))],
    )
    .expect("batch");
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &schema).expect("writer");
        w.write(&batch).expect("write");
        w.finish().expect("finish");
    }
    buf
}

fn batch(rows: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>()))],
    )
    .expect("batch")
}

fn lineage(run: RunId, schema: &str, name: &str) -> LineageEvent {
    let out = TableRef { schema: schema.into(), name: name.into() };
    LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(&out).dataset_ref()],
        payload: serde_json::json!({ "source": "test" }),
    }
}

/// Force snapshot `snap_id` to look aged-out by backdating its `snapshot_time`.
async fn age_snapshot(pool: &sqlx::PgPool, snap_id: i64) {
    let old = OffsetDateTime::now_utc() - time::Duration::days(365);
    sqlx::query(AssertSqlSafe(
        "update iceberg_mirror.snapshot set snapshot_time = $1 where snapshot_id = $2",
    ))
    .bind(old)
    .bind(snap_id)
    .execute(pool)
    .await
    .expect("age snapshot");
}

/// Happy path + in-window protection, for real data files (+ their Parquet/stats).
///
/// land s1 (file A, 10 rows) → overwrite s2 (file B, 4 rows; end-caps A@s2) →
/// overwrite s3 (file C, 2 rows; end-caps B@s3). Age s2 only ⇒ H = s2.
/// Reclaimable: A (end=s2 ≤ H). Retained: B (end=s3 > H), C (live).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_reclaims_aged_data_files_and_keeps_in_window() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let ice = IcebergCatalog::new(pool.clone());
    let t = TableRef { schema: "wh".into(), name: "t".into() };

    let s1 = land(&pool, &catalog, &t, &columns(), &ipc_body(10), 0, i64::MAX,
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "t")).await.expect("land");
    // file A's path, captured before it is reclaimed.
    let a_path = local_path(&ice.files_with_stats(&t, s1).await.expect("files@s1")[0].path);

    let s2 = overwrite_parquet_snapshot(&pool, &catalog, &t, &columns(), vec![batch(4)],
        Some(&lineage(RunId(uuid::Uuid::new_v4()), "wh", "t"))).await.expect("ow s2");
    let b_path = local_path(&ice.files_with_stats(&t, s2).await.expect("files@s2")[0].path);

    let s3 = overwrite_parquet_snapshot(&pool, &catalog, &t, &columns(), vec![batch(2)],
        Some(&lineage(RunId(uuid::Uuid::new_v4()), "wh", "t"))).await.expect("ow s3");
    let c_path = local_path(&ice.files_with_stats(&t, s3).await.expect("files@s3")[0].path);

    assert!(a_path.exists() && b_path.exists() && c_path.exists(), "all 3 objects on disk");

    age_snapshot(&pool, s2.0).await; // H = s2 (s1, s3 stay recent)

    let summary = gc_table(&catalog, &pool, &t, Duration::from_secs(7 * 24 * 3600))
        .await
        .expect("gc");
    assert_eq!(summary, GcSummary { data_file_rows: 1, inline_rows: 0, objects_deleted: 1 });

    // (a) A's row + object reclaimed; (b) B retained (end=s3 > H); C live.
    assert!(!a_path.exists(), "aged-out object A deleted");
    assert!(b_path.exists(), "in-window object B retained");
    assert!(c_path.exists(), "live object C retained");

    // (c) live read at current (s3) unchanged: file C, 2 rows.
    let cur = ice.current_snapshot(&t).await.expect("current");
    assert_eq!(cur.id, s3);
    let now = ice.files_with_stats(&t, s3).await.expect("files@s3");
    assert_eq!(now.len(), 1);
    assert_eq!(now[0].record_count, 2);

    // (b) structural: B still resolvable via the mirror at s2 (end=s3 > H).
    let at_s2 = ice.files_with_stats(&t, s2).await.expect("files@s2 post-gc");
    assert_eq!(at_s2.len(), 1, "B retained in the mirror");
    assert_eq!(at_s2[0].record_count, 4);
}

/// Inline source: inline_append → flush end-caps the inline rows at the flush
/// snapshot Sf; aging Sf makes them reclaimable. The flushed real file (live)
/// survives and still reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_reclaims_aged_inline_rows() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let ice = IcebergCatalog::new(pool.clone());
    let t = TableRef { schema: "wh".into(), name: "inl".into() };
    let run = RunId(uuid::Uuid::new_v4());

    inline_append(&pool, &t, &columns(), &batch(3), lineage(run, "wh", "inl"), None)
        .await
        .expect("inline_append");
    let sf = flush_table(&catalog, &pool, &t, run).await.expect("flush").expect("flushed");

    age_snapshot(&pool, sf.0).await; // H = Sf; inline rows end-capped @ Sf are reclaimable

    let summary = gc_table(&catalog, &pool, &t, Duration::from_secs(7 * 24 * 3600))
        .await
        .expect("gc");
    assert!(summary.inline_rows >= 1, "end-capped inline rows reclaimed");

    // The flushed real file (live, end=NULL) is untouched: current read still has 3 rows.
    let cur = ice.current_snapshot(&t).await.expect("current");
    let files = ice.files_with_stats(&t, cur.id).await.expect("files");
    let live_rows: i64 = files.iter().map(|f| f.record_count).sum();
    assert_eq!(live_rows, 3, "live flushed data intact after gc");
}

/// No-op: nothing aged out ⇒ horizon undefined ⇒ reclaim nothing, succeed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_is_a_noop_when_nothing_aged_out() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let ice = IcebergCatalog::new(pool.clone());
    let t = TableRef { schema: "wh".into(), name: "fresh".into() };

    let s1 = land(&pool, &catalog, &t, &columns(), &ipc_body(10), 0, i64::MAX,
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "fresh")).await.expect("land");
    let s2 = overwrite_parquet_snapshot(&pool, &catalog, &t, &columns(), vec![batch(4)], None)
        .await.expect("ow");
    let a_path = local_path(&ice.files_with_stats(&t, s1).await.expect("files@s1")[0].path);

    // No age injection: snapshots are fresh, so a 7d horizon reclaims nothing.
    let summary = gc_table(&catalog, &pool, &t, Duration::from_secs(7 * 24 * 3600))
        .await
        .expect("gc");
    assert_eq!(summary, GcSummary::default(), "nothing reclaimed");

    assert!(a_path.exists(), "end-capped-but-in-window object retained");
    assert_eq!(ice.current_snapshot(&t).await.expect("current").id, s2);
}

/// Lock coexistence: gc_table and a concurrent flush_table on the same table take
/// the same advisory key and serialize — neither errors, final state is consistent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gc_serializes_with_concurrent_flush() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog_g = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let catalog_f = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let t = TableRef { schema: "wh".into(), name: "race".into() };
    let run = RunId(uuid::Uuid::new_v4());

    // Seed: a real file end-capped + aged (gc has work), plus live inline rows (flush has work).
    let s1 = land(&pool, &catalog_g, &t, &columns(), &ipc_body(10), 0, i64::MAX,
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "race")).await.expect("land");
    let s2 = overwrite_parquet_snapshot(&pool, &catalog_g, &t, &columns(), vec![batch(4)], None)
        .await.expect("ow");
    age_snapshot(&pool, s2.0).await;
    inline_append(&pool, &t, &columns(), &batch(2), lineage(run, "wh", "race"), None)
        .await.expect("inline_append");
    let _ = s1;

    let pool_f = pool.clone();
    let t_f = t.clone();
    let flush = tokio::spawn(async move { flush_table(&catalog_f, &pool_f, &t_f, run).await });
    let gc = tokio::spawn({
        let pool_g = pool.clone();
        let t_g = t.clone();
        async move {
            gc_table(&catalog_g, &pool_g, &t_g, Duration::from_secs(7 * 24 * 3600)).await
        }
    });
    let (gc_res, flush_res) = (gc.await.expect("gc join"), flush.await.expect("flush join"));
    gc_res.expect("gc ok under contention");
    flush_res.expect("flush ok under contention");

    // Final state is readable and consistent (no half-applied corruption).
    let ice = IcebergCatalog::new(pool.clone());
    let cur = ice.current_snapshot(&t).await.expect("current");
    let _ = ice.files_with_stats(&t, cur.id).await.expect("files readable");
}
```

- [ ] **Step 3: Run the suite (routes to RE)**

```bash
cd /home/user/loom
buck2 test //src/control-plane/postgres:iceberg-gc > /tmp/gc.log 2>&1; echo "EXIT=$?"
grep -iE "Tests finished|Pass|Fail|error\[|panicked" /tmp/gc.log | head -40
```

Expected: `Pass`, 5 tests passed (delete-file + 4 GC tests). Debug any failure against `/tmp/gc.log` before proceeding (e.g. the `inline_rows >= 1` assertion reveals the actual inline end-cap count; adjust the exact-count assertion in the data-file test if the seeding produces extra inline rows — but the overwrite path writes real files, not inline, so `inline_rows` there must be 0).

- [ ] **Step 4: Run the freshness gate + the full postgres suite**

```bash
cd /home/user/loom
buck2 test //src/control-plane/postgres:sqlx-cache-check //src/control-plane/postgres:iceberg-flush \
  > /tmp/pg.log 2>&1; echo "EXIT=$?"; grep -iE "Tests finished|Fail" /tmp/pg.log
```

Expected: both `Pass`. `sqlx-cache-check` green confirms the committed cache matches the live schema.

- [ ] **Step 5: Commit**

```bash
cd /home/user/loom
git add src/control-plane/postgres/tests/iceberg_gc.rs src/control-plane/postgres/BUCK
git commit -m "test(iceberg): gc_table fixture tests (reclaim, in-window, no-op, lock)"
```

**Layer 1 is now a complete, tested, shippable slice.** Verify `buck2 build //src/...` is green before starting Layer 2.

---

# Layer 2 — operator-triggered execution model (engine-wire)

> Each task mirrors the existing flush vertical exactly; the deltas are spelled out in full. If a Layer-2 task proves intractable to land green remotely, stop, keep Layer 1, and record the remainder as a FUTURE follow-on (see Task 10).

## Task 5: core `gc` job contract

**Files:**
- Create: `src/control-plane/core/src/gc.rs`
- Modify: `src/control-plane/core/src/lib.rs` (add `mod gc;` and `pub use gc::{GC_JOB_KIND, GcJob};` next to the flush lines)
- Create: `src/control-plane/core/tests/gc_job.rs`
- Modify: `src/control-plane/core/BUCK` (add a `gc-job` `rust_test` mirroring `flush-job`)

**Interfaces:**
- Produces: `pub const GC_JOB_KIND: &str = "gc_table";` and `pub struct GcJob { pub schema: String, pub name: String }` (derive `serde::Serialize, serde::Deserialize, Debug, Clone`).

- [ ] **Step 1: Write the failing test** — `src/control-plane/core/tests/gc_job.rs`:

```rust
use control_plane_core::{GC_JOB_KIND, GcJob};

#[test]
fn gc_job_serde_roundtrip_and_kind() {
    assert_eq!(GC_JOB_KIND, "gc_table");
    let j = GcJob { schema: "wh".into(), name: "t".into() };
    let v = serde_json::to_value(&j).unwrap();
    assert_eq!(v["schema"], "wh");
    assert_eq!(v["name"], "t");
    let back: GcJob = serde_json::from_value(v).unwrap();
    assert_eq!(back.schema, "wh");
    assert_eq!(back.name, "t");
}
```

- [ ] **Step 2: Add the BUCK target** — in `src/control-plane/core/BUCK`, mirror `flush-job`:

```python
rust_test(
    name = "gc-job",
    crate = "gc_job",
    srcs = ["tests/gc_job.rs"],
    deps = [":core", "//third-party:serde_json"],
)
```

- [ ] **Step 3: Run to verify it fails** — `buck2 build //src/control-plane/core:gc-job 2>&1 | tail`. Expected: unresolved import `GC_JOB_KIND`.

- [ ] **Step 4: Implement** — `src/control-plane/core/src/gc.rs`:

```rust
//! The gc-table job contract, shared by the producer (operator HTTP endpoint)
//! and the consumer (the worker). Lives in core so a zero-pool worker can read
//! it without depending on the postgres adapter.

/// The queue `kind` for a physical-GC job.
pub const GC_JOB_KIND: &str = "gc_table";

/// The payload of a `gc_table` job: which table to GC.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct GcJob {
    pub schema: String,
    pub name: String,
}
```

Then in `src/control-plane/core/src/lib.rs` add `mod gc;` and `pub use gc::{GC_JOB_KIND, GcJob};` next to the `flush` equivalents.

- [ ] **Step 5: Run + commit**

```bash
cd /home/user/loom
buck2 test //src/control-plane/core:gc-job 2>&1 | grep -iE "Tests finished|Fail"
git add src/control-plane/core/src/gc.rs src/control-plane/core/src/lib.rs \
  src/control-plane/core/tests/gc_job.rs src/control-plane/core/BUCK
git commit -m "feat(core): gc_table job contract (GC_JOB_KIND, GcJob)"
```

---

## Task 6: `gc_retention` engine config

**Files:**
- Modify: `src/services/runtime/src/lib.rs` (the `Config` struct ~76–84 and `Config::from_map` ~202–244)
- Test: the runtime crate's existing config test (find it via `grep -rn "from_map" src/services/runtime/tests`); add a case there, or create `tests/config_gc.rs` if no config test exists.

**Interfaces:**
- Produces: `Config { …, pub gc_retention: std::time::Duration }`, parsed from `LOOM_GC_RETENTION_SECS` (unit-suffixed to match `LOOM_LOCK_TIMEOUT_MS`), default `7 * 24 * 3600` seconds.

- [ ] **Step 1: Write/extend the failing test** — assert a default and an override. If `src/services/runtime/tests/` has a config test, add:

```rust
#[test]
fn gc_retention_defaults_to_seven_days_and_parses_override() {
    let mut vars = minimal_valid_vars(); // reuse the test's existing valid-config helper
    assert_eq!(
        service_runtime::Config::from_map(&vars).unwrap().gc_retention,
        std::time::Duration::from_secs(7 * 24 * 3600)
    );
    vars.insert("LOOM_GC_RETENTION_SECS".into(), "60".into());
    assert_eq!(
        service_runtime::Config::from_map(&vars).unwrap().gc_retention,
        std::time::Duration::from_secs(60)
    );
}
```

(If no config test exists, create `src/services/runtime/tests/config_gc.rs` building a full valid `HashMap<String,String>` from the required vars listed in `from_map`, plus a BUCK `rust_test` target mirroring an existing runtime test.)

- [ ] **Step 2: Run to verify it fails** — `buck2 build <the config test target> 2>&1 | tail`. Expected: no field `gc_retention`.

- [ ] **Step 3: Implement** — add the field to `Config`, and in `from_map` (next to `lock_timeout`):

```rust
let gc_retention = match vars.get("LOOM_GC_RETENTION_SECS") {
    Some(s) => Duration::from_secs(
        s.parse::<u64>()
            .map_err(|e| invalid("LOOM_GC_RETENTION_SECS", e.to_string()))?,
    ),
    None => Duration::from_secs(7 * 24 * 3600),
};
```

Add `gc_retention` to the returned `Config { … }`.

- [ ] **Step 4: Run + commit**

```bash
cd /home/user/loom
buck2 test <the config test target> 2>&1 | grep -iE "Tests finished|Fail"
git add src/services/runtime
git commit -m "feat(runtime): LOOM_GC_RETENTION_SECS config (default 7d)"
```

---

## Task 7: engine-wire `GcTable` RPC + client

**Files:**
- Modify: `src/services/engine-wire/proto/engine_control.proto`
- Modify: `src/services/engine-wire/src/client.rs`

**Interfaces:**
- Produces (proto): `rpc GcTable(GcTableRequest) returns (GcTableResponse);`, `message GcTableRequest { string schema = 1; string name = 2; }`, `message GcTableResponse { uint64 data_file_rows = 1; uint64 inline_rows = 2; uint64 objects_deleted = 3; }`.
- Produces (client): `pub async fn gc_table(&self, schema: String, name: String) -> Result<(u64, u64, u64)>` returning `(data_file_rows, inline_rows, objects_deleted)`.

- [ ] **Step 1: Edit the proto** — add the RPC to the `EngineControl` service (after `FlushTable`) and the two messages (after the `FlushTable*` messages):

```protobuf
  rpc GcTable   (GcTableRequest)    returns (GcTableResponse);
```
```protobuf
message GcTableRequest  { string schema = 1; string name = 2; }
message GcTableResponse { uint64 data_file_rows = 1; uint64 inline_rows = 2; uint64 objects_deleted = 3; }
```

- [ ] **Step 2: Add the client method** — in `src/services/engine-wire/src/client.rs`, after `flush_table`:

```rust
pub async fn gc_table(&self, schema: String, name: String) -> Result<(u64, u64, u64)> {
    let resp = self
        .inner
        .clone()
        .gc_table(pb::GcTableRequest { schema, name })
        .await
        .map_err(be)?
        .into_inner();
    Ok((resp.data_file_rows, resp.inline_rows, resp.objects_deleted))
}
```

- [ ] **Step 3: Build (codegen runs via the `pb-gen` genrule)**

```bash
cd /home/user/loom
buck2 build //src/services/engine-wire:engine-wire 2>&1 | tail -15
```

Expected: `BUILD SUCCEEDED`. (The genrule re-runs protoc from the edited `.proto`.)

- [ ] **Step 4: Commit**

```bash
cd /home/user/loom
git add src/services/engine-wire
git commit -m "feat(engine-wire): GcTable RPC + client"
```

---

## Task 8: engine-side `GcTable` handler

**Files:**
- Modify: `src/services/engine/src/service.rs` (the `EngineControlService` struct + its `EngineControl` impl, after `flush_table` ~93–113)
- Modify: `src/services/engine/src/main.rs` (build `EngineControlService` with the new `retention` field)

**Interfaces:**
- Consumes: `control_plane_postgres::iceberg_gc::gc_table`; `cfg.gc_retention` (Task 6).
- Produces: `EngineControlService { cp, catalog, pool, retention: std::time::Duration }` and an `async fn gc_table` RPC impl.

- [ ] **Step 1: Add the field** — in `service.rs`, add `pub retention: std::time::Duration,` to `EngineControlService`. Import `use control_plane_postgres::iceberg_gc::gc_table;` next to the `iceberg_flush::flush_table` import (alias one if names collide: `use control_plane_postgres::iceberg_gc::gc_table as gc_table_primitive;`).

- [ ] **Step 2: Add the RPC impl** — after the `flush_table` handler:

```rust
async fn gc_table(
    &self,
    req: Request<pb::GcTableRequest>,
) -> std::result::Result<Response<pb::GcTableResponse>, Status> {
    let r = req.into_inner();
    let table = TableRef { schema: r.schema, name: r.name };
    let summary = control_plane_postgres::iceberg_gc::gc_table(
        &self.catalog,
        &self.pool,
        &table,
        self.retention,
    )
    .await
    .map_err(status)?;
    Ok(Response::new(pb::GcTableResponse {
        data_file_rows: summary.data_file_rows,
        inline_rows: summary.inline_rows,
        objects_deleted: summary.objects_deleted,
    }))
}
```

- [ ] **Step 3: Wire main.rs** — in `src/services/engine/src/main.rs`, add `retention: cfg.gc_retention,` to the `EngineControlService { … }` constructor.

- [ ] **Step 4: Build + commit**

```bash
cd /home/user/loom
buck2 build //src/services/engine:engine 2>&1 | tail -15
git add src/services/engine
git commit -m "feat(engine): GcTable RPC handler wired to gc_table"
```

---

## Task 9: worker `handle_gc` + multi-kind dispatch

**Files:**
- Modify: `src/services/worker/src/handler.rs` (add `handle_gc`)
- Modify: `src/services/worker/src/main.rs` (subscribe to both kinds; dispatch on `job.kind`)
- Modify: `src/services/worker/tests/e2e.rs` (extend with a GC case — inspect the file first to match its harness shape)

**Interfaces:**
- Consumes: `engine_wire::GrpcQueueClient::gc_table`; `control_plane_core::{GC_JOB_KIND, GcJob, FLUSH_JOB_KIND, FlushJob}`.
- Produces: `pub async fn handle_gc(engine: GrpcQueueClient, job: Job) -> Result<(), JobFailure>`.

- [ ] **Step 1: Add `handle_gc`** — mirror `handle_flush` in `handler.rs`:

```rust
pub async fn handle_gc(engine: GrpcQueueClient, job: Job) -> std::result::Result<(), JobFailure> {
    let gc: GcJob = serde_json::from_value(job.payload)
        .map_err(|e| JobFailure { error: e.to_string(), policy: RetryPolicy::Abandon })?;
    engine
        .gc_table(gc.schema, gc.name)
        .await
        .map_err(|e| JobFailure {
            error: e.to_string(),
            policy: RetryPolicy::Retry { delay: backoff(job.attempts) },
        })?;
    Ok(())
}
```

(Match the exact `JobFailure`/`RetryPolicy` construction the existing `handle_flush` uses — copy its error arms verbatim, swapping `FlushJob`→`GcJob` and `flush_table`→`gc_table`.)

- [ ] **Step 2: Dispatch on kind in main.rs** — change the worker subscription and handler closure:

```rust
worker
    .run(
        &[FLUSH_JOB_KIND.to_string(), GC_JOB_KIND.to_string()],
        shutdown,
        move |job| {
            let engine = engine.clone();
            async move {
                match job.kind.as_str() {
                    GC_JOB_KIND => handle_gc(engine, job).await,
                    _ => handle_flush(engine, job).await,
                }
            }
        },
    )
    .await
```

(Adapt to the closure's exact signature in the current `main.rs` — keep its clone/move structure; only add the `GC_JOB_KIND` kind and the `match`.)

- [ ] **Step 3: Extend the worker e2e** — read `src/services/worker/tests/e2e.rs`, then add a test that enqueues a `gc_table` job (an aged end-capped table seeded like Task 4) and asserts the worker drives it to completion through the real engine RPC. Reuse the file's existing engine+worker boot harness.

- [ ] **Step 4: Build + test + commit**

```bash
cd /home/user/loom
buck2 build //src/services/worker:worker 2>&1 | tail -10
buck2 test //src/services/worker:e2e > /tmp/we2e.log 2>&1; echo "EXIT=$?"; grep -iE "Tests finished|Fail" /tmp/we2e.log
git add src/services/worker
git commit -m "feat(worker): handle_gc + gc_table/flush_table dispatch"
```

---

## Task 10: operator HTTP endpoint (query-api) + register close-out

**Files:**
- Modify: `src/services/query-api/src/http.rs` (add a maintenance route + handler; `AppState` already holds `cp: Arc<dyn ControlPlane>`)
- Test: `src/services/query-api/tests/` (e2e, via `e2e_support`)
- Modify: `docs/ROADMAP.md`, `docs/FUTURE.md` (via `loom-docs-update`)

**Interfaces:**
- Consumes: `AppState.cp` (implements `control_plane_core::Queue::enqueue`); `control_plane_core::{NewJob, GC_JOB_KIND, GcJob}`.
- Produces: `POST /maintenance/gc/:schema/:table` → enqueues a `gc_table` `NewJob`, returns `202 Accepted` with the job id.

- [ ] **Step 1: Add the route + handler** — in `query-api/src/http.rs`:

```rust
// in router():
.route("/maintenance/gc/:schema/:table", post(enqueue_gc))
```
```rust
async fn enqueue_gc(
    State(st): State<AppState>,
    Path((schema, table)): Path<(String, String)>,
) -> impl IntoResponse {
    let job = NewJob {
        kind: GC_JOB_KIND.to_string(),
        payload: serde_json::json!({ "schema": schema, "name": table }),
        run_at: None,
        priority: 0,
    };
    match st.cp.enqueue(job).await {
        Ok(id) => (StatusCode::ACCEPTED, Json(serde_json::json!({ "job_id": id.0.to_string() }))).into_response(),
        Err(e) => internal_error("enqueue gc_table", e),
    }
}
```

(Confirm how `AppState.cp` exposes `enqueue` — `Arc<dyn ControlPlane>` may surface the queue via a `.queue()` accessor or implement `Queue` directly; match the call other handlers use. `JobId`'s inner field name comes from `core::queue` — use whatever `flush`'s enqueue site uses to stringify it.)

- [ ] **Step 2: Add an e2e test** — in `query-api/tests/`, `POST /maintenance/gc/wh/t` and assert `202` + a `job_id` body, and that a `gc_table` row lands in `queue.jobs`. Reuse `e2e_support` helpers.

- [ ] **Step 3: Build + test + commit**

```bash
cd /home/user/loom
buck2 build //src/services/query-api:query-api 2>&1 | tail -10
buck2 test //src/services/query-api/... > /tmp/qa.log 2>&1; echo "EXIT=$?"; grep -iE "Tests finished|Fail" /tmp/qa.log
git add src/services/query-api
git commit -m "feat(query-api): operator POST /maintenance/gc/:schema/:table"
```

- [ ] **Step 4: Close the register** — run `loom-docs-update`: flip `road-iceberg-gc` to `- [x]` / `status:done` with `pr:#N`; mark `fut-iceberg-gc` `status:promoted`; add a FUTURE item for the deferred orphaned-Parquet sweep (the spec's "Out of scope"), and for dropped-table GC. Stage the register edits in the PR.

---

## Self-Review

- **Spec coverage:** retention model → snapshot_time reuse (Global Constraints + Task 2); horizon resolution → Task 2 step 1; safety invariant → Task 2 doc-comment + Task 4 tests; reclaim primitive steps 1–5 → Task 2; commit-then-delete → Task 2; delete seam → Task 1; execution model (job kind, worker, engine RPC, operator endpoint, retention config, no Arrow Flight) → Tasks 5–10; all four spec tests (happy/in-window/no-op/lock) → Task 4; `.sqlx` refresh → Task 3; "out of scope" items recorded → Task 10 step 4. **Deliberate deviation:** migration 0017 dropped (snapshot_time already exists) — recorded in Global Constraints.
- **Placeholder scan:** none — every code step shows real code; Layer-2 "match the existing X" notes point at named, located patterns, not TODOs.
- **Type consistency:** `gc_table(catalog, pool, table, retention: Duration) -> Result<GcSummary>` used identically in Tasks 2, 4, 8; `GcSummary` fields `data_file_rows/inline_rows/objects_deleted` consistent across Tasks 2, 4, 7, 8; `GcJob{schema,name}` / `GC_JOB_KIND` consistent across Tasks 5, 9, 10; client `gc_table -> (u64,u64,u64)` consistent Tasks 7, 8.
