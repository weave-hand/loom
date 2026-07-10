# Refuse stream-table targets on legacy transform write paths — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close a latent stream-framing corruption class by refusing (not threading) stream/CDC-registered target tables on the two framing-unaware legacy write paths — `iceberg_landing::write_steps` (multi-step actions) and `IcebergTx::commit` (transforms) — plus a define-time UX guard, and surface the refusal as a clean client error (HTTP 422 for actions; worker *abandon* + Failed-terminal run for transforms) instead of an opaque 500/retry-storm.

**Architecture:** One shared postgres helper `pg_refuse_stream_target` performs the registry lookup (refusing a `table_id` OR `changelog_table_id` hit). It is called inside the two commit transactions and once at define time. The refusal is a `ControlPlaneError::Validation`; a small set of error-mapping fixes carries that `Validation` class faithfully across two wire hops so it renders as HTTP 422 (action path) or a deterministic worker *abandon* (transform path).

**Tech Stack:** Rust, buck2, sqlx (runtime `AssertSqlSafe` — no `.sqlx` regen), tonic/gRPC, Arrow Flight, DataFusion, axum, hermetic Postgres fixtures (`loom_fixture_test`).

## Global Constraints

- **Spec:** `docs/superpowers/specs/2026-07-09-stream-framing-refuse-design.md`. This plan implements it, with two corrections noted below.
- **Tests are `rust_test` integration targets only** — never inline `#[cfg(test)] mod tests`. Fixture (Postgres-touching) tests MUST use the **`loom_fixture_test`** macro, never a bare `rust_test`, or they run without the fixture env and fail to boot.
- **Build:** `buck2 build -v0 --console none //src/...` (silent on success). **Test:** `buck2 test --console none //src/...` (prints only the `Tests finished: Pass N. Fail 0` summary). On this root/cloud host, test *runs* go to RE automatically via the buck2 shim.
- **Clippy is strict** on production lib/bin code (`unwrap_used`, `expect_used`, `indexing_slicing`, `panic`, `todo`, … all enforced). Test code is exempt from the panic-safety lints via `loom_fixture_test`. Do not introduce `.unwrap()`/`.expect()`/`panic!`/indexing in non-test code.
- **New SQL uses runtime `sqlx::AssertSqlSafe`, NOT the compile-time `query!` macro.** `.sqlx` regen (`tools/sqlx-prepare.sh`) needs `initdb`, which refuses to run as root in cloud sessions. Mirror the `ontology::version_for_table` pattern.
- **Stable error contract:** the refusal message MUST begin with the exact prefix `stream-table target refused:` (matchable, like `schema evolution unsupported:`). Do not change this string.
- **SPEC CORRECTION 1 (verified in code):** the spec's error contract claims the engine's `invalid_argument` round-trips to `ControlPlaneError::Validation` because "the engine-wire client already maps `InvalidArgument → Validation` (client.rs:59)". That mapping is `sql_status`, used ONLY by the Flight **SQL** plane. The transform path uses `GrpcQueueClient::commit_transform`, which maps via **`cp_status`** (`client.rs:453`), and `cp_status` maps `InvalidArgument → Backend`. So this plan **adds `Code::InvalidArgument => ControlPlaneError::Validation` to `cp_status`** (Task 5) to make the round-trip real.
- **SPEC CORRECTION 2 (verified in code):** the action path uses `GrpcQueueClient::write_steps` (`client.rs:241`), which maps EVERY error via **`be`** (`.map_err(be)`, flatten to `Backend`). So this plan **switches that one call from `.map_err(be)` to `.map_err(cp_status)`** (Task 6) so the `Validation` class survives to `to_serving_write`.

---

## File map

| File | Change | Task |
|---|---|---|
| `src/control-plane/postgres/src/stream.rs` | **new** `pg_refuse_stream_target` helper | 1 |
| `src/control-plane/postgres/tests/refuse_stream_target.rs` | **new** direct helper fixture test | 1 |
| `src/control-plane/postgres/BUCK` | wire new test target(s) | 1,2,3,4 |
| `src/control-plane/postgres/src/iceberg_landing.rs` | guard in `write_steps` (Site 1) | 2 |
| `src/control-plane/postgres/tests/write_steps_refuse.rs` | **new** Site-1 fixture test | 2 |
| `src/control-plane/postgres/src/iceberg_control_plane.rs` | guard in `IcebergTx::commit` (Site 2) | 3 |
| `src/control-plane/postgres/tests/iceberg_tx_refuse.rs` | **new** Site-2 fixture test | 3 |
| `src/control-plane/postgres/src/transforms.rs` | define-time guard in `define_transform` | 4 |
| `src/control-plane/postgres/tests/define_transform_refuse.rs` | **new** define-time fixture test | 4 |
| `src/services/engine/src/service.rs` | `status()` gains `Validation` arm; `write_steps` handler narrow match | 5,6 |
| `src/services/engine-wire/src/client.rs` | `cp_status` gains `InvalidArgument` arm; `write_steps` `be`→`cp_status` | 5,6 |
| `src/services/worker/src/transform.rs` | `commit_transform` abandon-on-`Validation` match | 5 |
| `src/services/worker/tests/transform_e2e.rs` | **new** commit-time refusal e2e | 5 |
| `src/services/engine-serving/src/serving.rs` | new `EngineServingError::Validation` variant | 6 |
| `src/services/engine/src/flight.rs` | `serving_status` gains `Validation` arm | 6 |
| `src/services/engine-serving/src/action_writer.rs` | `write_steps` constructs `Validation` | 6 |
| `src/services/query-api/src/engine_action_client.rs` | `to_serving_write` gains `Validation` arm | 6 |
| `src/services/query-api/src/action.rs` | `run_multi_step` maps `Unsupported`→`ActionError::Unsupported` | 6 |
| `src/services/query-api/tests/*` | **new** HTTP-422 multi-step e2e | 6 |

---

## Task 1: The shared refusal helper `pg_refuse_stream_target`

**Files:**
- Modify: `src/control-plane/postgres/src/stream.rs` (add fn near `pg_stream_bucket_count`, ~line 342)
- Create: `src/control-plane/postgres/tests/refuse_stream_target.rs`
- Modify: `src/control-plane/postgres/BUCK` (new `loom_fixture_test` target)

**Interfaces:**
- Consumes: `crate::iceberg_mirror::live_table_id(conn: &mut PgConnection, ns: &str, name: &str) -> Result<Option<i64>>`; `backend` (in scope via `use crate::{PgControlPlane, backend};`); `ControlPlaneError`, `Result`, `TableRef` (already imported in `stream.rs`); the `stream.stream_table` columns `table_id` (PK) and `changelog_table_id` (nullable bigint).
- Produces (later tasks rely on this EXACT signature):
  ```rust
  pub(crate) async fn pg_refuse_stream_target(
      conn: &mut sqlx::PgConnection,
      table: &TableRef,
  ) -> Result<()>
  ```
  Returns `Ok(())` when `table` has no live mirror row OR is not stream-registered; returns `Err(ControlPlaneError::Validation(msg))` with `msg` starting `stream-table target refused:` when the table's live `table_id` appears as either `table_id` or `changelog_table_id` in `stream.stream_table`.

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/postgres/tests/refuse_stream_target.rs`:

```rust
//! Direct unit coverage for `pg_refuse_stream_target`: a declared log table and a
//! declared CDC table (both the base row and its changelog table) are refused; a
//! plain batch table and a table with no live mirror row pass.

use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_mirror::{ensure_table, next_snapshot};
use control_plane_postgres::stream::pg_refuse_stream_target;
use control_plane_core::{ControlPlaneError, MergeEngine, StreamTables, TableRef};

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef { schema: schema.into(), name: name.into() }
}

/// Create a live mirror row for `table` and return its `table_id`.
async fn ensure(pool: &sqlx::PgPool, table: &TableRef) -> i64 {
    let mut tx = pool.begin().await.expect("begin");
    let at = next_snapshot(&mut tx, None).await.expect("next_snapshot");
    let tid = ensure_table(&mut tx, &table.schema, &table.name, at)
        .await
        .expect("ensure_table");
    tx.commit().await.expect("commit");
    tid
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refuses_declared_streams_passes_batch_and_absent() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    // (a) absent: no live mirror row at all → passes.
    let absent = tref("s", "absent");
    let mut c = pool.acquire().await.expect("acquire");
    pg_refuse_stream_target(&mut c, &absent).await.expect("absent passes");

    // (b) batch: has a live mirror row, not stream-declared → passes.
    let batch = tref("s", "batch");
    ensure(&pool, &batch).await;
    pg_refuse_stream_target(&mut c, &batch).await.expect("batch passes");

    // (c) log stream: declared log table → refused.
    let logt = tref("s", "logt");
    let log_tid = ensure(&pool, &logt).await;
    cp.declare_stream(log_tid, 4).await.expect("declare_stream");
    let e = pg_refuse_stream_target(&mut c, &logt).await.expect_err("log refused");
    assert!(matches!(e, ControlPlaneError::Validation(_)), "got {e:?}");
    assert!(
        e.to_string().contains("stream-table target refused:"),
        "message prefix, got: {e}"
    );

    // (d) CDC base: declared CDC table → refused (matched on `table_id`).
    let cdc = tref("s", "cdc");
    let cdc_tid = ensure(&pool, &cdc).await;
    cp.declare_cdc(cdc_tid, 1, "id", MergeEngine::LastRow).await.expect("declare_cdc");
    let e = pg_refuse_stream_target(&mut c, &cdc).await.expect_err("cdc refused");
    assert!(matches!(e, ControlPlaneError::Validation(_)), "got {e:?}");

    // (e) CDC changelog: a separate table pointed at by the base row's
    // `changelog_table_id` → refused even though it has no `stream_table` row of its own.
    let clog = tref("s", "cdc__changelog");
    let clog_tid = ensure(&pool, &clog).await;
    // Wire the base CDC row to point at the changelog's mirror id, via the public
    // StreamTables trait method (no need to widen any crate-private helper).
    cp.set_changelog_table_id(cdc_tid, clog_tid).await.expect("set changelog id");
    let e = pg_refuse_stream_target(&mut c, &clog).await.expect_err("changelog refused");
    assert!(matches!(e, ControlPlaneError::Validation(_)), "got {e:?}");
}
```

> Note: `ensure_table`, `next_snapshot`, and `pg_refuse_stream_target` must be reachable from tests. `ensure_table`/`next_snapshot` are already `pub`. `pg_refuse_stream_target` is made `pub` in Step 4. `set_changelog_table_id`/`declare_stream`/`declare_cdc` are public `StreamTables` trait methods on `cp`.

- [ ] **Step 2: Add the test target to `src/control-plane/postgres/BUCK`**

Mirror the `stream-cdc-emission` target (no warehouse/flush needed). Add after it:

```python
loom_fixture_test(
    name = "refuse-stream-target",
    crate = "refuse_stream_target",
    srcs = ["tests/refuse_stream_target.rs"],
    crate_root = "tests/refuse_stream_target.rs",
    deps = [
        ":postgres",
        "//src/control-plane/core:core",
        "//third-party:sqlx",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 3: Run the test to verify it fails to compile**

Run: `buck2 test --console none //src/control-plane/postgres:refuse-stream-target`
Expected: FAIL — `pg_refuse_stream_target` is unresolved (`no function ... in module stream`).

- [ ] **Step 4: Implement `pg_refuse_stream_target` and export it**

In `src/control-plane/postgres/src/stream.rs`, add after `pg_stream_bucket_count` (after ~line 342):

```rust
/// Refuse `table` if it is a declared stream/CDC target that the caller's write
/// path cannot frame. Source of truth is the `stream.stream_table` registry: the
/// resolved live `table_id` is refused when it appears as EITHER `table_id` (a
/// declared log/CDC base) OR `changelog_table_id` (a CDC table's durable changelog,
/// which has no `stream_table` row of its own). A table with no live mirror row
/// passes — a brand-new output cannot be stream-declared. Returns
/// `ControlPlaneError::Validation` with the stable prefix `stream-table target
/// refused:`. `AssertSqlSafe`: static query, sqlx regen unavailable in-env (initdb
/// as root); convert to `query!` when regenerating locally.
pub async fn pg_refuse_stream_target(
    conn: &mut sqlx::PgConnection,
    table: &TableRef,
) -> Result<()> {
    let Some(tid) =
        crate::iceberg_mirror::live_table_id(&mut *conn, &table.schema, &table.name).await?
    else {
        return Ok(());
    };
    let hit: Option<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(
        "select table_id from stream.stream_table \
         where table_id = $1 or changelog_table_id = $1 limit 1",
    ))
    .bind(tid)
    .fetch_optional(&mut *conn)
    .await
    .map_err(backend)?;
    if hit.is_some() {
        return Err(ControlPlaneError::Validation(format!(
            "stream-table target refused: {}.{} is a declared stream/CDC table; \
             transform and multi-target write paths cannot stamp stream framing",
            table.schema, table.name
        )));
    }
    Ok(())
}
```

> This makes the helper `pub` (the spec names it `pub(crate)`, but the direct test in Step 1 lives in a separate integration crate and needs `pub`; the two production call sites are in-crate so `pub` is a superset).

- [ ] **Step 5: Run the test to verify it passes**

Run: `buck2 test --console none //src/control-plane/postgres:refuse-stream-target`
Expected: PASS (`Tests finished: Pass 1. Fail 0`).

- [ ] **Step 6: Verify clippy is clean on the postgres crate**

Run: `buck2 build --console none '//src/control-plane/postgres:postgres[clippy.txt]'`
Expected: exit 0, empty clippy output.

- [ ] **Step 7: Commit**

```bash
git add src/control-plane/postgres/src/stream.rs \
        src/control-plane/postgres/tests/refuse_stream_target.rs \
        src/control-plane/postgres/BUCK
git commit -m "feat(stream): pg_refuse_stream_target registry-refusal helper"
```

---

## Task 2: Guard Site 1 — `iceberg_landing::write_steps`

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_landing.rs` (`write_steps`, Phase-2 tx, ~line 573)
- Create: `src/control-plane/postgres/tests/write_steps_refuse.rs`
- Modify: `src/control-plane/postgres/BUCK`

**Interfaces:**
- Consumes: `crate::stream::pg_refuse_stream_target` (Task 1); the existing `write_steps(pool, catalog, steps: Vec<StepLand>, lineage, jobs) -> Result<SnapshotId>`; `StepLand { table, columns, batches, overwrite }`.
- Produces: no new symbols; `write_steps` now returns `Err(Validation)` (prefix `stream-table target refused:`) and commits nothing if ANY staged target is stream-registered.

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/postgres/tests/write_steps_refuse.rs`:

```rust
//! Site 1: `iceberg_landing::write_steps` refuses when any staged target is a
//! declared stream table, and commits nothing (a co-staged batch target's rows
//! are absent). A pure-batch multi-target write is byte-identical to before.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    Catalog, ColumnSpec, ControlPlaneError, EventType, LineageEvent, MergeEngine, PageReq, RunId,
    StreamTables, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_landing::{write_steps, StepLand};
use control_plane_postgres::iceberg_mirror::{ensure_table, next_snapshot};
use loom_test_seed::local_sql_catalog;

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef { schema: schema.into(), name: name.into() }
}
fn cols() -> Vec<ColumnSpec> {
    vec![ColumnSpec { name: "id".into(), ty: "long".into(), nullable: false }]
}
fn batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64, 2]))]).expect("batch")
}
// LineageEvent has no Default; mirror stream_overwrite_framing.rs's `lin()`.
fn lin(run: RunId) -> LineageEvent {
    LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![],
        payload: serde_json::json!({ "source": "test" }),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_steps_refuses_stream_target_and_commits_nothing() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;

    let plain = tref("s", "plain");
    let streamt = tref("s", "stream_out");

    // Declare `stream_out` a log table BEFORE the write.
    let mut tx = pool.begin().await.expect("begin");
    let at = next_snapshot(&mut tx, None).await.expect("snap");
    let tid = ensure_table(&mut tx, &streamt.schema, &streamt.name, at).await.expect("ensure");
    tx.commit().await.expect("commit");
    cp.declare_stream(tid, 4).await.expect("declare_stream");

    // A two-target write: one batch target + the stream target → must refuse.
    let steps = vec![
        StepLand { table: plain.clone(), columns: cols(), batches: vec![batch()], overwrite: false },
        StepLand { table: streamt.clone(), columns: cols(), batches: vec![batch()], overwrite: false },
    ];
    let err = write_steps(&pool, &catalog, steps, lin(RunId(uuid::Uuid::new_v4())), &[])
        .await
        .expect_err("stream target must be refused");
    assert!(matches!(err, ControlPlaneError::Validation(_)), "got {err:?}");
    assert!(err.to_string().contains("stream-table target refused:"), "msg: {err}");

    // Nothing committed: the co-staged batch target was never registered, so it has
    // no live mirror snapshot at all (the whole multi-target tx rolled back).
    let ice = IcebergCatalog::new(pool.clone());
    assert!(
        matches!(ice.current_snapshot(&plain).await, Err(ControlPlaneError::NotFound(_))),
        "batch target must have no live snapshot after a refused multi-target commit"
    );
}
```

> Field shapes are pinned above (`ColumnSpec.ty`, the explicit `LineageEvent`). Read-back uses `IcebergCatalog::new(pool.clone())` (model: `stream_overwrite_framing.rs:128`); the `Catalog` trait has no `list_files` — a never-registered table has no `current_snapshot` (returns `NotFound`). `PageReq` is imported for the Task-3 variant that reads `.files(...).items`.

- [ ] **Step 2: Add the BUCK target**

Mirror `stream-overwrite-framing`'s deps (it flushes to a warehouse, so it has `//src/testing:seed` + `tempfile`):

```python
loom_fixture_test(
    name = "write-steps-refuse",
    crate = "write_steps_refuse",
    srcs = ["tests/write_steps_refuse.rs"],
    crate_root = "tests/write_steps_refuse.rs",
    deps = [
        "//src/testing:seed",
        ":postgres",
        "//src/control-plane/core:core",
        "//third-party:arrow-array",
        "//third-party:arrow-schema",
        "//third-party:serde_json",
        "//third-party:tempfile",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```

- [ ] **Step 3: Run to verify it fails**

Run: `buck2 test --console none //src/control-plane/postgres:write-steps-refuse`
Expected: FAIL — the write currently succeeds (stream target committed silently), so `expect_err` panics.

- [ ] **Step 4: Add the guard**

In `src/control-plane/postgres/src/iceberg_landing.rs`, in `write_steps`, immediately after `let at = next_snapshot(&mut tx, None).await?;` (~line 573) and BEFORE the `for s in &staged` register loop, insert a refusal pass over the distinct staged targets:

```rust
    // Refuse any stream/CDC-registered target before registering files: the
    // multi-target path is framing-unaware, so a stream target would corrupt the
    // stream (missing/garbage framing). In-tx placement makes the refusal atomic —
    // if any step targets a stream table, the whole commit rolls back (nothing
    // lands), honoring the all-or-nothing contract multi-step actions promise.
    let mut refuse: Vec<&TableRef> = staged.iter().map(|s| &s.table).collect();
    refuse.sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));
    refuse.dedup();
    for table in refuse {
        crate::stream::pg_refuse_stream_target(&mut *tx, table).await?;
    }
```

> `staged` is the `Vec<Staged>` built in Phase 1; `Staged.table` is a `TableRef`. `&mut *tx` yields the `&mut PgConnection` the helper wants (`Transaction` derefs to `PgConnection`). The `?` propagates `Validation`; the `tx` is dropped un-committed → rollback.

- [ ] **Step 5: Run to verify it passes**

Run: `buck2 test --console none //src/control-plane/postgres:write-steps-refuse`
Expected: PASS.

- [ ] **Step 6: Run the non-regression suite for this crate's write paths**

Run: `buck2 test --console none //src/control-plane/postgres:write-steps-refuse //src/control-plane/postgres:stream-overwrite-framing`
Expected: PASS both (batch paths untouched).

- [ ] **Step 7: Clippy + commit**

```bash
buck2 build --console none '//src/control-plane/postgres:postgres[clippy.txt]'
git add src/control-plane/postgres/src/iceberg_landing.rs \
        src/control-plane/postgres/tests/write_steps_refuse.rs \
        src/control-plane/postgres/BUCK
git commit -m "feat(action): refuse stream targets in iceberg_landing::write_steps (Site 1)"
```

---

## Task 3: Guard Site 2 — `IcebergTx::commit`

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_control_plane.rs` (`IcebergTx::commit`, ~line 141)
- Modify: `src/control-plane/postgres/src/iceberg_landing.rs` (make `write_object_data_files` `pub`)
- Create: `src/control-plane/postgres/tests/iceberg_tx_refuse.rs`
- Modify: `src/control-plane/postgres/BUCK`

**Interfaces:**
- Consumes: `crate::stream::pg_refuse_stream_target` (Task 1); the `IcebergControlPlane`/`IcebergTx` staging API — `begin_table()`, `tx.create_table(&table, &columns)`, `tx.append_files(&table, &files)`, `tx.commit()`; `IcebergTx.staged_files: Vec<(TableRef, Vec<DataFile>, WriteMode)>`; `iceberg_landing::write_object_data_files` (made `pub` in this task).
- Produces: no new symbols; `IcebergTx::commit` returns `Err(Validation)` and rolls back if any distinct staged-files table is stream-registered. `staged_compacts` are NOT guarded.

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/postgres/tests/iceberg_tx_refuse.rs`. This drives the staging seam directly (the same seam the engine's `CommitTransform` handler uses):

```rust
//! Site 2: `IcebergTx::commit` refuses when a staged-files target is a declared
//! stream table, rolling back (no snapshot, no files). A batch target commits as
//! before.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    Catalog, ColumnSpec, ControlPlaneError, DataFile, EventType, LineageEvent, PageReq, RunId,
    StreamTables, TableRef, Tx,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_control_plane::IcebergControlPlane;
use control_plane_postgres::iceberg_mirror::{ensure_table, next_snapshot};
use loom_test_seed::local_sql_catalog;

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef { schema: schema.into(), name: name.into() }
}
fn cols() -> Vec<ColumnSpec> {
    vec![ColumnSpec { name: "id".into(), ty: "long".into(), nullable: false }]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn iceberg_tx_commit_refuses_stream_target() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = Arc::new(local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await);
    let pool = fx.pool_for(&db).await;

    let streamt = tref("s", "tx_stream_out");

    // Declare the output a log stream BEFORE the transform commit.
    let mut tx = pool.begin().await.expect("begin");
    let at = next_snapshot(&mut tx, None).await.expect("snap");
    let tid = ensure_table(&mut tx, &streamt.schema, &streamt.name, at).await.expect("ensure");
    tx.commit().await.expect("commit");
    cp.declare_stream(tid, 4).await.expect("declare_stream");

    // Build a DataFile the staging seam can register. Write one real Parquet file
    // to the warehouse via the same helper the landing path uses, OR (simpler)
    // reuse the write path to produce a DataFile. See note below.
    let batch = {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64]))]).expect("b")
    };
    let files: Vec<DataFile> =
        control_plane_postgres::iceberg_landing::write_object_data_files(
            &catalog, &streamt, &cols(), vec![batch],
        )
        .await
        .expect("write files");

    // Stage create + append against the stream target, then commit → refused.
    // `PgControlPlane` is `Clone`; the engine builds this exactly as below (service.rs:301).
    let icp = IcebergControlPlane::new(cp.clone(), catalog.clone());
    let mut txn = icp.begin_table().await.expect("begin_table");
    txn.create_table(&streamt, &cols()).await.expect("create");
    txn.append_files(&streamt, &files).await.expect("stage append");
    let lineage = LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![],
        payload: serde_json::json!({ "source": "test" }),
    };
    txn.emit(lineage).await.expect("emit");
    let err = txn.commit().await.expect_err("stream target must be refused");
    assert!(matches!(err, ControlPlaneError::Validation(_)), "got {err:?}");
    assert!(err.to_string().contains("stream-table target refused:"), "msg: {err}");

    // Rolled back: the stream table HAS a live snapshot (from declare_stream) but no
    // data files were registered.
    let ice = IcebergCatalog::new(pool.clone());
    let snap = ice.current_snapshot(&streamt).await.expect("snap");
    let live = ice.files(&streamt, snap.id, PageReq::unbounded()).await.expect("files");
    assert!(live.items.is_empty(), "no files after refused commit, got {}", live.items.len());
}
```

> Shapes confirmed against the code:
> 1. **`IcebergControlPlane::new(pg: PgControlPlane, catalog: impl Into<Arc<SqlCatalog>>)`** (`iceberg_control_plane.rs:46`). `PgControlPlane` derives `Clone` (`lib.rs:52`), so `cp.clone()` is correct (the engine does the same at `service.rs:301`).
> 2. **`write_object_data_files` is private** — Task 3 Step 4 makes it `pub` (see file map / git add). Signature: `write_object_data_files(catalog: &SqlCatalog, table: &TableRef, columns: &[ColumnSpec], batches: Vec<RecordBatch>) -> Result<Vec<DataFile>>`.
> 3. Read-back: the `Catalog` trait has no `list_files`; use `IcebergCatalog::current_snapshot(&table)` → `Snapshot` (`.id: SnapshotId`), then `files(&table, snap.id, PageReq::unbounded()) -> Page<FileRef>` and assert `.items.is_empty()`.
> 4. `Tx` is the trait bringing `create_table`/`append_files`/`emit`/`commit` into scope (imported above).

- [ ] **Step 2: Add the BUCK target** (same dep set as `write-steps-refuse`, plus `//third-party:arrow-array`/`arrow-schema` already present):

```python
loom_fixture_test(
    name = "iceberg-tx-refuse",
    crate = "iceberg_tx_refuse",
    srcs = ["tests/iceberg_tx_refuse.rs"],
    crate_root = "tests/iceberg_tx_refuse.rs",
    deps = [
        "//src/testing:seed",
        ":postgres",
        "//src/control-plane/core:core",
        "//third-party:arrow-array",
        "//third-party:arrow-schema",
        "//third-party:serde_json",
        "//third-party:tempfile",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```

- [ ] **Step 2b: Make `write_object_data_files` public**

In `src/control-plane/postgres/src/iceberg_landing.rs`, change `async fn write_object_data_files` (~line 454) to `pub async fn write_object_data_files` so the integration test can build `DataFile`s. It is a pure Parquet-writer helper — widening visibility is safe.

- [ ] **Step 3: Run to verify it fails**

Run: `buck2 test --console none //src/control-plane/postgres:iceberg-tx-refuse`
Expected: FAIL — commit currently succeeds against the stream target.

- [ ] **Step 4: Add the guard**

In `src/control-plane/postgres/src/iceberg_control_plane.rs`, inside `IcebergTx::commit`, after the destructure (~line 126) and the empty-check (line 128-137), and BEFORE the `ensure_iceberg_table` create loop (line 141), add a refusal pass over the distinct `staged_files` tables:

```rust
    // Refuse stream/CDC-registered targets before any create/register: the
    // transform-commit seam is framing-unaware. `staged_compacts` are NOT guarded —
    // compaction is schema-invariant (columns `&[]`, files keep their framing) and
    // the stream consolidate path rides overwrite, not this seam. Placed before
    // `ensure_iceberg_table` so a refused commit creates no bare Iceberg table.
    let mut refuse: Vec<&TableRef> = staged_files.iter().map(|(t, _, _)| t).collect();
    refuse.sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));
    refuse.dedup();
    for table in refuse {
        crate::stream::pg_refuse_stream_target(&mut *tx, table).await?;
    }
```

> `tx` here is the owned `Transaction<'static, Postgres>` moved out by the destructure; `&mut *tx` is the `&mut PgConnection`. `staged_files` items are `(TableRef, Vec<DataFile>, WriteMode)`.

- [ ] **Step 5: Run to verify it passes**

Run: `buck2 test --console none //src/control-plane/postgres:iceberg-tx-refuse`
Expected: PASS.

- [ ] **Step 6: Clippy + commit**

```bash
buck2 build --console none '//src/control-plane/postgres:postgres[clippy.txt]'
git add src/control-plane/postgres/src/iceberg_control_plane.rs \
        src/control-plane/postgres/src/iceberg_landing.rs \
        src/control-plane/postgres/tests/iceberg_tx_refuse.rs \
        src/control-plane/postgres/BUCK
git commit -m "feat(transform): refuse stream targets in IcebergTx::commit (Site 2)"
```

---

## Task 4: Define-time UX guard in `define_transform`

**Files:**
- Modify: `src/control-plane/postgres/src/transforms.rs` (`define_transform`, before the insert ~line 359)
- Create: `src/control-plane/postgres/tests/define_transform_refuse.rs`
- Modify: `src/control-plane/postgres/BUCK`

**Interfaces:**
- Consumes: `crate::stream::pg_refuse_stream_target` (Task 1); `pg_type_tables(&mut *tx, &[(TransformName, TransformBody)]) -> Result<HashMap<String, TableRef>>` (same module); `TransformBody::{Physical{output,..}, Typed{output,..}}`. `TableRef` is already in scope in this module.
- Produces: no new symbols; `define_transform` returns `Err(Validation)` (prefix `stream-table target refused:`) when the def's resolved output table is stream-registered.

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/postgres/tests/define_transform_refuse.rs`:

```rust
//! Define-time UX guard: `define_transform` refuses a Physical def whose output
//! names a declared stream table, and a Typed def whose output type binds to one.
//! A batch output, or an output table that does not exist yet, defines cleanly.

use control_plane_core::{
    ControlPlane, ControlPlaneError, ObjectType, Ontology, OutputMode, StreamTables, TableRef,
    TransformBody, TransformDef, TransformName, Transforms,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_mirror::{ensure_table, next_snapshot};

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef { schema: schema.into(), name: name.into() }
}

fn physical_def(name: &str, input: TableRef, output: TableRef) -> TransformDef {
    TransformDef {
        name: TransformName(name.into()),
        body: TransformBody::Physical {
            inputs: vec![input],
            output,
            sql: "select * from src".into(),
            output_mode: OutputMode::Append,
        },
        schedule: None,
        on_input_commit: false,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn define_transform_refuses_stream_output() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    // (a) output does not exist yet → defines cleanly (nothing to refuse).
    cp.transforms()
        .define_transform(physical_def("t_new", tref("s", "src"), tref("s", "not_yet")))
        .await
        .expect("undeclared output defines");

    // (b) output is a declared stream table → refused.
    let out = tref("s", "declared_stream_out");
    let mut tx = pool.begin().await.expect("begin");
    let at = next_snapshot(&mut tx, None).await.expect("snap");
    let tid = ensure_table(&mut tx, &out.schema, &out.name, at).await.expect("ensure");
    tx.commit().await.expect("commit");
    cp.declare_stream(tid, 4).await.expect("declare_stream");

    let err = cp
        .transforms()
        .define_transform(physical_def("t_stream", tref("s", "src"), out.clone()))
        .await
        .expect_err("stream output must be refused at define time");
    assert!(matches!(err, ControlPlaneError::Validation(_)), "got {err:?}");
    assert!(err.to_string().contains("stream-table target refused:"), "msg: {err}");
}
```

Add a SECOND, mandatory test for the Typed acceptance bullet (spec Acceptance requires "…and of a Typed def whose output type binds to one"):

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn define_transform_refuses_typed_stream_output() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    // Bind a type to a backing table, then declare that backing table a stream.
    cp.ontology()
        .define_type(ObjectType::build("LineStream", ("s", "line_stream")) /* + props, identity */)
        .await
        .expect("define type");
    let mut tx = pool.begin().await.expect("begin");
    let at = next_snapshot(&mut tx, None).await.expect("snap");
    let tid = ensure_table(&mut tx, "s", "line_stream", at).await.expect("ensure");
    tx.commit().await.expect("commit");
    cp.declare_stream(tid, 4).await.expect("declare_stream");

    // A Typed def whose output type binds to the stream table → refused at define time.
    let def = TransformDef {
        name: TransformName("typed_stream".into()),
        body: TransformBody::Typed {
            inputs: vec!["LineStream".into()],
            output: "LineStream".into(),
            sql: "select * from LineStream".into(),
            output_mode: OutputMode::Append,
        },
        schedule: None,
        on_input_commit: false,
    };
    let err = cp
        .transforms()
        .define_transform(def)
        .await
        .expect_err("typed stream output must be refused");
    assert!(matches!(err, ControlPlaneError::Validation(_)), "got {err:?}");
    assert!(err.to_string().contains("stream-table target refused:"), "msg: {err}");
}
```

> Fill the `/* + props, identity */` with the same `ObjectType::build(...)` prop/identity form the other e2e seeds use (`action_multi_object_e2e.rs:126-160`). `Ontology`/`ObjectType` are already in the test header's `use` list. This test shares the Typed branch of the define-time guard (`pg_type_tables(...).get(output)`); it must be present, not optional.

- [ ] **Step 2: Add the BUCK target** (model deps on `stream-cdc-emission`; needs `core` + `sqlx`/`tokio`; add `//src/testing:seed` only if the Typed test uses a warehouse — the Physical-only test does not):

```python
loom_fixture_test(
    name = "define-transform-refuse",
    crate = "define_transform_refuse",
    srcs = ["tests/define_transform_refuse.rs"],
    crate_root = "tests/define_transform_refuse.rs",
    deps = [
        ":postgres",
        "//src/control-plane/core:core",
        "//third-party:sqlx",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 3: Run to verify it fails**

Run: `buck2 test --console none //src/control-plane/postgres:define-transform-refuse`
Expected: FAIL — define currently succeeds against the stream output.

- [ ] **Step 4: Add the define-time guard**

In `src/control-plane/postgres/src/transforms.rs`, in `define_transform`, after the trigger-cycle check (after ~line 358) and BEFORE the `insert into transforms.transform` query (line 359), add:

```rust
    // Define-time UX guard (not the authority — the commit-time guards are the hard
    // gate): resolve the def's output table and refuse if it is already a declared
    // stream/CDC table. A Physical output is the literal `TableRef`; a Typed output
    // resolves via its bound backing table. An output that does not exist yet passes.
    let output_table: Option<TableRef> = match &def.body {
        TransformBody::Physical { output, .. } => Some(output.clone()),
        TransformBody::Typed { output, .. } => {
            let bodies = [(def.name.clone(), def.body.clone())];
            pg_type_tables(&mut *tx, &bodies).await?.get(output).cloned()
        }
    };
    if let Some(t) = &output_table {
        crate::stream::pg_refuse_stream_target(&mut *tx, t).await?;
    }
```

> `pg_type_tables` is defined in this same file (~line 32); call it unqualified. `def.body.clone()` and `def.name.clone()` are cheap (`TransformBody`/`TransformName` are `Clone`). `TableRef` is already in scope (constructed at line 65). The `?` aborts the define tx → rollback (nothing inserted).

- [ ] **Step 5: Run to verify it passes**

Run: `buck2 test --console none //src/control-plane/postgres:define-transform-refuse`
Expected: PASS.

- [ ] **Step 6: Clippy + commit**

```bash
buck2 build --console none '//src/control-plane/postgres:postgres[clippy.txt]'
git add src/control-plane/postgres/src/transforms.rs \
        src/control-plane/postgres/tests/define_transform_refuse.rs \
        src/control-plane/postgres/BUCK
git commit -m "feat(transform): define-time refusal of stream-table transform outputs"
```

---

## Task 5: Path A — transform commit refusal surfaces as worker *abandon* + Failed run

**Files:**
- Modify: `src/services/engine/src/service.rs` (`status()`, ~line 21-28)
- Modify: `src/services/engine-wire/src/client.rs` (`cp_status`, ~line 27-40)
- Modify: `src/services/worker/src/transform.rs` (`commit_transform` map_err, ~line 359)
- Create/extend: `src/services/worker/tests/transform_e2e.rs` (new test)
- Modify: `src/services/worker/BUCK` only if a new dep is needed (none expected)

**Interfaces:**
- Consumes: the `IcebergTx::commit` guard from Task 3; `JobFailure::{abandon, retry}`, `RetryPolicy::Abandon`, `RunState::Failed`, `ControlPlaneError::Validation` (all already imported in `transform.rs`).
- Produces: no new symbols; the engine emits `invalid_argument` for `Validation`, `cp_status` inverts it back to `Validation`, and the worker's `commit_transform` step abandons (not retries) on `Validation`.

- [ ] **Step 1: Write the failing e2e test**

Add to `src/services/worker/tests/transform_e2e.rs` a test modeled EXACTLY on `run_lifecycle_fails_terminally_on_bad_sql` (its fixture-boot preamble, `spawn_engine_uds`, `build_ctx`, `submit_run`, `handle_transform` scaffolding — copy that preamble verbatim), changing only the scenario:

```rust
/// A transform defined while its output was undeclared, whose output is THEN
/// declared a stream table, must fail at run time: the engine refuses the
/// `CommitTransform`, the worker ABANDONS (deterministic, no retry), the run is
/// Failed-terminal with the refusal message, and nothing is registered.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_refuses_stream_output_abandons_terminal() {
    // --- copy the boot preamble VERBATIM from run_lifecycle_succeeds_with_commit_snapshot
    //     (transform_e2e.rs:907-948): PgFixture::shared() → fresh_db → warehouse →
    //     spawn_engine_uds(EngineOpts{control:true, flight:true, ..default}) →
    //     build_ctx(...) giving `ctx`, plus `cp` and `pool`. Then seed the batch input
    //     `t.src` with a couple rows via the same `land(...)` helper that model uses
    //     (transform_e2e.rs:930-948) — WITHOUT it the run abandons on "unknown input"
    //     before ever reaching the commit guard. ---

    let src = TableRef { schema: "t".into(), name: "src".into() };
    let dst = TableRef { schema: "t".into(), name: "out".into() };

    // 1. Define the transform WHILE `dst` is undeclared (define-time guard passes).
    let def = TransformDef {
        name: TransformName("stream_out".into()),
        body: TransformBody::Physical {
            inputs: vec![src.clone()],
            output: dst.clone(),
            sql: "select * from src".into(),
            output_mode: OutputMode::Append,
        },
        schedule: None,
        on_input_commit: false,
    };
    cp.transforms().define_transform(def.clone()).await.expect("define");

    // 2. NOW declare `dst` a stream table (create its mirror, then declare_stream).
    let mut tx = pool.begin().await.expect("begin");
    let at = next_snapshot(&mut tx, None).await.expect("snap");
    let tid = ensure_table(&mut tx, &dst.schema, &dst.name, at).await.expect("ensure");
    tx.commit().await.expect("commit");
    cp.declare_stream(tid, 4).await.expect("declare_stream");

    // 3. Submit + dequeue over the wire + run → refused at IcebergTx::commit.
    //    NOTE: `run_id` is a plain `uuid::Uuid` (TransformRun has no `id`/`Default`);
    //    `to_job` and `get_run` also take `Uuid`.
    let rid = uuid::Uuid::new_v4();
    let run = TransformRun {
        run_id: rid,
        transform: Some(def.name.clone()),
        trigger: RunTrigger::AdHoc,
        state: RunState::Queued,
        body: def.body.clone(),
        queued_at: time::OffsetDateTime::now_utc(),
        started_at: None,
        finished_at: None,
        snapshot_id: None,
        error: None,
    };
    cp.transforms().submit_run(run, def.body.to_job(rid)).await.expect("submit");
    let job = ctx
        .control
        .dequeue(&[TRANSFORM_JOB_KIND.to_string()], "e2e-worker")
        .await
        .expect("dequeue")
        .expect("a queued transform job");
    let err = handle_transform(&ctx, job).await.expect_err("stream commit must fail");

    // 4. Assertions: ABANDON (not retry), Failed-terminal, refusal message, nothing registered.
    assert!(matches!(err.policy, RetryPolicy::Abandon),
        "stream refusal is deterministic → Abandon, got {:?}", err.policy);
    assert!(err.error.contains("stream-table target refused:"), "err text: {}", err.error);
    let r = cp.transforms().get_run(rid).await.expect("run");
    assert_eq!(r.state, RunState::Failed, "run must be Failed-terminal");
    // Nothing registered: `dst` has a live snapshot (from declare_stream) but no files.
    let ice = IcebergCatalog::new(pool.clone());
    let snap = ice.current_snapshot(&dst).await.expect("snap");
    let live = ice.files(&dst, snap.id, PageReq::unbounded()).await.expect("files");
    assert!(live.items.is_empty(), "nothing registered against the stream table");
}
```

> Verified against the models (`transform_e2e.rs:907-1117`): `TransformRun` fields are `{ run_id: Uuid, transform: Option<TransformName>, trigger: RunTrigger, state: RunState, body: TransformBody, queued_at, started_at, finished_at, snapshot_id: Option<i64>, error: Option<String> }` (no `id`, no `Default`); `TransformBody::to_job(rid: Uuid)`; `Transforms::submit_run(run, job)`; `get_run(rid: Uuid)`. The dequeue mirrors `transform_e2e.rs:1097-1100`. **Add these imports** to `transform_e2e.rs` (not currently present): `control_plane_postgres::iceberg_mirror::{ensure_table, next_snapshot}`, `control_plane_core::{StreamTables, PageReq}`. Already imported there: `Catalog`, `IcebergCatalog`, `RetryPolicy`, `RunState`, `RunTrigger`, `TransformRun`, `TransformDef`, `TransformName`, `TransformBody`, `OutputMode`, `TRANSFORM_JOB_KIND`. No placeholder may remain in the committed test.

- [ ] **Step 2: Run to verify it fails**

Run: `buck2 test --console none //src/services/worker:transform-e2e`
Expected: FAIL — today the worker wraps the commit error in `JobFailure::retry` (policy `Retry`, not `Abandon`) and the engine returns `internal` (not the class that lets the worker distinguish it). The `assert!(matches!(err.policy, RetryPolicy::Abandon))` fails.

- [ ] **Step 3: Add the engine `status()` `Validation` arm**

In `src/services/engine/src/service.rs`, change `status()` (lines 21-28) to:

```rust
fn status(e: control_plane_core::ControlPlaneError) -> Status {
    use control_plane_core::ControlPlaneError::*;
    match e {
        NotFound(m) => Status::not_found(m.to_string()),
        Conflict(m) => Status::aborted(m.to_string()),
        Validation(m) => Status::invalid_argument(m),
        other => Status::internal(other.to_string()),
    }
}
```

> Accepted side effect (an error-fidelity improvement): other deterministic `Validation` faults on `EngineControl` handlers that go through `status()` also become `invalid_argument` instead of `internal`.

- [ ] **Step 4: Add the `cp_status` inverse arm (SPEC CORRECTION 1)**

In `src/services/engine-wire/src/client.rs`, change `cp_status` (lines 27-40) to map `InvalidArgument` back to `Validation`, inverting Step 3 so the class survives the wire:

```rust
#[must_use]
pub fn cp_status(s: tonic::Status) -> ControlPlaneError {
    use tonic::Code;
    match s.code() {
        Code::NotFound => ControlPlaneError::NotFound(s.message().to_string()),
        Code::Aborted => ControlPlaneError::Conflict(s.message().to_string()),
        Code::InvalidArgument => ControlPlaneError::Validation(s.message().to_string()),
        other => ControlPlaneError::Backend(
            format!("engine governance RPC failed ({other:?}): {}", s.message()).into(),
        ),
    }
}
```

> Blast radius (acceptable, an improvement): governance-RPC handlers that already emit `Status::invalid_argument` for malformed input (e.g. `commit_transform`'s `bad columns_json` / `bad lineage`) now surface as `ControlPlaneError::Validation` instead of `Backend`. On the worker those are deterministic faults that SHOULD abandon (Step 5), which is the correct behavior — no path regresses.

- [ ] **Step 5: Add the worker abandon-on-`Validation` match**

In `src/services/worker/src/transform.rs`, change the `commit_transform` `.map_err` closure (lines 358-365) from the unconditional retry to a variant match:

```rust
        .await
        .map_err(|e| match e {
            // A refusal (stream-target guard) or any deterministic control-plane
            // validation fault is not retryable — abandon so the run fails terminally
            // instead of retrying until the queue gives up.
            ControlPlaneError::Validation(m) => {
                JobFailure::abandon(format!("commit_transform refused: {m}"))
            }
            other => JobFailure::retry(
                ctx.worker_tuning.backoff(attempts),
                format!("commit_transform: {other}"),
            ),
        })?;
```

> `ControlPlaneError` is already imported in `transform.rs` (line 10). `report_run_failure` (called by both handlers after `run_wire_transform`) derives `terminal` from `RetryPolicy::Abandon`, so the abandon marks the run `Failed`-terminal automatically — no other change needed.

- [ ] **Step 6: Run to verify it passes**

Run: `buck2 test --console none //src/services/worker:transform-e2e`
Expected: PASS.

- [ ] **Step 7: Non-regression — the rest of the transform suite**

Run: `buck2 test --console none //src/services/worker/...`
Expected: PASS (existing `run_lifecycle_*`, `unknown_input_abandons`, etc. unaffected — `Validation` never arose on the success paths).

- [ ] **Step 8: Clippy + commit**

```bash
buck2 build --console none '//src/services/engine:engine[clippy.txt]' \
                           '//src/services/engine-wire:engine-wire[clippy.txt]' \
                           '//src/services/worker:worker[clippy.txt]'
git add src/services/engine/src/service.rs \
        src/services/engine-wire/src/client.rs \
        src/services/worker/src/transform.rs \
        src/services/worker/tests/transform_e2e.rs
git commit -m "feat(transform): stream-refusal abandons the run terminally (Path A wire)"
```

---

## Task 6: Path B — multi-step action against a stream target returns HTTP 422

**Files:**
- Modify: `src/services/engine-serving/src/serving.rs` (`EngineServingError` — new `Validation` variant)
- Modify: `src/services/engine/src/flight.rs` (`serving_status` — new arm, keeps match exhaustive)
- Modify: `src/services/engine-serving/src/action_writer.rs` (`write_steps` — construct `Validation`)
- Modify: `src/services/engine/src/service.rs` (`write_steps` handler — narrow `Validation`→`invalid_argument` match)
- Modify: `src/services/engine-wire/src/client.rs` (`write_steps` — `be`→`cp_status`, SPEC CORRECTION 2)
- Modify: `src/services/query-api/src/engine_action_client.rs` (`to_serving_write` — `Validation`→`Unsupported`)
- Modify: `src/services/query-api/src/action.rs` (`run_multi_step` — `Unsupported`→`ActionError::Unsupported`)
- Create/extend: a query-api HTTP e2e test asserting 422
- Modify: relevant `BUCK` if a test dep is added

**Interfaces:**
- Consumes: the `write_steps` guard from Task 2 (returns `ControlPlaneError::Validation`); `cp_status`'s new `InvalidArgument`→`Validation` arm (Task 5); `ServingError::Unsupported(String)` (exists) → `ActionError::Unsupported(String)` (exists) → HTTP 422 (exists, `http.rs:1243`).
- Produces: `EngineServingError::Validation(String)` (new variant); the full `Validation` carry-through so a multi-step action targeting a stream table renders as 422 with the refusal message, never 500, and writes nothing.

- [ ] **Step 1: Write the failing HTTP e2e test**

Create `src/services/query-api/tests/multi_step_stream_refuse_http.rs` (or add to an existing multi-step action e2e). Model the fixture + multi-step ActionDef on `action_multi_object_e2e.rs` and the HTTP boundary on `action_conformance_http.rs` + `e2e_support::post_action_raw`:

```rust
//! A multi-step action whose second step targets a declared stream table must fail
//! with HTTP 422 (the refusal), never 500, and write nothing.

use std::sync::Arc;

use control_plane_core::{ControlPlane, ObjectType, Ontology, StreamTables, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_mirror::{ensure_table, next_snapshot};
use e2e_support::InProcessServingEngine;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_step_action_targeting_stream_returns_422() {
    // Boot: mirror action_multi_object_e2e.rs:120-171 exactly.
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await; // `cp: Arc<PgControlPlane>` in that harness
    let warehouse = tempfile::tempdir().expect("wh");
    let pool = fx.pool_for(&db).await;

    // 1. Define the two ontology types (backing tables main.order / main.line_item).
    cp.ontology()
        .define_type(ObjectType::build("Order", ("main", "order")) /* + props, identity */)
        .await
        .expect("define Order");
    cp.ontology()
        .define_type(ObjectType::build("LineItem", ("main", "line_item")) /* + props, identity */)
        .await
        .expect("define LineItem");

    // 2. Define the 3-step create-order-with-lines action + grant Write.
    e2e_support::define_create_order_with_lines_action(&cp).await;
    let (subj, _role) = e2e_support::writer_on(&cp, &["Order", "LineItem"]).await;

    // 3. Engine writer over a real UDS (exercises GrpcQueueClient::write_steps) + serving.
    let (action_engine, _eg) = e2e_support::spawn_engine_writer(
        fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX,
    ).await;
    let serving = Arc::new(InProcessServingEngine::new(IcebergCatalog::new(pool.clone())));

    // 4. Declare LineItem's backing table (main.line_item) a stream table.
    let lines_tbl = TableRef { schema: "main".into(), name: "line_item".into() };
    let mut tx = pool.begin().await.expect("begin");
    let at = next_snapshot(&mut tx, None).await.expect("snap");
    let tid = ensure_table(&mut tx, &lines_tbl.schema, &lines_tbl.name, at).await.expect("ensure");
    tx.commit().await.expect("commit");
    cp.declare_stream(tid, 4).await.expect("declare_stream");

    // 5. POST the multi-step action over HTTP → expect 422 with the refusal message.
    let (status, _headers, body) = e2e_support::post_action_raw(
        cp.clone(),
        serving.clone(),
        action_engine.clone(),
        "/actions/createOrderWithLines",
        &serde_json::json!({ "oid": "500", "li1": "1", "li2": "2" }),
        subj.0.as_str(),
    ).await;
    assert_eq!(status, axum::http::StatusCode::UNPROCESSABLE_ENTITY, "body: {body}");
    assert!(body.to_string().contains("stream-table target refused:"), "body: {body}");
}
```

> Verified against the models: `post_action_raw(cp: Arc<PgControlPlane>, eng: Arc<dyn ServingEngine>, action_engine: Arc<dyn ActionEngine>, uri: &str, body: &serde_json::Value, subject: &str) -> (StatusCode, HeaderMap, serde_json::Value)` (`e2e_support.rs:1307`; call model `action_downstream_atomicity.rs:179-189`). Type/action/ACL/engine setup mirrors `action_multi_object_e2e.rs:120-171`: `define_type` with the exact `ObjectType::build(...)` props+identity from that file (fill the `/* + props, identity */`), `define_create_order_with_lines_action` (`e2e_support.rs:947`), `writer_on(&cp, &["Order","LineItem"])`, `spawn_engine_writer(...)`, `InProcessServingEngine::new(IcebergCatalog::new(pool.clone()))`. The param keys `oid`/`li1`/`li2` match the action's step params (`action_downstream_atomicity.rs:181-187`). Add `":e2e-support"` and any missing `//third-party` deps to the target. Resolve the two `/* ... */` (type props/identity) from `action_multi_object_e2e.rs:126-160` before running — no placeholder may survive.

- [ ] **Step 1b: Add the BUCK test target**

In `src/services/query-api/BUCK`, add a `loom_fixture_test` mirroring the existing **`action-multi-object-e2e`** target's deps (it already wires `:e2e-support`, the engine writer, and axum). Name it `multi-step-stream-refuse-http`, `crate = "multi_step_stream_refuse_http"`, `srcs`/`crate_root = ["tests/multi_step_stream_refuse_http.rs"]`, and copy that target's `deps` verbatim (it must include `:query-api`, `:e2e-support`, `//src/control-plane/core:core`, `//src/control-plane/postgres:postgres`, `//third-party:serde_json`, `//third-party:tempfile`, `//third-party:tokio`, and whatever axum/uuid deps `action-multi-object-e2e` carries). If a symbol is unresolved at build, add the corresponding `//third-party:<crate>` the model target lists.

- [ ] **Step 2: Run to verify it fails**

Run: `buck2 test --console none //src/services/query-api:multi-step-stream-refuse-http`
Expected: FAIL — today the refusal from `write_steps` collapses to `ServingError::Engine` → `ActionError::Serving` → the `http.rs` catch-all → **500**, so `assert_eq!(status, 422)` fails.

- [ ] **Step 3: Add `EngineServingError::Validation`**

In `src/services/engine-serving/src/serving.rs`, add a variant to `EngineServingError` (after `Conflict`, ~line 66):

```rust
    /// A control-plane `Validation` refusal reached the write executor (e.g. a
    /// multi-target write targeting a declared stream table). Wire callers map this
    /// to `invalid_argument`; query-api renders it as 422. Distinct from `Engine`
    /// (internal/500) so the class survives.
    #[error("{0}")]
    Validation(String),
```

- [ ] **Step 4: Keep `serving_status` exhaustive**

In `src/services/engine/src/flight.rs`, add the arm to `serving_status` (adding an enum variant breaks the exhaustive match otherwise):

```rust
        E::Validation(m) => Status::invalid_argument(m),
```

(place it alongside `E::DimMismatch(m) => Status::invalid_argument(m)`).

- [ ] **Step 5: Construct `Validation` in the action writer**

In `src/services/engine-serving/src/action_writer.rs`, change the terminal `iceberg_landing::write_steps` error map (lines 139-141) from the class-erasing `Engine` to preserve `Validation`:

```rust
        iceberg_landing::write_steps(&self.pool, &self.catalog, steps, event, jobs)
            .await
            .map_err(|e| match e {
                ControlPlaneError::Validation(m) => EngineServingError::Validation(m),
                other => EngineServingError::Engine(other.to_string()),
            })
```

> `ControlPlaneError` is already imported in `action_writer.rs`. Leave the IPC-decode `.map_err` (line 129) as `Engine` — a decode fault is not a validation refusal.

- [ ] **Step 6: Narrow the engine `write_steps` handler mapping**

In `src/services/engine/src/service.rs`, change the `write_steps` handler's error map (line ~418-422) from blanket `Status::internal` to route only the new variant to `invalid_argument`:

```rust
        let snap = self
            .writer
            .write_steps(&writes, event, &jobs)
            .await
            .map_err(|e| match e {
                engine_serving::EngineServingError::Validation(m) => Status::invalid_argument(m),
                other => Status::internal(other.to_string()),
            })?;
```

> Everything except `Validation` stays byte-identical (`internal`).

- [ ] **Step 7: Preserve the class on the query-api wire client (SPEC CORRECTION 2)**

In `src/services/engine-wire/src/client.rs`, change `GrpcQueueClient::write_steps`'s `.map_err(be)?` (line 256) to `.map_err(cp_status)?` so the `invalid_argument` inverts to `ControlPlaneError::Validation` (using the arm added in Task 5):

```rust
            .await
            .map_err(cp_status)?
            .into_inner();
```

> `write_steps` is a governance-class RPC on `EngineControl`; `cp_status` is the correct governance inverse. The only behavioral change vs `be` is that `NotFound`/`Aborted`/`InvalidArgument` now carry their class (previously all `Backend`) — the write path only produces `InvalidArgument` (our refusal), so this is scoped.

- [ ] **Step 8: Map `Validation`→`Unsupported` in `to_serving_write`**

In `src/services/query-api/src/engine_action_client.rs`, add a `Validation` arm to `to_serving_write` (lines 24-29):

```rust
fn to_serving_write(e: control_plane_core::ControlPlaneError) -> ServingError {
    match e {
        control_plane_core::ControlPlaneError::Conflict(m) => ServingError::Conflict(m),
        control_plane_core::ControlPlaneError::Validation(m) => ServingError::Unsupported(m),
        other => ServingError::Engine(other.to_string()),
    }
}
```

- [ ] **Step 9: Map `ServingError::Unsupported`→`ActionError::Unsupported` in `run_multi_step`**

In `src/services/query-api/src/action.rs`, the single atomic write in `run_multi_step` (lines 1440-1442) currently does `.await?`, auto-converting any `ServingError` to `ActionError::Serving` (→ 500). Change it to route `Unsupported` to `ActionError::Unsupported` (→ 422):

```rust
    deps.action_engine
        .write_steps(&writes, event, &jobs)
        .await
        .map_err(|e| match e {
            crate::serving::ServingError::Unsupported(m) => ActionError::Unsupported(m),
            other => ActionError::from(other),
        })?;
```

> `ActionError::from(ServingError)` exists (`#[from]` on the `Serving` variant), so non-`Unsupported` serving errors keep their current mapping (→ catch-all 500), byte-identical. `ActionError::Unsupported(m)` renders as 422 with body `m` at `http.rs:1243`.

- [ ] **Step 10: Run to verify it passes**

Run: `buck2 test --console none //src/services/query-api:multi-step-stream-refuse-http`
Expected: PASS (status 422, body contains the refusal message).

- [ ] **Step 11: Non-regression — the action/write suites**

Run: `buck2 test --console none //src/services/query-api/... //src/services/engine/... //src/services/engine-serving/...`
Expected: PASS (existing `write_steps_e2e`, `action_multi_object_e2e`, `action_conformance_http`, etc. unaffected — no success path changed; only the previously-500 refusal now 422).

- [ ] **Step 12: Clippy + commit**

```bash
buck2 build --console none \
  '//src/services/engine-serving:engine-serving[clippy.txt]' \
  '//src/services/engine:engine[clippy.txt]' \
  '//src/services/engine-wire:engine-wire[clippy.txt]' \
  '//src/services/query-api:query-api[clippy.txt]'
git add src/services/engine-serving/src/serving.rs \
        src/services/engine/src/flight.rs \
        src/services/engine-serving/src/action_writer.rs \
        src/services/engine/src/service.rs \
        src/services/engine-wire/src/client.rs \
        src/services/query-api/src/engine_action_client.rs \
        src/services/query-api/src/action.rs \
        src/services/query-api/tests/multi_step_stream_refuse_http.rs \
        src/services/query-api/BUCK
git commit -m "feat(action): stream-refusal surfaces as HTTP 422 (Path B wire)"
```

---

## Final verification (before finishing the branch)

- [ ] **Full first-party sweep** (main must stay green): `buck2 build -v0 --console none //src/...` then `buck2 test --console none //src/...`. Expected: `Tests finished: Pass N. Fail 0`.
- [ ] **Clippy over all first-party Rust**: `tools/clippy-all.sh`. Expected: clean.
- [ ] **Metric gate** (from loom-work-checkout): run `loom-complexity diff` and `loom-duplication diff` (changed files only, no commit). Report any NEW hotspot over census thresholds (cc > 15, cognitive > 15, MI < 20, SLOC > 100) or NEW cross-file duplication pair ≥ 20 lines as a finding — fix or justify each in the PR body. The guard bodies are small and structurally similar across the three call sites; if the duplication detector flags the sort/dedup/refuse-loop pattern, that is acceptable given each lives in a different transaction context (justify in the PR).
- [ ] **Register close** via `loom-docs-update`: remove `road-stream-framing-write-paths` from `docs/ROADMAP.md`, fold the landed capability into `docs/system-capabilities/` (naming the id + PR), staged in the same PR.

## Acceptance mapping (spec → task)

| Spec acceptance | Task |
|---|---|
| Define-time: Physical/Typed stream output → `Validation` `stream-table target refused:`; batch/absent → OK | 4 |
| Commit-time (transform e2e): defined-then-declared stream output → engine `invalid_argument`, worker **abandon**, run Failed-terminal, nothing registered | 3 (guard) + 5 (surface) |
| Multi-target: `write_steps` with any stream (or CDC-changelog) target → `Validation`, commits nothing; over the wire → HTTP 422 | 2 (guard) + 6 (surface) |
| CDC changelog table (`changelog_table_id`) also refused | 1 (helper) |
| Byte-identical elsewhere: batch flows succeed as before; full suite passes unmodified | all (non-regression steps) |
| `staged_compacts` NOT guarded; overwrite seam unchanged | 3 |
| Memory adapter unguarded (no mirror); testkit contracts unchanged | (no change — inherent) |
