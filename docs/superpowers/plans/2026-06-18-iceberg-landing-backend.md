# Iceberg Landing Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the ingest service land data through the Iceberg adapter, selected at boot by `LOOM_LANDING_BACKEND`, routing each request by in-memory size to inline (mirror-only rows) or real Parquet — both emitting lineage atomically.

**Architecture:** A `LandingMaterializer` port in the ingest crate with `DuckLake` (today's path, refactored) and `Iceberg` impls, chosen in `main` like query-api's `LOOM_SERVING_BACKEND`. The arrow-57 Iceberg landing logic (IPC decode, byte routing, inline/Parquet, create-if-absent) lives in a new `iceberg_landing` module in the postgres crate (arrow-57 native); the ingest `IcebergMaterializer` is a thin forwarder passing the raw IPC body. Atomic Parquet lineage comes from a per-call `LineageEmittingCatalog` decorator + a `SqlCatalog::do_update_table(commit, lineage)` that `pg_emit`s inside the pointer-CAS/mirror tx.

**Tech Stack:** Rust 2024, buck2, iceberg 0.9.1 (arrow-57 / parquet57), arrow-58 (ingest), sqlx 0.9, DataFusion 54, axum, Postgres control plane.

**Reference spec:** `docs/superpowers/specs/2026-06-18-iceberg-landing-backend-design.md`

**Conventions (read before starting):**
- Tests are `rust_test` / `loom_fixture_test` targets in `tests/<name>.rs` — **never** inline `#[cfg(test)]` (a prek hook fails the build otherwise).
- Fixture tests (boot Postgres/DuckDB) must use `loom_fixture_test`, not bare `rust_test`.
- **Never** pipe `buck2 test`/`buck2 bxl` to `tail`/`head` — redirect to a file and grep: `buck2 test //… > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/t.log`. `buck2 build … | tail` is fine.
- rustfmt is check-only: run `buck2 run //tools:rustfmt -- <file>` (write mode) before committing, or the hook loops.
- After changing any SQL, regenerate the `.sqlx` cache (`./tools/sqlx-prepare.sh`) and commit it. **This plan adds no new compile-time `query!` SQL** (the new `pg_emit` call reuses the existing committed query), so no `.sqlx` change is expected — but if a build error mentions sqlx offline data, run the prepare script.
- Commit messages: Conventional Commits, ending with `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.

---

## File Structure

**postgres crate (`src/control-plane/postgres/`, arrow-57):**
- `src/iceberg_sql_catalog/catalog.rs` — extract `do_update_table`; trait `update_table` delegates.
- `src/iceberg_writer.rs` — add `LineageEmittingCatalog` + `append_batches_with_lineage`.
- `src/iceberg_landing.rs` (new) — `decode_ipc_57`, `land`, `land_parquet`.
- `src/lib.rs` — `pub mod iceberg_landing;`.
- `Cargo.toml` + `BUCK` — add `arrow-ipc` (57) and `arrow-select` (57); new test targets.

**ingest crate (`src/services/ingest/`, arrow-58):**
- `src/landing.rs` (new) — `LandingBackend`, `parse_landing_backend`, `LandingMaterializer`, `LandRequest`, `DuckLakeMaterializer`, `IcebergMaterializer`.
- `src/materialize.rs` — keep the write tail as `DuckLakeMaterializer`'s body; gate/schema head moves to the handler.
- `src/http.rs` — resolve gate/columns/lineage in `land`; `AppState { materializer }`; thread `ipc_body`.
- `src/main.rs` — env reads + materializer construction.
- `src/lib.rs` — `pub mod landing;`.
- `BUCK` — deps on `//src/control-plane/postgres`, `//third-party:iceberg`, `//third-party:sqlx`; new test targets.

**docs:** `docs/spike/ICEBERG_ROADMAP.md` — mark Slice B done.

---

## Task 1: Extract `do_update_table` with optional lineage (postgres)

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs:962-1025`

This is a behaviour-preserving refactor: the trait `update_table` keeps its exact current behaviour (lineage `None`); a new inherent `do_update_table` gains the `Option<&LineageEvent>` parameter and emits inside the tx when `Some`. No new test here — Task 2's test exercises the `Some` path; the existing `iceberg_write_roundtrip` + full suite prove the `None` path is unchanged.

- [ ] **Step 1: Add the lineage import**

At the top of `catalog.rs`, add (near the other `crate::` imports):

```rust
use crate::lineage::pg_emit;
use control_plane_core::LineageEvent;
```

(`pg_emit` is `pub(crate)`; `catalog.rs` is in the same crate. Confirm `control_plane_core` is already a dep — it is.)

- [ ] **Step 2: Replace the trait `update_table` body with a delegation + inherent method**

Replace the whole `async fn update_table(&self, commit: TableCommit) -> Result<Table>` (lines 962-1025) with a thin trait method that delegates, and add an inherent method holding the original body plus the lineage emit. Find the `#[async_trait] impl Catalog for SqlCatalog {` block and change its `update_table` to:

```rust
    /// Updates an existing table within the SQL catalog. Lineage-free path:
    /// the loom landing path uses `do_update_table(.., Some(ev))` to attach a
    /// lineage event atomically (see `iceberg_writer::append_batches_with_lineage`).
    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        self.do_update_table(commit, None).await
    }
```

Then add to the **inherent** `impl SqlCatalog { … }` block (the one that already has `new`/`execute`/`project_mirror` — `project_mirror` is at line 333, in `impl SqlCatalog`):

```rust
    /// The real commit: pointer CAS + mirror projection (+ optional lineage),
    /// all in one Postgres transaction. `update_table` calls this with `None`;
    /// the loom landing path passes `Some(event)` so the lineage row commits or
    /// rolls back together with the snapshot it describes.
    pub(crate) async fn do_update_table(
        &self,
        commit: TableCommit,
        lineage: Option<&LineageEvent>,
    ) -> Result<Table> {
        let table_ident = commit.identifier().clone();
        let current_table = self.load_table(&table_ident).await?;
        let current_metadata_location = current_table.metadata_location_result()?.to_string();

        let staged_table = commit.apply(current_table)?;
        let staged_metadata_location = staged_table.metadata_location_result()?;

        staged_table
            .metadata()
            .write_to(staged_table.file_io(), &staged_metadata_location)
            .await?;

        let mut tx = self.connection.begin().await.map_err(from_sqlx_error)?;

        let update_result = self
            .execute(
                &format!(
                    "UPDATE {CATALOG_TABLE_NAME}
                     SET {CATALOG_FIELD_METADATA_LOCATION_PROP} = ?, {CATALOG_FIELD_PREVIOUS_METADATA_LOCATION_PROP} = ?
                     WHERE {CATALOG_FIELD_CATALOG_NAME} = ?
                      AND {CATALOG_FIELD_TABLE_NAME} = ?
                      AND {CATALOG_FIELD_TABLE_NAMESPACE} = ?
                      AND (
                        {CATALOG_FIELD_RECORD_TYPE} = '{CATALOG_FIELD_TABLE_RECORD_TYPE}'
                        OR {CATALOG_FIELD_RECORD_TYPE} IS NULL
                      )
                      AND {CATALOG_FIELD_METADATA_LOCATION_PROP} = ?"
                ),
                vec![
                    Some(staged_metadata_location),
                    Some(current_metadata_location.as_str()),
                    Some(&self.name),
                    Some(table_ident.name()),
                    Some(&table_ident.namespace().join(".")),
                    Some(current_metadata_location.as_str()),
                ],
                Some(&mut tx),
            )
            .await?;

        if update_result.rows_affected() == 0 {
            let _ = tx.rollback().await;
            return Err(Error::new(
                ErrorKind::CatalogCommitConflicts,
                format!("Commit conflicted for table: {table_ident}"),
            )
            .with_retryable(true));
        }

        self.project_mirror(&mut tx, &table_ident, &staged_table)
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;

        if let Some(ev) = lineage {
            pg_emit(&mut *tx, ev)
                .await
                .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;
        }

        tx.commit().await.map_err(from_sqlx_error)?;
        Ok(staged_table)
    }
```

(The body is the original `update_table` verbatim plus the `if let Some(ev)` block before `tx.commit()`. `pg_emit` accepts `&mut *tx` — a `&mut PgConnection` satisfies `sqlx::PgExecutor`.)

- [ ] **Step 3: Build + format**

Run: `buck2 build //src/control-plane/postgres:postgres > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|FAIL|error\[" /tmp/b.log`
Expected: BUILD SUCCEEDED. Then `buck2 run //tools:rustfmt -- src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs`.

- [ ] **Step 4: Verify the existing Iceberg write path is unchanged**

Run: `buck2 test //src/control-plane/postgres:iceberg-writer //src/control-plane/postgres:iceberg_catalog > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: pass, no failures (the `None` path is byte-identical behaviour).

- [ ] **Step 5: Commit**

```bash
git add src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs
git commit -m "refactor(iceberg): extract SqlCatalog::do_update_table with optional lineage

Trait update_table now delegates to do_update_table(commit, None); the new
inherent method emits a LineageEvent via pg_emit inside the pointer-CAS +
mirror-projection tx when Some, so lineage commits/rolls back atomically with
the snapshot. No behaviour change to the existing (None) path.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: `LineageEmittingCatalog` + `append_batches_with_lineage` (postgres)

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_writer.rs`
- Test: `src/control-plane/postgres/tests/iceberg_writer.rs` (existing fixture test file — add a case)

The decorator delegates every `iceberg::Catalog` method to the inner `&SqlCatalog` except `update_table`, which calls `do_update_table(commit, Some(lineage))`. `append_batches_with_lineage` is `append_batches` with the commit retargeted at the decorator.

- [ ] **Step 1: Write the failing test**

Open `src/control-plane/postgres/tests/iceberg_writer.rs`. It already drives `append_batches` end to end (namespace → create_table → load_table → append → assert mirror). Add a sibling test that uses the lineage variant and asserts a lineage event landed. Mirror the existing test's setup (reuse its helpers `make_catalog`, `batch`, `cs`, fixture). Add:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn append_with_lineage_emits_one_event_atomically() {
    use control_plane_core::{DatasetId, EventType, LineageEvent, Lineage, RunId, TableRef};
    use control_plane_postgres::iceberg_writer::append_batches_with_lineage;

    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;            // cp: PgControlPlane (impl Lineage)
    let wh = tempfile::tempdir().expect("wh");
    let whs = wh.path().display().to_string();
    let catalog = make_catalog(fx.pg_dsn(&db), &whs).await;

    // namespace + table + load (same as the append_round_trips test)
    let ns = NamespaceIdent::new("wh".to_string());
    catalog.create_namespace(&ns, HashMap::new()).await.expect("ns");
    let schema = test_schema();                     // reuse the existing helper if present,
    let creation = TableCreation::builder()         // else inline the id/name schema builder
        .name("t".to_string())
        .location(format!("file://{whs}/wh/t"))
        .schema(schema)
        .build();
    catalog.create_table(&ns, creation).await.expect("create");
    let table = catalog
        .load_table(&TableIdent::new(NamespaceIdent::new("wh".into()), "t".into()))
        .await
        .expect("load");

    let run = RunId(uuid::Uuid::new_v4());
    let lineage = LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(&TableRef { schema: "wh".into(), name: "t".into() }).dataset_ref()],
        payload: serde_json::json!({ "source": "test" }),
    };

    append_batches_with_lineage(&catalog, &table, vec![batch(&cs, vec![1, 2, 3])], &lineage)
        .await
        .expect("append+lineage");

    // exactly one lineage event for the run, with our output dataset
    let page = cp.events_for(&run, control_plane_core::PageReq::default()).await.expect("events");
    assert_eq!(page.items.len(), 1, "one lineage event");
    assert_eq!(page.items[0].outputs[0].name, "t");
}
```

(If the existing test file already has a `test_schema()`/`cs`/`batch` helper, reuse it; otherwise copy the `Schema::builder()` block from the existing `append_round_trips_through_the_mirror` test. `PageReq::default()` — confirm the constructor; the lineage `events_for` contract test shows the exact call.)

- [ ] **Step 2: Run it to verify it fails to compile**

Run: `buck2 test //src/control-plane/postgres:iceberg-writer > /tmp/t.log 2>&1; grep -E "error\[|cannot find|Tests finished|FAIL" /tmp/t.log`
Expected: FAIL — `append_batches_with_lineage` not found.

- [ ] **Step 3: Implement the decorator + function**

In `src/control-plane/postgres/src/iceberg_writer.rs`, add imports and the new items. Add near the top:

```rust
use crate::iceberg_sql_catalog::SqlCatalog;
use async_trait::async_trait;
use control_plane_core::LineageEvent;
use iceberg::table::Table as IceTable;
use iceberg::{
    Namespace, NamespaceIdent, Result as IceResult, TableCommit, TableCreation, TableIdent,
};
use std::collections::HashMap;
```

(Adjust the precise `iceberg::` paths to what the trait signature needs — copy them from the `impl Catalog for SqlCatalog` method signatures in `catalog.rs`, which import the same types.)

Add the decorator and function:

```rust
/// A per-call `Catalog` decorator that attaches a loom `LineageEvent` to the one
/// `update_table` the iceberg commit performs, so the lineage row lands in the
/// same Postgres tx as the pointer CAS + mirror projection. Every other method
/// delegates to the inner `SqlCatalog`. Constructed fresh per append (holds
/// borrows), so there is no shared mutable state across concurrent commits.
#[derive(Debug)]
struct LineageEmittingCatalog<'a> {
    inner: &'a SqlCatalog,
    lineage: &'a LineageEvent,
}

#[async_trait]
impl iceberg::Catalog for LineageEmittingCatalog<'_> {
    async fn update_table(&self, commit: TableCommit) -> IceResult<IceTable> {
        self.inner.do_update_table(commit, Some(self.lineage)).await
    }

    // --- pure delegation below ---
    async fn list_namespaces(&self, parent: Option<&NamespaceIdent>) -> IceResult<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }
    async fn create_namespace(&self, ns: &NamespaceIdent, props: HashMap<String, String>) -> IceResult<Namespace> {
        self.inner.create_namespace(ns, props).await
    }
    async fn get_namespace(&self, ns: &NamespaceIdent) -> IceResult<Namespace> {
        self.inner.get_namespace(ns).await
    }
    async fn namespace_exists(&self, ns: &NamespaceIdent) -> IceResult<bool> {
        self.inner.namespace_exists(ns).await
    }
    async fn update_namespace(&self, ns: &NamespaceIdent, props: HashMap<String, String>) -> IceResult<()> {
        self.inner.update_namespace(ns, props).await
    }
    async fn drop_namespace(&self, ns: &NamespaceIdent) -> IceResult<()> {
        self.inner.drop_namespace(ns).await
    }
    async fn list_tables(&self, ns: &NamespaceIdent) -> IceResult<Vec<TableIdent>> {
        self.inner.list_tables(ns).await
    }
    async fn create_table(&self, ns: &NamespaceIdent, creation: TableCreation) -> IceResult<IceTable> {
        self.inner.create_table(ns, creation).await
    }
    async fn load_table(&self, id: &TableIdent) -> IceResult<IceTable> {
        self.inner.load_table(id).await
    }
    async fn drop_table(&self, id: &TableIdent) -> IceResult<()> {
        self.inner.drop_table(id).await
    }
    async fn table_exists(&self, id: &TableIdent) -> IceResult<bool> {
        self.inner.table_exists(id).await
    }
    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> IceResult<()> {
        self.inner.rename_table(src, dest).await
    }
    async fn register_table(&self, id: &TableIdent, metadata_location: String) -> IceResult<IceTable> {
        self.inner.register_table(id, metadata_location).await
    }
}

/// Like [`append_batches`], but emits `lineage` atomically with the commit (the
/// mirror projection + pointer CAS + lineage row share one Postgres tx via the
/// `LineageEmittingCatalog` decorator). Takes a concrete `&SqlCatalog` because the
/// decorator needs the inherent `do_update_table`.
pub async fn append_batches_with_lineage(
    catalog: &SqlCatalog,
    table: &Table,
    batches: Vec<RecordBatch>,
    lineage: &LineageEvent,
) -> Result<Vec<WrittenFile>> {
    let data_files = write_parquet(table, batches).await?;
    let summaries: Vec<WrittenFile> = data_files
        .iter()
        .map(|df| WrittenFile {
            path: df.file_path().to_string(),
            record_count: df.record_count() as i64,
            file_size_bytes: df.file_size_in_bytes() as i64,
        })
        .collect();

    let wrapper = LineageEmittingCatalog { inner: catalog, lineage };
    let tx = Transaction::new(table);
    let action = tx.fast_append().add_data_files(data_files);
    let tx = action.apply(tx)?;
    tx.commit(&wrapper).await?;
    Ok(summaries)
}
```

**Notes for the implementer:**
- The exact `iceberg::Catalog` trait method signatures (param names/types, and whether `register_table` exists / its signature) must match the vendored trait. Copy them verbatim from `catalog.rs`'s `impl Catalog for SqlCatalog` (each method is there). If a method's signature differs from the sketch above, match the real one.
- `SqlCatalog` must be reachable as `crate::iceberg_sql_catalog::SqlCatalog` and `do_update_table` must be `pub(crate)` (Task 1) — both in the same crate, so visible.
- `SqlCatalog` derives `Debug` (the iceberg `Catalog` trait requires `Debug`); if `LineageEmittingCatalog`'s derive fails because `SqlCatalog`/`LineageEvent` isn't `Debug`, add the missing derive there.

- [ ] **Step 4: Run the test to verify it passes**

Run: `buck2 test //src/control-plane/postgres:iceberg-writer > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: pass (both the old append test and the new lineage test).

- [ ] **Step 5: Format + commit**

```bash
buck2 run //tools:rustfmt -- src/control-plane/postgres/src/iceberg_writer.rs src/control-plane/postgres/tests/iceberg_writer.rs
git add src/control-plane/postgres/src/iceberg_writer.rs src/control-plane/postgres/tests/iceberg_writer.rs
git commit -m "feat(iceberg): append_batches_with_lineage emits lineage atomically

A per-call LineageEmittingCatalog decorator routes the iceberg commit's single
update_table through SqlCatalog::do_update_table(commit, Some(ev)), so the
lineage row shares the pointer-CAS + mirror-projection tx and rolls back with a
lost CAS. Closes the lineage gap on the Iceberg Parquet write path.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: `iceberg_landing` module — decode, route, inline/Parquet (postgres)

**Files:**
- Create: `src/control-plane/postgres/src/iceberg_landing.rs`
- Modify: `src/control-plane/postgres/src/lib.rs` (add `pub mod iceberg_landing;`)
- Modify: `src/control-plane/postgres/Cargo.toml` (add `arrow-ipc`, `arrow-select` at 57)
- Modify: `src/control-plane/postgres/BUCK` (lib deps + new fixture test target)
- Test: `src/control-plane/postgres/tests/iceberg_landing.rs` (new `loom_fixture_test`)

- [ ] **Step 1: Add the arrow-57 deps and buckify**

In `src/control-plane/postgres/Cargo.toml`, add to `[dependencies]` (match the arrow-array version already there — 57):

```toml
arrow-ipc = "=57.3.1"
arrow-select = "=57.3.1"
```

Refresh the lock + regenerate `third-party/BUCK`:

```bash
buck2 run //tools:reindeer -- update
./tools/buckify.sh
```

Then verify the bare aliases resolve to 57: `grep -nA2 'name = "arrow-ipc"' third-party/BUCK | head` should show `actual = ":arrow-ipc-57"` (and likewise `arrow-select`). **If** buckify instead leaves only versioned targets (two-public-majors collision, as with parquet), use the documented named-deps escape hatch: add a Cargo rename (`arrow-ipc57 = { package = "arrow-ipc", version = "=57.3.1" }`) and consume via `named_deps = {"arrow_ipc57": "//third-party:arrow-ipc-57"}` in the BUCK lib target (mirror the existing `parquet57` pattern at `src/control-plane/postgres/BUCK:142`). Adjust the `use` lines below to the resulting crate name.

Add both to the `postgres` `rust_library` `deps` in `src/control-plane/postgres/BUCK`:

```python
        "//third-party:arrow-ipc",
        "//third-party:arrow-select",
```

- [ ] **Step 2: Write the failing tests**

Create `src/control-plane/postgres/tests/iceberg_landing.rs`. Build an Arrow-57 IPC body in-test (arrow-ipc-57 `StreamWriter`), then drive `land` and assert routing + lineage + readback. Two tests:

```rust
//! Fixture tests for the Iceberg landing entrypoint: byte-size routing between
//! inline and Parquet, both emitting lineage atomically.

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    ColumnSpec, DatasetId, EventType, Lineage, LineageEvent, PageReq, RunId, TableRef,
};
use control_plane_postgres::iceberg_landing::land;
use control_plane_postgres::PgFixture; // or the crate's fixture entry — match existing tests

fn ipc_body(rows: i64) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>()))],
    )
    .unwrap();
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &schema).unwrap();
        w.write(&batch).unwrap();
        w.finish().unwrap();
    }
    buf
}

fn columns() -> Vec<ColumnSpec> {
    vec![ColumnSpec { name: "id".into(), ty: "long".into(), nullable: false }]
}

fn lineage(run: RunId, name: &str) -> LineageEvent {
    LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(&TableRef { schema: "wh".into(), name: name.into() }).dataset_ref()],
        payload: serde_json::json!({ "source": "test" }),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn small_request_inlines_and_emits_lineage() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().unwrap();
    let catalog = /* build SqlCatalog: reuse the same helper iceberg_writer.rs tests use */
        make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool(&db).await; // a sqlx PgPool for the db — match how other fixture tests get one

    let run = RunId(uuid::Uuid::new_v4());
    let table = TableRef { schema: "wh".into(), name: "small".into() };
    // huge limit -> always inline
    let snap = land(&pool, &catalog, &table, &columns(), &ipc_body(3), usize::MAX, lineage(run, "small"))
        .await
        .expect("land inline");

    assert!(snap.0 > 0);
    let page = cp.events_for(&run, PageReq::default()).await.unwrap();
    assert_eq!(page.items.len(), 1);
    // inline rows visible through the mirror inline table (assert via a count query
    // or IcebergCatalog::current_snapshot matching `snap`)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_request_writes_parquet_and_emits_lineage() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().unwrap();
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool(&db).await;

    let run = RunId(uuid::Uuid::new_v4());
    let table = TableRef { schema: "wh".into(), name: "big".into() };
    // limit 0 -> always Parquet
    let snap = land(&pool, &catalog, &table, &columns(), &ipc_body(5), 0, lineage(run, "big"))
        .await
        .expect("land parquet");

    // returned snapshot id == the mirror's current snapshot for the table
    let cur = control_plane_postgres::IcebergCatalog::new(pool.clone())
        .current_snapshot(&table)
        .await
        .unwrap();
    assert_eq!(cur.id, snap);

    let page = cp.events_for(&run, PageReq::default()).await.unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].outputs[0].name, "big");
}
```

**Implementer:** match the fixture API to the existing postgres fixture tests (`PgFixture`, `fresh_db`, `pg_dsn`, how a `PgPool` and `PgControlPlane` are obtained, and the `make_catalog` helper from `tests/iceberg_writer.rs` — copy it or lift it into a shared test module). Use `Catalog` trait import for `current_snapshot`.

- [ ] **Step 3: Run to verify it fails**

Run: `buck2 test //src/control-plane/postgres:iceberg-landing > /tmp/t.log 2>&1; grep -E "error\[|cannot find|Tests finished|FAIL" /tmp/t.log`
(Target added in Step 5.) Expected: FAIL — `land` not found / target missing.

- [ ] **Step 4: Implement `iceberg_landing.rs`**

Create `src/control-plane/postgres/src/iceberg_landing.rs`:

```rust
//! The Iceberg landing entrypoint: decode an Arrow IPC body (arrow-57), route by
//! in-memory size between an inline (mirror-only) write and a real Parquet write,
//! and return the loom mirror snapshot id. Both branches emit lineage atomically.
//! Lives here (not the ingest crate) because the iceberg writer chain is arrow-57
//! and the ingest crate is arrow-58 — the ingest `IcebergMaterializer` forwards
//! the raw IPC body so the cross-major boundary stays inside this crate.

use std::io::Cursor;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_schema::Schema;
use arrow_select::concat::concat_batches;
use control_plane_core::{ColumnSpec, LineageEvent, Result, SnapshotId, TableRef};
use iceberg::spec::{NestedField, PrimitiveType, Schema as IceSchema, Type};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use sqlx::PgPool;

use crate::iceberg_catalog::IcebergCatalog;
use crate::iceberg_inline::inline_append;
use crate::iceberg_sql_catalog::SqlCatalog;
use crate::iceberg_type::iceberg_physical_type;
use crate::iceberg_writer::append_batches_with_lineage;
use crate::{backend, ControlPlaneError}; // match the crate's error helper names

/// Decode an Arrow IPC stream body into its (arrow-57) schema + batches.
fn decode_ipc_57(body: &[u8]) -> Result<(Arc<Schema>, Vec<RecordBatch>)> {
    let reader = StreamReader::try_new(Cursor::new(body), None).map_err(|e| backend(e.into()))?;
    let schema = reader.schema();
    let batches = reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| backend(e.into()))?;
    Ok((schema, batches))
}

/// Land an Iceberg request. `inline_byte_limit` is the in-memory (uncompressed)
/// Arrow size at/below which the request inlines instead of writing Parquet.
pub async fn land(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    columns: &[ColumnSpec],
    ipc_body: &[u8],
    inline_byte_limit: usize,
    lineage: LineageEvent,
) -> Result<SnapshotId> {
    let (schema, batches) = decode_ipc_57(ipc_body)?;
    let bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
    if bytes <= inline_byte_limit {
        let batch = concat_batches(&schema, &batches).map_err(|e| backend(e.into()))?;
        inline_append(pool, table, columns, &batch, lineage).await
    } else {
        land_parquet(pool, catalog, table, columns, batches, lineage).await
    }
}

async fn land_parquet(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    columns: &[ColumnSpec],
    batches: Vec<RecordBatch>,
    lineage: LineageEvent,
) -> Result<SnapshotId> {
    let ns = NamespaceIdent::new(table.schema.clone());
    if !catalog.namespace_exists(&ns).await.map_err(|e| backend(e.into()))? {
        catalog
            .create_namespace(&ns, Default::default())
            .await
            .map_err(|e| backend(e.into()))?;
    }
    let ident = TableIdent::new(ns.clone(), table.name.clone());
    if !catalog.table_exists(&ident).await.map_err(|e| backend(e.into()))? {
        let schema = ice_schema(columns)?;
        let creation = TableCreation::builder().name(table.name.clone()).schema(schema).build();
        catalog.create_table(&ns, creation).await.map_err(|e| backend(e.into()))?;
    }
    let ice_table = catalog.load_table(&ident).await.map_err(|e| backend(e.into()))?;

    append_batches_with_lineage(catalog, &ice_table, batches, &lineage)
        .await
        .map_err(|e| backend(e.into()))?;

    Ok(IcebergCatalog::new(pool.clone()).current_snapshot(table).await?.id)
}

/// Build an iceberg `Schema` from loom `ColumnSpec`s, assigning 1-based field ids.
fn ice_schema(columns: &[ColumnSpec]) -> Result<IceSchema> {
    let fields = columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let phys = iceberg_physical_type(&c.ty)
                .ok_or_else(|| ControlPlaneError::Backend(format!("landing: no iceberg type for {:?}", c.ty).into()))?;
            let ty = Type::Primitive(primitive_from(phys)?);
            let id = (i + 1) as i32;
            Ok(Arc::new(if c.nullable {
                NestedField::optional(id, &c.name, ty)
            } else {
                NestedField::required(id, &c.name, ty)
            }))
        })
        .collect::<Result<Vec<_>>>()?;
    IceSchema::builder()
        .with_fields(fields)
        .build()
        .map_err(|e| backend(e.into()))
}

fn primitive_from(phys: &str) -> Result<PrimitiveType> {
    Ok(match phys {
        "int" => PrimitiveType::Int,
        "long" => PrimitiveType::Long,
        "double" => PrimitiveType::Double,
        "boolean" => PrimitiveType::Boolean,
        "string" => PrimitiveType::String,
        "date" => PrimitiveType::Date,
        "timestamp" => PrimitiveType::Timestamp,
        other => return Err(ControlPlaneError::Backend(format!("landing: unsupported iceberg primitive {other:?}").into())),
    })
}
```

**Implementer notes (resolve against the real crate APIs):**
- Match the crate's error constructor: `backend(...)` / `ControlPlaneError::Backend(...)` are how `iceberg_inline.rs` builds `control_plane_core::Result` errors — copy that exact pattern (see `iceberg_inline.rs`'s `backend` import / `ControlPlaneError`).
- `iceberg_physical_type` returns e.g. `"int"`/`"long"`; `primitive_from` maps those to `iceberg::spec::PrimitiveType`. (We could instead reuse any existing logical→`PrimitiveType` helper if one exists — check `iceberg_type.rs`; if so, use it and drop `primitive_from`.)
- `TableCreation::builder()` here omits an explicit `.location(...)`, relying on the catalog's warehouse-relative default (the vendored `create_table` falls back to `<warehouse>/<ns>/<name>` when `location` is `None` — confirmed in `catalog.rs:create_table`). The seeder set an explicit location; the default is correct for production.
- `current_snapshot` is the `control_plane_core::Catalog` trait method on `IcebergCatalog`; import the trait.
- `decode_ipc_57`'s `batches` order matches `columns` order because both derive from the same wire schema (the inline contract's positional requirement holds).

Add to `src/control-plane/postgres/src/lib.rs`:

```rust
pub mod iceberg_landing;
```

(Place it alphabetically/with the other `iceberg_*` module declarations.)

- [ ] **Step 5: Add the test target**

In `src/control-plane/postgres/BUCK`, add (mirror `iceberg-writer`'s `loom_fixture_test`, adding the arrow-ipc/select test deps):

```python
loom_fixture_test(
    name = "iceberg-landing",
    crate = "iceberg_landing",
    srcs = ["tests/iceberg_landing.rs"],
    crate_root = "tests/iceberg_landing.rs",
    named_deps = {"parquet57": "//third-party:parquet57"},
    deps = [
        ":postgres",
        "//src/control-plane/core:core",
        "//third-party:arrow-array",
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

(Match `duckdb = True`/`postgres` fixture flags to what `iceberg-writer` uses — copy its attributes. If `make_catalog`/fixture helpers are duplicated, that's fine for tests.)

- [ ] **Step 6: Build, test, format**

```bash
buck2 build //src/control-plane/postgres:postgres > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|error\[" /tmp/b.log
buck2 test //src/control-plane/postgres:iceberg-landing > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
buck2 run //tools:rustfmt -- src/control-plane/postgres/src/iceberg_landing.rs src/control-plane/postgres/tests/iceberg_landing.rs
```
Expected: build succeeds; both landing tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/control-plane/postgres/src/iceberg_landing.rs src/control-plane/postgres/src/lib.rs \
        src/control-plane/postgres/Cargo.toml Cargo.lock third-party/BUCK \
        src/control-plane/postgres/BUCK src/control-plane/postgres/tests/iceberg_landing.rs
git commit -m "feat(iceberg): iceberg_landing — IPC decode, byte routing, inline/parquet

land() decodes an Arrow-57 IPC body, routes by in-memory size to inline_append
(<= limit) or a create-if-absent Parquet append_batches_with_lineage, and returns
the mirror snapshot id. Both branches emit lineage atomically. Adds arrow-ipc /
arrow-select (57) deps.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: `landing.rs` — backend enum, port, request (ingest)

**Files:**
- Create: `src/services/ingest/src/landing.rs`
- Modify: `src/services/ingest/src/lib.rs` (`pub mod landing;`)
- Test: `src/services/ingest/tests/landing.rs` (new `rust_test`, pure unit — RE-eligible)

This task adds only the backend enum/parser, the trait, and `LandRequest` — no impls yet (they come in Tasks 5-6), so it compiles and the parse test runs without Postgres.

- [ ] **Step 1: Write the failing test**

Create `src/services/ingest/tests/landing.rs`:

```rust
use ingest::landing::{parse_landing_backend, LandingBackend};

#[test]
fn parses_default_and_variants() {
    assert_eq!(parse_landing_backend(None).unwrap(), LandingBackend::DuckLake);
    assert_eq!(parse_landing_backend(Some("")).unwrap(), LandingBackend::DuckLake);
    assert_eq!(parse_landing_backend(Some("ducklake")).unwrap(), LandingBackend::DuckLake);
    assert_eq!(parse_landing_backend(Some("ICEBERG")).unwrap(), LandingBackend::Iceberg);
    assert!(parse_landing_backend(Some("delta")).is_err());
}
```

- [ ] **Step 2: Verify it fails**

Run: `buck2 test //src/services/ingest:landing > /tmp/t.log 2>&1; grep -E "error|FAIL|cannot find" /tmp/t.log`
(Target added in Step 4.) Expected: FAIL — module/target missing.

- [ ] **Step 3: Implement `landing.rs` (enum, parser, trait, request)**

Create `src/services/ingest/src/landing.rs`:

```rust
//! Landing backend selection + the `LandingMaterializer` port. `DuckLakeMaterializer`
//! (Task 5) preserves today's path; `IcebergMaterializer` (Task 6) forwards to the
//! postgres-crate `iceberg_landing` entrypoint. Selected in `main` by
//! `LOOM_LANDING_BACKEND`, mirroring query-api's `LOOM_SERVING_BACKEND`.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use async_trait::async_trait;
use control_plane_core::{ColumnSpec, LineageEvent, SnapshotId, TableRef};

use crate::IngestError;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LandingBackend {
    /// DuckLake via DataFusion Parquet write (default; today's behaviour).
    DuckLake,
    /// Iceberg via the loom-native landing path (inline + Parquet).
    Iceberg,
}

/// Parse `LOOM_LANDING_BACKEND`. Unset/empty -> DuckLake. Case-insensitive.
pub fn parse_landing_backend(v: Option<&str>) -> Result<LandingBackend, String> {
    match v.map(|s| s.trim().to_ascii_lowercase()).as_deref() {
        None | Some("") | Some("ducklake") => Ok(LandingBackend::DuckLake),
        Some("iceberg") => Ok(LandingBackend::Iceberg),
        Some(other) => Err(format!(
            "LOOM_LANDING_BACKEND must be 'ducklake' or 'iceberg', got {other:?}"
        )),
    }
}

/// One landing request: gate already passed, `columns` resolved, lineage built.
pub struct LandRequest<'a> {
    pub table: &'a TableRef,
    pub schema: Arc<Schema>,        // arrow-58 schema (DuckLake path)
    pub columns: &'a [ColumnSpec],  // resolved physical schema
    pub batches: &'a [RecordBatch], // arrow-58 batches (DuckLake path)
    pub ipc_body: &'a [u8],         // raw Arrow IPC body (Iceberg path, decoded arrow-57)
    pub file_prefix: &'a str,
    pub lineage: LineageEvent,
}

#[async_trait]
pub trait LandingMaterializer: Send + Sync {
    /// Land `req` and return the new snapshot id.
    async fn land(&self, req: LandRequest<'_>) -> Result<SnapshotId, IngestError>;
}
```

Add to `src/services/ingest/src/lib.rs`: `pub mod landing;` (with the other `pub mod`s). Confirm `IngestError`, `ColumnSpec` are already exported/reachable (they are — `materialize.rs` uses both).

- [ ] **Step 4: Add the test target + async-trait dep**

In `src/services/ingest/BUCK`, add `"//third-party:async-trait"` to the `ingest` `rust_library` `deps`, and add:

```python
rust_test(
    name = "landing",
    crate = "landing",
    srcs = ["tests/landing.rs"],
    crate_root = "tests/landing.rs",
    edition = "2024",
    deps = [":ingest"],
)
```

Also add `async-trait` to `src/services/ingest/Cargo.toml` `[dependencies]` and run `buck2 run //tools:reindeer -- update && ./tools/buckify.sh` if the lock lacks it (it's already vendored, so likely a no-op for `third-party/BUCK`).

- [ ] **Step 5: Build, test, format, commit**

```bash
buck2 test //src/services/ingest:landing > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
buck2 run //tools:rustfmt -- src/services/ingest/src/landing.rs src/services/ingest/tests/landing.rs
git add src/services/ingest/src/landing.rs src/services/ingest/src/lib.rs src/services/ingest/BUCK \
        src/services/ingest/Cargo.toml Cargo.lock third-party/BUCK src/services/ingest/tests/landing.rs
git commit -m "feat(ingest): LandingBackend enum + LandingMaterializer port

parse_landing_backend(LOOM_LANDING_BACKEND) mirrors query-api's serving-backend
parser; LandingMaterializer + LandRequest are the seam DuckLake/Iceberg impls
plug into. No impls yet.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: `DuckLakeMaterializer` + handler refactor (ingest)

**Files:**
- Modify: `src/services/ingest/src/landing.rs` (add `DuckLakeMaterializer`)
- Modify: `src/services/ingest/src/materialize.rs` (split gate/schema head from write tail)
- Modify: `src/services/ingest/src/http.rs` (resolve gate/columns/lineage in handler; `AppState { materializer }`)
- Tests: existing `tests/http_land.rs`, `tests/materialize.rs` must still pass (behaviour-preserving)

The goal: today's `materialize()` becomes (a) handler-side gate+schema resolution and (b) `DuckLakeMaterializer::land` (the write tail), with zero behaviour change. The existing http/materialize tests are the regression gate.

- [ ] **Step 1: Extract gate + column resolution into reusable helpers in `materialize.rs`**

In `src/services/ingest/src/materialize.rs`, split `materialize` so the gate check + column resolution are callable from the handler, and the write tail is callable from `DuckLakeMaterializer`. Replace the current `materialize` with two functions:

```rust
/// Validate the optional gate and resolve the physical schema. Backend-agnostic;
/// the handler calls this once before dispatch.
pub fn resolve_columns(
    schema: &Schema,
    gate: Option<&ModelShape>,
) -> Result<Vec<ColumnSpec>, IngestError> {
    if let Some(shape) = gate {
        validate(shape, schema).map_err(IngestError::DoesNotConform)?;
    }
    Ok(match gate {
        Some(shape) => shape
            .columns
            .iter()
            .map(|c| ColumnSpec { name: c.name.clone(), ty: c.ty.clone(), nullable: !c.required })
            .collect(),
        None => infer_columns(schema)?,
    })
}

/// The DuckLake write tail: DataFusion Parquet write + one atomic cp tx.
pub async fn land_ducklake(
    cp: &dyn ControlPlane,
    object_store: Arc<dyn ObjectStore>,
    table: &TableRef,
    schema: Arc<Schema>,
    columns: &[ColumnSpec],
    batches: &[RecordBatch],
    file_prefix: &str,
    lineage: LineageEvent,
) -> Result<SnapshotId, IngestError> {
    let dir_prefix = format!("{}/{}/{}", table.schema, table.name, file_prefix);
    let files = write_dataset(object_store, &dir_prefix, schema, batches, &WriteConfig::default()).await?;
    let data_files: Vec<DataFile> = files
        .into_iter()
        .map(|f| DataFile {
            path: f.path,
            path_is_relative: true,
            file_format: control_plane_core::FileFormat::Parquet,
            record_count: f.record_count,
            file_size_bytes: f.file_size_bytes,
            column_stats: f.column_stats,
            parquet_footer_size: Some(f.footer_size),
        })
        .collect();
    let mut tx = cp.begin().await?;
    tx.create_table(table, columns).await?;
    tx.append_files(table, &data_files).await?;
    tx.emit(lineage).await?;
    tx.commit().await?.ok_or(IngestError::NoSnapshot)
}
```

Keep `MaterializeRequest` only if other code still needs it; otherwise remove it (the handler now passes discrete args). Update imports as needed (`ColumnSpec`, `TableRef`, etc. already imported).

- [ ] **Step 2: Add `DuckLakeMaterializer` in `landing.rs`**

```rust
use control_plane_core::ControlPlane;
use object_store::ObjectStore;

use crate::materialize::land_ducklake;

pub struct DuckLakeMaterializer {
    pub cp: Arc<dyn ControlPlane>,
    pub store: Arc<dyn ObjectStore>,
}

#[async_trait]
impl LandingMaterializer for DuckLakeMaterializer {
    async fn land(&self, req: LandRequest<'_>) -> Result<SnapshotId, IngestError> {
        land_ducklake(
            self.cp.as_ref(),
            self.store.clone(),
            req.table,
            req.schema.clone(),
            req.columns,
            req.batches,
            req.file_prefix,
            req.lineage,
        )
        .await
    }
}
```

- [ ] **Step 3: Refactor the handler (`http.rs`)**

Change `AppState` to carry the materializer:

```rust
#[derive(Clone)]
pub struct AppState {
    pub materializer: Arc<dyn LandingMaterializer>,
}
```

In `land`, after decoding (`schema`, `batches`) and building `gate`/`run_id`/`table`/`file_prefix`/`lineage`, resolve columns and dispatch:

```rust
    let columns = match crate::materialize::resolve_columns(&schema, gate.as_ref()) {
        Ok(c) => c,
        Err(IngestError::DoesNotConform(violations)) => {
            return (StatusCode::UNPROCESSABLE_ENTITY, Json(violations_json(&violations))).into_response();
        }
        Err(IngestError::Infer(_)) => {
            return (StatusCode::BAD_REQUEST, "unsupported column type").into_response();
        }
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    };

    let req = LandRequest {
        table: &table,
        schema: schema.clone(),
        columns: &columns,
        batches: &batches,
        ipc_body: &body,        // raw IPC bytes for the Iceberg path
        file_prefix: &file_prefix,
        lineage,
    };

    match st.materializer.land(req).await {
        Ok(snap) => Json(serde_json::json!({
            "snapshot_id": snap.0,
            "dataset": format!("{}.{}", table.schema, table.name),
        }))
        .into_response(),
        Err(IngestError::DoesNotConform(violations)) =>
            (StatusCode::UNPROCESSABLE_ENTITY, Json(violations_json(&violations))).into_response(),
        Err(IngestError::Infer(_)) =>
            (StatusCode::BAD_REQUEST, "unsupported column type").into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    }
```

Add `use crate::landing::{LandRequest, LandingMaterializer};`. Note `body: Bytes` is already in scope; `&body` derefs to `&[u8]`. Keep the gate/run-id parsing exactly as-is.

- [ ] **Step 4: Update existing tests' construction of `AppState`**

`tests/http_land.rs` builds an `AppState { cp, store }` — change those sites to wrap a `DuckLakeMaterializer`:

```rust
let state = AppState {
    materializer: std::sync::Arc::new(ingest::landing::DuckLakeMaterializer { cp, store }),
};
```

`tests/materialize.rs` calls `materialize(...)` directly — repoint it to `land_ducklake(...)` + `resolve_columns(...)` (or keep a thin `materialize` shim that calls both, to minimise test churn — implementer's choice, but prefer updating the test to the new API). Add `//third-party:async-trait` to those test targets' deps if needed.

- [ ] **Step 5: Build, test, format**

```bash
buck2 test //src/services/ingest:http-land //src/services/ingest:materialize //src/services/ingest:gate > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
buck2 run //tools:rustfmt -- src/services/ingest/src/landing.rs src/services/ingest/src/materialize.rs src/services/ingest/src/http.rs src/services/ingest/tests/http_land.rs src/services/ingest/tests/materialize.rs
```
Expected: all pass — behaviour is preserved, only the seam changed.

- [ ] **Step 6: Commit**

```bash
git add src/services/ingest/src/landing.rs src/services/ingest/src/materialize.rs src/services/ingest/src/http.rs \
        src/services/ingest/tests/http_land.rs src/services/ingest/tests/materialize.rs src/services/ingest/BUCK
git commit -m "refactor(ingest): route landing through LandingMaterializer (DuckLake impl)

Gate + column resolution move into the handler; the DuckLake write tail becomes
DuckLakeMaterializer::land. AppState now carries Arc<dyn LandingMaterializer>.
Behaviour-preserving — existing http/materialize tests green. Handler threads the
raw IPC body for the upcoming Iceberg path.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: `IcebergMaterializer` (ingest) + end-to-end fixture test

**Files:**
- Modify: `src/services/ingest/src/landing.rs` (add `IcebergMaterializer`)
- Modify: `src/services/ingest/BUCK` (lib deps: postgres, iceberg, sqlx; new fixture test)
- Modify: `src/services/ingest/Cargo.toml` (postgres, iceberg, sqlx deps) + buckify
- Test: `src/services/ingest/tests/iceberg_land.rs` (new `loom_fixture_test`)

- [ ] **Step 1: Add the deps**

In `src/services/ingest/Cargo.toml` add `control-plane-postgres` (path dep), `iceberg`, `sqlx` (match the workspace versions used by the postgres crate). Run `buck2 run //tools:reindeer -- update && ./tools/buckify.sh`. In `src/services/ingest/BUCK`, add to the `ingest` `rust_library` `deps`:

```python
        "//src/control-plane/postgres:postgres",
        "//third-party:iceberg",
        "//third-party:sqlx",
```

- [ ] **Step 2: Write the failing end-to-end test**

Create `src/services/ingest/tests/iceberg_land.rs` (`loom_fixture_test`). POST a small Arrow IPC body through the real router with an Iceberg-backed `AppState`, assert 200 + `snapshot_id`, and that lineage + mirror reflect it. Model the request plumbing on `tests/http_land.rs` (tower `oneshot`), and the SqlCatalog/pool/fixture setup on the postgres `iceberg_landing` test.

```rust
// Build an IcebergMaterializer-backed AppState:
let materializer = std::sync::Arc::new(ingest::landing::IcebergMaterializer {
    catalog: std::sync::Arc::new(make_catalog(fx.pg_dsn(&db), &warehouse).await),
    pool: fx.pool(&db).await,
    inline_byte_limit: 16 * 1024 * 1024,
});
let app = ingest::http::router(ingest::http::AppState { materializer });
// POST /datasets/wh/t with a small arrow-58 IPC body (StreamWriter, arrow-58 in this crate)
// assert 200, body has snapshot_id; then assert cp.events_for(run).len() == 1
```

- [ ] **Step 3: Verify it fails**

Run: `buck2 test //src/services/ingest:iceberg-land > /tmp/t.log 2>&1; grep -E "error|cannot find|FAIL" /tmp/t.log`
Expected: FAIL — `IcebergMaterializer` not found.

- [ ] **Step 4: Implement `IcebergMaterializer`**

In `src/services/ingest/src/landing.rs`:

```rust
use control_plane_postgres::iceberg_landing::land as iceberg_land;
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use sqlx::PgPool;

pub struct IcebergMaterializer {
    pub catalog: Arc<SqlCatalog>,
    pub pool: PgPool,
    pub inline_byte_limit: usize,
}

#[async_trait]
impl LandingMaterializer for IcebergMaterializer {
    async fn land(&self, req: LandRequest<'_>) -> Result<SnapshotId, IngestError> {
        iceberg_land(
            &self.pool,
            &self.catalog,
            req.table,
            req.columns,
            req.ipc_body,
            self.inline_byte_limit,
            req.lineage,
        )
        .await
        .map_err(IngestError::from) // map control_plane_core::Error -> IngestError
    }
}
```

**Implementer:** confirm `IngestError` has a `From<control_plane_core::Error>` (or `ControlPlaneError`) arm — `materialize.rs`'s `cp.begin()?` path implies one exists (`tx.commit().await?`). If not, add a `#[from]` variant. Ensure `SqlCatalog` is exported from the postgres crate (`pub use`/`pub mod iceberg_sql_catalog` with `pub struct SqlCatalog`).

- [ ] **Step 5: Add the fixture test target**

In `src/services/ingest/BUCK`, mirror `runtime-land`'s `loom_fixture_test` (duckdb/postgres fixture flags) with deps including `:ingest`, `//src/control-plane/postgres:postgres`, `//third-party:{arrow,iceberg,sqlx,axum,http-body-util,tower,serde_json,tokio,uuid,time,tempfile}`.

- [ ] **Step 6: Build, test, format, commit**

```bash
buck2 test //src/services/ingest:iceberg-land > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
buck2 run //tools:rustfmt -- src/services/ingest/src/landing.rs src/services/ingest/tests/iceberg_land.rs
git add src/services/ingest/src/landing.rs src/services/ingest/BUCK src/services/ingest/Cargo.toml \
        Cargo.lock third-party/BUCK src/services/ingest/tests/iceberg_land.rs
git commit -m "feat(ingest): IcebergMaterializer forwards to iceberg_landing

Thin forwarder passing the raw IPC body + resolved columns + lineage + byte limit
to control_plane_postgres::iceberg_landing::land; arrow-57 stays inside the
postgres crate. End-to-end fixture test: small POST inlines, emits lineage.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: `main.rs` wiring — env reads + materializer construction (ingest)

**Files:**
- Modify: `src/services/ingest/src/main.rs`
- Modify: `src/services/ingest/BUCK` (`ingest-bin` deps: postgres, iceberg, sqlx if needed)

- [ ] **Step 1: Wire backend selection in `main`**

Replace `src/services/ingest/src/main.rs` body with backend selection mirroring query-api's `main`:

```rust
use std::sync::Arc;

use ingest::http::{router, AppState};
use ingest::landing::{parse_landing_backend, DuckLakeMaterializer, IcebergMaterializer, LandingBackend, LandingMaterializer};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = service_runtime::Config::from_env()?;
    let pool = service_runtime::build_pool(&cfg.db).await?;

    let backend = parse_landing_backend(std::env::var("LOOM_LANDING_BACKEND").ok().as_deref())
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

    let materializer: Arc<dyn LandingMaterializer> = match backend {
        LandingBackend::DuckLake => {
            let cp = Arc::new(service_runtime::control_plane(pool, cfg.lock_timeout));
            let store = Arc::new(service_runtime::local_store(&cfg.data_path)?);
            Arc::new(DuckLakeMaterializer { cp, store })
        }
        LandingBackend::Iceberg => {
            let limit = std::env::var("LOOM_INLINE_BYTE_LIMIT")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(16 * 1024 * 1024);
            let catalog = Arc::new(build_iceberg_catalog(&cfg).await?);
            Arc::new(IcebergMaterializer { catalog, pool, inline_byte_limit: limit })
        }
    };

    let app = router(AppState { materializer });
    service_runtime::serve(cfg.bind_addr, app).await?;
    Ok(())
}
```

Add `build_iceberg_catalog` constructing the vendored `SqlCatalog` from the config (DSN + `file://` warehouse), mirroring the fixture's `catalog()` helper:

```rust
async fn build_iceberg_catalog(
    cfg: &service_runtime::Config,
) -> Result<control_plane_postgres::iceberg_sql_catalog::SqlCatalog, Box<dyn std::error::Error>> {
    use control_plane_postgres::iceberg_sql_catalog::{
        SqlCatalogBuilder, LocalFsStorageFactory, SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE,
    };
    use iceberg::CatalogBuilder;

    let mut props = std::collections::HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), cfg.db.dsn());           // confirm DSN accessor
    props.insert(SQL_CATALOG_PROP_WAREHOUSE.to_string(), format!("file://{}", cfg.data_path.display()));
    let catalog = SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("loom", props)
        .await?;
    Ok(catalog)
}
```

**Implementer:** confirm the exact DSN accessor on `service_runtime`'s `DbConfig` (the fixture uses `fx.pg_dsn`; `DbConfig` likely has a libpq/url method — grep `ducklake_libpq`/`dsn`/`url` in `src/services/runtime/`). Confirm `LocalFsStorageFactory`, `SqlCatalogBuilder`, and the `SQL_CATALOG_PROP_*` consts are re-exported from `control_plane_postgres::iceberg_sql_catalog` (export them from the module if not). Iceberg's `CatalogBuilder` trait must be in scope for `.load(...)`.

- [ ] **Step 2: Add bin deps**

In `src/services/ingest/BUCK`, add to `ingest-bin` `deps`: `//src/control-plane/postgres:postgres`, `//third-party:iceberg` (and `//third-party:sqlx` if the bin names `PgPool`/types directly — it passes `pool` through, so likely needed). Build to find the minimal set.

- [ ] **Step 3: Build the binary + full ingest suite**

```bash
buck2 build //src/services/ingest:ingest-bin > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|error\[" /tmp/b.log
buck2 test //src/services/ingest/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
buck2 run //tools:rustfmt -- src/services/ingest/src/main.rs
```
Expected: binary builds; all ingest tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/services/ingest/src/main.rs src/services/ingest/BUCK
git commit -m "feat(ingest): select landing backend in main via LOOM_LANDING_BACKEND

Default ducklake (unchanged path); iceberg builds a vendored SqlCatalog from the
DSN + file:// warehouse and reads LOOM_INLINE_BYTE_LIMIT (default 16 MiB).

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 8: Roadmap update + full-suite verification + final review

**Files:**
- Modify: `docs/spike/ICEBERG_ROADMAP.md`

- [ ] **Step 1: Mark Slice B done in the roadmap**

In `docs/spike/ICEBERG_ROADMAP.md`, under "Done", add a Slice B entry (selection via `LOOM_LANDING_BACKEND`, byte-size routing at 16 MiB default, atomic Parquet lineage via `do_update_table`/`LineageEmittingCatalog`, arrow-57 logic in the postgres `iceberg_landing` module). Remove item #3 (write/ingest service wiring) and the "inline-vs-Parquet threshold + LandingBackend wiring" follow-up from the "Left"/Slice A remaining lists. Keep flush/compaction deferred. End the file with exactly one trailing newline, no trailing whitespace (markdown-lint).

- [ ] **Step 2: Full first-party suite (the real gate)**

```bash
buck2 build //src/... > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|error\[" /tmp/b.log
buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
./tools/clippy-all.sh > /tmp/c.log 2>&1; grep -iE "warning|error|clean|^$" /tmp/c.log | tail -5
```
Expected: build succeeds; whole suite passes (incl. the sqlx-cache-check test — if it fails, run `./tools/sqlx-prepare.sh` and commit the `.sqlx` change); clippy clean.

- [ ] **Step 3: Commit the roadmap + any sqlx refresh**

```bash
git add docs/spike/ICEBERG_ROADMAP.md
git commit -m "docs(iceberg): mark slice B (landing backend) done

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 4: Final code review**

Dispatch a final code-reviewer over the whole branch (`git diff origin/main...HEAD`), focused on: the atomic-lineage tx (no lineage on rollback path / no double-emit on retry), the arrow-57/58 boundary (no type leakage into ingest), behaviour-preservation of the DuckLake path, and BUCK/Cargo dep hygiene (reindeer in sync). Address Critical/Important findings before finishing.

---

## Self-Review notes (for the controller)

- **Spec coverage:** §1 selection → T4/T7; §2 port + DuckLake → T4/T5; §3 routing → T3; §4 atomic lineage → T1/T2; §5 create-if-absent → T3; §6 snapshot id → T3; cross-major boundary → T3 (postgres) + T6 (forwarder); all 8 spec tests → T2/T3/T4/T6 (+ existing in T5).
- **Risk order:** the load-bearing, highest-uncertainty work (catalog refactor + decorator + arrow-57 deps) lands first (T1-T3) behind fixture tests, so a problem surfaces before the ingest wiring depends on it.
- **Dep churn:** T3 and T6 touch `Cargo.toml`/`third-party/BUCK`; each runs buckify and commits the generated diff so `reindeer-check` stays green. Watch for the two-public-majors alias case on arrow-ipc/arrow-select (parquet57 named-dep precedent is the fix).
