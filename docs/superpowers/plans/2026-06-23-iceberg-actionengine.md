# Iceberg `ActionEngine` (governed object writes) — Implementation Plan

> **For agentic workers:** implement this plan task-by-task under TDD (write the
> failing test first, then the code). Steps use checkbox (`- [ ]`) syntax.

**Goal:** Replace the query-api Iceberg serving backend's `UnsupportedActionEngine`
with an `IcebergActionWriter` that routes a governed typed-insert through the
existing atomic inline-write seam (`control_plane_postgres::iceberg_landing::land`),
so a `LOOM_SERVING_BACKEND=iceberg` deployment accepts governed actions, commits the
row and its lineage in one Postgres transaction, and reads the row back immediately
through the Iceberg DataFusion serving engine. Closes `road-iceberg-actionengine`.

**Spec:** `docs/superpowers/specs/2026-06-22-iceberg-actionengine-design.md`

**Architecture:** The `ActionEngine` trait (`src/services/query-api/src/serving.rs:197`)
is format-neutral and Arrow-free (`columns`/`values`/`logical_types` + a
`LineageEvent`). The DuckLake impl `DuckLakeActionWriter` builds a one-row batch with
`build_object_batch` and lands it via `ingest::materialize::land_ducklake`. The new
`IcebergActionWriter` builds the *same* one-row batch with the *same*
`build_object_batch` (already `pub fn` in `crate::serving` — **no lift refactor
needed**, the spec's only conditional refactor does not apply), encodes it to an
Arrow IPC stream (arrow-58 `StreamWriter`), and forwards it to
`iceberg_landing::land` — the identical entrypoint ingest's `IcebergMaterializer`
uses (`src/services/ingest/src/landing.rs:104`). A single action row is far below the
inline byte limit, so it lands as a mirror-only inline row (one PG tx with lineage),
drained to real Parquet later by the existing flush vertical. ACL write-enforcement
is unchanged — it runs in the action handler *before* the engine, on both backends.

**Tech Stack:** Rust, buck2, arrow-58 (IPC writer), sqlx, the vendored iceberg SQL
catalog. Tests are `rust_test` integration targets (NO inline `#[cfg(test)]`);
fixture-backed tests use `loom_fixture_test`.

## Global Constraints

- **Tests are `rust_test` integration targets only** — NO inline `#[test]`/
  `#[tokio::test]` in `src/**.rs` (the `no-inline-tests` prek hook fails otherwise).
- **Fixture tests use `loom_fixture_test`** (`src/control-plane/postgres/defs.bzl`),
  NOT a bare `rust_test`, or they route to remote execution and fail as root. The
  Iceberg e2e boots hermetic Postgres + a LocalFs warehouse → `loom_fixture_test`
  **without** `duckdb = True` (it uses no DuckDB), mirroring `iceberg-pruning-e2e`.
- **No new third-party crate is added** — `iceberg`, `sqlx`, `arrow` (with `ipc`),
  `tempfile` are all already in `third-party/BUCK`. So **no `buckify.sh` run** and the
  `Cargo.lock`/`duckdb`-downgrade footgun does not apply.
- **No DuckLake behavior changes** — the DuckLake action path, defaults, and
  `DuckLakeActionWriter` are untouched. The default serving backend stays DuckLake.
- **Run tests with the file-redirect pattern** (never pipe `buck2 test` through
  `tail`/`head`): `buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests
  finished|FAIL|error\[" /tmp/t.log`.
- Commit messages: Conventional Commits.

---

### Task 1: `IcebergActionWriter` + `encode_ipc_stream` + unit shaping test

Add the writer and the IPC-encode helper to `serving_datafusion.rs`, alongside (not
yet replacing) `UnsupportedActionEngine`, so the crate keeps building. TDD: the unit
shaping test first.

**Files:**
- Modify: `src/services/query-api/src/serving_datafusion.rs`
- Modify: `src/services/query-api/BUCK` — add `//third-party:sqlx` to `:query-api`
  deps; add the `iceberg-action-ipc` rust_test target
- Create: `src/services/query-api/tests/iceberg_action_ipc.rs`

**Interfaces produced (used by Tasks 2–3):**
- `pub fn encode_ipc_stream(batch: &arrow::array::RecordBatch) -> Result<Vec<u8>, ServingError>`
- `pub struct IcebergActionWriter` with
  `pub fn new(catalog: Arc<SqlCatalog>, pool: PgPool, inline_byte_limit: usize, flush_byte_threshold: i64) -> Self`
  and `impl ActionEngine for IcebergActionWriter`

- [ ] **Step 1: Write the failing unit test**

Create `src/services/query-api/tests/iceberg_action_ipc.rs`:

```rust
//! IcebergActionWriter's batch->IPC shaping: build_object_batch + encode_ipc_stream
//! produce an Arrow IPC stream that round-trips back to the same one-row batch.
//! Pure logic (no DB).

use arrow::array::{Array, Int64Array, StringArray};
use arrow::ipc::reader::StreamReader;
use query_api::serving::{SqlValue, build_object_batch};
use query_api::serving_datafusion::encode_ipc_stream;

#[test]
fn batch_encodes_to_ipc_and_round_trips() {
    let (_schema, batch, specs) = build_object_batch(
        &["id".into(), "name".into()],
        &[SqlValue::Int(42), SqlValue::Text("gadget".into())],
        &["Long".into(), "String".into()],
    )
    .expect("batch");
    // ColumnSpec.ty is the loom logical canonical name (what land() consumes).
    assert_eq!(
        specs.iter().map(|s| s.ty.as_str()).collect::<Vec<_>>(),
        vec!["long", "string"]
    );

    let body = encode_ipc_stream(&batch).expect("encode");
    let mut reader = StreamReader::try_new(std::io::Cursor::new(body), None).expect("reader");
    let decoded = reader.next().expect("one batch").expect("ok");
    assert!(reader.next().is_none(), "single-batch stream");
    assert_eq!(decoded.num_rows(), 1);
    assert_eq!(decoded.num_columns(), 2);
    let id = decoded.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id.value(0), 42);
    let name = decoded.column(1).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(name.value(0), "gadget");
}
```

Add to `src/services/query-api/BUCK`:

```python
rust_test(
    name = "iceberg-action-ipc",
    crate = "iceberg_action_ipc",
    srcs = ["tests/iceberg_action_ipc.rs"],
    crate_root = "tests/iceberg_action_ipc.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//third-party:arrow",
    ],
)
```

- [ ] **Step 2: Add the imports + `encode_ipc_stream` + `IcebergActionWriter`**

In `src/services/query-api/src/serving_datafusion.rs`, add to the `use` block:

```rust
use control_plane_postgres::iceberg_landing;
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use sqlx::PgPool;

use crate::serving::build_object_batch;
```

(`RecordBatch`, `TableRef`, `ActionEngine`, `ServingError`, `SqlValue`, `to_serving`,
`async_trait`, `Arc` are already in scope in this file.)

Add the encode helper near `to_serving` (it reuses `to_serving` for error mapping):

```rust
/// Encode a (one-row) arrow-58 `RecordBatch` to an Arrow IPC *stream* body — the
/// bytes `iceberg_landing::land` decodes in arrow-57 (the established cross-major
/// IPC boundary ingest already crosses). Any writer error maps to an opaque
/// serving error.
pub fn encode_ipc_stream(batch: &RecordBatch) -> Result<Vec<u8>, ServingError> {
    let mut buf = Vec::new();
    {
        let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema())
            .map_err(to_serving)?;
        w.write(batch).map_err(to_serving)?;
        w.finish().map_err(to_serving)?;
    }
    Ok(buf)
}
```

Replace the `UnsupportedActionEngine` doc-comment region by ADDING the new writer
just above it (leave `UnsupportedActionEngine` itself in place until Task 2):

```rust
/// The `ActionEngine` for the Iceberg serving backend: a governed typed-insert is
/// built into the same one-row batch the DuckLake writer uses, encoded to Arrow IPC,
/// and forwarded to the atomic inline-write seam `iceberg_landing::land`. A single
/// action row inlines (mirror-only typed rows): one Postgres transaction committing
/// the row and its lineage together, drained to real Parquet later by the flush
/// vertical. Holds the same dependencies as ingest's `IcebergMaterializer`.
pub struct IcebergActionWriter {
    catalog: Arc<SqlCatalog>,
    pool: PgPool,
    inline_byte_limit: usize,
    flush_byte_threshold: i64,
}

impl IcebergActionWriter {
    pub fn new(
        catalog: Arc<SqlCatalog>,
        pool: PgPool,
        inline_byte_limit: usize,
        flush_byte_threshold: i64,
    ) -> Self {
        Self {
            catalog,
            pool,
            inline_byte_limit,
            flush_byte_threshold,
        }
    }
}

#[async_trait]
impl ActionEngine for IcebergActionWriter {
    async fn write_object(
        &self,
        table: &TableRef,
        columns: &[String],
        values: &[SqlValue],
        logical_types: &[String],
        event: control_plane_core::LineageEvent,
    ) -> Result<control_plane_core::SnapshotId, ServingError> {
        let (_schema, batch, specs) = build_object_batch(columns, values, logical_types)?;
        let ipc_body = encode_ipc_stream(&batch)?;
        iceberg_landing::land(
            &self.pool,
            &self.catalog,
            table,
            &specs,
            &ipc_body,
            self.inline_byte_limit,
            self.flush_byte_threshold,
            event,
        )
        .await
        .map_err(|e| ServingError::Engine(e.to_string()))
    }
}
```

Add `//third-party:sqlx` to the `deps` of the `:query-api` `rust_library` rule (it
is needed to name `sqlx::PgPool`).

- [ ] **Step 3: Run the unit test + clippy**

```
buck2 test //src/services/query-api:iceberg-action-ipc > /tmp/t.log 2>&1
grep -E "Tests finished|FAIL|error\[" /tmp/t.log
buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log
```

---

### Task 2: Wire the Iceberg branch in `main.rs`; remove `UnsupportedActionEngine`

**Files:**
- Modify: `src/services/query-api/src/main.rs`
- Modify: `src/services/query-api/src/serving_datafusion.rs` — delete
  `UnsupportedActionEngine`
- Delete: `src/services/query-api/tests/unsupported_action.rs`
- Modify: `src/services/query-api/BUCK` — add `//third-party:iceberg` to
  `:query-api-bin` deps; remove the `unsupported-action` rust_test target

- [ ] **Step 1: Rewrite the query-api Iceberg branch + add `build_iceberg_catalog`**

In `src/services/query-api/src/main.rs`:

- Add imports:
  ```rust
  use std::collections::HashMap;

  use control_plane_postgres::iceberg_sql_catalog::{
      SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
  };
  use iceberg::CatalogBuilder;
  use iceberg::io::LocalFsStorageFactory;
  ```
- Change the `serving_datafusion` import to drop `UnsupportedActionEngine` and add
  `IcebergActionWriter`:
  ```rust
  use query_api::serving_datafusion::{
      DataFusionServingEngine, IcebergActionWriter, ServingBackend, parse_serving_backend,
  };
  ```
- Add the inline/flush defaults (identical values to ingest's `main.rs`, kept local —
  the spec scopes this slice to query-api wiring; the small glue duplication with
  ingest is acceptable and out of scope to dedupe here):
  ```rust
  /// Inline routing threshold (in-memory uncompressed Arrow). Below this an action
  /// row inlines (mirror-only); tunable via `LOOM_INLINE_BYTE_LIMIT`. Matches ingest.
  const DEFAULT_INLINE_BYTE_LIMIT: usize = 16 * 1024 * 1024;
  /// Live-inline-byte total that triggers a flush, via `LOOM_FLUSH_BYTE_THRESHOLD`.
  const DEFAULT_FLUSH_BYTE_THRESHOLD: i64 = 64 * 1024 * 1024;
  ```
- Replace the `ServingBackend::Iceberg => (...)` arm with:
  ```rust
  ServingBackend::Iceberg => {
      let inline_byte_limit = std::env::var("LOOM_INLINE_BYTE_LIMIT")
          .ok()
          .and_then(|v| v.parse::<usize>().ok())
          .unwrap_or(DEFAULT_INLINE_BYTE_LIMIT);
      let flush_byte_threshold = std::env::var("LOOM_FLUSH_BYTE_THRESHOLD")
          .ok()
          .and_then(|v| v.parse::<i64>().ok())
          .unwrap_or(DEFAULT_FLUSH_BYTE_THRESHOLD);
      let catalog = Arc::new(build_iceberg_catalog(&cfg).await?);
      let action: Arc<dyn ActionEngine> = Arc::new(IcebergActionWriter::new(
          catalog,
          pool.clone(),
          inline_byte_limit,
          flush_byte_threshold,
      ));
      (
          Arc::new(DataFusionServingEngine::new(IcebergCatalog::new(pool))),
          action,
      )
  }
  ```
  (`pool.clone()` for the action writer; the bare `pool` moves into
  `IcebergCatalog::new`. Clone must come first.)
- Add the helper (copied from `src/services/ingest/src/main.rs:69`):
  ```rust
  /// Construct the vendored Iceberg SQL catalog over the same Postgres + a `file://`
  /// warehouse rooted at the service data path. Mirrors ingest's helper.
  async fn build_iceberg_catalog(
      cfg: &service_runtime::Config,
  ) -> Result<SqlCatalog, Box<dyn std::error::Error>> {
      let mut props = HashMap::new();
      props.insert(SQL_CATALOG_PROP_URI.to_string(), cfg.db.pg_url());
      props.insert(
          SQL_CATALOG_PROP_WAREHOUSE.to_string(),
          format!("file://{}", cfg.data_path.display()),
      );
      let catalog = SqlCatalogBuilder::default()
          .with_storage_factory(Arc::new(LocalFsStorageFactory))
          .load("loom", props)
          .await?;
      Ok(catalog)
  }
  ```

- [ ] **Step 2: Delete `UnsupportedActionEngine` and its test**

- Remove the `UnsupportedActionEngine` struct + `impl ActionEngine` block from
  `src/services/query-api/src/serving_datafusion.rs`.
- Delete `src/services/query-api/tests/unsupported_action.rs`.
- Remove the `unsupported-action` `rust_test` target from
  `src/services/query-api/BUCK`.
- Add `//third-party:iceberg` to the `:query-api-bin` `rust_binary` deps.

- [ ] **Step 3: Build the binary + lib + clippy**

```
buck2 build //src/services/query-api:query-api //src/services/query-api:query-api-bin > /tmp/b.log 2>&1
grep -E "FAILED|error\[|error:" /tmp/b.log
buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log
```

Confirm no remaining references to `UnsupportedActionEngine`:
```
buck2 build //src/services/query-api:query-api 2>&1 | grep -i unsupportedaction || echo "clean"
```

---

### Task 3: Iceberg action e2e (fixture)

The primary acceptance test: a governed action on the Iceberg backend lands the row +
lineage atomically and reads back through the Iceberg DataFusion serving engine; an
ungranted subject is forbidden and writes nothing; a failed write commits neither.

**Files:**
- Create: `src/services/query-api/tests/iceberg_action_e2e.rs`
- Modify: `src/services/query-api/BUCK` — add the `iceberg-action-e2e`
  `loom_fixture_test` target

- [ ] **Step 1: Write the e2e test**

Create `src/services/query-api/tests/iceberg_action_e2e.rs`:

```rust
//! Iceberg ActionEngine e2e: a governed typed-insert on the Iceberg serving backend
//! lands the row + its lineage atomically (one PG tx), reads back through the
//! loom-native DataFusion serving engine, and is governed by write-enforcement.
//! loom_fixture_test (Postgres + LocalFsStorage warehouse; no DuckDB).

use std::collections::HashMap;
use std::sync::Arc;

// `Acl` is needed in scope to call `define_subject`/`define_role`/`assign_role`/
// `grant` on the concrete `PgControlPlane`. `IcebergCatalog::live_tables` is an
// inherent method, so the `Catalog` trait is intentionally NOT imported (importing
// it unused fails the clippy/lint gate).
use control_plane_core::{
    Acl, Action, ActionDef, ActionName, ControlPlane, DatasetRef, Effect, ObjectType,
    PageReq, ParamDef, PolicyTarget, PropertyDef, RoleId, SubjectId, TableRef, TypeName,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;
use query_api::action::{ActionDeps, ActionError, run_action};
use query_api::handler::{ObjectQuery, QueryDeps, Subject, read_object};
use query_api::render::objects_to_json;
use query_api::serving::ActionEngine;
use query_api::serving_datafusion::{DataFusionServingEngine, IcebergActionWriter};
use serde_json::json;

/// Build a vendored SqlCatalog over `dsn` + a `file://warehouse` (the action writer
/// needs one even though a single inline row never touches it — `land` only uses the
/// catalog on the Parquet branch).
async fn build_catalog(dsn: &str, warehouse: &std::path::Path) -> SqlCatalog {
    let mut props = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), dsn.to_string());
    props.insert(
        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
        format!("file://{}", warehouse.display()),
    );
    SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("loom", props)
        .await
        .expect("build SqlCatalog")
}

/// Define `Widget(id Long required, name String)` + a `createWidget` insert action.
async fn define_widget(cp: &control_plane_postgres::PgControlPlane) -> TypeName {
    let widget = TypeName("Widget".into());
    cp.ontology()
        .define_type(ObjectType {
            name: widget.clone(),
            table: TableRef { schema: "main".into(), name: "widget".into() },
            properties: vec![
                PropertyDef { name: "id".into(), ty: "Long".into(), required: true },
                PropertyDef { name: "name".into(), ty: "String".into(), required: false },
            ],
            derived: vec![],
            identity: None,
        })
        .await
        .unwrap();
    cp.ontology()
        .define_action(ActionDef {
            name: ActionName("createWidget".into()),
            target: widget.clone(),
            parameters: vec![
                ParamDef { name: "id".into(), ty: "Long".into(), required: true },
                ParamDef { name: "name".into(), ty: "String".into(), required: false },
            ],
        })
        .await
        .unwrap();
    widget
}

async fn grant_writer(
    cp: &control_plane_postgres::PgControlPlane,
    widget: &TypeName,
) -> SubjectId {
    let subj = SubjectId("writer".into());
    let role = RoleId("writers".into());
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    cp.grant(&role, Action::Write, PolicyTarget::Type(widget.clone()), Effect::Allow)
        .await
        .unwrap();
    cp.grant(&role, Action::Read, PolicyTarget::Type(widget.clone()), Effect::Allow)
        .await
        .unwrap();
    subj
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn action_inserts_typed_object_readable_with_atomic_lineage() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let warehouse = tempfile::tempdir().expect("warehouse");
    let catalog = Arc::new(build_catalog(&dsn, warehouse.path()).await);

    let widget = define_widget(&cp).await;
    let subj = grant_writer(&cp, &widget).await;

    // Large flush threshold so the single inline row never enqueues a flush job.
    let engine = IcebergActionWriter::new(catalog, pool.clone(), 16 * 1024 * 1024, i64::MAX);
    let deps = ActionDeps { cp: &cp, action_engine: &engine };
    let body = json!({ "id": "42", "name": "gadget" });
    let (created, run_id) = run_action("createWidget", body.as_object().unwrap(), &subj, &deps)
        .await
        .expect("action runs");
    assert_eq!(
        objects_to_json(&created)["objects"][0],
        json!({ "id": "42", "name": "gadget" })
    );

    // Read back through the Iceberg DataFusion serving engine (inline+file union).
    let serving = DataFusionServingEngine::new(IcebergCatalog::new(pool.clone()));
    let qdeps = QueryDeps { ontology: cp.ontology(), acl: cp.acl(), serving: &serving };
    let rows = read_object(
        &ObjectQuery { type_name: "Widget".into(), eq_filters: vec![], ids: vec![] },
        &Subject(subj.clone()),
        &qdeps,
    )
    .await
    .unwrap();
    assert_eq!(
        objects_to_json(&rows)["objects"][0],
        json!({ "id": "42", "name": "gadget" }),
        "round-trips through the Iceberg serving engine"
    );

    // Lineage committed atomically with the row, findable by run_id.
    let events = cp.lineage().events_for(&run_id, PageReq::unbounded()).await.unwrap();
    assert_eq!(events.items.len(), 1, "one lineage event for the action's run");
    assert_eq!(
        events.items[0].outputs,
        vec![DatasetRef::from(&TypeName("Widget".into()))]
    );

    drop(warehouse);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ungranted_subject_is_forbidden_and_writes_nothing() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let warehouse = tempfile::tempdir().expect("warehouse");
    let catalog = Arc::new(build_catalog(&dsn, warehouse.path()).await);

    let _widget = define_widget(&cp).await; // type + action defined, NO grant.
    let engine = IcebergActionWriter::new(catalog, pool.clone(), 16 * 1024 * 1024, i64::MAX);
    let deps = ActionDeps { cp: &cp, action_engine: &engine };

    let subj = SubjectId("nobody".into());
    let err = run_action("createWidget", json!({ "id": "1" }).as_object().unwrap(), &subj, &deps)
        .await
        .unwrap_err();
    assert!(matches!(err, ActionError::Forbidden), "ungranted -> Forbidden");

    // Nothing written: enforcement short-circuits before the engine, so no mirror
    // table was ever created.
    let live = IcebergCatalog::new(pool.clone()).live_tables().await.unwrap();
    assert!(live.is_empty(), "forbidden action created no mirror table");

    drop(warehouse);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_write_commits_neither_row_nor_lineage() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let warehouse = tempfile::tempdir().expect("warehouse");
    let catalog = Arc::new(build_catalog(&dsn, warehouse.path()).await);

    let engine = IcebergActionWriter::new(catalog, pool.clone(), 16 * 1024 * 1024, i64::MAX);
    let table = TableRef { schema: "main".into(), name: "widget".into() };
    let run_id = control_plane_core::RunId(uuid::Uuid::new_v4());
    let event = control_plane_core::LineageEvent {
        run_id: run_id.clone(),
        event_type: control_plane_core::EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetRef::from(&TypeName("Widget".into()))],
        payload: json!({}),
    };

    // A value whose variant does not match its declared type fails batch-building
    // BEFORE land() — the engine's write is all-or-nothing.
    let err = engine
        .write_object(
            &table,
            &["id".to_string()],
            &[query_api::serving::SqlValue::Text("not-a-long".into())],
            &["Long".to_string()],
            event,
        )
        .await
        .expect_err("type mismatch must fail");
    match err {
        query_api::serving::ServingError::Engine(_) => {}
    }

    // Neither a mirror table nor a lineage event was committed.
    let live = IcebergCatalog::new(pool.clone()).live_tables().await.unwrap();
    assert!(live.is_empty(), "failed write created no mirror table");
    let events = cp.lineage().events_for(&run_id, PageReq::unbounded()).await.unwrap();
    assert!(events.items.is_empty(), "failed write emitted no lineage");

    drop(warehouse);
}
```

> Implementer notes: confirm the exact import paths/field names against the existing
> `tests/action_e2e.rs` and `tests/iceberg_pruning_e2e.rs` (e.g. `PgFixture::pool_for`,
> `pg_dsn`, `IcebergCatalog::live_tables`, `RunId`/`EventType`/`LineageEvent` field
> names). Adjust the `Catalog` trait import if `live_tables` resolves via a different
> trait. If `run_action`'s conformance check rejects the type-mismatch input in the
> third test before reaching the engine, drive `engine.write_object` directly (as
> written above) — it already bypasses the handler.

- [ ] **Step 2: Add the BUCK target**

In `src/services/query-api/BUCK` add (NOT `duckdb = True` — Iceberg path uses no
DuckDB, like `iceberg-pruning-e2e`):

```python
loom_fixture_test(
    name = "iceberg-action-e2e",
    crate = "iceberg_action_e2e",
    srcs = ["tests/iceberg_action_e2e.rs"],
    crate_root = "tests/iceberg_action_e2e.rs",
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:iceberg",
        "//third-party:serde_json",
        "//third-party:tempfile",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```

- [ ] **Step 3: Run the e2e**

```
buck2 test //src/services/query-api:iceberg-action-e2e > /tmp/t.log 2>&1
grep -E "Tests finished|FAIL|error\[|panicked" /tmp/t.log
```

If a dep is missing at link/compile, add the named `//third-party:*` it asks for.

---

### Task 4: Full suite, docs, commit/push, PR

- [ ] **Step 1: Full build + test sweep** (the shared-dep regression backstop — a
  green per-crate run is not enough):

```
buck2 build //src/... > /tmp/b.log 2>&1; grep -E "FAILED|error\[|error:" /tmp/b.log || echo build-clean
buck2 test //src/... > /tmp/full.log 2>&1; grep -E "Tests finished|FAIL" /tmp/full.log
```

- [ ] **Step 2: clippy + prek hooks**

```
bash tools/clippy-all.sh > /tmp/clip.log 2>&1; tail -5 /tmp/clip.log
buck2 run //tools:prek -- run --all-files > /tmp/prek.log 2>&1; tail -20 /tmp/prek.log
```

Commit any in-place fixes the hooks make.

- [ ] **Step 3: Close the register item** (via `loom-docs-update`): in
  `docs/ROADMAP.md`, flip `road-iceberg-actionengine` `- [ ]`→`- [x]`,
  `status:planned`→`status:done`, set `pr:#<n>` (after the PR exists), and append a
  one-line as-built note. Run `bash tools/docs.sh validate` to confirm grammar.

- [ ] **Step 4: Commit + push + open PR** with head branch `work/road-iceberg-actionengine`.

```
git add -A
git commit -m "feat(iceberg): governed action writes via IcebergActionWriter (close road-iceberg-actionengine)"
git push -u origin work/road-iceberg-actionengine
```
