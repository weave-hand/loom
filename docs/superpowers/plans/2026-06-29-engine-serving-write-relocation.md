# Relocate Governed Writes to engine-serving (slice 1) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the governed-write Iceberg I/O out of query-api into the engine over two new `EngineControl` unary RPCs, so query-api's **library** target no longer depends on `control-plane/postgres` while every governed-write behavior stays identical.

**Architecture:** `IcebergActionWriter` (today in `query-api/src/serving_datafusion.rs`, holding `SqlCatalog` + `PgPool`) relocates into `engine-serving`. The engine binary gains two unary RPCs — `WriteObject` and `OverwriteTable` — that decode an Arrow-IPC payload + JSON-encoded `ColumnSpec`s and lineage envelope, then call the relocated writer. query-api's `ActionEngine` impl becomes a thin wire client (`EngineActionClient`) over the existing `EngineControl` UDS channel: it builds the one-row/N-row Arrow batch and encodes it client-side (unchanged batch/IPC code), then sends a *pre-authorized* write. ACL enforcement stays in the query-api handler, pre-wire — the engine remains a governance-free I/O executor, exactly mirroring the read path.

**Tech Stack:** Rust 2024, buck2, tonic/prost (pure-Rust `protox` codegen), Arrow 58 IPC, sqlx 0.9 compile-time queries, vendored `iceberg` (git pin), hermetic Postgres fixtures (`loom_fixture_test`).

## Global Constraints

- **Tests are `rust_test` integration targets only** — never inline `#[cfg(test)]`. Fixture-backed (hermetic Postgres) tests MUST use the `loom_fixture_test` macro (`src/control-plane/postgres/defs.bzl`), loaded via `load("//src/control-plane/postgres:defs.bzl", "loom_fixture_test")`. Pure-logic tests use `rust_test` (via the `loom_rust_test` wrapper already loaded in each BUCK).
- **No new third-party crate features / no lockfile churn.** Do NOT enable `serde` on `time` or `uuid` (it forces a `cargo generate-lockfile` + `./tools/buckify.sh` with known native-crate-downgrade risk). The lineage envelope crosses the wire via a hand-written DTO (`LineageWire`) whose fields are all serde-native (`String`/`i64`/`serde_json::Value`).
- **Behavior preservation is the acceptance bar.** The existing governed-write e2e suite (`action_e2e`, `overwrite_table_e2e`, `iceberg_action_e2e`, `update_delete_e2e`, `update_delete_governance_e2e`, `update_delete_tiers_e2e`, `http_wire_e2e`) must stay green, now driving through the engine wire. The `ActionEngine` *trait* signature does not change, so `action.rs` call sites are untouched.
- **Clippy is strict** (pedantic + restriction groups; `unwrap_used`/`expect_used`/`indexing_slicing`/`panic` enforced on non-test code). Use `?`, `.map_err(...)`, and `#[expect(lint, reason="...")]` (never bare `#[allow]`). Test code is exempted from panic-safety lints via the test-target wrappers.
- **Buck test runs:** never pipe `buck2 test` through `tail`/`head`. Redirect: `buck2 test //path:target > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/t.log`.
- **The measurable outcome:** after the final task, `buck2 cquery 'deps(//src/services/query-api:query-api)'` lists neither `//src/control-plane/postgres:postgres` nor `//src/services/ingest:ingest`.

---

## File Structure

**Created:**
- `src/services/engine-serving/src/action_writer.rs` — the relocated `IcebergActionWriter` (engine-side write executor).
- `src/services/engine-serving/tests/action_writer.rs` — fixture test for the relocated writer (T3).
- `src/services/engine/tests/write_wire.rs` — engine-level wire test for `WriteObject`/`OverwriteTable` (T4).
- `src/services/query-api/src/engine_action_client.rs` — query-api's thin `ActionEngine` wire client (T5).
- `src/services/query-api/tests/action_client_wire.rs` — focused wire-client test (T5).
- `src/control-plane/core/tests/column_spec_serde.rs` — `ColumnSpec` serde round-trip (T1).
- `src/services/engine-wire/tests/lineage_wire.rs` — `LineageWire` round-trip (T2).
- `tools/check-query-api-postgres-free.sh` — decoupling guard (T7).

**Modified:**
- `src/control-plane/core/src/snapshot.rs` — add serde derives to `ColumnSpec` (T1).
- `src/services/engine-wire/proto/engine_control.proto` — two new RPCs + messages (T4).
- `src/services/engine-wire/src/convert.rs` — `LineageWire`/`DatasetRefWire` DTO + conversions (T2).
- `src/services/engine-wire/src/client.rs` — `write_object`/`overwrite_table` client methods (T4).
- `src/services/engine-serving/src/lib.rs`, `BUCK`, `Cargo.toml` — export writer; add deps (T3).
- `src/services/engine/src/service.rs` — `writer` field + two RPC handlers (T4).
- `src/services/engine/src/main.rs` — build the writer from env knobs (T4).
- `src/services/engine/tests/wire.rs`, `src/services/engine/tests/compact_wire.rs` — add `writer` field to their `EngineControlService` constructions (T4).
- `src/services/query-api/src/lib.rs` — register `engine_action_client` module; drop `serving_datafusion` (T5/T7).
- `src/services/query-api/src/main.rs` — wire-client construction; drop catalog builder (T6).
- The 7 e2e test files + `tests/e2e_support.rs` — `spawn_engine_writer` helper + migrations (T5/T6).
- `src/services/query-api/src/config.rs` — drop `routing` (T7).
- `src/services/query-api/Cargo.toml` + `BUCK` — drop `control-plane-postgres` + `ingest` from lib (T7).

**Deleted:**
- `src/services/query-api/src/serving_datafusion.rs` — the old writer (its `encode_ipc_stream`/`to_serving` move to `engine_action_client.rs`) (T7).

---

## Task 1: Make `ColumnSpec` serde-serializable

`ColumnSpec` (`{name, ty, nullable}` — all `String`/`bool`) crosses the wire as a JSON string. It currently derives only `Clone, Debug, PartialEq, Eq`. Add serde derives. (`DataFile` in the same file already does this, so it's an established pattern.)

**Files:**
- Modify: `src/control-plane/core/src/snapshot.rs:8-13`
- Test: `src/control-plane/core/tests/column_spec_serde.rs`
- Modify: `src/control-plane/core/BUCK` (new test target)

**Interfaces:**
- Produces: `control_plane_core::ColumnSpec` now implements `serde::Serialize + serde::Deserialize`.

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/core/tests/column_spec_serde.rs`:

```rust
//! ColumnSpec must round-trip through serde_json — it crosses the engine wire as a
//! JSON string in WriteObject/OverwriteTable requests.

use control_plane_core::ColumnSpec;

#[test]
fn column_spec_json_round_trip() {
    let specs = vec![
        ColumnSpec { name: "id".into(), ty: "Long".into(), nullable: false },
        ColumnSpec { name: "name".into(), ty: "String".into(), nullable: true },
    ];
    let json = serde_json::to_string(&specs).expect("serialize");
    let back: Vec<ColumnSpec> = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(specs, back);
}
```

- [ ] **Step 2: Add the test target**

In `src/control-plane/core/BUCK`, mirror an existing `rust_test` (e.g. the `page` test). Add:

```python
rust_test(
    name = "column-spec-serde",
    crate = "column_spec_serde",
    srcs = ["tests/column_spec_serde.rs"],
    crate_root = "tests/column_spec_serde.rs",
    edition = "2024",
    deps = [
        ":core",
        "//third-party:serde_json",
    ],
)
```

- [ ] **Step 3: Run it to confirm it fails**

Run: `buck2 test //src/control-plane/core:column-spec-serde > /tmp/t.log 2>&1; grep -E "FAIL|error\[|Tests finished" /tmp/t.log`
Expected: build error — `ColumnSpec` does not implement `Serialize`/`Deserialize`.

- [ ] **Step 4: Add the derives**

In `src/control-plane/core/src/snapshot.rs`, change `ColumnSpec`'s derive line:

```rust
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ColumnSpec {
    pub name: String,
    pub ty: String,
    pub nullable: bool,
}
```

- [ ] **Step 5: Run the test to confirm it passes**

Run: `buck2 test //src/control-plane/core:column-spec-serde > /tmp/t.log 2>&1; grep -E "FAIL|Tests finished" /tmp/t.log`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/control-plane/core/src/snapshot.rs src/control-plane/core/tests/column_spec_serde.rs src/control-plane/core/BUCK
git commit -m "feat(core): make ColumnSpec serde-serializable for the engine write wire"
```

---

## Task 2: Lineage wire DTO + conversions (engine-wire)

`LineageEvent` holds `OffsetDateTime` and `RunId(Uuid)` — fields whose serde support would require enabling `serde` on `time`/`uuid` (forbidden by Global Constraints). Instead add a serde-native DTO in `engine-wire`'s `convert.rs` (the shared contract crate, which already depends on `control_plane_core`), with lossless conversions. The engine and query-api both use it.

**Files:**
- Modify: `src/services/engine-wire/src/convert.rs`
- Test: `src/services/engine-wire/tests/lineage_wire.rs`
- Modify: `src/services/engine-wire/BUCK` (new test target; confirm `uuid`/`time` deps available to the test)

**Interfaces:**
- Produces:
  - `engine_wire::convert::DatasetRefWire { namespace: String, name: String }` (serde).
  - `engine_wire::convert::LineageWire { run_id: String, event_type: String, event_time_micros: i64, inputs: Vec<DatasetRefWire>, outputs: Vec<DatasetRefWire>, payload: serde_json::Value }` (serde).
  - `impl From<&control_plane_core::LineageEvent> for LineageWire`
  - `impl TryFrom<LineageWire> for control_plane_core::LineageEvent` (Err = `String`).

- [ ] **Step 1: Write the failing round-trip test**

Create `src/services/engine-wire/tests/lineage_wire.rs`:

```rust
//! LineageWire must losslessly round-trip a LineageEvent (the engine-write RPCs
//! carry the lineage envelope as a JSON string of LineageWire).

use control_plane_core::{DatasetRef, EventType, LineageEvent, RunId};
use engine_wire::convert::LineageWire;
use time::OffsetDateTime;
use uuid::Uuid;

#[test]
fn lineage_event_wire_round_trip() {
    let event = LineageEvent {
        run_id: RunId(Uuid::from_u128(0x1234_5678_9abc_def0_1122_3344_5566_7788)),
        event_type: EventType::Complete,
        // Whole seconds → micros-safe (no precision loss across the i64 micros hop).
        event_time: OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("ts"),
        inputs: vec![DatasetRef { namespace: "ns-in".into(), name: "a.b.c".into() }],
        outputs: vec![DatasetRef { namespace: "ns-out".into(), name: "d.e.f".into() }],
        payload: serde_json::json!({ "action": "createWidget", "op": "update" }),
    };

    let wire = LineageWire::from(&event);
    let json = serde_json::to_string(&wire).expect("serialize");
    let parsed: LineageWire = serde_json::from_str(&json).expect("deserialize");
    let back: LineageEvent = parsed.try_into().expect("convert back");

    assert_eq!(event, back);
}

#[test]
fn lineage_wire_rejects_bad_uuid() {
    let wire = LineageWire {
        run_id: "not-a-uuid".into(),
        event_type: "Complete".into(),
        event_time_micros: 0,
        inputs: vec![],
        outputs: vec![],
        payload: serde_json::Value::Null,
    };
    let res: Result<LineageEvent, _> = wire.try_into();
    assert!(res.is_err());
}
```

- [ ] **Step 2: Add the test target**

In `src/services/engine-wire/BUCK`, add (mirroring the lib's deps for `core`/`serde_json`, plus `time`/`uuid`/`uuid`'s v4 is not needed here):

```python
rust_test(
    name = "lineage-wire",
    crate = "lineage_wire",
    srcs = ["tests/lineage_wire.rs"],
    crate_root = "tests/lineage_wire.rs",
    edition = "2024",
    deps = [
        ":engine-wire",
        "//src/control-plane/core:core",
        "//third-party:serde_json",
        "//third-party:time",
        "//third-party:uuid",
    ],
)
```

(If `rust_test` is not yet loaded in this BUCK, add it to the existing `load(... "rust_test")` line as the other crates do.)

- [ ] **Step 3: Run to confirm it fails**

Run: `buck2 test //src/services/engine-wire:lineage-wire > /tmp/t.log 2>&1; grep -E "FAIL|error\[|Tests finished" /tmp/t.log`
Expected: build error — `LineageWire` / `convert::LineageWire` does not exist.

- [ ] **Step 4: Implement the DTO + conversions**

Append to `src/services/engine-wire/src/convert.rs`:

```rust
use control_plane_core::{DatasetRef, EventType, LineageEvent, RunId};

/// Serde-native mirror of `DatasetRef` (`{namespace, name}`).
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DatasetRefWire {
    pub namespace: String,
    pub name: String,
}

/// Serde-native mirror of `LineageEvent` for the engine write RPCs. The two fields
/// `time`/`uuid` cannot serialize without crate-feature changes (see the plan's
/// Global Constraints) are carried as primitives: `run_id` as the Uuid string,
/// `event_time_micros` as unix microseconds.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LineageWire {
    pub run_id: String,
    pub event_type: String,
    pub event_time_micros: i64,
    pub inputs: Vec<DatasetRefWire>,
    pub outputs: Vec<DatasetRefWire>,
    pub payload: serde_json::Value,
}

fn event_type_str(t: EventType) -> &'static str {
    match t {
        EventType::Start => "Start",
        EventType::Running => "Running",
        EventType::Complete => "Complete",
        EventType::Abort => "Abort",
        EventType::Fail => "Fail",
    }
}

fn event_type_from(s: &str) -> Result<EventType, String> {
    match s {
        "Start" => Ok(EventType::Start),
        "Running" => Ok(EventType::Running),
        "Complete" => Ok(EventType::Complete),
        "Abort" => Ok(EventType::Abort),
        "Fail" => Ok(EventType::Fail),
        other => Err(format!("unknown EventType `{other}`")),
    }
}

impl From<&DatasetRef> for DatasetRefWire {
    fn from(d: &DatasetRef) -> Self {
        Self { namespace: d.namespace.clone(), name: d.name.clone() }
    }
}

impl From<DatasetRefWire> for DatasetRef {
    fn from(d: DatasetRefWire) -> Self {
        Self { namespace: d.namespace, name: d.name }
    }
}

impl From<&LineageEvent> for LineageWire {
    fn from(e: &LineageEvent) -> Self {
        // i128 nanos → i64 micros. Lineage timestamps are well within i64-micros range.
        let micros = (e.event_time.unix_timestamp_nanos() / 1_000) as i64;
        Self {
            run_id: e.run_id.0.to_string(),
            event_type: event_type_str(e.event_type).to_string(),
            event_time_micros: micros,
            inputs: e.inputs.iter().map(DatasetRefWire::from).collect(),
            outputs: e.outputs.iter().map(DatasetRefWire::from).collect(),
            payload: e.payload.clone(),
        }
    }
}

impl TryFrom<LineageWire> for LineageEvent {
    type Error = String;

    fn try_from(w: LineageWire) -> Result<Self, Self::Error> {
        let run_id = RunId(uuid::Uuid::parse_str(&w.run_id).map_err(|e| e.to_string())?);
        let event_type = event_type_from(&w.event_type)?;
        let event_time =
            time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(w.event_time_micros) * 1_000)
                .map_err(|e| e.to_string())?;
        Ok(LineageEvent {
            run_id,
            event_type,
            event_time,
            inputs: w.inputs.into_iter().map(DatasetRef::from).collect(),
            outputs: w.outputs.into_iter().map(DatasetRef::from).collect(),
            payload: w.payload,
        })
    }
}
```

`engine-wire/src/convert.rs` already uses `time::OffsetDateTime::from_unix_timestamp_nanos`, and the `engine-wire` `BUCK` already lists `//third-party:time` and `//third-party:uuid`. **Verify `src/services/engine-wire/Cargo.toml` declares both** (cargo/clippy + reindeer-check need them even though buck2 builds from BUCK). If either is missing from `[dependencies]`, add:

```toml
# src/services/engine-wire/Cargo.toml [dependencies]
time = "=0.3.47"
uuid = { version = "1" }
```

Then run `./tools/buckify.sh` and confirm `third-party/BUCK` is unchanged (these crates are already vendored; this only keeps the manifest in sync). The `reindeer-check` prek hook fails if Cargo.toml and the generated rules drift.

- [ ] **Step 5: Run to confirm it passes**

Run: `buck2 test //src/services/engine-wire:lineage-wire > /tmp/t.log 2>&1; grep -E "FAIL|Tests finished" /tmp/t.log`
Expected: PASS (both tests).

- [ ] **Step 6: Commit**

```bash
git add src/services/engine-wire/src/convert.rs src/services/engine-wire/tests/lineage_wire.rs src/services/engine-wire/BUCK
git commit -m "feat(engine-wire): add LineageWire DTO for the governed-write RPCs"
```

---

## Task 3: Relocate `IcebergActionWriter` into engine-serving

Move the write executor into `engine-serving`, taking the already-encoded artifacts (the client builds + IPC-encodes the batch). It calls the unchanged `iceberg_landing::{land, overwrite_parquet_snapshot}`. This is **additive** — the old query-api `IcebergActionWriter` stays in place until Task 7.

**Files:**
- Create: `src/services/engine-serving/src/action_writer.rs`
- Modify: `src/services/engine-serving/src/lib.rs`
- Modify: `src/services/engine-serving/BUCK`, `src/services/engine-serving/Cargo.toml`
- Test: `src/services/engine-serving/tests/action_writer.rs`

**Interfaces:**
- Consumes: `control_plane_postgres::iceberg_landing::{land, overwrite_parquet_snapshot}` (signatures below), `control_plane_core::{ColumnSpec, LineageEvent, SnapshotId, TableRef}`, `control_plane_postgres::iceberg_sql_catalog::SqlCatalog`, `sqlx::PgPool`, `engine_serving::serving::EngineServingError`.
  - `land(pool: &PgPool, catalog: &SqlCatalog, table: &TableRef, columns: &[ColumnSpec], ipc_body: &[u8], inline_byte_limit: usize, flush_byte_threshold: i64, lineage: LineageEvent) -> control_plane_core::Result<SnapshotId>`
  - `overwrite_parquet_snapshot(pool: &PgPool, catalog: &SqlCatalog, table: &TableRef, columns: &[ColumnSpec], batches: Vec<RecordBatch>, lineage: Option<&LineageEvent>) -> control_plane_core::Result<SnapshotId>`
- Produces:
  - `engine_serving::action_writer::IcebergActionWriter` with `new(catalog: Arc<SqlCatalog>, pool: PgPool, inline_byte_limit: usize, flush_byte_threshold: i64) -> Self`
  - `async fn write_object(&self, table: &TableRef, columns: &[ColumnSpec], ipc: &[u8], event: LineageEvent) -> Result<SnapshotId, EngineServingError>`
  - `async fn overwrite_table(&self, table: &TableRef, columns: &[ColumnSpec], ipc: &[u8], event: LineageEvent) -> Result<SnapshotId, EngineServingError>` (empty `ipc` ⇒ truncate / delete-all).
  - Re-exported as `engine_serving::IcebergActionWriter`.

- [ ] **Step 1: Write the failing fixture test**

Create `src/services/engine-serving/tests/action_writer.rs`. It builds a one-row Arrow IPC stream, calls `write_object`, and asserts a snapshot id comes back and the row is readable via the inline mirror; then `overwrite_table` with a replacement batch; then `overwrite_table` with empty `ipc` (truncate).

```rust
//! The relocated engine-side write executor: build a one-row IPC stream, land it,
//! overwrite it, and truncate it — asserting snapshot ids and committed rows.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{
    ColumnSpec, DatasetRef, EventType, LineageEvent, RunId, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use engine_serving::IcebergActionWriter;
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;
use uuid::Uuid;

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

fn one_row_ipc(id: i64, name: &str) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(StringArray::from(vec![name])),
        ],
    )
    .expect("batch");
    let mut buf = Vec::new();
    {
        let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &schema).expect("writer");
        w.write(&batch).expect("write");
        w.finish().expect("finish");
    }
    buf
}

fn cols() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec { name: "id".into(), ty: "Long".into(), nullable: true },
        ColumnSpec { name: "name".into(), ty: "String".into(), nullable: true },
    ]
}

fn event(op: &str) -> LineageEvent {
    let ds = DatasetRef { namespace: "loom".into(), name: "main.widget".into() };
    LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("ts"),
        inputs: vec![],
        outputs: vec![ds],
        payload: serde_json::json!({ "action": "test", "op": op }),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_overwrite_truncate() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let warehouse = tempfile::tempdir().expect("warehouse");
    let catalog = Arc::new(build_catalog(&dsn, warehouse.path()).await);

    // Define the table in the mirror catalog so land/overwrite have a target.
    e2e_seed_widget_table(&cp).await;

    let table = TableRef { schema: "main".into(), name: "widget".into() };
    // Large inline limit so the single row inlines (no flush job needed).
    let writer = IcebergActionWriter::new(catalog, pool.clone(), 16 * 1024 * 1024, i64::MAX);

    let s1 = writer
        .write_object(&table, &cols(), &one_row_ipc(1, "a"), event("insert"))
        .await
        .expect("write_object");
    assert!(s1.0 > 0);

    let s2 = writer
        .overwrite_table(&table, &cols(), &one_row_ipc(2, "b"), event("update"))
        .await
        .expect("overwrite_table");
    assert!(s2.0 > s1.0);

    // Empty ipc ⇒ truncate (delete-all).
    let s3 = writer
        .overwrite_table(&table, &[], &[], event("delete"))
        .await
        .expect("truncate");
    assert!(s3.0 > s2.0);
}
```

> **Implementer note — `e2e_seed_widget_table`:** the relocated writer test does NOT go through query-api's `run_action` (which would define ontology + read back), so it must put the engine into a state where `land`/`overwrite_parquet_snapshot` succeed on a fresh table. Define `e2e_seed_widget_table(&PgControlPlane)` to call `cp.ontology().define_type(...)` for the `Widget`/`main.widget` type (copy the `define_type` block from `action_e2e.rs::setup_widget_writer` verbatim). Then **read `src/control-plane/postgres/src/iceberg_landing.rs` (`land`) and `iceberg_mirror.rs` (`ensure_table`)** and determine whether the first `land` auto-creates the mirror/Iceberg table or whether it must pre-exist:
> - If `land` calls `ensure_table` (auto-create on first land), the `define_type` seed is sufficient (and the test mirrors what `action_e2e` relies on, which already passes today through the old writer).
> - If the physical Iceberg table must pre-exist, extend the seed to create it via the same catalog API `land` expects (e.g. an `ensure_table`/create call against the `SqlCatalog`).
>
> Confirm by running the test: a green `write_object` proves the seed is complete. Keep `e2e_seed_widget_table` test-local (inline `async fn` in this file); do not build a shared helper for it here. Because `action_e2e.rs` already lands a `Widget` row through the old writer today with only `define_type` + the catalog, the auto-create path is the expected outcome — but verify, don't assume.

- [ ] **Step 2: Add deps + test target**

In `src/services/engine-serving/Cargo.toml`, ensure these deps exist (add any missing): `arrow` (IPC reader/writer), `control-plane-core`, `control-plane-postgres`, `sqlx`. The crate already uses arrow + postgres for reads, so most are present; add `arrow` if only `arrow-array`/`datafusion` are listed.

In `src/services/engine-serving/BUCK`:
- Add to the `engine-serving` lib `deps`: `//third-party:arrow` (or `//third-party:arrow-ipc` if the tree splits it — match how reads import IPC), `//src/control-plane/core:core`, `//src/control-plane/postgres:postgres`, `//third-party:sqlx`, `//third-party:async-trait` is NOT needed (inherent async methods). Most are already present (reads use postgres); add only what's missing.
- Add the fixture test target:

```python
load("//src/control-plane/postgres:defs.bzl", "loom_fixture_test")

loom_fixture_test(
    name = "action-writer",
    crate = "action_writer",
    srcs = ["tests/action_writer.rs"],
    crate_root = "tests/action_writer.rs",
    deps = [
        ":engine-serving",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:arrow",
        "//third-party:iceberg",
        "//third-party:serde_json",
        "//third-party:tempfile",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```

- [ ] **Step 3: Run to confirm it fails**

Run: `buck2 test //src/services/engine-serving:action-writer > /tmp/t.log 2>&1; grep -E "FAIL|error\[|Tests finished" /tmp/t.log`
Expected: build error — `engine_serving::IcebergActionWriter` does not exist.

- [ ] **Step 4: Implement the writer**

Create `src/services/engine-serving/src/action_writer.rs`:

```rust
//! The engine-side governed-write executor (relocated from query-api). It receives
//! a pre-authorized write: the caller (query-api) has already enforced ACL, built
//! the typed Arrow batch, and IPC-encoded it. This executor lands or overwrites it,
//! committing the row(s) and lineage atomically via `iceberg_landing`. It is
//! governance-free — exactly mirroring the read path.

use std::sync::Arc;

use arrow::array::RecordBatch;
use control_plane_core::{ColumnSpec, LineageEvent, SnapshotId, TableRef};
use control_plane_postgres::iceberg_landing;
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use sqlx::PgPool;

use crate::serving::EngineServingError;

/// Decode an Arrow IPC stream body into its record batches. An empty body yields
/// an empty vector (the truncate / delete-all signal for overwrite).
fn decode_ipc(ipc: &[u8]) -> Result<Vec<RecordBatch>, EngineServingError> {
    if ipc.is_empty() {
        return Ok(Vec::new());
    }
    let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(ipc), None)
        .map_err(|e| EngineServingError::Engine(e.to_string()))?;
    reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| EngineServingError::Engine(e.to_string()))
}

/// The relocated `ActionEngine` executor. Holds the same dependencies the old
/// query-api writer held: an Iceberg `SqlCatalog`, a `PgPool`, and the inline/flush
/// byte routing knobs.
pub struct IcebergActionWriter {
    catalog: Arc<SqlCatalog>,
    pool: PgPool,
    inline_byte_limit: usize,
    flush_byte_threshold: i64,
}

impl IcebergActionWriter {
    #[must_use]
    pub fn new(
        catalog: Arc<SqlCatalog>,
        pool: PgPool,
        inline_byte_limit: usize,
        flush_byte_threshold: i64,
    ) -> Self {
        Self { catalog, pool, inline_byte_limit, flush_byte_threshold }
    }

    /// Governed typed-insert: land one IPC-encoded row + its lineage atomically.
    pub async fn write_object(
        &self,
        table: &TableRef,
        columns: &[ColumnSpec],
        ipc: &[u8],
        event: LineageEvent,
    ) -> Result<SnapshotId, EngineServingError> {
        iceberg_landing::land(
            &self.pool,
            &self.catalog,
            table,
            columns,
            ipc,
            self.inline_byte_limit,
            self.flush_byte_threshold,
            event,
        )
        .await
        .map_err(|e| EngineServingError::Engine(e.to_string()))
    }

    /// Copy-on-write overwrite (UPDATE/DELETE): replace the table's entire live
    /// contents with the decoded batch(es), committing `event` atomically. An empty
    /// `ipc` truncates the table (delete-all).
    pub async fn overwrite_table(
        &self,
        table: &TableRef,
        columns: &[ColumnSpec],
        ipc: &[u8],
        event: LineageEvent,
    ) -> Result<SnapshotId, EngineServingError> {
        let batches = decode_ipc(ipc)?;
        iceberg_landing::overwrite_parquet_snapshot(
            &self.pool,
            &self.catalog,
            table,
            columns,
            batches,
            Some(&event),
        )
        .await
        .map_err(|e| EngineServingError::Engine(e.to_string()))
    }
}
```

In `src/services/engine-serving/src/lib.rs`, add:

```rust
pub mod action_writer;
pub use action_writer::IcebergActionWriter;
```

- [ ] **Step 5: Run the test to confirm it passes**

Run: `buck2 test //src/services/engine-serving:action-writer > /tmp/t.log 2>&1; grep -E "FAIL|Tests finished" /tmp/t.log`
Expected: PASS. If `land` errors on a missing table, fix `e2e_seed_widget_table` to create the mirror table the way `action_e2e` does, then re-run.

- [ ] **Step 6: Commit**

```bash
git add src/services/engine-serving/src/action_writer.rs src/services/engine-serving/src/lib.rs src/services/engine-serving/BUCK src/services/engine-serving/Cargo.toml src/services/engine-serving/tests/action_writer.rs
git commit -m "feat(engine-serving): relocate IcebergActionWriter as the engine-side write executor"
```

---

## Task 4: WriteObject/OverwriteTable RPCs + engine handlers + wire test

Add the two unary RPCs to the proto, the engine-wire client methods, wire the relocated writer into `EngineControlService`, implement the handlers, and prove it end-to-end with a new engine wire test.

**Files:**
- Modify: `src/services/engine-wire/proto/engine_control.proto`
- Modify: `src/services/engine-wire/src/client.rs`
- Modify: `src/services/engine/src/service.rs`
- Modify: `src/services/engine/src/main.rs`
- Modify: `src/services/engine/tests/wire.rs`, `src/services/engine/tests/compact_wire.rs` (add `writer` field)
- Modify: `src/services/engine/BUCK` (new test target)
- Test: `src/services/engine/tests/write_wire.rs`

**Interfaces:**
- Consumes: `engine_serving::IcebergActionWriter` (T3), `engine_wire::convert::LineageWire` (T2), serde of `Vec<ColumnSpec>` (T1).
- Produces:
  - proto: `rpc WriteObject (WriteObjectRequest) returns (WriteObjectResponse);` and `rpc OverwriteTable (OverwriteTableRequest) returns (OverwriteTableResponse);` with messages `{schema, name, ipc, columns_json, lineage_json}` → `{snapshot_id}`.
  - `engine_wire::client::GrpcQueueClient::write_object(&self, schema: String, name: String, ipc: Vec<u8>, columns_json: String, lineage_json: String) -> Result<i64>`
  - `engine_wire::client::GrpcQueueClient::overwrite_table(&self, ...same args...) -> Result<i64>`
  - `EngineControlService.writer: engine_serving::IcebergActionWriter` (new field).

- [ ] **Step 1: Extend the proto**

In `src/services/engine-wire/proto/engine_control.proto`, add two rpc lines to `service EngineControl` (after `CompactTable`):

```proto
  rpc WriteObject    (WriteObjectRequest)    returns (WriteObjectResponse);
  rpc OverwriteTable (OverwriteTableRequest) returns (OverwriteTableResponse);
```

And the messages (after `CompactTableResponse`):

```proto
message WriteObjectRequest {
  string schema = 1;
  string name = 2;
  bytes  ipc = 3;            // one-row Arrow IPC stream (built + encoded client-side)
  string columns_json = 4;   // serde_json of Vec<ColumnSpec>
  string lineage_json = 5;   // serde_json of LineageWire
}
message WriteObjectResponse { int64 snapshot_id = 1; }

message OverwriteTableRequest {
  string schema = 1;
  string name = 2;
  bytes  ipc = 3;            // N-row Arrow IPC stream; empty = truncate (delete-all)
  string columns_json = 4;
  string lineage_json = 5;
}
message OverwriteTableResponse { int64 snapshot_id = 1; }
```

- [ ] **Step 2: Add the engine-wire client methods**

In `src/services/engine-wire/src/client.rs`, add to `impl GrpcQueueClient` (mirroring `flush_table`):

```rust
    /// Governed typed-insert over the wire. Returns the new snapshot id.
    pub async fn write_object(
        &self,
        schema: String,
        name: String,
        ipc: Vec<u8>,
        columns_json: String,
        lineage_json: String,
    ) -> Result<i64> {
        let resp = self
            .inner
            .clone()
            .write_object(pb::WriteObjectRequest { schema, name, ipc, columns_json, lineage_json })
            .await
            .map_err(be)?
            .into_inner();
        Ok(resp.snapshot_id)
    }

    /// Copy-on-write overwrite (UPDATE/DELETE) over the wire. Returns the new
    /// snapshot id. Empty `ipc` truncates the table.
    pub async fn overwrite_table(
        &self,
        schema: String,
        name: String,
        ipc: Vec<u8>,
        columns_json: String,
        lineage_json: String,
    ) -> Result<i64> {
        let resp = self
            .inner
            .clone()
            .overwrite_table(pb::OverwriteTableRequest { schema, name, ipc, columns_json, lineage_json })
            .await
            .map_err(be)?
            .into_inner();
        Ok(resp.snapshot_id)
    }
```

- [ ] **Step 3: Write the failing wire test**

Create `src/services/engine/tests/write_wire.rs`. Spawn the engine (reuse `wire.rs`'s `spawn_server` pattern but with the new `writer` field), connect a `GrpcQueueClient`, and drive WriteObject + OverwriteTable + truncate, asserting snapshot ids and that the row is committed (query the mirror via the catalog/serving, or assert via the same inline-read the engine-serving test uses).

```rust
//! Engine wire test for the governed-write RPCs: drive WriteObject / OverwriteTable
//! over the UDS and assert committed snapshots.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{ColumnSpec, DatasetRef, EventType, LineageEvent, RunId};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use engine::service::EngineControlService;
use engine_serving::IcebergActionWriter;
use engine_wire::client::GrpcQueueClient;
use engine_wire::convert::LineageWire;
use engine_wire::pb::engine_control_server::EngineControlServer;
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;
use tonic::transport::Server;
use uuid::Uuid;

async fn make_catalog(dsn: String, warehouse: &str) -> SqlCatalog {
    let mut props = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), dsn);
    props.insert(SQL_CATALOG_PROP_WAREHOUSE.to_string(), format!("file://{warehouse}"));
    SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("loom", props)
        .await
        .expect("catalog")
}

// one_row_ipc / cols / event helpers: copy verbatim from
// engine-serving/tests/action_writer.rs (same shapes).

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_object_over_wire() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    // Seed the main.widget mirror table (same as engine-serving test).
    seed_widget_table(&cp).await;

    let wh = tempfile::tempdir().expect("wh");
    let sock_dir = tempfile::tempdir().expect("sock");
    let sock = sock_dir.path().join("engine.sock");
    let sock_str = sock.to_string_lossy().to_string();

    let pool = fx.pool_for(&db).await;
    let cp2 = control_plane_postgres::PgControlPlane::new(pool.clone(), Duration::from_millis(5000));
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let writer_catalog = Arc::new(make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await);
    let writer = IcebergActionWriter::new(writer_catalog, pool.clone(), 16 * 1024 * 1024, i64::MAX);

    let svc = EngineControlService {
        cp: cp2,
        catalog,
        pool,
        retention: Duration::from_secs(7 * 24 * 3600),
        writer,
    };
    let listener = tokio::net::UnixListener::bind(&sock).expect("bind");
    let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);
    tokio::spawn(async move {
        let _wh = wh;
        drop(Server::builder().add_service(EngineControlServer::new(svc)).serve_with_incoming(incoming).await);
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    let client = GrpcQueueClient::connect(&sock_str).await.expect("connect");
    let columns_json = serde_json::to_string(&cols()).expect("cols");
    let lineage_json = serde_json::to_string(&LineageWire::from(&event("insert"))).expect("ev");

    let s1 = client
        .write_object("main".into(), "widget".into(), one_row_ipc(1, "a"), columns_json.clone(), lineage_json.clone())
        .await
        .expect("write_object");
    assert!(s1 > 0);

    let s2 = client
        .overwrite_table("main".into(), "widget".into(), one_row_ipc(2, "b"), columns_json, lineage_json)
        .await
        .expect("overwrite_table");
    assert!(s2 > s1);

    // truncate
    let empty_cols = serde_json::to_string::<Vec<ColumnSpec>>(&vec![]).expect("empty");
    let s3 = client
        .overwrite_table("main".into(), "widget".into(), Vec::new(), empty_cols, serde_json::to_string(&LineageWire::from(&event("delete"))).expect("ev"))
        .await
        .expect("truncate");
    assert!(s3 > s2);
}
```

> **Implementer note:** factor `one_row_ipc`, `cols`, `event`, and `seed_widget_table` into the test file (test-local). They are identical in shape to the engine-serving test; copying is acceptable for a test fixture, but if it reads cleaner, lift them into the engine `tests/` via a small `mod` include — do not add a cross-crate dependency for them.

- [ ] **Step 4: Add `writer` to the service struct + handlers**

In `src/services/engine/src/service.rs`, add the field to `EngineControlService`:

```rust
pub struct EngineControlService {
    pub cp: PgControlPlane,
    pub catalog: SqlCatalog,
    pub pool: PgPool,
    /// Retention window for `gc_table` (from `LOOM_GC_RETENTION_SECS`).
    pub retention: std::time::Duration,
    /// Governed-write executor (relocated from query-api).
    pub writer: engine_serving::IcebergActionWriter,
}
```

Add the two handler methods inside `impl pb::engine_control_server::EngineControl for EngineControlService` (mirroring `compact_table`'s JSON-parse pattern):

```rust
    async fn write_object(
        &self,
        req: Request<pb::WriteObjectRequest>,
    ) -> std::result::Result<Response<pb::WriteObjectResponse>, Status> {
        let r = req.into_inner();
        let table = TableRef { schema: r.schema, name: r.name };
        let columns: Vec<control_plane_core::ColumnSpec> = serde_json::from_str(&r.columns_json)
            .map_err(|e| Status::invalid_argument(format!("bad columns_json: {e}")))?;
        let wire: engine_wire::convert::LineageWire = serde_json::from_str(&r.lineage_json)
            .map_err(|e| Status::invalid_argument(format!("bad lineage_json: {e}")))?;
        let event: control_plane_core::LineageEvent = wire
            .try_into()
            .map_err(|e: String| Status::invalid_argument(format!("bad lineage: {e}")))?;
        let snap = self
            .writer
            .write_object(&table, &columns, &r.ipc, event)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(pb::WriteObjectResponse { snapshot_id: snap.0 }))
    }

    async fn overwrite_table(
        &self,
        req: Request<pb::OverwriteTableRequest>,
    ) -> std::result::Result<Response<pb::OverwriteTableResponse>, Status> {
        let r = req.into_inner();
        let table = TableRef { schema: r.schema, name: r.name };
        let columns: Vec<control_plane_core::ColumnSpec> = serde_json::from_str(&r.columns_json)
            .map_err(|e| Status::invalid_argument(format!("bad columns_json: {e}")))?;
        let wire: engine_wire::convert::LineageWire = serde_json::from_str(&r.lineage_json)
            .map_err(|e| Status::invalid_argument(format!("bad lineage_json: {e}")))?;
        let event: control_plane_core::LineageEvent = wire
            .try_into()
            .map_err(|e: String| Status::invalid_argument(format!("bad lineage: {e}")))?;
        let snap = self
            .writer
            .overwrite_table(&table, &columns, &r.ipc, event)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(pb::OverwriteTableResponse { snapshot_id: snap.0 }))
    }
```

Confirm `engine/src/service.rs` imports/uses are satisfied: `engine_serving` is already a dep of the `engine` crate; add `use engine_serving;` only if needed (the field is fully-qualified). `control_plane_core::ColumnSpec`/`LineageEvent` are reachable (the crate already imports `control_plane_core` items like `TableRef`, `RunId`).

- [ ] **Step 5: Build the writer in `main.rs`**

In `src/services/engine/src/main.rs`, read the two byte knobs from env (mirroring the existing direct `LOOM_ENGINE_SOCKET` read) with the same defaults the ingest `RoutingTuning` uses (16 MiB / 64 MiB), build a writer catalog, and set the field. Insert before constructing `control`:

```rust
    // Governed-write executor config (defaults mirror ingest's RoutingTuning).
    let inline_byte_limit: usize = std::env::var("LOOM_INLINE_BYTE_LIMIT")
        .ok()
        .map(|s| s.parse())
        .transpose()
        .map_err(|e: std::num::ParseIntError| -> Box<dyn std::error::Error> {
            format!("LOOM_INLINE_BYTE_LIMIT: {e}").into()
        })?
        .unwrap_or(16 * 1024 * 1024);
    let flush_byte_threshold: i64 = std::env::var("LOOM_FLUSH_BYTE_THRESHOLD")
        .ok()
        .map(|s| s.parse())
        .transpose()
        .map_err(|e: std::num::ParseIntError| -> Box<dyn std::error::Error> {
            format!("LOOM_FLUSH_BYTE_THRESHOLD: {e}").into()
        })?
        .unwrap_or(64 * 1024 * 1024);

    // A third SqlCatalog for the writer (SqlCatalog is not Clone; the engine already
    // builds two for control + flight).
    let writer_catalog = SqlCatalogBuilder::default()
        .with_storage_factory(service_runtime::build_storage_factory(&cfg.object_store)?)
        .load("loom", props_for_writer)
        .await?;
    let writer = engine_serving::IcebergActionWriter::new(
        std::sync::Arc::new(writer_catalog),
        pool.clone(),
        inline_byte_limit,
        flush_byte_threshold,
    );
```

`props` is consumed building `flight_catalog`; clone it once more up front. Change the two existing `props` builds to keep a clone available: rename so the writer gets its own map — e.g. add `let props_for_writer = props.clone();` immediately after `props` is fully populated (before `catalog`/`flight_catalog` consume their clones). Then add `writer,` to the `EngineControlService { ... }` literal.

- [ ] **Step 6: Fix the two existing engine wire tests**

`src/services/engine/tests/wire.rs` and `src/services/engine/tests/compact_wire.rs` construct `EngineControlService { cp, catalog, pool, retention }` directly and will no longer compile. In each `spawn_server` (or equivalent), build a writer and add the field:

```rust
    let writer_catalog = make_catalog(fx.pg_dsn(db), &wh.path().display().to_string()).await;
    let writer = engine_serving::IcebergActionWriter::new(
        std::sync::Arc::new(writer_catalog),
        pool.clone(),
        16 * 1024 * 1024,
        i64::MAX,
    );
    let svc = EngineControlService { cp, catalog, pool, retention: /* unchanged */, writer };
```

Add `engine-serving` to the `wire` and `compact-wire` test target `deps` in `src/services/engine/BUCK` (they already dep `engine`, `postgres`, `engine-wire`; add `//src/services/engine-serving:engine-serving`). `make_catalog` already exists in `wire.rs`; in `compact_wire.rs` reuse its local catalog builder.

- [ ] **Step 7: Add the wire test target**

In `src/services/engine/BUCK`, add (mirroring `compact-wire`'s deps, plus `engine-serving`):

```python
loom_fixture_test(
    name = "write-wire",
    crate = "write_wire",
    srcs = ["tests/write_wire.rs"],
    crate_root = "tests/write_wire.rs",
    deps = [
        ":engine",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//src/services/engine-serving:engine-serving",
        "//src/services/engine-wire:engine-wire",
        "//third-party:arrow",
        "//third-party:iceberg",
        "//third-party:serde_json",
        "//third-party:tempfile",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:tokio-stream",
        "//third-party:tonic",
        "//third-party:uuid",
    ],
)
```

- [ ] **Step 8: Regenerate the proto + run**

The `:pb-gen` genrule regenerates automatically on build. Run:

```bash
buck2 test //src/services/engine:write-wire //src/services/engine:wire //src/services/engine:compact-wire //src/services/engine-wire:lineage-wire > /tmp/t.log 2>&1; grep -E "FAIL|error\[|Tests finished" /tmp/t.log
```
Expected: all PASS. Then `buck2 build //src/services/engine:engine-bin > /tmp/b.log 2>&1; grep -E "error|BUILD SUCCEEDED|Build ID" /tmp/b.log` to confirm `main.rs` compiles.

- [ ] **Step 9: Commit**

```bash
git add src/services/engine-wire/proto/engine_control.proto src/services/engine-wire/src/client.rs src/services/engine/src/service.rs src/services/engine/src/main.rs src/services/engine/tests/ src/services/engine/BUCK
git commit -m "feat(engine): add WriteObject/OverwriteTable governed-write RPCs"
```

---

## Task 5: query-api wire `ActionEngine` client + e2e spawn helper

Add `EngineActionClient` implementing query-api's `ActionEngine` trait by building/encoding the batch client-side and calling the new RPCs. Add a shared `spawn_engine_writer` helper to `e2e_support`. Prove both with a focused wire test. The old `IcebergActionWriter` stays until Task 7.

**Files:**
- Create: `src/services/query-api/src/engine_action_client.rs`
- Modify: `src/services/query-api/src/lib.rs` (register module)
- Modify: `src/services/query-api/BUCK` (lib already deps `engine-wire`; no new lib dep)
- Modify: `src/services/query-api/tests/e2e_support.rs` (+ `:e2e-support` BUCK deps)
- Test: `src/services/query-api/tests/action_client_wire.rs`

**Interfaces:**
- Consumes: `crate::serving::{ActionEngine, ServingError, SqlValue, build_object_batch, build_object_batches}`, `engine_wire::client::GrpcQueueClient`, `engine_wire::convert::LineageWire`, `control_plane_core::{ColumnSpec, LineageEvent, SnapshotId, TableRef}`.
- Produces:
  - `query_api::engine_action_client::EngineActionClient` with `async fn connect(socket: impl Into<String>) -> Result<Self, ServingError>` and `impl ActionEngine for EngineActionClient`.
  - `query_api::engine_action_client::encode_ipc_stream(batch: &RecordBatch) -> Result<Vec<u8>, ServingError>` (moved here from the old `serving_datafusion.rs`).
  - `e2e_support::spawn_engine_writer(fx: &PgFixture, db: &str, warehouse: &Path, inline_byte_limit: usize, flush_byte_threshold: i64) -> (EngineActionClient, EngineGuard)`.

- [ ] **Step 1: Write the failing wire-client test**

Create `src/services/query-api/tests/action_client_wire.rs`:

```rust
//! query-api's EngineActionClient drives the governed-write RPCs end-to-end:
//! spawn an engine over a UDS, write/overwrite via the trait, read back inline.

use std::sync::Arc;

use control_plane_core::{
    Action, ControlPlane, DatasetRef, Effect, EventType, LineageEvent, PolicyTarget, RunId,
    SnapshotId, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use e2e_support::{define_widget, grant_writer, spawn_engine_writer, InProcessServingEngine};
use query_api::serving::{ActionEngine, SqlValue};
use uuid::Uuid;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_object_through_wire_client() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let widget = define_widget(&cp).await;
    let _subj = grant_writer(&cp, &widget).await;

    let warehouse = tempfile::tempdir().expect("warehouse");
    let (engine, _guard) =
        spawn_engine_writer(&fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX).await;

    let table = TableRef { schema: "main".into(), name: "widget".into() };
    let event = LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetRef { namespace: "loom".into(), name: "main.widget".into() }],
        payload: serde_json::json!({ "action": "createWidget" }),
    };
    let snap: SnapshotId = engine
        .write_object(
            &table,
            &["id".to_string(), "name".to_string()],
            &[SqlValue::Int(7), SqlValue::Text("hi".into())],
            &["Long".to_string(), "String".to_string()],
            event,
        )
        .await
        .expect("write_object");
    assert!(snap.0 > 0);

    // Read back via the in-process serving engine over the same pool.
    let _serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    // (Assert the row is visible via read_object exactly as action_e2e does; copy
    //  that read+assert block here so the test proves the row landed.)
}
```

> **Implementer note:** `define_widget`/`grant_widget` exist today as local helpers inside individual e2e files (e.g. `update_delete_tiers_e2e.rs`). Promote `define_widget(&PgControlPlane) -> TypeName` and `grant_writer(&PgControlPlane, &TypeName) -> SubjectId` into `e2e_support.rs` as `pub` helpers (they are already duplicated across files — this is the shared-helper consolidation CLAUDE.md calls for). Use the exact bodies from `update_delete_tiers_e2e.rs`. Reuse the read-back assertion from `action_e2e.rs`.

`SqlValue` variant names (`Int`/`Text`/...) — confirm against `crate::serving::SqlValue` while implementing and use the real variants.

- [ ] **Step 2: Implement `EngineActionClient`**

Create `src/services/query-api/src/engine_action_client.rs`:

```rust
//! query-api's `ActionEngine` as a thin wire client over the engine's
//! `EngineControl` UDS channel. ACL is enforced by the handler BEFORE this runs;
//! this sends a pre-authorized write. The typed Arrow batch is built and IPC-encoded
//! here (client-side); only the encoded bytes + JSON metadata cross the wire.

use arrow::array::RecordBatch;
use async_trait::async_trait;
use engine_wire::client::GrpcQueueClient;
use engine_wire::convert::LineageWire;

use crate::serving::{
    ActionEngine, ServingError, SqlValue, build_object_batch, build_object_batches,
};

fn to_serving<E: std::fmt::Display>(e: E) -> ServingError {
    ServingError::Engine(e.to_string())
}

/// Encode a single record batch as an Arrow IPC stream body. (Moved verbatim from
/// the old `serving_datafusion.rs`; stays client-side.)
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

/// Wire-backed governed-write engine.
pub struct EngineActionClient {
    ctl: GrpcQueueClient,
}

impl EngineActionClient {
    /// Connect to the engine's `EngineControl` service at `socket` (a UDS path).
    pub async fn connect(socket: impl Into<String>) -> Result<Self, ServingError> {
        let ctl = GrpcQueueClient::connect(socket).await.map_err(to_serving)?;
        Ok(Self { ctl })
    }
}

#[async_trait]
impl ActionEngine for EngineActionClient {
    async fn write_object(
        &self,
        table: &control_plane_core::TableRef,
        columns: &[String],
        values: &[SqlValue],
        logical_types: &[String],
        event: control_plane_core::LineageEvent,
    ) -> Result<control_plane_core::SnapshotId, ServingError> {
        let (_schema, batch, specs) = build_object_batch(columns, values, logical_types)?;
        let ipc = encode_ipc_stream(&batch)?;
        let columns_json = serde_json::to_string(&specs).map_err(to_serving)?;
        let lineage_json = serde_json::to_string(&LineageWire::from(&event)).map_err(to_serving)?;
        let id = self
            .ctl
            .write_object(table.schema.clone(), table.name.clone(), ipc, columns_json, lineage_json)
            .await
            .map_err(to_serving)?;
        Ok(control_plane_core::SnapshotId(id))
    }

    async fn overwrite_table(
        &self,
        table: &control_plane_core::TableRef,
        columns: &[String],
        rows: &[Vec<SqlValue>],
        logical_types: &[String],
        event: control_plane_core::LineageEvent,
    ) -> Result<control_plane_core::SnapshotId, ServingError> {
        let lineage_json = serde_json::to_string(&LineageWire::from(&event)).map_err(to_serving)?;
        let (ipc, columns_json) = if rows.is_empty() {
            // Delete-all: empty payload drives the truncate branch engine-side.
            (Vec::new(), serde_json::to_string::<Vec<control_plane_core::ColumnSpec>>(&vec![]).map_err(to_serving)?)
        } else {
            let (_schema, batch, specs) = build_object_batches(columns, rows, logical_types)?;
            (encode_ipc_stream(&batch)?, serde_json::to_string(&specs).map_err(to_serving)?)
        };
        let id = self
            .ctl
            .overwrite_table(table.schema.clone(), table.name.clone(), ipc, columns_json, lineage_json)
            .await
            .map_err(to_serving)?;
        Ok(control_plane_core::SnapshotId(id))
    }
}
```

Register in `src/services/query-api/src/lib.rs`: add `pub mod engine_action_client;`.

- [ ] **Step 3: Add the `spawn_engine_writer` helper to e2e_support**

In `src/services/query-api/tests/e2e_support.rs`, add the helper + guard. It mirrors `engine/tests/wire.rs::spawn_server` but injects the relocated writer and returns the query-api wire client:

```rust
use std::time::Duration;

/// Keeps the spawned engine server + its socket dir alive for the test's lifetime.
pub struct EngineGuard {
    _sock_dir: tempfile::TempDir,
    handle: tokio::task::JoinHandle<()>,
}
impl Drop for EngineGuard {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Spawn an `EngineControlService` on a UDS over `db` + `warehouse`, and return a
/// query-api `EngineActionClient` pointing at it (plus a keep-alive guard). The
/// engine writes to the same Postgres + warehouse the test reads from.
pub async fn spawn_engine_writer(
    fx: &PgFixture,
    db: &str,
    warehouse: &std::path::Path,
    inline_byte_limit: usize,
    flush_byte_threshold: i64,
) -> (query_api::engine_action_client::EngineActionClient, EngineGuard) {
    use control_plane_postgres::iceberg_sql_catalog::{
        SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalogBuilder,
    };
    use engine::service::EngineControlService;
    use engine_serving::IcebergActionWriter;
    use engine_wire::pb::engine_control_server::EngineControlServer;
    use iceberg::CatalogBuilder;
    use iceberg::io::LocalFsStorageFactory;
    use tonic::transport::Server;

    let mk_props = || {
        let mut p = std::collections::HashMap::new();
        p.insert(SQL_CATALOG_PROP_URI.to_string(), fx.pg_dsn(db));
        p.insert(
            SQL_CATALOG_PROP_WAREHOUSE.to_string(),
            format!("file://{}", warehouse.display()),
        );
        p
    };
    let build = || async {
        SqlCatalogBuilder::default()
            .with_storage_factory(std::sync::Arc::new(LocalFsStorageFactory))
            .load("loom", mk_props())
            .await
            .expect("build SqlCatalog")
    };

    let pool = fx.pool_for(db).await;
    let cp = control_plane_postgres::PgControlPlane::new(pool.clone(), Duration::from_millis(5000));
    let catalog = build().await;
    let writer = IcebergActionWriter::new(
        std::sync::Arc::new(build().await),
        pool.clone(),
        inline_byte_limit,
        flush_byte_threshold,
    );
    let svc = EngineControlService {
        cp,
        catalog,
        pool,
        retention: Duration::from_secs(7 * 24 * 3600),
        writer,
    };

    let sock_dir = tempfile::tempdir().expect("sock dir");
    let sock = sock_dir.path().join("engine.sock");
    let sock_str = sock.to_string_lossy().to_string();
    let listener = tokio::net::UnixListener::bind(&sock).expect("bind uds");
    let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);
    let handle = tokio::spawn(async move {
        drop(Server::builder().add_service(EngineControlServer::new(svc)).serve_with_incoming(incoming).await);
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    let client = query_api::engine_action_client::EngineActionClient::connect(sock_str)
        .await
        .expect("connect EngineActionClient");
    (client, EngineGuard { _sock_dir: sock_dir, handle })
}
```

> **Implementer note:** `e2e_support.rs` carries a crate/module `#![allow(...)]` for test-lint exemption (it is not a `rust_test` target). Keep panic-on-setup (`.expect`) — consistent with the existing `spawn_http` helper. Also promote `define_widget`/`grant_writer` here (Step 1 note).

- [ ] **Step 4: Wire the e2e-support + test BUCK deps**

In `src/services/query-api/BUCK`:
- Add to the `e2e-support` `rust_library` `deps`: `//src/services/engine:engine`, `//src/services/engine-serving:engine-serving`, `//src/services/engine-wire:engine-wire`, `//third-party:tonic`, `//third-party:tokio-stream` (it already deps `postgres`, `iceberg`, `tokio`, `tempfile`, query-api).
- Add the new test target:

```python
loom_fixture_test(
    name = "action-client-wire",
    crate = "action_client_wire",
    srcs = ["tests/action_client_wire.rs"],
    crate_root = "tests/action_client_wire.rs",
    deps = [
        ":e2e-support",
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

- [ ] **Step 5: Run to confirm fail → implement → pass**

Run: `buck2 test //src/services/query-api:action-client-wire > /tmp/t.log 2>&1; grep -E "FAIL|error\[|Tests finished" /tmp/t.log`
Expected: PASS once the module + helper compile and the row reads back.

- [ ] **Step 6: Commit**

```bash
git add src/services/query-api/src/engine_action_client.rs src/services/query-api/src/lib.rs src/services/query-api/tests/e2e_support.rs src/services/query-api/tests/action_client_wire.rs src/services/query-api/BUCK
git commit -m "feat(query-api): add EngineActionClient wire client + e2e spawn helper"
```

---

## Task 6: Switch `main.rs` + migrate the governed-write e2e suite to the wire

Flip the binary and all governed-write e2e tests to the wire client. The old `IcebergActionWriter` still exists (deleted in T7), so this task is purely a swap; the suite proves behavior parity.

**Files:**
- Modify: `src/services/query-api/src/main.rs`
- Modify: `src/services/query-api/tests/{action_e2e,overwrite_table_e2e,iceberg_action_e2e,update_delete_e2e,update_delete_governance_e2e,update_delete_tiers_e2e,http_wire_e2e}.rs`
- Modify: `src/services/query-api/BUCK` (add `:e2e-support` + engine deps to any migrated target that lacks them)

**Interfaces:**
- Consumes: `query_api::engine_action_client::EngineActionClient`, `e2e_support::spawn_engine_writer` (T5).

- [ ] **Step 1: Switch `main.rs` to the wire client**

In `src/services/query-api/src/main.rs`, replace the `IcebergActionWriter` construction block with an `EngineActionClient` over the same `engine_socket`, and delete `build_iceberg_catalog` + its `control_plane_postgres` imports (`SqlCatalogBuilder`, `SQL_CATALOG_PROP_*`) and the `IcebergActionWriter` import. Result:

```rust
    let (serving, action_engine): (Arc<dyn ServingEngine>, Arc<dyn ActionEngine>) = (
        Arc::new(EngineServingClient::connect(engine_socket.clone()).await?),
        Arc::new(query_api::engine_action_client::EngineActionClient::connect(engine_socket).await?),
    );
```

Stop reading `app_cfg.routing` (the field is removed in T7; leaving it unread now is fine). Keep `app_cfg.serving.default_limit`.

- [ ] **Step 2: Build the binary**

Run: `buck2 build //src/services/query-api:query-api-bin > /tmp/b.log 2>&1; grep -E "error|BUILD SUCCEEDED|Build ID" /tmp/b.log`
Expected: success. (`control_plane_postgres` may now be an unused direct dep of the bin — left for T7.)

- [ ] **Step 3: Migrate each direct-instantiation test**

For each of the six `run_action`-driven files (`action_e2e`, `overwrite_table_e2e`, `iceberg_action_e2e`, `update_delete_e2e`, `update_delete_governance_e2e`, `update_delete_tiers_e2e`): in every setup block, **replace**

```rust
let catalog = Arc::new(build_catalog(&dsn, warehouse.path()).await);
// ...
let engine = IcebergActionWriter::new(catalog, pool.clone(), INLINE, FLUSH);
```

**with**

```rust
let (engine, _eg) = e2e_support::spawn_engine_writer(&fx, &db, warehouse.path(), INLINE, FLUSH).await;
```

preserving each call site's exact `INLINE`/`FLUSH` values (note `update_delete_tiers_e2e.rs`'s `inline_byte_limit = 0` case at line ~385). Remove the now-unused local `build_catalog` fn and the `use ...::IcebergActionWriter;` / `SqlCatalog*` imports where they become unused. Keep `cp`, `pool`, `warehouse` (the warehouse tempdir must stay in scope — bind it to a `_warehouse`-style variable so it lives to the test's end). `ActionDeps { cp: &cp, action_engine: &engine, serving: &serving }` is unchanged (`&EngineActionClient` coerces to `&dyn ActionEngine`).

**Preserve ALL ontology/ACL setup.** Only the catalog+writer construction lines change. The `define_type`/`define_action`/`grant`/`define_widget`/`grant_writer` calls each test makes (e.g. `action_e2e.rs`'s `setup_widget_writer` defines the `Widget` type + `createWidget` action + grants inline alongside the old writer; `update_delete_tiers_e2e.rs` calls `define_widget(&cp)` + `grant_writer(&cp, &widget)` separately) MUST stay — the spawned engine has no ontology of its own, it writes to the same Postgres the test seeds. For `action_e2e.rs`, keep everything in `setup_widget_writer` except the `catalog`/`engine` lines (swap those for `spawn_engine_writer`, and have the returned struct carry the `EngineActionClient` + guard instead of the old `engine`). Do NOT drop any `cp.ontology()`/`cp.grant()` call.

Hold the `_eg` guard in scope for the test's duration.

- [ ] **Step 4: Migrate `http_wire_e2e.rs`**

This one injects the writer into `query_api::http::AppState`. Replace

```rust
action_engine: Arc::new(IcebergActionWriter::new(catalog.clone(), pool, INLINE_BYTE_LIMIT, i64::MAX)),
```

with a spawned-engine client:

```rust
let (action_client, eg) = e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), INLINE_BYTE_LIMIT, i64::MAX).await;
// ...
action_engine: Arc::new(action_client),
```

The ingest router still uses the in-process `IcebergMaterializer` over `catalog.clone()` (unchanged — ingest is out of scope). Return the `EngineGuard` from `iceberg_backend` alongside the warehouse keep-alive (extend the `Box<dyn Any + Send>` tuple or return the guard explicitly) so it outlives the spawned HTTP servers. Drop the now-unused `IcebergActionWriter` import.

- [ ] **Step 5: Update test BUCK deps**

For each migrated target in `src/services/query-api/BUCK`, ensure `deps` include `:e2e-support` (most already do) and that `:e2e-support` transitively provides engine/engine-serving/engine-wire (added in T5). Targets that constructed the catalog directly may keep their `postgres`/`iceberg` deps (still used for `cp`/seed). Remove deps that are now genuinely unused only if the build warns; otherwise leave them (a green build is the bar).

- [ ] **Step 6: Run the full governed-write suite**

```bash
buck2 test //src/services/query-api:action-e2e //src/services/query-api:overwrite-table-e2e //src/services/query-api:iceberg-action-e2e //src/services/query-api:update-delete-e2e //src/services/query-api:update-delete-governance-e2e //src/services/query-api:update-delete-tiers-e2e //src/services/query-api:http-wire-e2e > /tmp/t.log 2>&1; grep -E "FAIL|error\[|Tests finished" /tmp/t.log
```
Expected: all PASS. Investigate any failure as a behavior regression (compare to pre-change output) — do NOT weaken assertions.

- [ ] **Step 7: Commit**

```bash
git add src/services/query-api/src/main.rs src/services/query-api/tests/ src/services/query-api/BUCK
git commit -m "refactor(query-api): drive governed writes through the engine wire"
```

---

## Task 7: Remove the old writer, drop postgres from the library, add the decoupling guard

Delete the relocated-away code and the now-unneeded library deps, drop `routing` from query-api's config, and add a guard that the boundary cannot silently regress.

**Files:**
- Delete: `src/services/query-api/src/serving_datafusion.rs`
- Modify: `src/services/query-api/src/lib.rs` (drop the `serving_datafusion` module)
- Modify: `src/services/query-api/src/config.rs` (drop `routing`)
- Modify: `src/services/query-api/Cargo.toml` (drop `control-plane-postgres`; drop `ingest` if only used by `routing`)
- Modify: `src/services/query-api/BUCK` (drop `//src/control-plane/postgres` + `//src/services/ingest` from the `:query-api` **lib** target, and from `:query-api-bin` if `main.rs` no longer names them)
- Create: `tools/check-query-api-postgres-free.sh`

- [ ] **Step 1: Delete the old writer**

Delete `src/services/query-api/src/serving_datafusion.rs`. Remove `pub mod serving_datafusion;` from `src/services/query-api/src/lib.rs`. Confirm `encode_ipc_stream`/`to_serving` now live only in `engine_action_client.rs` (T5). Grep for any remaining references:

```bash
grep -rn "serving_datafusion\|IcebergActionWriter" src/services/query-api/src
```
Expected: no hits in `src/` (test files were migrated in T6; the only `IcebergActionWriter` left is `engine_serving::IcebergActionWriter`).

- [ ] **Step 2: Drop `routing` from query-api config**

In `src/services/query-api/src/config.rs`, remove the `routing` field from `QueryApiConfig`:

```rust
#[derive(Default, serde::Deserialize)]
#[serde(default)]
pub struct QueryApiConfig {
    pub serving: ServingTuning,
}
```

Grep for `ingest::` usage in `src/services/query-api/src`:

```bash
grep -rn "ingest::" src/services/query-api/src
```
If the only hit was the removed `routing: ingest::config::RoutingTuning`, the `ingest` dep can be dropped from the lib (next step). If `ingest` is referenced elsewhere in the lib, keep it.

- [ ] **Step 3: Drop the postgres (+ ingest) deps from the library**

In `src/services/query-api/Cargo.toml`, remove `control-plane-postgres` (and `ingest` if Step 2 cleared it). In `src/services/query-api/BUCK`, remove `//src/control-plane/postgres:postgres` (and `//src/services/ingest:ingest` if applicable) from the `:query-api` lib `deps`. Then check whether `main.rs` still names `control_plane_postgres` directly:

```bash
grep -n "control_plane_postgres" src/services/query-api/src/main.rs
```
If no hits, also remove `//src/control-plane/postgres:postgres` from the `:query-api-bin` `deps` (the bin still links it transitively via `//src/services/runtime`, which is slice-2's concern). If `main.rs` still names it, leave the bin dep (do not chase slice 2).

- [ ] **Step 4: Add the decoupling guard script**

Create `tools/check-query-api-postgres-free.sh`:

```bash
#!/usr/bin/env bash
# Slice-1 boundary guard: query-api's LIBRARY target must not (transitively) depend
# on the control-plane/postgres crate. See
# docs/superpowers/specs/2026-06-29-engine-serving-write-relocation-design.md.
set -euo pipefail
out="$(buck2 cquery 'deps(//src/services/query-api:query-api)' 2>/dev/null)"
if grep -q '//src/control-plane/postgres:postgres' <<<"$out"; then
  echo "FAIL: //src/services/query-api:query-api still depends on control-plane/postgres" >&2
  exit 1
fi
echo "OK: query-api library is postgres-free"
```

Make it executable: `chmod +x tools/check-query-api-postgres-free.sh`.

- [ ] **Step 5: Prove the decoupling + full build/test**

```bash
bash tools/check-query-api-postgres-free.sh
buck2 build //src/... > /tmp/b.log 2>&1; grep -E "error|BUILD SUCCEEDED|Build ID" /tmp/b.log
buck2 test //src/services/query-api/... //src/services/engine/... //src/services/engine-serving/... //src/services/engine-wire/... //src/control-plane/core/... > /tmp/t.log 2>&1; grep -E "FAIL|Tests finished" /tmp/t.log
```
Expected: guard prints OK; build succeeds; tests pass. Also run `./tools/clippy-all.sh > /tmp/c.log 2>&1; grep -E "warning|error" /tmp/c.log` and resolve any new lints.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor(query-api): drop control-plane/postgres from the library (zero-postgres lib, slice 1)"
```

---

## Final verification (whole-tree green + sqlx cache)

- [ ] Run the full suite the way CI does and confirm green:

```bash
buck2 build //src/... > /tmp/build.log 2>&1; grep -E "error|BUILD SUCCEEDED|Build ID" /tmp/build.log
buck2 test //src/... > /tmp/test.log 2>&1; grep -E "FAIL|Tests finished" /tmp/test.log
```

- [ ] If any SQL changed (none expected — the writer's SQL moved verbatim, not edited), run `tools/sqlx-prepare.sh` and commit `.sqlx`. (No SQL text changed here, so the committed cache should be untouched; if `//src/control-plane/postgres:sqlx-cache-check` fails, investigate before proceeding.)
- [ ] Run `buck2 run //tools:prek -- run --all-files > /tmp/prek.log 2>&1; grep -E "Failed|Passed|error" /tmp/prek.log` and commit any hook fixes.
- [ ] Update the registers with `loom-docs-update`: close `road-engine-serving-write-relocation` (`- [ ]`→`- [x]`, `status:done`, add `pr:#N`) and confirm `fut-query-api-wire-control-plane` (slice 2) is recorded.

---

## Self-Review

**Spec coverage:**
- *Relocate the writer* → T3 (writer into engine-serving), T7 (old one deleted). ✓
- *`inline_byte_limit`/`flush_byte_threshold` move to the engine; query-api no longer carries write-tuning config* → T4 Step 5 (engine reads the env knobs), T7 Step 2 (query-api `routing` dropped). ✓
- *`encode_ipc_stream` stays query-api-side* → T5 (moved into `engine_action_client.rs`). ✓
- *Two new `EngineControl` unary RPCs (`WriteObject`, `OverwriteTable`)* → T4 (proto + handlers + client). ✓
- *query-api becomes a thin wire client* → T5 (`EngineActionClient`), T6 (main + tests). ✓
- *Governance boundary unchanged (ACL pre-wire)* → preserved: `ActionEngine` trait + `action.rs` handler untouched; only the impl swaps. ✓
- *Error mapping: writer errors → gRPC `Status` → query-api `ServingError`* → T3 (`EngineServingError`), T4 (`Status::internal`/`invalid_argument`), T5 (`to_serving`). ✓
- *Existing governed-write e2e suite stays green through the wire* → T6 (all 7 files migrated). ✓
- *New engine-level wire test for `WriteObject`/`OverwriteTable`* → T4 (`write_wire.rs`). ✓
- *All new tests are `rust_test`; fixtures via `loom_fixture_test`* → every test target above. ✓
- *Decoupling assertion (`cquery` no longer lists postgres), optionally encoded as a check* → T7 (`check-query-api-postgres-free.sh`). ✓
- *Out of scope (slice 2): wire-backed `ControlPlane`, removing postgres from the binary* → explicitly not done (T7 leaves the bin's transitive postgres via `runtime`). ✓

**Placeholder scan:** No `TBD`/`handle edge cases`/"similar to Task N" — every code step has concrete code. The three `Implementer note`s flag genuine local-helper resolutions (`define_widget`/`grant_writer` promotion, `SqlValue` variant confirmation, `seed_widget_table` shape) against named existing sources, not deferred work.

**Type consistency:** `IcebergActionWriter::new(Arc<SqlCatalog>, PgPool, usize, i64)` is consistent across T3/T4/T5. The relocated writer methods take `ipc: &[u8]` (T3) and the handlers pass `&r.ipc` (T4). `LineageWire::from(&LineageEvent)` (encode) and `LineageWire::try_into()` (decode) are paired (T2/T4/T5). `GrpcQueueClient::{write_object, overwrite_table}` signatures match between definition (T4) and call (T5). `EngineControlService` gains exactly one field `writer` (T4), updated in all four constructors (main, wire.rs, compact_wire.rs, e2e_support). RPC response field `snapshot_id: i64` ↔ `SnapshotId(i64)` round-trips consistently.
