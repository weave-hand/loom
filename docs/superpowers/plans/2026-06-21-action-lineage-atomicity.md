# Action lineage atomicity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make an ontology action's row write and its lineage event commit atomically (both land or neither does), and surface the action's `run_id` so its lineage is queryable.

**Architecture:** Route the action write through loom's own snapshot-commit primitive (`ingest::materialize::land_ducklake`) instead of DuckDB's `DATA_INLINING` insert. The `ActionEngine` seam is reshaped from `insert_row` (opaque, non-atomic) to `write_object(... , event) -> SnapshotId` (writes the row AND commits the lineage event in one Postgres transaction). The caller mints the `run_id`, builds the `LineageEvent`, hands it to the engine, and returns the `run_id`; `post_action` echoes it in an `X-Loom-Run-Id` header.

**Tech Stack:** Rust 2024, buck2, async-trait, Arrow, DataFusion/object_store (via the ingest crate), DuckLake-on-DuckDB, Postgres control plane.

## Global Constraints

- **Tests are `rust_test` integration targets only** — NO inline `#[cfg(test)]` modules (the `no-inline-tests` prek hook fails the build otherwise). Each test is a sibling `tests/<name>.rs` wired as its own target in `src/services/query-api/BUCK`.
- **Fixture-backed tests** (real Postgres/DuckDB) MUST use the `loom_fixture_test` macro, not a bare `rust_test`. Pure-logic tests use `rust_test`.
- **Run the suite with** `buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log` — never pipe `buck2 test` through `tail`/`head` (it stalls). The full sweep matters: this change touches a shared trait, so per-crate green is not enough.
- **Markdown lint:** any `.md` file must end with exactly one trailing newline and have no trailing whitespace (`end-of-file-fixer` / `trim trailing whitespace` hooks run on all files in CI `lint`).
- No SQL changes ⇒ the `.sqlx` cache is untouched.
- `ingest` must remain acyclic with `query-api` (ingest does NOT depend on query-api — verified).

---

### Task 1: Pure one-row Arrow batch builder (`build_object_batch`)

A pure helper that turns an aligned `(columns, values, logical_types)` triple into a one-row Arrow `RecordBatch` + `Schema` + loom `ColumnSpec` list. This is the only genuinely standalone, unit-testable unit; `DuckLakeActionWriter` (Task 2) consumes it.

**Files:**
- Modify: `src/services/query-api/src/serving.rs` (add `build_object_batch` + the private `one_cell` helper; new imports).
- Create: `src/services/query-api/tests/build_object_batch.rs`
- Modify: `src/services/query-api/BUCK` (add the `build-object-batch` `rust_test` target).

**Interfaces:**
- Produces: `pub fn build_object_batch(columns: &[String], values: &[SqlValue], logical_types: &[String]) -> Result<(std::sync::Arc<arrow::datatypes::Schema>, arrow::array::RecordBatch, Vec<control_plane_core::ColumnSpec>), ServingError>` in module `query_api::serving`.

- [ ] **Step 1: Write the failing test**

Create `src/services/query-api/tests/build_object_batch.rs`:

```rust
//! build_object_batch: a one-row Arrow batch + schema + ColumnSpec list from an
//! aligned (columns, values, logical_types) triple. Pure logic (no DB).

use arrow::array::{Array, Int64Array, StringArray};
use query_api::serving::{SqlValue, build_object_batch};

#[test]
fn builds_one_row_batch_with_typed_null_and_specs() {
    let (schema, batch, specs) = build_object_batch(
        &["id".to_string(), "name".to_string()],
        &[SqlValue::Int(7), SqlValue::Null],
        &["Long".to_string(), "String".to_string()],
    )
    .expect("builds");

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 2);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(1).name(), "name");
    // ColumnSpec.ty is the loom LOGICAL canonical name (not the physical type).
    assert_eq!(
        specs.iter().map(|s| s.ty.as_str()).collect::<Vec<_>>(),
        vec!["long", "string"]
    );
    assert!(specs.iter().all(|s| s.nullable), "action columns are nullable");

    let id = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(id.value(0), 7);
    let name = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(name.is_null(0), "unset property is a typed null");
}

#[test]
fn rejects_unknown_logical_type() {
    let err = build_object_batch(
        &["x".to_string()],
        &[SqlValue::Int(1)],
        &["Bogus".to_string()],
    )
    .unwrap_err();
    assert!(
        format!("{err}").contains("unknown logical type"),
        "got {err}"
    );
}

#[test]
fn rejects_value_type_mismatch() {
    // A Long column handed a Text value is a fault, not a silent coercion.
    let err = build_object_batch(
        &["id".to_string()],
        &[SqlValue::Text("nope".into())],
        &["Long".to_string()],
    )
    .unwrap_err();
    assert!(format!("{err}").contains("does not match"), "got {err}");
}

#[test]
fn rejects_length_mismatch() {
    let err =
        build_object_batch(&["x".to_string()], &[], &["Long".to_string()]).unwrap_err();
    assert!(format!("{err}").contains("columns"), "got {err}");
}
```

Add the `rust_test` target to `src/services/query-api/BUCK` (place it near the other pure `rust_test`s):

```python
rust_test(
    name = "build-object-batch",
    crate = "build_object_batch",
    srcs = ["tests/build_object_batch.rs"],
    crate_root = "tests/build_object_batch.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//third-party:arrow",
    ],
)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test //src/services/query-api:build-object-batch > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[|cannot find" /tmp/t.log`
Expected: FAIL — `build_object_batch` does not exist (compile error `cannot find function`).

- [ ] **Step 3: Implement `build_object_batch` + `one_cell` in `serving.rs`**

At the top of `src/services/query-api/src/serving.rs`, add the imports the helper needs (the file currently imports only `async_trait` and `crate::sql::...`; add these alongside):

```rust
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use control_plane_core::{BaseType, ColumnSpec, resolve_logical};
```

Then add the public function and its private cell helper (place them after the `SqlValue` definition, before the `ServingEngine` trait):

```rust
/// Build a one-row Arrow `RecordBatch` + `Schema` + loom `ColumnSpec` list from an
/// aligned `(columns, values, logical_types)` triple. Each logical type resolves
/// (via `resolve_logical`) to a `BaseType` that fixes BOTH the Arrow `DataType` and
/// the loom logical `ColumnSpec.ty` (its canonical name). Every field is nullable —
/// an action write passes `SqlValue::Null` for any property it does not set. A length
/// mismatch, an empty input, an unknown logical type, or a value whose variant does
/// not match its column's base type is a `ServingError::Engine`.
pub fn build_object_batch(
    columns: &[String],
    values: &[SqlValue],
    logical_types: &[String],
) -> Result<(Arc<Schema>, RecordBatch, Vec<ColumnSpec>), ServingError> {
    if columns.is_empty()
        || columns.len() != values.len()
        || columns.len() != logical_types.len()
    {
        return Err(ServingError::Engine(format!(
            "build_object_batch: {} columns / {} values / {} types (need >= 1, equal counts)",
            columns.len(),
            values.len(),
            logical_types.len()
        )));
    }
    let mut fields: Vec<Field> = Vec::with_capacity(columns.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    let mut specs: Vec<ColumnSpec> = Vec::with_capacity(columns.len());
    for ((name, value), logical) in columns.iter().zip(values).zip(logical_types) {
        let base = resolve_logical(logical)
            .ok_or_else(|| ServingError::Engine(format!("unknown logical type `{logical}`")))?;
        let (dt, array) = one_cell(base, value, name)?;
        fields.push(Field::new(name, dt, true));
        arrays.push(array);
        specs.push(ColumnSpec {
            name: name.clone(),
            ty: base.canonical_name().to_string(),
            nullable: true,
        });
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| ServingError::Engine(e.to_string()))?;
    Ok((schema, batch, specs))
}

/// One single-row Arrow array for a cell of base type `base`. `SqlValue::Null`
/// yields a typed null; any non-null variant must match `base` (the same
/// scalar-to-Arrow mapping `to_duck` uses) or it is a `ServingError`.
fn one_cell(
    base: BaseType,
    v: &SqlValue,
    col: &str,
) -> Result<(DataType, ArrayRef), ServingError> {
    let mismatch =
        || ServingError::Engine(format!("value for column `{col}` does not match its {base:?} type"));
    Ok(match base {
        BaseType::Integer => {
            let cell: Option<i32> = match v {
                SqlValue::Null => None,
                SqlValue::Int(i) => Some((*i).try_into().map_err(|_| {
                    ServingError::Engine(format!("integer overflow for column `{col}`"))
                })?),
                _ => return Err(mismatch()),
            };
            (DataType::Int32, Arc::new(Int32Array::from(vec![cell])))
        }
        BaseType::Long => {
            let cell: Option<i64> = match v {
                SqlValue::Null => None,
                SqlValue::Int(i) => Some(*i),
                _ => return Err(mismatch()),
            };
            (DataType::Int64, Arc::new(Int64Array::from(vec![cell])))
        }
        BaseType::Double => {
            let cell: Option<f64> = match v {
                SqlValue::Null => None,
                SqlValue::Double(f) => Some(*f),
                _ => return Err(mismatch()),
            };
            (DataType::Float64, Arc::new(Float64Array::from(vec![cell])))
        }
        BaseType::Boolean => {
            let cell: Option<bool> = match v {
                SqlValue::Null => None,
                SqlValue::Bool(b) => Some(*b),
                _ => return Err(mismatch()),
            };
            (DataType::Boolean, Arc::new(BooleanArray::from(vec![cell])))
        }
        BaseType::String => {
            let cell: Option<String> = match v {
                SqlValue::Null => None,
                SqlValue::Text(s) => Some(s.clone()),
                _ => return Err(mismatch()),
            };
            (DataType::Utf8, Arc::new(StringArray::from(vec![cell])))
        }
        BaseType::Date => {
            let cell: Option<i32> = match v {
                SqlValue::Null => None,
                SqlValue::Date(d) => {
                    Some((*d - time::macros::date!(1970 - 01 - 01)).whole_days() as i32)
                }
                _ => return Err(mismatch()),
            };
            (DataType::Date32, Arc::new(Date32Array::from(vec![cell])))
        }
        BaseType::Timestamp => {
            let cell: Option<i64> = match v {
                SqlValue::Null => None,
                SqlValue::Timestamp(ts) => Some(
                    (ts.assume_utc() - time::OffsetDateTime::UNIX_EPOCH)
                        .whole_microseconds()
                        .try_into()
                        .unwrap_or(i64::MAX),
                ),
                _ => return Err(mismatch()),
            };
            (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                Arc::new(TimestampMicrosecondArray::from(vec![cell])),
            )
        }
    })
}
```

Note: `BaseType`, `ColumnSpec`, and `resolve_logical` are all re-exported from `control_plane_core` (verified: `action.rs` already imports `resolve_logical` from there; `BaseType::canonical_name` and `ColumnSpec { name, ty, nullable }` exist).

- [ ] **Step 4: Run the test to verify it passes**

Run: `buck2 test //src/services/query-api:build-object-batch > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (4 tests).

Also confirm the library still builds and clippy is clean:
Run: `buck2 build //src/services/query-api:query-api '//src/services/query-api:query-api[clippy.txt]' > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|error" /tmp/b.log; wc -c < buck-out/*/gen/src/services/query-api/query-api*clippy.txt 2>/dev/null || true`
Expected: build succeeds.

- [ ] **Step 5: Commit**

```bash
git add src/services/query-api/src/serving.rs src/services/query-api/tests/build_object_batch.rs src/services/query-api/BUCK
git commit -m "feat(query-api): one-row Arrow batch builder for action writes"
```

---

### Task 2: Reshape the `ActionEngine` seam to an atomic `write_object` + `DuckLakeActionWriter`

This is the compilation-atomic core: renaming the trait method ripples to every implementor and the caller, so they all change together. It adds `DuckLakeActionWriter` (the loom-owned atomic writer), reworks `run_action` to build the lineage event up front / return the `run_id`, removes `EmbeddedDuckDbWriter`, rewires `main.rs`, and updates every `ActionEngine` impl and the e2e/handler tests to the new seam — leaving the whole tree green. The handler and e2e tests are the TDD drivers for the new atomic behavior.

**Files:**
- Modify: `src/services/query-api/src/serving.rs` — new trait method `write_object`; add `DuckLakeActionWriter`; remove `EmbeddedDuckDbWriter` + `INLINE_ROW_LIMIT`.
- Modify: `src/services/query-api/src/serving_datafusion.rs` — `UnsupportedActionEngine` to `write_object`.
- Modify: `src/services/query-api/src/action.rs` — `run_action` rework; return `(ObjectRows, RunId)`.
- Modify: `src/services/query-api/src/http.rs` — `post_action` destructures the tuple (header comes in Task 3).
- Modify: `src/services/query-api/src/main.rs` — construct `DuckLakeActionWriter`.
- Modify: `src/services/query-api/BUCK` — add `ingest` dep to `:query-api`; add `object_store` dep to `query-api-bin`; remove the `action-engine` target; extend `action-e2e` deps.
- Modify: `src/services/query-api/tests/e2e_support.rs` — `StubAction` to `write_object`.
- Modify: `src/services/query-api/tests/http_smoke.rs` — its stub `ActionEngine` to `write_object`.
- Modify: `src/services/query-api/tests/action_conformance_handler.rs` — `RecordingEngine` to `write_object`; add the run_id / atomic-lineage assertions.
- Modify: `src/services/query-api/tests/action_conformance_http.rs` — `OkEngine` to `write_object`.
- Modify: `src/services/query-api/tests/unsupported_action.rs` — call `write_object`.
- Rewrite: `src/services/query-api/tests/action_e2e.rs` — use `DuckLakeActionWriter`; assert read-back AND lineage-for-run_id AND a Parquet file was written.
- Delete: `src/services/query-api/tests/action_engine.rs` (tested the removed `EmbeddedDuckDbWriter`).

**Interfaces:**
- Consumes: `query_api::serving::build_object_batch` (Task 1); `ingest::materialize::land_ducklake(cp: &dyn ControlPlane, store: Arc<dyn ObjectStore>, table: &TableRef, schema: Arc<Schema>, columns: &[ColumnSpec], batches: &[RecordBatch], file_prefix: &str, lineage: LineageEvent) -> Result<SnapshotId, IngestError>`.
- Produces:
  - `ActionEngine::write_object(&self, table: &TableRef, columns: &[String], values: &[SqlValue], logical_types: &[String], event: LineageEvent) -> Result<SnapshotId, ServingError>` (replaces `insert_row`).
  - `pub struct DuckLakeActionWriter` with `pub fn new(cp: Arc<dyn ControlPlane>, store: Arc<dyn ObjectStore>) -> Self`.
  - `query_api::action::run_action(...) -> Result<(ObjectRows, RunId), ActionError>` (was `Result<ObjectRows, ActionError>`).

- [ ] **Step 1: Reshape the trait + add `DuckLakeActionWriter`; remove `EmbeddedDuckDbWriter` (serving.rs)**

In `src/services/query-api/src/serving.rs`:

Replace the `ActionEngine` trait (currently `insert_row`) with:

```rust
/// A write-capable serving engine — the atomic action write-back seam. An impl
/// writes ONE row AND commits its lineage event in the same transaction, so the
/// snapshot and its lineage land or roll back together (no dangling slice). The
/// seam is Arrow-free (takes `logical_types`, not a `RecordBatch`); each impl
/// builds Arrow internally. Mirrors the Iceberg `inline_append(.., lineage) ->
/// SnapshotId` contract.
#[async_trait]
pub trait ActionEngine: Send + Sync {
    async fn write_object(
        &self,
        table: &control_plane_core::TableRef,
        columns: &[String],
        values: &[SqlValue],
        logical_types: &[String],
        event: control_plane_core::LineageEvent,
    ) -> Result<control_plane_core::SnapshotId, ServingError>;
}
```

Delete the entire `EmbeddedDuckDbWriter` struct, its `impl EmbeddedDuckDbWriter` (incl. `INLINE_ROW_LIMIT` and `attach`), and its `impl ActionEngine for EmbeddedDuckDbWriter`. Keep `EmbeddedDuckDb` (the reader), `run_sync`, `to_duck`, `from_duck`, and everything else.

Add the new writer (place it where `EmbeddedDuckDbWriter` was). It needs `ControlPlane`, `LineageEvent`, `SnapshotId`, `TableRef` from core and `ObjectStore`:

```rust
use control_plane_core::{ControlPlane, LineageEvent, SnapshotId, TableRef};
use object_store::ObjectStore;

/// Action writer that routes a single-row write through loom's OWN atomic
/// snapshot-commit primitive (`ingest::materialize::land_ducklake`): build a
/// one-row Parquet file, then create_table (idempotent — the target table already
/// exists) + append_files + emit(lineage) + commit, all in one Postgres
/// transaction, returning the new `SnapshotId`. The row and its lineage land or
/// roll back together. Replaces the inline `EmbeddedDuckDbWriter`; the part-1
/// "inline row, no Parquet" low-latency property is retired in favor of atomicity
/// (actions are interactive, low-frequency; small files are handled by compaction).
pub struct DuckLakeActionWriter {
    cp: Arc<dyn ControlPlane>,
    store: Arc<dyn ObjectStore>,
}

impl DuckLakeActionWriter {
    pub fn new(cp: Arc<dyn ControlPlane>, store: Arc<dyn ObjectStore>) -> Self {
        Self { cp, store }
    }
}

#[async_trait]
impl ActionEngine for DuckLakeActionWriter {
    async fn write_object(
        &self,
        table: &TableRef,
        columns: &[String],
        values: &[SqlValue],
        logical_types: &[String],
        event: LineageEvent,
    ) -> Result<SnapshotId, ServingError> {
        let (schema, batch, specs) = build_object_batch(columns, values, logical_types)?;
        // Unique per action: the run id keeps each write's files in their own dir.
        let file_prefix = format!("action-{}", event.run_id.0);
        ingest::materialize::land_ducklake(
            self.cp.as_ref(),
            self.store.clone(),
            table,
            schema,
            &specs,
            std::slice::from_ref(&batch),
            &file_prefix,
            event,
        )
        .await
        .map_err(|e| ServingError::Engine(e.to_string()))
    }
}
```

(`Arc` is already imported from Task 1's additions. The crate is named `ingest` — `e2e_support.rs` already does `use ingest::{...}`, so `ingest::materialize::land_ducklake` resolves once the BUCK dep is added in Step 6.)

- [ ] **Step 2: Update `UnsupportedActionEngine` (serving_datafusion.rs)**

In `src/services/query-api/src/serving_datafusion.rs`, replace the `insert_row` impl with `write_object` (it still rejects). Its imports already include `ActionEngine, ServingError, SqlValue`; add `LineageEvent`, `SnapshotId` (and keep `TableRef`) from `control_plane_core` to the existing use:

```rust
#[async_trait]
impl ActionEngine for UnsupportedActionEngine {
    async fn write_object(
        &self,
        _table: &TableRef,
        _columns: &[String],
        _values: &[SqlValue],
        _logical_types: &[String],
        _event: control_plane_core::LineageEvent,
    ) -> Result<control_plane_core::SnapshotId, ServingError> {
        Err(ServingError::Engine(
            "actions unsupported on the iceberg serving backend".into(),
        ))
    }
}
```

(Use the path-qualified `control_plane_core::LineageEvent` / `::SnapshotId` if adding them to the `use` is noisier; either is fine as long as it compiles. Wiring the Iceberg `inline_append` as a real backend stays deferred — `fut-iceberg-actionengine`.)

- [ ] **Step 3: Rework `run_action` (action.rs)**

In `src/services/query-api/src/action.rs`:

Change the signature return type:

```rust
pub async fn run_action(
    action_name: &str,
    body: &serde_json::Map<String, Value>,
    subject: &SubjectId,
    deps: &ActionDeps<'_>,
) -> Result<(ObjectRows, RunId), ActionError> {
```

Keep steps 1–4b unchanged (resolve action, resolve target, coarse Write gate, conformance, `parse_params` → `pairs`/`columns`/`values`, the fine-grained `set_columns`/`set_values` write-policy gate). Then REPLACE the old steps 5–7 (the `insert_row` call, the `current_snapshot` lookup, the best-effort `lineage().emit()`, and the final `Ok(ObjectRows {...})`) with:

```rust
    // 5. Expand to the target type's FULL property set (declared order): the parsed
    //    value when the action set the column, else NULL. The loom-owned Parquet
    //    write must carry every column so the file schema matches the table (part-1
    //    relied on DuckDB defaulting unspecified columns to NULL).
    use std::collections::HashMap;
    let parsed: HashMap<&str, &SqlValue> =
        pairs.iter().map(|(c, v)| (c.as_str(), v)).collect();
    let mut full_columns: Vec<String> = Vec::with_capacity(target.properties.len());
    let mut full_values: Vec<SqlValue> = Vec::with_capacity(target.properties.len());
    let mut full_logical: Vec<String> = Vec::with_capacity(target.properties.len());
    for p in &target.properties {
        full_columns.push(p.name.clone());
        full_values.push(parsed.get(p.name.as_str()).copied().cloned().unwrap_or(SqlValue::Null));
        full_logical.push(p.ty.clone());
    }

    // 6. Mint the run id and build the lineage event UP FRONT, so the caller owns the
    //    run_id and hands it to the engine, which commits row + event atomically.
    //    inputs=[] (a create-from-params action has no upstream datasets). The old
    //    post-hoc snapshot_id payload is dropped: the event now commits WITH the
    //    snapshot, so their linkage is structural, not a best-effort breadcrumb.
    let run_id = RunId(Uuid::new_v4());
    let event = LineageEvent {
        run_id,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetRef::from(&action.target)],
        payload: serde_json::json!({ "action": action_name }),
    };

    // 7. Atomic write: row + lineage in one transaction (no dangling slice). On any
    //    failure the Tx rolls back — no snapshot, no lineage, no partial state.
    deps.action_engine
        .write_object(&target.table, &full_columns, &full_values, &full_logical, event)
        .await?;

    // 8. Return the created object (action-provided columns only, as part-1 returns)
    //    plus the run_id so the caller can locate the action's lineage.
    let logical_types = columns
        .iter()
        .map(|c| {
            target
                .properties
                .iter()
                .find(|p| &p.name == c)
                .map(|p| p.ty.clone())
                .unwrap_or_default()
        })
        .collect();
    Ok((
        ObjectRows {
            columns,
            logical_types,
            rows: vec![values],
        },
        run_id,
    ))
```

Remove now-unused imports if the compiler flags them. After this rework `ControlPlaneError` is still used (the `?`/match in steps 1–2) and `PageReq` still used (write policy); `EventType`, `LineageEvent`, `RunId`, `DatasetRef`, `Uuid`, `resolve_logical` are all still used. Update the module doc comment at the top of `action.rs` — it currently says "executes the inline write via the ActionEngine; emits best-effort type-named lineage (a documented dangling slice…)". Replace with: "executes an ATOMIC write via the ActionEngine (`write_object`), which commits the row and its lineage event in one transaction, and returns the action's `run_id`."

- [ ] **Step 4: Adapt `post_action` to the tuple (http.rs)**

In `src/services/query-api/src/http.rs`, `post_action`'s match arm changes from `Ok(rows) =>` to destructure the tuple (the header is added in Task 3 — ignore `run_id` for now to keep this step minimal):

```rust
    match crate::action::run_action(&action_name, &obj, &SubjectId(subject), &deps).await {
        Ok((rows, _run_id)) => {
            let body = crate::render::objects_to_json(&rows);
            // objects_to_json yields {"objects":[{...}]}; return the single created object.
            let one = body
                .get("objects")
                .and_then(|a| a.as_array())
                .and_then(|a| a.first())
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            (StatusCode::CREATED, Json(one)).into_response()
        }
        // ... unchanged error arms ...
    }
```

- [ ] **Step 5: Rewire `main.rs`**

In `src/services/query-api/src/main.rs`:

Change the import line from `EmbeddedDuckDbWriter` to `DuckLakeActionWriter`:

```rust
use query_api::serving::{ActionEngine, DuckLakeActionWriter, EmbeddedDuckDb, ServingEngine};
```

Replace the `ServingBackend::DuckLake` arm (the control plane `cp` is already an `Arc<dyn ControlPlane>` built above; build a `LocalFileSystem` store rooted at `cfg.data_path` and share `cp`):

```rust
    let (serving, action_engine): (Arc<dyn ServingEngine>, Arc<dyn ActionEngine>) = match backend {
        ServingBackend::DuckLake => {
            let store: Arc<dyn object_store::ObjectStore> =
                Arc::new(service_runtime::local_store(&cfg.data_path)?);
            (
                Arc::new(EmbeddedDuckDb::attach(&cfg.db.ducklake_libpq(), &cfg.data_path).await?),
                Arc::new(DuckLakeActionWriter::new(cp.clone(), store)),
            )
        }
        ServingBackend::Iceberg => (
            Arc::new(DataFusionServingEngine::new(IcebergCatalog::new(pool))),
            Arc::new(UnsupportedActionEngine),
        ),
    };
```

(`service_runtime::local_store(&Path) -> Result<LocalFileSystem, RuntimeError>` exists and `RuntimeError` already converts into the `Box<dyn Error>` return via `?` — verify the `?` compiles; if `RuntimeError` lacks the `std::error::Error` impl path, map it: `.map_err(|e| -> Box<dyn std::error::Error> { e.into() })?`. `cp.clone()` is an `Arc` clone; `cp` is still moved into `AppState` afterward.)

- [ ] **Step 6: BUCK — deps, remove dead target**

In `src/services/query-api/BUCK`:

1. Add `"//src/services/ingest:ingest",` to the `:query-api` `rust_library` `deps` (keep the list sorted-ish; it sits before `//third-party:arrow`).
2. Add `"//third-party:object_store",` to the `query-api-bin` `rust_binary` `deps`.
3. DELETE the entire `action-engine` `loom_fixture_test` target (the `name = "action-engine"` block).
4. Extend the `action-e2e` `loom_fixture_test` `deps` with `"//third-party:object_store",` (the rewrite constructs a `LocalFileSystem`). Keep the existing core/postgres/serde_json/tokio deps.

- [ ] **Step 7: Update the compile-only impls (stubs) — e2e_support, http_smoke, action_conformance_http**

Each of these has an `ActionEngine` impl that must move to `write_object` returning a dummy `SnapshotId`. They are never exercised on the paths these files test, so the body just returns `Ok(SnapshotId(0))` (or records, see Step 8 for the handler).

`tests/e2e_support.rs` — `StubAction`:

```rust
#[async_trait]
impl ActionEngine for StubAction {
    async fn write_object(
        &self,
        _table: &TableRef,
        _columns: &[String],
        _values: &[SqlValue],
        _logical_types: &[String],
        _event: control_plane_core::LineageEvent,
    ) -> std::result::Result<control_plane_core::SnapshotId, ServingError> {
        Ok(control_plane_core::SnapshotId(0))
    }
}
```

Add whatever import makes `SnapshotId`/`LineageEvent` resolve (e2e_support already imports many `control_plane_core` items; extend that `use` or path-qualify). Update the `StubAction` doc comment to say "no-op atomic write engine".

`tests/http_smoke.rs` — its stub `ActionEngine` impl: same `write_object` shape returning `Ok(control_plane_core::SnapshotId(0))`. `TableRef` is already imported there; path-qualify `control_plane_core::SnapshotId` and `_event: control_plane_core::LineageEvent` in the impl (the target already deps `//src/control-plane/core:core`, so they resolve — no new BUCK dep needed).

`tests/action_conformance_http.rs` — `OkEngine`: same `write_object` shape returning `Ok(SnapshotId(1))` (the post test only checks status codes).

- [ ] **Step 8: Update `RecordingEngine` + add the atomic-lineage handler assertions (action_conformance_handler.rs)**

In `src/services/query-api/tests/action_conformance_handler.rs`, change `RecordingEngine` to record the delivered `LineageEvent`s (so the test can assert the run_id and that lineage is delivered to the engine, not via a separate `Lineage::emit`):

```rust
use control_plane_core::{ /* existing… */ DatasetRef, LineageEvent, SnapshotId };

/// An ActionEngine that records every LineageEvent it is handed (the atomic seam).
struct RecordingEngine {
    events: Mutex<Vec<LineageEvent>>,
}

impl RecordingEngine {
    fn new() -> Self {
        Self { events: Mutex::new(Vec::new()) }
    }
    fn events(&self) -> Vec<LineageEvent> {
        self.events.lock().unwrap().clone()
    }
}

#[async_trait]
impl ActionEngine for RecordingEngine {
    async fn write_object(
        &self,
        _table: &TableRef,
        _columns: &[String],
        _values: &[SqlValue],
        _logical_types: &[String],
        event: LineageEvent,
    ) -> Result<SnapshotId, ServingError> {
        self.events.lock().unwrap().push(event);
        Ok(SnapshotId(1))
    }
}
```

Update the existing tests:
- `misconfigured_action_is_rejected_before_insert`: replace `engine.calls()` assertion with `assert!(engine.events().is_empty(), "no write for a misconfigured action");`.
- `write_denied_subject_is_forbidden_not_misconfigured`: replace `assert_eq!(engine.calls(), 0)` with `assert!(engine.events().is_empty());`.
- `conformant_action_runs_the_insert`: it now gets a tuple and one recorded event:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn conformant_action_runs_the_insert() {
    let (cp, subj) = seeded().await;
    let engine = RecordingEngine::new();
    let deps = ActionDeps { cp: &cp, action_engine: &engine };
    let body = json!({"id": "42", "name": "gadget"});
    let (rows, run_id) = run_action("createWidget", body.as_object().unwrap(), &subj, &deps)
        .await
        .unwrap();
    assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
    let events = engine.events();
    assert_eq!(events.len(), 1, "conformant action writes once");
    // The returned run_id IS the run_id of the event handed to the engine (atomic seam).
    assert_eq!(events[0].run_id, run_id);
    assert_eq!(
        events[0].outputs,
        vec![DatasetRef::from(&TypeName("Widget".into()))],
        "lineage names the target type's dataset"
    );
    // run_action no longer emits separately: nothing landed in the control plane's
    // own lineage (only the engine received the event, in its atomic commit).
    use control_plane_core::{Lineage, PageReq};
    let found = cp
        .lineage()
        .events_for(&run_id, PageReq::unbounded())
        .await
        .unwrap();
    assert!(found.items.is_empty(), "no separate best-effort emit on the handler path");
}
```

(Imports to add to the file: `DatasetRef`, `LineageEvent`, `SnapshotId`, `Lineage`, `PageReq` from `control_plane_core`. `Mutex` is already imported; `Duration` stays. Also refresh the now-stale docs: the file's top `//!` comment and the `RecordingEngine` doc comment both say "records how many times insert_row was called" — update them to describe recording the delivered `LineageEvent`s on the atomic `write_object` seam.)

- [ ] **Step 9: Update `unsupported_action.rs`**

In `src/services/query-api/tests/unsupported_action.rs`, call `write_object` instead of `insert_row` (needs a dummy `LineageEvent`):

```rust
use control_plane_core::{DatasetRef, EventType, LineageEvent, RunId, TableRef, TypeName};
use query_api::serving::{ActionEngine, ServingError, SqlValue};
use query_api::serving_datafusion::UnsupportedActionEngine;
use uuid::Uuid;

#[tokio::test(flavor = "current_thread")]
async fn write_object_is_rejected() {
    let engine = UnsupportedActionEngine;
    let table = TableRef { schema: "sales".into(), name: "orders".into() };
    let event = LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetRef::from(&TypeName("Order".into()))],
        payload: serde_json::json!({}),
    };
    let err = engine
        .write_object(&table, &["id".to_string()], &[SqlValue::Int(1)], &["Long".to_string()], event)
        .await
        .expect_err("must reject");
    match err {
        ServingError::Engine(m) => assert!(m.contains("iceberg"), "message names the backend: {m}"),
    }
}
```

This test's BUCK target (`unsupported-action`) needs `uuid`, `serde_json`, and `time` deps added (it currently has only `:query-api`, core, tokio). Update the `unsupported-action` `rust_test` `deps` to add `"//third-party:uuid"`, `"//third-party:serde_json"`, `"//third-party:time"`.

- [ ] **Step 10: Rewrite `action_e2e.rs` to the atomic writer + lineage assertion**

Rewrite `src/services/query-api/tests/action_e2e.rs`. The key changes vs. today: the engine is `DuckLakeActionWriter` (needs an `Arc<dyn ControlPlane>` + an `Arc<dyn ObjectStore>` rooted at `data_path`); `run_action` returns `(rows, run_id)`; the headline test now asserts a Parquet file WAS written (was `== 0`) AND that lineage for `run_id` exists naming the Widget dataset.

The widget table stays pre-seeded via `DuckLakeWriter::seed` (loom's `land_ducklake` create_table is idempotent — it skips tables already live in the catalog, verified in `snapshot.rs`), so the `query_scalar("SELECT count(*) …")` assertions in `write_policy_enforces_row_filter_and_deny_column` keep working.

Replace the imports + `WidgetWriter` engine field + `setup_widget_writer` engine construction:

```rust
use std::sync::Arc;

use control_plane_core::{
    Acl, Action, ActionDef, ActionName, CompareOp, ControlPlane, DatasetRef, Effect, Lineage,
    ObjectType, PageReq, Policy, PolicyTarget, PropertyDef, RoleId, RowFilter, RunId, ScalarValue,
    SubjectId, TableRef, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use query_api::action::{ActionDeps, ActionError, run_action};
use query_api::handler::{ObjectQuery, QueryDeps, Subject, read_object};
use query_api::render::objects_to_json;
use query_api::serving::{DuckLakeActionWriter, EmbeddedDuckDb};
use serde_json::json;
```

In `WidgetWriter`, replace `engine: EmbeddedDuckDbWriter` with `engine: DuckLakeActionWriter`.

In `setup_widget_writer`, replace the engine construction (`EmbeddedDuckDbWriter::attach(...)`) with:

```rust
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(&data_path).unwrap());
    let engine = DuckLakeActionWriter::new(Arc::new(cp.clone()), store);
```

(`PgControlPlane` is `#[derive(Clone)]`, verified — `cp.clone()` wrapped in `Arc` gives the `Arc<dyn ControlPlane>` the writer needs, while the struct keeps the owned `cp` for the admin/read calls.)

Rewrite `action_inserts_a_typed_object_that_reads_back` to destructure the tuple, assert a Parquet file landed, read back, and assert lineage:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn action_inserts_a_typed_object_that_reads_back_with_atomic_lineage() {
    let fx = PgFixture::start();
    let WidgetWriter {
        cp,
        data_path,
        pg_conn,
        subj,
        engine,
        writer_fx: _writer,
        widget: _,
        role: _,
    } = setup_widget_writer(&fx).await;
    let deps = ActionDeps { cp: &cp, action_engine: &engine };
    let body = json!({ "id": "42", "name": "gadget" });
    let (created, run_id) = run_action("createWidget", body.as_object().unwrap(), &subj, &deps)
        .await
        .expect("action runs");

    // The created object is returned with typed values (Long id as string).
    let created_json = objects_to_json(&created);
    assert_eq!(
        created_json["objects"][0],
        json!({ "id": "42", "name": "gadget" }),
    );

    // The loom-owned write produced a Parquet data file (the part-1 inline property
    // is retired in favor of atomicity).
    assert!(parquet_count(&data_path) > 0, "action write produced a Parquet file");

    // It reads back through the governed read path.
    let reader = EmbeddedDuckDb::attach(&pg_conn, &data_path).await.unwrap();
    let qdeps = QueryDeps { ontology: cp.ontology(), acl: cp.acl(), serving: &reader };
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
        "round-trips"
    );

    // Lineage is now committed ATOMICALLY with the row and is findable by run_id —
    // exactly the assertion part-1 could not make (it skipped lineage as the dangling
    // slice). The event's outputs name the Widget dataset.
    let events = cp.lineage().events_for(&run_id, PageReq::unbounded()).await.unwrap();
    assert_eq!(events.items.len(), 1, "one lineage event for the action's run");
    assert_eq!(events.items[0].outputs, vec![DatasetRef::from(&TypeName("Widget".into()))]);
}
```

Update `ungranted_subject_is_forbidden` to build a `DuckLakeActionWriter` the same way (replace its `EmbeddedDuckDbWriter::attach`), and `run_action(...).await.unwrap_err()` still returns the `ActionError` (the `Result`'s `Ok` is now a tuple, but `unwrap_err` is unaffected). Its `query_scalar("… count(*) … widget")` stays "0".

`write_policy_enforces_row_filter_and_deny_column` needs only that its successful `run_action(...)` calls now return a tuple — change `.expect("…")` call sites that bind nothing (they discard the value, so they compile unchanged) and any that bind `let created = run_action(...)` to `let (_created, _run_id) = run_action(...)`. The `query_scalar` count assertions are unchanged.

Update the file's top module doc to drop the "best-effort lineage / dangling slice" framing and say the action write + lineage are atomic.

- [ ] **Step 11: Build, lint, and run the full action surface**

Run the whole sweep (the trait change is cross-cutting):

Run: `buck2 build //src/... > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|error\[|error:" /tmp/b.log | head`
Expected: build succeeds.

Run: `./tools/clippy-all.sh > /tmp/c.log 2>&1; tail -5 /tmp/c.log`
Expected: clean.

Run: `buck2 test //src/services/query-api/... //src/services/ingest/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS — in particular `action-e2e`, `action-conformance-handler`, `action-conformance-http`, `unsupported-action`, `build-object-batch`, and all e2e graph/read tests (they build `AppState` with `StubAction`). `action-engine` is gone.

- [ ] **Step 12: Commit**

```bash
git add -A src/services/query-api docs/superpowers/plans/2026-06-21-action-lineage-atomicity.md
git commit -m "feat(query-api): atomic action write + lineage via DuckLakeActionWriter

Reshape ActionEngine::insert_row -> write_object(.., event) -> SnapshotId so an
action's row and its lineage commit in one Tx (no dangling slice). run_action
mints the run_id, builds the event, and returns (ObjectRows, RunId). Remove
EmbeddedDuckDbWriter/INLINE_ROW_LIMIT; wire DuckLakeActionWriter in main."
```

---

### Task 3: Surface the `run_id` as an `X-Loom-Run-Id` response header

The created-object JSON body is unchanged (non-invasive); the action's `run_id` rides on a response header so a caller can locate its lineage via `Lineage::events_for`.

**Files:**
- Modify: `src/services/query-api/src/http.rs` — set `X-Loom-Run-Id` on the 201.
- Create: `src/services/query-api/tests/action_run_id_http.rs`
- Modify: `src/services/query-api/BUCK` — add the `action-run-id-http` `rust_test` target.

**Interfaces:**
- Consumes: `query_api::action::run_action(...) -> Result<(ObjectRows, RunId), ActionError>` (Task 2).

- [ ] **Step 1: Write the failing test**

Create `src/services/query-api/tests/action_run_id_http.rs`. It seeds a `MemoryControlPlane` (Widget + createWidget + a granted subject), drives `post_action` through the router with a `RecordingEngine` that captures the delivered event's `run_id`, and asserts the 201 carries `X-Loom-Run-Id` equal to that run_id:

```rust
//! post_action sets an X-Loom-Run-Id header on the 201 equal to the action's run_id.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use control_plane_core::{
    Acl, Action, ActionDef, ActionName, ControlPlane, Effect, LineageEvent, ObjectType, Ontology,
    ParamDef, PolicyTarget, PropertyDef, RoleId, RunId, SnapshotId, SubjectId, TableRef, TypeName,
};
use control_plane_memory::MemoryControlPlane;
use http_body_util::BodyExt;
use query_api::http::{AppState, router};
use query_api::serving::{ActionEngine, Rows, ServingEngine, ServingError, SqlValue};
use tower::ServiceExt;

/// Records the run_id of the single event it is handed.
struct CapturingEngine {
    run_id: Mutex<Option<RunId>>,
}
#[async_trait]
impl ActionEngine for CapturingEngine {
    async fn write_object(
        &self,
        _table: &TableRef,
        _columns: &[String],
        _values: &[SqlValue],
        _logical_types: &[String],
        event: LineageEvent,
    ) -> Result<SnapshotId, ServingError> {
        *self.run_id.lock().unwrap() = Some(event.run_id);
        Ok(SnapshotId(1))
    }
}

/// No-op read engine: the action path never queries it, and this is a plain
/// `rust_test` (no `DUCKDB_EXTENSION_DIR`), so we MUST NOT construct a real
/// `EmbeddedDuckDb` (its `attach` errors when that env var is unset on RE).
struct NoServing;
#[async_trait]
impl ServingEngine for NoServing {
    async fn fetch_rows(&self, _sql: &str, _params: &[SqlValue]) -> Result<Rows, ServingError> {
        Ok(Rows::default())
    }
}

async fn seeded() -> (MemoryControlPlane, SubjectId) {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    cp.define_type(ObjectType {
        name: TypeName("Widget".into()),
        properties: vec![
            PropertyDef { name: "id".into(), ty: "Long".into(), required: true },
            PropertyDef { name: "name".into(), ty: "String".into(), required: false },
        ],
        derived: vec![],
        table: TableRef { schema: "main".into(), name: "widget".into() },
        identity: Some("id".into()),
    })
    .await
    .unwrap();
    cp.define_action(ActionDef {
        name: ActionName("createWidget".into()),
        target: TypeName("Widget".into()),
        parameters: vec![
            ParamDef { name: "id".into(), ty: "Long".into(), required: true },
            ParamDef { name: "name".into(), ty: "String".into(), required: false },
        ],
    })
    .await
    .unwrap();
    let subj = SubjectId("writer".into());
    let role = RoleId("writers".into());
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    cp.grant(&role, Action::Write, PolicyTarget::Type(TypeName("Widget".into())), Effect::Allow)
        .await
        .unwrap();
    (cp, subj)
}

#[tokio::test(flavor = "multi_thread")]
async fn created_response_carries_run_id_header() {
    let (cp, subj) = seeded().await;
    let engine = Arc::new(CapturingEngine { run_id: Mutex::new(None) });
    // A serving engine is required by AppState but the action path never reads it.
    let serving: Arc<dyn ServingEngine> = Arc::new(NoServing);
    let app = router(AppState {
        cp: Arc::new(cp) as Arc<dyn ControlPlane>,
        serving,
        action_engine: engine.clone(),
    });
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/actions/createWidget")
                .header("X-Loom-Subject", subj.0.clone())
                .header("content-type", "application/json")
                .body(Body::from(r#"{"id":"42","name":"gadget"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::CREATED);
    let header = res
        .headers()
        .get("X-Loom-Run-Id")
        .expect("X-Loom-Run-Id present")
        .to_str()
        .unwrap()
        .to_string();
    let captured = engine.run_id.lock().unwrap().expect("engine saw the event");
    assert_eq!(header, captured.0.to_string(), "header equals the action's run_id");
}
```

Note: this is a plain `rust_test` (NOT `loom_fixture_test`), so `DUCKDB_EXTENSION_DIR` is not guaranteed set on remote execution. `EmbeddedDuckDb::attach` errors when that env var is unset, so the test uses the `NoServing` stub above instead of a real engine — the action path never queries the serving engine anyway. Add the `rust_test` target to `BUCK`:

```python
rust_test(
    name = "action-run-id-http",
    crate = "action_run_id_http",
    srcs = ["tests/action_run_id_http.rs"],
    crate_root = "tests/action_run_id_http.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
        "//src/control-plane/memory:memory",
        "//third-party:async-trait",
        "//third-party:axum",
        "//third-party:http-body-util",
        "//third-party:tokio",
        "//third-party:tower",
    ],
)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test //src/services/query-api:action-run-id-http > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|panicked|X-Loom-Run-Id" /tmp/t.log`
Expected: FAIL — the header is absent (`X-Loom-Run-Id present` panics).

- [ ] **Step 3: Set the header in `post_action` (http.rs)**

In `src/services/query-api/src/http.rs`, change the `Ok(...)` arm of `post_action` to use the `run_id` and attach the header. Add `use axum::http::header::HeaderValue;` (or build the header inline). The arm becomes:

```rust
        Ok((rows, run_id)) => {
            let body = crate::render::objects_to_json(&rows);
            let one = body
                .get("objects")
                .and_then(|a| a.as_array())
                .and_then(|a| a.first())
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            // Surface the action's run_id so a caller can locate its lineage via
            // Lineage::events_for. The body is unchanged (non-invasive).
            let mut resp = (StatusCode::CREATED, Json(one)).into_response();
            if let Ok(v) = axum::http::HeaderValue::from_str(&run_id.0.to_string()) {
                resp.headers_mut().insert("X-Loom-Run-Id", v);
            }
            resp
        }
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `buck2 test //src/services/query-api:action-run-id-http > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS.

Also re-run the conformance http test (unchanged behavior, but it shares `post_action`):
Run: `buck2 test //src/services/query-api:action-conformance-http > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/services/query-api/src/http.rs src/services/query-api/tests/action_run_id_http.rs src/services/query-api/BUCK
git commit -m "feat(query-api): X-Loom-Run-Id header surfaces an action's run_id"
```

---

### Task 4: Close the register item

**Files:**
- Modify: `docs/ISSUES.md` — close `iss-action-lineage-atomicity`.

- [ ] **Step 1: Flip the item to fixed**

Invoke the `loom-docs-update` skill (it knows the register grammar). It should edit the `iss-action-lineage-atomicity` entry in `docs/ISSUES.md`: change `- [ ]` → `- [x]`, set `status:open` → `status:fixed`, and set `pr:-` → the PR number once known (leave `pr:-` until the PR exists; the PR description will carry the closure). Validate:

Run: `bash tools/docs.sh validate docs/ISSUES.md > /tmp/d.log 2>&1; cat /tmp/d.log`
Expected: validation passes (grammar/ids/vocab/links OK).

- [ ] **Step 2: Markdown lint + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p.log 2>&1; tail -5 /tmp/p.log`
Expected: hooks pass (and auto-fix any EOF/whitespace; re-add if changed).

```bash
git add docs/ISSUES.md
git commit -m "docs(issues): close iss-action-lineage-atomicity (atomic action write + lineage)"
```

---

## Final verification (before finishing the branch)

Run the FULL sweep — the reshaped trait touches multiple crates, so per-crate green is insufficient:

Run: `buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: all pass.

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p.log 2>&1; tail -5 /tmp/p.log`
Expected: clean (rustfmt, clippy, file checks, docs-validate).

## Self-Review

**1. Spec coverage:**
- "Reshape `ActionEngine` to atomic `write_object(.., event) -> SnapshotId`" → Task 2 Step 1.
- "`DuckLakeActionWriter` via `land_ducklake`, file_prefix `action-<run_id>`, IngestError→ServingError" → Task 2 Step 1.
- "Build one-row Arrow batch + Schema + ColumnSpec from `(columns, values, logical_types)` via `resolve_logical`" → Task 1.
- "Full-property-set expansion (unset → NULL)" → Task 2 Step 3.
- "`run_action` mints run_id, builds event (inputs=[], outputs=[target dataset], payload action name, drop snapshot_id), deletes snapshot lookup + best-effort emit, returns (ObjectRows, RunId)" → Task 2 Step 3.
- "Remove `EmbeddedDuckDbWriter` + `DATA_INLINING` writer variant + `INLINE_ROW_LIMIT`; reader unchanged" → Task 2 Step 1 (reader `EmbeddedDuckDb` kept).
- "`query-api` gains `ingest` dep (acyclic)" → Task 2 Step 6.
- "`UnsupportedActionEngine` updated, still errors" → Task 2 Step 2.
- "`X-Loom-Run-Id` header on 201; body unchanged" → Task 3.
- "Binary wiring: `DuckLakeActionWriter::new(cp.clone(), store)`" → Task 2 Step 5.
- Testing: e2e asserts read-back AND lineage-for-run_id naming the dataset (Task 2 Step 10); handler asserts returned run_id == event handed to engine + lineage via engine not separate emit (Task 2 Step 8); http asserts X-Loom-Run-Id == run_id (Task 3); seam update ports part-1 tests to `write_object` and retires the `EmbeddedDuckDbWriter` test (Task 2 Steps 7–10, delete `action_engine.rs`).
- "Close `iss-action-lineage-atomicity` in ISSUES.md" → Task 4.
- Out of scope (Iceberg ActionEngine, update/delete actions, lightweight single-row writer) — none attempted. ✓

**2. Placeholder scan:** No TBD/TODO/"handle edge cases"; every code step shows real code. ✓

**3. Type consistency:** `write_object(table, columns, values, logical_types, event) -> Result<SnapshotId, ServingError>` is identical across the trait, `DuckLakeActionWriter`, `UnsupportedActionEngine`, and every test stub. `run_action -> Result<(ObjectRows, RunId), ActionError>` matches its `post_action` and test call sites. `build_object_batch` signature matches its caller and test. `land_ducklake` arg order matches the verified source signature. `RunId` is `Copy` (so `run_id` is usable after moving into `event`). `SnapshotId(pub i64)`, `RunId(pub Uuid)` field accesses (`.0`) are correct. ✓
