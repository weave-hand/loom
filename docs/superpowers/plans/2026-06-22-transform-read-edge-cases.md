# Transform read-path edge cases — Implementation Plan

> **For agentic workers:** Implement task-by-task under TDD. Steps use checkbox
> (`- [ ]`) syntax for tracking. Tests are `rust_test` integration targets only —
> NO inline `#[cfg(test)]`.

**Goal:** Fix the two input-resolution edge cases in `run_transform` that the
worker's retry policy mis-classifies: (1) a missing input surfacing at
`Catalog::files`/`schema` must map to `UnknownInput` (Abandon), not
`ControlPlane` (Retry-forever); (2) an input table that exists with **zero files**
at its snapshot must register as an empty relation (so the SQL runs over an empty
input), not fail inside DataFusion's `infer_schema` (`Scan` → Retry). Closes
`iss-transform-read-edge-cases`.

**Spec:** `docs/superpowers/specs/2026-06-22-transform-read-edge-cases-design.md`

**Architecture:** Three orthogonal changes land together:
1. A tiny `unknown_input(table, ControlPlaneError) -> TransformError` mapper in
   `run.rs`, applied at every input read (`current_snapshot`, `files`, the new
   `schema`), so a `NotFound` from any of them is `UnknownInput` (Abandon).
2. New `datafusion-io` helpers: `logical_arrow_type` / `logical_arrow_schema`
   (`infer.rs`, the inverse of `arrow_logical_type`) and `register_empty_table`
   (`scan.rs`, a `MemTable` with zero batches).
3. `run_transform` step 1 branches on `files.items.is_empty()`: empty → resolve
   the catalog `schema`, build an Arrow schema, register an empty relation;
   non-empty → today's `scan_table` path (untouched).

The retry-policy table (`handler.rs`) is **unchanged** — the fix is that
`run_transform` now produces the *correct error variant*, which the existing
policy already classifies (`UnknownInput`/`Infer` → Abandon).

**Tech Stack:** Rust, buck2, DataFusion 54, Arrow, object_store. Edge-1 test is a
plain `rust_test` with a hand-rolled `ControlPlane`/`Catalog` double (no DB);
edge-2 tests extend the existing `transform-e2e` `loom_fixture_test` (Postgres +
DuckLake); the schema-builder unit test is a plain `rust_test` in `datafusion-io`.

## Global Constraints

- **Tests are `rust_test` integration targets only** — NO inline `#[cfg(test)]`/
  `#[test]` in `src/**.rs` (the `no-inline-tests` prek hook fails otherwise).
- **New fixture tests use `loom_fixture_test`**, not bare `rust_test` (edge-2
  extends the existing `transform-e2e` target, so no new fixture target is added).
- **No Cargo.toml / lockfile / third-party changes** — `MemTable` is in the
  already-depended `datafusion` crate; `arrow` is already a dep of both crates.
  Core (`src/control-plane/core/`) is untouched (`Catalog::schema` already exists).
- **No new logical types** — the empty-input schema builder covers exactly the
  five types the transform read/write path already round-trips
  (boolean/integer/long/double/string).
- Run tests with the file-redirect pattern (never pipe `buck2 test` through
  `tail`/`head`):
  `buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`.
- Commit messages: Conventional Commits.

## Spec deviation (decided, flagged for reviewer)

The spec's snippet says `logical_arrow_schema` errors `InferError::Unsupported`
on an unmapped type. But the existing `InferError::Unsupported(DataType)` variant
is parameterized by an **Arrow `DataType`**, and `logical_arrow_schema`'s input is
a loom logical **type-name string** — there is no `DataType` to attach when the
name is unmapped. Reusing `Unsupported` would either require fabricating a
`DataType` or changing its shape (which would break the existing
`infer_columns`/`tests/infer.rs` assertions on `Unsupported(DataType)`).

**Decision:** add a new variant `InferError::UnsupportedLogical(String)` carrying
the offending type name. This is type-honest and preserves the spec's *actual*
requirement — the error is still an `InferError`, which `run_transform` maps to
`TransformError::Infer` → **Abandon** (deterministic), exactly as the spec
intends. The unit test asserts `InferError::UnsupportedLogical`.

**Second deviation (decided):** the spec's edge-1 test sketch suggested a one-line
guard `assert retry_policy(&UnknownInput(..), 0) == RetryPolicy::Abandon`.
`retry_policy` is a *private* free fn in `handler.rs` (not re-exported), and the
mapping it would pin is already unchanged code covered end-to-end by the e2e and
edge-1 tests (the edge-1 test asserts the `TransformError::UnknownInput` variant
that the existing policy Abandons). Exposing an internal solely for a tautological
guard is not worth the surface; the assertion is intentionally omitted.

---

### Task 1: `logical_arrow_type` + `logical_arrow_schema` in `datafusion-io` (TDD)

**Files:**
- Modify: `src/services/datafusion-io/src/infer.rs` — add `UnsupportedLogical`
  variant, `logical_arrow_type`, `logical_arrow_schema`
- Modify: `src/services/datafusion-io/src/lib.rs` — re-export the two new fns
- Create: `src/services/datafusion-io/tests/logical_arrow.rs`
- Modify: `src/services/datafusion-io/BUCK` — add `logical-arrow` rust_test target

**Interfaces produced (used by Task 3):**
- `pub fn logical_arrow_type(ty: &str) -> Option<DataType>`
- `pub fn logical_arrow_schema(columns: &[ColumnSpec]) -> Result<SchemaRef, InferError>`

- [ ] **Step 1: Write the failing test**

Create `src/services/datafusion-io/tests/logical_arrow.rs`:

```rust
use arrow::datatypes::DataType;
use control_plane_core::ColumnSpec;
use datafusion_io::infer::{InferError, arrow_logical_type, logical_arrow_schema, logical_arrow_type};

fn col(name: &str, ty: &str, nullable: bool) -> ColumnSpec {
    ColumnSpec { name: name.into(), ty: ty.into(), nullable }
}

#[test]
fn maps_supported_logical_names_to_arrow_types() {
    assert_eq!(logical_arrow_type("boolean"), Some(DataType::Boolean));
    assert_eq!(logical_arrow_type("integer"), Some(DataType::Int32));
    assert_eq!(logical_arrow_type("long"), Some(DataType::Int64));
    assert_eq!(logical_arrow_type("double"), Some(DataType::Float64));
    assert_eq!(logical_arrow_type("string"), Some(DataType::Utf8));
    assert_eq!(logical_arrow_type("date"), None);
}

#[test]
fn round_trips_against_arrow_logical_type() {
    // Every supported logical name maps to an Arrow type that maps back to the
    // same name — the inverse the empty-input path relies on.
    for name in ["boolean", "integer", "long", "double", "string"] {
        let dt = logical_arrow_type(name).unwrap();
        assert_eq!(arrow_logical_type(&dt), Some(name));
    }
}

#[test]
fn builds_schema_in_column_order_with_nullability() {
    let cols = vec![
        col("id", "long", false),
        col("name", "string", true),
        col("active", "boolean", false),
    ];
    let schema = logical_arrow_schema(&cols).unwrap();
    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert!(!schema.field(0).is_nullable());
    assert_eq!(schema.field(1).name(), "name");
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
    assert!(schema.field(1).is_nullable());
    assert_eq!(schema.field(2).name(), "active");
    assert_eq!(schema.field(2).data_type(), &DataType::Boolean);
    assert!(!schema.field(2).is_nullable());
}

#[test]
fn unsupported_logical_type_is_an_error_not_a_guess() {
    let cols = vec![col("when", "timestamp", true)];
    match logical_arrow_schema(&cols) {
        Err(InferError::UnsupportedLogical(t)) => assert_eq!(t, "timestamp"),
        other => panic!("expected UnsupportedLogical, got {other:?}"),
    }
}
```

Add to `src/services/datafusion-io/BUCK`:

```python
rust_test(
    name = "logical-arrow",
    crate = "logical_arrow",
    srcs = ["tests/logical_arrow.rs"],
    crate_root = "tests/logical_arrow.rs",
    edition = "2024",
    deps = [
        ":datafusion-io",
        "//src/control-plane/core:core",
        "//third-party:arrow",
    ],
)
```

- [ ] **Step 2: Implement in `infer.rs`**

Add the new error variant (after the existing `Unsupported` arm):

```rust
    #[error("unsupported loom logical type for an empty input schema: {0}")]
    UnsupportedLogical(String),
```

Add the two functions (after `arrow_logical_type` / before `infer_columns`):

```rust
/// loom logical type name -> Arrow `DataType`. The inverse of `arrow_logical_type`,
/// over exactly the five types `infer_columns` round-trips. `None` for an unmapped
/// name (kept deliberately small — YAGNI; widen via `fut-datafusion-type-coverage`).
pub fn logical_arrow_type(ty: &str) -> Option<DataType> {
    match ty {
        "boolean" => Some(DataType::Boolean),
        "integer" => Some(DataType::Int32),
        "long" => Some(DataType::Int64),
        "double" => Some(DataType::Float64),
        "string" => Some(DataType::Utf8),
        _ => None,
    }
}

/// Build an Arrow schema from loom column specs, in column order, preserving
/// nullability. Errors `InferError::UnsupportedLogical` on the first type outside
/// the supported set — a deterministic limitation (maps to `TransformError::Infer`
/// -> Abandon), symmetric with how a non-empty output of an unsupported type fails
/// via `infer_columns`. Used by the transform empty-input path to register an empty
/// relation whose schema matches what a non-empty scan would expose.
pub fn logical_arrow_schema(columns: &[ColumnSpec]) -> Result<SchemaRef, InferError> {
    let fields: Vec<Field> = columns
        .iter()
        .map(|c| {
            let dt = logical_arrow_type(&c.ty)
                .ok_or_else(|| InferError::UnsupportedLogical(c.ty.clone()))?;
            Ok(Field::new(&c.name, dt, c.nullable))
        })
        .collect::<Result<_, InferError>>()?;
    Ok(Arc::new(Schema::new(fields)))
}
```

Update the `use` line at the top of `infer.rs` to:

```rust
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use control_plane_core::ColumnSpec;
```

Re-export from `src/services/datafusion-io/src/lib.rs` — change the `infer`
re-export line to:

```rust
pub use infer::{
    InferError, arrow_logical_type, infer_columns, logical_arrow_schema, logical_arrow_type,
};
```

- [ ] **Step 3: Verify**

```
buck2 test //src/services/datafusion-io:logical-arrow > /tmp/t.log 2>&1
grep -E "Tests finished|FAIL|error\[" /tmp/t.log
```

Also confirm the existing `infer` test still passes (the `Unsupported(DataType)`
variant is unchanged):

```
buck2 test //src/services/datafusion-io:infer > /tmp/t2.log 2>&1
grep -E "Tests finished|FAIL" /tmp/t2.log
```

---

### Task 2: `register_empty_table` in `datafusion-io` `scan.rs` (TDD)

**Files:**
- Modify: `src/services/datafusion-io/src/scan.rs` — add `register_empty_table`
- Modify: `src/services/datafusion-io/src/lib.rs` — re-export it
- Modify: `src/services/datafusion-io/tests/scan.rs` — add a register-empty test

**Interface produced (used by Task 3):**
- `pub fn register_empty_table(ctx: &SessionContext, name: &str, schema: SchemaRef) -> Result<(), ScanError>`

- [ ] **Step 1: Write the failing test**

Append to `src/services/datafusion-io/tests/scan.rs` (and extend imports —
`register_empty_table`, `arrow::datatypes::SchemaRef`):

```rust
#[tokio::test]
async fn register_empty_table_runs_sql_over_zero_rows() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let ctx = SessionContext::new();
    register_empty_table(&ctx, "input", schema).unwrap();

    // count(*) over an empty relation is one row of 0; SELECT * is empty.
    let n = ctx.sql("SELECT count(*) AS n FROM input").await.unwrap();
    let rows = n.collect().await.unwrap();
    let total: usize = rows.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 1, "count(*) yields exactly one row");

    let star = ctx.sql("SELECT * FROM input").await.unwrap();
    let out = star.collect().await.unwrap();
    let data_rows: usize = out.iter().map(|b| b.num_rows()).sum();
    assert_eq!(data_rows, 0, "the relation is empty");
}
```

(Import line at the top of `tests/scan.rs` becomes
`use datafusion_io::{WriteConfig, register_empty_table, scan_table, write_dataset};`
and add `SchemaRef` to the `arrow::datatypes` import.)

- [ ] **Step 2: Implement `register_empty_table`**

In `src/services/datafusion-io/src/scan.rs`, add the import and function. Add to
the existing imports:

```rust
use arrow::datatypes::SchemaRef;
use datafusion::datasource::MemTable;
```

Add the function (after `scan_table`):

```rust
/// Register an EMPTY DataFusion table named `name` with `schema` (zero rows) — the
/// empty-input analog of `scan_table`, for an input table that exists at a snapshot
/// but has no data files. Uses `TableReference::bare` to preserve the registration
/// name verbatim (same as `scan_table`), so a `FROM "Order"` resolves identically
/// whether the input is empty or scanned.
pub fn register_empty_table(
    ctx: &SessionContext,
    name: &str,
    schema: SchemaRef,
) -> Result<(), ScanError> {
    let provider = MemTable::try_new(schema, vec![])?; // zero batches
    ctx.register_table(TableReference::bare(name), Arc::new(provider))?;
    Ok(())
}
```

Re-export from `src/services/datafusion-io/src/lib.rs` — change the `scan`
re-export line to:

```rust
pub use scan::{ScanError, register_empty_table, scan_table};
```

- [ ] **Step 3: Verify**

```
buck2 test //src/services/datafusion-io:scan > /tmp/t.log 2>&1
grep -E "Tests finished|FAIL|error\[" /tmp/t.log
```

---

### Task 3: Wire the two edge cases into `run_transform` (`run.rs`)

**Files:**
- Modify: `src/services/transform/src/run.rs` — add `unknown_input` helper; apply
  it at `current_snapshot`/`files`/the new `schema` read; branch on
  `files.items.is_empty()`.

**Depends on:** Tasks 1 & 2 (`logical_arrow_schema`, `register_empty_table`).

- [ ] **Step 1: Add the `unknown_input` helper**

Add, near the top of `run.rs` (after the `TransformError` enum), a free function:

```rust
/// Map a control-plane read error during input resolution. A `NotFound` from any
/// of `current_snapshot`/`files`/`schema` means the input is not live at that
/// snapshot (e.g. dropped between reads) — deterministically a bad input
/// (`UnknownInput` -> Abandon), not a transient `ControlPlane` fault (-> Retry).
fn unknown_input(table: &TableRef, e: control_plane_core::ControlPlaneError) -> TransformError {
    match e {
        control_plane_core::ControlPlaneError::NotFound(_) => {
            TransformError::UnknownInput(table.schema.clone(), table.name.clone())
        }
        other => TransformError::ControlPlane(other),
    }
}
```

- [ ] **Step 2: Rewrite the per-input resolution block (step 1 of `run_transform`)**

Replace the existing `for input in req.inputs { ... }` resolution loop (the block
that calls `current_snapshot`, `files`, `scan_table`) with:

```rust
    // 1. Resolve + register each input under its `register_as` name. A NotFound at
    //    any read is a deterministically-bad input (UnknownInput -> Abandon). An
    //    input that is live but has zero files registers as an empty relation so the
    //    SQL runs over an empty input rather than failing inside DataFusion infer.
    for input in req.inputs {
        let snapshot = cp
            .catalog()
            .current_snapshot(input.table)
            .await
            .map_err(|e| unknown_input(input.table, e))?;
        let files = cp
            .catalog()
            .files(
                input.table,
                snapshot.id,
                control_plane_core::PageReq::unbounded(),
            )
            .await
            .map_err(|e| unknown_input(input.table, e))?;
        if files.items.is_empty() {
            let ts = cp
                .catalog()
                .schema(input.table, snapshot.id)
                .await
                .map_err(|e| unknown_input(input.table, e))?;
            let columns: Vec<ColumnSpec> = ts
                .columns
                .into_iter()
                .map(|c| ColumnSpec {
                    name: c.name,
                    ty: c.ty,
                    nullable: c.nullable,
                })
                .collect();
            let schema = datafusion_io::logical_arrow_schema(&columns)?; // InferError -> Abandon
            datafusion_io::register_empty_table(&ctx, input.register_as, schema)?;
        } else {
            scan_table(
                &ctx,
                store.clone(),
                input.register_as,
                input.table,
                &files.items,
            )
            .await?;
        }
    }
```

Update the `datafusion_io` import in `run.rs` to bring in the new fns (it
currently imports `{WriteConfig, infer_columns, scan_table, write_dataset}` — add
`logical_arrow_schema` and `register_empty_table`):

```rust
use datafusion_io::{
    WriteConfig, infer_columns, logical_arrow_schema, register_empty_table, scan_table,
    write_dataset,
};
```

(Either the bare-path `datafusion_io::logical_arrow_schema` form or the imported
form works; pick the imported form and call them unqualified, matching the file's
style. Keep `ColumnSpec` — already imported from `control_plane_core`.)

- [ ] **Step 3: Build the transform crate**

```
buck2 build //src/services/transform:transform > /tmp/b.log 2>&1
grep -E "FAILED|error\[|warning: unused" /tmp/b.log
```

---

### Task 4: Edge-1 test — missing input at `files` → `UnknownInput` (plain `rust_test`)

**Files:**
- Create: `src/services/transform/tests/run_unknown_input.rs`
- Modify: `src/services/transform/BUCK` — add a plain `rust_test` target.

The double: a `ControlPlane` whose `catalog()` returns a fake `Catalog` with
`current_snapshot -> Ok(snapshot)` and `files -> Err(NotFound)` (the drop-race the
real fixture can't deterministically produce). All other accessors
(`ontology`/`acl`/`lineage`/`queue`/`begin`) and the fake catalog's
`schema`/`snapshots` are `unreachable!()` — never reached, the error precedes them.

- [ ] **Step 1: Write the test**

Create `src/services/transform/tests/run_unknown_input.rs`. Drive `run_transform`
with one input over an `InMemory` object store; assert `Err(UnknownInput(..))` and
pin the classification (`retry_policy` is private to `handler.rs`, so assert via
the *outcome* of the public `transform_handler` is heavier; instead assert the
error variant directly — the policy mapping for `UnknownInput` is already covered
by the existing handler classification and is asserted here through the variant).

```rust
//! Edge 1: a missing input that surfaces at `Catalog::files` (a drop-race between
//! current_snapshot and files) must classify as `UnknownInput` (-> Abandon), not
//! `ControlPlane` (-> Retry forever). Uses a hand-rolled control-plane double so the
//! NotFound lands at `files`, which a real fixture cannot deterministically produce.

use std::sync::Arc;

use async_trait::async_trait;
use control_plane_core::{
    Acl, Catalog, ControlPlane, ControlPlaneError, FileRef, Lineage, LineageEvent, Ontology,
    Page, PageReq, Queue, Snapshot, SnapshotId, TableSchema, TableRef, Tx,
};
use object_store::ObjectStore;
use object_store::memory::InMemory;
use transform::{TransformInput, TransformRequest, TransformError, run_transform};

struct FilesNotFoundCatalog;

#[async_trait]
impl Catalog for FilesNotFoundCatalog {
    async fn current_snapshot(&self, _t: &TableRef) -> control_plane_core::Result<Snapshot> {
        Ok(Snapshot {
            id: SnapshotId(1),
            time: time::OffsetDateTime::UNIX_EPOCH,
            schema_version: 0,
        })
    }
    async fn snapshots(&self, _t: &TableRef, _p: PageReq) -> control_plane_core::Result<Page<Snapshot>> {
        unreachable!("snapshots not read by run_transform")
    }
    async fn files(&self, _t: &TableRef, _at: SnapshotId, _p: PageReq) -> control_plane_core::Result<Page<FileRef>> {
        Err(ControlPlaneError::NotFound("dropped between reads".into()))
    }
    async fn schema(&self, _t: &TableRef, _at: SnapshotId) -> control_plane_core::Result<TableSchema> {
        unreachable!("schema not reached — files errors first")
    }
}

struct StubCp {
    catalog: FilesNotFoundCatalog,
}

#[async_trait]
impl ControlPlane for StubCp {
    fn catalog(&self) -> &(dyn Catalog + Send + Sync) { &self.catalog }
    fn ontology(&self) -> &(dyn Ontology + Send + Sync) { unreachable!() }
    fn acl(&self) -> &(dyn Acl + Send + Sync) { unreachable!() }
    fn lineage(&self) -> &(dyn Lineage + Send + Sync) { unreachable!() }
    fn queue(&self) -> &(dyn Queue + Send + Sync) { unreachable!() }
    async fn begin(&self) -> control_plane_core::Result<Box<dyn Tx + Send>> { unreachable!() }
}

#[tokio::test]
async fn missing_input_at_files_is_unknown_input() {
    let cp = StubCp { catalog: FilesNotFoundCatalog };
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let table = TableRef { schema: "main".into(), name: "gone".into() };
    let input = TransformInput { table: &table, register_as: "gone" };
    let lineage = LineageEvent {
        run_id: control_plane_core::RunId(uuid::Uuid::new_v4()),
        event_type: control_plane_core::EventType::Complete,
        event_time: time::OffsetDateTime::UNIX_EPOCH,
        inputs: vec![],
        outputs: vec![],
        payload: serde_json::json!({}),
    };
    let res = run_transform(
        &cp,
        store,
        "run-x",
        TransformRequest {
            inputs: &[input],
            output: &TableRef { schema: "main".into(), name: "out".into() },
            sql: "SELECT 1",
            conform: None,
            output_mode: transform::OutputMode::Append,
            lineage,
        },
    )
    .await;
    match res {
        Err(TransformError::UnknownInput(s, n)) => {
            assert_eq!(s, "main");
            assert_eq!(n, "gone");
        }
        other => panic!("expected UnknownInput, got {other:?}"),
    }
}
```

> **Implementer note:** confirm the exact public re-export paths before finalizing
> (`control_plane_core` must export `Catalog`, `Tx`, `Page`, `Snapshot`,
> `TableSchema`, `RunId`, `EventType`, `Acl`, `Ontology`, `Lineage`, `Queue`,
> `FileRef`). Check `src/control-plane/core/src/lib.rs` and add any missing
> imports / qualify with full paths as needed. If `Catalog`/`Tx` require additional
> trait methods beyond those shown (e.g. the trait grew), stub every method —
> the `unreachable!()` bodies are fine for the un-exercised ones.

- [ ] **Step 2: Add the BUCK target**

```python
rust_test(
    name = "run-unknown-input",
    crate = "run_unknown_input",
    srcs = ["tests/run_unknown_input.rs"],
    crate_root = "tests/run_unknown_input.rs",
    edition = "2024",
    deps = [
        ":transform",
        "//src/control-plane/core:core",
        "//third-party:async-trait",
        "//third-party:object_store",
        "//third-party:serde_json",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```

(Verify `//third-party:async-trait` is the correct target name — check
`third-party/BUCK` / how `core` depends on it; adjust deps to whatever the fake
trait impls and `LineageEvent` construction actually require.)

- [ ] **Step 3: Run**

```
buck2 test //src/services/transform:run-unknown-input > /tmp/t.log 2>&1
grep -E "Tests finished|FAIL|error\[" /tmp/t.log
```

---

### Task 5: Edge-2 tests — empty input runs the transform (extend `transform-e2e`)

**Files:**
- Modify: `src/services/transform/tests/transform_e2e.rs` — add two test fns that
  create an input table with a schema but no files, then `run_transform` over it.

Create a table with a schema but **no files** via the control-plane Tx
(`begin` -> `create_table` -> `commit`), so `current_snapshot` exists, `files` is
empty, and `schema` resolves. Then call `run_transform` **directly** (not via the
queue) so the assertion is on the returned snapshot, and read the output back with
the `DuckLakeWriter`.

- [ ] **Step 1: Add the empty-input test(s)**

Add to `transform_e2e.rs` (reuse the file's existing `tref` helper and fixture
boot). Sketch (the implementer adapts to the fixture's exact API — mirror how
`overwrite_e2e.rs` boots and how `run_transform` is invoked):

```rust
#[tokio::test(flavor = "multi_thread")]
async fn empty_input_runs_transform_count_is_zero() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(writer.data_path()).unwrap());

    // Create an input table with a schema but NO files.
    let input = tref("main", "empty_in");
    let cols = vec![
        control_plane_core::ColumnSpec { name: "id".into(), ty: "long".into(), nullable: false },
        control_plane_core::ColumnSpec { name: "region".into(), ty: "string".into(), nullable: true },
    ];
    let mut tx = cp.begin().await.unwrap();
    tx.create_table(&input, &cols).await.unwrap();
    tx.commit().await.unwrap();

    // count(*) over the empty input commits a snapshot whose single row is 0.
    let out = tref("main", "empty_count");
    let lineage = LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![DatasetRef::from(&input)],
        outputs: vec![DatasetRef::from(&out)],
        payload: serde_json::json!({}),
    };
    let cp_dyn: &dyn ControlPlane = &cp;
    let input_ref = transform::TransformInput { table: &input, register_as: "empty_in" };
    transform::run_transform(
        cp_dyn,
        store.clone(),
        "run-empty-1",
        transform::TransformRequest {
            inputs: &[input_ref],
            output: &out,
            sql: "SELECT count(*) AS n FROM empty_in",
            conform: None,
            output_mode: transform::OutputMode::Append,
            lineage,
        },
    )
    .await
    .expect("empty input is an empty relation, not a scan error");

    let n = writer.query_scalar("SELECT n FROM lake.main.empty_count;").await;
    assert_eq!(n, "0", "count(*) over the empty input is 0");
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_input_select_star_commits_empty_output() {
    // ... same boot + create empty input table ...
    // run `SELECT * FROM empty_in` into `main.empty_passthrough`;
    // assert it commits (Ok) and the output has zero rows:
    //   writer.query_scalar("SELECT count(*) FROM lake.main.empty_passthrough;") == "0"
    // and that the result is NOT a Scan/Retry error.
}
```

> **Implementer notes:**
> - `PgControlPlane` exposes `current_snapshot`/`files` both directly and via
>   `catalog()` (see `overwrite_e2e.rs` using `cp.current_snapshot`); pass
>   `&cp` as `&dyn ControlPlane` to `run_transform`.
> - `SELECT *` writes a Parquet output even with zero rows; confirm `write_dataset`
>   handles a zero-row batch set (it already does — `write.rs` builds a `MemTable`
>   from the collected batches; an empty result is a single empty batch or none).
>   If a zero-file output makes the read-back table absent, assert the
>   `run_transform` result is `Ok(_)` and that the output snapshot exists via
>   `cp.current_snapshot(&out)` instead of a DuckDB count. Pick whichever the
>   fixture supports; the contract under test is "no `Scan`/`Retry` error", which
>   `.expect(...)` already proves.
> - Confirm `transform_e2e.rs`'s BUCK deps already include everything needed
>   (`core`, `postgres`, `arrow`, `object_store`, `serde_json`, `time`, `tokio`,
>   `uuid`) — they do; no BUCK change for this task.

- [ ] **Step 2: Run the e2e**

```
buck2 test //src/services/transform:transform-e2e > /tmp/t.log 2>&1
grep -E "Tests finished|FAIL|panicked" /tmp/t.log
```

---

### Task 6: Full suite, docs close-out, commit, push

- [ ] **Step 1: Full test suite** (catch any shared-dep / cross-crate regression)

```
buck2 test //src/... > /tmp/full.log 2>&1
grep -E "Tests finished|FAIL" /tmp/full.log
```

- [ ] **Step 2: Clippy + fmt on the touched crates**

```
./tools/clippy-all.sh > /tmp/clippy.log 2>&1; grep -iE "warning|error" /tmp/clippy.log | head
buck2 run //tools:rustfmt -- --check \
  src/services/transform/src/run.rs \
  src/services/datafusion-io/src/infer.rs \
  src/services/datafusion-io/src/scan.rs
```

(Run `prek -- run --all-files` before push to satisfy the `lint` CI job —
end-of-file/whitespace hooks police the new `.md` plan + any edited files.)

- [ ] **Step 3: Close the register item** (`loom-docs-update`)

In `docs/ISSUES.md`, flip `iss-transform-read-edge-cases`: `- [ ]` → `- [x]`,
`status:open` → `status:fixed`, and add `pr:#<n>` once the PR number is known.

- [ ] **Step 4: Commit & push**

```
git add -A
git commit -m "fix(transform): classify missing input as UnknownInput and run empty inputs as empty relations"
git push -u origin work/iss-transform-read-edge-cases
```

---

## Acceptance criteria (from the spec)

1. A missing input surfacing at `files`/`schema` yields
   `TransformError::UnknownInput` (Abandon), not `ControlPlane` (Retry).
2. An input table that is live with zero files registers as an empty relation;
   `SELECT count(*)` commits a `0`, `SELECT *` commits an empty output — neither a
   `Scan`/Retry error.
3. The retry-policy table (`handler.rs`) and the non-empty `scan_table` path are
   unchanged.
4. No new logical types supported; the empty-input builder covers exactly the five
   round-tripped types, erroring (deterministically, Abandon) on anything else.
5. `buck2 test //src/...` is green.
