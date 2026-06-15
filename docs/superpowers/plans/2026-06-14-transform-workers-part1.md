# Transform Workers (part 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A queue-driven `transform` worker that reads DuckLake table(s) with DataFusion, runs a SQL computation, and commits the result as a new snapshot + lineage — built on a shared `datafusion-io` crate extracted from `ingest`.

**Architecture:** Extract the DuckLake↔DataFusion IO (`write_dataset` + a new `scan_table`) into a shared `datafusion-io` library; `ingest` and a new `transform` service both depend on it. `transform` holds `run_transform` (resolve inputs → `ctx.sql` → infer → write → atomic `Tx` commit+lineage), a worker handler, and a binary.

**Tech Stack:** Rust 2024, DataFusion 54, arrow/parquet 58, object_store 0.13, buck2, the `control-plane-worker` loop, `service_runtime`, the `PgFixture`/`DuckLakeWriter` test harness.

**Spec:** `docs/superpowers/specs/2026-06-14-transform-workers-part1-design.md`

---

## Before you start

- **Activate the dev shell:** `eval "$(./tools/env.sh)"`. Everything runs locally (no `--prefer-remote`); fixture tests + the full suite run here.
- **Never pipe `buck2 test` through `tail`** — redirect and grep: `buck2 test //… > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`.
- **First-party crates are buck-only** — do NOT create `Cargo.toml` files or touch the workspace lock/reindeer. The new crates reference existing `//third-party:*` targets (all already vendored via ingest).
- **Tests are integration `rust_test`/`loom_fixture_test` targets only** (no inline `#[cfg(test)]`).
- Work on branch `feat/transform-workers` (already created; the spec commit is there). Git identity: `git config user.email jackomayo@gmail.com; git config user.name rsJames-ttrpg`.

## File structure

- **Create `src/services/datafusion-io/`** — `BUCK`, `src/lib.rs`, `src/write.rs` (moved from ingest), `src/infer.rs` (moved), `src/scan.rs` (new read path), `tests/write.rs` + `tests/infer.rs` (moved), `tests/scan.rs` (new).
- **Modify `src/services/ingest/`** — delete `src/write.rs` + `src/infer.rs`; redirect `src/materialize.rs` + `src/lib.rs`; drop the `write`/`infer` test targets; add the `datafusion-io` dep in `BUCK`.
- **Create `src/services/transform/`** — `BUCK`, `src/lib.rs`, `src/run.rs`, `src/handler.rs`, `src/main.rs`, `tests/transform_e2e.rs`.
- **Modify** `docs/superpowers/specs/2026-06-06-loom-roadmap.md`.

---

## Task 1: Extract `datafusion-io` (move `write_dataset` + `infer_columns`)

Create the new crate by relocating the two files from `ingest` and renaming the one ingest-specific type. `ingest` keeps its own copies for now (both crates compile independently); Task 2 deletes the ingest copies.

**Files:**
- Create: `src/services/datafusion-io/{BUCK, src/lib.rs, src/write.rs, src/infer.rs, tests/write.rs, tests/infer.rs}`

- [ ] **Step 1: Copy the two source files verbatim**

Copy `src/services/ingest/src/write.rs` → `src/services/datafusion-io/src/write.rs` and `src/services/ingest/src/infer.rs` → `src/services/datafusion-io/src/infer.rs`, byte-for-byte.

```bash
mkdir -p src/services/datafusion-io/src src/services/datafusion-io/tests
cp src/services/ingest/src/write.rs src/services/datafusion-io/src/write.rs
cp src/services/ingest/src/infer.rs src/services/datafusion-io/src/infer.rs
cp src/services/ingest/tests/write.rs src/services/datafusion-io/tests/write.rs
cp src/services/ingest/tests/infer.rs src/services/datafusion-io/tests/infer.rs
```

- [ ] **Step 2: Rename `IngestWriteConfig` → `WriteConfig` in the new `write.rs`**

In `src/services/datafusion-io/src/write.rs`, rename the type (it is no longer ingest-specific) at its 4 occurrences: the `pub struct IngestWriteConfig` declaration, the `impl Default for IngestWriteConfig`, the `estimate_partitions(in_memory_bytes: u64, cfg: &IngestWriteConfig)` parameter, and the `write_dataset(..., cfg: &IngestWriteConfig)` parameter. Use a scoped replace:

```bash
sed -i 's/IngestWriteConfig/WriteConfig/g' src/services/datafusion-io/src/write.rs
```

Update the doc comment on the struct from "Tunables for the DataFusion ingest write." to "Tunables for the DataFusion write." (drop "ingest").

- [ ] **Step 3: Write `src/services/datafusion-io/src/lib.rs`**

```rust
//! DuckLake <-> DataFusion IO: write Arrow batches as size-targeted Snappy Parquet
//! into object storage (with the per-file DuckLake stats the snapshot-commit
//! primitive needs), read a DuckLake table's Parquet back as a DataFusion table,
//! and infer DuckLake column specs from an Arrow schema. Shared by `ingest` and
//! `transform`.

pub mod infer;
pub mod scan;
pub mod write;

pub use infer::{InferError, duck_type, infer_columns};
pub use scan::{ScanError, scan_table};
pub use write::{
    WriteConfig, WriteError, WrittenFile, estimate_partitions, file_stats_from_bytes, write_dataset,
};
```

(`scan` is added in Task 3; declare it now and create a placeholder so the crate compiles — see Step 4.)

- [ ] **Step 4: Create a temporary empty `scan.rs` so the crate compiles**

`src/services/datafusion-io/src/scan.rs`:

```rust
//! Read path — registering a DuckLake table's Parquet files as a DataFusion table.
//! Implemented in Task 3.

#[derive(Debug, thiserror::Error)]
pub enum ScanError {
    #[error("placeholder")]
    Placeholder,
}

/// Placeholder; real signature + body land in Task 3.
pub async fn scan_table() {}
```

(The `pub use scan::{ScanError, scan_table};` in lib.rs resolves against these; Task 3 replaces this file and adjusts the re-export.)

- [ ] **Step 5: Re-point the moved test files**

In `src/services/datafusion-io/tests/write.rs` and `tests/infer.rs`, change every `ingest::write::` → `datafusion_io::write::`, every `ingest::infer::` → `datafusion_io::infer::`, and `IngestWriteConfig` → `WriteConfig`:

```bash
sed -i 's/ingest::write::/datafusion_io::write::/g; s/IngestWriteConfig/WriteConfig/g' src/services/datafusion-io/tests/write.rs
sed -i 's/ingest::infer::/datafusion_io::infer::/g' src/services/datafusion-io/tests/infer.rs
```

- [ ] **Step 6: Write `src/services/datafusion-io/BUCK`**

```python
rust_library(
    name = "datafusion-io",
    crate = "datafusion_io",
    srcs = glob(["src/**/*.rs"]),
    crate_root = "src/lib.rs",
    edition = "2024",
    deps = [
        "//src/control-plane/core:core",
        "//third-party:arrow",
        "//third-party:bytes",
        "//third-party:datafusion",
        "//third-party:futures",
        "//third-party:object_store",
        "//third-party:parquet",
        "//third-party:thiserror",
    ],
    visibility = ["PUBLIC"],
)

rust_test(
    name = "write",
    crate = "write",
    srcs = ["tests/write.rs"],
    crate_root = "tests/write.rs",
    edition = "2024",
    deps = [
        ":datafusion-io",
        "//third-party:arrow",
        "//third-party:datafusion",
        "//third-party:object_store",
        "//third-party:parquet",
        "//third-party:tokio",
    ],
)

rust_test(
    name = "infer",
    crate = "infer",
    srcs = ["tests/infer.rs"],
    crate_root = "tests/infer.rs",
    edition = "2024",
    deps = [":datafusion-io", "//third-party:arrow"],
)
```

- [ ] **Step 7: Build + test the new crate**

Run:
```bash
eval "$(./tools/env.sh)" 2>/dev/null
buck2 build //src/services/datafusion-io:datafusion-io > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|error" /tmp/b.log | tail
buck2 test //src/services/datafusion-io:write //src/services/datafusion-io:infer > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```
Expected: BUILD SUCCEEDED; `Tests finished: Pass 2. Fail 0.` (the relocated write + infer tests pass unchanged).

- [ ] **Step 8: Commit**

```bash
git config user.email jackomayo@gmail.com; git config user.name rsJames-ttrpg
git add src/services/datafusion-io
git commit -m "refactor(datafusion-io): extract write_dataset + infer from ingest into a shared crate"
```

---

## Task 2: Point `ingest` at `datafusion-io`

Delete ingest's now-duplicated `write.rs`/`infer.rs`, redirect its consumers, and drop the relocated tests.

**Files:**
- Delete: `src/services/ingest/src/write.rs`, `src/services/ingest/src/infer.rs`, `src/services/ingest/tests/write.rs`, `src/services/ingest/tests/infer.rs`
- Modify: `src/services/ingest/src/lib.rs`, `src/services/ingest/src/materialize.rs`, `src/services/ingest/BUCK`

- [ ] **Step 1: Delete the moved files**

```bash
git rm src/services/ingest/src/write.rs src/services/ingest/src/infer.rs src/services/ingest/tests/write.rs src/services/ingest/tests/infer.rs
```

- [ ] **Step 2: Update `src/services/ingest/src/lib.rs`**

Remove the `pub mod infer;` and `pub mod write;` lines. Re-point `IngestError`'s `#[from]` arms at the new crate. Replace the two arms:

```rust
    #[error(transparent)]
    Infer(#[from] infer::InferError),
    #[error(transparent)]
    Write(#[from] write::WriteError),
```

with:

```rust
    #[error(transparent)]
    Infer(#[from] datafusion_io::InferError),
    #[error(transparent)]
    Write(#[from] datafusion_io::WriteError),
```

(The `pub use materialize::{…}`, `pub use bind::{…}`, `pub use gate::{…}` lines and the other modules stay.)

- [ ] **Step 3: Update `src/services/ingest/src/materialize.rs`**

Replace the two imports:

```rust
use crate::infer::infer_columns;
use crate::write::{IngestWriteConfig, write_dataset};
```

with one:

```rust
use datafusion_io::{WriteConfig, infer_columns, write_dataset};
```

and the call `&IngestWriteConfig::default()` → `&WriteConfig::default()`. Everything else in `materialize.rs` is unchanged (the `WrittenFile` → `DataFile` mapping uses the returned struct's fields, which are identical).

- [ ] **Step 4: Update `src/services/ingest/BUCK`**

Add `"//src/services/datafusion-io:datafusion-io",` to the `ingest` `rust_library`'s `deps` (keep sorted with the `//src/...` entries). Remove the now-unused third-party deps from the library that lived only in the moved files — delete the `"//third-party:bytes"`, `"//third-party:datafusion"`, `"//third-party:futures"`, and `"//third-party:parquet"` lines from the `ingest` library `deps`. Then **delete the entire `rust_test(name = "write", …)` and `rust_test(name = "infer", …)` target blocks** (the tests moved).

> If the Step-5 build reports any of the removed third-party deps is still needed by ingest's remaining code (bind/gate/http/materialize), re-add that one line — the build is the oracle.

- [ ] **Step 5: Build ingest + run its remaining tests**

Run:
```bash
eval "$(./tools/env.sh)" 2>/dev/null
buck2 build //src/services/ingest:ingest //src/services/ingest:ingest-bin > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|error" /tmp/b.log | tail
buck2 test //src/services/ingest:materialize //src/services/ingest:ducklake-interop //src/services/ingest:http-land > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```
Expected: BUILD SUCCEEDED; `Tests finished: Pass 3. Fail 0.` (ingest behavior is unchanged — the materializer + interop oracle are the regression guard).

- [ ] **Step 6: Commit**

```bash
git config user.email jackomayo@gmail.com; git config user.name rsJames-ttrpg
git add -A src/services/ingest
git commit -m "refactor(ingest): consume datafusion-io; drop the moved write/infer modules"
```

---

## Task 3: The read path — `scan_table`

Add the new DuckLake→DataFusion read to `datafusion-io`: register a table's Parquet files (from the catalog file list) as a named DataFusion table.

**Files:**
- Modify: `src/services/datafusion-io/src/scan.rs`, `src/services/datafusion-io/BUCK`
- Test: `src/services/datafusion-io/tests/scan.rs`

- [ ] **Step 1: Write the failing test**

Create `src/services/datafusion-io/tests/scan.rs`:

```rust
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{FileRef, TableRef};
use datafusion::execution::context::SessionContext;
use datafusion_io::{WriteConfig, scan_table, write_dataset};
use object_store::memory::InMemory;
use object_store::{ObjectStore, ObjectStoreExt};

#[tokio::test]
async fn scan_registers_written_files_for_sql() {
    // Write a 2-row table via the write path, then scan it back via SQL.
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
        ],
    )
    .unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let written = write_dataset(
        store.clone(),
        "main/customer/run-1",
        schema.clone(),
        &[batch],
        &WriteConfig::default(),
    )
    .await
    .unwrap();
    // Map WrittenFile -> the catalog's FileRef shape (path is table-dir-relative).
    let files: Vec<FileRef> = written
        .iter()
        .map(|w| FileRef {
            path: w.path.clone(),
            record_count: w.record_count,
            file_size_bytes: w.file_size_bytes,
        })
        .collect();

    let table = TableRef {
        schema: "main".into(),
        name: "customer".into(),
    };
    let ctx = SessionContext::new();
    scan_table(&ctx, store.clone(), "customer", &table, &files)
        .await
        .unwrap();

    let df = ctx
        .sql("SELECT id, name FROM customer ORDER BY id")
        .await
        .unwrap();
    let out = df.collect().await.unwrap();
    let n: usize = out.iter().map(|b| b.num_rows()).sum();
    assert_eq!(n, 2, "both rows are scannable via SQL");
}
```

- [ ] **Step 2: Wire the test target + run it to verify it fails**

In `src/services/datafusion-io/BUCK`, add:

```python
rust_test(
    name = "scan",
    crate = "scan",
    srcs = ["tests/scan.rs"],
    crate_root = "tests/scan.rs",
    edition = "2024",
    deps = [
        ":datafusion-io",
        "//src/control-plane/core:core",
        "//third-party:arrow",
        "//third-party:datafusion",
        "//third-party:object_store",
        "//third-party:tokio",
    ],
)
```

Run: `eval "$(./tools/env.sh)" 2>/dev/null; buck2 test //src/services/datafusion-io:scan > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error" /tmp/t.log`
Expected: FAIL — `scan_table` signature mismatch (the placeholder takes no args).

- [ ] **Step 3: Implement `scan_table`**

Replace `src/services/datafusion-io/src/scan.rs` with:

```rust
//! Read path: register a DuckLake table's Parquet files (from the catalog file
//! list) as a named DataFusion table, so a transform's SQL can reference it.
//! The inverse of `write_dataset` — same loom object-store URL + relative layout.

use std::sync::Arc;

use control_plane_core::{FileRef, TableRef};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::context::SessionContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::ObjectStore;

use crate::write::LOOM_STORE_URL;

#[derive(Debug, thiserror::Error)]
pub enum ScanError {
    #[error("datafusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),
}

/// Register `files` (a DuckLake table's data files at some snapshot) as a DataFusion
/// table named `name`. Each `FileRef.path` is table-dir-relative; the full object key
/// is reconstructed as `<schema>/<table>/<path>` under the loom object store — matching
/// how `write_dataset` lays files out (Decision A in the design).
pub async fn scan_table(
    ctx: &SessionContext,
    store: Arc<dyn ObjectStore>,
    name: &str,
    table: &TableRef,
    files: &[FileRef],
) -> Result<(), ScanError> {
    let url = ObjectStoreUrl::parse(LOOM_STORE_URL)?;
    ctx.register_object_store(url.as_ref(), store.clone());

    let paths: Vec<ListingTableUrl> = files
        .iter()
        .map(|f| {
            let key = format!(
                "{LOOM_STORE_URL}/{}/{}/{}",
                table.schema, table.name, f.path
            );
            ListingTableUrl::parse(key)
        })
        .collect::<Result<_, _>>()?;

    let opts = ListingOptions::new(Arc::new(ParquetFormat::default()));
    let cfg = ListingTableConfig::new_with_multi_paths(paths)
        .with_listing_options(opts)
        .infer_schema(&ctx.state())
        .await?;
    let provider = ListingTable::try_new(cfg)?;
    ctx.register_table(name, Arc::new(provider))?;
    Ok(())
}
```

Then make `LOOM_STORE_URL` visible to `scan.rs`: in `src/services/datafusion-io/src/write.rs`, change `const LOOM_STORE_URL: &str = "loom://data";` to `pub(crate) const LOOM_STORE_URL: &str = "loom://data";`.

> **DataFusion 54 API note:** the `ListingOptions`/`ListingTableConfig`/`ListingTableUrl`/`ParquetFormat` import paths and method names (`new_with_multi_paths`, `with_listing_options`, `infer_schema`, `register_table`) are written against DataFusion 54; if a path or method has drifted, the Step-4 build is the oracle — adjust the import/call to match the compiler error, keeping the behavior (register the explicit file list as a named table). The `register_table` return is `Result<Option<Arc<dyn TableProvider>>, _>`; if the compiler complains about the unused `Option`, bind it with `let _ = ctx.register_table(...)?;`.

- [ ] **Step 4: Run the test to verify it passes**

Run: `eval "$(./tools/env.sh)" 2>/dev/null; buck2 test //src/services/datafusion-io:scan > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: `Tests finished: Pass 1. Fail 0.`

- [ ] **Step 5: Commit**

```bash
git config user.email jackomayo@gmail.com; git config user.name rsJames-ttrpg
git add src/services/datafusion-io/src/scan.rs src/services/datafusion-io/src/write.rs src/services/datafusion-io/tests/scan.rs src/services/datafusion-io/BUCK
git commit -m "feat(datafusion-io): scan_table registers a DuckLake table for DataFusion SQL"
```

---

## Task 4: The `transform` primitive — `run_transform`

Create the `transform` crate with the orchestration: resolve inputs → `ctx.sql` → infer → write → atomic `Tx` commit + lineage.

**Files:**
- Create: `src/services/transform/{BUCK, src/lib.rs, src/run.rs}`

- [ ] **Step 1: Write `src/services/transform/src/run.rs`**

```rust
//! The transform primitive: resolve input DuckLake table(s), have DataFusion run a
//! SQL query over them, and commit the result as a new snapshot of the output table
//! plus lineage (inputs -> output), atomically. Append semantics.

use std::sync::Arc;

use control_plane_core::{
    ColumnSpec, ControlPlane, DataFile, LineageEvent, SnapshotId, TableRef,
};
use datafusion::execution::context::SessionContext;
use datafusion_io::{WriteConfig, infer_columns, scan_table, write_dataset};
use object_store::ObjectStore;

/// One transform: read `inputs`, run `sql`, write the result to `output`.
pub struct TransformRequest<'a> {
    pub inputs: &'a [TableRef],
    pub output: &'a TableRef,
    pub sql: &'a str,
    /// Built by the caller; inputs -> output. Emitted in the commit transaction.
    pub lineage: LineageEvent,
}

#[derive(Debug, thiserror::Error)]
pub enum TransformError {
    #[error("unknown input table {0}.{1}")]
    UnknownInput(String, String),
    #[error("sql/datafusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),
    #[error(transparent)]
    Scan(#[from] datafusion_io::ScanError),
    #[error(transparent)]
    Write(#[from] datafusion_io::WriteError),
    #[error(transparent)]
    Infer(#[from] datafusion_io::InferError),
    #[error(transparent)]
    ControlPlane(#[from] control_plane_core::ControlPlaneError),
    #[error("commit produced no snapshot id")]
    NoSnapshot,
}

/// Run one transform. `run_id` is a caller-unique output-file prefix (e.g. a UUID).
pub async fn run_transform(
    cp: &dyn ControlPlane,
    store: Arc<dyn ObjectStore>,
    run_id: &str,
    req: TransformRequest<'_>,
) -> Result<SnapshotId, TransformError> {
    let ctx = SessionContext::new();

    // 1. Resolve + register each input as a DataFusion table named by its table name.
    for input in req.inputs {
        let snapshot = cp
            .catalog()
            .current_snapshot(input)
            .await
            .map_err(|e| match e {
                control_plane_core::ControlPlaneError::NotFound(_) => {
                    TransformError::UnknownInput(input.schema.clone(), input.name.clone())
                }
                other => TransformError::ControlPlane(other),
            })?;
        let files = cp
            .catalog()
            .files(input, snapshot.id, control_plane_core::PageReq::unbounded())
            .await?;
        scan_table(&ctx, store.clone(), &input.name, input, &files.items).await?;
    }

    // 2. Run the SQL; collect the result + its Arrow schema.
    let df = ctx.sql(req.sql).await?;
    let schema: Arc<arrow::datatypes::Schema> = Arc::new(df.schema().as_arrow().clone());
    let batches = df.collect().await?;

    // 3. Output physical columns inferred from the result schema.
    let columns: Vec<ColumnSpec> = infer_columns(&schema)?;

    // 4. Write the result as N Snappy Parquet files under the output table dir.
    let dir_prefix = format!("{}/{}/{}", req.output.schema, req.output.name, run_id);
    let written = write_dataset(store, &dir_prefix, schema, &batches, &WriteConfig::default()).await?;
    let data_files: Vec<DataFile> = written
        .into_iter()
        .map(|f| DataFile {
            path: f.path,
            path_is_relative: true,
            record_count: f.record_count,
            file_size_bytes: f.file_size_bytes,
            footer_size: f.footer_size,
            column_stats: f.column_stats,
        })
        .collect();

    // 5. One atomic Tx: create_table (idempotent) + append_files + emit lineage.
    let mut tx = cp.begin().await?;
    tx.create_table(req.output, &columns).await?;
    tx.append_files(req.output, &data_files).await?;
    tx.emit(req.lineage).await?;
    tx.commit().await?.ok_or(TransformError::NoSnapshot)
}
```

> **API-shape notes (compiler is the oracle):** `Snapshot` has an `id: SnapshotId` field (used as `snapshot.id`); `catalog.files(table, SnapshotId, PageReq)` returns a `Page<FileRef>` whose `.items` is the `Vec<FileRef>`; `PageReq::unbounded()` is the existing convention. `df.schema().as_arrow()` yields the `&arrow::datatypes::Schema` (DataFusion's `DFSchema` → Arrow). If any of these accessor names differ in the vendored versions, adjust to match — the Task 6 e2e is the behavioral oracle. (Confirm `Snapshot`'s id field name and `Page`'s items field name against `src/control-plane/core/src/catalog.rs` / `page.rs` before relying on them.)

- [ ] **Step 2: Write `src/services/transform/src/lib.rs`**

The `handler` module lands in Task 5; this task ships only `run`. Write exactly:

```rust
//! loom transform service: queue-driven SQL transforms. A worker reads input
//! DuckLake table(s) with DataFusion, runs a SQL query, and commits the result as a
//! new snapshot + lineage. See docs/superpowers/specs/.

pub mod run;

pub use run::{TransformError, TransformRequest, run_transform};
```

(Task 5 Step 2 adds the `pub mod handler;` + `pub use handler::transform_handler;` lines.)

- [ ] **Step 3: Write `src/services/transform/BUCK`**

```python
rust_library(
    name = "transform",
    crate = "transform",
    srcs = glob(["src/**/*.rs"]),
    crate_root = "src/lib.rs",
    edition = "2024",
    deps = [
        "//src/control-plane/core:core",
        "//src/services/datafusion-io:datafusion-io",
        "//third-party:arrow",
        "//third-party:datafusion",
        "//third-party:object_store",
        "//third-party:serde",
        "//third-party:serde_json",
        "//third-party:thiserror",
    ],
    visibility = ["PUBLIC"],
)
```

(`main.rs` is excluded from the library because `crate_root = src/lib.rs`; the `glob` includes it but it's only compiled by the binary target. To avoid the library glob picking up `main.rs`, the binary target in Task 5 uses `srcs = ["src/main.rs"]` and the library `glob` is fine because `main.rs` is not referenced from `lib.rs`. If buck objects to `main.rs` in the library glob, change the library `srcs` to `glob(["src/**/*.rs"], exclude = ["src/main.rs"])`.)

- [ ] **Step 4: Build the crate**

Run: `eval "$(./tools/env.sh)" 2>/dev/null; buck2 build //src/services/transform:transform > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|error" /tmp/b.log | tail`
Expected: BUILD SUCCEEDED. (If the build flags one of the API-shape accessors in the note, fix it to match the compiler and rebuild.)

- [ ] **Step 5: Commit**

```bash
git config user.email jackomayo@gmail.com; git config user.name rsJames-ttrpg
git add src/services/transform
git commit -m "feat(transform): run_transform primitive (resolve inputs -> sql -> snapshot+lineage)"
```

---

## Task 5: Worker handler + binary

Wire `run_transform` as a queue handler and a runnable worker binary.

**Files:**
- Create: `src/services/transform/src/handler.rs`, `src/services/transform/src/main.rs`
- Modify: `src/services/transform/src/lib.rs`, `src/services/transform/BUCK`

- [ ] **Step 1: Write `src/services/transform/src/handler.rs`**

```rust
//! Queue handler: parse a transform job payload, run it, map the outcome to a
//! `JobFailure` (deterministic errors -> Abandon, transient -> Retry with backoff).

use std::sync::Arc;
use std::time::Duration;

use control_plane_core::{
    ControlPlane, DatasetRef, EventType, Job, JobFailure, LineageEvent, RetryPolicy, RunId,
    TableRef,
};
use object_store::ObjectStore;
use serde::Deserialize;
use uuid::Uuid;

use crate::run::{TransformError, TransformRequest, run_transform};

/// Wire form of a transform job payload. `{schema, name}` per table.
#[derive(Deserialize)]
struct TableSpec {
    schema: String,
    name: String,
}
impl From<&TableSpec> for TableRef {
    fn from(t: &TableSpec) -> Self {
        TableRef {
            schema: t.schema.clone(),
            name: t.name.clone(),
        }
    }
}

#[derive(Deserialize)]
struct TransformPayload {
    inputs: Vec<TableSpec>,
    output: TableSpec,
    sql: String,
}

/// Run one transform job. Pure-ish: takes the deps + the job, returns the worker
/// outcome. Used directly by the binary's handler closure and by tests.
pub async fn transform_handler(
    cp: &dyn ControlPlane,
    store: Arc<dyn ObjectStore>,
    job: Job,
) -> Result<(), JobFailure> {
    // Deserialize the payload. Malformed -> Abandon (retry can't help).
    let payload: TransformPayload = match serde_json::from_value(job.payload.clone()) {
        Ok(p) => p,
        Err(e) => {
            return Err(JobFailure {
                error: format!("malformed transform payload: {e}"),
                policy: RetryPolicy::Abandon,
            });
        }
    };
    let inputs: Vec<TableRef> = payload.inputs.iter().map(TableRef::from).collect();
    let output = TableRef::from(&payload.output);
    let run_id = Uuid::new_v4().to_string();

    let lineage = LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: inputs.iter().map(DatasetRef::from).collect(),
        outputs: vec![DatasetRef::from(&output)],
        payload: serde_json::json!({ "sql": payload.sql }),
    };

    let res = run_transform(
        cp,
        store,
        &run_id,
        TransformRequest {
            inputs: &inputs,
            output: &output,
            sql: &payload.sql,
            lineage,
        },
    )
    .await;

    res.map(|_snapshot| ()).map_err(|e| JobFailure {
        error: e.to_string(),
        policy: retry_policy(&e, job.attempts),
    })
}

/// Deterministic failures can't be retried; transient ones back off on attempts.
fn retry_policy(err: &TransformError, attempts: i32) -> RetryPolicy {
    match err {
        // Deterministic: the job will fail identically on retry.
        TransformError::UnknownInput(..)
        | TransformError::DataFusion(_)
        | TransformError::Infer(_)
        | TransformError::NoSnapshot => RetryPolicy::Abandon,
        // Transient: control-plane / object-store / scan IO may succeed later.
        TransformError::ControlPlane(_)
        | TransformError::Scan(_)
        | TransformError::Write(_) => RetryPolicy::Retry {
            delay: Duration::from_secs(2u64.saturating_pow(attempts.clamp(0, 6) as u32)),
        },
    }
}
```

> **Accessor note:** confirm `DatasetRef: From<&TableRef>` exists (ingest uses `DatasetRef::from(&t)` — it does), and `LineageEvent`'s field set (`run_id`, `event_type`, `event_time`, `inputs`, `outputs`, `payload`) against `materialize.rs`'s usage / `src/control-plane/core/src/lineage.rs`. `EventType::Complete`/`RunId` are the same types ingest uses.

- [ ] **Step 2: Restore the handler exports in `src/services/transform/src/lib.rs`**

```rust
//! loom transform service: queue-driven SQL transforms. See docs/superpowers/specs/.

pub mod handler;
pub mod run;

pub use handler::transform_handler;
pub use run::{TransformError, TransformRequest, run_transform};
```

- [ ] **Step 3: Write `src/services/transform/src/main.rs`**

```rust
//! transform binary: build the control plane + object store from env config via
//! service_runtime, then run the queue worker loop with the transform handler.
//! Queue-driven — no HTTP surface.

use std::sync::Arc;

use control_plane_core::{ControlPlane, Job, JobFailure};
use control_plane_worker::Worker;
use object_store::ObjectStore;
use tokio_util::sync::CancellationToken;
use transform::transform_handler;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = service_runtime::Config::from_env()?;
    let pool = service_runtime::build_pool(&cfg.db).await?;
    let cp = service_runtime::control_plane(pool, cfg.lock_timeout);
    let store: Arc<dyn ObjectStore> = Arc::new(service_runtime::local_store(&cfg.data_path)?);

    // The worker owns the queue (a PgControlPlane clone); the handler borrows the cp
    // + store as a trait object / Arc per job.
    let cp_for_handler: Arc<dyn ControlPlane> = Arc::new(cp.clone());
    let worker = Worker::new(cp, "transform-1", cfg.lock_timeout);
    let shutdown = CancellationToken::new();

    worker
        .run(&["transform".to_string()], shutdown, move |job: Job| {
            let cp = cp_for_handler.clone();
            let store = store.clone();
            async move {
                transform_handler(cp.as_ref(), store, job).await
            }
        })
        .await?;
    Ok(())
}
```

> **Type note:** `service_runtime::control_plane(...)` returns a concrete `PgControlPlane` which is `Clone` and implements both `ControlPlane` and `Queue`. `Worker::new` takes the queue by value (the `cp` move), while the handler needs a `&dyn ControlPlane` — hence the `Arc<dyn ControlPlane>` clone built before the move. If the borrow checker objects to moving `cp` into `Worker::new` after `cp.clone()`, clone first (`let cp_for_handler = Arc::new(cp.clone());`) then move `cp` — which is the order shown. The handler closure returns a `Future<Output = Result<(), JobFailure>>` as `Worker::run` requires. `Worker::run` only returns when `shutdown` is cancelled; for the long-running binary that's process lifetime (no signal handling in part-1).

- [ ] **Step 4: Update `src/services/transform/BUCK`**

Set the library `srcs` to exclude `main.rs`, and add the binary target. Replace the file with:

```python
rust_library(
    name = "transform",
    crate = "transform",
    srcs = glob(["src/**/*.rs"], exclude = ["src/main.rs"]),
    crate_root = "src/lib.rs",
    edition = "2024",
    deps = [
        "//src/control-plane/core:core",
        "//src/services/datafusion-io:datafusion-io",
        "//third-party:arrow",
        "//third-party:datafusion",
        "//third-party:object_store",
        "//third-party:serde",
        "//third-party:serde_json",
        "//third-party:thiserror",
        "//third-party:time",
        "//third-party:uuid",
    ],
    visibility = ["PUBLIC"],
)

rust_binary(
    name = "transform-bin",
    crate = "transform_bin",
    srcs = ["src/main.rs"],
    crate_root = "src/main.rs",
    edition = "2024",
    deps = [
        ":transform",
        "//src/control-plane/core:core",
        "//src/control-plane/worker:worker",
        "//src/services/runtime:runtime",
        "//third-party:object_store",
        "//third-party:tokio",
        "//third-party:tokio-util",
    ],
    visibility = ["PUBLIC"],
)
```

(The library gained `//third-party:time` + `//third-party:uuid` for the handler's `LineageEvent`/`Uuid`.)

- [ ] **Step 5: Build library + binary**

Run: `eval "$(./tools/env.sh)" 2>/dev/null; buck2 build //src/services/transform:transform //src/services/transform:transform-bin > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|error" /tmp/b.log | tail`
Expected: BUILD SUCCEEDED.

- [ ] **Step 6: Commit**

```bash
git config user.email jackomayo@gmail.com; git config user.name rsJames-ttrpg
git add src/services/transform
git commit -m "feat(transform): worker handler + queue-driven binary"
```

---

## Task 6: End-to-end fixture test (the load-bearing proof)

Prove the full loop: land two inputs via ingest → enqueue a transform job → run the worker → assert the output snapshot, the joined rows, the lineage, and a DuckDB read-back.

**Files:**
- Create: `src/services/transform/tests/transform_e2e.rs`
- Modify: `src/services/transform/BUCK`

- [ ] **Step 1: Write the test**

Create `src/services/transform/tests/transform_e2e.rs`:

```rust
//! Queue -> worker -> transform -> snapshot + lineage -> read-back, against real
//! Postgres + DuckDB. Lands inputs via the ingest materializer, then runs a SQL join.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{
    Acl, ControlPlane, DatasetRef, EventType, Lineage, LineageEvent, NewJob, PageReq, Queue, RunId,
    TableRef,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use control_plane_worker::Worker;
use ingest::{MaterializeRequest, materialize};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use time::OffsetDateTime;
use tokio_util::sync::CancellationToken;
use transform::transform_handler;
use uuid::Uuid;

fn tref(s: &str, n: &str) -> TableRef {
    TableRef {
        schema: s.into(),
        name: n.into(),
    }
}

async fn land(
    cp: &PgControlPlane,
    store: &Arc<dyn ObjectStore>,
    table: &TableRef,
    schema: Arc<Schema>,
    batch: RecordBatch,
) {
    let lineage = LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetRef::from(table)],
        payload: serde_json::json!({}),
    };
    materialize(
        cp,
        store.clone(),
        MaterializeRequest {
            table,
            schema,
            batches: &[batch],
            file_prefix: "run-1",
            gate: None,
            lineage,
        },
    )
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn transform_joins_two_inputs_into_a_new_snapshot() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(writer.data_path()).unwrap());

    // Land customers(id, region) and orders(id, customer_id, amount).
    let customers = tref("main", "customers");
    let cust_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, true),
    ]));
    land(
        &cp,
        &store,
        &customers,
        cust_schema.clone(),
        RecordBatch::try_new(
            cust_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("CA"), Some("NY")])),
            ],
        )
        .unwrap(),
    )
    .await;

    let orders = tref("main", "orders");
    let ord_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
    ]));
    land(
        &cp,
        &store,
        &orders,
        ord_schema.clone(),
        RecordBatch::try_new(
            ord_schema,
            vec![
                Arc::new(Int64Array::from(vec![10, 11, 12])),
                Arc::new(Int64Array::from(vec![1, 1, 2])),
            ],
        )
        .unwrap(),
    )
    .await;

    // Enqueue a transform: join orders to customers -> orders_enriched(id, region).
    cp.enqueue(NewJob {
        kind: "transform".into(),
        payload: serde_json::json!({
            "inputs": [
                { "schema": "main", "name": "customers" },
                { "schema": "main", "name": "orders" }
            ],
            "output": { "schema": "main", "name": "orders_enriched" },
            "sql": "SELECT o.id AS id, c.region AS region \
                    FROM orders o JOIN customers c ON o.customer_id = c.id"
        }),
        run_at: None,
        priority: 0,
    })
    .await
    .unwrap();

    // Run the worker until it drains the one job, then cancel.
    let store_h = store.clone();
    let token = CancellationToken::new();
    let t = token.clone();
    let worker =
        Worker::new(cp.clone(), "transform-test", Duration::from_millis(300))
            .with_poll_interval(Duration::from_millis(50));
    let cp_h: Arc<dyn ControlPlane> = Arc::new(cp.clone());
    let handle = tokio::spawn(async move {
        worker
            .run(&["transform".to_string()], t, move |job| {
                let cp = cp_h.clone();
                let store = store_h.clone();
                async move { transform_handler(cp.as_ref(), store, job).await }
            })
            .await
    });
    tokio::time::sleep(Duration::from_millis(800)).await;
    token.cancel();
    handle.await.unwrap().unwrap();

    // The job completed (removed from the queue).
    assert!(
        cp.dequeue(&["transform".to_string()], "probe")
            .await
            .unwrap()
            .is_none(),
        "transform job completed"
    );

    // The output table has a snapshot with the 3 joined rows; DuckDB reads them.
    let count = writer
        .query_scalar("SELECT count(*) FROM lake.main.orders_enriched;")
        .await;
    assert_eq!(count, "3", "DuckDB reads the transform output");
    let regions = writer
        .query_scalar(
            "SELECT string_agg(region, ',' ORDER BY id) FROM lake.main.orders_enriched;",
        )
        .await;
    assert_eq!(regions, "CA,CA,NY", "join produced the right regions");

    // Lineage records inputs -> output for the transform's run.
    let out_ds = DatasetRef::from(&tref("main", "orders_enriched"));
    let ups = cp
        .lineage()
        .upstream(&out_ds, PageReq::unbounded())
        .await
        .unwrap();
    let up_names: std::collections::HashSet<String> =
        ups.items.iter().map(|d| d.name.clone()).collect();
    assert!(
        up_names.iter().any(|n| n.contains("customers"))
            && up_names.iter().any(|n| n.contains("orders")),
        "lineage upstream of orders_enriched includes both inputs, got {up_names:?}"
    );
}
```

> **Accessor notes:** verify `Lineage::upstream(&DatasetRef, PageReq) -> Page<DatasetRef>` and `DatasetRef.name` against `src/control-plane/core/src/lineage.rs`; `DuckLakeWriter::query_scalar` / `bootstrap` / `data_path` are the same helpers the ingest interop oracle uses. If `DatasetRef` names are namespace-qualified (e.g. `"main.customers"` or a datasource-prefixed form), the `.contains("customers")` check tolerates it; tighten if you confirm the exact format.

- [ ] **Step 2: Wire the fixture test target**

In `src/services/transform/BUCK`, add (it lands inputs via `ingest::materialize`, so it deps `ingest` + the fixture stack, with `duckdb = True`):

```python
loom_fixture_test(
    name = "transform-e2e",
    crate = "transform_e2e",
    srcs = ["tests/transform_e2e.rs"],
    crate_root = "tests/transform_e2e.rs",
    duckdb = True,
    deps = [
        ":transform",
        "//src/services/ingest:ingest",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//src/control-plane/worker:worker",
        "//third-party:arrow",
        "//third-party:object_store",
        "//third-party:serde_json",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:tokio-util",
        "//third-party:uuid",
    ],
)
```

Add the `load("//src/control-plane/postgres:defs.bzl", "loom_fixture_test")` line at the TOP of `src/services/transform/BUCK` (required for `loom_fixture_test`).

- [ ] **Step 3: Run the e2e**

Run: `eval "$(./tools/env.sh)" 2>/dev/null; rm -f /dev/shm/PostgreSQL.* 2>/dev/null; buck2 test //src/services/transform:transform-e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: `Tests finished: Pass 1. Fail 0.` — the full queue→worker→transform→snapshot→lineage→read-back loop. If it fails, debug with superpowers:systematic-debugging (likely candidates: the §3 path reconstruction, a DataFusion accessor name, or the lineage format) — do NOT weaken an assertion.

- [ ] **Step 4: Commit**

```bash
git config user.email jackomayo@gmail.com; git config user.name rsJames-ttrpg
git add src/services/transform/tests/transform_e2e.rs src/services/transform/BUCK
git commit -m "test(transform): e2e queue->worker->transform->snapshot+lineage->read-back"
```

---

## Task 7: Roadmap + full verification

**Files:**
- Modify: `docs/superpowers/specs/2026-06-06-loom-roadmap.md`

- [ ] **Step 1: Update the roadmap**

In `docs/superpowers/specs/2026-06-06-loom-roadmap.md`, under **Step 3 → the layers above**, change the **Transform workers** bullet (it currently reads as future) to mark part-1 delivered. Replace:

```
- **Transform workers** — built on `control-plane-worker`: consume the queue, run
  DataFusion, write snapshots, emit lineage + enqueue downstream atomically; optional
  Ballista escalation.
```
with:
```
- **Transform workers** —
  - *Part 1 — queue-driven SQL transform* ✅ DELIVERED
    (`2026-06-14-transform-workers-part1-design.md`). A worker (on `control-plane-worker`)
    reads input DuckLake table(s) with DataFusion (the new shared `datafusion-io` `scan_table`),
    runs a SQL query, and commits the result as a new snapshot + lineage (inputs → output),
    atomically. Physical `TableRef` in/out, multi-input, append semantics. Proven by an e2e
    that joins two landed tables off the queue and reads the output back through DuckDB.
  - *Later:* object-model-typed transforms (`Type → Type`, via `resolve` + `bind`); programmatic
    (registered-plan) transforms; overwrite/incremental output (with compaction); DAG /
    transactional enqueue-downstream; optional Ballista escalation.
```

In **"Where we are,"** note that the Transform worker (part-1) is delivered, so all three service pillars now have a load-bearing primitive.

- [ ] **Step 2: Full first-party build + test**

Run:
```bash
eval "$(./tools/env.sh)" 2>/dev/null
rm -f /dev/shm/PostgreSQL.* 2>/dev/null
buck2 build //src/... > /tmp/build.log 2>&1; echo "build=$?"; grep -E "BUILD SUCCEEDED|error" /tmp/build.log | tail -2
buck2 test //src/... > /tmp/test.log 2>&1; echo "test=$?"; grep -E "Tests finished|FAIL" /tmp/test.log
```
Expected: build SUCCEEDED; `Tests finished: … Fail 0.` (all first-party, including the moved `datafusion-io` tests, the unchanged ingest tests, and the new transform e2e). If a fixture test trips on `/dev/shm`, clear `PostgreSQL.*` and re-run.

- [ ] **Step 3: Clippy + reindeer-in-sync + prek**

Run:
```bash
eval "$(./tools/env.sh)" 2>/dev/null
./tools/clippy-all.sh > /tmp/clippy.log 2>&1; echo "clippy=$?"; grep -iE "warning|error" /tmp/clippy.log | head
buck2 run //tools:prek -- run --all-files > /tmp/prek.log 2>&1; echo "prek=$?"; tail -6 /tmp/prek.log
```
Expected: clippy exit 0, no warnings; prek passes (reindeer-in-sync is a no-op — no Cargo manifests changed). Commit any markdown the hooks fix.

- [ ] **Step 4: Commit**

```bash
git config user.email jackomayo@gmail.com; git config user.name rsJames-ttrpg
git add docs/superpowers/specs/2026-06-06-loom-roadmap.md
git commit -m "docs(roadmap): transform workers part 1 delivered"
```

---

## Self-review notes (for the executor)

- **Spec coverage:** Task 1 = extract `datafusion-io` (`write_dataset`/`infer` moved, renamed); Task 2 = ingest consumes it; Task 3 = `scan_table` (the new read path, Decision A path reconstruction); Task 4 = `run_transform` primitive + `TransformError`; Task 5 = handler (Decision-C-free, deterministic→Abandon / transient→Retry, §5) + binary; Task 6 = the load-bearing queue→worker→snapshot+lineage→read-back e2e; Task 7 = roadmap + full verify. Append semantics (Decision B) is inherent in `append_files`.
- **Type consistency:** `WriteConfig` (renamed) is used identically in `datafusion-io` (write/scan/tests), ingest's `materialize`, and transform's `run`. `scan_table(ctx, store, name, table, files)` matches its call in `run_transform`. `TransformRequest`/`TransformError`/`transform_handler` names match across `run.rs`, `handler.rs`, `main.rs`, and the e2e. `FileRef`/`DataFile`/`SnapshotId`/`LineageEvent`/`DatasetRef` are the existing core types.
- **Buck-only:** no `Cargo.toml`/lockfile/reindeer changes; the new crates reference already-vendored `//third-party:*` targets.
- **Known oracles:** the DataFusion 54 read API (Task 3) and the catalog/lineage accessor names (Tasks 4/6) are "compiler/e2e is the oracle" spots — flagged inline; the Task 6 fixture e2e is the behavioral gate for the whole slice.
