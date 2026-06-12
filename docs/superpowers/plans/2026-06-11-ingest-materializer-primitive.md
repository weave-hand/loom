# Ingest Materializer Primitive Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build loom's landing-edge ingest: turn Arrow `RecordBatch`es into a Parquet file in object storage and register it as a DuckLake snapshot + lineage via the part-1 snapshot-commit primitive, with an optional model-conformance gate at the front edge.

**Architecture:** A new `src/services/ingest` library crate (no network endpoint, no DataFusion) with five focused units — `infer` (Arrow→DuckLake schema), `gate` (optional `ModelShape` validation), `write` (Parquet + stats extraction), `store` (object-store put), and `materialize` (the orchestrator that calls `Tx::create_table` + `append_files` + `emit` + `commit` as one atomic transaction). The control-plane crates stay free of Parquet/object-store deps; those live only in this crate.

**Tech Stack:** Rust, buck2, `arrow 58`, `parquet 58` (Snappy-only, C-free), `object_store` (local-only), the existing `control-plane-{core,postgres,memory}` crates, the pinned DuckDB CLI for the interop guardrail.

**Spec:** `docs/superpowers/specs/2026-06-11-ingest-materializer-primitive-design.md`

---

## Background the implementer needs

**The part-1 primitive you build on** (already on `main`, in `control_plane_core`):

```rust
// control_plane_core re-exports (src/control-plane/core/src/lib.rs):
pub use catalog::{SnapshotId, TableRef};          // TableRef { schema: String, name: String }
pub use snapshot::{ColumnSpec, ColumnStat, DataFile};
pub use lineage::{DatasetRef, EventType, LineageEvent, RunId};
pub use transaction::{ControlPlane, Tx};

// SnapshotId(pub i64)
// ColumnSpec  { name: String, ty: String, nullable: bool }   // ty = DuckLake type string
// ColumnStat  { column_name: String, min: Option<String>, max: Option<String>,
//               null_count: i64, value_count: i64, column_size_bytes: i64 }
// DataFile    { path: String, path_is_relative: bool, record_count: i64,
//               file_size_bytes: i64, footer_size: i64, column_stats: Vec<ColumnStat> }
// DatasetRef  { namespace: String, name: String }
// LineageEvent{ run_id: RunId, event_type: EventType, event_time: OffsetDateTime,
//               inputs: Vec<DatasetRef>, outputs: Vec<DatasetRef>, payload: serde_json::Value }
```

```rust
#[async_trait]
pub trait ControlPlane: Send + Sync {
    async fn begin(&self) -> Result<Box<dyn Tx + Send>>;
}
#[async_trait]
pub trait Tx: Send {
    async fn commit(self: Box<Self>) -> Result<Option<SnapshotId>>; // Some when a catalog op was staged
    async fn rollback(self: Box<Self>) -> Result<()>;
    async fn enqueue(&mut self, job: NewJob) -> Result<JobId>;
    async fn emit(&mut self, event: LineageEvent) -> Result<()>;
    async fn create_table(&mut self, table: &TableRef, columns: &[ColumnSpec]) -> Result<()>; // idempotent
    async fn append_files(&mut self, table: &TableRef, files: &[DataFile]) -> Result<()>;
}
```

**DuckLake file-path resolution (critical for `store` + the interop test):** a `DataFile` with `path_is_relative = true` and `path = "x.parquet"` for table `main.t` is resolved by DuckDB as `<data_path>/main/t/x.parquet` (data_path + schema.path + table.path + file.path; loom's `create_table` sets schema.path=`main/`, table.path=`t/`). So the object-store **key** is `"<schema>/<table>/<file_name>"` while the registered `DataFile.path` is just `"<file_name>"`. This is confirmed by `src/control-plane/postgres/tests/ducklake_interop.rs` (`duckdb_scans_loom_appended_file`).

**The interop fixture** (`control_plane_postgres::fixture`): `PgFixture::start()` → `fixture.fresh_db().await -> (PgControlPlane, String)`; `DuckLakeWriter::new(fixture.socket_path(), &db)` with `.bootstrap()`, `.data_path() -> &Path`, `.exec(sql)`, `.query_scalar(sql) -> String`, `.max_snapshot_id() -> i64`. `PgControlPlane` implements `ControlPlane`.

**Conventions (non-negotiable):**
- Tests are `rust_test`/`loom_fixture_test` integration targets only — NO inline `#[cfg(test)]` (a prek hook fails the build). Each test file is its own BUCK target.
- Fixture tests (boot postgres/duckdb) MUST use `loom_fixture_test`, never bare `rust_test`.
- Run tests with `buck2 test //src/...` (do NOT pipe to `tail`; redirect to a file and grep).
- `rustfmt` is **check-only** — run `buck2 run //tools:rustfmt -- <files>` and apply before committing `.rs`.
- Commits: Conventional Commits, ending with `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`. Never `--no-verify`.
- Branch is `feat/ingest-materializer-primitive` (already created; the spec commit is on it).

---

## File structure

| File | Responsibility |
| --- | --- |
| `src/services/ingest/Cargo.toml` | Crate manifest; declares `arrow`/`parquet`/`object_store` deps (drives reindeer). |
| `src/services/ingest/BUCK` | `rust_library` + per-test-file targets. |
| `src/services/ingest/src/lib.rs` | Module wiring + `IngestError` + the public `materialize` API surface. |
| `src/services/ingest/src/infer.rs` | `duck_type` + `infer_columns`: Arrow `DataType` → DuckLake type string. |
| `src/services/ingest/src/gate.rs` | `ModelShape`/`ColumnShape`/`Violation` + `validate`. |
| `src/services/ingest/src/write.rs` | `write_parquet`: Arrow batches → `WrittenParquet` (bytes + stats). |
| `src/services/ingest/src/store.rs` | `put`: object-store write → `StoredPath`. |
| `src/services/ingest/src/materialize.rs` | `MaterializeRequest` + `materialize` orchestrator. |
| `src/services/ingest/tests/infer.rs` | Unit tests for `infer`. |
| `src/services/ingest/tests/gate.rs` | Unit tests for `gate`. |
| `src/services/ingest/tests/write.rs` | Unit tests for `write` (stats extraction). |
| `src/services/ingest/tests/store.rs` | Unit test for `store`. |
| `src/services/ingest/tests/materialize.rs` | Orchestration tests via `MemoryControlPlane`. |
| `src/services/ingest/tests/ducklake_interop.rs` | The make-or-break: DuckDB reads back loom-materialized data. |

---

## Task 1: Crate scaffold + dependency import

Stand up the crate with the three new third-party deps and prove it builds (including on RE). Nothing functional yet — this isolates the reindeer/buckify risk before any logic.

**Files:**
- Create: `src/services/ingest/Cargo.toml`
- Create: `src/services/ingest/src/lib.rs`
- Create: `src/services/ingest/BUCK`
- Modify: workspace `Cargo.toml` (add the crate to `members` if the workspace lists them — check first)
- Modify (generated): `third-party/BUCK`, `Cargo.lock`

- [ ] **Step 1: Write `Cargo.toml`**

```toml
[package]
name = "ingest"
version = "0.1.0"
edition = "2024"

[dependencies]
control-plane-core = { path = "../../control-plane/core" }
arrow = "58"
parquet = { version = "58", default-features = false, features = ["arrow", "snap"] }
object_store = "0.11"
thiserror = "1"

[dev-dependencies]
control-plane-memory = { path = "../../control-plane/memory" }
control-plane-postgres = { path = "../../control-plane/postgres" }
tokio = { version = "1", features = ["rt", "rt-multi-thread", "macros"] }
tempfile = "3"
uuid = { version = "1", features = ["v4"] }
time = "0.3"
serde_json = "1"
```

Note the deliberate `default-features = false, features = ["arrow", "snap"]` on `parquet`: Snappy is pure-Rust, so we avoid the `zstd`/`lz4` `*-sys` C build-script crates (the RE native-dep risk). `object_store` with no extra features is local-only (no `aws`/`gcp`/`azure` → no `reqwest`/`hyper`).

- [ ] **Step 2: Write a trivial `src/lib.rs` so the crate compiles**

```rust
//! loom ingest: the landing-edge materializer. Turns Arrow batches into a
//! registered DuckLake snapshot + lineage via the part-1 snapshot-commit
//! primitive. See docs/superpowers/specs/2026-06-11-ingest-materializer-primitive-design.md.

pub mod infer;
```

And a stub `src/infer.rs` so the module resolves:

```rust
//! Arrow DataType -> DuckLake type string. (Filled in Task 2.)
```

- [ ] **Step 3: Refresh the lockfile and buckify**

```bash
cd /home/jackm/repos/loom
# If the workspace Cargo.toml has a [workspace] members list, add "src/services/ingest" first.
buck2 run //tools:reindeer -- update      # or: cargo generate-lockfile
./tools/buckify.sh
git diff --stat third-party/BUCK Cargo.lock
```

Expected: `third-party/BUCK` gains `parquet`, `object_store`, and their transitive crates (e.g. `bytes`, `futures`, `snap`, `twox-hash`, `thrift`-equivalent). If reindeer warns about a build-script crate, add `third-party/fixups/<crate>/fixups.toml` with `[buildscript]\nrun = true|false` (mirror the existing `proc-macro2` fixup) and re-run `./tools/buckify.sh`.

- [ ] **Step 4: Write `BUCK` (library only for now)**

Use the alias names buckify actually generated (check `third-party/BUCK` — arrow is `//third-party:arrow-58`; parquet/object_store are likely `//third-party:parquet` / `//third-party:object_store`).

```python
rust_library(
    name = "ingest",
    crate = "ingest",
    srcs = glob(["src/**/*.rs"]),
    crate_root = "src/lib.rs",
    edition = "2024",
    deps = [
        "//src/control-plane/core:core",
        "//third-party:arrow-58",
        "//third-party:parquet",
        "//third-party:object_store",
        "//third-party:thiserror",
    ],
    visibility = ["PUBLIC"],
)
```

- [ ] **Step 5: Build, including on RE**

```bash
buck2 build //src/services/ingest:ingest > /tmp/b.log 2>&1; tail -5 /tmp/b.log
# RE check (per the verify-native-deps-on-RE rule): force remote-preferred build.
BUCK_PREFER_REMOTE=true buck2 build //src/services/ingest:ingest > /tmp/bre.log 2>&1; tail -5 /tmp/bre.log
```

Expected: both succeed. If the RE build fails with a link/native error, a transitive `*-sys` crate slipped in — inspect `third-party/BUCK` for `links = ...` crates and re-check the `parquet` features.

- [ ] **Step 6: Commit**

```bash
buck2 run //tools:prek -- run --all-files   # apply any hook fixups
git add src/services/ingest Cargo.lock Cargo.toml third-party/BUCK
git commit -m "$(cat <<'EOF'
build(ingest): scaffold the ingest crate + import parquet/object_store (C-free)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: `infer` — Arrow schema → DuckLake columns

**Files:**
- Modify: `src/services/ingest/src/infer.rs`
- Modify: `src/services/ingest/src/lib.rs` (it already declares `pub mod infer;`)
- Test: `src/services/ingest/tests/infer.rs`
- Modify: `src/services/ingest/BUCK` (add the test target)

- [ ] **Step 1: Write the failing test** (`tests/infer.rs`)

```rust
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use ingest::infer::{InferError, duck_type, infer_columns};

#[test]
fn maps_supported_arrow_types_to_ducklake_strings() {
    assert_eq!(duck_type(&DataType::Int64), Some("int64"));
    assert_eq!(duck_type(&DataType::Utf8), Some("varchar"));
    assert_eq!(duck_type(&DataType::LargeUtf8), Some("varchar"));
    assert_eq!(duck_type(&DataType::Boolean), Some("boolean"));
    assert_eq!(duck_type(&DataType::Float64), Some("double"));
    assert_eq!(duck_type(&DataType::Int32), Some("int32"));
}

#[test]
fn infer_columns_carries_name_and_nullability_in_order() {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]);
    let cols = infer_columns(&schema).unwrap();
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0].name, "id");
    assert_eq!(cols[0].ty, "int64");
    assert!(!cols[0].nullable);
    assert_eq!(cols[1].name, "name");
    assert_eq!(cols[1].ty, "varchar");
    assert!(cols[1].nullable);
    let _ = Arc::new(schema); // schema is cheap to clone; Arc here only to exercise import
}

#[test]
fn unsupported_arrow_type_is_an_error_not_a_guess() {
    let schema = Schema::new(vec![Field::new(
        "blob",
        DataType::Binary,
        false,
    )]);
    match infer_columns(&schema) {
        Err(InferError::Unsupported(dt)) => assert_eq!(dt, DataType::Binary),
        other => panic!("expected Unsupported, got {other:?}"),
    }
}
```

- [ ] **Step 2: Add the BUCK test target and run to verify it fails to compile**

Add to `src/services/ingest/BUCK`:

```python
rust_test(
    name = "infer",
    crate = "infer",
    srcs = ["tests/infer.rs"],
    crate_root = "tests/infer.rs",
    edition = "2024",
    deps = [":ingest", "//third-party:arrow-58"],
)
```

```bash
buck2 test //src/services/ingest:infer > /tmp/t.log 2>&1; grep -E "error\[|Tests finished|FAIL|cannot find" /tmp/t.log
```

Expected: FAIL — `duck_type` / `infer_columns` / `InferError` not found.

- [ ] **Step 3: Implement `infer.rs`**

```rust
//! Arrow `DataType` -> DuckLake type string. Used only on the un-modeled landing
//! path; when a model is supplied, the model's column types win (see materialize).

use arrow::datatypes::{DataType, Schema};
use control_plane_core::ColumnSpec;

#[derive(Debug, thiserror::Error)]
pub enum InferError {
    #[error("unsupported arrow type for ingest: {0:?}")]
    Unsupported(DataType),
}

/// The DuckLake type string for an Arrow type, or `None` if loom does not yet
/// land that type. Kept deliberately small (YAGNI) — extend as real data needs it.
pub fn duck_type(dt: &DataType) -> Option<&'static str> {
    match dt {
        DataType::Boolean => Some("boolean"),
        DataType::Int32 => Some("int32"),
        DataType::Int64 => Some("int64"),
        DataType::Float64 => Some("double"),
        DataType::Utf8 | DataType::LargeUtf8 => Some("varchar"),
        _ => None,
    }
}

/// Infer DuckLake `ColumnSpec`s from an Arrow schema, in field order. Errors on the
/// first unsupported type rather than guessing.
pub fn infer_columns(schema: &Schema) -> Result<Vec<ColumnSpec>, InferError> {
    schema
        .fields()
        .iter()
        .map(|f| {
            let ty = duck_type(f.data_type())
                .ok_or_else(|| InferError::Unsupported(f.data_type().clone()))?;
            Ok(ColumnSpec {
                name: f.name().clone(),
                ty: ty.to_string(),
                nullable: f.is_nullable(),
            })
        })
        .collect()
}
```

- [ ] **Step 4: Run the tests**

```bash
buck2 run //tools:rustfmt -- src/services/ingest/src/infer.rs src/services/ingest/tests/infer.rs
buck2 test //src/services/ingest:infer > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```

Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/services/ingest
git commit -m "$(cat <<'EOF'
feat(ingest): infer DuckLake columns from an Arrow schema

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: `gate` — optional model-conformance

**Files:**
- Create: `src/services/ingest/src/gate.rs`
- Modify: `src/services/ingest/src/lib.rs` (add `pub mod gate;`)
- Test: `src/services/ingest/tests/gate.rs`
- Modify: `src/services/ingest/BUCK`

- [ ] **Step 1: Write the failing test** (`tests/gate.rs`)

```rust
use arrow::datatypes::{DataType, Field, Schema};
use ingest::gate::{ColumnShape, ModelShape, ViolationReason, validate};

fn customer_shape() -> ModelShape {
    ModelShape {
        columns: vec![
            ColumnShape { name: "id".into(), ty: "int64".into(), required: true },
            ColumnShape { name: "email".into(), ty: "varchar".into(), required: true },
        ],
    }
}

#[test]
fn conforming_batch_passes() {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("email", DataType::Utf8, true),
    ]);
    assert!(validate(&customer_shape(), &schema).is_ok());
}

#[test]
fn missing_required_column_is_a_violation() {
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let v = validate(&customer_shape(), &schema).unwrap_err();
    assert!(v.iter().any(|x| x.column == "email"
        && matches!(x.reason, ViolationReason::MissingRequired)));
}

#[test]
fn type_mismatch_is_a_violation() {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Utf8, false), // wrong: varchar, model wants int64
        Field::new("email", DataType::Utf8, true),
    ]);
    let v = validate(&customer_shape(), &schema).unwrap_err();
    assert!(v.iter().any(|x| x.column == "id"
        && matches!(x.reason, ViolationReason::TypeMismatch { .. })));
}
```

- [ ] **Step 2: Add BUCK target, run to verify it fails**

```python
rust_test(
    name = "gate",
    crate = "gate",
    srcs = ["tests/gate.rs"],
    crate_root = "tests/gate.rs",
    edition = "2024",
    deps = [":ingest", "//third-party:arrow-58"],
)
```

```bash
buck2 test //src/services/ingest:gate > /tmp/t.log 2>&1; grep -E "error\[|cannot find|Tests finished|FAIL" /tmp/t.log
```

Expected: FAIL — `gate` symbols not found.

- [ ] **Step 3: Implement `gate.rs`**

```rust
//! The optional model-conformance gate ("this data is this model"). Takes a plain
//! `ModelShape` value — NOT the ontology — so this crate stays ontology-free; a
//! later slice derives a `ModelShape` from an `ObjectType`. This slice ships the
//! seam plus a minimal check (required columns present, types match). Richer
//! constraints (ranges, regex, coercion) extend `ViolationReason`.

use arrow::datatypes::Schema;

use crate::infer::duck_type;

/// One expected column of a model. `ty` is a DuckLake type string.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnShape {
    pub name: String,
    pub ty: String,
    pub required: bool,
}

/// The physical shape a batch must satisfy to be "this model".
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelShape {
    pub columns: Vec<ColumnShape>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Violation {
    pub column: String,
    pub reason: ViolationReason,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ViolationReason {
    MissingRequired,
    /// The column is present but its inferred DuckLake type does not match the model.
    TypeMismatch { expected: String, found: String },
    /// The column has an Arrow type loom cannot land at all.
    Unsupported,
}

/// Validate a batch schema against a model. `Ok(())` if every required column is
/// present and every present model column's inferred type matches. Returns ALL
/// violations (not just the first) so callers can report them together.
pub fn validate(shape: &ModelShape, batch: &Schema) -> Result<(), Vec<Violation>> {
    let mut violations = Vec::new();
    for col in &shape.columns {
        match batch.fields().iter().find(|f| f.name() == &col.name) {
            None => {
                if col.required {
                    violations.push(Violation {
                        column: col.name.clone(),
                        reason: ViolationReason::MissingRequired,
                    });
                }
            }
            Some(field) => match duck_type(field.data_type()) {
                None => violations.push(Violation {
                    column: col.name.clone(),
                    reason: ViolationReason::Unsupported,
                }),
                Some(found) if found != col.ty => violations.push(Violation {
                    column: col.name.clone(),
                    reason: ViolationReason::TypeMismatch {
                        expected: col.ty.clone(),
                        found: found.to_string(),
                    },
                }),
                Some(_) => {}
            },
        }
    }
    if violations.is_empty() {
        Ok(())
    } else {
        Err(violations)
    }
}
```

Add `pub mod gate;` to `src/lib.rs`.

- [ ] **Step 4: Run the tests**

```bash
buck2 run //tools:rustfmt -- src/services/ingest/src/gate.rs src/services/ingest/tests/gate.rs src/services/ingest/src/lib.rs
buck2 test //src/services/ingest:gate > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```

Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/services/ingest
git commit -m "$(cat <<'EOF'
feat(ingest): optional model-conformance gate (ModelShape validation)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: `write` — Parquet + stats extraction (load-bearing)

This is the fidelity-critical unit. It writes Snappy Parquet from Arrow batches and extracts exactly the `DataFile` metadata `append_files` needs. Note: verify the exact `parquet` API names against the version reindeer resolved in Task 1 — the symbols below match `parquet 58`, but `statistics()` accessor names have churned across versions (`min_opt`/`max_opt`/`null_count_opt`). If a name differs, the compiler will point you at it; the *shape* (re-read metadata, sum per-column null_count + compressed size, best-effort min/max) is what matters.

**Files:**
- Create: `src/services/ingest/src/write.rs`
- Modify: `src/services/ingest/src/lib.rs` (add `pub mod write;`)
- Test: `src/services/ingest/tests/write.rs`
- Modify: `src/services/ingest/BUCK`

- [ ] **Step 1: Write the failing test** (`tests/write.rs`)

```rust
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use ingest::write::write_parquet;

fn sample() -> (Arc<Schema>, RecordBatch) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
        ],
    )
    .unwrap();
    (schema, batch)
}

#[test]
fn writes_parquet_and_extracts_load_bearing_stats() {
    let (schema, batch) = sample();
    let w = write_parquet(schema, &[batch]).unwrap();

    // It is a real parquet file: PAR1 magic + a plausible footer length.
    assert_eq!(&w.bytes[w.bytes.len() - 4..], b"PAR1");
    assert_eq!(w.file_size_bytes, w.bytes.len() as i64);
    assert!(w.footer_size > 0 && w.footer_size < w.file_size_bytes);
    assert_eq!(w.record_count, 3);

    // Per-column stats, in schema order.
    assert_eq!(w.column_stats.len(), 2);
    let id = &w.column_stats[0];
    assert_eq!(id.column_name, "id");
    assert_eq!(id.null_count, 0);
    assert_eq!(id.value_count, 3);
    assert!(id.column_size_bytes > 0);

    let name = &w.column_stats[1];
    assert_eq!(name.column_name, "name");
    assert_eq!(name.null_count, 1); // the None
    assert_eq!(name.value_count, 2);
}
```

- [ ] **Step 2: Add BUCK target, run to verify it fails**

```python
rust_test(
    name = "write",
    crate = "write",
    srcs = ["tests/write.rs"],
    crate_root = "tests/write.rs",
    edition = "2024",
    deps = [":ingest", "//third-party:arrow-58"],
)
```

```bash
buck2 test //src/services/ingest:write > /tmp/t.log 2>&1; grep -E "error\[|cannot find|Tests finished|FAIL" /tmp/t.log
```

Expected: FAIL — `write_parquet` not found.

- [ ] **Step 3: Implement `write.rs`**

```rust
//! Arrow batches -> Snappy Parquet bytes + the DuckLake `DataFile` stats the
//! snapshot-commit primitive needs. The load-bearing fidelity unit: the DuckDB
//! read-back interop test (tests/ducklake_interop.rs) is its executable oracle.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use bytes::Bytes;
use control_plane_core::ColumnStat;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::statistics::Statistics;

#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    #[error("parquet write failed: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
}

/// A written Parquet file plus the metadata `append_files` registers.
#[derive(Clone, Debug)]
pub struct WrittenParquet {
    pub bytes: Vec<u8>,
    pub record_count: i64,
    pub file_size_bytes: i64,
    pub footer_size: i64,
    pub column_stats: Vec<ColumnStat>,
}

/// Write `batches` as one Snappy Parquet file and extract its DuckLake stats.
pub fn write_parquet(
    schema: Arc<Schema>,
    batches: &[RecordBatch],
) -> Result<WrittenParquet, WriteError> {
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut buf: Vec<u8> = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), Some(props))?;
    for b in batches {
        writer.write(b)?;
    }
    writer.close()?; // flushes the footer

    let file_size_bytes = buf.len() as i64;
    let footer_size = parquet_footer_size(&buf);

    // Re-read metadata to get typed, aggregated per-column statistics.
    let reader = SerializedFileReader::new(Bytes::from(buf.clone()))?;
    let meta = reader.metadata();
    let record_count: i64 = meta.file_metadata().num_rows();
    let single_rg = meta.num_row_groups() == 1;

    let mut column_stats = Vec::with_capacity(schema.fields().len());
    for (i, field) in schema.fields().iter().enumerate() {
        let mut null_count: i64 = 0;
        let mut column_size_bytes: i64 = 0;
        let mut min: Option<String> = None;
        let mut max: Option<String> = None;
        for rg in meta.row_groups() {
            let col = rg.column(i);
            column_size_bytes += col.compressed_size();
            if let Some(stats) = col.statistics() {
                null_count += stats.null_count_opt().unwrap_or(0) as i64;
                // min/max are best-effort: a pruning hint, not required for reads.
                // Only populate for a single row group to avoid type-aware merging.
                if single_rg {
                    min = stat_min_string(stats);
                    max = stat_max_string(stats);
                }
            }
        }
        column_stats.push(ColumnStat {
            column_name: field.name().clone(),
            min,
            max,
            null_count,
            value_count: record_count - null_count,
            column_size_bytes,
        });
    }

    Ok(WrittenParquet {
        bytes: buf,
        record_count,
        file_size_bytes,
        footer_size,
        column_stats,
    })
}

/// The 4 bytes before the trailing `PAR1` magic are the little-endian footer
/// length DuckLake records as `footer_size` (an I/O hint for the metadata read).
fn parquet_footer_size(bytes: &[u8]) -> i64 {
    debug_assert!(bytes.len() >= 8 && &bytes[bytes.len() - 4..] == b"PAR1");
    let len = &bytes[bytes.len() - 8..bytes.len() - 4];
    u32::from_le_bytes(len.try_into().unwrap()) as i64
}

fn stat_min_string(stats: &Statistics) -> Option<String> {
    match stats {
        Statistics::Boolean(s) => s.min_opt().map(|v| v.to_string()),
        Statistics::Int32(s) => s.min_opt().map(|v| v.to_string()),
        Statistics::Int64(s) => s.min_opt().map(|v| v.to_string()),
        Statistics::Double(s) => s.min_opt().map(|v| v.to_string()),
        Statistics::ByteArray(s) => s
            .min_opt()
            .and_then(|v| v.as_utf8().ok().map(|s| s.to_string())),
        _ => None,
    }
}

fn stat_max_string(stats: &Statistics) -> Option<String> {
    match stats {
        Statistics::Boolean(s) => s.max_opt().map(|v| v.to_string()),
        Statistics::Int32(s) => s.max_opt().map(|v| v.to_string()),
        Statistics::Int64(s) => s.max_opt().map(|v| v.to_string()),
        Statistics::Double(s) => s.max_opt().map(|v| v.to_string()),
        Statistics::ByteArray(s) => s
            .max_opt()
            .and_then(|v| v.as_utf8().ok().map(|s| s.to_string())),
        _ => None,
    }
}
```

Add `pub mod write;` to `src/lib.rs`. The `write` unit needs `bytes` — it comes in transitively via `object_store`/`parquet`; if the crate name isn't directly resolvable, add `bytes = "1"` to `[dependencies]` and re-buckify, then add `//third-party:bytes` to the `ingest` library deps.

- [ ] **Step 4: Run the tests**

```bash
buck2 run //tools:rustfmt -- src/services/ingest/src/write.rs src/services/ingest/tests/write.rs src/services/ingest/src/lib.rs
buck2 test //src/services/ingest:write > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```

Expected: PASS. If `null_count_opt`/`min_opt`/`as_utf8` names differ in the resolved parquet version, fix per the compiler and re-run.

- [ ] **Step 5: Commit**

```bash
git add src/services/ingest
git commit -m "$(cat <<'EOF'
feat(ingest): write Snappy Parquet + extract DuckLake DataFile stats

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: `store` — object-store put

**Files:**
- Create: `src/services/ingest/src/store.rs`
- Modify: `src/services/ingest/src/lib.rs` (add `pub mod store;`)
- Test: `src/services/ingest/tests/store.rs`
- Modify: `src/services/ingest/BUCK`

- [ ] **Step 1: Write the failing test** (`tests/store.rs`)

```rust
use ingest::store::put;
use object_store::local::LocalFileSystem;

#[tokio::test]
async fn put_writes_under_the_schema_table_key() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();

    let stored = put(&store, "main/t/loom.parquet", b"hello".to_vec())
        .await
        .unwrap();

    // The registered DataFile.path is the filename only (DuckLake resolves
    // data_path + schema.path + table.path + file.path).
    assert_eq!(stored.path, "loom.parquet");
    assert!(stored.path_is_relative);

    // The bytes landed at <data_path>/main/t/loom.parquet.
    let on_disk = std::fs::read(dir.path().join("main").join("t").join("loom.parquet")).unwrap();
    assert_eq!(on_disk, b"hello");
}
```

- [ ] **Step 2: Add BUCK target, run to verify it fails**

```python
rust_test(
    name = "store",
    crate = "store",
    srcs = ["tests/store.rs"],
    crate_root = "tests/store.rs",
    edition = "2024",
    deps = [":ingest", "//third-party:object_store", "//third-party:tokio", "//third-party:tempfile"],
)
```

```bash
buck2 test //src/services/ingest:store > /tmp/t.log 2>&1; grep -E "error\[|cannot find|Tests finished|FAIL" /tmp/t.log
```

Expected: FAIL — `put` not found.

- [ ] **Step 3: Implement `store.rs`**

```rust
//! Object-store put. `LocalFileSystem` for now (hermetic, no creds); S3 (the
//! object_store `aws` feature) is a later slice. The store is rooted at the
//! catalog's data_path; the key is "<schema>/<table>/<file_name>" so DuckLake's
//! relative-path resolution (data_path + schema.path + table.path + file.path)
//! finds the file. The registered DataFile.path is the file_name alone.

use object_store::ObjectStore;
use object_store::path::Path as ObjectPath;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("object store put failed: {0}")]
    Put(#[from] object_store::Error),
}

/// What the caller registers in `append_files`: the file name and its
/// relative-resolution flag.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredPath {
    pub path: String,
    pub path_is_relative: bool,
}

/// Write `bytes` at `key` (e.g. "main/t/loom.parquet"). Returns the file name to
/// register (the last path segment). Intermediate directories are created by the
/// local store on put.
pub async fn put(
    store: &dyn ObjectStore,
    key: &str,
    bytes: Vec<u8>,
) -> Result<StoredPath, StoreError> {
    store.put(&ObjectPath::from(key), bytes.into()).await?;
    let file_name = key.rsplit('/').next().unwrap_or(key).to_string();
    Ok(StoredPath {
        path: file_name,
        path_is_relative: true,
    })
}
```

Add `pub mod store;` to `src/lib.rs`. The `ingest` library now depends on `object_store` (already in Task 1's BUCK deps).

- [ ] **Step 4: Run the tests**

```bash
buck2 run //tools:rustfmt -- src/services/ingest/src/store.rs src/services/ingest/tests/store.rs src/services/ingest/src/lib.rs
buck2 test //src/services/ingest:store > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```

Expected: PASS. (`bytes.into()` builds an `object_store::PutPayload`; if the resolved object_store version names it differently, adjust per the compiler.)

- [ ] **Step 5: Commit**

```bash
git add src/services/ingest
git commit -m "$(cat <<'EOF'
feat(ingest): object-store put (local) with DuckLake relative-path keying

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: `materialize` — the orchestrator

Ties the units together and owns the single atomic transaction. Fast orchestration tests use `MemoryControlPlane` (no postgres); the DuckDB fidelity proof is Task 7.

**Files:**
- Create: `src/services/ingest/src/materialize.rs`
- Modify: `src/services/ingest/src/lib.rs` (add `pub mod materialize;` + re-exports + `IngestError`)
- Test: `src/services/ingest/tests/materialize.rs`
- Modify: `src/services/ingest/BUCK`

- [ ] **Step 1: Write the failing test** (`tests/materialize.rs`)

```rust
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{
    ControlPlane, DatasetRef, EventType, LineageEvent, RunId, TableRef,
};
use control_plane_memory::MemoryControlPlane;
use ingest::gate::{ColumnShape, ModelShape};
use ingest::{IngestError, MaterializeRequest, materialize};
use object_store::local::LocalFileSystem;
use time::OffsetDateTime;
use uuid::Uuid;

fn table() -> TableRef {
    TableRef { schema: "main".into(), name: "customer".into() }
}

fn lineage(t: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetRef { namespace: "loom-ingest".into(), name: format!("{}.{}", t.schema, t.name) }],
        payload: serde_json::json!({}),
    }
}

fn batch() -> (Arc<Schema>, RecordBatch) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("email", DataType::Utf8, true),
    ]));
    let b = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("a@x"), Some("b@x")])),
        ],
    )
    .unwrap();
    (schema, b)
}

#[tokio::test]
async fn unmodeled_landing_returns_a_snapshot() {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let (schema, b) = batch();
    let t = table();

    let snap = materialize(
        &cp,
        &store,
        MaterializeRequest {
            table: &t,
            schema,
            batches: &[b],
            file_name: "part-0.parquet",
            gate: None,
            lineage: lineage(&t),
        },
    )
    .await
    .unwrap();

    assert!(snap.0 >= 0);
    // The parquet landed under the schema/table key.
    assert!(dir.path().join("main").join("customer").join("part-0.parquet").exists());
}

#[tokio::test]
async fn gate_rejection_happens_before_any_write() {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let (schema, b) = batch();
    let t = table();

    // Model requires a column the batch lacks.
    let shape = ModelShape {
        columns: vec![
            ColumnShape { name: "id".into(), ty: "int64".into(), required: true },
            ColumnShape { name: "ssn".into(), ty: "varchar".into(), required: true },
        ],
    };

    let err = materialize(
        &cp,
        &store,
        MaterializeRequest {
            table: &t,
            schema,
            batches: &[b],
            file_name: "part-0.parquet",
            gate: Some(&shape),
            lineage: lineage(&t),
        },
    )
    .await
    .unwrap_err();

    assert!(matches!(err, IngestError::DoesNotConform(_)));
    // Nothing was written — the gate is the front edge.
    assert!(!dir.path().join("main").join("customer").join("part-0.parquet").exists());
}
```

- [ ] **Step 2: Add BUCK target, run to verify it fails**

```python
rust_test(
    name = "materialize",
    crate = "materialize",
    srcs = ["tests/materialize.rs"],
    crate_root = "tests/materialize.rs",
    edition = "2024",
    deps = [
        ":ingest",
        "//src/control-plane/core:core",
        "//src/control-plane/memory:memory",
        "//third-party:arrow-58",
        "//third-party:object_store",
        "//third-party:serde_json",
        "//third-party:tempfile",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```

```bash
buck2 test //src/services/ingest:materialize > /tmp/t.log 2>&1; grep -E "error\[|cannot find|Tests finished|FAIL" /tmp/t.log
```

Expected: FAIL — `materialize`/`MaterializeRequest`/`IngestError` not found.

- [ ] **Step 3: Implement `materialize.rs`**

```rust
//! The orchestrator: gate -> schema-selection -> write -> put -> one atomic Tx
//! (create_table + append_files + emit + commit). Ordering is put-then-commit;
//! a commit failure after put orphans the Parquet (documented; GC is deferred).

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use control_plane_core::{ColumnSpec, ControlPlane, DataFile, LineageEvent, SnapshotId, TableRef};
use object_store::ObjectStore;

use crate::IngestError;
use crate::gate::{ModelShape, validate};
use crate::infer::infer_columns;
use crate::store::put;
use crate::write::write_parquet;

/// One materialize call: land `batches` for `table`, optionally gated by a model.
pub struct MaterializeRequest<'a> {
    pub table: &'a TableRef,
    pub schema: Arc<Schema>,
    pub batches: &'a [RecordBatch],
    /// Caller-unique file name, e.g. "part-0.parquet".
    pub file_name: &'a str,
    pub gate: Option<&'a ModelShape>,
    /// The lineage event to emit in the same transaction (output dataset = the
    /// landed table). Built by the caller, which knows the datasource namespace.
    pub lineage: LineageEvent,
}

/// Land data as a registered DuckLake snapshot + lineage, atomically.
pub async fn materialize(
    cp: &dyn ControlPlane,
    object_store: &dyn ObjectStore,
    req: MaterializeRequest<'_>,
) -> Result<SnapshotId, IngestError> {
    // 1. Optional model gate — the front edge; reject before any write.
    if let Some(shape) = req.gate {
        validate(shape, &req.schema).map_err(IngestError::DoesNotConform)?;
    }

    // 2. Physical schema: the model wins when supplied; otherwise infer.
    let columns: Vec<ColumnSpec> = match req.gate {
        Some(shape) => shape
            .columns
            .iter()
            .map(|c| ColumnSpec { name: c.name.clone(), ty: c.ty.clone(), nullable: !c.required })
            .collect(),
        None => infer_columns(&req.schema)?,
    };

    // 3. Write Parquet + extract stats.
    let written = write_parquet(req.schema.clone(), req.batches)?;

    // 4. Put to object storage (key = schema/table/file).
    let key = format!("{}/{}/{}", req.table.schema, req.table.name, req.file_name);
    let stored = put(object_store, &key, written.bytes).await?;

    // 5. One atomic transaction: create_table (idempotent) + append_files + emit.
    let mut tx = cp.begin().await?;
    tx.create_table(req.table, &columns).await?;
    tx.append_files(
        req.table,
        &[DataFile {
            path: stored.path,
            path_is_relative: stored.path_is_relative,
            record_count: written.record_count,
            file_size_bytes: written.file_size_bytes,
            footer_size: written.footer_size,
            column_stats: written.column_stats,
        }],
    )
    .await?;
    tx.emit(req.lineage).await?;
    tx.commit()
        .await?
        .ok_or(IngestError::NoSnapshot)
}
```

> The `?` operators rely on `IngestError`'s `#[from]` conversions (wired in Step 4) for `InferError`, `WriteError`, `StoreError`, and `ControlPlaneError`; `validate`'s `Vec<Violation>` is mapped explicitly to `DoesNotConform`. No other imports are needed here.

- [ ] **Step 4: Wire `IngestError` + public surface in `src/lib.rs`**

```rust
//! loom ingest: the landing-edge materializer. Arrow batches -> registered
//! DuckLake snapshot + lineage via the part-1 snapshot-commit primitive, with an
//! optional model-conformance gate. See the spec under docs/superpowers/specs/.

pub mod gate;
pub mod infer;
pub mod materialize;
pub mod store;
pub mod write;

pub use gate::{ColumnShape, ModelShape, Violation, ViolationReason};
pub use materialize::{MaterializeRequest, materialize};

/// Everything that can go wrong landing data. Fail-fast: a failure before
/// `commit` leaves no catalog rows (commit is never reached).
#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    /// The batch did not satisfy the supplied model (rejected before any write).
    #[error("data does not conform to model: {} violation(s)", .0.len())]
    DoesNotConform(Vec<Violation>),
    #[error(transparent)]
    Infer(#[from] infer::InferError),
    #[error(transparent)]
    Write(#[from] write::WriteError),
    #[error(transparent)]
    Store(#[from] store::StoreError),
    #[error(transparent)]
    ControlPlane(#[from] control_plane_core::ControlPlaneError),
    /// commit() returned None — a catalog op was staged yet no snapshot was
    /// produced. Indicates an adapter contract violation; surfaced, never ignored.
    #[error("commit produced no snapshot id")]
    NoSnapshot,
}
```

- [ ] **Step 5: Run the tests**

```bash
buck2 run //tools:rustfmt -- src/services/ingest/src/materialize.rs src/services/ingest/src/lib.rs src/services/ingest/tests/materialize.rs
buck2 test //src/services/ingest:materialize > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```

Expected: PASS (2 tests).

- [ ] **Step 6: Clippy + commit**

```bash
./tools/clippy-all.sh 2>&1 | grep -iE "ingest|warning|error" | head
git add src/services/ingest
git commit -m "$(cat <<'EOF'
feat(ingest): materialize orchestrator (gate -> write -> put -> atomic commit)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: DuckDB read-back interop guardrail (make-or-break)

The executable oracle: loom materializes data, then the **pinned DuckDB** `ATTACH`es the catalog and reads it back — and appends its own snapshot on top. If loom's Parquet or `ducklake_*` rows diverge, DuckDB rejects them here. Mirrors `src/control-plane/postgres/tests/ducklake_interop.rs`, but loom writes the Parquet (not DuckDB).

**Files:**
- Create: `src/services/ingest/tests/ducklake_interop.rs`
- Modify: `src/services/ingest/BUCK` (a `loom_fixture_test(duckdb = True)` target; add the `load(...)` line at the top of the file)

- [ ] **Step 1: Write the failing test** (`tests/ducklake_interop.rs`)

```rust
//! Interop guardrail: the pinned DuckDB engine READS data that loom's materializer
//! wrote (Parquet + ducklake_* rows), and BUILDS ON IT (appends its own snapshot).
//! If loom's output diverges from what DuckDB expects, DuckDB rejects it here.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{DatasetRef, EventType, LineageEvent, RunId, TableRef};
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use ingest::{MaterializeRequest, materialize};
use object_store::local::LocalFileSystem;
use time::OffsetDateTime;
use uuid::Uuid;

#[tokio::test]
async fn duckdb_reads_loom_materialized_data_and_appends() {
    let fixture = PgFixture::start();
    let (cp, db) = fixture.fresh_db().await;
    let writer = DuckLakeWriter::new(fixture.socket_path(), &db);
    writer.bootstrap().await;

    let t = TableRef { schema: "main".into(), name: "customer".into() };
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("email", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("a@x"), Some("b@x")])),
        ],
    )
    .unwrap();

    // The object store is rooted at the catalog's data_path, so loom's key
    // "main/customer/loom.parquet" lands where DuckLake resolves it.
    let store = LocalFileSystem::new_with_prefix(writer.data_path()).unwrap();

    let lineage = LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetRef { namespace: "loom-ingest".into(), name: "main.customer".into() }],
        payload: serde_json::json!({}),
    };

    let loom_snap = materialize(
        &cp,
        &store,
        MaterializeRequest {
            table: &t,
            schema,
            batches: &[batch],
            file_name: "loom.parquet",
            gate: None,
            lineage,
        },
    )
    .await
    .unwrap()
    .0;

    // THE GUARDRAIL: DuckDB scans loom's materialized Parquet from loom's catalog.
    let count = writer.query_scalar("SELECT count(*) FROM lake.main.customer;").await;
    assert_eq!(count, "2", "DuckDB must scan loom's materialized file");
    let emails = writer
        .query_scalar("SELECT string_agg(email, ',' ORDER BY id) FROM lake.main.customer;")
        .await;
    assert_eq!(emails, "a@x,b@x");

    // DuckDB appends its own row on top — proves counters/versioning are correct.
    writer.exec("INSERT INTO lake.main.customer VALUES (3, 'c@x');").await;
    let count2 = writer.query_scalar("SELECT count(*) FROM lake.main.customer;").await;
    assert_eq!(count2, "3");
    let max_snap = writer.max_snapshot_id().await;
    assert_eq!(max_snap, loom_snap + 1, "DuckDB's snapshot must sit atop loom's");
}
```

- [ ] **Step 2: Add the fixture-test target**

At the **top** of `src/services/ingest/BUCK` (above the `rust_library`), add:

```python
load("//src/control-plane/postgres:defs.bzl", "loom_fixture_test")
```

Then add the target:

```python
loom_fixture_test(
    name = "ducklake-interop",
    crate = "ducklake_interop",
    srcs = ["tests/ducklake_interop.rs"],
    crate_root = "tests/ducklake_interop.rs",
    duckdb = True,
    deps = [
        ":ingest",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:arrow-58",
        "//third-party:object_store",
        "//third-party:serde_json",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```

- [ ] **Step 3: Run to verify it fails first (compile), then passes**

```bash
buck2 test //src/services/ingest:ducklake-interop > /tmp/t.log 2>&1; grep -E "error\[|Tests finished|FAIL" /tmp/t.log
```

Expected first run: PASS once it compiles. If DuckDB errors reading the file (e.g. footer/stats mismatch), that is the guardrail doing its job — debug `write.rs` against `2026-06-09-ducklake-single-catalog-write-recipe.md`. Common culprits: `footer_size` off by the magic length, or `column_size_bytes`/`value_count` not matching DuckLake's expectation. The `string_agg` assertion confirms row *values* survive the round-trip, not just the count.

- [ ] **Step 4: Commit**

```bash
buck2 run //tools:rustfmt -- src/services/ingest/tests/ducklake_interop.rs
git add src/services/ingest
git commit -m "$(cat <<'EOF'
test(ingest): DuckDB reads back loom-materialized data (interop guardrail)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

## Task 8: Full-suite verification + spec/doc reconciliation

- [ ] **Step 1: Run the whole first-party suite**

```bash
buck2 test //src/... > /tmp/all.log 2>&1; grep -E "Tests finished|FAIL" /tmp/all.log
```

Expected: all green, including the new `ingest` targets. (The atomicity guarantee at commit is part-1's `snapshot_rollback` conformance; the ingest layer's fail-before-commit behavior is covered by `gate_rejection_happens_before_any_write` in Task 6 — together they show no partial catalog state.)

- [ ] **Step 2: Lint + clippy across the tree**

```bash
./tools/clippy-all.sh 2>&1 | grep -iE "warning|error" | head
buck2 run //tools:prek -- run --all-files > /tmp/prek.log 2>&1; grep -iE "Failed|Passed|error" /tmp/prek.log | tail -20
```

Expected: clippy clean; prek all green (incl. `reindeer-check` now that the new deps are committed, and `no-inline-tests`). Commit any hook fixups.

- [ ] **Step 3: Confirm the RE build of the new third-party deps**

```bash
BUCK_PREFER_REMOTE=true buck2 build //src/services/ingest:ingest > /tmp/re.log 2>&1; tail -3 /tmp/re.log
```

Expected: success (no native-dep materialization failure). This is the standing "verify native deps on RE before merge" check.

- [ ] **Step 4: Update the roadmap "Where we are"**

The roadmap (`docs/superpowers/specs/2026-06-06-loom-roadmap.md`) lists "the ingest service shell" under *Ingest → Later*. Add a line under the Ingest bullet recording that **part 2a (the landing materializer)** is delivered, and that the **dataset→model binding** and the **network endpoint + DataFusion** remain. Keep edits to that one bullet; do not rewrite the doc.

- [ ] **Step 5: Commit the doc update**

```bash
buck2 run //tools:prek -- run --all-files
git add docs/superpowers/specs/2026-06-06-loom-roadmap.md
git commit -m "$(cat <<'EOF'
docs(roadmap): ingest part 2a (landing materializer) delivered

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

## Self-review notes (for the executor)

- **Spec coverage:** `infer`=Task 2; `gate`=Task 3; `write`+stats=Task 4; `store`=Task 5; `materialize`+authoritative-schema rule=Task 6; DuckDB interop guardrail=Task 7; atomicity (fail-before-commit) + RE build + C-free deps=Tasks 6/8/1. The deferred dataset→model binding and network endpoint are explicitly out of scope and untouched.
- **Type consistency:** `MaterializeRequest`/`materialize`/`IngestError`/`ModelShape`/`ColumnShape`/`Violation`/`WrittenParquet`/`StoredPath` are defined once and used consistently across tasks. `ColumnStat`/`DataFile`/`ColumnSpec`/`TableRef`/`LineageEvent` come from `control_plane_core` verbatim.
- **Known soft spots flagged in-task:** exact `parquet` accessor names may differ by resolved version (Task 4 note); `bytes`/`PutPayload`/object_store version naming (Tasks 4/5 notes). None are placeholders — each has a concrete resolution path.
```
