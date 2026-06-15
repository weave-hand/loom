# Typed Transforms — Part 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Object-Model-typed transforms (`Type(s) → Type`) that resolve input ontology types to their DuckLake tables, run type-name SQL, validate the result exactly conforms to the output type's property contract, and commit a new snapshot with first-class type-named lineage.

**Architecture:** A thin typed primitive (`run_typed_transform`) resolves types and delegates to the existing physical `run_transform`, which gains two additive seams — a `register_as` input-name mapping (so SQL reads in type terms) and an optional conformance contract checked between schema-inference and write. A new `"typed-transform"` job kind + handler routes to it; the worker binary dispatches both kinds. Core gains a `TypeId` lineage identity so provenance nodes are the ontology types.

**Tech Stack:** Rust 2024, buck2, DataFusion 54, DuckLake/DuckDB, Postgres control plane, `control-plane-worker` queue loop. Tests are `rust_test` integration targets (pure logic) and `loom_fixture_test` (Postgres+DuckDB) — never inline `#[cfg(test)]`.

**Spec:** `docs/superpowers/specs/2026-06-15-typed-transforms-part1-design.md`

---

## File Structure

| File | Responsibility | Action |
|------|----------------|--------|
| `src/control-plane/core/src/identity.rs` | Add `TypeId`/`LOOM_TYPE_NAMESPACE`/`From<&TypeName>` — type→`DatasetRef` lineage identity | Modify |
| `src/control-plane/core/src/lib.rs` | Export the new identity symbols | Modify |
| `src/control-plane/core/tests/identity.rs` | Type-identity round-trip + namespace-distinctness tests | Modify |
| `src/services/transform/src/conform.rs` | Pure exact-match conformance check (`Violation`, `check_conformance`) | Create |
| `src/services/transform/tests/conform.rs` | Unit tests for `check_conformance` | Create |
| `src/services/transform/src/run.rs` | `TransformInput` + `register_as`; optional `conform` contract; `DoesNotConform` | Modify |
| `src/services/transform/src/typed.rs` | `run_typed_transform`, `TypedTransformError` | Create |
| `src/services/transform/src/handler.rs` | Physical path → `TransformInput`; `typed_transform_handler` + retry policy | Modify |
| `src/services/transform/src/main.rs` | Worker dispatch on `job.kind` over both kinds | Modify |
| `src/services/transform/src/lib.rs` | Module wiring + exports | Modify |
| `src/services/transform/tests/typed_transform_e2e.rs` | Fixture e2e: positive round-trip + negative non-conforming | Create |
| `src/services/transform/BUCK` | `conform` (pure) + `typed-transform-e2e` (fixture) test targets | Modify |
| `docs/superpowers/specs/2026-06-06-loom-roadmap.md` | Mark typed transforms part-1 delivered | Modify |

---

## Task 1: Core `TypeId` lineage identity

Adds a type-level dataset identity parallel to the existing `DatasetId(TableRef)`, so typed transforms emit type-named lineage nodes. Pure logic, no I/O.

**Files:**
- Modify: `src/control-plane/core/src/identity.rs`
- Modify: `src/control-plane/core/src/lib.rs:22`
- Test: `src/control-plane/core/tests/identity.rs`

- [ ] **Step 1: Write the failing tests**

Append to `src/control-plane/core/tests/identity.rs`. Also extend the existing top-of-file import line `use control_plane_core::{DatasetId, DatasetRef, LOOM_DATASET_NAMESPACE, TableRef};` to add `LOOM_TYPE_NAMESPACE, TypeId, TypeName`:

```rust
use control_plane_core::{
    DatasetId, DatasetRef, LOOM_DATASET_NAMESPACE, LOOM_TYPE_NAMESPACE, TableRef, TypeId, TypeName,
};
```

Append these tests:

```rust
#[test]
fn type_maps_to_loom_type_namespaced_ref() {
    let dr: DatasetRef = (&TypeName("Customer".into())).into();
    assert_eq!(
        dr,
        DatasetRef {
            namespace: "loom:type".into(),
            name: "Customer".into(),
        }
    );
    assert_eq!(LOOM_TYPE_NAMESPACE, "loom:type");
}

#[test]
fn type_dataset_ref_round_trips() {
    let ty = TypeName("Order".into());
    let dr: DatasetRef = (&ty).into();
    assert_eq!(TypeId::from_dataset_ref(&dr), Some(TypeId::from(&ty)));
}

#[test]
fn type_and_table_refs_do_not_collide() {
    // A table "main.Customer" and a type "Customer" live in different namespaces and
    // never parse as each other.
    let table_dr: DatasetRef = (&tref("main", "Customer")).into();
    let type_dr: DatasetRef = (&TypeName("Customer".into())).into();
    assert_ne!(table_dr.namespace, type_dr.namespace);
    assert_eq!(DatasetId::from_dataset_ref(&type_dr), None, "a type ref is not a table dataset");
    assert_eq!(TypeId::from_dataset_ref(&table_dr), None, "a table ref is not a type");
}

#[test]
fn external_and_empty_are_not_type_refs() {
    for (ns, nm) in [("s3://b", "Customer"), ("loom", "Customer"), ("loom:type", "")] {
        let dr = DatasetRef {
            namespace: ns.into(),
            name: nm.into(),
        };
        assert_eq!(TypeId::from_dataset_ref(&dr), None, "ns={ns} nm={nm}");
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `buck2 build //src/control-plane/core:core 2>&1 | tail -20`
Expected: FAIL to build — `TypeId` / `LOOM_TYPE_NAMESPACE` / `From<&TypeName>` do not exist yet.

- [ ] **Step 3: Implement `TypeId` in `identity.rs`**

Add `use crate::ontology::TypeName;` to the imports at the top of `src/control-plane/core/src/identity.rs` (it currently imports `crate::catalog::TableRef` and `crate::lineage::DatasetRef`). Then append:

```rust
/// loom's canonical logical namespace for ontology *types* it governs. Distinct from
/// `LOOM_DATASET_NAMESPACE` so a type "Customer" and a table "x.Customer" never collide.
pub const LOOM_TYPE_NAMESPACE: &str = "loom:type";

/// loom's canonical lineage identity for an ontology type. Parallel to [`DatasetId`]
/// (which identifies a physical table); a typed transform's provenance nodes are these.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TypeId(TypeName);

impl TypeId {
    /// The ontology type this identity refers to.
    pub fn type_name(&self) -> &TypeName {
        &self.0
    }

    /// The OpenLineage identity for this type: the loom-type namespace plus the bare
    /// type name (type names are single identifiers, not schema-qualified).
    pub fn dataset_ref(&self) -> DatasetRef {
        DatasetRef {
            namespace: LOOM_TYPE_NAMESPACE.to_string(),
            name: self.0.0.clone(),
        }
    }

    /// Parse a `DatasetRef` back into a `TypeId`. `None` when the ref is not
    /// loom-type-namespaced or its name is empty.
    pub fn from_dataset_ref(dr: &DatasetRef) -> Option<TypeId> {
        if dr.namespace != LOOM_TYPE_NAMESPACE || dr.name.is_empty() {
            return None;
        }
        Some(TypeId(TypeName(dr.name.clone())))
    }
}

impl From<&TypeName> for TypeId {
    fn from(name: &TypeName) -> Self {
        TypeId(name.clone())
    }
}

/// Convenience for call sites that just want the lineage ref for an ontology type.
impl From<&TypeName> for DatasetRef {
    fn from(name: &TypeName) -> Self {
        TypeId::from(name).dataset_ref()
    }
}
```

- [ ] **Step 4: Export the new symbols from `lib.rs`**

In `src/control-plane/core/src/lib.rs`, change line 22 from:

```rust
pub use identity::{DatasetId, LOOM_DATASET_NAMESPACE};
```

to:

```rust
pub use identity::{DatasetId, LOOM_DATASET_NAMESPACE, LOOM_TYPE_NAMESPACE, TypeId};
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `buck2 test //src/control-plane/core:identity > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/t.log`
Expected: `Tests finished: Pass <n>. Fail 0.` (all identity tests, old + new, pass).

- [ ] **Step 6: Lint + commit**

Run: `buck2 build '//src/control-plane/core:core[clippy.txt]' 2>&1 | tail -5` (expect empty clippy output).

```bash
git add src/control-plane/core/src/identity.rs src/control-plane/core/src/lib.rs src/control-plane/core/tests/identity.rs
git commit -m "feat(core): add TypeId lineage identity for ontology types"
```

---

## Task 2: `conform` module — exact-match conformance check

Pure validation: an inferred result schema must produce exactly the output type's properties. Mirrors `ingest::bind`'s collect-all-violations style but also rejects extra columns.

**Files:**
- Create: `src/services/transform/src/conform.rs`
- Modify: `src/services/transform/src/lib.rs`
- Create: `src/services/transform/tests/conform.rs`
- Modify: `src/services/transform/BUCK`

- [ ] **Step 1: Create `conform.rs`**

Create `src/services/transform/src/conform.rs`:

```rust
//! Exact-match conformance: the SQL result of a typed transform must produce precisely
//! the output type's properties. Pure logic, no I/O. Parallels `ingest::bind`'s
//! validation but ALSO rejects extra columns — a typed transform materializes the
//! table, it is not a view over a wider one.

use control_plane_core::{ColumnSpec, PropertyDef, UnknownLogicalType, satisfies};

/// One way a result schema fails to conform to the output type. Collected, not fatal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Violation {
    /// A declared property has no same-named result column.
    MissingColumn { property: String, logical: String },
    /// A result column's physical type does not satisfy the property's logical type.
    TypeMismatch { property: String, logical: String, physical: String },
    /// The property's logical type is not in loom's vocabulary.
    UnknownLogicalType { property: String, logical: String },
    /// A required property is backed by a nullable result column.
    NullabilityViolation { property: String },
    /// A result column has no matching property (the exact-match half).
    UnexpectedColumn { column: String },
}

/// Exact-match: `result` columns must be precisely `properties`. Collects ALL violations
/// (never short-circuits) so an author fixes everything in one pass.
pub fn check_conformance(
    result: &[ColumnSpec],
    properties: &[PropertyDef],
) -> Result<(), Vec<Violation>> {
    let mut violations = Vec::new();

    // Every property must have a conforming, same-named result column.
    for p in properties {
        let Some(col) = result.iter().find(|c| c.name == p.name) else {
            violations.push(Violation::MissingColumn {
                property: p.name.clone(),
                logical: p.ty.clone(),
            });
            continue;
        };
        match satisfies(&p.ty, &col.ty) {
            Err(UnknownLogicalType(t)) => violations.push(Violation::UnknownLogicalType {
                property: p.name.clone(),
                logical: t,
            }),
            Ok(false) => violations.push(Violation::TypeMismatch {
                property: p.name.clone(),
                logical: p.ty.clone(),
                physical: col.ty.clone(),
            }),
            Ok(true) => {}
        }
        if p.required && col.nullable {
            violations.push(Violation::NullabilityViolation {
                property: p.name.clone(),
            });
        }
    }

    // Exact-match half: no result column may lack a matching property.
    for c in result {
        if !properties.iter().any(|p| p.name == c.name) {
            violations.push(Violation::UnexpectedColumn {
                column: c.name.clone(),
            });
        }
    }

    if violations.is_empty() {
        Ok(())
    } else {
        Err(violations)
    }
}
```

- [ ] **Step 2: Wire the module into `lib.rs`**

In `src/services/transform/src/lib.rs`, add `pub mod conform;` to the module list (above `pub mod handler;`):

```rust
pub mod conform;
pub mod handler;
pub mod run;
```

- [ ] **Step 3: Create the failing tests**

Create `src/services/transform/tests/conform.rs`:

```rust
//! Unit tests for the exact-match conformance check. Pure logic — no fixtures.

use control_plane_core::{ColumnSpec, PropertyDef};
use transform::conform::{Violation, check_conformance};

fn prop(name: &str, ty: &str, required: bool) -> PropertyDef {
    PropertyDef {
        name: name.into(),
        ty: ty.into(),
        required,
    }
}

fn col(name: &str, ty: &str, nullable: bool) -> ColumnSpec {
    ColumnSpec {
        name: name.into(),
        ty: ty.into(),
        nullable,
    }
}

#[test]
fn exact_match_conforms() {
    let props = vec![prop("id", "Long", true), prop("region", "String", false)];
    let cols = vec![col("id", "int64", false), col("region", "varchar", true)];
    assert_eq!(check_conformance(&cols, &props), Ok(()));
}

#[test]
fn missing_column_is_a_violation() {
    let props = vec![prop("id", "Long", true), prop("region", "String", false)];
    let cols = vec![col("id", "int64", false)];
    assert_eq!(
        check_conformance(&cols, &props),
        Err(vec![Violation::MissingColumn {
            property: "region".into(),
            logical: "String".into(),
        }])
    );
}

#[test]
fn type_mismatch_is_a_violation() {
    let props = vec![prop("id", "Long", true)];
    let cols = vec![col("id", "varchar", false)];
    assert_eq!(
        check_conformance(&cols, &props),
        Err(vec![Violation::TypeMismatch {
            property: "id".into(),
            logical: "Long".into(),
            physical: "varchar".into(),
        }])
    );
}

#[test]
fn unknown_logical_type_is_a_violation() {
    let props = vec![prop("id", "Wibble", true)];
    let cols = vec![col("id", "int64", false)];
    assert_eq!(
        check_conformance(&cols, &props),
        Err(vec![Violation::UnknownLogicalType {
            property: "id".into(),
            logical: "Wibble".into(),
        }])
    );
}

#[test]
fn required_property_over_nullable_column_is_a_violation() {
    let props = vec![prop("id", "Long", true)];
    let cols = vec![col("id", "int64", true)];
    assert_eq!(
        check_conformance(&cols, &props),
        Err(vec![Violation::NullabilityViolation {
            property: "id".into(),
        }])
    );
}

#[test]
fn extra_result_column_is_a_violation() {
    let props = vec![prop("id", "Long", true)];
    let cols = vec![col("id", "int64", false), col("extra", "varchar", true)];
    assert_eq!(
        check_conformance(&cols, &props),
        Err(vec![Violation::UnexpectedColumn {
            column: "extra".into(),
        }])
    );
}

#[test]
fn all_violations_are_collected() {
    // `id` mismatched, `region` missing, `extra` unexpected — all three reported.
    let props = vec![prop("id", "Long", true), prop("region", "String", false)];
    let cols = vec![col("id", "varchar", false), col("extra", "boolean", true)];
    let err = check_conformance(&cols, &props).unwrap_err();
    assert!(err.contains(&Violation::TypeMismatch {
        property: "id".into(),
        logical: "Long".into(),
        physical: "varchar".into(),
    }));
    assert!(err.contains(&Violation::MissingColumn {
        property: "region".into(),
        logical: "String".into(),
    }));
    assert!(err.contains(&Violation::UnexpectedColumn {
        column: "extra".into(),
    }));
    assert_eq!(err.len(), 3, "exactly three violations, none short-circuited");
}
```

- [ ] **Step 4: Add the `conform` test target to BUCK**

Append to `src/services/transform/BUCK` (a pure `rust_test`, not a fixture test):

```python
rust_test(
    name = "conform",
    crate = "conform",
    srcs = ["tests/conform.rs"],
    crate_root = "tests/conform.rs",
    edition = "2024",
    deps = [
        ":transform",
        "//src/control-plane/core:core",
    ],
)
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `buck2 test //src/services/transform:conform > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: `Tests finished: Pass 7. Fail 0.`

- [ ] **Step 6: Lint + commit**

Run: `buck2 build '//src/services/transform:transform[clippy.txt]' 2>&1 | tail -5` (expect empty).

```bash
git add src/services/transform/src/conform.rs src/services/transform/src/lib.rs src/services/transform/tests/conform.rs src/services/transform/BUCK
git commit -m "feat(transform): exact-match conformance check for typed outputs"
```

---

## Task 3: `run_transform` seams — `TransformInput` + conformance hook

Refactor the physical primitive so both physical and typed paths share it: inputs carry a `register_as` name, and an optional `conform` contract is checked between schema-inference and write. The physical handler is updated to compile; behavior is unchanged (the existing `transform-e2e` is the regression guard).

**Files:**
- Modify: `src/services/transform/src/run.rs` (full replacement below)
- Modify: `src/services/transform/src/handler.rs` (physical path only)
- Modify: `src/services/transform/src/lib.rs` (export `TransformInput`)

- [ ] **Step 1: Replace `run.rs`**

Replace the entire contents of `src/services/transform/src/run.rs` with:

```rust
//! The transform primitive: resolve input DuckLake table(s), have DataFusion run a SQL
//! query over them, and commit the result as a new snapshot of the output table plus
//! lineage (inputs -> output), atomically. Append semantics. Shared by the physical
//! path (inputs registered under their table name) and the typed path (registered under
//! the ontology type name, with a conformance contract).

use std::sync::Arc;

use control_plane_core::{
    ColumnSpec, ControlPlane, DataFile, LineageEvent, PropertyDef, SnapshotId, TableRef,
};
use datafusion::execution::context::SessionContext;
use datafusion_io::{WriteConfig, infer_columns, scan_table, write_dataset};
use object_store::ObjectStore;

use crate::conform::{Violation, check_conformance};

/// One input to a transform: a physical table plus the name it is registered under in
/// DataFusion (what the SQL references). The physical path registers tables under their
/// own name; the typed path registers them under the ontology type name.
pub struct TransformInput<'a> {
    pub table: &'a TableRef,
    pub register_as: &'a str,
}

/// One transform: read `inputs`, run `sql`, write the result to `output`.
pub struct TransformRequest<'a> {
    pub inputs: &'a [TransformInput<'a>],
    pub output: &'a TableRef,
    pub sql: &'a str,
    /// When `Some`, the result schema must EXACTLY conform to these properties (typed
    /// transforms); checked before any write. `None` skips the check (physical path).
    pub conform: Option<&'a [PropertyDef]>,
    /// Built by the caller; inputs -> output. Emitted in the commit transaction.
    pub lineage: LineageEvent,
}

#[derive(Debug, thiserror::Error)]
pub enum TransformError {
    #[error("unknown input table {0}.{1}")]
    UnknownInput(String, String),
    #[error("ambiguous input table name {0}: two inputs would register under it")]
    AmbiguousInput(String),
    #[error("output does not conform to the declared type: {} violation(s)", .0.len())]
    DoesNotConform(Vec<Violation>),
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

    // Inputs register under `register_as`; DataFusion silently overwrites a same-named
    // table, so two inputs sharing a registration name would shadow and the SQL would
    // compute against the wrong one. Reject that up front.
    let mut seen = std::collections::HashSet::new();
    for input in req.inputs {
        if !seen.insert(input.register_as) {
            return Err(TransformError::AmbiguousInput(input.register_as.to_string()));
        }
    }

    // 1. Resolve + register each input under its `register_as` name.
    for input in req.inputs {
        let snapshot = cp
            .catalog()
            .current_snapshot(input.table)
            .await
            .map_err(|e| match e {
                control_plane_core::ControlPlaneError::NotFound(_) => TransformError::UnknownInput(
                    input.table.schema.clone(),
                    input.table.name.clone(),
                ),
                other => TransformError::ControlPlane(other),
            })?;
        let files = cp
            .catalog()
            .files(
                input.table,
                snapshot.id,
                control_plane_core::PageReq::unbounded(),
            )
            .await?;
        scan_table(&ctx, store.clone(), input.register_as, input.table, &files.items).await?;
    }

    // 2. Run the SQL; collect the result + its Arrow schema.
    let df = ctx.sql(req.sql).await?;
    let schema: Arc<arrow::datatypes::Schema> = Arc::new(df.schema().as_arrow().clone());
    let batches = df.collect().await?;

    // 3. Output physical columns inferred from the result schema.
    let columns: Vec<ColumnSpec> = infer_columns(&schema)?;

    // 3a. Typed transforms: the result must EXACTLY conform to the declared type before
    //     anything is written or committed.
    if let Some(properties) = req.conform {
        check_conformance(&columns, properties).map_err(TransformError::DoesNotConform)?;
    }

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

- [ ] **Step 2: Update the physical handler to the new input shape**

In `src/services/transform/src/handler.rs`:

(a) Change the `run` import line from:

```rust
use crate::run::{TransformError, TransformRequest, run_transform};
```
to:
```rust
use crate::run::{TransformError, TransformInput, TransformRequest, run_transform};
```

(b) In `transform_handler`, replace the input/output construction and the `TransformRequest` literal. Find:

```rust
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
```

Replace with:

```rust
    let input_tables: Vec<TableRef> = payload.inputs.iter().map(TableRef::from).collect();
    let output = TableRef::from(&payload.output);
    let run_id = Uuid::new_v4().to_string();

    // Physical inputs register under their own table name.
    let inputs: Vec<TransformInput> = input_tables
        .iter()
        .map(|t| TransformInput {
            table: t,
            register_as: &t.name,
        })
        .collect();

    let lineage = LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: input_tables.iter().map(DatasetRef::from).collect(),
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
            conform: None,
            lineage,
        },
    )
    .await;
```

(c) Add the `DoesNotConform` arm to the deterministic side of `retry_policy`. Find:

```rust
        TransformError::UnknownInput(..)
        | TransformError::AmbiguousInput(_)
        | TransformError::DataFusion(_)
        | TransformError::Infer(_)
        | TransformError::NoSnapshot => RetryPolicy::Abandon,
```

Replace with:

```rust
        TransformError::UnknownInput(..)
        | TransformError::AmbiguousInput(_)
        | TransformError::DoesNotConform(_)
        | TransformError::DataFusion(_)
        | TransformError::Infer(_)
        | TransformError::NoSnapshot => RetryPolicy::Abandon,
```

- [ ] **Step 3: Export `TransformInput` from `lib.rs`**

In `src/services/transform/src/lib.rs`, change the `run` re-export from:

```rust
pub use run::{TransformError, TransformRequest, run_transform};
```
to:
```rust
pub use run::{TransformError, TransformInput, TransformRequest, run_transform};
```

- [ ] **Step 4: Build + lint**

Run: `buck2 build //src/services/transform:transform //src/services/transform:transform-bin 2>&1 | tail -20`
Expected: builds clean.

Run: `buck2 build '//src/services/transform:transform[clippy.txt]' 2>&1 | tail -5`
Expected: empty (no clippy findings).

- [ ] **Step 5: Run the physical regression guard**

Run: `buck2 test //src/services/transform:transform-e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: `Tests finished: Pass 1. Fail 0.` — the physical transform path is unchanged by the refactor.

- [ ] **Step 6: Commit**

```bash
git add src/services/transform/src/run.rs src/services/transform/src/handler.rs src/services/transform/src/lib.rs
git commit -m "refactor(transform): TransformInput register_as + optional conformance hook"
```

---

## Task 4: The typed primitive — `run_typed_transform`

Resolves input/output types, builds type-named lineage, and delegates to `run_transform` with the type-name registrations and the conformance contract. Verified end-to-end by Task 6; this task's gate is build + clippy.

**Files:**
- Create: `src/services/transform/src/typed.rs`
- Modify: `src/services/transform/src/lib.rs`

- [ ] **Step 1: Create `typed.rs`**

Create `src/services/transform/src/typed.rs`:

```rust
//! The typed transform primitive: resolve input/output ontology types to their DuckLake
//! tables, run the SQL (written in type terms), validate the result conforms to the
//! output type, and commit — emitting first-class type-named lineage. Delegates the
//! scan/SQL/write/commit skeleton to `run_transform`.

use std::sync::Arc;

use control_plane_core::{
    ControlPlane, ControlPlaneError, DatasetRef, EventType, LineageEvent, RunId, SnapshotId,
    TypeName,
};
use object_store::ObjectStore;
use uuid::Uuid;

use crate::run::{TransformError, TransformInput, TransformRequest, run_transform};

#[derive(Debug, thiserror::Error)]
pub enum TypedTransformError {
    #[error("unknown ontology type {0}")]
    UnknownType(String),
    #[error(transparent)]
    Transform(#[from] TransformError),
}

/// Run one typed transform. `run_id` is a caller-unique output-file prefix (a UUID).
pub async fn run_typed_transform(
    cp: &dyn ControlPlane,
    store: Arc<dyn ObjectStore>,
    run_id: &str,
    inputs: &[TypeName],
    output: &TypeName,
    sql: &str,
) -> Result<SnapshotId, TypedTransformError> {
    // 1. Resolve each input type to its backing table.
    let mut input_tables = Vec::with_capacity(inputs.len());
    for ty in inputs {
        let table = cp.ontology().resolve(ty).await.map_err(|e| match e {
            ControlPlaneError::NotFound(_) => TypedTransformError::UnknownType(ty.0.clone()),
            other => TypedTransformError::Transform(TransformError::ControlPlane(other)),
        })?;
        input_tables.push((ty.clone(), table));
    }

    // 2. Resolve the output type: its properties are the conformance contract; its table
    //    is the write target (which need not yet exist — create_table is idempotent).
    let out_type = cp.ontology().get_type(output).await.map_err(|e| match e {
        ControlPlaneError::NotFound(_) => TypedTransformError::UnknownType(output.0.clone()),
        other => TypedTransformError::Transform(TransformError::ControlPlane(other)),
    })?;

    // 3. Inputs register in DataFusion under their TYPE name (type-term SQL).
    let specs: Vec<TransformInput> = input_tables
        .iter()
        .map(|(ty, table)| TransformInput {
            table,
            register_as: &ty.0,
        })
        .collect();

    // 4. First-class type-named lineage; backing tables + SQL retained in the payload.
    let lineage = LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: inputs.iter().map(DatasetRef::from).collect(),
        outputs: vec![DatasetRef::from(output)],
        payload: serde_json::json!({
            "sql": sql,
            "input_tables": input_tables
                .iter()
                .map(|(_, t)| format!("{}.{}", t.schema, t.name))
                .collect::<Vec<_>>(),
            "output_table": format!("{}.{}", out_type.table.schema, out_type.table.name),
        }),
    };

    run_transform(
        cp,
        store,
        run_id,
        TransformRequest {
            inputs: &specs,
            output: &out_type.table,
            sql,
            conform: Some(&out_type.properties),
            lineage,
        },
    )
    .await
    .map_err(TypedTransformError::Transform)
}
```

- [ ] **Step 2: Wire `typed` into `lib.rs`**

In `src/services/transform/src/lib.rs`, add `pub mod typed;` to the module list and add the typed re-export. The module list becomes:

```rust
pub mod conform;
pub mod handler;
pub mod run;
pub mod typed;
```

And add below the `run` re-export:

```rust
pub use typed::{TypedTransformError, run_typed_transform};
```

- [ ] **Step 3: Build + lint**

Run: `buck2 build //src/services/transform:transform 2>&1 | tail -20`
Expected: builds clean.

Run: `buck2 build '//src/services/transform:transform[clippy.txt]' 2>&1 | tail -5`
Expected: empty.

- [ ] **Step 4: Commit**

```bash
git add src/services/transform/src/typed.rs src/services/transform/src/lib.rs
git commit -m "feat(transform): run_typed_transform — resolve types, conform, type-named lineage"
```

---

## Task 5: Typed handler + worker dispatch

Adds the `"typed-transform"` job kind handler and routes both kinds in the binary.

**Files:**
- Modify: `src/services/transform/src/handler.rs`
- Modify: `src/services/transform/src/main.rs`
- Modify: `src/services/transform/src/lib.rs`

- [ ] **Step 1: Add the typed handler to `handler.rs`**

In `src/services/transform/src/handler.rs`:

(a) Add `TypeName` to the `control_plane_core` import list, and add a `typed` import. After the existing `use crate::run::{...}` line add:

```rust
use crate::typed::{TypedTransformError, run_typed_transform};
```

Ensure `TypeName` is imported — the core import block should include it, e.g.:

```rust
use control_plane_core::{
    ControlPlane, DatasetRef, EventType, Job, JobFailure, LineageEvent, RetryPolicy, RunId,
    TableRef, TypeName,
};
```

(b) After the `transform_handler` function (before `fn retry_policy`), add the typed payload, handler, and retry classifier:

```rust
/// Wire form of a typed transform job (`"typed-transform"` kind): ontology type names.
#[derive(Deserialize)]
struct TypedTransformPayload {
    inputs: Vec<String>,
    output: String,
    sql: String,
}

/// Run one TYPED transform job. Inputs/output are ontology type names; the SQL
/// references inputs by type name; the result must conform to the output type.
pub async fn typed_transform_handler(
    cp: &dyn ControlPlane,
    store: Arc<dyn ObjectStore>,
    job: Job,
) -> Result<(), JobFailure> {
    let payload: TypedTransformPayload = match serde_json::from_value(job.payload.clone()) {
        Ok(p) => p,
        Err(e) => {
            return Err(JobFailure {
                error: format!("malformed typed-transform payload: {e}"),
                policy: RetryPolicy::Abandon,
            });
        }
    };
    let inputs: Vec<TypeName> = payload.inputs.iter().map(|s| TypeName(s.clone())).collect();
    let output = TypeName(payload.output.clone());
    let run_id = Uuid::new_v4().to_string();

    let res = run_typed_transform(cp, store, &run_id, &inputs, &output, &payload.sql).await;

    res.map(|_snapshot| ()).map_err(|e| JobFailure {
        error: e.to_string(),
        policy: typed_retry_policy(&e, job.attempts),
    })
}

/// Typed-transform classification: an unknown type is deterministic; otherwise defer to
/// the physical mapping (which already classifies `DoesNotConform` as Abandon).
fn typed_retry_policy(err: &TypedTransformError, attempts: i32) -> RetryPolicy {
    match err {
        TypedTransformError::UnknownType(_) => RetryPolicy::Abandon,
        TypedTransformError::Transform(t) => retry_policy(t, attempts),
    }
}
```

- [ ] **Step 2: Export the typed handler from `lib.rs`**

In `src/services/transform/src/lib.rs`, change the `handler` re-export from:

```rust
pub use handler::transform_handler;
```
to:
```rust
pub use handler::{transform_handler, typed_transform_handler};
```

- [ ] **Step 3: Dispatch both kinds in `main.rs`**

Replace the entire contents of `src/services/transform/src/main.rs` with:

```rust
//! transform binary: build the control plane + object store from env config via
//! service_runtime, then run the queue worker loop dispatching the two transform
//! handlers by job kind. Queue-driven — no HTTP surface.

use std::sync::Arc;

use control_plane_core::{ControlPlane, Job};
use control_plane_worker::Worker;
use object_store::ObjectStore;
use tokio_util::sync::CancellationToken;
use transform::{transform_handler, typed_transform_handler};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = service_runtime::Config::from_env()?;
    let pool = service_runtime::build_pool(&cfg.db).await?;
    let cp = service_runtime::control_plane(pool, cfg.lock_timeout);
    let store: Arc<dyn ObjectStore> = Arc::new(service_runtime::local_store(&cfg.data_path)?);

    let cp_for_handler: Arc<dyn ControlPlane> = Arc::new(cp.clone());
    let worker = Worker::new(cp, "transform-1", cfg.lock_timeout);
    let shutdown = CancellationToken::new();

    worker
        .run(
            &["transform".to_string(), "typed-transform".to_string()],
            shutdown,
            move |job: Job| {
                let cp = cp_for_handler.clone();
                let store = store.clone();
                async move {
                    match job.kind.as_str() {
                        "typed-transform" => typed_transform_handler(cp.as_ref(), store, job).await,
                        _ => transform_handler(cp.as_ref(), store, job).await,
                    }
                }
            },
        )
        .await?;
    Ok(())
}
```

- [ ] **Step 4: Build + lint**

Run: `buck2 build //src/services/transform:transform //src/services/transform:transform-bin 2>&1 | tail -20`
Expected: builds clean.

Run: `buck2 build '//src/services/transform:transform[clippy.txt]' 2>&1 | tail -5`
Expected: empty.

- [ ] **Step 5: Commit**

```bash
git add src/services/transform/src/handler.rs src/services/transform/src/main.rs src/services/transform/src/lib.rs
git commit -m "feat(transform): typed-transform job kind handler + worker dispatch"
```

---

## Task 6: Typed transform e2e (fixture) — the acceptance test

The load-bearing proof: enqueue a `"typed-transform"` job, the worker resolves input types, runs type-name SQL, conformance passes, the output commits with type-named lineage, and it reads back through query-api as the typed object. Plus a negative case proving a non-conforming result commits nothing.

**Files:**
- Create: `src/services/transform/tests/typed_transform_e2e.rs`
- Modify: `src/services/transform/BUCK`

- [ ] **Step 1: Create the e2e test**

Create `src/services/transform/tests/typed_transform_e2e.rs`:

```rust
//! Typed transform e2e: enqueue a "typed-transform" job; the worker resolves input
//! TYPES to tables, runs type-name SQL, validates the result conforms to the output
//! TYPE, commits, and emits first-class type-named lineage. The output reads back
//! through query-api as the typed object. Real Postgres + DuckDB.
//!
//! The positive case uses non-required output properties: an inner join can widen
//! column nullability in DataFusion, which would make a required-over-nullable check
//! flaky. The deterministic `required` semantics are covered by the pure `conform`
//! unit tests; the negative case here uses a single-table select (no join, no widening)
//! so its MissingColumn violation is deterministic.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{
    Acl, Action, Catalog, ControlPlane, ControlPlaneError, DatasetRef, Effect, EventType, Lineage,
    LineageEvent, NewJob, ObjectType, Ontology, PageReq, PolicyTarget, PropertyDef, Queue, RoleId,
    RunId, SubjectId, TableRef, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use control_plane_worker::Worker;
use ingest::{MaterializeRequest, materialize};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use query_api::handler::{ObjectQuery, QueryDeps, Subject, read_object};
use query_api::render::objects_to_json;
use query_api::serving::EmbeddedDuckDb;
use serde_json::json;
use time::OffsetDateTime;
use tokio_util::sync::CancellationToken;
use transform::{transform_handler, typed_transform_handler};
use uuid::Uuid;

fn tref(s: &str, n: &str) -> TableRef {
    TableRef {
        schema: s.into(),
        name: n.into(),
    }
}

fn prop(name: &str, ty: &str, required: bool) -> PropertyDef {
    PropertyDef {
        name: name.into(),
        ty: ty.into(),
        required,
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
        payload: json!({}),
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

/// Spawn the worker on BOTH transform kinds; return a cancel token + join handle.
fn spawn_worker(
    cp: &PgControlPlane,
    store: &Arc<dyn ObjectStore>,
) -> (CancellationToken, tokio::task::JoinHandle<()>) {
    let token = CancellationToken::new();
    let t = token.clone();
    let store_h = store.clone();
    let cp_h: Arc<dyn ControlPlane> = Arc::new(cp.clone());
    let worker = Worker::new(cp.clone(), "typed-transform-test", Duration::from_millis(300))
        .with_poll_interval(Duration::from_millis(50));
    let handle = tokio::spawn(async move {
        worker
            .run(
                &["transform".to_string(), "typed-transform".to_string()],
                t,
                move |job| {
                    let cp = cp_h.clone();
                    let store = store_h.clone();
                    async move {
                        match job.kind.as_str() {
                            "typed-transform" => typed_transform_handler(cp.as_ref(), store, job).await,
                            _ => transform_handler(cp.as_ref(), store, job).await,
                        }
                    }
                },
            )
            .await
            .unwrap();
    });
    (token, handle)
}

#[tokio::test(flavor = "multi_thread")]
async fn typed_transform_materializes_and_governs_the_output_model() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(writer.data_path()).unwrap());

    // 1. LAND two input tables.
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
        Field::new("amount", DataType::Float64, false),
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
                Arc::new(Float64Array::from(vec![5.5, 7.5, 2.5])),
            ],
        )
        .unwrap(),
    )
    .await;

    // 2. DEFINE the input types (so resolve() finds their tables) and the OUTPUT type
    //    (its backing table main.order_enriched does NOT exist yet).
    cp.ontology()
        .define_type(ObjectType {
            name: TypeName("Customer".into()),
            properties: vec![prop("id", "Long", true), prop("region", "String", false)],
            table: customers.clone(),
        })
        .await
        .unwrap();
    cp.ontology()
        .define_type(ObjectType {
            name: TypeName("Order".into()),
            properties: vec![
                prop("id", "Long", true),
                prop("customer_id", "Long", true),
                prop("amount", "Double", true),
            ],
            table: orders.clone(),
        })
        .await
        .unwrap();
    let enriched = tref("main", "order_enriched");
    cp.ontology()
        .define_type(ObjectType {
            name: TypeName("OrderEnriched".into()),
            // Non-required to stay robust against inner-join nullability widening; see
            // the module doc comment.
            properties: vec![
                prop("id", "Long", false),
                prop("region", "String", false),
                prop("amount", "Double", false),
            ],
            table: enriched.clone(),
        })
        .await
        .unwrap();

    // 3. ENQUEUE a typed transform: SQL references inputs by TYPE name ("Order" is a
    //    reserved word, so quote both identifiers; aliases produce the property names).
    cp.enqueue(NewJob {
        kind: "typed-transform".into(),
        payload: json!({
            "inputs": ["Customer", "Order"],
            "output": "OrderEnriched",
            "sql": "SELECT \"Order\".id AS id, \"Customer\".region AS region, \
                    \"Order\".amount AS amount \
                    FROM \"Order\" JOIN \"Customer\" ON \"Order\".customer_id = \"Customer\".id"
        }),
        run_at: None,
        priority: 0,
    })
    .await
    .unwrap();

    // 4. RUN the worker until the job drains.
    let (token, handle) = spawn_worker(&cp, &store);
    tokio::time::sleep(Duration::from_millis(900)).await;
    token.cancel();
    handle.await.unwrap();

    assert!(
        cp.dequeue(&["typed-transform".to_string()], "probe")
            .await
            .unwrap()
            .is_none(),
        "typed-transform job completed"
    );

    // 5a. Output rows landed (DuckDB).
    let count = writer
        .query_scalar("SELECT count(*) FROM lake.main.order_enriched;")
        .await;
    assert_eq!(count, "3", "DuckDB reads the typed transform output");
    let regions = writer
        .query_scalar("SELECT string_agg(region, ',' ORDER BY id) FROM lake.main.order_enriched;")
        .await;
    assert_eq!(regions, "CA,CA,NY", "join produced the right regions");

    // 5b. First-class TYPE-named lineage: upstream(OrderEnriched) == {Customer, Order}.
    let out_ds: DatasetRef = (&TypeName("OrderEnriched".into())).into();
    let ups = cp
        .lineage()
        .upstream(&out_ds, PageReq::unbounded())
        .await
        .unwrap();
    let up: std::collections::HashSet<String> = ups.items.iter().map(|d| d.name.clone()).collect();
    assert_eq!(
        up,
        std::collections::HashSet::from(["Customer".to_string(), "Order".to_string()]),
        "type-named lineage upstream, got {up:?}"
    );
    assert!(
        ups.items.iter().all(|d| d.namespace == "loom:type"),
        "lineage nodes are type-namespaced"
    );

    // 5c. The Object Model round-trips: read OrderEnriched through query-api.
    let subj = SubjectId("analyst".into());
    let role = RoleId("analysts".into());
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    cp.grant(
        &role,
        Action::Read,
        PolicyTarget::Type(TypeName("OrderEnriched".into())),
        Effect::Allow,
    )
    .await
    .unwrap();

    let eng = EmbeddedDuckDb::attach(
        &format!(
            "dbname={} host={} user=postgres",
            db,
            fx.socket_path().display()
        ),
        writer.data_path(),
    )
    .await
    .unwrap();
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &eng,
    };
    let rows = read_object(
        &ObjectQuery {
            type_name: "OrderEnriched".into(),
            eq_filters: vec![],
        },
        &Subject(subj),
        &deps,
    )
    .await
    .unwrap();
    assert_eq!(
        rows.columns,
        vec!["id".to_string(), "region".to_string(), "amount".to_string()]
    );
    assert_eq!(rows.rows.len(), 3, "all three enriched rows are governed-readable");

    let body = objects_to_json(&rows);
    let mut objs: Vec<serde_json::Value> = body["objects"].as_array().unwrap().clone();
    objs.sort_by_key(|o| o["id"].as_str().unwrap().to_string());
    assert_eq!(
        objs,
        vec![
            json!({ "id": "10", "region": "CA", "amount": 5.5 }),
            json!({ "id": "11", "region": "CA", "amount": 7.5 }),
            json!({ "id": "12", "region": "NY", "amount": 2.5 }),
        ],
        "OrderEnriched round-trips as typed JSON (Long id as string, Double amount as number)"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn non_conforming_typed_transform_commits_nothing() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(writer.data_path()).unwrap());

    // One landed input + its type.
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
    cp.ontology()
        .define_type(ObjectType {
            name: TypeName("Customer".into()),
            properties: vec![prop("id", "Long", true), prop("region", "String", false)],
            table: customers.clone(),
        })
        .await
        .unwrap();

    // Output type requires `region`, but the SQL omits it -> DoesNotConform. Single-table
    // select (no join) keeps `id` deterministically non-null.
    let bad = tref("main", "customer_bad");
    cp.ontology()
        .define_type(ObjectType {
            name: TypeName("CustomerBad".into()),
            properties: vec![prop("id", "Long", true), prop("region", "String", false)],
            table: bad.clone(),
        })
        .await
        .unwrap();

    cp.enqueue(NewJob {
        kind: "typed-transform".into(),
        payload: json!({
            "inputs": ["Customer"],
            "output": "CustomerBad",
            "sql": "SELECT \"Customer\".id AS id FROM \"Customer\""
        }),
        run_at: None,
        priority: 0,
    })
    .await
    .unwrap();

    let (token, handle) = spawn_worker(&cp, &store);
    tokio::time::sleep(Duration::from_millis(700)).await;
    token.cancel();
    handle.await.unwrap();

    // The job was abandoned (deterministic), and NOTHING was committed.
    assert!(
        cp.dequeue(&["typed-transform".to_string()], "probe")
            .await
            .unwrap()
            .is_none(),
        "non-conforming job is not left runnable (abandoned)"
    );
    assert!(
        matches!(
            cp.catalog().current_snapshot(&bad).await,
            Err(ControlPlaneError::NotFound(_))
        ),
        "non-conforming transform committed no snapshot for the output table"
    );
}
```

- [ ] **Step 2: Add the fixture test target to BUCK**

Append to `src/services/transform/BUCK` (a `loom_fixture_test` with `duckdb = True`; note the `query-api` dep for the read-back):

```python
loom_fixture_test(
    name = "typed-transform-e2e",
    crate = "typed_transform_e2e",
    srcs = ["tests/typed_transform_e2e.rs"],
    crate_root = "tests/typed_transform_e2e.rs",
    duckdb = True,
    deps = [
        ":transform",
        "//src/services/ingest:ingest",
        "//src/services/query-api:query-api",
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

- [ ] **Step 3: Run the e2e**

Run: `buck2 test //src/services/transform:typed-transform-e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: `Tests finished: Pass 2. Fail 0.`

If it fails, debug systematically (superpowers:systematic-debugging): inspect `/tmp/t.log`. Likely culprits and fixes:
- **DataFusion identifier case/quoting:** registered table names are `Customer`/`Order` (mixed case); SQL must quote them (`"Order"`) or DataFusion lowercases unquoted identifiers and won't find the table. Already quoted in the SQL above.
- **`UnexpectedColumn`/`MissingColumn` from the positive job:** the result column names must equal the property names exactly — they come from the `AS id`/`AS region`/`AS amount` aliases. If DataFusion emits a different case, align the aliases.
- **Utf8View:** if `region` infers as an unexpected type, confirm `datafusion-io::scan_table` builds `ParquetFormat` with `with_force_view_types(false)` (it does); strings should infer as `varchar`.

- [ ] **Step 4: Lint + commit**

Run: `tools/clippy-all.sh 2>&1 | tail -5` (or `buck2 build '//src/services/transform:transform[clippy.txt]'`) — expect clean.

```bash
git add src/services/transform/tests/typed_transform_e2e.rs src/services/transform/BUCK
git commit -m "test(transform): typed transform e2e — conform, type-named lineage, governed read-back"
```

---

## Task 7: Roadmap update

Record the slice as delivered.

**Files:**
- Modify: `docs/superpowers/specs/2026-06-06-loom-roadmap.md`

- [ ] **Step 1: Mark typed transforms part-1 delivered**

Open `docs/superpowers/specs/2026-06-06-loom-roadmap.md`, find the Transform-workers section (where part-1 / physical transforms is recorded as delivered), and add a sibling entry noting **typed transforms part-1 (`Type(s) → Type`)** as delivered: type-name SQL, exact-match conformance, first-class type-named lineage, governed read-back. Mirror the wording/format of the adjacent delivered entries (read the surrounding lines first and match their style — checkbox or prose as used there). Reference this spec: `docs/superpowers/specs/2026-06-15-typed-transforms-part1-design.md`.

- [ ] **Step 2: Commit**

```bash
git add docs/superpowers/specs/2026-06-06-loom-roadmap.md
git commit -m "docs(roadmap): typed transforms part 1 delivered"
```

---

## Final Verification

After all tasks, run the full first-party sweep to confirm nothing regressed:

- [ ] Run: `buck2 build //src/... 2>&1 | tail -20` — expect clean build.
- [ ] Run: `buck2 test //src/... > /tmp/sweep.log 2>&1; grep -E "Tests finished|FAIL" /tmp/sweep.log` — expect `Fail 0`, including `transform:conform`, `transform:transform-e2e`, `transform:typed-transform-e2e`, and `core:identity`.
- [ ] Run: `tools/clippy-all.sh 2>&1 | tail -5` — expect clean.
- [ ] Confirm `duckdb` stayed pinned (no lockfile churn): `git status` shows no `Cargo.lock`/`third-party/BUCK` changes (this slice adds no third-party deps).

Then proceed to **superpowers:finishing-a-development-branch**.
```
