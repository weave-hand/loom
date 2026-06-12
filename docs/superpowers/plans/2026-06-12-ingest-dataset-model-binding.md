# Dataset→Model Binding Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bind a landed DuckLake dataset to an ontology type, validating the table's physical schema satisfies the declared type (incl. a new `core` logical-type vocabulary) before persisting it — so a bound type is guaranteed-serveable by the query read path.

**Architecture:** A pure logical-type vocabulary in `control-plane-core` (base scalars → DuckLake physical affinities + semantic aliases + a `satisfies` check), and a `bind` orchestrator in `src/services/ingest` that reads `Catalog::schema`, validates each property, then calls `Ontology::define_type`. Proven by an accept/reject matrix against the real catalog and a materialize→bind→read end-to-end test.

**Tech Stack:** Rust, buck2, the existing `control-plane-{core,postgres}` + `ingest` + `query-api` crates, the pinned DuckDB/Postgres fixtures.

**Spec:** `docs/superpowers/specs/2026-06-12-ingest-dataset-model-binding-design.md`

---

## Background the implementer needs

**Existing types/traits (in `control_plane_core`):**
```rust
// catalog.rs
pub struct TableRef { pub schema: String, pub name: String }
pub struct Snapshot { pub id: SnapshotId, /* ... */ }
pub struct ColumnDef { pub order: i64, pub name: String, pub ty: String /* DuckLake physical string */, pub nullable: bool }
pub struct TableSchema { pub columns: Vec<ColumnDef> }
#[async_trait] pub trait Catalog {
    async fn current_snapshot(&self, table: &TableRef) -> Result<Snapshot>;   // NotFound if absent
    async fn schema(&self, table: &TableRef, at: SnapshotId) -> Result<TableSchema>;
    // ... files, snapshots
}
// ontology.rs
pub struct TypeName(pub String);
pub struct PropertyDef { pub name: String, pub ty: String /* LOGICAL */, pub required: bool }
pub struct ObjectType { pub name: TypeName, pub properties: Vec<PropertyDef>, pub table: TableRef }
#[async_trait] pub trait Ontology {
    async fn define_type(&self, ty: ObjectType) -> Result<()>;
    async fn get_type(&self, name: &TypeName) -> Result<ObjectType>;          // NotFound if absent
    async fn resolve(&self, name: &TypeName) -> Result<TableRef>;
    // ...
}
// error.rs
pub enum ControlPlaneError { NotFound(String), /* ... #[non_exhaustive] */ }
```

**Adapter:** `PgControlPlane` (postgres) implements BOTH `Catalog` and `Ontology` (and the rest). So in tests one `cp` value is passed as both `&dyn Catalog` and `&dyn Ontology`: `bind(&cp, &cp, type_def)`.

**Fixtures (`control_plane_postgres::fixture`):**
- `PgFixture::start()`; `fixture.fresh_db().await -> (PgControlPlane, String /*db*/)`; `fixture.socket_path() -> &Path`.
- `DuckLakeWriter::new(socket, &db)`; `.bootstrap().await` (creates an empty `ducklake_*` catalog); `.seed(schema, table, columns: &[(String,String,bool /*name,SQL-type,nullable*/)], batches: &[usize]).await` (creates a DuckLake table via DuckDB with exact columns/nullability + inserts `sum(batches)` rows); `.data_path() -> &Path`.
  - DuckDB SQL types map to DuckLake canonical strings: `BIGINT`→`int64`, `INTEGER`→`int32`, `VARCHAR`→`varchar`, `DOUBLE`→`double`, `BOOLEAN`→`boolean`, `DATE`→`date`, `TIMESTAMP`→`timestamp`.

**`query-api` read harness** (see `src/services/query-api/tests/governed_read.rs`): `EmbeddedDuckDb::attach(socket, &db, data_path).await -> Result<EmbeddedDuckDb>`; `QueryDeps { ontology: &dyn Ontology, acl: &dyn Acl, serving: &dyn ServingEngine }`; `read_object(&ObjectQuery{type_name, eq_filters}, &Subject(SubjectId), &deps).await -> Result<Rows, QueryError>`; `Rows { columns: Vec<String>, rows: Vec<Vec<SqlValue>> }`; `SqlValue::Int(i64)`. ACL grant: `cp.define_subject(&SubjectId)`, `cp.define_role(&RoleId)`, `cp.assign_role(&subj,&role)`, `cp.grant(&role, Action::Read, PolicyTarget::Type(TypeName), Effect::Allow)`.

**Conventions (non-negotiable):**
- Tests are `rust_test`/`loom_fixture_test` integration targets — NO inline `#[cfg(test)]` (prek hook fails the build). Fixture tests (boot postgres/duckdb) MUST use `loom_fixture_test(duckdb = True)` loaded from `//src/control-plane/postgres:defs.bzl`.
- Run tests with `buck2 test //src/...` (do NOT pipe to `tail`; redirect to a file + grep).
- `rustfmt` is CHECK-ONLY: `buck2 run //tools:rustfmt -- <files>` and apply before committing `.rs`.
- Commits: Conventional Commits ending `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`. Never `--no-verify`.
- Branch is `feat/ingest-dataset-model-binding` (already created; the spec commit is on it). Do NOT switch branches.
- arrow buck alias is `//third-party:arrow` (NOT arrow-58).

---

## File structure

| File | Responsibility |
| --- | --- |
| `src/control-plane/core/src/logical_type.rs` | The logical-type vocabulary: `BaseType`, affinities, aliases, `resolve_logical`, `satisfies`, `UnknownLogicalType`. |
| `src/control-plane/core/src/lib.rs` | Add `mod logical_type;` + re-exports. |
| `src/control-plane/core/tests/logical_type.rs` | Vocabulary unit tests. |
| `src/services/ingest/src/bind.rs` | `bind` + `BindError`/`BindViolation`/`BindViolationReason`. |
| `src/services/ingest/src/lib.rs` | Add `pub mod bind;` + re-exports. |
| `src/services/ingest/tests/bind.rs` | Accept/reject matrix against the real catalog. |
| `src/services/query-api/tests/bind_read_e2e.rs` | materialize→bind→read_object end-to-end. |

---

## Task 1: The `core` logical-type vocabulary

**Files:**
- Create: `src/control-plane/core/src/logical_type.rs`
- Modify: `src/control-plane/core/src/lib.rs`
- Test: `src/control-plane/core/tests/logical_type.rs`
- Modify: `src/control-plane/core/BUCK`

- [ ] **Step 1: Write the failing test** (`tests/logical_type.rs`)

```rust
use control_plane_core::{BaseType, UnknownLogicalType, resolve_logical, satisfies};

#[test]
fn resolves_base_names_case_insensitively() {
    assert_eq!(resolve_logical("Integer"), Some(BaseType::Integer));
    assert_eq!(resolve_logical("integer"), Some(BaseType::Integer));
    assert_eq!(resolve_logical("LONG"), Some(BaseType::Long));
    assert_eq!(resolve_logical("Timestamp"), Some(BaseType::Timestamp));
}

#[test]
fn resolves_semantic_aliases_to_base() {
    assert_eq!(resolve_logical("EmailAddress"), Some(BaseType::String));
    assert_eq!(resolve_logical("Url"), Some(BaseType::String));
    assert_eq!(resolve_logical("PhoneNumber"), Some(BaseType::String));
}

#[test]
fn unknown_logical_type_does_not_resolve() {
    assert_eq!(resolve_logical("Money"), None);
}

#[test]
fn satisfies_matches_physical_affinity() {
    assert_eq!(satisfies("Long", "int64"), Ok(true));
    assert_eq!(satisfies("Integer", "int32"), Ok(true));
    assert_eq!(satisfies("Double", "double"), Ok(true));
    assert_eq!(satisfies("Boolean", "boolean"), Ok(true));
    assert_eq!(satisfies("String", "varchar"), Ok(true));
    assert_eq!(satisfies("EmailAddress", "varchar"), Ok(true));
    assert_eq!(satisfies("Date", "date"), Ok(true));
    assert_eq!(satisfies("Timestamp", "timestamp"), Ok(true));
}

#[test]
fn satisfies_rejects_width_mismatch() {
    assert_eq!(satisfies("Integer", "int64"), Ok(false));
    assert_eq!(satisfies("Long", "int32"), Ok(false));
}

#[test]
fn satisfies_normalizes_physical_casing_and_whitespace() {
    assert_eq!(satisfies("String", "VARCHAR"), Ok(true));
    assert_eq!(satisfies("Long", " int64 "), Ok(true));
}

#[test]
fn satisfies_errors_on_unknown_logical_type() {
    assert_eq!(satisfies("Money", "double"), Err(UnknownLogicalType("Money".into())));
}
```

- [ ] **Step 2: Add the BUCK test target + verify it fails**

Add to `src/control-plane/core/BUCK`:
```python
rust_test(
    name = "logical-type",
    crate = "logical_type",
    srcs = ["tests/logical_type.rs"],
    crate_root = "tests/logical_type.rs",
    edition = "2024",
    deps = [":core"],
)
```
```bash
buck2 test //src/control-plane/core:logical-type > /tmp/t.log 2>&1; grep -E "error\[|cannot find|Tests finished|FAIL" /tmp/t.log
```
Expected: FAIL — `BaseType`/`resolve_logical`/`satisfies`/`UnknownLogicalType` not found.

- [ ] **Step 3: Implement `src/control-plane/core/src/logical_type.rs`**

```rust
//! loom's logical-type vocabulary: the base scalar types, their DuckLake physical
//! affinities, and the semantic aliases. Used by dataset->model binding to check a
//! landed physical column satisfies a declared logical property type. Pure logic,
//! no I/O. The vocabulary is CLOSED: an unrecognized logical type is an error, never
//! a silent pass — that keeps the ontology authoritative.
//!
//! NOTE: this vocabulary is the natural anchor for a later query-path typed JSON
//! serialization (Date/Timestamp -> ISO-8601 strings, Long -> JSON string to keep
//! int64 precision past 2^53). That wire-encoding axis is intentionally NOT here.

/// A loom base scalar logical type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BaseType {
    Integer,
    Long,
    Double,
    Boolean,
    String,
    Date,
    Timestamp,
}

/// A logical type loom does not recognize (neither a base type nor a known alias).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnknownLogicalType(pub String);

impl BaseType {
    /// The DuckLake physical type strings (canonical lowercase) that satisfy this
    /// base type. Exact-match, no implicit widening (Integer is 32-bit, Long 64-bit).
    pub fn physical_affinity(self) -> &'static [&'static str] {
        match self {
            BaseType::Integer => &["int32"],
            BaseType::Long => &["int64"],
            BaseType::Double => &["double"],
            BaseType::Boolean => &["boolean"],
            BaseType::String => &["varchar"],
            BaseType::Date => &["date"],
            BaseType::Timestamp => &["timestamp"],
        }
    }
}

/// Resolve a logical type name (a base name or a known semantic alias,
/// case-insensitively) to its BaseType. `None` if loom does not recognize it.
pub fn resolve_logical(ty: &str) -> Option<BaseType> {
    match ty.trim().to_ascii_lowercase().as_str() {
        "integer" => Some(BaseType::Integer),
        "long" => Some(BaseType::Long),
        "double" => Some(BaseType::Double),
        "boolean" => Some(BaseType::Boolean),
        "string" => Some(BaseType::String),
        "date" => Some(BaseType::Date),
        "timestamp" => Some(BaseType::Timestamp),
        // semantic aliases -> base
        "emailaddress" => Some(BaseType::String),
        "url" => Some(BaseType::String),
        "phonenumber" => Some(BaseType::String),
        _ => None,
    }
}

/// Does a DuckLake physical type string satisfy a logical type? Both sides are
/// normalized (trim + lowercase) before comparison. `Err(UnknownLogicalType)` if
/// the logical type is neither a base nor a known alias.
pub fn satisfies(logical_ty: &str, physical_ty: &str) -> Result<bool, UnknownLogicalType> {
    let base = resolve_logical(logical_ty)
        .ok_or_else(|| UnknownLogicalType(logical_ty.trim().to_string()))?;
    let phys = physical_ty.trim().to_ascii_lowercase();
    Ok(base.physical_affinity().contains(&phys.as_str()))
}
```

- [ ] **Step 4: Wire `lib.rs`**

In `src/control-plane/core/src/lib.rs`, add `mod logical_type;` alongside the other `mod` lines (alphabetical: after `mod lineage;`), and add a re-export line after the `lineage` re-export:
```rust
pub use logical_type::{BaseType, UnknownLogicalType, resolve_logical, satisfies};
```

- [ ] **Step 5: Run the tests**

```bash
buck2 run //tools:rustfmt -- src/control-plane/core/src/logical_type.rs src/control-plane/core/src/lib.rs src/control-plane/core/tests/logical_type.rs
buck2 test //src/control-plane/core:logical-type > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```
Expected: PASS (7 tests).

- [ ] **Step 6: Commit**

```bash
git add src/control-plane/core
git commit -m "$(cat <<'EOF'
feat(core): logical-type vocabulary (base scalars, affinities, aliases, satisfies)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: The `bind` operation + validation matrix

**Files:**
- Create: `src/services/ingest/src/bind.rs`
- Modify: `src/services/ingest/src/lib.rs`
- Test: `src/services/ingest/tests/bind.rs`
- Modify: `src/services/ingest/BUCK`

- [ ] **Step 1: Write the failing test** (`tests/bind.rs`)

```rust
//! bind validation against the real catalog: a conforming type binds and persists;
//! a non-conforming type is rejected with ALL violations and nothing is persisted.

use control_plane_core::{ObjectType, Ontology, PropertyDef, TableRef, TypeName};
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use ingest::{BindError, BindViolationReason, bind};

fn prop(name: &str, ty: &str, required: bool) -> PropertyDef {
    PropertyDef { name: name.into(), ty: ty.into(), required }
}

fn customer() -> TableRef {
    TableRef { schema: "main".into(), name: "customer".into() }
}

// Seed main.customer: id BIGINT (int64) NOT NULL, email VARCHAR (varchar) NULL.
async fn seed_customer(writer: &DuckLakeWriter) {
    writer
        .seed(
            "main",
            "customer",
            &[
                ("id".into(), "BIGINT".into(), false),
                ("email".into(), "VARCHAR".into(), true),
            ],
            &[2],
        )
        .await;
}

#[tokio::test]
async fn bind_accepts_conforming_type_and_persists_it() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    seed_customer(&writer).await;

    let type_def = ObjectType {
        name: TypeName("Customer".into()),
        properties: vec![
            prop("id", "Long", true),            // int64, non-null -> ok
            prop("email", "EmailAddress", false), // varchar, optional -> ok
        ],
        table: customer(),
    };
    bind(&cp, &cp, type_def.clone()).await.unwrap();

    // Persisted + serveable: get_type returns exactly what we bound.
    let got = cp.get_type(&TypeName("Customer".into())).await.unwrap();
    assert_eq!(got, type_def);
}

#[tokio::test]
async fn bind_collects_all_violations_and_persists_nothing() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    seed_customer(&writer).await;

    // phone: not a column (MissingColumn)
    // id: Integer over int64 column (TypeMismatch)
    // amount: Money is unknown (UnknownLogicalType)
    // email: String required over a nullable column (NullabilityViolation)
    let type_def = ObjectType {
        name: TypeName("Bad".into()),
        properties: vec![
            prop("phone", "String", false),
            prop("id", "Integer", false),
            prop("amount", "Money", false),
            prop("email", "String", true),
        ],
        table: customer(),
    };

    let err = bind(&cp, &cp, type_def).await.unwrap_err();
    let BindError::DoesNotConform(v) = err else {
        panic!("expected DoesNotConform, got {err:?}");
    };
    assert!(v.iter().any(|x| x.property == "phone"
        && matches!(x.reason, BindViolationReason::MissingColumn)));
    assert!(v.iter().any(|x| x.property == "id"
        && matches!(x.reason, BindViolationReason::TypeMismatch { .. })));
    assert!(v.iter().any(|x| x.property == "amount"
        && matches!(x.reason, BindViolationReason::UnknownLogicalType(_))));
    assert!(v.iter().any(|x| x.property == "email"
        && matches!(x.reason, BindViolationReason::NullabilityViolation)));

    // Nothing persisted on rejection.
    let missing = cp.get_type(&TypeName("Bad".into())).await;
    assert!(missing.is_err(), "a rejected type must not be persisted");
}

#[tokio::test]
async fn bind_rejects_an_unknown_table() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await; // empty catalog, no tables

    let type_def = ObjectType {
        name: TypeName("Ghost".into()),
        properties: vec![prop("id", "Long", true)],
        table: TableRef { schema: "main".into(), name: "ghost".into() },
    };
    let err = bind(&cp, &cp, type_def).await.unwrap_err();
    assert!(matches!(err, BindError::TableNotFound(_)), "got {err:?}");
}
```

- [ ] **Step 2: Add the BUCK test target + verify it fails**

In `src/services/ingest/BUCK` (the `load(...)` for `loom_fixture_test` is already present from the materializer slice), add:
```python
loom_fixture_test(
    name = "bind",
    crate = "bind",
    srcs = ["tests/bind.rs"],
    crate_root = "tests/bind.rs",
    duckdb = True,
    deps = [
        ":ingest",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:tokio",
    ],
)
```
```bash
buck2 test //src/services/ingest:bind > /tmp/t.log 2>&1; grep -E "error\[|cannot find|Tests finished|FAIL" /tmp/t.log
```
Expected: FAIL — `bind`/`BindError`/`BindViolationReason` not found.

- [ ] **Step 3: Implement `src/services/ingest/src/bind.rs`**

```rust
//! dataset->model binding: validate a landed DuckLake table's physical schema
//! against a declared ontology type, then persist it (define_type). A bound type is
//! guaranteed-serveable by the query read path. See the part-2b design doc.

use control_plane_core::{
    Catalog, ControlPlaneError, ObjectType, Ontology, TableRef, UnknownLogicalType, satisfies,
};

#[derive(Debug, thiserror::Error)]
pub enum BindError {
    #[error("table not found in catalog: {0:?}")]
    TableNotFound(TableRef),
    #[error("type does not conform to the landed table: {} violation(s)", .0.len())]
    DoesNotConform(Vec<BindViolation>),
    #[error(transparent)]
    ControlPlane(#[from] ControlPlaneError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindViolation {
    pub property: String,
    pub reason: BindViolationReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindViolationReason {
    MissingColumn,
    UnknownLogicalType(String),
    TypeMismatch { logical: String, physical: String },
    NullabilityViolation, // a required property backed by a nullable column
}

/// Validate `type_def` against the physical schema of its target table, then persist
/// it via `define_type`. Collects ALL violations; persists nothing on rejection.
pub async fn bind(
    catalog: &dyn Catalog,
    ontology: &dyn Ontology,
    type_def: ObjectType,
) -> Result<(), BindError> {
    // 1. The table must be live in the catalog.
    let snap = match catalog.current_snapshot(&type_def.table).await {
        Ok(s) => s,
        Err(ControlPlaneError::NotFound(_)) => {
            return Err(BindError::TableNotFound(type_def.table.clone()));
        }
        Err(e) => return Err(BindError::ControlPlane(e)),
    };

    // 2. Its physical columns at that snapshot.
    let schema = catalog.schema(&type_def.table, snap.id).await?;

    // 3. Validate every declared property against its same-named physical column.
    //    Extra physical columns are fine — a type is a view over the table.
    let mut violations = Vec::new();
    for p in &type_def.properties {
        let Some(col) = schema.columns.iter().find(|c| c.name == p.name) else {
            violations.push(BindViolation {
                property: p.name.clone(),
                reason: BindViolationReason::MissingColumn,
            });
            continue;
        };
        match satisfies(&p.ty, &col.ty) {
            Err(UnknownLogicalType(t)) => violations.push(BindViolation {
                property: p.name.clone(),
                reason: BindViolationReason::UnknownLogicalType(t),
            }),
            Ok(false) => violations.push(BindViolation {
                property: p.name.clone(),
                reason: BindViolationReason::TypeMismatch {
                    logical: p.ty.clone(),
                    physical: col.ty.clone(),
                },
            }),
            Ok(true) => {}
        }
        if p.required && col.nullable {
            violations.push(BindViolation {
                property: p.name.clone(),
                reason: BindViolationReason::NullabilityViolation,
            });
        }
    }

    if !violations.is_empty() {
        return Err(BindError::DoesNotConform(violations));
    }

    // 4. Persist — the type is now serveable by the governed read path.
    ontology.define_type(type_def).await?;
    Ok(())
}
```

Add to `src/services/ingest/src/lib.rs`:
```rust
pub mod bind;
```
and a re-export alongside the existing ones:
```rust
pub use bind::{BindError, BindViolation, BindViolationReason, bind};
```

- [ ] **Step 4: Run the tests**

```bash
buck2 run //tools:rustfmt -- src/services/ingest/src/bind.rs src/services/ingest/src/lib.rs src/services/ingest/tests/bind.rs
buck2 test //src/services/ingest:bind > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```
Expected: PASS (3 tests). If `seed`/`get_type`/`current_snapshot` behave unexpectedly (e.g. the seeded physical type string isn't what the affinity expects), inspect the actual DuckLake `column_type` via the fixture and adjust the SQL types in `seed_customer` — do NOT weaken the assertions.

- [ ] **Step 5: Commit**

```bash
git add src/services/ingest
git commit -m "$(cat <<'EOF'
feat(ingest): dataset->model binding (validate physical schema, then define_type)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: The connective-tissue e2e (materialize → bind → read)

**Files:**
- Test: `src/services/query-api/tests/bind_read_e2e.rs`
- Modify: `src/services/query-api/BUCK`

- [ ] **Step 1: Write the test** (`tests/bind_read_e2e.rs`)

```rust
//! Connective-tissue e2e: a dataset landed by the ingest materializer, bound to an
//! ontology type by `bind`, is retrievable through the governed read path. Proves
//! loom's two layers (landing + model) meet.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{
    Acl, Action, DatasetRef, Effect, EventType, LineageEvent, ObjectType, PolicyTarget,
    PropertyDef, RoleId, RunId, SubjectId, TableRef, TypeName,
};
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use ingest::{MaterializeRequest, bind, materialize};
use object_store::local::LocalFileSystem;
use query_api::handler::{ObjectQuery, QueryDeps, Subject, read_object};
use query_api::serving::{EmbeddedDuckDb, SqlValue};
use time::OffsetDateTime;
use uuid::Uuid;

#[tokio::test(flavor = "multi_thread")]
async fn landed_then_bound_dataset_is_queryable() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;

    let table = TableRef { schema: "main".into(), name: "customer".into() };

    // 1. LAND: materialize a dataset (id int64, email varchar).
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
    let store = LocalFileSystem::new_with_prefix(writer.data_path()).unwrap();
    let lineage = LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetRef { namespace: "loom-ingest".into(), name: "main.customer".into() }],
        payload: serde_json::json!({}),
    };
    materialize(
        &cp,
        &store,
        MaterializeRequest {
            table: &table,
            schema,
            batches: &[batch],
            file_name: "loom.parquet",
            gate: None,
            lineage,
        },
    )
    .await
    .unwrap();

    // 2. BIND: a Customer type over the landed table (validated against physical schema).
    bind(
        &cp,
        &cp,
        ObjectType {
            name: TypeName("Customer".into()),
            properties: vec![
                PropertyDef { name: "id".into(), ty: "Long".into(), required: true },
                PropertyDef { name: "email".into(), ty: "EmailAddress".into(), required: false },
            ],
            table: table.clone(),
        },
    )
    .await
    .unwrap();

    // 3. GRANT a Read ACL.
    let subj = SubjectId("analyst".into());
    let role = RoleId("analysts".into());
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    cp.grant(&role, Action::Read, PolicyTarget::Type(TypeName("Customer".into())), Effect::Allow)
        .await
        .unwrap();

    // 4. READ through the governed front door.
    let eng = EmbeddedDuckDb::attach(fx.socket_path(), &db, writer.data_path())
        .await
        .unwrap();
    let deps = QueryDeps { ontology: &cp, acl: &cp, serving: &eng };
    let rows = read_object(
        &ObjectQuery { type_name: "Customer".into(), eq_filters: vec![] },
        &Subject(subj),
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(rows.columns, vec!["id".to_string(), "email".to_string()]);
    assert_eq!(rows.rows.len(), 2, "both landed rows are retrievable");
    let ids: Vec<&SqlValue> = rows.rows.iter().map(|r| &r[0]).collect();
    assert!(ids.contains(&&SqlValue::Int(1)) && ids.contains(&&SqlValue::Int(2)));
}
```

- [ ] **Step 2: Add the BUCK target + verify it fails**

In `src/services/query-api/BUCK` (the `load(...)` for `loom_fixture_test` is already present), add:
```python
loom_fixture_test(
    name = "bind-read-e2e",
    crate = "bind_read_e2e",
    srcs = ["tests/bind_read_e2e.rs"],
    crate_root = "tests/bind_read_e2e.rs",
    duckdb = True,
    deps = [
        ":query-api",
        "//src/services/ingest:ingest",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:arrow",
        "//third-party:object_store",
        "//third-party:serde_json",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```
(`//src/services/ingest:ingest` already has `visibility = ["PUBLIC"]`, so query-api's test can depend on it.)
```bash
buck2 test //src/services/query-api:bind-read-e2e > /tmp/t.log 2>&1; grep -E "error\[|cannot find|Tests finished|FAIL|panicked" /tmp/t.log
```
Expected: PASS once it compiles. If `read_object` returns zero rows, the materialized file isn't being read — confirm `EmbeddedDuckDb::attach` points at `writer.data_path()` and the materialize landed under `main/customer/`. Do NOT weaken assertions.

- [ ] **Step 3: Run + commit**

```bash
buck2 run //tools:rustfmt -- src/services/query-api/tests/bind_read_e2e.rs
buck2 test //src/services/query-api:bind-read-e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
git add src/services/query-api
git commit -m "$(cat <<'EOF'
test(query-api): e2e materialize -> bind -> governed read (connective tissue)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Full-suite verification + roadmap update

- [ ] **Step 1: Full first-party suite**

```bash
buck2 test //src/... > /tmp/all.log 2>&1; grep -E "Tests finished|FAIL" /tmp/all.log
```
Expected: all green (the prior suite was 46; this adds `logical-type`, `bind`, `bind-read-e2e` → ~49). If anything fails, STOP and report BLOCKED with the failing target.

- [ ] **Step 2: Clippy + prek**

```bash
./tools/clippy-all.sh 2>&1 | grep -iE "warning|error" | head -30
buck2 run //tools:prek -- run --all-files > /tmp/prek.log 2>&1; grep -iE "Failed|Passed" /tmp/prek.log | tail -20
```
Expected: clippy clean; prek all green. Fix minor issues, re-run, note them.

- [ ] **Step 3: Update the roadmap**

In `docs/superpowers/specs/2026-06-06-loom-roadmap.md`, under the **Ingest** bullet (which now lists part 1 and part 2a delivered), add a delivered sub-bullet for **part 2b** in the same style:
```
  - *Part 2b — dataset→model binding* ✅ DELIVERED
    (`2026-06-12-ingest-dataset-model-binding-design.md`). A validated promotion: bind a
    landed dataset to an ontology type, checking the physical schema satisfies the declared
    type (new core logical-type vocabulary: base scalars + affinities + semantic aliases)
    before define_type — so a bound type is guaranteed-serveable. Proven by an accept/reject
    matrix and a materialize→bind→read e2e.
```
Adjust the "*Later:*" line so the dataset→model binding is no longer listed as pending (the ingest service shell/endpoint + DataFusion, schema evolution, delete/compaction, GC remain). Keep edits to this one bullet. Ensure one trailing newline, no trailing whitespace.

- [ ] **Step 4: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add docs/superpowers/specs/2026-06-06-loom-roadmap.md
git commit -m "$(cat <<'EOF'
docs(roadmap): ingest part 2b (dataset->model binding) delivered

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

## Self-review notes (for the executor)

- **Spec coverage:** the `core` vocabulary (base scalars, affinities, aliases, `resolve_logical`/`satisfies`)=Task 1; the `bind` orchestrator + `BindError`/violations + validate-then-`define_type` + accept/reject matrix=Task 2; the materialize→bind→read e2e=Task 3; verification + roadmap=Task 4. The JSON-out seam is a documented non-goal (noted in the `logical_type.rs` module comment).
- **Type consistency:** `BaseType`/`resolve_logical`/`satisfies`/`UnknownLogicalType` (core), `bind`/`BindError`/`BindViolation`/`BindViolationReason` (ingest) are defined once and used verbatim downstream. `ObjectType`/`PropertyDef`/`TableRef`/`Catalog`/`Ontology`/`ColumnDef` come from `control_plane_core` unchanged.
- **Known soft spot:** the seeded DuckLake physical type strings (`BIGINT`→`int64`, etc.) are asserted to match the affinity map; if a DuckLake version spells one differently, Task 2 Step 4 says to inspect the real `column_type` and adjust the seed SQL types, never the assertions. The e2e relies on the materializer's already-proven DuckDB read-back.
