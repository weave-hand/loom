# Ingest directly into a model — `POST /models/{type}` (slice 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a governed `POST /models/{type}` ingest route that conforms an Arrow batch to a pre-existing ontology type and lands it as typed objects in one call.

**Architecture:** A new handler on the ingest router gates on `Action::Write`/`PolicyTarget::Type` (deny-by-default, no existence leak — a new ACL posture for the ingest plane, reusing `service_runtime::Subject` + `cp.acl()`), resolves the `ObjectType`, derives a conformance `ModelShape` from it (a new pure helper, the seam `gate.rs` documented), runs the existing conformance gate (422 + `violations_json` on mismatch), and lands into `otype.table` through the existing `LandingMaterializer` with **type-named** lineage. The existing `/datasets` raw-landing route is untouched.

**Tech Stack:** Rust 2024, axum, Arrow IPC, buck2; control-plane (`Acl`/`Ontology`/`ControlPlane` traits), `service_runtime` (auth middleware already wired in ingest's `main.rs`), `engine_serving::execute_query` (test read-back), hermetic Postgres fixture (`loom_fixture_test`).

## Global Constraints

- **Tests are `rust_test` integration targets only — never inline `#[cfg(test)]`.** Put unit tests in `tests/<name>.rs` wired as their own target. The `no-inline-tests` prek hook fails on any `#[test]`/`#[tokio::test]` in `src/**.rs`.
- **Fixture (Postgres-backed) tests use the `loom_fixture_test` macro**, not bare `rust_test` — they boot `initdb`/`postgres` (refuse to run as root on RE), so the macro pins the test command local. **These cannot be run locally in this cloud session (root); they are verified by CI's `affected` job.** Pure-logic tests use `rust_test` and run on RE.
- **Don't pipe long-running `buck2 test`/`bxl` through `tail`/`head`** — redirect to a file and grep: `buck2 test … > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`. `buck2 build … | tail` is fine.
- **Strict clippy** (`pedantic` + `restriction`, enforced panic-safety set: `unwrap_used`, `expect_used`, `indexing_slicing`, `panic`, …) applies to production lib/bin code. Test code is exempted from the panic-safety lints via `loom_rust_test`/`loom_fixture_test`. Use `#[expect(lint, reason = "…")]` for local allows in production code (bare `#[allow]` needs a `reason`).
- **Behavior-preserving for `/datasets`:** the existing raw-landing route, handler (`land`), and `IcebergMaterializer` are not modified.
- **Append-only:** rows append; no identity-dedup/upsert this slice. Conformance only checks the identity *property* is present and required (via `ModelShape`).
- **Markdown lint:** any `.md` file must end with exactly one trailing newline and have no trailing whitespace (`end-of-file-fixer` / `trim trailing whitespace` hooks run on all files).

---

## File Structure

- **Create** `src/services/ingest/src/model.rs` — the `model_shape_from_type(&ObjectType) -> ModelShape` pure helper. Lives in its own module (not `gate.rs`) so `gate` stays ontology-free, as its module doc promises.
- **Modify** `src/services/ingest/src/lib.rs` — declare `pub mod model;` and re-export `model_shape_from_type`.
- **Modify** `src/services/ingest/src/gate.rs` — update the seam comment to point at the new `model` module (docs accuracy only; no code change).
- **Modify** `src/services/ingest/src/http.rs` — add the `/models/:type` route and the `land_model` handler.
- **Modify** `src/services/ingest/BUCK` — add `//src/services/runtime:runtime` to the `ingest` **library** deps (for `service_runtime::Subject`); add the `model` (pure) and `http-model` (fixture) test targets.
- **Create** `src/services/ingest/tests/model.rs` — unit tests for `model_shape_from_type` (pure, RE).
- **Create** `src/services/ingest/tests/http_model.rs` — the 4-case e2e fixture test.
- **Modify** `docs/ROADMAP.md` — close `[[road-ingest-into-model]]` at PR time (Task 5).

---

### Task 1: `model_shape_from_type` pure helper

Derive a conformance `ModelShape` from an ontology `ObjectType`: one `ColumnShape` per declared property; a property is `required` if it is non-nullable **or** it is the type's declared `identity`.

**Files:**
- Create: `src/services/ingest/src/model.rs`
- Modify: `src/services/ingest/src/lib.rs` (add `pub mod model;` + re-export)
- Modify: `src/services/ingest/src/gate.rs:1-9` (seam comment)
- Modify: `src/services/ingest/BUCK` (add `model` rust_test target)
- Test: `src/services/ingest/tests/model.rs`

**Interfaces:**
- Consumes: `control_plane_core::{ObjectType, PropertyDef}` (`ObjectType { name: TypeName, properties: Vec<PropertyDef>, derived: Vec<DerivedPropertyDef>, table: TableRef, identity: Option<String> }`; `PropertyDef { name: String, ty: String, required: bool }`); `crate::gate::{ColumnShape, ModelShape}` (`ColumnShape { name: String, ty: String, required: bool }`; `ModelShape { columns: Vec<ColumnShape> }`).
- Produces: `pub fn model_shape_from_type(ty: &control_plane_core::ObjectType) -> crate::gate::ModelShape` — re-exported as `ingest::model_shape_from_type`.

- [ ] **Step 1: Write the failing test**

Create `src/services/ingest/tests/model.rs`:

```rust
//! Unit tests for `model_shape_from_type`: ObjectType -> conformance ModelShape.

use control_plane_core::{ObjectType, PropertyDef, TableRef, TypeName};
use ingest::{ColumnShape, ModelShape, model_shape_from_type};

fn ty(properties: Vec<PropertyDef>, identity: Option<&str>) -> ObjectType {
    ObjectType {
        name: TypeName("Thing".into()),
        properties,
        derived: vec![],
        table: TableRef {
            schema: "main".into(),
            name: "thing".into(),
        },
        identity: identity.map(Into::into),
    }
}

#[test]
fn maps_one_columnshape_per_property_in_order() {
    let ot = ty(
        vec![
            PropertyDef {
                name: "a".into(),
                ty: "long".into(),
                required: true,
            },
            PropertyDef {
                name: "b".into(),
                ty: "string".into(),
                required: false,
            },
        ],
        None,
    );
    assert_eq!(
        model_shape_from_type(&ot),
        ModelShape {
            columns: vec![
                ColumnShape {
                    name: "a".into(),
                    ty: "long".into(),
                    required: true,
                },
                ColumnShape {
                    name: "b".into(),
                    ty: "string".into(),
                    required: false,
                },
            ],
        }
    );
}

#[test]
fn identity_property_is_required_even_when_property_required_is_false() {
    let ot = ty(
        vec![
            PropertyDef {
                name: "id".into(),
                ty: "long".into(),
                required: false,
            },
            PropertyDef {
                name: "name".into(),
                ty: "string".into(),
                required: false,
            },
        ],
        Some("id"),
    );
    assert_eq!(
        model_shape_from_type(&ot),
        ModelShape {
            columns: vec![
                ColumnShape {
                    name: "id".into(),
                    ty: "long".into(),
                    required: true,
                },
                ColumnShape {
                    name: "name".into(),
                    ty: "string".into(),
                    required: false,
                },
            ],
        }
    );
}
```

- [ ] **Step 2: Wire the BUCK target and lib module so the test builds and fails**

In `src/services/ingest/src/lib.rs`, add the module declaration after `pub mod materialize;`:

```rust
pub mod model;
```

and add the re-export after `pub use materialize::{MaterializeRequest, materialize};`:

```rust
pub use model::model_shape_from_type;
```

Create `src/services/ingest/src/model.rs` with a stub that compiles but is wrong (forces a red test):

```rust
//! Derive a conformance [`ModelShape`](crate::gate::ModelShape) from an ontology
//! `ObjectType`. This fills the seam `gate` documents: the gate stays ontology-free
//! (it takes a plain `ModelShape`); the `ObjectType` -> `ModelShape` derivation lives
//! here so the conversion's ontology dependency does not leak into the gate.

use control_plane_core::ObjectType;

use crate::gate::ModelShape;

/// One `ColumnShape` per declared property, in declaration order. A property is
/// `required` if it is non-nullable **or** it is the type's declared `identity`
/// (the identity must always be present for the row to be addressable).
#[must_use]
pub fn model_shape_from_type(ty: &ObjectType) -> ModelShape {
    let _ = ty;
    ModelShape { columns: vec![] }
}
```

In `src/services/ingest/BUCK`, add after the `gate` test target (around line 104):

```python
# ObjectType -> ModelShape derivation — pure logic, RE-eligible (no fixture).
rust_test(
    name = "model",
    crate = "model",
    srcs = ["tests/model.rs"],
    crate_root = "tests/model.rs",
    edition = "2024",
    deps = [
        ":ingest",
        "//src/control-plane/core:core",
    ],
)
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `buck2 test //src/services/ingest:model > /tmp/model.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/model.log`
Expected: both tests FAIL (the stub returns an empty `columns` vec, so the `assert_eq!`s mismatch).

- [ ] **Step 4: Implement the helper**

Replace the body of `model_shape_from_type` in `src/services/ingest/src/model.rs`:

```rust
//! Derive a conformance [`ModelShape`](crate::gate::ModelShape) from an ontology
//! `ObjectType`. This fills the seam `gate` documents: the gate stays ontology-free
//! (it takes a plain `ModelShape`); the `ObjectType` -> `ModelShape` derivation lives
//! here so the conversion's ontology dependency does not leak into the gate.

use control_plane_core::ObjectType;

use crate::gate::{ColumnShape, ModelShape};

/// One `ColumnShape` per declared property, in declaration order. A property is
/// `required` if it is non-nullable **or** it is the type's declared `identity`
/// (the identity must always be present for the row to be addressable).
#[must_use]
pub fn model_shape_from_type(ty: &ObjectType) -> ModelShape {
    let identity = ty.identity.as_deref();
    ModelShape {
        columns: ty
            .properties
            .iter()
            .map(|p| ColumnShape {
                name: p.name.clone(),
                ty: p.ty.clone(),
                required: p.required || identity == Some(p.name.as_str()),
            })
            .collect(),
    }
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `buck2 test //src/services/ingest:model > /tmp/model.log 2>&1; grep -E "Tests finished|FAIL" /tmp/model.log`
Expected: `Tests finished: Pass 2. Fail 0.`

- [ ] **Step 6: Update the gate seam comment (docs accuracy)**

In `src/services/ingest/src/gate.rs`, change lines 1-5. Old:

```rust
//! The optional model-conformance gate ("this data is this model"). Takes a plain
//! `ModelShape` value — NOT the ontology — so this crate stays ontology-free; a
//! later slice derives a `ModelShape` from an `ObjectType`. This slice ships the
//! seam plus a minimal check (required columns present, types match). Richer
//! constraints (ranges, regex, coercion) extend `ViolationReason`.
```

New:

```rust
//! The optional model-conformance gate ("this data is this model"). Takes a plain
//! `ModelShape` value — NOT the ontology — so this gate stays ontology-free; the
//! `model` module (`model_shape_from_type`) derives a `ModelShape` from an
//! `ObjectType` for the `POST /models/{type}` path. This ships the seam plus a
//! minimal check (required columns present, types match). Richer constraints
//! (ranges, regex, coercion) extend `ViolationReason`.
```

- [ ] **Step 7: Lint and commit**

Run: `buck2 build '//src/services/ingest:ingest[clippy.txt]' > /tmp/clippy.log 2>&1; cat $(buck2 build --show-output '//src/services/ingest:ingest[clippy.txt]' 2>/dev/null | awk '{print $2}')` — expected: empty (clean). If clippy output is non-empty, fix the lints.

```bash
git add src/services/ingest/src/model.rs src/services/ingest/src/lib.rs src/services/ingest/src/gate.rs src/services/ingest/tests/model.rs src/services/ingest/BUCK
git commit -m "feat(ingest): derive ModelShape from ObjectType (model_shape_from_type)"
```

---

### Task 2: `POST /models/{type}` route + governed `land_model` handler

Add the route and handler. Auth is already wired in ingest's `main.rs` (`service_runtime::protect(router(...), auth_state)`), so the new route is automatically behind `require_auth` and the `Subject` extractor works.

**Files:**
- Modify: `src/services/ingest/src/http.rs`
- Modify: `src/services/ingest/BUCK` (add `//src/services/runtime:runtime` to the `ingest` library deps)

**Interfaces:**
- Consumes: `service_runtime::Subject` (`pub struct Subject(pub SubjectId)`, an axum `FromRequestParts` extractor); `crate::model::model_shape_from_type`; `crate::materialize::resolve_columns`; `crate::landing::{LandRequest, LandingMaterializer}`; `AppState { materializer: Arc<dyn LandingMaterializer>, cp: Arc<dyn ControlPlane> }`; control-plane `ControlPlane::{acl, ontology}`, `Acl::check(&SubjectId, Action, &PolicyTarget) -> Result<Decision>`, `Ontology::get_type(&TypeName) -> Result<ObjectType>`; `control_plane_core::{Action, Decision, PolicyTarget, TypeName, ControlPlaneError, DatasetRef, LineageEvent, EventType, RunId}`; `LandingMaterializer::land(LandRequest<'_>) -> Result<SnapshotId, IngestError>`.
- Produces: `async fn land_model(State<AppState>, Path<String>, Subject, Bytes) -> Response` registered at `POST /models/:type`. Success body: `{"snapshot_id": <i64>, "type": "<name>"}`. Errors: 403 (deny or unknown type), 422 (`{"violations":[…]}`), 400 (bad IPC / unsupported column), 500 (opaque backend fault).

- [ ] **Step 1: Add the runtime dep to the ingest library BUCK**

In `src/services/ingest/BUCK`, in the `rust_library(name = "ingest", …)` `deps` list, add `"//src/services/runtime:runtime",` (keep the list alphabetical-ish; place it before the `//third-party:` entries):

```python
    deps = [
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//src/loom-config:loom-config",
        "//src/services/datafusion-io:datafusion-io",
        "//src/services/runtime:runtime",
        "//third-party:arrow",
        "//third-party:async-trait",
        "//third-party:axum",
        "//third-party:iceberg",
        "//third-party:object_store",
        "//third-party:serde",
        "//third-party:serde_json",
        "//third-party:sqlx",
        "//third-party:thiserror",
        "//third-party:time",
        "//third-party:uuid",
    ],
```

(`service_runtime` does not depend on `ingest`, so there is no dependency cycle.)

- [ ] **Step 2: Add the route**

In `src/services/ingest/src/http.rs`, add the route to `router` (after the `/datasets` line):

```rust
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/datasets/:schema/:table", post(land))
        .route("/models/:type", post(land_model))
        .route("/tables/:schema/:table/compact", post(compact))
        .with_state(state)
}
```

- [ ] **Step 3: Add imports**

Extend the `control_plane_core` import block in `http.rs` to add the ACL/ontology types and traits, and add the two `use` lines below it. The block becomes:

```rust
use control_plane_core::{
    Acl, Action, COMPACT_JOB_KIND, CompactJob, ControlPlane, ControlPlaneError, DatasetId,
    DatasetRef, Decision, EventType, LineageEvent, NewJob, Ontology, PolicyTarget, RunId, TableRef,
    TypeName,
};
```

and after `use crate::materialize::resolve_columns;`:

```rust
use crate::model::model_shape_from_type;
use service_runtime::Subject;
```

> Note for the implementer: `Acl` and `Ontology` are imported because `.check()`/`.get_type()` are trait methods called on `dyn` objects (mirroring `query-api/src/handler.rs`). If `buck2 build //src/services/ingest:ingest` reports either as an unused import, remove it; if it reports a missing-method/trait error, it is needed. Resolve against the compiler — the library builds on RE without Postgres, so this is fast local feedback.

- [ ] **Step 4: Add the handler**

Append the `land_model` handler to `http.rs` (after `land`):

```rust
/// Governed model ingest: conform an Arrow batch to a pre-existing ontology type
/// and land it as typed objects into the type's table. Authorize before landing
/// (deny-by-default, no existence leak); a denied write never reaches the store.
async fn land_model(
    State(st): State<AppState>,
    Path(type_name): Path<String>,
    subject: Subject,
    body: Bytes,
) -> Response {
    let type_name = TypeName(type_name);

    // 1. Coarse ACL gate BEFORE the type is resolved: a missing grant — including an
    //    unknown/anonymous subject — is 403, returned before we reveal whether the
    //    type exists. First Write use on the ingest plane.
    match st
        .cp
        .acl()
        .check(&subject.0, Action::Write, &PolicyTarget::Type(type_name.clone()))
        .await
    {
        Ok(Decision::Allow) => {}
        Ok(Decision::Deny) => return StatusCode::FORBIDDEN.into_response(),
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    }

    // 2. Resolve the model. A granted-but-nonexistent type is still 403 (no leak),
    //    distinct from a 404.
    let otype = match st.cp.ontology().get_type(&type_name).await {
        Ok(t) => t,
        Err(ControlPlaneError::NotFound(_)) => return StatusCode::FORBIDDEN.into_response(),
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    };

    // 3. Derive the conformance shape from the type (the gate seam).
    let shape = model_shape_from_type(&otype);

    // 4. Decode the Arrow IPC body.
    let (schema, batches) = match decode_ipc(&body) {
        Ok(sb) => sb,
        Err(_) => return (StatusCode::BAD_REQUEST, "invalid arrow ipc stream").into_response(),
    };

    // 5. Gate + resolve the physical schema (422 + violations on mismatch).
    let columns = match resolve_columns(&schema, Some(&shape)) {
        Ok(c) => c,
        Err(IngestError::DoesNotConform(violations)) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(violations_json(&violations)),
            )
                .into_response();
        }
        Err(IngestError::Infer(_)) => {
            return (StatusCode::BAD_REQUEST, "unsupported column type").into_response();
        }
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    };

    // 6. Land into the type's table with type-named lineage (rows trace to the model).
    let table = otype.table.clone();
    let type_label = type_name.0.clone();
    let file_prefix = Uuid::new_v4().to_string();
    let lineage = LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetRef::from(&type_name)],
        payload: serde_json::json!({ "source": "http-model", "type": type_label }),
    };
    let req = LandRequest {
        table: &table,
        schema: schema.clone(),
        columns: &columns,
        batches: &batches,
        ipc_body: body.as_ref(),
        file_prefix: &file_prefix,
        lineage,
    };

    match st.materializer.land(req).await {
        Ok(snap) => Json(serde_json::json!({
            "snapshot_id": snap.0,
            "type": type_label,
        }))
        .into_response(),
        Err(IngestError::DoesNotConform(violations)) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(violations_json(&violations)),
        )
            .into_response(),
        Err(IngestError::Infer(_)) => {
            (StatusCode::BAD_REQUEST, "unsupported column type").into_response()
        }
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    }
}
```

> Note: `DatasetId` stays imported (the existing `land` handler uses it); `DatasetRef` is the element type of `LineageEvent::outputs` and `impl From<&TypeName> for DatasetRef` produces the `loom:type`-namespaced ref.

- [ ] **Step 5: Build and lint**

Run: `buck2 build //src/services/ingest:ingest > /tmp/build.log 2>&1; grep -E "BUILD SUCCEEDED|error\[|error:" /tmp/build.log`
Expected: `BUILD SUCCEEDED` (fix any compiler errors, especially the trait-import note in Step 3).

Run clippy: `buck2 build '//src/services/ingest:ingest[clippy.txt]' 2>/dev/null; cat "$(buck2 build --show-output '//src/services/ingest:ingest[clippy.txt]' 2>/dev/null | awk '{print $2}')"`
Expected: empty output (clean). Fix any lints (e.g. add `#[expect(…, reason="…")]` only if genuinely warranted).

- [ ] **Step 6: Commit**

```bash
git add src/services/ingest/src/http.rs src/services/ingest/BUCK
git commit -m "feat(ingest): governed POST /models/{type} typed-land route"
```

---

### Task 3: e2e fixture test for `POST /models/{type}`

The four spec cases over the hermetic-Postgres + temp-warehouse harness, driving the **protected** router with a minted bearer token. Proves the conforming land round-trips through the engine serving path.

**Files:**
- Create: `src/services/ingest/tests/http_model.rs`
- Modify: `src/services/ingest/BUCK` (add the `http-model` `loom_fixture_test` target)

**Interfaces:**
- Consumes: `ingest::http::{AppState, router}`; `ingest::landing::IcebergMaterializer`; `service_runtime::{control_plane, protect, AuthState, generate_session_token, token_sha256, hash_password}`; `control_plane_postgres::PgControlPlane` (implements `ControlPlane` + `Auth` + `Acl` + `Ontology`); `control_plane_postgres::fixture::PgFixture`; `control_plane_postgres::iceberg_catalog::IcebergCatalog`; `engine_serving::execute_query(&IcebergCatalog, &str, None) -> Result<Vec<RecordBatch>, _>`; control-plane `Auth::{create_user, create_session}`, `Acl::{define_subject, define_role, assign_role, grant, check}`, `Ontology::define_type`, types `{NewUser, ObjectType, PropertyDef, SubjectId, RoleId, Action, PolicyTarget, TypeName, Effect, TableRef}`.
- Produces: the `http-model` test target (CI-verified).

- [ ] **Step 1: Write the test file**

Create `src/services/ingest/tests/http_model.rs`:

```rust
//! Hermetic `POST /models/{type}` e2e: define an ontology type, POST a conforming
//! Arrow batch as a Write-granted subject through the *protected* router, and prove
//! the rows land into the type's table and read back through the engine serving
//! path. Plus the 422 (non-conforming), 403 (ACL deny), and 403 (unknown type — no
//! existence leak) paths.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use axum::Router;
use axum::body::Body;
use axum::http::header::AUTHORIZATION;
use axum::http::{Request, StatusCode};
use control_plane_core::{
    Acl, Action, Auth, Catalog, ControlPlaneError, Effect, NewUser, ObjectType, Ontology,
    PolicyTarget, PropertyDef, RoleId, SubjectId, TableRef, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalogBuilder,
};
use http_body_util::BodyExt;
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;
use ingest::http::{AppState, router};
use ingest::landing::IcebergMaterializer;
use service_runtime::{AuthState, generate_session_token, protect, token_sha256};
use sqlx::PgPool;
use time::OffsetDateTime;
use tower::ServiceExt;

/// A 2-row batch matching the `Thing` model: id: Int64 (required), name: Utf8.
fn sample_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ],
    )
    .unwrap()
}

/// Encode a batch as an Arrow IPC stream.
fn ipc_bytes(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    {
        let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();
        w.write(batch).unwrap();
        w.finish().unwrap();
    }
    buf
}

/// Build an Iceberg-backed `AppState` over the fixture db + temp warehouse, returning
/// the concrete `PgControlPlane` (needed for auth/ACL/ontology setup) and the pool.
async fn app_state(
    fx: &PgFixture,
    db: &str,
) -> (Arc<PgControlPlane>, PgPool, tempfile::TempDir, AppState) {
    let pool = fx.pool_for(db).await;
    let wh = tempfile::tempdir().unwrap();
    let mut props = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), fx.pg_dsn(db));
    props.insert(
        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
        format!("file://{}", wh.path().display()),
    );
    let catalog = SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("loom", props)
        .await
        .expect("catalog");
    let pg = Arc::new(service_runtime::control_plane(
        pool.clone(),
        Duration::from_millis(300),
    ));
    let state = AppState {
        materializer: Arc::new(IcebergMaterializer {
            catalog: Arc::new(catalog),
            pool: pool.clone(),
            inline_byte_limit: 16 * 1024 * 1024,
            flush_byte_threshold: 64 * 1024 * 1024,
        }),
        cp: pg.clone(),
    };
    (pg, pool, wh, state)
}

/// Wrap the router with the auth gate, exactly as the binary does.
fn protected(state: AppState, pg: Arc<PgControlPlane>) -> Router {
    protect(
        router(state),
        AuthState {
            auth: pg,
            session_ttl: Duration::from_secs(3600),
        },
    )
}

/// Create the user (ensures the ACL subject exists) and mint a live bearer token —
/// no password flow.
async fn session_token(pg: &PgControlPlane, subject: &str) -> String {
    let phc = service_runtime::hash_password("e2e-password").expect("hash");
    match pg
        .create_user(&NewUser {
            subject_id: SubjectId(subject.into()),
            username: subject.into(),
            password_phc: phc,
        })
        .await
    {
        Ok(()) | Err(ControlPlaneError::Conflict(_)) => {}
        Err(e) => panic!("create_user({subject}): {e}"),
    }
    let token = generate_session_token();
    let expires = OffsetDateTime::now_utc() + time::Duration::hours(1);
    pg.create_session(&SubjectId(subject.into()), &token_sha256(&token), expires)
        .await
        .expect("create_session");
    token
}

/// Grant Write + Read on `type_name` to `subject` via a fresh role.
async fn grant_write(pg: &PgControlPlane, subject: &str, type_name: &str) {
    let subj = SubjectId(subject.into());
    let role = RoleId(format!("{subject}-role"));
    pg.define_subject(&subj).await.unwrap();
    pg.define_role(&role).await.unwrap();
    pg.assign_role(&subj, &role).await.unwrap();
    pg.grant(
        &role,
        Action::Write,
        PolicyTarget::Type(TypeName(type_name.into())),
        Effect::Allow,
    )
    .await
    .unwrap();
    pg.grant(
        &role,
        Action::Read,
        PolicyTarget::Type(TypeName(type_name.into())),
        Effect::Allow,
    )
    .await
    .unwrap();
}

/// Define a `Thing` model (id: long identity, name: string) over `table`.
async fn define_thing(pg: &PgControlPlane, type_name: &str, table: TableRef) {
    pg.define_type(ObjectType {
        name: TypeName(type_name.into()),
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "long".into(),
                required: true,
            },
            PropertyDef {
                name: "name".into(),
                ty: "string".into(),
                required: false,
            },
        ],
        derived: vec![],
        table,
        identity: Some("id".into()),
    })
    .await
    .unwrap();
}

/// Drive a `POST /models/{type}` request with a bearer token; return (status, body).
async fn post_model(
    app: Router,
    type_name: &str,
    token: &str,
    body: Vec<u8>,
) -> (StatusCode, serde_json::Value) {
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/models/{type_name}"))
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let json = if bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
    };
    (status, json)
}

fn thing_table() -> TableRef {
    TableRef {
        schema: "main".into(),
        name: "thing".into(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn conforming_land_into_model_round_trips() {
    let fx = PgFixture::start();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, _wh, state) = app_state(&fx, &db).await;
    define_thing(&pg, "Thing", thing_table()).await;
    grant_write(&pg, "alice", "Thing").await;
    let token = session_token(&pg, "alice").await;

    let app = protected(state, pg.clone());
    let (status, json) = post_model(app, "Thing", &token, ipc_bytes(&sample_batch())).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["type"], "Thing");
    json["snapshot_id"]
        .as_i64()
        .expect("snapshot_id is an integer");

    // Round-trip: the landed rows are readable through the engine serving path.
    let catalog = IcebergCatalog::new(pool.clone());
    let batches = engine_serving::execute_query(
        &catalog,
        "SELECT \"id\", \"name\" FROM \"main\".\"thing\" ORDER BY \"id\"",
        None,
    )
    .await
    .expect("serving read");
    let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, 2, "two landed rows are servable as the model");
    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id column is Int64");
    assert_eq!(ids.value(0), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn nonconforming_is_422_and_nothing_lands() {
    let fx = PgFixture::start();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, _wh, state) = app_state(&fx, &db).await;
    define_thing(&pg, "Thing", thing_table()).await;
    grant_write(&pg, "alice", "Thing").await;
    let token = session_token(&pg, "alice").await;

    // Batch missing the required identity property "id" (only "name").
    let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["x"]))]).unwrap();

    let app = protected(state, pg.clone());
    let (status, json) = post_model(app, "Thing", &token, ipc_bytes(&batch)).await;

    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(json["violations"][0]["column"], "id");
    assert_eq!(json["violations"][0]["reason"], "missing_required");

    assert!(
        IcebergCatalog::new(pool.clone())
            .current_snapshot(&thing_table())
            .await
            .is_err(),
        "a rejected land writes no catalog rows"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn acl_deny_is_403_and_nothing_lands() {
    let fx = PgFixture::start();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, _wh, state) = app_state(&fx, &db).await;
    define_thing(&pg, "Thing", thing_table()).await;
    // Authenticated subject, but NO Write grant.
    let token = session_token(&pg, "mallory").await;

    let app = protected(state, pg.clone());
    let (status, _json) = post_model(app, "Thing", &token, ipc_bytes(&sample_batch())).await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(
        IcebergCatalog::new(pool.clone())
            .current_snapshot(&thing_table())
            .await
            .is_err(),
        "a denied write lands nothing"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_type_with_grant_is_403_no_leak() {
    let fx = PgFixture::start();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, _pool, _wh, state) = app_state(&fx, &db).await;
    // Grant Write on a type that is never defined.
    grant_write(&pg, "alice", "Ghost").await;
    let token = session_token(&pg, "alice").await;

    let app = protected(state, pg.clone());
    let (status, _json) = post_model(app, "Ghost", &token, ipc_bytes(&sample_batch())).await;

    // get_type NotFound after the Write grant resolves to 403, not a 404 — no leak.
    assert_eq!(status, StatusCode::FORBIDDEN);
}
```

- [ ] **Step 2: Add the BUCK target**

In `src/services/ingest/BUCK`, add after the `http-land` target:

```python
loom_fixture_test(
    name = "http-model",
    crate = "http_model",
    srcs = ["tests/http_model.rs"],
    crate_root = "tests/http_model.rs",
    deps = [
        ":ingest",
        "//src/services/runtime:runtime",
        "//src/services/engine-serving:engine-serving",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:arrow",
        "//third-party:axum",
        "//third-party:http-body-util",
        "//third-party:iceberg",
        "//third-party:serde_json",
        "//third-party:sqlx",
        "//third-party:tempfile",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:tower",
    ],
)
```

- [ ] **Step 3: Build the test target (compile-check — it cannot RUN locally as root)**

Run: `buck2 build //src/services/ingest:http-model > /tmp/httpmodel.log 2>&1; grep -E "BUILD SUCCEEDED|error\[|error:" /tmp/httpmodel.log`
Expected: `BUILD SUCCEEDED`. Fix any compile errors (import names, field names, the `execute_query` arg shape). Reference `src/services/engine-serving/tests/execute_query_e2e.rs` for the exact `execute_query` + quoted-identifier SQL pattern and `src/services/query-api/tests/e2e_support.rs` (`session_token`, `grant_writer`) for the auth-helper shapes.

> The fixture test boots Postgres and **refuses to run as root**, so it cannot be executed in this cloud session — only built. It is verified by CI's `affected` job on the PR. Do not attempt `buck2 test //src/services/ingest:http-model` here.

- [ ] **Step 4: Commit**

```bash
git add src/services/ingest/tests/http_model.rs src/services/ingest/BUCK
git commit -m "test(ingest): e2e for POST /models/{type} (conform+land, 422, 403)"
```

---

### Task 4: Whole-crate build + lint gate

A final mechanical gate before the docs/PR step: the entire ingest crate (lib, bin, all test targets) builds and lints clean.

**Files:** none (verification only).

- [ ] **Step 1: Build the whole ingest tree**

Run: `buck2 build //src/services/ingest/... > /tmp/all.log 2>&1; grep -E "BUILD SUCCEEDED|error\[|error:" /tmp/all.log`
Expected: `BUILD SUCCEEDED`.

- [ ] **Step 2: Clippy across first-party Rust**

Run: `bash tools/clippy-all.sh > /tmp/clippyall.log 2>&1; grep -iE "clippy|error|warning" /tmp/clippyall.log | head`
Expected: no clippy findings introduced by this change (the script treats empty `[clippy.txt]` as clean).

- [ ] **Step 3: Run the pure-logic tests that can run locally (RE)**

Run: `buck2 test //src/services/ingest:model > /tmp/model.log 2>&1; grep -E "Tests finished|FAIL" /tmp/model.log`
Expected: `Tests finished: Pass 2. Fail 0.`

(The fixture tests — `http-model`, `http-land`, etc. — are CI-verified, not run here.)

---

### Task 5: Close the register item (at PR time)

Mark `[[road-ingest-into-model]]` done. Per `loom-work-checkout`, this happens in the PR via `loom-docs-update`.

**Files:**
- Modify: `docs/ROADMAP.md` (the `road-ingest-into-model` entry)

- [ ] **Step 1: Flip the checkbox and tags**

In `docs/ROADMAP.md`, on the `road-ingest-into-model` entry: change `- [ ]` to `- [x]`, set `status:planned` → `status:done`, and set `pr:-` → `pr:#<N>` (the opened PR number). Validate: `bash tools/docs.sh validate` (expected: no errors).

- [ ] **Step 2: Commit**

```bash
git add docs/ROADMAP.md
git commit -m "docs(roadmap): close road-ingest-into-model"
```

---

## Self-Review

**1. Spec coverage:**
- *Auth + ACL (deny-by-default, no leak)* → Task 2 Step 4 (coarse `Action::Write` gate before resolve; `NotFound` → 403). Tests: Task 3 `acl_deny_is_403…`, `unknown_type_with_grant_is_403_no_leak`.
- *Resolve the model; land target = `otype.table`* → Task 2 Step 4 (`get_type`, `otype.table`).
- *Derive `ModelShape` from `ObjectType` (the gate seam)* → Task 1 (`model_shape_from_type`, identity-is-required rule). Tests: Task 1 both cases.
- *Gate the batch; 422 + `violations_json`* → Task 2 Step 4 (`resolve_columns(_, Some(&shape))`). Test: Task 3 `nonconforming_is_422…`.
- *Land into `otype.table` with type-named lineage; return `snapshot_id`* → Task 2 Step 4 (`DatasetRef::from(&type_name)`, `payload.source = "http-model"`, success body). Test: Task 3 `conforming_land_into_model_round_trips` (round-trip read-back proves "landed as the model").
- *`POST /models/:type` route; Arrow IPC decode reused* → Task 2 Steps 2-4 (`decode_ipc` reused).
- *Subject extraction + Write ACL gate new to ingest (reuse `service_runtime::Subject` + `cp.acl()`)* → Task 2 Step 1 (library `runtime` dep), Step 4. Auth middleware already wired in `main.rs` (no change).
- *Out of scope honored:* no inference, no upsert, `/datasets` untouched — confirmed (no edits to `land`/`IcebergMaterializer`).

**2. Placeholder scan:** No TBD/TODO/"handle errors"/"similar to" — every step has full code or an exact command. The one judgement call (success body shape) is decided: `{"snapshot_id", "type"}` (snapshot_id is the field the spec names; `type` replaces `/datasets`' `dataset`).

**3. Type consistency:** `model_shape_from_type(&ObjectType) -> ModelShape` is defined identically in Task 1's interface, the helper, and consumed in Task 2 Step 4. `ColumnShape`/`ModelShape`/`PropertyDef`/`ObjectType` field names match the source (`name`/`ty`/`required`; `properties`/`derived`/`table`/`identity`). `Acl::check(&SubjectId, Action, &PolicyTarget) -> Result<Decision>`, `Ontology::get_type(&TypeName) -> Result<ObjectType>`, `LandRequest`'s field set, and the auth-helper signatures (`create_user(&NewUser)`, `create_session(&SubjectId, &[u8;32], OffsetDateTime)`, `grant(&RoleId, Action, PolicyTarget, Effect)`) all match the verified sources. `engine_serving::execute_query(&IcebergCatalog, &str, Option<&(String, Arc<dyn ObjectStore>)>)` called with `None`.
