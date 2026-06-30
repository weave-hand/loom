# Ingest model inference (`POST /models/{type}` slice 2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete the auto-detect `POST /models/{type}` endpoint by building the **type-absent** branch: when an authorized subject POSTs an Arrow batch to a type that does not exist, infer an `ObjectType` from the batch schema, `define_type` it, and land the rows — instead of slice 1's 403 dead-end.

**Architecture:** Add a pure helper `infer_object_type` (the reverse of slice 1's `model_shape_from_type` seam) that maps an Arrow schema to an `ObjectType` using the *existing* `datafusion_io::arrow_logical_type` mapping. Wire it into the `land_model` handler: decode the IPC body up front, then `get_type` — present ⇒ slice-1 conform-and-land (unchanged); absent ⇒ infer → `define_type` → **re-resolve** → fall through to the same conform-and-land tail. The re-resolve is the create-or-conform race guard (`define_type` is an upsert, so a concurrent first-batch race resolves to one stored type; the loser conforms against it and 422s if its batch differs).

**Tech Stack:** Rust 2024, axum (HTTP), arrow (IPC + schema), buck2 (`rust_test` / `loom_fixture_test` targets), sqlx (runtime query in the e2e test for the direct grant seed), control-plane `Ontology`/`Acl` traits.

## Global Constraints

- **Tests are `rust_test` integration targets only** — never inline `#[cfg(test)]`. New pure-logic test → `rust_test` (RE-eligible); new fixture/e2e test → `loom_fixture_test`. (CLAUDE.md Testing.)
- **No new ACL surface / no ACL semantics change.** `grant`'s existing type-existence validation and the testkit ACL contract stay untouched (spec: a coarse ontology-authoring capability is deferred). The infer-and-create test therefore **seeds the Write grant on the absent type by direct row insertion** via runtime sqlx, not the validating `grant` API.
- **Behavior-preserving for the type-present branch and for `/datasets`** — slice 1's conform-and-land path and the raw-land path are unchanged.
- **No-leak ACL posture preserved.** The coarse `acl.check(subject, Write, Type(name))` gate runs first and unchanged; a subject without the grant gets 403 whether or not the type exists. The existing `unknown_type_is_403_no_leak` test stays green unmodified (alice has no grant on `Ghost`, so the gate 403s before `get_type`).
- **Append-only.** No identity dedup/upsert; the declared identity only sets the property (`required` + recorded on the type).
- **Strict clippy** (pedantic + restriction on lib/bin): no `unwrap`/`expect`/`panic`/indexing in `src/**`; tests are exempt from panic-safety lints via the `loom_rust_test`/`loom_fixture_test` wrappers.
- **Inferred type → table convention:** `main.<TypeName>` (schema `"main"` — the repo-wide default; table name = the type name verbatim, lossless and deterministic). Serving reads back whatever is stored on the type, so any deterministic choice round-trips.
- **Reuse the existing Arrow→logical-type mapping** (`datafusion_io::arrow_logical_type`) wholesale — no new type-mapping table.

---

### Task 1: `infer_object_type` pure helper + `InferTypeError`

Build the reverse of `model_shape_from_type`: Arrow `Schema` → `ObjectType`. Pure, no I/O, unit-tested as a `rust_test`.

**Files:**
- Modify: `src/services/ingest/src/model.rs` (add `infer_object_type` + `InferTypeError`)
- Modify: `src/services/ingest/src/lib.rs` (re-export them)
- Create: `src/services/ingest/tests/infer.rs`
- Modify: `src/services/ingest/BUCK` (add an `infer` `rust_test` target)

**Interfaces:**
- Consumes: `datafusion_io::arrow_logical_type(&DataType) -> Option<&'static str>` (existing); `control_plane_core::{ObjectType, PropertyDef, TableRef, TypeName}`; `crate::gate::{Violation, ViolationReason}` (existing, both `pub`).
- Produces (later tasks rely on these exact names/types):
  - `pub fn infer_object_type(name: &TypeName, schema: &arrow::datatypes::Schema, identity: Option<&str>) -> Result<ObjectType, InferTypeError>`
  - `pub enum InferTypeError { UnsupportedColumns(Vec<crate::gate::Violation>), IdentityNotFound(String) }`

Behavior: one ordered `PropertyDef` per Arrow field (`name` = field name, `ty` = `arrow_logical_type(field)`, `required = !field.is_nullable()`). Collect **all** fields with no logical mapping into `Violation { column, reason: Unsupported }`; if any, return `UnsupportedColumns` (nothing else runs). Then, if `identity` is `Some(id)`: find the property named `id`; present ⇒ force `required = true`; absent ⇒ return `IdentityNotFound(id)`. `table = main.<name>`, `derived = vec![]`, `identity = identity.map(str::to_string)`.

- [ ] **Step 1: Write the failing unit tests**

Create `src/services/ingest/tests/infer.rs`:

```rust
//! Unit tests for `infer_object_type`: Arrow schema -> inferred ObjectType (the
//! reverse of `model_shape_from_type`). Pure logic, no fixture.

use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{TableRef, TypeName};
use ingest::{InferTypeError, infer_object_type};

fn schema(fields: Vec<Field>) -> Schema {
    Schema::new(fields)
}

#[test]
fn maps_each_field_to_a_property_in_order() {
    let s = schema(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
    ]);
    let ty = infer_object_type(&TypeName("gadget".into()), &s, None).expect("infer");

    assert_eq!(ty.name, TypeName("gadget".into()));
    assert_eq!(
        ty.table,
        TableRef { schema: "main".into(), name: "gadget".into() }
    );
    assert_eq!(ty.identity, None);
    assert!(ty.derived.is_empty());

    let props: Vec<(&str, &str, bool)> = ty
        .properties
        .iter()
        .map(|p| (p.name.as_str(), p.ty.as_str(), p.required))
        .collect();
    // required = !nullable: id is non-null -> required; name/score nullable -> not.
    assert_eq!(
        props,
        vec![("id", "long", true), ("name", "string", false), ("score", "double", false)]
    );
}

#[test]
fn declared_identity_is_recorded_and_forced_required() {
    let s = schema(vec![
        Field::new("sku", DataType::Utf8, true), // nullable in the batch...
        Field::new("qty", DataType::Int64, true),
    ]);
    let ty = infer_object_type(&TypeName("widget".into()), &s, Some("sku")).expect("infer");

    assert_eq!(ty.identity, Some("sku".into()));
    let sku = ty.properties.iter().find(|p| p.name == "sku").expect("sku prop");
    assert!(sku.required, "the declared identity is forced required even if the field is nullable");
}

#[test]
fn unmappable_column_is_unsupported_naming_the_column() {
    let s = schema(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("when", DataType::Date32, true), // no loom logical mapping
    ]);
    let err = infer_object_type(&TypeName("ev".into()), &s, None).expect_err("unmappable");
    match err {
        InferTypeError::UnsupportedColumns(vs) => {
            assert_eq!(vs.len(), 1);
            assert_eq!(vs[0].column, "when");
        }
        other => panic!("expected UnsupportedColumns, got {other:?}"),
    }
}

#[test]
fn identity_naming_absent_column_is_identity_not_found() {
    let s = schema(vec![Field::new("id", DataType::Int64, false)]);
    let err = infer_object_type(&TypeName("ev".into()), &s, Some("nope")).expect_err("bad id");
    match err {
        InferTypeError::IdentityNotFound(col) => assert_eq!(col, "nope"),
        other => panic!("expected IdentityNotFound, got {other:?}"),
    }
}
```

- [ ] **Step 2: Wire the `infer` test target in `src/services/ingest/BUCK`**

Add after the existing `model` `rust_test` target (it needs `arrow` for the schema builders, which `model` does not dep):

```python
# Arrow schema -> inferred ObjectType — pure logic, RE-eligible (no fixture).
rust_test(
    name = "infer",
    crate = "infer",
    srcs = ["tests/infer.rs"],
    crate_root = "tests/infer.rs",
    edition = "2024",
    deps = [
        ":ingest",
        "//src/control-plane/core:core",
        "//third-party:arrow",
    ],
)
```

- [ ] **Step 3: Run the test target — verify it fails to compile (symbol absent)**

Run: `buck2 test //src/services/ingest:infer > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\\[|cannot find" /tmp/t.log`
Expected: FAIL — `infer_object_type` / `InferTypeError` not found in `ingest`.

- [ ] **Step 4: Implement `infer_object_type` + `InferTypeError` in `src/services/ingest/src/model.rs`**

Append the new code to `model.rs` (keep the existing `model_shape_from_type` above it). First fix up the imports — **REPLACE** the two existing import lines (do NOT add new duplicates, or `ObjectType` is imported twice → E0252):

- Replace `use control_plane_core::ObjectType;` (line 6) with:
  ```rust
  use control_plane_core::{ObjectType, PropertyDef, TableRef, TypeName};
  ```
- Replace `use crate::gate::{ColumnShape, ModelShape};` (line 8) with:
  ```rust
  use crate::gate::{ColumnShape, ModelShape, Violation, ViolationReason};
  ```
- And add these two new import lines (no existing line to merge with):
  ```rust
  use arrow::datatypes::Schema;
  use datafusion_io::arrow_logical_type;
  ```

```rust
/// Why an Arrow batch schema could not be turned into an `ObjectType`. Each variant
/// names the offending column so the HTTP layer can render a precise client error:
/// `UnsupportedColumns` -> 422 (a `violations`-shaped body), `IdentityNotFound` -> 400.
#[derive(Debug)]
pub enum InferTypeError {
    /// One or more Arrow columns have no loom logical-type mapping. Carries one
    /// `Violation` per offending column (reason `Unsupported`).
    UnsupportedColumns(Vec<Violation>),
    /// The caller-declared `?identity=` column is not present in the batch schema.
    IdentityNotFound(String),
}

/// Infer an `ObjectType` from an Arrow batch `schema` — the reverse of
/// [`model_shape_from_type`]. One ordered [`PropertyDef`] per Arrow field: `name` is the
/// field name, `ty` is the loom logical type via the shared landing mapping
/// ([`arrow_logical_type`]), and `required` mirrors the field's non-nullability. The
/// inferred type is bound to the conventional `main.<name>` table.
///
/// `identity`, if `Some`, must name a field in the batch: that property is forced
/// `required` and recorded as the type's `identity` (a wrong identity is hard to undo,
/// so it is caller-declared, never guessed). An Arrow type with no logical mapping is an
/// `UnsupportedColumns` error naming every offending column; a `?identity=` naming an
/// absent column is `IdentityNotFound`.
pub fn infer_object_type(
    name: &TypeName,
    schema: &Schema,
    identity: Option<&str>,
) -> Result<ObjectType, InferTypeError> {
    let mut properties = Vec::with_capacity(schema.fields().len());
    let mut violations = Vec::new();
    for field in schema.fields() {
        match arrow_logical_type(field.data_type()) {
            Some(ty) => properties.push(PropertyDef {
                name: field.name().clone(),
                ty: ty.to_string(),
                required: !field.is_nullable(),
            }),
            None => violations.push(Violation {
                column: field.name().clone(),
                reason: ViolationReason::Unsupported,
            }),
        }
    }
    if !violations.is_empty() {
        return Err(InferTypeError::UnsupportedColumns(violations));
    }

    if let Some(id) = identity {
        match properties.iter_mut().find(|p| p.name == id) {
            Some(p) => p.required = true,
            None => return Err(InferTypeError::IdentityNotFound(id.to_string())),
        }
    }

    Ok(ObjectType {
        name: name.clone(),
        properties,
        derived: vec![],
        // Conventional landing target for an inferred type: the default `main` schema,
        // table named for the type. Serving resolves type -> this table at read time.
        table: TableRef {
            schema: "main".to_string(),
            name: name.0.clone(),
        },
        identity: identity.map(str::to_string),
    })
}
```

- [ ] **Step 5: Re-export from `src/services/ingest/src/lib.rs`**

Find the existing `pub use model::model_shape_from_type;` line and replace it with:

```rust
pub use model::{InferTypeError, infer_object_type, model_shape_from_type};
```

- [ ] **Step 6: Run the test target — verify it passes**

Run: `buck2 test //src/services/ingest:infer > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (4 tests).

- [ ] **Step 7: Clippy-gate the changed library**

Run: `buck2 build '//src/services/ingest:ingest[clippy.txt]' > /tmp/c.log 2>&1; cat $(buck2 build --show-output '//src/services/ingest:ingest[clippy.txt]' 2>/dev/null | awk '{print $2}') 2>/dev/null; grep -iE "warning|error" /tmp/c.log || echo "clippy clean"`
Expected: no warnings/errors attributable to `model.rs` (empty `clippy.txt`).

- [ ] **Step 8: Commit**

```bash
git add src/services/ingest/src/model.rs src/services/ingest/src/lib.rs \
        src/services/ingest/tests/infer.rs src/services/ingest/BUCK
git commit -m "feat(ingest): infer_object_type — Arrow schema to inferred ObjectType"
```

---

### Task 2: Wire the type-absent branch into `land_model` (+ `?identity=` + OpenAPI) with an e2e suite

Turn the absent-type 403 dead-end into infer-and-create, behind the unchanged ACL gate. Tested by new `loom_fixture_test` e2e scenarios in `tests/http_model.rs`.

**Files:**
- Modify: `src/services/ingest/src/http.rs` (decode-up-front restructure, `?identity=` query param, absent branch, OpenAPI annotation)
- Modify: `src/services/ingest/tests/http_model.rs` (new e2e tests + a `grant_write_absent_type` helper)

**Interfaces:**
- Consumes: `ingest::{infer_object_type, InferTypeError}` (Task 1); `crate::http::violations_json` (existing private fn); `control_plane_core::{ControlPlaneError}` (existing import); axum `Query`; `serde::Deserialize`.
- Produces: no new exported API; the `POST /models/{type}` endpoint now accepts an optional `?identity=<col>` query param and infers-and-creates an absent type for an authorized subject.

- [ ] **Step 1: Write the failing e2e tests in `src/services/ingest/tests/http_model.rs`**

Add a direct-grant helper (the public `grant` API rejects a grant on an absent type; this seeds the row the same way the adapter encodes it — `target_kind='type'`, `target_b=''`, `action='write'`, `effect='allow'`). Place it next to `grant_write`. Note `post_model` is updated in Step 1b to carry a query string; add that overload now too.

```rust
/// Seed a Write grant on a type that does NOT exist yet (the public `grant` API
/// validates type existence, which the infer-and-create flow must precede). Inserts the
/// `acl.role_grant` row directly, mirroring the adapter's `(kind,a,b)`/action/effect
/// encoding. Standing in for the deferred ontology-authoring capability.
async fn grant_write_absent_type(pg: &PgControlPlane, pool: &PgPool, subject: &str, type_name: &str) {
    let subj = SubjectId(subject.into());
    let role = RoleId(format!("{subject}-role"));
    pg.define_subject(&subj).await.unwrap();
    pg.define_role(&role).await.unwrap();
    pg.assign_role(&subj, &role).await.unwrap();
    sqlx::query(
        "insert into acl.role_grant (role_id, action, target_kind, target_a, target_b, effect) \
         values ($1, 'write', 'type', $2, '', 'allow') \
         on conflict (role_id, action, target_kind, target_a, target_b) do nothing",
    )
    .bind(&role.0)
    .bind(type_name)
    .execute(pool)
    .await
    .expect("seed grant on absent type");
}
```

Add `use sqlx::PgPool;` is already imported. Add a query-param-aware request driver next to `post_model`:

```rust
/// Like `post_model` but appends a raw query string (e.g. "identity=id").
async fn post_model_q(
    app: Router,
    type_name: &str,
    query: &str,
    token: &str,
    body: Vec<u8>,
) -> (StatusCode, serde_json::Value) {
    let uri = if query.is_empty() {
        format!("/models/{type_name}")
    } else {
        format!("/models/{type_name}?{query}")
    };
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
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
```

Now the five scenarios (the existing 4 slice-1 tests stay as-is):

```rust
#[tokio::test(flavor = "multi_thread")]
async fn infer_and_create_lands_and_records_the_type() {
    let fx = PgFixture::start();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, _wh, state) = app_state(&fx, &db).await;
    // Absent type "gadget"; alice is Write-granted on it via the direct seed.
    grant_write_absent_type(&pg, &pool, "alice", "gadget").await;
    let token = session_token(&pg, "alice").await;

    let app = protected(state, pg.clone());
    let (status, json) = post_model_q(app, "gadget", "", &token, ipc_bytes(&sample_batch())).await;
    assert_eq!(status, StatusCode::OK, "authorized POST to an absent type infers + creates");
    assert_eq!(json["type"], "gadget");
    json["snapshot_id"].as_i64().expect("snapshot_id is an integer");

    // The inferred ObjectType matches the batch schema (names, logical types, nullability).
    let ot = pg.get_type(&TypeName("gadget".into())).await.expect("type created");
    let props: Vec<(&str, &str, bool)> = ot
        .properties
        .iter()
        .map(|p| (p.name.as_str(), p.ty.as_str(), p.required))
        .collect();
    assert_eq!(props, vec![("id", "long", true), ("name", "string", false)]);
    assert_eq!(ot.identity, None);
    assert_eq!(ot.table, TableRef { schema: "main".into(), name: "gadget".into() });

    // The rows landed and are servable through the engine path.
    let catalog = IcebergCatalog::new(pool.clone());
    let batches = engine_serving::execute_query(
        &catalog,
        "SELECT \"id\", \"name\" FROM \"main\".\"gadget\" ORDER BY \"id\"",
        None,
    )
    .await
    .expect("serving read");
    let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn identity_query_param_is_honored() {
    let fx = PgFixture::start();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, _wh, state) = app_state(&fx, &db).await;
    grant_write_absent_type(&pg, &pool, "alice", "keyed").await;
    let token = session_token(&pg, "alice").await;

    let app = protected(state, pg.clone());
    let (status, _json) =
        post_model_q(app, "keyed", "identity=id", &token, ipc_bytes(&sample_batch())).await;
    assert_eq!(status, StatusCode::OK);

    let ot = pg.get_type(&TypeName("keyed".into())).await.expect("type created");
    assert_eq!(ot.identity, Some("id".into()), "declared identity recorded");
    let id = ot.properties.iter().find(|p| p.name == "id").expect("id prop");
    assert!(id.required, "identity property is forced required");

    // The identity value addresses a row (the column carries addressable values).
    let catalog = IcebergCatalog::new(pool.clone());
    let batches = engine_serving::execute_query(
        &catalog,
        "SELECT \"name\" FROM \"main\".\"keyed\" WHERE \"id\" = 1",
        None,
    )
    .await
    .expect("serving read by identity");
    let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, 1, "identity value round-trips");
}

#[tokio::test(flavor = "multi_thread")]
async fn identity_naming_absent_column_is_rejected_and_nothing_created() {
    let fx = PgFixture::start();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, _wh, state) = app_state(&fx, &db).await;
    grant_write_absent_type(&pg, &pool, "alice", "badid").await;
    let token = session_token(&pg, "alice").await;

    let app = protected(state, pg.clone());
    let (status, _json) =
        post_model_q(app, "badid", "identity=nope", &token, ipc_bytes(&sample_batch())).await;
    assert!(
        status == StatusCode::BAD_REQUEST || status == StatusCode::UNPROCESSABLE_ENTITY,
        "a ?identity naming an absent column is rejected (got {status})"
    );
    assert!(
        matches!(
            pg.get_type(&TypeName("badid".into())).await,
            Err(ControlPlaneError::NotFound(_))
        ),
        "nothing is created on a bad identity"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn re_post_conforms_then_rejects_a_differing_batch() {
    let fx = PgFixture::start();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, _wh, state) = app_state(&fx, &db).await;
    grant_write_absent_type(&pg, &pool, "alice", "again").await;
    let token = session_token(&pg, "alice").await;

    // First POST infers + creates.
    let app = protected(state.clone(), pg.clone());
    let (s1, _) = post_model_q(app, "again", "", &token, ipc_bytes(&sample_batch())).await;
    assert_eq!(s1, StatusCode::OK);

    // Second POST that CONFORMS to the now-existing type -> 200 (hits the slice-1 path).
    let app = protected(state.clone(), pg.clone());
    let (s2, _) = post_model_q(app, "again", "", &token, ipc_bytes(&sample_batch())).await;
    assert_eq!(s2, StatusCode::OK, "a conforming re-post lands via slice-1 conform");

    // Second POST that DIFFERS (missing the required "id") -> 422, nothing new lands.
    let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
    let differing = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["z"]))]).unwrap();
    let app = protected(state, pg.clone());
    let (s3, json) = post_model_q(app, "again", "", &token, ipc_bytes(&differing)).await;
    assert_eq!(s3, StatusCode::UNPROCESSABLE_ENTITY, "a differing batch is a conformance failure");
    assert_eq!(json["violations"][0]["column"], "id");

    // The type is unchanged (still the inferred shape).
    let ot = pg.get_type(&TypeName("again".into())).await.expect("type still there");
    assert_eq!(ot.properties.len(), 2);

    let _ = pool; // keep the fixture db alive for the duration of the test
}

#[tokio::test(flavor = "multi_thread")]
async fn unmappable_column_is_422_and_nothing_created() {
    use arrow::array::Date32Array;
    use arrow::datatypes::DataType;

    let fx = PgFixture::start();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, _wh, state) = app_state(&fx, &db).await;
    grant_write_absent_type(&pg, &pool, "alice", "evt").await;
    let token = session_token(&pg, "alice").await;

    // "when" is Date32 — no loom logical mapping.
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("when", DataType::Date32, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![1i64])), Arc::new(Date32Array::from(vec![0]))],
    )
    .unwrap();

    let app = protected(state, pg.clone());
    let (status, json) = post_model_q(app, "evt", "", &token, ipc_bytes(&batch)).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(json["violations"][0]["column"], "when");
    assert_eq!(json["violations"][0]["reason"], "unsupported");
    assert!(
        matches!(
            pg.get_type(&TypeName("evt".into())).await,
            Err(ControlPlaneError::NotFound(_))
        ),
        "an unmappable batch creates nothing"
    );
}
```

- [ ] **Step 2: Run the e2e target — verify the new tests fail (absent type still 403s)**

Run: `buck2 test //src/services/ingest:http-model > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/t.log`
Expected: the 4 existing tests PASS; the 5 new tests FAIL (current handler returns 403 for an absent type / ignores `?identity=`).

- [ ] **Step 3: Restructure `land_model` in `src/services/ingest/src/http.rs` — decode up front + absent branch**

Add the imports (merge into the existing `use` lists): `use axum::extract::Query;`, and add `infer_object_type, InferTypeError` to the `use crate::...` (they live in `crate::model`, re-exported at crate root):

```rust
use crate::model::{infer_object_type, model_shape_from_type, InferTypeError};
```

Add the query DTO near `LandModel` (top of the module):

```rust
/// Query params for `POST /models/{type}`. `identity` names the column to record as the
/// inferred type's primary key (type-absent branch only; ignored when the type exists).
#[derive(Deserialize)]
struct ModelQuery {
    identity: Option<String>,
}
```

Replace the `land_model` signature and steps 1–5 (down to the `let shape = ...` line) with the restructured version. The success/error tail (lines mapping `materializer.land` results) is unchanged.

```rust
pub(crate) async fn land_model(
    State(st): State<AppState>,
    Path(type_name): Path<String>,
    Query(q): Query<ModelQuery>,
    subject: Subject,
    body: Bytes,
) -> Response {
    let type_name = TypeName(type_name);

    // 1. Coarse ACL gate BEFORE anything is revealed: an authenticated subject without a
    //    Write grant on this type is 403 — returned whether or not the type exists (no
    //    existence leak). `require_auth` already 401s an unauthenticated caller.
    match st
        .cp
        .acl()
        .check(
            &subject.0,
            Action::Write,
            &PolicyTarget::Type(type_name.clone()),
        )
        .await
    {
        Ok(Decision::Allow) => {}
        Ok(Decision::Deny) => return StatusCode::FORBIDDEN.into_response(),
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    }

    // 2. Decode the Arrow IPC body. Needed by both branches (inference reads the schema).
    let (schema, batches) = match decode_ipc(&body) {
        Ok(sb) => sb,
        Err(_) => return (StatusCode::BAD_REQUEST, "invalid arrow ipc stream").into_response(),
    };

    // 3. Resolve the type, or — when absent and authorized — infer it from the batch
    //    schema, create it, and re-resolve. The re-resolve is the create-or-conform race
    //    guard: `define_type` is an upsert, so concurrent first-batches resolve to one
    //    stored type; each then conforms its batch against it (the loser 422s if it
    //    differs). A granted-but-present type takes the unchanged slice-1 path.
    let otype = match st.cp.ontology().get_type(&type_name).await {
        Ok(t) => t,
        Err(ControlPlaneError::NotFound(_)) => {
            let inferred =
                match infer_object_type(&type_name, &schema, q.identity.as_deref()) {
                    Ok(t) => t,
                    Err(InferTypeError::UnsupportedColumns(violations)) => {
                        return (
                            StatusCode::UNPROCESSABLE_ENTITY,
                            Json(violations_json(&violations)),
                        )
                            .into_response();
                    }
                    Err(InferTypeError::IdentityNotFound(col)) => {
                        return (
                            StatusCode::BAD_REQUEST,
                            format!("identity column `{col}` is not present in the batch"),
                        )
                            .into_response();
                    }
                };
            if st.cp.ontology().define_type(inferred).await.is_err() {
                return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response();
            }
            match st.cp.ontology().get_type(&type_name).await {
                Ok(t) => t,
                Err(_) => {
                    return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response();
                }
            }
        }
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    };

    // 4. Derive the conformance shape from the (resolved or just-created) type.
    let shape = model_shape_from_type(&otype);
```

Leave everything from `// 5. Gate + resolve the physical schema` onward unchanged (the existing `resolve_columns` block, lineage build, `materializer.land` match) — but note the original `// 4. Decode the Arrow IPC body` block is now removed (decode moved to step 2) and the step comments renumber naturally. Verify there is exactly one `decode_ipc` call left in the handler.

- [ ] **Step 4: Update the OpenAPI annotation on `land_model`**

Add the `identity` query param and adjust the doc/403 description so it no longer claims an unknown type is always 403 (it now infers for an authorized subject; the 403 is the no-grant case). Update the `#[utoipa::path(...)]` `params(...)` and the doc comment:

```rust
/// Governed model ingest. With a pre-existing type, conform the Arrow batch to it and
/// land it (slice 1). With an absent type, an authorized subject's batch *infers* an
/// `ObjectType` from the batch schema (optionally keyed by `?identity=<col>`),
/// `define_type`s it, and lands. Authorize before either branch (deny-by-default, no
/// existence leak); a denied write never reaches the store.
#[utoipa::path(
    post, path = "/models/{type}",
    params(
        ("type" = String, Path, description = "Ontology type name"),
        ("identity" = Option<String>, Query, description = "Column to record as the inferred type's identity (type-absent branch only)"),
    ),
    request_body(
        content = Vec<u8>,
        description = "Arrow IPC stream (schema + record batches)",
        content_type = "application/vnd.apache.arrow.stream",
    ),
    responses(
        (status = 200, description = "Landed as typed objects; snapshot committed", body = ModelLandAck),
        (status = 400, description = "Invalid Arrow IPC / unsupported column type / identity names an absent column"),
        (status = 403, description = "Not authorized to write the type (returned whether or not the type exists — no existence leak)"),
        (status = 422, description = "Data does not conform / an Arrow column has no loom logical type", body = ViolationsBody),
        (status = 500, description = "Internal error"),
    ),
    security(("bearer_auth" = [])),
    tag = "models",
)]
```

- [ ] **Step 5: Run the e2e target — verify all tests pass**

Run: `buck2 test //src/services/ingest:http-model > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (9 tests: 4 existing + 5 new).

- [ ] **Step 6: Run the OpenAPI drift guard + the unit target**

Run: `buck2 test //src/services/ingest:openapi //src/services/ingest:infer //src/services/ingest:model //src/services/ingest:gate > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (the path set is unchanged, so `openapi` stays green).

- [ ] **Step 7: Clippy-gate the changed library**

Run: `buck2 build '//src/services/ingest:ingest[clippy.txt]' '//src/services/ingest:ingest-bin[clippy.txt]' > /tmp/c.log 2>&1; grep -iE "warning|error" /tmp/c.log || echo "clippy clean"`
Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add src/services/ingest/src/http.rs src/services/ingest/tests/http_model.rs
git commit -m "feat(ingest): infer-and-create on POST /models/{type} for an absent type"
```

---

### Task 3: Full-suite verification + register update

Prove the whole first-party suite is green (fixture tests included) and close the register item.

**Files:**
- Modify: `docs/ROADMAP.md` (via `loom-docs-update`)

- [ ] **Step 1: Build the whole tree**

Run: `buck2 build //src/... > /tmp/b.log 2>&1; tail -3 /tmp/b.log`
Expected: build succeeds.

- [ ] **Step 2: Run the whole test suite** (fixture tests route local via `loom_fixture_test`)

Run: `buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: all pass, 0 fail. If a fixture test for an unrelated crate fails, investigate (shared-dep regression) before proceeding.

- [ ] **Step 3: Close the register item** via `loom-docs-update`

In `docs/ROADMAP.md`, flip `road-ingest-model-inference` to done: `- [ ]` → `- [x]`, `status:planned` → `status:done`, and set `pr:#<N>` once the PR number is known. Promote/close `fut-ingest-model-inference` if it is still open (the spec says this slice promotes it).

- [ ] **Step 4: Run the docs validator + lint hooks**

Run: `bash tools/docs.sh validate > /tmp/d.log 2>&1; tail -5 /tmp/d.log` and `buck2 run //tools:prek -- run --all-files > /tmp/p.log 2>&1; grep -iE "Failed|passed" /tmp/p.log`
Expected: docs valid; prek hooks pass (commit any in-place fixes the hooks make).

- [ ] **Step 5: Commit the register update**

```bash
git add docs/ROADMAP.md docs/FUTURE.md
git commit -m "docs(roadmap): close ingest model inference (slice 2)"
```

---

## Self-Review

**Spec coverage:**
- *Auth + ACL unchanged, branch on existence* → Task 2 Step 3 (gate unchanged; `get_type` match).
- *Infer the `ObjectType` (PropertyDef per field, table, identity validation, unmappable→422)* → Task 1.
- *`?identity=` caller-declared, forced required* → Task 1 (logic) + Task 2 (query param wiring) + e2e `identity_query_param_is_honored`.
- *Create-or-conform race guard (re-resolve, loser conforms)* → Task 2 Step 3 (`define_type` upsert + re-resolve + shared conform tail) + e2e `re_post_conforms_then_rejects_a_differing_batch`.
- *Land with type-named lineage, returns snapshot_id* → unchanged slice-1 tail (still runs after the shared `shape`).
- *Re-inference is conform-only* → e2e `re_post_conforms_then_rejects_a_differing_batch`.
- *No-leak: unknown type, no grant → 403* → existing `unknown_type_is_403_no_leak` (still green; the 403 now comes from the ACL gate, which runs first).
- All 5 spec test scenarios → mapped to the 5 new e2e tests + the retained slice-1 tests.

**Placeholder scan:** none — every code step shows full code; every run step shows the command + expected outcome.

**Type consistency:** `infer_object_type(&TypeName, &Schema, Option<&str>) -> Result<ObjectType, InferTypeError>` is defined in Task 1 and consumed verbatim in Task 2. `InferTypeError::{UnsupportedColumns(Vec<Violation>), IdentityNotFound(String)}` variants match their match-arms in the handler. `violations_json(&[Violation])`, `model_shape_from_type`, `decode_ipc`, `resolve_columns` are all existing symbols used with their real signatures. The inferred `table` (`main.<name>`) is asserted identically in the unit test and the e2e test.

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-06-30-ingest-model-inference.md`. Proceeding with **subagent-driven execution** (fresh subagent per task + two-stage review), per the loom-work-checkout pipeline.
