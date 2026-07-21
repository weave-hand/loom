# Ontology derived-property & vector-index read surface Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Surface `DerivedPropertyDef.description` and `VectorIndexDef.description` (and the definitions themselves) on the read path — `GET /ontology/types/{name}` gains `derived[]` + `vector_indexes[]`, and the per-type generated OpenAPI read schema declares derived properties as `readOnly`.

**Architecture:** Three additive legs over existing, already-wired read ops. (1) The type-detail handler (`query-api/src/http.rs`) already holds `ObjectType.derived`; it gains one `onto.vector_indexes_for(&type_name)` call and renders both as JSON arrays using the file's existing `set_description` convention. (2) The static OpenAPI doc (`query-api/src/openapi.rs`) gains two doc-shape structs on `TypeDetailResponse`. (3) The pure ontology→OpenAPI generator (`query-api/src/openapi_gen.rs`) additionally iterates `ty.derived`, emitting each as a `readOnly` property of the per-type read component. No control-plane, schema, wire, or `.sqlx` changes; every op used already exists on the `Ontology` trait and the wire client.

**Tech Stack:** Rust, axum, `serde_json`, utoipa 5.5.0 (`ObjectBuilder`/`Object.read_only`), buck2 (`rust_test` integration targets), `MemoryControlPlane` for route/generator tests.

## Global Constraints

- **Tests are `rust_test` integration targets only** — NOT inline `#[cfg(test)]`. Add cases to the existing `//src/services/query-api:ontology_type_detail` and `//src/services/query-api:openapi_gen` targets (both already wired). The `no-inline-tests` prek hook fails on any `#[test]` in `src/**.rs`.
- **Strict clippy** (whole `clippy::pedantic` + `clippy::restriction`): no `unwrap`/`expect`/`panic`/`indexing_slicing`/`todo`/`dbg!` in production `src/**.rs`. Test code is exempted from the panic-safety lints via the `loom_rust_test` wrapper. Insert into a `serde_json::Value` map via `as_object_mut()` + `map.insert(..)` (NOT `v["k"] = ..`, which trips `indexing_slicing`) — the existing `set_description` helper already does this; reuse it.
- **serde representations are the wire form for `agg`/`metric`/`spec`** (per spec): `Aggregation` is externally tagged (`Count` → `"Count"`, `Sum("amount")` → `{"Sum":"amount"}`); `Metric` → `"Cosine"`/`"L2"`; `IndexSpec` → `"Flat"` / `{"IvfFlat":{"nlist":..}}` / `{"Hnsw":{"m":..,"ef_construction":..}}`. Do NOT invent a lowercase token form — the search route's separate `as_str` token is out of scope.
- **Response keys are additive.** Every existing key on `TypeDetailResponse` stays; only `derived[]` and `vector_indexes[]` are added. No route is added or removed, so the OpenAPI route drift-guards are unaffected.
- **`buck2 run //tools:prek -- run --all-files`** must pass before every commit (rustfmt + clippy + file hooks); commit whatever the hooks rewrite.
- **Build/test the touched crate** with `buck2 build -v0 --console none //src/services/query-api/...` and `buck2 test --console none //src/services/query-api:ontology_type_detail //src/services/query-api:openapi_gen` (cloud sessions: scope to these targets, never a bare whole-tree build — `-M none` if building wider).

---

### Task 1: Type-detail handler renders `derived[]` and `vector_indexes[]`

**Files:**
- Modify: `src/services/query-api/src/http.rs` (imports; add `derived_view_json` + `vector_index_view_json` helpers near `link_view_json` at `:185-197`; extend `get_ontology_type` at `:215-253`)
- Test: `src/services/query-api/tests/ontology_type_detail.rs` (extend `seeded_described` at `:77-100`; extend the Order exact-array oracle at `:157-181`; extend/add description assertions after `:225`)

**Interfaces:**
- Consumes: `control_plane_core::{DerivedPropertyDef, VectorIndexDef}`; `ObjectType.derived: Vec<DerivedPropertyDef>`; `Ontology::vector_indexes_for(&TypeName) -> Result<Vec<VectorIndexDef>>` (order unspecified — the handler sorts); the file-local `set_description(&mut serde_json::Value, Option<&String>)` (`http.rs:174-183`) and the `cp_read_error(msg, e)` fault-mapping helper already used at `:224`/`:228`/`:232`.
- Produces: `derived_view_json(&DerivedPropertyDef) -> serde_json::Value` and `vector_index_view_json(&VectorIndexDef) -> serde_json::Value`; two new keys (`derived`, `vector_indexes`) on the `GET /ontology/types/{name}` body.

- [ ] **Step 1: Write the failing tests**

In `src/services/query-api/tests/ontology_type_detail.rs`, first extend the imports (line 12-14) to add the new core types:

```rust
use control_plane_core::{
    Aggregation, Cardinality, DerivedPropertyDef, IndexSpec, LinkDef, Metric, ObjectType, Ontology,
    PropertyDef, SubjectId, TableRef, VectorIndexDef,
};
```

Extend `seeded_described` (`:77-100`) so `Doc` also carries a `vector(4)` property, two derived properties (one described, one not), and two vector indexes (one described, one not). Replace the body of `seeded_described` with:

```rust
async fn seeded_described() -> MemoryControlPlane {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    cp.define_type(
        ObjectType::build("Doc", ("main", "docs"))
            .described("A document.")
            .add_prop(
                PropertyDef::new("id", "Long")
                    .required()
                    .described("The document id."),
            )
            .prop("body", "String")
            .add_prop(PropertyDef::new("embedding", "vector(4)"))
            .identity("id")
            .derived(
                DerivedPropertyDef::new("child_count", "Long", "parent", Aggregation::Count)
                    .described("Number of child documents."),
            )
            .derived(DerivedPropertyDef::new(
                "weight_sum",
                "Double",
                "parent",
                Aggregation::Sum("weight".into()),
            ))
            .done(),
    )
    .await
    .unwrap();
    cp.define_link(
        LinkDef::fk("parent", "Doc", "Doc", Cardinality::One, "parent_id", "id")
            .described("The parent document."),
    )
    .await
    .unwrap();
    cp.define_vector_index(
        VectorIndexDef::new("flat", "Doc", "embedding", Metric::Cosine, IndexSpec::Flat)
            .described("Exact cosine index over the embedding."),
    )
    .await
    .unwrap();
    cp.define_vector_index(VectorIndexDef::new(
        "hnsw",
        "Doc",
        "embedding",
        Metric::L2,
        IndexSpec::Hnsw {
            m: Some(16),
            ef_construction: Some(200),
        },
    ))
    .await
    .unwrap();
    cp
}
```

Extend the Order exact-array oracle in `type_detail_serves_properties_identity_and_links` (after the `links_to` assertion at `:180`) so a type with neither derived properties nor indexes documents empty arrays:

```rust
    assert_eq!(json["derived"], serde_json::json!([]));
    assert_eq!(json["vector_indexes"], serde_json::json!([]));
```

Add two new tests after `type_detail_omits_absent_descriptions` (`:225`) — an exact-array oracle for `derived[]`/`vector_indexes[]` including agg/metric/spec serde forms, and the description key presence/absence:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn type_detail_serves_derived_and_vector_indexes() {
    let app = app(seeded_described().await);
    let (status, json) = get(&app, "/ontology/types/Doc").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        json["derived"],
        serde_json::json!([
            {
                "name": "child_count", "ty": "Long", "link": "parent",
                "agg": "Count", "description": "Number of child documents."
            },
            {
                "name": "weight_sum", "ty": "Double", "link": "parent",
                "agg": { "Sum": "weight" }
            },
        ])
    );
    // vector_indexes_for's order is unspecified (HashMap-backed in memory); the handler
    // sorts by name, so "flat" precedes "hnsw" deterministically.
    assert_eq!(
        json["vector_indexes"],
        serde_json::json!([
            {
                "name": "flat", "property": "embedding", "metric": "Cosine",
                "spec": "Flat", "description": "Exact cosine index over the embedding."
            },
            {
                "name": "hnsw", "property": "embedding", "metric": "L2",
                "spec": { "Hnsw": { "m": 16, "ef_construction": 200 } }
            },
        ])
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn type_detail_omits_absent_derived_and_index_descriptions() {
    let app = app(seeded_described().await);
    let (_, json) = get(&app, "/ontology/types/Doc").await;
    // The undescribed derived property + index carry no `description` key.
    assert_eq!(json["derived"][1]["name"], "weight_sum");
    assert!(json["derived"][1].get("description").is_none());
    assert_eq!(json["vector_indexes"][1]["name"], "hnsw");
    assert!(json["vector_indexes"][1].get("description").is_none());
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `buck2 test --console none //src/services/query-api:ontology_type_detail`
Expected: FAIL — `json["derived"]`/`json["vector_indexes"]` are `Null` (handler does not emit them yet); the new tests' oracle assertions mismatch. (If instead the target fails to COMPILE because `Ontology`/`define_vector_index` are unused, that's the pre-implementation state — proceed to Step 3.)

- [ ] **Step 3: Add the view helpers**

In `src/services/query-api/src/http.rs`, extend the `control_plane_core::{...}` import (`:23-26`) to add `DerivedPropertyDef` and `VectorIndexDef`:

```rust
use control_plane_core::{
    ActionKind, ControlPlane, ControlPlaneError, Cursor, DatasetRef, DerivedPropertyDef,
    GC_JOB_KIND, LinkDef, NewJob, PageReq, RunId, TableRef, TypeName, VectorIndexDef,
};
```

Immediately after `link_view_json` (`:197`), add the two render helpers (they mirror `link_view_json`'s `json!` + `set_description` shape; `agg`/`metric`/`spec` are interpolated by `json!`, which serializes any `Serialize` value):

```rust
/// Render one `DerivedPropertyDef` as its documentation shape:
/// `{ name, ty, link, agg, description? }`. `agg` is the externally-tagged serde form
/// (`"Count"` or `{"Sum":"col"}`); `description` is present only when set.
fn derived_view_json(d: &DerivedPropertyDef) -> serde_json::Value {
    let mut v = serde_json::json!({
        "name": d.name,
        "ty": d.ty,
        "link": d.link,
        "agg": d.agg,
    });
    set_description(&mut v, d.description.as_ref());
    v
}

/// Render one `VectorIndexDef` as its documentation shape:
/// `{ name, property, metric, spec, description? }`. `type_name` is omitted (redundant on
/// the type's own detail). `metric`/`spec` carry their serde forms (`"Cosine"`/`"L2"`;
/// `"Flat"`/`{"IvfFlat":..}`/`{"Hnsw":..}`); `description` is present only when set.
fn vector_index_view_json(idx: &VectorIndexDef) -> serde_json::Value {
    let mut v = serde_json::json!({
        "name": idx.name,
        "property": idx.property,
        "metric": idx.metric,
        "spec": idx.spec,
    });
    set_description(&mut v, idx.description.as_ref());
    v
}
```

- [ ] **Step 4: Wire the helpers into `get_ontology_type`**

In `get_ontology_type` (`:215-253`), after the `links_to` read (`:230-233`) and before the `properties` mapping, fetch and sort the indexes; then add both arrays to the response body. Replace the body-building block (`:234-252`) with:

```rust
    let mut indexes = match onto.vector_indexes_for(&type_name).await {
        Ok(v) => v,
        Err(e) => return cp_read_error("ontology vector_indexes_for fault", e),
    };
    // vector_indexes_for's order is unspecified; sort by name for a stable read surface.
    indexes.sort_by(|a, b| a.name.cmp(&b.name));
    let properties: Vec<serde_json::Value> = ty
        .properties
        .iter()
        .map(|p| {
            let mut v = serde_json::json!({ "name": p.name, "ty": p.ty, "required": p.required });
            set_description(&mut v, p.description.as_ref());
            v
        })
        .collect();
    let mut body = serde_json::json!({
        "name": ty.name.0,
        "table": { "schema": ty.table.schema, "name": ty.table.name },
        "identity": ty.identity,
        "properties": properties,
        "derived": ty.derived.iter().map(derived_view_json).collect::<Vec<_>>(),
        "vector_indexes": indexes.iter().map(vector_index_view_json).collect::<Vec<_>>(),
        "links": links.iter().map(link_view_json).collect::<Vec<_>>(),
        "links_to": links_to.iter().map(link_view_json).collect::<Vec<_>>(),
    });
    set_description(&mut body, ty.description.as_ref());
    Json(body).into_response()
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `buck2 test --console none //src/services/query-api:ontology_type_detail`
Expected: PASS (all cases, including the pre-existing `type_detail_carries_descriptions` / `type_detail_omits_absent_descriptions` — the `embedding` property is appended after `body`, so `properties[0]`/`[1]` are undisturbed).

- [ ] **Step 6: Lint + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/services/query-api/src/http.rs src/services/query-api/tests/ontology_type_detail.rs
git commit -m "feat(query-api): surface derived properties + vector indexes on type detail"
```

---

### Task 2: Document `derived[]` + `vector_indexes[]` on `TypeDetailResponse`

**Files:**
- Modify: `src/services/query-api/src/openapi.rs` (add `DerivedPropertyView` + `VectorIndexView` near `LinkView` at `:121-135`; add two fields to `TypeDetailResponse` at `:137-154`; register both schemas in `components(schemas(...))` at `:243-269`)
- Test: `src/services/query-api/tests/openapi_gen.rs` (add a static-doc assertion; `build_openapi` is `pub`)

**Interfaces:**
- Consumes: `utoipa::ToSchema`; `query_api::openapi::build_openapi() -> utoipa::openapi::OpenApi` (already `pub`, `openapi.rs:278`).
- Produces: `DerivedPropertyView`, `VectorIndexView` doc-shape structs; `TypeDetailResponse.derived: Vec<DerivedPropertyView>` and `TypeDetailResponse.vector_indexes: Vec<VectorIndexView>`.

- [ ] **Step 1: Write the failing test**

In `src/services/query-api/tests/openapi_gen.rs`, add a test that the static document's `TypeDetailResponse` documents the two new arrays:

```rust
#[test]
fn type_detail_response_documents_derived_and_vector_indexes() {
    let doc = query_api::openapi::build_openapi();
    let j = serde_json::to_value(&doc).unwrap();
    let td = &j["components"]["schemas"]["TypeDetailResponse"]["properties"];
    assert!(
        td["derived"].is_object(),
        "TypeDetailResponse must document `derived`: {td}"
    );
    assert!(
        td["vector_indexes"].is_object(),
        "TypeDetailResponse must document `vector_indexes`: {td}"
    );
    // Both reference their view component schemas.
    assert!(j["components"]["schemas"]["DerivedPropertyView"].is_object());
    assert!(j["components"]["schemas"]["VectorIndexView"].is_object());
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test --console none //src/services/query-api:openapi_gen`
Expected: FAIL — `td["derived"]`/`td["vector_indexes"]` are `Null`; `DerivedPropertyView`/`VectorIndexView` schemas absent.

- [ ] **Step 3: Add the doc-shape structs**

In `src/services/query-api/src/openapi.rs`, immediately after `LinkView` (`:135`), add:

```rust
/// Documentation shape for one derived (aggregate-over-link) property in a type-detail
/// response. `agg` is the aggregation's serde form (`"Count"` or `{"Sum":"col"}`).
#[derive(ToSchema)]
pub struct DerivedPropertyView {
    pub name: String,
    /// The ontology's LOGICAL result type (e.g. `Long` for a count, `Double` for a sum).
    pub ty: String,
    /// The link whose target rows are aggregated.
    pub link: String,
    /// The aggregation, e.g. `"Count"` or `{"Sum":"amount"}`.
    #[schema(value_type = Object)]
    pub agg: serde_json::Value,
    /// Optional human-readable prose. Omitted from the response when the entity carries none.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Documentation shape for one declared vector index in a type-detail response. The owning
/// `type_name` is omitted (redundant on the type's own detail).
#[derive(ToSchema)]
pub struct VectorIndexView {
    pub name: String,
    /// The `vector(N)` property the index is built over.
    pub property: String,
    /// Distance metric: `"Cosine"` | `"L2"`.
    pub metric: String,
    /// Index spec: `"Flat"` | `{"IvfFlat":{..}}` | `{"Hnsw":{..}}`.
    #[schema(value_type = Object)]
    pub spec: serde_json::Value,
    /// Optional human-readable prose. Omitted from the response when the entity carries none.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}
```

> Note: `serde_json::Value` derives `ToSchema` directly (see `ObjectsResponse`), but `#[schema(value_type = Object)]` documents `agg`/`spec` as a free-form object explicitly since their shape is a tagged-enum union. If the derive rejects the attribute, drop the `#[schema(...)]` line — the bare `serde_json::Value` field still compiles and maps to a free-form object.

- [ ] **Step 4: Add the fields to `TypeDetailResponse` and register the schemas**

In `TypeDetailResponse` (`:137-154`), add the two array fields after `properties` (`:146`) — keep them adjacent to the existing arrays, before `links`:

```rust
    /// The declared properties, in order.
    pub properties: Vec<PropertyView>,
    /// Derived (aggregate-over-link) properties, in declared order.
    pub derived: Vec<DerivedPropertyView>,
    /// Declared vector indexes, name-ordered.
    pub vector_indexes: Vec<VectorIndexView>,
    /// Outbound links (`from` = this type).
    pub links: Vec<LinkView>,
```

In the `components(schemas(...))` list (`:243-269`), add `DerivedPropertyView` and `VectorIndexView` next to `LinkView`/`TypeDetailResponse`:

```rust
        PropertyView,
        DerivedPropertyView,
        VectorIndexView,
        LinkView,
        TypeDetailResponse,
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `buck2 test --console none //src/services/query-api:openapi_gen`
Expected: PASS.

- [ ] **Step 6: Lint + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/services/query-api/src/openapi.rs src/services/query-api/tests/openapi_gen.rs
git commit -m "docs(query-api): document derived + vector-index arrays on TypeDetailResponse"
```

---

### Task 3: Per-type OpenAPI read schema declares derived properties as `readOnly`

**Files:**
- Modify: `src/services/query-api/src/openapi_gen.rs` (add a `read_only_derived` helper; extend `type_component_schema` at `:143-159`)
- Test: `src/services/query-api/tests/openapi_gen.rs` (assert derived-in-read-schema + absent-from-action-schema)

**Interfaces:**
- Consumes: `ObjectType.derived: Vec<DerivedPropertyDef>`; the file-local `property_schema(ty: &str, required: bool, description: Option<&str>) -> RefOr<Schema>` (`:91-118`); utoipa `Object.read_only: Option<bool>` (public field, utoipa 5.5.0 `schema.rs:993`).
- Produces: derived properties present as `readOnly: true` entries in each type's component (read) schema, carrying `description` when set; unchanged action-request schemas (derived props are never action parameters, so they never appear there).

- [ ] **Step 1: Write the failing tests**

In `src/services/query-api/tests/openapi_gen.rs`, extend the `customer()` fixture-based coverage with a derived property. Add a new test that uses a type carrying a derived property and asserts it appears `readOnly` in the read component but NOT in the action request schema:

```rust
#[test]
fn derived_properties_are_read_only_in_the_component_schema() {
    let cust = ObjectType::build("Customer", ("main", "customer"))
        .prop_req("id", "long")
        .derived(
            control_plane_core::DerivedPropertyDef::new(
                "orderCount",
                "Long",
                "orders",
                control_plane_core::Aggregation::Count,
            )
            .described("How many orders this customer has."),
        )
        .identity("id")
        .done();
    let create = create_customer_action(); // params: name (req), tier — no derived props
    let (_paths, schemas) = ontology_openapi(&[cust], &[], &[create]);

    let doc = serde_json::to_value(schemas.get("Customer").expect("Customer schema")).unwrap();
    // Derived property is present in the READ component, marked readOnly, with its prose.
    // `orderCount` is declared `ty = "Long"`, so `property_schema` folds the property prose
    // together with `base_type_to_schema(Long)`'s encoding note (same pattern the pre-existing
    // `generated_document_carries_ontology_descriptions` test asserts, openapi_gen.rs:492-495).
    assert_eq!(doc["properties"]["orderCount"]["readOnly"], true);
    assert_eq!(
        doc["properties"]["orderCount"]["description"],
        "How many orders this customer has. (int64 encoded as a decimal string)"
    );
    // A physical property is NOT readOnly.
    assert!(
        doc["properties"]["id"].get("readOnly").is_none(),
        "physical properties must not be readOnly: {}",
        doc["properties"]["id"]
    );
}
```

Add a second test that the derived property is absent from the action (write) request schema. `create_customer_action` targets `Customer`; its request schema is derived from action parameters only:

```rust
#[test]
fn derived_properties_do_not_appear_in_action_request_schemas() {
    let cust = ObjectType::build("Customer", ("main", "customer"))
        .prop_req("id", "long")
        .derived(control_plane_core::DerivedPropertyDef::new(
            "orderCount",
            "Long",
            "orders",
            control_plane_core::Aggregation::Count,
        ))
        .identity("id")
        .done();
    let (paths, _schemas) = ontology_openapi(&[cust], &[], &[create_customer_action()]);
    let op = op_json(&paths, "/actions/createCustomer", "post");
    let schema = &op["requestBody"]["content"]["application/json"]["schema"];
    assert!(
        schema["properties"]["orderCount"].is_null(),
        "derived properties are computed, never writable — must not appear in the action \
         request schema: {schema}"
    );
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `buck2 test --console none //src/services/query-api:openapi_gen`
Expected: `derived_properties_are_read_only_in_the_component_schema` FAILS (`orderCount` absent from the component). `derived_properties_do_not_appear_in_action_request_schemas` PASSES already (action schemas never included derived props) — it is a regression guard, so it staying green is correct.

- [ ] **Step 3: Add the `read_only_derived` helper and extend `type_component_schema`**

In `src/services/query-api/src/openapi_gen.rs`, add a small helper immediately before `type_component_schema` (`:143`):

```rust
/// Mark a property schema `readOnly` — derived/computed columns are served on reads but are
/// never writable. Non-object schemas (none arise for derived scalar aggregates) pass through.
fn read_only_derived(schema: RefOr<Schema>) -> RefOr<Schema> {
    match schema {
        RefOr::T(Schema::Object(mut obj)) => {
            obj.read_only = Some(true);
            RefOr::T(Schema::Object(obj))
        }
        other => other,
    }
}
```

Extend `type_component_schema` (`:143-159`) to iterate `ty.derived` after the physical properties, before applying `required`:

```rust
fn type_component_schema(ty: &ObjectType) -> RefOr<Schema> {
    let mut b = ObjectBuilder::new().schema_type(SchemaType::Type(Type::Object));
    if let Some(d) = &ty.description {
        b = b.description(Some(d.clone()));
    }
    for p in &ty.properties {
        b = b.property(
            p.name.clone(),
            property_schema(&p.ty, p.required, p.description.as_deref()),
        );
    }
    // Derived (aggregate-over-link) properties are computed, served on reads, and never
    // writable — declare them readOnly so the read document matches object-read rows while
    // the write/action schemas (built from action params) never gain them.
    for d in &ty.derived {
        b = b.property(
            d.name.clone(),
            read_only_derived(property_schema(&d.ty, false, d.description.as_deref())),
        );
    }
    if let Some(id) = &ty.identity {
        b = b.required(id.clone());
    }
    RefOr::T(Schema::Object(b.build()))
}
```

> `property_schema(&d.ty, false, ..)`: derived aggregates pass `required = false` (nullable) because SUM/AVG/MIN/MAX over an empty link set are NULL; the `readOnly` marker is what distinguishes them, not requiredness. They are deliberately NOT added to the schema's `required` array.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `buck2 test --console none //src/services/query-api:openapi_gen`
Expected: PASS (both new tests, and the pre-existing `generated_document_carries_ontology_descriptions` / codec tests unchanged).

- [ ] **Step 5: Lint + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/services/query-api/src/openapi_gen.rs src/services/query-api/tests/openapi_gen.rs
git commit -m "feat(query-api): declare derived properties readOnly in per-type read schema"
```

---

### Task 4: Whole-crate verification + register close

**Files:**
- Modify: `docs/ROADMAP.md` (remove the `road-ontology-derived-index-read-surface` entry), `docs/system-capabilities/query-api.md` (record the landed read surface) — via `loom-docs-update` at finish time.

- [ ] **Step 1: Build the whole query-api crate**

Run: `buck2 build -v0 --console none //src/services/query-api/...`
Expected: exit 0 (silent). A `query!`/clippy failure prints `BUILD FAILED` + the error.

- [ ] **Step 2: Run the full query-api test sweep**

Run: `buck2 test --console none //src/services/query-api/...`
Expected: `Tests finished: Pass N. Fail 0`. The e2e suites that pin full type-detail bodies (if any beyond `ontology_type_detail.rs`) must stay green — the two new keys are additive; if a body-oracle elsewhere breaks, add `derived: []`/`vector_indexes: []` to it (the only intended churn).

- [ ] **Step 3: Metric gate (FIX, not report) — run before opening the PR**

Fast-forward local `main` to `origin/main` first (stale base sweeps unrelated files), then:

```bash
git fetch origin main
loom-complexity diff -p src/services/query-api/src/http.rs -p src/services/query-api/src/openapi_gen.rs -p src/services/query-api/src/openapi.rs
loom-duplication diff -p src/services/query-api/src/http.rs -p src/services/query-api/src/openapi_gen.rs -p src/services/query-api/src/openapi.rs
```

Compare each touched function against the merge-base. `get_ontology_type`, `type_component_schema`, and the new small helpers are the only changed functions. If any hotspot worsened on any axis (cc/cognitive/MI/SLOC) or any cross-file duplication pair ≥ 20 lines appeared, FIX IT IN THIS PR (the `derived_view_json`/`vector_index_view_json` helpers already factor the render seam; `read_only_derived` factors the readOnly seam). Put before/after numbers in the PR body.

- [ ] **Step 4: Finish — PR + register close**

Use `superpowers:finishing-a-development-branch`: push `work/road-ontology-derived-index-read-surface`, open a PR from that head. In the PR, run `loom-docs-update` to remove `#road-ontology-derived-index-read-surface` from `docs/ROADMAP.md` and fold the landed capability into `docs/system-capabilities/query-api.md`, naming the id + PR number in the PR body. (Registers carry open work only.)

---

## Self-Review

**1. Spec coverage:**
- Spec leg 1 (type detail `derived[]`, `{name, ty, link, agg, description?}`, `set_description` convention) → Task 1 (`derived_view_json`). ✅
- Spec leg 2 (type detail `vector_indexes[]`, `{name, property, metric, spec, description?}`, omit `type_name`, serde reps, `vector_indexes_for` on both direct + wire CPs) → Task 1 (`vector_index_view_json` + the `onto.vector_indexes_for` call; the call is a trait method so the wire CP is covered). ✅
- Spec leg 3 (per-type read schema declares derived props `readOnly: true`, description-carrying, absent from write/action schemas) → Task 3. ✅
- Spec `TypeDetailResponse` doc extension (`DerivedPropertyView`, `VectorIndexView`) → Task 2. ✅
- Spec testing bullets (extend `:167-180` exact-array oracle for empty-array shape; extend `:206-215` description assertions; `openapi_gen.rs` readOnly-present + write-absent + `TypeDetailResponse` documents the arrays) → Tasks 1–3. ✅
- Spec non-regression (additive keys; no control-plane/schema/wire/`.sqlx` change; `POST /search` untouched) → honored; no adapter or migration files touched. ✅
- Spec acceptance 1–4 → Tasks 1 (routes), 3 (schema), 1+3 (no-longer-write-only), 4 (suites green). ✅

**2. Placeholder scan:** No TBD/TODO/"add error handling"/"similar to Task N". Every code step shows complete code. The one conditional (`#[schema(value_type = Object)]` fallback) is a named, explicit branch, not a placeholder.

**3. Type consistency:** `derived_view_json(&DerivedPropertyDef)` / `vector_index_view_json(&VectorIndexDef)` / `read_only_derived(RefOr<Schema>) -> RefOr<Schema>` are used with matching signatures throughout. `vector_indexes_for` returns `Result<Vec<VectorIndexDef>>`, matched by `Ok(v) => v` + `sort_by`. `DerivedPropertyView`/`VectorIndexView` field names (`name`/`ty`/`link`/`agg`/`description`; `name`/`property`/`metric`/`spec`/`description`) match the JSON keys the handler emits in Task 1. `Aggregation::Sum(String)`, `Metric::{Cosine,L2}`, `IndexSpec::{Flat,Hnsw{m,ef_construction}}` used consistently in seeds and oracles.
