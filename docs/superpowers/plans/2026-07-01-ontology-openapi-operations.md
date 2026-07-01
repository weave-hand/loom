# Ontology-derived OpenAPI operations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** query-api's `GET /openapi.json` regenerates per request from the live ontology, so a client sees concrete per-type operations (`GET /objects/Customer` returning a typed `Customer`, per-link traversals, and a typed-insert `POST` body) instead of only the opaque `{type}` templates.

**Architecture:** A **pure** generator `ontology_openapi(types, links) -> (Paths, Schemas)` maps an ontology snapshot to OpenAPI content via a base/logical-type → OpenAPI schema codec. A dynamic `/openapi.json` handler reads the live ontology (`list_types` + per-type `links`) through query-api's **direct** `PgControlPlane` (the wire governance client does not implement `list_types`), runs the generator, and `.extend`s the result onto a clone of the static `build_openapi()`. A new `service_runtime::with_openapi_provider` seam serves that dynamic doc and points `/docs` (Scalar) at the `/openapi.json` URL so the UI reflects runtime types too. Ingest is untouched (still static `with_openapi`).

**Tech Stack:** Rust, axum, utoipa 5.5 (`utoipa::openapi::*` programmatic builders), utoipa-scalar 0.2, buck2 (`rust_test` targets), `control_plane_core` logical-type vocabulary (`BaseType`/`resolve_logical`).

## Global Constraints

- **Tests are `rust_test` integration targets only** — one `tests/<name>.rs` file per target, wired in the crate `BUCK` via the `loom_rust_test`/`loom_fixture_test` macros. NO inline `#[cfg(test)]` (the `no-inline-tests` prek hook fails the build otherwise).
- **New test targets use `loom_rust_test`** (pure-logic, RE-eligible). The generator/liveness tests are backed by `MemoryControlPlane` and need NO postgres — do **not** use `loom_fixture_test` for them.
- **Clippy is strict** (pedantic + restriction) on production lib code: no `unwrap`/`expect`/`panic`/`indexing_slicing`/`todo` in `src/**`. Use `#[expect(lint, reason = "...")]` locally if unavoidable. Test code is exempted from panic-safety lints by the macro wrapper.
- **Do NOT modify `build_openapi()`'s output** — the static route set is drift-guarded by `tests/openapi.rs::documents_exactly_the_expected_routes`. All ontology content is added at request time, never baked into `build_openapi()`.
- **utoipa emits OpenAPI 3.1** — nullability is a type array (`["string","null"]`), NOT a `nullable: true` keyword.
- **buck2 build/test discipline:** never pipe `buck2 test`/`buck2 bxl` through `tail`/`head` — redirect to a file and grep it. `buck2 build … | tail` is fine.
- **Faithful-to-wire codec:** the OpenAPI schema for each property MUST describe what `render::render_cell` actually emits on the wire (see Task 1 rationale). This is the North Star ("a faithful, current contract").

---

## Design decisions (read before starting)

These resolve ambiguities in the spec; the reviewer should sanity-check them.

1. **Ontology source = the direct `PgControlPlane`, not the wire plane.** `WireOntology::list_types` returns `Err(read_only("list_types"))` (`src/services/query-api/src/wire_control_plane.rs:135`) — the wire governance client cannot enumerate types. `main.rs` already retains `pg: Arc<PgControlPlane>` (used for Auth/bootstrap/GC). The doc provider captures `pg` (as `Arc<dyn ControlPlane + Send + Sync>`) and reads the ontology mirror directly from Postgres. This is consistent with `pg` already being query-api's escape hatch for surfaces not on the wire, and the ontology mirror in Postgres is the source of truth.

2. **`Long` maps to OpenAPI `string`, not `integer`.** The spec text says "integer kinds → integer", but `render::render_cell` (`src/services/query-api/src/render.rs:67`) emits a `Long` (`JsonRepr::NumericString`) as `json!(i.to_string())` — a JSON **string**, to preserve int64 precision past 2^53. Documenting it as `integer` would make the doc lie about the wire and break codegen clients. So the codec is keyed off the actual wire rendering (`BaseType::json_repr`), which is the spec's faithful-contract intent. `Integer`/`Double` stay JSON numbers.

3. **Typed-insert POST path = `POST /objects/{Type}`.** The real insert route is `POST /actions/{action_name}` with an arbitrary user-chosen action name; there is no `list_actions` and the generator's input is `(types, links)` only, so a concrete per-action path is impossible. Paralleling the spec's `GET /objects/{Type}` per-type specialization, the typed insert is emitted as `POST /objects/{Type}` whose description states the real invocation path (invoke the type's `Insert` action via `POST /actions/{actionName}`). The request body references the same per-type `{Type}` component schema (the spec explicitly allows "the per-type component schema (or a write-variant of it)"). A real per-type insert route is recorded as a FUTURE follow-up.

4. **Component schema per type = read shape `{Type}` only.** Properties are the type's `PropertyDef`s mapped by the codec; the identity property is `required`; each property's nullability = `!prop.required`. Derived properties are out of scope (spec line 141). The insert POST reuses this same schema as its body.

## File Structure

- **Create `src/services/query-api/src/openapi_gen.rs`** — the pure generator + codec. `ontology_openapi(types, links) -> (Paths, Schemas)` and `base_type_to_schema(BaseType, nullable) -> RefOr<Schema>`. No I/O.
- **Modify `src/services/query-api/src/openapi.rs`** — add `live_openapi(cp) -> OpenApi` (async: reads the ontology, calls the generator, merges onto a `build_openapi()` clone).
- **Modify `src/services/query-api/src/lib.rs`** — `mod openapi_gen;` + re-export `live_openapi`.
- **Modify `src/services/runtime/src/openapi.rs`** — factor out `register_bearer_scheme`; add `with_openapi_provider`.
- **Modify `src/services/runtime/src/lib.rs`** — re-export `with_openapi_provider`.
- **Modify `src/services/query-api/src/main.rs`** — swap the static `with_openapi(app, build_openapi())` for `with_openapi_provider(app, provider)` capturing `pg`.
- **Create `src/services/query-api/tests/openapi_gen.rs`** — codec + generator + liveness + static-intact + full-catalog unit tests (memory-backed).
- **Modify `src/services/query-api/BUCK`** — add the `openapi_gen` lib source to the `query-api` crate is not needed (it's a `mod`); add the new `rust_test` target `openapi-gen`.
- **Modify `src/services/runtime/tests/openapi.rs`** (+ its BUCK target already exists) — add a `with_openapi_provider` seam test.
- **Modify `docs/ROADMAP.md` / `docs/FUTURE.md`** — close `road-autogen-api-specs`, record the follow-ups (done in the finish step via `loom-docs-update`).

---

### Task 1: Base/logical-type → OpenAPI schema codec

**Files:**
- Create: `src/services/query-api/src/openapi_gen.rs`
- Modify: `src/services/query-api/src/lib.rs` (add `mod openapi_gen;`)
- Test: `src/services/query-api/tests/openapi_gen.rs`
- Modify: `src/services/query-api/BUCK` (add `openapi_gen` test target)

**Interfaces:**
- Consumes: `control_plane_core::{BaseType, resolve_logical}`; `utoipa::openapi::{RefOr, Schema, Object, Array, schema::{Type, SchemaType, SchemaFormat, KnownFormat}}`.
- Produces: `pub fn base_type_to_schema(bt: BaseType, nullable: bool) -> utoipa::openapi::RefOr<utoipa::openapi::Schema>` and a private `pub(crate) fn property_schema(ty: &str, required: bool) -> RefOr<Schema>` (resolves the logical name, falls back to a free-form string schema on an unknown type).

**Notes for the implementer:**
- utoipa 5.5's schema builders live under `utoipa::openapi::schema`. Confirm the exact names against the crate by building (they are: `ObjectBuilder`, `ArrayBuilder`, `Type` enum `{ Object, String, Integer, Number, Boolean, Array, Null }`, `SchemaType`, `SchemaFormat::KnownFormat(KnownFormat::{Int32, Int64, Double, Float, Date, DateTime})`).
- OpenAPI 3.1 nullability = a **type array** including `Null`. Build the schema_type as `SchemaType::new_array_from(...)` / `SchemaType::from_iter([Type::String, Type::Null])` when `nullable`. Verify the exact constructor by building; if `from_iter` is unavailable, use `SchemaType::Array(vec![Type::String, Type::Null])`.
- Mapping (faithful to `render::render_cell`, see Design decision 2):
  - `Integer` → `integer`, format `Int32`
  - `Long` → `string` (add `.description("int64 encoded as a decimal string")`)
  - `Double` → `number`, format `Double`
  - `Boolean` → `boolean`
  - `String` → `string`
  - `Date` → `string`, format `Date`
  - `Timestamp` → `string`, format `DateTime`
  - `Vector(n)` → `array` of `number`(format `Float`), `min_items(Some(n as usize))`, `max_items(Some(n as usize))`
- `property_schema(ty, required)`: `resolve_logical(ty)` → `Some(bt)` → `base_type_to_schema(bt, !required)`; `None` → a plain `string` schema (unknown logical type falls back, mirroring `render`'s "never fail a permitted read" posture).

- [ ] **Step 1: Write the failing codec tests**

Create `src/services/query-api/tests/openapi_gen.rs`:

```rust
//! Pure unit tests for the ontology→OpenAPI generator and its type codec. Memory-backed
//! (no postgres): the generator is pure and the liveness handler reads any `ControlPlane`.

use control_plane_core::BaseType;
use query_api::openapi_gen::base_type_to_schema;

/// Serialize a schema to JSON for structural assertions (utoipa's typed builders are
/// awkward to pattern-match; the emitted JSON is the contract a client consumes).
fn to_json(schema: &utoipa::openapi::RefOr<utoipa::openapi::Schema>) -> serde_json::Value {
    serde_json::to_value(schema).unwrap()
}

#[test]
fn integer_maps_to_int32_number() {
    let j = to_json(&base_type_to_schema(BaseType::Integer, false));
    assert_eq!(j["type"], "integer");
    assert_eq!(j["format"], "int32");
}

#[test]
fn long_maps_to_string_not_integer() {
    // The wire renders Long as a decimal string (render.rs NumericString); the doc must match.
    let j = to_json(&base_type_to_schema(BaseType::Long, false));
    assert_eq!(j["type"], "string");
}

#[test]
fn double_maps_to_number() {
    let j = to_json(&base_type_to_schema(BaseType::Double, false));
    assert_eq!(j["type"], "number");
}

#[test]
fn boolean_maps_to_boolean() {
    assert_eq!(to_json(&base_type_to_schema(BaseType::Boolean, false))["type"], "boolean");
}

#[test]
fn string_maps_to_string() {
    assert_eq!(to_json(&base_type_to_schema(BaseType::String, false))["type"], "string");
}

#[test]
fn date_and_timestamp_are_formatted_strings() {
    let d = to_json(&base_type_to_schema(BaseType::Date, false));
    assert_eq!(d["type"], "string");
    assert_eq!(d["format"], "date");
    let t = to_json(&base_type_to_schema(BaseType::Timestamp, false));
    assert_eq!(t["type"], "string");
    assert_eq!(t["format"], "date-time");
}

#[test]
fn vector_maps_to_bounded_number_array() {
    let j = to_json(&base_type_to_schema(BaseType::Vector(4), false));
    assert_eq!(j["type"], "array");
    assert_eq!(j["items"]["type"], "number");
    assert_eq!(j["minItems"], 4);
    assert_eq!(j["maxItems"], 4);
}

#[test]
fn nullable_adds_null_to_type() {
    // OpenAPI 3.1: nullability is a type array including "null".
    let j = to_json(&base_type_to_schema(BaseType::String, true));
    let ty = &j["type"];
    // Either ["string","null"] (array) — assert null is present.
    let types: Vec<String> = match ty {
        serde_json::Value::Array(a) => a.iter().filter_map(|v| v.as_str().map(String::from)).collect(),
        serde_json::Value::String(s) => vec![s.clone()],
        _ => vec![],
    };
    assert!(types.contains(&"null".to_string()), "nullable string must include null: {ty}");
    assert!(types.contains(&"string".to_string()));
}
```

- [ ] **Step 2: Wire the BUCK test target and confirm it fails to build**

Add to `src/services/query-api/BUCK` (mirror the existing `openapi` test target near line 992). Confirm `openapi_gen` is exported by the crate first (Step 3 adds `mod openapi_gen; pub use ...`). The test target:

```python
loom_rust_test(
    name = "openapi-gen",
    srcs = ["tests/openapi_gen.rs"],
    crate = "openapi_gen",
    crate_root = "tests/openapi_gen.rs",
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
        "//third-party:serde_json",
        "//third-party:utoipa",
    ],
)
```

Run: `buck2 build //src/services/query-api:openapi-gen 2>&1 | tail -20`
Expected: FAIL — `base_type_to_schema` / `openapi_gen` unresolved.

- [ ] **Step 3: Implement the codec**

Add `mod openapi_gen;` and `pub use openapi_gen::{...};` to `src/services/query-api/src/lib.rs` (make the module `pub` so the test can reach `query_api::openapi_gen::base_type_to_schema`). Then create the codec in `openapi_gen.rs`:

```rust
//! Pure ontology→OpenAPI generation: map an ontology snapshot (types + links) to OpenAPI
//! `Paths` + component `Schemas`. No I/O — the caller reads the live ontology and hands it
//! a snapshot. The type codec is faithful to `crate::render`'s wire rendering (e.g. Long is
//! a decimal string, not a JSON integer).

use control_plane_core::{BaseType, resolve_logical};
use utoipa::openapi::schema::{
    ArrayBuilder, KnownFormat, ObjectBuilder, SchemaFormat, SchemaType, Type,
};
use utoipa::openapi::{RefOr, Schema};

/// The OpenAPI schema for one loom base type, `nullable` widening the type to also admit
/// JSON null (OpenAPI 3.1 type-array form). Faithful to `crate::render::render_cell`.
#[must_use]
pub fn base_type_to_schema(bt: BaseType, nullable: bool) -> RefOr<Schema> {
    let (ty, format): (Type, Option<SchemaFormat>) = match bt {
        BaseType::Integer => (Type::Integer, Some(SchemaFormat::KnownFormat(KnownFormat::Int32))),
        // Long renders as a decimal string on the wire (int64 > 2^53 safe range).
        BaseType::Long => (Type::String, None),
        BaseType::Double => (Type::Number, Some(SchemaFormat::KnownFormat(KnownFormat::Double))),
        BaseType::Boolean => (Type::Boolean, None),
        BaseType::String => (Type::String, None),
        BaseType::Date => (Type::String, Some(SchemaFormat::KnownFormat(KnownFormat::Date))),
        BaseType::Timestamp => {
            (Type::String, Some(SchemaFormat::KnownFormat(KnownFormat::DateTime)))
        }
        BaseType::Vector(n) => return vector_schema(n, nullable),
    };
    let schema_type = if nullable {
        SchemaType::from_iter([ty, Type::Null])
    } else {
        SchemaType::new(ty)
    };
    let mut b = ObjectBuilder::new().schema_type(schema_type);
    if let Some(f) = format {
        b = b.format(Some(f));
    }
    if matches!(bt, BaseType::Long) {
        b = b.description(Some("int64 encoded as a decimal string"));
    }
    RefOr::T(Schema::Object(b.build()))
}

fn vector_schema(n: u32, nullable: bool) -> RefOr<Schema> {
    let item = ObjectBuilder::new()
        .schema_type(SchemaType::new(Type::Number))
        .format(Some(SchemaFormat::KnownFormat(KnownFormat::Float)))
        .build();
    let schema_type = if nullable {
        SchemaType::from_iter([Type::Array, Type::Null])
    } else {
        SchemaType::new(Type::Array)
    };
    let arr = ArrayBuilder::new()
        .schema_type(schema_type)
        .items(RefOr::T(Schema::Object(item)))
        .min_items(Some(n as usize))
        .max_items(Some(n as usize))
        .build();
    RefOr::T(Schema::Array(arr))
}

/// The schema for a property's logical type. An unrecognized logical type falls back to a
/// free-form string (mirrors `render`'s never-fail-a-permitted-read posture).
#[must_use]
pub(crate) fn property_schema(ty: &str, required: bool) -> RefOr<Schema> {
    match resolve_logical(ty) {
        Some(bt) => base_type_to_schema(bt, !required),
        None => RefOr::T(Schema::Object(
            ObjectBuilder::new().schema_type(SchemaType::new(Type::String)).build(),
        )),
    }
}
```

> **Implementer:** the exact utoipa 5.5 method names (`SchemaType::new`, `SchemaType::from_iter`, `ArrayBuilder::schema_type`, `min_items`) may differ slightly — build, read the compiler error, and adjust to the crate's real surface. The JSON assertions in Step 1 are the contract; the builder path is an implementation detail. If `ArrayBuilder` has no `schema_type`, drop that line (an array's type is implicit) and represent a nullable vector via a `OneOf`/`AllOf` only if a test requires it — the `nullable` vector case is not asserted, so a non-nullable array is sufficient for the vector test.

- [ ] **Step 4: Run the codec tests**

Run: `buck2 test //src/services/query-api:openapi-gen 2>&1 > /tmp/t1.log; grep -E "Tests finished|FAIL|PASS" /tmp/t1.log`
Expected: all codec tests PASS. Iterate on builder names until green.

- [ ] **Step 5: Commit**

```bash
git add src/services/query-api/src/openapi_gen.rs src/services/query-api/src/lib.rs \
        src/services/query-api/tests/openapi_gen.rs src/services/query-api/BUCK
git commit -m "feat(query-api): base/logical-type to OpenAPI schema codec"
```

---

### Task 2: The pure `ontology_openapi` generator

**Files:**
- Modify: `src/services/query-api/src/openapi_gen.rs`
- Test: `src/services/query-api/tests/openapi_gen.rs` (extend)

**Interfaces:**
- Consumes: `control_plane_core::{ObjectType, LinkDef, TypeName, PropertyDef}`; Task 1's `property_schema`; `service_runtime::BEARER_SCHEME_NAME`; `utoipa::openapi::{Paths, Components, RefOr, Schema, Ref, Object, Array, path::{Operation, OperationBuilder, HttpMethod, Parameter, ParameterIn}, PathItem, PathItemBuilder, PathsBuilder, ResponseBuilder, ResponsesBuilder, ContentBuilder, security::SecurityRequirement}}`.
- Produces:
  ```rust
  pub fn ontology_openapi(
      types: &[control_plane_core::ObjectType],
      links: &[control_plane_core::LinkDef],
  ) -> (utoipa::openapi::path::Paths, std::collections::BTreeMap<String, utoipa::openapi::RefOr<utoipa::openapi::Schema>>)
  ```
  (Confirm the `Paths` path: it is `utoipa::openapi::path::Paths` re-exported as `utoipa::openapi::Paths`. `Components::schemas` is a `BTreeMap<String, RefOr<Schema>>`, so returning that map lets the caller `.extend` directly.)

**Behavior:** For each `ObjectType`:
- **component schema** keyed by the type name: an `object` whose `properties` are the type's `PropertyDef`s via `property_schema(p.ty, p.required)`; `required` = the identity property name if `ty.identity` is `Some`.
- **`GET /objects/{name}`** — 200 response `application/json` = an object `{ "objects": { type: array, items: $ref #/components/schemas/{name} } }`; `security` = bearer; `tag = "objects"`; summary `"List {name} objects"`.
- **`POST /objects/{name}`** — request body `application/json` = `$ref #/components/schemas/{name}`; 201 response; `security` = bearer; `tag = "actions"`; description names the real invocation (`POST /actions/{actionName}` for the type's Insert action).

For each `LinkDef` whose `from`/`to` types are in the snapshot:
- **`GET /objects/{from}/links/{link_name}`** — 200 response = `{ "objects": { array, items: $ref #/components/schemas/{to} } }`; bearer; `tag = "links"`; summary `"Traverse {from}.{link_name} -> {to}"`.

Helper (private) builders keep it DRY:
- `objects_response_ref(type_name: &str) -> RefOr<Schema>` — the `{ objects: [ $ref ] }` wrapper.
- `bearer() -> SecurityRequirement` — `SecurityRequirement::new(BEARER_SCHEME_NAME, Vec::<String>::new())`.
- `get_op(summary, tag, response_schema) -> Operation`, `insert_op(type_name) -> Operation`.

- [ ] **Step 1: Write the failing generator test**

Extend `tests/openapi_gen.rs`:

```rust
use control_plane_core::{LinkBacking, LinkDef, ObjectType, PropertyDef, TableRef, TypeName, Cardinality};
use query_api::openapi_gen::ontology_openapi;

fn tref(schema: &str, table: &str) -> TableRef {
    TableRef { schema: schema.into(), name: table.into() }
}

fn customer() -> ObjectType {
    ObjectType {
        name: TypeName("Customer".into()),
        properties: vec![
            PropertyDef { name: "id".into(), ty: "long".into(), required: true },
            PropertyDef { name: "email".into(), ty: "string".into(), required: false },
            PropertyDef { name: "score".into(), ty: "double".into(), required: false },
        ],
        derived: vec![],
        table: tref("main", "customer"),
        identity: Some("id".into()),
    }
}

fn order() -> ObjectType {
    ObjectType {
        name: TypeName("Order".into()),
        properties: vec![PropertyDef { name: "id".into(), ty: "long".into(), required: true }],
        derived: vec![],
        table: tref("main", "orders"),
        identity: Some("id".into()),
    }
}

fn orders_link() -> LinkDef {
    LinkDef {
        name: "orders".into(),
        from: TypeName("Customer".into()),
        to: TypeName("Order".into()),
        cardinality: Cardinality::Many,
        backing: LinkBacking::ForeignKey { from_column: "id".into(), to_column: "customer_id".into() },
    }
}

/// Flatten (method, path) pairs from generated Paths via JSON.
fn methods_and_paths(paths: &utoipa::openapi::path::Paths) -> std::collections::BTreeSet<(String, String)> {
    const METHODS: [&str; 8] = ["get", "put", "post", "delete", "options", "head", "patch", "trace"];
    let json = serde_json::to_value(paths).unwrap();
    let mut out = std::collections::BTreeSet::new();
    if let Some(obj) = json["paths"].as_object().or_else(|| json.as_object()) {
        for (path, item) in obj {
            if let Some(ops) = item.as_object() {
                for m in ops.keys() {
                    if METHODS.contains(&m.as_str()) {
                        out.insert((m.clone(), path.clone()));
                    }
                }
            }
        }
    }
    out
}

#[test]
fn generates_per_type_operations() {
    let (paths, schemas) = ontology_openapi(&[customer(), order()], &[orders_link()]);
    let mp = methods_and_paths(&paths);
    assert!(mp.contains(&("get".into(), "/objects/Customer".into())));
    assert!(mp.contains(&("post".into(), "/objects/Customer".into())));
    assert!(mp.contains(&("get".into(), "/objects/Order".into())));
    assert!(mp.contains(&("get".into(), "/objects/Customer/links/orders".into())));

    // Component schema present with typed properties + identity required.
    let cust = serde_json::to_value(schemas.get("Customer").expect("Customer schema")).unwrap();
    assert_eq!(cust["properties"]["id"]["type"], "string"); // long -> string
    assert_eq!(cust["properties"]["score"]["type"], "number");
    let required: Vec<String> = cust["required"].as_array().unwrap_or(&vec![])
        .iter().filter_map(|v| v.as_str().map(String::from)).collect();
    assert!(required.contains(&"id".to_string()), "identity must be required");
}

#[test]
fn link_response_targets_the_to_type() {
    let (paths, _schemas) = ontology_openapi(&[customer(), order()], &[orders_link()]);
    let json = serde_json::to_value(&paths).unwrap();
    // Walk to the link op's 200 response items $ref; it must reference Order.
    let s = serde_json::to_string(&json).unwrap();
    assert!(s.contains("/objects/Customer/links/orders"));
    assert!(s.contains("Order"), "link response should reference the target type schema");
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `buck2 build //src/services/query-api:openapi-gen 2>&1 | tail -20`
Expected: FAIL — `ontology_openapi` unresolved.

- [ ] **Step 3: Implement the generator**

Append to `src/services/query-api/src/openapi_gen.rs`. (Add `":query-api"`'s dep on `//src/services/runtime:runtime` is already present for `BEARER_SCHEME_NAME` — verify in the crate BUCK `deps`.) Implement with utoipa builders:

```rust
use std::collections::BTreeMap;

use control_plane_core::{LinkDef, ObjectType};
use service_runtime::BEARER_SCHEME_NAME;
use utoipa::openapi::path::{HttpMethod, Operation, OperationBuilder, Paths, PathsBuilder};
use utoipa::openapi::security::SecurityRequirement;
use utoipa::openapi::{
    Content, PathItem, Ref, RefOr as OApiRefOr, ResponseBuilder, Schema as OApiSchema,
};

fn bearer() -> SecurityRequirement {
    SecurityRequirement::new(BEARER_SCHEME_NAME, Vec::<String>::new())
}

/// `{ "objects": [ $ref #/components/schemas/{type_name} ] }` — the typed read wrapper.
fn objects_response_schema(type_name: &str) -> OApiRefOr<OApiSchema> {
    let item = OApiRefOr::Ref(Ref::from_schema_name(type_name));
    let array = ArrayBuilder::new().items(item).build();
    let obj = ObjectBuilder::new()
        .schema_type(SchemaType::new(Type::Object))
        .property("objects", OApiRefOr::T(OApiSchema::Array(array)))
        .build();
    OApiRefOr::T(OApiSchema::Object(obj))
}

fn json_response(schema: OApiRefOr<OApiSchema>, description: &str) -> utoipa::openapi::Response {
    ResponseBuilder::new()
        .description(description)
        .content("application/json", Content::new(Some(schema)))
        .build()
}

/// The component (read) schema for a type: properties by codec, identity required.
fn type_component_schema(ty: &ObjectType) -> OApiRefOr<OApiSchema> {
    let mut b = ObjectBuilder::new().schema_type(SchemaType::new(Type::Object));
    for p in &ty.properties {
        b = b.property(p.name.clone(), property_schema(&p.ty, p.required));
    }
    if let Some(id) = &ty.identity {
        b = b.required(id.clone());
    }
    OApiRefOr::T(OApiSchema::Object(b.build()))
}

fn get_objects_op(type_name: &str) -> Operation {
    OperationBuilder::new()
        .summary(Some(format!("List {type_name} objects")))
        .tag("objects")
        .security(bearer())
        .response(
            "200",
            json_response(objects_response_schema(type_name), "Matching objects"),
        )
        .build()
}

fn insert_op(type_name: &str) -> Operation {
    use utoipa::openapi::request_body::RequestBodyBuilder;
    let body = RequestBodyBuilder::new()
        .content(
            "application/json",
            Content::new(Some(OApiRefOr::Ref(Ref::from_schema_name(type_name)))),
        )
        .build();
    OperationBuilder::new()
        .summary(Some(format!("Create a {type_name}")))
        .description(Some(format!(
            "Insert a {type_name}. Invoked via the type's Insert action at POST /actions/{{actionName}}."
        )))
        .tag("actions")
        .security(bearer())
        .request_body(Some(body))
        .response("201", json_response(
            OApiRefOr::Ref(Ref::from_schema_name(type_name)), "Created object"))
        .build()
}

fn link_op(from: &str, link_name: &str, to: &str) -> Operation {
    OperationBuilder::new()
        .summary(Some(format!("Traverse {from}.{link_name} -> {to}")))
        .tag("links")
        .security(bearer())
        .response("200", json_response(objects_response_schema(to), "Linked objects"))
        .build()
}

/// Map an ontology snapshot to OpenAPI paths + component schemas. Pure.
#[must_use]
pub fn ontology_openapi(
    types: &[ObjectType],
    links: &[LinkDef],
) -> (Paths, BTreeMap<String, OApiRefOr<OApiSchema>>) {
    let mut schemas: BTreeMap<String, OApiRefOr<OApiSchema>> = BTreeMap::new();
    let mut pb = PathsBuilder::new();
    for ty in types {
        let name = &ty.name.0;
        schemas.insert(name.clone(), type_component_schema(ty));
        let item = PathItem::new(HttpMethod::Get, get_objects_op(name));
        pb = pb.path(format!("/objects/{name}"), item);
        // POST onto the same path key: merge a second operation.
        pb = pb.path(
            format!("/objects/{name}"),
            PathItem::new(HttpMethod::Post, insert_op(name)),
        );
    }
    for l in links {
        // Only emit links whose endpoints are both in the snapshot (defensive).
        pb = pb.path(
            format!("/objects/{}/links/{}", l.from.0, l.name),
            PathItem::new(HttpMethod::Get, link_op(&l.from.0, &l.name, &l.to.0)),
        );
    }
    (pb.build(), schemas)
}
```

> **Implementer footgun — the GET+POST-on-one-path merge:** `PathsBuilder::path` inserts a whole `PathItem`; calling it twice for the same key **may overwrite** rather than merge the operations, dropping the GET. Verify with the `generates_per_type_operations` test (it asserts BOTH get and post on `/objects/Customer`). If the second call overwrites: build ONE `PathItem` carrying both operations instead — construct `PathItem::new(HttpMethod::Get, get_op)` then add the post via the `PathItem` builder/`operations` map (utoipa 5 `PathItem` has per-method `Option<Operation>` fields or an `operations` map; set both `get` and `post`), then `pb.path(key, item)` once. Adjust to the real utoipa 5.5 `PathItem` surface — the test is the gate.

- [ ] **Step 4: Run the generator tests**

Run: `buck2 test //src/services/query-api:openapi-gen 2>&1 > /tmp/t2.log; grep -E "Tests finished|FAIL|PASS" /tmp/t2.log`
Expected: all PASS (both GET and POST present on `/objects/Customer`, link targets Order). Iterate on the PathItem merge until green.

- [ ] **Step 5: Commit**

```bash
git add src/services/query-api/src/openapi_gen.rs src/services/query-api/tests/openapi_gen.rs
git commit -m "feat(query-api): pure ontology->OpenAPI paths+schemas generator"
```

---

### Task 3: `live_openapi` — read the ontology and merge onto the static base

**Files:**
- Modify: `src/services/query-api/src/openapi.rs`
- Modify: `src/services/query-api/src/lib.rs` (re-export `live_openapi`)
- Test: `src/services/query-api/tests/openapi_gen.rs` (extend — memory-backed liveness + static-intact + full-catalog)
- Modify: `src/services/query-api/BUCK` (add memory + async deps to the `openapi-gen` test target)

**Interfaces:**
- Consumes: `crate::openapi_gen::ontology_openapi`; `crate::build_openapi`; `control_plane_core::{ControlPlane, Ontology, TypeName, PageReq, Cursor}`.
- Produces:
  ```rust
  pub async fn live_openapi(cp: std::sync::Arc<dyn control_plane_core::ControlPlane + Send + Sync>)
      -> utoipa::openapi::OpenApi
  ```
  Reads all types (draining `list_types` pages, bounded), then per-type `links`, runs `ontology_openapi`, and `.paths.paths.extend(...)` + `components.schemas.extend(...)` onto a `build_openapi()` clone. On any read error, logs (`tracing::warn!`) and returns the static base unchanged — a doc endpoint must never 500 the whole document because the ontology read hiccuped.

- [ ] **Step 1: Write the failing liveness + static-intact tests**

Extend `tests/openapi_gen.rs`:

```rust
use std::sync::Arc;
use control_plane_core::{ControlPlane, Ontology};
use control_plane_memory::MemoryControlPlane;

#[tokio::test]
async fn live_doc_reflects_defined_types_without_restart() {
    let cp: Arc<dyn ControlPlane + Send + Sync> = Arc::new(MemoryControlPlane::new());
    cp.ontology().define_type(customer()).await.unwrap();

    let doc1 = query_api::live_openapi(cp.clone()).await;
    let j1 = serde_json::to_value(&doc1).unwrap();
    assert!(j1["paths"]["/objects/Customer"]["get"].is_object());
    assert!(j1["components"]["schemas"]["Customer"].is_object());
    assert!(j1["paths"]["/objects/Order"].is_null(), "Order not defined yet");

    // Define a NEW type; the next generation must include it (per-request liveness).
    cp.ontology().define_type(order()).await.unwrap();
    let doc2 = query_api::live_openapi(cp.clone()).await;
    let j2 = serde_json::to_value(&doc2).unwrap();
    assert!(j2["paths"]["/objects/Order"]["get"].is_object(), "new type appears live");
}

#[tokio::test]
async fn static_framework_survives_merge() {
    let cp: Arc<dyn ControlPlane + Send + Sync> = Arc::new(MemoryControlPlane::new());
    cp.ontology().define_type(customer()).await.unwrap();
    let doc = query_api::live_openapi(cp).await;
    let j = serde_json::to_value(&doc).unwrap();
    // The hand-written template + the bearer scheme survive.
    assert!(j["paths"]["/objects/{type_name}"]["get"].is_object(), "static template intact");
    assert_eq!(j["openapi"].as_str().unwrap().get(0..3), Some("3.1"));
}

#[tokio::test]
async fn full_catalog_is_generated() {
    let cp: Arc<dyn ControlPlane + Send + Sync> = Arc::new(MemoryControlPlane::new());
    cp.ontology().define_type(customer()).await.unwrap();
    cp.ontology().define_type(order()).await.unwrap();
    let doc = query_api::live_openapi(cp).await;
    let j = serde_json::to_value(&doc).unwrap();
    assert!(j["paths"]["/objects/Customer"].is_object());
    assert!(j["paths"]["/objects/Order"].is_object());
}
```

Add the deps to the `openapi-gen` BUCK target: `"//src/control-plane/memory:memory"`, `"//third-party:tokio"`.

- [ ] **Step 2: Run to verify it fails**

Run: `buck2 build //src/services/query-api:openapi-gen 2>&1 | tail -20`
Expected: FAIL — `live_openapi` unresolved.

- [ ] **Step 3: Implement `live_openapi`**

Add to `src/services/query-api/src/openapi.rs`:

```rust
use std::sync::Arc;

use control_plane_core::{ControlPlane, PageReq, TypeName};

/// Bound on `list_types` page draining — a defensive cap so a misbehaving cursor can never
/// spin forever (adapters return one full page today; this tolerates future keyset paging).
const MAX_TYPE_PAGES: usize = 10_000;

/// Build the OpenAPI document with per-request ontology-derived operations merged onto the
/// static base. Reads the live ontology through `cp`; on a read error, logs and returns the
/// static base (a docs endpoint must never fail the whole document on a transient read).
#[must_use]
pub async fn live_openapi(cp: Arc<dyn ControlPlane + Send + Sync>) -> utoipa::openapi::OpenApi {
    let mut doc = build_openapi();
    let onto = cp.ontology();

    // Drain all types.
    let mut types = Vec::new();
    let mut after = None;
    for _ in 0..MAX_TYPE_PAGES {
        let page = match onto.list_types(PageReq { after: after.clone(), limit: None }).await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(error = %e, "openapi: list_types failed; serving static base");
                return doc;
            }
        };
        types.extend(page.items);
        match page.next {
            Some(cursor) => after = Some(cursor),
            None => break,
        }
    }

    // Per-type links.
    let mut links = Vec::new();
    for ty in &types {
        match onto.links(&ty.name, PageReq::unbounded()).await {
            Ok(page) => links.extend(page.items),
            Err(e) => {
                tracing::warn!(type_name = %ty.name.0, error = %e, "openapi: links read failed");
            }
        }
    }

    let (paths, schemas) = crate::openapi_gen::ontology_openapi(&types, &links);
    doc.paths.paths.extend(paths.paths);
    if let Some(components) = doc.components.as_mut() {
        components.schemas.extend(schemas);
    }
    doc
}
```

Re-export from `src/services/query-api/src/lib.rs`: `pub use openapi::{build_openapi, live_openapi};` (keep the existing `build_openapi` export).

> **Implementer:** verify `doc.paths.paths` is the public `BTreeMap` field (utoipa 5 `Paths { paths: BTreeMap<String, PathItem>, extensions }`). If `paths` is private, use the `Paths` public API to iterate/insert, or merge via `doc.paths.paths` if `pub`. Confirm `TypeName` import is actually used; drop unused imports to satisfy clippy.

- [ ] **Step 4: Run the liveness tests**

Run: `buck2 test //src/services/query-api:openapi-gen 2>&1 > /tmp/t3.log; grep -E "Tests finished|FAIL|PASS" /tmp/t3.log`
Expected: all PASS (liveness, static-intact, full-catalog).

- [ ] **Step 5: Commit**

```bash
git add src/services/query-api/src/openapi.rs src/services/query-api/src/lib.rs src/services/query-api/BUCK
git commit -m "feat(query-api): live_openapi merges ontology ops onto the static base"
```

---

### Task 4: `with_openapi_provider` seam + live `/docs`

**Files:**
- Modify: `src/services/runtime/src/openapi.rs`
- Modify: `src/services/runtime/src/lib.rs` (re-export)
- Test: `src/services/runtime/tests/openapi.rs` (extend)

**Interfaces:**
- Consumes: `axum::{Router, routing::get, response::{Json, Html}}`; `utoipa::openapi::OpenApi`.
- Produces:
  ```rust
  pub fn register_bearer_scheme(doc: &mut utoipa::openapi::OpenApi)   // factored from with_openapi
  pub fn with_openapi_provider<F, Fut>(router: Router, provider: F) -> Router
  where
      F: Fn() -> Fut + Clone + Send + Sync + 'static,
      Fut: std::future::Future<Output = utoipa::openapi::OpenApi> + Send + 'static;
  ```
  `with_openapi_provider` mounts `GET /openapi.json` (calls `provider()` per request, applies `register_bearer_scheme`, serves JSON) and `GET /docs` (Scalar HTML pointed at `/openapi.json`). The existing `with_openapi` is refactored to call `register_bearer_scheme` (behavior unchanged for ingest — verified by the existing seam tests).

- [ ] **Step 1: Write the failing seam test**

Extend `src/services/runtime/tests/openapi.rs` (mirror the existing `sample_doc()` helper):

```rust
#[tokio::test]
async fn provider_serves_openapi_json_with_bearer_scheme() {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    // A provider that returns a doc carrying a marker path, proving per-request generation.
    let app = service_runtime::with_openapi_provider(axum::Router::new(), || async {
        let mut d = sample_doc();
        d.paths.paths.insert(
            "/live/marker".to_string(),
            utoipa::openapi::path::PathItem::new(
                utoipa::openapi::path::HttpMethod::Get,
                utoipa::openapi::path::OperationBuilder::new().build(),
            ),
        );
        d
    });

    let resp = app
        .oneshot(Request::builder().uri("/openapi.json").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    // The provider's marker path is present (per-request generation ran).
    assert!(json["paths"]["/live/marker"].is_object());
    // The bearer security scheme was applied by the seam.
    assert!(json["components"]["securitySchemes"]["bearer_auth"].is_object());
}

#[tokio::test]
async fn provider_serves_docs_html() {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;
    let app = service_runtime::with_openapi_provider(axum::Router::new(), || async { sample_doc() });
    let resp = app
        .oneshot(Request::builder().uri("/docs").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let html = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(html.contains("/openapi.json"), "docs must load the live spec URL");
}
```

Ensure the runtime `openapi` test target's deps include `tower` and `serde_json` (mirror the existing test's deps in `src/services/runtime/BUCK`).

- [ ] **Step 2: Run to verify it fails**

Run: `buck2 build //src/services/runtime:openapi 2>&1 | tail -20`
Expected: FAIL — `with_openapi_provider` unresolved.

- [ ] **Step 3: Implement the seam**

Rewrite `src/services/runtime/src/openapi.rs` to factor the scheme registration and add the provider:

```rust
use std::future::Future;

use axum::Router;
use axum::response::{Html, Json};
use axum::routing::get;
use utoipa::openapi::OpenApi;
use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa_scalar::{Scalar, Servable};

pub const BEARER_SCHEME_NAME: &str = "bearer_auth";

/// Minimal Scalar page that loads the live spec from `/openapi.json` at view time (so the
/// rendered UI reflects runtime-generated ontology operations). Scalar's viewer is fetched
/// from its CDN, matching the static `with_openapi` path's no-build-asset posture.
const SCALAR_DOCS_HTML: &str = r#"<!doctype html>
<html>
  <head>
    <title>loom API</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
  </head>
  <body>
    <script id="api-reference" data-url="/openapi.json"></script>
    <script src="https://cdn.jsdelivr.net/npm/@scalar/api-reference"></script>
  </body>
</html>"#;

/// Register loom's bearer-session security scheme on a document's components. Shared by the
/// static (`with_openapi`) and dynamic (`with_openapi_provider`) serving paths.
pub fn register_bearer_scheme(doc: &mut OpenApi) {
    let components = doc
        .components
        .get_or_insert_with(utoipa::openapi::Components::new);
    components.add_security_scheme(
        BEARER_SCHEME_NAME,
        SecurityScheme::Http(
            HttpBuilder::new()
                .scheme(HttpAuthScheme::Bearer)
                .bearer_format("opaque")
                .build(),
        ),
    );
}

/// Mount a STATIC OpenAPI document + Scalar docs UI (baked inline). Used by services with no
/// runtime-derived content (ingest).
#[must_use = "the returned Router must be used to serve requests"]
pub fn with_openapi(router: Router, mut doc: OpenApi) -> Router {
    register_bearer_scheme(&mut doc);
    let scalar: Router = Scalar::with_url("/docs", doc.clone()).into();
    let json_doc = doc;
    router
        .route(
            "/openapi.json",
            get(move || {
                let d = json_doc.clone();
                async move { Json(d) }
            }),
        )
        .merge(scalar)
}

/// Mount a DYNAMIC OpenAPI document: `/openapi.json` calls `provider` per request (applying
/// the bearer scheme to whatever it returns), and `/docs` loads that live spec by URL. Used by
/// query-api, whose document merges live ontology operations each request.
#[must_use = "the returned Router must be used to serve requests"]
pub fn with_openapi_provider<F, Fut>(router: Router, provider: F) -> Router
where
    F: Fn() -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = OpenApi> + Send + 'static,
{
    router
        .route(
            "/openapi.json",
            get(move || {
                let provider = provider.clone();
                async move {
                    let mut doc = provider().await;
                    register_bearer_scheme(&mut doc);
                    Json(doc)
                }
            }),
        )
        .route("/docs", get(|| async { Html(SCALAR_DOCS_HTML) }))
}
```

Re-export in `src/services/runtime/src/lib.rs`: extend the existing line to
`pub use openapi::{BEARER_SCHEME_NAME, register_bearer_scheme, with_openapi, with_openapi_provider};`.

- [ ] **Step 4: Run the seam tests (new + existing regression)**

Run: `buck2 test //src/services/runtime:openapi 2>&1 > /tmp/t4.log; grep -E "Tests finished|FAIL|PASS" /tmp/t4.log`
Expected: new provider tests PASS **and** the existing `with_openapi` seam tests still PASS (ingest path unchanged).

- [ ] **Step 5: Commit**

```bash
git add src/services/runtime/src/openapi.rs src/services/runtime/src/lib.rs src/services/runtime/tests/openapi.rs
git commit -m "feat(runtime): with_openapi_provider seam for dynamic OpenAPI docs"
```

---

### Task 5: Wire the dynamic provider into query-api's binary

**Files:**
- Modify: `src/services/query-api/src/main.rs`

**Interfaces:**
- Consumes: `service_runtime::with_openapi_provider`; `query_api::live_openapi`; the existing `pg: Arc<PgControlPlane>`.

**Behavior:** Replace `let app = service_runtime::with_openapi(app, query_api::build_openapi());` (main.rs:80) with a provider that captures `pg` and regenerates per request. `pg` is `Arc<PgControlPlane>`; coerce to `Arc<dyn ControlPlane + Send + Sync>` for `live_openapi`.

- [ ] **Step 1: Make the change**

In `src/services/query-api/src/main.rs`, replace line 80:

```rust
    // Dynamic OpenAPI: `/openapi.json` regenerates per request from the LIVE ontology,
    // read through the direct Postgres control plane (`pg`) — the wire governance client
    // does not implement `list_types`. `/docs` (Scalar) loads the live spec by URL.
    let openapi_cp: Arc<dyn ControlPlane> = pg.clone();
    let app = service_runtime::with_openapi_provider(app, move || {
        let cp = openapi_cp.clone();
        async move { query_api::live_openapi(cp).await }
    });
```

> `ControlPlane` is already imported at main.rs:7 (`use control_plane_core::ControlPlane;`). `live_openapi` takes `Arc<dyn ControlPlane + Send + Sync>`; the `dyn ControlPlane` trait object here is `Send + Sync` (its supertraits require it) — if the coercion complains, annotate `openapi_cp: Arc<dyn ControlPlane + Send + Sync>`.

- [ ] **Step 2: Build the binary**

Run: `buck2 build //src/services/query-api:query-api 2>&1 | tail -20`
Expected: builds clean (the binary target compiles main.rs).

- [ ] **Step 3: Clippy-check the touched production crates**

Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' '//src/services/runtime:runtime[clippy.txt]' 2>&1 | tail -20`
Then confirm both `clippy.txt` outputs are empty (no warnings). If non-empty, fix and rebuild.

- [ ] **Step 4: Commit**

```bash
git add src/services/query-api/src/main.rs
git commit -m "feat(query-api): serve dynamic ontology-derived OpenAPI at /openapi.json"
```

---

### Task 6: Whole-suite verification + register update

**Files:**
- Modify: `docs/ROADMAP.md`, `docs/FUTURE.md` (via `loom-docs-update` at finish)

- [ ] **Step 1: Run the affected test sweep**

Run:
```bash
buck2 test //src/services/query-api/... //src/services/runtime/... 2>&1 > /tmp/all.log
grep -E "Tests finished|FAIL" /tmp/all.log
```
Expected: `Tests finished: … 0 failed`. In particular the existing `//src/services/query-api:openapi` (`documents_exactly_the_expected_routes`) and `//src/services/runtime:openapi` targets stay green — the static base is untouched.

- [ ] **Step 2: Run prek (rustfmt + clippy + file hooks)**

Run: `buck2 run //tools:prek -- run --all-files 2>&1 | tail -30`
Expected: all hooks pass. Commit any hook-applied fixes.

- [ ] **Step 3: Close the register item**

Invoke `loom-docs-update`: flip `road-autogen-api-specs` to `- [x]` / `status:done` with `pr:#<n>` (filled after the PR opens), and record the follow-ups the spec names: `fut-openapi-per-subject-catalog`, `fut-ingest-ontology-openapi`, plus a new `fut-openapi-per-type-insert-route` (a real per-type insert route / concrete Insert-action naming, deferred per Design decision 3). Stage those edits.

- [ ] **Step 4: Final commit**

```bash
git add docs/ROADMAP.md docs/FUTURE.md
git commit -m "docs(registers): close road-autogen-api-specs; record openapi follow-ups"
```

---

## Self-Review

**1. Spec coverage:**
- Pure `ontology_openapi` generator (per-type schema, `GET /objects/{Type}`, per-link read, typed-insert POST) → Task 2. ✓
- Base/logical-type → OpenAPI codec (incl. `vector(N)` bounded array, nullability, identity-required) → Task 1 (codec) + Task 2 (identity-required in `type_component_schema`). ✓
- Dynamic `/openapi.json` + live `/docs`, `with_openapi` seam change (static path unchanged for ingest) → Tasks 3 + 4 + 5. ✓
- Freshness (per-request live generation) → Task 3 `live_openapi` + Task 5 wiring; liveness test in Task 3. ✓
- Exposure (public, full catalog, un-gated) → preserved (the routes mount outside `service_runtime::protect`; `/openapi.json` + `/docs` were already public and stay so). Test `full_catalog_is_generated` (Task 3). ✓
- Testing items 1–5 (codec unit, per-type ops, liveness, static intact, full catalog) → Tasks 1–3 cover all five (memory-backed instead of `loom_fixture_test`; see Design decision + Global Constraints — memory exercises `list_types`/`links`/`define_type` identically and keeps tests RE-eligible). ✓
- Out-of-scope items (ingest per-type ops, per-subject catalog, semantic descriptions, derived docs, caching) → not built; follow-ups recorded in Task 6. ✓

**2. Placeholder scan:** No TBD/TODO/"handle edge cases"/"similar to Task N". Each code step shows full code. utoipa builder-name caveats are explicit implementer notes with the JSON-contract tests as the gate, not placeholders. ✓

**3. Type consistency:** `base_type_to_schema(BaseType, bool) -> RefOr<Schema>` (Task 1) is used by `property_schema` (Task 1) used by `type_component_schema` (Task 2). `ontology_openapi(&[ObjectType], &[LinkDef]) -> (Paths, BTreeMap<String, RefOr<Schema>>)` (Task 2) is consumed by `live_openapi` (Task 3) which merges via `doc.paths.paths.extend` + `components.schemas.extend`. `live_openapi(Arc<dyn ControlPlane + Send + Sync>) -> OpenApi` (Task 3) is called by the provider in Task 5 and the seam `with_openapi_provider<F, Fut>` (Task 4). `register_bearer_scheme` (Task 4) applied by the seam. Names consistent across tasks. ✓
