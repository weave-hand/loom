# Admin HTTP surface gaps — design

**Item:** `fut-admin-governance-http-surface` · **Closes:** GitHub issue #363 · **Relates:** #361

## Problem

Validating "everything over HTTP" against a chart deploy (issue #363, external
report) found six control-plane capabilities that are implemented, enforced,
and tested — but administrable only via raw SQL:

1. fine-grained ACL policies (row filter / column deny / column mask),
2. table-target grants (`PolicyTarget::Table`),
3. derived properties (`DerivedPropertyDef`),
4. vector index definitions,
5. per-value `PropertyConstraints`,
6. model deletion.

Recon confirms the backends are complete for 1–5: trait methods, both
adapters (memory + postgres), write-time validation, and storage migrations
all exist. The gap is purely the admin HTTP layer
(`src/services/runtime/src/admin.rs`): request types don't carry the fields,
and no routes call the trait methods.

## Scope

Expose capabilities **1–5** over the existing admin-gated HTTP surface, with
OpenAPI coverage and tests. One new trait method (`Acl::list_policies`) for
read-back parity with grants.

**Non-goals (explicit):**
- **Model deletion (gap 6)** stays deferred — `fut-ontology-type-delete`
  records the operator decision (2026-07-03): unsafe without a referrer-scan
  guard (dangling grants/policies, non-cascading inbound links). The closing
  PR says so on issue #363 (the reporter offered a split).
- **Vector index drop** stays deferred (`fut-vector-index-drop`).
- **#361's absent-type grant deadlock** is *not* resolved here: the ingest
  model gate checks the exact `PolicyTarget::Type` key
  (`src/services/ingest/src/http.rs:258-262`) and `Acl::check` matches
  targets exactly, so a Table grant cannot satisfy it. Exposing table grants
  *does* unblock #361's documented workaround (pre-authorize the landing
  table, land as dataset, bind to model) — comment on #361, leave it open.
- No structured error bodies — new routes keep the existing bare-status
  `status_for` idiom (`fut-service-runtime-error-idiom` tracks the upgrade).

## Design

All routes live in `src/services/runtime/src/admin.rs` behind the existing
`require_admin` layer, annotated with `#[utoipa::path]`, and registered in
`AdminApiDoc` (paths + schemas) so `admin_openapi()` → query-api's
`build_openapi()` picks them up automatically.

### 1. Table-target grants — widen `GrantReq`

```rust
#[derive(serde::Deserialize, utoipa::ToSchema)]
struct GrantReq {
    action: String,                 // "read" | "write" (unchanged)
    #[serde(default)]
    r#type: Option<String>,
    #[serde(default)]
    table: Option<TableReq>,        // reuse the existing TableReq {schema, name}
}
```

Exactly one of `type`/`table` must be set; otherwise 400 (message:
`exactly one of type or table`). Maps to `PolicyTarget::Type` /
`PolicyTarget::Table`. Applies to **both** `POST` and `DELETE`
`/admin/roles/{role}/grants` (they share `GrantReq`). Type-target behavior is
unchanged (unknown type still 400 via `check_grant_target`); Table targets
pass validation by design (deferred existence check, `acl.rs:292-302`).
`GET /admin/roles/{role}/grants` already renders `target` via domain serde
(`{"Table":{"schema":..,"name":..}}`) — no change.

### 2. Fine-grained policy routes (+ `Acl::list_policies`)

New request type (row filter rides as the domain `RowFilter` serde encoding,
the same one stored in `acl.policy.row_filter` jsonb — e.g.
`{"Compare":{"property":"region","op":"Eq","value":{"Text":"emea"}}}`,
`{"And":[..]}`; documented in OpenAPI as a free-form object like the
link/action bodies):

```rust
#[derive(serde::Deserialize, utoipa::ToSchema)]
struct PolicyReq {
    action: String,                     // "read" | "write"
    #[serde(default)]
    r#type: Option<String>,             // exactly one of type/table, as GrantReq
    #[serde(default)]
    table: Option<TableReq>,
    #[serde(default)]
    row_filter: Option<serde_json::Value>, // -> RowFilter via from_value, 400 on decode error
    #[serde(default)]
    deny_columns: Vec<String>,
    #[serde(default)]
    mask_columns: Vec<String>,
}
```

- `POST /admin/roles/{role}/policies` → `Acl::set_policy` (upsert by
  `(role, action, target)`). Adapter validation (`check_policy_write`:
  unknown Type target, unknown row-filter property, caller-predicate-only
  ops like `Contains`) surfaces as 400; unknown role 404.
- `DELETE /admin/roles/{role}/policies` → `Acl::clear_policy`; body is
  `{action, type|table}` (a `PolicyReq` with filter fields ignored — reuse
  the type, document that only action+target are read). Idempotent 200.
- `GET /admin/roles/{role}/policies` → **new trait method**:

```rust
/// All policies of `role`, ordered by `(action, target-key)`. Role must
/// exist, else `NotFound`. The `page` request is accepted but not yet
/// enforced; results are a single full page. (Mirrors `list_grants`.)
async fn list_policies(&self, role: &RoleId, page: PageReq) -> Result<Page<RolePolicy>>;
```

with `RolePolicy { pub action: Action, pub policy: Policy }` in core
(`acl.rs`, next to `Grant`). Memory: filter the policy map by role, sort by
`(action, target key)`. Postgres: `SELECT` from `acl.policy` by role with
the same ordering (compile-time `query!`, `.sqlx` refreshed via
`tools/sqlx-prepare.sh`). Response mirrors `GrantView`: each entry
`{action, target: <domain serde>, row_filter, deny_columns, mask_columns}`.
Contract legs in `testkit` (`acl` contract) cover define→list→clear on both
adapters.

### 3. Derived properties in `DefineModelReq`

```rust
#[derive(serde::Deserialize, utoipa::ToSchema)]
struct DerivedReq {
    name: String,
    ty: String,          // result type, e.g. "Int"
    link: String,        // link the aggregation traverses
    agg: AggReq,
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
struct AggReq {
    kind: String,               // "count" | "sum" | "avg" | "min" | "max"
    #[serde(default)]
    column: Option<String>,     // required for all kinds except count
}
```

`DefineModelReq` gains `#[serde(default)] derived: Vec<DerivedReq>` and the
handler maps into the existing `ObjectType.derived`
(`DerivedPropertyDef {name, ty, link, agg}`). Mapping validation → 400:
unknown `kind`; `count` with a `column`; non-count without one. Link
existence is deliberately **not** validated — matches `define_type` today
(the dangling-link degradation is tracked as
`iss-delete-link-derived-dangle`; this route adds no new validation gap).

### 4. Property constraints in `PropReq`

`PropReq` gains `#[serde(default)] constraints: Option<ConstraintsReq>`:

```rust
#[derive(serde::Deserialize, utoipa::ToSchema)]
struct ConstraintsReq {
    #[serde(default)] range: Option<RangeReq>,     // {min: Option<f64>, max: Option<f64>}
    #[serde(default)] length: Option<LengthReq>,   // {min: Option<u32>, max: Option<u32>}
    #[serde(default)] pattern: Option<String>,
    #[serde(default)] one_of: Option<Vec<String>>,
}
```

mapped 1:1 onto `PropertyConstraints` (`constraints.rs:39-51`; absent →
`default()` as today). The existing define-gate (`validate_constraints`,
called inside `define_type` on both adapters — range on non-numeric, string
constraints on non-string, invalid regex) surfaces as 400. Enforcement
(land gate + action gate 422s) already exists and is untouched.

### 5. Vector index routes

- `POST /admin/models/{type}/vector-indexes` → `Ontology::define_vector_index`
  (upsert by `(type, name)`):

```rust
#[derive(serde::Deserialize, utoipa::ToSchema)]
struct VectorIndexReq {
    name: String,
    property: String,
    #[serde(default)]
    metric: Option<String>,          // "cosine" (default) | "l2"
    #[serde(default)]
    spec: Option<VectorIndexSpecReq>, // default flat
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
struct VectorIndexSpecReq {
    kind: String,                    // "flat" | "ivf_flat" | "hnsw"
    #[serde(default)] nlist: Option<u32>,            // ivf_flat only
    #[serde(default)] m: Option<u32>,                // hnsw only
    #[serde(default)] ef_construction: Option<u32>,  // hnsw only
}
```

  Mapping validation → 400: unknown `metric`/`kind`; a tuning field set for
  the wrong kind. Adapter validation (type/property checks per
  `define_vector_index`'s existing rules) surfaces 400/404 via `status_for`.
- `GET /admin/models/{type}/vector-indexes` → `vector_indexes_for`, rendered
  `{indexes: [{name, property, metric, spec}]}` with the same lowercase
  wire vocabulary as the request. No drop route (deferred,
  `fut-vector-index-drop`).

## Testing

- **Route tests** (`src/services/runtime/tests/admin_management.rs`, memory
  adapter, existing idioms): per surface — happy path + list read-back,
  each 400 branch (both/neither target, bad row_filter, bad agg kind,
  count+column, bad metric/kind, constraint on wrong type), 404s (unknown
  role/type), idempotent deletes/upserts.
- **Contract** (`testkit`): `list_policies` legs (set → list ordered →
  clear → empty; unknown role NotFound) run against memory and postgres.
- **Postgres**: `.sqlx` cache refreshed; fixture suite covers the new query
  via the contract.
- **Governed e2e** (`src/services/query-api` e2e, reusing `e2e-support`):
  the money test — author a row-filter + mask-column policy **over the admin
  HTTP routes** (not SQL), then `GET /objects/{type}` and assert the row
  filter and `***` mask are enforced; author a table-target grant over HTTP
  and assert `check` allows the table read it previously denied.
- OpenAPI: assert the new paths appear in `build_openapi()` (existing
  openapi test file pattern).

## Landing

Single PR from `work/fut-admin-governance-http-surface`: removes the
`fut-admin-governance-http-surface` FUTURE entry (links/actions halves
already landed via `#road-api-management-crud`), documents the surface in
`docs/system-capabilities/` (acl + ontology pages), `Closes #363`, comments
on #361. Register items untouched: `fut-ontology-type-delete`,
`fut-vector-index-drop`, `fut-fgac-subject-attribute`.
