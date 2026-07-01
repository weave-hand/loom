# Object-read pagination + `gov_list_types` (design)

**Status:** approved for planning
**Date:** 2026-07-01
**Area:** query (query-api read surface) + a supporting engine-wire RPC
**Consumers:** the object-explorer UI (slice 1b, `2026-07-01-object-explorer-ui-design`, not yet written) — this slice is its backend prerequisite.

## Goal

Expose over HTTP the cursor pagination that already exists internally, and add a
governed type-listing endpoint, so the object-explorer UI has (a) a paginated object
table and (b) a clean source for the type sidebar:

- **`GET /objects/{type}`** gains `?limit=` + `?cursor=` and returns a `next` cursor.
- **`GET /ontology/types`** returns the ontology's object-type names, backed by a new
  `gov_list_types` engine-wire RPC (the query-api wire client currently hard-rejects
  `list_types`).

## Non-goals (this slice)

- The UI itself (slice 1b).
- Filtering/sorting by arbitrary (non-identity) columns, descending order, or total
  counts — pagination here is forward-only, keyset-ordered by the type's identity.
- Any change to the control-plane `Ontology`/`Catalog` traits (`list_types` already
  exists and is implemented by the postgres/memory adapters).
- Physical dataset-catalog listing (name/owner/health) — a separate, larger arc.

## Background (verified anchors)

- `GET /objects/{type}` → `http::get_object` (`src/services/query-api/src/http.rs:130`)
  → `handler::read_object` → `handler::compile_object_read`
  (`handler.rs:219-393`) → `sql::compile_select_with` (`sql.rs:397-433`). The compiled
  SQL is **`SELECT <cols> FROM <table> [WHERE …] LIMIT <default_limit>` with no
  `ORDER BY`** — unordered, executed as governed SQL on the DataFusion **engine** over
  Flight SQL (not Postgres/sqlx, so **no `.sqlx` cache is involved**).
- Response is `render::objects_to_json(&rows)` → `{"objects":[…]}`
  (`render.rs:15-33`), doc DTO `ObjectsResponse` (`openapi.rs:8-15`).
- Identity: `ObjectType.identity: Option<String>` names 0-or-1 property
  (`control-plane/core/src/ontology.rs:47-48`); no-identity types are legal (drive
  `QueryError::NoIdentity` elsewhere).
- Pagination convention to reuse: `PageReq{after:Option<Cursor>, limit:Option<u32>}`,
  `Page<T>{items, next}`, `Page::from_keyset(items, limit, cursor_fn)` — the
  "fetch `limit+1`, `ORDER BY key`, keyset `WHERE key > ?`" pattern
  (`control-plane/core/src/page.rs:14-93`, exemplar `postgres/src/lineage.rs:150-207`).
  `Cursor(String)` is the opaque wire type with int/string encode helpers
  (`lineage.rs:62-80`).
- Wire RPC exemplar: `gov_links` — proto JSON-envelope
  (`services/engine-wire/proto/engine_control.proto`), client
  `GrpcQueueClient::gov_links` (`engine-wire/src/client.rs:257-298`), server handler
  (`services/engine/src/service.rs:349-365`), `WireOntology::links`
  (`query-api/src/wire_control_plane.rs:101-137`; `list_types` is the
  `read_only("list_types")` stub at `:135-137`). `ObjectType` + `Page<T>` already
  derive `Serialize`/`Deserialize` — no new wire encoding.

## Part A — cursor pagination on `GET /objects/{type}`

### Wire contract

- Request: `GET /objects/{type}?limit=<u32>&cursor=<opaque>` (both optional; existing
  `_ids` and filter params unchanged).
- Response: `{"objects":[…], "next": "<cursor>" | null}`. `next` is present (non-null)
  iff a further page exists.
- `limit`: clamped to `[1, MAX_PAGE=200]`; absent → server `default_limit`.
- `cursor`: opaque; the client echoes back the `next` from the previous page. It
  encodes the identity value of the last row of that page.

### Ordering + keyset (the real change)

`compile_select_with` gains, on the paginated path, a deterministic
`ORDER BY <identity> ASC` and, when a cursor is supplied, a keyset predicate
`WHERE <identity> > <cursor-value>`; `LIMIT` becomes `limit + 1` (the probe row).
The handler:

1. Reads the type's `identity` (already has the `ObjectType`).
2. Decodes the `cursor` string into a `SqlValue` typed by the identity property's
   logical type (integer identities compare numerically; string/uuid lexically) and
   adds it as a keyset predicate param (reusing the existing `CallerPredicate`/
   `SqlValue` bind path).
3. Requests `limit + 1` rows, then `Page::from_keyset(rows, Some(limit), |last| encode
   identity-of-last)` to derive `next` and truncate the probe row.

`ORDER BY <identity>` is applied **only on the paginated path** (when `limit` or
`cursor` is present), so the default un-paginated read keeps today's behavior and cost.

### Edge cases (explicit contract)

- **No-identity type** + pagination params → **400** (`"pagination requires a type with
  a declared identity"`), mirroring the existing `NoIdentity` 400. Without a stable key,
  keyset pagination is impossible.
- **Masked identity** (the subject's ACL masks the identity column) + pagination →
  **400** (`"identity column is not readable for pagination"`). Keyset ordering/cursor
  would leak the masked identity value through `next`; fail closed rather than leak.
- **`_ids` + pagination params together** → **400** (`"_ids and pagination are mutually
  exclusive"`): an id-set fetch is already bounded and unordered by construction.
- **Malformed cursor** (won't decode to the identity type) → **400**
  (`"invalid cursor"`).
- **Empty result / last page** → `{"objects":[…], "next": null}`.

### Files (Part A)

- `src/services/query-api/src/sql.rs` — `compile_select_with` gains `ORDER BY` +
  keyset predicate on the paginated path (new params: identity column + optional
  cursor bind + effective limit).
- `src/services/query-api/src/handler.rs` — `compile_object_read`/`read_object` thread
  `limit`/`after`, enforce the edge cases, build `Page::from_keyset`, surface `next`.
  `ObjectRows` (or the read result) carries the optional `next` cursor.
- `src/services/query-api/src/http.rs` — `get_object` parses `limit`/`cursor` out of
  the query params (reserved keys, like `_ids`); emits `next`.
- `src/services/query-api/src/render.rs` — `objects_to_json` emits `next`
  (add an `Option<&Cursor>`/`Option<String>` param, or wrap in `http.rs`).
- `src/services/query-api/src/openapi.rs` — `ObjectsResponse` gains
  `next: Option<String>`; `get_object`'s `#[utoipa::path]` documents `limit`/`cursor`.

## Part B — `gov_list_types` RPC → `GET /ontology/types`

A near-mechanical mirror of `gov_links` (same `page_json` request shape, `Page<T>`
response). Governance stance matches `/openapi.json`: the type catalog is **ontology
metadata**, so the endpoint is **auth-required but not per-type ACL-gated** (ACL governs
the object *data*, read through `/objects/{type}`).

### Wire contract

- `GET /ontology/types` → `{"types":["Order","Customer","LineItem", …]}` — the list of
  object-type names (`ObjectType.name`), order unspecified. Names only keep the payload
  small and the sidebar simple; richer per-type detail stays with `/openapi.json` (and a
  later expansion if 1b needs it).
- Backed by `Ontology::list_types(PageReq::unbounded())` through the wire; the endpoint
  itself is un-paginated for now (the ontology is small; `list_types` accepts the
  `PageReq` but returns a single full page today, per its trait doc).

### Files (Part B)

- `src/services/engine-wire/proto/engine_control.proto` — add
  `rpc ListTypes (ListTypesRequest) returns (ListTypesResponse);` +
  `message ListTypesRequest { string page_json = 1; }` /
  `message ListTypesResponse { string page_json = 1; }`. Buck's `:pb-gen` genrule
  regenerates `pb` on build — **no manual codegen step**.
- `src/services/engine-wire/src/client.rs` — `GrpcQueueClient::gov_list_types(&self,
  page: &PageReq) -> Result<Page<ObjectType>>`, mirroring `gov_links`.
- `src/services/engine/src/service.rs` — `list_types` handler on
  `impl EngineControl for EngineControlService`: `de_arg` the page →
  `self.cp.ontology().list_types(page)` → `se_out` into `page_json`.
- `src/services/query-api/src/wire_control_plane.rs` — replace the
  `read_only("list_types")` stub with `self.client.gov_list_types(&page).await`.
- `src/services/query-api/src/http.rs` — `GET /ontology/types` handler:
  `st.cp.ontology().list_types(PageReq::unbounded())` → map to
  `{"types":[names…]}`; add the route to the router (`http.rs:69-80`) and a
  `#[utoipa::path]` annotation.
- `src/services/query-api/src/openapi.rs` (or wherever DTOs live) — an
  `OntologyTypesResponse { types: Vec<String> }` doc DTO.

## Error handling

All new failure modes are `400` with a specific message (listed under Part A edge
cases) except transport/engine errors, which reuse the existing `ControlPlaneError` →
HTTP mapping. `gov_list_types` reuses `cp_status`/`status` (`NotFound`→404,
`Conflict`→409, else 500), same as `gov_links`.

## Testing

Tests are `rust_test` integration targets only (no inline `#[test]`). Reuse the
`//src/services/query-api:e2e-support` helpers (`tref`/`prop`/`land`/`setup`,
`ids`/`ids_i64`).

- **Part A** — new e2e mirroring `tests/object_set_e2e.rs`:
  - Seed a type with `identity: Some("id")` and enough rows to exceed a small `limit`.
    `GET …?limit=2` → 2 objects + non-null `next`; `GET …?limit=2&cursor=<next>` → the
    next 2, disjoint from page 1 and contiguous (no gap/overlap); final page → `next:
    null`. Assert full coverage equals the unpaginated set.
  - No-identity type + `?limit=` → 400.
  - `_ids` + `?limit=` → 400. Malformed `cursor` → 400.
- **Part B** —
  - Extend `tests/wire_governance_e2e.rs`: `client.gov_list_types(...)` parity with the
    direct `cp.ontology().list_types(...)`; confirm `WireOntology::list_types` is no
    longer the `read_only` stub (update `wire_acl_is_read_only` if it asserted that).
  - e2e hitting `GET /ontology/types` → the seeded type names.
- Full `buck2 test //src/...` must stay green (the proto change rebuilds the wire).

## Global constraints

- No inline `#[test]`; tests are sibling `rust_test` targets (`no-inline-tests` hook).
- Strict clippy (pedantic + restriction) on all touched production code.
- The object-read SQL is engine SQL built as strings — **not** sqlx compile-time macros,
  so **no `.sqlx` regen**. (If any touched code did use `query!`, `tools/sqlx-prepare.sh`
  + commit the cache would be required — it does not.)
- `MAX_PAGE = 200`; `limit` clamped to `[1, 200]`; absent → `default_limit`.
- Fail-closed on masked identity (400) — never emit a cursor derived from a column the
  subject cannot read.

## Register updates

- `docs/ROADMAP.md`: add `road-object-read-pagination` (area `query`, status `planned`,
  this spec). On completion, `loom-docs-update` marks it done and records the deferred
  `GET /ontology/types` pagination + non-identity sort as FUTURE items if still open.
