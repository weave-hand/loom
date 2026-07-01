# Object-read pagination + `gov_list_types` — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose the internal cursor over `GET /objects/{type}` (`?limit=`/`?cursor=` + a `next` in the response) and add `GET /ontology/types` backed by a new `gov_list_types` engine-wire RPC.

**Architecture:** Part A (pagination) is a query-api change — add `ORDER BY <identity>` + a keyset `WHERE identity > cursor` predicate to the SQL compiler on a new paginated read path, reusing `Page::from_keyset`/`Cursor`. Part B (types) mirrors the existing `gov_links` RPC across the engine-wire proto, the gRPC client, the engine service handler, and the query-api `WireOntology` stub, then adds the HTTP endpoint. The object read executes as governed SQL on the DataFusion engine (no sqlx, no `.sqlx`).

**Tech Stack:** Rust 2024, axum, tonic/prost (protox codegen via buck genrule), DataFusion engine over Flight SQL, buck2.

## Global Constraints

- Tests are `rust_test` integration targets only — **never** inline `#[cfg(test)]` (the `no-inline-tests` prek hook fails the build). Reuse `//src/services/query-api:e2e-support` helpers (`tref`/`prop`/`land`/`setup`, `ids`/`ids_i64`).
- Strict clippy (pedantic + restriction) on all touched production code. Use `#[expect(lint, reason=…)]` locally if needed; no bare `#[allow]`.
- **Don't pipe `buck2 test` through `tail`** — redirect to a file and grep: `buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`.
- The object-read SQL is engine SQL built as strings (`sql.rs`), **not** sqlx `query!` — **no `.sqlx` regen**.
- The proto is regenerated automatically by the `//src/services/engine-wire:pb-gen` buck genrule on build — **no manual codegen command**.
- `MAX_PAGE = 200`; `limit` query param clamped to `[1, 200]`; absent → the server `default_limit`.
- **Fail closed:** never emit a cursor derived from an identity column the subject cannot read (masked/denied identity + pagination → 400).
- Cursor is opaque-by-contract: currently the last row's identity value as a string (URL-decoded by axum's query parser); clients must echo `next`, not construct it.

---

### Task 1: `ORDER BY` support in the SQL compiler

`compile_select_with` (`src/services/query-api/src/sql.rs:397-433`) emits `SELECT … [WHERE …] LIMIT n` with **no `ORDER BY`**. Add an optional order-by column so the paginated path can request deterministic ordering. The keyset `WHERE identity > cursor` predicate rides in the existing `predicates` argument (a `CallerPredicate` with `CompareOp::Gt`), so this task is only the `ORDER BY`.

**Files:**
- Modify: `src/services/query-api/src/sql.rs` (signature + emission; all call sites)
- Test: `src/services/query-api/tests/<existing sql test>.rs` (find the test that covers `compile_select_with`; if none exists, add `tests/sql_pagination.rs` as a `rust_test` mirroring an existing query-api unit test target in `BUCK`)

**Interfaces:**
- Produces: `compile_select_with(dialect, table, allowed_cols, mask_cols, row_filters, predicates, derived, order_by: Option<&str>, limit) -> Result<(String, Vec<SqlValue>)>`. When `order_by = Some(col)`, the SQL includes `ORDER BY <quoted col> ASC` immediately before the `LIMIT` clause. `None` preserves today's output exactly.

- [ ] **Step 1: Write the failing test**

Add to the chosen test file (imports mirror the existing sql test; use the same dialect the tests use):

```rust
#[test]
fn compile_select_emits_order_by_when_requested() {
    let dialect = /* the test dialect, as the existing sql tests construct it */;
    let table = TableRef { schema: "s".into(), name: "t".into() };
    let (sql, _params) = compile_select_with(
        &dialect, &table, &["id".into(), "name".into()], &[], &[], &[], &[],
        Some("id"), 10,
    ).unwrap();
    assert!(sql.contains("ORDER BY"), "expected ORDER BY, got: {sql}");
    // ORDER BY must precede LIMIT.
    assert!(sql.find("ORDER BY").unwrap() < sql.find("LIMIT").unwrap(), "got: {sql}");
    assert!(sql.contains("\"id\"") || sql.contains("id"), "orders by identity: {sql}");
}

#[test]
fn compile_select_no_order_by_by_default() {
    let dialect = /* same */;
    let table = TableRef { schema: "s".into(), name: "t".into() };
    let (sql, _params) = compile_select_with(
        &dialect, &table, &["id".into()], &[], &[], &[], &[], None, 10,
    ).unwrap();
    assert!(!sql.contains("ORDER BY"), "default read stays unordered: {sql}");
}
```

- [ ] **Step 2: Run it — verify it fails to build (signature mismatch)**

Run: `buck2 test //src/services/query-api:<sql-test-target> > /tmp/t1.log 2>&1; grep -E "error|FAIL|Tests finished" /tmp/t1.log`
Expected: build failure — `compile_select_with` takes 8 args, not 9.

- [ ] **Step 3: Implement**

In `sql.rs`, add the `order_by: Option<&str>` parameter (before `limit`). Emit the clause between the `WHERE` block and the `LIMIT`:

```rust
if let Some(col) = order_by {
    let _ = write!(sql, " ORDER BY {} ASC", dialect.quote_ident(col));
}
let _write = write!(sql, " {}", dialect.limit_clause(limit));
```

Update **every** call site of `compile_select_with` to pass `None` (grep for callers: `grep -rn "compile_select_with" src/services/query-api/src`). The object-read caller in `handler.rs` also passes `None` for now — Task 2 flips it to `Some(identity)` on the paginated path.

- [ ] **Step 4: Run tests — verify pass**

Run: `buck2 test //src/services/query-api:<sql-test-target> > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t1.log`
Expected: pass.

- [ ] **Step 5: Clippy + commit**

```bash
tools/clippy-all.sh 2>&1 | grep -iE "query-api|error" | tail   # clean
git add src/services/query-api/src/sql.rs src/services/query-api/tests/
git commit -m "feat(query-api): optional ORDER BY in compile_select_with"
```

---

### Task 2: Paginated object read + `?limit=`/`?cursor=` on `GET /objects/{type}`

Add a paginated read path that orders by identity, applies the keyset predicate, fetches `limit+1`, and returns `next` via `Page::from_keyset`; wire the query params + response field + the four 400 edge cases.

**Files:**
- Modify: `src/services/query-api/src/handler.rs` (new `read_object_page`, `QueryError::BadPagination`, cursor encode/decode)
- Modify: `src/services/query-api/src/render.rs` (`objects_to_json` emits `next`)
- Modify: `src/services/query-api/src/http.rs` (`get_object` parses `limit`/`cursor`, branches to the paged path)
- Modify: `src/services/query-api/src/openapi.rs` (`ObjectsResponse.next`, `#[utoipa::path]` params)
- Test: `src/services/query-api/tests/object_pagination_e2e.rs` (new `rust_test`, wired in `BUCK` mirroring `object_set_e2e`)

**Interfaces:**
- Consumes: `compile_select_with(..., order_by, limit)` (Task 1); `Page::from_keyset` (`control_plane_core::page`); `filter::{CallerPredicate, coerce_filter}`, `control_plane_core::CompareOp::Gt`; `identity_is_governed` (`handler.rs:150`).
- Produces:
  - `handler::read_object_page(q: &ObjectQuery, subject: &Subject, deps: &QueryDeps, limit: u32, after: Option<Cursor>) -> Result<(ObjectRows, Option<Cursor>), QueryError>` — orders by the type's identity, applies the keyset, returns the (truncated) rows + `next`.
  - `QueryError::BadPagination(String)` → HTTP 400.
  - `render::objects_to_json(rows, next: Option<&Cursor>) -> Value` → `{"objects":[…], "next": <string>|null}`.

- [ ] **Step 1: Write the failing e2e test**

Create `src/services/query-api/tests/object_pagination_e2e.rs` mirroring `tests/object_set_e2e.rs`'s `setup` (seed a type with `identity: Some("id")` and ≥5 rows). Use the `e2e_support::get(...)` router driver. (Copy the exact seed/driver idiom from `object_set_e2e.rs` — do not invent new helpers.)

```rust
// Pseudocode shape — fill in with object_set_e2e.rs's exact fixtures/driver.
#[tokio::test]
async fn paginates_with_cursor_covering_all_rows() {
    let fx = PgFixture::fresh().await;
    let ctx = setup(&fx).await;               // seeds ~5 orders with ids 1..=5, subject with Read grant
    // page 1
    let (status, body) = get(&ctx, "/objects/orders?limit=2").await;
    assert_eq!(status, 200);
    assert_eq!(ids_i64(&body).len(), 2);
    let next = body["next"].as_str().expect("next cursor present").to_string();
    // page 2
    let (_s, body2) = get(&ctx, &format!("/objects/orders?limit=2&cursor={next}")).await;
    let p2 = ids_i64(&body2);
    assert_eq!(p2.len(), 2);
    // no overlap between page 1 and page 2
    // ... assert disjoint + contiguous, and that draining all pages == the full unpaginated set,
    //     and the final page has next == null.
}

#[tokio::test]
async fn pagination_on_no_identity_type_is_400() { /* seed a Plain type (identity: None); ?limit=2 → 400 */ }

#[tokio::test]
async fn ids_and_pagination_are_mutually_exclusive_400() { /* ?_ids=1&limit=2 → 400 */ }

#[tokio::test]
async fn malformed_cursor_is_400() { /* an int-identity type, ?cursor=notanumber → 400 */ }
```

Wire the target in `src/services/query-api/BUCK` (copy the `object_set_e2e` `rust_test` block; add `":e2e-support"` to `deps`).

- [ ] **Step 2: Run it — verify it fails**

Run: `buck2 test //src/services/query-api:object_pagination_e2e > /tmp/t2.log 2>&1; grep -E "error|FAIL|Tests finished" /tmp/t2.log`
Expected: build failure (no `next` field / no `?limit` handling) or assertion failures.

- [ ] **Step 3: Implement the handler path**

In `handler.rs`:
- Add `#[error("bad pagination: {0}")] BadPagination(String)` to `QueryError`.
- Add cursor helpers:
  ```rust
  fn encode_id_cursor(v: &SqlValue) -> Cursor { Cursor(sqlvalue_to_cursor_string(v)) }
  // decode: coerce the cursor string to the identity's logical type via filter::coerce_filter.
  ```
  (For the cursor *string* form, reuse the same scalar rendering the renderer uses for an identity cell — an integer id → its digits, a string id → itself. Keep it a total, round-trippable mapping; a `SqlValue::Null` identity is impossible for a primary key.)
- Add `read_object_page`:
  1. Resolve type + policy exactly as `compile_object_read` does (reuse it or factor a shared prefix). Get `object_type`, `denied`, `masked`.
  2. Identity guard: `let id = object_type.identity.as_deref().ok_or_else(|| QueryError::BadPagination("type has no declared identity".into()))?;` then if `identity_is_governed(&object_type, &denied, &masked)` → `Err(QueryError::BadPagination("identity column not readable".into()))`.
  3. If `!q.ids.is_empty()` → `Err(QueryError::BadPagination("_ids and pagination are mutually exclusive".into()))`.
  4. Build predicates = the type's governed filters **plus**, when `after` is `Some`, a keyset `CallerPredicate { column: id, op: CompareOp::Gt, values: vec![coerce_filter(id, id_ty, &cursor_str)?] }` (a coerce failure → map to `BadPagination("invalid cursor")`).
  5. Compile with `order_by = Some(id)` and `limit = limit + 1`.
  6. `fetch_rows`, then `Page::from_keyset(rows, Some(limit), |row| encode_id_cursor(id-cell-of(row)))` — but `from_keyset` operates on the row Vec; apply it over `ObjectRows.rows`, deriving the cursor from the identity column's index in `columns`. Truncate `ObjectRows.rows` to `limit`, return `(rows, next)`.

In `render.rs`: change `objects_to_json` to take `next: Option<&Cursor>` and add `"next": next.map(|c| c.0.clone())` (JSON `null` when `None`) to the emitted object. Update `associations_to_json`/other callers only if they call `objects_to_json` (they don't).

- [ ] **Step 4: Implement the HTTP layer**

In `http.rs::get_object`, while pulling `_ids` out of `params`, also pull reserved keys `limit` and `cursor`:
- `limit`: parse to `u32`, clamp to `[1, 200]`; a non-numeric value → 400. Presence of `limit` **or** `cursor` ⇒ paginated.
- Paginated branch → `read_object_page(&q, &subject, &deps, effective_limit, cursor.map(Cursor))`, render with `objects_to_json(&rows, next.as_ref())`.
- Non-paginated branch → unchanged `read_object` + `objects_to_json(&rows, None)`.
- Add `Err(QueryError::BadPagination(m)) => (StatusCode::BAD_REQUEST, m).into_response()` to the match.

In `openapi.rs`: add `pub next: Option<String>` to `ObjectsResponse`; add `("limit" = Option<u32>, Query, …)` and `("cursor" = Option<String>, Query, …)` to `get_object`'s `#[utoipa::path]` params, and note the 400s.

- [ ] **Step 5: Run the e2e — verify pass**

Run: `buck2 test //src/services/query-api:object_pagination_e2e > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t2.log`
Expected: all pass. Also re-run `//src/services/query-api:object_set_e2e` to confirm the `objects_to_json` signature change didn't regress the non-paged shape.

- [ ] **Step 6: Clippy + commit**

```bash
tools/clippy-all.sh 2>&1 | grep -iE "query-api|error" | tail
git add src/services/query-api/src/handler.rs src/services/query-api/src/render.rs src/services/query-api/src/http.rs src/services/query-api/src/openapi.rs src/services/query-api/tests/object_pagination_e2e.rs src/services/query-api/BUCK
git commit -m "feat(query-api): cursor pagination on GET /objects/{type}"
```

---

### Task 3: `gov_list_types` engine-wire RPC

Mechanical mirror of `gov_links` across proto → client → engine handler → `WireOntology`. `ObjectType` and `Page<T>` already derive serde, so the wire is JSON-envelope only.

**Files:**
- Modify: `src/services/engine-wire/proto/engine_control.proto`
- Modify: `src/services/engine-wire/src/client.rs`
- Modify: `src/services/engine/src/service.rs`
- Modify: `src/services/query-api/src/wire_control_plane.rs`
- Test: `src/services/query-api/tests/wire_governance_e2e.rs` (extend)

**Interfaces:**
- Produces: `GrpcQueueClient::gov_list_types(&self, page: &PageReq) -> Result<Page<ObjectType>>`; `WireOntology::list_types` now delegates to it (no longer `read_only`).

- [ ] **Step 1: Extend the proto**

In `engine_control.proto`, add to the `EngineControl` service (next to `Links`):
```proto
rpc ListTypes (ListTypesRequest) returns (ListTypesResponse);
```
and in the governance-read messages block:
```proto
message ListTypesRequest  { string page_json = 1; }   // PageReq
message ListTypesResponse { string page_json = 1; }   // Page<ObjectType>
```

- [ ] **Step 2: Client method**

In `client.rs`, mirror `gov_links` (`:275-298`):
```rust
/// Governance: list all defined object types.
pub async fn gov_list_types(&self, page: &PageReq) -> Result<Page<ObjectType>> {
    let resp = self.inner.clone()
        .list_types(pb::ListTypesRequest { page_json: se(page)? })
        .await.map_err(cp_status)?.into_inner();
    de(&resp.page_json)
}
```
Add `ObjectType` to the `use control_plane_core::{…}` if not already imported.

- [ ] **Step 3: Server handler**

In `engine/src/service.rs`, mirror the `links` handler (`:349-365`):
```rust
async fn list_types(
    &self,
    req: Request<pb::ListTypesRequest>,
) -> std::result::Result<Response<pb::ListTypesResponse>, Status> {
    let page: control_plane_core::PageReq = de_arg(&req.into_inner().page_json, "page")?;
    let types = self.cp.ontology().list_types(page).await.map_err(status)?;
    Ok(Response::new(pb::ListTypesResponse { page_json: se_out(&types)? }))
}
```

- [ ] **Step 4: Un-stub `WireOntology::list_types`**

In `wire_control_plane.rs:135-137`, replace:
```rust
async fn list_types(&self, page: PageReq) -> Result<Page<ObjectType>> {
    self.client.gov_list_types(&page).await
}
```

- [ ] **Step 5: Build (regenerates the proto) + extend the wire parity test**

Run: `buck2 build //src/services/engine-wire //src/services/engine //src/services/query-api > /tmp/t3b.log 2>&1; tail -3 /tmp/t3b.log`
Expected: BUILD SUCCEEDED (the `pb-gen` genrule regenerates `pb` with the new RPC).

In `tests/wire_governance_e2e.rs`, extend `rpc_roundtrips_match_direct_reads`: assert `client.gov_list_types(&PageReq::unbounded())` equals the direct `cp.ontology().list_types(PageReq::unbounded())` (compare the type-name sets). If `wire_acl_is_read_only` asserted `list_types` was rejected, update it (per the exploration it only exercises `define_type`, so likely no change — verify).

Run: `buck2 test //src/services/query-api:wire_governance_e2e > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t3.log`
Expected: pass.

- [ ] **Step 6: Clippy + commit**

```bash
tools/clippy-all.sh 2>&1 | grep -iE "engine|query-api|error" | tail
git add src/services/engine-wire src/services/engine/src/service.rs src/services/query-api/src/wire_control_plane.rs src/services/query-api/tests/wire_governance_e2e.rs
git commit -m "feat(engine-wire): gov_list_types RPC + WireOntology::list_types"
```

---

### Task 4: `GET /ontology/types` endpoint

**Files:**
- Modify: `src/services/query-api/src/http.rs` (handler + route)
- Modify: `src/services/query-api/src/openapi.rs` (`OntologyTypesResponse` DTO)
- Test: `src/services/query-api/tests/ontology_types_e2e.rs` (new `rust_test`)

**Interfaces:**
- Consumes: `st.cp.ontology().list_types(PageReq::unbounded())` (now wired, Task 3).
- Produces: `GET /ontology/types` → `{"types":["Order","Customer",…]}` (auth-required; not per-type ACL-gated — ontology metadata, matching `/openapi.json`).

- [ ] **Step 1: Write the failing e2e**

Create `tests/ontology_types_e2e.rs` (mirror an existing simple query-api e2e that seeds a couple of types via the direct control plane + drives the router):
```rust
#[tokio::test]
async fn lists_ontology_type_names() {
    let fx = PgFixture::fresh().await;
    let ctx = setup(&fx).await;   // define_type for e.g. "customer" and "orders"
    let (status, body) = get(&ctx, "/ontology/types").await;
    assert_eq!(status, 200);
    let mut names: Vec<String> = body["types"].as_array().unwrap()
        .iter().map(|v| v.as_str().unwrap().to_string()).collect();
    names.sort();
    assert_eq!(names, vec!["customer".to_string(), "orders".to_string()]);
}
```
Wire the target in `BUCK`.

- [ ] **Step 2: Run — verify fail (404 route / no handler)**

Run: `buck2 test //src/services/query-api:ontology_types_e2e > /tmp/t4.log 2>&1; grep -E "error|FAIL|Tests finished" /tmp/t4.log`
Expected: fail.

- [ ] **Step 3: Implement**

In `http.rs`, add the route to `router` (`:69-80`): `.route("/ontology/types", get(list_ontology_types))`. Add the handler:
```rust
#[utoipa::path(get, path = "/ontology/types",
    responses((status = 200, description = "Object-type names", body = OntologyTypesResponse)),
    security(("bearer_auth" = [])), tag = "ontology")]
async fn list_ontology_types(State(st): State<AppState>, _subject: Subject) -> impl IntoResponse {
    match st.cp.ontology().list_types(PageReq::unbounded()).await {
        Ok(page) => {
            let types: Vec<String> = page.items.into_iter().map(|t| t.name.0).collect();
            Json(serde_json::json!({ "types": types })).into_response()
        }
        Err(e) => internal_error("ontology list_types fault", e),
    }
}
```
(Keep the `Subject` extractor so the route requires auth even though it isn't per-type gated.) In `openapi.rs` add `pub struct OntologyTypesResponse { pub types: Vec<String> }` deriving `ToSchema`, and register the path/DTO in `build_openapi()` alongside the others.

- [ ] **Step 4: Run — verify pass**

Run: `buck2 test //src/services/query-api:ontology_types_e2e > /tmp/t4.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t4.log`
Expected: pass.

- [ ] **Step 5: Clippy + commit**

```bash
tools/clippy-all.sh 2>&1 | grep -iE "query-api|error" | tail
git add src/services/query-api/src/http.rs src/services/query-api/src/openapi.rs src/services/query-api/tests/ontology_types_e2e.rs src/services/query-api/BUCK
git commit -m "feat(query-api): GET /ontology/types via gov_list_types"
```

---

### Task 5: Full sweep + register

**Files:**
- Modify: `docs/ROADMAP.md`

- [ ] **Step 1: Full green sweep**

```bash
buck2 test //src/... > /tmp/tf.log 2>&1; grep -E "Tests finished|FAIL" /tmp/tf.log
tools/clippy-all.sh 2>&1 | tail
buck2 run //tools:rustfmt -- $(git ls-files 'src/services/query-api/*.rs' 'src/services/engine/*.rs' 'src/services/engine-wire/*.rs')
```
Expected: full sweep green (the proto change rebuilds the wire graph — this is where a cross-service break shows up), clippy clean, rustfmt no-op (or apply + amend).

- [ ] **Step 2: Register**

Add to `docs/ROADMAP.md` (new or existing `## query` section) via `loom-docs-update` (preferred) or by hand mirroring an existing item:
`road-object-read-pagination` (area `query`, status `done`, `spec:2026-07-01-object-read-pagination-ontology-types-design`). Record any deferrals as FUTURE items (e.g. `/ontology/types` pagination; descending / non-identity sort). Run `bash tools/docs.sh validate` → OK.

- [ ] **Step 3: Commit**

```bash
git add docs/ROADMAP.md docs/FUTURE.md
git commit -m "docs(query): register object-read pagination slice"
```

---

## Self-review notes (for the executor)

- **Spec coverage:** Task 1 = ORDER BY; Task 2 = pagination path + params + `next` + the four 400s; Tasks 3-4 = `gov_list_types` wire RPC + `/ontology/types`; Task 5 = sweep + register. Every spec section maps to a task.
- **Type consistency:** `read_object_page` returns `(ObjectRows, Option<Cursor>)`; `objects_to_json(rows, next)` takes `Option<&Cursor>`; `ObjectsResponse.next: Option<String>`; `QueryError::BadPagination(String)` → 400. `gov_list_types(&PageReq) -> Page<ObjectType>` matches the `gov_links` shape.
- **Fail-closed:** masked/denied identity + pagination → `BadPagination` 400 (Task 2 step 3.2), never emitting a cursor over an unreadable column — the spec's security requirement.
- **No sqlx / no manual codegen:** engine SQL is strings (Task 1/2); the proto regenerates via the buck genrule (Task 3). Neither needs a manual regen step.
- **Cross-service break surfaces in Task 5's full sweep**, not the per-crate builds — do not skip it.
