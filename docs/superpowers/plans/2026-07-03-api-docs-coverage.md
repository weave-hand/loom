# API Docs Coverage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the served OpenAPI documents truthful and complete: real `POST /actions/{name}` ops generated from the ontology's actions (phantom `POST /objects/{Type}` deleted), per-type tag grouping, and the runtime's `/auth/*` + `/admin/*` routes documented via mergeable fragments.

**Architecture:** (1) `Ontology::list_actions` lands across all three implementors (memory, postgres, engine-wire proxy) plus the `ListActions` RPC, mirroring `list_types` exactly. (2) `openapi_gen::ontology_openapi` gains an `actions` input, emits param-schema'd `/actions/{name}` ops tagged by target type, and drops `insert_op`. (3) `service_runtime` exports `auth_openapi()` / `service_account_openapi()` / `admin_openapi()` fragments built from utoipa annotations; query-api merges all three, ingest merges two.

**Tech Stack:** Rust, utoipa 5 (already a dep everywhere needed — **no new third-party crates, no buckify**), tonic/prost for the RPC, sqlx compile-time queries.

**Spec:** `docs/superpowers/specs/2026-07-03-api-docs-coverage-design.md`

## Global Constraints

- **No runtime behavior changes.** Documented statuses/bodies follow the handlers as they are; if a handler disagrees with the spec's status table, the doc follows the handler.
- **No new dependencies.** utoipa is already in `runtime`, `query-api`, `ingest` Cargo.toml + BUCK. If you think you need a new crate, stop and re-read the task.
- Tests are sibling `tests/*.rs` files wired as `rust_test` targets (via the repo's `load("//src:loom_test.bzl", "rust_test")`) — NEVER inline `#[cfg(test)]`.
- Postgres-touching tests use `loom_fixture_test` targets; pure-logic tests plain `rust_test`.
- Run tests with `--unstable-allow-all-tests-on-re`; never pipe `buck2 test` through `tail`/`head` — redirect to a file and grep.
- Build with `buck2 build -M none <targets>`; never a bare whole-tree build (disk cap).
- After changing any postgres SQL: `bash tools/sqlx-prepare.sh`, commit the `.sqlx` diff.
- Before EVERY commit: `buck2 run -v0 //tools:prek -- run --all-files > /tmp/prek.log 2>&1; grep -c Failed /tmp/prek.log` must print `0` (rustfmt hook is check-only — fix formatting manually, re-run).
- Every commit ends with:
  `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>` and
  `Claude-Session: https://claude.ai/code/session_01KrF8cSY7q1odAy9AW6aAnd`
- Branch: `work/road-api-docs-coverage`. Commit plan-checkbox updates with each task's commit.

---

### Task 1: `Ontology::list_actions` — trait, three implementors, RPC, contracts

**Files:**
- Modify: `src/control-plane/core/src/ontology.rs` (trait method after `get_action`, ~line 625)
- Modify: `src/control-plane/memory/src/ontology.rs` (impl after `get_action`, ~line 123)
- Modify: `src/control-plane/postgres/src/ontology.rs` (impl after `get_action`, ~line 397)
- Modify: `src/control-plane/testkit/src/lib.rs` (extend `ontology_contract`, insert after the `get_action` NotFound assert ~line 992)
- Modify: `src/services/engine-wire/proto/engine_control.proto` (RPC + messages, beside `ListTypes` at lines 27, 149–150)
- Modify: `src/services/engine-wire/src/client.rs` (client method, mirror the `gov_list_types` macro entry at lines 452–455)
- Modify: `src/services/engine/src/service.rs` (handler, mirror `list_types` at lines 505–509)
- Modify: `src/services/query-api/src/wire_control_plane.rs` (WireOntology impl, ~line 151)
- Modify: `src/services/query-api/tests/resolve_governed.rs` (the `MissingType` test stub at :23 implements `Ontology` — add `async fn list_actions(&self, _page: PageReq) -> control_plane_core::Result<Page<ActionDef>> { unreachable!("not exercised by resolve_governed") }` or it fails E0046)
- Modify: `src/services/query-api/tests/wire_governance_e2e.rs` (wire round-trip assert; the fixture already seeds a `createCustomer` action — only the assert is needed)
- Modify: `src/services/query-api/src/serve.rs` (:77–78 comment "the wire governance client does not implement `list_types`" is already false — update it while here; note `live_openapi` receives the **direct** CP, so the RPC leg exists for `WireOntology` trait-completeness + wire symmetry, exercised by the e2e assert, not for docs serving)
- Run: `bash tools/sqlx-prepare.sh` (new `query_scalar!`)

**Interfaces:**
- Produces: `async fn list_actions(&self, page: PageReq) -> Result<Page<ActionDef>>` on the `Ontology` trait; `gov_list_actions(page: &PageReq) -> Page<ActionDef>` on the wire client. Full set in one page (`next: None`), **name-ordered**; `page` accepted for future keyset paging (matching `list_types`).

- [x] **Step 1: Write the failing contract test.** In `testkit/src/lib.rs` inside `ontology_contract`, after the `get_action(NotFound)` assert (~line 992), add (reuse the target type already defined earlier in the contract — read the fn; the `createWidget` action defined at ~line 945 targets it):

```rust
// list_actions: full set in one page, name-ordered, faithful ActionDefs.
let listed = o
    .list_actions(PageReq::unbounded())
    .await
    .expect("list_actions");
assert!(listed.next.is_none(), "single full page");
let names: Vec<&str> = listed.items.iter().map(|a| a.name.0.as_str()).collect();
let mut sorted = names.clone();
sorted.sort_unstable();
// Byte-order comparison; the postgres `order by name` sorts under DB collation.
// They agree for the ASCII action names this contract defines — keep it that way.
assert_eq!(names, sorted, "list_actions is name-ordered");
let widget = listed
    .items
    .iter()
    .find(|a| a.name.0 == "createWidget")
    .expect("createWidget listed");
assert_eq!(
    *widget,
    o.get_action(&ActionName("createWidget".into())).await.unwrap(),
    "listed ActionDef is faithful to get_action"
);
```

(If `createWidget` was redefined later in the contract, compare against the *current* `get_action` result as above — the assert is self-consistent by construction. Deliberate narrowing vs. the spec's "empty page / keyset" bullets: keyset paging isn't implemented anywhere — `page` is accepted-for-future exactly like `list_types` — and an empty-set probe isn't possible at this point in the shared contract; ordering + single-full-page + fidelity is the contract.)

- [x] **Step 2: Run to verify it fails (compile error — method missing).**

Run: `buck2 test //src/control-plane/memory:ontology --unstable-allow-all-tests-on-re > /tmp/t1.log 2>&1; grep -E "error|Tests finished|FAIL" /tmp/t1.log | head`
Expected: compile error `no method named list_actions`.

- [x] **Step 3: Trait + memory + postgres impls.**

`core/src/ontology.rs` (after `get_action`):
```rust
/// Page through every defined action, name-ordered. Adapters return the
/// full set in a single page (`next: None`); `page` is accepted for future
/// keyset paging, like [`Ontology::list_types`].
async fn list_actions(&self, page: PageReq) -> Result<Page<ActionDef>>;
```

`memory/src/ontology.rs`:
```rust
async fn list_actions(&self, _page: PageReq) -> Result<Page<ActionDef>> {
    let mut out: Vec<ActionDef> = self.ontology.lock().actions.values().cloned().collect();
    out.sort_by(|a, b| a.name.0.cmp(&b.name.0));
    Ok(Page::from_full(out))
}
```

`postgres/src/ontology.rs` (mirrors `list_types` at :209):
```rust
async fn list_actions(&self, _page: PageReq) -> Result<Page<ActionDef>> {
    let names = sqlx::query_scalar!("select name from ontology.action order by name")
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
    let mut out = Vec::with_capacity(names.len());
    for n in names {
        out.push(self.get_action(&ActionName(n)).await?);
    }
    Ok(Page::from_full(out))
}
```

- [x] **Step 4: Wire leg.** Proto (`engine_control.proto`, beside ListTypes):
```proto
rpc ListActions      (ListActionsRequest)      returns (ListActionsResponse);
...
message ListActionsRequest  { string page_json = 1; }  // PageReq
message ListActionsResponse { string page_json = 1; }  // Page<ActionDef>
```
Client (`engine-wire/src/client.rs`): copy the `gov_list_types` macro entry (lines 452–455) verbatim with `list_actions`/`ListActionsRequest`/`Page<ActionDef>` substituted. Engine handler (`engine/src/service.rs`): copy the `list_types` handler (lines 505–509) delegating to `self.cp.ontology().list_actions(page)`. `WireOntology` (`wire_control_plane.rs`, beside `list_types` at :149):
```rust
async fn list_actions(&self, page: PageReq) -> Result<Page<ActionDef>> {
    self.client.gov_list_actions(&page).await
}
```

- [x] **Step 5: Refresh `.sqlx`.** Run: `bash tools/sqlx-prepare.sh` — commit the new `query-*.json`.

- [x] **Step 6: Wire e2e assert.** In `query-api/tests/wire_governance_e2e.rs`, where `gov_list_types`/ontology reads are exercised, define an action against an existing type (via the direct CP the fixture holds) and assert the wire client's `list_actions` returns it:
```rust
let acts = wire_cp.ontology().list_actions(PageReq::unbounded()).await.unwrap();
assert!(acts.items.iter().any(|a| a.name.0 == "createThing"));
```
(Adapt names to the fixture's seeded types; keep the assert on round-trip fidelity of `name`, `target`, `kind`, `parameters`.)

- [x] **Step 7: Run the touched targets.**

Run: `buck2 test //src/control-plane/... //src/services/engine-wire/... //src/services/engine:wire //src/services/query-api:wire-governance-e2e //src/services/query-api:resolve-governed --unstable-allow-all-tests-on-re > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t1.log`
Expected: PASS (note: postgres contract targets are fixture tests — they run under the shared PG fixture env automatically via their existing `loom_fixture_test` wiring).

- [x] **Step 8: prek, commit.**
```bash
git add -A && git commit -m "feat(ontology): list_actions across store adapters + engine wire"
```

---

### Task 2: Generator — real action ops, per-type tags, phantom POST deleted

**Files:**
- Modify: `src/services/query-api/src/openapi_gen.rs`
- Modify: `src/services/query-api/src/openapi.rs` (`live_openapi` drains actions)
- Test: `src/services/query-api/tests/openapi_gen.rs`

**Interfaces:**
- Consumes: `ActionDef { name: ActionName(String), target: TypeName, parameters: Vec<ParamDef{name, ty, required, binds}>, kind: ActionKind{Insert|Update|Delete}, assignments }` (Task 1's `list_actions` feeds it).
- Produces: `pub fn ontology_openapi(types: &[ObjectType], links: &[LinkDef], actions: &[ActionDef]) -> (Paths, BTreeMap<String, RefOr<Schema>>)`.

- [x] **Step 1: Rewrite the tests to the new contract (failing).** In `tests/openapi_gen.rs`:
  - Update every `ontology_openapi(...)` call site to pass a third arg (`&[]` where actions are irrelevant).
  - In `generates_per_type_operations`: **assert the phantom is gone** — `assert!(!mp.contains(&("post".into(), "/objects/Customer".into())))`.
  - Add a test `generates_real_action_operations`: build `ActionDef { name: ActionName("createCustomer".into()), target: TypeName("Customer".into()), parameters: vec![ParamDef{name: "name".into(), ty: "string".into(), required: true, binds: None}, ParamDef{name: "tier".into(), ty: "integer".into(), required: false, binds: None}], kind: ActionKind::Insert, assignments: vec![] }`; assert `("post", "/actions/createCustomer")` is present; serialize the doc to JSON and assert the op's `requestBody` schema has properties `name` + `tier` with `required == ["name"]`, response `201` `$ref`s `Customer`, and `tags == ["Customer"]`.
  - Add kind coverage: an `ActionKind::Update` action documents `200` (not `201`); a `Delete` action documents `200`.
  - Add skew guard: an action targeting an absent type emits no path.
  - Add tag coverage: generated GET `/objects/Customer` op and link op tags equal the type name (`["Customer"]`), not `"objects"`/`"links"`.

- [x] **Step 2: Run to verify failure.**

Run: `buck2 test //src/services/query-api:openapi-gen --unstable-allow-all-tests-on-re > /tmp/t2.log 2>&1; grep -E "error|FAIL|Tests finished" /tmp/t2.log | head`
Expected: compile error (arity) then assertion failures.

- [x] **Step 3: Implement.** In `openapi_gen.rs`:
  - Extend the `control_plane_core` import with `ActionDef, ActionKind` (the tests additionally need `ActionName, ParamDef`).
  - Delete `insert_op` and the `merge_operations` POST in the type loop (the type loop emits GET only).
  - `get_objects_op(type_name)`: `.tag("objects")` → `.tag(type_name)`. `link_op(from, ..)`: `.tag("links")` → `.tag(from)`.
  - Add:
```rust
/// Request schema for an action: one property per parameter (`binds` renames the
/// written property, not the wire parameter), `required` from the param flags.
fn action_request_schema(action: &ActionDef) -> RefOr<Schema> {
    let mut b = ObjectBuilder::new().schema_type(SchemaType::Type(Type::Object));
    for p in &action.parameters {
        b = b.property(p.name.clone(), property_schema(&p.ty, p.required));
    }
    for p in action.parameters.iter().filter(|p| p.required) {
        b = b.required(p.name.clone());
    }
    RefOr::T(Schema::Object(b.build()))
}

fn plain_response(description: &str) -> utoipa::openapi::Response {
    ResponseBuilder::new().description(description).build()
}

/// `POST /actions/{name}` for one defined action, tagged by its target type.
fn action_op(action: &ActionDef) -> Operation {
    let target = &action.target.0;
    let (summary, ok_status, ok_desc) = match action.kind {
        ActionKind::Insert => (format!("Insert a {target}"), "201", "Created object"),
        ActionKind::Update => (
            format!("Update a {target} (identity-targeted PATCH)"),
            "200",
            "Updated object",
        ),
        ActionKind::Delete => (
            format!("Delete a {target} by identity"),
            "200",
            "Deleted object (pre-deletion values)",
        ),
    };
    let body = RequestBodyBuilder::new()
        .content("application/json", Content::new(Some(action_request_schema(action))))
        .build();
    OperationBuilder::new()
        .summary(Some(summary))
        .tag(target)
        .security(bearer())
        .request_body(Some(body))
        .response(ok_status, json_response(RefOr::Ref(Ref::from_schema_name(target)), ok_desc))
        .response("400", plain_response("Malformed or undecodable request body"))
        .response("403", plain_response("Write denied by ACL policy"))
        .response("404", plain_response("Unknown action or target object"))
        .response("422", plain_response("Constraint violation or bad parameters"))
        .build()
}
```
  - Signature + loop (same skew guard as links):
```rust
for a in actions {
    if !schemas.contains_key(&a.target.0) {
        continue;
    }
    pb = pb.path(
        format!("/actions/{}", a.name.0),
        PathItem::new(HttpMethod::Post, action_op(a)),
    );
}
```
  - Update the module doc + `ontology_openapi` doc comment (no more "typed-insert POST /objects/{Type}").
  - In `openapi.rs` `live_openapi`: after the links loop, drain actions with the same bounded loop shape as types (`list_actions`, `MAX_TYPE_PAGES`); on error `tracing::warn!` and proceed with empty actions (never fail the document); pass `&actions` to `ontology_openapi`.

- [x] **Step 4: Extend the liveness test.** In `tests/openapi_gen.rs` `live_doc_reflects_defined_types_without_restart` (or a sibling): `define_action` against the memory CP, assert `j["paths"]["/actions/<name>"]["post"].is_object()`.

- [x] **Step 5: Run.**

Run: `buck2 test //src/services/query-api:openapi-gen //src/services/query-api:openapi --unstable-allow-all-tests-on-re > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t2.log`
Expected: PASS.

- [x] **Step 6: prek, commit** — `feat(openapi): real action ops + per-type tags, drop phantom insert path`.

---

### Task 3: Runtime OpenAPI fragments for `/auth/*` + `/admin/*`

**Files:**
- Modify: `src/services/runtime/src/auth.rs` (annotations + `AuthApiDoc` + `ServiceAccountApiDoc` + two pub fns)
- Modify: `src/services/runtime/src/admin.rs` (annotations + `AdminApiDoc` + pub fn)
- Modify: `src/services/runtime/src/lib.rs` (re-export the three fns)
- Test: `src/services/runtime/tests/openapi_fragments.rs` (new)
- Modify: `src/services/runtime/BUCK` (new `rust_test` target `openapi-fragments`, mirror the existing `openapi` target's deps + add `//third-party:serde_json` if absent there)

**Interfaces:**
- Produces: `service_runtime::{auth_openapi, service_account_openapi, admin_openapi}() -> utoipa::openapi::OpenApi`.
- Route inventory (exact, from the routers):
  - auth: `POST /auth/login`, `POST /auth/logout`, `POST /auth/password`
  - service-accounts: `POST /auth/service-accounts`, `GET /auth/service-accounts`, `POST /auth/service-accounts/{id}/tokens`, `GET /auth/service-accounts/{id}/tokens`, `DELETE /auth/service-accounts/{id}/tokens/{token_id}`
  - admin: `POST /admin/users`, `GET /admin/users`, `POST /admin/users/{username}/disable`, `POST /admin/users/{username}/enable`, `POST /admin/users/{username}/password`, `POST /admin/models`, `POST /admin/roles`, `GET /admin/roles`, `POST /admin/roles/{role}/grants`

- [x] **Step 1: Write the failing test** (`tests/openapi_fragments.rs`). Copy the `documented()` helper **verbatim from `query-api/tests/openapi.rs:30–52`** — it filters path-item keys to the 8 HTTP methods (path items also serialize non-operation keys like `summary`/`parameters`, which a naive key scan would misreport). Then:

```rust
#[tokio::test]
async fn auth_fragment_documents_exactly_the_auth_routes() {
    let set = documented(&service_runtime::auth_openapi());
    let expected: BTreeSet<(String, String)> = [
        ("post", "/auth/login"),
        ("post", "/auth/logout"),
        ("post", "/auth/password"),
    ]
    .into_iter()
    .map(|(m, p)| (m.to_string(), p.to_string()))
    .collect();
    assert_eq!(set, expected);
}
```
Plus the analogous `service_account_fragment_...` and `admin_fragment_...` tests with the full inventories above, and:
```rust
#[tokio::test]
async fn no_response_schema_echoes_a_secret() {
    for doc in [
        service_runtime::auth_openapi(),
        service_runtime::service_account_openapi(),
        service_runtime::admin_openapi(),
    ] {
        let json = serde_json::to_value(&doc).unwrap();
        // Walk every documented RESPONSE body schema ref, resolve it in
        // components, and reject secret fields. MintTokenResp is the one
        // deliberate exception: minting is the single moment the raw token
        // is shown. Request DTOs may carry passwords; responses must not.
        let schemas = json["components"]["schemas"].as_object().cloned().unwrap_or_default();
        let mut response_refs = std::collections::BTreeSet::new();
        for (_path, item) in json["paths"].as_object().into_iter().flatten() {
            for (_m, op) in item.as_object().into_iter().flatten() {
                for (_code, resp) in op["responses"].as_object().into_iter().flatten() {
                    if let Some(r) = resp["content"]["application/json"]["schema"]["$ref"].as_str() {
                        response_refs.insert(r.rsplit('/').next().unwrap().to_string());
                    }
                }
            }
        }
        for name in &response_refs {
            let props = schemas[name]["properties"].as_object().cloned().unwrap_or_default();
            assert!(
                !props.contains_key("password") && !props.contains_key("current"),
                "response schema {name} echoes a password field"
            );
            if name != "MintTokenResp" {
                assert!(
                    !props.contains_key("token"),
                    "response schema {name} echoes a token"
                );
            }
        }
    }
}

#[tokio::test]
async fn every_op_requires_bearer_except_login() {
    // The scheme COMPONENT is registered by the serve seam
    // (`register_bearer_scheme`, runtime/src/openapi.rs:41), not by the raw
    // fragments — assert the per-op security REQUIREMENT here instead.
    for doc in [
        service_runtime::auth_openapi(),
        service_runtime::service_account_openapi(),
        service_runtime::admin_openapi(),
    ] {
        let json = serde_json::to_value(&doc).unwrap();
        for (path, item) in json["paths"].as_object().into_iter().flatten() {
            for (method, op) in item.as_object().into_iter().flatten() {
                if !["get", "post", "put", "delete", "patch", "head", "options", "trace"]
                    .contains(&method.as_str())
                {
                    continue;
                }
                let requires_bearer = op["security"]
                    .as_array()
                    .is_some_and(|reqs| reqs.iter().any(|r| r.get("bearer_auth").is_some()));
                if path == "/auth/login" {
                    assert!(!requires_bearer, "login must be unauthenticated");
                } else {
                    assert!(requires_bearer, "{method} {path} must require bearer auth");
                }
            }
        }
    }
}
```

- [x] **Step 2: BUCK target + run to verify failure.** Mirror the runtime `openapi` test target (BUCK ~line 250) as `openapi-fragments` (deps: `:runtime`, `//third-party:utoipa`, `//third-party:serde_json`, `//third-party:tokio`).

Run: `buck2 test //src/services/runtime:openapi-fragments --unstable-allow-all-tests-on-re > /tmp/t3.log 2>&1; grep -E "error|FAIL" /tmp/t3.log | head`
Expected: compile error (`auth_openapi` not found).

- [x] **Step 3: Annotate + derive.** For every handler in the inventory add `#[utoipa::path(...)]` in the exact style of `query-api/src/http.rs:169–184`: method, path (utoipa `{param}` braces), `params(...)` for path params, `request_body = <DTO>` where a body exists, real response statuses (login `200` body `LoginResp` / `401`; logout `200`; password `200`/`403`; create-account `200` always — including fresh creates, `auth.rs:351–358`; mint-token `200` plus `400` for over-cap/zero TTL, `auth.rs:400–430`; revoke-token `200` plus `400` for bad hex, `auth.rs:481–492`; create-user `201`/`200`/`400`; disable/enable `200`; reset-password `200`/`404`; create-role `201`; list-roles `200`; grant `201`; define-model `201` — **verify each against the handler and follow the handler on any disagreement**), `security(("bearer_auth" = []))` on everything except `POST /auth/login`, and tags: `"auth"` / `"service-accounts"` / `"admin"`. Derive `utoipa::ToSchema` on the referenced DTOs (`LoginReq`, `LoginResp`, `ChangePasswordReq`, `CreateAccountReq`, `MintTokenReq`, `CreateUserReq`, `CreateUserResp`, `ListUsersResp`, `UserView`, `ResetPasswordReq`, `CreateRoleReq`, `GrantReq`, `DefineModelReq`, `TableReq`, `PropReq`, plus any response DTO a `body =` names). Then per module:

```rust
#[derive(utoipa::OpenApi)]
#[openapi(
    paths(login, logout, change_password),
    components(schemas(LoginReq, LoginResp, ChangePasswordReq))
)]
struct AuthApiDoc;

/// OpenAPI fragment for the `login_routes` + `session_routes` surface.
#[must_use]
pub fn auth_openapi() -> utoipa::openapi::OpenApi {
    AuthApiDoc::openapi()
}
```
(`ServiceAccountApiDoc` beside the service-account handlers in `auth.rs`; `AdminApiDoc` in `admin.rs`.) Re-export all three from `lib.rs` next to the existing openapi exports.

- [x] **Step 4: Run to green.**

Run: `buck2 test //src/services/runtime:openapi-fragments //src/services/runtime:openapi --unstable-allow-all-tests-on-re > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t3.log`
Expected: PASS.

(Deviation, handler-followed: `LoginResp { token }` joins `MintTokenResp` in the
secret-echo test's exception list — the plan itself mandates `login 200 body =
LoginResp`, and login is the single moment the session token is shown, same
rationale as minting. Also documented the handler-coded statuses the plan's list
omitted: `403` on the five admin-gated service-account ops (`ensure_admin`) and
`400` on grant (`action must be read|write`).)

- [x] **Step 5: prek, commit** — `feat(runtime): OpenAPI fragments for auth, service-account, and admin routes`.

---

### Task 4: Services merge the fragments they mount

**Files:**
- Modify: `src/services/query-api/src/openapi.rs` (`build_openapi` merges three fragments)
- Modify: `src/services/ingest/src/openapi.rs` (its `build_openapi` merges auth + service-accounts — read the file first; ingest mounts no admin router, `ingest/src/serve.rs:42–47`)
- Test: `src/services/query-api/tests/openapi.rs`, `src/services/ingest/tests/openapi.rs` (extend `expected()`)

**Interfaces:**
- Consumes: Task 3's three fragment fns; utoipa's `OpenApi::merge(&mut self, other: OpenApi)`.

- [ ] **Step 1: Extend both `expected()` sets (failing).** query-api's `expected()` (`tests/openapi.rs:9–27`) gains all 17 runtime routes (auth 3 + service-accounts 5 + admin 9); ingest's (`tests/openapi.rs:6–15`) gains the 8 non-admin routes.

- [ ] **Step 2: Run to verify failure.**

Run: `buck2 test //src/services/query-api:openapi //src/services/ingest:openapi --unstable-allow-all-tests-on-re > /tmp/t4.log 2>&1; grep -E "FAIL|Tests finished" /tmp/t4.log`
Expected: FAIL (documented ≠ expected).

- [ ] **Step 3: Merge.** query-api `openapi.rs`:
```rust
pub fn build_openapi() -> utoipa::openapi::OpenApi {
    let mut doc = ApiDoc::openapi();
    doc.merge(service_runtime::auth_openapi());
    doc.merge(service_runtime::service_account_openapi());
    doc.merge(service_runtime::admin_openapi());
    doc
}
```
ingest's `build_openapi` identically minus `admin_openapi`. Update each fn's doc comment (the merged doc now covers the runtime routes the service mounts).

- [ ] **Step 4: Run to green** (same command as Step 2). Expected: PASS. Also run the doc-serving integration targets: `buck2 test //src/services/runtime:openapi //src/services/query-api:openapi-gen --unstable-allow-all-tests-on-re`.

- [ ] **Step 5: prek, commit** — `feat(openapi): merge runtime auth/admin fragments into service documents`.

---

### Task 5: Sweep, docs, register close

**Files:**
- Modify: `docs/system-capabilities/query-api.md` (docs-surface section: real action ops, per-type grouping, merged runtime routes)
- Modify: `docs/system-capabilities/control-plane.md` (ontology concern: `list_actions` + the `ListActions` wire RPC)
- Modify: `docs/system-capabilities/build-and-test.md` (drop the `#road-api-docs-coverage` known-gap bullet — the gap is closed by this PR)
- Modify: `docs/system-capabilities/ui.md` (only if its `/openapi.json` note is stale — read it)
- Modify: `docs/ROADMAP.md` (remove the `road-api-docs-coverage` entry — registers carry open work only; the close is recorded in this PR)

- [ ] **Step 1: Affected-target sweep.** Build + test everything the branch touches:

Run: `buck2 build -M none //src/control-plane/... //src/services/engine-wire/... //src/services/engine/... //src/services/query-api/... //src/services/ingest/... //src/services/runtime/... > /tmp/t5b.log 2>&1; tail -3 /tmp/t5b.log`
Run: `buck2 test //src/control-plane/... //src/services/runtime/... //src/services/query-api/... //src/services/ingest/... //src/services/engine/... //src/services/engine-wire/... --unstable-allow-all-tests-on-re > /tmp/t5.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t5.log`
Expected: build clean; `Tests finished` with 0 fails (includes `sqlx-cache-check` validating the committed `.sqlx`).

- [ ] **Step 2: Docs + register edits** per the file list; then `bash tools/docs.sh validate`.

- [ ] **Step 3: prek, commit** — `docs(api-docs-coverage): capability docs + register close`.

---

### Final gate (orchestrator)

- [ ] Metric gate: `loom-complexity diff` + `loom-duplication diff`; report any NEW hotspot (cc>15 / cog>15 / MI<20 / SLOC>100) or NEW ≥20-line duplication pair — fix or justify in the PR body.
- [ ] Whole-branch final review subagent (spec: `2026-07-03-api-docs-coverage-design.md`).
- [ ] Lease-check (`git ls-remote origin work/road-api-docs-coverage` tip is an ancestor), push, PR from `work/road-api-docs-coverage`, poll CI via commit-status endpoint + BuildBuddy MCP (never `gh pr checks`), squash-merge on green.
