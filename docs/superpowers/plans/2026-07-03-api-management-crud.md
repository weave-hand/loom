# Management API Surface Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 13 management routes (per-type schema read, dataset list/detail, link + action define/delete, grant list/revoke, role delete, user↔role assign/unassign/list) over six new trait methods, every route in the #344 OpenAPI fragments.

**Architecture:** Three concern tasks land the trait methods (Acl, Ontology, Catalog — each across memory + postgres + wire proxy + testkit), then two route tasks surface them (query-api reads; runtime admin writes), then docs/register close. `WireControlPlane::catalog()` switches from panic to direct delegation (the `queue()`/`lineage()` precedent) so dataset reads work in wire mode without new RPCs.

**Tech Stack:** Rust, axum, utoipa 5, sqlx compile-time queries. **No new third-party crates, no buckify.**

**Spec:** `docs/superpowers/specs/2026-07-03-api-management-crud-design.md`

## Global Constraints

- Deletes are **idempotent** trait-level (`Ok(())` when absent), matching `revoke`/`unassign_role`; HTTP deletes → 200.
- No behavior changes to existing routes/handlers.
- Tests: sibling `tests/*.rs` `rust_test` targets only (repo wrapper); fixture tests via `loom_fixture_test`; run with `--unstable-allow-all-tests-on-re`; never pipe `buck2 test` output — redirect + grep.
- Build `-M none`; after SQL changes run `bash tools/sqlx-prepare.sh` and commit the `.sqlx` diff.
- Before EVERY commit: `buck2 run -v0 //tools:prek -- run --all-files > /tmp/prek.log 2>&1; grep -c Failed /tmp/prek.log` prints `0` (rustfmt check-only — fix manually).
- Commit trailers (every commit):
  `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>` and
  `Claude-Session: https://claude.ai/code/session_01KrF8cSY7q1odAy9AW6aAnd`
- Branch `work/road-api-management-crud`; tick plan checkboxes in each task's commit.

---

### Task 1: Acl — `Grant` DTO, `list_grants`, `roles_of`, `delete_role`

**Files:**
- Modify: `src/control-plane/core/src/acl.rs` (Grant struct near `Policy` ~line 154; trait methods after `revoke` ~line 306)
- Modify: `src/control-plane/core/src/lib.rs` (re-export `Grant`)
- Modify: `src/control-plane/memory/src/acl.rs` (impls; state at lines 18–26)
- Modify: `src/control-plane/postgres/src/acl.rs` (impls; grant/revoke SQL at 215–249)
- Modify: `src/services/query-api/src/wire_control_plane.rs` (WireAcl: three `read_only(...)` rejections beside the existing ones)
- Modify: `src/control-plane/testkit/src/lib.rs` (extend `acl_contract`, after ~line 1795)
- Run: `bash tools/sqlx-prepare.sh`

**Interfaces (Produces):**
```rust
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Grant {
    pub action: Action,
    pub target: PolicyTarget,
    pub effect: Effect,
}
// on trait Acl:
async fn list_grants(&self, role: &RoleId, page: PageReq) -> Result<Page<Grant>>;
async fn roles_of(&self, subject: &SubjectId, page: PageReq) -> Result<Page<RoleId>>;
async fn delete_role(&self, role: &RoleId) -> Result<()>;
```
`list_grants`: `NotFound` on unknown role; deterministic order `(action, target-key)`. `roles_of`: `NotFound` on unknown subject; id-ordered. `delete_role`: idempotent; memberships/grants/policies/inheritance edges all go.

- [ ] **Step 1: Failing contract asserts.** In `acl_contract` after the existing grant/revoke/inheritance/policy asserts (~line 1795), using the roles/subjects already defined in the fn (read it — reuse in-scope names, or define fresh `role-mgmt-*` ids via `define_role`/`define_subject` to avoid entangling earlier asserts):

```rust
// list_grants: content, order, NotFound, reflects revoke.
let mgmt = RoleId("mgmt".into());
a.define_role(&mgmt).await.expect("define mgmt");
a.grant(&mgmt, Action::Read, PolicyTarget::Type(TypeName("Widget".into())), Effect::Allow)
    .await
    .expect("grant read");
a.grant(&mgmt, Action::Write, PolicyTarget::Type(TypeName("Widget".into())), Effect::Allow)
    .await
    .expect("grant write");
let grants = a.list_grants(&mgmt, PageReq::unbounded()).await.expect("list_grants");
assert_eq!(grants.items.len(), 2);
assert!(grants.next.is_none());
assert_eq!(grants.items[0].action, Action::Read, "action-ordered");
assert_eq!(grants.items[0].effect, Effect::Allow);
assert_eq!(grants.items[0].target, PolicyTarget::Type(TypeName("Widget".into())));
assert!(matches!(
    a.list_grants(&RoleId("no-such-role".into()), PageReq::unbounded()).await,
    Err(ControlPlaneError::NotFound(_))
));
a.revoke(&mgmt, Action::Write, &PolicyTarget::Type(TypeName("Widget".into())))
    .await
    .expect("revoke");
assert_eq!(
    a.list_grants(&mgmt, PageReq::unbounded()).await.expect("relist").items.len(),
    1
);

// roles_of: after assign/unassign; NotFound on unknown subject.
let who = SubjectId("mgmt-user".into());
a.define_subject(&who).await.expect("subject");
a.assign_role(&who, &mgmt).await.expect("assign");
let mine = a.roles_of(&who, PageReq::unbounded()).await.expect("roles_of");
assert_eq!(mine.items, vec![mgmt.clone()]);
a.unassign_role(&who, &mgmt).await.expect("unassign");
assert!(a.roles_of(&who, PageReq::unbounded()).await.expect("empty").items.is_empty());
assert!(matches!(
    a.roles_of(&SubjectId("no-such-subject".into()), PageReq::unbounded()).await,
    Err(ControlPlaneError::NotFound(_))
));

// delete_role: cascade observed, idempotent, re-creatable.
a.assign_role(&who, &mgmt).await.expect("re-assign");
a.delete_role(&mgmt).await.expect("delete");
assert!(!a.has_role(&who, &mgmt).await.expect("membership gone"));
assert!(!a.list_roles().await.expect("roles").contains(&mgmt));
a.delete_role(&mgmt).await.expect("idempotent");
a.define_role(&mgmt).await.expect("re-create after delete");
assert!(
    a.list_grants(&mgmt, PageReq::unbounded()).await.expect("fresh").items.is_empty(),
    "re-created role has no stale grants"
);
```
(The contract has an Ontology bound already for grant-target checks — `Widget` is the type it defines; verify the in-scope name and existing imports before writing.)

- [ ] **Step 2: Verify red.** `buck2 test //src/control-plane/memory:acl --unstable-allow-all-tests-on-re > /tmp/t1.log 2>&1; grep -E "error|Tests finished" /tmp/t1.log | head` — expect E0599 (no method `list_grants`).

- [ ] **Step 3: Core + memory + postgres + wire.**
Memory (state fields cited above; lock style per file):
```rust
async fn list_grants(&self, role: &RoleId, _page: PageReq) -> Result<Page<Grant>> {
    let acl = self.acl.lock();
    if !acl.roles.contains(&role.0) {
        return Err(ControlPlaneError::NotFound(format!("role {}", role.0)));
    }
    let mut out: Vec<Grant> = acl
        .grants
        .iter()
        .filter(|((r, _, _), _)| r == &role.0)
        .map(|((_, action, key), effect)| Grant {
            action: *action,
            target: key.to_policy_target(),
            effect: *effect,
        })
        .collect();
    out.sort_by_key(|g| (g.action, g.target.key_parts()));
    Ok(Page::from_full(out))
}
```
(`TargetKey` is the memory adapter's internal key — read its shape; add a `to_policy_target()` helper on it, or map inline from its fields. If `Action`/`PolicyTarget::key_parts()` aren't `Ord`-friendly, sort by the `(action.as_str(), key_parts())` tuple instead.)
`roles_of`: unknown-subject check against `acl.subjects`, then collect `members` pairs with matching subject, sort, `Page::from_full`. `delete_role`: remove from `roles`; `members.retain`, `grants.retain`, `policies.retain`, `inherits.retain` (both positions).
Postgres:
```rust
async fn list_grants(&self, role: &RoleId, _page: PageReq) -> Result<Page<Grant>> {
    if !role_exists(&self.pool, &role.0).await? {
        return Err(ControlPlaneError::NotFound(format!("role {}", role.0)));
    }
    let rows = sqlx::query!(
        "select action, target_kind, target_a, target_b, effect from acl.role_grant \
         where role_id = $1 order by action, target_kind, target_a, target_b",
        role.0,
    )
    .fetch_all(&self.pool)
    .await
    .map_err(backend)?;
    ...
}
```
(Reuse the file's existing role-existence check and the `(kind, a, b)` → `PolicyTarget` decode helper — grep for where `policies_for`/`check` decode target rows; if only an encode helper exists, add the decode next to it. `action`/`effect` decode via their existing `FromStr`/match, same as the file's other readers.)
`roles_of`: existence check on `acl.subject`, then `select role_id from acl.role_member where subject_id = $1 order by role_id`. `delete_role`: `delete from acl.role where id = $1` (children cascade per migrations 0003/0007/0011).
WireAcl: three `Err(read_only("list_grants"))`-style rejections (reads too — served direct per spec; keep the message accurate, e.g. `read_only("list_grants (served direct)")` if `read_only` wording fits; otherwise mirror the existing pattern exactly).

- [ ] **Step 4: `bash tools/sqlx-prepare.sh`; commit the `.sqlx` diff with the task.**

- [ ] **Step 5: Green.** `buck2 test //src/control-plane/... //src/services/query-api:resolve-governed --unstable-allow-all-tests-on-re > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t1.log` — expect PASS. (`resolve_governed.rs`'s `MissingType` stub implements `Ontology`, not `Acl` — no edit expected; the target is in the run to prove it.)

- [ ] **Step 6: prek; commit** `feat(acl): list_grants, roles_of, delete_role across adapters`.

---

### Task 2: Ontology — `delete_link`, `delete_action`

**Files:**
- Modify: `src/control-plane/core/src/ontology.rs` (trait methods beside `define_link`/`define_action`)
- Modify: `src/control-plane/memory/src/ontology.rs`, `src/control-plane/postgres/src/ontology.rs`
- Modify: `src/services/query-api/src/wire_control_plane.rs` (WireOntology rejections)
- Modify: `src/services/query-api/tests/resolve_governed.rs` (`MissingType` stub: two `unreachable!` impls — it implements `Ontology`, so this IS required here)
- Modify: `src/control-plane/testkit/src/lib.rs` (extend `ontology_contract`)
- Run: `bash tools/sqlx-prepare.sh`

**Interfaces (Produces):**
```rust
async fn delete_link(&self, from: &TypeName, name: &str) -> Result<()>;
async fn delete_action(&self, name: &ActionName) -> Result<()>;
```
Both idempotent. Definitions only — physical columns/join tables untouched.

- [ ] **Step 1: Failing contract asserts** (in `ontology_contract`, after the `list_actions` block; reuse the in-scope `Widget` type / `createWidget` action and the link the contract defines — read the fn):
```rust
// delete_link: gone from reads, idempotent, re-definable.
// (use the actual in-scope link: its `from` type and name — read the contract)
o.delete_link(&TypeName("Widget".into()), "made_of").await.expect("delete_link");
assert!(
    !o.links(&TypeName("Widget".into()), PageReq::unbounded())
        .await
        .expect("links")
        .items
        .iter()
        .any(|l| l.name == "made_of"),
    "deleted link no longer listed"
);
o.delete_link(&TypeName("Widget".into()), "made_of").await.expect("idempotent");

// delete_action: gone from get + list, idempotent, re-definable.
o.delete_action(&ActionName("createWidget".into())).await.expect("delete_action");
assert!(matches!(
    o.get_action(&ActionName("createWidget".into())).await,
    Err(ControlPlaneError::NotFound(_))
));
assert!(
    !o.list_actions(PageReq::unbounded())
        .await
        .expect("list")
        .items
        .iter()
        .any(|a| a.name.0 == "createWidget"),
    "deleted action not listed"
);
o.delete_action(&ActionName("createWidget".into())).await.expect("idempotent");
```
**Placement caution:** these asserts REMOVE definitions earlier asserts created — insert them at the END of the contract (after every assert that still reads them), or define+delete fresh `mgmt-*` names instead. Read the tail of the fn first and choose whichever keeps every existing assert green.

- [ ] **Step 2: Red** (same command shape, `//src/control-plane/memory:ontology`). Expect E0599.

- [ ] **Step 3: Implement.** Memory: `ont.links.retain(|l| !(l.from == *from && l.name == name)); Ok(())` and `ont.actions.remove(&name.0); Ok(())`. Postgres: `delete from ontology.link where from_type = $1 and name = $2` and `delete from ontology.action where name = $1` (steps/params/assignments cascade per migration 0030). WireOntology: `Err(read_only("delete_link"))` / `Err(read_only("delete_action"))` beside the `define_*` rejections. `MissingType` stub: `unreachable!("not exercised by resolve_governed")` bodies.

- [ ] **Step 4: sqlx-prepare; Step 5: green** (`//src/control-plane/... //src/services/query-api:resolve-governed //src/services/query-api:wire-governance-e2e`); **Step 6: prek; commit** `feat(ontology): delete_link + delete_action across adapters`.

---

### Task 3: Catalog — `list_tables` + wire delegation

**Files:**
- Modify: `src/control-plane/core/src/catalog.rs` (trait method after `schema` ~line 83)
- Modify: `src/control-plane/memory/src/catalog.rs` (state at lines 12–19)
- Modify: `src/control-plane/postgres/src/iceberg_catalog.rs` (beside `current_snapshot` ~line 160)
- Modify: `src/services/query-api/src/wire_control_plane.rs` (`catalog()` panic → `self.direct.catalog()`, comment updated)
- Modify: `src/control-plane/testkit/src/lib.rs` (extend `catalog_contract` — it takes a `CatalogSeed` with `seed` + `drop_table`)
- Run: `bash tools/sqlx-prepare.sh`

**Interfaces (Produces):**
```rust
/// Every live table in the mirror, `(schema, name)`-ordered, single full page.
async fn list_tables(&self, page: PageReq) -> Result<Page<TableRef>>;
```

- [ ] **Step 1: Failing contract asserts** in `catalog_contract` (it already seeds tables via `seeder.seed(SeedSpec{..})` and can `drop_table` — read the fn to reuse its seeded `TableRef`s):
```rust
// list_tables: seeded tables listed, ordered; dropped tables disappear.
let listed = catalog.list_tables(PageReq::unbounded()).await.expect("list_tables");
assert!(listed.next.is_none());
let mut sorted = listed.items.clone();
sorted.sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));
assert_eq!(listed.items, sorted, "(schema, name)-ordered");
assert!(listed.items.contains(&seeded_table), "seeded table listed");
seeder.drop_table(&seeded_table).await;
assert!(
    !catalog.list_tables(PageReq::unbounded()).await.expect("relist").items.contains(&seeded_table),
    "dropped table no longer live"
);
```
(Adapt `seeded_table` to the contract's actual variable; if the contract already drops that table later, order the asserts to respect it.)

- [ ] **Step 2: Red** (memory catalog test target — find it in `src/control-plane/memory/BUCK`, likely `:catalog`).

- [ ] **Step 3: Implement.** Memory: keys of `tables` whose `Versioned` is live (read the `Versioned` type for its end-bound field/liveness check — mirror how `files`/`columns` filter liveness), sorted by `(schema.clone(), name.clone())`, `Page::from_full`. Postgres:
```rust
async fn list_tables(&self, _page: PageReq) -> Result<Page<TableRef>> {
    let rows = sqlx::query!(
        "select table_namespace, table_name from iceberg_mirror.\"table\" \
         where end_snapshot is null order by table_namespace, table_name"
    )
    ...
}
```
(Verify the exact mirror table identifier + quoting against `current_snapshot`'s query in the same file; map rows to `TableRef { schema: table_namespace, name: table_name }`.) Wire: replace the `catalog()` panic with `self.direct.catalog()` and rewrite the comment (catalog metadata reads are served by the direct control plane, like `queue()`/`lineage()`).

- [ ] **Step 4: sqlx-prepare; Step 5: green** (`//src/control-plane/...` + the query-api build); **Step 6: prek; commit** `feat(catalog): list_tables + direct wire delegation`.

---

### Task 4: query-api reads — type detail, datasets

**Files:**
- Modify: `src/services/query-api/src/http.rs` (three handlers + routes at the router fn ~line 90–111)
- Modify: `src/services/query-api/src/openapi.rs` (ApiDoc `paths(...)` + `components(schemas(...))` additions)
- Modify: `src/services/query-api/tests/openapi.rs` (`expected()` +3)
- Test: `src/services/query-api/tests/ontology_type_detail.rs` (new), `src/services/query-api/tests/datasets_routes.rs` (new)
- Modify: `src/services/query-api/BUCK` (two new test targets — mirror an existing memory-backed http test target; if seeding the memory catalog needs the fixture instead, mirror a `loom_fixture_test` target — decide by reading how existing tests land tables against the memory CP, e.g. the testkit snapshot-commit path or `e2e_support::land`)

**Routes (exact contracts from the spec):**
- `GET /ontology/types/{name}` → 200 `TypeDetailResponse { name, table: {schema, name}, identity: Option<String>, properties: [{name, ty, required}], links: [LinkView], links_to: [LinkView] }`, `LinkView { name, from, to, cardinality }`; 404 unknown type. Handler reads `st.cp.ontology()` (wire-capable: `gov_get_type`/`gov_links`/`gov_links_to`). Map `NotFound` → 404, else the file's `internal_error`.
- `GET /datasets` → 200 `DatasetsResponse { datasets: [{schema, name}] }` via `st.cp.catalog().list_tables(...)` (works in wire mode after Task 3's delegation).
- `GET /datasets/{schema}/{table}` → 200 `DatasetDetailResponse { table, snapshot_id, snapshot_time, columns: [{name, ty, nullable}] }` composing `current_snapshot` + `schema`; `NotFound` → 404.
All three: `security(("bearer_auth" = []))`, tags `"ontology"` / `"datasets"`, DTOs with `ToSchema` in `openapi.rs` beside `OntologyTypesResponse`, full utoipa annotations in the file's exact style.

- [ ] **Step 1: Failing route tests.** Type detail (memory CP, mirroring the style of existing http tests — define a type + a link via the memory ontology, drive the router with `tower::ServiceExt::oneshot` + a session bearer from the shared helpers): assert 200 shape (name/table/identity/properties incl. required flags, links + links_to non-empty for the seeded link) and 404 for `no-such-type`. Datasets: land/commit a table (per whichever seeding path the chosen BUCK flavor supports), assert `GET /datasets` lists it, `GET /datasets/{schema}/{table}` returns the snapshot + columns, and unknown table → 404.
- [ ] **Step 2: Red** (new targets fail E0599/404-route). **Step 3: Implement** handlers + ApiDoc + `expected()` (+`("get","/ontology/types/{name}")`, `("get","/datasets")`, `("get","/datasets/{schema}/{table}")`). **Step 4: Green:** the two new targets + `//src/services/query-api:openapi` + `:openapi-gen`. **Step 5: prek; commit** `feat(query-api): type-detail and dataset read routes`.

---

### Task 5: runtime admin — define/delete links + actions, grants, roles, user↔role

**Files:**
- Modify: `src/services/runtime/src/admin.rs` (8 handlers, DTOs, router chain at ~line 415–427, `AdminApiDoc` at ~429–455)
- Modify: `src/services/runtime/tests/openapi_fragments.rs` (admin route-set 9 → 17)
- Test: `src/services/runtime/tests/admin_management.rs` (new; mirror the existing admin-routes test target in `src/services/runtime/BUCK`)
- Modify: `src/services/query-api/tests/openapi.rs` (`expected()` +8 — the admin fragment is merged into query-api's doc; ingest is untouched, it mounts no admin router)

**Routes (contracts from the spec; every handler `#[utoipa::path]`-annotated, `tag = "admin"`, bearer + `require_admin` via the existing router chain):**
- `POST /admin/links` body = `LinkDef` serde shape (`request_body = control_plane_core::LinkDef`? No — derive a local `ToSchema` DTO is wrong here; `LinkDef` lacks `ToSchema`. Annotate `request_body = serde_json::Value` with a description naming the `LinkDef` serde shape, deserialize to `LinkDef` in the handler — the exact pattern `post_action` uses for its open body) → 201 `"defined"`; `NotFound`/`Validation` from `define_link` → `status_for` (404/400 — follow the handler; document what `status_for` yields).
- `POST /admin/actions` body = `ActionDef` serde shape, same open-body pattern → 201; validation errors per `status_for`.
- `DELETE /admin/links/{from}/{name}` → `delete_link` → 200 `{"deleted": {"from": .., "name": ..}}`.
- `DELETE /admin/actions/{name}` → 200 `{"deleted": {"name": ..}}`.
- `DELETE /admin/roles/{role}` → `delete_role` → 200 `{"deleted": {"role": ..}}`.
- `GET /admin/roles/{role}/grants` → `list_grants` → 200 `RoleGrantsResp { grants: Vec<GrantView> }`, `GrantView { action: String, target: serde_json::Value, effect: String }` (render via the core types' serde + `as_str()`); 404 unknown role.
- `DELETE /admin/roles/{role}/grants` body = existing `GrantReq` → parse action exactly as the POST does (same 400 branch), `PolicyTarget::Type`, `revoke` → 200 `"revoked"`.
- `PUT /admin/users/{username}/roles/{role}` → `assign_role(SubjectId(username), RoleId(role))` (username IS the subject id — `create_user` at admin.rs:92–99) → 200; `NotFound` → 404.
- `DELETE /admin/users/{username}/roles/{role}` → `unassign_role` → 200 (idempotent; still 404 when the subject is undefined — match the trait's existence behavior, read it).
- `GET /admin/users/{username}/roles` → `roles_of` → 200 `{ "roles": [..] }`; 404 unknown user.
Router: `.route("/admin/links", post(define_link_route))`, `.route("/admin/links/:from/:name", delete(delete_link_route))`, `.route("/admin/actions", post(define_action_route))`, `.route("/admin/actions/:name", delete(delete_action_route))`, `.route("/admin/roles/:role", delete(delete_role_route))`, `.route("/admin/roles/:role/grants", post(grant).get(list_role_grants).delete(revoke_grant))`, `.route("/admin/users/:username/roles", get(user_roles))`, `.route("/admin/users/:username/roles/:role", put(assign_user_role).delete(unassign_user_role))`.

- [ ] **Step 1: Failing tests.** Extend the fragment route-set test to the 17-route admin inventory (9 existing + the 8 above with `{param}` braces). New `admin_management.rs` (memory CP + the admin router driven like the existing admin tests — mirror their setup): happy paths for all 8, 404s (unknown role grants-list, unknown user roles), 400 (bad revoke action string), idempotent second delete → 200, and grants list reflects grant→revoke.
- [ ] **Step 2: Red.** **Step 3: Implement** handlers + `AdminApiDoc` additions (+ any new `ToSchema` DTOs) + query-api `expected()` +8. **Step 4: Green:** `//src/services/runtime/... //src/services/query-api:openapi --unstable-allow-all-tests-on-re`. **Step 5: prek; commit** `feat(runtime): management admin routes — links, actions, grants, roles, user-role`.

---

### Task 6: Sweep, docs, register close

**Files:**
- Modify: `docs/system-capabilities/control-plane.md` (new trait surface: list_grants/roles_of/delete_role, delete_link/delete_action, list_tables)
- Modify: `docs/system-capabilities/query-api.md` (type-detail + dataset reads) and `docs/system-capabilities/build-and-test.md` (Self-documenting HTTP API paragraph: the admin fragment now carries the management surface — one sentence)
- Modify: `docs/ROADMAP.md` (remove the `road-api-management-crud` entry — close in this PR; keep `fut-ontology-type-delete` in FUTURE as landed earlier on the branch)
- Use `#PRNUM` placeholders; swap post-PR-creation (targeted sed on ONLY the files this branch owns).

- [ ] **Step 1: Whole-tree affected sweep:** `buck2 build -M none //src/control-plane/... //src/services/... > /tmp/t6b.log 2>&1; tail -2 /tmp/t6b.log` then `buck2 test //src/control-plane/... //src/services/... --unstable-allow-all-tests-on-re > /tmp/t6.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t6.log` — 0 fails (re-run individual targets on transient RE upload errors).
- [ ] **Step 2: Docs edits + `bash tools/docs.sh validate`.**
- [ ] **Step 3: prek; commit** `docs(api-management-crud): capability docs + register close`.

---

### Final gate (orchestrator)

- [ ] Metric gate: `loom-complexity diff` + `loom-duplication diff`; fix or justify new findings.
- [ ] Whole-branch final review subagent (spec: `2026-07-03-api-management-crud-design.md`).
- [ ] Lease-check, push, PR from `work/road-api-management-crud`, `#PRNUM` swap + push, CI via commit-status endpoint (+ BuildBuddy MCP on failure), squash-merge on green.
