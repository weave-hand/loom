# Admin HTTP Surface Gaps Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose the five already-enforced, SQL-only-administrable capabilities (fine-grained policies, table-target grants, derived properties, property constraints, vector index definitions) over the admin-gated HTTP surface. Closes GitHub issue #363.

**Architecture:** All new/changed routes live in `src/services/runtime/src/admin.rs` behind the existing `require_auth` → `require_admin` layers, mapped onto backend surfaces that already exist and are validated in both adapters. One new trait method (`Acl::list_policies`) gives policy read-back parity with `list_grants`. OpenAPI comes from the existing `#[utoipa::path]` + `AdminApiDoc` machinery; query-api's `documents_exactly_the_expected_routes` test pins the route set.

**Tech Stack:** Rust, axum, utoipa, sqlx compile-time queries (postgres adapter), buck2 (`rust_test` targets only — NEVER inline `#[cfg(test)]`).

**Spec:** `docs/superpowers/specs/2026-07-04-admin-surface-gaps-design.md` (committed on this branch — read it for the problem statement; this plan is self-contained for code).

## Global Constraints

- **Wire vocabulary (exact):** action `"read"|"write"`; metric `"cosine"|"l2"`; index kind `"flat"|"ivf_flat"|"hnsw"`; agg kind `"count"|"sum"|"avg"|"min"|"max"`; the exactly-one-of-target 400 error string is `exactly one of type or table`.
- **Row filters and targets ride the domain serde encodings** (`RowFilter` e.g. `{"Compare":{"property":"region","op":"Eq","value":{"Text":"emea"}}}`; `PolicyTarget` e.g. `{"Type":"Widget"}` / `{"Table":{"schema":"main","name":"widget"}}`) — never invent a parallel encoding.
- **Error idiom:** backend `ControlPlaneError` → `status_for(&e).into_response()` (bare status), exactly like every existing admin handler. Handler-level validation failures are 400 with either a `Json(json!({"error": ...}))` body (grant idiom) or a `format!` string body (link idiom) — each task says which it uses.
- **Tests:** integration `rust_test` targets only; unit tests go in `tests/<name>.rs` wired in the crate's BUCK. The prek hook `no-inline-tests` rejects `#[test]` under `src/`.
- **Test commands:** always `buck2 test <targets> --unstable-allow-all-tests-on-re > /tmp/t.log 2>&1` then `grep -E "Tests finished|FAIL" /tmp/t.log` — NEVER pipe buck2 test through `head`/`tail` (it deadlocks). Build with `-M none`.
- **After changing any SQL in `src/control-plane/postgres`:** run `bash tools/sqlx-prepare.sh` and commit the `.sqlx/` change with the code.
- **Before every commit:** `buck2 run -v0 //tools:prek -- run --all-files > /tmp/prek.log 2>&1; grep -c Failed /tmp/prek.log` must print `0` (grep exits 1 when the count is 0 — do not `&&`-chain it). Commit with `--no-verify` and these trailers:

  ```
  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01KrF8cSY7q1odAy9AW6aAnd
  ```
- Do NOT push. The controller pushes after the final review.
- Clippy is pedantic+restriction on prod code (no `unwrap`/`expect`/indexing in `src/`); test code is exempt via the `loom_rust_test`/`loom_fixture_test` wrappers already in the BUCK files.

---

### Task 1: `Acl::list_policies` — core trait + both adapters + contract

**Files:**
- Modify: `src/control-plane/core/src/acl.rs` (add `RolePolicy` after `Grant` ~line 222; add trait method after `clear_policy` ~line 386)
- Modify: `src/control-plane/core/src/lib.rs` (re-export `RolePolicy` alongside `Grant` — find the existing `pub use acl::{...}` list)
- Modify: `src/control-plane/memory/src/acl.rs`
- Modify: `src/control-plane/postgres/src/acl.rs` (+ `.sqlx` refresh)
- Modify: `src/control-plane/testkit/src/lib.rs` (legs inside `acl_contract`, after the clear-policy legs ~line 1930)

**Interfaces:**
- Produces: `pub struct RolePolicy { pub action: Action, pub policy: Policy }` (core) and `async fn list_policies(&self, role: &RoleId, page: PageReq) -> Result<Page<RolePolicy>>` on `trait Acl`. Task 3's `GET /admin/roles/{role}/policies` consumes both.

- [ ] **Step 1: Write the failing contract legs** — in `src/control-plane/testkit/src/lib.rs`, inside `acl_contract`, after the existing `clear_policy` legs (the block asserting "Read policy cleared" / Write intact, ~line 1930). The contract already seeds types `Customer`, `Invoice`, `Ticket`, `Widget`, `Gadget` and defines roles via `a.define_role(&rid(...))`; helpers `rid`/`ttype`/`ttable` are in scope:

```rust
    // --- list_policies: role-scoped read-back, ordered by (action, target-key) ---
    a.define_role(&rid("auditor")).await.unwrap();
    let p_widget_write = Policy {
        target: ttype("Widget"),
        row_filter: None,
        deny_columns: vec!["cost".into()],
        mask_columns: vec![],
    };
    let p_gadget_read = Policy {
        target: ttype("Gadget"),
        row_filter: None,
        deny_columns: vec![],
        mask_columns: vec!["name".into()],
    };
    let p_table_read = Policy {
        target: ttable("main", "widget"),
        row_filter: None,
        deny_columns: vec![],
        mask_columns: vec![],
    };
    // Insert out of order to prove the listing sorts.
    a.set_policy(&rid("auditor"), Action::Write, p_widget_write.clone())
        .await
        .unwrap();
    a.set_policy(&rid("auditor"), Action::Read, p_gadget_read.clone())
        .await
        .unwrap();
    a.set_policy(&rid("auditor"), Action::Read, p_table_read.clone())
        .await
        .unwrap();
    let listed = a
        .list_policies(&rid("auditor"), PageReq::unbounded())
        .await
        .unwrap();
    // Order: action ("read" < "write"), then target kind ("table" < "type"), then a, b.
    assert_eq!(
        listed.items,
        vec![
            RolePolicy { action: Action::Read, policy: p_table_read },
            RolePolicy { action: Action::Read, policy: p_gadget_read },
            RolePolicy { action: Action::Write, policy: p_widget_write.clone() },
        ],
        "list_policies returns the role's policies ordered by (action, target-key)"
    );
    // clear shrinks the listing; other rows untouched
    a.clear_policy(&rid("auditor"), Action::Read, &ttype("Gadget"))
        .await
        .unwrap();
    let listed = a
        .list_policies(&rid("auditor"), PageReq::unbounded())
        .await
        .unwrap();
    assert_eq!(listed.len(), 2, "cleared policy no longer listed");
    // unknown role -> NotFound (mirrors list_grants)
    assert!(matches!(
        a.list_policies(&rid("no-such-role"), PageReq::unbounded()).await,
        Err(ControlPlaneError::NotFound(_))
    ));
    // a policy with a row_filter round-trips through the listing intact
    let p_filtered = Policy {
        target: ttype("Customer"),
        row_filter: Some(RowFilter::Compare {
            property: "region".into(),
            op: CompareOp::Eq,
            value: ScalarValue::Text("emea".into()),
        }),
        deny_columns: vec![],
        mask_columns: vec![],
    };
    a.set_policy(&rid("auditor"), Action::Read, p_filtered.clone())
        .await
        .unwrap();
    let listed = a
        .list_policies(&rid("auditor"), PageReq::unbounded())
        .await
        .unwrap();
    assert!(
        listed.items.contains(&RolePolicy { action: Action::Read, policy: p_filtered }),
        "row_filter survives the role-scoped listing"
    );
```

Add `RolePolicy` to the testkit's existing `control_plane_core` import list. (`Policy`, `RowFilter`, `CompareOp`, `ScalarValue`, `ControlPlaneError`, `PageReq` are already imported — verify, don't duplicate.)

- [ ] **Step 2: Add the core type + trait method.** In `src/control-plane/core/src/acl.rs`, directly after the `Grant` struct (~line 222):

```rust
/// One row of a role's policy listing: the action the policy binds plus the
/// policy itself. The role-scoped dual of [`Grant`], returned by
/// [`Acl::list_policies`].
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RolePolicy {
    pub action: Action,
    pub policy: Policy,
}
```

In `trait Acl`, directly after `clear_policy`:

```rust
    /// All policies of `role`, ordered by `(action, target-key)`. Role must
    /// exist, else `NotFound`. The `page` request is accepted but not yet
    /// enforced; results are a single full page. (Mirrors `list_grants`.)
    async fn list_policies(&self, role: &RoleId, page: PageReq) -> Result<Page<RolePolicy>>;
```

Re-export `RolePolicy` from `src/control-plane/core/src/lib.rs` in the same `pub use` that exports `Grant`.

- [ ] **Step 3: Memory adapter.** In `src/control-plane/memory/src/acl.rs`, after `list_grants` (mirror it exactly — role-exists check, filter, sort by string forms; add `RolePolicy` to the `control_plane_core` import):

```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn list_policies(&self, role: &RoleId, _page: PageReq) -> Result<Page<RolePolicy>> {
        let acl = self.acl.lock();
        if !acl.roles.contains(&role.0) {
            return Err(ControlPlaneError::NotFound(format!("role {}", role.0)));
        }
        let mut out = Vec::new();
        for ((r, action, _), policy) in &acl.policies {
            if r == &role.0 {
                out.push(RolePolicy {
                    action: *action,
                    policy: policy.clone(),
                });
            }
        }
        // Action is NOT Ord — sort by the string forms (matches postgres's
        // `order by action, target_kind, ...` on the text columns).
        out.sort_by(|x, y| {
            (x.action.as_str(), x.policy.target.key_parts())
                .cmp(&(y.action.as_str(), y.policy.target.key_parts()))
        });
        Ok(Page::from_full(out))
    }
```

- [ ] **Step 4: Postgres adapter.** In `src/control-plane/postgres/src/acl.rs`, after `clear_policy` (add `RolePolicy` to the import):

```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn list_policies(&self, role: &RoleId, _page: PageReq) -> Result<Page<RolePolicy>> {
        if !role_exists(&self.pool, &role.0).await? {
            return Err(ControlPlaneError::NotFound(format!("role {}", role.0)));
        }
        let rows = sqlx::query!(
            "select action, target_kind, target_a, target_b, row_filter, deny_columns, mask_columns \
             from acl.policy where role_id = $1 \
             order by action, target_kind, target_a, target_b",
            &role.0,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let row_filter = match r.row_filter {
                Some(v) => Some(
                    serde_json::from_value(v)
                        .map_err(|e| ControlPlaneError::Serialization(e.to_string()))?,
                ),
                None => None,
            };
            out.push(RolePolicy {
                action: r.action.parse()?,
                policy: Policy {
                    target: PolicyTarget::from_key_parts(&r.target_kind, &r.target_a, &r.target_b)?,
                    row_filter,
                    deny_columns: r.deny_columns,
                    mask_columns: r.mask_columns,
                },
            });
        }
        Ok(Page::from_full(out))
    }
```

Then refresh the compile-time cache: `bash tools/sqlx-prepare.sh` (commit the `.sqlx/` diff with this task).

**Other `Acl` implementors:** grep `impl Acl for` across `src/` — besides the two adapters there is query-api's `WireAcl` (`src/services/query-api/src/wire_control_plane.rs`), which implements read-side methods and errors on unsupported ones. Add `list_policies` there returning the same "unsupported over the wire" error its `list_grants`/other admin reads use (copy the adjacent method's idiom exactly). If any other implementor turns up (testkit fakes), mirror the memory impl.

- [ ] **Step 5: Run the contract on both adapters**

Run: `buck2 test //src/control-plane/memory:acl //src/control-plane/postgres:acl --unstable-allow-all-tests-on-re > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t1.log`
Expected: PASS (both). Before the impl exists the build fails (missing trait method) — that build error is this task's "red".

- [ ] **Step 6: prek + commit**

```bash
buck2 run -v0 //tools:prek -- run --all-files > /tmp/prek.log 2>&1; grep -c Failed /tmp/prek.log   # must print 0
git add -A && git commit --no-verify -m "feat(acl): role-scoped policy listing — Acl::list_policies + RolePolicy" -m "..."
```

---

### Task 2: Table-target grants — widen `GrantReq`, shared target/action parsers

**Files:**
- Modify: `src/services/runtime/src/admin.rs` (GrantReq ~line 306; `grant` ~line 326; `revoke_grant` ~line 614)
- Test: `src/services/runtime/tests/admin_management.rs` (existing `rust_test` target `//src/services/runtime:admin-management`)

**Interfaces:**
- Consumes: nothing new.
- Produces: `fn parse_action(s: &str) -> Result<Action, Response>` and `fn parse_target(ty: Option<String>, table: Option<TableReq>) -> Result<PolicyTarget, Response>` — private helpers in `admin.rs` that Task 3 reuses for `PolicyReq`. `GrantReq` fields become `action: String, r#type: Option<String>, table: Option<TableReq>`.

- [ ] **Step 1: Write the failing tests** in `src/services/runtime/tests/admin_management.rs` (reuse the file's `app`/`seed_admin_session`/`seed_types`/`req_json`/`send` helpers; look at an existing grant test for the exact call shape):

```rust
#[tokio::test]
async fn grant_table_target_roundtrips() {
    let cp = Arc::new(MemoryControlPlane::default());
    let token = seed_admin_session(&cp, "root").await;
    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/roles/admin/grants", &token,
            r#"{"action":"read","table":{"schema":"main","name":"widget"}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    // listed with the PolicyTarget serde shape
    let (status, body) = send(
        app(cp.clone()),
        req_json("GET", "/admin/roles/admin/grants", &token, ""),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let grants = v["grants"].as_array().unwrap();
    assert!(
        grants.iter().any(|g| g["target"]["Table"]["schema"] == "main"
            && g["target"]["Table"]["name"] == "widget"),
        "table grant listed: {body}"
    );
    // revoke with the same body shape (idempotent)
    let (status, _) = send(
        app(cp.clone()),
        req_json("DELETE", "/admin/roles/admin/grants", &token,
            r#"{"action":"read","table":{"schema":"main","name":"widget"}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_, body) = send(
        app(cp),
        req_json("GET", "/admin/roles/admin/grants", &token, ""),
    )
    .await;
    assert!(!body.contains("\"Table\""), "revoked table grant gone: {body}");
}

#[tokio::test]
async fn grant_requires_exactly_one_target() {
    let cp = Arc::new(MemoryControlPlane::default());
    seed_types(&cp).await;
    let token = seed_admin_session(&cp, "root").await;
    // neither
    let (status, body) = send(
        app(cp.clone()),
        req_json("POST", "/admin/roles/admin/grants", &token, r#"{"action":"read"}"#),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("exactly one of type or table"), "{body}");
    // both
    let (status, _) = send(
        app(cp),
        req_json("POST", "/admin/roles/admin/grants", &token,
            r#"{"action":"read","type":"Widget","table":{"schema":"main","name":"widget"}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn grant_type_target_still_works_and_unknown_type_still_400() {
    let cp = Arc::new(MemoryControlPlane::default());
    seed_types(&cp).await;
    let token = seed_admin_session(&cp, "root").await;
    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/roles/admin/grants", &token,
            r#"{"action":"read","type":"Widget"}"#),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = send(
        app(cp),
        req_json("POST", "/admin/roles/admin/grants", &token,
            r#"{"action":"read","type":"NoSuchType"}"#),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}
```

(If a nearly identical unknown-type-400 / type-grant test already exists in the file, extend it rather than duplicating.)

- [ ] **Step 2: Run to verify failure**

Run: `buck2 test //src/services/runtime:admin-management --unstable-allow-all-tests-on-re > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t2.log`
Expected: FAIL (table-target grant → 400 "unknown field" / missing-field decode error today, since `r#type` is required `String`).

- [ ] **Step 3: Implement.** In `admin.rs`:

Replace `GrantReq`:

```rust
#[derive(serde::Deserialize, utoipa::ToSchema)]
struct GrantReq {
    action: String,
    /// Exactly one of `type`/`table` must be set.
    #[serde(default)]
    r#type: Option<String>,
    #[serde(default)]
    table: Option<TableReq>,
}
```

Add the shared parsers near `GrantReq` (these replace the duplicated action-match in `grant` and `revoke_grant`):

```rust
/// Parse the wire action token shared by grant/policy bodies.
fn parse_action(s: &str) -> std::result::Result<Action, Response> {
    match s {
        "read" => Ok(Action::Read),
        "write" => Ok(Action::Write),
        _ => Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "action must be read|write" })),
        )
            .into_response()),
    }
}

/// Resolve the exclusive `type`/`table` target pair shared by grant/policy
/// bodies. Exactly one must be set.
fn parse_target(
    ty: Option<String>,
    table: Option<TableReq>,
) -> std::result::Result<PolicyTarget, Response> {
    match (ty, table) {
        (Some(t), None) => Ok(PolicyTarget::Type(TypeName(t))),
        (None, Some(t)) => Ok(PolicyTarget::Table(TableRef {
            schema: t.schema,
            name: t.name,
        })),
        _ => Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "exactly one of type or table" })),
        )
            .into_response()),
    }
}
```

Rewrite the bodies of `grant` and `revoke_grant` to use them:

```rust
    let action = match parse_action(&req.action) {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    let target = match parse_target(req.r#type, req.table) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
```

(then the existing `.grant(...)` / `.revoke(...)` calls, unchanged). Update the two `#[utoipa::path]` docs: request-body description mentions the exclusive pair; the 400 response description becomes "action is not read|write, exactly-one-of-target violated, or unknown grant-target type". `TableReq` is already in `AdminApiDoc` schemas; `GrantReq` stays listed — no `AdminApiDoc` change needed.

- [ ] **Step 4: Run to verify pass**

Run: `buck2 test //src/services/runtime:admin-management --unstable-allow-all-tests-on-re > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t2.log`
Expected: PASS.

- [ ] **Step 5: prek + commit** (same recipe as Task 1 Step 6). Message: `feat(admin): table-target grants over HTTP — GrantReq takes type XOR table`.

---

### Task 3: Policy routes — `POST`/`GET`/`DELETE /admin/roles/{role}/policies`

**Files:**
- Modify: `src/services/runtime/src/admin.rs` (new types + 3 handlers + route + `AdminApiDoc`)
- Modify: `src/services/query-api/tests/openapi.rs` (`expected()` set gains 3 routes)
- Test: `src/services/runtime/tests/admin_management.rs`

**Interfaces:**
- Consumes: `parse_action`/`parse_target` (Task 2), `Acl::{set_policy, clear_policy, list_policies}` + `RolePolicy` (Task 1). New imports in `admin.rs`: `Policy`, `RowFilter` from `control_plane_core`.
- Produces: routes `POST|GET|DELETE /admin/roles/{role}/policies`.

- [ ] **Step 1: Write the failing tests** in `admin_management.rs`:

```rust
#[tokio::test]
async fn policy_set_list_clear_roundtrip() {
    let cp = Arc::new(MemoryControlPlane::default());
    seed_types(&cp).await;
    let token = seed_admin_session(&cp, "root").await;
    let body = r#"{
        "action": "read",
        "type": "Widget",
        "row_filter": {"Compare": {"property": "id", "op": "Eq", "value": {"Int": 1}}},
        "deny_columns": ["cost"],
        "mask_columns": ["id"]
    }"#;
    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/roles/admin/policies", &token, body),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, listed) = send(
        app(cp.clone()),
        req_json("GET", "/admin/roles/admin/policies", &token, ""),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&listed).unwrap();
    let pols = v["policies"].as_array().unwrap();
    assert_eq!(pols.len(), 1, "{listed}");
    assert_eq!(pols[0]["action"], "read");
    assert_eq!(pols[0]["target"]["Type"], "Widget");
    assert_eq!(pols[0]["row_filter"]["Compare"]["property"], "id");
    assert_eq!(pols[0]["deny_columns"][0], "cost");
    assert_eq!(pols[0]["mask_columns"][0], "id");
    // clear (idempotent), listing empties
    let (status, _) = send(
        app(cp.clone()),
        req_json("DELETE", "/admin/roles/admin/policies", &token,
            r#"{"action":"read","type":"Widget"}"#),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_, listed) = send(
        app(cp),
        req_json("GET", "/admin/roles/admin/policies", &token, ""),
    )
    .await;
    let v: serde_json::Value = serde_json::from_str(&listed).unwrap();
    assert!(v["policies"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn policy_validation_errors_are_400() {
    let cp = Arc::new(MemoryControlPlane::default());
    seed_types(&cp).await;
    let token = seed_admin_session(&cp, "root").await;
    // row_filter that does not decode as a RowFilter
    let (status, body) = send(
        app(cp.clone()),
        req_json("POST", "/admin/roles/admin/policies", &token,
            r#"{"action":"read","type":"Widget","row_filter":{"Bogus":1}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("invalid RowFilter"), "{body}");
    // unknown type target -> adapter Validation -> 400
    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/roles/admin/policies", &token,
            r#"{"action":"read","type":"NoSuchType"}"#),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // row_filter naming an unknown property -> adapter Validation -> 400
    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/roles/admin/policies", &token,
            r#"{"action":"read","type":"Widget","row_filter":{"Compare":{"property":"nope","op":"Eq","value":{"Int":1}}}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // exactly-one-of-target
    let (status, body) = send(
        app(cp.clone()),
        req_json("POST", "/admin/roles/admin/policies", &token, r#"{"action":"read"}"#),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("exactly one of type or table"), "{body}");
    // unknown role -> 404
    let (status, _) = send(
        app(cp),
        req_json("POST", "/admin/roles/no-such-role/policies", &token,
            r#"{"action":"read","type":"Widget"}"#),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn policy_table_target_accepted() {
    let cp = Arc::new(MemoryControlPlane::default());
    let token = seed_admin_session(&cp, "root").await;
    // Table targets skip type/property validation by design (deferred existence).
    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/roles/admin/policies", &token,
            r#"{"action":"read","table":{"schema":"main","name":"widget"},"mask_columns":["name"]}"#),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (_, listed) = send(
        app(cp),
        req_json("GET", "/admin/roles/admin/policies", &token, ""),
    )
    .await;
    let v: serde_json::Value = serde_json::from_str(&listed).unwrap();
    assert_eq!(v["policies"][0]["target"]["Table"]["name"], "widget", "{listed}");
}
```

- [ ] **Step 2: Run to verify failure** (same command as Task 2 Step 2; expect 404-not-405 style failures since the route doesn't exist).

- [ ] **Step 3: Implement.** In `admin.rs` (add `Policy`, `RowFilter`, `RolePolicy` to the `control_plane_core` import):

```rust
#[derive(serde::Deserialize, utoipa::ToSchema)]
struct PolicyReq {
    action: String,
    /// Exactly one of `type`/`table` must be set.
    #[serde(default)]
    r#type: Option<String>,
    #[serde(default)]
    table: Option<TableReq>,
    /// A `RowFilter` in its serde shape, e.g.
    /// `{"Compare":{"property":"region","op":"Eq","value":{"Text":"emea"}}}`,
    /// `{"And":[...]}`, `{"Not":{...}}`. Absent/null means no row filter.
    #[serde(default)]
    row_filter: Option<serde_json::Value>,
    #[serde(default)]
    deny_columns: Vec<String>,
    #[serde(default)]
    mask_columns: Vec<String>,
}

/// Create or replace the fine-grained policy for `(role, action, target)`.
#[utoipa::path(
    post, path = "/admin/roles/{role}/policies",
    params(("role" = String, Path, description = "Role the policy binds")),
    request_body = PolicyReq,
    responses(
        (status = 201, description = "Policy set (upsert by role/action/target)"),
        (status = 400, description = "action is not read|write, exactly-one-of-target violated, \
            row_filter does not decode as a RowFilter, or validation failed (unknown type, \
            unknown row-filter property, caller-predicate-only operator)"),
        (status = 404, description = "Unknown role"),
    ),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn set_policy_route(
    State(st): State<AdminState>,
    Path(role): Path<String>,
    Json(req): Json<PolicyReq>,
) -> Response {
    let action = match parse_action(&req.action) {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    let target = match parse_target(req.r#type, req.table) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    let row_filter: Option<RowFilter> = match req.row_filter {
        Some(v) => match serde_json::from_value(v) {
            Ok(f) => Some(f),
            Err(e) => {
                return (StatusCode::BAD_REQUEST, format!("invalid RowFilter: {e}"))
                    .into_response();
            }
        },
        None => None,
    };
    let policy = Policy {
        target,
        row_filter,
        deny_columns: req.deny_columns,
        mask_columns: req.mask_columns,
    };
    match st.cp.acl().set_policy(&RoleId(role), action, policy).await {
        Ok(()) => (StatusCode::CREATED, "defined").into_response(),
        Err(e) => status_for(&e).into_response(),
    }
}

/// One policy row as rendered on the admin read surface.
#[derive(serde::Serialize, utoipa::ToSchema)]
struct PolicyView {
    action: String,
    /// The `PolicyTarget` serde shape, e.g. `{"Type": "Widget"}`.
    target: serde_json::Value,
    /// The stored `RowFilter` serde shape, or `null`.
    row_filter: serde_json::Value,
    deny_columns: Vec<String>,
    mask_columns: Vec<String>,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
struct RolePoliciesResp {
    policies: Vec<PolicyView>,
}

/// List a role's fine-grained policies.
#[utoipa::path(
    get, path = "/admin/roles/{role}/policies",
    params(("role" = String, Path, description = "Role whose policies to list")),
    responses(
        (status = 200, description = "The role's policies", body = RolePoliciesResp),
        (status = 404, description = "Unknown role"),
    ),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn list_role_policies(State(st): State<AdminState>, Path(role): Path<String>) -> Response {
    match st
        .cp
        .acl()
        .list_policies(&RoleId(role), PageReq::unbounded())
        .await
    {
        Ok(page) => {
            let policies = page
                .items
                .into_iter()
                .map(|rp| PolicyView {
                    action: rp.action.as_str().to_string(),
                    target: serde_json::to_value(&rp.policy.target).unwrap_or_default(),
                    row_filter: rp
                        .policy
                        .row_filter
                        .as_ref()
                        .map(|f| serde_json::to_value(f).unwrap_or_default())
                        .unwrap_or(serde_json::Value::Null),
                    deny_columns: rp.policy.deny_columns,
                    mask_columns: rp.policy.mask_columns,
                })
                .collect();
            Json(RolePoliciesResp { policies }).into_response()
        }
        Err(e) => status_for(&e).into_response(),
    }
}

/// Remove the policy for `(role, action, target)` (idempotent).
///
/// The body is a `PolicyReq`; only `action` and the target pair are read.
#[utoipa::path(
    delete, path = "/admin/roles/{role}/policies",
    params(("role" = String, Path, description = "Role whose policy to clear")),
    request_body = PolicyReq,
    responses(
        (status = 200, description = "Cleared (idempotent)"),
        (status = 400, description = "action is not read|write, or exactly-one-of-target violated"),
    ),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn clear_policy_route(
    State(st): State<AdminState>,
    Path(role): Path<String>,
    Json(req): Json<PolicyReq>,
) -> Response {
    let action = match parse_action(&req.action) {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    let target = match parse_target(req.r#type, req.table) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    match st.cp.acl().clear_policy(&RoleId(role), action, &target).await {
        Ok(()) => (StatusCode::OK, "cleared").into_response(),
        Err(e) => status_for(&e).into_response(),
    }
}
```

Wire the route in `admin_routes` (after the grants route):

```rust
        .route(
            "/admin/roles/:role/policies",
            post(set_policy_route)
                .get(list_role_policies)
                .delete(clear_policy_route),
        )
```

Add `set_policy_route`, `list_role_policies`, `clear_policy_route` to `AdminApiDoc` paths and `PolicyReq`, `PolicyView`, `RolePoliciesResp` to its schemas.

- [ ] **Step 4: Extend `expected()`** in `src/services/query-api/tests/openapi.rs` with:

```rust
        ("post", "/admin/roles/{role}/policies"),
        ("get", "/admin/roles/{role}/policies"),
        ("delete", "/admin/roles/{role}/policies"),
```

- [ ] **Step 5: Run to verify pass**

Run: `buck2 test //src/services/runtime:admin-management //src/services/query-api:openapi --unstable-allow-all-tests-on-re > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t3.log`
Expected: PASS.

- [ ] **Step 6: prek + commit.** Message: `feat(admin): fine-grained policy authoring over HTTP — set/list/clear role policies`.

---

### Task 4: Derived properties + property constraints in `DefineModelReq`

**Files:**
- Modify: `src/services/runtime/src/admin.rs` (`PropReq`, `DefineModelReq`, `define_model` ~line 384, new mapper fns + request types, `AdminApiDoc` schemas)
- Test: `src/services/runtime/tests/admin_management.rs`

**Interfaces:**
- Consumes: `control_plane_core::{Aggregation, DerivedPropertyDef, PropertyConstraints, RangeConstraint, LengthConstraint}` (all exist; check `lib.rs` re-exports and import whatever path `define_model` already uses for `PropertyConstraints`).
- Produces: `DefineModelReq` gains `derived: Vec<DerivedReq>`; `PropReq` gains `constraints: Option<ConstraintsReq>`.

- [ ] **Step 1: Write the failing tests:**

```rust
#[tokio::test]
async fn define_model_with_derived_and_constraints_roundtrips() {
    let cp = Arc::new(MemoryControlPlane::default());
    let token = seed_admin_session(&cp, "root").await;
    let body = r#"{
        "name": "Order",
        "table": {"schema": "main", "name": "orders"},
        "identity": "id",
        "properties": [
            {"name": "id", "ty": "Int", "required": true},
            {"name": "qty", "ty": "Int", "constraints": {"range": {"min": 1, "max": 100}}},
            {"name": "status", "ty": "Text",
             "constraints": {"length": {"min": 2, "max": 16}, "one_of": ["open", "closed"]}}
        ],
        "derived": [
            {"name": "line_count", "ty": "Int", "link": "lines", "agg": {"kind": "count"}},
            {"name": "total", "ty": "Int", "link": "lines", "agg": {"kind": "sum", "column": "amount"}}
        ]
    }"#;
    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/models", &token, body),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    // stored intact on the ontology (assert through the control plane)
    let t = cp.get_type(&TypeName("Order".into())).await.unwrap();
    assert_eq!(t.derived.len(), 2);
    assert_eq!(t.derived[0].name, "line_count");
    assert!(matches!(t.derived[0].agg, Aggregation::Count));
    assert!(matches!(t.derived[1].agg, Aggregation::Sum(ref c) if c == "amount"));
    let qty = t.properties.iter().find(|p| p.name == "qty").unwrap();
    assert_eq!(qty.constraints.range.as_ref().unwrap().min, Some(1.0));
    assert_eq!(qty.constraints.range.as_ref().unwrap().max, Some(100.0));
    let status_p = t.properties.iter().find(|p| p.name == "status").unwrap();
    assert_eq!(status_p.constraints.one_of.as_ref().unwrap().len(), 2);
    assert_eq!(status_p.constraints.length.as_ref().unwrap().max, Some(16));
}

#[tokio::test]
async fn define_model_agg_and_constraint_errors_are_400() {
    let cp = Arc::new(MemoryControlPlane::default());
    let token = seed_admin_session(&cp, "root").await;
    // unknown agg kind
    let (status, body) = send(
        app(cp.clone()),
        req_json("POST", "/admin/models", &token,
            r#"{"name":"T","table":{"schema":"main","name":"t"},"identity":null,
                "properties":[{"name":"id","ty":"Int"}],
                "derived":[{"name":"d","ty":"Int","link":"l","agg":{"kind":"median"}}]}"#),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("unknown agg kind"), "{body}");
    // count with a column
    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/models", &token,
            r#"{"name":"T","table":{"schema":"main","name":"t"},"identity":null,
                "properties":[{"name":"id","ty":"Int"}],
                "derived":[{"name":"d","ty":"Int","link":"l","agg":{"kind":"count","column":"x"}}]}"#),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // sum without a column
    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/models", &token,
            r#"{"name":"T","table":{"schema":"main","name":"t"},"identity":null,
                "properties":[{"name":"id","ty":"Int"}],
                "derived":[{"name":"d","ty":"Int","link":"l","agg":{"kind":"sum"}}]}"#),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // range constraint on a Text property -> define-gate Validation -> 400
    let (status, _) = send(
        app(cp),
        req_json("POST", "/admin/models", &token,
            r#"{"name":"T","table":{"schema":"main","name":"t"},"identity":null,
                "properties":[{"name":"s","ty":"Text","constraints":{"range":{"min":1}}}]}"#),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}
```

(Import `Aggregation` and any missing core types in the test file. If `get_type` needs an `Ontology` trait import, add it — mirror how other tests in the file call `cp.define_type`.)

- [ ] **Step 2: Run to verify failure** (unknown-field errors: serde rejects `constraints`/`derived`? No — axum's `Json` ignores unknown fields by default only if the struct allows; serde structs reject unknown fields only with `deny_unknown_fields`, which these don't set, so today's decode silently DROPS the new fields. Expected failure mode: 201 but `t.derived` empty / constraints default — the roundtrip asserts fail. The 400 tests fail with 201 too.)

- [ ] **Step 3: Implement.** In `admin.rs`:

```rust
#[derive(serde::Deserialize, utoipa::ToSchema)]
struct RangeReq {
    #[serde(default)]
    min: Option<f64>,
    #[serde(default)]
    max: Option<f64>,
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
struct LengthReq {
    #[serde(default)]
    min: Option<u32>,
    #[serde(default)]
    max: Option<u32>,
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
struct ConstraintsReq {
    /// Numeric bound; valid only on numeric property types.
    #[serde(default)]
    range: Option<RangeReq>,
    /// String length bound; valid only on string property types.
    #[serde(default)]
    length: Option<LengthReq>,
    /// Regex the value must match; valid only on string property types.
    #[serde(default)]
    pattern: Option<String>,
    /// Closed value vocabulary; valid only on string property types.
    #[serde(default)]
    one_of: Option<Vec<String>>,
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
struct AggReq {
    /// "count" | "sum" | "avg" | "min" | "max".
    kind: String,
    /// Target-type column to aggregate; required for every kind except "count".
    #[serde(default)]
    column: Option<String>,
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
struct DerivedReq {
    name: String,
    /// Result type, e.g. "Int".
    ty: String,
    /// The link the aggregation traverses.
    link: String,
    agg: AggReq,
}

fn to_constraints(c: Option<ConstraintsReq>) -> PropertyConstraints {
    let Some(c) = c else {
        return PropertyConstraints::default();
    };
    PropertyConstraints {
        range: c.range.map(|r| RangeConstraint { min: r.min, max: r.max }),
        length: c.length.map(|l| LengthConstraint { min: l.min, max: l.max }),
        pattern: c.pattern,
        one_of: c.one_of,
    }
}

fn parse_agg(agg: AggReq) -> std::result::Result<Aggregation, String> {
    fn need(
        column: Option<String>,
        kind: &str,
        f: fn(String) -> Aggregation,
    ) -> std::result::Result<Aggregation, String> {
        column
            .map(f)
            .ok_or_else(|| format!("agg kind {kind} requires a column"))
    }
    match agg.kind.as_str() {
        "count" => match agg.column {
            None => Ok(Aggregation::Count),
            Some(_) => Err("agg kind count takes no column".to_string()),
        },
        "sum" => need(agg.column, "sum", Aggregation::Sum),
        "avg" => need(agg.column, "avg", Aggregation::Avg),
        "min" => need(agg.column, "min", Aggregation::Min),
        "max" => need(agg.column, "max", Aggregation::Max),
        other => Err(format!("unknown agg kind `{other}` (want count|sum|avg|min|max)")),
    }
}
```

Extend the request structs and handler:

```rust
#[derive(serde::Deserialize, utoipa::ToSchema)]
struct PropReq {
    name: String,
    ty: String,
    #[serde(default)]
    required: bool,
    /// Per-value constraints enforced by the land and action gates (422 on violation).
    #[serde(default)]
    constraints: Option<ConstraintsReq>,
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
struct DefineModelReq {
    name: String,
    table: TableReq,
    identity: Option<String>,
    properties: Vec<PropReq>,
    /// Aggregate-over-link derived properties. Link existence is not validated
    /// here (matches `define_type`; see iss-delete-link-derived-dangle).
    #[serde(default)]
    derived: Vec<DerivedReq>,
}
```

In `define_model`, before building `otype`:

```rust
    let mut derived = Vec::with_capacity(req.derived.len());
    for d in req.derived {
        match parse_agg(d.agg) {
            Ok(agg) => derived.push(DerivedPropertyDef {
                name: d.name,
                ty: d.ty,
                link: d.link,
                agg,
            }),
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": e })),
                )
                    .into_response();
            }
        }
    }
```

and in the `ObjectType` literal: `constraints: to_constraints(p.constraints)` replaces the `default()`, and `derived` replaces `vec![]`. Update the `define_model` `#[utoipa::path]` responses with a 400 line ("invalid agg kind/column pairing, or constraint invalid for the property type"). Add `ConstraintsReq`, `RangeReq`, `LengthReq`, `AggReq`, `DerivedReq` to `AdminApiDoc` schemas. Add `Aggregation`, `DerivedPropertyDef`, `RangeConstraint`, `LengthConstraint` imports (and switch the existing `control_plane_core::PropertyConstraints::default()` usage to the imported name if that reads cleaner — keep it consistent).

- [ ] **Step 4: Run to verify pass** (same targets as Task 2). Expected: PASS.
- [ ] **Step 5: prek + commit.** Message: `feat(admin): derived properties + per-value constraints in POST /admin/models`.

---

### Task 5: Vector index routes — `POST`/`GET /admin/models/{type}/vector-indexes`

**Files:**
- Modify: `src/services/runtime/src/admin.rs`
- Modify: `src/services/query-api/tests/openapi.rs` (2 routes)
- Test: `src/services/runtime/tests/admin_management.rs`

**Interfaces:**
- Consumes: `Ontology::{define_vector_index, vector_indexes_for}`, `control_plane_core::{VectorIndexDef}` and `control_plane_core` vector-index types `Metric` (FromStr: `"cosine"|"l2"`), `IndexSpec` (`as_cols() -> (&'static str, Option<u32>, Option<u32>, Option<u32>)` with kind tokens `flat|ivf_flat|hnsw`) — check `lib.rs` re-export paths (`control_plane_core::vector_index::{IndexSpec, Metric}` or re-exported at root; use whatever query-api already imports).
- Produces: the two routes.

- [ ] **Step 1: Write the failing tests.** Note: memory `define_vector_index` requires the type to exist and the property to have a `vector(N)` type — seed one:

```rust
async fn seed_vector_type(cp: &MemoryControlPlane) {
    cp.define_type(
        ObjectType::build("Doc", ("main", "doc"))
            .prop("id", "Int")
            .prop("embedding", "vector(3)")
            .done(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn vector_index_define_and_list_roundtrip() {
    let cp = Arc::new(MemoryControlPlane::default());
    seed_vector_type(&cp).await;
    let token = seed_admin_session(&cp, "root").await;
    // defaults: flat + cosine
    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/models/Doc/vector-indexes", &token,
            r#"{"name":"embed_idx","property":"embedding"}"#),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    // explicit hnsw + l2
    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/models/Doc/vector-indexes", &token,
            r#"{"name":"embed_hnsw","property":"embedding","metric":"l2",
                "spec":{"kind":"hnsw","m":16,"ef_construction":200}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, body) = send(
        app(cp),
        req_json("GET", "/admin/models/Doc/vector-indexes", &token, ""),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let idx = v["indexes"].as_array().unwrap();
    assert_eq!(idx.len(), 2, "{body}");
    let flat = idx.iter().find(|i| i["name"] == "embed_idx").unwrap();
    assert_eq!(flat["metric"], "cosine");
    assert_eq!(flat["spec"]["kind"], "flat");
    let hnsw = idx.iter().find(|i| i["name"] == "embed_hnsw").unwrap();
    assert_eq!(hnsw["metric"], "l2");
    assert_eq!(hnsw["spec"]["kind"], "hnsw");
    assert_eq!(hnsw["spec"]["m"], 16);
    assert_eq!(hnsw["spec"]["ef_construction"], 200);
}

#[tokio::test]
async fn vector_index_errors() {
    let cp = Arc::new(MemoryControlPlane::default());
    seed_vector_type(&cp).await;
    let token = seed_admin_session(&cp, "root").await;
    // unknown metric
    let (status, body) = send(
        app(cp.clone()),
        req_json("POST", "/admin/models/Doc/vector-indexes", &token,
            r#"{"name":"i","property":"embedding","metric":"dotproduct"}"#),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("metric"), "{body}");
    // unknown kind
    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/models/Doc/vector-indexes", &token,
            r#"{"name":"i","property":"embedding","spec":{"kind":"annoy"}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // tuning field on the wrong kind
    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/models/Doc/vector-indexes", &token,
            r#"{"name":"i","property":"embedding","spec":{"kind":"hnsw","nlist":10}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // non-vector property -> adapter Validation -> 400
    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/models/Doc/vector-indexes", &token,
            r#"{"name":"i","property":"id"}"#),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // unknown type -> adapter NotFound -> 404
    let (status, _) = send(
        app(cp),
        req_json("POST", "/admin/models/Nope/vector-indexes", &token,
            r#"{"name":"i","property":"embedding"}"#),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
```

- [ ] **Step 2: Run to verify failure** (routes 404/405 today).

- [ ] **Step 3: Implement.** In `admin.rs`:

```rust
#[derive(serde::Deserialize, utoipa::ToSchema)]
struct VectorIndexSpecReq {
    /// "flat" | "ivf_flat" | "hnsw".
    kind: String,
    /// ivf_flat only.
    #[serde(default)]
    nlist: Option<u32>,
    /// hnsw only.
    #[serde(default)]
    m: Option<u32>,
    /// hnsw only.
    #[serde(default)]
    ef_construction: Option<u32>,
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
struct VectorIndexReq {
    name: String,
    /// The vector-typed property the index covers.
    property: String,
    /// "cosine" (default) | "l2".
    #[serde(default)]
    metric: Option<String>,
    /// Defaults to `{"kind": "flat"}`.
    #[serde(default)]
    spec: Option<VectorIndexSpecReq>,
}

fn parse_index_spec(spec: Option<VectorIndexSpecReq>) -> std::result::Result<IndexSpec, String> {
    let Some(s) = spec else {
        return Ok(IndexSpec::Flat);
    };
    match s.kind.as_str() {
        "flat" => {
            if s.nlist.is_some() || s.m.is_some() || s.ef_construction.is_some() {
                return Err("flat takes no tuning fields".to_string());
            }
            Ok(IndexSpec::Flat)
        }
        "ivf_flat" => {
            if s.m.is_some() || s.ef_construction.is_some() {
                return Err("ivf_flat takes only nlist".to_string());
            }
            Ok(IndexSpec::IvfFlat { nlist: s.nlist })
        }
        "hnsw" => {
            if s.nlist.is_some() {
                return Err("hnsw takes only m and ef_construction".to_string());
            }
            Ok(IndexSpec::Hnsw {
                m: s.m,
                ef_construction: s.ef_construction,
            })
        }
        other => Err(format!("unknown index kind `{other}` (want flat|ivf_flat|hnsw)")),
    }
}

/// Declare (or replace, by `(type, name)`) a vector index over a vector-typed property.
#[utoipa::path(
    post, path = "/admin/models/{type}/vector-indexes",
    params(("type" = String, Path, description = "Ontology type the index belongs to")),
    request_body = VectorIndexReq,
    responses(
        (status = 201, description = "Vector index declared (upsert by type/name)"),
        (status = 400, description = "Unknown metric or index kind, tuning field on the \
            wrong kind, or the property is not vector-typed"),
        (status = 404, description = "Unknown type"),
    ),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn define_vector_index_route(
    State(st): State<AdminState>,
    Path(ty): Path<String>,
    Json(req): Json<VectorIndexReq>,
) -> Response {
    let metric = match req.metric.as_deref() {
        None => Metric::default(),
        Some(s) => match s.parse::<Metric>() {
            Ok(m) => m,
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": "metric must be cosine|l2" })),
                )
                    .into_response();
            }
        },
    };
    let spec = match parse_index_spec(req.spec) {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": e })),
            )
                .into_response();
        }
    };
    let def = VectorIndexDef {
        name: req.name.clone(),
        type_name: TypeName(ty),
        property: req.property,
        metric,
        spec,
    };
    match st.cp.ontology().define_vector_index(def).await {
        Ok(()) => (
            StatusCode::CREATED,
            Json(serde_json::json!({ "name": req.name })),
        )
            .into_response(),
        Err(e) => status_for(&e).into_response(),
    }
}

/// One vector index as rendered on the admin read surface (request vocabulary).
#[derive(serde::Serialize, utoipa::ToSchema)]
struct VectorIndexView {
    name: String,
    property: String,
    metric: String,
    /// `{"kind": ...}` plus the kind's tuning fields when set.
    spec: serde_json::Value,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
struct VectorIndexesResp {
    indexes: Vec<VectorIndexView>,
}

/// List a type's declared vector indexes.
#[utoipa::path(
    get, path = "/admin/models/{type}/vector-indexes",
    params(("type" = String, Path, description = "Ontology type whose indexes to list")),
    responses((status = 200, description = "The type's vector indexes", body = VectorIndexesResp)),
    security(("bearer_auth" = [])),
    tag = "admin",
)]
async fn list_vector_indexes_route(
    State(st): State<AdminState>,
    Path(ty): Path<String>,
) -> Response {
    match st.cp.ontology().vector_indexes_for(&TypeName(ty)).await {
        Ok(defs) => {
            let indexes = defs
                .into_iter()
                .map(|d| {
                    let (kind, nlist, m, ef) = d.spec.as_cols();
                    let mut spec = serde_json::Map::new();
                    spec.insert("kind".into(), kind.into());
                    if let Some(v) = nlist {
                        spec.insert("nlist".into(), v.into());
                    }
                    if let Some(v) = m {
                        spec.insert("m".into(), v.into());
                    }
                    if let Some(v) = ef {
                        spec.insert("ef_construction".into(), v.into());
                    }
                    VectorIndexView {
                        name: d.name,
                        property: d.property,
                        metric: d.metric.as_str().to_string(),
                        spec: serde_json::Value::Object(spec),
                    }
                })
                .collect();
            Json(VectorIndexesResp { indexes }).into_response()
        }
        Err(e) => status_for(&e).into_response(),
    }
}
```

Route wiring:

```rust
        .route(
            "/admin/models/:type/vector-indexes",
            post(define_vector_index_route).get(list_vector_indexes_route),
        )
```

Add the two handlers to `AdminApiDoc` paths and `VectorIndexReq`, `VectorIndexSpecReq`, `VectorIndexView`, `VectorIndexesResp` to schemas. Imports: `VectorIndexDef` plus `IndexSpec`/`Metric` from wherever core re-exports them (grep an existing consumer, e.g. query-api's search handler, for the import path). `vector_indexes_for` sorting: whatever the adapters return; the test above uses `find`, not order.

- [ ] **Step 4: Extend `expected()`** in `src/services/query-api/tests/openapi.rs`:

```rust
        ("post", "/admin/models/{type}/vector-indexes"),
        ("get", "/admin/models/{type}/vector-indexes"),
```

- [ ] **Step 5: Run to verify pass**

Run: `buck2 test //src/services/runtime:admin-management //src/services/query-api:openapi --unstable-allow-all-tests-on-re > /tmp/t5.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t5.log`
Expected: PASS.

- [ ] **Step 6: prek + commit.** Message: `feat(admin): vector index declare/list over HTTP`.

---

### Task 6: Governed e2e, capability docs, register close

**Files:**
- Modify: `src/services/query-api/tests/admin_e2e.rs` (target `//src/services/query-api:admin-e2e`, a `loom_fixture_test`)
- Modify: `docs/system-capabilities/control-plane.md` (the "Administration is layered on top" section ~line 279)
- Modify: `docs/FUTURE.md` (remove the `fut-admin-governance-http-surface` entry — title line + prose line)

**Interfaces:**
- Consumes: everything from Tasks 1–5.

- [ ] **Step 1: Write the failing e2e.** Read `governance_routes_end_to_end` (`admin_e2e.rs:250`) first — it already builds `full_app` (admin routes merged with the governed object router over the real pg fixture + Iceberg serving engine), seeds a `main.widget` table via `IcebergWriter`/`SeedCol`, defines the `Widget` model over HTTP, creates a `reader` user/role, grants Read over HTTP, and reads `/objects/Widget` as the reader. Add a new test in the same file that reuses its exact seeding/session idioms (copy the setup block, not the assertions):

```rust
/// Fine-grained governance authored purely over HTTP: an admin sets a
/// row-filter + column-mask policy via POST /admin/roles/{role}/policies and
/// the governed read path enforces both. Also proves a table-target grant is
/// grantable over HTTP (#361's documented workaround).
#[tokio::test]
async fn policy_authoring_end_to_end() { ... }
```

Test body outline (concrete assertions; adapt the seed values to the columns `governance_routes_end_to_end` seeds — if it seeds only an `id` column, extend the seeded table with a text column, mirroring its `SeedCol` usage):

1. Boot `PgFixture` + Iceberg catalog + `InProcessServingEngine` exactly as `governance_routes_end_to_end` does; `full_app(...)`.
2. Admin over HTTP: define the `Widget` model (`POST /admin/models`, properties `id: Int`, `region: Text` — matching the seeded columns), create role `reader` + user, grant `read` on type `Widget`.
3. Admin over HTTP: `POST /admin/roles/reader/policies` with body `{"action":"read","type":"Widget","row_filter":{"Compare":{"property":"region","op":"Eq","value":{"Text":"emea"}}},"mask_columns":["id"]}` → assert 201.
4. Reader `GET /objects/Widget`: assert only the `region == "emea"` rows come back and the `id` column value is the mask sentinel (grep the query-api masking code/tests for the exact sentinel — the issue report saw `***`; assert whatever the existing mask tests assert).
5. Admin over HTTP: `POST /admin/roles/reader/grants` `{"action":"read","table":{"schema":"main","name":"widget"}}` → 201; then assert through the control plane that `cp.acl().check(&reader_subject, Action::Read, &PolicyTarget::Table(...))` is `Decision::Allow`, and `GET /admin/roles/reader/grants` lists the `{"Table":...}` target.
6. Admin over HTTP: `GET /admin/roles/reader/policies` → the policy from step 3 is listed (read-your-writes over the real adapter).

- [ ] **Step 2: Run to verify red→green.** The test is new, so write it, then:

Run: `buck2 test //src/services/query-api:admin-e2e --unstable-allow-all-tests-on-re > /tmp/t6.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t6.log`
Expected: PASS (Tasks 1–5 are in). If it fails, the failure is in the new surface — fix before proceeding.

- [ ] **Step 3: Capability docs.** In `docs/system-capabilities/control-plane.md`'s administration section, extend the prose to record: fine-grained policy authoring (`POST|GET|DELETE /admin/roles/{role}/policies`, row filters in the domain serde shape, validated by `check_policy_write`), table-target grants (`GrantReq` `type` XOR `table`; unblocks pre-authorizing landing tables — the #361 workaround), derived properties + per-value constraints in `POST /admin/models`, vector index declare/list (`/admin/models/{type}/vector-indexes`), and the new `Acl::list_policies` read-back. Name PR `#NN` (placeholder — the controller patches the real number before merge). Keep it to one tight paragraph-per-surface in the file's existing voice.

- [ ] **Step 4: Register close.** In `docs/FUTURE.md`, delete the `fut-admin-governance-http-surface` entry (its `- [ ]` title line and the prose line under it). Run `bash tools/docs.sh validate` — must pass. (The links/actions halves of that entry landed earlier via `#road-api-management-crud`; this branch lands the policy half and the rest of issue #363's surface.)

- [ ] **Step 5: prek + commit.** Message: `test(query-api): policy authoring e2e + docs: admin surface gaps close (fut-admin-governance-http-surface)`.

---

## Post-plan pipeline note (controller)

After all tasks: final whole-branch review (most capable model) **including the metric gate** (`loom-complexity diff`, `loom-duplication diff`); PR from `work/fut-admin-governance-http-surface` with `Closes #363`, the model-deletion carve-out explained (pointer to `fut-ontology-type-delete`), a comment on #361; patch `#NN` placeholders in branch-owned docs only; CI via commit-status + BuildBuddy; squash-merge.
