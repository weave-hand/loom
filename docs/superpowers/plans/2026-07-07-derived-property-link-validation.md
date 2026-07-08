# Derived-property link & column validation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close two write-side defects where a derived property (aggregate-over-link) silently corrupts reads: (1) deleting a link a derived property names strands the column — add a **409 referrer guard**; (2) `define_type` never checks the aggregate column exists/is-numeric — add **define-time validation**.

**Architecture:** Both surfaces are ontology write-path additions with memory+postgres parity (pinned by the testkit contract). Surface 1: a new `derived_properties_referencing` trait read, called inside `delete_link` — non-empty ⇒ `Conflict` (→409). Surface 2: a new core `validate_derived_columns` free fn, called inside `define_type` beside `validate_constraints` — a resolving link whose agg column is missing/non-numeric ⇒ `Validation` (→400); an *unresolvable* link/target is **skipped** (best-effort — the read path keeps guarding it). No FK migration, no read-path change.

**Tech Stack:** Rust, sqlx (compile-time `query!` + committed `.sqlx`), axum admin routes, buck2 testkit contract + `rust_test`.

## Global Constraints

- **Memory/postgres parity is required** — every behavior is pinned by the testkit ontology contract, run against both adapters.
- **No FK schema migration.** `ontology.derived_property.link_name` stays FK-less. No read-path change (`handler.rs:317-367` omit/deny guards stay exactly as-is).
- **`.sqlx` regen** for each new postgres `query!` — run `bash tools/sqlx-prepare.sh` and commit the new `.sqlx/*.json`; `//src/control-plane/postgres:sqlx-cache-check` must be green.
- **Best-effort Surface-2 validation**: only a *resolvable* link+target is validated (agg column must exist; `Sum`/`Avg` need numeric via `column_applicable`/`resolve_logical`). An undefined link OR a not-yet-defined target ⇒ **skip** (a type-with-derived can still be defined before its link/target exist). Self-links resolve against the type being defined.
- **Error→status**: `status_for` already maps `Conflict→409`, `Validation→400` (`runtime/src/auth.rs:43-46`). `delete_link_route` needs a `Conflict` branch that returns a JSON body (not a bare status); `define_model` already funnels `Validation` through `status_for`.
- Clippy strict on production (no `unwrap`/`expect`/indexing/`panic`; `?`/`map_err`). Tests are `rust_test`. Commit trailers + Conventional Commits.

---

### Task 1: Referrer guard — `delete_link` 409 while a derived property names the link

**Files:**
- Modify: `src/control-plane/core/src/ontology.rs` (add `derived_properties_referencing` to the `Ontology` trait, near `delete_link`)
- Modify: `src/control-plane/postgres/src/ontology.rs` (impl the trait method + guard in `delete_link`; new `.sqlx`)
- Modify: `src/control-plane/memory/src/ontology.rs` (impl the trait method + guard in `delete_link`)
- Modify: `src/services/runtime/src/admin.rs` (`delete_link_route` Conflict→409 JSON + `#[utoipa::path]` doc)
- Test: `src/control-plane/testkit/src/lib.rs` (contract cases near the `delete_link` block ~1483)
- Test: `src/services/runtime/tests/admin_routes.rs` (409 HTTP case)

**Interfaces:**
- Produces: `async fn derived_properties_referencing(&self, from: &TypeName, name: &str) -> Result<Vec<String>>` — names of derived properties on type `from` whose `.link == name`; `[]` if none.
- Consumes: `ControlPlaneError::Conflict(String)` (`core/src/error.rs:18`); `status_for` (`runtime/src/auth.rs:43`).

- [ ] **Step 1: Add the trait method + failing testkit contract cases**

In `src/control-plane/core/src/ontology.rs`, add to the `Ontology` trait near `delete_link` (~782):

```rust
    /// Names of derived properties on `from` whose link is `name`. Empty if none.
    /// Only derived properties on `from` can be stranded by deleting link
    /// `(from, name)` (a derived property resolves its link among its own type's
    /// outbound links).
    async fn derived_properties_referencing(&self, from: &TypeName, name: &str)
        -> Result<Vec<String>>;
```

In `src/control-plane/testkit/src/lib.rs`, add a referrer-guard case **immediately AFTER the existing `delete_link` contract block** (which ends ~line 1520 with the "re-defined link listed again" assertion, before the `delete_action` block). Insert it there — NOT before — because the existing block deletes and re-defines the `customer` link and its opening `.find(|l| l.name == "customer").expect(...)` would panic if `customer` were already deleted. At this insertion point the `customer` link (`Order → Customer`) is present again, so the case is self-contained and may leave it deleted at the end (the next block, `delete_action`, does not need it):

```rust
    // --- referrer guard: a derived property naming a link blocks its deletion ---
    // (Runs after the delete_link block re-defined `customer`, so it is present here.)
    let order_ty = o.get_type(&tn("Order")).await.expect("Order exists");
    let mut order_with_derived = order_ty.clone();
    order_with_derived.derived = vec![DerivedPropertyDef {
        name: "customerCount".into(),
        ty: "Long".into(),
        link: "customer".into(),
        agg: Aggregation::Count, // Count: no agg column, so Surface-2 validation is a no-op here
    }];
    o.define_type(order_with_derived).await.expect("Order with derived");

    // derived_properties_referencing reports the referrer; [] for an unreferenced link.
    assert_eq!(
        o.derived_properties_referencing(&tn("Order"), "customer").await.unwrap(),
        vec!["customerCount".to_string()],
    );
    assert!(o.derived_properties_referencing(&tn("Order"), "no_such_link").await.unwrap().is_empty());

    // delete_link is BLOCKED with Conflict; the link is untouched.
    let blocked = o.delete_link(&tn("Order"), "customer").await;
    assert!(matches!(blocked, Err(control_plane_core::ControlPlaneError::Conflict(_))), "referenced link 409s: {blocked:?}");
    assert!(
        o.links(&tn("Order"), PageReq::unbounded()).await.unwrap()
            .items.iter().any(|l| l.name == "customer"),
        "blocked delete left the link in place",
    );

    // Redefine Order WITHOUT the derived property → delete now succeeds, idempotent again.
    o.define_type(order_ty.clone()).await.expect("Order without derived");
    o.delete_link(&tn("Order"), "customer").await.expect("delete after derived removed");
    o.delete_link(&tn("Order"), "customer").await.expect("idempotent re-delete");
```

(Read the full `delete_link` block first to confirm the exact insertion line after its final assertion.)

- [ ] **Step 2: Run it to verify it fails (RED)**

Run: `buck2 test --console none //src/control-plane/testkit/... //src/control-plane/memory/... //src/control-plane/postgres/...`
Expected: FAIL to compile first (`derived_properties_referencing` unimplemented on both adapters) — implement in Steps 3-4.

- [ ] **Step 3: Postgres impl + guard + `.sqlx`**

In `src/control-plane/postgres/src/ontology.rs`, add the trait method:

```rust
    async fn derived_properties_referencing(&self, from: &TypeName, name: &str) -> Result<Vec<String>> {
        let rows = sqlx::query_scalar!(
            "select name from ontology.derived_property where type_name = $1 and link_name = $2 order by ordinal",
            from.0,
            name,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        Ok(rows)
    }
```

Then guard `delete_link` (~153) — do the referrer read and the delete **in one transaction** so no derived property can be added between the check and the delete (TOCTOU-free):

```rust
    async fn delete_link(&self, from: &TypeName, name: &str) -> Result<()> {
        let mut tx = self.pool.begin().await.map_err(backend)?;
        let refs = sqlx::query_scalar!(
            "select name from ontology.derived_property where type_name = $1 and link_name = $2 order by ordinal",
            from.0, name,
        ).fetch_all(&mut *tx).await.map_err(backend)?;
        if !refs.is_empty() {
            return Err(ControlPlaneError::Conflict(format!(
                "link `{name}` is referenced by derived properties: {}", refs.join(", ")
            )));
        }
        sqlx::query!("delete from ontology.link where from_type = $1 and name = $2", from.0, name)
            .execute(&mut *tx).await.map_err(backend)?;
        tx.commit().await.map_err(backend)?;
        Ok(())
    }
```

(The `derived_properties_referencing` trait method above reads on `self.pool` — that's fine for the standalone read; `delete_link` uses its own tx-scoped copy of the same query for the atomic guard. The Conflict early-return drops `tx` without commit.) Run `bash tools/sqlx-prepare.sh` and confirm a new `.sqlx/query-*.json` for the `select name … derived_property …` query; `git add src/control-plane/postgres/.sqlx`.

- [ ] **Step 4: Memory impl + guard**

In `src/control-plane/memory/src/ontology.rs`, add the trait method and guard `delete_link` (~80). The referrer read filters the owning type's `derived`:

```rust
    async fn derived_properties_referencing(&self, from: &TypeName, name: &str) -> Result<Vec<String>> {
        let ont = self.ontology.lock();
        let names = ont.types.get(&from.0)
            .map(|t| t.derived.iter().filter(|d| d.link == name).map(|d| d.name.clone()).collect())
            .unwrap_or_default();
        Ok(names)
    }

    async fn delete_link(&self, from: &TypeName, name: &str) -> Result<()> {
        let mut ont = self.ontology.lock(); // ONE lock: read refs + retain atomically (no TOCTOU)
        let refs: Vec<String> = ont.types.get(&from.0)
            .map(|t| t.derived.iter().filter(|d| d.link == name).map(|d| d.name.clone()).collect())
            .unwrap_or_default();
        if !refs.is_empty() {
            return Err(ControlPlaneError::Conflict(format!(
                "link `{name}` is referenced by derived properties: {}", refs.join(", ")
            )));
        }
        ont.links.retain(|l| !(l.from == *from && l.name == name));
        Ok(())
    }
```

(A SINGLE `ont` lock held across both the read and the retain — a `parking_lot::Mutex` is non-reentrant, so do NOT take a second `self.ontology.lock()`; the one guard covers read + mutate with no race window. `ont` must be `mut` for `retain`.)

- [ ] **Step 5: Verify the contract passes on both adapters (GREEN)**

Run: `buck2 test --console none //src/control-plane/testkit/... //src/control-plane/memory/... //src/control-plane/postgres/...`
Expected: `Pass N. Fail 0` on both adapters, incl. `sqlx-cache-check`.

- [ ] **Step 6: Admin route 409 JSON + doc + HTTP test**

In `src/services/runtime/src/admin.rs`, `delete_link_route` (~806): add a `Conflict` branch returning 409 with a JSON body naming the blockers (mirror the spec's shape):

```rust
        Err(control_plane_core::ControlPlaneError::Conflict(msg)) => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": msg })),
        ).into_response(),
        Err(e) => status_for(&e).into_response(),
```

Add `(status = 409, description = "Link is referenced by a derived property; delete blocked", ...)` to the route's `#[utoipa::path]` responses (mirror an existing route's response doc style).

In `src/services/runtime/tests/admin_routes.rs`, add a test (using the file's `app`/`send`/`seed_admin_session`/`post_json` harness; add `use control_plane_core::Ontology;` at the top — the file lacks it — to call `cp.define_type`/`define_link`/`define_role` directly). Seed on the `Arc<MemoryControlPlane>` `cp` before building the router: a type (e.g. `Order`) with a link (`customer`) and a derived property naming that link, plus the target type. Then `DELETE /admin/links/{from}/{name}` for the referenced link ⇒ assert `StatusCode::CONFLICT` and the JSON body lists the derived property; a DELETE for an *unreferenced* link ⇒ 200 as today. (Use a `DELETE`-method request builder mirroring the file's `post_json` helper — add a small `delete_req(uri, token)` if the file has none.)

- [ ] **Step 7: Full sweep + commit**

Run: `buck2 test --console none //src/control-plane/... //src/services/runtime/...`
Expected: green. Then:

```bash
git add src/control-plane/core/src/ontology.rs src/control-plane/postgres/ \
        src/control-plane/memory/src/ontology.rs src/services/runtime/ \
        src/control-plane/testkit/src/lib.rs
git commit -m "feat(ontology): 409 referrer guard blocks deleting a link a derived property names"
```

---

### Task 2: Define-time validation of a derived property's aggregate column

**Files:**
- Modify: `src/control-plane/core/src/ontology.rs` (add `validate_derived_columns` free fn)
- Modify: `src/control-plane/core/src/lib.rs` (re-export it from the `pub use ontology::{…}` block ~62-67)
- Create: `src/control-plane/core/tests/derived_columns.rs` + a `rust_test` target in `src/control-plane/core/BUCK` (mirror the `:constraints` target)
- Modify: `src/control-plane/postgres/src/ontology.rs` (tx-scoped resolver + call in `define_type`; `.sqlx` regen)
- Modify: `src/control-plane/memory/src/ontology.rs` (resolver from the held `ont` lock + call in `define_type`)
- Test: `src/control-plane/testkit/src/lib.rs` (contract cases after the derived-property block ~1292)
- Test: `src/services/runtime/tests/admin_routes.rs` (400 HTTP case; add `use control_plane_core::Ontology;` — the file lacks it — for direct `cp.define_type`/`define_link` seeding)

**Interfaces:**
- Produces: `pub fn validate_derived_columns(derived: &[DerivedPropertyDef], resolve_target: impl Fn(&str) -> Option<&ObjectType>) -> Result<()>` — `resolve_target(link_name)` returns the link's target `ObjectType` if resolvable, else `None` (⇒ that derived property is skipped).
- Consumes: `Aggregation::column()` (`ontology.rs`), `Aggregation::column_applicable(Option<BaseType>)`, `resolve_logical(&str) -> Option<BaseType>` (`logical_type.rs:144`), `BaseType::is_numeric`.

- [ ] **Step 1: Add `validate_derived_columns` to core**

In `src/control-plane/core/src/ontology.rs` (near `validate_constraints`), add:

```rust
/// Validate each derived property's aggregate column against its link's target
/// type, when the link+target are resolvable. `resolve_target(link_name)` returns
/// the target `ObjectType` or `None` — an unresolvable link/target is SKIPPED
/// (best-effort; the read path keeps guarding a missing link). A resolvable link
/// whose `agg` column is absent, or is present but wrong-typed for the aggregation
/// (`Sum`/`Avg` need numeric, `Min`/`Max` need ordered), is a `Validation` error.
pub fn validate_derived_columns(
    derived: &[DerivedPropertyDef],
    resolve_target: impl Fn(&str) -> Option<&ObjectType>,
) -> Result<()> {
    for d in derived {
        let Some(col) = d.agg.column() else { continue }; // Count: no column
        let Some(target) = resolve_target(&d.link) else { continue }; // unresolvable: skip (deferred)
        let Some(prop) = target.properties.iter().find(|p| p.name == col) else {
            return Err(ControlPlaneError::Validation(format!(
                "derived `{}`: link `{}` target `{}` has no column `{col}`",
                d.name, d.link, target.name.0
            )));
        };
        if !d.agg.column_applicable(crate::logical_type::resolve_logical(&prop.ty)) {
            return Err(ControlPlaneError::Validation(format!(
                "derived `{}`: aggregation over column `{col}` (type `{}`) is not applicable \
                 (Sum/Avg need a numeric column; Min/Max need an ordered column)",
                d.name, prop.ty
            )));
        }
    }
    Ok(())
}
```

**Re-export**: `validate_derived_columns` lives in `ontology.rs`, so add it to the existing `pub use ontology::{…}` re-export block in `core/src/lib.rs` (~lines 62-67) — NOT next to `validate_constraints` (that one is re-exported from `constraints.rs` via a separate block ~lines 42-45).

**Core `rust_test`**: add a sibling `tests/<name>.rs` + a `rust_test` target in `src/control-plane/core/BUCK` (mirror the existing `:constraints` test target, ~`core/BUCK:23-31`). Cover: `Count` skips (no column); missing column → `Validation`; `Sum` over non-numeric → `Validation`; `Min` over ordered → Ok; unresolvable link (`resolve_target` returns `None`) → Ok; valid `Sum` over numeric → Ok. (Build the `resolve_target` closure in-test from a small `HashMap<String, ObjectType>`.)

- [ ] **Step 2: Failing testkit contract cases**

In `src/control-plane/testkit/src/lib.rs`, extend the derived-property block (~1292). **Verified: that block defines only `Account` (id: Long) with derived props over a link named `"transactions"` — but NO `Transaction` type, NO `transactions` link, and NO `prop_*` helper exist.** So this task's resolvable-case tests must create them. `tn` (a `|s: &str| TypeName(s.into())` closure) and `tref` exist and are in scope. Add, AFTER the existing `Account`-with-derived assertions:

```rust
    // Concrete target + link so Surface-2 validation resolves (a local prop helper —
    // no prop_* helper exists in this contract).
    let prop = |name: &str, ty: &str| PropertyDef {
        name: name.into(), ty: ty.into(), required: false,
        constraints: control_plane_core::PropertyConstraints::default(),
    };
    o.define_type(ObjectType {
        name: tn("Transaction"), table: tref("main", "txn"), identity: None,
        properties: vec![prop("id", "Long"), prop("amount", "Double"), prop("note", "String")],
        derived: vec![],
    }).await.expect("define Transaction");
    // define_link requires both endpoints to pre-exist; Account is defined above.
    o.define_link(LinkDef {
        name: "transactions".into(), from: tn("Account"), to: tn("Transaction"),
        cardinality: Cardinality::Many,
        backing: LinkBacking::ForeignKey { from_column: "id".into(), to_column: "account_id".into() },
    }).await.expect("define transactions link");

    // resolvable link + MISSING agg column → Validation.
    let missing_col = o.define_type(ObjectType {
        name: tn("Account"), table: tref("main", "account"), identity: None,
        properties: vec![prop("id", "Long")],
        derived: vec![DerivedPropertyDef { name: "x".into(), ty: "Double".into(), link: "transactions".into(), agg: Aggregation::Sum("nope".into()) }],
    }).await;
    assert!(matches!(missing_col, Err(control_plane_core::ControlPlaneError::Validation(_))), "missing agg column → Validation: {missing_col:?}");

    // resolvable link + NON-NUMERIC agg column (note: String) under Sum → Validation.
    let non_numeric = o.define_type(ObjectType {
        name: tn("Account"), table: tref("main", "account"), identity: None,
        properties: vec![prop("id", "Long")],
        derived: vec![DerivedPropertyDef { name: "x".into(), ty: "String".into(), link: "transactions".into(), agg: Aggregation::Sum("note".into()) }],
    }).await;
    assert!(matches!(non_numeric, Err(control_plane_core::ControlPlaneError::Validation(_))), "Sum over non-numeric → Validation: {non_numeric:?}");

    // valid Sum over a numeric column (amount: Double) → Ok.
    o.define_type(ObjectType {
        name: tn("Account"), table: tref("main", "account"), identity: None,
        properties: vec![prop("id", "Long")],
        derived: vec![DerivedPropertyDef { name: "balance".into(), ty: "Double".into(), link: "transactions".into(), agg: Aggregation::Sum("amount".into()) }],
    }).await.expect("valid Sum over numeric column");

    // unresolvable link (target link undefined) → Ok (deferred; read path still omits).
    o.define_type(ObjectType {
        name: tn("Account"), table: tref("main", "account"), identity: None,
        properties: vec![prop("id", "Long")],
        derived: vec![DerivedPropertyDef { name: "y".into(), ty: "Double".into(), link: "ghostlink".into(), agg: Aggregation::Sum("whatever".into()) }],
    }).await.expect("unresolvable link defers validation");
```

(`LinkDef`/`Cardinality`/`LinkBacking`/`DerivedPropertyDef`/`Aggregation`/`PropertyDef`/`ObjectType` are already imported in this contract's `use control_plane_core::{…}` block — confirm and add any missing name. The `Transaction`/`transactions` definitions must precede all four resolvable-case asserts. Note: adding `Transaction` does not disturb the file's one exact-set `list_types` assertion, which runs far earlier (~line 722).)

- [ ] **Step 3: Run it to verify it fails (RED)**

Run: `buck2 test --console none //src/control-plane/testkit/... //src/control-plane/memory/... //src/control-plane/postgres/...`
Expected: FAIL — `define_type` doesn't validate agg columns yet, so `missing_col`/`non_numeric` return `Ok` (assert fails) on both adapters.

- [ ] **Step 4: Postgres — call the validator in `define_type`**

In `src/control-plane/postgres/src/ontology.rs` `define_type`, after `validate_constraints` (~17) and where the tx is open, build the target resolver **entirely tx-scoped** (do NOT call `self.get_type`, which checks out a second pool connection while the tx holds one — under a small pool that risks starvation, and reads-inside-a-tx use the tx executor by convention, cf. `object_type_exists(&mut *tx, …)`). The validator only reads each target's `name` + `properties`, so build a minimal `ObjectType` per target from a tx-scoped property query (mirror `get_type`'s property fetch — the property table is `ontology.property (name, ty, required, constraints)`):

```rust
        // Resolve each outbound link's target type (name + properties only) for
        // derived agg-column validation — all reads on the open tx.
        let link_rows = sqlx::query!(
            "select name, to_type from ontology.link where from_type = $1", ty.name.0,
        ).fetch_all(&mut *tx).await.map_err(backend)?;
        let mut targets: std::collections::HashMap<String, ObjectType> = std::collections::HashMap::new();
        for l in link_rows {
            let target = if l.to_type == ty.name.0 {
                ty.clone() // self-link: the type being defined (not yet committed)
            } else {
                let props = sqlx::query!(
                    "select name, ty from ontology.property where type_name = $1 order by ordinal",
                    l.to_type,
                ).fetch_all(&mut *tx).await.map_err(backend)?;
                if props.is_empty() {
                    // target type not defined yet (no property rows) → skip (deferred)
                    continue;
                }
                ObjectType {
                    name: TypeName(l.to_type.clone()),
                    properties: props.into_iter().map(|p| PropertyDef {
                        name: p.name, ty: p.ty, required: false,
                        constraints: control_plane_core::PropertyConstraints::default(),
                    }).collect(),
                    // The validator reads only name + properties; these are filler.
                    derived: vec![], table: ty.table.clone(), identity: None,
                }
            };
            targets.insert(l.name, target);
        }
        control_plane_core::validate_derived_columns(&ty.derived, |ln| targets.get(ln))?;
```

Place this before `tx.commit()` so a `Validation` error rolls the tx back, exactly like the constraint gate. Both new queries (`select name, to_type from ontology.link …` and `select name, ty from ontology.property … order by ordinal`) are likely new shapes ⇒ **`.sqlx` regen** (run `sqlx-prepare.sh`, `git add` the cache). Confirm the exact `ontology.property` column names against `get_type`'s own property query before finalizing.

- [ ] **Step 5: Memory — call the validator in `define_type`**

In `src/control-plane/memory/src/ontology.rs` `define_type`, after locking `ont` and BEFORE `ont.types.insert` (so a rejected define leaves state unmutated), build the resolver from `ont` (self-links → `ty`; others → `ont.types`) and validate:

```rust
        let mut targets: std::collections::HashMap<String, ObjectType> = std::collections::HashMap::new();
        for l in ont.links.iter().filter(|l| l.from == ty.name) {
            let target = if l.to == ty.name { ty.clone() }
                else if let Some(t) = ont.types.get(&l.to.0) { t.clone() }
                else { continue };
            targets.insert(l.name.clone(), target);
        }
        control_plane_core::validate_derived_columns(&ty.derived, |ln| targets.get(ln))?;
```

(Runs while `ont` is held, before the insert — a `Validation` `?` returns with `ont` unmutated. `ty.clone()`/`t.clone()` keep the closure borrowing from the owned `targets` map, not `ont`, avoiding a borrow conflict with the later `ont.types.insert`.)

- [ ] **Step 6: Verify the contract passes on both adapters (GREEN)**

Run: `buck2 test --console none //src/control-plane/testkit/... //src/control-plane/memory/... //src/control-plane/postgres/...`
Expected: `Pass N. Fail 0` on both adapters — missing/non-numeric agg columns rejected, valid + deferred cases Ok, `sqlx-cache-check` green.

- [ ] **Step 7: Admin 400 HTTP test + doc note**

In `src/services/runtime/tests/admin_routes.rs`, add a test: `POST /admin/models` with a type whose derived property's agg column is missing (against a defined link+target) ⇒ assert `StatusCode::BAD_REQUEST` (mapped from `Validation` via `status_for`). Seed the target type + link on `cp` first so the link resolves.

In `src/services/runtime/src/admin.rs`, update the `DefineModelReq.derived` doc comment (~513-514) — it currently says "Link existence is not validated here (matches `define_type`; see iss-delete-link-derived-dangle)"; change it to note that a *resolvable* link's agg column is now validated at define time (missing/non-numeric ⇒ 400). If `define_model`'s `#[utoipa::path]` already documents 400 for validation, no response-doc change is needed beyond this note.

- [ ] **Step 8: Full sweep + commit**

Run: `buck2 test --console none //src/control-plane/... //src/services/runtime/...`
Expected: green. Then:

```bash
git add src/control-plane/core/src/ontology.rs src/control-plane/core/ \
        src/control-plane/postgres/src/ontology.rs src/control-plane/memory/src/ontology.rs \
        src/services/runtime/ src/control-plane/testkit/src/lib.rs
git commit -m "feat(ontology): validate a derived property's aggregate column at define time"
```
