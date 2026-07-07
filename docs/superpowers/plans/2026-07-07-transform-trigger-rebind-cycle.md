# Trigger cycle via ontology rebind — re-validate on `define_type` — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the gap where a post-define ontology **rebind** (`define_type` re-pointing a type's backing table) can create a trigger cycle between different data-triggered transform defs that no gate catches — by re-running the existing `validate_no_trigger_cycle` inside `define_type` when the binding changes, rejecting a rebind that would create a cycle.

**Architecture (design decision).** The originating spec (`2026-07-04-transform-ergonomics-design.md`, §Cycle safety) documents the residual — the run-time defense is deliberately scoped to *self*-matches, so a rebind can create a between-defs cycle that no gate catches. Two fix shapes close it: **(A)** re-validate on rebind, or **(B)** a runtime cycle-breaker at the commit seam. **We implement (A).** A is decisively more surgical: it reuses the exact helpers `define_transform` already uses (`validate_no_trigger_cycle`, `TriggerNode::resolve`, `pg_type_tables`), needs **no data-model change**, and both adapters' `define_type` already have access to the ontology binding and the transform defs. **(B) is rejected**: it would require threading a new `parent_run: Option<Uuid>` field through `TransformRun` (+ a schema migration + every run-construction site) and only suppresses firing at runtime while leaving the defs cyclic — a bigger change for a weaker outcome. A fails fast at the rebind with a clear `Validation` error.

**Mechanism.** `define_type` already computes a `binding_changed`/`changed` flag (the backing table moved). Inside that guard, resolve the current data-triggered def set against the **new** binding and run a cycle check; a cycle returns `Err(Validation)`, rejecting the rebind. Postgres validates *after* its in-tx upsert (the query sees the new binding; the `?` rolls the tx back on a cycle). Memory validates *before* the in-memory insert (no transaction — build the would-be type→table map and validate first, so a rejected rebind leaves state untouched).

**Reject only MULTI-DEF cycles — self-loops stay runtime-handled (critical).** The existing `validate_no_trigger_cycle` (`core/src/transforms.rs:325`) flags *self*-edges too (a def reading its own output — its edge loop `to.inputs.contains(out)` includes `to == from`). But the ISSUES bug is specifically the **between-defs** cycle; a single-def **self-loop** created by a rebind is *intentionally tolerated* — the commit-seam self-skip neutralizes it at runtime, and `transform_data_trigger_contract`'s self-skip leg deliberately rebinds a type onto another's table to create exactly such a self-loop and expects the rebind to **succeed**. So the rebind check must reject **only cycles spanning ≥2 distinct defs**, not self-loops. Add a `validate_no_multi_def_trigger_cycle` variant (skips self-edges) used by `define_type`; `define_transform` keeps the strict `validate_no_trigger_cycle` (its define-time self-loop rejection is existing behavior, unchanged).

**Tech Stack:** Rust, sqlx (postgres adds ONE new `query!` → **`.sqlx` regen required**; `pg_type_tables` reuses its cached query), buck2 testkit contract (both adapters).

## Global Constraints

- **No data-model / schema change** (that is option B, explicitly rejected). No new columns, no migration.
- **`.sqlx` regen required** — the postgres `define_type` validation adds ONE new `query!` (`select name, body from transforms.transform where on_input_commit` — distinct from `data_triggered_defs`'s column list, so not a cache hit) so it can decode with `de_body` skip-and-warn (poison-row-robust, consistent with item 5). It must run *in the tx* to pair with the in-tx binding. Run `bash tools/sqlx-prepare.sh` and commit the new `.sqlx/*.json`; `sqlx-cache-check` must be green. (`pg_type_tables` reuses its existing cached query.)
- **Reject, not warn** — a rebind that would create a trigger cycle returns `ControlPlaneError::Validation` (→ 400). A cycle is a real broken state (infinite ping-pong runs); warning would leave the bug in place.
- **Only re-validate when the binding actually changed** — hook inside the existing `binding_changed` (postgres) / `changed` (memory) guard, so a no-op or properties-only redefine pays nothing.
- **Postgres reuse** — promote `de_body` and `pg_type_tables` (`postgres/src/transforms.rs`) to `pub(crate)` so `ontology.rs` reuses them (mirrors how item 2 promoted `is_duplicate_object_race`). Undecodable existing bodies are skip-and-warned (consistent with `define_transform`'s scan).
- **Memory lock order** — `define_type` holds the `ontology` lock; take `transforms` *after* (never before) it, in a scoped block that drops before the insert, matching the adapter's documented ontology-first discipline.
- **Tests are `rust_test` integration targets** — the contract case lands in `transforms_contract` (run against both adapters).
- **Clippy strict** on production code — no `unwrap`/`expect`/indexing; errors via `map_err(backend)` / `?`.
- Commit messages end with the two required trailers; subjects follow Conventional Commits.

---

### Task 1: `define_type` re-validates the data-triggered DAG on a binding change

**Files:**
- Modify: `src/control-plane/postgres/src/transforms.rs` (promote `de_body` ~line 24 and `pg_type_tables` ~line 31 to `pub(crate)`)
- Modify: `src/control-plane/postgres/src/ontology.rs` (add the re-validation in `define_type`, inside the `binding_changed` guard, after the upsert ~line 43, before the lineage emission ~line 104)
- Modify: `src/control-plane/memory/src/ontology.rs` (add the re-validation in `define_type`, inside the `changed` guard, **before** `ont.types.insert` ~line 35)
- Test: `src/control-plane/testkit/src/lib.rs` (a rebind-cycle case in `transforms_contract`)

**Interfaces:**
- Consumes: `validate_no_trigger_cycle(&[TriggerNode]) -> Result<()>` and `TriggerNode::resolve(&TransformName, &TransformBody, &HashMap<String, TableRef>)` (core, public); postgres `de_body`/`pg_type_tables` (promoted `pub(crate)`); memory `self.transforms`/`self.ontology`.
- Produces: no new surface — `define_type` now rejects a binding change that creates a trigger cycle.

- [ ] **Step 1: Write the failing contract case**

In `src/control-plane/testkit/src/lib.rs`, in `transforms_contract` (after the reconcile block added by item 6, near the end of the fn), add. It builds three typed types on distinct tables and two typed data-triggered defs `X: A→B` and `Y: B→C` (a linear chain — no cycle), then rebinds `C` onto `A`'s table so `Y` now outputs where `X` reads, closing `X→Y→X`:

```rust
    // --- rebind cycle: a define_type that re-points a binding into a trigger
    // cycle among data-triggered defs is rejected (not silently allowed) ---
    // `tn` is a closure scoped to `ontology_contract`, NOT visible here, so
    // define a local one (TypeName is module-level imported).
    let tn = |s: &str| TypeName(s.to_string());
    let ta = tref("main", "rc_a");
    let tb = tref("main", "rc_b");
    let tc = tref("main", "rc_c");
    let mk_type = |name: &str, table: &TableRef| ObjectType {
        name: tn(name),
        table: table.clone(),
        properties: vec![PropertyDef {
            name: "id".into(),
            ty: "Long".into(),
            required: true,
            constraints: control_plane_core::PropertyConstraints::default(),
        }],
        derived: vec![],
        identity: Some("id".into()),
    };
    cp.define_type(mk_type("RcA", &ta)).await.unwrap();
    cp.define_type(mk_type("RcB", &tb)).await.unwrap();
    cp.define_type(mk_type("RcC", &tc)).await.unwrap();

    let typed_dt = |name: &str, input: &str, output: &str| TransformDef {
        name: TransformName(name.into()),
        body: TransformBody::Typed {
            inputs: vec![input.into()],
            output: output.into(),
            sql: format!("select * from {input}"),
            output_mode: OutputMode::Append,
        },
        schedule: None,
        on_input_commit: true,
    };
    // X: RcA -> RcB, Y: RcB -> RcC — a linear chain, no cycle at define time.
    cp.define_transform(typed_dt("rc-x", "RcA", "RcB")).await.expect("define X");
    cp.define_transform(typed_dt("rc-y", "RcB", "RcC")).await.expect("define Y");

    // Rebind RcC onto RcA's table: Y now outputs where X reads → X->Y->X cycle.
    // The rebind MUST be rejected.
    let rebind = cp.define_type(mk_type("RcC", &ta)).await;
    assert!(
        matches!(rebind, Err(control_plane_core::ControlPlaneError::Validation(_))),
        "a rebind that creates a trigger cycle is rejected: {rebind:?}"
    );
    // ... and the rejected rebind left RcC's binding untouched (no partial mutation):
    // a subsequent same-table redefine of RcC (its ORIGINAL table) is a no-op success.
    cp.define_type(mk_type("RcC", &tc)).await.expect("RcC still bound to its original table");
```

(`tn`, `tref`, `ObjectType`, `PropertyDef`, `TransformBody`, `TransformDef`, `TransformName`, `OutputMode` are all already imported/used in this contract.)

- [ ] **Step 2: Run it to verify it fails (RED)**

Run: `buck2 test --console none //src/control-plane/testkit/... //src/control-plane/postgres/... //src/control-plane/memory/...`
Expected: the new case FAILS on BOTH adapters — the rebind `define_type` currently returns `Ok(())` (no cycle check), so the `matches!(rebind, Err(Validation(_)))` assertion fails.

- [ ] **Step 2.5: Add `validate_no_multi_def_trigger_cycle` to core**

In `src/control-plane/core/src/transforms.rs`, add a self-loop-tolerant variant alongside `validate_no_trigger_cycle` (~line 325). Extract the shared body into a private impl parameterized by whether to skip self-edges, so `validate_no_trigger_cycle` keeps its exact current (strict) behavior and the new fn ignores single-def self-loops:

```rust
/// Like [`validate_no_trigger_cycle`] but TOLERANT of single-def self-cycles (a
/// def reading its own output), which the commit-seam self-skip already
/// neutralizes at runtime. Rejects only cycles spanning two or more distinct
/// defs — the between-defs case a post-definition ontology rebind can silently
/// create. Used by `define_type` when a binding change re-points trigger edges.
pub fn validate_no_multi_def_trigger_cycle(nodes: &[TriggerNode]) -> Result<()> {
    validate_trigger_cycle_impl(nodes, true)
}

pub fn validate_no_trigger_cycle(nodes: &[TriggerNode]) -> Result<()> {
    validate_trigger_cycle_impl(nodes, false)
}

fn validate_trigger_cycle_impl(nodes: &[TriggerNode], skip_self_edges: bool) -> Result<()> {
    // ... existing body, EXCEPT the successor filter (line ~331) gains the guard:
    //   for to in nodes.iter().filter(|to| {
    //       (!skip_self_edges || to.name != from.name) && to.inputs.contains(out)
    //   }) { ... }
}
```

Refactor the existing `validate_no_trigger_cycle` body into `validate_trigger_cycle_impl` (moving the Kahn's-algorithm code verbatim, adding only the `skip_self_edges` guard to the successor filter). Re-export `validate_no_multi_def_trigger_cycle` from `core/src/lib.rs` next to `validate_no_trigger_cycle`.

- [ ] **Step 3: Promote the postgres helpers to `pub(crate)`**

In `src/control-plane/postgres/src/transforms.rs`, change the visibility of the two helpers `ontology.rs` will reuse:

```rust
pub(crate) fn de_body(v: serde_json::Value) -> Result<TransformBody> {
```
```rust
pub(crate) async fn pg_type_tables<'e, E: sqlx::PgExecutor<'e>>(
```

(Bodies unchanged.)

- [ ] **Step 4: Postgres `define_type` re-validation**

In `src/control-plane/postgres/src/ontology.rs`, add the needed imports at the top (near the existing `use crate::backend;`):

```rust
use crate::transforms::{de_body, pg_type_tables};
use control_plane_core::{TransformBody, TransformName, TriggerNode, validate_no_multi_def_trigger_cycle};
```

(If any are already imported, fold in rather than duplicate.)

Then inside `define_type`, within the existing `if binding_changed { … }` region — after the `insert … on conflict … do update` upsert has run in `tx` (so a query now sees the NEW binding), and before the lineage-event emission — add:

```rust
        // A binding change can create a trigger cycle among data-triggered defs
        // whose typed I/O resolves through this type. Re-validate the DAG against
        // the just-applied binding and reject a rebind that would close a cycle.
        let dt = sqlx::query!(
            "select name, body from transforms.transform where on_input_commit",
        )
        .fetch_all(&mut *tx)
        .await
        .map_err(backend)?;
        let mut bodies: Vec<(TransformName, TransformBody)> = Vec::with_capacity(dt.len());
        for r in dt {
            match de_body(r.body) {
                Ok(b) => bodies.push((TransformName(r.name), b)),
                Err(e) => tracing::warn!(transform = %r.name, error = %e,
                    "rebind cycle scan: undecodable body skipped"),
            }
        }
        let types = pg_type_tables(&mut *tx, &bodies).await?;
        let nodes: Vec<TriggerNode> =
            bodies.iter().map(|(n, b)| TriggerNode::resolve(n, b, &types)).collect();
        validate_no_multi_def_trigger_cycle(&nodes)?;
```

On a cycle, `validate_no_trigger_cycle` returns `Err(Validation(...))`; the `?` propagates it, `tx` is dropped without commit, and the upsert rolls back — so the rejected rebind leaves the stored binding unchanged.

- [ ] **Step 5: Memory `define_type` re-validation**

In `src/control-plane/memory/src/ontology.rs`, add imports as needed. **Note: `TableRef` is already imported at the top of this file — do NOT re-import it (E0252).** Add only the names not already in scope (fold into the existing `use control_plane_core::{…}` block if present):

```rust
use control_plane_core::{TransformBody, TransformName, TriggerNode, validate_no_multi_def_trigger_cycle};
```

Then in `define_type`, inside the existing `if changed { … }` (or before the `ont.types.insert`), validate the would-be DAG **before** mutating `ont`:

```rust
        // Validate the data-triggered DAG against the WOULD-BE binding before
        // committing the rebind, so a cycle-creating rebind leaves state untouched.
        let mut type_tables: std::collections::HashMap<String, TableRef> = ont
            .types
            .iter()
            .map(|(n, t)| (n.clone(), t.table.clone()))
            .collect();
        type_tables.insert(ty.name.0.clone(), ty.table.clone()); // the new binding wins
        let dt: Vec<(TransformName, TransformBody)> = {
            let t = self.transforms.lock();
            t.defs
                .values()
                .filter(|d| d.on_input_commit)
                .map(|d| (d.name.clone(), d.body.clone()))
                .collect()
        }; // transforms lock dropped here (ontology still held — ontology-first order)
        let nodes: Vec<TriggerNode> =
            dt.iter().map(|(n, b)| TriggerNode::resolve(n, b, &type_tables)).collect();
        validate_no_multi_def_trigger_cycle(&nodes)?;
```

This runs before `ont.types.insert(...)`, so a rejected rebind returns `Err` with `ont` unmutated.

- [ ] **Step 6: Regenerate `.sqlx`, then run the contract (GREEN)**

The new `select name, body from transforms.transform where on_input_commit` `query!` needs a cache entry. Run `bash tools/sqlx-prepare.sh` (boots the pinned postgres, applies migrations, `cargo sqlx prepare`) and confirm a new `.sqlx/query-*.json` appeared.
Run: `buck2 test --console none //src/control-plane/testkit/... //src/control-plane/postgres/... //src/control-plane/memory/...`
Expected: `Pass N. Fail 0` on BOTH adapters — the rebind is rejected, the original-table redefine still succeeds, and `sqlx-cache-check` is green with the new entry.

- [ ] **Step 7: Commit**

```bash
git add src/control-plane/postgres/src/transforms.rs \
        src/control-plane/postgres/src/ontology.rs \
        src/control-plane/memory/src/ontology.rs \
        src/control-plane/testkit/src/lib.rs \
        src/control-plane/postgres/.sqlx
git commit -m "fix(ontology): re-validate the trigger DAG on define_type rebind, rejecting cycles"
```

(Commit body carries the two required trailers.)
