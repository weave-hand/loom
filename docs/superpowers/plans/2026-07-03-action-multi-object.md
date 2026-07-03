# Custom-logic actions slice 3 — multi-object / multi-step actions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One named action mutates several objects in one atomic transaction, where later steps reference earlier steps' just-minted identities (`createOrderWithLines` inserts an `Order` then N `LineItem`s whose `orderId` = the created order's id) — commit all or none.

**Architecture:** `ActionDef` becomes an ordered `steps: Vec<ActionStep>`; a single-step action is byte-compatible with today (serde flat form + it dispatches to today's inline-tiered write path unchanged). Multi-step actions resolve + govern every step **before** any write, then stage all steps' Parquet files (append for Insert, replace for Update/Delete) + one lineage event through the existing multi-table `IcebergTx` (`begin_table()`), committing once. Cross-step references are a first-class, define-time-validated reference resolved from a step-scoped binding environment.

**Tech Stack:** Rust (edition 2024), buck2, DataFusion + Iceberg + Postgres control plane, Arrow 58, sqlx compile-time `query!`, tonic/prost wire (`engine_control.proto`).

## Global Constraints

- **Tests are `rust_test`/`loom_fixture_test` integration targets only — never inline `#[cfg(test)]`.** New fixture (Postgres/MinIO) tests use the `loom_fixture_test` macro; pure-logic tests use the `rust_test` wrapper from `//src:loom_test.bzl`. The `no-inline-tests` prek hook fails on any first-party `src/**.rs` `#[test]`.
- **Strict clippy** (`pedantic` + `restriction`): production lib/bin code must not `unwrap`/`expect`/index-slice/`panic`/`todo`/`dbg`. Use `?`, `.ok_or_else`, `.get(..)`. Test code is exempted for panic-safety lints via the wrappers. Silence locally only with `#[expect(lint, reason = "…")]`.
- **Single-step actions are byte-compatible with today.** A one-step, bind-less action (a) serializes to the exact legacy flat JSON `{name,target,kind,parameters,assignments}` and (b) at invocation dispatches to the existing `run_insert`/`run_mutate` path — zero behavior change, inline tiering preserved.
- **One Tx, all-or-nothing.** All steps of a multi-step action commit in one `IcebergTx` (one snapshot, one Postgres transaction). Per-step governance (coarse `Action::Write`, fine-grained write/mutate ACL, model constraints) runs for **every** step **before** the single commit; any failure means nothing commits.
- **One `RunId` per action; `outputs` lists every step's target `DatasetRef`.**
- **Define-time rejects** forward / self / unbound cross-step references, and a reference to a non-property of the bound step's target.
- **`.sqlx` cache must be regenerated and committed** (`tools/sqlx-prepare.sh`) whenever the postgres SQL changes; the `//src/control-plane/postgres:sqlx-cache-check` test enforces freshness in the normal `buck2 test //src/...` sweep.
- **Never pipe `buck2 test`/`buck2 bxl` through `tail`/`head`** — redirect to a file and grep it. On a root/cloud host, test runs go to RE (the cloud shim injects `--unstable-allow-all-tests-on-re`).
- **Commit messages** are Conventional Commits (enforced by the `commit-msg` hook) and must end with the two loom trailers:
  `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` and `Claude-Session: https://claude.ai/code/session_012KA8bcR8qWMJtLXLPmQf32`.
- **Reference implementations to mirror (read them; do not reinvent):**
  - Atomic multi-table write: `src/services/transform/src/run.rs:192-199` (`begin_table` → `create_table` → `append_files`/`replace_files` → `emit` → `commit`).
  - Parquet-write-then-stage: `datafusion-io`'s `write_dataset` + `absolute_data_files` (used at `transform/run.rs:183-189`).
  - Wire RPC end-to-end: `write_object` through `engine-wire/proto/engine_control.proto` → `engine-wire/src/client.rs:196` → `engine-serving/src/action_writer.rs:47` → `query-api/src/engine_action_client.rs:58`.
  - Slice-2 expr resolution: `query-api/src/expr/{parse,typecheck,eval}.rs`, invoked at `query-api/src/params.rs:99` and validated at `query-api/src/action.rs:201` (`check_expr_assignment`).

---

## File Structure

- `src/control-plane/core/src/ontology.rs` — add `ActionStep`; refactor `ActionDef` to `{name, steps}`; serde back-compat repr; `single_step` constructor; step-aware builder.
- `src/control-plane/core/tests/*` — update literal constructions via `single_step`; new multi-step round-trip + serde tests.
- `src/control-plane/postgres/migrations/00NN_action_steps.sql` — new `action_step` table + `step_ordinal` on param/assignment tables + backfill.
- `src/control-plane/postgres/src/ontology.rs` — per-step `define_action`/`get_action`.
- `src/control-plane/postgres/.sqlx/` — regenerated cache (committed).
- `src/control-plane/testkit/src/lib.rs` — multi-step action contract (both adapters).
- `src/services/query-api/src/action.rs` — per-step conformance, cross-step ref define-time validation, multi-step `run_action` orchestration.
- `src/services/query-api/src/params.rs` — per-step row resolution + step binding environment; cross-step ref resolution.
- `src/services/query-api/src/serving.rs` — `ActionEngine::write_steps` trait method (default `Unsupported`) + `StepWrite` type.
- `src/services/engine-wire/proto/engine_control.proto` — `WriteSteps` RPC + messages.
- `src/services/engine-wire/src/client.rs` + `convert.rs` — `write_steps` client method.
- `src/services/engine-serving/src/action_writer.rs` — `write_steps` atomic composition via `begin_table`.
- `src/services/query-api/src/engine_action_client.rs` — `EngineActionClient::write_steps`.
- `src/services/query-api/tests/action_multi_object_e2e.rs` — the slice's e2e (`loom_fixture_test`).

---

## Task 1: Core `ActionStep`/`ActionDef` refactor + re-point every consumer to single-step (whole tree green, zero behavior change)

**This is the atomic domain refactor.** Changing `ActionDef` from flat fields to `steps` breaks every consumer that reads `action.target`/`.kind`/`.parameters`/`.assignments` — query-api (`action.rs`, `params.rs`), the postgres adapter, testkit, and the core tests. Task 1 changes the type **and** re-points every consumer to single-step semantics so the whole tree compiles and **all existing tests pass unchanged** — no new capability, no migration, no behavior change. (Plan-review finding C1: the field removal and the consumer re-point must land in one commit, or no intermediate commit compiles.)

**Files:**
- Modify: `src/control-plane/core/src/ontology.rs` (`ActionDef` ~483-495, `ActionDefBuilder` ~530-598)
- Modify (mechanical, re-point reads to `steps.first()`): `src/services/query-api/src/action.rs` (`action.target` at :443,446,561,634,840,897,898; `action.kind` at :465; `action.parameters`/`.assignments` in conformance), `src/services/query-api/src/params.rs` (:63,66,74,85)
- Modify: `src/control-plane/postgres/src/ontology.rs` (`define_action`/`get_action` — read/write single-step to the **existing** schema via `steps.first()` / `ActionDef::single_step`; NO migration yet)
- Verify unchanged: `src/control-plane/memory/src/ontology.rs` (stores `ActionDef` by clone — transparent)
- Modify (mechanical): `src/control-plane/core/tests/{action_kind,action_mapping,ontology_builder,governance_serde_roundtrip}.rs`, `src/control-plane/testkit/src/lib.rs` (use `ActionDef::single_step(...)`)
- Create test: `src/control-plane/core/tests/action_steps.rs` + BUCK target `action-steps`

**Interfaces:**
- Produces:
  - `pub struct ActionStep { pub target: TypeName, pub kind: ActionKind, pub parameters: Vec<ParamDef>, pub assignments: Vec<Assignment>, pub bind: Option<String> }` (derives `Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize`).
  - `pub struct ActionDef { pub name: ActionName, pub steps: Vec<ActionStep> }` (custom serde via `#[serde(from/into)]`).
  - `impl ActionDef { pub fn single_step(name: ActionName, target: TypeName, kind: ActionKind, parameters: Vec<ParamDef>, assignments: Vec<Assignment>) -> Self; }`  **No `first_target` helper** (plan-review I1: it had a contradictory signature and no caller). Consumers that need the single/first step use `action.steps.first().ok_or(ActionError::Misconfigured("action has no steps".into()))?` (panic-free; never index `steps[0]`).
  - `ActionDefBuilder` keeps `param`/`param_req`/`param_bound`/`assign`/`assign_expr` operating on the current (last) step; `done()` yields a one-step action. Add `pub fn step(self, target: TypeName, kind: ActionKind) -> ActionDefBuilder` and `pub fn bind(self, name: impl Into<String>) -> Self` for multi-step construction. The builder **seeds one open step** at construction so `steps.last_mut()` is always `Some` — but handle it panic-free (`if let Some(s) = self.steps.last_mut()`), never `.expect()`.

- [ ] **Step 1: Write the failing test** (`src/control-plane/core/tests/action_steps.rs`)

```rust
use control_plane_core::{
    ActionDef, ActionKind, ActionName, ActionStep, Assignment, ParamDef, TypeName,
};

fn tn(s: &str) -> TypeName {
    TypeName(s.into())
}

// A legacy single-step action serializes to the flat, pre-steps JSON (byte-compat).
#[test]
fn single_step_serializes_flat() {
    let a = ActionDef::single_step(
        ActionName("createWidget".into()),
        tn("Widget"),
        ActionKind::Insert,
        vec![ParamDef { name: "id".into(), ty: "Long".into(), required: true, binds: None }],
        vec![Assignment::constant("status", serde_json::json!("active"))],
    );
    let v = serde_json::to_value(&a).expect("serialize");
    assert!(v.get("target").is_some(), "flat form carries top-level target");
    assert!(v.get("steps").is_none(), "flat form has no steps key");
    // Round-trips through the flat form.
    let back: ActionDef = serde_json::from_value(v).expect("deserialize flat");
    assert_eq!(back, a);
}

// A multi-step action serializes to the stepped form and round-trips.
#[test]
fn multi_step_round_trips() {
    let a = ActionDef {
        name: ActionName("createOrderWithLines".into()),
        steps: vec![
            ActionStep {
                target: tn("Order"),
                kind: ActionKind::Insert,
                parameters: vec![ParamDef {
                    name: "orderId".into(),
                    ty: "Long".into(),
                    required: true,
                    binds: Some("id".into()),
                }],
                assignments: vec![],
                bind: Some("order".into()),
            },
            ActionStep {
                target: tn("LineItem"),
                kind: ActionKind::Insert,
                parameters: vec![ParamDef {
                    name: "sku".into(),
                    ty: "String".into(),
                    required: true,
                    binds: None,
                }],
                assignments: vec![Assignment::step_ref("orderId", "order", "id")],
                bind: None,
            },
        ],
    };
    let v = serde_json::to_value(&a).expect("serialize");
    assert!(v.get("steps").is_some(), "stepped form carries steps");
    let back: ActionDef = serde_json::from_value(v).expect("deserialize stepped");
    assert_eq!(back, a);
}

// Legacy flat JSON (no `steps` key) still deserializes into one implicit step.
#[test]
fn legacy_flat_json_lifts_to_one_step() {
    let json = serde_json::json!({
        "name": "createWidget",
        "target": "Widget",
        "kind": "Insert",
        "parameters": [{ "name": "id", "ty": "Long", "required": true }],
        "assignments": []
    });
    let a: ActionDef = serde_json::from_value(json).expect("deserialize legacy flat");
    assert_eq!(a.steps.len(), 1);
    assert_eq!(a.steps[0].target, tn("Widget"));
    assert_eq!(a.steps[0].bind, None);
}
```

> `Assignment::step_ref(property, bind, prop)` is added in Task 3's `AssignmentSource::StepRef` work; for Task 1 it does not yet exist. To keep Task 1 self-contained and its test compiling, **omit the `multi_step_round_trips` assignment referencing `step_ref` in Task 1** — construct that step with `assignments: vec![]` — and Task 3 extends the test to add the `StepRef` case. (The implementer: build the two Task-1 steps with only `parameters`, no `step_ref`.)

- [ ] **Step 2: Run it to verify it fails**

Run: `buck2 test //src/control-plane/core:action-steps > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: FAIL — `ActionStep` / `ActionDef::single_step` / `first_target` do not exist yet.

- [ ] **Step 3: Add `ActionStep`, refactor `ActionDef`, add serde repr + constructor** (`ontology.rs`)

Replace the `ActionDef` struct (currently ~483-495) with the step model. Insert `ActionStep` just above it and the serde repr just below:

```rust
/// One step of an [`ActionDef`]: a single-target mutation. `bind` names the step's
/// output row so later steps can reference its properties (`@order.id`). Slice-1
/// `ParamDef.binds` and slice-2 `Assignment` live inside a step unchanged.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ActionStep {
    pub target: TypeName,
    pub kind: ActionKind,
    #[serde(default)]
    pub parameters: Vec<ParamDef>,
    #[serde(default)]
    pub assignments: Vec<Assignment>,
    /// Names this step's resolved row for cross-step references. `None` ⇒ not bindable.
    #[serde(default)]
    pub bind: Option<String>,
}

/// A named action: an ordered list of single-target mutation [`ActionStep`]s committed
/// in one transaction. A single-step action is byte-compatible with the pre-steps flat
/// shape (see the `ActionDefRepr` serde bridge).
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(from = "ActionDefRepr", into = "ActionDefRepr")]
pub struct ActionDef {
    pub name: ActionName,
    pub steps: Vec<ActionStep>,
}

/// Wire bridge: a single-step, bind-less action reads/writes the legacy flat JSON;
/// anything else uses the explicit `steps` array. `#[serde(untagged)]` tries `Flat`
/// first on read, so legacy `{name,target,kind,parameters,assignments}` still parses.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
enum ActionDefRepr {
    Flat {
        name: ActionName,
        target: TypeName,
        #[serde(default)]
        kind: ActionKind,
        #[serde(default)]
        parameters: Vec<ParamDef>,
        #[serde(default)]
        assignments: Vec<Assignment>,
    },
    Stepped {
        name: ActionName,
        steps: Vec<ActionStep>,
    },
}

impl From<ActionDefRepr> for ActionDef {
    fn from(r: ActionDefRepr) -> Self {
        match r {
            ActionDefRepr::Flat { name, target, kind, parameters, assignments } => {
                ActionDef::single_step(name, target, kind, parameters, assignments)
            }
            ActionDefRepr::Stepped { name, steps } => ActionDef { name, steps },
        }
    }
}

impl From<ActionDef> for ActionDefRepr {
    fn from(a: ActionDef) -> Self {
        // A single bind-less step round-trips to the flat form (byte-compat). `into()`
        // consumes `a`, so match on the count first, then move the fields out.
        if a.steps.len() == 1 && a.steps[0].bind.is_none() {
            let mut steps = a.steps;
            let s = steps.remove(0);
            ActionDefRepr::Flat {
                name: a.name,
                target: s.target,
                kind: s.kind,
                parameters: s.parameters,
                assignments: s.assignments,
            }
        } else {
            ActionDefRepr::Stepped { name: a.name, steps: a.steps }
        }
    }
}

impl ActionDef {
    /// Build a one-step action from the pre-steps flat shape (the implicit-step migration).
    #[must_use]
    pub fn single_step(
        name: ActionName,
        target: TypeName,
        kind: ActionKind,
        parameters: Vec<ParamDef>,
        assignments: Vec<Assignment>,
    ) -> Self {
        ActionDef {
            name,
            steps: vec![ActionStep { target, kind, parameters, assignments, bind: None }],
        }
    }
}
```

> No `first_target` accessor (plan-review I1). Consumers read the single/first step via `action.steps.first()` and handle the (construction-impossible) empty case with a `Misconfigured` error — never `steps[0]` (clippy `indexing_slicing`), never `.expect()`.

- [ ] **Step 4: Make the `ActionDefBuilder` step-aware** (`ontology.rs` ~530-598)

Keep every existing builder method working by having them mutate the **current** step. The builder holds `name` + a `Vec<ActionStep>` with at least one open step. `param`/`param_req`/`param_bound`/`assign`/`assign_expr` push onto the last step. Add `step(target, kind)` (pushes a new open step) and `bind(name)` (sets the last step's `bind`). `done()` returns `ActionDef { name, steps }`. Show the full rewritten builder in the implementation (mirror the existing method bodies, redirecting `self.parameters`/`self.assignments` to `self.steps.last_mut()`).

- [ ] **Step 5: Re-point every consumer to single-step semantics (whole tree compiles, behavior identical)**

- **query-api `action.rs` / `params.rs`:** replace each read of `action.target`/`.kind`/`.parameters`/`.assignments` with the single step's field via `let step = action.steps.first().ok_or(ActionError::Misconfigured("action has no steps".into()))?;` then `step.target`/`step.kind`/`step.parameters`/`step.assignments`. `resolve_action_row(action, …)` takes the whole `ActionDef` today; keep its signature but read `action.steps.first()` internally (Task 3/5 generalize it to take a step + env). No behavior change: a single-step action reads exactly the old fields.
- **postgres `ontology.rs`:** `define_action` reads `action.steps.first()` and writes its target/kind/params/assignments to the **existing** `action`/`action_param`/`action_assignment` schema (unchanged). Guard: if `action.steps.len() > 1`, return `ControlPlaneError::Validation("multi-step action persistence lands in a later migration")` — a defensive stopgap Task 2 removes. `get_action` reconstructs `ActionDef::single_step(name, target, kind, params, assignments)` from the existing columns.
- **memory `ontology.rs`:** stores `ActionDef` by clone — confirm no change needed.
- **core tests + testkit:** in `action_kind.rs`, `action_mapping.rs`, `ontology_builder.rs`, `governance_serde_roundtrip.rs`, `testkit/src/lib.rs`: replace each `ActionDef { name, target, kind, parameters, assignments }` literal with `ActionDef::single_step(name, target, kind, parameters, assignments)`. `governance_serde_roundtrip.rs` stays green because a single-step action serializes to the identical flat JSON.

- [ ] **Step 6: Add the BUCK target for `action-steps`** (mirror an existing `rust_test` in `src/control-plane/core/BUCK`).

- [ ] **Step 7: Build the whole tree + run the affected tests + clippy**

Run: `buck2 build //src/... > /tmp/b.log 2>&1; grep -Ei "error|BUILD SUCCEEDED|finished" /tmp/b.log` (must compile — this proves C1 is resolved). Then `buck2 test //src/control-plane/... //src/services/query-api/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log` (all existing action tests must still pass — byte-compat). Clippy: `buck2 build '//src/control-plane/core:core[clippy.txt]' '//src/services/query-api:query-api[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log` (empty == clean). On a cloud host scope tests to affected targets and route to RE.

- [ ] **Step 8: Commit**

```bash
git add src/control-plane/core src/control-plane/postgres/src/ontology.rs src/control-plane/memory src/control-plane/testkit src/services/query-api/src/action.rs src/services/query-api/src/params.rs
git commit -m "refactor(core): ActionDef gains ordered steps; re-point all consumers to single-step (byte-compatible)"
```

---

## Task 2: Postgres migration to per-step persistence + `.sqlx` + testkit multi-step contract

Task 1 left the postgres adapter storing single-step actions in the existing schema (with a `steps.len() > 1` guard). Task 2 migrates to the normalized per-step tables so **all** steps persist, removes the guard, and adds the multi-step round-trip to the testkit contract.

**Files:**
- Create: `src/control-plane/postgres/migrations/00NN_action_steps.sql` (NN = next number after the highest existing migration — check `ls src/control-plane/postgres/migrations/`)
- Modify: `src/control-plane/postgres/src/ontology.rs` (`define_action` ~271-341, `get_action` ~344-397)
- Regenerate: `src/control-plane/postgres/.sqlx/`
- Modify: `src/control-plane/testkit/src/lib.rs` (action contract ~925-1082)
- Verify unchanged: `src/control-plane/memory/src/ontology.rs`

**Interfaces:**
- Consumes: `ActionDef { name, steps }`, `ActionStep`, `ActionKind::as_str()`, `AssignmentSource::{Const,Expr}` (Task 1). Task 3 adds `AssignmentSource::StepRef`; **persist a third `(bind, prop)` column pair now** so Task 3 needs no second migration — see Step 1.

- [ ] **Step 1: Write the migration** (`00NN_action_steps.sql`)

```sql
-- Multi-step actions: an action is an ordered list of single-target steps.
-- Move target_type/kind from `action` to a new per-step table, and re-key the
-- param/assignment tables by (action_name, step_ordinal, ordinal).

create table ontology.action_step (
    action_name text    not null references ontology.action (name) on delete cascade,
    ordinal     int     not null,
    target_type text    not null references ontology.object_type (name) on delete cascade,
    kind        text    not null,
    bind        text,
    primary key (action_name, ordinal)
);

-- Backfill one implicit step per existing action.
insert into ontology.action_step (action_name, ordinal, target_type, kind, bind)
select name, 0, target_type, kind, null from ontology.action;

-- Re-key params + assignments by step (existing rows belong to step 0).
alter table ontology.action_param add column step_ordinal int not null default 0;
alter table ontology.action_assignment add column step_ordinal int not null default 0;

alter table ontology.action_param drop constraint action_param_pkey;
alter table ontology.action_param add primary key (action_name, step_ordinal, ordinal);
alter table ontology.action_assignment drop constraint action_assignment_pkey;
alter table ontology.action_assignment add primary key (action_name, step_ordinal, ordinal);

-- Cross-step reference source (Task 3): an assignment may be a StepRef(bind, prop).
-- Extend the exactly-one-source check to include the ref pair.
alter table ontology.action_assignment add column ref_bind text;
alter table ontology.action_assignment add column ref_prop text;
alter table ontology.action_assignment drop constraint action_assignment_source_ck;
alter table ontology.action_assignment add constraint action_assignment_source_ck check (
    (value is not null and expr is null and ref_bind is null)
    or (value is null and expr is not null and ref_bind is null)
    or (value is null and expr is null and ref_bind is not null and ref_prop is not null)
);

-- target_type/kind now live on action_step; drop them from action.
alter table ontology.action drop column target_type;
alter table ontology.action drop column kind;
```

> Implementer: confirm the actual PK constraint names (`\d ontology.action_param`) — Postgres default is `<table>_pkey`. Adjust the `drop constraint` names if migrations named them otherwise. Check the highest migration number first.

- [ ] **Step 2: Rewrite `define_action`** to iterate steps (per-step `action_step` insert; per-step params/assignments carrying `step_ordinal`; `StepRef` writes `ref_bind`/`ref_prop`). **Remove Task 1's `steps.len() > 1` guard.** Clear existing step/param/assignment rows for the action first (the existing `on conflict`/delete pattern), then insert. Full SQL mirrors the current inserts with the added `step_ordinal` + `action_step` loop.

- [ ] **Step 3: Rewrite `get_action`** to load `action_step` rows (ordinal order), and for each step load its params + assignments filtered by `step_ordinal`, reconstructing `AssignmentSource` from the `(value, expr, ref_bind, ref_prop)` tuple. Assemble `ActionDef { name, steps }`.

- [ ] **Step 4: Regenerate + commit the `.sqlx` cache**

Run: `./tools/sqlx-prepare.sh > /tmp/sqlx.log 2>&1; grep -Ei "error|prepared|finished" /tmp/sqlx.log`
Then `git add src/control-plane/postgres/.sqlx`.

- [ ] **Step 5: Extend the testkit action contract** — add a multi-step `ActionDef` (Order+LineItem, with a `bind` and a `StepRef` assignment) to the round-trip assertion, keeping the existing single-step + mapping + expr round-trips. This runs against **both** adapters via the existing contract harness.

- [ ] **Step 6: Confirm the memory adapter is transparent** — `memory/src/ontology.rs` stores `ActionDef` by clone; no change needed. Read it to confirm and note in the report.

- [ ] **Step 7: Run the postgres + testkit fixture suite**

Run: `buck2 test //src/control-plane/postgres/... //src/control-plane/testkit/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (incl. `sqlx-cache-check`). On a root/cloud host these fixture tests route to RE via the shim.

- [ ] **Step 8: Commit**

```bash
git add src/control-plane/postgres src/control-plane/testkit
git commit -m "feat(postgres): persist action steps in normalized per-step tables; multi-step testkit contract"
```

---

## Task 3: Query-api define-time conformance + cross-step reference resolution

**Files:**
- Modify: `src/control-plane/core/src/ontology.rs` — add `AssignmentSource::StepRef { bind, prop }` + `Assignment::step_ref` constructor.
- Modify: `src/services/query-api/src/action.rs` — per-step `check_conformance`; cross-step ref define-time validation.
- Modify: `src/services/query-api/src/params.rs` — step binding environment + `StepRef` resolution.
- Create test: `src/services/query-api/tests/multi_step_conformance.rs` + BUCK `multi-step-conformance` (`rust_test`, memory-backed).

**Interfaces:**
- Produces: `AssignmentSource::StepRef { bind: String, prop: String }`; `Assignment::step_ref(property, bind, prop)`. A `StepEnv` mapping `bind → resolved row (prop → SqlValue, incl. identity)` used at invocation.
- Design decision (record in PR + register close): cross-step references are a **first-class `StepRef` assignment source** (`@order.id` semantics), define-time validated, resolved from the prior-step binding environment — NOT an extension of the slice-2 expression parser. This honors the spec's explicit fallback ("scope the ref env to prior-step identity/property values only"); cross-step values **inside** arithmetic expressions are deferred to a FUTURE follow-on (`fut-action-multi-object` retains it).

- [ ] **Step 1: Write the failing conformance test** (`multi_step_conformance.rs`) covering **all five** spec test-2 rejection cases + the valid case:
  - a `StepRef` naming a **later** step's bind → rejected;
  - a `StepRef` naming the **step's own** bind (self) → rejected;
  - a `StepRef` naming an **unbound** step → rejected;
  - a `StepRef` whose `prop` is **not a property** of the bound step's target → rejected;
  - a **property double-bound within one step** (two params/assignments writing the same property) → rejected (spec test 2's fifth case; confirm the existing single-step "no double-write" rule at `params.rs:51` fires per step);
  - a valid backward `StepRef` (`LineItem.orderId = @order.id`) → accepted.

  Use the memory control plane; define the two object types (Order with `id` identity, LineItem with `orderId`), then assert `check_conformance` (or the public conformance entry) returns the right error/ok per case.

- [ ] **Step 2: Run to verify it fails.** Run the target; expect FAIL (no `StepRef`, no cross-step validation).

- [ ] **Step 3: Add `AssignmentSource::StepRef` + constructor** (`core/ontology.rs`)

```rust
pub enum AssignmentSource {
    Const(serde_json::Value),
    Expr(String),
    /// A reference to an earlier step's resolved property (`@<bind>.<prop>`).
    StepRef { bind: String, prop: String },
}
// impl Assignment { pub fn step_ref(property, bind, prop) -> Self { … StepRef … } }
```

Update the postgres `(value, expr, ref_bind, ref_prop)` match arms (Task 2 already added the columns) and the `From<ActionDefRepr>` serde (StepRef is inside a step's assignments — no flat-form concern). Extend Task 1's `action_steps.rs` `multi_step_round_trips` to include the `step_ref` assignment now that it exists.

- [ ] **Step 4: Generalize `check_conformance` to iterate steps** and thread a growing set of **bound step names + their target property sets**. For each step in order: run the existing single-step checks (param coverage, required-property coverage for Insert, type compatibility, `check_expr_assignment` for `Expr`); additionally, for each `StepRef { bind, prop }`, validate `bind` is a **strictly earlier** step's `bind` and `prop` is a real property of that step's target — else a `Misconfigured`/conformance violation. Add the step's own `bind` to the bound set **after** its checks (so self-refs are rejected).

- [ ] **Step 5: Resolve `StepRef` at invocation** (`params.rs`) — extend `resolve_action_row` to accept a `StepEnv` (`&BTreeMap<String, BTreeMap<String, SqlValue>>` = bind → row). For `AssignmentSource::StepRef { bind, prop }`, read `step_env[bind][prop]`. The caller (Task 5) resolves steps in order and inserts each bound step's resolved row into the env before resolving the next.

- [ ] **Step 6: Run the conformance test.** Expect PASS. Clippy-check core + query-api.

- [ ] **Step 7: Commit**

```bash
git add src/control-plane/core src/control-plane/postgres src/services/query-api
git commit -m "feat(query-api): cross-step references (StepRef) with define-time forward/self/unbound rejection"
```

---

## Task 4: Atomic multi-target write seam (`ActionEngine::write_steps`) + wire RPC

**Files:**
- Modify: `src/services/query-api/src/serving.rs` — `StepWrite` type + `ActionEngine::write_steps` (default `Unsupported`).
- Modify: `src/services/engine-wire/proto/engine_control.proto` — `WriteSteps` rpc + `WriteStepsRequest`/`StepWrite` messages.
- Modify: `src/services/engine-wire/src/{client.rs,convert.rs}` — `write_steps` client method.
- Modify: `src/services/engine-serving/src/action_writer.rs` — `write_steps` composition via `begin_table`.
- Modify: `src/services/query-api/src/engine_action_client.rs` — `EngineActionClient::write_steps`.

**Interfaces:**
- Produces:
  ```rust
  pub struct StepWrite {
      pub table: control_plane_core::TableRef,
      pub columns: Vec<String>,
      pub rows: Vec<Vec<SqlValue>>,
      pub logical_types: Vec<String>,
      pub mode: WriteMode, // Append | Overwrite
  }
  pub enum WriteMode { Append, Overwrite }
  ```
  `async fn write_steps(&self, writes: &[StepWrite], event: LineageEvent) -> Result<SnapshotId, ServingError>` on `ActionEngine`, default returns `ServingError::Engine("write_steps unsupported".into())` (mirroring the existing `write_delta`/`current_inline_version` default-unsupported methods).
- Consumes engine-side: `begin_table()` (`TableControlPlane`), `write_dataset`/`absolute_data_files` (datafusion-io) or `write_parquet_with_schema` + `DataFile` assembly, `TableTx::{create_table,append_files,replace_files,emit,commit}`.

- [ ] **Step 1: Write the failing engine-serving test** (extend/create an engine-serving fixture test) that: creates two object types, calls `write_steps` with an Append `StepWrite` for table A (1 row) and an Append for table B (2 rows) + a lineage event with both outputs, then asserts (a) both tables read back the rows and (b) one lineage event lists both `DatasetRef`s. Mirror `iceberg_action_e2e.rs`'s in-process engine setup (`spawn_engine_writer`). This is the atomic-composition proof.

- [ ] **Step 2: Run to verify it fails** (no `write_steps`).

- [ ] **Step 3: Add `StepWrite`/`WriteMode` + trait method** (`serving.rs`) with the default-unsupported body.

- [ ] **Step 4: Implement `action_writer::write_steps`** — the atomic composition, generalizing `transform/run.rs:192-199` to N tables:

```
// For each StepWrite: build the Arrow batch (build_object_batches), ensure the
// Iceberg table exists + load it, write Parquet (write_parquet_with_schema),
// assemble DataFile stats (mirror datafusion-io / append_batches_with_extras' WrittenFile).
// Then ONE unit of work:
let mut tx = control_plane.begin_table().await?;
for w in writes {
    tx.create_table(&w.table, &cols).await?;          // idempotent
    match w.mode {
        Append    => tx.append_files(&w.table, &files).await?,
        Overwrite => tx.replace_files(&w.table, &files).await?,
    }
}
tx.emit(event).await?;      // one lineage event, all outputs
// commit() returns Result<Option<SnapshotId>>; None means nothing was staged.
tx.commit().await?.ok_or_else(|| ServingError::Engine("write_steps: no snapshot".into()))
```

Implementer: `tx.commit()` returns `Result<Option<SnapshotId>>` (plan-review I4) — map `None` to an error exactly as `transform/run.rs:199` maps it to `NoSnapshot`. Reuse the existing Parquet+stats path. `action_writer` already holds `pool` + `catalog`; construct the `IcebergControlPlane` (or reuse a held handle) to get `begin_table`. Write files **before** `begin_table` (pure IO), stage inside the tx. Grouping: `StepWrite`s already arrive one-per-target from Task 5 (steps sharing a table are pre-coalesced there), so no in-seam grouping is required — but assert distinct `(table, mode)` handling is correct if two writes target the same table.

- [ ] **Step 5: Add the `WriteSteps` wire RPC** — add to `engine_control.proto` a `WriteStepsRequest { repeated StepWrite steps; string lineage_json; }` where `StepWrite { string schema; string name; bytes ipc; string columns_json; bool overwrite; }`, and the `WriteSteps` rpc on the `EngineControl` service. Regenerate (buck2 build). Add `GrpcQueueClient::write_steps` (`client.rs`) mirroring `write_object` (200-ish). Wire the engine-serving server handler to decode the request and call `action_writer::write_steps`.

- [ ] **Step 6: Implement `EngineActionClient::write_steps`** (`engine_action_client.rs`) — for each `StepWrite`, `build_object_batches` → `encode_ipc_stream` → `columns_json`; one `lineage_json`; call `ctl.write_steps(...)`. Map conflict via `to_serving_write`.

- [ ] **Step 7: Run engine-serving + engine-wire tests**

Run: `buck2 test //src/services/engine-serving/... //src/services/engine-wire/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS. Clippy-check the touched crates.

- [ ] **Step 8: Commit**

```bash
git add src/services/query-api/src/serving.rs src/services/query-api/src/engine_action_client.rs src/services/engine-wire src/services/engine-serving
git commit -m "feat(engine): write_steps — stage N per-target writes + one lineage event in one atomic Tx"
```

---

## Task 5: Multi-step `run_action` orchestration

**Files:**
- Modify: `src/services/query-api/src/action.rs` — `run_action` dispatch (~423-470), a new `run_multi_step`, per-step resolve+govern.
- Modify (if needed): `src/services/query-api/src/params.rs` — reuse `resolve_action_row` per step with the `StepEnv`.

**Interfaces:**
- Consumes: `ActionDef.steps`, Task 3's `StepEnv` resolution, Task 4's `write_steps`, existing governance (`check_write_policy`, `enforce_mutate_policy`, `value_constraint_violations`), `build_object_batches`.

- [ ] **Step 1: Write the failing unit test** — a memory/stub `ActionEngine` recording `write_steps` calls; assert a 2-step action resolves the child step's `StepRef` from the parent's resolved identity and calls `write_steps` once with two `StepWrite`s and a lineage event whose `outputs` lists both targets. (Governance-denial paths are covered by the e2e in Task 6.)

- [ ] **Step 2: Run to verify it fails.**

- [ ] **Step 3: Dispatch on step count in `run_action`.** After resolving the `ActionDef`: bind `let single = action.steps.first().filter(|_| action.steps.len() == 1).filter(|s| s.bind.is_none());` — if `Some(step)`, run **today's path** unchanged (`run_insert`/`run_mutate` on `step`'s fields) — byte-compatible, inline tiering preserved. Otherwise call `run_multi_step`. Never index `steps[0]` (clippy `indexing_slicing`); always `.first()`.

  Implementer: Task 1 already re-pointed `run_insert`/`run_mutate` at `action.steps.first()`. Here, pass the resolved `&ActionStep` explicitly so the single-step path and `run_multi_step` share the same per-step helpers.

- [ ] **Step 4: Implement `run_multi_step`** — the pre-commit resolve+govern loop, then one atomic write:
  1. `let mut step_env = BTreeMap::new();`
  2. For each step in order:
     - coarse `Action::Write` gate on `step.target`;
     - `resolve_action_row(step, target, body, now, &step_env)` → `pairs` (params + assignments + `StepRef`s from `step_env`);
     - fine-grained governance: Insert → `check_write_policy`; Update/Delete → read existing row + `enforce_mutate_policy`; then `value_constraint_violations`. All identical to the single-object gates, run per step. Any denial → return (nothing written).
     - build the step's `StepWrite`:
       - **Insert** → `WriteMode::Append` with the resolved row.
       - **Update/Delete** → compute the **full post-image row set** for the table, `WriteMode::Overwrite`. Concretely (plan-review I3): read the table's full current logical contents through the serving engine (`deps.serving.fetch_rows(&select_object_sql_all(target), &[])` — the inline+file merge view, same reader `run_mutate` uses at `action.rs:918-937` but unfiltered by identity); locate the targeted identity row; for Update apply the SET pairs to it (governed by `enforce_mutate_policy` on that row's existing + new image, exactly as single-object Update), for Delete drop it; keep every **other** row verbatim (they are preserved, not re-written, so they are not re-gated). The resulting row set is the Overwrite payload. `ensure_cow_supported(target)` still rejects vector-typed targets (spec test 7). This is the file-tier COW (`replace_files`, which end-caps both inline and file tiers — verified safe), distinct from the single-object inline-delta `write_delta` path; it is O(table) per mutating step — acceptable for the slice, noted as a follow-on optimization.
       - insert the step's resolved row into `step_env` under `step.bind` (if set).
  3. Coalesce `StepWrite`s that share a target table into one multi-row `StepWrite` (Insert-append case) — `build_object_batches` handles N rows.
  4. Build one `LineageEvent` (`completed_with_run(run_id, all_targets, payload)`), call `deps.action_engine.write_steps(&writes, event)`.
  5. Return the affected objects + the single `RunId`.

  > Mixed kinds (Insert + Update/Delete across distinct tables) compose in the one `IcebergTx`. `select_object_sql_all` is the existing `select_object_sql` minus the identity `where` clause — reuse/param it rather than writing new SQL.

- [ ] **Step 5: Run the unit test + existing action tests.**

Run: `buck2 test //src/services/query-api/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log` (scope to affected targets on a cloud host).
Expected: PASS, incl. all existing single-step action tests (byte-compat).

- [ ] **Step 6: Commit**

```bash
git add src/services/query-api/src
git commit -m "feat(query-api): multi-step run_action — resolve+govern every step before one atomic write"
```

---

## Task 6: End-to-end multi-object action tests

**Files:**
- Create: `src/services/query-api/tests/action_multi_object_e2e.rs` + BUCK `action-multi-object-e2e` (`loom_fixture_test`, deps mirror `iceberg-action-e2e` + `:e2e-support`).

**Interfaces:** consumes the full stack (Tasks 1-5) via the in-process engine writer (`spawn_engine_writer`) + `e2e_support` seed helpers.

- [ ] **Step 1: Write the e2e tests** — mirror `iceberg_action_e2e.rs`'s setup, covering spec tests 3-7:
  - **Order + lines (test 3):** define `Order`(id) + `LineItem`(id, orderId) types + a `createOrderWithLines` multi-step action (Order step bound `order`; LineItem step with `orderId = @order.id`, 2 lines). Grant Write+Read. Invoke; assert all three objects read back and `LineItem.orderId == Order.id`.
  - **Atomic rollback (test 4):** a step that fails (deny a column via ACL, or a constraint violation on step 2, or a bad write) leaves **no** object from any step visible; assert reads return empty and no lineage event for the run.
  - **Per-step governance (test 5):** a denied column on step 2 → 403, nothing commits; a constraint violation on step 2 → 422, nothing commits — each identical to the single-object gate.
  - **Lineage (test 6):** the single `RunId`'s event `outputs` lists every step's target.
  - **Mixed kinds (test 7):** an action with an Insert step + an Update/Delete step on an existing object commits atomically; a vector-typed target still rejects Delete via `ensure_cow_supported`.

- [ ] **Step 2: Run the e2e** (RE on cloud):

Run: `buck2 test //src/services/query-api:action-multi-object-e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add src/services/query-api/tests/action_multi_object_e2e.rs src/services/query-api/BUCK
git commit -m "test(query-api): e2e multi-object actions — order+lines, atomic rollback, per-step governance, lineage, mixed kinds"
```

---

## Self-Review (post plan-review, findings resolved)

- **Spec coverage:** step model + `bind` (T1); implicit-step migration + byte-compat (T1 serde + T1 consumer re-point + T5 dispatch); both adapters + testkit (T1 single-step green, T2 per-step); cross-step capture + define-time/invocation resolution incl. double-bind rejection (T3, T5); per-step governance before one commit (T5); extended multi-target landing tx (T4); multi-target `LineageEvent.outputs` (T4/T5); all 7 test scenarios (T6). Covered.
- **C1 resolved:** Task 1 is now the atomic domain refactor that re-points every consumer (query-api, postgres, memory, testkit, core tests) to single-step semantics, so the whole tree compiles and all existing tests pass at every commit. Task 2 then migrates to per-step persistence.
- **I1 resolved:** `first_target` dropped; call sites use `.first().ok_or(Misconfigured)`.
- **I2 resolved:** double-bound-property rejection added to Task 3's test enumeration.
- **I3 resolved:** Update/Delete post-image concretely specified (serving full-table read → patch target → preserve others → Overwrite), citing `select_object_sql`/`fetch_rows`/`enforce_mutate_policy`/`ensure_cow_supported`.
- **I4 resolved:** `write_steps` maps `commit()`'s `Option<SnapshotId>` `None` → error.
- **Scope decision (confirmed defensible by plan-review):** cross-step refs are first-class `StepRef` (identity/property values), not expression-embedded `@bind.prop` — sanctioned by the spec's fallback; expression-embedded cross-step refs remain a FUTURE deferral to note in the PR + register close.
- **Open implementer checks:** `.sqlx` PK constraint names confirmed against live schema; next migration number checked; builder `steps.last_mut()` handled panic-free.
- **Type consistency:** `ActionStep`/`ActionDef{name,steps}`/`single_step`/`AssignmentSource::StepRef`/`Assignment::step_ref`/`StepWrite`/`WriteMode`/`write_steps`/`StepEnv` names used consistently across tasks.
</content>
</invoke>
