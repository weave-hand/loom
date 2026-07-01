# Custom-logic actions slice 1 — declarative param→property mapping — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decouple an action's parameters from its target type's properties — a parameter can bind a differently-named property (`binds`), and a property can be filled by a declared constant assignment when no parameter supplies it — without opening the door to computed expressions or bespoke code.

**Architecture:** Two additive pieces on the ontology `ActionDef`: `ParamDef.binds: Option<String>` (the property a param writes; `None` ⇒ its own name, so existing actions are byte-for-byte unchanged) and `ActionDef.assignments: Vec<ConstAssignment>` (ordered `{property, value}` constants). A property's written value resolves in order: the param bound to it, else its constant assignment, else unset (NULL). Storage extends the postgres ontology schema (a nullable `binds` column + a child `action_assignment` table); the memory fake stores the whole `ActionDef` and round-trips for free. query-api's invocation-time conformance generalizes through `binds`/constants, and the write path resolves each property from the mapping before the **unchanged** ACL / commit gates.

**Tech Stack:** Rust (edition 2024), buck2, sqlx compile-time `query!` (postgres adapter), axum (query-api HTTP), the loom control-plane `Ontology` trait + testkit contract.

## Global Constraints

- **Tests are `rust_test` integration targets only — never inline `#[cfg(test)]`.** Each new test is a sibling `tests/<name>.rs` wired as its own target in the crate's `BUCK`. The `no-inline-tests` prek hook enforces this.
- **BUCK test rules load the loom wrapper:** `load("//src:loom_test.bzl", "rust_test")` for pure-logic tests; **`loom_fixture_test`** (from `//src/control-plane/postgres/defs.bzl`) for any test that boots postgres. Never a bare `native.rust_test` for a fixture test.
- **Clippy is strict (pedantic + restriction) on production lib/bin code.** No `unwrap`/`expect`/`panic`/`todo`/`dbg!`/indexing-slicing in non-test code; carry source errors (no `map_err_ignore`). Use `#[expect(lint, reason = "…")]` locally when unavoidable. Test code is exempt from the panic-safety lints via the wrapper.
- **Compile-time SQL:** after any change to a `query!` in the postgres adapter, regenerate the committed `.sqlx` cache and commit it. In this root cloud session use **`tools/sqlx-prepare-cloud.sh`** (runs the postgres *server* as the unprivileged `pgrunner` user, clients as root — `initdb` refuses root). Freshness is enforced by `//src/control-plane/postgres:sqlx-cache-check`.
- **Migrations are forward-only** `NNNN_<desc>.sql` under `src/control-plane/postgres/migrations/`. Next number is **`0022`**.
- **Commit messages follow Conventional Commits** (enforced by the `conventional-commit` commit-msg hook), e.g. `feat(ontology): …`.
- **`serde_json::Value` is not `Eq`** — `ActionDef` and `ConstAssignment` therefore derive `PartialEq` but **not** `Eq`. `ActionDef` is only ever a `HashMap` *value* (never a key / set member), so dropping `Eq` is safe (verified: only `memory/src/ontology.rs:15` `HashMap<String, ActionDef>`).
- **Markdown lint:** any `.md` you touch must end with exactly one trailing newline and have no trailing whitespace (`end-of-file-fixer` / `trim trailing whitespace` prek hooks).

---

## File Structure

- `src/control-plane/core/src/ontology.rs` — add `ParamDef.binds`, `ConstAssignment`, `ActionDef.assignments`; drop `Eq` from `ActionDef`.
- `src/control-plane/core/src/lib.rs` — export `ConstAssignment`.
- `src/control-plane/core/tests/action_mapping.rs` (new) + `core/BUCK` target — serde round-trip + back-compat of the new fields.
- `src/control-plane/postgres/migrations/0022_action_binds_assignments.sql` (new) — `binds` column + `action_assignment` table.
- `src/control-plane/postgres/src/ontology.rs` — persist/read `binds` + `assignments`.
- `src/control-plane/postgres/.sqlx/` — regenerated cache (generated artifact, committed).
- `src/control-plane/testkit/src/lib.rs` — extend the `ontology_contract` actions section (binds + assignments round-trip + back-compat).
- `src/services/query-api/src/action.rs` — generalize `check_param_property_types` / `check_insert_conformance` / `check_mutate_conformance`; switch the write path to the mapping resolver.
- `src/services/query-api/src/params.rs` — add `resolve_action_row` + a public `validate_const` helper.
- `src/services/query-api/tests/action_conformance.rs`, `tests/mutate_conformance.rs`, `tests/params.rs` — extend with the mapping cases.
- `src/services/query-api/tests/action_mapping_e2e.rs` (new) + `query-api/BUCK` target — rename / constant-fill / gate-composition / update-mapping e2e.
- `docs/ROADMAP.md` — close `road-action-param-mapping` (final task, via `loom-docs-update`).

---

## Task 1: Core types — `ParamDef.binds`, `ConstAssignment`, `ActionDef.assignments`

**Files:**
- Modify: `src/control-plane/core/src/ontology.rs` (`ParamDef` ~152-158, `ActionDef` ~174-185)
- Modify: `src/control-plane/core/src/lib.rs` (the `pub use ontology::{…}` block ~37-40)
- Create: `src/control-plane/core/tests/action_mapping.rs`
- Modify: `src/control-plane/core/BUCK` (add the test target)

**Interfaces:**
- Produces:
  - `ParamDef { name: String, ty: String, required: bool, binds: Option<String> }`
  - `ConstAssignment { property: String, value: serde_json::Value }` (derives `Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize`)
  - `ActionDef { name: ActionName, target: TypeName, parameters: Vec<ParamDef>, kind: ActionKind, assignments: Vec<ConstAssignment> }` (derives `Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize` — **no `Eq`**)
  - A helper method `ParamDef::binds_property(&self) -> &str` returning `self.binds.as_deref().unwrap_or(&self.name)`.

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/core/tests/action_mapping.rs`:

```rust
use control_plane_core::{ActionDef, ActionKind, ActionName, ConstAssignment, ParamDef, TypeName};

fn param(name: &str, ty: &str, binds: Option<&str>) -> ParamDef {
    ParamDef {
        name: name.into(),
        ty: ty.into(),
        required: true,
        binds: binds.map(str::to_string),
    }
}

#[test]
fn param_binds_property_defaults_to_name() {
    assert_eq!(param("displayName", "String", None).binds_property(), "displayName");
    assert_eq!(param("displayName", "String", Some("name")).binds_property(), "name");
}

#[test]
fn action_def_carries_binds_and_assignments_through_serde() {
    let a = ActionDef {
        name: ActionName("createGadget".into()),
        target: TypeName("Gadget".into()),
        parameters: vec![param("displayName", "String", Some("name"))],
        kind: ActionKind::Insert,
        assignments: vec![ConstAssignment {
            property: "status".into(),
            value: serde_json::json!("active"),
        }],
    };
    let json = serde_json::to_string(&a).expect("serialize");
    let back: ActionDef = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(back, a, "ActionDef round-trips with binds + assignments");
}

#[test]
fn back_compat_action_has_empty_mapping() {
    // An action whose params are named for their properties and carries no constants:
    // binds is None and assignments is empty — the byte-for-byte-unchanged shape.
    let a = ActionDef {
        name: ActionName("createWidget".into()),
        target: TypeName("Widget".into()),
        parameters: vec![param("id", "Long", None)],
        kind: ActionKind::Insert,
        assignments: vec![],
    };
    assert!(a.assignments.is_empty());
    assert_eq!(a.parameters[0].binds, None);
    assert_eq!(a.parameters[0].binds_property(), "id");
}
```

- [ ] **Step 2: Wire the BUCK target and run to verify it fails to compile (fields absent)**

Add to `src/control-plane/core/BUCK` (mirror the `action_kind` target near line 170):

```starlark
rust_test(
    name = "action_mapping",
    crate = "action_mapping",
    srcs = ["tests/action_mapping.rs"],
    crate_root = "tests/action_mapping.rs",
    edition = "2024",
    visibility = ["PUBLIC"],
    deps = [
        ":core",
        "//third-party:serde_json",
    ],
)
```

Run: `buck2 build //src/control-plane/core:action_mapping 2>&1 | tail -20`
Expected: FAIL — `no field \`binds\` on type \`ParamDef\``, `ConstAssignment` unresolved, `no field \`assignments\``.

- [ ] **Step 3: Add the fields and helper**

In `src/control-plane/core/src/ontology.rs`, change `ParamDef` to:

```rust
/// A typed input to an action. `ty` is the ontology's logical vocabulary (like `PropertyDef.ty`).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ParamDef {
    pub name: String,
    pub ty: String,
    pub required: bool,
    /// The property this parameter writes. `None` ⇒ the property named `name` (so an action
    /// whose params are named for their properties is unchanged); `Some(p)` renames the
    /// param away from the property `p` it binds.
    #[serde(default)]
    pub binds: Option<String>,
}

impl ParamDef {
    /// The property this parameter writes: its explicit `binds`, else its own `name`.
    #[must_use]
    pub fn binds_property(&self) -> &str {
        self.binds.as_deref().unwrap_or(&self.name)
    }
}
```

Add the `ConstAssignment` type just above `ActionDef`:

```rust
/// A declared constant filling a property when no parameter supplies it (the
/// default/fixed-value case, e.g. `status = "active"`). `value` is the JSON wire form of a
/// scalar — the canonical representation the query-api write path coerces to the property's
/// logical type (the same path parameters take); it is validated against the property type at
/// invocation-time conformance. Not `Eq` because `serde_json::Value` is not `Eq`.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ConstAssignment {
    pub property: String,
    pub value: serde_json::Value,
}
```

Change `ActionDef` to (drop `Eq`, add `assignments`):

```rust
/// A named ontology operation. Slice-1 semantics: insert/update/delete one instance of
/// `target`. Parameters are mapped onto the target's properties (via `ParamDef.binds`), and
/// `assignments` fill properties with declared constants when no parameter supplies them.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ActionDef {
    pub name: ActionName,
    pub target: TypeName,
    /// Ordered.
    pub parameters: Vec<ParamDef>,
    /// The mutation kind. `Insert` (part-1 default) creates; `Update`/`Delete` mutate
    /// one existing object by `target`'s declared `identity`.
    pub kind: ActionKind,
    /// Ordered constant property assignments (the default/fixed-value case).
    #[serde(default)]
    pub assignments: Vec<ConstAssignment>,
}
```

In `src/control-plane/core/src/lib.rs`, add `ConstAssignment` to the `pub use ontology::{…}` list (alphabetical, after `Cardinality`):

```rust
pub use ontology::{
    ActionDef, ActionKind, ActionName, Aggregation, Cardinality, ConstAssignment,
    DerivedPropertyDef, LinkBacking, LinkDef, ObjectType, Ontology, ParamDef, PropertyDef,
    TypeName, VectorIndexDef,
};
```

> Note: adding `binds`/`assignments` fields is a **compile break** for every struct literal *and every test-helper builder* of `ParamDef`/`ActionDef` across the tree. This task fixes them all to compile; behavior comes in later tasks. Use `grep -rn "ParamDef {" src/ ; grep -rn "ActionDef {" src/` and also update the helper functions that construct these types — known ones: `src/services/query-api/tests/action_conformance.rs` (`fn param`, `fn action`), `tests/mutate_conformance.rs` (direct literals), `tests/params.rs` (`fn p`), `tests/action_conformance_handler.rs`, `tests/action_conformance_http.rs`, `tests/write_denial_http.rs` (`fn param`), `src/control-plane/core/tests/action_kind.rs` and `tests/governance_serde_roundtrip.rs` (direct literals), plus every fixture e2e (`action_e2e.rs`, `update_delete_e2e.rs`, `e2e_support.rs`, …) and the adapters/testkit. Add `binds: None` to each `ParamDef` and `assignments: vec![]` to each `ActionDef` (there is **no** `Default` derive; add the fields explicitly). Do a tree-wide `buck2 build //src/... 2>&1 | tail` and `buck2 test //src/... --build-only 2>&1 | tail` (or just run the touched test targets' build) to confirm no literal is missed — the build is the exhaustive check.

- [ ] **Step 4: Run the test to verify it passes and the tree builds**

Run: `buck2 test //src/control-plane/core:action_mapping 2>&1 | tail -20`
Expected: PASS (3 tests).
Run: `buck2 build //src/... 2>&1 | tail -5`
Expected: builds clean (all struct literals updated).

- [ ] **Step 5: Clippy-clean the changed crates**

Run: `tools/clippy-all.sh 2>&1 | tail -20`
Expected: no new findings. (`binds_property` returning `&str` is fine; if clippy flags `must_use`, keep the attribute.)

- [ ] **Step 6: Commit**

```bash
git add src/control-plane/core src/control-plane/memory src/control-plane/postgres src/control-plane/testkit src/services/query-api
git commit -m "feat(ontology): add ParamDef.binds and ActionDef constant assignments"
```

---

## Task 2: Postgres storage + testkit contract for binds/assignments

**Files:**
- Create: `src/control-plane/postgres/migrations/0022_action_binds_assignments.sql`
- Modify: `src/control-plane/postgres/src/ontology.rs` (`define_action` ~287-345, `get_action` ~347-382)
- Regenerate: `src/control-plane/postgres/.sqlx/` (via `tools/sqlx-prepare-cloud.sh`)
- Modify: `src/control-plane/testkit/src/lib.rs` (the actions section of `ontology_contract`, ~793-881)

**Interfaces:**
- Consumes: `ParamDef.binds`, `ActionDef.assignments`, `ConstAssignment` from Task 1.
- Produces: postgres persists and returns `binds` + `assignments`; the shared `ontology_contract` asserts their round-trip on **both** adapters.

- [ ] **Step 1: Write the failing contract assertions**

In `src/control-plane/testkit/src/lib.rs`, after the existing Widget action assertions (~881), add a mapping block. First ensure the imports include `ConstAssignment` (extend the existing `use control_plane_core::{…}` at the top of the file). Then insert:

```rust
    // --- Action param→property mapping (binds + constant assignments) ---
    o.define_type(ObjectType {
        name: tn("Gadget"),
        table: tref("main", "gadget"),
        properties: vec![
            PropertyDef { name: "id".into(), ty: "Long".into(), required: true },
            PropertyDef { name: "name".into(), ty: "String".into(), required: false },
            PropertyDef { name: "status".into(), ty: "String".into(), required: false },
        ],
        derived: vec![],
        identity: None,
    })
    .await
    .expect("define Gadget");

    let create_gadget = ActionDef {
        name: ActionName("createGadget".into()),
        target: tn("Gadget"),
        parameters: vec![
            ParamDef { name: "id".into(), ty: "Long".into(), required: true, binds: None },
            // Renamed: the operation param `displayName` writes the `name` property.
            ParamDef {
                name: "displayName".into(),
                ty: "String".into(),
                required: false,
                binds: Some("name".into()),
            },
        ],
        kind: ActionKind::Insert,
        assignments: vec![ConstAssignment {
            property: "status".into(),
            value: serde_json::json!("active"),
        }],
    };
    o.define_action(create_gadget.clone())
        .await
        .expect("define mapping action");
    assert_eq!(
        o.get_action(&ActionName("createGadget".into())).await.unwrap(),
        create_gadget,
        "action round-trips with binds + constant assignments",
    );

    // Redefining with an empty mapping clears binds + assignments (upsert replaces both).
    o.define_action(ActionDef {
        name: ActionName("createGadget".into()),
        target: tn("Gadget"),
        parameters: vec![ParamDef {
            name: "id".into(),
            ty: "Long".into(),
            required: true,
            binds: None,
        }],
        kind: ActionKind::Insert,
        assignments: vec![],
    })
    .await
    .expect("redefine mapping action");
    let redef = o.get_action(&ActionName("createGadget".into())).await.unwrap();
    assert!(redef.assignments.is_empty(), "redefine clears assignments");
    assert_eq!(redef.parameters.len(), 1, "redefine replaces parameters");
    assert_eq!(redef.parameters[0].binds, None, "redefine clears binds");
```

The `serde_json` crate is already a testkit dep (it constructs JSON elsewhere); if not, add `"//third-party:serde_json"` to the `testkit` library `deps` in `src/control-plane/testkit/BUCK`.

- [ ] **Step 2: Run the memory + postgres contract to see the postgres side fail**

Run: `buck2 test //src/control-plane/memory:ontology 2>&1 | tail -20`
Expected: PASS (memory stores the whole `ActionDef`, so binds+assignments already round-trip).
Run (routes to RE for the non-root postgres boot in this cloud session): `buck2 test //src/control-plane/postgres:ontology 2>&1 > /tmp/pg.log 2>&1; grep -E "Tests finished|FAIL|assert" /tmp/pg.log`
Expected: FAIL — postgres neither persists `binds` nor the assignments, so the round-trip assertion mismatches (or the `query!` for a not-yet-existing column fails the build).

- [ ] **Step 3: Add the migration**

Create `src/control-plane/postgres/migrations/0022_action_binds_assignments.sql`:

```sql
-- Custom-logic actions slice 1: declarative param->property mapping.

-- `binds` lets an action parameter write a differently-named property (rename).
-- NULL means the parameter binds the property of its own name (back-compatible).
alter table ontology.action_param
    add column binds text;

-- Constant assignments fill a property with a declared constant when no parameter
-- supplies it (the default/fixed-value case). `value` is the JSON wire form of the
-- scalar constant (the canonical representation the write path coerces to the
-- property's logical type). Ordered by `ordinal`.
create table ontology.action_assignment (
    action_name text  not null references ontology.action (name) on delete cascade,
    ordinal     int   not null,
    property    text  not null,
    value       jsonb not null,
    primary key (action_name, ordinal)
);
```

- [ ] **Step 4: Persist and read the new columns in the adapter**

In `src/control-plane/postgres/src/ontology.rs`, `define_action`:

Extend the param-insert loop to carry `binds`:

```rust
    for (i, p) in action.parameters.iter().enumerate() {
        sqlx::query!(
            "insert into ontology.action_param (action_name, ordinal, name, ty, required, binds) \
             values ($1, $2, $3, $4, $5, $6)",
            action.name.0,
            i as i32,
            p.name,
            p.ty,
            p.required,
            p.binds,
        )
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
    }
```

After that loop (still inside the transaction), replace the assignments wholesale (upsert semantics, mirroring the param delete-then-insert):

```rust
    sqlx::query!(
        "delete from ontology.action_assignment where action_name = $1",
        action.name.0,
    )
    .execute(&mut *tx)
    .await
    .map_err(backend)?;
    for (i, a) in action.assignments.iter().enumerate() {
        sqlx::query!(
            "insert into ontology.action_assignment (action_name, ordinal, property, value) \
             values ($1, $2, $3, $4)",
            action.name.0,
            i as i32,
            a.property,
            a.value,
        )
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
    }
```

In `get_action`, extend the param select to read `binds` and reconstruct it, then read the assignments:

```rust
    let params = sqlx::query!(
        "select name, ty, required, binds from ontology.action_param \
         where action_name = $1 order by ordinal",
        name.0,
    )
    .fetch_all(&self.pool)
    .await
    .map_err(backend)?;
    let assignment_rows = sqlx::query!(
        "select property, value from ontology.action_assignment \
         where action_name = $1 order by ordinal",
        name.0,
    )
    .fetch_all(&self.pool)
    .await
    .map_err(backend)?;
    // ... existing `kind` decode ...
    Ok(ActionDef {
        name: name.clone(),
        target: TypeName(row.target_type),
        parameters: params
            .into_iter()
            .map(|r| ParamDef {
                name: r.name,
                ty: r.ty,
                required: r.required,
                binds: r.binds,
            })
            .collect(),
        kind,
        assignments: assignment_rows
            .into_iter()
            .map(|r| ConstAssignment {
                property: r.property,
                value: r.value,
            })
            .collect(),
    })
```

Add `ConstAssignment` to the `use control_plane_core::{…}` import at the top of `postgres/src/ontology.rs`.

> **sqlx typing:** `value jsonb not null` is read back as `serde_json::Value` (non-`Option`), exactly like `lineage.event.payload` (`lineage.rs`, read with no override). If `cargo sqlx prepare` infers `Option<serde_json::Value>` for the select, add the column override `value as "value!: serde_json::Value"`. The nullable `binds text` correctly infers `Option<String>`.

- [ ] **Step 5: Regenerate the `.sqlx` cache and confirm it builds offline**

Run: `bash tools/sqlx-prepare-cloud.sh 2>&1 | tail -5`
Expected: `query data written to .sqlx …`. New `query-*.json` files appear under `src/control-plane/postgres/.sqlx/`.
Run: `buck2 build //src/control-plane/postgres:postgres 2>&1 | tail -5`
Expected: builds clean offline against the new cache.

- [ ] **Step 6: Run the contract on both adapters + the sqlx freshness gate**

Run: `buck2 test //src/control-plane/memory:ontology //src/control-plane/postgres:ontology //src/control-plane/postgres:sqlx-cache-check > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t2.log`
Expected: all PASS.

- [ ] **Step 7: Commit**

```bash
git add src/control-plane/postgres/migrations/0022_action_binds_assignments.sql \
        src/control-plane/postgres/src/ontology.rs \
        src/control-plane/postgres/.sqlx \
        src/control-plane/testkit/src/lib.rs src/control-plane/testkit/BUCK
git commit -m "feat(ontology): persist action binds + constant assignments in postgres"
```

---

## Task 3: query-api generalized conformance

**Files:**
- Modify: `src/services/query-api/src/action.rs` (`check_param_property_types` ~117-151, `check_insert_conformance` ~155-188, `check_mutate_conformance` ~194-240)
- Modify: `src/services/query-api/src/params.rs` (add `validate_const`)
- Modify: `src/services/query-api/tests/action_conformance.rs`, `tests/mutate_conformance.rs`

**Interfaces:**
- Consumes: `ParamDef::binds_property`, `ActionDef.assignments`, `ConstAssignment` (Task 1); `resolve_logical` (core); `parse_value` (params.rs, private).
- Produces:
  - `params::validate_const(property: &str, logical_ty: &str, value: &serde_json::Value) -> Result<(), ParamError>` — a define-time guard reusing `parse_value` (a constant must be a JSON scalar coercible to the property's logical type).
  - Generalized `check_conformance` accepting `binds` + `assignments` with these rules (all surfaced as `ActionError::Misconfigured` with a message naming every violation):
    1. every param's **bound property** (`binds_property`) is a real property of the target, and the param's `ty` is `resolve_logical`-compatible with that property's `ty`;
    2. every assignment's `property` is a real property, and its `value` passes `validate_const` against that property's logical type;
    3. **no double-bind:** no property is written by two params, nor by a param and a constant, nor by two constants;
    4. **required coverage** (Insert): every required property is covered by exactly one of {a required param binding it, a constant assignment};
    5. Update: the identity property must be bound by a **required** param; Delete: identity bound by a required param, no other params, **and no assignments**.

- [ ] **Step 1: Write the failing conformance tests**

In `src/services/query-api/tests/action_conformance.rs`, add (adapting to the file's existing helpers/imports — it already builds `ObjectType`/`ActionDef` and calls `check_conformance`):

```rust
// Helper mirrors the file's existing style; if one already exists, reuse it.
fn gadget() -> control_plane_core::ObjectType {
    use control_plane_core::{ObjectType, PropertyDef, TableRef, TypeName};
    ObjectType {
        name: TypeName("Gadget".into()),
        properties: vec![
            PropertyDef { name: "id".into(), ty: "Long".into(), required: true },
            PropertyDef { name: "name".into(), ty: "String".into(), required: false },
            PropertyDef { name: "status".into(), ty: "String".into(), required: false },
        ],
        derived: vec![],
        table: TableRef { schema: "main".into(), name: "gadget".into() },
        identity: None,
    }
}

fn p(name: &str, ty: &str, required: bool, binds: Option<&str>) -> control_plane_core::ParamDef {
    control_plane_core::ParamDef { name: name.into(), ty: ty.into(), required, binds: binds.map(str::to_string) }
}

fn insert_action(params: Vec<control_plane_core::ParamDef>, assignments: Vec<control_plane_core::ConstAssignment>) -> control_plane_core::ActionDef {
    control_plane_core::ActionDef {
        name: control_plane_core::ActionName("a".into()),
        target: control_plane_core::TypeName("Gadget".into()),
        parameters: params,
        kind: control_plane_core::ActionKind::Insert,
        assignments,
    }
}

#[test]
fn rename_and_constant_conform() {
    // `displayName` binds `name`; `id` covered by a required param; `status` filled by a constant.
    let a = insert_action(
        vec![p("id", "Long", true, None), p("displayName", "String", false, Some("name"))],
        vec![control_plane_core::ConstAssignment { property: "status".into(), value: serde_json::json!("active") }],
    );
    query_api::action::check_conformance(&a, &gadget()).expect("conforms");
}

#[test]
fn required_property_covered_by_constant_conforms() {
    // A REQUIRED property (`id`) covered ONLY by a constant is valid coverage.
    let a = insert_action(
        vec![],
        vec![control_plane_core::ConstAssignment { property: "id".into(), value: serde_json::json!("7") }],
    );
    query_api::action::check_conformance(&a, &gadget()).expect("constant covers required");
}

#[test]
fn binds_unknown_property_rejected() {
    let a = insert_action(vec![p("id", "Long", true, None), p("x", "String", false, Some("nope"))], vec![]);
    assert!(matches!(query_api::action::check_conformance(&a, &gadget()), Err(query_api::action::ActionError::Misconfigured(_))));
}

#[test]
fn constant_type_mismatch_rejected() {
    // `status` is String; a bool constant is incompatible.
    let a = insert_action(
        vec![p("id", "Long", true, None)],
        vec![control_plane_core::ConstAssignment { property: "status".into(), value: serde_json::json!(true) }],
    );
    assert!(matches!(query_api::action::check_conformance(&a, &gadget()), Err(query_api::action::ActionError::Misconfigured(_))));
}

#[test]
fn double_bind_param_and_constant_rejected() {
    // `name` bound by both a param and a constant.
    let a = insert_action(
        vec![p("id", "Long", true, None), p("name", "String", false, None)],
        vec![control_plane_core::ConstAssignment { property: "name".into(), value: serde_json::json!("x") }],
    );
    assert!(matches!(query_api::action::check_conformance(&a, &gadget()), Err(query_api::action::ActionError::Misconfigured(_))));
}

#[test]
fn double_bind_two_params_rejected() {
    let a = insert_action(
        vec![p("id", "Long", true, None), p("a", "String", false, Some("name")), p("b", "String", false, Some("name"))],
        vec![],
    );
    assert!(matches!(query_api::action::check_conformance(&a, &gadget()), Err(query_api::action::ActionError::Misconfigured(_))));
}

#[test]
fn uncovered_required_property_rejected() {
    // `id` (required) covered by neither a param nor a constant.
    let a = insert_action(vec![p("displayName", "String", false, Some("name"))], vec![]);
    assert!(matches!(query_api::action::check_conformance(&a, &gadget()), Err(query_api::action::ActionError::Misconfigured(_))));
}
```

> Confirm the crate name (`query_api`) and that `check_conformance`, `ActionError` are `pub` and reachable from the test (they are used by existing `action_conformance.rs`; mirror its exact import path). If the existing file already defines `gadget`/`p`/`insert_action`-style helpers, reuse them instead of redefining.

In `src/services/query-api/tests/mutate_conformance.rs`, extend the imports to add `ConstAssignment` (to `use control_plane_core::{…}`) and `ActionError` (to `use query_api::action::{…}`), then add these tests. They reuse the file's existing `widget(identity)` helper (properties `sku:String` required — the identity — and `qty:Long` optional):

```rust
#[test]
fn update_identity_via_binds_conforms() {
    // Identity `sku` is bound by a required param renamed to `key`; `quantity` renames `qty`.
    let action = ActionDef {
        name: ActionName("upd".into()),
        target: TypeName("Widget".into()),
        parameters: vec![
            ParamDef { name: "key".into(), ty: "String".into(), required: true, binds: Some("sku".into()) },
            ParamDef { name: "quantity".into(), ty: "Long".into(), required: false, binds: Some("qty".into()) },
        ],
        kind: ActionKind::Update,
        assignments: vec![],
    };
    check_conformance(&action, &widget(Some("sku"))).expect("update conforms via binds");
}

#[test]
fn delete_with_assignment_rejected() {
    // Delete takes only the identity param; a constant assignment is a misconfiguration.
    let action = ActionDef {
        name: ActionName("del".into()),
        target: TypeName("Widget".into()),
        parameters: vec![ParamDef {
            name: "key".into(),
            ty: "String".into(),
            required: true,
            binds: Some("sku".into()),
        }],
        kind: ActionKind::Delete,
        assignments: vec![ConstAssignment { property: "qty".into(), value: serde_json::json!(1) }],
    };
    assert!(matches!(
        check_conformance(&action, &widget(Some("sku"))),
        Err(ActionError::Misconfigured(_))
    ));
}
```

(The `serde_json` dep is already available to the query-api tests; if the target lacks it, add `"//third-party:serde_json"` to the `mutate-conformance` target's `deps`.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `buck2 test //src/services/query-api:action-conformance //src/services/query-api:mutate-conformance > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t3.log`
Expected: FAIL — the new cases don't yet hold (old conformance ignores `binds`/assignments; e.g. `binds_unknown_property_rejected` currently *passes* the old `name`-match because `displayName` matches no property → already rejected, but `rename_and_constant_conform` FAILS because `displayName` matches no property under the old rule).

- [ ] **Step 3: Add `validate_const` in params.rs**

In `src/services/query-api/src/params.rs`, add (reusing the private `parse_value`):

```rust
/// Define-time guard for a constant assignment: the JSON `value` must be a scalar (not
/// null/array/object) coercible to the property's logical type — the same acceptance the
/// write path applies via `parse_value`. Keeps the constant and the runtime coercion in
/// lockstep (a constant that conforms here cannot fail the write-path coercion later).
pub fn validate_const(property: &str, logical_ty: &str, value: &Value) -> Result<(), ParamError> {
    if value.is_null() || value.is_array() || value.is_object() {
        return Err(ParamError::BadValue(
            property.to_string(),
            "constant must be a scalar (string, number, or bool)".into(),
        ));
    }
    parse_value(property, logical_ty, value).map(|_| ())
}
```

- [ ] **Step 4: Generalize the conformance functions in action.rs**

Rewrite `check_param_property_types` to resolve each param through its **bound property** and add constant + double-bind + required-coverage checks. Concretely:

```rust
fn check_param_property_types(action: &ActionDef, target: &ObjectType, violations: &mut Vec<String>) {
    let target_name = &target.name.0;
    for prm in &action.parameters {
        let bound = prm.binds_property();
        match target.properties.iter().find(|prop| prop.name == bound) {
            None => violations.push(format!(
                "parameter `{}` binds property `{bound}`, which is not a property of type `{target_name}`",
                prm.name
            )),
            Some(prop) => {
                let prop_base = resolve_logical(&prop.ty);
                let param_base = resolve_logical(&prm.ty);
                if prop_base.is_none() {
                    violations.push(format!("property `{}` of type `{target_name}` has unknown logical type `{}`", prop.name, prop.ty));
                } else if param_base.is_none() {
                    violations.push(format!("parameter `{}` has unknown logical type `{}`", prm.name, prm.ty));
                } else if prop_base != param_base {
                    violations.push(format!(
                        "parameter `{}` type `{}` is incompatible with property `{}` type `{}`",
                        prm.name, prm.ty, prop.name, prop.ty
                    ));
                }
            }
        }
    }
}
```

Add a shared helper that both Insert and Update conformance call, checking constants + double-bind:

```rust
/// Validate constant assignments and that no property is written twice (by two params, a
/// param and a constant, or two constants). Appends to `violations`. Returns the set of
/// bound property names (params ∪ constants) for the caller's required-coverage check.
fn check_assignments_and_binds(
    action: &ActionDef,
    target: &ObjectType,
    violations: &mut Vec<String>,
) -> std::collections::HashSet<String> {
    use std::collections::HashSet;
    let target_name = &target.name.0;
    let mut bound: HashSet<String> = HashSet::new();
    let mut mark = |prop: &str, violations: &mut Vec<String>| {
        if !bound.insert(prop.to_string()) {
            violations.push(format!("property `{prop}` of type `{target_name}` is written by more than one parameter/constant"));
        }
    };
    for prm in &action.parameters {
        mark(prm.binds_property(), violations);
    }
    for a in &action.assignments {
        match target.properties.iter().find(|prop| prop.name == a.property) {
            None => violations.push(format!("constant assignment names property `{}`, which is not a property of type `{target_name}`", a.property)),
            Some(prop) => {
                if let Err(e) = crate::params::validate_const(&a.property, &prop.ty, &a.value) {
                    violations.push(format!("constant for property `{}`: {e}", a.property));
                }
            }
        }
        mark(&a.property, violations);
    }
    bound
}
```

> `mark` closes over `bound`; if the borrow checker objects to the `&mut violations` capture, inline the two loops instead of a closure (a small `for` over params then constants, pushing to a local `HashSet` and to `violations`). Keep the behavior identical.

Update `check_insert_conformance` to use the bound set for required coverage:

```rust
fn check_insert_conformance(action: &ActionDef, target: &ObjectType) -> Result<(), ActionError> {
    let target_name = &target.name.0;
    let mut violations: Vec<String> = Vec::new();
    check_param_property_types(action, target, &mut violations);
    let bound = check_assignments_and_binds(action, target, &mut violations);

    for prop in &target.properties {
        if prop.required {
            // Covered by a required param binding it, or by a constant.
            let by_required_param = action.parameters.iter().any(|prm| prm.binds_property() == prop.name && prm.required);
            let by_constant = action.assignments.iter().any(|a| a.property == prop.name);
            let by_optional_param = action.parameters.iter().any(|prm| prm.binds_property() == prop.name && !prm.required);
            if !by_required_param && !by_constant {
                if by_optional_param {
                    violations.push(format!("required property `{}` of type `{target_name}` is covered only by an optional parameter (it could be omitted, writing NULL)", prop.name));
                } else {
                    violations.push(format!("required property `{}` of type `{target_name}` is not covered by any parameter or constant", prop.name));
                }
            }
        }
    }
    let _ = bound;
    finish(action, target_name, violations)
}
```

Add a small `finish` helper to DRY the `if violations.is_empty()` epilogue shared by both conformance fns (or keep the inline blocks — reviewer's call; prefer the helper):

```rust
fn finish(action: &ActionDef, target_name: &str, violations: Vec<String>) -> Result<(), ActionError> {
    if violations.is_empty() {
        Ok(())
    } else {
        Err(ActionError::Misconfigured(format!(
            "action `{}` does not conform to type `{target_name}`: {}",
            action.name.0,
            violations.join("; ")
        )))
    }
}
```

Update `check_mutate_conformance`: run `check_param_property_types` + `check_assignments_and_binds`; resolve the identity param as the one whose **bound property** equals `target.identity`; for Delete, additionally reject any param binding a non-identity property **and** any assignment:

```rust
    // identity must be bound by a required param
    match &target.identity {
        None => violations.push(format!("type `{target_name}` has no declared identity; UPDATE/DELETE require one")),
        Some(idprop) => {
            match action.parameters.iter().find(|prm| prm.binds_property() == idprop) {
                None => violations.push(format!("UPDATE/DELETE on `{target_name}` requires a parameter binding the identity property `{idprop}`")),
                Some(prm) if !prm.required => violations.push(format!("identity parameter `{}` must be required", prm.name)),
                Some(_) => {}
            }
            if !is_update {
                for prm in &action.parameters {
                    if prm.binds_property() != idprop {
                        violations.push(format!("DELETE on `{target_name}` takes only the identity parameter; `{}` is extra", prm.name));
                    }
                }
                if !action.assignments.is_empty() {
                    violations.push(format!("DELETE on `{target_name}` takes no constant assignments"));
                }
            }
        }
    }
```

- [ ] **Step 5: Run the conformance tests to verify they pass**

Run: `buck2 test //src/services/query-api:action-conformance //src/services/query-api:mutate-conformance //src/services/query-api:action-conformance-handler //src/services/query-api:action-conformance-http > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t3.log`
Expected: all PASS (existing conformance tests still hold — `binds_property()` on a `None` binds is the param name, so old actions behave identically).

- [ ] **Step 6: Clippy + commit**

Run: `tools/clippy-all.sh 2>&1 | tail -20` — resolve any findings (e.g. a closure capturing `&mut` may need inlining).

```bash
git add src/services/query-api/src/action.rs src/services/query-api/src/params.rs \
        src/services/query-api/tests/action_conformance.rs src/services/query-api/tests/mutate_conformance.rs
git commit -m "feat(query-api): generalize action conformance through binds + constants"
```

---

## Task 4: query-api write-path resolution from the mapping

**Files:**
- Modify: `src/services/query-api/src/params.rs` (add `resolve_action_row`)
- Modify: `src/services/query-api/src/action.rs` (`run_insert` ~299-422, `run_mutate` ~483-… — replace `parse_params(&action.parameters, body)` at lines 310 and 499)
- Modify: `src/services/query-api/tests/params.rs`

**Interfaces:**
- Consumes: `parse_params` (existing), `parse_value` (private, params.rs), `ParamDef::binds_property`, `ActionDef.assignments`, `ObjectType`.
- Produces: `params::resolve_action_row(action: &ActionDef, target: &ObjectType, body: &serde_json::Map<String, Value>) -> Result<Vec<(String, SqlValue)>, ParamError>` — ordered `(property, SqlValue)` pairs: every param remapped to its **bound property** (omitted optional ⇒ `SqlValue::Null`), then every constant appended as `(property, coerced value)`. Body keys are validated against **param names**; the returned pairs are keyed by **property**, so the unchanged full-row expansion and the ACL gate (which key off property/column names) work as-is.

- [ ] **Step 1: Write the failing resolver tests**

In `src/services/query-api/tests/params.rs`, add these fully-concrete tests. First confirm the crate name and the value/error paths against the file's existing imports (existing `params.rs` tests already `use` `SqlValue` and `ParamError` — reuse whatever paths they use; the paths below assume `query_api::serving::SqlValue` and `query_api::params::{parse_params, ParamError}`, matching the other query-api test files).

```rust
use control_plane_core::{
    ActionDef, ActionKind, ActionName, ConstAssignment, ObjectType, ParamDef, PropertyDef,
    TableRef, TypeName,
};
use query_api::serving::SqlValue;

fn gadget() -> ObjectType {
    ObjectType {
        name: TypeName("Gadget".into()),
        properties: vec![
            PropertyDef { name: "id".into(), ty: "Long".into(), required: true },
            PropertyDef { name: "name".into(), ty: "String".into(), required: false },
            PropertyDef { name: "status".into(), ty: "String".into(), required: false },
        ],
        derived: vec![],
        // NOTE: control_plane_core::TableRef fields are `{ schema, name }` — NOT `table`.
        table: TableRef { schema: "main".into(), name: "gadget".into() },
        identity: None,
    }
}

fn param(name: &str, ty: &str, required: bool, binds: Option<&str>) -> ParamDef {
    ParamDef { name: name.into(), ty: ty.into(), required, binds: binds.map(str::to_string) }
}

fn insert(params: Vec<ParamDef>, assignments: Vec<ConstAssignment>) -> ActionDef {
    ActionDef {
        name: ActionName("a".into()),
        target: TypeName("Gadget".into()),
        parameters: params,
        kind: ActionKind::Insert,
        assignments,
    }
}

#[test]
fn resolve_maps_binds_to_property() {
    let action = insert(
        vec![param("id", "Long", true, None), param("displayName", "String", false, Some("name"))],
        vec![],
    );
    let mut body = serde_json::Map::new();
    body.insert("id".into(), serde_json::json!("7"));
    body.insert("displayName".into(), serde_json::json!("Widget A"));
    let pairs = query_api::params::resolve_action_row(&action, &gadget(), &body).expect("resolve");
    // keyed by PROPERTY, not param name:
    assert!(pairs.iter().any(|(c, v)| c == "name" && *v == SqlValue::Text("Widget A".into())));
    assert!(pairs.iter().all(|(c, _)| c != "displayName"));
}

#[test]
fn resolve_appends_constants() {
    let action = insert(
        vec![param("id", "Long", true, None)],
        vec![ConstAssignment { property: "status".into(), value: serde_json::json!("active") }],
    );
    let mut body = serde_json::Map::new();
    body.insert("id".into(), serde_json::json!("7"));
    let pairs = query_api::params::resolve_action_row(&action, &gadget(), &body).expect("resolve");
    assert!(pairs.iter().any(|(c, v)| c == "status" && *v == SqlValue::Text("active".into())));
}

#[test]
fn resolve_back_compat_no_binds_no_constants() {
    // params named for properties, no constants ⇒ identical to parse_params today.
    let action = insert(vec![param("id", "Long", true, None), param("name", "String", false, None)], vec![]);
    let mut body = serde_json::Map::new();
    body.insert("id".into(), serde_json::json!("7"));
    let pairs = query_api::params::resolve_action_row(&action, &gadget(), &body).expect("resolve");
    assert_eq!(pairs.iter().find(|(c, _)| c == "id").map(|(_, v)| v.clone()), Some(SqlValue::Int(7)));
    // omitted optional `name` ⇒ Null pair (matches parse_params behavior).
    assert!(pairs.iter().any(|(c, v)| c == "name" && *v == SqlValue::Null));
}

#[test]
fn resolve_rejects_unknown_body_key() {
    let action = insert(vec![param("id", "Long", true, None)], vec![]);
    let mut body = serde_json::Map::new();
    body.insert("id".into(), serde_json::json!("7"));
    body.insert("nope".into(), serde_json::json!("x"));
    assert!(matches!(
        query_api::params::resolve_action_row(&action, &gadget(), &body),
        Err(query_api::params::ParamError::Unknown(_))
    ));
}
```

Note: `Long` arrives as a JSON **string** (`"7"`) per `JsonRepr::NumericString` and parses to `SqlValue::Int(7)` — copy the exact wire form the existing `parse_params` tests in this file use.

- [ ] **Step 2: Run to verify failure**

Run: `buck2 test //src/services/query-api:params > /tmp/t4.log 2>&1; grep -E "Tests finished|FAIL|error\\[" /tmp/t4.log`
Expected: FAIL to compile — `resolve_action_row` undefined.

- [ ] **Step 3: Implement `resolve_action_row`**

In `src/services/query-api/src/params.rs`:

```rust
use control_plane_core::{ActionDef, ObjectType};

/// Resolve an action invocation body into ordered (property, SqlValue) write pairs, applying
/// the action's param→property mapping (`binds`) and constant assignments. The body is keyed
/// by PARAMETER name; the returned pairs are keyed by the PROPERTY each param binds (or a
/// constant fills). Assumes the action already passed conformance (so constants coerce and no
/// property is double-written). Constants reuse the same `parse_value` coercion — against the
/// PROPERTY's logical type — that parameters use, so a constant is a first-class equal of a param.
pub fn resolve_action_row(
    action: &ActionDef,
    target: &ObjectType,
    body: &serde_json::Map<String, Value>,
) -> Result<Vec<(String, SqlValue)>, ParamError> {
    // Param leg: reuse parse_params (rejects unknown keys, enforces required, coerces by
    // param.ty), then remap each pair from param name → bound property.
    let param_pairs = parse_params(&action.parameters, body)?;
    let mut out: Vec<(String, SqlValue)> = Vec::with_capacity(param_pairs.len() + action.assignments.len());
    for (i, (_, value)) in param_pairs.into_iter().enumerate() {
        // parse_params preserves action.parameters order, so index i is parameters[i].
        let prop = action.parameters[i].binds_property().to_string();
        out.push((prop, value));
    }
    // Constant leg: coerce each constant against its PROPERTY's logical type.
    for a in &action.assignments {
        let prop_ty = target
            .properties
            .iter()
            .find(|p| p.name == a.property)
            .map(|p| p.ty.as_str())
            .ok_or_else(|| ParamError::BadValue(a.property.clone(), "constant names an unknown property".into()))?;
        out.push((a.property.clone(), parse_value(&a.property, prop_ty, &a.value)?));
    }
    Ok(out)
}
```

> `action.parameters[i]` indexing is inside `params.rs`; if the strict `indexing_slicing` clippy lint fires on production code here, use `action.parameters.get(i)` with an `ok_or_else(|| ParamError::…)` or restructure to `zip` the param refs with `parse_params`' output. Prefer `zip`: `for (prm, (_, value)) in action.parameters.iter().zip(param_pairs) { out.push((prm.binds_property().to_string(), value)); }` — no indexing, and it makes the "same order" contract explicit.

- [ ] **Step 4: Wire it into the write path**

In `src/services/query-api/src/action.rs`, `run_insert` (line ~310) replace:

```rust
    let pairs = parse_params(&action.parameters, body)?;
```

with:

```rust
    let pairs = crate::params::resolve_action_row(action, target, body)?;
```

The rest of `run_insert` is unchanged: `set_columns/set_values` filter non-null pairs (constants are non-null ⇒ counted as "set", so a denied/constant-filled column is still gated — correct), and the full-row expansion looks up `parsed.get(prop.name)` by property (now the pairs are property-keyed, so this resolves correctly for renamed/constant columns). The returned `ObjectRows.columns` are now property names (correct — the created object's columns are its properties; identical to before for non-renamed actions).

In `run_mutate` (line ~499) replace the same `parse_params(&action.parameters, body)?` with `crate::params::resolve_action_row(action, target, body)?`. `run_mutate` already locates the identity pair by property name (`c == &idprop`) and filters `set_pairs` by property name, so property-keyed pairs drop in unchanged. (For Delete, conformance guarantees no assignments and only the identity param, so `resolve_action_row` returns just the identity pair.)

- [ ] **Step 5: Run the resolver + existing action tests**

Run: `buck2 test //src/services/query-api:params //src/services/query-api:action-conformance //src/services/query-api:mutate-conformance > /tmp/t4.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t4.log`
Expected: PASS.

- [ ] **Step 6: Clippy + commit**

Run: `tools/clippy-all.sh 2>&1 | tail -20` — resolve findings (favor the `zip` form to avoid indexing).

```bash
git add src/services/query-api/src/params.rs src/services/query-api/src/action.rs src/services/query-api/tests/params.rs
git commit -m "feat(query-api): resolve action write row from the param->property mapping"
```

---

## Task 5: End-to-end through the fixture action path

**Files:**
- Create: `src/services/query-api/tests/action_mapping_e2e.rs`
- Modify: `src/services/query-api/BUCK` (add a `loom_fixture_test` target)

**Interfaces:**
- Consumes: the full path (Tasks 1-4) + the `e2e-support` library (`//src/services/query-api:e2e-support`).
- **Study `tests/action_e2e.rs` — it is the exact template.** That test does NOT go over HTTP; it drives the action path **in-process** by calling `query_api::action::run_action(name, body.as_object().unwrap(), &subject, &deps)` directly, where `deps: ActionDeps { cp, action_engine: &engine, serving: &serving }`. It builds the engine with `e2e_support::spawn_engine_writer(&fx, &db, warehouse.path(), …)` and an `InProcessServingEngine::new(IcebergCatalog::new(pool.clone()))`, reads back with `query_api::handler::read_object(...)`, and asserts denials via `run_action(...).await.unwrap_err()` matching `ActionError::Forbidden` / `ActionError::WriteDenied(_)`. **Mirror this pattern exactly** — do NOT invent an HTTP/router/`get`/`StubAction` path (those belong to the read/governance tests, not the action write path).

- [ ] **Step 1: Write the failing e2e tests**

Create `src/services/query-api/tests/action_mapping_e2e.rs`, mirroring `action_e2e.rs`'s setup boilerplate verbatim (the fixture boot, `spawn_engine_writer`, `InProcessServingEngine`, `ActionDeps`, ACL grant of `Action::Write` to the subject, and `read_object` readback). Copy its imports and helper usage. Implement four `#[tokio::test]`s:

```rust
// Shared setup per test mirrors action_e2e.rs. `subj` is an ACL subject GRANTED Action::Write
// on the target type (copy the grant helper action_e2e.rs uses). Seed a "Gadget" type with
// properties id:Long (identity for the Update case), name:String, status:String, bound to a
// real Iceberg table via the fixture. Then:

// TEST 1 — rename: define an Insert action `createGadget` with params
//   [ id:Long required (binds None), displayName:String optional binds "name" ]
// run_action("createGadget", json!({"id":"7","displayName":"Widget A"}).as_object().unwrap(), &subj, &deps)
//   -> Ok. read_object(Gadget) -> the row has name == "Widget A" (written via the bound property),
//   and no "displayName" column exists on the object.

// TEST 2 — constant fill: define an Insert action `makeGadget` with params
//   [ displayName:String optional binds "name" ] and assignments
//   [ {property:"id", value: json!("7")}, {property:"status", value: json!("active")} ]
//   (a REQUIRED property `id` covered ONLY by a constant).
// run_action("makeGadget", json!({"displayName":"Widget B"}).as_object().unwrap(), &subj, &deps)
//   -> Ok. read_object -> row has id == 7 AND status == "active" (both constant-filled).

// TEST 3 — gate composition: grant the subject Action::Write but attach a Write policy that
//   DENIES the `status` column (copy the deny-column policy shape from a governance test, e.g.
//   update_delete_governance_e2e.rs / write_denial_http.rs). Then:
//   - run_action on an action that fills `status` via a CONSTANT -> unwrap_err() matches
//     ActionError::WriteDenied(WriteDenialReason::Column(c)) with c == "status";
//   - run_action on an action that fills `status` via a renamed PARAM -> the SAME denial.
//   This proves the resolved row (constant- or param-filled) is gated identically to a direct insert.

// TEST 4 — update mapping: seed a landed Gadget row (via TEST 1's action or a direct insert).
//   Define an Update action `renameGadget` on Gadget (identity "id") with params
//   [ key:Long required binds "id", displayName:String optional binds "name" ].
//   run_action("renameGadget", json!({"key":"7","displayName":"Renamed"}).as_object().unwrap(), &subj, &deps)
//   -> Ok. read_object -> the row with id 7 now has name == "Renamed" (PATCH via the bound property).
```

Fill each in concretely against `action_e2e.rs`'s actual API (the exact `spawn_engine_writer` arity, the grant helper, and `read_object`'s `QueryDeps`/`ObjectQuery` args are all visible there — copy them). Use `SqlValue`/JSON readback assertions exactly as `action_e2e.rs` does.

- [ ] **Step 2: Wire the BUCK target**

Add to `src/services/query-api/BUCK` (mirror the existing `action-e2e` target at `BUCK:823`, copying its `deps` verbatim — it already includes `":e2e-support"`):

```starlark
loom_fixture_test(
    name = "action-mapping-e2e",
    crate = "action_mapping_e2e",
    srcs = ["tests/action_mapping_e2e.rs"],
    crate_root = "tests/action_mapping_e2e.rs",
    deps = [
        # copy the exact dep list from the `action-e2e` target (BUCK:823), verbatim.
    ],
)
```

- [ ] **Step 3: Run to verify failure, then iterate to green**

Run (routes to RE for the non-root postgres boot): `buck2 test //src/services/query-api:action-mapping-e2e > /tmp/t5.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t5.log`
Expected: initially compiles/runs; assertions drive any remaining fixes. Iterate until PASS.

- [ ] **Step 4: Commit**

```bash
git add src/services/query-api/tests/action_mapping_e2e.rs src/services/query-api/BUCK
git commit -m "test(query-api): e2e for action param->property mapping"
```

---

## Task 6: Full-suite gate + close the register item

**Files:**
- Modify: `docs/ROADMAP.md` (via `loom-docs-update`)

- [ ] **Step 1: Run the whole first-party suite**

Run: `buck2 test //src/... > /tmp/all.log 2>&1; grep -E "Tests finished|FAIL" /tmp/all.log`
Expected: all PASS (fixtures route to RE for the non-root postgres boot in this cloud session).

- [ ] **Step 2: Lint the whole tree**

Run: `buck2 run //tools:prek -- run --all-files 2>&1 | tail -30`
Expected: green; commit anything the hooks auto-fix.

- [ ] **Step 3: Close the register item**

Use `loom-docs-update` to flip `road-action-param-mapping` in `docs/ROADMAP.md` from `- [ ]`→`- [x]`, set `status:done`, and set `pr:#<N>` once the PR number is known. Record any newly-deferred follow-ups it referenced ([[fut-action-computed-assignments]], [[fut-action-multi-object]], [[fut-action-enqueue-downstream]]) if not already present.

- [ ] **Step 4: Commit**

```bash
git add docs/ROADMAP.md
git commit -m "docs(ontology): close road-action-param-mapping"
```

---

## Notes for the executor

- **Scope discipline:** declarative mapping only. No computed expressions, no multi-object, no enqueue, no bespoke code — those are the sequenced follow-ons and must not leak in.
- **Governance is unchanged:** the mapping sits *before* the ACL `write_filter` and the snapshot+lineage commit. Only row construction changes; the gates run on the resolved row exactly as on a direct insert. Task 5's gate-composition test is the proof.
- **`binds` defaults to name** and `assignments` defaults empty (`#[serde(default)]`), so every existing action is byte-for-byte unchanged — this is what makes the change additive.
- **sqlx cache is a real artifact:** never hand-edit `.sqlx/`. Regenerate with `tools/sqlx-prepare-cloud.sh` after any `query!` change and commit the result; `sqlx-cache-check` will fail the suite otherwise.
