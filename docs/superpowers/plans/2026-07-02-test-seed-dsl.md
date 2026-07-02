# road-test-seed-dsl Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A fluent seed-builder DSL for `ObjectType`/`ActionDef` in
`control_plane_core` (plain data assembly — production-legit, no I/O, zero new
deps), plus its proving ground: the **update_delete e2e family migration**. The
three test files' ~130-line private `define_widget`/`grant_writer` copies (the
duplication register's top-6 pairs, 91–127 lines each — the
finished-promotion/unfinished-migration anti-pattern) are deleted in favor of the
already-promoted `e2e_support` helpers; `e2e_support` gains
`grant_writer_role(...) -> (SubjectId, RoleId)` (absorbing the governance file's
one signature delta) and the promoted `read_widget`; then `e2e_support`'s own
`define_widget` (and the governance file's `vector_guard` literals) are rewritten
over the new builders. Proof: a duplication diff shows the six update_delete
pairs cleared.

**Architecture:**

- **Builders live in core, next to the types** (`src/control-plane/core/src/ontology.rs`).
  Rationale: the literal appears across 4+ crates (query-api tests, testkit
  contracts, ingest tests, postgres/engine-serving vector tests), and testkit
  itself would be a consumer — so the only home upstream of *all* of them is
  core. Construction is plain data assembly (no validation, no I/O — the "No
  I/O" charter holds; `define_type` keeps owning validation exactly as it does
  for a struct literal), so the builders are legitimate production API, not
  test-only scaffolding. Zero dependency changes: `ConstAssignment` already
  pulls `serde_json` into core.
- **Method set, derived from the full field inventory** (`ontology.rs:26-49`
  `PropertyDef`/`ObjectType`, `:160-219` `ParamDef`/`ActionDef`):
  - `ObjectType::build(name, (schema, table)) -> ObjectTypeBuilder`; then
    `.prop(name, ty)` (optional, unconstrained), `.prop_req(name, ty)`
    (required, unconstrained), `.prop_with(name, ty, required, constraints)`
    (the full `PropertyDef` surface — the **constraints hook**, needed by
    testkit's `Account` contract literal with range/length constraints),
    `.derived(DerivedPropertyDef)` (passthrough **derived hook** — the
    `DerivedPropertyDef` literal is already minimal and rare, e.g. ingest's
    `bind_validation.rs::customer_with`; no aggregate mini-DSL), `.identity(prop)`,
    `.done() -> ObjectType`.
  - `ActionDef::build(name, target, kind) -> ActionDefBuilder`; then
    `.param(name, ty)` (optional, `binds: None`), `.param_req(name, ty)`,
    `.param_bound(name, ty, required, binds)` (the **binds hook**, covers
    `action_mapping`-style renamed params), `.assign(property, json_value)`
    (the **assignments hook**, `ConstAssignment`), `.done() -> ActionDef`.
  - **`LinkDef` builder is an explicit non-goal**: a `LinkDef` is 5 flat fields
    whose weight is the `LinkBacking` enum literal a builder can't compress,
    and no duplication-register pair is LinkDef-driven.
- **`done()` over `From`/`Into`.** The spec sketch uses `done()`; call sites
  pass the result to `define_type(...)`/`define_action(...)` which take the
  struct by value, so `Into` would still need an explicit `.into()` of the same
  length while being less greppable and hiding the terminator in a trait impl.
  `done()` is the single, discoverable finish line. (No `From<Builder>` impl —
  YAGNI; add later without breakage if a use appears.)
- **Clippy posture (core = full pedantic+restriction):** `impl Into<String>`
  params and consume-self-return-Self methods are safe — `impl_trait_in_params`,
  `must_use_candidate`, and `return_self_not_must_use` are all in the toolchain
  `CLIPPY_ALLOWS` (`toolchains/BUCK:233,240,243`). Every public item gets a doc
  comment (repo style).
- **e2e_support keeps exporting `prop()` and `tref()` unchanged** — `prop` has
  ~90 call sites across 4+ other test files (`object_set_e2e`, `association_e2e`,
  `http_wire_e2e`, `ontology_types_e2e`, the `setup` fixture); sweeping them is
  the declared non-goal. `define_widget`/`grant_writer` keep their exact
  signatures (pinned by consumers `action_client_wire.rs`,
  `overwrite_table_e2e.rs`, `iceberg_action_e2e.rs`); `grant_writer` becomes a
  one-line delegate to the new `grant_writer_role`.
- **`read_widget` is promoted too.** The 127-line top pair
  (`update_delete_e2e.rs:26-196` ↔ `update_delete_tiers_e2e.rs:25-197`) spans
  define_widget + grant_writer + a byte-identical ~32-line `read_widget` helper;
  deleting only the first two would leave a >20-line residual pair. The
  governance file has no `read_widget` (it asserts on `run_action` errors only).
- **Scope discipline (explicit):** do **NOT** sweep the other ~76
  `ObjectType {`-literal files tree-wide (testkit contracts, vector tests,
  ingest tests, `ingest/src/model.rs`'s programmatic construction). The register
  item's value is the DSL + the top-6-pairs migration; incremental adoption
  rides with `road-test-wire-harness` and future touches.

**Tech Stack:** Rust (edition 2024), buck2, `loom_fixture_test` (hermetic,
shared Postgres fixture), `//tools:lucidshark-duplo` + `//tools:jq` for the
proof metric. No Cargo.toml/lockfile/reindeer changes anywhere in this plan.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-07-02-pillar-idioms-audit-design.md`
  (§ "road-test-seed-dsl"); register: `docs/ROADMAP.md#road-test-seed-dsl`.
- **TDD:** the builder unit tests are written first and observed RED (compile
  error — the builders don't exist) before any implementation.
- Tests are separate `rust_test` targets, never inline `#[cfg(test)]` (the
  `no-inline-tests` hook enforces this). The new builder test is pure-logic
  (runs on RE); no new fixture tests are created.
- **Never pipe `buck2 test`/`buck2 bxl` through `tail`/`head`** — redirect to a
  file and grep it.
- `buck2 run //tools:prek -- run --all-files` must report zero `Failed` before
  **every** commit; conventional-commit message format.
- **`.sqlx` untouched expected**; `third-party/BUCK` untouched expected (zero
  dependency changes).
- Behavior parity is structural: builders assemble the *identical* structs the
  literals did (the unit tests assert builder output `==` handwritten literal),
  so every migrated e2e keeps asserting the same seeded world.

## Behavior pinned by existing tests (must NOT change)

- `//src/services/query-api:action-client-wire` — the ONLY existing consumer of
  `e2e_support::{define_widget, grant_writer}` (verified by grep); the builder
  rewrite and the `grant_writer` delegation must be drop-in for it
  (`-> TypeName` / `-> SubjectId`, `Widget` @ `main.widget`; `id Long req
  identity`, `name String`, `qty Long`; create/update/delete actions).
  NOTE: `:overwrite-table-e2e` and `:iceberg-action-e2e` carry PRIVATE
  reduced-shape copies (`id`+`name`, create-only) — they are NOT migrated by
  this item (their 71-line mutual pair, census line 32, survives as documented
  follow-up); keep both targets in the verification sweep anyway (cheap), but
  the close-out prose must not claim them.
- `//src/services/query-api:update-delete-e2e`, `:update-delete-tiers-e2e`,
  `:update-delete-governance-e2e` — every test body (PATCH merge semantics,
  time travel, file-tier COW, deny-column, row-filter, vector-guard, NotFound)
  stays byte-identical; only the seed helpers move/shrink.
- `//src/control-plane/core:serde-roundtrip`, `:action_mapping`, `:action_kind`
  — the ontology structs themselves are untouched (builders are additive).

---

### Task 1: Builder DSL in `control_plane_core` (TDD)

**Files:**
- Create: `src/control-plane/core/tests/ontology_builder.rs`
- Modify: `src/control-plane/core/BUCK` (new `rust_test` target, mirroring `:page`)
- Modify: `src/control-plane/core/src/ontology.rs` (builders next to the types)
- Modify: `src/control-plane/core/src/lib.rs:53-56` (extend the `pub use ontology::{…}` list)

**Interfaces:**
- Produces: `ObjectType::build`, `ObjectTypeBuilder` (`prop`, `prop_req`,
  `prop_with`, `derived`, `identity`, `done`), `ActionDef::build`,
  `ActionDefBuilder` (`param`, `param_req`, `param_bound`, `assign`, `done`) —
  all re-exported from the crate root.
- Consumed by Tasks 2–3 (e2e_support, the governance file's `vector_guard`).

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/core/tests/ontology_builder.rs`:

```rust
//! The seed-builder DSL assembles structs identical to handwritten literals —
//! builder output == literal, field for field, including ordering.

use control_plane_core::{
    ActionDef, ActionKind, ActionName, Aggregation, ConstAssignment, DerivedPropertyDef,
    LengthConstraint, ObjectType, ParamDef, PropertyConstraints, PropertyDef, TableRef, TypeName,
};

#[test]
fn object_type_builder_matches_literal() {
    let built = ObjectType::build("Widget", ("main", "widget"))
        .prop_req("id", "Long")
        .prop("name", "String")
        .prop("qty", "Long")
        .identity("id")
        .done();
    let literal = ObjectType {
        name: TypeName("Widget".into()),
        table: TableRef {
            schema: "main".into(),
            name: "widget".into(),
        },
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
                constraints: PropertyConstraints::default(),
            },
            PropertyDef {
                name: "name".into(),
                ty: "String".into(),
                required: false,
                constraints: PropertyConstraints::default(),
            },
            PropertyDef {
                name: "qty".into(),
                ty: "Long".into(),
                required: false,
                constraints: PropertyConstraints::default(),
            },
        ],
        derived: vec![],
        identity: Some("id".into()),
    };
    assert_eq!(built, literal);
}

#[test]
fn object_type_builder_defaults_are_empty() {
    let t = ObjectType::build("Bare", ("wh", "bare")).done();
    assert_eq!(t.properties, vec![]);
    assert_eq!(t.derived, vec![]);
    assert_eq!(t.identity, None, "no declared identity by default");
}

#[test]
fn object_type_builder_constraints_and_derived_hooks() {
    let constraints = PropertyConstraints {
        length: Some(LengthConstraint {
            min: Some(1),
            max: Some(8),
        }),
        ..PropertyConstraints::default()
    };
    let agg = DerivedPropertyDef {
        name: "order_count".into(),
        ty: "Long".into(),
        link: "orders".into(),
        agg: Aggregation::Count,
    };
    let t = ObjectType::build("Account", ("main", "account"))
        .prop_with("code", "String", true, constraints.clone())
        .derived(agg.clone())
        .done();
    assert_eq!(t.properties[0].constraints, constraints);
    assert!(t.properties[0].required);
    assert_eq!(t.derived, vec![agg]);
}

#[test]
fn action_def_builder_matches_literal() {
    let built = ActionDef::build("updateWidget", "Widget", ActionKind::Update)
        .param_req("id", "Long")
        .param_req("qty", "Long")
        .done();
    let literal = ActionDef {
        name: ActionName("updateWidget".into()),
        target: TypeName("Widget".into()),
        parameters: vec![
            ParamDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
                binds: None,
            },
            ParamDef {
                name: "qty".into(),
                ty: "Long".into(),
                required: true,
                binds: None,
            },
        ],
        kind: ActionKind::Update,
        assignments: vec![],
    };
    assert_eq!(built, literal);
}

#[test]
fn action_def_builder_binds_and_assignment_hooks() {
    let a = ActionDef::build("renameCustomer", "Customer", ActionKind::Update)
        .param_req("customerId", "Long")
        .param_bound("newName", "String", false, "name")
        .assign("status", serde_json::json!("active"))
        .done();
    assert_eq!(a.parameters[0].binds, None);
    assert_eq!(a.parameters[1].binds.as_deref(), Some("name"));
    assert!(!a.parameters[1].required);
    assert_eq!(
        a.assignments,
        vec![ConstAssignment {
            property: "status".into(),
            value: serde_json::json!("active"),
        }]
    );
    assert_eq!(a.kind, ActionKind::Update);
}
```

Add to `src/control-plane/core/BUCK` (mirroring `:page`):

```python
rust_test(
    name = "ontology-builder",
    crate = "ontology_builder",
    srcs = ["tests/ontology_builder.rs"],
    crate_root = "tests/ontology_builder.rs",
    edition = "2024",
    visibility = ["PUBLIC"],
    deps = [
        ":core",
        "//third-party:serde_json",
    ],
)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `buck2 test //src/control-plane/core:ontology-builder > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t1.log`
Expected: FAIL — compile error (`build` doesn't exist on `ObjectType`/`ActionDef`).

- [ ] **Step 3: Implement the builders**

In `src/control-plane/core/src/ontology.rs`, directly below the `ObjectType`
struct (after line 49):

```rust
impl ObjectType {
    /// Start a fluent [`ObjectTypeBuilder`] for a type named `name`, backed by the
    /// physical table `(schema, table)`. Plain construction — no validation, no
    /// I/O (that stays with [`Ontology::define_type`], exactly as for a literal).
    ///
    /// ```
    /// use control_plane_core::ObjectType;
    /// let docs = ObjectType::build("Docs", ("wh", "docs"))
    ///     .prop_req("id", "Long")
    ///     .prop("note", "String")
    ///     .identity("id")
    ///     .done();
    /// assert_eq!(docs.identity.as_deref(), Some("id"));
    /// ```
    pub fn build(
        name: impl Into<String>,
        table: (impl Into<String>, impl Into<String>),
    ) -> ObjectTypeBuilder {
        ObjectTypeBuilder {
            inner: ObjectType {
                name: TypeName(name.into()),
                properties: Vec::new(),
                derived: Vec::new(),
                table: TableRef {
                    schema: table.0.into(),
                    name: table.1.into(),
                },
                identity: None,
            },
        }
    }
}

/// Fluent constructor for [`ObjectType`] — see [`ObjectType::build`]. Methods
/// append in call order (`properties`/`derived` are ordered).
#[derive(Clone, Debug)]
pub struct ObjectTypeBuilder {
    inner: ObjectType,
}

impl ObjectTypeBuilder {
    /// Append an optional (`required: false`), unconstrained property.
    pub fn prop(self, name: impl Into<String>, ty: impl Into<String>) -> Self {
        self.prop_with(
            name,
            ty,
            false,
            crate::constraints::PropertyConstraints::default(),
        )
    }

    /// Append a required, unconstrained property.
    pub fn prop_req(self, name: impl Into<String>, ty: impl Into<String>) -> Self {
        self.prop_with(
            name,
            ty,
            true,
            crate::constraints::PropertyConstraints::default(),
        )
    }

    /// Append a property with explicit requiredness and constraints — the full
    /// [`PropertyDef`] surface.
    pub fn prop_with(
        mut self,
        name: impl Into<String>,
        ty: impl Into<String>,
        required: bool,
        constraints: crate::constraints::PropertyConstraints,
    ) -> Self {
        self.inner.properties.push(PropertyDef {
            name: name.into(),
            ty: ty.into(),
            required,
            constraints,
        });
        self
    }

    /// Append a derived (aggregate-over-link) property. Passthrough — the
    /// [`DerivedPropertyDef`] literal is already minimal.
    pub fn derived(mut self, def: DerivedPropertyDef) -> Self {
        self.inner.derived.push(def);
        self
    }

    /// Declare `prop` as the type's identity (primary key). Should name one of
    /// the declared properties; validated by `define_type`, not here.
    pub fn identity(mut self, prop: impl Into<String>) -> Self {
        self.inner.identity = Some(prop.into());
        self
    }

    /// Finish: the assembled [`ObjectType`].
    pub fn done(self) -> ObjectType {
        self.inner
    }
}
```

And directly below the `ActionDef` struct (after line 219):

```rust
impl ActionDef {
    /// Start a fluent [`ActionDefBuilder`] for an action named `name` targeting
    /// the type `target`, of mutation kind `kind`. Plain construction — no
    /// validation, no I/O (that stays with [`Ontology::define_action`]).
    ///
    /// ```
    /// use control_plane_core::{ActionDef, ActionKind};
    /// let update = ActionDef::build("updateWidget", "Widget", ActionKind::Update)
    ///     .param_req("id", "Long")
    ///     .param_req("qty", "Long")
    ///     .done();
    /// assert_eq!(update.parameters.len(), 2);
    /// ```
    pub fn build(
        name: impl Into<String>,
        target: impl Into<String>,
        kind: ActionKind,
    ) -> ActionDefBuilder {
        ActionDefBuilder {
            inner: ActionDef {
                name: ActionName(name.into()),
                target: TypeName(target.into()),
                parameters: Vec::new(),
                kind,
                assignments: Vec::new(),
            },
        }
    }
}

/// Fluent constructor for [`ActionDef`] — see [`ActionDef::build`]. Methods
/// append in call order (`parameters`/`assignments` are ordered).
#[derive(Clone, Debug)]
pub struct ActionDefBuilder {
    inner: ActionDef,
}

impl ActionDefBuilder {
    /// Append an optional (`required: false`) parameter binding the property of
    /// the same name (`binds: None`).
    pub fn param(mut self, name: impl Into<String>, ty: impl Into<String>) -> Self {
        self.inner.parameters.push(ParamDef {
            name: name.into(),
            ty: ty.into(),
            required: false,
            binds: None,
        });
        self
    }

    /// Append a required parameter binding the property of the same name.
    pub fn param_req(mut self, name: impl Into<String>, ty: impl Into<String>) -> Self {
        self.inner.parameters.push(ParamDef {
            name: name.into(),
            ty: ty.into(),
            required: true,
            binds: None,
        });
        self
    }

    /// Append a parameter renamed away from the property it writes
    /// ([`ParamDef::binds`] = `Some(binds)`) — the full [`ParamDef`] surface.
    pub fn param_bound(
        mut self,
        name: impl Into<String>,
        ty: impl Into<String>,
        required: bool,
        binds: impl Into<String>,
    ) -> Self {
        self.inner.parameters.push(ParamDef {
            name: name.into(),
            ty: ty.into(),
            required,
            binds: Some(binds.into()),
        });
        self
    }

    /// Append a declared constant assignment ([`ConstAssignment`]) filling
    /// `property` with `value` when no parameter supplies it.
    pub fn assign(mut self, property: impl Into<String>, value: serde_json::Value) -> Self {
        self.inner.assignments.push(ConstAssignment {
            property: property.into(),
            value,
        });
        self
    }

    /// Finish: the assembled [`ActionDef`].
    pub fn done(self) -> ActionDef {
        self.inner
    }
}
```

In `src/control-plane/core/src/lib.rs`, extend the `pub use ontology::{…}` list
(lines 53-56) with `ActionDefBuilder` and `ObjectTypeBuilder` (alphabetical
within the list).

- [ ] **Step 4: Run tests + clippy**

Run: `buck2 test //src/control-plane/core:ontology-builder > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t1.log`
Expected: PASS (all 5 tests).
Run: `buck2 build '//src/control-plane/core:core[clippy.txt]' > /tmp/c1.log 2>&1` and check the artifact is empty.

- [ ] **Step 5: prek + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p1.log 2>&1; grep -c Failed /tmp/p1.log` — expected `0`.

```bash
git add src/control-plane/core docs/superpowers/plans/2026-07-02-test-seed-dsl.md
git commit -m "feat(core): ObjectType/ActionDef seed-builder DSL

Fluent builders next to the ontology types: ObjectType::build(name,
(schema, table)).prop_req/.prop/.prop_with/.derived/.identity/.done and
ActionDef::build(name, target, kind).param_req/.param/.param_bound/
.assign/.done. Plain data assembly (no validation, no I/O — define_type/
define_action keep owning validation), so the builders are production API,
not test scaffolding; zero new deps. prop_with/param_bound/assign expose
the full PropertyDef/ParamDef/ConstAssignment surface; LinkDef gets no
builder (5 flat fields whose weight is the LinkBacking enum). done() over
Into: explicit, greppable terminator.

Part of road-test-seed-dsl."
```

---

### Task 2: e2e_support — `grant_writer_role`, promoted `read_widget`, builders under `define_widget`

**Files:**
- Modify: `src/services/query-api/tests/e2e_support.rs` (`define_widget` at
  837-939 rewritten over the DSL; `grant_writer` at 943-966 re-expressed through
  a new `grant_writer_role`; new `read_widget`; module doc + imports)

**Interfaces:**
- Produces: `pub async fn grant_writer_role(cp: &PgControlPlane, widget: &TypeName) -> (SubjectId, RoleId)`;
  `pub async fn read_widget(cp: &PgControlPlane, pool: &sqlx::PgPool, subj: &SubjectId, id: i64) -> Option<serde_json::Value>`.
- Preserved signatures: `define_widget(cp) -> TypeName`,
  `grant_writer(cp, widget) -> SubjectId` (pinned by `action_client_wire.rs:11`,
  `overwrite_table_e2e.rs:109-110,251-252`, `iceberg_action_e2e.rs:112-113,186`).
  `prop()`/`tref()` untouched.
- Consumed by Task 3 (the three update_delete files).

- [ ] **Step 1: Rewrite `define_widget` over the builders**

Replace the body of `define_widget` (`e2e_support.rs:837-939`) — same doc
comment, same signature, same seeded world:

```rust
pub async fn define_widget(cp: &PgControlPlane) -> TypeName {
    let widget = TypeName("Widget".into());
    cp.ontology()
        .define_type(
            ObjectType::build("Widget", ("main", "widget"))
                .prop_req("id", "Long")
                .prop("name", "String")
                .prop("qty", "Long")
                .identity("id")
                .done(),
        )
        .await
        .unwrap();
    cp.ontology()
        .define_action(
            ActionDef::build("createWidget", "Widget", ActionKind::Insert)
                .param_req("id", "Long")
                .param("name", "String")
                .param("qty", "Long")
                .done(),
        )
        .await
        .unwrap();
    cp.ontology()
        .define_action(
            ActionDef::build("updateWidget", "Widget", ActionKind::Update)
                .param_req("id", "Long")
                .param_req("qty", "Long")
                .done(),
        )
        .await
        .unwrap();
    cp.ontology()
        .define_action(
            ActionDef::build("deleteWidget", "Widget", ActionKind::Delete)
                .param_req("id", "Long")
                .done(),
        )
        .await
        .unwrap();
    widget
}
```

(Structural parity with the deleted literal is exactly what Task 1's
`object_type_builder_matches_literal`/`action_def_builder_matches_literal` unit
tests pin — same shape, same field values.) After the rewrite, PRUNE the
`ActionName` and `ParamDef` imports from e2e_support — they were used only by
the old `define_widget` body, and leaving them trips `unused_imports` in the
prek clippy gate. `PropertyDef` stays (used by `prop()` and the setup fixtures).

- [ ] **Step 2: Add `grant_writer_role`; delegate `grant_writer`**

Replace `grant_writer` (`e2e_support.rs:943-966`) with:

```rust
/// Grant `Write` + `Read` on `widget` to a fresh `writer` subject (role `writers`),
/// returning both the subject and the role so callers can `set_policy` on the role.
/// Promoted from `update_delete_governance_e2e.rs` (the one signature delta in the
/// update_delete family).
pub async fn grant_writer_role(cp: &PgControlPlane, widget: &TypeName) -> (SubjectId, RoleId) {
    let subj = SubjectId("writer".into());
    let role = RoleId("writers".into());
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    for action in [Action::Write, Action::Read] {
        cp.grant(&role, action, PolicyTarget::Type(widget.clone()), Effect::Allow)
            .await
            .unwrap();
    }
    (subj, role)
}

/// Grant `Write` + `Read` on `widget` to a fresh `writer` subject (role `writers`).
/// Promoted from `update_delete_tiers_e2e.rs`.
pub async fn grant_writer(cp: &PgControlPlane, widget: &TypeName) -> SubjectId {
    grant_writer_role(cp, widget).await.0
}
```

- [ ] **Step 3: Promote `read_widget`**

Add below `grant_writer` (byte-for-byte the helper from
`update_delete_e2e.rs:159-190` / `update_delete_tiers_e2e.rs:154-186`, doc
comment merged):

```rust
/// The single Widget object visible to `subj` for `id`, as JSON (or `None`).
/// Promoted from `update_delete_e2e.rs`/`update_delete_tiers_e2e.rs`.
pub async fn read_widget(
    cp: &PgControlPlane,
    pool: &sqlx::PgPool,
    subj: &SubjectId,
    id: i64,
) -> Option<serde_json::Value> {
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let qdeps = QueryDeps {
        ontology: cp.ontology(),
        acl: cp.acl(),
        serving: &serving,
        default_limit: 1000,
    };
    let rows = read_object(
        &ObjectQuery {
            type_name: "Widget".into(),
            filters: vec![],
            ids: vec![],
            or_raw: Vec::new(),
        },
        &Subject(subj.clone()),
        &qdeps,
    )
    .await
    .unwrap();
    objects_to_json(&rows, None)["objects"]
        .as_array()
        .unwrap()
        .iter()
        .find(|o| o["id"] == serde_json::json!(id.to_string()))
        .cloned()
}
```

Extend the imports: add
`use query_api::handler::{ObjectQuery, QueryDeps, Subject, read_object};`
(`objects_to_json` and `serde_json` are already imported; if the file uses
`json!` bare, keep the fully-qualified `serde_json::json!` form above to avoid
a new macro import).

- [ ] **Step 4: Compile-check the library and its pinned consumers**

Run: `buck2 build //src/services/query-api:e2e-support > /tmp/b2.log 2>&1; grep -E "BUILD|error" /tmp/b2.log`
Expected: BUILD SUCCEEDED.
Run: `buck2 test //src/services/query-api:action-client-wire //src/services/query-api:overwrite-table-e2e //src/services/query-api:iceberg-action-e2e > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t2.log`
Expected: PASS — proves the builder-rewritten `define_widget` and the delegated
`grant_writer` are drop-in for the already-migrated consumers.

- [ ] **Step 5: prek + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p2.log 2>&1; grep -c Failed /tmp/p2.log` — expected `0`.

```bash
git add src/services/query-api/tests/e2e_support.rs
git commit -m "refactor(query-api): e2e-support widget helpers on the builder DSL

define_widget's ~100-line ObjectType/ActionDef literals collapse onto
core's new seed builders (structural parity pinned by the core unit
tests); grant_writer_role(cp, widget) -> (SubjectId, RoleId) absorbs the
governance file's signature delta with grant_writer delegating to it; the
byte-identical read_widget helper from update_delete_e2e/tiers is
promoted. prop()/tref() and the define_widget/grant_writer signatures are
untouched (pinned by action_client_wire, overwrite_table_e2e,
iceberg_action_e2e).

Part of road-test-seed-dsl."
```

---

### Task 3: Migrate the update_delete family (pure deletion + import swap)

**Files:**
- Modify: `src/services/query-api/tests/update_delete_e2e.rs` (delete lines
  23-190: private `define_widget`/`grant_writer`/`read_widget`)
- Modify: `src/services/query-api/tests/update_delete_tiers_e2e.rs` (delete
  lines 22-186: same three private copies)
- Modify: `src/services/query-api/tests/update_delete_governance_e2e.rs`
  (delete lines 19-150: private `define_widget`/`grant_writer`; rewrite
  `vector_guard`'s literals over the builders)
- No BUCK changes: all three targets already depend on `:e2e-support`
  (`src/services/query-api/BUCK:1374-1424`).

**Interfaces:**
- Consumes: `e2e_support::{define_widget, grant_writer, grant_writer_role, read_widget}` (Task 2),
  `ObjectType::build`/`ActionDef::build` (Task 1).

- [ ] **Step 1: `update_delete_e2e.rs` and `update_delete_tiers_e2e.rs`**

In each file: delete the private `define_widget`, `grant_writer`, and
`read_widget` functions; add the imports
`use e2e_support::{InProcessServingEngine, define_widget, grant_writer, read_widget};`
(replacing the existing `use e2e_support::InProcessServingEngine;`). Test
bodies are untouched — call sites (`define_widget(&cp)`, `grant_writer(&cp, &widget)`,
`read_widget(&cp, &pool, &subj, N)`) resolve identically.

Prune the now-unused `control_plane_core` imports in each file (the compiler
is the arbiter; expected survivors are roughly `{Acl, Action, ControlPlane, Effect, PolicyTarget, SubjectId, TypeName}`
in `update_delete_e2e.rs` — `ObjectType`/`ActionDef`/`ActionKind`/`ActionName`/`ParamDef`/`PropertyDef`/`RoleId`/`TableRef`
all leave with the deleted helpers; similarly in tiers, which keeps `Catalog`).
`query_api::handler`/`query_api::render` imports leave `update_delete_e2e.rs`/tiers
only if no test body uses them directly — check before deleting (tiers' bodies
call `read_object` directly in the time-travel test; verify with a grep, let
rustc confirm).

- [ ] **Step 2: `update_delete_governance_e2e.rs`**

Delete the private `define_widget` (lines 21-123) and `grant_writer` (lines
125-150). Import `use e2e_support::{InProcessServingEngine, define_widget, grant_writer_role};`.
Four governance tests call `let (subj, role) = grant_writer(&cp, &widget).await;`
today (the fifth, `vector_guard`, keeps its inline grant block untouched) —
switch each of the four call sites to `grant_writer_role(&cp, &widget)` (same
tuple shape, pure rename).

In `vector_guard` (lines 441-530), rewrite the two seed literals over the
builders (dogfood at a site with an optional `vector(4)` property and a
Delete-only action — API-shaping evidence the Widget shape doesn't exercise):

```rust
    // Define VectorWidget(id Long identity, embedding vector(4)).
    let vwidget = TypeName("VectorWidget".into());
    cp.ontology()
        .define_type(
            ObjectType::build("VectorWidget", ("main", "vector_widget"))
                .prop_req("id", "Long")
                .prop("embedding", "vector(4)")
                .identity("id")
                .done(),
        )
        .await
        .unwrap();

    // Define deleteVectorWidget (Delete, param id).
    cp.ontology()
        .define_action(
            ActionDef::build("deleteVectorWidget", "VectorWidget", ActionKind::Delete)
                .param_req("id", "Long")
                .done(),
        )
        .await
        .unwrap();
```

The rest of `vector_guard` (the inline vwriter/vwriters grant block and the
assertion) is untouched — it is not one of the target pairs. Prune unused
imports (`PropertyDef`, `ParamDef`, `ActionName`, `TableRef` leave;
`ObjectType`, `ActionDef`, `ActionKind` stay for `vector_guard`).

- [ ] **Step 3: Run the three migrated targets**

Run: `buck2 test //src/services/query-api:update-delete-e2e //src/services/query-api:update-delete-tiers-e2e //src/services/query-api:update-delete-governance-e2e > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t3.log`
Expected: PASS — every test body unchanged, seeded world identical.

- [ ] **Step 4: Clippy + prek + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p3.log 2>&1; grep -c Failed /tmp/p3.log` — expected `0`.

```bash
git add src/services/query-api/tests/update_delete_e2e.rs \
        src/services/query-api/tests/update_delete_tiers_e2e.rs \
        src/services/query-api/tests/update_delete_governance_e2e.rs
git commit -m "refactor(query-api): migrate update_delete family onto e2e-support helpers

Deletes the three files' ~130-line private define_widget/grant_writer
(+read_widget) copies — the promotion into e2e_support landed long ago but
the source files were never migrated, leaving the duplication register's
top-6 pairs (91-127 lines each). Pure deletion + import swap; governance's
(SubjectId, RoleId) delta rides the new grant_writer_role; vector_guard's
remaining seed literals move onto the core builder DSL.

Part of road-test-seed-dsl."
```

---

### Task 4: Proof metric + register close-out

**Files:**
- Modify: `docs/ROADMAP.md:55` (`road-test-seed-dsl` → done)

- [ ] **Step 1: Duplication proof — the update_delete pairs are cleared**

Primary: run the loom-duplication skill's diff mode on the branch (analyzes
exactly the changed files) — invoke the **`loom-duplication`** skill with args
`diff`, whose BLOCK A executes:

```bash
buck2 run -v0 //tools:lucidshark-duplo -- --git --changed-only --json -m 20 \
  --baseline "$PWD/docs/code-health/duplication-baseline.json" > /tmp/dup.json 2>/tmp/dup-err.txt || true
[ -s /tmp/dup.json ] || echo '{"duplicates":[]}' > /tmp/dup.json
buck2 run -v0 //tools:jq -- -rf "$PWD/.claude/skills/loom-duplication/render.jq" \
  --slurpfile baseline "$PWD/docs/code-health/duplication-baseline.json" /tmp/dup.json
```

Deterministic pass/fail check (family-scoped, independent of what else changed
on the branch):

```bash
cat > /tmp/dup-fam.txt <<'EOF'
src/services/query-api/tests/e2e_support.rs
src/services/query-api/tests/update_delete_e2e.rs
src/services/query-api/tests/update_delete_tiers_e2e.rs
src/services/query-api/tests/update_delete_governance_e2e.rs
EOF
buck2 run -v0 //tools:lucidshark-duplo -- /tmp/dup-fam.txt --json -m 20 \
  --baseline "$PWD/docs/code-health/duplication-baseline.json" > /tmp/dup-fam.json 2>/tmp/dup-fam-err.txt || true
buck2 run -v0 //tools:jq -- -rf "$PWD/.claude/skills/loom-duplication/render.jq" \
  --slurpfile baseline "$PWD/docs/code-health/duplication-baseline.json" /tmp/dup-fam.json
```

Expected: the six census pairs this item targets
(`update_delete_e2e ↔ update_delete_tiers_e2e` 127; `e2e_support ↔ update_delete_e2e` 92;
`e2e_support ↔ update_delete_tiers_e2e` 92; `e2e_support ↔ update_delete_governance_e2e` 91;
`update_delete_e2e ↔ update_delete_governance_e2e` 91;
`update_delete_governance_e2e ↔ update_delete_tiers_e2e` 91) are **absent**.
Acceptable residuals: the ≤23-line *test-body* pairs already in the census
(e.g. `update_delete_e2e.rs:303-335 ≈ 232-266`, governance's `345-379 ≈ 276-310`,
and the `iceberg_action_e2e`/`overwrite_table_e2e` run_action blocks) — those
are seed/act sequences inside test functions, not the helper copies, and are
explicitly follow-up material. If any ≥90-line family pair survives, STOP —
a private copy was missed.

(Do NOT run the skill's full mode / regenerate `docs/code-health/duplication.md`
here — the register refresh is the scheduled `loom-duplication` routine's PR,
not this branch's.)

- [ ] **Step 2: Test sweep of everything this branch touched**

Run: `buck2 test //src/control-plane/core:ontology-builder //src/services/query-api:update-delete-e2e //src/services/query-api:update-delete-tiers-e2e //src/services/query-api:update-delete-governance-e2e //src/services/query-api:action-client-wire //src/services/query-api:overwrite-table-e2e //src/services/query-api:iceberg-action-e2e > /tmp/t4.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t4.log`
Expected: PASS.
Verify: `git status src/control-plane/postgres/.sqlx` clean;
`git diff origin/main -- third-party/BUCK Cargo.lock` empty.

- [ ] **Step 3: Close the register item**

In `docs/ROADMAP.md:55`, flip `road-test-seed-dsl` to `- [x]` / `status:done` /
`pr:#N` (substitute the real PR number at PR time), and append a one-line
outcome to the prose: builders landed in core (`ObjectType::build`/`ActionDef::build`,
`done()` terminator, `prop_with`/`param_bound`/`assign` full-surface hooks,
LinkDef deliberately excluded); the six update_delete pairs cleared; tree-wide
literal adoption deliberately deferred to incremental touches +
[[road-test-wire-harness]].
Validate: `bash tools/docs.sh validate`.

- [ ] **Step 4: prek + commit + finish**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p4.log 2>&1; grep -c Failed /tmp/p4.log` — expected `0`.

```bash
git add docs/ROADMAP.md
git commit -m "docs(registers): close road-test-seed-dsl"
```

Then finish the branch per `superpowers:finishing-a-development-branch` (push +
open PR against `main` from `work/road-test-seed-dsl`; poll CI via the
commit-status endpoint + BuildBuddy MCP, not `gh pr checks`).
