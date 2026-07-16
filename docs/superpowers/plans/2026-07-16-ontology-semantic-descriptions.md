# Ontology Semantic Descriptions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give every declarable ontology entity an optional human-readable `description`, carried from `define_*` through both control-plane adapters to the JSON and OpenAPI read surfaces.

**Architecture:** Two strictly-ordered PRs. **PR 1 (Tasks 1–12)** is a pure constructor refactor with no `description` in it: each of the 7 ontology structs gains a plain constructor + chainable modifiers (the idiom `ObjectType::build`/`ActionDef::build` already establish), and the 705 test/testkit struct literals migrate onto them. **PR 2 (Tasks 13–19)** adds `description: Option<String>` to those 7 structs, one nullable-add migration, postgres persistence, and the read surface. PR 1 is what makes PR 2's field ~free at call sites.

**Tech Stack:** Rust, buck2, sqlx (compile-time macros + committed `.sqlx`), axum, utoipa, Postgres.

**Spec:** `docs/superpowers/specs/2026-07-16-ontology-semantic-descriptions-design.md`

## Global Constraints

Every task's requirements implicitly include this section.

- **Tests are `rust_test` integration targets only — never inline `#[cfg(test)] mod tests`.** buck2 builds inline tests but never runs them; the `no-inline-tests` prek hook fails the commit. Unit tests go in a sibling `tests/<name>.rs` wired as its own `rust_test` target in the crate's `BUCK`.
- **Fixture tests (hermetic Postgres) must use `loom_fixture_test`**, not bare `rust_test`, or they run without the fixture env and fail to boot.
- **Build:** `buck2 build -v0 --console none //src/...` (silent on success). **Test:** `buck2 test --console none <target>`. Never pipe buck2 through `tail`/`head` — it stalls and leaves zombies.
- **Full-suite runs need `-j 8`**: `buck2 test --console none -j 8 //src/...`. Without it the 8 postgres boot-slots starve and throw non-deterministic 120s timeouts.
- **Run `buck2 run //tools:prek -- run --all-files` before EVERY commit.** rustfmt is a separate hook — clippy-clean ≠ lint-clean. **`git add` new files first**: prek skips untracked files.
- **Clippy is pedantic + restriction.** Relevant here: **`too_many_arguments` is ENFORCED (threshold 7)** — it is not in `CLIPPY_ALLOWS`, which is why `LinkBacking::join_table` exists rather than a 9-arg `LinkDef::join_table`. `must_use_candidate` and `too_many_lines` ARE allowed, so builder methods need no `#[must_use]` (match the existing `ObjectTypeBuilder`, which has none). To silence a lint locally use `#[expect(lint, reason = "...")]` — a bare `#[allow]` trips `allow_attributes_without_reason`.
- **Commits follow Conventional Commits** (enforced by the `conventional-commit` hook at commit-msg stage).
- **Branch:** `work/fut-ontology-semantic-descriptions`, based on `origin/main`. Local `main` is routinely stale — never diff against it.

## File Structure

**PR 1 — constructors**

| File | Responsibility |
|---|---|
| `src/control-plane/core/src/ontology.rs` (modify) | The 7 structs + their new constructors. All constructor code lands here — it is where `ObjectTypeBuilder`/`ActionDefBuilder` already live. |
| `src/control-plane/core/tests/ontology_builder.rs` (modify) | Constructor tests. Already the home of `ObjectType::build` tests; the `//src/control-plane/core:ontology-builder` target. |
| ~90 files under `src/**/tests/`, `testkit/src/lib.rs`, 5 production files (migrate) | Literal → constructor migration, batched per crate (Tasks 5–12). |

**PR 2 — feature**

| File | Responsibility |
|---|---|
| `src/control-plane/core/src/ontology.rs` (modify) | `description` field on 7 structs + `.described()` on each constructor. |
| `src/control-plane/postgres/migrations/0029_ontology_description.sql` (create) | Nullable `description` column on 7 `ontology.*` tables. |
| `src/control-plane/postgres/.sqlx/` (regenerate) | Committed offline query cache; `sqlx-cache-check` enforces freshness. |
| `src/control-plane/postgres/src/ontology.rs` (modify) | `description` in each `define_*` insert and each row→struct mapping. |
| `src/control-plane/testkit/src/lib.rs` (modify) | `ontology_contract` description assertions — one contract, both adapters. |
| `src/services/query-api/src/http.rs` (modify) | `description` on the `/ontology/types/{name}` JSON. |
| `src/services/query-api/src/openapi.rs` (modify) | `description` on `TypeDetailResponse`/`PropertyView`/`LinkView`. |
| `src/services/query-api/src/openapi_gen.rs` (modify) | Descriptions into generated schemas + operations. |

---

# PR 1 — Constructors (no `description` anywhere)

## The migration rule (Tasks 5–12 all use this)

Each migration task rewrites struct literals to the constructors from Tasks 1–4. The complete mapping:

| Literal | Constructor |
|---|---|
| `PropertyDef { name: "e".into(), ty: "T".into(), required: false, constraints: Default::default() }` | `PropertyDef::new("e", "T")` |
| `PropertyDef { … required: true … }` | `PropertyDef::new("e", "T").required()` |
| `PropertyDef { … constraints: c … }` | `PropertyDef::new("e", "T").constrained(c)` |
| `LinkDef { name, from: TypeName("A".into()), to: TypeName("B".into()), cardinality: Cardinality::One, backing: LinkBacking::ForeignKey { from_column: "x".into(), to_column: "y".into() } }` | `LinkDef::fk("name", "A", "B", Cardinality::One, "x", "y")` |
| `LinkDef { … backing: LinkBacking::JoinTable { … } }` | `LinkDef::new("name", "A", "B", Cardinality::Many, LinkBacking::join_table(("s","t"), "fk", "fc", "tc", "tk"))` |
| `DerivedPropertyDef { name, ty, link, agg }` | `DerivedPropertyDef::new(name, ty, link, agg)` |
| `ParamDef { name, ty, required: false, binds: None }` | `ParamDef::new(name, ty)` |
| `ParamDef { … required: true … }` | `ParamDef::new(name, ty).required()` |
| `ParamDef { … binds: Some("p".into()) … }` | `ParamDef::new(name, ty).binds("p")` |
| `VectorIndexDef { name, type_name, property, metric, spec }` | `VectorIndexDef::new(name, type_name, property, metric, spec)` |
| `ObjectType { name, properties, derived, table, identity, version }` | `ObjectType::build(name, (schema, table)).prop…().identity(…).done()` |

**⚠ THE ONE REAL RISK — read this before every migration task.** `PropertyDef { required: true }` migrated to `PropertyDef::new(..)` (which is `required: false`) **compiles clean and silently inverts the test's meaning**. The compiler cannot catch it; only a test that asserts on requiredness will. Likewise `ParamDef`. Therefore:

1. Migrate **file-by-file**, never with one global regex.
2. For every literal, **read the `required:` value** and carry it. When in doubt, re-read the original in `git diff`.
3. Each task's final review step is `git diff` read **line-by-line**, not skimmed.

If a task's diff is too large to review honestly, split it and say so — do not skim it.

---

### Task 1: `PropertyDef` constructor

**Files:**
- Modify: `src/control-plane/core/src/ontology.rs` (after the `PropertyDef` struct, ~line 36)
- Test: `src/control-plane/core/tests/ontology_builder.rs`

**Interfaces:**
- Consumes: `PropertyDef`, `crate::constraints::PropertyConstraints` (existing).
- Produces: `PropertyDef::new(name: impl Into<String>, ty: impl Into<String>) -> PropertyDef` (`required: false`, default constraints); `PropertyDef::required(self) -> PropertyDef`; `PropertyDef::constrained(self, c: PropertyConstraints) -> PropertyDef`. Used by Tasks 4–12.

- [ ] **Step 1: Write the failing test**

Append to `src/control-plane/core/tests/ontology_builder.rs`:

```rust
#[test]
fn property_def_new_is_optional_and_unconstrained() {
    let p = PropertyDef::new("email", "EmailAddress");
    assert_eq!(p.name, "email");
    assert_eq!(p.ty, "EmailAddress");
    assert!(!p.required);
    assert!(p.constraints.is_empty());
}

#[test]
fn property_def_required_sets_the_flag() {
    let p = PropertyDef::new("id", "Long").required();
    assert!(p.required);
}

#[test]
fn property_def_constrained_carries_constraints() {
    let mut c = control_plane_core::constraints::PropertyConstraints::default();
    c.max_length = Some(255);
    let p = PropertyDef::new("email", "EmailAddress").constrained(c.clone());
    assert_eq!(p.constraints, c);
    assert!(!p.required, "constrained must not change requiredness");
}
```

Add `PropertyDef` to that file's existing `use control_plane_core::{...}` import if absent.

> If `PropertyConstraints`'s field is not literally `max_length`, open `src/control-plane/core/src/constraints.rs` and use a real field — do not invent one.

- [ ] **Step 2: Run test to verify it fails**

Run: `buck2 test --console none //src/control-plane/core:ontology-builder`
Expected: FAIL — `no function or associated item named 'new' found for struct 'PropertyDef'`

- [ ] **Step 3: Write minimal implementation**

In `src/control-plane/core/src/ontology.rs`, directly after the `PropertyDef` struct definition:

```rust
impl PropertyDef {
    /// An optional (`required: false`), unconstrained property. Plain construction —
    /// no validation, no I/O (that stays with [`Ontology::define_type`]).
    ///
    /// ```
    /// use control_plane_core::PropertyDef;
    /// let p = PropertyDef::new("email", "EmailAddress").required();
    /// assert!(p.required);
    /// ```
    pub fn new(name: impl Into<String>, ty: impl Into<String>) -> PropertyDef {
        PropertyDef {
            name: name.into(),
            ty: ty.into(),
            required: false,
            constraints: crate::constraints::PropertyConstraints::default(),
        }
    }

    /// Mark this property required.
    pub fn required(mut self) -> PropertyDef {
        self.required = true;
        self
    }

    /// Attach per-value validation constraints.
    pub fn constrained(mut self, constraints: crate::constraints::PropertyConstraints) -> PropertyDef {
        self.constraints = constraints;
        self
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `buck2 test --console none //src/control-plane/core:ontology-builder`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/control-plane/core/src/ontology.rs src/control-plane/core/tests/ontology_builder.rs
git commit -m "refactor(core): add PropertyDef::new constructor"
```

---

### Task 2: `LinkDef` + `LinkBacking` constructors

**Files:**
- Modify: `src/control-plane/core/src/ontology.rs` (after `LinkBacking`'s existing `impl`, ~line 248; and after the `LinkDef` struct, ~line 256)
- Test: `src/control-plane/core/tests/ontology_builder.rs`

**Interfaces:**
- Consumes: `LinkDef`, `LinkBacking`, `Cardinality`, `TypeName`, `TableRef` (existing).
- Produces: `LinkBacking::fk(from_column, to_column) -> LinkBacking`; `LinkBacking::join_table(table: (impl Into<String>, impl Into<String>), from_key, from_column, to_column, to_key) -> LinkBacking` (5 args — the `too_many_arguments` ceiling is why the table is a tuple); `LinkDef::new(name, from, to, cardinality, backing) -> LinkDef`; `LinkDef::fk(name, from, to, cardinality, from_column, to_column) -> LinkDef` (6 args). Used by Tasks 5–12.

- [ ] **Step 1: Write the failing test**

Append to `src/control-plane/core/tests/ontology_builder.rs`:

```rust
#[test]
fn link_def_fk_builds_a_foreign_key_link() {
    let l = LinkDef::fk("customer", "Order", "Customer", Cardinality::One, "customer_id", "id");
    assert_eq!(l.name, "customer");
    assert_eq!(l.from, TypeName("Order".to_string()));
    assert_eq!(l.to, TypeName("Customer".to_string()));
    assert_eq!(l.cardinality, Cardinality::One);
    assert_eq!(
        l.backing,
        LinkBacking::ForeignKey { from_column: "customer_id".to_string(), to_column: "id".to_string() }
    );
}

#[test]
fn link_backing_join_table_builds_a_mapping_backing() {
    let b = LinkBacking::join_table(("wh", "doc_tag"), "id", "doc_id", "tag_id", "id");
    assert_eq!(
        b,
        LinkBacking::JoinTable {
            table: TableRef { schema: "wh".to_string(), name: "doc_tag".to_string() },
            from_key: "id".to_string(),
            from_column: "doc_id".to_string(),
            to_column: "tag_id".to_string(),
            to_key: "id".to_string(),
        }
    );
}

#[test]
fn link_def_new_takes_an_explicit_backing() {
    let l = LinkDef::new(
        "tags", "Doc", "Tag", Cardinality::Many,
        LinkBacking::join_table(("wh", "doc_tag"), "id", "doc_id", "tag_id", "id"),
    );
    assert_eq!(l.cardinality, Cardinality::Many);
    assert!(matches!(l.backing, LinkBacking::JoinTable { .. }));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `buck2 test --console none //src/control-plane/core:ontology-builder`
Expected: FAIL — `no function or associated item named 'fk' found for struct 'LinkDef'`

- [ ] **Step 3: Write minimal implementation**

Add to the **existing** `impl LinkBacking` block in `src/control-plane/core/src/ontology.rs`:

```rust
    /// A direct equijoin backing: `from_table.from_column = to_table.to_column`.
    pub fn fk(from_column: impl Into<String>, to_column: impl Into<String>) -> LinkBacking {
        LinkBacking::ForeignKey {
            from_column: from_column.into(),
            to_column: to_column.into(),
        }
    }

    /// A many-to-many backing through the mapping table `table` (a `(schema, name)` pair —
    /// a tuple, not two args, to stay under the enforced `too_many_arguments` ceiling).
    pub fn join_table(
        table: (impl Into<String>, impl Into<String>),
        from_key: impl Into<String>,
        from_column: impl Into<String>,
        to_column: impl Into<String>,
        to_key: impl Into<String>,
    ) -> LinkBacking {
        LinkBacking::JoinTable {
            table: TableRef { schema: table.0.into(), name: table.1.into() },
            from_key: from_key.into(),
            from_column: from_column.into(),
            to_column: to_column.into(),
            to_key: to_key.into(),
        }
    }
```

And a new `impl LinkDef` block directly after the `LinkDef` struct:

```rust
impl LinkDef {
    /// A directed link with an explicit physical `backing`. Plain construction — no
    /// validation, no I/O (that stays with [`Ontology::define_link`]).
    pub fn new(
        name: impl Into<String>,
        from: impl Into<String>,
        to: impl Into<String>,
        cardinality: Cardinality,
        backing: LinkBacking,
    ) -> LinkDef {
        LinkDef {
            name: name.into(),
            from: TypeName(from.into()),
            to: TypeName(to.into()),
            cardinality,
            backing,
        }
    }

    /// The common case: a link backed by a direct foreign-key equijoin.
    ///
    /// ```
    /// use control_plane_core::{Cardinality, LinkDef};
    /// let l = LinkDef::fk("customer", "Order", "Customer", Cardinality::One, "customer_id", "id");
    /// assert_eq!(l.name, "customer");
    /// ```
    pub fn fk(
        name: impl Into<String>,
        from: impl Into<String>,
        to: impl Into<String>,
        cardinality: Cardinality,
        from_column: impl Into<String>,
        to_column: impl Into<String>,
    ) -> LinkDef {
        LinkDef::new(name, from, to, cardinality, LinkBacking::fk(from_column, to_column))
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `buck2 test --console none //src/control-plane/core:ontology-builder`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/control-plane/core/src/ontology.rs src/control-plane/core/tests/ontology_builder.rs
git commit -m "refactor(core): add LinkDef and LinkBacking constructors"
```

---

### Task 3: `DerivedPropertyDef`, `ParamDef`, `VectorIndexDef` constructors

**Files:**
- Modify: `src/control-plane/core/src/ontology.rs` (after each struct's definition)
- Test: `src/control-plane/core/tests/ontology_builder.rs`

**Interfaces:**
- Consumes: `DerivedPropertyDef`, `Aggregation`, `ParamDef`, `VectorIndexDef`, `TypeName`, `Metric`, `IndexSpec` (existing).
- Produces: `DerivedPropertyDef::new(name, ty, link, agg) -> DerivedPropertyDef`; `ParamDef::new(name, ty) -> ParamDef` + `.required()` + `.binds(p)`; `VectorIndexDef::new(name, type_name, property, metric, spec) -> VectorIndexDef`. Used by Tasks 5–12.

- [ ] **Step 1: Write the failing test**

Append to `src/control-plane/core/tests/ontology_builder.rs`:

```rust
#[test]
fn derived_property_def_new_carries_its_aggregation() {
    let d = DerivedPropertyDef::new("orderCount", "Long", "orders", Aggregation::Count);
    assert_eq!(d.name, "orderCount");
    assert_eq!(d.ty, "Long");
    assert_eq!(d.link, "orders");
    assert_eq!(d.agg, Aggregation::Count);
}

#[test]
fn param_def_new_is_optional_and_self_binding() {
    let p = ParamDef::new("email", "EmailAddress");
    assert!(!p.required);
    assert_eq!(p.binds, None);
    assert_eq!(p.binds_property(), "email");
}

#[test]
fn param_def_required_and_binds() {
    let p = ParamDef::new("email", "EmailAddress").required().binds("email_address");
    assert!(p.required);
    assert_eq!(p.binds_property(), "email_address");
}

#[test]
fn vector_index_def_new_carries_metric_and_spec() {
    let v = VectorIndexDef::new("byEmbedding", "Doc", "embedding", Metric::Cosine, IndexSpec::Flat);
    assert_eq!(v.name, "byEmbedding");
    assert_eq!(v.type_name, TypeName("Doc".to_string()));
    assert_eq!(v.property, "embedding");
    assert_eq!(v.metric, Metric::Cosine);
    assert_eq!(v.spec, IndexSpec::Flat);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `buck2 test --console none //src/control-plane/core:ontology-builder`
Expected: FAIL — `no function or associated item named 'new' found for struct 'DerivedPropertyDef'`

- [ ] **Step 3: Write minimal implementation**

In `src/control-plane/core/src/ontology.rs`, after `DerivedPropertyDef`:

```rust
impl DerivedPropertyDef {
    /// A computed property: aggregate `agg` over the rows reachable via the link named
    /// `link`. Plain construction — the define path validates the aggregation/result types.
    pub fn new(
        name: impl Into<String>,
        ty: impl Into<String>,
        link: impl Into<String>,
        agg: Aggregation,
    ) -> DerivedPropertyDef {
        DerivedPropertyDef { name: name.into(), ty: ty.into(), link: link.into(), agg }
    }
}
```

After `ParamDef`'s existing `impl` block (keep `binds_property` where it is — add these to the **same** block):

```rust
    /// An optional (`required: false`) parameter binding the property of the same name.
    pub fn new(name: impl Into<String>, ty: impl Into<String>) -> ParamDef {
        ParamDef { name: name.into(), ty: ty.into(), required: false, binds: None }
    }

    /// Mark this parameter required.
    pub fn required(mut self) -> ParamDef {
        self.required = true;
        self
    }

    /// Rename this parameter away from the property it writes.
    pub fn binds(mut self, property: impl Into<String>) -> ParamDef {
        self.binds = Some(property.into());
        self
    }
```

After `VectorIndexDef`:

```rust
impl VectorIndexDef {
    /// A named vector index on `type_name.property`. Dimension is NOT restated — it is
    /// derived from the property's `vector(N)` type at define time.
    pub fn new(
        name: impl Into<String>,
        type_name: impl Into<String>,
        property: impl Into<String>,
        metric: Metric,
        spec: IndexSpec,
    ) -> VectorIndexDef {
        VectorIndexDef {
            name: name.into(),
            type_name: TypeName(type_name.into()),
            property: property.into(),
            metric,
            spec,
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `buck2 test --console none //src/control-plane/core:ontology-builder`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/control-plane/core/src/ontology.rs src/control-plane/core/tests/ontology_builder.rs
git commit -m "refactor(core): add derived-property, param and vector-index constructors"
```

---

### Task 4: `ObjectTypeBuilder::add_prop`

**Files:**
- Modify: `src/control-plane/core/src/ontology.rs` (in the existing `impl ObjectTypeBuilder`, ~line 95)
- Test: `src/control-plane/core/tests/ontology_builder.rs`

**Interfaces:**
- Consumes: `PropertyDef::new` (Task 1), `ObjectTypeBuilder` (existing).
- Produces: `ObjectTypeBuilder::add_prop(p: PropertyDef) -> ObjectTypeBuilder` — appends a pre-built `PropertyDef`, so a type whose properties need the full `PropertyDef` surface (constraints, or PR 2's description) can still be built fluently. Used by Tasks 5–12.

- [ ] **Step 1: Write the failing test**

Append to `src/control-plane/core/tests/ontology_builder.rs`:

```rust
#[test]
fn add_prop_appends_a_prebuilt_property_in_order() {
    let t = ObjectType::build("Customer", ("wh", "customers"))
        .prop_req("id", "Long")
        .add_prop(PropertyDef::new("email", "EmailAddress").required())
        .prop("note", "String")
        .identity("id")
        .done();

    let names: Vec<&str> = t.properties.iter().map(|p| p.name.as_str()).collect();
    assert_eq!(names, vec!["id", "email", "note"], "add_prop must append in call order");
    assert!(t.properties[1].required);
    assert_eq!(t.identity.as_deref(), Some("id"));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `buck2 test --console none //src/control-plane/core:ontology-builder`
Expected: FAIL — `no method named 'add_prop' found for struct 'ObjectTypeBuilder'`

- [ ] **Step 3: Write minimal implementation**

Add to the existing `impl ObjectTypeBuilder` block, next to `prop_with`:

```rust
    /// Append an already-built [`PropertyDef`] — the escape hatch for properties needing
    /// the full surface (constraints, description) rather than the `prop`/`prop_req` shorthands.
    pub fn add_prop(mut self, prop: PropertyDef) -> Self {
        self.inner.properties.push(prop);
        self
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `buck2 test --console none //src/control-plane/core:ontology-builder`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/control-plane/core/src/ontology.rs src/control-plane/core/tests/ontology_builder.rs
git commit -m "refactor(core): add ObjectTypeBuilder::add_prop"
```

---

## Tasks 5–12: literal → constructor migration

Every one of these tasks has the **same 5 steps**, differing only in scope. Re-read *The migration rule* above — especially the `required:` inversion risk — before starting each.

**The 5 steps (apply to each task's file set):**

- [ ] **Step 1: Migrate the files** — file-by-file, applying the mapping table. Carry every `required:` value across. Do not change any test's assertions or semantics; this is a refactor.
- [ ] **Step 2: Build** — `buck2 build -v0 --console none <targets>`. Expected: silent (exit 0).
- [ ] **Step 3: Test** — `buck2 test --console none <targets>`. Expected: `Tests finished: Pass N. Fail 0` with **N identical to the pre-migration count** (capture it before Step 1).
- [ ] **Step 4: Review the diff line-by-line** — `git diff`. For every `PropertyDef::new(..)` / `ParamDef::new(..)` with no `.required()`, confirm the original literal said `required: false`. This step is the only thing standing between a wrong migration and a silently weakened test. Do not skim.
- [ ] **Step 5: Commit** — `buck2 run //tools:prek -- run --all-files`, then `git add -u`, then commit with the message given per task.

| Task | Scope | Sites | Files | Targets for Steps 2–3 | Commit message |
|---|---|---|---|---|---|
| **5** | `src/control-plane/testkit/src/lib.rs` — the shared fixtures every adapter contract runs. Do this first: it is the single biggest file and both `memory` and `postgres` consume it. | 87 | 1 | `//src/control-plane/testkit/... //src/control-plane/memory/... //src/control-plane/postgres/...` | `refactor(testkit): build ontology fixtures via constructors` |
| **6** | `src/control-plane/core/` — `src/ontology.rs`'s own 21 internal literals (builders, serde bridges, doctests) + `tests/*.rs`. **Leave the constructor `impl` bodies themselves as literals** — they are the definition. | 57 | 9 | `//src/control-plane/core/...` | `refactor(core): build ontology structs via constructors` |
| **7** | `src/control-plane/postgres/` — `src/ontology.rs`'s row→struct mapping + `tests/*.rs`. Fixture-heavy: expect a slow run. | 59 | 14 | `//src/control-plane/postgres/...` | `refactor(postgres): build ontology structs via constructors` |
| **8** | `src/services/query-api/tests/e2e_support.rs` **only** — the shared e2e support library (`//src/services/query-api:e2e-support`). Isolated because ~40 test files depend on it. | 23 | 1 | `//src/services/query-api/...` | `refactor(query-api): build e2e-support ontology fixtures via constructors` |
| **9** | `src/services/query-api/tests/` — the action/conformance/params half: `action_*.rs`, `mutate_conformance.rs`, `multi_step_*.rs`, `params.rs`, `constraints_action_http.rs`, `iceberg_action_e2e.rs`, `openapi_gen.rs`, `write_denial_http.rs`. **Highest `ParamDef` density — the `required:` risk concentrates here.** | ~150 | ~22 | `//src/services/query-api/...` | `refactor(query-api): build action-test ontology structs via constructors` |
| **10** | `src/services/query-api/tests/` — everything remaining: `graph_*.rs`, `link_traversal.rs`, `association*.rs`, `resolve_*.rs`, `object_*.rs`, `governed_*.rs`, `typed_filter_e2e.rs`, `subscribe_http.rs`, `view_write_e2e.rs`, `ontology_type_detail.rs`, and the rest. | ~215 | ~42 | `//src/services/query-api/...` | `refactor(query-api): build remaining test ontology structs via constructors` |
| **11** | `src/services/ingest/` (`src/model.rs` + `tests/*.rs`) and `src/services/engine/`, `engine-serving/`, `worker/` tests. | 119 | 19 | `//src/services/ingest/... //src/services/engine/... //src/services/engine-serving/... //src/services/worker/...` | `refactor(services): build ontology structs via constructors` |
| **12** | `src/services/runtime/src/admin.rs` + `src/testing/seed.rs` — the last production literals. | 8 | 4 | `//src/services/runtime/... //src/testing/...` | `refactor(runtime): build ontology structs via constructors` |

> Site counts are from the spec's census and are approximate for the two query-api splits — the split is by file theme, not by exact count. If a task's file list turns out not to partition cleanly, adjust the boundary and note it; do not leave a file unmigrated.

### Task 12b: PR 1 gate

- [ ] **Step 1: Confirm no ontology struct literals remain outside `core/src/ontology.rs`**

Run:
```bash
grep -rn "ObjectType {\|PropertyDef {\|LinkDef {\|DerivedPropertyDef {\|ParamDef {\|VectorIndexDef {\|ActionDef {" --include=*.rs src/ | grep -v "src/control-plane/core/src/ontology.rs"
```
Expected: **no output**. Any hit is either a missed literal (migrate it) or a pattern-match/destructuring (leave it — and note it here so the next reader isn't confused).

- [ ] **Step 2: Full suite green**

Run: `buck2 test --console none -j 8 //src/...`
Expected: `Tests finished: Pass N. Fail 0`. **`-j 8` is mandatory** — the postgres fixture has 8 boot-slots and an uncapped run starves them into 120s timeouts.

- [ ] **Step 3: Open PR 1**

```bash
git push -u origin work/fut-ontology-semantic-descriptions
gh pr create --title "refactor(ontology): construct ontology structs via constructors" --body "$(cat <<'EOF'
Pure refactor, no behavior change. Prepares `road-ontology-semantic-descriptions`
by giving all 7 ontology structs a plain constructor + chainable modifiers and
migrating the 705 test/testkit struct literals onto them, so the upcoming
`description` field (and the next field after it) costs ~nothing at call sites.

Spec: `docs/superpowers/specs/2026-07-16-ontology-semantic-descriptions-design.md`

Review note: the risk in this diff is semantic, not structural — a
`PropertyDef { required: true }` migrated to `PropertyDef::new(..)` without
`.required()` compiles clean and silently inverts a test. It was migrated
file-by-file and reviewed line-by-line for exactly this.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

https://claude.ai/code/session_01RTXmJiJhj1awb1tg9iFf38
EOF
)"
```

**PR 1 must be merged before Task 13.** PR 2 rebases onto it.

---

# PR 2 — The feature

### Task 13: `description` field + `.described()` on the 7 structs

**Files:**
- Modify: `src/control-plane/core/src/ontology.rs`
- Test: `src/control-plane/core/tests/ontology_builder.rs`, `src/control-plane/core/tests/governance_serde_roundtrip.rs`

**Interfaces:**
- Consumes: every constructor from Tasks 1–4.
- Produces: `pub description: Option<String>` on `ObjectType`, `PropertyDef`, `LinkDef`, `DerivedPropertyDef`, `ActionDef`, `ParamDef`, `VectorIndexDef`; `.described(text: impl Into<String>) -> Self` on each of them and on `ObjectTypeBuilder`/`ActionDefBuilder`. Used by Tasks 14–18.

- [ ] **Step 1: Write the failing tests**

Append to `src/control-plane/core/tests/ontology_builder.rs`:

```rust
#[test]
fn described_carries_prose_and_defaults_to_none() {
    assert_eq!(PropertyDef::new("email", "EmailAddress").description, None);
    assert_eq!(
        PropertyDef::new("email", "EmailAddress").described("The customer's primary email").description.as_deref(),
        Some("The customer's primary email")
    );
}

#[test]
fn described_trims_and_normalizes_blank_to_none() {
    assert_eq!(PropertyDef::new("e", "T").described("  spaced  ").description.as_deref(), Some("spaced"));
    assert_eq!(PropertyDef::new("e", "T").described("   ").description, None);
    assert_eq!(PropertyDef::new("e", "T").described("").description, None);
}

#[test]
fn described_is_available_on_every_declarable_entity() {
    assert!(ObjectType::build("C", ("wh", "c")).described("A customer").done().description.is_some());
    assert!(LinkDef::fk("customer", "Order", "Customer", Cardinality::One, "cid", "id")
        .described("The order's placer").description.is_some());
    assert!(DerivedPropertyDef::new("n", "Long", "orders", Aggregation::Count).described("Order count").description.is_some());
    assert!(ParamDef::new("email", "EmailAddress").described("Contact address").description.is_some());
    assert!(VectorIndexDef::new("i", "Doc", "embedding", Metric::Cosine, IndexSpec::Flat)
        .described("Semantic search index").description.is_some());
    assert!(ActionDef::build("createCustomer", "Customer", ActionKind::Insert)
        .described("Registers a new customer").done().description.is_some());
}
```

Append to `src/control-plane/core/tests/governance_serde_roundtrip.rs`:

```rust
#[test]
fn description_absent_from_json_decodes_to_none() {
    // The engine-wire compat guarantee: ontology structs cross that wire as serde-JSON
    // strings, so a payload written before this field existed must still decode.
    let json = r#"{"name":"email","ty":"EmailAddress","required":true}"#;
    let p: PropertyDef = serde_json::from_str(json).unwrap();
    assert_eq!(p.description, None);
}

#[test]
fn none_description_is_omitted_from_json() {
    // ... and re-encodes byte-identically to today's payload.
    let json = serde_json::to_string(&PropertyDef::new("email", "EmailAddress").required()).unwrap();
    assert!(!json.contains("description"), "None must not serialize a key, got: {json}");
}

#[test]
fn some_description_round_trips() {
    let p = PropertyDef::new("email", "EmailAddress").described("Primary email");
    let back: PropertyDef = serde_json::from_str(&serde_json::to_string(&p).unwrap()).unwrap();
    assert_eq!(back, p);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `buck2 test --console none //src/control-plane/core:ontology-builder //src/control-plane/core:governance-serde-roundtrip`
Expected: FAIL — `no field 'description' on type 'PropertyDef'`

- [ ] **Step 3: Write minimal implementation**

Add to each of the 7 structs (shown for `PropertyDef`; repeat verbatim for `ObjectType`, `LinkDef`, `DerivedPropertyDef`, `ActionDef`, `ParamDef`, `VectorIndexDef`):

```rust
    /// Optional human-readable prose describing this entity. Pure annotation — nothing in
    /// the engine consumes it; it feeds the ontology read surface and generated API docs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
```

Set `description: None` in every constructor from Tasks 1–4 and in the internal literals in `core/src/ontology.rs`.

Add a shared normalizer near the top of the file:

```rust
/// Trim a description and normalize blank/whitespace-only to `None`, so an empty
/// string cannot round-trip as `Some("")`.
fn normalize_description(text: impl Into<String>) -> Option<String> {
    let text = text.into();
    let trimmed = text.trim();
    if trimmed.is_empty() { None } else { Some(trimmed.to_string()) }
}
```

Then `.described()` on each of the 7 structs (shown for `PropertyDef`):

```rust
    /// Attach human-readable prose. Trims; blank/whitespace-only becomes `None`.
    pub fn described(mut self, text: impl Into<String>) -> PropertyDef {
        self.description = normalize_description(text);
        self
    }
```

And on the two builders, which set it on the struct under construction:

```rust
    /// Attach human-readable prose to the type being built.
    pub fn described(mut self, text: impl Into<String>) -> Self {
        self.inner.description = normalize_description(text);
        self
    }
```

> `ActionDefBuilder` has no `inner` — it holds `name`/`steps`/`downstream` directly. Add a `description: Option<String>` field to the builder, seed it `None` in `ActionDef::build`, and move it into the `ActionDef` in `done()`.

> **`ActionDef` has a serde bridge** (`#[serde(from = "ActionDefRepr", into = "ActionDefRepr")]`). `ActionDefRepr` must carry `description` on **both** its `Flat` and `Stepped` variants with the same `#[serde(default, skip_serializing_if = "Option::is_none")]`, or the field is silently dropped on every action round-trip. Check the `From`/`Into` impls both ways.

- [ ] **Step 4: Run tests to verify they pass**

Run: `buck2 test --console none //src/control-plane/core/...`
Expected: PASS (whole crate — the field addition touches every core test)

- [ ] **Step 5: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/control-plane/core/
git commit -m "feat(core): add description to ontology domain structs"
```

---

### Task 14: Migration + `.sqlx` regen

**Files:**
- Create: `src/control-plane/postgres/migrations/0029_ontology_description.sql`
- Modify: `src/control-plane/postgres/.sqlx/` (regenerated)

**Interfaces:**
- Consumes: nothing.
- Produces: a nullable `description text` column on `ontology.object_type`, `property`, `link`, `derived_property`, `action`, `action_param`, `vector_index_definition`. Used by Task 15.

- [ ] **Step 1: Confirm the migration number is free**

Run: `ls src/control-plane/postgres/migrations/ | tail -3`
Expected: highest is `0028_auth_lockout.sql` ⇒ `0029` is free. **If `0029` is taken** (a concurrent PR landed one), use the next free number and rename. Two PRs taking the same number turns every fixture red at once — the `embedded-migrations-unit` test is the tell.

- [ ] **Step 2: Write the migration**

Create `src/control-plane/postgres/migrations/0029_ontology_description.sql`:

```sql
-- Optional human-readable prose on every declarable ontology entity. Pure annotation:
-- nothing in the engine consumes it; it feeds the ontology read surface and generated
-- API docs. Nullable-add only — no rewrite, no backfill.
-- action_step gets no column: steps are positional, not named.
alter table ontology.object_type            add column description text;
alter table ontology.property               add column description text;
alter table ontology.link                   add column description text;
alter table ontology.derived_property       add column description text;
alter table ontology.action                 add column description text;
alter table ontology.action_param           add column description text;
alter table ontology.vector_index_definition add column description text;
```

- [ ] **Step 3: Apply and verify it boots**

Run: `buck2 test --console none //src/control-plane/postgres:embedded-migrations-unit`
Expected: PASS (the migration applies cleanly against a fresh hermetic postgres)

> If that target name is wrong, find it: `grep -rn "migration" src/control-plane/postgres/BUCK`.

- [ ] **Step 4: Commit** (the `.sqlx` regen lands in Task 15, once the SQL actually changes)

```bash
buck2 run //tools:prek -- run --all-files
git add src/control-plane/postgres/migrations/0029_ontology_description.sql
git commit -m "feat(postgres): add ontology description columns"
```

---

### Task 15: Postgres persistence

**Files:**
- Modify: `src/control-plane/postgres/src/ontology.rs`
- Modify: `src/control-plane/postgres/.sqlx/` (regenerated)
- Test: covered by Task 16's contract (both adapters)

**Interfaces:**
- Consumes: Task 13's `description` field, Task 14's columns.
- Produces: `description` persisted by every `define_*` and read back by every `get`/`list`. Used by Tasks 17–18.

- [ ] **Step 1: Write the failing test**

This task's test IS Task 16's contract. **Do Task 16 Step 1 first**, watch it fail against postgres, then return here. (The contract is shared: writing it once covers memory and postgres both. Duplicating it per-adapter is exactly the duplication the repo's dedup routine collapses.)

- [ ] **Step 2: Run it to verify it fails**

Run: `buck2 test --console none //src/control-plane/postgres:ontology`
Expected: FAIL — the description round-trips as `None` (column written by Task 14, never read or written by the adapter)

- [ ] **Step 3: Write the implementation**

In `src/control-plane/postgres/src/ontology.rs`, for each of the 7 entities:

- add `description` to the `insert into ontology.<table> (...) values (...)` column list and bind `def.description` (as `Option<&str>` — sqlx maps `Option` to NULL);
- add `description` to each `select` that hydrates the struct, and set it on the constructed value.

These are compile-time `sqlx::query!`/`query_scalar!` macros, so a column/bind mismatch is a **build** error, not a runtime one.

- [ ] **Step 4: Regenerate the `.sqlx` cache**

Run: `./tools/sqlx-prepare.sh`
Expected: `src/control-plane/postgres/.sqlx/` changes. **Commit it** — a stale cache fails the build, and `//src/control-plane/postgres:sqlx-cache-check` fails the test sweep.

- [ ] **Step 5: Run tests to verify they pass**

Run: `buck2 test --console none //src/control-plane/postgres/... //src/control-plane/memory/...`
Expected: `Tests finished: Pass N. Fail 0`

- [ ] **Step 6: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/control-plane/postgres/
git commit -m "feat(postgres): persist ontology descriptions"
```

---

### Task 16: `ontology_contract` description assertions

**Files:**
- Modify: `src/control-plane/testkit/src/lib.rs` (in `ontology_contract`, from line 1215)

**Interfaces:**
- Consumes: Task 13's `.described()`, Task 15's persistence.
- Produces: description round-trip + clear-on-redefine coverage for **both** adapters (memory and postgres both run this contract).

- [ ] **Step 1: Write the failing test**

Append to the existing `ontology_contract` function in `src/control-plane/testkit/src/lib.rs`:

```rust
    // --- descriptions: round-trip, absence, and clear-on-redefine ---

    let described = ObjectType::build("Described", ("wh", "described"))
        .described("A type that carries prose")
        .prop_req("id", "Long")
        .add_prop(PropertyDef::new("email", "EmailAddress").described("Primary contact address"))
        .identity("id")
        .done();
    o.define_type(described.clone()).await.unwrap();

    let got = o.get_type(&TypeName("Described".to_string())).await.unwrap();
    assert_eq!(got.description.as_deref(), Some("A type that carries prose"));
    assert_eq!(got.properties[1].description.as_deref(), Some("Primary contact address"));
    assert_eq!(got.properties[0].description, None, "an undescribed property stays None");

    o.define_link(
        LinkDef::fk("self_link", "Described", "Described", Cardinality::One, "id", "id")
            .described("Points at itself"),
    )
    .await
    .unwrap();
    let links = o.links(&TypeName("Described".to_string()), PageReq::unbounded()).await.unwrap();
    assert_eq!(links.items[0].description.as_deref(), Some("Points at itself"));

    // Redefining WITHOUT a description clears it — define_* is replace-not-merge.
    let mut plain = described.clone();
    plain.description = None;
    plain.properties[1].description = None;
    o.define_type(plain).await.unwrap();
    let got = o.get_type(&TypeName("Described".to_string())).await.unwrap();
    assert_eq!(got.description, None, "redefine without a description must clear it");
    assert_eq!(got.properties[1].description, None);
```

Extend the same contract to cover `ActionDef`, `ParamDef`, `DerivedPropertyDef` and `VectorIndexDef` descriptions, following the shape of whatever those entities' existing assertions in this contract look like — reuse the types already defined earlier in the function rather than seeding new ones.

- [ ] **Step 2: Run it to verify it fails**

Run: `buck2 test --console none //src/control-plane/memory/...`
Expected: FAIL if Task 15 is not yet done for postgres; the **memory** fake should PASS immediately (it stores the structs whole — that asymmetry is the point of running one contract against both).

- [ ] **Step 3: Make it pass**

No new implementation — this is Task 15's test. If memory fails, the fake is dropping the field somewhere; fix that. If postgres fails, return to Task 15 Step 3.

- [ ] **Step 4: Run tests to verify they pass**

Run: `buck2 test --console none //src/control-plane/memory/... //src/control-plane/postgres/...`
Expected: `Tests finished: Pass N. Fail 0`

- [ ] **Step 5: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/control-plane/testkit/src/lib.rs
git commit -m "test(testkit): cover ontology descriptions in the shared contract"
```

---

### Task 17: `/ontology/types/{name}` read surface

**Files:**
- Modify: `src/services/query-api/src/http.rs` (`link_view_json` at :168, `get_ontology_type` at :196)
- Modify: `src/services/query-api/src/openapi.rs` (`LinkView` :121, `TypeDetailResponse` :133, and `PropertyView`)
- Test: `src/services/query-api/tests/ontology_type_detail.rs`

**Interfaces:**
- Consumes: Task 13's field, Task 15's persistence.
- Produces: `description` in the type-detail JSON, omitted when absent.

- [ ] **Step 1: Write the failing test**

Add to `src/services/query-api/tests/ontology_type_detail.rs`. It already has everything needed: a `seeded_control_plane()` fixture (`Customer`, `Order`, and the FK link `Order.customer -> Customer`), an `app(cp)` router builder, and a `get(&app, uri) -> (StatusCode, serde_json::Value)` driver. Add a **second** seeder rather than describing the existing one — the existing tests assert `json["properties"]` against an exact array literal, and adding prose to the shared fixture would break them for no reason:

```rust
/// A `Doc` type carrying prose on the type, one of its two properties, and a self-link.
/// Separate from `seeded_control_plane` so the undescribed fixture keeps proving that an
/// absent description is omitted rather than emitted as null.
async fn seeded_described() -> MemoryControlPlane {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    cp.define_type(
        ObjectType::build("Doc", ("main", "docs"))
            .described("A document in the corpus")
            .add_prop(PropertyDef::new("id", "Long").required().described("The document's id"))
            .prop("body", "String")
            .identity("id")
            .done(),
    )
    .await
    .unwrap();
    cp.define_link(
        LinkDef::fk("parent", "Doc", "Doc", Cardinality::One, "parent_id", "id")
            .described("The document this one was split from"),
    )
    .await
    .unwrap();
    cp
}

#[tokio::test(flavor = "multi_thread")]
async fn type_detail_carries_descriptions() {
    let app = app(seeded_described().await);

    let (status, json) = get(&app, "/ontology/types/Doc").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["description"], "A document in the corpus");
    assert_eq!(json["properties"][0]["description"], "The document's id");
    assert_eq!(json["links"][0]["description"], "The document this one was split from");
}

#[tokio::test(flavor = "multi_thread")]
async fn type_detail_omits_absent_descriptions() {
    let app = app(seeded_described().await);

    let (status, json) = get(&app, "/ontology/types/Doc").await;
    assert_eq!(status, StatusCode::OK);
    // `body` carries no prose: the key is absent entirely, not null.
    assert!(
        json["properties"][1].get("description").is_none(),
        "absent description must be omitted, got: {}",
        json["properties"][1]
    );
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `buck2 test --console none //src/services/query-api:ontology-type-detail`
Expected: FAIL — `type_detail_carries_descriptions` fails on `json["description"]` being `Null` (the handler emits no such key). `type_detail_omits_absent_descriptions` **passes vacuously** at this point — it is the regression guard for Step 3, not a red test.

- [ ] **Step 3: Write the implementation**

In `src/services/query-api/src/http.rs`, `link_view_json` — note both handlers build JSON by hand, so the omit-when-absent behavior must be explicit:

```rust
fn link_view_json(l: &LinkDef) -> serde_json::Value {
    let mut v = serde_json::json!({
        "name": l.name,
        "from": l.from.0,
        "to": l.to.0,
        "cardinality": l.cardinality.as_str(),
    });
    if let Some(d) = &l.description {
        v["description"] = serde_json::Value::String(d.clone());
    }
    v
}
```

In `get_ontology_type`, the same shape for the type itself and each property:

```rust
    let properties: Vec<serde_json::Value> = ty
        .properties
        .iter()
        .map(|p| {
            let mut v = serde_json::json!({ "name": p.name, "ty": p.ty, "required": p.required });
            if let Some(d) = &p.description {
                v["description"] = serde_json::Value::String(d.clone());
            }
            v
        })
        .collect();

    let mut body = serde_json::json!({
        "name": ty.name.0,
        "table": { "schema": ty.table.schema, "name": ty.table.name },
        "identity": ty.identity,
        "properties": properties,
        "links": links.iter().map(link_view_json).collect::<Vec<_>>(),
        "links_to": links_to.iter().map(link_view_json).collect::<Vec<_>>(),
    });
    if let Some(d) = &ty.description {
        body["description"] = serde_json::Value::String(d.clone());
    }
    Json(body).into_response()
```

In `src/services/query-api/src/openapi.rs`, add to `TypeDetailResponse`, `PropertyView` and `LinkView`:

```rust
    /// Optional human-readable prose. Omitted when the entity carries none.
    pub description: Option<String>,
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `buck2 test --console none //src/services/query-api/...`
Expected: `Tests finished: Pass N. Fail 0`

- [ ] **Step 5: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/services/query-api/
git commit -m "feat(query-api): surface ontology descriptions on type detail"
```

---

### Task 18: OpenAPI generation

**Files:**
- Modify: `src/services/query-api/src/openapi_gen.rs` (`property_schema` :91, `type_component_schema` :126, `link_op` :231, `action_op` :319)
- Test: `src/services/query-api/tests/openapi_gen.rs`

**Interfaces:**
- Consumes: Task 13's field.
- Produces: ontology descriptions in the generated OpenAPI document.

- [ ] **Step 1: Write the failing test**

Add to `src/services/query-api/tests/openapi_gen.rs`. It already provides `ontology_openapi(&[types], &[links], &[actions]) -> (Paths, schemas)` and the `op_json(&paths, path, method)` helper:

```rust
#[test]
fn generated_document_carries_ontology_descriptions() {
    let doc_ty = ObjectType::build("Doc", ("main", "docs"))
        .described("A document in the corpus")
        .add_prop(PropertyDef::new("id", "Long").required().described("The document's id"))
        .prop("body", "String")
        .identity("id")
        .done();
    let create = ActionDef::build("createDoc", "Doc", ActionKind::Insert)
        .described("Registers a new document")
        .param_req("id", "Long")
        .done();

    let (paths, schemas) = ontology_openapi(&[doc_ty], &[], &[create]);

    let doc = serde_json::to_value(schemas.get("Doc").expect("Doc schema")).unwrap();
    assert_eq!(doc["description"], "A document in the corpus");
    assert_eq!(doc["properties"]["id"]["description"], "The document's id");
    assert!(
        doc["properties"]["body"].get("description").is_none(),
        "an undescribed property must carry no description key: {}",
        doc["properties"]["body"]
    );
    assert_eq!(
        op_json(&paths, "/actions/createDoc", "post")["description"],
        "Registers a new document"
    );
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `buck2 test --console none //src/services/query-api:openapi-gen`
Expected: FAIL — `doc["description"]` is `Null`; the generator sets no description on the component schema.

- [ ] **Step 3: Write the implementation**

`type_component_schema` — the type's own prose:

```rust
fn type_component_schema(ty: &ObjectType) -> RefOr<Schema> {
    let mut b = ObjectBuilder::new().schema_type(SchemaType::Type(Type::Object));
    if let Some(d) = &ty.description {
        b = b.description(Some(d.clone()));
    }
    // ... existing property loop unchanged
```

Thread each property's description into its schema. `property_schema(ty: &str, required: bool)` takes no `PropertyDef`, so give it the prose rather than widening it to the whole struct:

```rust
pub(crate) fn property_schema(ty: &str, required: bool, description: Option<&str>) -> RefOr<Schema> {
    // ... existing body, then before returning:
    // if let Some(d) = description { b = b.description(Some(d.to_string())); }
}
```

Update every `property_schema` call site (`grep -rn "property_schema" src/services/query-api/`), passing `p.description.as_deref()` where a `PropertyDef` is in scope and `None` elsewhere.

`action_op` and `link_op` set the operation description from `action.description` / `link.description` when present, leaving the existing generated summary untouched when absent:

```rust
    if let Some(d) = &action.description {
        op = op.description(Some(d.clone()));
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `buck2 test --console none //src/services/query-api/...`
Expected: `Tests finished: Pass N. Fail 0`

- [ ] **Step 5: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/services/query-api/
git commit -m "feat(query-api): carry ontology descriptions into generated OpenAPI"
```

---

### Task 19: Registers, capability doc, PR 2

**Files:**
- Modify: `docs/ROADMAP.md` (remove `road-ontology-semantic-descriptions`)
- Modify: `docs/FUTURE.md` (add the persist-only follow-on)
- Modify: `docs/system-capabilities/` (document the landed capability)

- [ ] **Step 1: Close the roadmap item**

Remove the `road-ontology-semantic-descriptions` entry from `docs/ROADMAP.md` entirely — the registers carry **open work only**; git history keeps the record. If it was the only item under `## ontology`, leave the empty section header (ROADMAP keeps empty area sections).

- [ ] **Step 2: Record the persist-only follow-on in `docs/FUTURE.md`**

Under `## ontology`:

```markdown
- [ ] **Surface derived properties and vector indexes on the ontology read path** `{#fut-ontology-derived-index-read-surface area:ontology status:deferred from:2026-07-16-ontology-semantic-descriptions-design pr:- spec:-}`
  `road-ontology-semantic-descriptions` gave `DerivedPropertyDef` and `VectorIndexDef` a persisted `description` with **nowhere to read it**: neither derived properties nor vector indexes appear on `GET /ontology/types/{name}` or in the generated OpenAPI document (the type detail reports `properties`/`identity`/`table`/`links` only). Their descriptions are therefore write-only today. Surfacing them means deciding what a derived property looks like on the read surface (it is served alongside real properties by the engine, but is not declared in the type detail) and whether index declarations belong on a governed metadata endpoint at all. Deferred until an ontology-browsing consumer needs either.
```

- [ ] **Step 3: Document the landed capability**

Add the description capability to the ontology page under `docs/system-capabilities/` (see that directory's `README.md` for the per-subsystem convention). Cover: the 7 entities, clear-on-redefine, the two persist-only entities, and that it is annotation only — nothing plans or executes on it.

- [ ] **Step 4: Validate + full suite**

Run:
```bash
bash tools/docs.sh validate
buck2 test --console none -j 8 //src/...
```
Expected: `docs.sh validate: OK` and `Tests finished: Pass N. Fail 0`

- [ ] **Step 5: Commit and open PR 2**

```bash
buck2 run //tools:prek -- run --all-files
git add docs/
git commit -m "docs: close road-ontology-semantic-descriptions"
git push
gh pr create --title "feat(ontology): semantic description fields across the ontology" --body "$(cat <<'EOF'
Optional human-readable `description` on all 7 declarable ontology entities,
persisted on the `define_*` path and surfaced on `GET /ontology/types/{name}`
and in the generated OpenAPI document.

- No engine-wire `.proto` change: ontology structs cross that wire as serde-JSON
  strings, so `serde(default, skip_serializing_if)` makes old payloads decode and
  `None` re-encode byte-identically.
- Descriptions clear on redefine — `define_*` is replace-not-merge, pinned by the
  shared `ontology_contract` (memory + postgres).
- Derived properties and vector indexes are persist-only: neither is on any read
  surface today. Tracked as `fut-ontology-derived-index-read-surface`.

Spec: `docs/superpowers/specs/2026-07-16-ontology-semantic-descriptions-design.md`
Closes `road-ontology-semantic-descriptions`.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

https://claude.ai/code/session_01RTXmJiJhj1awb1tg9iFf38
EOF
)"
```
