# Vector-index define returns 404 for an unknown type — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the postgres `define_vector_index` adapter return `NotFound` (→ HTTP 404) for an unknown object type, matching the memory adapter and the documented `POST /admin/models/{type}/vector-indexes` contract.

**Architecture:** The postgres adapter currently runs a single property lookup, so an unknown type falls into the missing-property arm and yields `Validation` (→ 400 with a misleading "type X has no property Y" message). Add a leading `object_type_exists` probe (the shared existence helper already used by `define_link`/`define_action`) that returns `NotFound` before the property lookup. Pin the behavior for both adapters with a testkit ontology-contract case.

**Tech Stack:** Rust, sqlx compile-time macros (no `.sqlx` regen — reuses an existing `query_scalar!`), buck2 `loom_fixture_test` for the postgres contract leg.

## Global Constraints

- **Tests are `rust_test` integration targets only** — the change lands in the existing testkit contract (`//src/control-plane/testkit`), not an inline `#[cfg(test)]` module.
- **No `.sqlx` regen** — the probe reuses the pre-existing `object_type_exists` `query_scalar!`; no new SQL text. The `sqlx-cache-check` fixture test still re-validates the cache.
- **Clippy strict** — production code is under `pedantic + restriction`; the added `if !… { return Err(…) }` uses no `unwrap`/`expect`/indexing.
- Commit messages end with the two required trailers (`Co-Authored-By:` + `Claude-Session:`); commit subjects follow Conventional Commits.

---

### Task 1: Postgres `define_vector_index` returns `NotFound` for an unknown type

**Files:**
- Modify: `src/control-plane/postgres/src/ontology.rs:496-504` (add the probe at the top of `define_vector_index`)
- Test: `src/control-plane/testkit/src/lib.rs:1442-1464` (extend the vector-index validation block with an unknown-type case)

**Interfaces:**
- Consumes: `object_type_exists(ex: impl sqlx::PgExecutor<'_>, name: &str) -> Result<bool>` (`ontology.rs:635`, `pub(crate)`); `ControlPlaneError::NotFound(String)`; `VectorIndexDef { name, type_name: TypeName, property, metric, spec }`; `Metric::Cosine`, `IndexSpec::Flat`; the contract's `tn(&str) -> TypeName` helper.
- Produces: no new public surface — behavior change only (postgres unknown-type → `NotFound`).

- [ ] **Step 1: Write the failing contract case**

In `src/control-plane/testkit/src/lib.rs`, immediately after the existing missing-property assertion (currently ending at line 1464, the `"missing property rejected"` block), add:

```rust
    // unknown type -> NotFound (not Validation): the type, not the property,
    // is what is missing. Pins postgres to the memory adapter's answer.
    let unknown_type = VectorIndexDef {
        name: "bad3".into(),
        type_name: tn("Ghost"), // never defined in this contract
        property: "embedding".into(),
        metric: Metric::Cosine,
        spec: IndexSpec::Flat,
    };
    assert!(
        matches!(
            o.define_vector_index(unknown_type).await,
            Err(ControlPlaneError::NotFound(_))
        ),
        "unknown type is NotFound, not Validation"
    );
```

- [ ] **Step 2: Run the contract against postgres to verify it fails**

Run: `buck2 test --console none //src/control-plane/testkit/... //src/control-plane/postgres/...`
Expected: the postgres leg of the ontology contract FAILS the new assertion (postgres currently returns `Err(Validation(_))`, so `matches!(…, Err(NotFound(_)))` is false). The memory leg already passes.

- [ ] **Step 3: Add the early type-existence probe**

In `src/control-plane/postgres/src/ontology.rs`, at the very top of `define_vector_index` (before the `select ty from ontology.property …` lookup at line 497), insert:

```rust
        if !object_type_exists(&self.pool, &def.type_name.0).await? {
            return Err(ControlPlaneError::NotFound(def.type_name.0.clone()));
        }
```

`object_type_exists` is already in scope in this module (`ontology.rs:635`, same file). With the type known to exist, the subsequent `None` arm now unambiguously means "type exists but has no such property," so its `Validation` message stays correct.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `buck2 test --console none //src/control-plane/testkit/... //src/control-plane/postgres/...`
Expected: `Pass N. Fail 0` — both adapter legs of the ontology contract pass, including the new unknown-type case; the `sqlx-cache-check` test still passes (no SQL text changed).

- [ ] **Step 5: Confirm the HTTP boundary is already guarded (no change)**

The admin HTTP test already asserts 404 for an unknown type on the memory adapter (`src/services/runtime/tests/admin_management.rs:1248-1259`, `vector_index_errors`, `POST /admin/models/Nope/vector-indexes` → `StatusCode::NOT_FOUND`). No new HTTP test is required; verify it still passes:

Run: `buck2 test --console none //src/services/runtime/...`
Expected: `vector_index_errors` passes (unchanged).

- [ ] **Step 6: Commit**

```bash
git add src/control-plane/postgres/src/ontology.rs src/control-plane/testkit/src/lib.rs
git commit -m "fix(ontology): postgres vector-index define returns 404 for an unknown type"
```

(Commit body carries the two required trailers.)
