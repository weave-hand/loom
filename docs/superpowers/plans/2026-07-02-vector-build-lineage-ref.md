# iss-vector-build-lineage-ref Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `build_vector_index`'s lineage event uses the canonical loom
`DatasetRef` for its input node, so the index-build edge joins the same lineage
node as every other producer/consumer of the table.

**Architecture:** One-line production fix — replace the ad-hoc
`DatasetRef { namespace: table.schema, name: table.name }` literal in
`postgres/src/vector_index.rs` with `DatasetRef::from(table)` (the
`impl From<&TableRef> for DatasetRef` convenience in
`core/src/identity.rs:66`, which yields `{namespace: "loom", name:
"schema.name"}` — identical to what the flush path emits at
`iceberg_flush.rs:147`). TDD: extend the existing lineage assertion in the
build e2e first.

**Tech Stack:** Rust, buck2, `loom_fixture_test` (hermetic Postgres).

## Global Constraints

- Spec: `docs/superpowers/specs/2026-07-02-pillar-idioms-audit-design.md`, Wave 0 defect 3 (`iss-vector-build-lineage-ref`).
- Tests are separate `rust_test` targets, never inline `#[cfg(test)]` (repo rule).
- The output node (`namespace: "loom-vector-index"`, name = puffin path) is **unchanged** — only the input node changes.
- Conventional-commit message format (commit-msg hook enforces it).

---

### Task 1: Canonical input DatasetRef in build_vector_index

**Files:**
- Modify: `src/control-plane/postgres/src/vector_index.rs:506-509`
- Test: `src/control-plane/postgres/tests/vector_index_build.rs` (existing target `//src/control-plane/postgres:vector-index-build`)

**Interfaces:**
- Consumes: `impl From<&TableRef> for DatasetRef` (`control_plane_core::identity`, already exported; `DatasetRef` is already imported by `vector_index.rs`).
- Produces: no new API — behavior fix only.

- [ ] **Step 1: Write the failing assertion**

In `src/control-plane/postgres/tests/vector_index_build.rs`, extend the
lineage check in `build_covers_all_rows_live_at_s` (after the existing
`outputs[0]` assertions at lines 253-260):

```rust
    // The input node must be the CANONICAL loom dataset ref (same node the
    // landing/flush paths emit), not an ad-hoc {schema, name} pair — otherwise
    // the index-build edge is disconnected from the table's lineage graph.
    // Guards iss-vector-build-lineage-ref.
    assert_eq!(
        events.items[0].inputs,
        vec![control_plane_core::DatasetRef::from(&table)],
        "input is the canonical loom dataset ref for the source table"
    );
```

- [ ] **Step 2: Run test to verify it fails**

Run: `buck2 test //src/control-plane/postgres:vector-index-build > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL|assert" /tmp/t1.log`
Expected: FAIL — left has `namespace: "wh"` (the raw schema), right has `namespace: "loom"`.

- [ ] **Step 3: Minimal implementation**

In `src/control-plane/postgres/src/vector_index.rs`, replace lines 506-509:

```rust
        inputs: vec![DatasetRef::from(table)],
```

(replacing the four-line `DatasetRef { namespace: table.schema.clone(), name: table.name.clone() }` literal; `table` is already `&TableRef` in scope).

- [ ] **Step 4: Run the test and the sibling vector suites**

Run: `buck2 test //src/control-plane/postgres:vector-index-build //src/control-plane/postgres:vector-index-multi //src/control-plane/postgres:flush-vector-rebuild > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t2.log`
Expected: PASS (no other test asserts the build event's inputs — verified by grep during planning).

- [ ] **Step 5: Close the register item**

In `docs/ISSUES.md`, flip the entry: `- [ ]` → `- [x]`, `status:open` →
`status:fixed`, `pr:-` → `pr:#N` once the PR number exists (done at PR time
per loom-docs-update), and prepend `Fixed (PR #N): ` to the prose with a
one-line resolution note.

- [ ] **Step 6: Commit**

```bash
git add src/control-plane/postgres/src/vector_index.rs src/control-plane/postgres/tests/vector_index_build.rs
git commit -m "fix(lineage): canonical DatasetRef input on vector-index build event

build_vector_index emitted {namespace: schema, name: table} while every
other emitter uses DatasetId::from(table).dataset_ref() ({loom,
schema.name}), giving the same table two disconnected lineage nodes.
Guarded by an inputs assertion in vector-index-build.

Closes iss-vector-build-lineage-ref."
```

(Register close commits separately with the PR-number edit.)
