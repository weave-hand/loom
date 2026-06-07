# Per-Concern Adapter File Split Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split each control-plane adapter crate's monolithic `lib.rs` into one file per concern (queue/catalog/ontology/acl/lineage/transaction), with zero behaviour change.

**Architecture:** Rust child modules can `impl Trait for ParentStruct` and read the parent's private fields, so each concern's trait impl moves to its own module file with no visibility change to the struct. Only helpers/state that move out of `lib.rs` but stay referenced by `lib.rs` or a sibling become `pub(crate)`. Correctness is proven by the unchanged existing test suite passing identically.

**Tech Stack:** Rust, buck2, async-trait, sqlx (postgres), tokio.

---

## Conventions for this plan

- **Build/test:** `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...` (pg/duckdb refuse root on RE). Library targets are `//src/control-plane/memory:memory` and `//src/control-plane/postgres:postgres` (target name is `memory`/`postgres`, NOT `control-plane-memory`).
- **No BUCK edits:** both crates already use `srcs = glob(["src/**/*.rs"])`, so new files under `src/` are auto-included. Step 2 in each task is now just a confirmation, not an edit.
- **Format before every commit:** `eval "$(./tools/env.sh)"` once, then `rustfmt --edition 2024 <new files>` (the prek rustfmt hook is check-only and will block the commit otherwise).
- **Clippy:** `tools/clippy-all.sh` must be clean — watch for `unused_imports` / `dead_code` introduced by the move.
- **No BUCK changes needed:** new `.rs` files under `src/` are picked up by the existing `rust_library` glob; verify by building. (If the crate's BUCK lists `srcs` explicitly rather than a glob, add the new files — check first.)
- This is a **pure move**: do not change any signature, logic, comment, or test. If a diff shows anything other than relocated lines + added `use`/`mod`/`pub(crate)`, that's a bug.

---

## Task 1: Split the memory adapter

**Files:**
- Modify: `src/control-plane/memory/src/lib.rs` (becomes struct + constructor + shared primitives + `impl ControlPlane` + `mod` decls)
- Create: `src/control-plane/memory/src/queue.rs`
- Create: `src/control-plane/memory/src/catalog.rs`
- Create: `src/control-plane/memory/src/ontology.rs`
- Create: `src/control-plane/memory/src/acl.rs`
- Create: `src/control-plane/memory/src/lineage.rs`
- Create: `src/control-plane/memory/src/transaction.rs`
- Check: `src/control-plane/memory/BUCK` (confirm glob `srcs`; add files if explicit)

- [ ] **Step 1: Baseline the suite (must be green before touching anything)**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/memory/...`
Expected: PASS. Record the per-test pass count — it must be identical after the refactor.

- [ ] **Step 2: Check the BUCK srcs style**

Run: `cat src/control-plane/memory/BUCK`
Expected: note whether `srcs` is a `glob(["src/**/*.rs"])` (no edit needed) or an explicit list (you must add the six new files in a later step). Most loom crates use a glob.

- [ ] **Step 3: Create `acl.rs` (move `impl Acl` + AclState + TargetKey + target_key)**

Move out of `lib.rs`, verbatim:
- `struct AclState { ... }` → make it `pub(crate) struct AclState`
- the `TargetKey` type alias/struct and `fn target_key(t: &PolicyTarget) -> TargetKey` → keep private to `acl.rs`
- the entire `#[async_trait] impl Acl for MemoryControlPlane { ... }` block (`define_subject`, `define_role`, `assign_role`, `unassign_role`, `grant`, `revoke`, `set_policy`, `clear_policy`, `check`, `policies_for`)

Add at the top of `acl.rs` a `use` block importing what these items reference (e.g. `use crate::MemoryControlPlane;`, `use async_trait::async_trait;`, the `control_plane_core` ACL types, `std::collections::*`, etc.). Don't guess exhaustively — build in Step 9 and let the compiler/clippy tell you what's missing or unused.

- [ ] **Step 4: Create `catalog.rs` (move `impl Catalog` + CatalogState + impl CatalogState)**

Move verbatim: `struct CatalogState` → `pub(crate) struct CatalogState`; the `impl CatalogState { new_snapshot, latest_live, ... }` block; the `#[async_trait] impl Catalog for MemoryControlPlane { current_snapshot, snapshots, files, schema }` block. Add a `use` block.

- [ ] **Step 5: Create `ontology.rs` (move `impl Ontology` + OntologyState)**

Move verbatim: `struct OntologyState` → `pub(crate) struct OntologyState`; the `#[async_trait] impl Ontology for MemoryControlPlane { define_type, define_link, get_type, list_types, links, resolve }` block. Add a `use` block.

- [ ] **Step 6: Create `lineage.rs` (move `impl Lineage` + LineageState)**

Move verbatim: `struct LineageState` → `pub(crate) struct LineageState`; the `#[async_trait] impl Lineage for MemoryControlPlane { emit, events_for, upstream, downstream }` block. Add a `use` block.

- [ ] **Step 7: Create `queue.rs` (move `impl Queue`)**

Move verbatim the `#[async_trait] impl Queue for MemoryControlPlane { enqueue, dequeue, complete, fail, heartbeat, await_jobs }` block. (No dedicated state struct — queue fields live on `MemoryControlPlane`.) Add a `use` block.

- [ ] **Step 8: Create `transaction.rs` (move `MemoryTx` + impl Tx)**

Move verbatim: `struct MemoryTx { ... }` and the `#[async_trait] impl Tx for MemoryTx { commit, rollback, enqueue, emit }` block. `MemoryTx` is constructed in `MemoryControlPlane::begin` (which stays in `lib.rs`), so `MemoryTx` and its fields must be at least `pub(crate)`. Add a `use` block.

- [ ] **Step 9: Wire `lib.rs` — declare modules, fix visibility of retained primitives**

In `lib.rs`:
- Add module declarations: `mod queue; mod catalog; mod ontology; mod acl; mod lineage; mod transaction;`
- Keep: `pub struct MemoryControlPlane { ... }` (fields now referenced from sibling modules — child modules can read private parent fields, so **no field visibility change needed**), `impl MemoryControlPlane { pub fn new(...) }`, the `#[async_trait] impl ControlPlane for MemoryControlPlane { begin }` block.
- Keep `struct Row` and `struct Versioned<T>` (+ its `impl`) but make them `pub(crate)` since catalog/ontology/acl modules now reference them.
- Remove the now-moved `use` imports that `lib.rs` no longer needs (clippy will flag leftovers in Step 11).

- [ ] **Step 10: Format the new files**

Run: `eval "$(./tools/env.sh)" && rustfmt --edition 2024 src/control-plane/memory/src/*.rs`
Expected: no diff complaints later from the prek rustfmt hook.

- [ ] **Step 11: Build + clippy the memory crate**

Run: `buck2 build //src/control-plane/memory:memory && tools/clippy-all.sh 2>&1 | grep -i memory`
Expected: builds clean; clippy reports nothing for memory (no `unused_imports`, no `dead_code`). Fix `use` blocks until clean.

- [ ] **Step 12: Run the memory suite — must match the Step 1 baseline exactly**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/memory/...`
Expected: PASS, identical per-test counts to Step 1.

- [ ] **Step 13: Sanity-check the diff is a pure move**

Run: `git diff --stat && git diff src/control-plane/memory/src/lib.rs`
Expected: `lib.rs` only *lost* lines (the moved blocks) plus gained `mod`/`pub(crate)`; new files only *gained* the moved lines + `use`. No logic/signature/comment changes.

- [ ] **Step 14: Commit**

```bash
git add src/control-plane/memory/
git commit -m "refactor(memory): split adapter into per-concern files

Pure move: each trait impl + its state to its own module. No behaviour change.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: Split the postgres adapter

**Files:**
- Modify: `src/control-plane/postgres/src/lib.rs` (becomes struct + constructor + `impl ControlPlane` + `mod` decls + `pub mod fixture`)
- Create: `src/control-plane/postgres/src/queue.rs`
- Create: `src/control-plane/postgres/src/catalog.rs`
- Create: `src/control-plane/postgres/src/ontology.rs`
- Create: `src/control-plane/postgres/src/acl.rs`
- Create: `src/control-plane/postgres/src/lineage.rs`
- Create: `src/control-plane/postgres/src/transaction.rs`
- Leave untouched: `src/control-plane/postgres/src/fixture.rs`
- Check: `src/control-plane/postgres/BUCK` (confirm glob `srcs`; add files if explicit)

- [ ] **Step 1: Baseline the suite**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/postgres/...`
Expected: PASS. Record per-test counts.

- [ ] **Step 2: Check the BUCK srcs style**

Run: `cat src/control-plane/postgres/BUCK`
Expected: note glob vs explicit `srcs`; if explicit, add the six new files later. Confirm `fixture.rs` stays listed/globbed unchanged.

- [ ] **Step 3: Create `queue.rs` (move `impl Queue` + `pg_insert`)**

Move verbatim: `async fn pg_insert<'e, E: sqlx::PgExecutor<'e>>(...)` → make it `pub(crate) async fn pg_insert` (used by `transaction.rs` too); the `#[async_trait] impl Queue for PgControlPlane { enqueue, dequeue, complete, fail, heartbeat, await_jobs }` block. Add a `use` block.

- [ ] **Step 4: Create `lineage.rs` (move `impl Lineage` + `pg_emit` + `event_datasets` + `graph_step`)**

Move verbatim:
- `async fn pg_emit<'e, E: sqlx::PgExecutor<'e>>(...)` → `pub(crate) async fn pg_emit` (used by `transaction.rs`)
- the inherent helpers `event_datasets` and `graph_step` (currently in an `impl PgControlPlane` block) — move them into a `catalog.rs`/`lineage.rs`-local `impl PgControlPlane` block in `lineage.rs`; keep **private** (only `Lineage::{upstream,downstream}` use them)
- the `#[async_trait] impl Lineage for PgControlPlane { emit, events_for, upstream, downstream }` block

Add a `use` block.

- [ ] **Step 5: Create `catalog.rs` (move `impl Catalog` + `resolve_table`)**

Move verbatim: the `resolve_table` inherent method into a `catalog.rs`-local `impl PgControlPlane` block, kept **private** (only `Catalog::{files,schema}` use it); the `#[async_trait] impl Catalog for PgControlPlane { current_snapshot, snapshots, files, schema }` block. Add a `use` block.

Note: the original `impl PgControlPlane { resolve_table, event_datasets, graph_step }` block is now fully distributed (resolve_table→catalog.rs, the other two→lineage.rs); delete the now-empty original block from `lib.rs`.

- [ ] **Step 6: Create `ontology.rs` (move `impl Ontology`)**

Move verbatim the `#[async_trait] impl Ontology for PgControlPlane { define_type, define_link, get_type, list_types, links, resolve }` block. Add a `use` block.

- [ ] **Step 7: Create `acl.rs` (move `impl Acl`)**

Move verbatim the `#[async_trait] impl Acl for PgControlPlane { define_subject, define_role, assign_role, unassign_role, grant, revoke, set_policy, clear_policy, check, policies_for }` block. Add a `use` block.

- [ ] **Step 8: Create `transaction.rs` (move `PgTx` + impl Tx)**

Move verbatim: `struct PgTx { ... }` and the `#[async_trait] impl Tx for PgTx { commit, rollback, enqueue, emit }` block. `PgTx::{enqueue,emit}` call `pg_insert`/`pg_emit` — import them: `use crate::queue::pg_insert; use crate::lineage::pg_emit;`. `PgTx` is constructed in `PgControlPlane::begin` (stays in `lib.rs`) so it must be `pub(crate)`. Add the rest of the `use` block.

- [ ] **Step 9: Wire `lib.rs`**

In `lib.rs`:
- Add: `mod queue; mod catalog; mod ontology; mod acl; mod lineage; mod transaction;` (keep `pub mod fixture;`)
- Keep: `pub struct PgControlPlane { ... }`, the `impl PgControlPlane { new/from_pool/... }` constructor block, the `#[async_trait] impl ControlPlane for PgControlPlane { begin }` block.
- Ensure the old multi-helper `impl PgControlPlane { resolve_table, event_datasets, graph_step }` block is gone (its methods moved in Steps 4–5).
- Drop `use` imports `lib.rs` no longer needs (clippy flags them).

- [ ] **Step 10: Format**

Run: `eval "$(./tools/env.sh)" && rustfmt --edition 2024 src/control-plane/postgres/src/*.rs`

- [ ] **Step 11: Build + clippy**

Run: `buck2 build //src/control-plane/postgres:postgres && tools/clippy-all.sh 2>&1 | grep -i postgres`
Expected: clean build; no clippy findings for postgres. Fix `use` blocks until clean.

- [ ] **Step 12: Run the postgres suite — must match Step 1 baseline**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/postgres/...`
Expected: PASS, identical counts to Step 1.

- [ ] **Step 13: Sanity-check the diff is a pure move**

Run: `git diff --stat && git diff src/control-plane/postgres/src/lib.rs`
Expected: pure relocation + `mod`/`pub(crate)`/`use` adjustments only.

- [ ] **Step 14: Commit**

```bash
git add src/control-plane/postgres/
git commit -m "refactor(postgres): split adapter into per-concern files

Pure move: each trait impl + its helpers to its own module. No behaviour change.
fixture.rs untouched.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: Full-tree verification

- [ ] **Step 1: Full build + test**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...`
Expected: PASS — total count identical to before the refactor (no tests added or removed).

- [ ] **Step 2: Full lint**

Run: `buck2 run //tools:prek -- run --all-files`
Expected: all hooks green (rustfmt, clippy, file checks, reindeer-in-sync, conventional-commit n/a for --all-files).

- [ ] **Step 3: Confirm no stray public-API change**

Run: `git diff main --stat src/control-plane/`
Expected: only the two adapter `src/` dirs changed; `core/`, `testkit/`, `worker/`, and all `tests/` untouched (the public trait surface is unchanged, so dependents don't move).
