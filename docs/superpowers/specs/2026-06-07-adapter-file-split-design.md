# Design: per-concern adapter file split (Step 2b, group 2)

> **Status:** approved design. Step 2b hardening group 2 from
> `2026-06-06-control-plane-critical-review.md` (§coupling). A pure mechanical
> refactor: no behaviour change, no public API change, no new tests.

## Problem

Each adapter crate is a single monolith:

- `postgres/src/lib.rs` — 909 lines: `PgControlPlane` + the `Queue`, `Catalog`,
  `Ontology`, `Acl`, `Lineage` trait impls + `PgTx` + free/inherent helpers.
- `memory/src/lib.rs` — 665 lines: `MemoryControlPlane` + the same five trait impls +
  per-concern state structs + `MemoryTx`.

Five concerns in one file means the blast radius of any concern-local change is the
whole file, and the upcoming Group 3 (tracing) / Group 4 (sqlx offline, proptests)
edits would all land in the same two files. Splitting by concern now shrinks that
radius and makes each concern independently holdable in context.

## Key fact that makes this safe

In Rust, **a child module can implement a trait for a type defined in an ancestor
module and can access that ancestor's private items**. So `postgres/src/catalog.rs`
(child of the crate root) can write `impl Catalog for PgControlPlane` and read
`PgControlPlane`'s private fields with **no `pub`/`pub(crate)` change to the struct or
its fields**. Only items that *move out of* `lib.rs` and are *still used by* `lib.rs`
or a sibling module need `pub(crate)` (the shared helpers and the memory state
structs the central struct embeds).

## Target layout (mirrored across both crates)

| File | Contents |
|------|----------|
| `lib.rs` | struct definition + constructor (`new`/`from_pool`) + `impl ControlPlane` (`begin`) + `mod` declarations. `postgres`: keeps `pub mod fixture`. `memory`: keeps the shared `Row` + `Versioned<T>` primitives. |
| `queue.rs` | `impl Queue` for the struct. **postgres:** + `pub(crate) async fn pg_insert`. |
| `catalog.rs` | `impl Catalog`. **postgres:** + `resolve_table` (as `pub(crate)`). **memory:** + `CatalogState` + `impl CatalogState`. |
| `ontology.rs` | `impl Ontology`. **memory:** + `OntologyState`. |
| `acl.rs` | `impl Acl`. **memory:** + `AclState` + `TargetKey` + `target_key`. |
| `lineage.rs` | `impl Lineage`. **postgres:** + `pub(crate) async fn pg_emit` + `event_datasets` + `graph_step`. **memory:** + `LineageState`. |
| `transaction.rs` | `PgTx`/`MemoryTx` struct + `impl Tx`. Imports `pg_insert`/`pg_emit` (postgres) or touches the shared state (memory). |

### Shared-helper ownership (postgres)

- `pg_insert` is called by `impl Queue::enqueue` (queue.rs) **and** `PgTx::enqueue`
  (transaction.rs) → lives in `queue.rs`, `pub(crate)`.
- `pg_emit` is called by `impl Lineage::emit` (lineage.rs) **and** `PgTx::emit`
  (transaction.rs) → lives in `lineage.rs`, `pub(crate)`.
- `resolve_table` is used only by `Catalog::{files,schema}` → moves to `catalog.rs`,
  kept **private** to that module (an inherent method on `PgControlPlane` in a
  `catalog.rs`-local `impl` block, or a free fn — implementer's choice).
- `event_datasets` + `graph_step` are used only by `Lineage::{upstream,downstream}` →
  `lineage.rs`, private to that module.

### State-struct ownership (memory)

- `Row` and `Versioned<T>` are generic primitives used across catalog/ontology/acl →
  stay in `lib.rs`, `pub(crate)`.
- `CatalogState`/`OntologyState`/`AclState`/`LineageState` each move to their concern
  file as `pub(crate)` (the `MemoryControlPlane` struct in `lib.rs` embeds them, so
  `lib.rs` must see them).
- `TargetKey` + `target_key` move to `acl.rs` (used only there).
- The queue has no dedicated state struct (its fields live directly on
  `MemoryControlPlane`); `impl Queue` simply moves to `queue.rs`.

## Imports

Each new file carries its own `use` block — only the symbols that file actually
references (the implementer adds them and lets `cargo` / clippy prune unused ones).
`lib.rs` adds `mod queue; mod catalog; mod ontology; mod acl; mod lineage; mod
transaction;` (all private — the trait impls are exported via the traits, the structs
via `pub`).

## Verification (no new tests)

This is behaviour-preserving. Correctness = the existing suite still passes and the
tree is lint-clean:

- `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...` → identical pass count
  to pre-refactor (worker 5, memory:* and postgres:* unchanged).
- `tools/clippy-all.sh` clean (no new `dead_code`/`unused_imports`).
- `rustfmt --edition 2024` applied to every new file before commit.
- `prek run --all-files` green.

## Scope / non-goals

- **Pure move.** No signature changes, no logic edits, no new behaviour, no new tests.
- Do **not** split `fixture.rs` (postgres) — it's already its own file and a separate
  concern (test harness), out of scope.
- Do **not** introduce a `state.rs` catch-all in memory; primitives stay in `lib.rs`.
- Commit per crate (two commits) so each diff is reviewable as "same code, moved".
