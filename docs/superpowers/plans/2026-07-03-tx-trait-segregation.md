# Tx Trait Segregation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split `Tx` (unit of work) from `TableTx: Tx` (staging surface) with `TableControlPlane::begin_table()`, delete `PgTx`'s dead stubs, and add `auth()` to the `ControlPlane` facade — compile-time honesty about which planes can stage files.

**Architecture:** The trait split is one compile-atomic migration (core + 4 adapters + transform + testkit + the transform test stub must move together — staging methods leaving `Tx` breaks every impl simultaneously), landed as one green commit. The facade `auth()` probe is red-first in the testkit contract. Whole-suite sweep matches the spec's acceptance.

**Tech Stack:** Rust (`async_trait`, stable dyn-upcasting `Box<dyn TableTx + Send>` → `Box<dyn Tx + Send>`), buck2.

**Spec:** `docs/superpowers/specs/2026-07-03-tx-trait-segregation-design.md`

## Global Constraints

- `commit`'s `Option<SnapshotId>` return STAYS on `Tx` (accepted residue; its doc comment must stop naming `create_table`/`append_files`).
- `WireControlPlane::begin()` keeps its `read_only("begin")` error — deliberate guard, do not touch.
- No `AuthState`/`bootstrap` rewiring — the accessor only.
- prek clean before every commit; test output redirected to files, never piped; test runs carry `--unstable-allow-all-tests-on-re` (root host).

---

### Task 1: The trait split + `auth()` accessor (one compile-atomic commit)

**Files:**
- Modify: `src/control-plane/core/src/transaction.rs` (both traits + `ControlPlane`), `src/control-plane/core/src/lib.rs` (exports)
- Modify: `src/control-plane/postgres/src/transaction.rs:26-73` (delete stubs + `no_table_format`), `src/control-plane/postgres/src/lib.rs:94-…` (`auth()`), `src/control-plane/postgres/src/iceberg_control_plane.rs:56-96+` (split impls, `TableControlPlane`, `auth()` → `self.pg.auth()`)
- Modify: `src/control-plane/memory/src/lib.rs:189-208` (`TableControlPlane`, `auth()` → `self`), `src/control-plane/memory/src/transaction.rs:38` (split `impl Tx` / `impl TableTx`)
- Modify: `src/services/query-api/src/wire_control_plane.rs:191-221` (`auth()` → `self.direct.auth()`)
- Modify: `src/services/transform/src/{run.rs:95+192, typed.rs:33, handler.rs:46+119, main.rs:39}` (`&dyn TableControlPlane` / `Arc<dyn TableControlPlane>`, `begin()` → `begin_table()`)
- Modify: `src/control-plane/testkit/src/lib.rs` — retighten `snapshot_commit_contract` (:3247), `snapshot_replace_contract` (:3372), `snapshot_write_order_contract` (:3449), `snapshot_compact_contract` (:3519), `tx_atomic_rollback_contract` (:3056) to `TableControlPlane` + `begin_table()`; extend `control_plane_facade_contract` (:3185) with the auth probe
- Modify: `src/services/transform/tests/run_unknown_input.rs:61+` (stub gains `TableControlPlane`/`TableTx` impls + `auth()`)

**Interfaces (produced, in `core/src/transaction.rs`):**

```rust
#[async_trait]
pub trait TableTx: Tx {
    // the four staging methods move here VERBATIM with their doc comments:
    async fn create_table(&mut self, table: &TableRef, columns: &[ColumnSpec]) -> Result<()>;
    async fn append_files(&mut self, table: &TableRef, files: &[DataFile]) -> Result<()>;
    async fn replace_files(&mut self, table: &TableRef, files: &[DataFile]) -> Result<()>;
    async fn compact_files(&mut self, table: &TableRef, expire: &[String], write: &[DataFile]) -> Result<()>;
}

#[async_trait]
pub trait TableControlPlane: ControlPlane {
    /// Open a unit of work that can also stage table-format writes.
    async fn begin_table(&self) -> Result<Box<dyn TableTx + Send>>;
}
```

and on `ControlPlane` (with `use crate::auth::Auth;`):

```rust
    /// The authentication surface (sessions, service tokens, credentials).
    fn auth(&self) -> &(dyn Auth + Send + Sync);
```

`Tx::commit`'s doc comment is rewritten: "Returns the new `SnapshotId` if the unit of work staged table-format writes (see `TableTx`), else `None` — `Tx`-only planes always return `None`."

- [ ] **Step 1 (red-first probe): extend `control_plane_facade_contract`** — append an auth probe: resolve a session that cannot exist through the facade accessor and assert `Ok(None)`; mirror the contract's existing probe style, e.g.

```rust
    // auth(): reachable through the facade; unknown session resolves to None.
    let resolved = cp
        .auth()
        .resolve_session("facade-probe-nonexistent-hash")
        .await
        .expect("auth resolve through facade");
    assert!(resolved.is_none(), "unknown session must resolve to None");
```

(Check `core/src/auth.rs:98`'s actual method names — use whichever read returns `Option` for an unknown credential/session; mirror `auth_contract`'s calls at testkit:1999. This does not compile until `auth()` exists — that IS the red step for the accessor half; the trait split's "red" is the compiler.)

- [ ] **Step 2: core** — move the four methods `Tx` → `TableTx` verbatim; add `TableControlPlane`; add `auth()` to `ControlPlane`; fix `commit`'s doc; export `TableTx`, `TableControlPlane` from `core/src/lib.rs` next to `Tx`/`ControlPlane`.
- [ ] **Step 3: adapters** —
  - `PgTx`: delete the four stub methods and `no_table_format()`; prune now-unused imports (`ColumnSpec`, `DataFile`, `TableRef` if unused).
  - `PgControlPlane`: add `fn auth(&self) -> &(dyn Auth + Send + Sync) { self }` (it already `impl Auth`).
  - `IcebergTx`: split the single `impl Tx` into `impl Tx` (commit/rollback/enqueue/emit) + `impl TableTx` (the four staging methods, bodies unchanged).
  - `IcebergControlPlane`: `impl TableControlPlane` gets today's `begin()` body as `begin_table()`; `begin()` becomes `Ok(self.begin_table().await?)` (dyn upcast); `auth()` → `self.pg.auth()`.
  - `MemoryTx`/`MemoryControlPlane`: same split; `auth()` → `self` (memory `impl Auth` exists per spec).
  - `WireControlPlane`: `auth()` → `self.direct.auth()`; `begin()` untouched.
- [ ] **Step 4: consumers** — transform's four signatures + `main.rs:39`'s `Arc<dyn ControlPlane>` → `Arc<dyn TableControlPlane>`, `cp.begin()` → `cp.begin_table()` (run.rs:192 and any sibling); testkit: the five staging contracts' bounds → `control_plane_core::TableControlPlane` + `begin_table()` (leave `queue_contract`/`lineage_contract`/`tx_isolation_contract` on `ControlPlane` — they run against `PgControlPlane`); `run_unknown_input.rs`'s `StubCp` gains `impl TableControlPlane` (begin_table returning its stub tx) + its stub tx gains `impl TableTx`, and `StubCp` gains an `auth()` (test code may `unimplemented!()` with a reason string).
- [ ] **Step 5: compile + targeted suites**

```bash
buck2 build -M none //src/... > /tmp/x1.log 2>&1; grep -cE 'BUILD FAILED' /tmp/x1.log  # 0
buck2 test //src/control-plane/... //src/services/transform: --unstable-allow-all-tests-on-re > /tmp/x2.log 2>&1; grep -E "Tests finished|FAIL" /tmp/x2.log
```

Expected: build succeeds; contracts (incl. the new auth probe on all planes' facade-contract runs) and transform suites PASS.

- [ ] **Step 6: prek + commit**

```bash
buck2 run -v0 //tools:prek -- run --all-files > /tmp/p.log 2>&1; grep -c Failed /tmp/p.log  # 0
git add -A src docs/superpowers/plans/2026-07-03-tx-trait-segregation.md
git commit -m "refactor(core): split Tx/TableTx + TableControlPlane; auth() on the facade"
```

### Task 2: Whole-suite sweep (spec acceptance)

- [ ] **Step 1:** `buck2 test //src/... --unstable-allow-all-tests-on-re > /tmp/x3.log 2>&1; grep -E "Tests finished|FAIL" /tmp/x3.log` — expected PASS (this mirrors CI's build-test job; builds and test runs stay on RE, so the ~38 GiB local disk cap is not threatened). If disk pressure appears anyway: `buck2 clean` and rerun.
- [ ] **Step 2:** If red: fix forward (a missed consumer or stub), re-run, amend into Task 1's commit.

### Task 3: Register close + capability docs

**Files:**
- Modify: `docs/ROADMAP.md` (remove the `road-tx-trait-segregation` entry), `docs/system-capabilities/control-plane.md`, `docs/FUTURE.md` (only if a `[[road-tx-trait-segregation]]` link exists — check)

- [ ] **Step 1:** Remove the ROADMAP entry (whole block); `grep -rn 'road-tx-trait-segregation\|fut-tx-trait-segregation' docs/ .claude/ src/` — rewrite any surviving `[[...]]` link as a `` `#id` `` span (the promotion prose in FUTURE's `fut-wider-tx-composition` and the capability docs' Known-gaps `#road-tx-trait-segregation` line in `build-and-test.md` must be handled: delete the gap bullet, and reword any "Do this before" sequencing prose to past tense). `bash tools/docs.sh validate` → OK.
- [ ] **Step 2:** `docs/system-capabilities/control-plane.md` — in the Tx/transactions theme: the split (`Tx` vs `TableTx`/`begin_table()`, PgTx honest at compile time) and the `auth()` facade accessor, `(#PRNUM)`.
- [ ] **Step 3:** prek + commit `docs: close road-tx-trait-segregation`.
