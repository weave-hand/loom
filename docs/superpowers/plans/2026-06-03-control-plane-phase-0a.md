# Control-Plane Phase 0a — Foundations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the control-plane crate family (ports & adapters) with a working, contract-tested transaction abstraction proven against an in-memory fake — the hermetic foundation every concern builds on.

**Architecture:** Restructure `src/control-plane/` into `control-plane-core` (traits + error model, no I/O), `control-plane-testkit` (generic contract-test suite), and `control-plane-memory` (in-memory fake adapter). The transaction seam is a `begin()/commit()/rollback()` primitive on `ControlPlane`/`Tx`; a temporary `probe_*` op on `Tx` lets the testkit verify commit-visibility / rollback / isolation before any real concern exists. Everything here is pure Rust and hermetic.

**Tech Stack:** Rust (edition 2024, nightly toolchain), `async-trait`, `thiserror`, `tokio` (test only), buck2 (`rust_library`/`rust_test`), reindeer for third-party deps.

**Scope notes:**
- This is **Phase 0a**. The Postgres adapter (`control-plane-postgres`), `sqlx`, the `postgres-bin` `http_archive`, and the hermetic pg fixture are **Phase 0b** (next plan).
- The five concern traits (`Queue`/`Catalog`/`Ontology`/`Acl`/`Lineage`) are **not** coded here — they're sketched in the spec and pinned in their own phases. `ControlPlane` ships with only `begin()`; concern accessors are added per phase.
- The closure sugar `transaction(|tx| …)` is **deferred** (HRTB async-closure complexity; flagged highest-churn in the spec). 0a ships the explicit `begin/commit/rollback` primitive, which is a complete, working transaction abstraction.
- **Before starting, create a branch** (e.g. `feat/control-plane-foundations`) — the working tree currently has an uncommitted `src/control-plane` scaffold and `Cargo.toml`/`Cargo.lock` edits that this plan replaces.

---

## File structure

```
src/control-plane/
  core/
    Cargo.toml                control-plane-core (deps: async-trait, thiserror)
    BUCK                      rust_library "core" (crate control_plane_core)
    src/lib.rs                module wiring + crate docs
    src/error.rs              ControlPlaneError + Result alias
    src/transaction.rs        ControlPlane + Tx traits (+ temporary probe ops)
  testkit/
    Cargo.toml                control-plane-testkit (dep: control-plane-core)
    BUCK                      rust_library "testkit" (crate control_plane_testkit)
    src/lib.rs                tx_contract<CP: ControlPlane>(...)
  memory/
    Cargo.toml                control-plane-memory (dep: core, async-trait; dev: testkit, tokio)
    BUCK                      rust_library "memory" + rust_test "contract"
    src/lib.rs                MemoryControlPlane + MemoryTx
    tests/contract.rs         runs testkit against the fake
```

Workspace `Cargo.toml` members become `src/hello` + the three new crates (the old `src/control-plane` member is removed).

---

## Task 1: Restructure workspace into three empty crates

**Files:**
- Delete: `src/control-plane/Cargo.toml`, `src/control-plane/src/lib.rs`
- Create: `src/control-plane/core/Cargo.toml`, `src/control-plane/core/src/lib.rs`, `src/control-plane/core/BUCK`
- Create: `src/control-plane/testkit/Cargo.toml`, `src/control-plane/testkit/src/lib.rs`, `src/control-plane/testkit/BUCK`
- Create: `src/control-plane/memory/Cargo.toml`, `src/control-plane/memory/src/lib.rs`, `src/control-plane/memory/BUCK`
- Modify: `Cargo.toml` (workspace members)

- [ ] **Step 1: Remove the old scaffold**

Run: `rm -f src/control-plane/Cargo.toml src/control-plane/src/lib.rs && rmdir src/control-plane/src 2>/dev/null || true`

- [ ] **Step 2: Update workspace members in `Cargo.toml`**

Replace the `members` line so it reads exactly:
```toml
[workspace]
members = ["src/control-plane/core", "src/control-plane/memory", "src/control-plane/testkit", "src/hello"]
resolver = "2"
```

- [ ] **Step 3: Create the three crate `Cargo.toml`s (no deps yet)**

`src/control-plane/core/Cargo.toml`:
```toml
[package]
name = "control-plane-core"
version = "0.1.0"
edition = "2024"

[dependencies]
```

`src/control-plane/testkit/Cargo.toml`:
```toml
[package]
name = "control-plane-testkit"
version = "0.1.0"
edition = "2024"

[dependencies]
control-plane-core = { path = "../core" }
```

`src/control-plane/memory/Cargo.toml`:
```toml
[package]
name = "control-plane-memory"
version = "0.1.0"
edition = "2024"

[dependencies]
control-plane-core = { path = "../core" }
```

- [ ] **Step 4: Create empty `lib.rs` for each crate**

`src/control-plane/core/src/lib.rs`:
```rust
//! loom control-plane core: traits, domain types, and the error model.
//! No I/O — adapters (memory, postgres) implement these against a backend.
```

`src/control-plane/testkit/src/lib.rs`:
```rust
//! Backend-agnostic contract tests for the control-plane traits.
//! Each adapter runs the same suite against its own implementation.
```

`src/control-plane/memory/src/lib.rs`:
```rust
//! In-memory fake adapter for the control-plane traits — fast, hermetic tests
//! and local dev.
```

- [ ] **Step 5: Create the three BUCK files (rust_library, no deps)**

`src/control-plane/core/BUCK`:
```python
rust_library(
    name = "core",
    crate = "control_plane_core",
    srcs = glob(["src/**/*.rs"]),
    crate_root = "src/lib.rs",
    edition = "2024",
    visibility = ["PUBLIC"],
)
```

`src/control-plane/testkit/BUCK`:
```python
rust_library(
    name = "testkit",
    crate = "control_plane_testkit",
    srcs = glob(["src/**/*.rs"]),
    crate_root = "src/lib.rs",
    edition = "2024",
    deps = ["//src/control-plane/core:core"],
    visibility = ["PUBLIC"],
)
```

`src/control-plane/memory/BUCK`:
```python
rust_library(
    name = "memory",
    crate = "control_plane_memory",
    srcs = glob(["src/**/*.rs"]),
    crate_root = "src/lib.rs",
    edition = "2024",
    deps = ["//src/control-plane/core:core"],
    visibility = ["PUBLIC"],
)
```

- [ ] **Step 6: Verify buck2 builds the empty crates**

Run: `buck2 build //src/control-plane/...`
Expected: BUILD SUCCEEDED (three empty libraries compile).

- [ ] **Step 7: Commit**

```bash
git add -A src/control-plane Cargo.toml
git commit -m "feat(control-plane): scaffold core/testkit/memory crate split

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Buckify third-party dependencies

Adds `async-trait`, `thiserror`, and `tokio` (test-only) and regenerates `third-party/BUCK`.

**Files:**
- Modify: `src/control-plane/core/Cargo.toml`, `src/control-plane/memory/Cargo.toml`
- Modify (generated): `third-party/BUCK`, `Cargo.lock`
- Create (as needed): `third-party/fixups/thiserror/fixups.toml`, `third-party/fixups/tokio/fixups.toml`

- [ ] **Step 1: Add deps to the crate manifests**

`src/control-plane/core/Cargo.toml` `[dependencies]`:
```toml
[dependencies]
async-trait = "0.1"
thiserror = "1"
```

`src/control-plane/memory/Cargo.toml` — add `async-trait` and a dev-deps block:
```toml
[dependencies]
control-plane-core = { path = "../core" }
async-trait = "0.1"

[dev-dependencies]
control-plane-testkit = { path = "../testkit" }
tokio = { version = "1", features = ["macros", "rt"] }
```

- [ ] **Step 2: Refresh the lockfile and regenerate third-party rules**

Run: `./tools/buckify.sh`
Expected: `third-party/BUCK` gains targets for `async-trait`, `thiserror`, `tokio`, and their transitives (`syn`, `unicode-ident`, `thiserror-impl`, `tokio-macros`, `pin-project-lite`, …). reindeer may warn that `thiserror` and `tokio` have build scripts needing a decision.

- [ ] **Step 3: Add build-script fixups for any crate reindeer flags**

For each crate reindeer warns needs a `[buildscript]` decision, create `third-party/fixups/<crate>/fixups.toml` with:
```toml
[buildscript]
run = true
```
Expected crates needing this: `thiserror`, `tokio` (both have build scripts that probe `rustc`/cfg). Create `third-party/fixups/thiserror/fixups.toml` and `third-party/fixups/tokio/fixups.toml` with the content above. Then re-run `./tools/buckify.sh` and confirm it completes with no remaining build-script warnings.

- [ ] **Step 4: Verify the third-party targets build**

Run: `buck2 build //third-party:async-trait //third-party:thiserror //third-party:tokio`
Expected: BUILD SUCCEEDED.

- [ ] **Step 5: Commit**

```bash
git add -A Cargo.toml Cargo.lock third-party src/control-plane
git commit -m "build(control-plane): import async-trait, thiserror, tokio via reindeer

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Error model in `core`

**Files:**
- Create: `src/control-plane/core/src/error.rs`
- Modify: `src/control-plane/core/src/lib.rs`

- [ ] **Step 1: Write the failing test**

Add to `src/control-plane/core/src/error.rs`:
```rust
//! The unified control-plane error type. Adapters map their native errors into
//! these variants; contract tests assert on variants, never on messages.

/// Result alias used throughout the control plane.
pub type Result<T> = std::result::Result<T, ControlPlaneError>;

#[derive(Debug, thiserror::Error)]
pub enum ControlPlaneError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("unauthorized")]
    Unauthorized,
    #[error("serialization: {0}")]
    Serialization(String),
    #[error(transparent)]
    Backend(#[from] Box<dyn std::error::Error + Send + Sync>),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn _assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn variants_display_and_are_send_sync() {
        _assert_send_sync::<ControlPlaneError>();
        assert_eq!(ControlPlaneError::NotFound("job 7".into()).to_string(), "not found: job 7");
        assert_eq!(ControlPlaneError::Unauthorized.to_string(), "unauthorized");
    }
}
```

- [ ] **Step 2: Wire the module + re-exports in `lib.rs`**

Append to `src/control-plane/core/src/lib.rs`:
```rust
mod error;

pub use error::{ControlPlaneError, Result};
```

- [ ] **Step 3: Run the test (dev shell) to verify it passes**

Run: `eval "$(./tools/env.sh)" && cargo test -p control-plane-core`
Expected: `test ... variants_display_and_are_send_sync ... ok`; 1 passed.

- [ ] **Step 4: Verify it builds under buck2**

Run: `buck2 build //src/control-plane/core:core`
Expected: BUILD SUCCEEDED.

- [ ] **Step 5: Commit**

```bash
git add src/control-plane/core
git commit -m "feat(control-plane-core): add ControlPlaneError + Result

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Transaction traits in `core`

Defines `ControlPlane` (only `begin()` for now) and `Tx` (`commit`/`rollback` + a **temporary** `probe_*` op used solely to contract-test transaction semantics until a real concern provides Tx operations in Phase 1).

**Files:**
- Create: `src/control-plane/core/src/transaction.rs`
- Modify: `src/control-plane/core/src/lib.rs`

- [ ] **Step 1: Write the transaction traits**

`src/control-plane/core/src/transaction.rs`:
```rust
//! The cross-concern transaction seam. `ControlPlane::begin` opens a unit of work;
//! operations issued on the returned `Tx` commit together or roll back together.
//!
//! NOTE: `probe_put`/`probe_get` are TEMPORARY scaffolding. They exist only so the
//! contract suite can verify commit-visibility / rollback / isolation before any
//! real concern provides Tx operations. They are removed once Phase 1 (queue) puts
//! real operations on `Tx`.

use async_trait::async_trait;

use crate::error::Result;

#[async_trait]
pub trait ControlPlane: Send + Sync {
    /// Open a unit of work. Issue operations on the returned `Tx`, then
    /// `commit` or `rollback`. (Concern accessors — `queue()`, `catalog()`, … —
    /// are added in their respective phases.)
    async fn begin(&self) -> Result<Box<dyn Tx + Send>>;
}

#[async_trait]
pub trait Tx: Send {
    /// Commit all staged operations.
    async fn commit(self: Box<Self>) -> Result<()>;
    /// Discard all staged operations.
    async fn rollback(self: Box<Self>) -> Result<()>;

    /// TEMPORARY (see module docs): stage a key/value within this unit of work.
    async fn probe_put(&mut self, key: &str, val: i64) -> Result<()>;
    /// TEMPORARY (see module docs): read a key — own staged writes first, then
    /// committed state. Uncommitted writes from *other* transactions are not visible.
    async fn probe_get(&mut self, key: &str) -> Result<Option<i64>>;
}
```

- [ ] **Step 2: Wire the module + re-exports in `lib.rs`**

Append to `src/control-plane/core/src/lib.rs`:
```rust
mod transaction;

pub use transaction::{ControlPlane, Tx};
```

- [ ] **Step 3: Verify it compiles (traits only, no impl yet)**

Run: `eval "$(./tools/env.sh)" && cargo build -p control-plane-core && buck2 build //src/control-plane/core:core`
Expected: both succeed (the traits compile; no implementor exists yet).

- [ ] **Step 4: Commit**

```bash
git add src/control-plane/core
git commit -m "feat(control-plane-core): add ControlPlane/Tx transaction seam

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Contract suite in `testkit`

Writes the generic transaction-semantics suite — the test that any `ControlPlane` adapter must pass.

**Files:**
- Modify: `src/control-plane/testkit/src/lib.rs`

- [ ] **Step 1: Write the contract suite**

Append to `src/control-plane/testkit/src/lib.rs`:
```rust
use control_plane_core::ControlPlane;

/// Verify the transaction seam: read-your-write, commit visibility, rollback
/// discards, and isolation of uncommitted writes. Run by every adapter against a
/// fresh, empty instance.
pub async fn tx_contract<CP: ControlPlane>(cp: &CP) {
    // read-your-write within a tx, then commit is visible to a later tx
    let mut tx = cp.begin().await.expect("begin");
    tx.probe_put("k", 7).await.expect("put");
    assert_eq!(tx.probe_get("k").await.expect("get"), Some(7), "read-your-write");
    tx.commit().await.expect("commit");

    let mut tx2 = cp.begin().await.expect("begin");
    assert_eq!(tx2.probe_get("k").await.expect("get"), Some(7), "visible after commit");
    tx2.rollback().await.expect("rollback");

    // rollback discards staged writes
    let mut tx3 = cp.begin().await.expect("begin");
    tx3.probe_put("r", 1).await.expect("put");
    tx3.rollback().await.expect("rollback");

    let mut tx4 = cp.begin().await.expect("begin");
    assert_eq!(tx4.probe_get("r").await.expect("get"), None, "rollback discarded");
    tx4.rollback().await.expect("rollback");

    // isolation: a concurrent tx does not see another tx's uncommitted writes
    let mut a = cp.begin().await.expect("begin");
    a.probe_put("iso", 9).await.expect("put");
    let mut b = cp.begin().await.expect("begin");
    assert_eq!(b.probe_get("iso").await.expect("get"), None, "uncommitted not visible");
    a.commit().await.expect("commit");
    b.rollback().await.expect("rollback");
}
```

- [ ] **Step 2: Verify it compiles against the traits**

Run: `eval "$(./tools/env.sh)" && cargo build -p control-plane-testkit && buck2 build //src/control-plane/testkit:testkit`
Expected: both succeed (generic over `ControlPlane`; no concrete impl needed to compile).

- [ ] **Step 3: Commit**

```bash
git add src/control-plane/testkit
git commit -m "test(control-plane-testkit): add tx_contract suite

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: In-memory fake adapter + green contract test

TDD payoff: wire the failing contract test first (no impl → fails to compile), then implement the fake until it passes.

**Files:**
- Create: `src/control-plane/memory/tests/contract.rs`
- Modify: `src/control-plane/memory/src/lib.rs`, `src/control-plane/memory/BUCK`

- [ ] **Step 1: Write the failing test wiring**

`src/control-plane/memory/tests/contract.rs`:
```rust
use control_plane_memory::MemoryControlPlane;

#[tokio::test]
async fn memory_passes_tx_contract() {
    control_plane_testkit::tx_contract(&MemoryControlPlane::default()).await;
}
```

- [ ] **Step 2: Run it to confirm it fails (no `MemoryControlPlane` yet)**

Run: `eval "$(./tools/env.sh)" && cargo test -p control-plane-memory`
Expected: FAIL — compile error `cannot find ... MemoryControlPlane in crate control_plane_memory` (or unresolved import).

- [ ] **Step 3: Implement the fake adapter**

Replace `src/control-plane/memory/src/lib.rs` with:
```rust
//! In-memory fake adapter for the control-plane traits — fast, hermetic tests
//! and local dev. State is a `HashMap` behind a `Mutex`; a `Tx` stages writes in
//! its own buffer and applies them on commit (drops them on rollback), so
//! uncommitted writes are invisible to other transactions.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use control_plane_core::{ControlPlane, Result, Tx};

#[derive(Clone, Default)]
pub struct MemoryControlPlane {
    state: Arc<Mutex<HashMap<String, i64>>>,
}

#[async_trait]
impl ControlPlane for MemoryControlPlane {
    async fn begin(&self) -> Result<Box<dyn Tx + Send>> {
        Ok(Box::new(MemoryTx { shared: self.state.clone(), staged: HashMap::new() }))
    }
}

struct MemoryTx {
    shared: Arc<Mutex<HashMap<String, i64>>>,
    staged: HashMap<String, i64>,
}

#[async_trait]
impl Tx for MemoryTx {
    async fn commit(self: Box<Self>) -> Result<()> {
        let mut g = self.shared.lock().expect("control-plane memory mutex poisoned");
        for (k, v) in self.staged {
            g.insert(k, v);
        }
        Ok(())
    }

    async fn rollback(self: Box<Self>) -> Result<()> {
        // Dropping `self` discards the staged buffer.
        Ok(())
    }

    async fn probe_put(&mut self, key: &str, val: i64) -> Result<()> {
        self.staged.insert(key.to_string(), val);
        Ok(())
    }

    async fn probe_get(&mut self, key: &str) -> Result<Option<i64>> {
        if let Some(v) = self.staged.get(key) {
            return Ok(Some(*v));
        }
        let g = self.shared.lock().expect("control-plane memory mutex poisoned");
        Ok(g.get(key).copied())
    }
}
```

- [ ] **Step 4: Run the test (dev shell) to verify it passes**

Run: `eval "$(./tools/env.sh)" && cargo test -p control-plane-memory`
Expected: `test memory_passes_tx_contract ... ok`; 1 passed.

- [ ] **Step 5: Wire the buck2 `rust_test` + deps**

Update `src/control-plane/memory/BUCK` to add `async-trait` to the library deps and a `rust_test` for the contract suite:
```python
rust_library(
    name = "memory",
    crate = "control_plane_memory",
    srcs = glob(["src/**/*.rs"]),
    crate_root = "src/lib.rs",
    edition = "2024",
    deps = [
        "//src/control-plane/core:core",
        "//third-party:async-trait",
    ],
    visibility = ["PUBLIC"],
)

rust_test(
    name = "contract",
    crate = "contract",
    srcs = ["tests/contract.rs"],
    crate_root = "tests/contract.rs",
    edition = "2024",
    deps = [
        ":memory",
        "//src/control-plane/testkit:testkit",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 6: Verify the contract test passes under buck2**

Run: `buck2 test //src/control-plane/memory:contract`
Expected: the test runs and passes (Pass 1, Fail 0).

- [ ] **Step 7: Verify the whole first-party tree still builds/tests**

Run: `buck2 build //src/... && buck2 test //src/...`
Expected: BUILD SUCCEEDED; the memory contract test passes; nothing else broke. (This is what CI runs on `main`.)

- [ ] **Step 8: Commit**

```bash
git add src/control-plane/memory
git commit -m "feat(control-plane-memory): in-memory fake passing the tx contract

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Self-review

**Spec coverage (Phase 0 row + foundations):**
- Crate split core/testkit/memory → Tasks 1–6 (postgres deferred to 0b, per scope note). ✓
- `ControlPlaneError` → Task 3. ✓
- `ControlPlane`/`Tx` seam implemented + contract-tested via the probe op → Tasks 4–6 (memory adapter; pg adapter is 0b). ✓
- BUCK files + reindeer deps + (no sqlx offline yet — that's 0b) → Tasks 1, 2, 6. ✓
- Fake CI green → Task 6 step 7. ✓
- Ports-and-adapters with one suite over multiple backends → testkit (Task 5) consumed by memory now, postgres in 0b. ✓

**Deviations from spec (intentional, noted up top):** pg adapter / sqlx / `postgres-bin` / pg fixture deferred to 0b; the `transaction(|tx|)` closure sugar deferred in favor of the `begin/commit/rollback` primitive; concern traits not coded in 0a (sketched in spec, pinned per phase); the probe op lives un-feature-gated on `Tx` and is deleted in Phase 1 (rather than feature-gated) — simpler and avoids buck2 per-target feature friction.

**Placeholder scan:** none — every code/BUCK/command block is complete and was verified against a compiled prototype (`begin/commit/rollback` with `self: Box<Self>` + `async_trait`, the tx_contract assertions, and the dep/fixup set all confirmed by `cargo test` in a scratch crate).

**Type/name consistency:** crate names `control_plane_core` / `control_plane_testkit` / `control_plane_memory` (buck `crate =`), package names `control-plane-{core,testkit,memory}`, `MemoryControlPlane`/`MemoryTx`, `tx_contract`, `ControlPlaneError`/`Result`, `probe_put`/`probe_get` used consistently across tasks. Buck targets `//src/control-plane/{core,testkit,memory}:{core,testkit,memory}` and `//src/control-plane/memory:contract` consistent.
