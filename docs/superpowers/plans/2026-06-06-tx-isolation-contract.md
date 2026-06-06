# Tx Isolation Contract + Memory Atomic Commit (Step 2a #2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the missing Tx isolation contract (uncommitted writes invisible until commit; rollback invisible), run against both adapters, and fix `MemoryTx::commit` to apply its staged buffers under one critical section so commit is atomic w.r.t. any single-lock reader.

**Architecture:** A new `tx_isolation_contract` in `control-plane-testkit`, exercised from new `tx` test targets in `memory` and `postgres`. A surgical change to `MemoryTx::commit` in `control-plane-memory`. No `core`/trait change.

**Tech Stack:** Rust (edition 2024), `async-trait`, `tokio`, `control-plane-core`, the two adapters, buck2.

**Spec:** `docs/superpowers/specs/2026-06-06-tx-isolation-contract-design.md` — implements it exactly.

---

## File Structure

- **Modify** `src/control-plane/testkit/src/lib.rs` — add `tx_isolation_contract`.
- **Modify** `src/control-plane/memory/src/lib.rs` — make `MemoryTx::commit` hold both locks across the apply.
- **Create** `src/control-plane/memory/tests/tx.rs`; **modify** `src/control-plane/memory/BUCK` (add `tx` target).
- **Create** `src/control-plane/postgres/tests/tx.rs`; **modify** `src/control-plane/postgres/BUCK` (add `tx` target).

No new dependencies. The branch is created by the executor before Task 1 — do **not** implement on `main`.

**Formatting (avoids a commit loop):** the prek `rustfmt` hook is check-only. Before each commit, run `eval "$(./tools/env.sh)" >/dev/null 2>&1 && rustfmt --edition 2024 <changed .rs files>`, then `git add` + commit. pg tests run `--local-only`.

These tasks are independent and each leaves the tree building, so commits run hooks normally (no `--no-verify` expected).

---

### Task 1: `tx_isolation_contract` in testkit

**Files:**
- Modify: `src/control-plane/testkit/src/lib.rs`

The testkit already imports what's needed (`Acl`-era import block includes `ControlPlane, Queue, Lineage, NewJob, RunId, EventType, LineageEvent, DatasetRef, OffsetDateTime`, and `Job` via `Queue`). Verify the `use control_plane_core::{…}` block already brings in `ControlPlane, Queue, Lineage, NewJob, LineageEvent, RunId, EventType` (it does — `lineage_contract` uses all of them). No import changes should be needed; if the compiler flags a missing name, add it to that block.

- [ ] **Step 1: Append `tx_isolation_contract` to the end of `testkit/src/lib.rs`:**

```rust
/// Contract for `Tx` isolation: while a transaction is open and uncommitted, the
/// autocommit read path observes none of its writes; on commit the whole unit
/// (enqueue + emit) becomes visible; on rollback nothing ever does. Deterministic —
/// the read happens on the same thread while the `Tx` handle is still alive (for pg
/// the read uses a distinct pool connection, so READ COMMITTED hides the open tx).
pub async fn tx_isolation_contract<CP: ControlPlane + Queue + Lineage>(cp: &CP) {
    let ts = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let event = |run: RunId| LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: ts,
        inputs: vec![],
        outputs: vec![DatasetRef {
            namespace: "ducklake".into(),
            name: "main.out".into(),
        }],
        payload: serde_json::json!({}),
    };

    // --- commit path: invisible while open, both visible after commit ---
    let run = RunId(uuid::Uuid::new_v4());
    let kinds = vec!["tx-iso".to_string()];
    let mut tx = cp.begin().await.expect("begin");
    tx.enqueue(NewJob {
        kind: "tx-iso".into(),
        payload: serde_json::json!({}),
        run_at: None,
        priority: 0,
    })
    .await
    .expect("staged enqueue");
    tx.emit(event(run)).await.expect("staged emit");

    // Open + uncommitted: the autocommit read path sees neither write.
    assert!(
        cp.dequeue(&kinds, "reader").await.unwrap().is_none(),
        "uncommitted enqueue is invisible while the tx is open"
    );
    assert!(
        cp.events_for(&run).await.unwrap().is_empty(),
        "uncommitted emit is invisible while the tx is open"
    );

    tx.commit().await.expect("commit");

    // After commit: the whole unit is visible.
    let job = cp
        .dequeue(&kinds, "reader")
        .await
        .unwrap()
        .expect("committed enqueue is visible");
    cp.complete(job.id).await.unwrap();
    assert_eq!(
        cp.events_for(&run).await.unwrap().len(),
        1,
        "committed emit is visible"
    );

    // --- rollback path: nothing ever becomes visible ---
    let run_rb = RunId(uuid::Uuid::new_v4());
    let kinds_rb = vec!["tx-iso-rb".to_string()];
    let mut tx = cp.begin().await.expect("begin");
    tx.enqueue(NewJob {
        kind: "tx-iso-rb".into(),
        payload: serde_json::json!({}),
        run_at: None,
        priority: 0,
    })
    .await
    .unwrap();
    tx.emit(event(run_rb)).await.unwrap();
    tx.rollback().await.expect("rollback");

    assert!(
        cp.dequeue(&kinds_rb, "reader").await.unwrap().is_none(),
        "rolled-back enqueue never becomes visible"
    );
    assert!(
        cp.events_for(&run_rb).await.unwrap().is_empty(),
        "rolled-back emit never becomes visible"
    );
}
```

- [ ] **Step 2: Format + build testkit.**
```bash
eval "$(./tools/env.sh)" >/dev/null 2>&1 && rustfmt --edition 2024 src/control-plane/testkit/src/lib.rs
env -u BUCK_PREFER_REMOTE buck2 build //src/control-plane/testkit:testkit
```
Expected: builds clean.

- [ ] **Step 3: Commit.**
```bash
git add src/control-plane/testkit/src/lib.rs
git commit -m "feat(control-plane): add tx_isolation_contract to testkit"
```

---

### Task 2: Atomic `MemoryTx::commit` + memory tx test

**Files:**
- Modify: `src/control-plane/memory/src/lib.rs`
- Create: `src/control-plane/memory/tests/tx.rs`
- Modify: `src/control-plane/memory/BUCK`

- [ ] **Step 1: Make `MemoryTx::commit` hold both locks across the apply.**

In `src/control-plane/memory/src/lib.rs`, replace the current `commit`:
```rust
    async fn commit(self: Box<Self>) -> Result<()> {
        let staged_any = !self.staged.is_empty();
        {
            let mut rows = self.rows.lock().unwrap();
            for (id, job) in self.staged {
                MemoryControlPlane::insert_with_id(&mut rows, id, job);
            }
        }
        if !self.staged_events.is_empty() {
            self.lineage.lock().unwrap().events.extend(self.staged_events);
        }
        if staged_any {
            self.notify.notify_waiters();
        }
        Ok(())
    }
```
with:
```rust
    async fn commit(self: Box<Self>) -> Result<()> {
        let staged_any = !self.staged.is_empty();
        {
            // Hold BOTH locks across the whole apply so commit is atomic w.r.t. any
            // single-lock reader (dequeue locks `rows`; events_for locks `lineage`):
            // no partial commit is observable. Lock order rows-then-lineage must be
            // consistent everywhere to stay deadlock-free (readers take only one
            // lock; no reader takes both).
            let mut rows = self.rows.lock().unwrap();
            let mut lin = self.lineage.lock().unwrap();
            for (id, job) in self.staged {
                MemoryControlPlane::insert_with_id(&mut rows, id, job);
            }
            lin.events.extend(self.staged_events);
        }
        if staged_any {
            self.notify.notify_waiters();
        }
        Ok(())
    }
```
(The `rollback` no-op and `enqueue`/`emit` staging methods are unchanged.)

- [ ] **Step 2: Create `src/control-plane/memory/tests/tx.rs`:**
```rust
#[tokio::test]
async fn memory_passes_tx_isolation_contract() {
    let cp = control_plane_memory::MemoryControlPlane::new(std::time::Duration::from_millis(300));
    control_plane_testkit::tx_isolation_contract(&cp).await;
}
```

- [ ] **Step 3: Add the `tx` test target to `src/control-plane/memory/BUCK`** (after the `lineage` target):
```python
rust_test(
    name = "tx",
    crate = "tx",
    srcs = ["tests/tx.rs"],
    crate_root = "tests/tx.rs",
    edition = "2024",
    deps = [
        ":memory",
        "//src/control-plane/testkit:testkit",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 4: Format, run, clippy.**
```bash
eval "$(./tools/env.sh)" >/dev/null 2>&1 && rustfmt --edition 2024 src/control-plane/memory/src/lib.rs src/control-plane/memory/tests/tx.rs
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/memory:tx
env -u BUCK_PREFER_REMOTE buck2 build '//src/control-plane/memory:memory[clippy.txt]'
```
Expected: `tx` test passes (1); clippy `[clippy.txt]` empty.

- [ ] **Step 5: Commit.**
```bash
git add src/control-plane/memory
git commit -m "fix(control-plane): atomic MemoryTx::commit (single critical section); tx isolation test"
```

---

### Task 3: Postgres tx test

**Files:**
- Create: `src/control-plane/postgres/tests/tx.rs`
- Modify: `src/control-plane/postgres/BUCK`

- [ ] **Step 1: Create `src/control-plane/postgres/tests/tx.rs`:**
```rust
use control_plane_postgres::fixture::PgFixture;

#[tokio::test]
async fn postgres_passes_tx_isolation_contract() {
    let fixture = PgFixture::start();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::tx_isolation_contract(&cp).await;
}
```

- [ ] **Step 2: Add the `tx` test target to `src/control-plane/postgres/BUCK`** (after the `lineage` target):
```python
rust_test(
    name = "tx",
    crate = "tx",
    srcs = ["tests/tx.rs"],
    crate_root = "tests/tx.rs",
    edition = "2024",
    env = {
        "POSTGRES_BIN_DIR": "$(location :postgres-bin)/bin",
        "POSTGRES_LD_LIBRARY_PATH": "$(location :postgres-bin)/lib:$(location :libxml2)",
        "LOOM_MIGRATIONS_DIR": "$(location :migrations)/migrations",
    },
    deps = [":postgres", "//src/control-plane/testkit:testkit", "//third-party:tokio"],
)
```

- [ ] **Step 3: Run.**
```bash
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/postgres:tx
```
Expected: passes (1 test). This confirms pg READ COMMITTED hides the open tx from the pool reader (and the `max_connections(5)` pool doesn't deadlock holding the tx connection while reading).

- [ ] **Step 4: Commit.**
```bash
git add src/control-plane/postgres
git commit -m "test(control-plane): Postgres tx isolation contract"
```

---

## Final Verification

- [ ] **Branch + commits** (per the `verify-branch-after-subagents` lesson):
```bash
git branch --show-current   # feature branch, NOT main
git log --oneline -5
```

- [ ] **Full suite + tally:**
```bash
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...
```
Expected: all pass, with **two more** passing targets than before (`//src/control-plane/memory:tx` and `//src/control-plane/postgres:tx`) → **Pass 20**.

- [ ] **Lint:**
```bash
buck2 run //tools:prek -- run --all-files
```
Expected: rustfmt, clippy, file checks, reindeer-in-sync all pass.

- [ ] Hand off to **superpowers:finishing-a-development-branch**.

---

## Self-Review Notes (for the implementer)

- **Spec coverage:** open-tx invisibility (dequeue None + events_for empty) for both enqueue and emit; commit → both visible; rollback → neither visible; run against both adapters; memory commit holds both locks in one critical section with the invariant comment. All covered.
- **Type consistency:** `tx_isolation_contract<CP: ControlPlane + Queue + Lineage>`; uses `cp.begin()`, `tx.enqueue`/`tx.emit`/`commit`/`rollback`, and autocommit `cp.dequeue`/`cp.events_for`/`cp.complete`. `RunId(uuid::Uuid::new_v4())` (testkit already deps `uuid` from 2a-era lineage work). Whole-second `event_time` so any pg round-trip is exact (not asserted here, but consistent).
- **Determinism:** no spawned tasks; the open `Tx` is read around on one thread (stage → read → commit → read). pg pool is `max_connections(5)`, so the tx connection + the pool read coexist without deadlock.
- **Independence:** the three tasks each leave the tree building; commits run hooks normally.
