# Queue/Worker Robustness (Step 2b, group 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** (A) contain a panicking worker handler (Abandon the job, keep the loop alive) and (B) add a queue concurrency contract proving concurrent workers claim each job exactly once.

**Architecture:** Item B is a new `testkit` contract run from both adapters' existing `queue.rs` (no deps). Item A adds `futures-util` `catch_unwind` to the `worker` crate. Independent; each task leaves the tree building.

**Tech Stack:** Rust (edition 2024), `tokio`, `futures-util` (new on `worker`), `control-plane-core`, buck2, reindeer.

**Spec:** `docs/superpowers/specs/2026-06-07-queue-worker-robustness-design.md` — implements it exactly.

---

## File Structure
- **Modify** `src/control-plane/testkit/src/lib.rs` — add `queue_concurrency_contract`.
- **Modify** `src/control-plane/memory/tests/queue.rs` + `src/control-plane/postgres/tests/queue.rs` — call it (new test fns; no BUCK change).
- **Modify** `src/control-plane/worker/Cargo.toml` + `BUCK` — add `futures-util`; **modify** `third-party/BUCK` (buckify-generated alias).
- **Modify** `src/control-plane/worker/src/lib.rs` — `catch_unwind` the handler; `panic_message` helper.
- **Modify** `src/control-plane/worker/tests/worker.rs` — panic-containment test.

**Formatting:** prek `rustfmt` is check-only — `eval "$(./tools/env.sh)" >/dev/null 2>&1 && rustfmt --edition 2024 <files>` before committing. pg tests `--local-only`.

Branch already exists (`feat/queue-worker-robustness`, the spec commit is on it). Both tasks keep the tree green → commit normally.

---

### Task 1: Queue concurrency contract (Item B)

**Files:**
- Modify: `src/control-plane/testkit/src/lib.rs`
- Modify: `src/control-plane/memory/tests/queue.rs`, `src/control-plane/postgres/tests/queue.rs`

- [ ] **Step 1: Append `queue_concurrency_contract` to `testkit/src/lib.rs`:**
```rust
/// Contract for concurrent dequeue: N workers draining M jobs must claim each job
/// **exactly once** (no double-claim, none lost) — the `SKIP LOCKED` fairness guarantee.
/// `cp` is taken by value (`Clone + Send + Sync + 'static`) so clones move into spawned
/// tasks. A plain current-thread runtime suffices: the spawned tasks interleave at each
/// `dequeue().await`, so the pg adapter issues genuinely concurrent `SKIP LOCKED` queries.
pub async fn queue_concurrency_contract<CP>(cp: CP)
where
    CP: ControlPlane + Queue + Clone + Send + Sync + 'static,
{
    use std::collections::HashSet;

    let kind = "conc";
    let m: usize = 50; // jobs
    let n: usize = 8; // concurrent workers

    for _ in 0..m {
        cp.enqueue(NewJob {
            kind: kind.into(),
            payload: serde_json::json!({}),
            run_at: None,
            priority: 0,
        })
        .await
        .expect("enqueue");
    }

    let mut handles = Vec::new();
    for w in 0..n {
        let cp = cp.clone();
        let kinds = vec![kind.to_string()];
        handles.push(tokio::spawn(async move {
            let me = format!("w{w}");
            let mut claimed = Vec::new();
            while let Some(job) = cp.dequeue(&kinds, &me).await.expect("dequeue") {
                claimed.push(job.id);
                cp.complete(job.id).await.expect("complete");
            }
            claimed
        }));
    }

    let mut all = Vec::new();
    for h in handles {
        all.extend(h.await.expect("worker task"));
    }

    assert_eq!(all.len(), m, "every job claimed exactly once (none lost, none double-claimed)");
    assert_eq!(
        all.iter().collect::<HashSet<_>>().len(),
        m,
        "no job claimed by two workers"
    );
}
```
(All names — `ControlPlane`, `Queue`, `NewJob`, `serde_json`, `tokio` — are already in scope in `testkit/src/lib.rs`. `ControlPlane` is in the bound only for symmetry with the other queue contracts / fixture freshness; it's harmless if dropped, but keep it since both adapters satisfy it.)

- [ ] **Step 2: Build testkit.**
```bash
env -u BUCK_PREFER_REMOTE buck2 build //src/control-plane/testkit:testkit
```
Expected: clean.

- [ ] **Step 3: Wire into both adapters' `queue.rs`.** Add `queue_concurrency_contract` to the `use control_plane_testkit::{…}` import in each, then append a test fn.

`src/control-plane/memory/tests/queue.rs`:
```rust
#[tokio::test]
async fn memory_passes_queue_concurrency_contract() {
    let cp = MemoryControlPlane::new(std::time::Duration::from_millis(300));
    control_plane_testkit::queue_concurrency_contract(cp).await;
}
```
`src/control-plane/postgres/tests/queue.rs`:
```rust
#[tokio::test]
async fn postgres_passes_queue_concurrency_contract() {
    let fixture = PgFixture::start();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::queue_concurrency_contract(cp).await;
}
```
(If a file calls the contracts via a glob/explicit import, add the name; if it fully-qualifies as `control_plane_testkit::…`, no import edit is needed — match the file's existing style.)

- [ ] **Step 4: Format, run both.**
```bash
eval "$(./tools/env.sh)" >/dev/null 2>&1 && rustfmt --edition 2024 src/control-plane/testkit/src/lib.rs src/control-plane/memory/tests/queue.rs src/control-plane/postgres/tests/queue.rs
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/memory:queue //src/control-plane/postgres:queue
```
Expected: both `queue` targets gain a passing test (memory queue: was 2 → 3; postgres queue: was 2 → 3).

- [ ] **Step 5: Commit.**
```bash
git add src/control-plane/testkit src/control-plane/memory src/control-plane/postgres
git commit -m "test(control-plane): queue concurrency contract (SKIP LOCKED exactly-once)"
```

---

### Task 2: Handler-panic containment in `Worker` (Item A)

**Files:**
- Modify: `src/control-plane/worker/Cargo.toml`, `src/control-plane/worker/BUCK`, `third-party/BUCK` (buckify)
- Modify: `src/control-plane/worker/src/lib.rs`
- Modify: `src/control-plane/worker/tests/worker.rs`

- [ ] **Step 1: Add `futures-util` to `worker/Cargo.toml`** under `[dependencies]`:
```toml
# catch_unwind to contain a panicking handler (FutureExt::catch_unwind needs `std`).
futures-util = { version = "0.3", default-features = false, features = ["std"] }
```

- [ ] **Step 2: Refresh the lock + regenerate third-party rules** (the `//third-party:futures-util` alias doesn't exist yet):
```bash
buck2 run //tools:reindeer -- update
./tools/buckify.sh
git diff --stat third-party/BUCK Cargo.lock
```
Expected: `third-party/BUCK` gains a public `futures-util` alias; `Cargo.lock` gains the `control-plane-worker → futures-util` edge. If large unrelated churn appears, stop and investigate (the `reindeer-check` hook will reject drift).

- [ ] **Step 3: Add `//third-party:futures-util` to `worker/BUCK`** in the `worker` `rust_library` deps (alphabetical):
```python
    deps = [
        "//src/control-plane/core:core",
        "//third-party:futures-util",
        "//third-party:tokio",
        "//third-party:tokio-util",
    ],
```

- [ ] **Step 4: Edit `src/control-plane/worker/src/lib.rs`.**

(a) Add imports near the top:
```rust
use std::panic::AssertUnwindSafe;

use futures_util::FutureExt;
```

(b) Add a free helper (e.g. just above `impl<Q…>`):
```rust
/// Best-effort extraction of a panic payload's message.
fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = panic.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}
```

(c) Replace the `Some(job)` arm's handler execution + outcome handling. The current arm
builds `let fut = handler(job); tokio::pin!(fut);` then the `select!` loop breaking to
`outcome` matched as `Ok(())`/`Err(JobFailure…)`. Change it to wrap with `catch_unwind`
and handle the panic branch:
```rust
                Some(job) => {
                    let id = job.id;
                    // Run the handler while heartbeating the lease (see Step 2a#1).
                    // catch_unwind contains a panicking handler so it fails the job
                    // (Abandon) instead of tearing down the loop.
                    let fut = AssertUnwindSafe(handler(job)).catch_unwind();
                    tokio::pin!(fut);
                    let mut hb = tokio::time::interval(self.heartbeat_interval);
                    let outcome = loop {
                        tokio::select! {
                            res = &mut fut => break res,
                            _ = hb.tick() => {
                                let _ = self.queue.heartbeat(id).await;
                            }
                        }
                    };
                    match outcome {
                        Ok(Ok(())) => self.queue.complete(id).await?,
                        Ok(Err(JobFailure { error, policy })) => {
                            self.queue.fail(id, &error, policy).await?
                        }
                        Err(panic) => {
                            let msg = panic_message(&*panic);
                            self.queue
                                .fail(id, &format!("panic: {msg}"), RetryPolicy::Abandon)
                                .await?
                        }
                    }
                }
```
Add `RetryPolicy` to the `use control_plane_core::{…}` import if not already present (the worker already imports `Job, JobFailure, Queue, Result`; add `RetryPolicy`).

- [ ] **Step 5: Build worker + clippy.**
```bash
env -u BUCK_PREFER_REMOTE buck2 build //src/control-plane/worker:worker '//src/control-plane/worker:worker[clippy.txt]'
```
Expected: builds clean; `[clippy.txt]` empty. (If clippy flags `&Box<…>` anywhere, the `&*panic` + `&(dyn Any + Send)` signature already avoids it.)

- [ ] **Step 6: Add the panic-containment test** to `src/control-plane/worker/tests/worker.rs`:
```rust
// A panicking handler is contained: that job is Abandoned and the worker survives to
// process other jobs, instead of the panic tearing down the run loop. (The panic
// message is printed to stderr by the default hook before being caught — expected noise.)
#[tokio::test]
async fn handler_panic_is_contained() {
    let cp = MemoryControlPlane::new(LOCK_TIMEOUT);
    cp.enqueue(job("boom")).await.unwrap();
    cp.enqueue(job("ok")).await.unwrap();

    let ok_ran = Arc::new(AtomicU32::new(0));
    let r = ok_ran.clone();
    let token = CancellationToken::new();
    let t = token.clone();

    let worker =
        Worker::new(cp.clone(), "w1", LOCK_TIMEOUT).with_poll_interval(Duration::from_millis(50));
    let handle = tokio::spawn(async move {
        worker
            .run(&["boom".to_string(), "ok".to_string()], t, move |j| {
                let r = r.clone();
                async move {
                    if j.kind == "boom" {
                        panic!("handler blew up");
                    }
                    r.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            })
            .await
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    token.cancel();
    let run_result = handle.await.unwrap();
    assert!(run_result.is_ok(), "worker survived the panic (run returned Ok)");
    assert_eq!(
        ok_ran.load(Ordering::SeqCst),
        1,
        "the non-panicking job was processed"
    );
    assert!(
        cp.dequeue(&["boom".to_string(), "ok".to_string()], "probe")
            .await
            .unwrap()
            .is_none(),
        "panicking job abandoned (not retried/stuck); good job completed"
    );
}
```
(`job`, `KIND`/`LOCK_TIMEOUT`, `Arc`, `AtomicU32`, `Ordering`, `Duration`, `CancellationToken`, `Worker`, `MemoryControlPlane`, `Queue` are already imported in this file.)

- [ ] **Step 7: Format, run worker tests.**
```bash
eval "$(./tools/env.sh)" >/dev/null 2>&1 && rustfmt --edition 2024 src/control-plane/worker/src/lib.rs src/control-plane/worker/tests/worker.rs
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/worker:worker_test
```
Expected: all worker unit tests pass incl. `handler_panic_is_contained` (was 4 → 5). A `thread '…' panicked at 'handler blew up'` line on stderr is expected (the caught panic still runs the default hook).

- [ ] **Step 8: Commit.**
```bash
git add src/control-plane/worker third-party/BUCK Cargo.lock
git commit -m "feat(worker): contain handler panics via catch_unwind (Abandon the job)"
```

---

## Final Verification

- [ ] **Branch + commits:**
```bash
git branch --show-current   # feat/queue-worker-robustness
git log --oneline -5
```
- [ ] **Full suite:**
```bash
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...
```
Expected: all pass; `memory:queue` and `postgres:queue` each +1 test (concurrency), `worker_test` +1 (panic). Target Pass count unchanged (no new targets) — verify the per-target counts.
- [ ] **Lint:**
```bash
buck2 run //tools:prek -- run --all-files
```
Expected: rustfmt, clippy, file checks, **reindeer-in-sync** all pass (the `futures-util` lockfile + alias are committed).
- [ ] Hand off to **superpowers:finishing-a-development-branch**.

---

## Self-Review Notes (for the implementer)

- **Spec coverage:** catch_unwind (futures-util) inline; Abandon on panic with best-effort message; panic-containment test; concurrency contract (exactly-once) on both adapters via current-thread interleaving. Covered.
- **Type consistency:** outcome is `Result<Result<(), JobFailure>, Box<dyn Any + Send>>`; the three match arms handle complete / fail / panic→Abandon. `panic_message(&*panic)` takes `&(dyn Any + Send)` (clippy-clean, no `&Box`). `RetryPolicy` imported.
- **Dep hygiene:** `futures-util` added to `worker/Cargo.toml` **and** lockfile refreshed (`reindeer update`) **and** `third-party/BUCK` buckified **and** `worker/BUCK` dep added — all four, or `reindeer-check` fails (the refresh-lockfile lesson).
- **No new BUCK targets / no new tokio features:** concurrency test uses plain `#[tokio::test]` (only `rt`, already enabled); new tests live in existing files.
