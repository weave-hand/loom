# Worker Lease Heartbeating (Step 2a #1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `Worker` renew its lease while a handler runs, so a handler outliving the queue's `lock_timeout` is no longer reclaimed and double-executed.

**Architecture:** A single-crate fix in `control-plane-worker`. `Worker::new` takes the `lease` (the queue's `lock_timeout`); `run` races the handler future against a `tokio::time::interval` and calls `Queue::heartbeat` (best-effort) on each tick. No `core`/trait change.

**Tech Stack:** Rust (edition 2024), `tokio` (`select!`, `time::interval`, `pin!`), `tokio-util` `CancellationToken`, `control-plane-core`, `control-plane-memory` (dev), buck2.

**Spec:** `docs/superpowers/specs/2026-06-06-worker-heartbeat-design.md` — implements it exactly.

---

## File Structure

- **Modify** `src/control-plane/worker/src/lib.rs` — add `heartbeat_interval` field; `new` takes `lease`; `run` heartbeats the in-flight job.
- **Modify** `src/control-plane/worker/tests/worker.rs` — update the 3 existing `Worker::new` call sites; add the regression test.
- **Modify** `src/control-plane/worker/tests/postgres.rs` — update its `Worker::new` call site(s).

No new dependencies. `tokio` is already a dep with the features used (`time`, `macros`, `rt`, `sync`); confirm `time` is present (it is — `await_jobs` uses `tokio::time::timeout`).

**Formatting (avoids a commit loop):** the prek `rustfmt` hook is check-only. Before committing, run `eval "$(./tools/env.sh)" >/dev/null 2>&1 && rustfmt --edition 2024 <changed .rs files>`, then `git add` + commit.

The branch is created by the executor before Task 1 — do **not** implement on `main`.

---

### Task 1: Add the lease + heartbeat interval to `Worker`

**Files:**
- Modify: `src/control-plane/worker/src/lib.rs`

- [ ] **Step 1: Add the `heartbeat_interval` field and the `lease` constructor param.**

Change the struct and `new` (and leave `with_poll_interval` as-is). Replace:
```rust
pub struct Worker<Q> {
    queue: Q,
    worker_id: String,
    poll_interval: Duration,
}

impl<Q: Queue + Send + Sync> Worker<Q> {
    /// Create a worker. `worker_id` stamps the lock (`locked_by`) so reclaim and
    /// observability can attribute in-flight jobs.
    pub fn new(queue: Q, worker_id: impl Into<String>) -> Self {
        Self {
            queue,
            worker_id: worker_id.into(),
            poll_interval: DEFAULT_POLL_INTERVAL,
        }
    }
```
with:
```rust
pub struct Worker<Q> {
    queue: Q,
    worker_id: String,
    poll_interval: Duration,
    heartbeat_interval: Duration,
}

impl<Q: Queue + Send + Sync> Worker<Q> {
    /// Create a worker. `worker_id` stamps the lock (`locked_by`) so reclaim and
    /// observability can attribute in-flight jobs. `lease` MUST match the queue's
    /// configured `lock_timeout` — it is how long a claimed job stays locked
    /// without a heartbeat. The worker heartbeats the in-flight job every
    /// `lease / 3` (floored at 1ms) so a handler running longer than the lease is
    /// not reclaimed and double-executed.
    pub fn new(queue: Q, worker_id: impl Into<String>, lease: Duration) -> Self {
        Self {
            queue,
            worker_id: worker_id.into(),
            poll_interval: DEFAULT_POLL_INTERVAL,
            heartbeat_interval: (lease / 3).max(Duration::from_millis(1)),
        }
    }
```

- [ ] **Step 2: Build to confirm the signature change compiles the library.**

Run: `env -u BUCK_PREFER_REMOTE buck2 build //src/control-plane/worker:worker`
Expected: builds clean (the tests won't build yet — they still call the 2-arg `new`; that's fixed in Task 3).

- [ ] **Step 3: Commit.**
```bash
eval "$(./tools/env.sh)" >/dev/null 2>&1 && rustfmt --edition 2024 src/control-plane/worker/src/lib.rs
git add src/control-plane/worker/src/lib.rs
git commit -m "feat(worker): take lease in Worker::new, derive heartbeat interval"
```
(The pre-commit clippy hook lints the whole tree; the worker tests don't build yet because their `Worker::new` calls are stale. If the hook fails for that reason, commit with `--no-verify` — Task 3 restores the tree, after which Task 4's commit runs hooks normally. If you prefer, fold Steps in Task 1 and Task 2 into a single commit with Task 3 so the tree never breaks; either is fine.)

---

### Task 2: Heartbeat the in-flight job in `run`

**Files:**
- Modify: `src/control-plane/worker/src/lib.rs`

- [ ] **Step 1: Replace the `Some(job)` arm of the `run` loop with the heartbeating version.**

Replace this block:
```rust
                Some(job) => {
                    let id = job.id;
                    match handler(job).await {
                        Ok(()) => self.queue.complete(id).await?,
                        Err(JobFailure { error, policy }) => {
                            self.queue.fail(id, &error, policy).await?
                        }
                    }
                }
```
with:
```rust
                Some(job) => {
                    let id = job.id;
                    // Run the handler while heartbeating the lease on a timer, so a
                    // handler that outlives `lock_timeout` isn't reclaimed and
                    // double-executed. The heartbeat is best-effort: a missed tick is
                    // recoverable (the next tick retries; worst case the lease lapses
                    // and reclaim does its job) — deliberately asymmetric with the
                    // `?`-propagating dequeue/complete/fail below.
                    let fut = handler(job);
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
                        Ok(()) => self.queue.complete(id).await?,
                        Err(JobFailure { error, policy }) => {
                            self.queue.fail(id, &error, policy).await?
                        }
                    }
                }
```
(`tokio::time::interval` fires its first tick immediately, producing one heartbeat right after the claim — harmless. The idle `None` branch and shutdown handling are unchanged.)

- [ ] **Step 2: Build the library.**

Run: `env -u BUCK_PREFER_REMOTE buck2 build //src/control-plane/worker:worker`
Expected: builds clean.

- [ ] **Step 3: Commit.**
```bash
eval "$(./tools/env.sh)" >/dev/null 2>&1 && rustfmt --edition 2024 src/control-plane/worker/src/lib.rs
git add src/control-plane/worker/src/lib.rs
git commit -m "feat(worker): heartbeat the in-flight job while the handler runs"
```
(Same tree-wide-clippy caveat as Task 1 Step 3 — `--no-verify` is acceptable until Task 3 fixes the test call sites.)

---

### Task 3: Update existing call sites and add the regression test

**Files:**
- Modify: `src/control-plane/worker/tests/worker.rs`
- Modify: `src/control-plane/worker/tests/postgres.rs`

- [ ] **Step 1: Update the three `Worker::new` calls in `tests/worker.rs`** to pass the lease (the file already defines `const LOCK_TIMEOUT: Duration = Duration::from_millis(300);`). Each call:
```rust
let worker = Worker::new(cp.clone(), "w1").with_poll_interval(Duration::from_millis(50));
```
becomes:
```rust
let worker = Worker::new(cp.clone(), "w1", LOCK_TIMEOUT).with_poll_interval(Duration::from_millis(50));
```

- [ ] **Step 2: Update both `Worker::new` call sites in `tests/postgres.rs`.**

Both tests build their control plane via `fixture.fresh_control_plane().await`, which constructs `PgControlPlane::new(pool, Duration::from_millis(300))` — i.e. the queue's `lock_timeout` is **300ms**. Pass that as the lease so the worker's heartbeat cadence matches the fixture. (Both handlers complete instantly, so the lease value doesn't affect these tests' behavior; matching the fixture keeps it correct and clear.)

In `worker_drains_postgres_jobs`:
```rust
let worker = Worker::new(cp.clone(), "w1").with_poll_interval(Duration::from_millis(100));
```
becomes:
```rust
let worker =
    Worker::new(cp.clone(), "w1", Duration::from_millis(300)).with_poll_interval(Duration::from_millis(100));
```

In `notify_delivers_before_poll_timeout`:
```rust
let worker = Worker::new(cp.clone(), "w1").with_poll_interval(Duration::from_secs(30));
```
becomes:
```rust
let worker =
    Worker::new(cp.clone(), "w1", Duration::from_millis(300)).with_poll_interval(Duration::from_secs(30));
```

- [ ] **Step 3: Add the regression test to `tests/worker.rs`.**

Append:
```rust
// A handler that runs longer than lock_timeout is NOT reclaimed: the worker
// heartbeats the lease, so the job runs exactly once and no other worker can
// steal it mid-flight. Without heartbeating this job would be double-executed.
#[tokio::test]
async fn heartbeat_keeps_long_handler_single() {
    let cp = MemoryControlPlane::new(LOCK_TIMEOUT); // 300ms lease
    cp.enqueue(job(KIND)).await.unwrap();

    let runs = Arc::new(AtomicU32::new(0));
    let r = runs.clone();
    let started = Arc::new(tokio::sync::Notify::new());
    let started_w = started.clone();

    let token = CancellationToken::new();
    let t = token.clone();
    let worker = Worker::new(cp.clone(), "w1", LOCK_TIMEOUT);
    let handle = tokio::spawn(async move {
        worker
            .run(&[KIND.to_string()], t, move |_job| {
                let r = r.clone();
                let started_w = started_w.clone();
                async move {
                    r.fetch_add(1, Ordering::SeqCst);
                    started_w.notify_one();
                    // Run well past LOCK_TIMEOUT so an un-heartbeated lease would expire.
                    tokio::time::sleep(LOCK_TIMEOUT * 3).await;
                    Ok(())
                }
            })
            .await
    });

    // Once the handler is in flight, a different worker must NOT be able to claim
    // the job: the lease is held by heartbeating, even though we're already past
    // LOCK_TIMEOUT relative to the original claim by the time we check.
    started.notified().await;
    tokio::time::sleep(LOCK_TIMEOUT + Duration::from_millis(100)).await;
    assert!(
        cp.dequeue(&[KIND.to_string()], "intruder")
            .await
            .unwrap()
            .is_none(),
        "lease held by heartbeat: another worker cannot reclaim the in-flight job"
    );

    // Let the handler finish and the worker drain, then shut down.
    tokio::time::sleep(LOCK_TIMEOUT * 3).await;
    token.cancel();
    handle.await.unwrap().unwrap();

    assert_eq!(runs.load(Ordering::SeqCst), 1, "handler ran exactly once");
    assert!(
        cp.dequeue(&[KIND.to_string()], "probe")
            .await
            .unwrap()
            .is_none(),
        "job completed (removed)"
    );
}
```

- [ ] **Step 4: Format, then run the worker tests.**
```bash
eval "$(./tools/env.sh)" >/dev/null 2>&1 && rustfmt --edition 2024 src/control-plane/worker/tests/worker.rs src/control-plane/worker/tests/postgres.rs
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/worker:worker_test
```
Expected: all worker unit tests pass, including `heartbeat_keeps_long_handler_single` (4 tests).

Then the pg integration test:
```bash
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/worker:postgres-integration
```
Expected: passes (unchanged behavior; just the threaded lease).

Then clippy:
```bash
env -u BUCK_PREFER_REMOTE buck2 build '//src/control-plane/worker:worker[clippy.txt]'
```
Expected: `[clippy.txt]` empty.

- [ ] **Step 5: Commit.**
```bash
git add src/control-plane/worker
git commit -m "test(worker): regression test for lease heartbeating; thread lease through call sites"
```

---

## Final Verification

- [ ] **Branch + commits** (per the `verify-branch-after-subagents` lesson):
```bash
git branch --show-current     # expect the feature branch, NOT main
git log --oneline -5
```

- [ ] **Full suite + tally:**
```bash
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...
```
Expected: all pass, with **one more** passing test than before (the new `heartbeat_keeps_long_handler_single` in the `worker_test` target — its per-target count goes 3 → 4; overall target Pass count is unchanged since it's a new test in an existing target, so verify the `worker_test` line shows 4 passed).

- [ ] **Lint** (CI's `lint` job):
```bash
buck2 run //tools:prek -- run --all-files
```
Expected: rustfmt, clippy, file checks, reindeer-in-sync all pass.

- [ ] Hand off to **superpowers:finishing-a-development-branch**.

---

## Self-Review Notes (for the implementer)

- **Spec coverage:** lease param on `new`; `lease/3` floored at 1ms; `select!` heartbeat loop best-effort; existing tests threaded; regression test asserting single execution + lease-held-against-intruder. All covered.
- **Type consistency:** `heartbeat_interval: Duration` field set in `new`; `run` reads `self.heartbeat_interval`; handler signature `Fn(Job) -> Fut` unchanged; no new trait bounds (`tokio::pin!` handles the non-`Unpin` future).
- **Timing:** the regression test sleeps in multiples of `LOCK_TIMEOUT` (300ms) — generous margins, consistent with the existing time-based tests in this file. The handler sleeps `LOCK_TIMEOUT * 3` and the intruder check happens at `LOCK_TIMEOUT + 100ms` after the handler starts (past the original lease, so only a heartbeat keeps it held).
- **Tree-breakage window:** Tasks 1–2 change `new`'s arity, breaking the tests until Task 3. The tree-wide clippy pre-commit hook may require `--no-verify` for the Tasks 1–2 commits (as in the lineage cycle); Task 3's commit and the final `prek --all-files` validate the whole tree. Folding Tasks 1–3 into fewer commits to avoid the window is acceptable.
