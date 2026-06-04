# Control-Plane Queue — Phase 1b (Worker) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add low-latency job wakeups (`await_jobs`) to the `Queue` trait and both adapters, then ship a generic `control-plane-worker` crate — a dequeue→handle→complete/fail loop with `LISTEN`/`NOTIFY` wakeups, a polling fallback, and graceful shutdown — integration-tested against the hermetic Postgres fixture.

**Architecture:** `await_jobs(kinds, timeout)` is a new `Queue` method: Postgres uses `LISTEN`/`pg_notify` (notify fired inside the same statement as the insert, so it is transaction-buffered — a rolled-back enqueue sends nothing); the in-memory fake uses a `tokio::sync::Notify`. Both fall back to `timeout`. A new `Worker<Q: Queue>` crate (core + tokio + tokio-util only — `core` stays runtime-free) owns the loop and a `tokio_util::sync::CancellationToken` for shutdown. Retry stays caller-driven: the handler returns `Result<(), JobFailure>` carrying a `RetryPolicy`.

**Tech Stack:** Rust 2024, buck2, sqlx 0.8 (`PgListener`), tokio (`sync`/`time`/`macros`), tokio-util (`CancellationToken`), reindeer for third-party, hermetic `PgFixture`.

---

## Background for the implementer

This is the second cycle of the queue (1a — the `Queue` trait, both adapters, transactional `Tx::enqueue`, `queue.jobs` schema — is already merged on `main`). Read the design at `docs/superpowers/specs/2026-06-04-control-plane-queue-design.md` ("Worker (Phase 1b)" and "Testing" sections) for context, but everything you need is in this plan.

Key facts about the existing code you will extend:

- **`Queue` trait** lives in `src/control-plane/core/src/queue.rs`. Methods: `enqueue`, `dequeue`, `complete`, `fail`, `heartbeat`, all `async` returning `control_plane_core::Result<_>`. `std::time::Duration`, `time::OffsetDateTime`, `async_trait`, `uuid::Uuid` are already imported there.
- **`core` is deliberately runtime-free** — it depends on no async runtime. Do NOT add a `tokio` dependency to `core`. `await_jobs` is declared on the trait (no default body) so each adapter supplies its own; that is why both adapters must implement it in the same task (the workspace would not compile otherwise).
- **Memory adapter** (`src/control-plane/memory/src/lib.rs`): `MemoryControlPlane { rows: Arc<Mutex<Vec<Row>>>, lock_timeout }`, `Clone`. `MemoryTx { rows, staged }`. `tokio` is already a (non-dev) dependency with features `["macros", "rt"]`.
- **Postgres adapter** (`src/control-plane/postgres/src/lib.rs`): `PgControlPlane { pool: PgPool, lock_timeout }`, `Clone`. `pg_insert<'e, E: PgExecutor<'e>>(ex, job)` is the shared insert used by both `Queue::enqueue` (autocommit) and `PgTx::enqueue` (transactional). `backend(sqlx::Error) -> ControlPlaneError` maps errors. The fixture (`src/control-plane/postgres/src/fixture.rs`) hands out a fresh `PgControlPlane` per test via `fresh_control_plane()`; its lock_timeout is `Duration::from_millis(300)`.
- **testkit** (`src/control-plane/testkit/src/lib.rs`): `queue_contract<CP: ControlPlane + Queue>(&CP, lock_timeout)` — the shared suite. Has a private `job(kind) -> NewJob` helper. `tokio` dep features `["time"]`.
- **reindeer / third-party:** third-party Rust is managed by reindeer (non-vendored). To add a crate: add it to the relevant crate's `Cargo.toml`, run `cargo generate-lockfile`, run `./tools/buckify.sh` (regenerates `third-party/BUCK`), then depend on `//third-party:<crate>`. The generated `//third-party:tokio` target enables the **union** of features requested across the whole workspace, so adding `sync`/`time` to one crate's `tokio` makes them available to the shared target.
- **Running tests:** the pg fixture refuses to run as root, and buck2 RE workers are root, so pg tests run locally: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/postgres:<target>`. Memory/worker tests that do not touch pg can run normally, but `--local-only` is always safe.
- **Formatting:** the rustfmt hook checks but does not fix. Run `cargo fmt --all` before every commit. Lint with `tools/clippy-all.sh` (or the `clippy` prek hook).

The design choices below were validated by throwaway spikes before this plan was written: the `INSERT … RETURNING kind` → `pg_notify` CTE delivers a wakeup on commit and nothing on rollback; `PgListener::connect_with(&pool)` + `listen_all` + `recv()` under `tokio::time::timeout` works under buck2; `tokio_util::sync::CancellationToken` needs no tokio-util feature; and the generic `Worker<Q>` loop drains then shuts down cleanly.

---

## File Structure

**Task 1 — `await_jobs` wakeup primitive**
- Modify: `src/control-plane/core/src/queue.rs` — add `await_jobs` to the `Queue` trait.
- Modify: `src/control-plane/memory/src/lib.rs` — `Notify` field, notify on enqueue/commit, `await_jobs` impl.
- Modify: `src/control-plane/memory/Cargo.toml` — add `sync`,`time` to tokio features.
- Modify: `src/control-plane/postgres/src/lib.rs` — CTE `pg_notify` in `pg_insert`; `await_jobs` via `PgListener`.
- Modify: `src/control-plane/postgres/Cargo.toml` — `tokio` as a normal dep (`time`).
- Modify: `src/control-plane/postgres/BUCK` — add `//third-party:tokio` to the library target.
- Modify: `src/control-plane/testkit/src/lib.rs` — add `await_jobs_contract`.
- Modify: `src/control-plane/testkit/Cargo.toml` — add `sync`,`rt`,`macros` to tokio features.
- Modify: `src/control-plane/memory/tests/queue.rs` and `src/control-plane/postgres/tests/queue.rs` — call the new contract.
- Run: `cargo generate-lockfile` + `./tools/buckify.sh` (tokio feature union changed).

**Task 2 — `control-plane-worker` crate**
- Create: `src/control-plane/worker/Cargo.toml`, `src/control-plane/worker/BUCK`, `src/control-plane/worker/src/lib.rs`.
- Create: `src/control-plane/worker/tests/worker.rs` — memory-backed worker tests.
- Modify: root `Cargo.toml` — add the worker crate to `[workspace] members` + add `tokio-util` to the workspace lock by depending on it.
- Run: `cargo generate-lockfile` + `./tools/buckify.sh` (new crate `tokio-util`).

**Task 3 — Postgres worker integration tests**
- Create: `src/control-plane/worker/tests/postgres.rs` — `Worker` over the pg fixture.
- Modify: `src/control-plane/worker/BUCK` — add the pg integration `rust_test` (fixture env wired from the postgres crate's public targets).

---

## Task 1: `await_jobs` wakeup primitive (trait + both adapters + contract)

**Files:**
- Modify: `src/control-plane/core/src/queue.rs`
- Modify: `src/control-plane/memory/src/lib.rs`, `src/control-plane/memory/Cargo.toml`, `src/control-plane/memory/tests/queue.rs`
- Modify: `src/control-plane/postgres/src/lib.rs`, `src/control-plane/postgres/Cargo.toml`, `src/control-plane/postgres/BUCK`, `src/control-plane/postgres/tests/queue.rs`
- Modify: `src/control-plane/testkit/src/lib.rs`, `src/control-plane/testkit/Cargo.toml`

This single task adds the trait method and both adapter implementations together because the trait has no default body — the workspace will not compile until every adapter implements it.

- [ ] **Step 1: Add the `await_jobs` contract to testkit (the failing test)**

In `src/control-plane/testkit/src/lib.rs`, add this public function (the existing `job` helper and imports are already present; add `serde_json` use if the compiler asks — `serde_json::json!` is already used via the `job` helper):

```rust
/// Contract for `Queue::await_jobs`. Takes `cp` by value (must be `Clone + Send +
/// Sync + 'static`) so the test can hold one handle in a spawned waiter and use
/// another to enqueue. Both adapters satisfy these bounds.
pub async fn await_jobs_contract<CP>(cp: CP)
where
    CP: ControlPlane + Queue + Clone + Send + Sync + 'static,
{
    let k = vec!["w".to_string()];

    // (a) idle: returns cleanly at/after the timeout (no job ever arrives).
    let t0 = std::time::Instant::now();
    cp.await_jobs(&k, Duration::from_millis(150))
        .await
        .expect("await_jobs returns Ok on timeout");
    let idle = t0.elapsed();
    assert!(
        idle >= Duration::from_millis(120) && idle < Duration::from_secs(2),
        "idle await_jobs should block ~the timeout, blocked {idle:?}"
    );

    // (b) wakeup: a concurrent enqueue releases a waiter well before its long timeout.
    let cp2 = cp.clone();
    let kk = k.clone();
    let waiter = tokio::spawn(async move { cp2.await_jobs(&kk, Duration::from_secs(30)).await });
    tokio::time::sleep(Duration::from_millis(100)).await; // let the waiter register / LISTEN
    let t1 = std::time::Instant::now();
    cp.enqueue(NewJob {
        kind: "w".into(),
        payload: serde_json::json!({}),
        run_at: None,
        priority: 0,
    })
    .await
    .expect("enqueue");
    waiter.await.expect("waiter task").expect("await_jobs ok");
    assert!(
        t1.elapsed() < Duration::from_secs(5),
        "enqueue wakeup should beat the 30s timeout, took {:?}",
        t1.elapsed()
    );
}
```

- [ ] **Step 2: Widen testkit's tokio features**

In `src/control-plane/testkit/Cargo.toml`, change the tokio line so the harness can spawn and notify:

```toml
tokio = { version = "1", features = ["macros", "rt", "sync", "time"] }
```

- [ ] **Step 3: Add `await_jobs` to the `Queue` trait**

In `src/control-plane/core/src/queue.rs`, add this method to the `Queue` trait (after `heartbeat`). `Duration` is already imported.

```rust
    /// Block until a job of one of `kinds` may have become available, or until
    /// `timeout` elapses — whichever comes first. A best-effort wakeup hint for
    /// workers: spurious early returns are allowed (the caller re-checks via
    /// `dequeue`), and the `timeout` is the polling fallback that bounds latency
    /// when a notification is missed.
    async fn await_jobs(&self, kinds: &[String], timeout: Duration) -> Result<()>;
```

- [ ] **Step 4: Implement `await_jobs` for the memory adapter**

In `src/control-plane/memory/src/lib.rs`:

Add the import (top of file, alongside the others):

```rust
use tokio::sync::Notify;
```

Add a `notify` field to `MemoryControlPlane` and construct it in `new`:

```rust
#[derive(Clone)]
pub struct MemoryControlPlane {
    rows: Arc<Mutex<Vec<Row>>>,
    notify: Arc<Notify>,
    lock_timeout: Duration,
}

impl MemoryControlPlane {
    pub fn new(lock_timeout: Duration) -> Self {
        Self {
            rows: Arc::new(Mutex::new(Vec::new())),
            notify: Arc::new(Notify::new()),
            lock_timeout,
        }
    }
```

In `Queue::enqueue`, notify waiters after inserting (release the lock first so a woken waiter can immediately dequeue):

```rust
    async fn enqueue(&self, job: NewJob) -> Result<JobId> {
        let id = Self::insert(&mut self.rows.lock().unwrap(), job);
        self.notify.notify_waiters();
        Ok(JobId(id))
    }
```

Add the `await_jobs` impl at the end of the `impl Queue for MemoryControlPlane` block:

```rust
    async fn await_jobs(&self, _kinds: &[String], timeout: Duration) -> Result<()> {
        // notify_waiters only wakes already-registered waiters; a notification
        // racing ahead of `notified()` is intentionally lost — the `timeout`
        // polling fallback bounds the resulting latency (same contract as pg).
        let _ = tokio::time::timeout(timeout, self.notify.notified()).await;
        Ok(())
    }
```

Give `MemoryTx` access to the `Notify` and ring it on commit. Update `begin`:

```rust
    async fn begin(&self) -> Result<Box<dyn Tx + Send>> {
        Ok(Box::new(MemoryTx {
            rows: self.rows.clone(),
            notify: self.notify.clone(),
            staged: Vec::new(),
        }))
    }
```

Update the `MemoryTx` struct and its `commit`:

```rust
struct MemoryTx {
    rows: Arc<Mutex<Vec<Row>>>,
    notify: Arc<Notify>,
    staged: Vec<(Uuid, NewJob)>,
}
```

```rust
    async fn commit(self: Box<Self>) -> Result<()> {
        let staged_any = !self.staged.is_empty();
        {
            let mut rows = self.rows.lock().unwrap();
            for (id, job) in self.staged {
                MemoryControlPlane::insert_with_id(&mut rows, id, job);
            }
        }
        if staged_any {
            self.notify.notify_waiters();
        }
        Ok(())
    }
```

(Leave `rollback` unchanged — it must NOT notify.)

- [ ] **Step 5: Widen memory's tokio features**

In `src/control-plane/memory/Cargo.toml`, the library now uses tokio (`Notify`, `timeout`), so update the comment and features:

```toml
# tokio is a normal (not dev) dependency so reindeer emits //third-party:tokio.
# The library uses it for await_jobs (Notify + timeout); the #[tokio::test]
# harness in tests/ uses macros + rt.
tokio = { version = "1", features = ["macros", "rt", "sync", "time"] }
```

- [ ] **Step 6: Implement `await_jobs` for the postgres adapter**

In `src/control-plane/postgres/src/lib.rs`:

Replace `pg_insert` so the insert and the notify happen in one statement (transaction-buffered — a rolled-back `Tx::enqueue` sends no notification):

```rust
async fn pg_insert<'e, E: sqlx::PgExecutor<'e>>(ex: E, job: &NewJob) -> Result<JobId> {
    let id = Uuid::new_v4();
    // Insert and fire the wakeup in a single statement: pg_notify inside a
    // transaction is buffered until commit, so a rolled-back enqueue is silent.
    // Channel is per-kind so workers only wake for kinds they handle.
    sqlx::query(
        "with ins as ( \
             insert into queue.jobs (id, kind, payload, state, run_at, priority) \
             values ($1, $2, $3, 'available', coalesce($4, now()), $5) \
             returning kind) \
         select pg_notify('loom_queue:' || kind, '') from ins",
    )
    .bind(id)
    .bind(&job.kind)
    .bind(&job.payload)
    .bind(job.run_at)
    .bind(job.priority)
    .execute(ex)
    .await
    .map_err(backend)?;
    Ok(JobId(id))
}
```

Add the `await_jobs` impl to the `impl Queue for PgControlPlane` block (after `heartbeat`):

```rust
    async fn await_jobs(&self, kinds: &[String], timeout: Duration) -> Result<()> {
        let mut listener = sqlx::postgres::PgListener::connect_with(&self.pool)
            .await
            .map_err(backend)?;
        let channels: Vec<String> = kinds.iter().map(|k| format!("loom_queue:{k}")).collect();
        listener
            .listen_all(channels.iter().map(String::as_str))
            .await
            .map_err(backend)?;
        // A notification, or the polling-fallback timeout — whichever first.
        let _ = tokio::time::timeout(timeout, listener.recv()).await;
        Ok(())
    }
```

- [ ] **Step 7: Make tokio a library dependency of the postgres adapter**

In `src/control-plane/postgres/Cargo.toml`, add tokio to `[dependencies]` (the library now uses `tokio::time::timeout`). Keep the existing `[dev-dependencies]` tokio line as-is.

```toml
# Used by await_jobs for the polling-fallback timeout around PgListener::recv.
tokio = { version = "1", features = ["time"] }
```

In `src/control-plane/postgres/BUCK`, add `"//third-party:tokio",` to the `rust_library(name = "postgres")` `deps` list (keep it alphabetically between `time` and `uuid`):

```python
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:uuid",
```

- [ ] **Step 8: Wire the new contract into both adapters' test files**

In `src/control-plane/memory/tests/queue.rs`, add a test calling the new contract (read the file first to match its existing style; it currently constructs `MemoryControlPlane::new(...)` and calls `queue_contract`). Add:

```rust
#[tokio::test]
async fn memory_passes_await_jobs_contract() {
    let cp = control_plane_memory::MemoryControlPlane::new(std::time::Duration::from_millis(300));
    control_plane_testkit::await_jobs_contract(cp).await;
}
```

In `src/control-plane/postgres/tests/queue.rs`, add:

```rust
#[tokio::test]
async fn postgres_passes_await_jobs_contract() {
    let fixture = PgFixture::start();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::await_jobs_contract(cp).await;
}
```

(The postgres test file already imports `PgFixture` and uses `#[tokio::test]`. `await_jobs_contract` takes `cp` by value; `PgControlPlane` is `Clone`.)

- [ ] **Step 9: Regenerate third-party (tokio feature union changed)**

The tokio feature set requested by the workspace grew (`sync`, `time` added). Regenerate the lockfile and buck rules:

```bash
cargo generate-lockfile
./tools/buckify.sh
git diff --stat third-party/BUCK
```

Expected: `third-party/BUCK` shows tokio gaining `sync`/`time` feature wiring. If `buckify.sh` reports an error or the diff is empty when you expected a change, stop and report.

- [ ] **Step 10: Build, test, format, lint**

```bash
cargo fmt --all
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/...
tools/clippy-all.sh
```

Expected: all targets build; `memory:queue` (both tests) and `postgres:queue` (both tests) pass; clippy clean. The pg `await_jobs` contract proves NOTIFY wakes a waiter; the memory one proves the `Notify` path.

- [ ] **Step 11: Commit**

```bash
git add -A
git commit -m "feat(control-plane): add Queue::await_jobs (LISTEN/NOTIFY + Notify wakeups)"
```

---

## Task 2: `control-plane-worker` crate

**Files:**
- Create: `src/control-plane/worker/Cargo.toml`
- Create: `src/control-plane/worker/BUCK`
- Create: `src/control-plane/worker/src/lib.rs`
- Create: `src/control-plane/worker/tests/worker.rs`
- Modify: root `Cargo.toml` (workspace members + tokio-util)

The worker is generic over `Q: Queue` and owns a `Q` by value (adapters are `Clone`, so callers pass `cp.clone()`). It depends only on `core` + `tokio` + `tokio-util` — keeping `core` runtime-free while the worker carries the runtime concerns.

- [ ] **Step 1: Create the crate manifest**

`src/control-plane/worker/Cargo.toml`:

```toml
[package]
name = "control-plane-worker"
version = "0.1.0"
edition = "2024"

[dependencies]
control-plane-core = { path = "../core" }
# select! needs the macros feature; the loop itself does not spawn.
tokio = { version = "1", features = ["macros"] }
# CancellationToken for graceful shutdown (no extra tokio-util feature needed).
tokio-util = { version = "0.7", default-features = false }

[dev-dependencies]
control-plane-memory = { path = "../memory" }
control-plane-postgres = { path = "../postgres" }
serde_json = "1"
# Tests spawn the worker and drive time; the normal-dep tokio-util is reused.
tokio = { version = "1", features = ["macros", "rt", "rt-multi-thread", "time", "sync"] }
```

- [ ] **Step 2: Add the crate to the workspace and pull tokio-util into the lock**

In the root `Cargo.toml`, add the worker crate to `[workspace] members` (keep the list readable):

```toml
members = ["src/control-plane/core", "src/control-plane/memory", "src/control-plane/postgres", "src/control-plane/testkit", "src/control-plane/worker", "src/hello"]
```

- [ ] **Step 3: Write the failing worker test (memory-backed)**

`src/control-plane/worker/tests/worker.rs`:

```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use control_plane_core::{JobFailure, NewJob, Queue, RetryPolicy};
use control_plane_memory::MemoryControlPlane;
use control_plane_worker::Worker;
use tokio_util::sync::CancellationToken;

fn job(kind: &str) -> NewJob {
    NewJob {
        kind: kind.into(),
        payload: serde_json::json!({}),
        run_at: None,
        priority: 0,
    }
}

const KIND: &str = "t";
const LOCK_TIMEOUT: Duration = Duration::from_millis(300);

// All enqueued jobs get processed, then the worker shuts down on cancel.
#[tokio::test]
async fn drains_jobs_then_shuts_down() {
    let cp = MemoryControlPlane::new(LOCK_TIMEOUT);
    for _ in 0..5 {
        cp.enqueue(job(KIND)).await.unwrap();
    }
    let count = Arc::new(AtomicU32::new(0));
    let c = count.clone();
    let token = CancellationToken::new();
    let t = token.clone();

    let worker = Worker::new(cp.clone(), "w1").with_poll_interval(Duration::from_millis(50));
    let handle = tokio::spawn(async move {
        worker
            .run(&[KIND.to_string()], t, move |_job| {
                let c = c.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            })
            .await
    });

    // Give it time to drain, then ask it to stop.
    tokio::time::sleep(Duration::from_millis(300)).await;
    token.cancel();
    handle.await.unwrap().unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 5);
    assert!(
        cp.dequeue(&[KIND.to_string()], "probe").await.unwrap().is_none(),
        "all jobs completed (removed)"
    );
}

// A handler that fails once (Retry) then succeeds; the job is retried.
#[tokio::test]
async fn retry_then_succeed() {
    let cp = MemoryControlPlane::new(LOCK_TIMEOUT);
    cp.enqueue(job(KIND)).await.unwrap();
    let attempts = Arc::new(AtomicU32::new(0));
    let a = attempts.clone();
    let token = CancellationToken::new();
    let t = token.clone();

    let worker = Worker::new(cp.clone(), "w1").with_poll_interval(Duration::from_millis(50));
    let handle = tokio::spawn(async move {
        worker
            .run(&[KIND.to_string()], t, move |_job| {
                let a = a.clone();
                async move {
                    if a.fetch_add(1, Ordering::SeqCst) == 0 {
                        Err(JobFailure {
                            error: "first try fails".into(),
                            policy: RetryPolicy::Retry {
                                delay: Duration::from_millis(50),
                            },
                        })
                    } else {
                        Ok(())
                    }
                }
            })
            .await
    });

    tokio::time::sleep(Duration::from_millis(400)).await;
    token.cancel();
    handle.await.unwrap().unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 2, "failed once, then succeeded");
    assert!(
        cp.dequeue(&[KIND.to_string()], "probe").await.unwrap().is_none(),
        "job eventually completed"
    );
}

// An always-failing handler that Abandons ends terminal: it is never re-run.
#[tokio::test]
async fn abandon_is_terminal() {
    let cp = MemoryControlPlane::new(LOCK_TIMEOUT);
    cp.enqueue(job(KIND)).await.unwrap();
    let attempts = Arc::new(AtomicU32::new(0));
    let a = attempts.clone();
    let token = CancellationToken::new();
    let t = token.clone();

    let worker = Worker::new(cp.clone(), "w1").with_poll_interval(Duration::from_millis(50));
    let handle = tokio::spawn(async move {
        worker
            .run(&[KIND.to_string()], t, move |_job| {
                let a = a.clone();
                async move {
                    a.fetch_add(1, Ordering::SeqCst);
                    Err(JobFailure {
                        error: "always".into(),
                        policy: RetryPolicy::Abandon,
                    })
                }
            })
            .await
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    token.cancel();
    handle.await.unwrap().unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 1, "abandoned job runs exactly once");
}
```

- [ ] **Step 4: Run the test to confirm it fails (no crate yet)**

```bash
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/worker:worker 2>&1 | tail -20
```

Expected: build failure — `control-plane-worker` / `Worker` / `JobFailure` do not exist yet. (You will create the BUCK target in Step 6.)

- [ ] **Step 5: Implement the worker**

First, `JobFailure` belongs in `core` so handlers can name it without depending on the worker crate. In `src/control-plane/core/src/queue.rs`, add (near `RetryPolicy`):

```rust
/// What a worker's job handler returns on failure: a message plus the
/// caller-driven [`RetryPolicy`] to apply.
#[derive(Debug)]
pub struct JobFailure {
    pub error: String,
    pub policy: RetryPolicy,
}
```

Export it from `src/control-plane/core/src/lib.rs`:

```rust
pub use queue::{Job, JobFailure, JobId, NewJob, Queue, RetryPolicy};
```

Now `src/control-plane/worker/src/lib.rs`:

```rust
//! A generic job-queue worker: a `dequeue → handle → complete/fail` loop with
//! `await_jobs` wakeups (LISTEN/NOTIFY for the pg adapter, a `Notify` for the
//! fake), a polling fallback, and graceful shutdown via a `CancellationToken`.
//!
//! Generic over any [`Queue`]; owns the queue by value (adapters are `Clone`).
//! `core` stays runtime-free — the worker carries the tokio dependency.

use std::future::Future;
use std::time::Duration;

use control_plane_core::{Job, JobFailure, Queue, Result};
use tokio_util::sync::CancellationToken;

/// Default polling fallback interval. NOTIFY drives normal latency; this bounds
/// the wait when a notification is missed or a lock expires for reclaim.
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// A worker that drains jobs of the given kinds from a [`Queue`].
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

    /// Override the polling fallback interval (mainly for tests).
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Run the loop until `shutdown` is cancelled. Per iteration: claim a job and
    /// run `handler` (`Ok` → complete, `Err(JobFailure)` → fail with its policy);
    /// if none is available, wait for a wakeup or the poll interval. An in-flight
    /// job is always finished before shutdown returns; cancellation is only
    /// observed between jobs and while idle.
    pub async fn run<F, Fut>(
        &self,
        kinds: &[String],
        shutdown: CancellationToken,
        handler: F,
    ) -> Result<()>
    where
        F: Fn(Job) -> Fut + Send,
        Fut: Future<Output = std::result::Result<(), JobFailure>> + Send,
    {
        loop {
            if shutdown.is_cancelled() {
                break;
            }
            match self.queue.dequeue(kinds, &self.worker_id).await? {
                Some(job) => {
                    let id = job.id;
                    match handler(job).await {
                        Ok(()) => self.queue.complete(id).await?,
                        Err(JobFailure { error, policy }) => {
                            self.queue.fail(id, &error, policy).await?
                        }
                    }
                }
                None => {
                    tokio::select! {
                        _ = shutdown.cancelled() => break,
                        _ = self.queue.await_jobs(kinds, self.poll_interval) => {}
                    }
                }
            }
        }
        Ok(())
    }
}
```

- [ ] **Step 6: Create the worker BUCK target**

`src/control-plane/worker/BUCK`:

```python
rust_library(
    name = "worker",
    crate = "control_plane_worker",
    srcs = glob(["src/**/*.rs"]),
    crate_root = "src/lib.rs",
    edition = "2024",
    deps = [
        "//src/control-plane/core:core",
        "//third-party:tokio",
        "//third-party:tokio-util",
    ],
    visibility = ["PUBLIC"],
)

rust_test(
    name = "worker",
    crate = "worker_test",
    srcs = ["tests/worker.rs"],
    crate_root = "tests/worker.rs",
    edition = "2024",
    deps = [
        ":worker",
        "//src/control-plane/core:core",
        "//src/control-plane/memory:memory",
        "//third-party:serde_json",
        "//third-party:tokio",
        "//third-party:tokio-util",
    ],
)
```

- [ ] **Step 7: Regenerate third-party (new crate: tokio-util)**

```bash
cargo generate-lockfile
./tools/buckify.sh
git diff --stat third-party/BUCK
```

Expected: `third-party/BUCK` gains a `tokio-util` target (and its deps, e.g. `pin-project-lite` if not already present). tokio-util has no build script, so `buckify.sh` should not warn about a missing `[buildscript]` decision. If it warns, stop and report.

- [ ] **Step 8: Build, test, format, lint**

```bash
cargo fmt --all
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/worker:worker //src/control-plane/...
tools/clippy-all.sh
```

Expected: the worker library builds; all three memory-backed worker tests pass; the rest of the control plane still passes; clippy clean.

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "feat(control-plane): add control-plane-worker (dequeue/handle loop + graceful shutdown)"
```

---

## Task 3: Postgres worker integration tests

**Files:**
- Create: `src/control-plane/worker/tests/postgres.rs`
- Modify: `src/control-plane/worker/BUCK`

Proves the worker drives the real Postgres adapter end to end, including that NOTIFY (not the poll timeout) delivers a job. The fixture's bin/lib/migrations env is wired from the postgres crate's PUBLIC targets.

- [ ] **Step 1: Write the integration test**

`src/control-plane/worker/tests/postgres.rs`:

```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use control_plane_core::{NewJob, Queue};
use control_plane_postgres::fixture::PgFixture;
use control_plane_worker::Worker;
use tokio_util::sync::CancellationToken;

fn job(kind: &str) -> NewJob {
    NewJob {
        kind: kind.into(),
        payload: serde_json::json!({}),
        run_at: None,
        priority: 0,
    }
}

const KIND: &str = "t";

// N enqueued jobs are all completed by a worker running against real Postgres.
#[tokio::test]
async fn worker_drains_postgres_jobs() {
    let fixture = PgFixture::start();
    let cp = fixture.fresh_control_plane().await;
    for _ in 0..5 {
        cp.enqueue(job(KIND)).await.unwrap();
    }
    let count = Arc::new(AtomicU32::new(0));
    let c = count.clone();
    let token = CancellationToken::new();
    let t = token.clone();

    let worker = Worker::new(cp.clone(), "w1").with_poll_interval(Duration::from_millis(100));
    let handle = tokio::spawn(async move {
        worker
            .run(&[KIND.to_string()], t, move |_job| {
                let c = c.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            })
            .await
    });

    // Poll until all five are processed (bounded), then shut down.
    for _ in 0..100 {
        if count.load(Ordering::SeqCst) == 5 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    token.cancel();
    handle.await.unwrap().unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 5);
}

// NOTIFY (not the poll fallback) delivers a job: with a long poll interval, a job
// enqueued after the worker is idle still completes quickly.
#[tokio::test]
async fn notify_delivers_before_poll_timeout() {
    let fixture = PgFixture::start();
    let cp = fixture.fresh_control_plane().await;
    let done = Arc::new(AtomicU32::new(0));
    let d = done.clone();
    let token = CancellationToken::new();
    let t = token.clone();

    // Poll fallback is 30s; only NOTIFY can make this finish promptly.
    let worker = Worker::new(cp.clone(), "w1").with_poll_interval(Duration::from_secs(30));
    let handle = tokio::spawn(async move {
        worker
            .run(&[KIND.to_string()], t, move |_job| {
                let d = d.clone();
                async move {
                    d.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            })
            .await
    });

    // Let the worker reach its idle await_jobs (LISTEN) state, then enqueue.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let start = std::time::Instant::now();
    cp.enqueue(job(KIND)).await.unwrap();
    for _ in 0..100 {
        if done.load(Ordering::SeqCst) == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    token.cancel();
    handle.await.unwrap().unwrap();
    assert_eq!(done.load(Ordering::SeqCst), 1, "job was processed");
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "NOTIFY delivered the job well before the 30s poll, took {:?}",
        start.elapsed()
    );
}
```

- [ ] **Step 2: Add the pg integration test target**

Append to `src/control-plane/worker/BUCK` a second `rust_test`. Its env is sourced from the postgres crate's PUBLIC targets (`:postgres-bin`, `:libxml2`, `:migrations`) — the same wiring the postgres crate's own test uses. Note `crate_root` points at `tests/postgres.rs`:

```python
rust_test(
    name = "postgres-integration",
    crate = "worker_postgres_integration",
    srcs = ["tests/postgres.rs"],
    crate_root = "tests/postgres.rs",
    edition = "2024",
    env = {
        "POSTGRES_BIN_DIR": "$(location //src/control-plane/postgres:postgres-bin)/bin",
        "POSTGRES_LD_LIBRARY_PATH": "$(location //src/control-plane/postgres:postgres-bin)/lib:$(location //src/control-plane/postgres:libxml2)",
        "LOOM_MIGRATIONS_DIR": "$(location //src/control-plane/postgres:migrations)/migrations",
    },
    deps = [
        ":worker",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:serde_json",
        "//third-party:tokio",
        "//third-party:tokio-util",
    ],
)
```

- [ ] **Step 3: Build, test, format, lint**

```bash
cargo fmt --all
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/worker:postgres-integration
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/...
tools/clippy-all.sh
```

Expected: both pg integration tests pass (the worker drains real jobs; NOTIFY delivers before the 30s poll). The whole control plane still builds and passes; clippy clean.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "test(control-plane): worker integration tests against the postgres fixture"
```

---

## Final verification (after all tasks)

```bash
cargo fmt --all --check
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...
tools/clippy-all.sh
buck2 run //tools:prek -- run --all-files
```

All green ⇒ ready to push, open a PR, and let CI (`build-test` on the eventual merge to `main`) confirm the pg-backed tests on the non-root runner.

## Notes / gotchas the spikes surfaced

- **Per-statement notify is the whole trick for transactional safety.** Because `pg_notify` is issued inside the same `INSERT` statement, `Tx::enqueue` followed by `rollback` sends nothing — no extra code, no separate notify call to forget. Do not split the insert and notify into two statements.
- **`notify_waiters()` (memory) intentionally drops races.** A notification arriving before a waiter calls `notified()` is lost; the poll-timeout fallback covers it. This matches the pg adapter (LISTEN is issued after the prior dequeue). Tests use short poll intervals so this never stalls them.
- **`core` must stay runtime-free.** `JobFailure` and the `await_jobs` signature go in `core`, but no tokio. The worker crate is where tokio/tokio-util live.
- **Adapters are `Clone`** — `Worker<Q>` owns `Q` by value; pass `cp.clone()`. No `Arc` wrapper needed.
- **Run pg tests locally** (`env -u BUCK_PREFER_REMOTE … --local-only`): the fixture's `initdb`/`postgres` refuse to run as root, and buck2 RE workers are root.
```
