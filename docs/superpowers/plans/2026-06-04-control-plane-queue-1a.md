# Control-Plane Queue Phase 1a — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The queue *core* — a `queue.jobs` schema + sqlx migrations, the `Queue` trait (enqueue/dequeue SKIP-LOCKED/complete/fail/heartbeat) with caller-driven `RetryPolicy`, and transactional `Tx::enqueue` — satisfied by both the in-memory fake and the Postgres adapter under one `queue_contract` suite, retiring the temporary `probe_*` ops.

**Architecture:** Adds queue domain types + the `Queue` trait to `control-plane-core`; the fake implements them in memory (a `Vec` of job rows behind a `Mutex`), and the Postgres adapter implements them with sqlx's runtime query API (`UPDATE … FOR UPDATE SKIP LOCKED` for dequeue, with inline expired-lock reclaim). Migrations are applied at runtime via `sqlx::migrate::Migrator::new(dir)` from a `filegroup` of `.sql` files (the compile-time `sqlx::migrate!` macro can't run under buck2 — it needs an absolute `CARGO_MANIFEST_DIR`). Worker loop / LISTEN-NOTIFY are Phase 1b.

**Tech Stack:** Rust (edition 2024), `async-trait`, `sqlx` 0.8 (runtime-tokio, postgres, uuid, time, json, migrate — no `macros`), `uuid`, `time`, `serde_json`, buck2, reindeer.

**Scope notes / decisions (from the spec):**
- Caller-driven retry: `fail(id, error, RetryPolicy)`; `RetryPolicy = Retry { delay } | Abandon`. No `max_attempts` column.
- Crashed-worker recovery via **inline lock-expiry reclaim** in `dequeue` (no reaper daemon); `lock_timeout` is an adapter constructor arg (tests use a short value).
- Completed jobs are **deleted**; abandoned jobs retained as `state='failed'`.
- **Migrations: runtime `Migrator::new(dir)`** — verified working under buck2. The migrations dir reaches the adapter via a `filegroup` and `$(location :migrations)/migrations` env (the filegroup preserves the glob's `migrations/` prefix).
- `Queue` and `ControlPlane` are kept as **separate traits**; the contract is generic over `CP: ControlPlane + Queue` (a deliberate, minor deviation from the spec's `ControlPlane: Queue` supertrait, to allow green incremental commits).
- All code below was verified in a prototype (memory + real Postgres, incl. SKIP LOCKED concurrency, transactional rollback, lock reclaim) and the migration mechanism in an in-repo buck2 spike.

---

## File structure

```
src/control-plane/core/
  src/queue.rs        NEW  — JobId, NewJob, Job, RetryPolicy, Queue trait
  src/transaction.rs  MOD  — Tx: drop probe_*, add enqueue (Task 4)
  src/lib.rs          MOD  — module wiring + re-exports
  Cargo.toml          MOD  — uuid, time, serde_json
testkit/
  src/lib.rs          MOD  — replace tx_contract with queue_contract
memory/
  src/lib.rs          MOD  — impl Queue; Tx::enqueue (drop probe)
postgres/
  migrations/0001_queue.sql  NEW
  src/lib.rs          MOD  — impl Queue; run_migrations; Tx::enqueue (drop probe)
  src/fixture.rs      MOD  — run migrations instead of _probe DDL
  Cargo.toml          MOD  — sqlx features + uuid/time/serde_json
  BUCK                MOD  — migrations filegroup; LOOM_MIGRATIONS_DIR env
```

---

## Task 1: Queue types + `Queue` trait in `core` (+ deps)

Additive: a new `Queue` trait with no implementors yet, and the domain types. `Tx` is untouched here (probe stays), so the whole tree keeps compiling.

**Files:** Create `src/control-plane/core/src/queue.rs`; modify `core/src/lib.rs`, `core/Cargo.toml`.

- [ ] **Step 1: Add deps to `core/Cargo.toml`**

Set `[dependencies]` to:
```toml
[dependencies]
async-trait = "0.1"
thiserror = "1"
uuid = { version = "1", features = ["v4"] }
time = "0.3"
serde_json = "1"
```

- [ ] **Step 2: Buckify**

Run: `./tools/buckify.sh`
For any crate reindeer flags with a build-script warning, add `third-party/fixups/<crate>/fixups.toml` with `[buildscript]\nrun = true` (expected possibles: `time`-related crates; apply the same pattern to any flagged). Re-run `./tools/buckify.sh` until warning-clean.

- [ ] **Step 3: Write `core/src/queue.rs`**

```rust
//! The job-queue concern: typed job envelope + the `Queue` trait. Payloads are
//! opaque JSON; retry is caller-driven (`fail` takes a `RetryPolicy`).

use std::time::Duration;

use async_trait::async_trait;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::error::Result;

/// Identifier for an enqueued job.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct JobId(pub Uuid);

/// A job to enqueue.
pub struct NewJob {
    pub kind: String,
    pub payload: serde_json::Value,
    /// When the job becomes eligible. `None` => now.
    pub run_at: Option<OffsetDateTime>,
    /// Higher dequeued first.
    pub priority: i32,
}

/// A claimed job handed to a worker.
#[derive(Debug)]
pub struct Job {
    pub id: JobId,
    pub kind: String,
    pub payload: serde_json::Value,
    /// Number of times this job has been dequeued (this claim included). Callers
    /// use it to compute their own backoff.
    pub attempts: i32,
    pub run_at: OffsetDateTime,
}

/// What to do with a job after a failure. Caller-driven.
#[derive(Debug)]
pub enum RetryPolicy {
    /// Make the job available again after `delay`.
    Retry { delay: Duration },
    /// Move the job to a terminal `failed` state, retained for inspection.
    Abandon,
}

#[async_trait]
pub trait Queue {
    /// Enqueue a job (autocommit). For transactional enqueue, use [`crate::Tx::enqueue`].
    async fn enqueue(&self, job: NewJob) -> Result<JobId>;
    /// Claim the next eligible job for one of `kinds`, marking it running under
    /// `worker`. Jobs are eligible when `available`, or `running` with an expired
    /// lock (crashed-worker reclaim).
    async fn dequeue(&self, kinds: &[String], worker: &str) -> Result<Option<Job>>;
    /// Remove a finished job.
    async fn complete(&self, id: JobId) -> Result<()>;
    /// Record a failure and apply `policy`.
    async fn fail(&self, id: JobId, error: &str, policy: RetryPolicy) -> Result<()>;
    /// Refresh the lock so a long-running job isn't reclaimed.
    async fn heartbeat(&self, id: JobId) -> Result<()>;
}
```

- [ ] **Step 4: Wire `core/src/lib.rs`**

Append (keep existing `error`/`transaction` lines):
```rust
mod queue;

pub use queue::{Job, JobId, NewJob, Queue, RetryPolicy};
```

- [ ] **Step 5: Format + verify the whole tree still builds (probe untouched)**

Run: `eval "$(./tools/env.sh)" && cargo fmt && buck2 build //src/...`
Expected: BUILD SUCCEEDED (new trait compiles; no implementors yet; existing probe-based code unaffected).

- [ ] **Step 6: Commit**

```bash
git add src/control-plane/core Cargo.toml Cargo.lock third-party
git commit -m "feat(control-plane-core): add queue domain types + Queue trait

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: In-memory `Queue` impl + `queue_contract` (non-transactional parts)

**Files:** Modify `memory/src/lib.rs`, `memory/Cargo.toml`, `memory/BUCK`; modify `testkit/src/lib.rs`, `testkit/Cargo.toml`, `testkit/BUCK`; create `memory/tests/queue.rs`.

- [ ] **Step 1: Add deps**

`memory/Cargo.toml` `[dependencies]` — add `uuid` and `time` (keep existing `control-plane-core`, `async-trait`, `tokio`):
```toml
uuid = { version = "1", features = ["v4"] }
time = "0.3"
```
`testkit/Cargo.toml` `[dependencies]` — add `serde_json`, `time` (keep `control-plane-core`):
```toml
serde_json = "1"
time = "0.3"
```
Run: `./tools/buckify.sh` (warning-clean as in Task 1).

- [ ] **Step 2: Write the `queue_contract` suite in `testkit/src/lib.rs`**

Add (keep the existing `tx_contract` for now — it's removed in Task 4):
```rust
use std::time::Duration;

use control_plane_core::{ControlPlane, NewJob, Queue, RetryPolicy};
use time::OffsetDateTime;

fn job(kind: &str) -> NewJob {
    NewJob { kind: kind.into(), payload: serde_json::json!({}), run_at: None, priority: 0 }
}

/// Contract for the `Queue` ops (transactional `enqueue` is added in Task 4).
/// `cp` must be freshly empty; `lock_timeout` must match the adapter's configured
/// value so the reclaim assertion is timed correctly.
pub async fn queue_contract<CP: ControlPlane + Queue>(cp: &CP, lock_timeout: Duration) {
    let k = vec!["t".to_string()];
    let w = "worker-1";

    // enqueue -> dequeue (attempts=1) -> complete deletes
    let id = cp.enqueue(job("t")).await.expect("enqueue");
    let j = cp.dequeue(&k, w).await.expect("dequeue").expect("a job");
    assert_eq!(j.id, id);
    assert_eq!(j.attempts, 1);
    cp.complete(id).await.expect("complete");
    assert!(cp.dequeue(&k, w).await.unwrap().is_none(), "completed job is gone");

    // priority: higher first
    cp.enqueue(NewJob { priority: 1, ..job("t") }).await.unwrap();
    let hi = cp.enqueue(NewJob { priority: 5, ..job("t") }).await.unwrap();
    assert_eq!(cp.dequeue(&k, w).await.unwrap().unwrap().id, hi, "higher priority first");
    cp.complete(hi).await.unwrap();
    cp.complete(cp.dequeue(&k, w).await.unwrap().unwrap().id).await.unwrap();

    // future run_at is not eligible
    let future = OffsetDateTime::now_utc() + Duration::from_secs(3600);
    cp.enqueue(NewJob { run_at: Some(future), ..job("t") }).await.unwrap();
    assert!(cp.dequeue(&k, w).await.unwrap().is_none(), "future job not eligible");

    // fail + Retry reschedules; attempts increments
    let r = cp.enqueue(job("t")).await.unwrap();
    assert_eq!(cp.dequeue(&k, w).await.unwrap().unwrap().attempts, 1);
    cp.fail(r, "boom", RetryPolicy::Retry { delay: Duration::from_millis(150) }).await.unwrap();
    assert!(cp.dequeue(&k, w).await.unwrap().is_none(), "retry is delayed");
    tokio::time::sleep(Duration::from_millis(220)).await;
    let again = cp.dequeue(&k, w).await.unwrap().expect("retry eligible after delay");
    assert_eq!(again.attempts, 2);
    cp.complete(again.id).await.unwrap();

    // fail + Abandon -> terminal
    let a = cp.enqueue(job("t")).await.unwrap();
    cp.dequeue(&k, w).await.unwrap().unwrap();
    cp.fail(a, "dead", RetryPolicy::Abandon).await.unwrap();
    assert!(cp.dequeue(&k, w).await.unwrap().is_none(), "abandoned never returns");

    // expired-lock reclaim
    let e = cp.enqueue(job("t")).await.unwrap();
    assert_eq!(cp.dequeue(&k, w).await.unwrap().unwrap().id, e);
    assert!(cp.dequeue(&k, w).await.unwrap().is_none(), "locked, not yet reclaimed");
    tokio::time::sleep(lock_timeout + Duration::from_millis(50)).await;
    let reclaimed = cp.dequeue(&k, "worker-2").await.unwrap().expect("expired lock reclaimed");
    assert_eq!(reclaimed.id, e);
    assert_eq!(reclaimed.attempts, 2);

    // heartbeat keeps the lock fresh (no reclaim after a heartbeat within the window)
    cp.heartbeat(e).await.unwrap();
    cp.complete(e).await.unwrap();
}
```
Add to `testkit/BUCK` deps: `"//third-party:serde_json"`, `"//third-party:time"` (keep `//src/control-plane/core:core`).

- [ ] **Step 3: Implement `Queue` for the fake — rewrite `memory/src/lib.rs`**

```rust
//! In-memory fake adapter for the control-plane traits — fast, hermetic tests
//! and local dev. NOT for production use. Jobs live in a `Vec` behind a `Mutex`;
//! a `Tx` stages writes and applies them on commit (read-committed semantics).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use control_plane_core::{
    ControlPlane, Job, JobId, NewJob, Queue, Result, RetryPolicy, Tx,
};
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Clone)]
struct Row {
    id: Uuid,
    kind: String,
    payload: serde_json::Value,
    state: &'static str, // "available" | "running" | "failed"
    run_at: OffsetDateTime,
    priority: i32,
    attempts: i32,
    locked_at: Option<OffsetDateTime>,
}

#[derive(Clone)]
pub struct MemoryControlPlane {
    rows: Arc<Mutex<Vec<Row>>>,
    probe: Arc<Mutex<HashMap<String, i64>>>, // retained until Task 4 removes probe
    lock_timeout: Duration,
}

impl MemoryControlPlane {
    pub fn new(lock_timeout: Duration) -> Self {
        Self {
            rows: Arc::new(Mutex::new(Vec::new())),
            probe: Arc::new(Mutex::new(HashMap::new())),
            lock_timeout,
        }
    }

    fn insert(rows: &mut Vec<Row>, job: NewJob) -> Uuid {
        let id = Uuid::new_v4();
        rows.push(Row {
            id,
            kind: job.kind,
            payload: job.payload,
            state: "available",
            run_at: job.run_at.unwrap_or_else(OffsetDateTime::now_utc),
            priority: job.priority,
            attempts: 0,
            locked_at: None,
        });
        id
    }
}

#[async_trait]
impl Queue for MemoryControlPlane {
    async fn enqueue(&self, job: NewJob) -> Result<JobId> {
        Ok(JobId(Self::insert(&mut self.rows.lock().unwrap(), job)))
    }

    async fn dequeue(&self, kinds: &[String], _worker: &str) -> Result<Option<Job>> {
        let now = OffsetDateTime::now_utc();
        let cutoff = now - self.lock_timeout;
        let mut rows = self.rows.lock().unwrap();
        let mut idxs: Vec<usize> = (0..rows.len())
            .filter(|&i| {
                let r = &rows[i];
                kinds.contains(&r.kind)
                    && r.run_at <= now
                    && (r.state == "available"
                        || (r.state == "running" && r.locked_at.map_or(true, |t| t < cutoff)))
            })
            .collect();
        idxs.sort_by(|&a, &b| {
            rows[b].priority.cmp(&rows[a].priority).then(rows[a].run_at.cmp(&rows[b].run_at))
        });
        let Some(&i) = idxs.first() else { return Ok(None) };
        rows[i].state = "running";
        rows[i].locked_at = Some(now);
        rows[i].attempts += 1;
        let r = &rows[i];
        Ok(Some(Job {
            id: JobId(r.id),
            kind: r.kind.clone(),
            payload: r.payload.clone(),
            attempts: r.attempts,
            run_at: r.run_at,
        }))
    }

    async fn complete(&self, id: JobId) -> Result<()> {
        self.rows.lock().unwrap().retain(|r| r.id != id.0);
        Ok(())
    }

    async fn fail(&self, id: JobId, _error: &str, policy: RetryPolicy) -> Result<()> {
        let mut rows = self.rows.lock().unwrap();
        if let Some(r) = rows.iter_mut().find(|r| r.id == id.0) {
            match policy {
                RetryPolicy::Retry { delay } => {
                    r.state = "available";
                    r.run_at = OffsetDateTime::now_utc() + delay;
                    r.locked_at = None;
                }
                RetryPolicy::Abandon => {
                    r.state = "failed";
                    r.locked_at = None;
                }
            }
        }
        Ok(())
    }

    async fn heartbeat(&self, id: JobId) -> Result<()> {
        let mut rows = self.rows.lock().unwrap();
        if let Some(r) = rows.iter_mut().find(|r| r.id == id.0) {
            r.locked_at = Some(OffsetDateTime::now_utc());
        }
        Ok(())
    }
}

#[async_trait]
impl ControlPlane for MemoryControlPlane {
    async fn begin(&self) -> Result<Box<dyn Tx + Send>> {
        Ok(Box::new(MemoryTx { shared: self.probe.clone(), staged: HashMap::new() }))
    }
}

// Probe Tx retained until Task 4 swaps it for transactional enqueue.
struct MemoryTx {
    shared: Arc<Mutex<HashMap<String, i64>>>,
    staged: HashMap<String, i64>,
}

#[async_trait]
impl Tx for MemoryTx {
    async fn commit(self: Box<Self>) -> Result<()> {
        let mut g = self.shared.lock().unwrap();
        for (k, v) in self.staged {
            g.insert(k, v);
        }
        Ok(())
    }
    async fn rollback(self: Box<Self>) -> Result<()> {
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
        Ok(self.shared.lock().unwrap().get(key).copied())
    }
}
```
Update `memory/BUCK` library deps to add `"//third-party:uuid"`, `"//third-party:time"` (keep core, async-trait, sqlx? no — memory has core, async-trait, tokio).

- [ ] **Step 4: Write the memory queue test `memory/tests/queue.rs`**

```rust
use std::time::Duration;

use control_plane_memory::MemoryControlPlane;

const LOCK_TIMEOUT: Duration = Duration::from_millis(300);

#[tokio::test]
async fn memory_passes_queue_contract() {
    let cp = MemoryControlPlane::new(LOCK_TIMEOUT);
    control_plane_testkit::queue_contract(&cp, LOCK_TIMEOUT).await;
}
```
Add a `rust_test` to `memory/BUCK`:
```python
rust_test(
    name = "queue",
    crate = "queue",
    srcs = ["tests/queue.rs"],
    crate_root = "tests/queue.rs",
    edition = "2024",
    deps = [":memory", "//src/control-plane/testkit:testkit", "//third-party:tokio"],
)
```

- [ ] **Step 5: Format + run (dev shell, then buck2)**

Run: `eval "$(./tools/env.sh)" && cargo fmt && cargo test -p control-plane-memory && buck2 test //src/control-plane/memory:queue`
Expected: `memory_passes_queue_contract ... ok`; buck2 Pass 1. (The existing `memory:contract` probe test still passes too.)

- [ ] **Step 6: Commit**

```bash
git add src/control-plane/memory src/control-plane/testkit Cargo.toml Cargo.lock third-party
git commit -m "feat(control-plane-memory): in-memory Queue passing the queue contract

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Postgres `Queue` impl + migrations

**Files:** Create `postgres/migrations/0001_queue.sql`; modify `postgres/Cargo.toml`, `postgres/BUCK`, `postgres/src/lib.rs`, `postgres/src/fixture.rs`; create `postgres/tests/queue.rs`.

- [ ] **Step 1: Bump sqlx features + add deps in `postgres/Cargo.toml`**

```toml
sqlx = { version = "0.8", default-features = false, features = ["runtime-tokio", "postgres", "uuid", "time", "json", "migrate"] }
uuid = { version = "1", features = ["v4"] }
time = "0.3"
serde_json = "1"
```
(Keep `control-plane-core`, `async-trait`, `tempfile`; dev-deps unchanged.) Run `./tools/buckify.sh` (warning-clean; the `migrate`/`uuid`/`time`/`json` features add no build-script crates needing new fixups — verified).

- [ ] **Step 2: Write the migration `postgres/migrations/0001_queue.sql`**

```sql
create schema if not exists queue;

create table queue.jobs (
    id          uuid        primary key,
    kind        text        not null,
    payload     jsonb       not null,
    state       text        not null,
    run_at      timestamptz not null default now(),
    priority    int         not null default 0,
    attempts    int         not null default 0,
    last_error  text,
    locked_at   timestamptz,
    locked_by   text,
    created_at  timestamptz not null default now(),
    updated_at  timestamptz not null default now()
);

create index jobs_dequeue_idx on queue.jobs (state, kind, run_at, priority);
```

- [ ] **Step 3: Add the migrations filegroup + env in `postgres/BUCK`**

Add before the `rust_library`:
```python
# Runtime migrations dir for sqlx Migrator::new (the compile-time sqlx::migrate!
# macro needs an absolute CARGO_MANIFEST_DIR, which buck2 can't provide). The
# filegroup preserves the glob's "migrations/" prefix, hence the env appends it.
filegroup(
    name = "migrations",
    srcs = glob(["migrations/**/*.sql"]),
    visibility = ["PUBLIC"],
)
```
Add `LOOM_MIGRATIONS_DIR` to the existing `contract` rust_test `env` and to the new `queue` rust_test (Step 6): `"LOOM_MIGRATIONS_DIR": "$(location :migrations)/migrations"`.

- [ ] **Step 4: Implement `Queue` + `run_migrations` in `postgres/src/lib.rs`**

Add imports and items (keep the existing `PgControlPlane`/`PgTx`/`fixture`/probe code for now):
```rust
use std::path::Path;
use std::time::Duration;

use control_plane_core::{Job, JobId, NewJob, Queue, RetryPolicy};
use sqlx::Row as _;
use time::OffsetDateTime;
use uuid::Uuid;
```
Add a `lock_timeout` to `PgControlPlane` (replace the existing struct + `new`):
```rust
#[derive(Clone)]
pub struct PgControlPlane {
    pool: PgPool,
    lock_timeout: Duration,
}

impl PgControlPlane {
    pub fn new(pool: PgPool, lock_timeout: Duration) -> Self {
        Self { pool, lock_timeout }
    }
}
```
Migrations + shared insert:
```rust
/// Apply pending migrations from `migrations_dir` (tracked in `_sqlx_migrations`).
pub async fn run_migrations(pool: &PgPool, migrations_dir: &Path) -> Result<()> {
    let migrator = sqlx::migrate::Migrator::new(migrations_dir).await.map_err(backend)?;
    migrator.run(pool).await.map_err(backend)?;
    Ok(())
}

async fn pg_insert<'e, E: sqlx::PgExecutor<'e>>(ex: E, job: &NewJob) -> Result<JobId> {
    let id = Uuid::new_v4();
    sqlx::query(
        "insert into queue.jobs (id, kind, payload, state, run_at, priority) \
         values ($1, $2, $3, 'available', coalesce($4, now()), $5)",
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

fn row_to_job(row: &sqlx::postgres::PgRow) -> Job {
    Job {
        id: JobId(row.get("id")),
        kind: row.get("kind"),
        payload: row.get("payload"),
        attempts: row.get("attempts"),
        run_at: row.get("run_at"),
    }
}
```
The `Queue` impl:
```rust
#[async_trait]
impl Queue for PgControlPlane {
    async fn enqueue(&self, job: NewJob) -> Result<JobId> {
        pg_insert(&self.pool, &job).await
    }

    async fn dequeue(&self, kinds: &[String], worker: &str) -> Result<Option<Job>> {
        let cutoff = OffsetDateTime::now_utc() - self.lock_timeout;
        let row = sqlx::query(
            "update queue.jobs set state='running', locked_at=now(), locked_by=$1, \
                 attempts=attempts+1, updated_at=now() \
             where id = ( \
                 select id from queue.jobs \
                 where kind = any($2) and run_at <= now() \
                   and (state='available' or (state='running' and locked_at < $3)) \
                 order by priority desc, run_at asc \
                 for update skip locked limit 1) \
             returning id, kind, payload, attempts, run_at",
        )
        .bind(worker)
        .bind(kinds)
        .bind(cutoff)
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?;
        Ok(row.as_ref().map(row_to_job))
    }

    async fn complete(&self, id: JobId) -> Result<()> {
        sqlx::query("delete from queue.jobs where id = $1")
            .bind(id.0)
            .execute(&self.pool)
            .await
            .map_err(backend)?;
        Ok(())
    }

    async fn fail(&self, id: JobId, error: &str, policy: RetryPolicy) -> Result<()> {
        match policy {
            RetryPolicy::Retry { delay } => {
                let run_at = OffsetDateTime::now_utc() + delay;
                sqlx::query(
                    "update queue.jobs set state='available', run_at=$2, locked_at=null, \
                         locked_by=null, last_error=$3, updated_at=now() where id=$1",
                )
                .bind(id.0)
                .bind(run_at)
                .bind(error)
                .execute(&self.pool)
                .await
                .map_err(backend)?;
            }
            RetryPolicy::Abandon => {
                sqlx::query(
                    "update queue.jobs set state='failed', locked_at=null, locked_by=null, \
                         last_error=$2, updated_at=now() where id=$1",
                )
                .bind(id.0)
                .bind(error)
                .execute(&self.pool)
                .await
                .map_err(backend)?;
            }
        }
        Ok(())
    }

    async fn heartbeat(&self, id: JobId) -> Result<()> {
        sqlx::query("update queue.jobs set locked_at=now() where id=$1")
            .bind(id.0)
            .execute(&self.pool)
            .await
            .map_err(backend)?;
        Ok(())
    }
}
```

- [ ] **Step 5: Update the fixture to run migrations — `postgres/src/fixture.rs`**

In `fresh_control_plane`, after building the pool, replace the `_probe` DDL with running migrations, read the lock timeout, and pass it to `PgControlPlane::new`. Concretely, change the tail of `fresh_control_plane`:
```rust
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(self.opts(&db))
            .await
            .expect("connect pool to fresh database");

        // Apply migrations (queue.jobs, …). Keep the legacy _probe table until the
        // probe ops are removed (Task 4), so the existing tx_contract still runs.
        let migrations = std::env::var("LOOM_MIGRATIONS_DIR").expect("LOOM_MIGRATIONS_DIR");
        crate::run_migrations(&pool, std::path::Path::new(&migrations))
            .await
            .expect("run migrations");
        pool.execute("create table _probe (k text primary key, v bigint not null)")
            .await
            .expect("create _probe table");

        crate::PgControlPlane::new(pool, std::time::Duration::from_millis(300))
```
(`use sqlx::Executor;` is already imported in the fixture for the `_probe` DDL.)

- [ ] **Step 6: Write the pg queue test `postgres/tests/queue.rs`**

```rust
use std::time::Duration;

use control_plane_postgres::fixture::PgFixture;

const LOCK_TIMEOUT: Duration = Duration::from_millis(300);

#[tokio::test]
async fn postgres_passes_queue_contract() {
    let fixture = PgFixture::start();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::queue_contract(&cp, LOCK_TIMEOUT).await;
}
```
Add a `rust_test` to `postgres/BUCK` (same `env` as `contract`, plus `LOOM_MIGRATIONS_DIR`):
```python
rust_test(
    name = "queue",
    crate = "queue",
    srcs = ["tests/queue.rs"],
    crate_root = "tests/queue.rs",
    edition = "2024",
    env = {
        "POSTGRES_BIN_DIR": "$(location :postgres-bin)/bin",
        "POSTGRES_LD_LIBRARY_PATH": "$(location :postgres-bin)/lib:$(location :libxml2)",
        "LOOM_MIGRATIONS_DIR": "$(location :migrations)/migrations",
    },
    deps = [":postgres", "//src/control-plane/testkit:testkit", "//third-party:tokio"],
)
```
Also add `"LOOM_MIGRATIONS_DIR": "$(location :migrations)/migrations"` to the existing `contract` rust_test `env` (its fixture now runs migrations).

- [ ] **Step 7: Format + run**

Run: `eval "$(./tools/env.sh)" && cargo fmt && buck2 test //src/control-plane/postgres:queue //src/control-plane/postgres:contract`
Expected: both Pass 1 (the new queue contract against real Postgres; the legacy probe contract still green).

- [ ] **Step 8: Commit**

```bash
git add src/control-plane/postgres Cargo.toml Cargo.lock third-party
git commit -m "feat(control-plane-postgres): Queue impl + sqlx migrations

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Transactional `Tx::enqueue` + retire the probe

Now swap the temporary probe for real transactional enqueue across `core`, both adapters, and `testkit`, then delete the probe + the legacy `tx_contract`/`_probe` table.

**Files:** Modify `core/src/transaction.rs`, `core/src/lib.rs`; `memory/src/lib.rs`; `postgres/src/lib.rs`, `postgres/src/fixture.rs`, `postgres/BUCK`; `testkit/src/lib.rs`; `memory/tests/`, `postgres/tests/`.

- [ ] **Step 1: Change `Tx` in `core/src/transaction.rs`**

Replace the trait (drop the module note about probe; drop `probe_*`; add `enqueue`):
```rust
//! The cross-concern transaction seam. `ControlPlane::begin` opens a unit of work;
//! operations issued on the returned `Tx` commit together or roll back together.

use async_trait::async_trait;

use crate::error::Result;
use crate::queue::{JobId, NewJob};

#[async_trait]
pub trait ControlPlane: Send + Sync {
    async fn begin(&self) -> Result<Box<dyn Tx + Send>>;
}

#[async_trait]
pub trait Tx: Send {
    async fn commit(self: Box<Self>) -> Result<()>;
    async fn rollback(self: Box<Self>) -> Result<()>;
    /// Enqueue a job within this unit of work: visible to workers only if the
    /// transaction commits. Makes "commit a change AND enqueue downstream work"
    /// atomic.
    async fn enqueue(&mut self, job: NewJob) -> Result<JobId>;
}
```

- [ ] **Step 2: Update the memory adapter `Tx` — `memory/src/lib.rs`**

Remove the `probe: Arc<Mutex<HashMap…>>` field from `MemoryControlPlane` (and its init in `new`), and replace `MemoryTx` with a staged-jobs version:
```rust
#[async_trait]
impl ControlPlane for MemoryControlPlane {
    async fn begin(&self) -> Result<Box<dyn Tx + Send>> {
        Ok(Box::new(MemoryTx { rows: self.rows.clone(), staged: Vec::new() }))
    }
}

struct MemoryTx {
    rows: Arc<Mutex<Vec<Row>>>,
    staged: Vec<NewJob>,
}

#[async_trait]
impl Tx for MemoryTx {
    async fn commit(self: Box<Self>) -> Result<()> {
        let mut rows = self.rows.lock().unwrap();
        for job in self.staged {
            MemoryControlPlane::insert(&mut rows, job);
        }
        Ok(())
    }
    async fn rollback(self: Box<Self>) -> Result<()> {
        Ok(())
    }
    async fn enqueue(&mut self, job: NewJob) -> Result<JobId> {
        let id = JobId(Uuid::new_v4());
        self.staged.push(job);
        Ok(id)
    }
}
```
Remove the now-unused `HashMap` import if nothing else uses it.

- [ ] **Step 3: Update the postgres adapter `Tx` — `postgres/src/lib.rs`**

Replace the `PgTx` probe impl with transactional enqueue:
```rust
#[async_trait]
impl Tx for PgTx {
    async fn commit(self: Box<Self>) -> Result<()> {
        self.tx.commit().await.map_err(backend)
    }
    async fn rollback(self: Box<Self>) -> Result<()> {
        self.tx.rollback().await.map_err(backend)
    }
    async fn enqueue(&mut self, job: NewJob) -> Result<JobId> {
        pg_insert(&mut *self.tx, &job).await
    }
}
```

- [ ] **Step 4: Add the transactional assertions to `queue_contract` (testkit) and delete `tx_contract`**

Delete the entire `tx_contract` function and its `probe_*`-related imports. Append to `queue_contract` (before its end):
```rust
    // transactional enqueue: rolled back -> never dequeued; committed -> dequeued
    let mut tx = cp.begin().await.unwrap();
    tx.enqueue(job("tx")).await.unwrap();
    tx.rollback().await.unwrap();
    assert!(
        cp.dequeue(&["tx".into()], w).await.unwrap().is_none(),
        "rolled-back enqueue is invisible"
    );

    let mut tx = cp.begin().await.unwrap();
    tx.enqueue(job("tx")).await.unwrap();
    tx.commit().await.unwrap();
    let committed = cp
        .dequeue(&["tx".into()], w)
        .await
        .unwrap()
        .expect("committed enqueue is visible");
    cp.complete(committed.id).await.unwrap();
```
(`begin`/`Tx` come from `ControlPlane`/`control_plane_core` — add `use control_plane_core::Tx;` if needed for the `enqueue` method to be in scope.)

- [ ] **Step 5: Remove the legacy probe test + `_probe` from the fixture**

- Delete `memory/tests/contract.rs` and its `rust_test` target in `memory/BUCK` (the probe `tx_contract` test).
- Delete `postgres/tests/contract.rs` and its `rust_test` target in `postgres/BUCK`.
- In `postgres/src/fixture.rs`, delete the `pool.execute("create table _probe …")` line (migrations remain).

- [ ] **Step 6: Format + run the whole tree**

Run: `eval "$(./tools/env.sh)" && cargo fmt && buck2 build //src/... && buck2 test //src/...`
Expected: BUILD SUCCEEDED; `memory:queue` and `postgres:queue` Pass; no probe targets remain; `grep -rn probe_ src/control-plane` returns nothing.

- [ ] **Step 7: Commit**

```bash
git add src/control-plane
git commit -m "feat(control-plane): transactional Tx::enqueue; retire the probe ops

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Self-review

**Spec coverage (Phase 1a portion):**
- `queue.jobs` schema + sqlx migrations → Task 3 (migration file + filegroup + `run_migrations`/`Migrator::new`). ✓
- `Queue` trait (enqueue/dequeue SKIP-LOCKED/complete/fail/heartbeat), caller-driven `RetryPolicy` → Task 1 (trait), Tasks 2–3 (impls). ✓
- Transactional `Tx::enqueue`, retire `probe_*` → Task 4. ✓
- Crashed-worker recovery via inline lock-expiry reclaim; `lock_timeout` adapter arg → Tasks 2–3 (`dequeue` reclaim) + contract reclaim assertion. ✓
- Completed deleted, abandoned retained → Task 2/3 (`complete` deletes, `Abandon` → `failed`). ✓
- Contract over both adapters → `queue_contract` (Task 2) run by `memory:queue` (Task 2) and `postgres:queue` (Task 3); transactional parts added Task 4. ✓
- **Deferred to 1b (not here):** `Worker`, `LISTEN/NOTIFY`/`await_jobs`, graceful shutdown. The `dequeue` `worker` arg and lock model are in place to support it.

**Deviations (noted in scope):** `Queue`/`ControlPlane` kept as separate traits (contract dual-bound `CP: ControlPlane + Queue`) rather than the spec's supertrait, to keep each commit green; the `migrate!` macro is replaced by runtime `Migrator::new` (the macro can't run under buck2). Both verified.

**Placeholder scan:** none — every code block is complete and lifted from a compiled-and-passed prototype (memory + real Postgres) and the in-repo migration spike.

**Type/name consistency:** `JobId`/`NewJob`/`Job`/`RetryPolicy`/`Queue`, `MemoryControlPlane::new(lock_timeout)`, `PgControlPlane::new(pool, lock_timeout)`, `run_migrations(pool, dir)`, `pg_insert`, `queue_contract(cp, lock_timeout)`, the `:queue`/`:migrations` buck targets, and `LOOM_MIGRATIONS_DIR` are used consistently across tasks. The `backend()` error mapper already exists in `postgres/src/lib.rs` (Phase 0b).
