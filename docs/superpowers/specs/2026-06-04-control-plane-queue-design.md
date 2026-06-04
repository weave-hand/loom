# Design: Control-Plane Queue (Phase 1)

> **Status:** approved design for the control plane's first real concern — the job
> queue. Sits under the umbrella roadmap (`2026-06-03-control-plane-roadmap-design.md`).
> Built in two cycles (1a, 1b); each gets its own implementation plan + PR.

## Goal

A Postgres-backed job queue (graphile_worker-shaped: `SELECT … FOR UPDATE SKIP
LOCKED` + `LISTEN`/`NOTIFY`), exposed through the control-plane traits and
satisfied by both the in-memory fake and the Postgres adapter via one contract
suite. This is the first concern with **real operations on `Tx`**, so it retires
the temporary `probe_*` scaffolding.

It delivers the architecture's headline property: **enqueue downstream work in the
same transaction as the change that produced it** (a snapshot commit and the job
it triggers are atomic — no lost work, no orphan jobs).

## Scope & staging

Built in two cycles, designed together here:

- **Phase 1a — Queue core:** `queue.jobs` schema + SQL migrations, the `Queue`
  trait ops, **transactional `enqueue`** on `Tx` (retires `probe_*`),
  `RetryPolicy`, contract tests over both adapters.
- **Phase 1b — Worker:** a generic `Worker` loop (dequeue → handle →
  complete/fail), `LISTEN`/`NOTIFY` low-latency wakeups + polling fallback,
  graceful shutdown; integration-tested against the hermetic pg fixture.

## Migrations (new in this phase)

Until now the pg fixture created its `_probe` table ad-hoc. Phase 1a introduces
**real SQL migrations**: the postgres adapter ships versioned `.sql` files under
`src/control-plane/postgres/migrations/` (e.g. `0001_queue.sql`), applied with
`sqlx::migrate!` at deployment and in the fixture's per-test database. The
in-memory fake needs no migrations. (Still no `query!` macros / `.sqlx` offline
metadata — runtime query API only, as established in Phase 0b.)

The `_probe` table and its fixture DDL are removed in 1a, replaced by the
`queue` migration.

## Schema: `queue.jobs`

```
queue.jobs
  id          uuid         primary key            -- JobId
  kind        text         not null               -- task identifier; workers filter on it
  payload     jsonb        not null               -- opaque job args (typed envelope, like lineage)
  state       text         not null               -- 'available' | 'running' | 'failed' (terminal)
  run_at      timestamptz  not null default now() -- eligibility time (delayed jobs, retry backoff)
  priority    int          not null default 0      -- higher dequeued first
  attempts    int          not null default 0      -- incremented on each dequeue; callers read it for backoff
  last_error  text                                 -- message from the most recent failure
  locked_at   timestamptz                          -- visibility lock; NULL when not running
  locked_by   text                                 -- worker id currently holding the job
  created_at  timestamptz  not null default now()
  updated_at  timestamptz  not null default now()
```

Index supporting dequeue: `(state, kind, run_at, priority)`. Lifecycle:
- **complete** → row **deleted** (keeps the table lean; graphile-style).
- **fail + Abandon** → `state='failed'`, retained for inspection.
- **fail + Retry** → `state='available'`, `run_at = now() + delay`, lock cleared.

`max_attempts` is intentionally **not** a column: retry is caller-driven, so the
queue enforces no attempt ceiling. `attempts` is tracked for observability and so
callers can compute their own backoff.

## Trait surface (`core`)

Domain types and `RetryPolicy` live in `core`; all ops `async`, returning
`Result<_, ControlPlaneError>`.

```rust
pub struct JobId(pub Uuid);

pub struct NewJob {
    pub kind: String,
    pub payload: serde_json::Value,
    pub run_at: Option<OffsetDateTime>, // None => now (eligible immediately)
    pub priority: i32,                  // default 0
}

pub struct Job {
    pub id: JobId,
    pub kind: String,
    pub payload: serde_json::Value,
    pub attempts: i32,        // this dequeue counts; callers use it for backoff
    pub run_at: OffsetDateTime,
}

pub enum RetryPolicy {
    Retry { delay: Duration }, // reschedule run_at = now + delay
    Abandon,                   // terminal 'failed' state, retained
}

#[async_trait]
pub trait Queue {
    async fn enqueue(&self, job: NewJob) -> Result<JobId>;
    /// Claim the next eligible job for one of `kinds`, marking it running and
    /// stamping `locked_by = worker`. Uses SELECT … FOR UPDATE SKIP LOCKED and
    /// reclaims jobs whose lock has expired (see crashed-worker recovery).
    async fn dequeue(&self, kinds: &[String], worker: &str) -> Result<Option<Job>>;
    async fn complete(&self, id: JobId) -> Result<()>;
    async fn fail(&self, id: JobId, error: &str, policy: RetryPolicy) -> Result<()>;
    /// Refresh `locked_at` so a long-running job isn't reclaimed.
    async fn heartbeat(&self, id: JobId) -> Result<()>;
}
```

### Transactional enqueue (retires the probe)

`Tx` gains a real operation — `enqueue` — and loses `probe_put`/`probe_get`:

```rust
#[async_trait]
pub trait Tx: Send {
    async fn commit(self: Box<Self>) -> Result<()>;
    async fn rollback(self: Box<Self>) -> Result<()>;
    /// Enqueue a job within this unit of work: it becomes visible to workers only
    /// if the transaction commits. This is how a snapshot commit and the job it
    /// triggers are made atomic.
    async fn enqueue(&mut self, job: NewJob) -> Result<JobId>;
}
```

`ControlPlane::enqueue` is the autocommit convenience (its own transaction);
`Tx::enqueue` is the transactional form. `dequeue`/`complete`/`fail`/`heartbeat`
are worker operations that manage their own transactions (not part of a caller's
`Tx`).

`Tx` stays flat (one `enqueue` method) for now. When a second `Tx`-participating
concern lands (lineage, P5), revisit whether to refactor to per-concern scoped
handles (`tx.queue()…`, `tx.lineage()…`).

## Crashed-worker recovery (resolves the queue open question)

Retry is caller-driven, but a worker that **crashes mid-job never calls `fail`** —
so recovery cannot depend on the caller. `dequeue` therefore claims a job that is
either `available`, or `running` with an **expired lock** (`locked_at < now() -
lock_timeout`), all in one `UPDATE … WHERE id IN (SELECT … FOR UPDATE SKIP LOCKED
LIMIT 1)`. `heartbeat` refreshes `locked_at` for legitimately long jobs.

**This inline reclaim is the stuck-job reaper** — no separate reaper daemon. The
roadmap open question ("LISTEN/NOTIFY + polling fallback, stuck-job reaper") is
resolved as: NOTIFY for latency + polling fallback (1b), inline lock-expiry for
reclaim (1a). `lock_timeout` is a configurable adapter setting (default e.g. 60s).

## Worker (Phase 1b)

A generic **`control-plane-worker`** crate (keeps `core` runtime-free; depends on
`core` + `tokio`):

```rust
pub struct JobFailure { pub error: String, pub policy: RetryPolicy }

Worker::new(queue, worker_id)
    .run(&kinds, shutdown_token, |job: Job| async move {
        // handler: Result<(), JobFailure>
    })
    .await
```

Loop, per iteration:
1. `dequeue(kinds, worker_id)`; if a job, run the handler:
   - `Ok(())` → `complete(id)`.
   - `Err(JobFailure { error, policy })` → `fail(id, &error, policy)` — the handler
     drives retry (caller-driven).
2. If no job, `await_jobs(kinds, timeout)` then loop.

Wakeups come from a trait method:

```rust
#[async_trait]
pub trait Queue {
    // … ops above …
    /// Block until a job of one of `kinds` may be available, or `timeout` elapses.
    async fn await_jobs(&self, kinds: &[String], timeout: Duration) -> Result<()>;
}
```

- **Postgres:** `enqueue` issues `pg_notify('loom_queue:<kind>', '')`; `await_jobs`
  `LISTEN`s and waits for a notification or `timeout` (polling fallback covers
  missed/extra notifications and reclaimed expired locks).
- **In-memory fake:** a `tokio::sync::Notify` triggered on `enqueue`, with the same
  `timeout` fallback.

Graceful shutdown: a `tokio_util::sync::CancellationToken`; the loop finishes the
in-flight job (or relies on lock expiry if killed) and exits.

## Testing

- **Queue contract suite** (`testkit`, run against both adapters):
  - enqueue → dequeue honours `run_at` (delayed jobs not returned early) and
    `priority` ordering;
  - two concurrent `dequeue`s never claim the same job (SKIP LOCKED);
  - `complete` removes the job; a second `dequeue` returns nothing;
  - `fail` + `Retry { delay }` reschedules (`run_at` advanced, dequeuable after
    the delay); `attempts` incremented;
  - `fail` + `Abandon` → terminal `failed`, never dequeued again;
  - `heartbeat` keeps a job from being reclaimed; an **expired lock is reclaimed**
    by a later `dequeue`;
  - **transactional**: a job enqueued in a `Tx` that `rollback`s is never
    dequeued; one in a committed `Tx` is.
- **Worker integration** (1b, pg fixture): enqueue N jobs, run a worker with a
  counting handler, assert all complete; a handler that fails then succeeds
  retries per its `RetryPolicy`; an always-failing handler ends `Abandon`ed;
  `LISTEN/NOTIFY` delivers a job without waiting out the poll timeout. The fake
  gets an equivalent worker test (using its `Notify`-based `await_jobs`).

## Non-goals (this phase)

- Cron/scheduled-recurring jobs (graphile's crontab) — only one-shot + delayed
  (`run_at`) jobs.
- Job dependencies / DAGs, batches, or job-result storage.
- A worker *binary* — `Worker` is a library the services embed; no standalone
  daemon process.
- `query!` macros / `.sqlx` offline metadata (runtime API continues).
- Multiple named queues beyond the `kind` discriminator.
