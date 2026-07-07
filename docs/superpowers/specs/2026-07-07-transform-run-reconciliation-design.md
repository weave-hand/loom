# Transform run reconciliation — design

**Item:** `#iss-transform-run-stuck-running`

## Problem

A transform run's failure reporting is best-effort. When a job fails terminally
(abandon) the worker records the outcome against the run over the engine wire,
but that report is deliberately non-masking: `report_run_failure`
(`src/services/worker/src/transform.rs:197-209`) warns and swallows any error
from the `finish_run_failed` RPC (`transform.rs:206-207`) so a reporting failure
never overturns the queue's own retry/abandon decision. The run record is
updated by a *separate* call from the one that abandons the queue job, and the
two are not atomic.

The consequence is asymmetric across the two failure policies:

- **Retryable failure self-heals.** `queue.fail(Retry)` returns the job to
  `state='available'` (`src/control-plane/postgres/src/queue.rs:118`). If the
  `finish_run` → `RetryQueued` report is lost, the run stays `running`, but the
  job is still live and gets re-dequeued; the retry's `mark_run_running`
  (`transform.rs:45`, `:99`) re-stamps the run and it eventually resolves.
- **Terminal failure strands.** `queue.fail(Abandon)` moves the job to the
  terminal `state='failed'` (`queue.rs:130`) and it is never dequeued again. If
  the `finish_run_failed(terminal=true)` RPC then fails, nothing will ever
  re-touch the run: it reads `Running` (`RunState::Running`,
  `src/control-plane/core/src/transforms.rs:132-137`) forever, while execution
  has terminally ended. There is no live queue job to heal it.

So exactly one moment — a lost terminal `FinishRunFailed` — permanently strands
a run as `Running`.

## Scope

- A **reconciliation sweep**: a background pass that finds runs reading
  `Running` whose queue job is no longer live, and marks each
  `Failed("reporting lost")`. One new `Transforms` trait method backs it; the
  sweep loop lives in the engine service (the sole Postgres owner), mirroring
  the existing scheduler loop.
- The **worker cleanup** flagged by the final review of the originating spec
  (`docs/superpowers/specs/2026-07-04-transform-ergonomics-design.md`): dedup
  the ~10-line `mark_run_running` block duplicated across the two handlers, drop
  `handle_typed_transform_inner`'s redundant `run_id` parameter, and refresh
  `handle_typed_transform`'s doc comment.

**Non-goals (explicit):**

- **Not** the synchronous-with-retry alternative — the worker's abandon-side
  reporting stays best-effort/non-masking exactly as today; the sweep is the
  chosen safety net, not a change to how the worker reports.
- No change to the happy-path lifecycle (`mark_run_running` →
  `CommitTransform{run_id}` succeeded-in-commit-tx → `finish_run`).
- No new job kinds and no change to the queue's own crashed-worker reclaim
  (expired-lock re-dequeue, `queue.rs:83`).
- No run cancellation, retention, or GC (those stay in `#fut-transform-followups`).

## Design

### 1. The reconciliation sweep

**Liveness signal (the crux).** A run is *stranded* iff it reads `Running` and
its queue job is **not live** — where "live" means present in a non-terminal
queue state, i.e. `state IN ('available','running')`. The mapping to
`queue.jobs` is via the run id embedded in the job payload: a run's job carries
`payload->>'run_id'` (`TransformJob`/`TypedTransformJob` both serialize
`run_id`; enqueued by `body.to_job(run_id)` in `submit_run`). The three
outcomes:

- job `state='running'` (locked, possibly with an *expired* lock awaiting
  reclaim) → **live, never swept**. This is the actively-executing and the
  crashed-worker-mid-flight case; the queue will reclaim and retry it.
- job `state='available'` → **live, never swept**. This is the retry case that
  already self-heals.
- job `state='failed'` (terminal abandon) **or no matching job row** (a
  `complete`d job is deleted, `queue.rs:104-110`) → **not live**. If the run is
  still `Running`, it is stranded.

The check must consult both the eligible/`available` set *and* the
in-flight/`running` set — an absence from `available` alone would wrongly sweep
a mid-execution run.

**Grace guard against the terminal-report race.** The normal terminal path is
two steps: `queue.fail(Abandon)` (job → `failed`) then `finish_run_failed` (run
→ `failed`). Between them a run momentarily reads `Running` with a `failed` job
— indistinguishable from a stranded run. Sweeping it there is *harmless in
state* (both land `Failed`) but would race the worker's own write and clobber
the real error text with `"reporting lost"`. To avoid that, the sweep only
considers runs whose `started_at` (stamped by `mark_run_running`,
`postgres/src/transforms.rs:407`) is older than a grace interval, passed by the
caller as a cutoff timestamp. The grace is comfortably longer than the
worker's two-RPC window, short relative to how long a genuine strand persists.

**Transition.** Each swept run goes to `Failed` with error `"reporting lost"` —
the same terminal write `finish_run(Failed)` performs (`state='failed'`,
`finished_at=now()`, `postgres/src/transforms.rs:434-442`). Idempotent: a run
already `Failed`/`Succeeded`/`Queued` is not matched (the predicate requires
`state='running'`), so a re-run of the sweep, or a lost report that later
arrives, is a no-op. `snapshot_id` stays null (no data was committed).

**New trait method** on `Transforms` (`core/src/transforms.rs`, both adapters +
testkit contract):

```rust
/// Sweep runs stuck `Running` whose queue job is no longer live (neither
/// `available` nor in-flight `running`) and whose `started_at` predates
/// `running_since_before`: mark each `Failed("reporting lost")`. Returns the
/// swept run ids. Runs with a live job are left untouched — they self-heal on
/// retry or crashed-worker reclaim.
async fn reconcile_stranded_runs(
    &self,
    running_since_before: OffsetDateTime,
) -> Result<Vec<Uuid>>;
```

- **Postgres**: a single `UPDATE transforms.run SET state='failed',
  error='reporting lost', finished_at=now() WHERE state='running' AND started_at
  <= $1 AND NOT EXISTS (SELECT 1 FROM queue.jobs j WHERE j.state IN
  ('available','running') AND (j.payload->>'run_id')::uuid = run_id) RETURNING
  run_id`. Both schemas share one database, so the join is a plain cross-schema
  subquery — no new linkage column. `.sqlx` refreshed via
  `tools/sqlx-prepare.sh`.
- **Memory**: iterate the runs map for `Running` runs older than the cutoff;
  for each, scan the queue `Row`s (payload `run_id`, `state` — both already on
  `Row`, `memory/src/lib.rs:40-41`) for a live match; strand the rest. Mirrors
  the postgres predicate exactly for contract parity.

**Where it runs.** A `reconcile_loop` in the engine service
(`src/services/engine/src/scheduler.rs` or a sibling module), structured like
`scheduler_loop` (`scheduler.rs:53-67`): a `tokio::time::interval` gated by the
`CancellationToken`, calling `reconcile_stranded_runs(now - grace)` each tick
and info-logging any swept ids. The worker cannot host it (zero-pool, no
Postgres). Cadence is slow (minutes) — stranding is rare and non-urgent; the
grace makes a fast tick pointless. Wired into engine startup alongside the
scheduler loop.

### 2. Worker cleanup refactor

- **Dedup `mark_run_running`.** The identical ~10-line
  `if let Some(rid) = run_id { … mark_run_running … retry }` block appears in
  both `handle_transform` (`transform.rs:42-51`) and `handle_typed_transform`
  (`transform.rs:96-105`). Extract a private helper
  `async fn mark_running_if_tracked(ctx, run_id, attempts) -> Result<(), JobFailure>`
  and call it from both.
- **Drop the redundant param.** `handle_typed_transform` already reads
  `parsed.run_id` (`transform.rs:95`) and then threads `run_id` into
  `handle_typed_transform_inner` (`transform.rs:106`, `:111-116`), which also
  receives `parsed: &TypedTransformJob` carrying the same field. Remove the
  `run_id` parameter; inner reads `parsed.run_id` at its two use sites
  (lineage `:151`, `WireTransform.run_id` `:175`).
- **Refresh the doc comment.** Update `handle_typed_transform`'s doc to describe
  the extracted mark-running helper (the `handle_transform` doc at
  `transform.rs:33-36` is the reference wording).

No behavior change — this is a pure readability fold-in that the reconciliation
tests and existing worker e2e's already exercise.

## Testing

- **Testkit contract** (`transforms` concern, memory + postgres) for
  `reconcile_stranded_runs`:
  - *Stranded → Failed*: create a run, `mark_run_running`, abandon its queue job
    (`queue.fail(Abandon)`) **without** finishing the run; with a zero/past
    cutoff, the sweep returns that run id and `get_run` reads
    `Failed("reporting lost")`, `finished_at` set.
  - *Live run untouched (in-flight)*: a `Running` run whose job is `running`
    (dequeued, not failed) is **not** swept.
  - *Live run untouched (retry)*: a `Running` run whose job is back to
    `available` is **not** swept (self-heal case).
  - *Grace guard*: a freshly `Running` run (`started_at` after the cutoff) with
    a failed job is **not** swept; sweeping with a later cutoff catches it.
  - *Idempotent*: a second sweep, and a run already `Failed`/`Succeeded`, are
    no-ops.
- **Postgres**: fixture suite covers the cross-schema `NOT EXISTS` query via the
  contract; `.sqlx` cache refreshed.
- **Engine**: a `tick`-style unit test over the memory adapter (mirroring the
  scheduler test) — a stranded run is reconciled once, a live one left alone.
- **Worker refactor**: covered by the existing transform worker e2e's
  (Queued→Running→Succeeded, and failing-transform→Failed) plus the existing
  handler tests — the extraction and param drop are behavior-preserving, so a
  green sweep is the proof.
