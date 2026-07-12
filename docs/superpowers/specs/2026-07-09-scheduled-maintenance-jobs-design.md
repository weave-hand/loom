# Scheduled maintenance jobs Design

> **Status:** design (direction). This spec makes `road-scheduled-maintenance-jobs`
> (the promotion of `fut-scheduled-jobs`) build-ready. A separate work agent
> writes the implementation plan from it and builds it.

## Goal

Cron scheduling for **maintenance job kinds** — `gc_table` and `compact_table`
today — with **per-table granularity**, managed over the admin HTTP surface. An
operator defines "GC `main.orders` nightly at 03:00" as a named schedule row;
the engine's existing scheduler loop fires it on tick, enqueueing the same
deduplicated queue job the manual `POST /maintenance/gc/{schema}/{table}` and
`POST /tables/{schema}/{table}/compact` endpoints enqueue today; the zero-pool
worker drains it unchanged. This is the residual of `road-transform-schedules`
(PR #367): the croner/`next_run_at`/atomic-claim mechanism, generalized from
transform definitions to arbitrary `(kind, payload)` jobs.

## Decision record (operator, 2026-07-09 — do not relitigate)

**Generalize the schedule mechanism, not env knobs.** The landed
transform-schedule mechanism (croner validation, derived `next_run_at`, atomic
claim-and-advance) is generalized to arbitrary job kinds: a schedule row carries
`kind` + `payload` (e.g. `gc_table` for table X), managed via an admin HTTP
endpoint. Per-table granularity, one scheduling mechanism — **not** global
`LOOM_GC_CRON`-style env knobs.

## Context — what ships today (verified refs)

The transform-schedules slice (`road-transform-schedules`, PR #367; plan
`docs/superpowers/plans/2026-07-04-transform-schedules.md`) landed the whole
mechanism, but welded to `TransformDef`:

- **Cron helpers in core** — `validate_cron` (`src/control-plane/core/src/transforms.rs:230`)
  and `next_cron_occurrence` (`transforms.rs:242`), croner-backed, 5-field UTC,
  strictly-after semantics; both re-exported at the core crate root.
- **Derived `next_run_at`** — a column on `transforms.transform` (migration
  `src/control-plane/postgres/migrations/0032_transform_schedule.sql`, partial
  index `transform_due`), computed at define time
  (`src/control-plane/postgres/src/transforms.rs:301`) and re-advanced at claim
  time. Not part of the authored `TransformDef` shape.
- **Atomic claim** — `Transforms::claim_due_schedules(now, limit)`
  (`postgres/src/transforms.rs:574`): one transaction, `SELECT … FOR UPDATE
  SKIP LOCKED` (`transforms.rs:585`) over due rows, per-row `UPDATE … set
  next_run_at` advance (`transforms.rs:627`), commit. Concurrent claimers never
  double-fire. Memory twin under the `transforms` lock
  (`src/control-plane/memory/src/transforms.rs:213`).
- **Engine tick loop** — `engine::scheduler::tick`
  (`src/services/engine/src/scheduler.rs:14`) claims due defs and calls
  `submit_run` per claim; `scheduler_loop` (`scheduler.rs:53`) ticks it every
  `EngineTuning.scheduler_tick` (`LOOM_SCHEDULER_TICK_SECS`, default 5;
  `src/services/engine/src/run.rs:46`, parsed `run.rs:75`), claim limit 32
  per tick (`scheduler.rs:60`); spawned in `run.rs:148` with a
  `CancellationToken`.
- **Admin surface** — `/admin/transforms` CRUD in
  `src/services/runtime/src/admin.rs` (define `:1378`, list `:1405`, get
  `:1427`, delete `:1452`; router `:1614`), gated by `require_admin`
  (`admin.rs:44`, the reserved `admin` role, fail-closed), documented by
  `admin_openapi()` (`admin.rs:1705`) which query-api merges
  (`src/services/query-api/src/openapi.rs:251`) and drift-guards
  (`src/services/query-api/tests/openapi.rs` `expected()`).

The maintenance jobs to schedule already exist end to end — only their
*triggering* is manual:

- **`gc_table`** — kind + payload contract in `src/control-plane/core/src/gc.rs:6`
  (`GcJob { schema, name }`); enqueued by query-api
  `POST /maintenance/gc/{schema}/{table}` (`src/services/query-api/src/http.rs:103`
  route, `:424` handler) via a plain `queue().enqueue` — **no dedup, no
  existence check**; drained by `worker::handler::handle_gc`
  (`src/services/worker/src/handler.rs:45`) → engine `GcTable` RPC.
- **`compact_table`** — contract in `src/control-plane/core/src/compact_job.rs:6`
  (`CompactJob { schema, name }`); enqueued by ingest
  `POST /tables/{schema}/{table}/compact` (`src/services/ingest/src/http.rs:123`
  route, `:142` handler), same plain enqueue; drained by
  `worker::compact::handle_compact` (`src/services/worker/src/compact.rs:23`).
- **Worker dispatch** — both kinds are in the zero-pool worker's kind list and
  dispatch match (`src/services/worker/src/main.rs:81`). Payload parse failure
  ⇒ `Abandon`, RPC failure ⇒ `Retry` with backoff (`run_wire_job`,
  `handler.rs:14`).
- **Dedup exists but is not on the operator path** — `pg_insert_if_absent`
  (`src/control-plane/postgres/src/queue.rs:37`) inserts only if no
  `state = 'available'` job with the same `(kind, payload)` exists; today it
  serves only the commit-path auto-triggers (`iceberg_inline.rs:743`,
  `iceberg_landing.rs:1186`, `commit_mirror.rs:90`).

## The design call: sibling table, shared mechanism

The grounding question was whether `transforms.transform` generalizes in place
(widen with `kind` + `payload jsonb`) or a sibling generic table serves better.
**The code says sibling table.** Four reasons, from the actual schema and claim
SQL:

1. **The transform schedule is a field of the authored def, not a row of its
   own.** `schedule` is a column on `transforms.transform` (migration 0031) and
   a field of `TransformDef` (`core/src/transforms.rs:111`); `next_run_at` is
   derived state beside it. There is no standalone schedule row to widen — an
   in-place generalization means either stuffing GC/compaction rows into a
   table whose `body jsonb` is a `TransformBody` and whose define path runs
   transform-specific validation (trigger-cycle scan, ontology-type checks,
   `postgres/src/transforms.rs:311-358`), or a rename/split migration that
   rewrites every `transforms.transform` query and its committed `.sqlx` cache.
2. **The claim's return type is transform-shaped.** `claim_due_schedules`
   returns `Vec<TransformDef>` and its consumer (`scheduler::tick`) builds a
   `TransformRun` ledger row and calls `submit_run`. Maintenance firings have
   no def body and no run ledger — a widened claim would need a polymorphic
   return rippling through the trait, both adapters, the testkit
   `transforms_contract`, the engine tick, and `TransformDefView`.
3. **The fire action is different — and a sibling on the queue concern makes it
   *stronger*.** A transform firing is claim → `submit_run` (two steps;
   documented at-most-once: a crash between them skips the occurrence). A
   maintenance firing is just a queue insert — and the queue concern owns both
   the schedule row and `queue.jobs` in the same database, so the claim-advance
   and the deduped enqueue can share **one transaction**: exactly-once per
   firing (never skipped, never double-fired), strictly better than the
   transform semantics, and impossible to get by bolting maintenance kinds onto
   the transforms claim.
4. **Additive beats destructive on the `.sqlx` cache.** A sibling table is a
   new migration plus new query files; landed transform queries stay
   byte-identical. An in-place widen invalidates the committed cache for every
   transforms query and reddens `sqlx-cache-check` breadth for no benefit.

"One scheduling mechanism" is satisfied at the mechanism layer, which is what
the operator decision names: the **same** `validate_cron`/`next_cron_occurrence`
core helpers, the **same** derived-`next_run_at` + due-index +
`FOR UPDATE SKIP LOCKED` claim shape, the **same** engine scheduler loop and
`LOOM_SCHEDULER_TICK_SECS` cadence, the **same** admin-gate + utoipa surface
pattern. Transform schedules are untouched.

## Schema

New migration `src/control-plane/postgres/migrations/0043_job_schedules.sql`
(next free slot — `0042_mv_watermark.sql` landed in PR #418), in the existing
`queue` schema — schedules are queue feeders, owned by the queue concern:

```sql
-- Scheduled maintenance jobs: named cron rows that enqueue (kind, payload)
-- queue jobs on the engine scheduler's tick. next_run_at is derived state
-- (computed from cron at define/fire time); the index serves the due scan.
create table queue.schedule (
    name        text primary key,
    kind        text not null,
    payload     jsonb not null,
    cron        text not null,
    next_run_at timestamptz not null
);

create index schedule_due on queue.schedule (next_run_at);
```

Unlike `transforms.transform` (where `schedule` is optional and `next_run_at`
nullable), both are `not null` here: a schedule row *is* a schedule, so the
index needs no partial predicate.

## Core domain (control-plane/core)

New module `src/control-plane/core/src/job_schedule.rs` (mirrors
`gc.rs`/`compact_job.rs` — the shared producer/consumer contract):

```rust
/// A named cron schedule that enqueues a `(kind, payload)` queue job on each
/// firing. The maintenance-cadence complement to transform schedules.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct JobSchedule {
    pub name: String,
    pub kind: String,
    pub payload: serde_json::Value,
    /// 5-field UTC cron (croner; same dialect as `TransformDef.schedule`).
    pub cron: String,
}

/// Job kinds an operator may put on a schedule. Deliberately narrower than
/// `KNOWN_JOB_KINDS`: transform kinds must go through `submit_run` (run
/// ledger); flush/consolidate/vector-index payloads are engine-computed.
pub const SCHEDULABLE_JOB_KINDS: &[&str] = &[crate::GC_JOB_KIND, crate::COMPACT_JOB_KIND];

/// Validate the authored shape: non-empty name, valid cron (`validate_cron`),
/// schedulable kind, and a payload that decodes as the kind's typed contract
/// (`GcJob` / `CompactJob`). Everything else is a `Validation` error -> 400.
pub fn validate_job_schedule(s: &JobSchedule) -> Result<()>;
```

The `Queue` trait (`src/control-plane/core/src/queue.rs:89`) grows the schedule
surface (schedules feed the queue; `kind`/`payload` are `NewJob` vocabulary):

```rust
/// A schedule plus its derived next fire time.
pub struct JobScheduleStatus { pub schedule: JobSchedule, pub next_run_at: OffsetDateTime }
/// One firing's outcome: `job` is `None` when deduplicated against a
/// still-`available` identical `(kind, payload)` job.
pub struct ScheduleFired { pub name: String, pub job: Option<JobId> }

// on trait Queue:
/// Validated upsert. Computes `next_run_at` from `now`; a redefine resets the
/// schedule clock (same refinement as transform schedules).
async fn define_job_schedule(&self, s: JobSchedule) -> Result<()>;
/// All schedules with their derived next fire, name-ordered.
async fn list_job_schedules(&self) -> Result<Vec<JobScheduleStatus>>;
/// `NotFound` for an unknown name.
async fn delete_job_schedule(&self, name: &str) -> Result<()>;
/// Atomically fire every due schedule (`next_run_at <= now`, at most `limit`):
/// advance `next_run_at` past `now` AND enqueue the deduped job in the SAME
/// unit of work — exactly-once per firing. Concurrent callers never both fire
/// the same schedule. Dedup: an identical `(kind, payload)` job still
/// `available` suppresses the insert (the schedule still advances).
async fn fire_due_job_schedules(&self, now: OffsetDateTime, limit: u32) -> Result<Vec<ScheduleFired>>;
```

`fire_due_job_schedules` is deliberately one method rather than the transforms'
claim-then-submit pair: the enqueue target is the same concern, so postgres
runs `SELECT … FOR UPDATE SKIP LOCKED` (the `claim_due_schedules` shape,
`postgres/src/transforms.rs:585`) + per-row advance + `pg_insert_if_absent`
(`postgres/src/queue.rs:37`, already generic over `PgExecutor`, notify buffered
until commit) **in one transaction**. Memory mirrors it under its locks
(schedule map + the `rows` job vec, `memory/src/lib.rs:66`), reusing the
available-state dedup scan the debounce enqueue already does
(`memory/src/transaction.rs:294`).

## Engine scheduler

`engine::scheduler` gains a maintenance tick beside the transform tick:

```rust
/// One maintenance pass: fire due job schedules. Returns fired count
/// (dedup-suppressed firings included in the log, not the count).
pub async fn maintenance_tick(cp: &dyn ControlPlane, now: OffsetDateTime, limit: u32) -> usize;
```

`scheduler_loop` (`scheduler.rs:53`) calls both ticks per interval — one loop,
one `LOOM_SCHEDULER_TICK_SECS` cadence, no new knob, no `run.rs` change (the
loop is already spawned, `run.rs:148`). Errors are logged and never kill the
loop, matching `tick`'s posture.

## Admin API

Three routes in `src/services/runtime/src/admin.rs`, mounted on the existing
`admin_routes` router (`admin.rs:1581`) behind the same `require_admin` gate
(`admin.rs:44`), registered in `admin_openapi()`'s `paths(...)`/`components(...)`
(`admin.rs:1705`) and added to query-api's drift guard
(`src/services/query-api/tests/openapi.rs` `expected()`):

- **`POST /admin/schedules`** — body `{"name", "kind", "payload", "cron"}`
  (a `JobSchedule`, open-body pattern like `define_transform_route`,
  `admin.rs:1378`). Upsert ⇒ 201. 400 on: undecodable body, invalid cron,
  non-schedulable kind, payload that fails the kind's typed decode, or a
  payload naming a table absent from the mirror (checked in the handler via
  `Catalog::current_snapshot`, `src/control-plane/core/src/catalog.rs:71` —
  `NotFound` ⇒ 400 naming the table).
- **`GET /admin/schedules`** — `{"schedules": [{name, kind, payload, cron,
  next_run_at}]}`, `next_run_at` RFC3339 UTC (the `rfc3339` helper,
  `admin.rs:1288`), name-ordered.
- **`DELETE /admin/schedules/{name}`** — 204; 404 unknown.

Transform schedules stay on `/admin/transforms` — the surfaces are siblings,
not merged.

## Validation & ACL judgment calls (recorded)

- **Define-time table existence: checked, in the admin handler.** Today's
  manual endpoints enqueue blind (no existence check; the worker's RPC failure
  retries with backoff). A schedule is long-lived, so a typo'd table would
  fail *forever* on cadence — worth the cheap `current_snapshot` probe at
  define. It lives in the handler (which holds the full `ControlPlane`), not
  the queue concern, so the concern contract stays catalog-free and the
  memory/postgres adapters stay symmetric.
- **Fire-time existence: NOT re-checked.** A table dropped after definition
  surfaces as retried/failed jobs, exactly like a manually enqueued job for a
  missing table today. Deleting the stale schedule is the operator's job; the
  admin list makes them visible. (Inherited behavior, documented, not new
  scope.)
- **Dedup uses the `available`-state scan.** A schedule firing while the prior
  identical job is still queued dedupes (no pile-up when cadence outpaces the
  queue); a job already `running` does not suppress — same semantics as the
  flush debounce (`pg_insert_if_absent` doc, `postgres/src/queue.rs:33`).
- **Kind allowlist is `SCHEDULABLE_JOB_KINDS`, not `KNOWN_JOB_KINDS`.**
  Scheduling a bare `transform` job would bypass the run ledger
  (`submit_run`); flush/consolidate/vector-index payloads are internal.
  Widening later is a one-line change.
- **Admin-only, no per-table ACL.** GC/compaction are physical-layout
  operations with no read surface; the coarse `require_admin` gate (the same
  gate that guards `/admin/transforms`, which can already rewrite any table via
  a scheduled transform) is the governance boundary. Fine-grained maintenance
  ACLs are out of scope.

## Relationship to neighboring work

- **`road-compaction-auto-trigger`** (specced separately) is the *event-driven*
  complement: compaction triggered at commit time by observed table state.
  Schedules are the *operator-cadence* path — they are not the trigger
  mechanism and do not replace it; the dedup means both paths can coexist
  without job pile-up.
- **`fut-iceberg-gc-orphan-sweep`** names "Pairs with scheduled GC" — the
  orphan sweep, when built as a job kind, becomes a one-line addition to
  `SCHEDULABLE_JOB_KINDS` and an immediate consumer of this surface.

## Out of scope

- New job kinds (orphan sweep, retention enforcement) — the surface is ready
  for them; they are their own slices.
- Event-driven compaction triggering (`road-compaction-auto-trigger`).
- Schedule run history / last-fired bookkeeping — the queue's `failed` rows
  and lineage remain the observability surface; a `last_fired_at` column is a
  cheap follow-on if wanted.
- Jitter/backoff between schedules, per-schedule pause/enable flags,
  timezone-aware cron (UTC only, matching transforms).
- UI surface for schedules (the transforms UI precedent suggests one later).

## Acceptance

- **e2e (fixture):** against the postgres fixture + engine wire, an
  admin-defined cron schedule for `gc_table` (and `compact_table`) fires on a
  probe tick past its `next_run_at`: `fire_due_job_schedules` enqueues exactly
  one deduped job with the kind's typed payload; an immediate re-fire at the
  same probe enqueues nothing; the zero-pool worker path
  (dequeue → `handle_gc`/`handle_compact` → complete, the
  `worker/tests/e2e.rs` drive form) drains it successfully.
- **Contract (both adapters):** define validates (bad cron / unknown kind /
  malformed payload ⇒ `Validation`); list exposes `next_run_at` in the future;
  redefine resets the clock; delete ⇒ gone, unknown ⇒ `NotFound`; not-due ⇒
  no fire; due ⇒ fired exactly once with `next_run_at` advanced past the
  probe; concurrent fire calls never double-enqueue; an identical `available`
  job suppresses the insert but still advances the schedule.
- **HTTP:** schedule CRUD over `/admin/schedules` (201/200/204), 400 on every
  validation failure incl. unknown table, 401/403 from the admin gate, openapi
  drift guard green.

## Interfaces (names the plan consumes)

- Consumes: `validate_cron`/`next_cron_occurrence` (`core/src/transforms.rs:230`/`:242`);
  `GC_JOB_KIND`/`GcJob` (`core/src/gc.rs:6`); `COMPACT_JOB_KIND`/`CompactJob`
  (`core/src/compact_job.rs:6`); `NewJob`/`Queue` (`core/src/queue.rs:18`/`:89`);
  `pg_insert_if_absent` (`postgres/src/queue.rs:37`);
  `Catalog::current_snapshot` (`core/src/catalog.rs:71`);
  `scheduler_loop` (`engine/src/scheduler.rs:53`); `require_admin`/`admin_routes`/
  `admin_openapi`/`status_for` (`runtime/src/admin.rs:44`/`:1581`/`:1705`,
  `runtime/src/auth.rs:41`); the memory queue state (`memory/src/lib.rs:66`,
  `memory/src/queue.rs:12`).
- Produces (plan tasks rely on these EXACT names):
  - `JobSchedule`, `SCHEDULABLE_JOB_KINDS`, `validate_job_schedule` in
    `core/src/job_schedule.rs`, re-exported at the core root.
  - `JobScheduleStatus`, `ScheduleFired`, and the four `Queue` trait methods
    `define_job_schedule` / `list_job_schedules` / `delete_job_schedule` /
    `fire_due_job_schedules`.
  - Migration `0043_job_schedules.sql` (`queue.schedule` + `schedule_due`).
  - `engine::scheduler::maintenance_tick`, folded into `scheduler_loop`.
  - `POST /admin/schedules`, `GET /admin/schedules`,
    `DELETE /admin/schedules/{name}` in `runtime/src/admin.rs`.
  - Testkit `job_schedules_contract`.

## Global constraints (loom-specific, carry into the plan)

- Tests are `rust_test` / `loom_fixture_test` integration targets only; new
  fixture tests MUST use `loom_fixture_test`
  (`src/control-plane/postgres/defs.bzl`); the `no-inline-tests` prek hook
  fails on inline `#[test]`.
- The postgres queries are compile-time sqlx: after any SQL change run
  `bash tools/sqlx-prepare.sh` and commit `.sqlx/`; `sqlx-cache-check`
  enforces freshness.
- Clippy is strict (pedantic + restriction): no `unwrap`/`expect`/`panic`/
  `indexing_slicing` in production code.
- Run `buck2 run //tools:prek -- run --all-files` before every commit;
  markdown ends with exactly one trailing newline, no trailing whitespace.
