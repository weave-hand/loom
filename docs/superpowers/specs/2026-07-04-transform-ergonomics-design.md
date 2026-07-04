# Transform ergonomics — named definitions, runs, schedules, data triggers

- **Date:** 2026-07-04
- **Area:** transform
- **Register items:** delivers [[road-transform-defs-runs]] (slice 1),
  [[road-transform-schedules]] (slice 2), [[road-transform-data-triggers]]
  (slice 3); slice 2 narrows `#fut-scheduled-jobs` to non-transform
  maintenance kinds
- **Status:** implementation-ready (three slices, one lane/PR each)

## North star

An operator can treat a transform as a first-class thing, not an anonymous
queue payload: define it once by name (SQL over physical tables or ontology
types), run it on demand, put it on a cron schedule, or have it fire
automatically when an input table receives a new snapshot — and afterwards
answer "what transforms exist, what ran last night, why did run X fail, which
snapshot did it produce" entirely over the documented HTTP surface. An ad-hoc
"run this SQL now" escape hatch shares the same run machinery for exploration
and backfills.

## Operator decisions (2026-07-04)

- **Layered model** — named `TransformDef`s are the primary surface; an
  ad-hoc one-shot submission path rides the same run/status machinery.
- **Full trigger trio** — manual run-now, cron schedules, and
  data-driven triggers (run on input commit) are all in scope, delivered as
  slices 1/2/3 respectively.
- **First-class runs** — a durable `TransformRun` record per execution; the
  queue stays pure transport.
- **Admin-gated, all of it** — define/read/run/delete all live under the
  existing `require_admin` gate. Per-role grants stay deferred
  (`#fut-transform-authoring-auth` remains open; this spec records the
  admin-gate decision there in prose).
- **Architecture A** — a sixth control-plane concern + worker-hosted run
  lifecycle over the existing engine-wire RPCs + a transactional
  commit-seam trigger hook (chosen over queue-native polling and a dedicated
  scheduler service).
- **One spec, three slices** — three ROADMAP items, three `work/` lanes,
  three PRs (copy-on-write precedent).

## Model (control-plane core, slice 1)

New concern `transforms` alongside queue/catalog/ontology/acl/lineage. Core
types (all serde, wire-stable like the governance types):

```rust
pub struct TransformName(String);          // newtype, mirrors ActionName

pub struct TransformDef {
    pub name: TransformName,
    pub body: TransformBody,
    pub schedule: Option<String>,          // 5-field cron, UTC; slice 2
    pub on_input_commit: bool,             // data trigger flag; slice 3
}

pub enum TransformBody {                   // mirrors the two job payloads exactly
    Physical { inputs: Vec<TableRef>, output: TableRef, sql: String, output_mode: OutputMode },
    Typed    { inputs: Vec<String>,   output: String,   sql: String, output_mode: OutputMode },
}

pub enum RunTrigger { Manual, Schedule, DataTrigger, AdHoc }
pub enum RunState   { Queued, Running, Succeeded, Failed }

pub struct TransformRun {
    pub run_id: Uuid,                      // == LineageEvent.run_id: runs↔lineage join key
    pub transform: Option<TransformName>,  // None = ad-hoc
    pub trigger: RunTrigger,
    pub state: RunState,
    pub body: TransformBody,               // frozen at enqueue; redefine never rewrites history
    pub queued_at: OffsetDateTime,
    pub started_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
    pub snapshot_id: Option<i64>,          // produced snapshot on success
    pub error: Option<String>,             // last failure text; survives retries
}
```

Trait (memory + postgres adapters, testkit contracts, the established
concern pattern):

```rust
#[async_trait]
pub trait Transforms {
    // definitions (slice 1)
    async fn define_transform(&self, def: TransformDef) -> Result<()>;   // upsert, like define_model
    async fn get_transform(&self, name: &TransformName) -> Result<TransformDef>;
    async fn list_transforms(&self, page: PageReq) -> Result<Page<TransformDef>>;
    async fn delete_transform(&self, name: &TransformName) -> Result<()>; // idempotent

    // runs (slice 1)
    async fn create_run(&self, run: TransformRun) -> Result<()>;
    async fn mark_run_running(&self, run_id: Uuid) -> Result<()>;
    async fn finish_run(&self, run_id: Uuid, outcome: RunOutcome) -> Result<()>;
    async fn get_run(&self, run_id: Uuid) -> Result<TransformRun>;
    async fn list_runs(&self, transform: Option<&TransformName>, page: PageReq)
        -> Result<Page<TransformRun>>;                                    // newest first

    // schedules (slice 2)
    async fn claim_due_schedules(&self, now: OffsetDateTime, limit: u32)
        -> Result<Vec<TransformDef>>;      // atomic claim; advances next_run_at

    // data triggers (slice 3)
    async fn data_triggered_defs(&self) -> Result<Vec<TransformDef>>;     // on_input_commit=true
}

pub enum RunOutcome {
    Succeeded { snapshot_id: i64 },        // a Succeeded run always has a snapshot (see Execution)
    RetryQueued { error: String },         // retryable fail: back to Queued, error retained
    Failed { error: String },              // terminal (abandon)
}
```

Semantics:

- `define_transform` is an upsert. Validation at define time: typed bodies'
  input/output type names must exist in the ontology (`Validation`
  otherwise); physical `TableRef`s are checked at run time, failing the run
  deterministically like today. Slice 2 adds cron validation; slice 3 adds
  DAG validation (below).
- `delete_transform` removes the definition only. Runs store the transform
  name as plain text (no FK), so history survives deletion and already-queued
  runs still execute their frozen body.
- Run state machine: `Queued → Running → Succeeded|Failed`, with
  `Running → Queued` on retryable failure (error retained on the row). The
  run's `run_id` doubles as the lineage `run_id`, so a run's lineage events
  are queryable with no extra linkage.

Postgres: new `transforms` schema (migration) with `transform` (name pk,
body jsonb, schedule text null, on_input_commit bool, next_run_at
timestamptz null) and `transform_run` (run_id uuid pk, transform text null,
trigger text, state text, body jsonb, timestamps, snapshot_id bigint null,
error text null). Indexes: `transform_run(transform, queued_at desc)` for
history; partial `transform_run(transform) where state='queued'` for the
debounce probe; `transform(next_run_at) where schedule is not null` for the
scheduler scan. `.sqlx` refreshed via `tools/sqlx-prepare.sh`.

`ControlPlane` gains a `transforms()` accessor. `WireControlPlane` delegates
it to the direct plane, exactly like `catalog()` post-#346 — the admin router
that consumes it holds the direct plane, so no new wire RPCs for management
reads/writes.

## Execution — run lifecycle (slice 1)

Starting a run = `create_run` (Queued) + `queue().enqueue(...)` of the
**existing** job kinds, in one control-plane transaction (`begin()` on the
direct plane). The only payload change: `TransformJob` and
`TypedTransformJob` gain `#[serde(default)] pub run_id: Option<Uuid>` —
fully back-compatible; existing enqueue sites (tests, seeds) are untouched
and run without a run record, exactly as today.

The worker is zero-pool (no Postgres), so lifecycle updates ride the
engine-wire seam it already uses:

- **Dequeue** with `run_id` present → new lightweight
  `EngineControl::MarkRunRunning { run_id }` RPC (engine calls
  `transforms().mark_run_running`).
- **Success** → `CommitTransform` gains an optional `run_id`; the engine
  marks the run `Succeeded { snapshot_id }` **in the same transaction as the
  Iceberg commit**. No window where the data exists but the run says Queued.
  A commit that yields no snapshot is already the worker's
  deterministic-abandon path today; with a `run_id` that path ends
  `Failed`, so `Succeeded` always carries a concrete `snapshot_id`.
- **Failure** → new `EngineControl::FinishRunFailed { run_id, error, terminal }`
  RPC, called by the worker alongside its existing `fail(...)` queue
  decision: retryable → `RetryQueued`, abandon → `Failed`.

Runs without a `run_id` (legacy payloads) skip all three calls — the worker
branches on `Option`.

## HTTP surface (slice 1; runtime admin router)

All routes live in the runtime admin router (it already holds the full
control plane and performs ontology defines), inheriting `require_admin`,
the #344 OpenAPI fragment, and the route-set drift tests. Eight operations:

| Route | Contract |
| --- | --- |
| `POST /admin/transforms` | body = `TransformDef` serde shape → 201; 400 on validation (unknown typed types; slice 2 bad cron; slice 3 cycle) |
| `GET /admin/transforms` | 200 `{ transforms: [TransformDefView] }` |
| `GET /admin/transforms/{name}` | 200 def (+ `next_run_at` when scheduled); 404 unknown |
| `DELETE /admin/transforms/{name}` | 200 always (idempotent; body `{deleted: name}`) |
| `POST /admin/transforms/{name}/run` | 202 `{ run_id }` (`trigger: Manual`); 404 unknown |
| `POST /admin/transforms/run` | body = `TransformBody` serde shape → 202 `{ run_id }` (ad-hoc: `transform: null`, `trigger: AdHoc`) |
| `GET /admin/transforms/{name}/runs` | 200 `{ runs: [TransformRunView] }`, newest first; 404 unknown transform |
| `GET /admin/runs/{run_id}` | 200 run view; 404 unknown |

202 (not 201) for both run submissions: the resource is asynchronous work,
not a created entity at rest. New `ToSchema` DTOs
(`TransformDefView`, `TransformRunView`, `RunSubmittedResp`, …); the admin
fragment's `documents_exactly_the_expected_routes` set grows accordingly, as
do query-api's `expected()` openapi drift tests.

## Schedules (slice 2)

- `schedule` is a standard 5-field cron expression, evaluated in UTC, parsed
  with the `croner` crate (native 5-field support), imported via reindeer.
  Invalid expression → `Validation` → 400 at define.
- Define/upsert computes and stores `next_run_at`; removing the schedule
  (upsert with `schedule: null`) clears it.
- A scheduler loop in the **engine service** (it owns Postgres) ticks on a
  short interval: `claim_due_schedules(now, limit)` atomically claims due
  defs and advances each `next_run_at` to the next cron occurrence
  (`UPDATE … RETURNING`-style in postgres; mutex in memory), then
  creates + enqueues a run per claimed def with `trigger: Schedule`. The
  atomic claim makes concurrent engines safe (each due def fires once).
- Narrows `#fut-scheduled-jobs` to non-transform maintenance kinds (GC,
  compaction) — the generic want that remains after transform scheduling
  lands; the FUTURE entry is reworded in the planning PR, not removed.

## Data triggers (slice 3)

- The **snapshot-commit seam in the postgres adapter** — which both ingest
  commits and engine `CommitTransform` flow through — matches each committed
  `(schema, table)` against `data_triggered_defs()` (typed inputs resolved
  through the ontology at eval time) and creates + enqueues a run
  (`trigger: DataTrigger`) for each match **inside the same commit
  transaction**. No polling, no watermarks, no missed-commit races: the
  queue, catalog mirror, and runs share one Postgres. The memory fake
  mirrors the hook so testkit can contract it.
- **Debounce:** skip enqueue when the transform already has a `Queued` run
  (at-most-one-pending). A `Running` run does not suppress — the new commit
  may postdate the running read, so one follow-up run queues.
- **Cycle safety:**
  - Define time: when `on_input_commit` is set, validation builds the edge
    set over data-triggered defs (X → Y where Y reads X's output, resolved
    to physical tables) and rejects a define that creates a cycle → 400.
  - Run time (defense in depth, since ontology bindings can change after
    definition): a commit produced by a transform run never triggers the
    same transform — the seam resolves the committing `run_id` to its
    transform name and skips self-matches.
- Composes with `road-action-enqueue-downstream` rather than replacing it:
  action/ingest writes commit → dependent transforms fire.

## Testing

- **Testkit contracts** (both adapters): define/get/list/delete + upsert +
  idempotent delete; typed-type validation; run lifecycle transitions incl.
  retry-requeue error retention and frozen-body-after-redefine; run listing
  order + pagination; `claim_due_schedules` claims-once under concurrent
  callers and advances `next_run_at`; trigger matching, debounce, self-skip,
  and cycle rejection.
- **Worker e2e** (fixture): extend the existing transform e2e's to enqueue
  via a run and assert Queued→Running→Succeeded with the committed
  `snapshot_id`; a failing transform ends Failed with the error recorded.
- **Engine**: scheduler loop against the memory adapter (due def fires once,
  `trigger: Schedule`); commit-seam trigger e2e (land into an input table →
  dependent run appears, same-transaction visibility).
- **Runtime route tests** (memory adapter) per handler: happy paths, 404s,
  400 validation branches, non-admin 403 spot check; fragment route-set
  growth.
- **OpenAPI drift**: query-api `expected()` additions.

## Slices and register outcome

Three ROADMAP items, all `area:transform`, `spec:` this file; slices 2 and 3
are `[[road-transform-defs-runs]]`-blocked:

1. `road-transform-defs-runs` — concern (defs CRUD + runs) + payload
   `run_id` + `MarkRunRunning`/`FinishRunFailed`/`CommitTransform{run_id}`
   RPCs + the eight admin routes + OpenAPI.
2. `road-transform-schedules` — cron validation, `next_run_at`,
   `claim_due_schedules`, engine scheduler loop. `#fut-scheduled-jobs` stays,
   narrowed to non-transform maintenance kinds.
3. `road-transform-data-triggers` — `on_input_commit` live, define-time DAG
   validation, commit-seam matcher + debounce + self-skip (postgres +
   memory).

Each landing PR folds its capability into
`docs/system-capabilities/transform.md` (+ control-plane/engine docs where
touched). `#fut-transform-followups`' DAG bullet is partially delivered by
slice 3 — its prose is trimmed accordingly in slice 3's PR, leaving
watermark/incremental, Ballista, and streaming scans.

## Non-goals

- Per-role authoring/run grants (`#fut-transform-authoring-auth` stays open;
  admin-gate decision recorded there).
- Programmatic (registered-plan / Rust) transforms and multi-output typed
  transforms (`#fut-programmatic-transforms`).
- Incremental/watermark processing, Ballista escalation, streaming input
  scans (`#fut-transform-followups`).
- Run cancellation, run-record retention/GC, and a pause/disable flag on
  definitions (delete or de-schedule covers today's need).
- Backfill orchestration beyond the ad-hoc escape hatch.
- Exposing run submission to non-admin subjects.
