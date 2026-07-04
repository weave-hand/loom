# Transform capabilities

This document describes what the transform subsystem of loom can do today: the
queue-driven derivation pillar that reads committed table snapshots over the
engine wire, runs SQL with DataFusion (physical table-to-table or typed, in the
ontology's vocabulary), and commits the result as a new snapshot plus lineage,
atomically. Transforms execute on the **zero-pool worker**
(`src/services/worker/src/transform.rs`) — the worker holds no Postgres pool
and no Iceberg catalog; inputs stream over Arrow Flight and the commit goes
through a single `EngineControl::CommitTransform` RPC. The former pool-owning
transform service (`src/services/transform/`) has been deleted (#342). The
shared payload/conformance types live in `control_plane_core`
(`transform_job.rs`, `conform.rs`) and the read/write compute layer is
`datafusion-io`.

_As of 403f7a6a._

## Queue-driven SQL transforms

The load-bearing primitive is the worker's `run_wire_transform`
(`src/services/worker/src/transform.rs`): a `transform` job
(`control_plane_core::TransformJob` — inputs, output, SQL, optional
`output_mode`) names input table(s), an output table, and a SQL query. The
handler resolves each input over the wire with `ListFiles` — whose response
carries the table's declared schema as `columns_json` (absent ⟺ the table does
not exist, the unknown-input discriminator) — streams each non-empty input's
live file set over Arrow Flight (`FlightTicket`), and registers the batches in
a fresh DataFusion `SessionContext` (`datafusion_io::register_batches`) under a
caller-chosen name (the table name on the physical path); two inputs claiming
the same SQL name is a deterministic `AmbiguousInput` abandon, checked before
any RPC. It runs the SQL, writes the result via the shared
`datafusion_io::write_dataset` path, and commits — in one transaction on the
engine side — the output table (idempotently created), the new data files, and
a lineage event recording inputs → output with the SQL in the payload (#57,
#342). Multi-input joins are supported. The end-to-end guarantee is
atomicity: either the snapshot, its files, and its lineage all commit, or
nothing does.

Errors are classified into the retry taxonomy: deterministic failures
(malformed payload, unknown or ambiguous input, SQL/planning errors,
schema-inference failure, registration errors, conformance violations, a
commit that produces no snapshot) abandon the job; transient failures (wire
RPC errors, Flight fetch races against compaction, object-store write faults)
retry with `WorkerTuning::backoff` — the same capped exponential every other
worker job uses, closing the old per-handler backoff drift (the old crate's
`2^attempts`s formula, 64s cap; `#fut-transform-backoff-unify`, resolved by
#342). A mid-handler drop race
(an input vanishing between existence check and file read) surfaces as a
transient wire error whose retry re-lists cleanly and converges to the
unknown-input abandon. A panicking handler is backstopped by the worker loop's
`catch_unwind` and abandoned.

An input table with zero files registers as an **empty relation** with the
table's declared schema (`ListFilesResponse.columns_json` →
`logical_arrow_schema` + `register_empty_table`), so the SQL runs over an
empty input — `SELECT count(*)` yields `0`, `SELECT *` commits an empty
output — rather than failing inside DataFusion's schema inference (#147,
carried onto the wire path by #342). The empty-input schema covers exactly
the canonical scalar set the transform read/write path round-trips (boolean,
integer, long, double, string); anything else is a deterministic abandon.

By design, a transform reads **raw** tables and bypasses ACL/ontology — it is
trusted pipeline code; governance reapplies when the output is read through
query-api. Who may author or enqueue a transform is not yet governed.

## Typed transforms (Type(s) → Type)

`handle_typed_transform` (same module) is the Object-Model layer over the same
orchestration (#59, #342): a `typed-transform` job
(`control_plane_core::TypedTransformJob`) names input ontology **types**, one
output **type**, and SQL written in type terms. The worker resolves each input
type to its backing table over the wire (`gov_resolve`) and fetches the output
type's contract (`gov_get_type`) — a gRPC `NotFound` from either is
deterministic (unknown type → abandon), not retried. Inputs register in
DataFusion under the **type name** (so SQL reads
`SELECT ... FROM Customer JOIN "Order" ...` — no physical names leak into
authored SQL), and the result is validated against the output type's property
contract **before any rows are collected or written**.

Conformance is exact-match (`control_plane_core::check_conformance`): every
property must have a same-named result column whose inferred physical type
satisfies its logical type, a `required` property's column must be
non-nullable, and no extra columns are allowed — all violations are collected,
never short-circuited. A violation abandons the job with no write and no
transaction, so every committed run is a faithful, append-compatible
materialization of the Object Model it claims to produce. The output type must
pre-exist (`define_type`); its backing table need not — the idempotent
`create_table` brings it into being on first run. Type authoring is
deliberately a separate action, not folded into the transform.

Typed transforms emit **type-named lineage**: the event's inputs/outputs are
`DatasetRef`s in the dedicated `loom:type` namespace (core's `TypeId`,
parallel to `DatasetId`), so provenance nodes are the ontology types themselves
and `upstream(OrderEnriched)` returns `{Customer, Order}`. The resolved
backing-table refs and the SQL ride in the event payload so the type→table
linkage stays traceable — byte-identical to the pre-migration payload shape.
Scope limits: one output type per job (multi-output is a follow-on), SQL
authoring only.

## Output writing and commit

Transform output goes through the shared `datafusion_io::write_dataset` path:
result batches are size-estimate repartitioned and written as N Snappy Parquet
files with per-file stats, under a caller-unique run prefix, then absolutized
against the worker's write-store root (`absolute_data_files`) so the committed
mirror paths match what the serving engine resolves. The commit is one
`EngineControl::CommitTransform` RPC mirroring `CompactTable`'s conventions
(#342): the engine decodes the inferred output columns, the written
`DataFile`s, and the `LineageWire` envelope (decode errors are
`invalid_argument`), then runs the same staged transaction the old binary ran
locally — `create_table` + `append_files`/`replace_files` + `emit` + `commit`
through an `IcebergControlPlane` — mapping `Conflict` to `aborted` and the
rest per the standard status mapping.

Two output modes exist on both the physical and typed paths, selected by an
optional `output_mode` payload field:

- **Append** (the default — absent field means append, so pre-existing job
  payloads are unaffected): each run's files are added to the output table and
  re-runs accumulate.
- **Overwrite**: the result becomes the table's entire live contents, via the
  `TableTx::replace_files` staging primitive — the currently-live files are
  expired at the new snapshot (older snapshots still time-travel to the prior
  contents), table-level stats are reset to the new files rather than
  accumulated, and row-ids are never reused. On a brand-new output table,
  overwrite degenerates to create+write. A given table is either appended or
  replaced in one transaction, never both. Overwrite replaces data, not
  schema.

Write tuning is uniform with the rest of the tree (#242): the worker composes
`datafusion_io::WriteConfig` (target file bytes / max files / compression
factor) from defaults < config file < `LOOM_WRITE_*` env via the shared
`JobConfig`, validates it at startup (a malformed value fails startup rather
than silently falling back), and threads it through both transform handlers.

## Worker execution model

There is one execution model: the **zero-pool worker** (`src/services/worker/`)
— the engine owns Postgres; the worker has **no Postgres in its dependency
closure** (a structural invariant pinned by the worker BUCK layout). It
connects to the engine over a UDS, runs the generic `control_plane_worker`
loop against a gRPC queue client, and drains `flush_table`, `gc_table`,
`compact_table`, `build_vector_index`, `transform`, and `typed-transform`
jobs. The single-RPC jobs share one shape (`run_wire_job`); compaction and
transforms are the full engine-wire compute pattern: list live files over
gRPC, stream bytes over Arrow Flight (the bulk data plane), compute locally
(coalesce for compaction, DataFusion SQL for transforms), rewrite to the
object store, and commit the result through a single engine RPC
(`CompactTable` / `CommitTransform`) — zero direct catalog access (#342).
Worker config parsing is strict: a malformed tuning knob fails startup instead
of silently falling back to the default (#202). Job payloads and kind strings
were wire-frozen across the migration, so jobs queued against the old binary
were executable by either binary during cutover. Transform inputs are
collected in worker memory like compaction's (streaming input scans are a
recorded follow-up under `#fut-transform-followups`).

## Named definitions and first-class runs

Transforms are a sixth control-plane concern (`Transforms`, `control_plane_core::transforms`):
a `TransformDef` names a `TransformBody` — `Physical` (table inputs/output +
SQL) or `Typed` (ontology-type inputs/output + SQL), each mirroring the
matching queue job payload one-for-one, including `output_mode`. `define_transform`
is an upsert (redefining an existing name replaces its body), backed by
`list_transforms`/`get_transform`/`delete_transform` (delete is idempotent and
history-preserving: runs keep their frozen body and transform name as plain
text with no FK, so deleting a definition never orphans or rewrites past
runs). Typed bodies are validated at define time — unknown input or output
type names are a `Validation` rejection on both adapters, the same
fail-at-define-not-at-read posture the rest of the ontology holds to.
`TransformDef` also carries `schedule` and `on_input_commit` fields, both live:
`schedule` (slice 2, see **Cron schedules** below) is a cron expression
validated and enforced at fire time, while `on_input_commit` (slice 3, see
**Data triggers** below) marks the def as firing whenever a commit writes new
data to one of its resolved inputs.

A `TransformRun` is the durable execution record: `run_id` **is** the lineage
`run_id` (the same UUID names both), so a run's lineage events are queryable
with no extra join, and `body` is frozen at submit time — redefining or
deleting the `TransformDef` never rewrites a run's history. The state machine
is `Queued → Running → Succeeded | Failed`, with `Running → Queued` on a
retryable failure (retry-requeue): `submit_run` atomically records the
`Queued` run and enqueues its job (the job — carrying `run_id` via the
existing back-compatible `#[serde(default)] run_id` payload field — is visible
to a worker iff the run row exists); the worker's job handlers call
`mark_run_running` before doing any work (a failure to reach the engine here
is itself retryable, leaving the run `Queued` for the retry to re-mark); on
failure, `finish_run_failed` applies `RunOutcome::RetryQueued` (deterministic
`Abandon` policy) or `RunOutcome::Failed` (terminal) depending on the job's
retry classification. On success there is no separate "mark succeeded" RPC:
`mark_run_succeeded` rides inside the same `EngineControl::CommitTransform`
transaction that stages the output files and emits lineage — `create_table` +
`append_files`/`replace_files` + `emit` + `mark_run_succeeded` + `commit`, so
a run's record and the data it produced become visible atomically, exactly
once, with `snapshot_id` populated from the committed snapshot. Lifecycle
methods carry no state-transition guards by design: the queue is
at-least-once, so a retried job that already committed may legitimately
re-mark a terminal run, and the record follows execution rather than gating
it. Runs are listed newest-first (`queued_at` desc, `run_id` desc tiebreak),
optionally filtered to one transform's history.

## Cron schedules

`TransformDef.schedule` is live: a 5-field UTC cron expression (minute, hour,
day-of-month, month, day-of-week), parsed and validated at define time by
croner (`validate_transform_def` → `core::transforms::validate_cron`) — an
invalid expression is a `Validation` 400 at `define_transform`, never
discovered later at fire time. A scheduled def carries a derived
`next_run_at`, computed from the definition (or redefinition) time as the
first occurrence strictly after "now"; redefining a schedule resets the
clock — `next_run_at` is recomputed from the redefinition time, not
carried forward from the original cadence.

The engine runs a scheduler loop (`src/services/engine/src/scheduler.rs`; see
[engine.md](engine.md)) that ticks on `LOOM_SCHEDULER_TICK_SECS` (default 5s)
and calls `Transforms::claim_due_schedules`, which the postgres adapter
implements as one transaction: `SELECT ... FOR UPDATE SKIP LOCKED` over defs
whose `next_run_at <= now`, advancing each claimed def's `next_run_at` to its
next occurrence **in the same transaction** as the claim. That pairing is the
whole correctness story — concurrent engines never claim the same due
definition twice, and because the clock always advances before a run is
submitted, a crash (or a `submit_run` failure) between claim and submit
**skips** that occurrence rather than firing it twice on the next tick:
at-most-once, not at-least-once. Each claimed def gets one fresh
`TransformRun` (`trigger: RunTrigger::Schedule`) submitted through the
ordinary `submit_run` path — downstream execution, retry-requeue, and
lineage are indistinguishable from a manual or ad-hoc run except for the
trigger tag. `GET /admin/transforms/{name}` exposes the derived
`next_run_at` (RFC3339 UTC, omitted when the transform is unscheduled); the
list route does not.

## Data triggers

`TransformDef.on_input_commit` (slice 3, `road-transform-data-triggers`, PR #NN)
is live: a data-triggered def fires a fresh run whenever a commit writes new
data to one of its resolved inputs, with no polling loop. Resolution and
cycle-checking are shared, backend-neutral logic in `core::transforms`:
`TriggerNode::resolve` maps a def's body to its physical input/output tables
(a typed body resolves input/output type names via the ontology snapshot at
hand — an unresolvable name, e.g. a deleted binding, contributes no edge), and
`validate_no_trigger_cycle` runs Kahn's algorithm over the edge set X → Y
("Y reads X's resolved output") and names every def caught in a cycle.

**Define-time rejection.** Both adapters build the edge set over the *complete*
data-triggered set — the def being (re)defined plus every other
`on_input_commit` def — and reject a cycle as a `Validation` error (HTTP 400 at
`POST /admin/transforms`) before the def is persisted. On postgres, the whole
check runs inside the same transaction as the upsert, under
`pg_advisory_xact_lock(TRANSFORM_DEFINE_LOCK)`: two racing defines cannot each
see the other absent and jointly commit a cycle, since the lock serializes
them. The memory adapter gets the same atomicity from its existing transforms
lock (the ontology snapshot used for type resolution is cloned and dropped
*before* that lock is taken, preserving the fake's established
lock-acquisition order).

**Same-tx firing.** The commit-seam hook — `pg_fire_data_triggers` on postgres,
mirrored inside `MemoryTx::commit` on the fake — runs inside every commit
transaction that writes genuinely new data: `IcebergTx::commit` (the primitive
behind engine `CommitTransform` and the snapshot-commit path), inline append,
inline delta write, multi-step writes, and overwrite/truncate call it directly;
`land_parquet` and `overwrite_parquet_snapshot` (dataset landing) reach it via
the new `CommitExtras.data_trigger_tables` field, applied in
`apply_commit_extras` alongside lineage emit and job enqueue. Compaction and the
inline-flush end-cap deliberately leave that field empty (or skip the hook
entirely) — data-preserving rewrites carry no new rows, so re-firing on a flush
or compaction would be a spurious duplicate run. See
[control-plane.md](control-plane.md) for the hook's exact seam list.

For each candidate def whose resolved inputs intersect the committed tables:
the def row is locked `FOR UPDATE` in name order (a fixed lock order across all
matched defs in one commit, deadlock-free, and the same serialization point
that makes the debounce race-free against a concurrent commit), the body is
re-decoded under that lock (so a redefine racing the commit is reflected — the
enqueued run freezes whatever body is live at the locked instant, not a stale
snapshot read earlier in the transaction), and a **debounce** check looks for
an existing `Queued` run of that def: if one exists, the commit skips it
(at-most-one-pending); a `Running` run does **not** suppress, so a run already
executing does not block a follow-up run from queuing once its input changes
again. Absent debounce, the hook inserts a new `TransformRun`
(`trigger: RunTrigger::DataTrigger`) and its queue job atomically with the
triggering commit — visible to a worker iff the commit lands.

**Self-trigger suppression.** The run performing the commit (if any) is looked
up by its `run_id` and excluded from firing even if its own transform is
data-triggered and reads its own output — defense in depth against a
self-referential loop that could otherwise arise from a post-define ontology
rebind (the def's typed input/output re-resolving to overlap after the cycle
check ran).

**Poison-body skip-and-warn.** A def body that fails to deserialize (a stale
shape from before a breaking change, or manual corruption) is skipped with a
`tracing::warn!` rather than failing the transaction — a single broken admin
artifact must never fail unrelated ingest or transform commits.

Tested by the cross-adapter `transform_data_trigger_contract` (testkit,
covering both memory and postgres) plus cycle-rejection legs in
`transforms_contract`; the postgres fixture suite adds eight dedicated legs in
`tests/data_triggers.rs` (inline land, parquet land + debounce, multi-step
write, unmatched table is a no-op, overwrite/truncate, `IcebergTx` firing +
self-skip, flush does not re-fire, and a poisoned body is skipped); and an
engine-wire e2e (`commit_transform_fires_downstream_data_trigger`) proves a
`CommitTransform` RPC fires a downstream data-triggered def. The admin surface
gets a 201/400 pair (`define_data_triggered_transform_is_accepted`,
`define_trigger_cycle_is_rejected_400`).

## Admin HTTP surface

Eight admin routes (`src/services/runtime/src/admin.rs`, `require_auth` then
`require_admin`, OpenAPI-annotated) put the concern within reach without a
direct control-plane handle: `POST /admin/transforms` (201, define/upsert;
400 on a bad body or failed validation), `GET /admin/transforms` (200, all
definitions), `GET /admin/transforms/{name}` (200, or 404 unknown), `DELETE
/admin/transforms/{name}` (200, idempotent), `POST /admin/transforms/{name}/run`
(202, runs the named definition's frozen body now with `RunTrigger::Manual`,
returning the new `run_id`; 404 unknown), `POST /admin/transforms/run` (202,
submits an ad-hoc `TransformBody` with no saved definition under
`RunTrigger::AdHoc`; 400 on a bad body), `GET /admin/transforms/{name}/runs`
(200, that transform's run history newest-first, or 404 for an unknown
transform — distinguishing "no runs" from "no such transform"), and `GET
/admin/runs/{run_id}` (200 the run, 400 non-UUID id, 404 unknown). Both
run-submission routes share one `submit_new_run` helper: mint a fresh
`run_id`, build the `Queued` `TransformRun`, and call `submit_run` with the
body's `to_job(run_id)`.

## Known gaps

- `#fut-programmatic-transforms` — SQL authoring only; no registered-plan
  (programmatic Rust) transforms or multi-output typed transforms yet.
- `#fut-transform-authoring-auth` — transforms run as trusted pipeline code;
  authoring/enqueue authorization is ungoverned (the admin HTTP surface is
  admin-gated, but that is coarse instance-admin, not a transform-authoring
  capability of its own).
- `#fut-transform-followups` — watermark/incremental output, DAG /
  transactional enqueue-downstream, optional Ballista escalation, streaming
  input scans for the wire path.
- `#fut-datafusion-type-coverage` — only the canonical scalar set round-trips;
  timestamps, dates, decimals, and small/unsigned ints abandon the job.
- `#fut-worker-lazy-compact-ctx` — the zero-pool worker builds its compaction
  context eagerly, so even a flush-only worker requires warehouse config and a
  reachable Flight endpoint at startup.
