# Transform capabilities

This document describes what the transform subsystem of loom can do today: the
queue-driven derivation pillar that reads committed table snapshots with
DataFusion, runs SQL (physical table-to-table or typed, in the ontology's
vocabulary), and commits the result as a new snapshot plus lineage, atomically.
It covers the transform service (`src/services/transform/`), the shared
`datafusion-io` read/write layer it is built on, and the two worker execution
models (the pool-owning transform binary and the zero-pool gRPC/Flight worker
in `src/services/worker/`), with guarantees, limits, and key decisions.

_As of 4861433b._

## Queue-driven SQL transforms

The load-bearing primitive is `run_transform`
(`src/services/transform/src/run.rs`): a job names input table(s), an output
table, and a SQL query; a worker resolves each input's current snapshot, has
DataFusion read its Parquet files (`datafusion_io::scan_table` registers the
file set as a listing table on the loom object store), runs the SQL, writes the
result, and commits — in one transaction — the output table (idempotently
created), the new data files, and a lineage event recording inputs → output
(#57). Multi-input joins are supported; each input is registered in DataFusion
under a caller-chosen name (`TransformInput.register_as` — the table name on
the physical path), and two inputs claiming the same SQL name is a
deterministic `AmbiguousInput` error. The end-to-end guarantee is atomicity:
either the snapshot, its files, and its lineage all commit, or nothing does.

Errors are classified by the handler into a retry taxonomy: deterministic
failures (malformed payload, SQL errors, unknown or ambiguous input,
schema-inference failure, conformance violations) abandon the job; transient
failures (control-plane/DB errors, object-store IO, scan/write faults) retry
with a capped exponential backoff derived from the job's attempt count. A
panicking handler is backstopped by the worker loop's `catch_unwind` and
abandoned.

Two input-resolution edge cases were fixed after the first slice (#147): a
missing input surfacing at `Catalog::files` (rather than `current_snapshot`)
is now mapped to `UnknownInput` → abandon at every input read, instead of
retrying a permanently-absent table forever; and an input table with zero
files at its snapshot is registered as an **empty relation** with the table's
declared schema (`logical_arrow_schema` + `register_empty_table`), so the SQL
runs over an empty input — `SELECT count(*)` yields `0`, `SELECT *` commits an
empty output — rather than failing inside DataFusion's schema inference. The
empty-input schema covers exactly the canonical scalar set the transform
read/write path round-trips (boolean, integer, long, double, string); anything
else is a deterministic `Unsupported` → abandon.

By design, a transform reads **raw** tables and bypasses ACL/ontology — it is
trusted pipeline code; governance reapplies when the output is read through
query-api. Who may author or enqueue a transform is not yet governed.

## Typed transforms (Type(s) → Type)

`run_typed_transform` (`src/services/transform/src/typed.rs`) is the
Object-Model layer over the same orchestration (#59): a `typed-transform` job
names input ontology **types**, one output **type**, and SQL written in type
terms. The worker resolves each input type to its backing table and registers
it in DataFusion under the **type name** (so SQL reads
`SELECT ... FROM Customer JOIN "Order" ...` — no physical names leak into
authored SQL), runs the SQL, and validates the result against the output
type's property contract **before anything is written**.

Conformance is exact-match (`transform::conform`): every property must have a
same-named result column whose inferred physical type satisfies its logical
type, a `required` property's column must be non-nullable, and no extra
columns are allowed — all violations are collected, never short-circuited. A
violation is `DoesNotConform` → abandon, with no write and no transaction, so
every committed run is a faithful, append-compatible materialization of the
Object Model it claims to produce. The output type must pre-exist
(`define_type`); its backing table need not — the idempotent `create_table`
brings it into being on first run. Type authoring is deliberately a separate
action, not folded into the transform.

Typed transforms emit **type-named lineage**: the event's inputs/outputs are
`DatasetRef`s in the dedicated `loom:type` namespace (core's `TypeId`,
parallel to `DatasetId`), so provenance nodes are the ontology types themselves
and `upstream(OrderEnriched)` returns `{Customer, Order}`. The resolved
backing-table refs and the SQL ride in the event payload so the type→table
linkage stays traceable. Scope limits: one output type per job (multi-output
is a follow-on), SQL authoring only.

## Output writing and tuning

Transform output goes through the shared `datafusion_io::write_dataset` path:
result batches are size-estimate repartitioned and written as N Snappy Parquet
files with per-file stats, under a caller-unique run prefix. Two output modes
exist on both the physical and typed paths, selected by an optional
`output_mode` payload field:

- **Append** (the default — absent field means append, so pre-existing job
  payloads are unaffected): each run's files are added to the output table and
  re-runs accumulate.
- **Overwrite**: the result becomes the table's entire live contents, via the
  `Tx::replace_files` staging primitive — the currently-live files are expired
  at the new snapshot (older snapshots still time-travel to the prior
  contents), table-level stats are reset to the new files rather than
  accumulated, and row-ids are never reused. On a brand-new output table,
  overwrite degenerates to create+write. A given table is either appended or
  replaced in one transaction, never both. Overwrite replaces data, not
  schema; compaction and watermark-incremental output were deliberately
  deferred (compaction has since shipped separately, on the worker path).

Write tuning is uniform with the rest of the tree (#242): the transform binary
composes `datafusion_io::WriteConfig` (target file bytes / max files /
compression factor) from defaults < config file < `LOOM_WRITE_*` env, validates
it at startup (a malformed value fails startup rather than silently falling
back), and threads it through both handlers into `run_transform`'s
`write_dataset` call — closing the earlier hole where the knobs were composed
at startup but transform output was silently written with hardcoded defaults.
With no `LOOM_WRITE_*` set, behavior equals the previous defaults.

## Worker execution model

Two execution models coexist, and the direction between them is settled.

The **transform binary** (`src/services/transform/src/main.rs`) is
queue-driven with no HTTP surface: it builds the control plane and object
store from env config via `service_runtime`, then runs the generic
`control_plane_worker::Worker` loop over the `transform` and `typed-transform`
job kinds, dispatching by kind to the two handlers. The queue is dequeued
through the Postgres control plane; the handlers commit output through an
`IcebergControlPlane` so derived snapshots land in the Iceberg mirror, with
data-file paths absolutized against the write store's warehouse root so the
committed mirror paths match what the serving engine resolves. Its Iceberg
catalog is built through the shared `service_runtime::build_storage_factory`,
fixing an earlier defect where it hardcoded a local-filesystem factory and
ignored the object-store config entirely — the binary could not commit to an
S3 warehouse even though every other writer could (#328). The same sweep
deleted the superseded in-transform `compact_table` path, hoisted the
duplicated worker/transform config into the shared `JobConfig`, and moved
`small_files` into core so the zero-pool worker carries no transform
dependency (#328).

The **zero-pool worker** (`src/services/worker/`) is the newer model and the
recorded direction: the engine owns Postgres; the worker has **no Postgres in
its dependency closure**. It connects to the engine over a UDS, runs the same
generic worker loop against a gRPC queue client, and drains `flush_table`,
`gc_table`, `compact_table`, and `build_vector_index` jobs. The single-RPC
jobs share one shape (`run_wire_job`: parse the typed payload — parse error
abandons; run one RPC — RPC error retries with the tuning's backoff).
Compaction is the full pattern for engine-wire compute: list the table's live
files over gRPC, pick the small ones, stream their bytes over Arrow Flight
(the bulk data plane), rewrite them coalesced to the object store, and commit
the swap through the engine's `CompactTable` RPC — zero direct catalog access.
Worker config parsing is strict: a malformed `LOOM_COMPACT_THRESHOLD_BYTES`
(or any tuning knob) fails startup instead of silently falling back to the
default (#202). The transform binary is now the last pool-owning compute
worker; migrating `run_transform`-shaped jobs onto the zero-pool worker is a
tracked follow-on (below), and until then the standing rule is that **no new
direct-PG job handlers** are added.

## Known gaps

- `#fut-programmatic-transforms` — SQL authoring only; no registered-plan
  (programmatic Rust) transforms or multi-output typed transforms yet.
- `#fut-transform-authoring-auth` — transforms run as trusted pipeline code;
  authoring/enqueue authorization is ungoverned.
- `#fut-transform-followups` — watermark/incremental output, DAG /
  transactional enqueue-downstream, optional Ballista escalation.
- `#fut-datafusion-type-coverage` — only the canonical scalar set round-trips;
  timestamps, dates, decimals, and small/unsigned ints abandon the job.
- `#fut-scheduled-jobs` — no cron-like scheduled transforms; jobs are
  enqueue-driven only.
- `#road-transform-wire-migration` — migrate transform jobs onto the zero-pool
  worker (inputs over Flight, commit via an `EngineControl::CommitTransform`
  RPC), retiring the last pool-owning worker.
- `#fut-transform-backoff-unify` — the transform handler keeps its own retry
  backoff formula instead of `WorkerTuning::backoff`; unifying changes retry
  timing and needs its own decision.
- `#fut-worker-lazy-compact-ctx` — the zero-pool worker builds its compaction
  context eagerly, so even a flush-only worker requires warehouse config and a
  reachable Flight endpoint at startup.
