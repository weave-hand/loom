# Transform capabilities

This document describes what the transform subsystem of loom can do today: the
queue-driven derivation pillar that reads committed table snapshots over the
engine wire, runs SQL with DataFusion (physical table-to-table or typed, in the
ontology's vocabulary), and commits the result as a new snapshot plus lineage,
atomically. Transforms execute on the **zero-pool worker**
(`src/services/worker/src/transform.rs`) — the worker holds no Postgres pool
and no Iceberg catalog; inputs stream over Arrow Flight and the commit goes
through a single `EngineControl::CommitTransform` RPC. The former pool-owning
transform service (`src/services/transform/`) has been deleted (#PRNUM). The
shared payload/conformance types live in `control_plane_core`
(`transform_job.rs`, `conform.rs`) and the read/write compute layer is
`datafusion-io`.

_As of a266dbd2._

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
#PRNUM). Multi-input joins are supported. The end-to-end guarantee is
atomicity: either the snapshot, its files, and its lineage all commit, or
nothing does.

Errors are classified into the retry taxonomy: deterministic failures
(malformed payload, unknown or ambiguous input, SQL/planning errors,
schema-inference failure, registration errors, conformance violations, a
commit that produces no snapshot) abandon the job; transient failures (wire
RPC errors, Flight fetch races against compaction, object-store write faults)
retry with `WorkerTuning::backoff` — the same capped exponential every other
worker job uses, closing the old per-handler backoff drift (`2s`-base formula;
`#fut-transform-backoff-unify`, resolved by #PRNUM). A mid-handler drop race
(an input vanishing between existence check and file read) surfaces as a
transient wire error whose retry re-lists cleanly and converges to the
unknown-input abandon. A panicking handler is backstopped by the worker loop's
`catch_unwind` and abandoned.

An input table with zero files registers as an **empty relation** with the
table's declared schema (`ListFilesResponse.columns_json` →
`logical_arrow_schema` + `register_empty_table`), so the SQL runs over an
empty input — `SELECT count(*)` yields `0`, `SELECT *` commits an empty
output — rather than failing inside DataFusion's schema inference (#147,
carried onto the wire path by #PRNUM). The empty-input schema covers exactly
the canonical scalar set the transform read/write path round-trips (boolean,
integer, long, double, string); anything else is a deterministic abandon.

By design, a transform reads **raw** tables and bypasses ACL/ontology — it is
trusted pipeline code; governance reapplies when the output is read through
query-api. Who may author or enqueue a transform is not yet governed.

## Typed transforms (Type(s) → Type)

`handle_typed_transform` (same module) is the Object-Model layer over the same
orchestration (#59, #PRNUM): a `typed-transform` job
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
(#PRNUM): the engine decodes the inferred output columns, the written
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
(`CompactTable` / `CommitTransform`) — zero direct catalog access (#PRNUM).
Worker config parsing is strict: a malformed tuning knob fails startup instead
of silently falling back to the default (#202). Job payloads and kind strings
were wire-frozen across the migration, so jobs queued against the old binary
were executable by either binary during cutover. Transform inputs are
collected in worker memory like compaction's (streaming input scans are a
recorded follow-up under `#fut-transform-followups`).

## Known gaps

- `#fut-programmatic-transforms` — SQL authoring only; no registered-plan
  (programmatic Rust) transforms or multi-output typed transforms yet.
- `#fut-transform-authoring-auth` — transforms run as trusted pipeline code;
  authoring/enqueue authorization is ungoverned.
- `#fut-transform-followups` — watermark/incremental output, DAG /
  transactional enqueue-downstream, optional Ballista escalation, streaming
  input scans for the wire path.
- `#fut-datafusion-type-coverage` — only the canonical scalar set round-trips;
  timestamps, dates, decimals, and small/unsigned ints abandon the job.
- `#fut-scheduled-jobs` — no cron-like scheduled transforms; jobs are
  enqueue-driven only.
- `#fut-worker-lazy-compact-ctx` — the zero-pool worker builds its compaction
  context eagerly, so even a flush-only worker requires warehouse config and a
  reachable Flight endpoint at startup.
