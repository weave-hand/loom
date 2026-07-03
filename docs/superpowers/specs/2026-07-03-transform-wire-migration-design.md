# Transform jobs over the engine wire (retire the pool-owning transform worker)

- **Date:** 2026-07-03
- **Status:** approved
- **Register item:** `fut-transform-wire-migration` (promoting to ROADMAP)

## Problem / Motivation

The transform binary (`src/services/transform/`) is the **last pool-owning compute
worker**: it builds its own Postgres pool, its own Iceberg `SqlCatalog`, and commits
through a local `IcebergControlPlane` (`main.rs`). Every other compute job — flush, GC,
compaction, vector-index build — already runs on the zero-pool worker
(`src/services/worker/`), which talks to the engine exclusively over gRPC
(`EngineControl`) and Arrow Flight, per the engine-owns-Postgres direction.

The pillar-idioms audit validated that direction structurally: the worker crate is
~290 lines against transform's ~770 for a comparable job surface, because the
engine wire replaces catalog resolution, transaction plumbing, and Iceberg-catalog
construction with single RPCs. The compute inside transform is already
engine-independent — `run.rs` steps 1–5 are `datafusion-io` calls (`scan_table`,
`logical_arrow_schema`/`register_empty_table`, `infer_columns`, `write_dataset`,
`absolute_data_files`); only step 6 (the `IcebergTx` commit: `create_table` +
`append_files`/`replace_files` + `emit(lineage)` + `commit`) needs a wire equivalent.
Until this lands, no new direct-PG job handlers may be added.

## Design

Migrate both `run_transform`-shaped job kinds — `transform` (physical SQL) and
`typed-transform` (ontology-typed) — onto the zero-pool worker. Inputs stream over
Flight; compute stays `datafusion-io`; commit goes over a new
`EngineControl::CommitTransform` RPC. Payload JSON and job-kind strings are
**unchanged**, so queued jobs are executable by either binary during cutover.

### `CommitTransform` RPC (mirrors `CompactTable` + `WriteObject` conventions)

```proto
rpc CommitTransform (CommitTransformRequest) returns (CommitTransformResponse);

message CommitTransformRequest {
  string schema = 1;              // output TableRef
  string name = 2;
  string columns_json = 3;        // serde_json Vec<ColumnSpec> — the inferred output schema
  repeated string write_json = 4; // one serde_json DataFile per written file (path, counts, per-column stats)
  string lineage_json = 5;        // serde_json LineageWire (inputs -> output, sql payload)
  bool replace = 6;               // false = Append (add files); true = Overwrite (replace live set)
}
message CommitTransformResponse { optional int64 snapshot_id = 1; } // absent => commit produced no snapshot
```

Engine implementation (`engine/src/service.rs`): construct the same
`IcebergControlPlane::new(self.cp.clone(), self.catalog.clone())` the transform
binary uses today (the engine already holds both), then run exactly `run.rs`
step 6 in one tx: `create_table` (idempotent) + `append_files`/`replace_files`
(per `replace`) + `emit(lineage)` + `commit`. `replace=true` rides the existing
`WriteMode::Overwrite` end-cap semantics (older snapshots time-travel). Decode
errors are `invalid_argument`; `Conflict` maps to `aborted`; the rest `internal`
— same `status()` mapping as `CompactTable`.

### Worker execution (`worker/src/transform.rs`, analog of `compact.rs`)

1. **Parse** the payload. `TransformPayload`/`TypedTransformPayload` (and
   `OutputMode`) move to `control_plane_core` as `TransformJob`/`TypedTransformJob`
   with `TRANSFORM_JOB_KIND = "transform"` / `TYPED_TRANSFORM_JOB_KIND =
   "typed-transform"` constants, joining `CompactJob` et al. Parse failure => Abandon.
2. **Typed resolution over the wire:** `gov_resolve` per input type and
   `gov_get_type` for the output (both already on `GrpcQueueClient`); the output
   type's `properties` are the conformance contract. `not_found` => `UnknownType`
   => Abandon.
3. **Per input:** `ListFiles` for the live file set. **Extend `ListFilesResponse`
   with `columns_json`** (serde_json `Vec<ColumnSpec>` of the table's declared
   schema) — required because a zero-file input must register as an empty relation
   (`logical_arrow_schema` + `register_empty_table`), and `FileMeta` carries no
   schema today (compaction never needed it). `not_found` => `UnknownInput` =>
   Abandon. Duplicate `register_as` names => `AmbiguousInput` => Abandon (checked
   before any RPC, as today).
4. **Fetch** each non-empty input via `FlightTicket { schema, name, files }`
   (`FlightTableClient::fetch`) and register the batches in a fresh
   `SessionContext` under `register_as` (table name on the physical path, type
   name on the typed path) via a new `datafusion_io::register_batches` helper
   (`MemTable`, sibling of `register_empty_table`). The engine's live-set guard
   can reject a ticket if compaction swapped files between list and fetch; that is
   a transient race — fetch errors stay **Retry** (re-listing converges), exactly
   like compaction.
5. **Compute:** `ctx.sql`, resolve the result schema, `infer_columns`, then the
   conformance check for typed jobs **before collecting any rows**.
   `conform.rs` (`check_conformance`, pure `ColumnSpec` vs `PropertyDef` logic)
   moves to `control_plane_core` — it has no DataFusion dependency and both
   worker and engine-side tests can reach it there. `DoesNotConform` => Abandon.
6. **Write:** `df.collect()`, `write_dataset` to the worker's `WriteStore` under
   `{schema}/{name}/{run_id}`, `absolute_data_files` against `root_url` — the
   identical path compaction uses.
7. **Commit:** `EngineControl::CommitTransform`. RPC transport errors => Retry
   with `WorkerTuning::backoff`; an absent `snapshot_id` reproduces today's
   `NoSnapshot` => Abandon. Lineage payloads are byte-identical to today's
   (`{"sql": ...}` physical; sql + backing-table names typed).

The retry taxonomy is preserved verbatim: deterministic failures (parse, unknown
type/input, ambiguous input, DataFusion/infer errors, conformance, NoSnapshot)
Abandon; transient ones (wire, object-store write) Retry with backoff. One nuance
the worker must implement that `compact.rs` never needed: gRPC `Code::NotFound`
from resolution RPCs is deterministic (Abandon), not Retry.

### Queue / dispatch changes

None structural. The queue is backend-neutral (engine's `Dequeue` serves arbitrary
kinds); the worker's `main.rs` kinds array grows by the two transform kinds and
gains two dispatch arms. During coexistence both binaries may drain the same
kinds safely — dequeue is exclusive, so a job runs on exactly one of them.

### Sequencing

1. **Wire plumbing:** `CommitTransform` proto + client method + engine impl;
   `columns_json` on `ListFilesResponse`; job payload structs + kind constants and
   `conform.rs` into `control_plane_core`; `register_batches` into `datafusion-io`.
   Transform binary keeps running unchanged.
2. **Physical `transform` on the worker:** `worker/src/transform.rs` handler +
   dispatch + e2e. Moves first — it needs no ontology reads.
3. **`typed-transform` on the worker:** `gov_resolve`/`gov_get_type` resolution +
   conformance + e2e (including a conformance-rejection case).
4. **Delete the pool-owning binary:** remove `src/services/transform/` and its
   BUCK targets once the worker e2es cover the surface (port
   `output_mode`/`overwrite`/`transform_chain`/`run_unknown_input` coverage to
   worker tests). No deploy manifests reference the transform binary, so deletion
   is code + docs only (`docs/system-capabilities/transform.md`, registers via
   `loom-docs-update`).

## Acceptance criteria

- **e2e (hermetic PG fixture, engine over UDS):** seed an input table, enqueue a
  `transform` job, run the zero-pool worker loop; the output table's rows are
  readable through the engine, the lineage event (inputs -> output, sql payload)
  is committed, and the snapshot id exists — with the worker holding **no
  Postgres pool** (structural: `//src/services/worker` BUCK deps contain no
  `sqlx`/`control-plane-postgres`; the test drives only `GrpcQueueClient` +
  `FlightTableClient`).
- Typed e2e: type-term SQL commits with type-named lineage; a non-conforming
  result abandons with no snapshot and no written files registered.
- Empty-input e2e: a zero-file input registers as an empty relation over the wire
  (via `ListFilesResponse.columns_json`) and `SELECT count(*)` commits `0`.
- Overwrite e2e: `output_mode: overwrite` replaces the live set; prior snapshot
  still time-travels.
- The pool-owning transform binary and crate are **removed** (step 4), and
  `buck2 test //src/...` is green without them.

## Out of scope

- Ballista / distributed execution, DAG scheduling, incremental transforms —
  they stay in `fut-transform-followups`.
- Streaming (non-materializing) input scans: inputs are collected in worker
  memory like compaction. Transform inputs can exceed compaction's small-file
  sets, and bytes hop engine -> worker -> object store; accepted for this slice,
  with a streaming/scan-pushdown follow-up recorded under
  `fut-transform-followups`.
- Governance of transform authoring/enqueue (still trusted pipeline code).
- `AwaitJobs` streaming, external SQL wire, and any change to landed compaction
  semantics.
