# Engine-wire compaction over an Arrow Flight data plane

_Design spec. 2026-06-22._

## Context

loom's compaction **primitive** exists and is tested but is **unscheduled** —
nothing triggers it (`[[fut-compaction-job]]`). `compact_table`
(`src/services/transform/src/compact.rs:48-102`) reads a table's live files,
coalesces the small ones into fewer size-targeted Parquet files, and commits the
swap via `Tx::compact_files` (`src/control-plane/core/src/transaction.rs:63-68`),
preserving time travel. It is exercised by `compact_e2e.rs` and the
`snapshot_compact_contract` testkit contract, but only by direct calls — there is
no job, trigger, or operator surface.

This slice makes compaction a **queue-driven job in the engine-wire model**, and
specs the **Arrow Flight data plane** (`[[fut-engine-wire-flight]]`) it streams
data over. The decision (operator, 2026-06-22) is to follow the architecture the
flush vertical established (`[[road-iceberg-flush-consumer]]`,
`2026-06-20-engine-wire-flush-vertical-design`) rather than the legacy
direct-Postgres transform worker:

- the **`engine`** process is the sole Postgres owner and commit authority,
- **zero-pool workers** (`src/services/worker/`) do the compute and commit back
  over gRPC,
- bulk file data moves over **Arrow Flight**, so the engine never holds it.

It is two layers, co-designed here: **(1)** the Flight data plane on the
engine-wire, and **(2)** the compaction job that consumes it. They land as two
linked ROADMAP items sharing this spec — Flight is the foundational dependency,
compaction the first consumer.

## Current engine-wire surface

- **`EngineControl`** gRPC (`src/services/engine-wire/proto/engine_control.proto`):
  `Dequeue`, `Complete`, `Fail`, `Heartbeat`, `AwaitJobs`, `FlushTable`. Served by
  the engine (`src/services/engine/src/service.rs:93-113` for `FlushTable`);
  consumed by `GrpcQueueClient` (`src/services/engine-wire/src/client.rs`).
- **Zero-pool worker** (`src/services/worker/`): `GrpcQueueClient` over UDS, runs
  the generic `control_plane_worker::Worker` loop on a `kinds` list, a handler per
  job. Today listens for `[FLUSH_JOB_KIND]` only.
- **Job model** (`src/control-plane/core/src/queue.rs`): `NewJob { kind, payload,
  run_at, priority }`; kinds are strings (`flush_table`). `FlushJob { schema, name }`
  (`core/src/flush.rs`) is the payload pattern to mirror.
- **No object-store streaming on the wire** — `FlushTable` moves no bulk data; the
  engine-wire has no Flight endpoint yet.

## Layer 1 — Arrow Flight data plane

Add an **Arrow Flight** server to the engine (alongside `EngineControl` on the
same socket/host), exposing a table's live Parquet data as Arrow record-batch
streams. A zero-pool worker is the Flight **client**: it requests a table (or an
explicit file set) and receives the rows as a stream, without a Postgres
connection and without reading object store itself.

- **Ticket**: a Flight `Ticket` identifies what to stream — minimally
  `{ schema, name, files: [relative_path] }` (an explicit file set so the
  compaction worker streams exactly the small files it will coalesce, not the
  whole table).
- **Server**: the engine resolves the file set against the catalog/mirror, opens
  the Parquet via its object store, and streams Arrow batches back. The engine
  already owns Postgres + object store, so it is the natural data source.
- **Schema fidelity**: the stream carries the table's Arrow schema so the worker
  can re-write Parquet with the identical schema (same canonical-scalar set the
  rest of loom uses).
- **Reusable**: this is `[[fut-engine-wire-flight]]` — the data plane is generic
  (future transform bulk reads, exports, etc. can use it); compaction is its first
  consumer. Non-compaction uses stay out of scope here.

### Why Flight (not worker-reads-object-store)

The worker could read object store directly today, but the operator chose to spec
the Flight path so the engine remains the single mediator of table data and the
worker stays a pure compute client over the wire — consistent with
[[engine-wire-write-path-direction]] and avoiding a second object-store-credential
surface in every worker.

## Layer 2 — the compaction job

- **Job type** (`core`): `COMPACT_JOB_KIND = "compact_table"` + `CompactJob
  { schema, name }`, mirroring `flush.rs`. (Threshold/config: a default from engine
  env for slice 1, payload-overridable later.)
- **Operator trigger**: an operator endpoint enqueues a `CompactJob`. loom has no
  admin HTTP today; add a minimal maintenance route —
  `POST /tables/{schema}/{table}/compact` — that calls `queue.enqueue(CompactJob)`
  and returns the `JobId`. (Host it on the existing ingest HTTP service, the
  write-side surface; an explicit operator action sidesteps the "when to compact"
  policy question an automatic threshold would raise.)
- **Worker dispatch**: the zero-pool worker adds `COMPACT_JOB_KIND` to its listen
  list and a handler branch. On a `compact_table` job it:
  1. resolves the live file set + picks the small files (the `small_files`
     partition logic from `compact.rs`, run worker-side against catalog metadata
     fetched over the wire, or returned by the engine in the ticket response),
  2. **streams those files' rows over Flight** (Layer 1),
  3. re-writes them into coalesced, size-targeted Parquet to object store,
  4. commits the swap over a **new `EngineControl::CompactTable` RPC**
     (`{ schema, name, expire: [path], write: [DataFile] }`) — the engine stages
     `Tx::compact_files(table, expire, write)` and commits in its Postgres,
     returning the new `SnapshotId`. **No compute in the engine.**
- **No-op + conflict**: fewer than two small files → the worker completes the job
  as a no-op (matches `compact_table`'s `Ok(None)`). If another compaction raced,
  `compact_files` returns `Conflict` at commit; the worker fails the job with a
  bounded `Retry` (the engine-wire failure path already exists).

## Data flow

```
operator: POST /tables/{schema}/{table}/compact
  -> queue.enqueue(CompactJob{schema,name})  -> JobId

zero-pool worker (no Postgres):
  Dequeue (gRPC) -> CompactJob
  resolve small-file set (metadata over the wire)
  if < 2 small files: Complete (no-op)
  else:
    Flight stream small files' rows  <-- engine reads object store, streams Arrow
    write coalesced Parquet -> object store
    EngineControl::CompactTable{expire, write} (gRPC)
        engine: Tx::compact_files + commit (its Postgres) -> SnapshotId
    Complete (gRPC)
```

The engine owns Postgres + the commit; the worker owns the compute; Flight carries
the bulk rows. The bulk data never lives in the engine's request/response path
(it streams), and never requires the worker to hold Postgres.

## Error handling

- **Atomic commit**: `CompactTable` does `compact_files` + commit in one engine
  Postgres transaction — the swap lands or rolls back; `compact_files`'
  optimistic-concurrency guard (`Conflict` if an expired file is no longer live)
  is surfaced over the RPC and retried.
- **Flight stream failure / worker crash mid-compaction**: the job is not
  completed, so it is redelivered (at-least-once); a re-run recomputes from the
  then-current live set. Newly-written coalesced Parquet from a failed attempt is
  orphaned (uncommitted) — reclaimed by GC (`[[fut-iceberg-gc]]` /
  `[[fut-gc-retention]]`), not this slice.
- **No lineage**: compaction is physical reorganization; like the primitive, it
  emits none.

## Testing

`rust_test` integration targets via `loom_fixture_test` (hermetic Postgres +
object store), extending `compact_e2e.rs` / the engine-wire flush e2e:

- **Flight round-trip**: a worker streams a known file set from the engine over
  Flight and reconstructs the exact rows (schema + values), no Postgres on the
  worker.
- **Compaction e2e (engine-wire)**: land several small files; enqueue via the
  operator endpoint; the worker dequeues, streams, coalesces, commits over
  `CompactTable`; assert the current snapshot lists the coalesced file, row count
  preserved, and a prior snapshot still time-travels to the originals (the
  engine-wire twin of `compact_coalesces_small_files_and_preserves_time_travel`).
- **No-op**: a table with < 2 small files completes the job without a commit.
- **Conflict/retry**: two concurrent compactions of one table — exactly one
  commits, the other gets `Conflict` and retries to a clean no-op (mirrors the
  flush idempotency concern, `[[iss-flush-at-least-once-idempotency]]`).
- **Operator endpoint**: `POST …/compact` enqueues a `compact_table` job and
  returns its `JobId`.

## Scope boundary

- **In:** the Arrow Flight data plane on the engine-wire (generic ticket/stream,
  compaction as first consumer); `COMPACT_JOB_KIND`/`CompactJob`; the
  `EngineControl::CompactTable` commit RPC; the operator enqueue endpoint; the
  zero-pool worker dispatch branch; the tests above.
- **Out (deferred, tracked):** watermark-tracked **incremental / append-delta**
  compaction output (the remaining half of `[[fut-compaction-job]]`, a distinct
  follow-on — slice 1 is full-rewrite of the small-file set); automatic
  threshold-based triggering (operator-triggered only here); non-compaction Flight
  consumers (transform bulk reads, exports) under the general
  `[[fut-engine-wire-flight]]`; physical GC of orphaned coalesced Parquet
  (`[[fut-iceberg-gc]]`/`[[fut-gc-retention]]`); row-level-delete interactions
  (compacted files get fresh row-ids — revisit when deletes land).

## Acceptance criteria

1. An operator can enqueue compaction for a table via an HTTP endpoint; a
   zero-pool worker performs it and the table's small files are coalesced, with
   time travel preserved (the engine-wire twin of the existing compaction e2e).
2. The worker holds **no** Postgres connection: it streams file data over Arrow
   Flight from the engine and commits the swap over `EngineControl::CompactTable`;
   the engine performs `compact_files` + commit and does no compaction compute.
3. The commit is atomic and conflict-guarded; a raced compaction retries to a
   clean no-op.
4. The Arrow Flight data plane is a reusable engine-wire component (generic
   ticket/stream), introduced here with compaction as its first consumer.
5. `buck2 test //src/...` is green; the existing direct-call `compact_table`
   primitive and the flush vertical are unchanged.
