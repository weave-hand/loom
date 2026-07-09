# Stream engine — Continuous / standing queries (slice 4) Design

> **Status:** design (direction). This spec makes `road-stream-continuous`
> build-ready as its own per-slice spec (it currently points at the umbrella
> `2026-07-06-stream-engine-design`). A separate work agent writes the
> implementation plan from it and builds it.

## Goal

**Materialized views as standing micro-batch queries.** A registered MV names a
source stream table, SQL, and an output table. On each trigger (a data-trigger
NOTIFY on a source commit, a cron tick, or a manual run), a worker re-runs the
SQL **over the delta since the last committed per-bucket offset** and commits
the result as new events in the MV's **own declared stream table** — so the
output is itself offset-framed, flushable, and (once slice 3 lands) subscribable:
MVs compose. The **offset watermark** — the last-processed per-bucket offset per
standing query — is new `stream`-schema control-plane state, advanced **in the
same transaction** as the output commit: the watermark moves iff the output
lands (exactly-once *effect* under the at-least-once queue).

Honest framing (from the umbrella spec): this approximates continuous queries
with micro-batch re-execution over deltas, **not** true incremental operators.
The v1 SQL contract is *per-delta*: the query runs over the delta rows only, so
it converges to the batch-equivalent result exactly for **delta-decomposable**
queries (row-wise map / filter / projection — `Q(Δ₁ ∪ … ∪ Δₙ) = Q(Δ₁) ∪ … ∪
Q(Δₙ)`). Aggregations over the whole history are *not* batch-equivalent under
append-only output and stay deferred (they need retract-correct output — see
Non-goals).

## Context — what ships today

- **Slices 0–2 + merge engines (done).** Gapless per-`(table, bucket)` offsets
  (`BucketOffsets::allocate_offset`/`peek_offset`, `core/src/stream.rs:92`;
  postgres `pg_allocate_offset`, `postgres/src/stream.rs:254`), transactional
  and serialized per bucket by the counter row's lock — so offsets in a bucket
  become **visible in commit order, gapless** (a concurrent allocator blocks on
  the row lock until the first tx resolves). Declared **log tables**
  (`StreamDecl::Log(n)`, framing columns `loom_change_kind`/`loom_bucket`/
  `loom_offset` stamped by `inline_append_decl`, `iceberg_inline.rs:448`) whose
  flushed Parquet **keeps the framing** (`include_framing` derived from
  `pg_stream_bucket_count`, `iceberg_flush.rs:155`); CDC tables with dual
  base+changelog Iceberg tables and replace-class merge engines.
- **The zero-pool worker + engine wire (the mandated shape for new jobs).**
  Transforms run on the worker: inputs stream over Arrow Flight, compute is
  DataFusion in the worker, and the commit is a single `EngineControl` RPC —
  the engine owns Postgres (`worker/src/transform.rs:54`, engine handler
  `engine/src/service.rs:276`). New write jobs MUST take this shape.
- **Trigger machinery (done, reused wholesale).** `TransformDef` +
  `on_input_commit` data triggers: `pg_fire_data_triggers`
  (`postgres/src/transforms.rs:137`) runs **inside every commit tx that writes
  new data** — including inline appends (`iceberg_inline.rs:746`/`:1234`) — with
  at-most-one-pending debounce, define-time cycle rejection
  (`validate_no_trigger_cycle`, `core/src/transforms.rs:325`), frozen-body runs,
  and the reconciliation sweep. Cron `schedule` and manual runs share the same
  `submit_run` path. This is exactly the NOTIFY/tick trigger the umbrella spec
  asks for — **no new trigger mechanism is built**.
- **The framed physical read (done, the delta scan's building blocks).**
  `consolidate_stream` (`engine-serving/src/consolidate.rs:166`) already reads a
  stream table's physical framed rows: `IcebergCatalog::files_with_stats` +
  `read_files_as_batches` (`iceberg_read.rs:25`) ∪
  `inline_live_batch_full` (`iceberg_inline.rs:1413`), under the per-table
  advisory lock (`iceberg_flush::lock_table`, `iceberg_flush.rs:414`) so a
  concurrent flush cannot make the files∪inline union double-read or drop rows.

**Slice 3 (`road-stream-subscribe`) is specced but NOT built.** This slice must
not call any subscribe code. "The MV output is itself subscribable" falls out of
the output being a **declared stream table with a durable, offset-framed
changelog** — slice 3, when built, reads it for free.

## The key insight

Everything but the watermark already exists. An MV is a `TransformDef` with a
new body kind; its trigger is the existing data-trigger/schedule machinery; its
delta read is `consolidate_stream`'s framed files∪inline read plus a per-bucket
offset predicate; its output commit is the existing inline landing path (which
already stamps framing, declares stream tables in-tx via
`reconcile_stream_mode`, arms the byte-trigger flush, and **fires data triggers
— which is what makes MVs compose**: an MV's output commit is itself a
commit-that-writes-new-data, so a second MV reading the first's output fires
automatically). The one genuinely new piece of state is
**`stream.mv_watermark`**, and the one genuinely new mechanism is its
**CAS-advance inside the output-commit transaction**.

Two consequences of the gapless-offset substrate do real work here:

1. **The worker can derive the watermark CAS bounds from the delta itself.**
   Per bucket, visible rows ≥ the watermark are contiguous from the watermark
   (offset allocation serializes per bucket on the counter row's lock, so
   offsets commit in order). Hence for every bucket with delta rows,
   `min(loom_offset) == watermark` and `max(loom_offset) + 1` is the new
   watermark — the ticket never has to carry offsets, and the commit-time CAS
   (`next_offset = from → to`) detects any concurrent run.
2. **Exactly-once effect without idempotence bookkeeping.** The queue is
   at-least-once; a retried or duplicate run re-reads the delta from the
   *committed* watermark. If a concurrent run already covered the delta, the
   CAS fails, the whole tx rolls back (no duplicate output), and the job
   abandons with a "superseded" error — the covering run already produced the
   rows, and the debounce guarantees any still-unprocessed tail has a queued
   run.

## Architecture

### MV registration — a new `TransformBody` kind

`TransformBody::MicroBatch { source: TableRef, output: TableRef, buckets: i32,
sql: String }` (serde tag `"microbatch"`), beside `Physical`/`Typed`
(`core/src/transforms.rs:40`). Registration is the existing surface:
`POST /admin/transforms` with the new body kind, typically
`on_input_commit: true` (fire on every source commit) and/or a cron `schedule`
(tick-driven). List/get/delete/run-history/manual-run/ad-hoc all work unchanged
— the body enum is the only extension point. A dedicated `/admin/views` sugar
surface is deferred.

- `to_job` (`core/src/transforms.rs:64`) gains a `MicroBatch` arm emitting a new
  queue kind `STREAM_MV_JOB_KIND = "stream_mv"` with payload `StreamMvJob`
  (mirroring `StreamConsolidateJob`, `core/src/stream_consolidate_job.rs:8`);
  the kind joins `KNOWN_JOB_KINDS` (`core/src/queue.rs:31`) and the worker's
  dequeue list (`worker/src/main.rs:84`).
- `TriggerNode::resolve` (`core/src/transforms.rs:295`) gains a `MicroBatch`
  arm (`inputs = [source]`, `output = Some(output)`), so the define-time cycle
  check and the commit-seam matcher cover MV→MV chains for free.
- `validate_transform_def` (`core/src/transforms.rs:261`) rejects `buckets < 1`,
  an empty `sql`, and `source == output` (a self-consuming MV is nonsense —
  rejected structurally, not left to runtime self-trigger suppression).
- **Run-time (not define-time) source validation.** Whether `source` is a
  declared log stream table is validated when the delta is read (a
  deterministic worker abandon), NOT at define time — the memory adapter has no
  `TableRef → table_id` mirror to resolve against, and loom's registers accept
  defs whose physical tables land later.

### The watermark — `stream.mv_watermark`

Migration `0042_mv_watermark.sql`:

```sql
create table stream.mv_watermark (
    mv              text   not null,
    source_table_id bigint not null,
    bucket          int    not null,
    next_offset     bigint not null check (next_offset >= 0),
    primary key (mv, source_table_id, bucket)
);
```

- **Keyed by the output, not the def name:** `mv` is the canonical output
  identifier `"{schema}.{name}"` (`mv_key(&TableRef)` helper). The watermark
  survives a def rename/redefine exactly when the output table is kept — which
  is exactly when resuming is correct — and ad-hoc (nameless) micro-batch runs
  work without a special case. An absent row reads as offset `0` (a fresh MV's
  first micro-batch processes the source's whole existing log — bootstrap and
  steady-state are one code path, which is what makes the convergence property
  testable).
- **Core concern** (five-layer stack, mirroring slice 0): trait `MvWatermarks`
  in `core/src/stream.rs` — `mv_watermarks(mv, source_table_id) ->
  BTreeMap<i32, i64>` and `advance_mv_watermark(mv, source_table_id, advances:
  &[WatermarkAdvance]) -> Result<()>` where `WatermarkAdvance { bucket, from,
  to }`; a CAS miss is `Conflict`. Memory fake + postgres adapter + testkit
  contract. The postgres helpers (`pg_mv_watermarks`,
  `pg_advance_mv_watermark`) are **executor-generic** so the commit tx calls
  them on its own `&mut PgConnection`, and use runtime `AssertSqlSafe`
  (mirroring `version_for_table` — no `.sqlx` regen required, which cloud
  sessions cannot run).
- **The CAS is the exactly-once mechanism:** `update stream.mv_watermark set
  next_offset = $to where mv = $1 and source_table_id = $2 and bucket = $3 and
  next_offset = $from` (insert-where-absent when `from = 0`); zero rows
  affected ⇒ `Conflict` ⇒ the enclosing output-commit tx rolls back.

### The delta read — a new Flight ticket, engine-resolved watermark

`MvDeltaTicket { mv: String, schema: String, name: String }` (JSON,
`deny_unknown_fields` — its required `mv` field keeps it disjoint from every
other ticket shape) joins `EngineTicket` (`engine-wire/src/flight.rs:149`),
decoded before the terminal file ticket. The engine's `do_get`
(`engine/src/flight.rs:240`) dispatches it to a new engine-serving primitive:

`mv_delta_scan(cp, catalog, pool, source_table, mv) -> (SchemaRef,
Vec<RecordBatch>)` — mirroring `consolidate_stream`'s read shape:

1. Resolve `live_table_id(source)`; require a declared `kind = 'log'` stream
   table (`stream_meta`) — anything else is a deterministic error (worker
   abandon). CDC sources are deferred (see Non-goals).
2. Read the watermark map (`pg_mv_watermarks`).
3. Under `lock_table` (the flush/GC/consolidate advisory lock — so a flush
   committing mid-read cannot move rows between the file and inline tiers and
   make the union double-read or drop them): `files_with_stats` +
   `read_files_as_batches` ∪ `inline_live_batch_full`.
4. One DataFusion pass: project `[user_cols…, loom_change_kind, loom_bucket,
   loom_offset]`, filter `OR`-per-bucket `loom_bucket = b AND loom_offset >=
   from_b` (`from_b = 0` for buckets absent from the map), `ORDER BY
   (loom_bucket, loom_offset)`. Collect and stream as Arrow IPC **with framing
   columns** — this is an internal data-plane read, not a logical read; the
   worker needs the framing to derive the CAS bounds.

The worker consumes it via a new `FlightTableClient::fetch_mv_delta(mv, schema,
name)` (beside `fetch`, `engine-wire/src/flight.rs:274`).

### The worker handler — `handle_stream_mv`

New `worker/src/stream_mv.rs`, registered in `main.rs` beside the other kinds
(`worker/src/main.rs:99`). Per job (payload `StreamMvJob { source, output,
buckets, sql, run_id }`):

1. `mark_running_if_tracked` (reuse `worker/src/transform.rs:112`).
2. `fetch_mv_delta` — the framed delta. **Empty delta:** call
   `CommitMicroBatch` with no rows and no advances; the engine just marks the
   run succeeded (`snapshot_id` absent) — a debounced spurious wakeup is a
   cheap no-op.
3. Derive per-bucket bounds from the framing: `from = min(loom_offset)`,
   `to = max(loom_offset) + 1` per `loom_bucket` (valid by gapless visibility —
   see key insight #1).
4. **Strip the `loom_*` framing columns**, register the user-column delta in a
   fresh `SessionContext` under the source's table `name` (the physical
   transform's registration convention) — the MV SQL never sees framing.
5. Run the SQL, `infer_columns` on the result schema, collect, encode as one
   Arrow IPC stream (the `WriteObject`/`WriteDelta` client-side convention —
   micro-batch outputs are inline-tier-sized by construction; a Parquet spill
   path for oversized outputs is deferred).
6. One `EngineControl::CommitMicroBatch` RPC. Error taxonomy mirrors
   `run_wire_transform`: decode/SQL/planning errors abandon; wire/store errors
   retry with `WorkerTuning::backoff`; a CAS `Conflict` (`aborted` status)
   abandons with a "superseded: watermark advanced concurrently" error —
   deterministic, benign, and documented (the covering run produced the rows).
7. `report_run_failure` best-effort on failure (reuse, `transform.rs:213`).

### The commit — `EngineControl::CommitMicroBatch`, one transaction

New RPC (proto beside `CommitTransformRequest`,
`engine-wire/proto/engine_control.proto:84`):

```proto
message MvAdvance { int32 bucket = 1; int64 from = 2; int64 to = 3; }
message CommitMicroBatchRequest {
  string mv            = 1;  // watermark key: "{output_schema}.{output_name}"
  string source_schema = 2;
  string source_name   = 3;
  string schema        = 4;  // output TableRef
  string name          = 5;
  int32  buckets       = 6;  // output log declaration bucket count
  bytes  ipc           = 7;  // N-row Arrow IPC result; empty => no rows
  string columns_json  = 8;  // serde_json Vec<ColumnSpec> — inferred output schema
  string lineage_json  = 9;  // serde_json LineageWire (offset ranges in payload)
  repeated MvAdvance advances = 10;
  optional string run_id      = 11;
}
message CommitMicroBatchResponse { optional int64 snapshot_id = 1; }
```

The engine handler resolves the source's `table_id` and calls a new postgres
entrypoint `inline_append_mv` — a thin public wrapper that widens the existing
`inline_append_decl` (`iceberg_inline.rs:448`, precedented: it grew `jobs` for
the action path) with an `mv: Option<&MvCommit>` extra executed **on the same
transaction** after the row inserts:

- the landing itself runs with `StreamDecl::Log(buckets)` — so the output table
  is **declared a log stream table in the same tx** (first commit declares it;
  `reconcile_stream_mode`'s existing guards apply: a pre-existing batch table as
  output is a `Validation` error, a bucket-count mismatch a `Conflict`), every
  output row is stamped `+I` with a fresh `(bucket, offset)`, the byte-trigger
  flush arms, and `pg_fire_data_triggers` fires for the output table
  (**composability**);
- `pg_advance_mv_watermark` CAS-advances every touched bucket (`Conflict`
  aborts the tx);
- `pg_mark_run_succeeded` (`postgres/src/transforms.rs:241`) stamps the run,
  exactly as `CommitTransform` does in-tx;
- lineage: inputs `[source]`, outputs `[output]`, payload
  `{ "sql", "mv", "offsets": { bucket: { "from", "to" } } }` — the consumed
  offset range is durable provenance, per the umbrella spec's lineage framing.

Empty request (`ipc` empty, no advances): no landing, no declare; just
`finish_run(run_id, Succeeded { snapshot_id: 0 })` (the `consolidate_stream`
no-op precedent for id `0`).

### Why the output is subscribable (without slice 3)

The MV's output is a **declared log stream table**: its inline rows carry
`loom_change_kind = '+I'`, `loom_bucket`, gapless `loom_offset`; its flushed
Parquet keeps that framing (`include_framing`, `iceberg_flush.rs:155`); its
events *are* its changelog (the umbrella's log-table model). Slice 3's
log-table subscribe follow-on reads it with zero work here; nothing in this
slice imports or awaits subscribe code. The e2e proves subscribability
structurally: the output's physical rows are complete, framed, and
per-bucket-gapless from offset 0.

## Non-regression

Additive except four narrow touch points, each with existing back-compat
posture:

- **`TransformBody` gains a variant.** Serde-tagged enum, additive. An *old*
  binary decoding a new `"microbatch"` body hits the established poison-body
  skip-and-warn paths (trigger scan, schedule claim, define scan) — degraded,
  never failing unrelated commits. Existing `Physical`/`Typed` wire shapes are
  byte-identical.
- **`inline_append_decl` widens by one `Option` param** (all existing callers
  pass `None` — byte-identical behavior, compiler-guided sweep).
- **`EngineTicket::decode` gains one JSON arm** before the terminal file
  ticket; `deny_unknown_fields` disjointness (the required `mv` field) keeps
  every existing ticket's routing unchanged.
- **`KNOWN_JOB_KINDS` + the worker dequeue list gain `"stream_mv"`.**

Existing suites (`transform_e2e`, `data_triggers`, `stream_*`, contract tests)
stay green and unmodified except mechanical enum-match additions.

## Testing

Tests are `rust_test` / `loom_fixture_test` integration targets only — never
inline `#[cfg(test)]`; new fixture tests use `loom_fixture_test`
(`src/control-plane/postgres/defs.bzl`) with BUCK targets mirroring named
siblings.

- **Core** (`rust_test`): `MicroBatch` serde round-trip; `to_job` kind/payload;
  `TriggerNode::resolve` arm; `validate_transform_def` rejections (buckets < 1,
  empty SQL, source == output); `mv_key`.
- **Watermark contract** (testkit, both adapters): absent reads as empty map;
  `from = 0` insert; CAS advance; CAS miss ⇒ `Conflict`; independence across
  `mv` keys, source tables, and buckets.
- **Delta scan** (engine fixture): seed a log table (inline + flushed rows,
  proving the files∪inline union); scan from zero returns all rows framed and
  `(bucket, offset)`-ordered; advance the watermark, scan returns only the
  tail; a non-stream source errors deterministically.
- **Commit wire** (engine fixture): `CommitMicroBatch` lands framed `+I` rows,
  declares the output a log stream table, advances the watermark, marks the run
  succeeded — atomically; a stale `from` ⇒ `aborted` and **nothing** commits
  (no output rows, watermark unchanged); the empty commit marks the run
  succeeded and writes nothing.
- **Convergence e2e** (worker fixture, the headline): an MV over a source log
  stream, driven through `define_transform` + `submit_run` + `handle_stream_mv`
  across **multiple micro-batches** (including a flush between them),
  converges to the batch-equivalent result (the same SQL over the whole
  source); the output's physical rows are framed, `+I`, per-bucket gapless from
  0 (**subscribable**); a re-run on an unchanged source is a no-op (no
  duplicates).
- **Composability + trigger e2e** (fixture): a source commit fires a
  data-triggered MV def (debounced); MV₁'s output commit fires MV₂
  (`on_input_commit` over MV₁'s output); an MV₁→MV₂→MV₁ define is rejected as a
  cycle at define time.

## Global constraints (loom-specific, carry into the plan)

- **Tests are `rust_test` / `loom_fixture_test` integration targets only.** New
  fixture tests MUST use `loom_fixture_test`
  (`src/control-plane/postgres/defs.bzl`), not a bare `rust_test`. The
  `no-inline-tests` prek hook fails the build on any inline `#[test]`.
- **No compile-time sqlx for the new SQL.** The watermark queries use runtime
  `sqlx::query(AssertSqlSafe(...))`, mirroring `version_for_table` /
  `identity_for_table` — cloud sessions cannot run `tools/sqlx-prepare.sh`
  (initdb-as-root). Do not touch existing `query!` sites.
- **Clippy is strict (pedantic + restriction):** no `unwrap`/`expect`/
  `indexing_slicing`/`panic`/`todo` in production lib/bin code;
  `#[expect(lint, reason = "...")]` for justified local exceptions. Test code
  is exempted from the panic-safety lints via the test macros.
- **Run prek before every commit:** `buck2 run //tools:prek -- run --all-files`
  (`git add` new files first). Markdown ends with exactly one trailing newline,
  no trailing whitespace.
- **The engine owns Postgres; the worker stays zero-pool.** All new
  reads/writes cross the wire (Flight ticket + one `EngineControl` RPC); no
  postgres dep may enter the worker's BUCK closure.
- **Framing stays hidden from logical reads.** The delta ticket is an internal
  data-plane surface; the MV SQL runs over stripped user columns; the output's
  framing is stamped engine-side and excluded from logical schemas by the
  existing `is_reserved` filter.
- **No slice-3 dependency.** Nothing imports subscribe code; subscribability is
  proven structurally (declared stream table, framed, gapless).

## Non-goals (deferred)

- **True incremental operators / retract-correct aggregate MVs** — v1 is
  per-delta micro-batch; whole-history aggregations are not batch-equivalent
  under append-only output. Needs −U/+U-emitting MV output (a CDC-flavored
  output table) and per-operator state (umbrella:
  `fut-stream-incremental-join` family).
- **CDC-table sources** — the delta of a CDC source is its changelog (files ∪
  full inline incl. `−U`); exposing event kinds to the MV SQL vocabulary is a
  follow-on. v1 sources are declared **log** tables — which MV outputs are, so
  composition is closed.
- **Multi-source MVs / stream-stream joins** — slice 5 (`road-stream-joins`)
  rides this machinery.
- **Backfill/replay control** (watermark reset API, `from`-offset registration)
  — v1 always starts at 0 and resumes from the committed watermark; rebuild =
  new output table.
- **Parquet spill for oversized micro-batch outputs** — v1 output lands on the
  inline tier (micro-batches are delta-sized); a framed direct-Parquet output
  path folds into `fut-stream-framing-write-paths`.
- **Stats-pruned delta file scans / streaming (non-collected) scans** — the v1
  scan reads live files whole (the `consolidate_stream` posture); pruning by
  `loom_offset` file stats and streaming execution are follow-ons
  (`fut-transform-followups`).
- **Watermark-aware retention** — `gc_table` stays age-based; a lagging MV
  whose unread tail is expired is a documented operational caveat (retention
  must exceed MV lag), shared with slice 3.
- **`/admin/views` sugar surface** — MVs register through the transforms admin
  surface in v1.

## Interfaces (names the plan consumes)

- Consumes: `TransformBody`/`TransformDef`/`to_job`/`TriggerNode::resolve`/
  `validate_transform_def`/`validate_no_trigger_cycle`/`Transforms`
  (`core/src/transforms.rs:40`/`:111`/`:64`/`:295`/`:261`/`:325`/`:398`);
  `STREAM_CONSOLIDATE_JOB_KIND` shape (`core/src/stream_consolidate_job.rs:8`);
  `KNOWN_JOB_KINDS` (`core/src/queue.rs:31`); `BucketOffsets`/`StreamMeta`/
  `StreamKind` (`core/src/stream.rs:87`/`:72`/`:14`); `pg_stream_meta`/
  `reconcile_stream_mode`/`StreamDecl` (`postgres/src/stream.rs:367`/`:55`/
  `:17`); `inline_append_decl` (+ its trigger-firing seams)
  (`iceberg_inline.rs:448`/`:746`/`:1234`); `inline_live_batch_full`
  (`iceberg_inline.rs:1413`); `read_files_as_batches` (`iceberg_read.rs:25`);
  `lock_table` (`iceberg_flush.rs:414`); `live_table_id`;
  `pg_mark_run_succeeded` (`postgres/src/transforms.rs:241`);
  `pg_fire_data_triggers` (`postgres/src/transforms.rs:137`); the
  `consolidate_stream` read/fold shape (`engine-serving/src/consolidate.rs:57`);
  `EngineTicket`/`FlightTableClient`/`FlightSqlClient`
  (`engine-wire/src/flight.rs:149`/`:258`/`:323`); `GrpcQueueClient`
  (`commit_transform` at `engine-wire/src/client.rs:410` as the new method's
  template); the engine `do_get` dispatch (`engine/src/flight.rs:240`) and
  service handlers (`engine/src/service.rs:276`); worker dispatch + kinds
  (`worker/src/main.rs:84`/`:99`); `mark_running_if_tracked`/
  `report_run_failure`/`TransformCtx` (`worker/src/transform.rs:112`/`:213`/
  `:26`); `datafusion_io::{register_batches, infer_columns}`; test harnesses
  `loom_test_flight::spawn_engine_uds` + `loom_test_seed::local_sql_catalog`
  (`worker/tests/transform_e2e.rs:7`).
- Produces (later plan tasks rely on these EXACT names/types):
  - `TransformBody::MicroBatch { source: TableRef, output: TableRef, buckets:
    i32, sql: String }` (serde tag `"microbatch"`), `core/src/transforms.rs`.
  - `pub const STREAM_MV_JOB_KIND: &str = "stream_mv"` + `pub struct
    StreamMvJob { source: TableRef, output: TableRef, buckets: i32, sql:
    String, run_id: Option<uuid::Uuid> }` in `core/src/stream_mv_job.rs`,
    re-exported at the core root and added to `KNOWN_JOB_KINDS`.
  - `pub trait MvWatermarks { async fn mv_watermarks(&self, mv: &str,
    source_table_id: i64) -> Result<std::collections::BTreeMap<i32, i64>>;
    async fn advance_mv_watermark(&self, mv: &str, source_table_id: i64,
    advances: &[WatermarkAdvance]) -> Result<()>; }` + `pub struct
    WatermarkAdvance { pub bucket: i32, pub from: i64, pub to: i64 }` + `pub fn
    mv_key(output: &TableRef) -> String` in `core/src/stream.rs`; memory +
    postgres impls; testkit `mv_watermarks_contract`.
  - Migration `postgres/migrations/0042_mv_watermark.sql`
    (`stream.mv_watermark`); executor-generic `pub` helpers `pg_mv_watermarks`
    / `pg_advance_mv_watermark` (`AssertSqlSafe`) in `postgres/src/stream.rs`.
  - `MvDeltaTicket { mv: String, schema: String, name: String }` +
    `EngineTicket::MvDelta` (`engine-wire/src/flight.rs`);
    `FlightTableClient::fetch_mv_delta(&self, mv: String, schema: String, name:
    String) -> Result<Vec<RecordBatch>>`.
  - `pub async fn mv_delta_scan(cp: &PgControlPlane, catalog: &SqlCatalog,
    pool: &PgPool, table: &TableRef, mv: &str) -> Result<(SchemaRef,
    Vec<RecordBatch>), EngineServingError>` in
    `engine-serving/src/mv_delta.rs`.
  - Proto `rpc CommitMicroBatch (CommitMicroBatchRequest) returns
    (CommitMicroBatchResponse)` + `MvAdvance` message;
    `GrpcQueueClient::commit_micro_batch(...) -> Result<Option<i64>>`.
  - `pub struct MvCommit { pub mv: String, pub source_table_id: i64, pub
    advances: Vec<WatermarkAdvance>, pub run_id: Option<uuid::Uuid> }` + `pub
    async fn inline_append_mv(pool, table, columns, batch, lineage,
    flush_threshold, buckets: i32, mv: &MvCommit) -> Result<SnapshotId>` in
    `postgres/src/iceberg_inline.rs` (via `inline_append_decl` widened with
    `mv: Option<&MvCommit>`).
  - `pub async fn handle_stream_mv(ctx: &StreamMvCtx, job: Job) ->
    Result<(), JobFailure>` + `pub struct StreamMvCtx { pub control:
    GrpcQueueClient, pub table: FlightTableClient, pub worker_tuning:
    WorkerTuning }` in `worker/src/stream_mv.rs`, dispatched from
    `worker/src/main.rs`.
