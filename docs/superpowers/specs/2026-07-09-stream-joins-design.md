# Stream engine — Stream joins / delta-join analog (slice 5) Design

> **Status:** design (direction). This spec makes `road-stream-joins`
> build-ready as its own per-slice spec (it currently points at the umbrella
> `2026-07-06-stream-engine-design`). A separate work agent writes the
> implementation plan from it and builds it.
>
> **Build dependency:** slice 4 (`road-stream-continuous`,
> `docs/superpowers/specs/2026-07-09-stream-continuous-design.md`, plan
> `docs/superpowers/plans/2026-07-09-stream-continuous.md`) is specced but NOT
> yet implemented, and **must land first** — this slice is a strict extension
> of its micro-batch MV machinery. References into slice-4-produced code
> (`TransformBody::MicroBatch`, `StreamMvJob`, `MvWatermarks`, `mv_delta_scan`,
> `CommitMicroBatch`, `handle_stream_mv`, …) are therefore **symbolic
> (name-based)**; references into shipped code are real, verified file:line.

## Goal

**The enriched stream.** A micro-batch MV whose SQL joins the **delta of a
source stream** against the **current state of a second table** — Fluss's
delta-join shape, on loom's grain. Two v1 flavors, both `Δ(A) ⋈ state(B)`
emitted as an append (`+I`) log:

- **State-join** — the worker fetches B's *full* folded current state and
  registers it beside A's delta; the SQL joins them with any predicate. The
  safe default.
- **Lookup-join** — the delta-join analog: the worker extracts the distinct
  join-key values from A's delta and fetches only B's current-state rows whose
  key is in that set — a **batched point-lookup against B's merged
  (inline ∪ Iceberg) current state**, carried as a keyed enrich ticket. Join
  state lives in storage, not in the worker — exactly Fluss's headline
  property, amortized to micro-batch grain.

The output is a declared log stream table committed through slice 4's
`CommitMicroBatch` — offset-framed, flushable, watermark-CAS'd, composable,
and (once slice 3 lands) subscribable. **True bilateral incremental joins —
retract-correct re-emission when B changes — stay deferred
(`fut-stream-incremental-join`); a dedicated PK index stays deferred
(`fut-stream-pk-index`)** — v1's lookup is a key-predicated merge-on-read scan
behind a ticket contract the index can later re-back (see *The lookup
mechanism* below).

## Context — what ships today, and what slice 4 produces

**Shipped (verified):**

- **The folded current-state read — the enrich side already exists.**
  `build_serving_provider` (`engine-serving/src/serving.rs:74`) serves any
  table's logical current contents: for a CDC table it applies the
  identity-aware merge-on-read fold (`Precedence::Offset` with the table's
  declared engine — `Precedence` at `serving.rs:256`, `offset_precedence` at
  `:296`, `build_merge_view` at `:325`), honoring the per-table `MergeEngine`
  (`core/src/stream.rs:27`; LastRow/FirstRow/Versioned, `StreamMeta` at
  `:72`); for a plain or log table it serves the files ∪ inline union.
  Framing/reserved columns are hidden from this logical read. The transform
  worker already consumes exactly this read for its inputs: `run_wire_transform`
  fetches `SELECT *` per input through the engine's SQL serving path
  (`worker/src/transform.rs:256-298`, `select_all_sql` at `:41`,
  `TransformCtx.sql: FlightSqlClient` at `:26`) — ungoverned internal data
  plane, system-privileged, the posture the enrich read inherits.
- **The engine data plane and its extensible ticket chain.** `EngineTicket`
  (`engine-wire/src/flight.rs:149`) decodes `deny_unknown_fields`-disjoint JSON
  shapes in a fall-through chain (`decode` at `:206`); the engine dispatches in
  `do_get` (`engine/src/flight.rs:240`). `FlightTableClient` (`:258`, `fetch`
  at `:274`) is the worker's zero-pool client.
- **The stream substrate** — gapless per-`(table, bucket)` offsets
  (`pg_allocate_offset`, `postgres/src/stream.rs:254`; trait
  `core/src/stream.rs:87`), declared log/CDC tables
  (`reconcile_stream_mode`, `postgres/src/stream.rs:55`; `pg_stream_meta`
  `:367`), framed inline appends (`inline_append_decl`,
  `iceberg_inline.rs:448`) that fire data triggers in-tx (`:746`/`:1234` →
  `pg_fire_data_triggers`, `postgres/src/transforms.rs:137`), framed flush
  (`include_framing`, `iceberg_flush.rs:155`; `flush_locked_cdc` `:193`;
  `lock_table` `:414`), and CDC declaration (`land_cdc`/`CdcDecl`,
  `iceberg_landing.rs:148`/`:45`).
- **The transforms registry** — `TransformBody` (`core/src/transforms.rs:40`),
  `to_job` (`:64`), `validate_transform_def` (`:261`), `TriggerNode::resolve`
  (`:295`), `validate_no_trigger_cycle` (`:325`) — the enum-arm extension
  points slice 4 already exercises.

**Slice-4-produced (consumed by name; NOT yet in the tree):**

- `TransformBody::MicroBatch` — the single-source MV body; `STREAM_MV_JOB_KIND`
  + `StreamMvJob` (`core/src/stream_mv_job.rs`); the worker handler
  `handle_stream_mv` + `StreamMvCtx` (`worker/src/stream_mv.rs`).
- `MvWatermarks`/`WatermarkAdvance`/`mv_key` + `stream.mv_watermark` +
  `pg_mv_watermarks`/`pg_advance_mv_watermark` — the per-`(mv, source, bucket)`
  offset watermark, CAS-advanced inside the output-commit tx.
- `MvDeltaTicket`/`EngineTicket::MvDelta` + `FlightTableClient::fetch_mv_delta`
  + `mv_delta_scan` (`engine-serving/src/mv_delta.rs`) — the framed,
  watermark-bounded delta read of a declared log source.
- `EngineControl::CommitMicroBatch` + `GrpcQueueClient::commit_micro_batch` +
  `MvCommit`/`inline_append_mv` — the one-transaction output commit: framed
  `+I` landing on the declared output log table, watermark CAS, run success,
  lineage; empty commit = no-op success.

**Slice 3 (`road-stream-subscribe`) stays a non-dependency** — nothing here
imports subscribe code; the output's subscribability is structural, exactly as
slice 4 frames it.

## The key insight

**Both join flavors are one new fetch on the slice-4 loop.** Slice 4's MV run
is: fetch framed delta → strip framing → register under the source name → run
SQL → `CommitMicroBatch`. A join MV inserts one step — *also register the
enrich table's current state* — and changes nothing else:

- The **delta side** is slice 4's `fetch_mv_delta`/`mv_delta_scan`, unchanged.
- The **state side** is the read the transform worker already performs for
  every input (`build_serving_provider`'s folded current state) — v1 merely
  gives it a ticket that can carry an optional key predicate.
- The **commit is `CommitMicroBatch`, byte-for-byte unchanged**: the watermark
  advances on the *source* only (enrichment state is read, never consumed);
  the output declaration, framing, CAS, run bookkeeping, and trigger firing
  are identical. **No new RPC, no proto change, no migration, no new job kind,
  no sqlx change** — the whole slice is one `TransformBody` variant, one
  ticket + scan, and one worker branch.

Consequently the exactly-once, composability, and convergence machinery is
inherited rather than rebuilt: a duplicate/concurrent run still loses the
watermark CAS and rolls back atomically; the output commit still fires data
triggers, so join MVs compose with plain MVs.

## Architecture

### Registration — `TransformBody::MicroBatchJoin`

A new variant beside slice 4's `MicroBatch` (serde tag `"microbatch_join"`):

```rust
TransformBody::MicroBatchJoin {
    source: TableRef,        // the driving stream (declared log table) — watermarked
    enrich: TableRef,        // the state side — read at processing time, never watermarked
    on: Option<LookupOn>,    // Some => lookup-join (keyed fetch); None => state-join (full fetch)
    output: TableRef,        // declared-on-first-commit log stream table
    buckets: i32,
    sql: String,             // sees `source.name` (the delta) and `enrich.name` (the state)
}

pub struct LookupOn {
    pub source_col: String,  // the delta column whose distinct values are the lookup keys
    pub enrich_col: String,  // the enrich column the keys probe (best: B's identity)
}
```

Registration, listing, deletion, cron/data triggers, manual runs — all the
existing transforms admin surface, via the body enum, as slice 4 established.

- `to_job` gains a `MicroBatchJoin` arm emitting the **same**
  `STREAM_MV_JOB_KIND`; `StreamMvJob` widens with two `#[serde(default)]`
  fields (`enrich: Option<TableRef>`, `on: Option<LookupOn>`) — a slice-4
  payload decodes unchanged (`None`/`None`), so no new queue kind, no new
  worker dispatch entry, no dequeue-list change.
- `TriggerNode::resolve` gains an arm: `inputs = [source, enrich]`,
  `output = Some(output)`. **Both edges are trigger edges and cycle edges**:
  an enrich commit debounce-wakes the MV — whose run then no-ops through
  slice 4's empty-delta path if the source has nothing new (deliberate: it
  keeps `resolve`'s single inputs-list semantics, and makes enrich→output
  cycles structurally impossible via the existing define-time cycle check).
  Enrichment freshness is thereby *at least* per-source-commit and typically
  better; it is never event-time-aligned (see the correctness contract).
- `validate_transform_def` rejects: `buckets < 1`; empty `sql`;
  `source == output`; `enrich == output` (reading your own output as state is
  a feedback footgun); `source == enrich` (v1 — the two registration names
  must be distinct; self-enrichment via aliasing is deferred);
  `source.name == enrich.name` (same-name/different-schema would collide in
  the DataFusion registration, mirroring `run_wire_transform`'s
  ambiguous-name rejection, `worker/src/transform.rs:236-242`); an `on` with
  an empty `source_col`/`enrich_col`.
- As in slice 4, whether `source` is a declared log table (and `enrich` an
  existing table) is a **run-time** deterministic abandon, not define-time
  validation.

### The enrich read — `MvEnrichTicket` + `mv_enrich_scan`

One new JSON ticket in the `EngineTicket` fall-through chain (beside slice 4's
`MvDeltaTicket`, before the terminal file ticket; its required
`enrich_schema`/`enrich_name` fields keep it `deny_unknown_fields`-disjoint
from every other shape):

```rust
MvEnrichTicket {
    enrich_schema: String,
    enrich_name: String,
    key: Option<String>,               // None => full state (state-join)
    keys: Vec<serde_json::Value>,      // distinct lookup keys (empty when key is None)
}
```

The engine's `do_get` (`engine/src/flight.rs:240`) dispatches it to a new
engine-serving primitive:

`mv_enrich_scan(catalog, table, key, serving_store) -> (SchemaRef,
Vec<RecordBatch>)` (no Postgres pool — `build_serving_provider` and the
`Catalog` trait cover it, `execute_query`'s exact dependency set):

1. Resolve the table's serving provider via `build_serving_provider`
   (`serving.rs:74`) — the **folded current state**: CDC tables fold per their
   declared `MergeEngine`, log/plain tables serve files ∪ inline. Framing and
   reserved columns are hidden — the enrich read is a **logical** read (unlike
   the framed delta read).
2. If the provider is absent (live-but-empty table), return the declared
   logical schema with zero rows (`logical_arrow_schema` over the mirror's
   columns — the `run_wire_transform` empty-input posture,
   `worker/src/transform.rs:286-292`). An *unknown* table is a deterministic
   error (worker abandon).
3. `key: Some((col, keys))` ⇒ apply `col IN (keys…)` as a DataFusion filter
   over the provider, coercing each JSON scalar to the column's Arrow type
   (integer and Utf8 key columns in v1; any other key type, or a
   non-coercible value, is a deterministic `InvalidArgument`). Filtering the
   *folded* state is correct for **any** column — the predicate applies above
   the merge view. When `col` is the CDC identity, DataFusion may additionally
   push the predicate below the fold (safe: identity is the fold's
   partition key) down to Parquet stats pruning on the base files — the
   optimization, never the correctness.
4. Collect and stream as Arrow IPC (user columns only).

The worker consumes it via a new `FlightTableClient::fetch_mv_enrich(ticket)`
(beside `fetch`, `engine-wire/src/flight.rs:274`). `StreamMvCtx` is untouched
— the enrich read rides the `FlightTableClient` it already holds.

### The worker — one branch in `handle_stream_mv`

Slice 4's handler (`worker/src/stream_mv.rs`, symbolic) grows one step between
"strip framing" and "run the SQL". When `job.enrich` is `Some`:

1. Derive the key set if `job.on` is `Some`: the **distinct** values of
   `on.source_col` in the delta's user columns, as JSON scalars. An empty
   delta never reaches here (slice 4's empty-commit path returns first). A
   missing/unsupported-type column is a deterministic abandon. If the distinct
   count exceeds `MAX_LOOKUP_KEYS` (`10_000`), **fall back to the full-state
   fetch** — a superset is always correct; the cap keeps the ticket bounded.
2. `fetch_mv_enrich` — keyed or full.
3. Register the enrich batches under `enrich.name` (empty ⇒
   `register_empty_table` with the returned schema), beside the delta
   registered under `source.name`.

Everything downstream — SQL, `infer_columns`, IPC encode, `CommitMicroBatch`
with the source-derived `advances`, the error taxonomy (decode/SQL abandon,
wire retry, CAS-conflict "superseded" abandon) — is slice 4's code, unchanged.
Lineage: `inputs = [source, enrich]`, `outputs = [output]`, payload gains
`"enrich"` and (when keyed) `"on"` beside slice 4's
`{"sql", "mv", "offsets"}` — carried in the existing `lineage_json` field, no
proto change.

### The lookup mechanism — why a predicated scan, not an index (v1)

`fut-stream-pk-index` (a real PK index spanning inline + Iceberg) stays
deferred, on evidence from what exists:

- **Correctness never needs it.** The folded current state is already
  correctly servable for every table kind (`build_serving_provider`,
  `serving.rs:74`, exercised by the shipped merge-on-read + consolidate
  suites); a key predicate over it is exact.
- **Micro-batch grain amortizes the lookup.** Fluss's per-record async
  point-lookup exists because Flink probes per event at stream rates; loom's
  v1 join probes **once per micro-batch** with the batch's whole key set — a
  single `IN`-predicated scan, where identity-key predicates can prune Parquet
  by stats and the inline tier is Postgres-indexed-adjacent already. At loom's
  scale the scan is the right cost point.
- **The ticket is the index's future seam.** `MvEnrichTicket` *is* the
  point-lookup API — `(table, key column, key set) → current-state rows`.
  `fut-stream-pk-index` re-backs `mv_enrich_scan` with an index probe without
  touching the ticket, the worker, or the SQL contract. v1 deliberately ships
  the contract with the honest backing.

### Correctness / convergence contract (mirroring slice 4's honesty)

- **Processing-time enrichment.** Each source event is joined against B's
  state *as observed at that micro-batch's execution* — Fluss delta-join /
  Flink lookup-join semantics, **not** an event-time temporal join. A source
  event and B's state are read at different instants and B is not frozen at
  the source event's offset time; a B update lands in **subsequent** batches'
  output only. Already-emitted rows are never retracted or re-emitted —
  retract-correct bilateral incrementality is `fut-stream-incremental-join`.
- **Convergence.** The v1 SQL contract is *per-delta in the source argument*:
  `Q(Δ₁ ∪ … ∪ Δₙ, B) = Q(Δ₁, B) ∪ … ∪ Q(Δₙ, B)` for row-wise join SQL
  (equijoin enrichment + projection/filter; `LEFT JOIN` for keep-unmatched is
  fine — the keyed fetch is a superset of matching keys, so unmatched source
  rows join NULLs identically under either fetch flavor). When B is quiescent
  across the batches, the output converges to the batch-equivalent
  `Q(A, B)` — the testable property. Aggregations over the joined stream stay
  deferred exactly as in slice 4.
- **The `on` contract (documented, not parsed).** A keyed fetch returns B rows
  with `enrich_col ∈` the delta's `source_col` values. If the SQL's equijoin
  is on exactly those columns, keyed ≡ full-fetch results. v1 keeps SQL opaque
  (the slice-4 posture) and does **not** verify `on` against the SQL — a
  mismatched `on` silently drops join partners, the same trust class as wrong
  SQL. `on: None` is the always-correct default; the e2e proves keyed ≡ full
  for the supported pattern.
- **Inherited from slice 4:** per-bucket ordering of the output, no
  cross-bucket transactionality, exactly-once *effect* via the watermark CAS
  under the at-least-once queue, at-most-one-pending debounce, empty-delta
  no-op.

## Non-regression

Additive except three narrow touch points, each with slice-4-precedented
back-compat posture:

- **`TransformBody` gains a variant** — serde-tagged, additive; old binaries
  hit the established poison-body skip-and-warn paths. `Physical`/`Typed`/
  `MicroBatch` wire shapes are byte-identical.
- **`StreamMvJob` widens by two `#[serde(default)]` `Option` fields** — a
  slice-4 payload (no `enrich`/`on` keys) decodes to `None`/`None` and
  `handle_stream_mv`'s non-join path is byte-identical.
- **`EngineTicket::decode` gains one JSON arm** before the terminal file
  ticket; required-field disjointness keeps every existing ticket's routing
  unchanged (pinned by `engine:ticket-errors` staying green).

No migration, no proto change, no new job kind, no `.sqlx` change. Existing
suites (`transform_e2e`, slice 4's MV suites, `stream_*`, ticket tests) stay
green, unmodified except mechanical enum-match additions.

## Testing

Tests are `rust_test` / `loom_fixture_test` integration targets only — never
inline `#[cfg(test)]`; new fixture tests use `loom_fixture_test`
(`src/control-plane/postgres/defs.bzl`) with BUCK targets mirroring named
siblings.

- **Core** (`rust_test`): `MicroBatchJoin` serde round-trip; `to_job` emits
  `STREAM_MV_JOB_KIND` with `enrich`/`on` populated; `StreamMvJob`
  back-compat (slice-4 JSON without the new keys decodes); `resolve` arm
  (`inputs = [source, enrich]`); the `validate_transform_def` rejections.
- **Ticket** (`rust_test`): `MvEnrichTicket` encode/decode round-trip (keyed,
  unkeyed, empty-keys default); `EngineTicket::decode` routes it and every
  existing ticket unchanged.
- **Enrich scan** (engine fixture): seed a CDC table with inline + flushed
  history including updates and a delete (proving the fold and the union);
  unkeyed scan returns exactly the folded current state, user columns only;
  keyed scan returns only the requested keys' rows; empty table returns the
  declared schema with zero rows; unknown table and a non-coercible key error
  deterministically.
- **Lookup-join e2e** (worker fixture, the headline): orders (log source) ⋈
  customers (CDC enrich) with `on = (customer_id, id)`, driven through
  `define_transform` + `submit_run` + `handle_stream_mv` across multiple
  micro-batches with a flush between them — **the lookup-join emits the
  enriched stream**: framed `+I`, per-bucket gapless from 0, converging to the
  batch-equivalent join while customers is quiescent; a customer update
  enriches **subsequent** batches only (no retro re-emission); keyed and
  unkeyed (`on: None`) runs produce identical enriched rows; a rerun on an
  unchanged source is a no-op.
- **Composability + trigger e2e** (fixture): an enrich-table commit
  debounce-wakes the join MV and no-ops (empty source delta, watermark
  unchanged); a join MV's output feeds a plain slice-4 MV
  (`on_input_commit`), which fires; an enrich-edge cycle
  (MV₁ output = MV₂ enrich, MV₂ output = MV₁ source) is rejected at define
  time.

## Global constraints (loom-specific, carry into the plan)

- **Slice 4 must be fully landed first** — the plan presumes every
  slice-4-produced interface above exists under its exact name; its file:line
  references into that code are symbolic.
- **Tests are `rust_test` / `loom_fixture_test` integration targets only.**
  New fixture tests MUST use `loom_fixture_test`
  (`src/control-plane/postgres/defs.bzl`), not a bare `rust_test`. The
  `no-inline-tests` prek hook fails the build on any inline `#[test]`.
- **No new SQL.** This slice adds no migration and touches no
  `query!`/`query_scalar!` site — no `tools/sqlx-prepare.sh` run needed. If an
  implementation detail ends up needing new adapter SQL, use runtime
  `sqlx::query(AssertSqlSafe(...))` mirroring `version_for_table`
  (`postgres/src/ontology.rs:709`) — cloud sessions cannot regenerate `.sqlx`.
- **Clippy is strict (pedantic + restriction):** no `unwrap`/`expect`/
  `indexing_slicing`/`panic`/`todo` in production lib/bin code;
  `#[expect(lint, reason = "...")]` for justified local exceptions. Test code
  is exempted from the panic-safety lints via the test macros.
- **Run prek before every commit:** `buck2 run //tools:prek -- run --all-files`
  (`git add` new files first). Markdown ends with exactly one trailing
  newline, no trailing whitespace.
- **The engine owns Postgres; the worker stays zero-pool.** The enrich read is
  a Flight ticket; the commit stays the single `CommitMicroBatch` RPC; no
  postgres dep may enter the worker's BUCK closure.
- **The enrich read is the internal, ungoverned data plane** — parity with the
  transform-input read (`worker/src/transform.rs:256-298`); governed
  change-feeds are slice 3's query-api surface, not this one.
- **Framing stays hidden where slice 4 hides it.** The delta read is framed
  (internal); the enrich read is logical (user columns only); the MV SQL sees
  neither table's `loom_*` columns; output framing is stamped engine-side.
- **`CommitMicroBatch` is consumed byte-for-byte unchanged.** Any temptation
  to widen the proto is a design smell here — enrichment carries no commit
  state (lineage rides `lineage_json`).
- **No slice-3 dependency.** Subscribability of the output stays structural.

## Non-goals (deferred)

- **True bilateral incremental / retract-correct joins**
  (`fut-stream-incremental-join`) — a B update re-emitting or retracting
  previously-joined output needs −U/+U-emitting MV output and per-operator
  state; v1 enrichment is processing-time, append-only.
- **A dedicated PK index across inline + Iceberg** (`fut-stream-pk-index`) —
  v1's lookup is a key-predicated merge-on-read scan behind `MvEnrichTicket`;
  the index later re-backs the same ticket.
- **Event-time / versioned temporal joins** (`FOR SYSTEM_TIME AS OF` the
  source event's time) — needs changelog time-travel on B; the durable
  changelog makes it *possible* later, not built here.
- **Delta×delta (windowed) joins** — joining two deltas within a batch window
  is a different operator; v1's state side is always full current state.
- **Multi-way joins / more than one enrich table** — v1 is exactly
  `source + enrich`; N-way enrichment is a follow-on widening
  (`Vec<(TableRef, Option<LookupOn>)>`) once the two-table shape is proven.
- **Self-enrichment (`source == enrich`)** — needs registration aliasing;
  rejected in v1.
- **Verifying `on` against the SQL's join predicate** — SQL stays opaque
  (slice-4 posture); `on` is a documented contract with a safe default.
- **CDC-table *sources*** — inherited slice-4 deferral; sources are declared
  log tables (which MV outputs are, so composition stays closed).
- **Governed/streaming enrich reads, Parquet spill, backfill control,
  watermark-aware retention** — all inherited slice-4 deferrals, unchanged.

## Interfaces (names the plan consumes)

- Consumes — **shipped (verified file:line)**:
  `TransformBody`/`to_job`/`TransformDef`/`validate_transform_def`/
  `TriggerNode::resolve`/`validate_no_trigger_cycle`/`Transforms`
  (`core/src/transforms.rs:40`/`:64`/`:111`/`:261`/`:295`/`:325`/`:398`);
  `KNOWN_JOB_KINDS` (`core/src/queue.rs:31`); `StreamKind`/`MergeEngine`/
  `StreamMeta`/`BucketOffsets`/`StreamTables` (`core/src/stream.rs:14`/`:27`/
  `:72`/`:87`/`:99`); `reconcile_stream_mode`/`pg_allocate_offset`/
  `pg_stream_meta` (`postgres/src/stream.rs:55`/`:254`/`:367`);
  `inline_append_decl` + trigger seams (`iceberg_inline.rs:448`/`:746`/
  `:1234`); `pg_fire_data_triggers` (`postgres/src/transforms.rs:137`);
  `land_cdc`/`CdcDecl` (`iceberg_landing.rs:148`/`:45`); `lock_table`/
  `include_framing`/`flush_locked_cdc` (`iceberg_flush.rs:414`/`:155`/`:193`);
  `build_serving_provider`/`Precedence`/`offset_precedence`/`build_merge_view`/
  `register_iceberg_table`/`execute_query`
  (`engine-serving/src/serving.rs:74`/`:256`/`:296`/`:325`/`:486`/`:828`);
  `consolidate_stream` (`engine-serving/src/consolidate.rs:57`);
  `EngineTicket`/`EngineTicket::decode`/`FlightTableClient`/`fetch`/
  `FlightSqlClient` (`engine-wire/src/flight.rs:149`/`:206`/`:258`/`:274`/
  `:323`); the engine `do_get` dispatch (`engine/src/flight.rs:240`) and
  `do_get_governed_sql` as its arm template (`:117`); `TransformCtx`/
  `handle_transform`/`select_all_sql`/the input read + ambiguous-name check
  (`worker/src/transform.rs:26`/`:54`/`:41`/`:256-298`/`:236-242`); worker
  dispatch (`worker/src/main.rs:88`/`:106`); `identity_for_table`/
  `version_for_table` (`postgres/src/ontology.rs:685`/`:709`);
  `datafusion_io::{register_batches, register_empty_table, infer_columns,
  logical_arrow_schema}` (`datafusion-io/src/scan.rs:125`/`:115`,
  `infer.rs:66`/`:52`); test harnesses `loom_test_flight::spawn_engine_uds` +
  `loom_test_seed::local_sql_catalog` (`worker/tests/transform_e2e.rs:7-8`).
- Consumes — **slice-4-produced (by name; symbolic until
  `road-stream-continuous` lands)**: `TransformBody::MicroBatch`;
  `STREAM_MV_JOB_KIND` + `StreamMvJob` (`core/src/stream_mv_job.rs`);
  `MvWatermarks`/`WatermarkAdvance`/`mv_key` + `pg_mv_watermarks`/
  `pg_advance_mv_watermark` + migration `0042_mv_watermark.sql`;
  `MvDeltaTicket`/`EngineTicket::MvDelta`/`FlightTableClient::fetch_mv_delta`;
  `mv_delta_scan` (`engine-serving/src/mv_delta.rs`); `CommitMicroBatch` proto
  + `GrpcQueueClient::commit_micro_batch`; `MvCommit`/`inline_append_mv`
  (`postgres/src/iceberg_inline.rs`); `handle_stream_mv`/`StreamMvCtx`
  (`worker/src/stream_mv.rs`) and its e2e harness/BUCK target.
- Produces (later plan tasks rely on these EXACT names/types):
  - `pub struct LookupOn { pub source_col: String, pub enrich_col: String }`
    in `core/src/stream_mv_job.rs`, re-exported at the core root.
  - `TransformBody::MicroBatchJoin { source: TableRef, enrich: TableRef, on:
    Option<LookupOn>, output: TableRef, buckets: i32, sql: String }` (serde
    tag `"microbatch_join"`), `core/src/transforms.rs`.
  - `StreamMvJob` widened with `#[serde(default)] pub enrich:
    Option<TableRef>` and `#[serde(default)] pub on: Option<LookupOn>`; `pub
    const MAX_LOOKUP_KEYS: usize = 10_000` in `core/src/stream_mv_job.rs`.
  - `MvEnrichTicket { enrich_schema: String, enrich_name: String, key:
    Option<String>, keys: Vec<serde_json::Value> }` + `EngineTicket::MvEnrich`
    (`engine-wire/src/flight.rs`); `FlightTableClient::fetch_mv_enrich(&self,
    ticket: MvEnrichTicket) -> Result<Vec<RecordBatch>>`.
  - `pub async fn mv_enrich_scan(catalog: &IcebergCatalog, table: &TableRef,
    key: Option<(&str, &[serde_json::Value])>, serving_store:
    Option<&ServingStore>) -> Result<(SchemaRef, Vec<RecordBatch>),
    EngineServingError>` in `engine-serving/src/mv_enrich.rs` (needs no
    Postgres pool — `build_serving_provider` + the `Catalog` trait cover the
    provider and the declared-schema fallback; `execute_query`'s exact
    dependency set, `serving.rs:828`).
  - `handle_stream_mv` extended with the enrich fetch/registration branch
    (same signature; no new handler, no new job kind).
