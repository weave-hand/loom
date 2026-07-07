# Stream engine — a loom-native streaming storage layer — design

**Item:** new (`road-stream-substrate`, to be registered) · **Relates:** `road-cow-inline-shadow`, `fut-transform-followups`, `fut-serving-stream-to-http`

## Problem

loom serves point-in-time analytical reads well: the engine unions un-flushed
inline rows (`PgTableProvider`) with historical Parquet (`IcebergMirrorTableProvider`)
and streams the result over Flight SQL. What it has **no model for** is *streaming*
data — sub-second-fresh appends, mutable primary-key tables that emit a change
stream, consumers that tail a table by offset, or standing queries that update as
new data lands.

Apache [Fluss](https://fluss.apache.org) is the reference design for this shape:
a real-time storage tier that sits *in front of* a lakehouse, is natively
columnar, supports primary-key upserts + point lookups, emits a CDC changelog,
and tiers into Iceberg/Paimon with a "union read" that stitches fresh streaming
data to historical lakehouse data. This document explains how Fluss works, then
lays out a loom-native streaming layer that reaches the same capability set by
**generalizing machinery loom already has** rather than cloning Fluss's
dual-server / RocksDB / ZooKeeper / ISR stack.

The deliverable is a **design + sliced roadmap**: the full target architecture,
decomposed into six independently-shippable slices. Slice 0 (the changelog
substrate) is the first to build; slices 1–5 are registered in the docs
registers and planned later.

## How Fluss works (reference)

Fluss ("Fast Low-latency Unified Streaming Storage") fixes the two things a
Kafka + Iceberg stack cannot do:

- **The log is columnar.** Records are Arrow batches, not opaque bytes, so
  readers get **column pruning + projection/filter pushdown on the live stream**.
- **Two table types.**
  - **Log Tables** — append-only, offset-ordered per *bucket* (the Kafka-topic
    analog).
  - **Primary-Key Tables** — mutable, a RocksDB-backed *materialized current
    state* plus a *child log that is simultaneously the WAL and the CDC
    changelog*. Every mutation emits Flink-style **+I / −U / +U / −D** change
    events synchronously.
- **The unified log+cache invariant.** PK write sequence: merge (RowMerger:
  LastRow / FirstRow / Versioned / Aggregation + partial-update) → append CDC to
  log → buffer aligned to offset → replicate → flush to RocksDB → ack. Log
  visibility is gated by the high-watermark, so *"if you can see it in the log,
  you can read it from the table."* Recovery = object-store snapshot + replay log
  from the snapshot's offset.
- **Bucket / tablet model.** A table is split into **buckets** (unit of
  parallelism and ordering); optionally partitioned above that (two-level
  sharding). Each bucket is a tablet: a LogTablet always, plus a KvTablet for PK
  tables, co-located in one process.
- **Two tierings.** Internal remote-log (sealed Arrow segments → object store,
  manifest committed to the coordinator) and **lakehouse tiering** (a separate
  Flink job writes Paimon/Iceberg and commits a **per-bucket switchover offset**
  to the coordinator).
- **Union read.** A query stitches *hybrid splits* — historical Parquet up to the
  switchover offset, then the fresh changelog tail beyond it — as one logical
  table (currently Flink-only).
- **Coordinator / TabletServer** split; ZooKeeper today (Raft planned);
  Kafka-style ISR replication of the log; KvTablets unreplicated (durability via
  WAL + snapshots).
- **Delta Join** (headline Flink feature): stream-stream joins keep *no* state in
  Flink — each arriving record does an async point-lookup against the Fluss PK
  index, externalizing join state to the storage layer.

## The key insight: loom already has the bones

loom's serving path **already performs a union read.** The engine stitches
`PgTableProvider` (un-flushed inline rows in Postgres) to
`IcebergMirrorTableProvider` (historical Parquet on S3) at query time. That
inline-rows-in-Postgres → **flush** → Iceberg-Parquet pipeline is *structurally
the same shape* as Fluss's live-log → tiered-lakehouse, and the flush point is
loom's equivalent of the per-bucket switchover offset.

The **shipped** `road-cow-inline-shadow` slice 1 (PR #331) — inline shadow rows
in `iceberg_inline.rs`, a `loom_tombstone` delete marker, a per-identity
compare-and-swap guard, and identity-aware merge-on-read — is already ~70% of a
PK/CDC table: a materialized current-state over a delta log with delete markers.
It simply does not yet expose an *offset-ordered change stream* or the full
+I/−U/+U/−D event kinds, and its **compaction-consolidation** step (folding the
deltas into a fresh base) is still deferred (`fut-cow-inline-shadow` slice 2).

Two consequences shape every choice below:

1. **Most of the "port" is generalizing loom primitives, not building a Fluss
   clone.** The inline tier, flush, merge-on-read, the per-identity CAS guard,
   and the union read already exist; compaction-consolidation of the deltas is
   the one still-deferred piece (`fut-cow-inline-shadow` slice 2).
2. **loom collapses Fluss's two tiers.** loom's historical tier is already
   Iceberg-native, so there is no separate Paimon and no separate lakehouse
   tiering job — **flush *is* the tiering**, and loom's control plane already
   *is* the coordinator (Postgres holds all metadata). loom therefore does **not**
   rebuild the dual-server / RocksDB / ZooKeeper / Raft / ISR machinery.

### Feature-by-feature mapping

| Fluss | loom analog | Status in loom |
| --- | --- | --- |
| Columnar Arrow log | Offset-ordered inline rows in Postgres → flush to Iceberg | inline tier + flush **exist** |
| Log Table (append) | Append-only dataset, sub-second via inline tier | ingest landing path **exists**, needs offsets |
| PK Table + RocksDB state | Identity type + inline shadow + merge-on-read + CAS | `road-cow-inline-shadow` **shipped (#331)** |
| CDC changelog (+I/−U/+U/−D) | `change_kind` column on the changelog | **new** (generalize `loom_tombstone`) |
| RowMerger / merge engines | Merge-on-read + write-path merge policy | **new** (LastRow implicit today) |
| Remote-log + lakehouse tiering | Flush inline → Iceberg snapshot | **exists** (one tiering, not two) |
| Union read (hybrid splits) | `PgTableProvider` ∪ `IcebergMirrorTableProvider` | **exists** |
| Point lookup (RocksDB) | Indexed read on inline tier + Iceberg pushdown | partial (no PK index yet) |
| Subscribe / tail | Offset-cursor feed over the changelog | **new** endpoint |
| Continuous query / MV | Micro-batch transform keyed by offset watermark | **new** (transform workers exist) |
| Delta Join | Lookup-join against PK index; micro-batch stream-join | **new**, hardest, last |
| Coordinator + ZooKeeper + ISR | Postgres (already the sole coordinator) | **N/A — loom already has this** |

## Architecture decision

Three approaches were weighed:

- **A — Postgres-log-native (chosen).** The changelog *is* offset-ordered rows in
  the existing inline tier, extended with a per-bucket offset and a `change_kind`
  column. Flush tiers to Iceberg as today; union read surfaces current state and
  changelog; subscribe is a cursor over the offset column with `LISTEN/NOTIFY`
  wakeups; continuous queries are micro-batch transforms watermarked by offset.
  No new datastore, no RocksDB, no broker. Reuses ~70% of loom's primitives and
  stays on loom's grain. Trade-off: Postgres is the throughput ceiling for the
  hot log — acceptable at loom's scale, and loom already bets everything on
  Postgres-as-coordinator.
- **B — Arrow-log-on-object-store (Fluss-faithful).** A real columnar Arrow
  append-log as sealed S3 segments + Postgres manifest, embedded LSM for PK state.
  Higher throughput and true live-stream columnar pushdown, but a large new
  subsystem and a *second* storage format beside Iceberg with its own
  durability/replication story. **Rejected as the substrate; retained as a
  documented future hot-path optimization** — because A's changelog abstraction is
  storage-backend-agnostic, the offset-ordered inline tier can later be re-backed
  by Arrow segments without changing the logical model (`fut-stream-arrow-log`).
- **C — Embed/depend on a broker (Kafka/Redpanda/Fluss).** Rejected: violates
  loom's explicit "no second broker," and the goal is a *custom* engine.

**Decision: A.**

## Target architecture

### Core data model

- **Bucket** — the unit of ordering and parallelism. `bucket_id = hash(bucket_key) mod bucket_num`.
  For PK tables `bucket_key ⊆ primary key` (a key's whole history stays in one
  bucket, enabling in-place merge and prefix lookups); for Log Tables the
  assignment is sticky (default) / round-robin / hash. Ordering is guaranteed
  **per bucket**, not across buckets — matching Fluss.
- **Per-bucket offset** — a monotonic counter per `(table, bucket)`, assigned at
  commit under the bucket's advisory lock so the log is gapless and totally
  ordered within the bucket. This is loom's analog of Fluss's per-bucket log
  offset; it is control-plane state, not derived from Iceberg snapshot ids.
- **Change event** — `(table, bucket, offset, change_kind ∈ {+I,−U,+U,−D}, row image)`.
  An update materializes as an adjacent `(−U, +U)` pair so retract-capable
  consumers stay correct; a delete is a `−D` (subsuming today's `loom_tombstone`).

### Storage tiers (full changelog materialization)

- **Live tier** — inline change-events in Postgres. Sub-second visible on commit;
  the hot tail. A bounded hot cache, flushed regularly — *not* the durable record.
- **Historical tier — two Iceberg tables per PK table:**
  - a **changelog table** — every change event, append-only; the *durable,
    always-resumable log*. A subscriber can resume from any offset without
    re-bootstrapping.
  - a **current-state base** — the compacted merge-on-read materialization (folds
    −U/+U/−D to the latest row per key).
  - **Log Tables** have only the changelog table (the events *are* the data).
- **Flush** appends inline events → the changelog Iceberg table — loom's existing
  inline→Parquet `flush_table`. **Compaction** folds the changelog → the
  current-state base; this is the still-deferred `fut-cow-inline-shadow` slice-2
  compaction-consolidation, which this design brings into scope for PK stream
  tables (it does *not* exist yet). The durability invariant: an event is durable
  once flushed to the changelog Iceberg table; the inline tier may be trimmed
  behind the flush watermark.

### The two union reads (both are loom's existing shape)

- **Current-state read** — `current-state base ∪ inline deltas`, merge-on-read →
  ordinary always-fresh `GET /objects`. *Exists today*; gains sub-second freshness
  for free once writes land in the offset-ordered inline tier.
- **Changelog read (subscribe)** — `changelog Iceberg table ∪ inline tail`,
  offset-ordered per bucket → the tail/subscribe feed, resumable from any offset.
  Deep history is served from the changelog Iceberg table with projection
  pushdown; the fresh tail from the inline tier. This is Fluss's hybrid split, but
  always-resumable because the changelog is fully materialized.

### Coordinator = more control-plane schema

Fluss's CoordinatorServer metadata role becomes additional Postgres control-plane
state — no new server:

- **bucket-assignment map** — `bucket_num` and bucket-key spec per stream table;
- **per-bucket high-water offset** — the next offset to assign;
- **per-bucket flush/switchover offset** — the boundary between the changelog
  Iceberg table and the inline tail (drives the changelog union read).

This lives beside `iceberg_mirror`, `ontology`, `acl`, `queue`, and `lineage` as
its own schema (working name `stream`).

### Continuous queries and joins

- **Continuous / standing queries** — transform workers gain an
  **offset-watermarked micro-batch mode**: on each `NOTIFY`/tick, re-run the query
  over the delta since the last committed per-bucket offset and commit the output
  as *its own changelog* (so materialized views are themselves subscribable —
  composable). Honest framing: this approximates continuous queries with
  micro-batch, not true incremental operators.
- **Stream joins / delta-join analog** — a **lookup-join** enriches a stream via
  point-lookups against a PK index (needs a real PK index spanning inline +
  Iceberg tiers); **stream-stream joins** ride the micro-batch machinery above.
  True stateful incremental joins stay deferred.

### Cross-cutting concerns

- **Governance.** The subscribe feed is a governed surface routed through the
  query-api chokepoint: row/column ACL policy is compiled into the changelog read
  exactly as for ordinary reads, so change events a subject may not see are never
  emitted.
- **Lineage.** A subscriber and a continuous query are lineage edges (a run that
  consumes offsets `[a, b)` of a source and produces a derived changelog),
  recorded transactionally with the commit as loom already does.
- **Retention.** The changelog Iceberg table is the durable record; the inline
  tier is a bounded hot cache trimmed behind the flush watermark. Changelog-table
  retention/GC reuses the age-based `gc_table` pass.
- **Latency budget.** Sub-second freshness is bounded by Postgres commit
  visibility, *not* by flush cadence — reads see inline events immediately on
  commit.
- **Multi-writer.** Per-bucket offset assignment under the bucket advisory lock
  serializes appends to a bucket; the existing Iceberg CAS + retry handles
  concurrent flush/compaction. Fits the ARCHITECTURE.md multi-writer open
  question.

## Sliced roadmap

Dependency spine: **0 → {1, 2} → 3 → 4 → 5.** Each slice is independently
shippable and mostly *extends* existing machinery.

- **Slice 0 — Offset substrate (`road-stream-substrate`, first to build).** A new
  `stream` control-plane schema and a `BucketOffsets` concern built as the full
  five-layer control-plane stack (core trait, memory fake, postgres adapter,
  testkit contract), exposing a **gapless, per-`(table, bucket)` monotonic offset
  allocator** — `allocate_offset(table_id, bucket, count) -> first_offset`,
  transaction-scoped so an offset is assigned iff the enclosing write commits, and
  serialized per bucket by the counter row's lock. Offsets are what make the inline
  tier a *log*; this is the spine every later slice reads and writes. Proven by a
  concurrency contract test (N concurrent single-row allocations on one bucket
  yield a gapless `0..N` with no dups; buckets and tables are independent). No
  change to existing write paths yet — the allocator's first consumer is Slice 1.
  The `change_kind` (+I/−U/+U/−D) column on the inline tier is deliberately *not*
  here: it has no reader until events are emitted, so it lands in Slices 1–2 where
  it earns its place.

- **Slice 1 — Log Tables (`road-stream-log-tables`).** An append-only stream
  dataset flavor: appends land in the inline tier stamped with `(bucket, offset)`
  from Slice 0's allocator and a `change_kind = +I` column (added to the inline
  DDL beside `loom_tombstone`, existing rows backfilled — appends `+I`, tombstones
  `−D`), are sub-second-visible via the current-state union read, and flush to the
  changelog Iceberg table on cadence. Reuses `iceberg_landing::land` +
  `flush_table` almost wholesale; adds bucket assignment (sticky/round-robin/hash).

- **Slice 2 — PK / CDC tables (`road-stream-pk-tables`).** Builds on
  `road-cow-inline-shadow`. The write path emits change events
  (insert→+I, update→(−U,+U), delete→−D) and writes the dual Iceberg tables
  (changelog + current-state base). Merge engine **LastRow** (default) in scope;
  FirstRow / Versioned / Aggregation + partial-update as follow-ons
  (`fut-stream-merge-engines`). Current-state read = merge-on-read.

- **Slice 3 — Subscribe / tail feed (`road-stream-subscribe`).** A governed
  query-api endpoint: consumer supplies `(table, {bucket → start_offset})` and
  receives a cursor feed of change events beyond it, with `LISTEN/NOTIFY` wakeups
  + polling fallback (the queue's proven pattern) and column projection. Resuming
  below the flush watermark serves from the changelog Iceberg table (the changelog
  union read); above it, from the inline tail. Streaming through to a chunked HTTP
  response folds in `fut-serving-stream-to-http`.

- **Slice 4 — Continuous / standing queries (`road-stream-continuous`).**
  Transform workers gain the offset-watermarked micro-batch mode; output committed
  as its own changelog (MVs are subscribable → composable). Materialized-view
  registration. Extends `fut-transform-followups` (watermark/incremental output).

- **Slice 5 — Stream joins / delta-join analog (`road-stream-joins`).**
  Lookup-join (point-lookup enrichment against a PK index — requires a PK index
  across inline + Iceberg tiers, `fut-stream-pk-index`) and stream-stream join via
  the slice-4 micro-batch machinery. Maps to "stream join emissions" +
  cross-engine. Hardest; last; true stateful incremental join stays deferred
  (`fut-stream-incremental-join`).

### Registered follow-ups (deferred)

- `fut-stream-arrow-log` — re-back the offset-ordered live tier with Arrow
  segments on object storage (Approach B) if Postgres becomes the hot-log
  bottleneck; the logical model is unchanged.
- `fut-stream-merge-engines` — FirstRow / Versioned / Aggregation merge engines +
  partial-update.
- `fut-stream-pk-index` — a primary-key index spanning inline + Iceberg tiers for
  high-QPS point lookups.
- `fut-stream-incremental-join` — true stateful incremental stream-stream joins
  (beyond micro-batch).
- `fut-stream-partitioning` — Fluss-style two-level partition → bucket sharding
  above the single-level bucketing v1 ships.

## Non-goals (explicit)

- **No second server role, broker, RocksDB, ZooKeeper, or ISR replication.**
  Postgres remains the sole coordinator; Iceberg + S3 provide durability.
- **No true incremental streaming operators** in v1 — continuous queries are
  micro-batch.
- **No cross-partition (across-bucket) total ordering** — ordering is per-bucket,
  matching Fluss.
- **No external SQL/streaming wire** beyond loom's governed endpoints in this
  design — that tracks under `fut-external-sql-wire`.

## Testing strategy

- **Slice 0** — `rust_test` targets over the offset-assignment primitive: gapless
  per-bucket ordering under concurrent appends (the concurrency-test pattern from
  the control-plane hardening pass), `change_kind` round-trip, and the
  `loom_tombstone` → −D generalization preserving existing delete behavior.
- **Slices 1–2** — `loom_fixture_test` e2e: append/upsert lands, is sub-second
  visible via the current-state read, flushes to the changelog Iceberg table, and
  compacts to the current-state base; the emitted change events match the expected
  +I/−U/+U/−D sequence.
- **Slice 3** — e2e over `e2e-support`: subscribe from offset 0 across a
  flush/switchover boundary returns the full, correctly-ordered event stream
  (changelog Iceberg table ∪ inline tail), and ACL policy redacts events a subject
  may not read.
- **Slices 4–5** — e2e: a micro-batch MV over a source stream converges to the
  batch-equivalent result and is itself subscribable; a lookup-join emits the
  enriched stream.

All tests are `rust_test`/`loom_fixture_test` integration targets (never inline
`#[cfg(test)]`), per the repo testing policy.
