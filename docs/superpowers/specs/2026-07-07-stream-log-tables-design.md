# Stream engine — Log Tables (Slice 1) — design

**Item:** `road-stream-log-tables` · **Builds on:** `road-stream-substrate` (Slice 0, shipped — the `BucketOffsets` allocator), `road-cow-inline-shadow` (shipped — `loom_tombstone` / `write_delta` / merge-on-read) · **Feeds:** `road-stream-pk-tables` (Slice 2), `road-stream-subscribe` (Slice 3) · **Parent:** `docs/superpowers/specs/2026-07-06-stream-engine-design.md`

## Problem

Slice 0 landed the spine — a gapless, per-`(table, bucket)` monotonic offset
allocator (`stream.bucket_offset`, `BucketOffsets::allocate_offset`). But nothing
*stamps* rows with offsets, so loom still has no notion of an ordered append log.

This slice makes **append-only "log tables"**: a dataset flavor whose appended
rows are assigned a gapless per-bucket offset and a change-kind, forming a
**durable, offset-ordered log** that survives the flush from the inline tier into
Iceberg. That log is what Slice 3's subscribe/tail feed will read; here we only
build the write-and-persist half. Reads for ordinary consumers are unchanged —
the log framing is invisible to them.

loom creates datasets implicitly on first ingest (`POST /datasets/{schema}/{table}`
→ `iceberg_land` → `ensure_table`); there is no dataset-CRUD surface and
`iceberg_mirror.table` carries no mode flag. Appends are a per-row INSERT loop in
`inline_append` (`iceberg_inline.rs`). `loom_tombstone` (a universal inline
column added via CREATE + `ALTER … ADD COLUMN IF NOT EXISTS`) is the precedent
for the framing columns. `PgTableProvider::build_scan_sql` projects columns **by
name from the logical schema**, so columns absent from the logical schema are
already invisible to reads.

## Scope

Deliver, as **one spec built as two plans** (see *Plan decomposition*):

1. A **`StreamTables` control-plane concern** + an **ingest-time declaration flag**
   that marks a new dataset as an append-only log table with a fixed bucket count.
2. **Universal log-framing columns** on the inline tier (`loom_change_kind`,
   `loom_bucket`, `loom_offset`), set at the existing write sites.
3. **Bucket assignment + offset allocation** on appends to a stream table.
4. **Durable persistence** of the framing columns through flush into the Iceberg
   table (as reserved physical columns), with the framing **excluded from the
   logical schema** so ordinary reads never see it.

## Design

### 1. Declaration — ingest-time flag + `StreamTables` concern

`POST /datasets/{schema}/{table}?mode=stream&buckets=N` (default `buckets=1`, and
`buckets` ignored unless `mode=stream`). On the **first** write that creates the
table (the `ensure_table` path), persist a row in a new control-plane table:

```sql
create schema if not exists stream;  -- already exists from Slice 0
create table stream.stream_table (
    table_id    bigint      primary key,
    bucket_count int        not null,
    created_at  timestamptz not null default now()
);
```

The row's **presence** is the "this is a log table" marker; `bucket_count` is
fixed at creation. The flag is honored at creation and **validated when present**
on later writes; it is never *required* on later writes, because stamping is
driven by the recorded `stream_table` row, not the request flag. **Immutable —
`400` on:** `mode=stream` against an existing **batch** table (no conversion), or
`mode=stream&buckets=M` where `M` ≠ the recorded `bucket_count`. A later write that
**omits** the flag simply appends using the table's recorded mode (a stream table
still gets stamped; a batch table stays batch). A no-`mode` write to a brand-new
table creates an ordinary batch table exactly as today.

This rides a new **`StreamTables`** concern built as the standard five-layer stack
(core trait → memory fake → postgres adapter → testkit contract), mirroring
Slice 0's `BucketOffsets`:

```rust
#[async_trait]
pub trait StreamTables {
    /// Declare table_id as a log table with bucket_count buckets. Idempotent for
    /// an identical redeclare; errors (Conflict) on a conflicting one.
    async fn declare_stream(&self, table_id: i64, bucket_count: i32) -> Result<()>;
    /// The bucket count if table_id is a log table, else None.
    async fn stream_bucket_count(&self, table_id: i64) -> Result<Option<i32>>;
}
```

The flag threads `ingest/src/http.rs` (parse query) → `LandRequest` →
`iceberg_land` → `ensure_table`/`inline_append`. Declaration happens in the same
transaction as table creation.

### 2. Log framing on the inline tier (universal columns)

Extend `inline_ddl` (CREATE) and `ensure_inline_schema` (`ALTER … ADD COLUMN IF
NOT EXISTS`, the `loom_tombstone` precedent) with three columns on **every** inline
table:

- `loom_change_kind text not null default '+I'` — universal; existing rows
  backfilled from `loom_tombstone` (`true` → `-D`, else `+I`).
- `loom_bucket int` — **nullable**; populated only for stream tables.
- `loom_offset bigint` — **nullable**; populated only for stream tables.

These are inline-physical bookkeeping columns (like `loom_row_id`,
`begin_snapshot`, `loom_tombstone`) — **never part of a table's `ColumnSpec`**, so
they are already excluded from the logical schema that drives reads.

**Write sites set the kind:**
- `inline_append` → `loom_change_kind = '+I'` (all appends); for stream tables also
  `loom_bucket`/`loom_offset` (§3). For batch tables both stay `NULL`.
- `write_delta` (COW, non-stream identity tables) → `'+U'` for a version row,
  `'-D'` for a tombstone; `loom_bucket`/`loom_offset` stay `NULL`.

We emit `{+I, +U, -D}` only. The **`-U` update-before** event is **not** synthesized
here (it needs the prior row image) — deferred to Slice 2. The column can hold all
four kinds.

### 3. Bucket assignment + offset allocation (stream tables, in `inline_append`)

`inline_append` looks up `stream_bucket_count(tid)`. If `Some(bucket_count)`:

- Assign each row a bucket: `bucket = row_index % bucket_count`. With the default
  `bucket_count = 1`, every row → bucket 0 (global order). **v1 simplification**
  (documented): per-row modulo within the batch, no cross-batch balancing cursor;
  fair hash-on-key bucketing is deferred (`fut-stream-partitioning`).
- For each **touched** bucket `b`, count its rows `count_b` and call
  `pg_allocate_offset(&mut *tx, tid, b, count_b)` **inside the append's
  transaction** to reserve a contiguous run; assign the bucket's rows sequential
  offsets `first_b .. first_b + count_b`. Because the allocation runs on the write's
  transaction, offsets are **assigned iff the append commits** — the property the
  executor-generic `pg_allocate_offset` seam (Slice 0) was built for; a rollback
  frees the run.

Ordering is guaranteed **per bucket**, not across buckets. Batch tables skip all of
this (no lookup cost beyond the single `stream_bucket_count` check, which is `None`).

### 4. Durable persistence through flush (reserved physical columns)

For a **stream** table the framing columns are **reserved physical columns**
carried into the Iceberg table so the log is resumable from any offset after flush
— honoring the parent design's "full changelog materialization." Mechanism:

- **Naming & exclusion:** framing columns keep the `loom_` prefix
  (`loom_bucket`, `loom_offset`, `loom_change_kind`). A single **logical-column
  filter** (`is_reserved(name)` = `loom_`-prefixed) excludes them **everywhere the
  logical schema is derived**: `PgTableProvider`'s schema, `IcebergMirrorTableProvider`'s
  schema, and `GET /datasets/{schema}/{table}`'s reported schema. Ordinary reads
  therefore never see them.
- **Flush:** `flush_table` for a stream table reads user columns **plus** the three
  framing columns from the inline tier, writes them all to Parquet, and **registers
  the framing columns in `iceberg_mirror.column`** (so the physical Iceberg schema
  includes them and Iceberg stays schema-consistent). Slice 3 will read them via a
  dedicated changelog projection. **Batch-table flush is byte-identical to today**
  (no framing columns exist in its inline rows / mirror set).
- A stream table thus has a **single** Iceberg table whose physical schema =
  user columns + `loom_` framing (the log *is* the data, per the parent design;
  the separate current-state base is a Slice 2 / PK concern).

### 5. Reads

Unchanged for consumers. The inline-∪-Iceberg union read already surfaces current
state; the `is_reserved` filter keeps the framing out of every logical schema, so
`GET /objects`, link traversal, and `GET /datasets/...` are byte-identical for a
stream table's user columns. (There is no user-facing way to read the log yet —
that is Slice 3.)

## Non-goals (explicit — deferred)

- **The subscribe / tail feed** — reading the log by offset — is Slice 3
  (`road-stream-subscribe`).
- **Updates/deletes on stream tables** — log tables are append-only; mutation +
  the `-U`/`+U`/`-D` CDC pair on PK tables is Slice 2 (`road-stream-pk-tables`).
- **`-U` (update-before) emission** — needs the prior row image; Slice 2.
- **Hash-on-key / fair cross-batch bucketing** — v1 uses `row_index % bucket_count`;
  richer assignment is `fut-stream-partitioning`.
- **Cross-bucket total ordering** — ordering is per-bucket, matching Fluss.
- **Changing a table's mode or bucket count after creation** — immutable in v1.

## Testing strategy

All tests are `rust_test` / `loom_fixture_test` integration targets (never inline
`#[cfg(test)]`).

- **`StreamTables` contract** (both adapters, testkit): declare → `stream_bucket_count`
  round-trip; idempotent identical redeclare; conflicting redeclare errors;
  unknown table → `None`.
- **Inline framing** (`loom_fixture_test`): an append to a stream table
  (`buckets=2`) stamps `loom_change_kind='+I'` and gapless per-bucket
  `loom_offset` starting at 0; a second append continues each bucket's offsets;
  a batch table's inline rows have `NULL` bucket/offset and `'+I'`; a `write_delta`
  tombstone/version row carries `'-D'`/`'+U'`. Offsets commit iff the append commits
  (rolled-back append leaves the allocator unadvanced).
- **Declaration e2e** (ingest fixture): `?mode=stream&buckets=2` on first write
  creates the `stream_table` row; a conflicting later write → `400`; a no-flag write
  creates an ordinary batch table (no row).
- **Durable-flush e2e** (`loom_fixture_test`): append to a stream table → flush →
  the framing columns are present in the Iceberg/Parquet data and offsets remain
  gapless and ordered per bucket across the flush boundary; a normal
  `GET /objects` / `GET /datasets` read does **not** expose any `loom_*` column;
  a batch table's ingest→flush is byte-identical (no framing, no mirror `column`
  churn).

## Plan decomposition

One spec, **two implementation plans**, each an independently shippable increment:

- **Plan 1a — hot-tier log.** `StreamTables` concern (+ migration
  `stream.stream_table`); ingest declaration flag threaded to `ensure_table`;
  universal `loom_change_kind`/`loom_bucket`/`loom_offset` inline columns +
  backfill; write-site kind mapping; bucket assignment + transactional offset
  stamping in `inline_append`. Deliverable: a log table you can create and append
  to, whose inline rows form a gapless per-bucket ordered log.
- **Plan 1b — durable persistence.** `is_reserved` logical-column filter applied at
  every logical-schema derivation site; stream-aware `flush_table` that persists +
  registers the framing columns in Iceberg; read-exclusion verified. **Also brings
  the Parquet (large-write) path to parity:** Plan 1a's `land_parquet` only *declares*
  the mode on a brand-new table and otherwise silently no-ops, so 1b applies the same
  `Conflict`/`Validation` reconcile the inline path has, plus offset stamping, on the
  Parquet path — and folds in a `bucket_count >= 1` CHECK on `stream.stream_table` as
  DB-level hardening. Deliverable: the log survives flush and stays resumable across
  both write paths, while ordinary reads stay byte-identical.
