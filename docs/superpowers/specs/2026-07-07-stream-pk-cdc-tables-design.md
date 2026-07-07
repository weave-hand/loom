# Stream engine — PK / CDC tables (Slice 2) — design

**Item:** `road-stream-pk-tables` · **Builds on:** `road-cow-inline-shadow` (shipped #331 — `write_delta` / `loom_tombstone` / identity-aware merge-on-read + per-identity CAS), `road-stream-substrate` (Slice 0 — the `BucketOffsets` allocator), `road-stream-log-tables` (Slice 1 — framing columns durable through flush, the `stream.stream_table` registry, `is_reserved`) · **Feeds:** `road-stream-subscribe` (Slice 3) · **Parent:** `docs/superpowers/specs/2026-07-06-stream-engine-design.md`

## Problem

Slice 1 made **append-only log tables**: a declared stream flavor whose appends
are stamped with a gapless per-bucket offset and a `loom_change_kind`, durable
through flush into a single Iceberg table, invisible to logical reads. It
deliberately deferred three things to this slice:

1. **Updates/deletes on stream tables** — log tables are append-only.
2. **The `−U` (update-before) event** — it needs the prior row image.
3. **The dual-table model + compaction** — a log table has only the changelog;
   a PK table needs both a changelog *and* a compacted current-state base.

This slice delivers **PK / CDC tables**: a declared, identity-bound stream flavor
whose mutations emit the full Flink-style **+I / −U / +U / −D** change sequence
and materialize the *dual* Iceberg tables the parent design calls for — an
append-only **changelog** (the durable, always-resumable log) beside a compacted
**current-state base** (the merge-on-read materialization). It builds almost
wholesale on `road-cow-inline-shadow`: identity-targeted PATCH/DELETE via
inline-delta copy-on-write with a per-identity CAS guard already exists
(`run_mutate`, `action.rs:1022`), already reads the prior row image before
building the PATCH (`action.rs:1116`), and already stamps `+U` on version rows
and `−D` on tombstones (`write_inline_delta`, `iceberg_inline.rs:987`). The gaps
are the `−U` partner event, offset/bucket stamping on the mutation path (today
`write_delta` leaves `loom_bucket`/`loom_offset` NULL), the second Iceberg table,
and the consolidation compaction (`fut-cow-inline-shadow` slice 2, not yet built).

Merge engine **LastRow** only (latest row per key wins; `−D` removes). FirstRow /
Versioned / Aggregation + partial-update stay deferred (`fut-stream-merge-engines`).

## Scope

Delivered as **one spec, two plans** (see *Plan decomposition*):

1. A **`kind` on the stream registry** (`log` | `cdc`) and a **`mode=cdc`
   declaration** on the model/bind surface, enforced to require an identity
   property.
2. **CDC event emission** on the mutation path: `insert → +I`,
   `update → (−U, +U)`, `delete → −D` carrying the prior image; each stamped with
   a per-bucket offset and `bucket = hash(identity) % bucket_count`.
3. A **changelog Iceberg table** per CDC table — append-only, every event —
   created at declaration and written by flush in the **same Postgres transaction**
   as the current-state base's snapshot commit.
4. **LastRow compaction** — a distinct worker job folding the current-state base's
   deltas into a fresh consolidated base and clearing `has_shadow`; the changelog
   is never compacted.

## Design

### 1. Declaration — `kind` on the registry, `mode=cdc` on model/bind

Slice 1's `stream.stream_table` registry marks "this table is a stream" by row
presence, with a fixed `bucket_count`. Add a discriminant:

```sql
alter table stream.stream_table
  add column kind text not null default 'log'
  check (kind in ('log', 'cdc'));
-- plus the changelog table pointer (§4)
alter table stream.stream_table
  add column changelog_table_id bigint;  -- null for kind='log'
```

A **CDC table is declared explicitly** — the parent design's "declared opt-in"
choice — via `mode=cdc&buckets=N` on the model/bind surface
(`POST /models/{type}?mode=cdc&buckets=N`), which is where an identity-bearing
type is established. Enforcement:

- The bound type **must declare an identity property**, else `400` (a CDC table
  keys its buckets and its LastRow merge on the identity — there is nothing to
  key without one). This is the one rule that distinguishes `mode=cdc` from
  slice 1's `mode=stream` (`log`), which needs no identity.
- `bucket_count = N` (default 1), fixed at creation, `>= 1` (the existing CHECK).
- **Immutable**, reusing `reconcile_stream_mode` (`stream.rs:24`): `mode=cdc`
  against an existing `log`/batch table, or a conflicting `buckets`, is a `400`.
- On declaration the **changelog table is created** (§4) in the same transaction
  and its `table_id` recorded in `changelog_table_id`. (Staging: 2a records
  `kind='cdc'` with `changelog_table_id` NULL; **2b** is what creates the
  changelog table at declaration and fills the pointer — see *Plan decomposition*.)

The `StreamTables` concern (core trait → memory fake → postgres adapter → testkit
contract) grows `declare_cdc(table_id, bucket_count, changelog_table_id)` and a
`stream_kind(table_id) -> Option<StreamKind>` beside slice 1's
`declare_stream`/`stream_bucket_count`.

### 2. CDC emission on the mutation path (2a)

The mutation entrypoint `run_mutate` (`action.rs:1022`) already: captures the
identity + version token, **reads the merged prior row** (`action.rs:1116`),
builds the PATCH by cloning the prior row and overwriting SET columns, and writes
one inline-delta via `write_delta` under a per-identity advisory lock + CAS guard.
`run_insert` (`action.rs:650`) writes a fresh row via `write_object`.

On a table whose `stream_kind` is `cdc`, emission becomes:

- **insert → `+I`.** The inserted row, `loom_change_kind='+I'` (as today for
  appends), now also stamped with bucket + offset (§3).
- **update → adjacent `(−U, +U)`.** `write_delta` gains a CDC variant that writes
  **two** inline rows for an update: a **`−U` before-image** (the prior row image
  `run_mutate` already holds) and the existing **`+U` after-image** version row.
  They receive **consecutive offsets in the same bucket** (the `−U` first), so a
  retract-capable consumer sees a well-formed retract/append pair.
- **delete → `−D` with the prior image.** Today's tombstone carries only the
  identity + `loom_tombstone=true` + NULL data columns (`write_inline_delta`,
  `iceberg_inline.rs:959`). For a CDC table the `−D` row carries the **full prior
  row image** (again already in hand) so the changelog event is complete;
  `loom_tombstone=true` is retained so it still hides the base row in merge-on-read.
  **Non-CDC identity tables keep the NULL tombstone — byte-identical to today.**

**The `−U` row is a changelog-only event, never a current-state row.** It is
written into the inline tier stamped `loom_change_kind='−U'`, carrying the
mutation's `begin_snapshot` (same version token as its `+U` partner, so it does
not perturb the CAS version), and is **excluded from every current-state
derivation**: `inline_live_batch` (`iceberg_inline.rs:1116`) and
`read_max_version` (`iceberg_inline.rs:733`) add `and (loom_change_kind is null or
loom_change_kind <> '−U')` to their live predicates. `−U` rows exist only to be
carried to the changelog at flush; they are dropped from the current-state base's
delta set. Because the base never receives `−U` rows, no base-side or engine
merge change is needed — the exclusion lives solely at the inline projection.

### 3. Bucketing — hash on identity

Slice 1 log tables assign `bucket = row_index % bucket_count` (a `+I`-only stream
has no key). PK tables must keep **a key's whole history in one bucket** so
LastRow merge and per-key ordering hold, so a CDC table assigns
**`bucket = stable_hash(identity_cell) % bucket_count`** (reusing the same
deterministic hash family as `advisory_key_for_id`, `iceberg_inline.rs:773`).
Every event for one identity — `+I`, each `(−U,+U)`, the eventual `−D` — lands in
the same bucket, and `pg_allocate_offset(&mut *tx, tid, bucket, count)`
(`stream.rs:103`) reserves the run **inside the mutation's transaction**, so
offsets are assigned iff the mutation commits. `write_delta` gains the bucket/offset
stamping that `inline_append` already has for log tables.

### 4. The dual Iceberg tables (2b)

A CDC table is **two** Iceberg tables, each an `iceberg_mirror` row with its own
`table_id`:

- **Current-state base** — *today's* table (created by `ensure_iceberg_table`,
  `iceberg_landing.rs:507`). Its physical schema and merge-on-read are unchanged;
  compaction (§6) keeps it consolidated. The base's flushed deltas are
  `+I/+U/−D` only.
- **Changelog table** — created at declaration with `include_framing=true` so its
  physical schema is `user columns + loom_change_kind + loom_bucket + loom_offset`
  (`augment_with_framing`, `iceberg_landing.rs:565`). Append-only; holds **every**
  event including `−U`. Named by suffix off the base (`<schema>.<table>__changelog`)
  and pointed to by `stream_table.changelog_table_id`. `is_reserved`
  (`iceberg_catalog.rs:17`) keeps its framing out of any logical schema, exactly
  as for slice 1.

### 5. Flush — atomic dual-write (2b)

`flush_locked` (`iceberg_flush.rs:57`) today captures live inline rows and appends
them to the single table in one commit. For a CDC table it:

1. Partitions the captured live inline rows by `loom_change_kind`.
2. Appends the **current-state deltas** (`+I/+U/−D`) → the **base** table (as today).
3. Appends **all events** (`+I/−U/+U/−D`) → the **changelog** table (append-only).
4. Commits **both snapshots in one Postgres transaction** via the
   `TxCommitCatalog` caller-provided-tx seam slice 1b built (`iceberg_writer.rs`),
   so the two `iceberg_mirror` snapshot rows advance atomically — a changelog
   event is durable iff the matching base delta is.
5. End-caps the flushed inline rows (marks them consumed) as today.

Object-store I/O for both tables happens **before** `begin()` (honoring
`iss-iceberg-tx-objectstore`); only the two fast pointer-CAS commits ride the
transaction. **Batch and log-table flush are byte-identical to today** (no
changelog table, no partition step).

### 6. Compaction — LastRow, a distinct job (2b)

A **new worker job kind** (`stream_consolidate`), distinct from `flush_table` and
the existing Parquet-coalesce `handle_compact` (`worker/src/compact.rs:23`, which
only merges small files and does **not** fold deltas). It:

- Reads the current-state base (consolidated base ∪ its `+I/+U/−D` delta rows),
- Folds by **LastRow per identity** — the row with the greatest `begin_snapshot`
  wins; a `−D`/tombstone removes the key,
- Writes a fresh consolidated base via the whole-table copy-on-write + commit-swap
  primitive the action-delete path already uses, and
- Clears `has_shadow` (`iceberg_inline.rs:687`) so subsequent reads carry no
  unfinalized deltas and the byte-trigger flush resumes.

The **changelog table is never touched by compaction** — it is the durable log;
its retention rides the age-based `gc_table` pass. Enqueued on a delta-count / age
threshold (mirroring slice 1's byte-trigger enqueue in `inline_append`,
`iceberg_inline.rs:625`); the exact threshold is a `WriteConfig`/`LOOM_*` knob.

### 7. Reads

- **Current-state read** — `current-state base ∪ inline deltas`, merge-on-read,
  **unchanged** for consumers except that `−U` inline rows are now filtered out
  (§2). `GET /objects`, link traversal, and `GET /datasets` stay byte-identical
  for user columns; `is_reserved` keeps all framing hidden.
- **Changelog read** — there is **no** user-facing changelog read in this slice;
  reading the log by offset is Slice 3 (`road-stream-subscribe`), which will union
  the changelog Iceberg table with the inline tail.

## Non-goals (explicit — deferred)

- **The subscribe / tail feed** (reading the changelog by offset) — Slice 3.
- **Merge engines other than LastRow** — FirstRow / Versioned / Aggregation +
  partial-update are `fut-stream-merge-engines`.
- **A PK index for point lookups** — `fut-stream-pk-index` (Slice 5 concern).
- **Hash-on-key *fair* rebalancing / two-level partitioning** — `bucket =
  hash(identity) % bucket_count` with a fixed count; richer sharding is
  `fut-stream-partitioning`.
- **Converting an existing log/batch table to CDC, or changing bucket count** —
  immutable in v1.
- **Multi-identity / composite bucket keys** — the single declared identity is the
  bucket key.

## Testing strategy

All tests are `rust_test` / `loom_fixture_test` integration targets (never inline
`#[cfg(test)]`), per the repo testing policy; e2e reuses `//src/services/query-api:e2e-support`.

- **`StreamTables` CDC contract** (both adapters, testkit): `declare_cdc` →
  `stream_kind`/`stream_bucket_count`/`changelog_table_id` round-trip; idempotent
  identical redeclare; conflicting kind/bucket redeclare → Conflict; a `log`
  table reports `kind=log` with null changelog pointer.
- **Declaration e2e** (query-api/ingest fixture): `mode=cdc&buckets=2` on an
  identity-bearing type creates the `stream_table` row (`kind=cdc`) **and** the
  changelog Iceberg table; `mode=cdc` on a type with **no identity** → `400`;
  `mode=cdc` against an existing `log`/batch table → `400`.
- **CDC emission** (`loom_fixture_test`): on a `buckets=2` CDC table — insert
  stamps `+I` + gapless per-bucket offset; update writes an adjacent `(−U, +U)`
  pair with consecutive offsets in the identity's bucket, the `−U` carrying the
  before-image; delete writes `−D` carrying the prior image with
  `loom_tombstone=true`; all events for one identity share a bucket. A current-state
  `GET /objects` read reflects only the latest row and **never** exposes a `−U`
  row or any `loom_*` column. CAS still guards concurrent updates of one identity.
- **Dual-write flush** (`loom_fixture_test`): mutate → flush → the base table holds
  `+I/+U/−D` deltas and the changelog table holds **all** events incl. `−U`, both
  advanced in one commit (kill between snapshots leaves neither); offsets stay
  gapless/ordered per bucket across the flush boundary.
- **LastRow compaction** (`loom_fixture_test`): after several updates + a delete on
  a CDC table, `stream_consolidate` folds the base to one row per live identity
  (deleted keys absent), clears `has_shadow`, and leaves the **changelog table
  unchanged** (every historical event still present); current-state reads are
  identical before and after compaction.

## Plan decomposition

One spec, **two implementation plans**, each an independently shippable increment:

- **Plan 2a — CDC emission.** Registry `kind` (+ `changelog_table_id` column,
  nullable until 2b uses it); `mode=cdc` declaration on model/bind with the
  identity requirement; `hash(identity) % bucket_count` bucketing; the CDC
  `write_delta` variant emitting `(−U, +U)` on update and `−D`-with-prior-image on
  delete, with offset/bucket stamping; merge-on-read `−U` exclusion in
  `inline_live_batch`/`read_max_version`. Deliverable: a declared CDC table whose
  mutations produce a correct `+I/−U/+U/−D` offset-ordered event sequence in the
  inline tier and through flush into the (single, existing) table — non-CDC tables
  byte-identical.
- **Plan 2b — dual tables + compaction.** Create/register the changelog Iceberg
  table at declaration; dual-write flush committing base + changelog atomically via
  `TxCommitCatalog`; the `stream_consolidate` worker job folding the base by
  LastRow and clearing `has_shadow`. Deliverable: the changelog is durable and
  resumable, the current-state base stays compacted, and reads stay byte-identical.
