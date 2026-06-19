# Iceberg Inline Flush / Compaction — Design

> The follow-on to Slice B. A `flush_table` library primitive that drains a
> table's loom-native inline rows into a real Iceberg Parquet snapshot — making
> them visible to external Iceberg clients and stopping loom's read engine from
> reconstructing them on every query — then end-caps the inline rows, atomically.

## Goal

Close the one gap Slice A/B knowingly deferred: inline rows
(`iceberg_mirror.inline_<table_id>`) are **loom-read-engine-only** — external
Iceberg clients never see them, and loom's DataFusion serving engine reconstructs
them to in-memory Parquet on *every* read. A flush rewrites the live inline rows
as a real Iceberg data file (real metadata + snapshot + mirror projection), so
they become externally visible and read-cheap, then retires the inline rows so
the read engine stops double-serving them.

This is a **library primitive only** — no scheduler, threshold, worker, or HTTP
surface. It mirrors how Slices A/B shipped primitives first; a future
Transform-worker or threshold policy composes on top.

## Background: the mechanics this builds on (verified)

- **Inline row schema** (`iceberg_inline.rs`, `inline_ddl`): each per-table
  `iceberg_mirror.inline_<table_id>` row has `loom_row_id bigserial PK`,
  `begin_snapshot bigint not null`, `end_snapshot bigint` (nullable), then the
  data columns. Today `end_snapshot` is **never set** — inline rows are
  append-only.
- **Live-row predicate** (identical for inline rows and `data_file` rows):
  `begin_snapshot <= at AND (end_snapshot IS NULL OR end_snapshot > at)`.
- **Read engine** (Slice 3 + A): unions live inline rows (reconstructed via
  `IcebergCatalog::inline_parquet`) with live `file://` Parquet at the current
  snapshot.
- **File retirement** already exists: `iceberg_mirror::mark_dropped` sets
  `end_snapshot` on `table`/`column`/`data_file` rows (used on table drop). Flush
  reuses the same end-cap idea, but for *inline* rows.
- **Snapshot allocation**: `next_snapshot(conn, Option<iceberg_id>)` via the
  `iceberg_mirror.snapshot_seq` Postgres sequence. `inline_append` passes `None`
  (synthetic); the real Parquet path passes the iceberg snapshot id.
- **The atomic commit boundary**: the vendored `SqlCatalog::do_update_table`
  (Slice B) runs the pointer-CAS + `project_mirror` (+ optional `pg_emit`
  lineage) in **one** Postgres transaction. `project_mirror` allocates the loom
  snapshot for the commit and projects the new `data_file` at it.
- **An inline-only table has no iceberg table** in the vendored catalog —
  `inline_append` never creates one; only the Parquet path (`land_parquet`)
  does create-if-absent.

## Architecture

### The primitive

```rust
// control_plane_postgres::iceberg_flush
pub async fn flush_table(
    catalog: &SqlCatalog,
    pool: &PgPool,
    table: &TableRef,
    run_id: RunId,
) -> Result<Option<SnapshotId>>;   // None = nothing live to flush (no-op)
```

Returns the new mirror snapshot id on a flush, `None` when there are no live
inline rows.

### Flow

1. **Serialize per table.** Acquire a **session-level** `pg_advisory_lock(<hash
   of table_id>)` on a dedicated connection `flush_table` holds for the whole
   operation (acquire before capture, release after commit on every path,
   including early-return and error). This serializes flushes on one table — the
   hazard is two flushes reading the same live rows and both writing Parquet →
   duplicate data (the loser's end-cap `WHERE end_snapshot IS NULL` no-ops while
   its file still lands). A *session* lock (not `xact`) is used deliberately: the
   commit runs inside the vendored catalog's own internal connection/tx (`do_update_table`),
   a different connection than `flush_table`'s, so the lock only needs to be
   *held by `flush_table`* across capture→commit — both contending flushes block
   on the same key regardless of which connection commits. Inline *writes* never
   take this lock — a concurrent `inline_append` gets its own snapshot and a row
   outside the captured set `R`, so it simply flushes next round.
2. **Capture.** Read the live inline rows (`end_snapshot IS NULL`), capturing
   their `loom_row_id` set `R` and reconstructing one arrow-57 `RecordBatch`
   (share `inline_parquet`'s row→array logic — extract it into a reusable
   `inline_live_batch(conn, table, tid) -> Option<(Vec<i64> row_ids, RecordBatch)>`).
   Empty → return `Ok(None)` (release lock, no snapshot, no lineage).
3. **Create-if-absent.** Ensure the namespace + iceberg table exist in the
   vendored catalog (an inline-only table has none yet). Reuse `land_parquet`'s
   create-if-absent sequence (`namespace_exists`/`create_namespace`,
   `table_exists`/`create_table` from the table's `ColumnSpec`s, `load_table`).
   The `ColumnSpec`s come from `IcebergCatalog::schema(table, current)`.
4. **Append + cap + lineage, atomic.** Append the reconstructed batch as real
   Parquet via a `FlushCatalog` decorator (below). In the one commit tx:
   pointer-CAS + `project_mirror` (new `data_file` at the allocated snapshot `S`)
   + `UPDATE inline_<tid> SET end_snapshot = S WHERE loom_row_id = ANY($R) AND
   end_snapshot IS NULL` + `pg_emit` the compaction lineage event. Return
   `Ok(Some(S))`.

### The atomic mechanism (generalizes the Slice B decorator)

Slice B added `do_update_table(commit, lineage: Option<&LineageEvent>)` +
`LineageEmittingCatalog`. Flush needs the same tx to *also* end-cap inline rows
at the snapshot the commit allocates. Three small changes:

- **`project_mirror` returns its allocated `SnapshotId`** (today it returns
  `()`), so the caller can thread `S` into the end-cap.
- **`do_update_table(commit, extras: CommitExtras)`** where
  ```rust
  #[derive(Default)]
  pub(crate) struct CommitExtras<'a> {
      pub lineage: Option<&'a LineageEvent>,
      pub end_cap: Option<InlineEndCap<'a>>,   // { table_id: i64, row_ids: &'a [i64] }
  }
  ```
  After `project_mirror` returns `S`, and before `tx.commit()`: if
  `extras.end_cap`, run the inline `UPDATE … end_snapshot = S …`; if
  `extras.lineage`, `pg_emit`. Both inside the existing CAS+mirror tx. The trait
  `update_table` delegates with `CommitExtras::default()`.
  (This replaces Slice B's `Option<&LineageEvent>` param;
  `LineageEmittingCatalog` updates to pass `CommitExtras { lineage: Some(..),
  ..Default::default() }`.)
- **`FlushCatalog<'a>`** — a sibling decorator of `LineageEmittingCatalog`
  holding `{ inner: &SqlCatalog, lineage: &LineageEvent, end_cap: InlineEndCap }`;
  its `update_table` calls `inner.do_update_table(commit, CommitExtras { lineage:
  Some(..), end_cap: Some(..) })`; all other `Catalog` methods delegate to
  `inner`. `flush_table` builds it per call and passes it to
  `append_batches_with_lineage`-style commit (or a thin `append_batches` variant
  that takes the decorator).

### Why it is correct — exactly-once + time-travel

With the new file at `begin_snapshot = S` and the inline rows at
`end_snapshot = S`, the shared live-row predicate gives, for any read at `at`:

- `at >= S`: file live (`begin_snapshot S <= at`); inline rows **not** live
  (`end_snapshot S > at` is false). Served once, from Parquet.
- `at < S` (time-travel): file **not** live (`S > at`); inline rows live
  (`end_snapshot S > at`). Served once, from inline.

Every row appears exactly once at every snapshot, and historical reads are
unchanged. Inline rows are **end-capped, not deleted** — physical GC of capped
inline rows (and of orphaned Parquet) is a separate, later concern, consistent
with loom's existing deferred-GC posture.

### Lineage

Flush emits **one** `lineage.event` with the table as both input and output —
`EventType::Complete`, `payload {"source":"flush"}` — built inside `flush_table`
from `(table, run_id)` and emitted atomically in the commit tx (via the
`CommitExtras.lineage` path). No new `EventType` variant; the compaction
semantics are carried by in==out==`DatasetId(table)` plus the payload marker.

Accepted tradeoff: `upstream(table)` / `downstream(table)` now include the table
itself (a self-loop). The `graph_step` queries already `select distinct`, so this
is a single benign extra row, not a traversal cycle hazard. (If self-loops ever
become noise, the read side can filter `a.name <> b.name`; out of scope here.)

## Error handling

- **No live inline rows** → `Ok(None)`: no Parquet, no snapshot, no lineage.
- **Parquet written, commit fails** → the Parquet orphans (documented;
  GC-deferred, same posture as ingest's write-then-commit). The inline rows stay
  live (not capped), so a re-run flushes them cleanly — **no data loss, no
  double-serve** (the orphaned file was never projected into the mirror).
- **Concurrent flush** → blocked by the advisory lock; serialized.
- **Concurrent inline write** → its row is outside `R` and gets a later
  snapshot; flushed next round.
- **Type/encode errors** during reconstruction → surfaced as
  `ControlPlaneError::Backend` (same as `inline_parquet`).

## Testing (all `loom_fixture_test`, postgres crate)

1. **Flush an inline-only table.** Several `inline_append`s, then `flush_table`.
   Assert: `IcebergCatalog::files(table, current)` now lists the flushed Parquet;
   `inline_parquet(table, current)` returns `None` (rows retired); reading the
   union yields each row **exactly once** (no dupes).
2. **Time-travel.** Capture the pre-flush current snapshot `S0`; after flush at
   `S`, assert inline rows are still live at `S0` (visible via `inline_parquet`)
   and the Parquet file is **not** live at `S0` (`files(table, S0)` excludes it);
   and the reverse at `S`.
3. **Mixed table (Parquet + inline).** A table that already has a Parquet
   snapshot plus later inline rows → flush merges the inline rows into a new
   snapshot; union at current is exact (old Parquet + new flushed Parquet, no
   inline).
4. **Concurrent inline write survives.** Insert a row, capture, flush, then a
   second `inline_append` lands a new row; assert the new row is still live
   inline after the flush (not capped), and a second flush moves it.
5. **Empty flush.** `flush_table` on a table with no live inline rows →
   `Ok(None)`, no snapshot allocated, no lineage event.
6. **Compaction lineage.** After a flush, `events_for(run_id)` has exactly one
   event with input==output==the table's dataset ref and the `flush` payload
   marker; it shares the flush snapshot's tx (present iff the flush committed).
7. **Slice B preserved.** `do_update_table`/`LineageEmittingCatalog` behaviour is
   unchanged through the `CommitExtras` refactor — the existing Slice B
   `append_batches_with_lineage` atomicity test stays green.

## Files

- **Create** `src/control-plane/postgres/src/iceberg_flush.rs` — `flush_table`,
  `FlushCatalog`, `InlineEndCap`; reuses `iceberg_inline` reconstruction +
  `land_parquet` create-if-absent (factor shared helpers out of those modules).
- **Modify** `src/control-plane/postgres/src/iceberg_inline.rs` — extract
  `inline_live_batch` (row_ids + RecordBatch) from `inline_parquet`'s body so
  flush and the read path share the reconstruction; add the end-cap `UPDATE`
  helper.
- **Modify** `src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs` —
  `do_update_table(commit, CommitExtras)`; `project_mirror` (an `impl SqlCatalog`
  method here) returns the allocated `SnapshotId`; trait `update_table` delegates
  with `CommitExtras::default()`; run the end-cap `UPDATE` + `pg_emit` inside the
  one commit tx after `project_mirror`.
- **Modify** `src/control-plane/postgres/src/iceberg_mirror.rs` — only if a
  shared mirror helper is needed (e.g. the inline end-cap `UPDATE` is better
  homed here than in `iceberg_flush`); otherwise unchanged.
- **Modify** `src/control-plane/postgres/src/iceberg_writer.rs` —
  `LineageEmittingCatalog` updates to the `CommitExtras` call; optionally a
  shared commit helper the `FlushCatalog` reuses.
- **Modify** `src/control-plane/postgres/src/lib.rs` — `pub mod iceberg_flush;`.
- **Modify** `src/control-plane/postgres/BUCK` — new `iceberg-flush`
  `loom_fixture_test` target.
- **Modify** `docs/spike/ICEBERG_ROADMAP.md` — move inline flush/compaction (#3)
  from deferred to done.

## Out of scope (deferred)

- **Triggering** — scheduler, byte/row threshold, HTTP/admin endpoint, or a
  queue-driven Transform-worker job. This slice ships only the callable
  primitive.
- **Physical GC** — deleting end-capped inline rows and orphaned Parquet.
- **Multi-table / batch flush**, partial flush, or flush of a chosen snapshot
  range. One table, all live inline rows, current state.
- **Overwrite/replace semantics** (roadmap #2) and per-column stats (#1) are
  independent.

## Branch

Stacks on PR #90 (Slice B) — it builds on `do_update_table`,
`append_batches_with_lineage`, and the inline tables. Branched off
`spec/iceberg-landing-backend`; rebase onto `main` once #90 merges.
