# Selective compaction (size-threshold) — design (2026-06-17)

> Transform-pillar maintenance slice. Coalesce a table's many small Parquet files
> into fewer size-targeted ones, leaving already-large files untouched. Reuses the
> DataFusion read/write path (`datafusion-io`) and the supersession mechanics from
> the overwrite slice (`2026-06-17-overwrite-output-mode-design.md`), but as a
> *partial* expire rather than a whole-table replace.

## Motivation

Repeated appends (ingest landings, append-mode transforms, inline action writes)
leave a table as a growing pile of small Parquet files. Every governed read scans
all live files, so file proliferation degrades read latency and inflates catalog
row counts. Compaction rewrites a table's small files into fewer large ones,
preserving the exact row set and full time-travel history.

"Selective" means **size-threshold**: only files below a threshold are rewritten;
files already at or above the target size are left in place, so a large table is
not needlessly re-materialized.

## Scope

In scope:

- A new control-plane `Tx` primitive — `compact_files` — that supersedes a
  *specified subset* of a table's live files and writes new ones, adjusting table
  stats by delta (not reset-to-zero, which is what the whole-table `replace_files`
  does).
- A service-layer `compact_table` function (in the transform crate) that selects
  the small files, rewrites them via DataFusion into size-targeted files, and
  commits through `compact_files`.

Out of scope (noted as later work):

- Any queue job type or HTTP endpoint for compaction. This slice ships the
  **library primitive only**, matching every prior pillar's "part-1 = primitive,
  endpoint/queue-job later" precedent.
- A maintenance-event audit log (see Lineage below).
- Row-level deletes / merge-on-read delete vectors (see Row-id assumption below).

## Component 1 — control-plane primitive: `Tx::compact_files`

The shipped `replace_files` (overwrite) expires **all** live files of a table and
resets `ducklake_table_stats` to zero, because the table's whole content is being
replaced. A *partial* compaction leaves other files live, so it must expire only
the named subset and adjust stats by delta. Rather than overload `replace_files`
with a mode, add an additive sibling and leave the shipped primitive untouched:

```rust
async fn compact_files(
    &mut self,
    table: &TableRef,
    expire: &[String],
    write: &[DataFile],
) -> Result<()>;
```

- `expire` — the relative paths of the live data files being superseded (exactly
  as stored in `ducklake_data_file.path`; the caller obtains them from a
  `Catalog::files` read, so they match byte-for-byte).
- `write` — the new coalesced data files, same `DataFile` shape `append_files`
  takes.

### Postgres adapter

In `commit_snapshot` (postgres `snapshot.rs`), add a compaction loop alongside the
existing append and replace loops. For each staged `(table, expire, write)`:

1. Resolve `table_id`.
2. Expire the named files and capture their exact contributions:

   ```sql
   UPDATE ducklake_data_file
      SET end_snapshot = $new
    WHERE table_id = $t
      AND path = ANY($expire)
      AND end_snapshot IS NULL
   RETURNING record_count, file_size_bytes
   ```

   **Assert `rows_affected == expire.len()`.** A shortfall means one or more of the
   named files was already superseded (e.g. a concurrent compaction) — return
   `ControlPlaneError::Conflict` so the whole transaction rolls back. Without this
   guard the loser of a race would write its coalesced output while the small
   files it "expired" were already gone, **duplicating** their rows.
3. Delta-adjust `ducklake_table_stats`: subtract the summed `record_count` and
   `file_size_bytes` returned above. `next_row_id` is **not** decremented (row-ids
   are monotonic; see assumption below).

   ```sql
   UPDATE ducklake_table_stats
      SET record_count = record_count - $expired_records,
          file_size_bytes = file_size_bytes - $expired_bytes
    WHERE table_id = $t
   ```
4. For each file in `write`, call the existing `write_data_file` (it re-adds
   `record_count`/`file_size_bytes`, assigns a fresh contiguous row-id range from
   `next_row_id`, writes the `ducklake_data_file` + `ducklake_file_column_stats`
   rows, and updates `ducklake_table_column_stats`). **No** `ducklake_table_column_stats`
   DELETE — other files remain live, so the table-level column aggregate must not be
   wiped. (The table-level column stat ends reflecting the last-written file's
   min/max, identical in fidelity to the existing append path.)

Snapshot-changes segments for the compaction: `deleted_from_table:<id>` followed
by `inserted_into_table:<id>` (when `write` is non-empty), mirroring the replace
loop's segment grammar.

New `.sqlx` cache entries for the added queries; regenerate via
`tools/sqlx-prepare.sh` and commit.

### Memory adapter

`MemoryTx` gains a `staged_compactions: Vec<(TableRef, Vec<String>, Vec<DataFile>)>`
field (constructed empty alongside `staged_replacements`). `compact_files` stages a
tuple. On `commit`, after the replace loop, run a compaction loop: allocate a new
snapshot `s`, set `end = Some(s)` on each live `FileRef` whose `path` is in the
expire set, then push the `write` files live at `s`. The memory adapter derives
table stats from live files on read, so no explicit stat arithmetic is needed —
removing the small files and adding the coalesced ones self-corrects record counts.

The `has_catalog_ops` short-circuit in `commit` must also consider
`staged_compactions` so a compaction-only transaction yields a snapshot id.

### Contract (testkit, both adapters)

`snapshot_compact_contract`: append `a` (3 rows) + `b` (5 rows) + `c` (2 rows) as
three separate files, then `compact_files(expire=[a, b], write=[d (8 rows)])`. Assert:

- The current snapshot lists exactly `{c, d}` (a and b expired, d added).
- A read at the pre-compaction snapshot still lists `{a, b, c}` (time-travel
  preserved).
- The current snapshot's total `record_count` equals the pre-compaction total
  (10 — compaction preserves the row set).

Wired into both adapters' snapshot conformance suites, plus a dedicated postgres
fixture test that drives a real DuckLake catalog.

### Row-id assumption (recorded constraint)

`write_data_file` assigns each new file a fresh contiguous row-id range from the
table's monotonic `next_row_id`; compacted files therefore receive **new** row-ids
rather than preserving the superseded files' ranges. This is sound today because
loom has no merge-on-read delete vectors — nothing references a row by its
`row_id_start`. If row-level deletes / delete files land later, compaction must
preserve row-ids (or rewrite delete vectors) to stay correct. Documented in
`docs/FUTURE.md`.

## Component 2 — service primitive: `compact_table`

New module `src/services/transform/src/compact.rs`:

```rust
pub struct CompactConfig {
    /// Files strictly smaller than this are candidates for compaction.
    pub small_file_threshold_bytes: i64,
    /// Output sizing for the coalesced files.
    pub write: WriteConfig,
}

pub async fn compact_table(
    cp: &dyn ControlPlane,
    store: Arc<dyn ObjectStore>,
    run_id: &str,
    table: &TableRef,
    cfg: &CompactConfig,
) -> Result<Option<SnapshotId>, CompactError>;
```

Flow:

1. Resolve the current snapshot; read its live files
   (`Catalog::files(table, snapshot, unbounded)`).
2. Partition into `small` (`file_size_bytes < cfg.small_file_threshold_bytes`) and
   untouched.
3. **No-op guard:** if `small.len() < 2`, return `Ok(None)` — coalescing fewer than
   two files cannot reduce file count and would churn the catalog without benefit.
   This also guarantees convergence: re-running compaction on an already-compacted
   table is a no-op.
4. `scan_table` over **only** the `small` subset, register under the table name,
   run `SELECT *`, and `df.collect()` the rows. Collecting fully materializes the
   small files' rows into memory *before* the transaction opens, so reading and
   then expiring the same files in one commit is safe (no read-after-expire).
5. `write_dataset(store, "<schema>/<table>/<run_id>", schema, &batches, &cfg.write)`
   → the coalesced `DataFile`s (size-targeted by `WriteConfig::target_file_size_bytes`).
6. One transaction: `tx.compact_files(table, &small_paths, &new_files)` →
   `tx.commit()` → `Ok(Some(snapshot))`.

No `create_table` (the table already exists). **No lineage event** — compaction is
a physical reorganization that does not change the data's provenance; emitting a
self-edge (`table → table`) would pollute `upstream()` traversals, and there is no
lineage cycle guard yet. A dedicated maintenance-event log is noted as later work.

`CompactError` mirrors `TransformError`'s shape (DataFusion / Scan / Write /
ControlPlane / NoSnapshot variants as needed).

## Component 3 — invocation surface

Library primitive only. No queue job type, no HTTP route. A queue-driven
compaction job (so a worker compacts on a schedule or on demand) and any operator
endpoint are explicitly later work, consistent with how ingest and transform each
shipped a load-bearing primitive before their networked surfaces.

## Testing

- **Adapter contract** — `snapshot_compact_contract` (above), run by both the
  memory and postgres conformance suites; a postgres fixture test drives a real
  DuckLake catalog end to end.
- **`compact_table` selection unit** — given a synthetic live-file list, the small
  partition is exactly the sub-threshold files; `< 2` small files yields `None`
  without scanning or writing.
- **e2e** (fixture + DuckDB read-back):
  - Append three small files, `compact_table` with a threshold above their size →
    a new snapshot whose current files are a single coalesced file; DuckDB reads
    back the identical rows and row count.
  - With one large file (≥ threshold) plus small files present, compaction leaves
    the large file live and coalesces only the smalls.
  - Time-travel to the pre-compaction snapshot reads the original small files.
  - The no-op case (`< 2` small files) returns `None` and creates no snapshot.

## Task breakdown

1. **`Tx::compact_files` primitive** — core trait method; memory adapter
   (`staged_compactions` + commit loop + `has_catalog_ops`); postgres adapter
   (compaction loop in `commit_snapshot`, RETURNING-driven delta, race assertion);
   `.sqlx` refresh; `snapshot_compact_contract` in testkit + both adapter
   conformance callers + a postgres fixture test.
2. **`compact_table` service function** — `compact.rs` (selection, no-op guard,
   scan-small/write/commit), `CompactConfig`/`CompactError`, a selection unit test,
   and the fixture e2e.
3. **Docs** — mark compaction delivered in
   `docs/superpowers/specs/2026-06-06-loom-roadmap.md`; update the compaction /
   file-supersession bullet in `docs/FUTURE.md` (mechanism now delivered; record
   the row-id assumption and the still-deferred watermark/incremental output and
   queue/endpoint surfaces).
