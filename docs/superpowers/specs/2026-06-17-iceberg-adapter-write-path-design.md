# Iceberg Adapter — Write Path (Slice 2) Design

> Slice 2 of the Iceberg adapter. Slice 1 (read path,
> `2026-06-16-iceberg-adapter-read-path-design.md`) shipped: a vendored SQL catalog, a
> loom-owned `iceberg_mirror.*` MVCC projection, and an `IcebergCatalog` implementing
> `core::Catalog` over the mirror, validated by the backend-agnostic `catalog_contract` /
> `catalog_delete_contract`. Slice 1 deliberately wrote only **synthetic** file metadata
> (no real Parquet bytes) from a test-only seeder. Slice 2 makes the write real.

**Goal:** Turn the test-only synthetic-file writer into a real Iceberg write path — real
Parquet bytes via the `iceberg` writer chain, committed atomically (Iceberg pointer CAS +
loom mirror projection in one Postgres transaction), with concurrency-safe snapshot ids.

**Status:** Design approved 2026-06-17. Supersedes the "deferred to slice 2" list in the
read-path spec (real Parquet writing, atomic pointer+mirror via the single transaction,
concurrency-safe snapshot allocation; per-column stats remain deferred — see Non-Goals).

---

## Decisions (locked)

1. **Scope — write machinery only.** Build a real Iceberg writer as a loom *library*
   component, validated by tests. Do **not** wire it into the production ingest service
   binary yet — DuckLake-vs-Iceberg backend selection in the HTTP ingest service is a
   separate later slice.
2. **Atomicity — single Postgres transaction.** The Iceberg pointer compare-and-swap and
   the loom mirror projection commit or roll back together, in one `Transaction<Postgres>`.
   No mirror/pointer divergence is ever observable. This is the payoff of vendoring the
   catalog.
3. **Per-column stats — deferred (YAGNI).** The slice-1 read path consumes no per-column
   stats (no pruning/predicate-pushdown surface exists). Project only `record_count` and
   `file_size_bytes` — now from the *real* written files. Add the full stats vocabulary
   (lower/upper bounds, null counts, value counts) when a pruning read slice needs it.
4. **Snapshot allocation — Postgres `SEQUENCE`.** `nextval` is concurrency-safe and
   monotonic and composes with the single-transaction model. Retires the
   `FIXME(slice-2)` `max(snapshot_id)+1` in `iceberg_mirror::next_snapshot`.
5. **Append only.** Slice 2 implements `fast_append`. Iceberg overwrite/replace (the
   analogue of DuckLake's `replace_files`) is a later slice.
6. **v57 third-party visibility — reindeer fixup first, buckify.sh fallback.** Attempt the
   native `visibility = ["PUBLIC"]` reindeer fixup; if reindeer rejects it on the
   transitive-only v57 packages, fall back to a named-allowlist visibility pass in
   `tools/buckify.sh`. (See "The v57 visibility blocker".)

## Non-Goals (deferred to later slices)

- Per-column file statistics and any pruning/predicate-pushdown read path.
- Iceberg overwrite/replace (table-content replacement).
- Wiring the Iceberg backend into the production ingest service binary.
- Multi-writer throughput tuning (the atomic commit holds the Postgres tx open across
  object-store manifest reads — acceptable for the single-writer tests; noted as a perf
  follow-up, not addressed here).

---

## Background: what slice 1 left in place

- **Vendored catalog** (`src/control-plane/postgres/src/iceberg_sql_catalog/`): Apache
  `iceberg-catalog-sql` v0.9.1, ported to sqlx 0.9, loom-owned. Its `update_table`
  (catalog.rs) does an optimistic pointer CAS:
  `UPDATE … SET metadata_location = $new WHERE … AND metadata_location = $current`; a
  0-row result is a retryable `CatalogCommitConflicts`. It currently calls
  `self.execute(…, None)` — opening *its own* Postgres transaction — and does **not** touch
  the mirror.
- **Mirror** (`iceberg_mirror.rs` + migration `0012_iceberg_mirror.sql`): the
  `iceberg_mirror.*` schema (`snapshot`, `table`, `column`, `data_file`), MVCC-versioned by
  `begin_snapshot`/`end_snapshot`, with a partial unique index enforcing one live row per
  `(namespace, name)`. Projection helpers: `next_snapshot`, `ensure_table`,
  `project_columns`, `project_files`, `mark_dropped`.
- **Read adapter** (`iceberg_catalog.rs`): `IcebergCatalog { pool }` implementing
  `core::Catalog`, serving `current_snapshot`/`snapshots`/`files`/`schema` from the mirror.
- **Seeder** (`fixture.rs` `IcebergWriter`, test-support): creates namespace+table via the
  vendored catalog, reads back the real schema, projects columns + **one synthetic
  `ProjectedFile` per batch** (no real Parquet). This is what slice 2 replaces.

## Critical constraint: `update_table` is the only atomic chokepoint

`iceberg`'s public commit path is `Transaction::commit(catalog)` → (private) `do_commit`,
which builds a `TableCommit` and calls `catalog.update_table(commit)`. The pieces inside
`do_commit` are **not** reachable from loom: `TransactionAction` (whose `commit(&table) ->
ActionCommit` produces the staged updates) is `pub(crate)`, and `do_commit`/`apply` are
private. Therefore loom **cannot** reimplement the commit to interleave its own Postgres
transaction around it.

The consequence drives the whole atomicity design: since `update_table` is the single
public chokepoint **and loom owns it** (vendored), the single-transaction atomicity must
live *inside* `update_table`. The mirror cannot be supplied by the writer through the
trait (the trait only carries a `TableCommit`); instead the mirror is **derived from the
canonical staged Iceberg metadata** that `update_table` already computes. This is also the
most correct design: the mirror becomes a deterministic projection of the Iceberg catalog,
written in the same transaction as the pointer it projects.

All the metadata-reading APIs this requires are public in `iceberg` 0.9.1 and verified
against the pinned source:

- `Snapshot::load_manifest_list(file_io, table_metadata)` → `ManifestList`
- `ManifestList::entries()` → `&[ManifestFile]`; `ManifestFile::load_manifest(file_io)` →
  `Manifest`
- `Manifest::entries()` → `&[ManifestEntryRef]`; `ManifestEntry::{data_file, file_path,
  record_count, file_size_in_bytes, content_type}`
- `staged_table.metadata().current_schema()` (in-memory; the column projection source)
- `iceberg::arrow::schema_to_arrow_schema(schema)` (build the arrow `SchemaRef` for the
  writer)

## The v57 visibility blocker (must be solved first)

The writer chain names types iceberg does **not** re-export: `parquet::file::properties::
WriterProperties` and `arrow_array::RecordBatch`. iceberg 0.9.1 pins arrow/parquet
**57.3.1**, but loom's `//third-party:parquet` / `//third-party:arrow` unversioned aliases
resolve to **58** (because `src/services/ingest` directly depends on arrow/parquet 58, so
reindeer marks those aliases `PUBLIC`). The v57 libraries exist as versioned targets
(`arrow-array-57`, `parquet-57`, …) but carry `visibility = []` (package-private to
`third-party/BUCK`). Adding `parquet = "57"` as a direct dep does **not** work: it collides
on the single unversioned `parquet` alias (already 58).

**Resolution (Task 0):** make the exact v57 crates the writer needs reachable from
`//src/control-plane/postgres`:

- **Primary — reindeer fixup.** Add `third-party/fixups/<crate>/fixups.toml` with
  `visibility = ["PUBLIC"]` for `parquet`, `arrow-array`, `arrow-schema` (and any
  additional arrow-* the writer transitively names). Regenerate with `./tools/buckify.sh`.
- **Fallback — buckify.sh pass.** If reindeer rejects setting visibility on transitive-only
  packages (the slice-1 symptom: "visibility only settable on public packages"), add a
  small, named-allowlist post-processing pass to `tools/buckify.sh` that flips
  `visibility = []` → `visibility = ["PUBLIC"]` for those exact versioned targets after
  reindeer generates `third-party/BUCK`. This is deterministic, re-applied on every
  buckify (cannot drift), and sits alongside the existing archive-ordering post-process —
  a structural transform of reindeer output, **not** a band-aid over stale fixup data.

Task 0 is proven by `buck2 build //src/control-plane/postgres:postgres` after the postgres
`BUCK` gains `//third-party:parquet-57` and `//third-party:arrow-array-57` deps.

---

## Architecture

```
writer (iceberg_writer.rs, library)
  ├─ arrow batches ── iceberg writer chain ──► real Parquet files ──► Vec<DataFile>
  │     ParquetWriterBuilder(WriterProperties, arrow_schema)
  │       └─ RollingFileWriterBuilder
  │            └─ DataFileWriterBuilder
  └─ Transaction::new(&table).fast_append().add_data_files(files).apply(tx)
        .commit(&catalog)                       # iceberg drives; funnels to ▼

vendored catalog update_table (iceberg_sql_catalog/catalog.rs, loom-owned)
  staged = commit.apply(current_table)          # in-memory
  write staged metadata JSON ──► object store   # before tx (orphaned on rollback, GC'd)
  BEGIN (one Transaction<Postgres>)
    CAS UPDATE pointer  (0 rows ⇒ ROLLBACK + retryable CatalogCommitConflicts)
    snap_id = nextval('iceberg_mirror.snapshot_seq')
    ensure_table + project_columns  ← staged.current_schema()       (in-memory)
    project_files                   ← new snapshot's manifests, Added entries (object-store reads)
  COMMIT                                          # pointer + mirror, one fsync

read path (iceberg_catalog.rs, unchanged) serves from iceberg_mirror.*
```

### Files

| File | Change |
|------|--------|
| `third-party/fixups/{parquet,arrow-array,arrow-schema,…}/fixups.toml` | New: `visibility = ["PUBLIC"]` (or buckify.sh pass if rejected) |
| `tools/buckify.sh` | Fallback only: named-allowlist v57 visibility pass |
| `src/control-plane/postgres/migrations/0013_iceberg_snapshot_seq.sql` | New: `create sequence iceberg_mirror.snapshot_seq` |
| `src/control-plane/postgres/src/iceberg_mirror.rs` | `next_snapshot` → `nextval`; tx-threaded projection forms; manifest→`ProjectedFile` derivation |
| `src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs` | `update_table` + `drop_table`: one Postgres tx threading CAS + mirror projection |
| `src/control-plane/postgres/src/iceberg_writer.rs` | New: real writer (writer chain + fast_append commit) |
| `src/control-plane/postgres/src/lib.rs` | `pub mod iceberg_writer;` |
| `src/control-plane/postgres/src/fixture.rs` | `IcebergWriter` seeder calls the real writer (drops synthetic `ProjectedFile`) |
| `src/control-plane/postgres/BUCK` | `//third-party:{parquet-57,arrow-array-57,arrow-schema-57}` deps; new `iceberg_writer` test target(s) |
| `src/control-plane/postgres/Cargo.toml` | arrow-array/arrow-schema/parquet (for sqlx-aware build env; deps resolved via reindeer) |
| `src/control-plane/postgres/.sqlx/` | Regenerated for the new sequence query (`tools/sqlx-prepare.sh`) |

### Component contracts

- **`iceberg_writer`** — *what:* writes Arrow `RecordBatch`es to real Parquet and commits
  them as an Iceberg `fast_append`. *Input:* a loaded `iceberg::table::Table`, the vendored
  `SqlCatalog`, and the batches. *Output:* the committed `Table` (and, via the catalog, a
  new loom `SnapshotId`). *Depends on:* `iceberg` writer chain, arrow-array/parquet 57,
  the vendored catalog. Pure write — no mirror logic (that lives in `update_table`).
- **`update_table` (extended)** — *what:* atomically commits the Iceberg pointer CAS and
  the loom mirror projection. *Input:* a `TableCommit` (from iceberg). *Output:* the staged
  `Table`. *Invariant:* on success, pointer and mirror agree; on CAS conflict, neither
  changed and the error is retryable.
- **mirror projection (tx-threaded)** — `iceberg_mirror.rs` functions take a
  `&mut PgConnection`/transaction handle so they enlist in `update_table`'s tx. A new
  derivation builds `ProjectedColumn`s from `current_schema()` and `ProjectedFile`s from
  the new snapshot's Added manifest entries.

### Error handling

- **CAS conflict** (`rows_affected == 0`): roll back the whole tx (pointer + mirror
  untouched), return `CatalogCommitConflicts` with `retryable = true` — iceberg's
  `Transaction::commit` backoff retries with a refreshed base.
- **Object-store / manifest read failure** inside the tx: roll back; the Parquet + metadata
  JSON already written orphan and are GC'd later (no catalog state changed).
- **Sequence/insert failure**: roll back; same orphan-and-GC outcome.
- All mapped through the existing `backend(...)` / `from_sqlx_error(...)` conversions; no
  new error variants.

---

## Testing

All tests are `rust_test` integration targets (no inline `#[cfg(test)]`), fixture-backed
ones via `loom_fixture_test`. They route real Parquet through the writer and read it back
through the slice-1 `IcebergCatalog`.

1. **Round-trip** (`tests/iceberg_writer.rs`): write real batches via `iceberg_writer` →
   read back through `IcebergCatalog`; assert the schema matches, the `data_file` rows
   point at files that exist on disk, and `record_count`s equal the batch row counts.
2. **Valid Parquet**: open the written file(s) (DuckLake/DuckDB `read_parquet` or the
   iceberg reader) and assert row count + column values — proves real Parquet bytes, not
   just metadata.
3. **Concurrency** (`tests/iceberg_snapshot_seq.rs` or within the writer test): two
   concurrent appends receive distinct, monotonically increasing loom snapshot ids and both
   commit without a PK collision — the behavior the retired `max+1` FIXME could not
   guarantee.
4. **Atomicity**: force a CAS conflict (commit against a stale base, or a concurrent writer)
   and assert the losing commit left **no** orphaned `iceberg_mirror` rows (snapshot/table/
   column/data_file) — pointer and mirror rolled back together.
5. **Contracts unchanged**: `iceberg_passes_catalog_contract` and
   `iceberg_passes_catalog_delete_contract` still pass, now backed by real writes (the
   seeder drives the real writer).

**Definition of done:** `buck2 test //src/...` green (incl. the new tests and the
unchanged contracts); `tools/clippy-all.sh` clean; `prek run --all-files` clean; committed
`.sqlx` refreshed; the real writer produces Parquet readable by an independent reader.
