# DuckLake single-catalog write recipe (source-grounded reference)

## What this is

A **source-authoritative, implementation-ready** reference for how the DuckDB
`ducklake` extension writes its Postgres control-plane catalog in the
**single-catalog, DuckDB-compatible** layout. loom is becoming a native DuckLake
writer — emitting `ducklake_*` rows itself via sqlx — and must match what the
DuckDB engine produces and reads back, byte-for-byte, so that a DuckDB-engine
interop test (`ATTACH` a loom-written catalog, `SELECT`) passes.

This document is grounded in the **official extension source**,
`duckdb/ducklake` at commit **`e6a3bd0a`** — the exact `ducklake` build shipped
in loom's pinned **DuckDB 1.5.3**. At this commit the metadata manager
hard-writes catalog **spec version `1.0`** (`ducklake_metadata_manager.cpp:207`,
and `MigrateV04` stamps `'1.0'` at `ducklake_metadata_manager.cpp:328`). Citations
are `ducklake/<path>:<line>` against that tree. It supersedes an earlier
empirically-captured draft; where the source and the prior transcript agree both
are cited, and where they differed **the source wins** and the discrepancy is
called out in-line.

### Pinned versions

- DuckDB CLI / engine: **v1.5.3**.
- `ducklake` extension build: **`e6a3bd0a`**. The catalog stamps
  `ducklake_metadata` with `version = '1.0'` (the DuckLake **spec** version, not
  the engine version) and `created_by = 'DuckDB <SourceID>'`
  (`ducklake_metadata_manager.cpp:207`, value built with `DuckDB::SourceID()` at
  `:210`). The empirical capture saw `created_by = DuckDB 14eca11bd9`.
- `postgres_scanner` is loaded alongside (transport for the
  `ducklake:postgres:` DSN). All metadata writes are shipped to Postgres via
  `CALL postgres_execute(<catalog>, '<sql>')`
  (`postgres_metadata_manager.cpp:111`).
- DuckLake catalog spec: **v1.0** (single catalog per Postgres database).

### IMPORTANT correction vs. the original brief: `ducklake_schema_versions`

The task brief asserted that at v1.0 `ducklake_schema_versions` has **no**
`table_id`, and that `table_id` is a "v1.1 migration" not applicable here. **That
is wrong for `e6a3bd0a`.** A fresh `InitializeDuckLake` at this commit creates

```sql
CREATE TABLE ... ducklake_schema_versions(begin_snapshot BIGINT, schema_version BIGINT, table_id BIGINT);
```

directly (`ducklake_metadata_manager.cpp:199`), i.e. the **3-column** shape *is*
the v1.0 bootstrap shape. The `table_id` column was introduced by the v0.3→v0.4
migration (`MigrateV03`, `ALTER TABLE ... ADD COLUMN ... table_id`,
`ducklake_metadata_manager.cpp:284`, plus the per-table backfill at `:301–323`);
by the time the catalog is at spec `1.0` (`MigrateV04`, `:326`) the column is
present, and a from-scratch initialize bakes it in. The writer
(`InsertNewSchema`) emits the full 3-tuple `(begin_snapshot, schema_version,
table_id)` (`ducklake_metadata_manager.cpp:4670`). **loom must write the 3-column
form.** This matches the empirical capture (which recorded `(1, 1, 1)`), so on
this point the capture was right and the brief's premise was wrong. (The thing
that does *not* exist at `e6a3bd0a` is a separate `ducklake_schema_versions.hpp`
/ `V1_1_DEV_1` per-table migration on top of v1.0 — confirmed; HEAD differs.)

### Single-catalog layout, in one sentence

One DuckLake catalog == one set of `ducklake_*` tables in one Postgres
schema (default `main`/`public`). There is **no** `ducklake_catalog`,
`ducklake_catalog_snapshot_map`, or `ducklake_catalog_schema_map` table, and
**no** `catalog_id` column anywhere — those exist only in the
`datafusion-contrib/datafusion-ducklake` *multicatalog* fork and must be
translated away (see §7). Confirmed: the official `InitializeDuckLake`
(`ducklake_metadata_manager.cpp:176–208`) creates no such tables/columns.

---

## 1. Bootstrap: who creates the `ducklake_*` tables, and the exact DDL

The entire bootstrap is one function, `DuckLakeMetadataManager::InitializeDuckLake`
(`ducklake_metadata_manager.cpp:165–215`). On the first `ATTACH` against an empty
Postgres database it runs one batch of `CREATE TABLE`s followed by the seed
INSERTs, inside the extension's commit transaction. (For Postgres the batch is
shipped via `postgres_execute`; the extension wraps writes in a
`REPEATABLE READ` transaction — see §2.)

### Exact bootstrap DDL (verbatim, `ducklake_metadata_manager.cpp:176–204`)

`{METADATA_CATALOG}` is substituted with the quoted metadata schema identifier
(`postgres_metadata_manager.cpp:106`; for the standard single-catalog ATTACH this
resolves to `"public"` — the empirical capture saw `"public"."ducklake_*"`).
`TIMESTAMPTZ` renders as `TIMESTAMP WITH TIME ZONE` on Postgres.

```sql
ducklake_metadata(key VARCHAR NOT NULL, value VARCHAR NOT NULL, scope VARCHAR, scope_id BIGINT);
ducklake_snapshot(snapshot_id BIGINT PRIMARY KEY, snapshot_time TIMESTAMPTZ, schema_version BIGINT, next_catalog_id BIGINT, next_file_id BIGINT);
ducklake_snapshot_changes(snapshot_id BIGINT PRIMARY KEY, changes_made VARCHAR, author VARCHAR, commit_message VARCHAR, commit_extra_info VARCHAR);
ducklake_schema(schema_id BIGINT PRIMARY KEY, schema_uuid UUID, begin_snapshot BIGINT, end_snapshot BIGINT, schema_name VARCHAR, path VARCHAR, path_is_relative BOOLEAN);
ducklake_table(table_id BIGINT, table_uuid UUID, begin_snapshot BIGINT, end_snapshot BIGINT, schema_id BIGINT, table_name VARCHAR, path VARCHAR, path_is_relative BOOLEAN);
ducklake_view(view_id BIGINT, view_uuid UUID, begin_snapshot BIGINT, end_snapshot BIGINT, schema_id BIGINT, view_name VARCHAR, dialect VARCHAR, sql VARCHAR, column_aliases VARCHAR);
ducklake_tag(object_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, key VARCHAR, value VARCHAR);
ducklake_column_tag(table_id BIGINT, column_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, key VARCHAR, value VARCHAR);
ducklake_data_file(data_file_id BIGINT PRIMARY KEY, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, file_order BIGINT, path VARCHAR, path_is_relative BOOLEAN, file_format VARCHAR, record_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, row_id_start BIGINT, partition_id BIGINT, encryption_key VARCHAR, mapping_id BIGINT, partial_max BIGINT);
ducklake_file_column_stats(data_file_id BIGINT, table_id BIGINT, column_id BIGINT, column_size_bytes BIGINT, value_count BIGINT, null_count BIGINT, min_value VARCHAR, max_value VARCHAR, contains_nan BOOLEAN, extra_stats VARCHAR);
ducklake_file_variant_stats(data_file_id BIGINT, table_id BIGINT, column_id BIGINT, variant_path VARCHAR, shredded_type VARCHAR, column_size_bytes BIGINT, value_count BIGINT, null_count BIGINT, min_value VARCHAR, max_value VARCHAR, contains_nan BOOLEAN, extra_stats VARCHAR);
ducklake_delete_file(delete_file_id BIGINT PRIMARY KEY, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, data_file_id BIGINT, path VARCHAR, path_is_relative BOOLEAN, format VARCHAR, delete_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, encryption_key VARCHAR, partial_max BIGINT);
ducklake_column(column_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, table_id BIGINT, column_order BIGINT, column_name VARCHAR, column_type VARCHAR, initial_default VARCHAR, default_value VARCHAR, nulls_allowed BOOLEAN, parent_column BIGINT, default_value_type VARCHAR, default_value_dialect VARCHAR);
ducklake_table_stats(table_id BIGINT, record_count BIGINT, next_row_id BIGINT, file_size_bytes BIGINT);
ducklake_table_column_stats(table_id BIGINT, column_id BIGINT, contains_null BOOLEAN, contains_nan BOOLEAN, min_value VARCHAR, max_value VARCHAR, extra_stats VARCHAR);
ducklake_partition_info(partition_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT);
ducklake_partition_column(partition_id BIGINT, table_id BIGINT, partition_key_index BIGINT, column_id BIGINT, transform VARCHAR);
ducklake_file_partition_value(data_file_id BIGINT, table_id BIGINT, partition_key_index BIGINT, partition_value VARCHAR);
ducklake_files_scheduled_for_deletion(data_file_id BIGINT, path VARCHAR, path_is_relative BOOLEAN, schedule_start TIMESTAMPTZ);
ducklake_inlined_data_tables(table_id BIGINT, table_name VARCHAR, schema_version BIGINT);
ducklake_column_mapping(mapping_id BIGINT, table_id BIGINT, type VARCHAR);
ducklake_name_mapping(mapping_id BIGINT, column_id BIGINT, source_name VARCHAR, target_field_id BIGINT, parent_column BIGINT, is_partition BOOLEAN);
ducklake_schema_versions(begin_snapshot BIGINT, schema_version BIGINT, table_id BIGINT);
ducklake_macro(schema_id BIGINT, macro_id BIGINT, macro_name VARCHAR, begin_snapshot BIGINT, end_snapshot BIGINT);
ducklake_macro_impl(macro_id BIGINT, impl_id BIGINT, dialect VARCHAR, sql VARCHAR, type VARCHAR);
ducklake_macro_parameters(macro_id BIGINT, impl_id BIGINT, column_id BIGINT, parameter_name VARCHAR, parameter_type VARCHAR, default_value VARCHAR, default_value_type VARCHAR);
ducklake_sort_info(sort_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT);
ducklake_sort_expression(sort_id BIGINT, table_id BIGINT, sort_key_index BIGINT, expression VARCHAR, dialect VARCHAR, sort_direction VARCHAR, null_order VARCHAR);
```

That is **27** `CREATE TABLE` statements emitted by `InitializeDuckLake`. (The
empirical capture counted "28 tables"; the discrepancy is that DuckLake
additionally creates per-table **inlined-data** tables named
`ducklake_inlined_data_<table_id>_<schema_version>` lazily on demand — see
`GetInlinedTableName`/`GetInlinedTableQuery`, `ducklake_metadata_manager.cpp:2241–2254`
and `:2292` — not at bootstrap, and **only when data inlining is enabled**. With
`DATA_INLINING_ROW_LIMIT 0` none are created. loom does not inline.) Confirmed:
**no** `ducklake_catalog`, `ducklake_catalog_snapshot_map`, or
`ducklake_catalog_schema_map`.

### Constraints / keys

`PRIMARY KEY` appears only on the `*_id` columns of `ducklake_snapshot`,
`ducklake_snapshot_changes`, `ducklake_schema`, `ducklake_data_file`, and
`ducklake_delete_file` (see the DDL above). **No** foreign keys, **no** sequences,
**no** `GENERATED ALWAYS AS IDENTITY`; the only `NOT NULL`s are
`ducklake_metadata.key`/`.value`. All id allocation is software-side (§5).

### Seed rows (`ducklake_metadata_manager.cpp:205–208`)

```sql
INSERT INTO {METADATA_CATALOG}.ducklake_snapshot         VALUES (0, NOW(), 0, 1, 0);
INSERT INTO {METADATA_CATALOG}.ducklake_snapshot_changes VALUES (0, 'created_schema:"main"', NULL, NULL, NULL);
INSERT INTO {METADATA_CATALOG}.ducklake_metadata (key, value)
  VALUES ('version', '1.0'), ('created_by', 'DuckDB <SourceID>'), ('data_path', '<DATA_PATH>'), ('encrypted', 'false');
INSERT INTO {METADATA_CATALOG}.ducklake_schema          VALUES (0, UUID(), 0, NULL, 'main', 'main/', true);
```

- Snapshot 0: `(snapshot_id=0, snapshot_time=NOW(), schema_version=0, next_catalog_id=1, next_file_id=0)`.
  `next_catalog_id` is **1** because id `0` was consumed by the `main` schema.
- `main` schema: `schema_id=0`, fresh `UUID()`, `begin_snapshot=0`,
  `end_snapshot=NULL`, `schema_name='main'`, `path='main/'`,
  `path_is_relative=true`.
- `ducklake_metadata` is the only seed INSERT that uses an **explicit column
  list** (`(key, value)`); everything else in the extension is positional
  (see §3). `data_path` is the `DATA_PATH` from ATTACH, normalized with a
  trailing slash via `StorePath` (`:174`). `encrypted` is `'false'` unless ATTACH
  requested encryption (`:175`).

So a fresh single-catalog DuckLake is **not** empty: snapshot 0, a `main` schema
(id 0), `next_catalog_id = 1`, `next_file_id = 0`, `schema_version = 0`. All four
match the empirical capture.

### Bootstrap ownership recommendation (unchanged)

**Recommendation: let the pinned duckdb-cli's bare `ATTACH` create+seed the
catalog (option 2); use this document's DDL knowledge only for loom's
compile-time `query!` validation and interop test fixtures (option 1's
knowledge), not for production bootstrap.** Rationale is unchanged from the prior
draft: the 27-table DDL is large and versioned, and `InitializeDuckLake` is the
single source of truth — reproducing it by hand invites drift, while loom already
vendors the exact binary+extension hermetically and `tools/sqlx-prepare.sh`
already performs this ATTACH. loom then becomes a pure **row writer** on the
hot path. The DDL above is now grounded in `ducklake_metadata_manager.cpp:176–208`
rather than a capture, so a hand-maintained fixture can be validated against it.

---

## 2. Transaction shape

A commit == exactly one Postgres transaction at `REPEATABLE READ`. The extension
loads the latest snapshot, builds the entire write batch as one multi-statement
string, ships it, then commits:

- `FlushChanges` (`ducklake_transaction.cpp:2575`) assembles
  `batch_queries = InsertSnapshot() + CommitChanges(...) + WriteSnapshotChanges(...)`
  (`:2621–2624`), executes it once via `metadata_manager->Execute(...)` (`:2625`),
  then `connection->Commit()` (`:2633`).
- On Postgres, `Execute` → `postgres_execute(<catalog>, '<batch sql>')`
  (`postgres_metadata_manager.cpp:113–115, 111`); the batch is the
  DuckDB-dialect SQL the writers below generate, run transactionally inside
  Postgres.
- The extension precedes each write with a separate read-only transaction that
  loads the whole current catalog into memory. loom does **not** need to
  reproduce those reads; it just needs its row writes to land correctly, and
  DuckDB re-reads the catalog on its next attach/refresh.

loom should write a commit as: take the commit serialization lock, read the
latest snapshot, allocate all ids in memory, emit the ordered INSERT/UPDATE
batch, `COMMIT`.

### Batch ordering within one commit (from `FlushChanges` + `CommitChanges`)

`CommitChanges` (`ducklake_transaction.cpp:2330–2469`) appends sub-queries in
this order; combined with `FlushChanges` the full per-commit order is:

1. `INSERT INTO ducklake_snapshot` (the new snapshot row) — `InsertSnapshot()`,
   `:2621`.
2. dropped schemas / new schemas (`WriteNewSchemas`, `:2367`).
3. new tables + their columns (`WriteNewTables` then `WriteNewColumns`,
   `:2375, :2381`); partition keys, views, tags, sort keys interleaved.
4. new data files: **table-level stats first, then the file rows.** Inside
   `GetNewDataFiles` the per-table `UpdateGlobalTableStats` (which emits
   `ducklake_table_stats` + `ducklake_table_column_stats`) is appended at
   `:2186`, *before* `WriteNewDataFiles` (which emits `ducklake_data_file` +
   `ducklake_file_column_stats`) at `:2403`.
5. delete files / compactions (n/a for plain append).
6. `INSERT INTO ducklake_schema_versions` for tables with schema changes —
   `InsertNewSchema`, `:2467`.
7. `INSERT INTO ducklake_snapshot_changes` — `WriteSnapshotChanges()`, `:2624`.

This ordering matches the empirical capture (snapshot → stats → file → file
stats → snapshot_changes), and pins down two things the capture left ambiguous:
the `ducklake_snapshot` row is written **first**, and `ducklake_schema_versions`
is written **after** the table/column rows.

---

## 3. Per-operation write recipe (single-catalog, positional INSERTs)

**Every extension write below is a positional `INSERT ... VALUES (...)` with no
column list** (the sole exception is the bootstrap `ducklake_metadata` seed,
§1). The tuple order therefore must match the DDL in §1 exactly. The literal
`{SNAPSHOT_ID}`/`{SCHEMA_VERSION}`/`{NEXT_CATALOG_ID}`/`{NEXT_FILE_ID}`
placeholders are substituted just before execution
(`ducklake_metadata_manager.cpp:2094–2103`; Postgres path
`postgres_metadata_manager.cpp:85–88`).

### Op A — `CREATE TABLE main.t (id BIGINT, name VARCHAR)` (new table in existing `main`)

```sql
-- 1. snapshot row  (InsertSnapshot, ducklake_metadata_manager.cpp:3515-3516)
INSERT INTO "public".ducklake_snapshot VALUES (1, NOW(), 1, 2, 0);
--   (snapshot_id=1, NOW(), schema_version=1, next_catalog_id=2, next_file_id=0)

-- 2. table row  (WriteNewTables, ducklake_metadata_manager.cpp:2273-2275, 2283)
INSERT INTO "public".ducklake_table VALUES (1, '<uuid>', 1, NULL, 0, 't', 't/', true);
--   (table_id, table_uuid, begin_snapshot={SNAPSHOT_ID}, end_snapshot=NULL,
--    schema_id, table_name, path, path_is_relative)

-- 3. column rows  (ColumnToSQLRecursive, ducklake_metadata_manager.cpp:2193-2196, 2286)
INSERT INTO "public".ducklake_column VALUES
  (1, 1, NULL, 1, 1, 'id',   'int64',   NULL, 'NULL', true, NULL, 'literal', 'duckdb'),
  (2, 1, NULL, 1, 2, 'name', 'varchar', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
--   (column_id, begin_snapshot={SNAPSHOT_ID}, end_snapshot=NULL, table_id,
--    column_order, column_name, column_type, initial_default, default_value,
--    nulls_allowed, parent_column, default_value_type, default_value_dialect)

-- 4. schema-version row  (InsertNewSchema, ducklake_metadata_manager.cpp:4670-4671)
INSERT INTO "public".ducklake_schema_versions VALUES (1, 1, 1);
--   (begin_snapshot={SNAPSHOT_ID}, schema_version, table_id)

-- 5. changes row  (WriteSnapshotChanges, ducklake_metadata_manager.cpp:3529-3532)
INSERT INTO "public".ducklake_snapshot_changes VALUES (1, 'created_table:"main"."t"', NULL, NULL, NULL);
--   (snapshot_id, changes_made, author, commit_message, commit_extra_info)
```

Source-confirmed value notes:

- **`column_order == column_id`** and both are **1-based**. `ColumnToSQLRecursive`
  sets `column_order = column_id` (`ducklake_metadata_manager.cpp:2190–2191`), and
  the per-table column counter starts at 1 (`DuckLakeSchemaEntry::CreateTable`
  sets `idx_t column_id = 1;`, `ducklake_schema_entry.cpp:82`, incremented per
  field in `DuckLakeFieldData::FromColumns`, `ducklake_field_data.cpp:77`).
- **`column_type`** is the canonical DuckLake type string, not a Postgres type:
  `BIGINT → 'int64'`, `VARCHAR → 'varchar'` (`ducklake_types.cpp:21, 42`).
- **`default_value`** is the **string literal** `'NULL'` (not SQL NULL) with
  `default_value_type='literal'` and `default_value_dialect='duckdb'`
  (`ducklake_metadata_manager.cpp:2166–2168`); `initial_default` is real SQL NULL
  when unset (`:2163–2164`). When a real default exists, `default_value` holds the
  quoted literal/expression text and `default_value_type` is `'literal'` or
  `'expression'` (`:2170–2188`).
- **`table.path`** is the table-name-derived relative path (`'t/'`),
  `path_is_relative=true`; the bootstrap `main` schema path was `'main/'`. The
  resolution chain is `data_path + schema.path + table.path + file.path`.
- **`schema_version` bumped 0→1** because this is DDL — `FlushChanges` does
  `commit_snapshot.schema_version++` only inside `if (SchemaChangesMade())`
  (`ducklake_transaction.cpp:2614–2617`). DML does **not** bump it.
- **`next_catalog_id` 1→2**: the table consumed one catalog id
  (`GetNewTable`: `table_entry.id = TableIndex(commit_snapshot.next_catalog_id++)`,
  `ducklake_transaction.cpp:1497`). Columns did **not** consume catalog ids — see
  §5.
- **No `ducklake_schema` INSERT** here: `main` pre-existed from bootstrap. If the
  statement also created a *new* schema, a `ducklake_schema` row would be emitted
  first (`WriteNewSchemas`, `ducklake_metadata_manager.cpp:2133, 2137`), tuple
  `(schema_id, schema_uuid, begin_snapshot={SNAPSHOT_ID}, end_snapshot=NULL,
  schema_name, path, path_is_relative)`, and a `created_schema:"<name>"` segment
  would lead `changes_made`.

### Op B — first `INSERT` into `t` (one data file, 10 rows, no nulls)

```sql
-- 1. snapshot row
INSERT INTO "public".ducklake_snapshot VALUES (2, NOW(), 1, 2, 1);
--   schema_version stays 1 (DML); next_file_id 0 -> 1

-- 2. table-level stats  (UpdateGlobalTableStats, INSERT branch, ducklake_metadata_manager.cpp:4024-4028)
INSERT INTO "public".ducklake_table_stats VALUES (1, 10, 10, 444);
--   (table_id, record_count, next_row_id, file_size_bytes)

INSERT INTO "public".ducklake_table_column_stats VALUES
  (1, 1, false, NULL, '0', '9', NULL),
  (1, 2, false, NULL, 'x', 'x', NULL);
--   (table_id, column_id, contains_null, contains_nan, min_value, max_value, extra_stats)

-- 3. data file  (WriteNewDataFiles, VALUES path, ducklake_metadata_manager.cpp:3306-3309, 3356)
INSERT INTO "public".ducklake_data_file VALUES
  (0, 1, 2, NULL, NULL, 'ducklake-<uuid>.parquet', true, 'parquet', 10, 444, 249, 0, NULL, NULL, NULL, NULL);
--   (data_file_id, table_id, begin_snapshot={SNAPSHOT_ID}, end_snapshot=NULL,
--    file_order=NULL, path, path_is_relative, file_format='parquet',
--    record_count, file_size_bytes, footer_size, row_id_start, partition_id,
--    encryption_key, mapping_id, partial_max)

-- 4. per-file column stats  (ducklake_metadata_manager.cpp:3317-3320, 3359)
INSERT INTO "public".ducklake_file_column_stats VALUES
  (0, 1, 1, 88, 10, 0, '0', '9', NULL, NULL),
  (0, 1, 2, 48, 10, 0, 'x', 'x', NULL, NULL);
--   (data_file_id, table_id, column_id, column_size_bytes, value_count,
--    null_count, min_value, max_value, contains_nan, extra_stats)

-- 5. changes row
INSERT INTO "public".ducklake_snapshot_changes VALUES (2, 'inserted_into_table:1', NULL, NULL, NULL);
```

Source-confirmed value notes:

- **`data_file_id` starts at 0**, from `next_file_id`
  (`GetNewDataFile`: `data_file.id = DataFileIndex(commit_snapshot.next_file_id++)`,
  `ducklake_transaction.cpp:2052`). Snapshot 2 records the post-commit
  `next_file_id = 1`.
- **`begin_snapshot`** renders as `{SNAPSHOT_ID}` (the commit id) unless an
  explicit one is set (`ducklake_metadata_manager.cpp:3295–3296`).
- **`file_format` is hard-coded `'parquet'`** in the tuple format string
  (`ducklake_metadata_manager.cpp:3307`). `file_order`, `partition_id`,
  `mapping_id`, `partial_max` are NULL for a plain append; `footer_size` is the
  Parquet footer length (loom must compute it from the file it writes);
  `file_size_bytes` is the real on-disk size; `row_id_start=0` for the first file.
- **`encryption_key`** is `NULL` when absent, else the key **base64-encoded** and
  single-quoted (`ducklake_metadata_manager.cpp:3299–3300`). The capture only saw
  NULL.
- **`path`** is the bare relative filename `ducklake-<uuidv7>.parquet`,
  `path_is_relative=true`.

### Op C — second `INSERT` into `t` (one data file, 5 rows) — stats now UPDATE

```sql
INSERT INTO "public".ducklake_snapshot VALUES (3, NOW(), 1, 2, 2);
--   next_file_id 1 -> 2; schema_version still 1

-- table-level stats now UPDATE  (UpdateGlobalTableStats, UPDATE branch, :4031-4049)
UPDATE "public".ducklake_table_stats
  SET record_count=15, file_size_bytes=848, next_row_id=15 WHERE table_id=1;

WITH new_values(tid, cid, new_contains_null, new_contains_nan, new_min, new_max, new_extra_stats) AS (
  VALUES (1,1,false,NULL,'0','9',NULL),(1,2,false,NULL,'x','y',NULL)
)
UPDATE "public".ducklake_table_column_stats
  SET contains_null=CAST(new_contains_null AS BOOLEAN), contains_nan=CAST(new_contains_nan AS BOOLEAN),
      min_value=new_min, max_value=new_max, extra_stats=new_extra_stats
  FROM new_values WHERE table_id=tid AND column_id=cid;

INSERT INTO "public".ducklake_data_file VALUES
  (1, 1, 3, NULL, NULL, 'ducklake-<uuid>.parquet', true, 'parquet', 5, 404, 246, 10, NULL, NULL, NULL, NULL);
--   data_file_id=1, begin_snapshot=3, record_count=5, footer_size=246, row_id_start=10

INSERT INTO "public".ducklake_file_column_stats VALUES
  (1, 1, 1, 51, 5, 0, '0', '4', NULL, NULL),
  (1, 1, 2, 48, 5, 0, 'y', 'y', NULL, NULL);

INSERT INTO "public".ducklake_snapshot_changes VALUES (3, 'inserted_into_table:1', NULL, NULL, NULL);
```

Source-confirmed notes:

- The **INSERT-vs-UPDATE switch** for both stats tables is driven by
  `stats.initialized` (`ducklake_metadata_manager.cpp:4022`): first commit
  inserts, later commits UPDATE. The `WITH new_values ... CAST(... AS BOOLEAN)`
  merge is verbatim source (`:4040–4049`); ANSI `CAST` (not `::boolean`) is used
  deliberately for cross-backend portability (comment at `:4035–4039`).
- `row_id_start=10` continues from `ducklake_table_stats.next_row_id` (which the
  prior commit advanced to 10); contiguous, monotonic, never reused.

---

## 4. `changes_made` grammar (verbatim from source)

Built in `DuckLakeTransaction::WriteSnapshotChanges`
(`ducklake_transaction.cpp:1047–1141`), one comma-joined string per snapshot;
segments are emitted in this order: dropped schemas, dropped tables/views,
created schemas, created tables/views, created/dropped macros, then
`inserted_into_table` / `deleted_from_table` / `altered_table` / …
(`AddChangeInfo`, `:1034–1045`, calls at `:1127–1139`).

| Operation | `changes_made` | Source |
|---|---|---|
| bootstrap (snapshot 0) | `created_schema:"main"` | `ducklake_metadata_manager.cpp:206` |
| `CREATE SCHEMA s` | `created_schema:"s"` | `ducklake_transaction.cpp:1072–1073` |
| `CREATE TABLE main.t` | `created_table:"main"."t"` | `:1083–1084` |
| `CREATE VIEW main.v` | `created_view:"main"."v"` | `:1083` (`is_view` branch) |
| `INSERT INTO t` (id=1) | `inserted_into_table:1` | `:1127`, `:1041–1043` |
| `DELETE FROM t` (id=1) | `deleted_from_table:1` | `:1128` |
| `ALTER TABLE t` (id=1) | `altered_table:1` | `:1129` |
| `DROP SCHEMA` (id=0) | `dropped_schema:0` | `:1063–1064` |
| `DROP TABLE` (id=1) | `dropped_table:1` | `:1066` |

Format rules (all confirmed):

- Names are double-quoted via `KeywordHelper::WriteQuoted(x, '"')`; `created_table`
  is fully qualified `"<schema>"."<table>"`.
- `inserted_into_table` / `deleted_from_table` / `altered_table` /
  `dropped_*` carry the **numeric id**, unquoted (`AddChangeInfo` appends
  `to_string(id.index)`). Note the deliberate asymmetry: create-segments use
  quoted names, mutate/drop-segments use ids.
- The whole string is then `SQLStringOrNull`-quoted into the INSERT
  (`ducklake_metadata_manager.cpp:3519–3532`); empty ⇒ SQL NULL.
- `author` / `commit_message` / `commit_extra_info` come from
  `transaction.GetCommitInfo()` and are NULL unless a commit message was set
  (`:3531–3532`). loom may populate these.

---

## 5. Id / counter allocation rules (definitive, from source)

All ids are software counters held in the **latest `ducklake_snapshot` row**;
there are no DB sequences. The latest snapshot is read by
`GetLatestSnapshotQuery`:
`SELECT snapshot_id, schema_version, next_catalog_id, next_file_id FROM ducklake_snapshot WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM ducklake_snapshot)`
(`ducklake_metadata_manager.cpp:3650–3652`; Postgres variant
`postgres_metadata_manager.cpp:121–129`). That snapshot is loaded into
`commit_snapshot`, the counters are advanced **in memory** during commit, and the
post-commit values are written into the new `ducklake_snapshot` row by
`InsertSnapshot` (`:3515–3516`). The `DuckLakeSnapshot` struct carries exactly
these four fields (`include/common/ducklake_snapshot.hpp:28–31`).

- **`snapshot_id`** — `commit_snapshot.snapshot_id++` in `FlushChanges`
  (`ducklake_transaction.cpp:2613`). Since `commit_snapshot` was initialized from
  the latest loaded snapshot (`= GetSnapshot()`, `:2611`), and that read returns
  `MAX(snapshot_id)`, **`snapshot_id` == max(snapshot_id) + 1**. This *reconciles*
  the prior draft's "max+1" with the source's `snapshot_id++` — they are the same
  thing; the in-memory counter started at the DB max. One new `ducklake_snapshot`
  row per commit. On retry the snapshot is re-read and re-incremented
  (`:2674–2678`).
- **`next_catalog_id`** — the single, unified catalog-object id counter. From
  source it advances via `commit_snapshot.next_catalog_id++` for **schemas**
  (`:1373`), **partitions** (`:1401`), **sort orders** (`:1450`), **tables**
  (`:1497`), **views** (`:1801`), and **macros** (`:1822`). The value stored in a
  snapshot row is the **next free** id after the commit. **CORRECTION to the prior
  draft (which marked this "partly inferred / inferred split"):** there is no
  split — but the set of consumers is narrower than "schema/table/**column**".
  **`column_id`s do NOT come from `next_catalog_id`.** Column (field) ids are a
  **per-table 1-based counter** assigned at table-create / alter time
  (`ducklake_schema_entry.cpp:82` seeds `column_id = 1`;
  `ducklake_field_data.cpp:77, 137, 306–311` increment it; the value rides on the
  `DuckLakeColumnInfo.id` and is written straight through by
  `ColumnToSQLRecursive`). This is exactly why Op A allocated `column_id` 1 and 2
  yet `next_catalog_id` advanced by only 1 (for the table). **loom must assign
  `column_id` densely per table starting at 1, and allocate `next_catalog_id`
  only for schema/table/partition/sort/view/macro objects.**
- **`next_file_id`** — the unified file/mapping id counter. Advances via
  `commit_snapshot.next_file_id++` for **data files** (`:2052`), **delete files**
  (`:2195`), and **column-mapping ids** (`:2273`). Confirmed: it spans data +
  delete + mapping ids (not just data files). Stored value is the post-commit
  next free id.
- **`row_id_start`** — per-table, drawn from `ducklake_table_stats.next_row_id`,
  not from a snapshot counter. First file: 0; after N rows the table's
  `next_row_id` is N; next file's `row_id_start = N`. Contiguous, monotonic, never
  reused. `next_row_id` is read back in the stats load
  (`ducklake_metadata_manager.cpp:853, 920`) and re-persisted by
  `UpdateGlobalTableStats` (`:4026` insert, `:4033` update). loom must persist
  this per-table counter.
- **`schema_version`** — starts 0 (bootstrap). Bumped `+1` **only on DDL**
  (`if (SchemaChangesMade()) commit_snapshot.schema_version++`,
  `ducklake_transaction.cpp:2614–2617`); carried unchanged on DML. On a DDL bump,
  `InsertNewSchema` writes **one `ducklake_schema_versions` row per table that had
  a schema change** (`:2453–2467` builds `tables_with_schema_changes`;
  `ducklake_metadata_manager.cpp:4664–4674` emits
  `(begin_snapshot={SNAPSHOT_ID}, schema_version, table_id)` per table). Captured
  progression 0→1 (CREATE) →1 (INSERT) →1 (INSERT) confirmed.

Because these counters live in `ducklake_snapshot`, loom **must serialize
commits** so two writers don't read the same latest snapshot and allocate
colliding ids. The extension's own conflict path retries on PK/unique/conflict
errors (`RetryOnError`, `ducklake_transaction.cpp:2560–2572`); loom (no
`ducklake_catalog` row to `FOR UPDATE`) should serialize on the latest
`ducklake_snapshot` row (`SELECT ... ORDER BY snapshot_id DESC LIMIT 1 FOR UPDATE`)
or a Postgres advisory lock, inside the same `REPEATABLE READ` transaction.

---

## 6. Atomicity + orphan-Parquet note

Commits are atomic: the entire `ducklake_*` write set is one Postgres
transaction (§2) — it all lands or none does. The general DuckLake write order is
**(1) write Parquet to object store, then (2) commit catalog rows** (the data
files are flushed before `FlushChanges` assembles the catalog batch). If a writer
flushes Parquet and then the catalog commit fails/crashes, the Parquet object is
orphaned: it exists on S3/MinIO but no `ducklake_data_file` row references it. On
commit failure the extension calls `CleanupFiles()` to remove files it wrote
(`ducklake_transaction.cpp:2652`, `:1143–1146`), but a hard crash between flush
and commit cannot run that cleanup. The catalog stays consistent; the object
store can accumulate orphans. This is why DuckLake ships
`ducklake_files_scheduled_for_deletion` (columns `data_file_id, path,
path_is_relative, schedule_start` — **no** `catalog_id`,
`ducklake_metadata_manager.cpp:195`) and a two-phase vacuum.

**loom's future GC must reconcile object-store contents against
`ducklake_data_file` to reclaim orphans from failed/crashed writers — it cannot
rely on the catalog alone to know every file it wrote.** (The empirical capture's
two abort cases leaked nothing because the failures fired before any Parquet
flush; that is not a guarantee for the crash window above.)

---

## 7. What loom must NOT copy from `datafusion-ducklake`

`datafusion-contrib/datafusion-ducklake` is a **multicatalog** native Postgres
writer. Each item below is validated against the official `e6a3bd0a` source:

1. **No multicatalog tables.** `ducklake_catalog`,
   `ducklake_catalog_snapshot_map`, `ducklake_catalog_schema_map` do not exist —
   `InitializeDuckLake` never creates them (`ducklake_metadata_manager.cpp:176–204`).
   Drop all INSERTs into the two map tables.
2. **No `catalog_id` column** anywhere, including on
   `ducklake_files_scheduled_for_deletion` (4 columns, no `catalog_id`,
   `:195`). Drop the field, the `lock_catalog`/`with_pool` machinery, and any
   `assert_*_in_catalog`.
3. **Replace the catalog lock.** There is no `ducklake_catalog` row to
   `FOR UPDATE`; serialize on the latest `ducklake_snapshot` row or a Postgres
   advisory lock (§5).
4. **Plain `schema/` paths**, not `cat_{id}/{schema}`. DuckDB writes `'main/'`
   (`:208`) and table paths like `'t/'`.
5. **Match DuckDB's full DDL.** dfdl's `ducklake_column` and `ducklake_data_file`
   DDL omit columns DuckDB writes (`initial_default`, `default_value`,
   `default_value_type`, `default_value_dialect` on `ducklake_column`, `:189`;
   `file_order`, `file_format`, `partition_id`, `partial_max`, `mapping_id` on
   `ducklake_data_file`, `:185`). Use the §1 DDL and the §3 full tuples.
6. **Software ids, not IDENTITY / RETURNING.** DuckDB assigns
   `snapshot_id`/`*_id`/`data_file_id` from the in-memory snapshot counters (§5);
   there are no `GENERATED ALWAYS AS IDENTITY` columns and no `RETURNING`. Allocate
   from `next_catalog_id` (schema/table/partition/sort/view/macro),
   `next_file_id` (data/delete/mapping), per-table 1-based `column_id`, and
   per-table `next_row_id`.
7. **Write the stats tables dfdl ignores.** Both `ducklake_file_column_stats`
   (per-file, INSERT each commit, `ducklake_metadata_manager.cpp:3317–3320`) and
   `ducklake_table_column_stats` (table aggregate, INSERT-then-merge-UPDATE,
   `:4024–4049`) are required for query pruning and DuckDB interop. **`value_count`
   is the count of NON-NULL values (`num_values − null_count`), not the row
   count** (`ducklake_transaction.cpp:2021–2031`) — the empirical capture's
   "value_count = record_count" held only because that file had zero nulls.
8. **Write `ducklake_snapshot_changes` every commit** with the §4 grammar (dfdl
   never does).
9. **Seed `ducklake_metadata`** (`version='1.0'`, `created_by`, `data_path`,
   `encrypted='false'`) and snapshot 0 + `main` — or (recommended) let the pinned
   duckdb-cli `ATTACH` do this once at provision time (§1).
10. **dfdl's row-id / `ducklake_table_stats` mechanics match DuckDB** and
    translate directly (`next_row_id`, accumulating
    `record_count`/`file_size_bytes`, `:4024–4034`) — keep them.

### Min/max stats encoding (confirmed)

Per-file and table-level `min_value`/`max_value` are stored as **VARCHAR text**
regardless of column type. The stats values are already strings in
`DuckLakeColumnStats`; `FromColumnStats` renders them via
`DuckLakeUtil::StatsToString` (`ducklake_transaction.cpp:2018–2019`), which
returns the SQL literal `NULL` if the value contains a `\0` byte and otherwise
single-quotes the text (`ducklake_util.cpp:90–96`). So `id BIGINT` min/max are
decimal text (`'0'`, `'9'`), `name VARCHAR` are the raw strings. `null_count`,
`value_count`, `contains_nan` are NULL when the underlying stat is absent
(`ducklake_transaction.cpp:2032–2040`). Populated `ducklake_file_column_stats`
columns: `data_file_id, table_id, column_id, column_size_bytes, value_count,
null_count, min_value, max_value, contains_nan, extra_stats` (all ten; `extra_stats`
NULL unless variant/extended stats exist, `:2041–2044`).

---

## 8. Summary of source corrections vs. the empirical draft

| Topic | Prior draft | Source (`e6a3bd0a`) verdict |
|---|---|---|
| `ducklake_schema_versions` shape | `(begin_snapshot, schema_version, table_id)` (capture) — brief said "no table_id at v1.0" | **3-column with `table_id` is the v1.0 bootstrap shape** (`:199`, writer `:4670`). Capture right, brief wrong. |
| `column_id` source | "inferred split" / "managed in table's own id space" | **Confirmed: per-table 1-based counter, NOT `next_catalog_id`** (`schema_entry.cpp:82`, `field_data.cpp:77`). |
| `next_catalog_id` consumers | "schema/table/column/object" | schema/table/**partition/sort/view/macro** only; **not columns** (`transaction.cpp:1373,1401,1450,1497,1801,1822`). |
| `next_file_id` consumers | "data files" | data + **delete files + column-mapping ids** (`:2052,2195,2273`). |
| `snapshot_id` rule | "max+1" | `snapshot_id++` on a snapshot loaded as `MAX(snapshot_id)` ⇒ same as max+1 (`:2611–2613`, `metadata_manager:3650`). |
| `value_count` | "= record_count (no nulls)" | **non-null count = `num_values − null_count`** (`transaction.cpp:2021–2031`). |
| table count at bootstrap | "28 tables created at ATTACH" | **27 `CREATE TABLE`s**; inlined-data tables are lazy/optional, not bootstrap (`:176–204`, `:2241`). |
| INSERT form | positional, no column list | **Confirmed** for all writers; bootstrap `ducklake_metadata` is the lone exception with `(key, value)` (`:207`). |
| batch order | snapshot → stats → file → file-stats → changes | **Confirmed and pinned**: snapshot first; schema_versions after columns; table stats before file rows (`FlushChanges`/`CommitChanges` `:2330–2469, 2621–2624`). |
| `encryption_key` | NULL (capture) | NULL when absent, else **base64** of the key (`:3299–3300`). |
