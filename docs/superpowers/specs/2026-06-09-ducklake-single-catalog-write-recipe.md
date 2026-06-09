# DuckLake single-catalog write recipe (spike reference)

## What this is

An **empirically captured, implementation-ready reference** for how the DuckDB
`ducklake` extension writes its Postgres control-plane catalog in the
**single-catalog, DuckDB-compatible** layout. loom is going to become a native
DuckLake writer — writing `ducklake_*` rows itself via sqlx — and must match
what the DuckDB engine produces and reads back, byte-for-byte, so that a
DuckDB-engine interop test (`ATTACH` a loom-written catalog, `SELECT`) passes.

The authoritative parts of this document were captured by booting the pinned
hermetic Postgres with `log_statement='all'` + `log_min_duration_statement=0`,
attaching a DuckLake catalog the standard single-catalog way via the pinned
duckdb-cli, running a fixed operation sequence, and reading back the exact SQL
the extension emitted plus the resulting rows. Sections marked **(captured)**
are from that log; sections marked **(inferred)** are reasoned from the
`datafusion-ducklake` source.

### Pinned versions (verified against the running binaries)

- DuckDB CLI: **v1.5.3** (`SELECT version()` → `v1.5.3`).
- `ducklake` extension build: **`e6a3bd0a`** (from `duckdb_extensions()`); the
  catalog stamps `ducklake_metadata` with `created_by = DuckDB 14eca11bd9` and
  `version = 1.0` (the DuckLake catalog **spec version**, not the engine
  version).
- `postgres_scanner` extension loaded alongside (transport for the
  `ducklake:postgres:` DSN).
- DuckLake catalog spec: **v1.0** (single catalog per Postgres database).

### Single-catalog layout, in one sentence

One DuckLake catalog == one set of `ducklake_*` tables in one Postgres
database/schema (`public`). There is **no** `ducklake_catalog`,
`ducklake_catalog_snapshot_map`, or `ducklake_catalog_schema_map` table, and
**no** `catalog_id` column anywhere — those exist only in the
`datafusion-contrib/datafusion-ducklake` *multicatalog* fork and must be
translated away (see "Differences from datafusion-ducklake").

## Annotation source

`/tmp/dfdl/src/metadata_writer_postgres.rs`
(`datafusion-contrib/datafusion-ducklake`) is a working native Postgres writer
and is cited throughout to explain the *why* of each field and the counter
math. It is **multicatalog**, so every citation below also flags where loom
(single-catalog) must differ.

---

## 1. Bootstrap: who creates the `ducklake_*` tables, and the exact schema

**(captured)** On the very first `ATTACH` against an empty Postgres database,
the extension runs, inside one `BEGIN ... ISOLATION LEVEL REPEATABLE READ` /
`COMMIT`, a long series of `CREATE TABLE "public"."ducklake_*"(...)` statements
(28 tables total, plus a couple of indexes), then seeds initial rows via
`COPY ... FROM STDIN (FORMAT BINARY)` (binary COPY, so the seed *values* are not
in the statement log — they were read back from the live rows, below).

### Tables the single-catalog catalog creates (all 28)

```
ducklake_column            ducklake_macro                  ducklake_snapshot
ducklake_column_mapping    ducklake_macro_impl             ducklake_snapshot_changes
ducklake_column_tag        ducklake_macro_parameters       ducklake_sort_expression
ducklake_data_file         ducklake_metadata               ducklake_sort_info
ducklake_delete_file       ducklake_name_mapping           ducklake_table
ducklake_file_column_stats ducklake_partition_column       ducklake_table_column_stats
ducklake_file_partition_value ducklake_partition_info      ducklake_table_stats
ducklake_file_variant_stats   ducklake_schema              ducklake_tag
ducklake_files_scheduled_for_deletion ducklake_schema_versions ducklake_view
ducklake_inlined_data_tables
```

Confirmed: **NO** `ducklake_catalog`, `ducklake_catalog_snapshot_map`, or
`ducklake_catalog_schema_map`. Those are multicatalog-only.

### Exact DDL for the tables loom writes (captured, verbatim)

These are the exact `CREATE TABLE` statements the extension emitted. **Note the
column ordering and types** — DuckDB reads these back by explicit column list,
but a native writer that uses positional `INSERT ... VALUES (...)` (as the
extension itself does — see §3) **must** match this order exactly.

```sql
CREATE TABLE "public"."ducklake_metadata"(
  "key" VARCHAR NOT NULL, "value" VARCHAR NOT NULL,
  "scope" VARCHAR, "scope_id" BIGINT);

CREATE TABLE "public"."ducklake_snapshot"(
  "snapshot_id" BIGINT PRIMARY KEY, "snapshot_time" TIMESTAMP WITH TIME ZONE,
  "schema_version" BIGINT, "next_catalog_id" BIGINT, "next_file_id" BIGINT);

CREATE TABLE "public"."ducklake_snapshot_changes"(
  "snapshot_id" BIGINT PRIMARY KEY, "changes_made" VARCHAR,
  "author" VARCHAR, "commit_message" VARCHAR, "commit_extra_info" VARCHAR);

CREATE TABLE "public"."ducklake_schema"(
  "schema_id" BIGINT PRIMARY KEY, "schema_uuid" UUID,
  "begin_snapshot" BIGINT, "end_snapshot" BIGINT,
  "schema_name" VARCHAR, "path" VARCHAR, "path_is_relative" BOOLEAN);

CREATE TABLE "public"."ducklake_table"(
  "table_id" BIGINT, "table_uuid" UUID,
  "begin_snapshot" BIGINT, "end_snapshot" BIGINT,
  "schema_id" BIGINT, "table_name" VARCHAR,
  "path" VARCHAR, "path_is_relative" BOOLEAN);

CREATE TABLE "public"."ducklake_column"(
  "column_id" BIGINT, "begin_snapshot" BIGINT, "end_snapshot" BIGINT,
  "table_id" BIGINT, "column_order" BIGINT,
  "column_name" VARCHAR, "column_type" VARCHAR,
  "initial_default" VARCHAR, "default_value" VARCHAR,
  "nulls_allowed" BOOLEAN, "parent_column" BIGINT,
  "default_value_type" VARCHAR, "default_value_dialect" VARCHAR);

CREATE TABLE "public"."ducklake_data_file"(
  "data_file_id" BIGINT PRIMARY KEY, "table_id" BIGINT,
  "begin_snapshot" BIGINT, "end_snapshot" BIGINT, "file_order" BIGINT,
  "path" VARCHAR, "path_is_relative" BOOLEAN, "file_format" VARCHAR,
  "record_count" BIGINT, "file_size_bytes" BIGINT, "footer_size" BIGINT,
  "row_id_start" BIGINT, "partition_id" BIGINT, "encryption_key" VARCHAR,
  "mapping_id" BIGINT, "partial_max" BIGINT);

CREATE TABLE "public"."ducklake_file_column_stats"(
  "data_file_id" BIGINT, "table_id" BIGINT, "column_id" BIGINT,
  "column_size_bytes" BIGINT, "value_count" BIGINT, "null_count" BIGINT,
  "min_value" VARCHAR, "max_value" VARCHAR,
  "contains_nan" BOOLEAN, "extra_stats" VARCHAR);

CREATE TABLE "public"."ducklake_table_stats"(
  "table_id" BIGINT, "record_count" BIGINT,
  "next_row_id" BIGINT, "file_size_bytes" BIGINT);

CREATE TABLE "public"."ducklake_table_column_stats"(
  "table_id" BIGINT, "column_id" BIGINT,
  "contains_null" BOOLEAN, "contains_nan" BOOLEAN,
  "min_value" VARCHAR, "max_value" VARCHAR, "extra_stats" VARCHAR);

CREATE TABLE "public"."ducklake_schema_versions"(
  "begin_snapshot" BIGINT, "schema_version" BIGINT, "table_id" BIGINT);
```

Notes on constraints/keys (captured): only `ducklake_snapshot`,
`ducklake_snapshot_changes`, `ducklake_schema`, `ducklake_data_file`, and
`ducklake_delete_file` carry a `PRIMARY KEY` (always on the `*_id` column).
**No** foreign keys, **no** sequences, **no** `GENERATED ALWAYS AS IDENTITY`,
**no** `NOT NULL` except `ducklake_metadata.key`/`.value`. All id allocation is
software-side (§5). This matches the prior spike's "no FKs/sequences; ids are
software counters" finding.

### Initial seed rows (captured, read back from live rows)

The bootstrap `COPY` populates:

`ducklake_metadata` (4 rows, all `scope`/`scope_id` NULL):

| key | value |
|---|---|
| `version` | `1.0` |
| `created_by` | `DuckDB 14eca11bd9` |
| `data_path` | `/tmp/dlcap/dldata/` (the `DATA_PATH` from ATTACH, with trailing slash) |
| `encrypted` | `false` |

`ducklake_snapshot` snapshot 0:

| snapshot_id | snapshot_time | schema_version | next_catalog_id | next_file_id |
|---|---|---|---|---|
| 0 | NOW() | 0 | 1 | 0 |

`ducklake_snapshot_changes` snapshot 0: `changes_made = 'created_schema:"main"'`.

`ducklake_schema` (the implicit `main` schema, **created at bootstrap**):

| schema_id | schema_uuid | begin_snapshot | end_snapshot | schema_name | path | path_is_relative |
|---|---|---|---|---|---|---|
| 0 | (a UUID) | 0 | NULL | `main` | `main/` | true |

So a fresh single-catalog DuckLake is **not** empty: it has snapshot 0, a
`main` schema with `schema_id = 0`, and `next_catalog_id = 1` (id 0 was consumed
by the `main` schema). `next_file_id = 0`.

### OPEN QUESTION for the plan: who owns the bootstrap DDL?

Two options:

1. **loom reproduces the DDL itself** (a loom migration / bootstrap routine that
   `CREATE TABLE IF NOT EXISTS`-es the 28 tables and seeds snapshot 0 +
   `main` + metadata). Pro: loom owns its schema, can co-locate it with the
   other loom Postgres schemas, no dependency on a duckdb-cli at provision time.
   Con: loom must keep 28 table DDLs in lockstep with the pinned extension; any
   drift breaks interop, and the extension may add columns across versions.

2. **Rely on an initial DuckDB `ATTACH` to create them** (loom shells the pinned
   duckdb-cli once at catalog-provision time to do the bare ATTACH, which
   creates+seeds everything, then loom only ever writes rows). Pro: the DDL is
   guaranteed to match the engine exactly, for whatever extension version is
   pinned; zero drift risk. Con: provisioning now needs the duckdb-cli on the
   path and a one-shot subprocess.

**Recommendation: option 2 for bootstrap, option 1's knowledge for writes.**
Let the pinned duckdb-cli's bare `ATTACH` create and seed the catalog (it is the
single source of truth for the schema and the snapshot-0/`main` seed, and loom
already vendors that exact binary + extension hermetically — see
`tools/sqlx-prepare.sh`, which does exactly this ATTACH today). loom then
becomes a pure **row writer** against the already-bootstrapped tables. This
eliminates the 28-table-DDL-drift risk entirely while keeping loom's hot path
(commits) native sqlx. loom should only need to hand-maintain the DDL knowledge
above for its `query!` compile-time validation and for an interop *test*
fixture, not for production bootstrap. (loom's existing sqlx flow already proves
this pattern: the `.sqlx` cache is generated against a duckdb-`ATTACH`-bootstrapped
catalog.)

---

## 2. Transaction shape (captured)

Every DuckLake write commit the extension does is wrapped in:

```sql
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
  -- ... all the ducklake_* row writes for this commit ...
COMMIT;
```

A commit == exactly one Postgres transaction at `REPEATABLE READ`. (This matches
the prior spike and loom's tx-isolation contract.) The extension precedes each
write transaction with a *separate* read-only transaction that `COPY`-outs the
entire current catalog (snapshot/schema/table/column/... ) to load it into
memory — loom does not need to reproduce those reads, but should know DuckDB
**reads the whole catalog on attach/refresh**, so loom's row writes just need to
land correctly and DuckDB will pick them up on its next load.

loom should write a commit as: one `BEGIN`, the ordered INSERTs/UPDATEs below,
one `COMMIT`. loom must allocate all ids *within* that transaction after taking
its commit lock (§5).

---

## 3. Per-operation write recipe (captured, single-catalog form)

The extension batches all writes for one commit into a single multi-statement
string (shown split here for readability). **Values are positional**
`INSERT ... VALUES (...)` with **no column list** — so the tuple order must
match the DDL above exactly.

### Op A — `CREATE TABLE lake.main.t (id BIGINT, name VARCHAR)`

```sql
INSERT INTO "public".ducklake_snapshot
  VALUES (1, NOW(), 1, 2, 0);
  -- (snapshot_id=1, time, schema_version=1, next_catalog_id=2, next_file_id=0)

INSERT INTO "public".ducklake_table
  VALUES (1, '019ead99-...-cff03', 1, NULL, 0, 't', 't/', true);
  -- (table_id=1, uuid, begin_snapshot=1, end_snapshot=NULL,
  --  schema_id=0, name='t', path='t/', path_is_relative=true)

INSERT INTO "public".ducklake_column VALUES
  (1, 1, NULL, 1, 1, 'id',   'int64',   NULL, 'NULL', true, NULL, 'literal', 'duckdb'),
  (2, 1, NULL, 1, 2, 'name', 'varchar', NULL, 'NULL', true, NULL, 'literal', 'duckdb');
  -- (column_id, begin_snapshot=1, end_snapshot=NULL, table_id=1, column_order,
  --  name, type, initial_default=NULL, default_value='NULL'(literal string),
  --  nulls_allowed=true, parent_column=NULL,
  --  default_value_type='literal', default_value_dialect='duckdb')

INSERT INTO "public".ducklake_schema_versions VALUES (1, 1, 1);
  -- (begin_snapshot=1, schema_version=1, table_id=1)

INSERT INTO "public".ducklake_snapshot_changes
  VALUES (1, 'created_table:"main"."t"', NULL, NULL, NULL);
```

Captured value notes:

- `column_order` is **1-based** (id=1, name=2), not 0-based.
- `column_type` strings are DuckLake type names: `int64`, `varchar` (BIGINT →
  `int64`). These are the canonical DuckLake type strings, not Postgres types.
- `default_value` is the **string** `'NULL'` (not SQL NULL) with
  `default_value_type='literal'`, `default_value_dialect='duckdb'`;
  `initial_default` is real SQL NULL.
- `table.path` is `'t/'` (table name + trailing slash), relative; `schema.path`
  was `'main/'`. The resolution chain is `data_path + schema.path + table.path +
  file.path` → `/tmp/dlcap/dldata/main/t/<file>.parquet` (confirmed by the
  written files).
- `schema_version` advanced 0 → 1 (DDL bump). A row was written to
  `ducklake_schema_versions` because this is DDL.
- `next_catalog_id` went 1 → 2 in this snapshot row because `table_id=1` was
  allocated from the catalog-id counter (the catalog-id space is shared by
  schemas, tables, and columns — see §5).
- No `ducklake_schema` insert here: `main` already existed from bootstrap.

**dfdl annotation:** `begin_write_transaction` (lines 659–934) does the same
sequence — snapshot insert, get-or-create schema/table, end+reinsert columns,
classify DDL/DML, bump `schema_version`, insert `ducklake_schema_versions` on
DDL. **loom must differ:**
- Drop the `INSERT INTO ducklake_catalog_snapshot_map` (lines 688–695) and
  `ducklake_catalog_schema_map` (730–737) — no such tables single-catalog.
- Drop `lock_catalog`'s `SELECT ... FROM ducklake_catalog ... FOR UPDATE`
  (231–253) — there is no `ducklake_catalog` row to lock. loom must use a
  *different* serialization point (a Postgres advisory lock, or
  `SELECT ... FOR UPDATE` on the latest `ducklake_snapshot` row) to serialize
  commits.
- dfdl encodes the catalog into `schema.path` as `cat_{id}/{schema}` (716–718).
  loom uses the **plain** `schema/` path (DuckDB writes `main/`).
- dfdl uses `GENERATED ALWAYS AS IDENTITY` (`RETURNING column_id`, etc.).
  Single-catalog DuckDB uses **explicit software-assigned ids** (§5); loom must
  match DuckDB (no identity columns).
- dfdl's `ducklake_column` DDL (lines 54–64) is **missing** `initial_default`,
  `default_value`, `default_value_type`, `default_value_dialect`,
  `default_value` etc. loom must write the full 13-column tuple above.
- dfdl never writes `ducklake_snapshot_changes`; **loom must** (DuckDB does, and
  time-travel/`changes_made` consumers expect it).

### Op B — first `INSERT` (one data file, 10 rows)

```sql
INSERT INTO "public".ducklake_snapshot
  VALUES (2, NOW(), 1, 2, 1);
  -- schema_version stays 1 (DML, no schema change); next_file_id 0 -> 1

INSERT INTO "public".ducklake_table_stats VALUES (1, 10, 10, 444);
  -- (table_id=1, record_count=10, next_row_id=10, file_size_bytes=444)
  -- INSERT (not upsert) because no stats row existed yet for table 1

WITH new_values(...) AS ( VALUES (1,1,false,NULL,'0','9',NULL),(1,2,false,NULL,'x','x',NULL) )
  -- on first insert this is an INSERT into ducklake_table_column_stats:
INSERT INTO "public".ducklake_table_column_stats VALUES
  (1, 1, false, NULL, '0', '9', NULL),
  (1, 2, false, NULL, 'x', 'x', NULL);
  -- (table_id, column_id, contains_null=false, contains_nan=NULL,
  --  min_value, max_value, extra_stats=NULL) -- aggregate (table-level) stats

INSERT INTO "public".ducklake_data_file VALUES
  (0, 1, 2, NULL, NULL, 'ducklake-019ead99-...-b517.parquet', true, 'parquet',
   10, 444, 249, 0, NULL, NULL, NULL, NULL);
  -- (data_file_id=0, table_id=1, begin_snapshot=2, end_snapshot=NULL,
  --  file_order=NULL, path, path_is_relative=true, file_format='parquet',
  --  record_count=10, file_size_bytes=444, footer_size=249,
  --  row_id_start=0, partition_id=NULL, encryption_key=NULL,
  --  mapping_id=NULL, partial_max=NULL)

INSERT INTO "public".ducklake_file_column_stats VALUES
  (0, 1, 1, 88, 10, 0, '0', '9', NULL, NULL),
  (0, 1, 2, 48, 10, 0, 'x', 'x', NULL, NULL);
  -- (data_file_id=0, table_id=1, column_id, column_size_bytes, value_count=10,
  --  null_count=0, min_value, max_value, contains_nan=NULL, extra_stats=NULL)

INSERT INTO "public".ducklake_snapshot_changes
  VALUES (2, 'inserted_into_table:1', NULL, NULL, NULL);
```

Captured value notes:

- **`data_file_id` starts at 0** (it is allocated from `next_file_id`, which was
  0 in snapshot 1). After the commit, snapshot 2 records `next_file_id = 1`.
- **`row_id_start = 0`** for the first file (the table's row-id counter started
  at 0).
- **`path_is_relative = true`**, `path` is just the bare filename
  `ducklake-<uuid>.parquet` (relative to `data_path + schema.path +
  table.path`). The file UUID is a fresh UUIDv7 per file.
- `record_count=10`, `file_size_bytes=444` (real on-disk size),
  `footer_size=249` (the Parquet footer length — loom must compute this from the
  file it writes). `file_format='parquet'`.
- **stats min/max encoding**: stored as **VARCHAR** text regardless of column
  type. For `id BIGINT`: `min='0'`, `max='9'` (decimal text). For `name`:
  `min='x'`, `max='x'`. `null_count=0`, `value_count=10` (= record_count here,
  no nulls). `contains_nan=NULL` for non-float columns.
- `ducklake_table_column_stats` (table-level aggregate) is **separate** from
  `ducklake_file_column_stats` (per-file). On the first insert both are
  populated; the table-level row is later UPDATEd (Op C).
- `column_size_bytes` is the compressed column chunk size in the Parquet file
  (88 / 48 bytes here) — loom must read this from the Parquet metadata it
  writes.

**dfdl annotation:** `register_data_file` (482–555) seeds/`ON CONFLICT DO
NOTHING`s `ducklake_table_stats`, reads `next_row_id` for `row_id_start`, inserts
`ducklake_data_file`, then `UPDATE`s the stats counters. **loom must differ:**
- dfdl **does not write `ducklake_file_column_stats` or
  `ducklake_table_column_stats` at all** — its `DataFileInfo` carries no
  per-column stats. **loom must write both** (DuckDB does, and the query planner
  needs `ducklake_file_column_stats` for pruning; without it loom's files would
  read but not prune).
- dfdl's `ducklake_data_file` DDL (65–78) is missing `file_order`,
  `file_format`, `partition_id`, `partial_max`. loom must write the full
  16-column tuple, including `file_format='parquet'`.
- dfdl allocates `row_id_start` from `ducklake_table_stats.next_row_id`. DuckDB
  single-catalog uses the **same** mechanism (the row 1,10,10,444 shows
  next_row_id advancing to 10) — so this part translates directly. Good.
- dfdl never writes `ducklake_snapshot_changes`; loom must
  (`inserted_into_table:<table_id>`).

### Op C — second `INSERT` (one data file, 5 rows)

```sql
INSERT INTO "public".ducklake_snapshot
  VALUES (3, NOW(), 1, 2, 2);
  -- next_file_id 1 -> 2; schema_version still 1

UPDATE "public".ducklake_table_stats
  SET record_count=15, file_size_bytes=848, next_row_id=15 WHERE table_id=1;
  -- now an UPDATE (row exists): 10+5=15 rows, 444+404=848 bytes,
  -- next_row_id 10 -> 15

WITH new_values(tid,cid,new_contains_null,new_contains_nan,new_min,new_max,new_extra_stats) AS (
  VALUES (1,1,false,NULL,'0','9',NULL),(1,2,false,NULL,'x','y',NULL)
)
UPDATE "public".ducklake_table_column_stats
  SET contains_null=CAST(new_contains_null AS BOOLEAN),
      contains_nan=CAST(new_contains_nan AS BOOLEAN),
      min_value=new_min, max_value=new_max, extra_stats=new_extra_stats
  FROM new_values WHERE table_id=tid AND column_id=cid;
  -- table-level aggregate stats are MERGED: id min/max stays '0'/'9',
  -- name max widens 'x' -> 'y'

INSERT INTO "public".ducklake_data_file VALUES
  (1, 1, 3, NULL, NULL, 'ducklake-019ead99-...-e6eb3.parquet', true, 'parquet',
   5, 404, 246, 10, NULL, NULL, NULL, NULL);
  -- data_file_id=1, begin_snapshot=3, record_count=5, file_size_bytes=404,
  -- footer_size=246, row_id_start=10 (continues from prior next_row_id)

INSERT INTO "public".ducklake_file_column_stats VALUES
  (1, 1, 1, 51, 5, 0, '0', '4', NULL, NULL),
  (1, 1, 2, 48, 5, 0, 'y', 'y', NULL, NULL);

INSERT INTO "public".ducklake_snapshot_changes
  VALUES (3, 'inserted_into_table:1', NULL, NULL, NULL);
```

Captured value notes (counter/id progression confirmed):

- `data_file_id` 0 → **1** (next_file_id was 1 going in; snapshot 3 records 2).
- **`row_id_start = 10`** — continues exactly from the prior file's
  `record_count` (10), i.e. from `ducklake_table_stats.next_row_id`. Row-id
  ranges are contiguous and never reused.
- `ducklake_table_stats` is now an **UPDATE** (row exists), accumulating totals.
- `ducklake_table_column_stats` is now an **UPDATE** (merge new file's
  per-column min/max into the table aggregate); the per-file stats are always a
  fresh INSERT.
- `schema_version` unchanged (DML), so **no** `ducklake_schema_versions` row.

---

## 4. `changes_made` string formats (captured)

`ducklake_snapshot_changes.changes_made` per op (one row per committed
snapshot):

| Operation | `changes_made` |
|---|---|
| bootstrap (snapshot 0) | `created_schema:"main"` |
| `CREATE TABLE main.t` (snapshot 1) | `created_table:"main"."t"` |
| `INSERT` (snapshot 2) | `inserted_into_table:1` |
| second `INSERT` (snapshot 3) | `inserted_into_table:1` |

Format rules observed:
- `created_schema:"<schema_name>"` — schema name double-quoted.
- `created_table:"<schema_name>"."<table_name>"` — fully qualified,
  double-quoted parts.
- `inserted_into_table:<table_id>` — the numeric `table_id`, **not** quoted/named
  (note the asymmetry: create uses names, insert uses the id).
- `author`, `commit_message`, `commit_extra_info` were all NULL (no commit
  message was set). loom can populate these.

---

## 5. Counter / id allocation rules (captured)

All ids are software counters held in the **latest `ducklake_snapshot` row**;
there are no sequences. A commit reads the latest snapshot, derives the next
ids, and writes a new snapshot row carrying the advanced counters.

- **`snapshot_id`**: monotonic, **`max(snapshot_id) + 1`**. Bootstrap = 0, then
  1, 2, 3, ... Each commit inserts exactly one new `ducklake_snapshot` row.
- **`next_catalog_id`** (shared id space for schema/table/column ids and any
  other catalog object): the *value stored in a snapshot row* is the **next free
  id** after that commit. Bootstrap snapshot 0 has `next_catalog_id = 1` (id 0
  was taken by the `main` schema). Op A took `table_id = 1` (one id) and the
  resulting snapshot 1 stored `next_catalog_id = 2`. **NB:** in this capture,
  `column_id`s (1, 2) appear to come from the *same* monotonic counter as the
  table id but the snapshot's `next_catalog_id` only advanced by 1 — i.e. the
  catalog-id counter advances for schema/table allocation; column ids are
  managed within the table's own id space. Treat `next_catalog_id` as "next
  schema/table/object id" and assign `column_id`s densely per table starting at
  1. **(partly inferred — see Caveats.)**
- **`next_file_id`**: the next free `data_file_id`. Bootstrap = 0. Each data
  file consumes one id; the *new* snapshot row stores the post-commit value.
  `data_file_id` sequence: 0, 1, ... and snapshot `next_file_id`: 0 → 1 → 2.
- **`row_id_start`**: per-table, allocated from
  `ducklake_table_stats.next_row_id`. First file: 0; after 10 rows the counter
  is 10; next file: `row_id_start = 10`. Contiguous, monotonic, never reused
  (even across end-snapshotting). loom must persist this per-table counter.
- **`schema_version`**: starts 0 (bootstrap). **Bumped (+1) on DDL** (table
  create, column-set change); **carried forward unchanged on DML** (plain
  insert). On a DDL bump, also insert one
  `ducklake_schema_versions(begin_snapshot, schema_version, table_id)` row.
  Captured: 0 → 1 (CREATE) → 1 (INSERT) → 1 (INSERT).

Because these counters live in `ducklake_snapshot` (not a sequence), loom **must
serialize commits** so two writers don't read the same latest snapshot and
allocate colliding ids. dfdl serializes via
`SELECT ... FROM ducklake_catalog ... FOR UPDATE`; loom (no such table) should
serialize on the latest `ducklake_snapshot` row (`SELECT ... ORDER BY snapshot_id
DESC LIMIT 1 FOR UPDATE`) or a Postgres advisory lock, inside the same
REPEATABLE READ transaction.

---

## 6. Atomicity + orphan-Parquet note (captured, probe #5)

Two aborted statements were run:

1. `INSERT INTO lake.main.t (id, nope) VALUES (1, 'z')` — references a
   non-existent column. **Failed at bind time** (before any execution). Captured:
   the only Postgres activity was the read-only catalog-load transaction
   (`COPY ... TO STDOUT`) which `COMMIT`ted, then a second catalog-load that
   `ROLLBACK`ed. **Zero `ducklake_*` write rows. Zero Parquet files written.**
2. `INSERT INTO ... WHERE 1/0 = 1` — runtime divide-by-zero. Also produced
   **zero `ducklake_*` writes and zero Parquet** (the error fired during scan
   planning before any data file was flushed).

After both aborts, the data directory held **exactly the 2 committed Parquet
files** and the catalog had exactly snapshots 0–3. Commits are atomic: the
entire `ducklake_*` write set lands in one Postgres transaction or not at all.

**GC / orphan-file implication for loom:** in *these* cases nothing leaked
because the failures happened before any Parquet flush. But the general DuckLake
write order is **(1) write Parquet to object store, then (2) commit catalog
rows**. If a writer flushes Parquet and then the catalog commit fails/crashes,
the Parquet object is orphaned — it exists on S3/MinIO but no `ducklake_data_file`
row references it. The catalog itself stays consistent (atomic), but the object
store accumulates orphans. This is exactly why DuckLake ships
`ducklake_files_scheduled_for_deletion` and a two-phase vacuum. **loom's future
GC must reconcile object-store contents against `ducklake_data_file` to reclaim
orphans from failed/crashed writers — it cannot rely on the catalog alone to
know every file it wrote.** (Single-catalog `ducklake_files_scheduled_for_deletion`
has columns `data_file_id, path, path_is_relative, schedule_start` and **no**
`catalog_id` — dfdl adds `catalog_id`, which loom must drop.)

---

## 7. Differences from datafusion-ducklake (summary checklist)

When porting `/tmp/dfdl/src/metadata_writer_postgres.rs` to loom's
single-catalog writer:

1. **Delete the multicatalog tables**: `ducklake_catalog`,
   `ducklake_catalog_snapshot_map`, `ducklake_catalog_schema_map`
   (`SQL_CREATE_MULTICATALOG_TABLES`, lines 112–165). And every INSERT into the
   two map tables (create_snapshot 322–329; get_or_create_schema 375–382;
   begin_write_transaction 688–695, 730–737).
2. **Drop `catalog_id` everywhere**: the `catalog_id` field, `with_pool`,
   `lock_catalog`, `assert_schema_in_catalog`, `assert_table_in_catalog`, and the
   `catalog_id` column on `ducklake_files_scheduled_for_deletion`.
3. **Replace the catalog lock** (`SELECT ... FROM ducklake_catalog ... FOR
   UPDATE`) with a single-catalog serialization point (latest-snapshot
   `FOR UPDATE`, or advisory lock).
4. **Use plain `schema/` paths**, not `cat_{id}/{schema}` (line 718).
5. **Match DuckDB's full DDL**: dfdl's `ducklake_column` and `ducklake_data_file`
   DDL omit columns DuckDB writes (`initial_default`, `default_value*`,
   `file_order`, `file_format`, `partition_id`, `partial_max`, ...). Use the §1
   DDL.
6. **Software ids, not IDENTITY**: DuckDB assigns `snapshot_id`/`*_id`/
   `data_file_id` from snapshot counters; do not use `GENERATED ALWAYS AS
   IDENTITY` / `RETURNING`. Allocate from `ducklake_snapshot.next_catalog_id` /
   `next_file_id` / per-table `next_row_id`.
7. **Write the stats tables dfdl ignores**: `ducklake_file_column_stats`
   (per-file, INSERT each commit) and `ducklake_table_column_stats` (table
   aggregate, INSERT-then-UPDATE/merge). Required for query pruning + interop.
8. **Write `ducklake_snapshot_changes`** every commit (dfdl never does) with the
   formats in §4.
9. **Seed `ducklake_metadata`** (`version=1.0`, `created_by`, `data_path`,
   `encrypted=false`) and snapshot 0 + `main` schema — or (recommended) let the
   pinned duckdb-cli `ATTACH` do this once at provision time (§1).
10. dfdl's row-id / `ducklake_table_stats` mechanics (`next_row_id`,
    accumulating `record_count`/`file_size_bytes`) **match** DuckDB and translate
    directly — keep them.

---

## 8. Reproducing this capture

Throwaway script lives at `/tmp/capture.sh` (not committed). It:

1. Builds the pinned `postgres-bin`, `libxml2`, `duckdb-cli`,
   `duckdb-extensions` (`env -u BUCK_PREFER_REMOTE buck2 build ... --local-only
   --show-output`).
2. `initdb` + `pg_ctl start` a private cluster on a unix socket with
   `-c log_statement=all -c log_min_duration_statement=0`, logging to a file.
3. Runs the duckdb-cli with `SET extension_directory=...; LOAD ducklake; LOAD
   postgres_scanner; ATTACH 'ducklake:postgres:dbname=loom host=$SOCK port=$PORT
   user=postgres' AS lake (DATA_PATH '...', DATA_INLINING_ROW_LIMIT 0);` then the
   5 operations (bare ATTACH, CREATE TABLE, two INSERTs, two aborts), each
   separated by a `SELECT '=====MARK X====='` written to Postgres so the log can
   be segmented.
4. Dumps every `ducklake_*` table (`psql -x`) and lists the written Parquet
   files.

Statement extraction joins continuation lines and segments on the MARK rows
(small inline Python in the capture run). The full captured log is at
`/tmp/dlcap/pg.log`; normalized statements at `/tmp/dlcap/stmts.txt`; row dump at
`/tmp/dlcap/rows.txt`.

### Caveats / captured-vs-inferred

- **Captured empirically**: all DDL (§1), all per-op write statements and values
  (§3), `changes_made` strings (§4), `snapshot_id`/`next_file_id`/`row_id_start`
  progression (§5), atomicity + zero-orphan-in-these-cases (§6).
- **Partly inferred**: the precise rule by which `next_catalog_id` advances vs.
  how `column_id`s are assigned (§5) — the capture shows `next_catalog_id`
  advancing by 1 per table while two `column_id`s were assigned, but a single
  3-op trace can't fully separate the two id spaces; a follow-up probe creating
  multiple schemas/tables/columns would pin this down before loom implements id
  allocation. The DuckLake spec and `datafusion-ducklake` both treat schema,
  table, and column ids as drawn from one monotonic "catalog id" space, so the
  conservative implementation is: allocate every new schema/table/column id from
  `next_catalog_id` and store the advanced value.
- The orphan-Parquet conclusion (§6) is "no orphan **in these two abort
  cases**"; the general write-then-commit ordering means orphans are still
  possible on a crash between Parquet flush and catalog commit — sized as a GC
  requirement, not observed here.
