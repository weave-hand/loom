//! Native DuckLake **append** writer: emit `ducklake_*` rows via sqlx so the
//! DuckDB engine reads them back identically. This is the single-catalog,
//! DuckDB-compatible layout — see
//! `docs/superpowers/specs/2026-06-09-ducklake-single-catalog-write-recipe.md`
//! (§2 batch order, §3 Op B/C tuples, §5 id/counter rules).
//!
//! `create_table` row-writing is Task 3; this module handles `append_files`
//! against tables that already exist in the catalog.

use control_plane_core::{ColumnSpec, ControlPlaneError, DataFile, Result, SnapshotId, TableRef};
use sqlx::{Postgres, Transaction};

use crate::backend;
use crate::ducklake_type::to_ducklake_stat_string;

/// Fixed advisory-lock key serializing DuckLake catalog commits in one database.
/// There is no `ducklake_catalog` row to `FOR UPDATE` in the single-catalog
/// layout (recipe §5/§7), so commits serialize on a per-database `xact` advisory
/// lock instead. The constant is arbitrary but stable: a hash of "ducklake".
const CATALOG_LOCK_KEY: i64 = 0x6475_636b_6c61_6b65u64 as i64;

/// The latest snapshot's id counters (recipe §5 `GetLatestSnapshotQuery`).
struct Head {
    snapshot_id: i64,
    schema_version: i64,
    next_catalog_id: i64,
    next_file_id: i64,
}

/// Write one append commit: for each staged `(table, files)` group, allocate ids
/// from the latest snapshot and emit the ordered `ducklake_*` rows for the new
/// snapshot. Returns the new [`SnapshotId`]. Assumes all referenced tables exist
/// in the catalog (created by DuckDB or, in Task 3, by loom's `create_table`).
#[tracing::instrument(
    skip(tx, staged_tables, staged_files),
    fields(tables = staged_tables.len(), files = staged_files.len()),
    level = "debug"
)]
pub(crate) async fn commit_snapshot(
    tx: &mut Transaction<'_, Postgres>,
    staged_tables: &[(TableRef, Vec<ColumnSpec>)],
    staged_files: &[(TableRef, Vec<DataFile>)],
) -> Result<SnapshotId> {
    lock_catalog(tx).await?;
    let head = read_head(tx).await?;

    let new_snapshot_id = head.snapshot_id + 1;
    // Counters advanced in memory during the commit (recipe §5): the table
    // create-loop consumes `next_catalog_id` (one per table) and bumps
    // `schema_version` (DDL); the append loop advances `next_file_id`. The single
    // new `ducklake_snapshot` row carries the post-commit values.
    let mut schema_version = head.schema_version;
    let mut next_catalog_id = head.next_catalog_id;
    let mut next_file_id = head.next_file_id;

    // Created-table change segments lead the snapshot_changes string (recipe §4).
    let mut create_segments: Vec<String> = Vec::new();

    // (2) create_table rows BEFORE the append loop resolves table_id, so a table
    //     created and appended-to in the same tx resolves correctly (recipe §2
    //     steps 2/3/6; §3 Op A tuples; §5 id rules).
    for (table, columns) in staged_tables {
        // Skip tables already live in the catalog (created by DuckDB earlier).
        if table_exists(tx, table).await? {
            continue;
        }
        let table_id = next_catalog_id;
        next_catalog_id += 1; // table consumes a catalog id; columns do NOT (§5)
        schema_version += 1; // DDL bumps schema_version (§5)
        write_table(
            tx,
            table,
            table_id,
            new_snapshot_id,
            schema_version,
            columns,
        )
        .await?;
        create_segments.push(format!(
            "created_table:\"{}\".\"{}\"",
            table.schema, table.name
        ));
    }

    // (3) append loop: resolve table_id (now present), write data-file/stats rows.
    for (table, files) in staged_files {
        let table_id = resolve_table_id(tx, table).await?;
        for file in files {
            let data_file_id = next_file_id;
            next_file_id += 1;
            write_data_file(tx, table_id, new_snapshot_id, data_file_id, file).await?;
        }
    }

    // (4) the single new snapshot row (recipe §2 step 1): id+1, with the
    //     post-commit schema_version, next_catalog_id, next_file_id.
    sqlx::query!(
        "insert into ducklake_snapshot \
           (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id) \
         values ($1, now(), $2, $3, $4)",
        new_snapshot_id,
        schema_version,
        next_catalog_id,
        next_file_id,
    )
    .execute(&mut **tx)
    .await
    .map_err(backend)?;

    // (5) the single snapshot_changes row: created_table segments first, then
    //     inserted_into_table segments (recipe §4 ordering).
    let mut segments: Vec<String> = create_segments;
    for (table, files) in staged_files {
        if files.is_empty() {
            continue;
        }
        let table_id = resolve_table_id(tx, table).await?;
        segments.push(format!("inserted_into_table:{table_id}"));
    }
    let changes_made = segments.join(",");
    sqlx::query!(
        "insert into ducklake_snapshot_changes \
           (snapshot_id, changes_made, author, commit_message, commit_extra_info) \
         values ($1, $2, NULL, NULL, NULL)",
        new_snapshot_id,
        changes_made,
    )
    .execute(&mut **tx)
    .await
    .map_err(backend)?;

    Ok(SnapshotId(new_snapshot_id))
}

/// Recipe §5/§7: serialize commits on a per-database advisory xact lock (released
/// automatically on COMMIT/ROLLBACK), since there is no catalog row to lock.
async fn lock_catalog(tx: &mut Transaction<'_, Postgres>) -> Result<()> {
    sqlx::query!("select pg_advisory_xact_lock($1)", CATALOG_LOCK_KEY)
        .execute(&mut **tx)
        .await
        .map_err(backend)?;
    Ok(())
}

/// Recipe §5 `GetLatestSnapshotQuery`: load the id counters from the newest snapshot.
async fn read_head(tx: &mut Transaction<'_, Postgres>) -> Result<Head> {
    let row = sqlx::query!(
        "select snapshot_id as \"snapshot_id!\", schema_version as \"schema_version!\", \
                next_catalog_id as \"next_catalog_id!\", next_file_id as \"next_file_id!\" \
         from ducklake_snapshot \
         where snapshot_id = (select max(snapshot_id) from ducklake_snapshot)",
    )
    .fetch_optional(&mut **tx)
    .await
    .map_err(backend)?
    .ok_or_else(|| {
        ControlPlaneError::Backend(Box::<dyn std::error::Error + Send + Sync>::from(
            "no ducklake_snapshot rows; catalog not bootstrapped",
        ))
    })?;
    Ok(Head {
        snapshot_id: row.snapshot_id,
        schema_version: row.schema_version,
        next_catalog_id: row.next_catalog_id,
        next_file_id: row.next_file_id,
    })
}

/// Resolve the live `table_id` for `table` (recipe: join `ducklake_table`/
/// `ducklake_schema` on names where `end_snapshot IS NULL`).
async fn resolve_table_id(tx: &mut Transaction<'_, Postgres>, table: &TableRef) -> Result<i64> {
    sqlx::query_scalar!(
        "select t.table_id as \"table_id!\" \
         from ducklake_table t join ducklake_schema s on t.schema_id = s.schema_id \
         where s.schema_name = $1 and t.table_name = $2 \
           and t.end_snapshot is null and s.end_snapshot is null",
        table.schema,
        table.name,
    )
    .fetch_optional(&mut **tx)
    .await
    .map_err(backend)?
    .ok_or_else(|| ControlPlaneError::NotFound(format!("{}.{}", table.schema, table.name)))
}

/// True if `table` is already live in the catalog (created by DuckDB or an
/// earlier commit). Used to make `create_table` idempotent within a commit.
async fn table_exists(tx: &mut Transaction<'_, Postgres>, table: &TableRef) -> Result<bool> {
    let row = sqlx::query_scalar!(
        "select 1 as \"one!\" \
         from ducklake_table t join ducklake_schema s on t.schema_id = s.schema_id \
         where s.schema_name = $1 and t.table_name = $2 \
           and t.end_snapshot is null and s.end_snapshot is null",
        table.schema,
        table.name,
    )
    .fetch_optional(&mut **tx)
    .await
    .map_err(backend)?;
    Ok(row.is_some())
}

/// Resolve the live `schema_id` for `schema_name` (the `main` schema exists from
/// bootstrap). Creating a brand-new schema is out of scope; `NotFound` otherwise.
async fn resolve_schema_id(tx: &mut Transaction<'_, Postgres>, schema_name: &str) -> Result<i64> {
    sqlx::query_scalar!(
        "select schema_id as \"schema_id!\" from ducklake_schema \
         where schema_name = $1 and end_snapshot is null",
        schema_name,
    )
    .fetch_optional(&mut **tx)
    .await
    .map_err(backend)?
    .ok_or_else(|| ControlPlaneError::NotFound(format!("schema {schema_name}")))
}

/// Emit the §3 Op A create-table rows for one new table: the `ducklake_table`
/// row, one `ducklake_column` per column (per-table 1-based dense `column_id`,
/// `column_order == column_id`), and the 3-column `ducklake_schema_versions` row.
async fn write_table(
    tx: &mut Transaction<'_, Postgres>,
    table: &TableRef,
    table_id: i64,
    snapshot_id: i64,
    schema_version: i64,
    columns: &[ColumnSpec],
) -> Result<()> {
    let schema_id = resolve_schema_id(tx, &table.schema).await?;

    // ducklake_table: (table_id, table_uuid, begin_snapshot, end_snapshot=NULL,
    // schema_id, table_name, path='<name>/', path_is_relative=true).
    let table_uuid = uuid::Uuid::new_v4();
    let path = format!("{}/", table.name);
    sqlx::query!(
        "insert into ducklake_table \
           (table_id, table_uuid, begin_snapshot, end_snapshot, schema_id, table_name, path, path_is_relative) \
         values ($1, $2, $3, NULL, $4, $5, $6, true)",
        table_id,
        table_uuid,
        snapshot_id,
        schema_id,
        table.name,
        path,
    )
    .execute(&mut **tx)
    .await
    .map_err(backend)?;

    // ducklake_column rows: column_id is a per-table 1-based dense counter,
    // column_order == column_id; default_value='NULL' literal, default_value_type
    // 'literal', default_value_dialect 'duckdb', initial_default NULL, parent NULL.
    for (i, col) in columns.iter().enumerate() {
        let column_id = (i + 1) as i64;
        sqlx::query!(
            "insert into ducklake_column \
               (column_id, begin_snapshot, end_snapshot, table_id, column_order, column_name, \
                column_type, initial_default, default_value, nulls_allowed, parent_column, \
                default_value_type, default_value_dialect) \
             values ($1, $2, NULL, $3, $4, $5, $6, NULL, 'NULL', $7, NULL, 'literal', 'duckdb')",
            column_id,
            snapshot_id,
            table_id,
            column_id,
            col.name,
            col.ty,
            col.nullable,
        )
        .execute(&mut **tx)
        .await
        .map_err(backend)?;
    }

    // ducklake_schema_versions: 3-column (begin_snapshot, schema_version, table_id).
    sqlx::query!(
        "insert into ducklake_schema_versions (begin_snapshot, schema_version, table_id) \
         values ($1, $2, $3)",
        snapshot_id,
        schema_version,
        table_id,
    )
    .execute(&mut **tx)
    .await
    .map_err(backend)?;

    Ok(())
}

/// Resolve the live `column_id` for `(table_id, column_name)` (recipe §3 Op C:
/// `WHERE table_id=$1 AND column_name=$2 AND end_snapshot IS NULL`).
async fn resolve_column_id(
    tx: &mut Transaction<'_, Postgres>,
    table_id: i64,
    column_name: &str,
) -> Result<i64> {
    sqlx::query_scalar!(
        "select column_id as \"column_id!\" from ducklake_column \
         where table_id = $1 and column_name = $2 and end_snapshot is null",
        table_id,
        column_name,
    )
    .fetch_optional(&mut **tx)
    .await
    .map_err(backend)?
    .ok_or_else(|| ControlPlaneError::NotFound(format!("column {column_name} of table {table_id}")))
}

/// Emit, for one data file, the table-level stats (INSERT-or-UPDATE per recipe §3
/// Op C), the `ducklake_data_file` row (16-col Op B tuple), the per-file column
/// stats (10-col tuple), advancing the table's `next_row_id` by `record_count`.
async fn write_data_file(
    tx: &mut Transaction<'_, Postgres>,
    table_id: i64,
    snapshot_id: i64,
    data_file_id: i64,
    file: &DataFile,
) -> Result<()> {
    // row_id_start from ducklake_table_stats.next_row_id (0 if no stats row yet).
    let existing = sqlx::query!(
        "select record_count as \"record_count!\", next_row_id as \"next_row_id!\", \
                file_size_bytes as \"file_size_bytes!\" \
         from ducklake_table_stats where table_id = $1",
        table_id,
    )
    .fetch_optional(&mut **tx)
    .await
    .map_err(backend)?;

    let row_id_start = existing.as_ref().map(|r| r.next_row_id).unwrap_or(0);

    // 4 (recipe §2): table-level stats first, then the file rows.
    match &existing {
        None => {
            // INSERT branch (recipe §3 Op B): record_count, next_row_id, file_size.
            sqlx::query!(
                "insert into ducklake_table_stats \
                   (table_id, record_count, next_row_id, file_size_bytes) \
                 values ($1, $2, $3, $4)",
                table_id,
                file.record_count,
                file.record_count,
                file.file_size_bytes,
            )
            .execute(&mut **tx)
            .await
            .map_err(backend)?;
        }
        Some(prev) => {
            // UPDATE branch (recipe §3 Op C): accumulate record_count/file_size,
            // advance next_row_id.
            let record_count = prev.record_count + file.record_count;
            let file_size_bytes = prev.file_size_bytes + file.file_size_bytes;
            let next_row_id = prev.next_row_id + file.record_count;
            sqlx::query!(
                "update ducklake_table_stats \
                 set record_count = $2, file_size_bytes = $3, next_row_id = $4 \
                 where table_id = $1",
                table_id,
                record_count,
                file_size_bytes,
                next_row_id,
            )
            .execute(&mut **tx)
            .await
            .map_err(backend)?;
        }
    }

    // ducklake_table_column_stats: INSERT first time, else merge-UPDATE (recipe §3
    // Op C). A per-row UPSERT is equivalent to DuckDB's WITH new_values merge for
    // the resulting rows. There is no unique constraint on (table_id, column_id),
    // so do an explicit "update else insert" per column.
    for stat in &file.column_stats {
        let column_id = resolve_column_id(tx, table_id, &stat.column_name).await?;
        let contains_null = stat.null_count > 0;
        let min_s = stat.min.as_ref().map(to_ducklake_stat_string);
        let max_s = stat.max.as_ref().map(to_ducklake_stat_string);
        let updated = sqlx::query!(
            "update ducklake_table_column_stats \
             set contains_null = $3, contains_nan = NULL, min_value = $4, \
                 max_value = $5, extra_stats = NULL \
             where table_id = $1 and column_id = $2",
            table_id,
            column_id,
            contains_null,
            min_s,
            max_s,
        )
        .execute(&mut **tx)
        .await
        .map_err(backend)?
        .rows_affected();
        if updated == 0 {
            sqlx::query!(
                "insert into ducklake_table_column_stats \
                   (table_id, column_id, contains_null, contains_nan, min_value, max_value, extra_stats) \
                 values ($1, $2, $3, NULL, $4, $5, NULL)",
                table_id,
                column_id,
                contains_null,
                min_s,
                max_s,
            )
            .execute(&mut **tx)
            .await
            .map_err(backend)?;
        }
    }

    // ducklake_data_file: full 16-col Op B tuple. begin_snapshot = new snapshot,
    // end_snapshot/file_order NULL, file_format 'parquet', row_id_start contiguous,
    // partition_id/encryption_key/mapping_id/partial_max NULL.
    let footer_size = file.parquet_footer_size.ok_or_else(|| {
        ControlPlaneError::Backend(Box::<dyn std::error::Error + Send + Sync>::from(
            "parquet data file missing footer_size",
        ))
    })?;
    debug_assert!(matches!(
        file.file_format,
        control_plane_core::FileFormat::Parquet
    ));
    sqlx::query!(
        "insert into ducklake_data_file \
           (data_file_id, table_id, begin_snapshot, end_snapshot, file_order, path, \
            path_is_relative, file_format, record_count, file_size_bytes, footer_size, \
            row_id_start, partition_id, encryption_key, mapping_id, partial_max) \
         values ($1, $2, $3, NULL, NULL, $4, $5, 'parquet', $6, $7, $8, $9, NULL, NULL, NULL, NULL)",
        data_file_id,
        table_id,
        snapshot_id,
        file.path,
        file.path_is_relative,
        file.record_count,
        file.file_size_bytes,
        footer_size,
        row_id_start,
    )
    .execute(&mut **tx)
    .await
    .map_err(backend)?;

    // ducklake_file_column_stats: 10-col tuple per ColumnStat (contains_nan NULL,
    // extra_stats NULL for a plain append).
    for stat in &file.column_stats {
        let column_id = resolve_column_id(tx, table_id, &stat.column_name).await?;
        let value_count = file.record_count - stat.null_count;
        let min_s = stat.min.as_ref().map(to_ducklake_stat_string);
        let max_s = stat.max.as_ref().map(to_ducklake_stat_string);
        sqlx::query!(
            "insert into ducklake_file_column_stats \
               (data_file_id, table_id, column_id, column_size_bytes, value_count, \
                null_count, min_value, max_value, contains_nan, extra_stats) \
             values ($1, $2, $3, $4, $5, $6, $7, $8, NULL, NULL)",
            data_file_id,
            table_id,
            column_id,
            stat.column_size_bytes,
            value_count,
            stat.null_count,
            min_s,
            max_s,
        )
        .execute(&mut **tx)
        .await
        .map_err(backend)?;
    }

    Ok(())
}
