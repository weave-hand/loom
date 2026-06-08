use async_trait::async_trait;
use control_plane_core::{
    Catalog, ColumnDef, ControlPlaneError, FileRef, Result, Snapshot, SnapshotId, TableRef,
    TableSchema,
};
use sqlx::{AssertSqlSafe, Row as _};

use crate::{PgControlPlane, backend, row_to_snapshot};

#[async_trait]
impl Catalog for PgControlPlane {
    async fn current_snapshot(&self, table: &TableRef) -> Result<Snapshot> {
        let row = sqlx::query(AssertSqlSafe(
            "select sn.snapshot_id, sn.snapshot_time, sn.schema_version \
             from ducklake_snapshot sn \
             where exists ( \
                 select 1 from ducklake_table t join ducklake_schema s on t.schema_id = s.schema_id \
                 where s.schema_name = $1 and t.table_name = $2 \
                   and t.begin_snapshot <= sn.snapshot_id and (t.end_snapshot is null or t.end_snapshot > sn.snapshot_id)) \
             order by sn.snapshot_id desc limit 1",
        ))
        .bind(&table.schema)
        .bind(&table.name)
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?
        .ok_or_else(|| ControlPlaneError::NotFound(format!("{}.{}", table.schema, table.name)))?;
        Ok(row_to_snapshot(&row))
    }

    async fn snapshots(&self, table: &TableRef) -> Result<Vec<Snapshot>> {
        let rows = sqlx::query(AssertSqlSafe(
            "select sn.snapshot_id, sn.snapshot_time, sn.schema_version \
             from ducklake_snapshot sn \
             where exists ( \
                 select 1 from ducklake_table t join ducklake_schema s on t.schema_id = s.schema_id \
                 where s.schema_name = $1 and t.table_name = $2 \
                   and t.begin_snapshot <= sn.snapshot_id and (t.end_snapshot is null or t.end_snapshot > sn.snapshot_id)) \
             order by sn.snapshot_id",
        ))
        .bind(&table.schema)
        .bind(&table.name)
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        if rows.is_empty() {
            return Err(ControlPlaneError::NotFound(format!(
                "{}.{}",
                table.schema, table.name
            )));
        }
        Ok(rows.iter().map(row_to_snapshot).collect())
    }

    async fn files(&self, table: &TableRef, at: SnapshotId) -> Result<Vec<FileRef>> {
        let tid = self.resolve_table(table, at).await?;
        let rows = sqlx::query(AssertSqlSafe(
            "select path, record_count, file_size_bytes from ducklake_data_file \
             where table_id = $1 and begin_snapshot <= $2 and (end_snapshot is null or end_snapshot > $2) \
             order by data_file_id",
        ))
        .bind(tid)
        .bind(at.0)
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        Ok(rows
            .iter()
            .map(|r| FileRef {
                path: r.get("path"),
                record_count: r.get("record_count"),
                file_size_bytes: r.get("file_size_bytes"),
            })
            .collect())
    }

    async fn schema(&self, table: &TableRef, at: SnapshotId) -> Result<TableSchema> {
        let tid = self.resolve_table(table, at).await?;
        let rows = sqlx::query(AssertSqlSafe(
            "select column_order, column_name, column_type, nulls_allowed from ducklake_column \
             where table_id = $1 and begin_snapshot <= $2 and (end_snapshot is null or end_snapshot > $2) \
             order by column_order",
        ))
        .bind(tid)
        .bind(at.0)
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        Ok(TableSchema {
            columns: rows
                .iter()
                .map(|r| ColumnDef {
                    order: r.get("column_order"),
                    name: r.get("column_name"),
                    ty: r.get("column_type"),
                    nullable: r.get("nulls_allowed"),
                })
                .collect(),
        })
    }
}

impl PgControlPlane {
    /// Resolve the `table_id` of `table` live at snapshot `at`, or `NotFound`.
    async fn resolve_table(&self, table: &TableRef, at: SnapshotId) -> Result<i64> {
        sqlx::query_scalar::<_, i64>(AssertSqlSafe(
            "select t.table_id from ducklake_table t join ducklake_schema s on t.schema_id = s.schema_id \
             where s.schema_name = $1 and t.table_name = $2 \
               and t.begin_snapshot <= $3 and (t.end_snapshot is null or t.end_snapshot > $3)",
        ))
        .bind(&table.schema)
        .bind(&table.name)
        .bind(at.0)
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?
        .ok_or_else(|| {
            ControlPlaneError::NotFound(format!("{}.{} @ {}", table.schema, table.name, at.0))
        })
    }
}
