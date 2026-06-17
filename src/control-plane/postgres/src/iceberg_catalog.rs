use async_trait::async_trait;
use control_plane_core::{
    BaseType, Catalog, ColumnDef, ControlPlaneError, FileRef, Page, PageReq, Result, Snapshot,
    SnapshotId, TableRef, TableSchema,
};
use sqlx::PgPool;

use crate::backend;
use crate::iceberg_type::logical_from_iceberg;

/// Read adapter serving `core::Catalog` from the loom-owned `iceberg_mirror.*` projection.
///
/// The read path is pure Postgres — it never touches `iceberg` or object storage. Snapshot ids
/// and MVCC `begin/end_snapshot` are loom's, assigned by the mirror projection; the structure
/// mirrors the DuckLake adapter (`catalog.rs`) against the `iceberg_mirror.*` tables.
#[derive(Clone)]
pub struct IcebergCatalog {
    pub pool: PgPool,
}

impl IcebergCatalog {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Resolve the `table_id` of `table` live at snapshot `at`, or `NotFound`.
    #[tracing::instrument(skip(self), level = "debug")]
    async fn resolve_table(&self, table: &TableRef, at: SnapshotId) -> Result<i64> {
        sqlx::query_scalar!(
            "select table_id as \"table_id!\" from iceberg_mirror.table \
             where table_namespace = $1 and table_name = $2 \
               and begin_snapshot <= $3 and (end_snapshot is null or end_snapshot > $3)",
            table.schema,
            table.name,
            at.0,
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?
        .ok_or_else(|| {
            ControlPlaneError::NotFound(format!("{}.{} @ {}", table.schema, table.name, at.0))
        })
    }
}

#[async_trait]
impl Catalog for IcebergCatalog {
    #[tracing::instrument(skip(self), level = "debug")]
    async fn current_snapshot(&self, table: &TableRef) -> Result<Snapshot> {
        let row = sqlx::query!(
            "select sn.snapshot_id as \"snapshot_id!\", sn.snapshot_time as \"snapshot_time!\", sn.schema_version as \"schema_version!\" \
             from iceberg_mirror.snapshot sn \
             where exists ( \
                 select 1 from iceberg_mirror.table t \
                 where t.table_namespace = $1 and t.table_name = $2 \
                   and t.begin_snapshot <= sn.snapshot_id and (t.end_snapshot is null or t.end_snapshot > sn.snapshot_id)) \
             order by sn.snapshot_id desc limit 1",
            table.schema,
            table.name,
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?
        .ok_or_else(|| ControlPlaneError::NotFound(format!("{}.{}", table.schema, table.name)))?;
        Ok(Snapshot {
            id: SnapshotId(row.snapshot_id),
            time: row.snapshot_time,
            schema_version: row.schema_version,
        })
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn snapshots(&self, table: &TableRef, _page: PageReq) -> Result<Page<Snapshot>> {
        let rows = sqlx::query!(
            "select sn.snapshot_id as \"snapshot_id!\", sn.snapshot_time as \"snapshot_time!\", sn.schema_version as \"schema_version!\" \
             from iceberg_mirror.snapshot sn \
             where exists ( \
                 select 1 from iceberg_mirror.table t \
                 where t.table_namespace = $1 and t.table_name = $2 \
                   and t.begin_snapshot <= sn.snapshot_id and (t.end_snapshot is null or t.end_snapshot > sn.snapshot_id)) \
             order by sn.snapshot_id",
            table.schema,
            table.name,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        if rows.is_empty() {
            return Err(ControlPlaneError::NotFound(format!(
                "{}.{}",
                table.schema, table.name
            )));
        }
        Ok(Page::from_full(
            rows.into_iter()
                .map(|r| Snapshot {
                    id: SnapshotId(r.snapshot_id),
                    time: r.snapshot_time,
                    schema_version: r.schema_version,
                })
                .collect(),
        ))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn files(
        &self,
        table: &TableRef,
        at: SnapshotId,
        _page: PageReq,
    ) -> Result<Page<FileRef>> {
        let tid = self.resolve_table(table, at).await?;
        let rows = sqlx::query!(
            "select path as \"path!\", record_count as \"record_count!\", file_size_bytes as \"file_size_bytes!\" \
             from iceberg_mirror.data_file \
             where table_id = $1 and begin_snapshot <= $2 and (end_snapshot is null or end_snapshot > $2) \
             order by data_file_id",
            tid,
            at.0,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        Ok(Page::from_full(
            rows.into_iter()
                .map(|r| FileRef {
                    path: r.path,
                    record_count: r.record_count,
                    file_size_bytes: r.file_size_bytes,
                })
                .collect(),
        ))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn schema(&self, table: &TableRef, at: SnapshotId) -> Result<TableSchema> {
        let tid = self.resolve_table(table, at).await?;
        let rows = sqlx::query!(
            "select column_order as \"column_order!\", column_name as \"column_name!\", column_type as \"column_type!\", nulls_allowed as \"nulls_allowed!\" \
             from iceberg_mirror.column \
             where table_id = $1 and begin_snapshot <= $2 and (end_snapshot is null or end_snapshot > $2) \
             order by column_order",
            tid,
            at.0,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        let columns = rows
            .into_iter()
            .map(|r| {
                let ty = logical_from_iceberg(&r.column_type)
                    .map(BaseType::canonical_name)
                    .ok_or_else(|| {
                        ControlPlaneError::Backend(
                            Box::<dyn std::error::Error + Send + Sync>::from(format!(
                                "catalog column type {:?} has no loom logical type",
                                r.column_type
                            )),
                        )
                    })?;
                Ok(ColumnDef {
                    order: r.column_order,
                    name: r.column_name,
                    ty: ty.to_string(),
                    nullable: r.nulls_allowed,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(TableSchema { columns })
    }
}
