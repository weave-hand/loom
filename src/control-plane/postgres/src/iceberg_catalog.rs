use async_trait::async_trait;
use control_plane_core::{
    BaseType, Catalog, ColumnDef, ControlPlaneError, FileRef, Page, PageReq, Result, Snapshot,
    SnapshotId, TableRef, TableSchema,
};
use sqlx::PgPool;
use time::OffsetDateTime;

use control_plane_core::snapshot::ColumnStat;

use crate::backend;
use crate::iceberg_type::logical_from_iceberg;

/// A live data file plus its per-column stats, for the pruning-aware serving
/// provider. Concrete to Iceberg — the shared `Catalog`/`FileRef` must not grow a
/// stats field. `column_stats` is empty for files written before per-column stats
/// landed (always kept by the pruner).
#[derive(Clone, Debug)]
pub struct FileWithStats {
    pub path: String,
    pub record_count: i64,
    pub file_size_bytes: i64,
    pub column_stats: Vec<ColumnStat>,
}

/// Read adapter serving `core::Catalog` from the loom-owned `iceberg_mirror.*` projection.
///
/// The read path is pure Postgres — it never touches `iceberg` or object storage. Snapshot ids
/// and MVCC `begin/end_snapshot` are loom's, assigned by the mirror projection; the structure
/// reads against the `iceberg_mirror.*` tables.
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

    /// Live data files at `at` joined to their persisted per-column stats, with the
    /// stored text bounds re-typed via each column's iceberg type. One query LEFT JOINs
    /// `data_file ⋈ data_file_column_stat ⋈ column` (the column join supplies the iceberg
    /// type for `stat_from_text`), then groups the rows by `data_file_id` in Rust. A file
    /// with no stat rows yields `column_stats: vec![]` (the pruner keeps it). For a stat
    /// row whose `column_type` carries no loom bound, `min`/`max` are `None` but the
    /// `null_count`/`column_size_bytes` are still recorded.
    #[tracing::instrument(skip(self), level = "debug")]
    pub async fn files_with_stats(
        &self,
        table: &TableRef,
        at: SnapshotId,
    ) -> Result<Vec<FileWithStats>> {
        let tid = self.resolve_table(table, at).await?;
        let rows = sqlx::query!(
            "select f.data_file_id as \"data_file_id!\", f.path as \"path!\", \
                    f.record_count as \"record_count!\", f.file_size_bytes as \"file_size_bytes!\", \
                    cs.column_name as \"column_name?\", cs.null_count as \"null_count?\", \
                    cs.column_size_bytes as \"column_size_bytes?\", \
                    cs.min_value as \"min_value?\", cs.max_value as \"max_value?\", \
                    c.column_type as \"column_type?\" \
             from iceberg_mirror.data_file f \
             left join iceberg_mirror.data_file_column_stat cs on cs.data_file_id = f.data_file_id \
             left join iceberg_mirror.column c \
               on c.table_id = f.table_id and c.column_name = cs.column_name \
                  and c.begin_snapshot <= $2 and (c.end_snapshot is null or c.end_snapshot > $2) \
             where f.table_id = $1 and f.begin_snapshot <= $2 \
                   and (f.end_snapshot is null or f.end_snapshot > $2) \
             order by f.data_file_id",
            tid,
            at.0,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;

        // Group by data_file_id, preserving the `order by` sequence. Each row carries the
        // file scalars (always present) plus an optional stat row. A NULL stat column_name
        // means the LEFT JOIN found no stats for the file -> column_stats stays empty.
        let mut out: Vec<FileWithStats> = Vec::new();
        let mut cur_id: Option<i64> = None;
        for r in rows {
            if cur_id != Some(r.data_file_id) {
                cur_id = Some(r.data_file_id);
                out.push(FileWithStats {
                    path: r.path,
                    record_count: r.record_count,
                    file_size_bytes: r.file_size_bytes,
                    column_stats: Vec::new(),
                });
            }
            let file = out
                .last_mut()
                .ok_or_else(|| ControlPlaneError::Backend("just pushed; must be present".into()))?;
            if let (Some(column_name), Some(null_count), Some(column_size_bytes)) =
                (r.column_name, r.null_count, r.column_size_bytes)
            {
                let ty = r.column_type;
                let retype = |text: Option<String>| {
                    let text = text?;
                    let ty = ty.as_deref()?;
                    crate::iceberg_stats::stat_from_text(&text, ty)
                };
                file.column_stats.push(ColumnStat {
                    column_name,
                    null_count,
                    column_size_bytes,
                    min: retype(r.min_value),
                    max: retype(r.max_value),
                });
            }
        }
        Ok(out)
    }

    /// Every table currently live in the mirror (those with no `end_snapshot`),
    /// as loom `TableRef`s (`table_namespace` -> schema, `table_name` -> name).
    /// The read engine registers each as a DataFusion table.
    #[tracing::instrument(skip(self), level = "debug")]
    pub async fn live_tables(&self) -> Result<Vec<TableRef>> {
        let rows = sqlx::query!(
            "select table_namespace as \"schema!\", table_name as \"name!\" \
             from iceberg_mirror.table where end_snapshot is null \
             order by table_namespace, table_name"
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        Ok(rows
            .into_iter()
            .map(|r| TableRef {
                schema: r.schema,
                name: r.name,
            })
            .collect())
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
    async fn snapshot_as_of(
        &self,
        table: &TableRef,
        ts: OffsetDateTime,
    ) -> Result<Option<Snapshot>> {
        let row = sqlx::query!(
            "select sn.snapshot_id as \"snapshot_id!\", sn.snapshot_time as \"snapshot_time!\", sn.schema_version as \"schema_version!\" \
             from iceberg_mirror.snapshot sn \
             where sn.snapshot_time <= $3 and exists ( \
                 select 1 from iceberg_mirror.table t \
                 where t.table_namespace = $1 and t.table_name = $2 \
                   and t.begin_snapshot <= sn.snapshot_id and (t.end_snapshot is null or t.end_snapshot > sn.snapshot_id)) \
             order by sn.snapshot_id desc limit 1",
            table.schema,
            table.name,
            ts,
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?;
        Ok(row.map(|r| Snapshot {
            id: SnapshotId(r.snapshot_id),
            time: r.snapshot_time,
            schema_version: r.schema_version,
        }))
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

    #[tracing::instrument(skip(self), level = "debug")]
    async fn list_tables(&self, _page: PageReq) -> Result<Page<TableRef>> {
        // `live_tables` is already this exact query: end-cap-free rows,
        // `(table_namespace, table_name)`-ordered.
        Ok(Page::from_full(self.live_tables().await?))
    }
}
