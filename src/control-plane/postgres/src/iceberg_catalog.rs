use async_trait::async_trait;
use control_plane_core::{
    BaseType, Catalog, ColumnDef, ControlPlaneError, FileRef, Page, PageReq, Result, Snapshot,
    SnapshotId, TableRef, TableSchema, ViewDef, validate_view_shape, view_definition_event,
};
use sqlx::PgPool;
use time::OffsetDateTime;

use control_plane_core::snapshot::ColumnStat;

use crate::backend;
use crate::iceberg_type::logical_from_iceberg;

/// Physical bookkeeping columns that must never appear in a table's logical
/// (user-facing) schema. `loom_`-prefixed framing/bookkeeping plus the MVCC
/// snapshot bounds (which are inline-only today, filtered here defensively).
pub(crate) fn is_reserved(name: &str) -> bool {
    name.starts_with("loom_") || name == "begin_snapshot" || name == "end_snapshot"
}

/// Look up `table`'s view definition, if it names a catalog view rather than a
/// physical table. `None` when `table` is not a view (including a physical
/// table, or a ref that does not exist at all — existence of the ref itself is
/// each caller's job).
async fn fetch_view(pool: &PgPool, table: &TableRef) -> Result<Option<ViewDef>> {
    let row = sqlx::query!(
        "select base_schema, base_name, predicate, columns \
         from dataset_view.view where view_schema = $1 and view_name = $2",
        table.schema,
        table.name,
    )
    .fetch_optional(pool)
    .await
    .map_err(backend)?;
    row.map(|r| {
        let predicate = r
            .predicate
            .map(serde_json::from_value)
            .transpose()
            .map_err(|e| ControlPlaneError::Serialization(e.to_string()))?;
        Ok(ViewDef {
            view: table.clone(),
            base: TableRef {
                schema: r.base_schema,
                name: r.base_name,
            },
            predicate,
            columns: r.columns,
        })
    })
    .transpose()
}

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

    /// Read the raw mirror columns for `tid` live at `at`, INCLUDING reserved
    /// (`loom_`) columns. The logical `schema()` filters these out; the flush path
    /// needs them to carry stream framing into Parquet.
    #[tracing::instrument(skip(self), level = "debug")]
    pub async fn physical_columns(&self, tid: i64, at: SnapshotId) -> Result<Vec<ColumnDef>> {
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
        rows.into_iter()
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
            .collect::<Result<Vec<_>>>()
    }

    /// The current snapshots of `base` and `clog`, read in ONE statement — hence
    /// against ONE Postgres MVCC snapshot, so the pair is mutually consistent by
    /// construction. This is the changelog feed's pin.
    ///
    /// Why one statement: the slice-2b flush appends the changelog files AND end-caps
    /// the base's inline rows in a single Postgres transaction (`iceberg_flush.rs`),
    /// so a single-statement read sees that flush wholly or not at all. Two
    /// independent `current_snapshot` calls can see HALF of it — the base already
    /// advanced past the flush while the changelog files are still invisible — which
    /// tears the feed's inline-XOR-files invariant and SILENTLY DROPS events
    /// (`iss-stream-feed-torn-read`). Do not "simplify" this back into two reads.
    ///
    /// `None` for either element means that table has no live snapshot (e.g. the
    /// changelog table before the first flush). Absence is an ANSWER, not an error —
    /// unlike `Catalog::current_snapshot`, whose `NotFound` the feed would only have
    /// to catch and discard.
    #[tracing::instrument(skip(self), level = "debug")]
    pub async fn current_snapshots_pair(
        &self,
        base: &TableRef,
        clog: &TableRef,
    ) -> Result<(Option<Snapshot>, Option<Snapshot>)> {
        let rows = sqlx::query!(
            "select v.tag as \"tag!\", \
                    s.snapshot_id as \"snapshot_id?\", \
                    s.snapshot_time as \"snapshot_time?\", \
                    s.schema_version as \"schema_version?\" \
             from (values ('base', $1::text, $2::text), ('clog', $3::text, $4::text)) \
                  as v(tag, ns, nm) \
             left join lateral ( \
                 select sn.snapshot_id, sn.snapshot_time, sn.schema_version \
                 from iceberg_mirror.snapshot sn \
                 where exists ( \
                     select 1 from iceberg_mirror.table t \
                     where t.table_namespace = v.ns and t.table_name = v.nm \
                       and t.begin_snapshot <= sn.snapshot_id \
                       and (t.end_snapshot is null or t.end_snapshot > sn.snapshot_id)) \
                 order by sn.snapshot_id desc limit 1 \
             ) s on true",
            base.schema,
            base.name,
            clog.schema,
            clog.name,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;

        let mut base_snap = None;
        let mut clog_snap = None;
        for r in rows {
            // The three lateral columns are null together (no live snapshot) or present
            // together — the lateral yields a whole row or no row.
            let snap = match (r.snapshot_id, r.snapshot_time, r.schema_version) {
                (Some(id), Some(time), Some(schema_version)) => Some(Snapshot {
                    id: SnapshotId(id),
                    time,
                    schema_version,
                }),
                _ => None,
            };
            if r.tag == "base" {
                base_snap = snap;
            } else {
                clog_snap = snap;
            }
        }
        Ok((base_snap, clog_snap))
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
        let resolved; // borrow gymnastics: delegate view -> base
        let (table, _projection) = match fetch_view(&self.pool, table).await? {
            Some(v) => {
                resolved = v.base;
                (&resolved, v.columns)
            }
            None => (table, None),
        };
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
        let resolved; // borrow gymnastics: delegate view -> base
        let (table, _projection) = match fetch_view(&self.pool, table).await? {
            Some(v) => {
                resolved = v.base;
                (&resolved, v.columns)
            }
            None => (table, None),
        };
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
    async fn snapshot(&self, table: &TableRef, id: SnapshotId) -> Result<Option<Snapshot>> {
        let resolved; // borrow gymnastics: delegate view -> base
        let (table, _projection) = match fetch_view(&self.pool, table).await? {
            Some(v) => {
                resolved = v.base;
                (&resolved, v.columns)
            }
            None => (table, None),
        };
        let row = sqlx::query!(
            "select sn.snapshot_id as \"snapshot_id!\", sn.snapshot_time as \"snapshot_time!\", sn.schema_version as \"schema_version!\" \
             from iceberg_mirror.snapshot sn \
             where sn.snapshot_id = $3 and exists ( \
                 select 1 from iceberg_mirror.table t \
                 where t.table_namespace = $1 and t.table_name = $2 \
                   and t.begin_snapshot <= sn.snapshot_id and (t.end_snapshot is null or t.end_snapshot > sn.snapshot_id))",
            table.schema,
            table.name,
            id.0,
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
    async fn snapshot_horizon(&self, cutoff: OffsetDateTime) -> Result<Option<SnapshotId>> {
        Ok(crate::iceberg_mirror::horizon_before(&self.pool, cutoff)
            .await?
            .map(SnapshotId))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn snapshots(&self, table: &TableRef, _page: PageReq) -> Result<Page<Snapshot>> {
        let resolved; // borrow gymnastics: delegate view -> base
        let (table, _projection) = match fetch_view(&self.pool, table).await? {
            Some(v) => {
                resolved = v.base;
                (&resolved, v.columns)
            }
            None => (table, None),
        };
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
        let resolved; // borrow gymnastics: delegate view -> base
        let (table, _projection) = match fetch_view(&self.pool, table).await? {
            Some(v) => {
                resolved = v.base;
                (&resolved, v.columns)
            }
            None => (table, None),
        };
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
        let resolved; // borrow gymnastics: delegate view -> base
        let (table, projection) = match fetch_view(&self.pool, table).await? {
            Some(v) => {
                resolved = v.base;
                (&resolved, v.columns)
            }
            None => (table, None),
        };
        let tid = self.resolve_table(table, at).await?;
        let mut columns: Vec<ColumnDef> = self
            .physical_columns(tid, at)
            .await?
            .into_iter()
            .filter(|c| !is_reserved(&c.name))
            .collect();
        if let Some(proj) = projection {
            columns.retain(|c| proj.contains(&c.name));
        }
        Ok(TableSchema { columns })
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn list_tables(&self, _page: PageReq) -> Result<Page<TableRef>> {
        // `live_tables` is already this exact query: end-cap-free rows,
        // `(table_namespace, table_name)`-ordered.
        Ok(Page::from_full(self.live_tables().await?))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn define_view(&self, view: ViewDef) -> Result<()> {
        // The base's schema/snapshot are read via the ordinary (view-delegating)
        // read path BEFORE the transaction opens below — simpler than repeating the
        // MVCC join inside the tx, at the cost of a small TOCTOU window between this
        // read and the transactional collision/existence checks (the base could be
        // dropped or re-schema'd in between). This mirrors loom's existing
        // cross-concern posture elsewhere (e.g. `ensure_table`'s check-then-insert).
        //
        // A base that is ITSELF a view is rejected here (before consulting its
        // schema, which would otherwise silently delegate through to the ultimate
        // physical table and let a view-over-view slip past `validate_view_shape`).
        if fetch_view(&self.pool, &view.base).await?.is_some() {
            return Err(ControlPlaneError::Validation(
                "view-over-view is not supported".into(),
            ));
        }
        let base_snapshot = self.current_snapshot(&view.base).await?;
        let base_schema = self.schema(&view.base, base_snapshot.id).await?;
        validate_view_shape(&view, &base_schema).map_err(ControlPlaneError::Validation)?;

        let mut tx = self.pool.begin().await.map_err(backend)?;

        let view_row_exists = sqlx::query_scalar!(
            "select exists(select 1 from dataset_view.view \
             where view_schema = $1 and view_name = $2) as \"e!\"",
            view.view.schema,
            view.view.name,
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(backend)?;
        if view_row_exists {
            return Err(ControlPlaneError::Conflict(format!(
                "view {}.{} already exists",
                view.view.schema, view.view.name
            )));
        }
        let view_is_live_table = sqlx::query_scalar!(
            "select exists(select 1 from iceberg_mirror.table \
             where table_namespace = $1 and table_name = $2 and end_snapshot is null) as \"e!\"",
            view.view.schema,
            view.view.name,
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(backend)?;
        if view_is_live_table {
            return Err(ControlPlaneError::Conflict(format!(
                "{}.{} is a physical table",
                view.view.schema, view.view.name
            )));
        }
        let base_is_live_table = sqlx::query_scalar!(
            "select exists(select 1 from iceberg_mirror.table \
             where table_namespace = $1 and table_name = $2 and end_snapshot is null) as \"e!\"",
            view.base.schema,
            view.base.name,
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(backend)?;
        if !base_is_live_table {
            return Err(ControlPlaneError::NotFound(format!(
                "{}.{}",
                view.base.schema, view.base.name
            )));
        }

        let predicate = view
            .predicate
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|e| ControlPlaneError::Serialization(e.to_string()))?;
        sqlx::query!(
            "insert into dataset_view.view \
             (view_schema, view_name, base_schema, base_name, predicate, columns) \
             values ($1, $2, $3, $4, $5, $6)",
            view.view.schema,
            view.view.name,
            view.base.schema,
            view.base.name,
            predicate,
            view.columns.as_deref(),
        )
        .execute(&mut *tx)
        .await
        .map_err(backend)?;

        crate::lineage::pg_emit(&mut *tx, &view_definition_event(&view)).await?;

        tx.commit().await.map_err(backend)?;
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn drop_view(&self, view: &TableRef) -> Result<()> {
        let deleted = sqlx::query!(
            "delete from dataset_view.view where view_schema = $1 and view_name = $2 \
             returning view_name",
            view.schema,
            view.name,
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?;
        if deleted.is_none() {
            return Err(ControlPlaneError::NotFound(format!(
                "{}.{}",
                view.schema, view.name
            )));
        }
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn get_view(&self, view: &TableRef) -> Result<Option<ViewDef>> {
        fetch_view(&self.pool, view).await
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn list_views(&self, _page: PageReq) -> Result<Page<ViewDef>> {
        let rows = sqlx::query!(
            "select view_schema, view_name, base_schema, base_name, predicate, columns \
             from dataset_view.view order by view_schema, view_name"
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let predicate = r
                .predicate
                .map(serde_json::from_value)
                .transpose()
                .map_err(|e| ControlPlaneError::Serialization(e.to_string()))?;
            out.push(ViewDef {
                view: TableRef {
                    schema: r.view_schema,
                    name: r.view_name,
                },
                base: TableRef {
                    schema: r.base_schema,
                    name: r.base_name,
                },
                predicate,
                columns: r.columns,
            });
        }
        Ok(Page::from_full(out))
    }
}
