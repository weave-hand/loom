//! The `iceberg_mirror.vector_index` binding (row type + insert/lookup) and the
//! build primitive (Task 6). loom records `(table, column, covered_snapshot) ->
//! puffin_path` as a mirror row in lieu of a REST catalog.

use std::sync::Arc;

use arrow_array::{Float32Array, Int32Array, Int64Array, ListArray, RecordBatch, StringArray};
use control_plane_core::{
    BUILD_VECTOR_INDEX_JOB_KIND, BuildVectorIndexJob, Catalog, ControlPlaneError, DatasetRef,
    EventType, IndexSpec, LineageEvent, Metric, NewJob, Result, RunId, SnapshotId, TableRef,
    TableSchema, VectorKey,
};
use iceberg::{Catalog as IceCatalog, TableIdent};
use sqlx::{AssertSqlSafe, PgConnection, PgPool};
use time::OffsetDateTime;

use crate::backend;

/// A bound vector index: the metadata loom needs to find and decode the sidecar.
#[derive(Clone, Debug)]
pub struct VectorIndexRow {
    pub table_id: i64,
    pub column: String,
    pub index_name: String,
    pub covered_snapshot: i64,
    pub metric: String,
    pub index_kind: String,
    pub dim: i32,
    pub row_count: i64,
    pub puffin_path: String,
}

// SQL-STYLE: the fixed-relation queries below (insert_vector_index,
// lookup_vector_index, identity_column_for) use compile-time `query!`/
// `query_scalar!` against the committed `.sqlx` cache, like the rest of the
// adapter. Only the queries in `inline_delta_batch` stay RUNTIME
// `sqlx::query(AssertSqlSafe(...))`: they interpolate the per-table inline
// relation name (`inline_table_name(tid)`) and the ontology-declared identity/
// vector column names, which a compile-time macro cannot accept. Those interpolated
// fragments are loom-controlled identifiers (not user input), and every value is a
// bound `$n` param, so AssertSqlSafe carries no injection risk — the same runtime
// pattern used in `iceberg_inline.rs`/`fixture.rs`.

/// Upsert a `vector_index` binding row in the caller's transaction.
///
/// Keyed on `(table_id, column_name, index_name, covered_snapshot)` (the table's
/// primary key): re-building the same named index for the same column at the same
/// covered snapshot replaces the binding so it points at the freshly written Puffin
/// sidecar. This keeps `build_vector_index` idempotent — a re-run or queue-retried
/// build job at an unchanged snapshot refreshes the pointer instead of failing on a
/// duplicate key (which would poison the job). Distinct `index_name`s coexist.
pub async fn insert_vector_index(tx: &mut PgConnection, row: &VectorIndexRow) -> Result<()> {
    sqlx::query!(
        "insert into iceberg_mirror.vector_index \
         (table_id, column_name, index_name, covered_snapshot, metric, index_kind, dim, row_count, puffin_path) \
         values ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
         on conflict (table_id, column_name, index_name, covered_snapshot) do update set \
             metric = excluded.metric, \
             index_kind = excluded.index_kind, \
             dim = excluded.dim, \
             row_count = excluded.row_count, \
             puffin_path = excluded.puffin_path, \
             created_at = now()",
        row.table_id,
        row.column,
        row.index_name,
        row.covered_snapshot,
        row.metric,
        row.index_kind,
        row.dim,
        row.row_count,
        row.puffin_path,
    )
    .execute(&mut *tx)
    .await
    .map_err(backend)?;
    Ok(())
}

/// The newest bound index for `(table_id, index_name)` with `covered_snapshot <= at`,
/// or `None` if none is bound.
pub async fn lookup_vector_index(
    pool: &PgPool,
    table_id: i64,
    index_name: &str,
    at: i64,
) -> Result<Option<VectorIndexRow>> {
    let row = sqlx::query!(
        "select table_id, column_name, index_name, covered_snapshot, metric, index_kind, dim, \
                row_count, puffin_path \
         from iceberg_mirror.vector_index \
         where table_id = $1 and index_name = $2 and covered_snapshot <= $3 \
         order by covered_snapshot desc limit 1",
        table_id,
        index_name,
        at,
    )
    .fetch_optional(pool)
    .await
    .map_err(backend)?;
    Ok(row.map(|r| VectorIndexRow {
        table_id: r.table_id,
        column: r.column_name,
        index_name: r.index_name,
        covered_snapshot: r.covered_snapshot,
        metric: r.metric,
        index_kind: r.index_kind,
        dim: r.dim,
        row_count: r.row_count,
        puffin_path: r.puffin_path,
    }))
}

// ---------------------------------------------------------------------------
// Task 6: build primitive
// ---------------------------------------------------------------------------

/// The result of a completed vector-index build.
#[derive(Clone, Debug)]
pub struct BuiltIndex {
    /// The loom snapshot id that was current when the build read its data.
    pub covered_snapshot: i64,
    /// The object-store path of the written Puffin sidecar.
    pub puffin_path: String,
    /// The number of vectors included in the index (cold + hot rows).
    pub row_count: i64,
}

/// Return the names of all vector indexes declared for the ontology type that backs
/// `table`, or an empty `Vec` if the table has no associated type. Used by the flush
/// path to enqueue rebuild jobs for stale indexes after a snapshot commit.
pub(crate) async fn declared_vector_index_names(
    pool: &PgPool,
    table: &TableRef,
) -> Result<Vec<String>> {
    let type_name = match type_name_for(pool, table).await {
        Ok(t) => t,
        Err(ControlPlaneError::NotFound(_)) => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    sqlx::query_scalar!(
        "select name from ontology.vector_index_definition where type_name = $1",
        type_name,
    )
    .fetch_all(pool)
    .await
    .map_err(backend)
}

/// One `build_vector_index` NewJob per vector index declared on the ontology
/// type backing `table` (empty when the table has no type or no indexes) —
/// the shared enqueue source for the flush AND overwrite/replace commit
/// paths, so their pending-dedup keys always collide.
pub(crate) async fn rebuild_jobs_for(pool: &PgPool, table: &TableRef) -> Result<Vec<NewJob>> {
    let index_names = declared_vector_index_names(pool, table).await?;
    index_names
        .iter()
        .map(|index_name| {
            let payload = serde_json::to_value(BuildVectorIndexJob {
                schema: table.schema.clone(),
                name: table.name.clone(),
                index_name: index_name.clone(),
            })
            .map_err(backend)?;
            Ok(NewJob {
                kind: BUILD_VECTOR_INDEX_JOB_KIND.to_string(),
                payload,
                run_at: None,
                priority: 0,
            })
        })
        .collect::<Result<Vec<_>>>()
}

/// Resolve the ontology type name backing `(table.schema, table.name)`.
pub async fn type_name_for(pool: &PgPool, table: &TableRef) -> Result<String> {
    sqlx::query_scalar!(
        "select name from ontology.object_type where table_schema = $1 and table_name = $2",
        table.schema,
        table.name,
    )
    .fetch_optional(pool)
    .await
    .map_err(backend)?
    .ok_or_else(|| ControlPlaneError::NotFound(format!("type for {}.{}", table.schema, table.name)))
}

/// Resolve the ontology's declared `identity` column for `(table.schema, table.name)`.
///
/// Returns an error if the type row is absent or the identity field is `NULL`
/// (the build requires a declared identity to populate `VectorKey`).
async fn identity_column_for(pool: &PgPool, table: &TableRef) -> Result<String> {
    let id = sqlx::query_scalar!(
        "select identity from ontology.object_type \
         where table_schema = $1 and table_name = $2",
        table.schema,
        table.name,
    )
    .fetch_optional(pool)
    .await
    .map_err(backend)?
    .flatten();
    id.ok_or_else(|| {
        ControlPlaneError::Backend(
            format!(
                "no identity column declared for {}.{}",
                table.schema, table.name
            )
            .into(),
        )
    })
}

/// Extract `(VectorKey, Vec<f32>)` rows from an Arrow `RecordBatch` using
/// `vector_col` (a `List<Float32>` column) and `identity_col` (an `Int64`,
/// `Int32`, or `Utf8` column).
fn extract_rows(
    batch: &RecordBatch,
    vector_col: &str,
    identity_col: &str,
) -> Result<Vec<(VectorKey, Vec<f32>)>> {
    let vec_idx = batch.schema().index_of(vector_col).map_err(backend)?;
    let id_idx = batch.schema().index_of(identity_col).map_err(backend)?;

    let vec_col = batch.column(vec_idx);
    let id_col = batch.column(id_idx);

    let list_arr = vec_col
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| {
            ControlPlaneError::Backend(
                format!("vector column '{vector_col}' is not a List array").into(),
            )
        })?;

    let n = batch.num_rows();
    let mut out = Vec::with_capacity(n);

    for row in 0..n {
        // Extract the VectorKey from the identity column.
        let key = if let Some(i64arr) = id_col.as_any().downcast_ref::<Int64Array>() {
            VectorKey::Int(i64arr.value(row))
        } else if let Some(i32arr) = id_col.as_any().downcast_ref::<Int32Array>() {
            VectorKey::Int(i32arr.value(row) as i64)
        } else if let Some(sarr) = id_col.as_any().downcast_ref::<StringArray>() {
            VectorKey::Str(sarr.value(row).to_string())
        } else {
            return Err(ControlPlaneError::Backend(
                format!(
                    "identity column '{identity_col}' has unsupported Arrow type {:?}",
                    id_col.data_type()
                )
                .into(),
            ));
        };

        // Extract the vector slice from the List<Float32> column.
        let elem_arr = list_arr.value(row);
        let f32arr = elem_arr
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| {
                ControlPlaneError::Backend(
                    format!("vector column '{vector_col}' child is not Float32 at row {row}")
                        .into(),
                )
            })?;
        let vec: Vec<f32> = f32arr.values().to_vec();
        out.push((key, vec));
    }
    Ok(out)
}

/// Read the SCOREABLE inline delta for `table`: the rows born AFTER `born_after`
/// that are still alive at snapshot `at`, returning only the `identity_col` and
/// `vector_col` columns. Returns `None` if the inline table does not exist or
/// has no matching rows.
///
/// "Scoreable" excludes three kinds of live inline row, none of which carries a
/// current, scoreable vector for its identity:
///   - **tombstones** (`loom_tombstone` — a non-CDC delete or a CDC `-D`): a
///     deleted identity contributes no hot vector, and a non-CDC tombstone row is
///     id-only (its vector column is physically NULL);
///   - **CDC `-U` before-images**: audit rows, never current state;
///   - **rows with a NULL vector**: unscoreable by construction.
///
/// A tombstoned identity's stale COLD hit is suppressed downstream by query-api's
/// survivor post-filter (2026-07-07-search-cold-suppression-design), not here.
/// The exclusion is what makes the output schema's non-nullable vector field
/// truthful (iss-search-vector-merge-view-nullable).
///
/// Used by Task 7/8 (hot-delta path) to fetch the rows appended between S and Q
/// so the serving layer can score them alongside the cold Puffin index.
///
/// The identity column is decoded per its DECLARED logical type (Long, Integer,
/// or String — the same kinds the cold path's `extract_rows` accepts), through
/// the shared `column_array` PG→Arrow bridge, so the delta batch can never
/// drift from `inline_live_batch` (iss-inline-delta-string-identity).
pub async fn inline_delta_batch(
    pool: &PgPool,
    table: &TableRef,
    born_after: i64,
    at: i64,
) -> Result<Option<RecordBatch>> {
    use crate::iceberg_inline::{
        column_array, inline_table_exists, inline_table_name, mvcc_live_pred, quote_ident,
    };
    use crate::iceberg_mirror::live_table_id;
    use control_plane_core::resolve_logical;

    let mut conn = pool.acquire().await.map_err(backend)?;
    let Some(tid) = live_table_id(&mut conn, &table.schema, &table.name).await? else {
        return Ok(None);
    };

    // Check the inline table exists.
    if !inline_table_exists(&mut conn, tid).await? {
        return Ok(None);
    }

    // Resolve identity + vector column names AND logical types from the
    // ontology/mirror. We look for a column whose type starts with "vector("
    // in the current mirror snapshot (at SnapshotId(at)); the identity's
    // BaseType drives the decode below.
    let identity_col = identity_column_for(pool, table).await?;
    let ice = crate::iceberg_catalog::IcebergCatalog::new(pool.clone());
    let schema = ice.schema(table, SnapshotId(at)).await?;
    let vec_def = schema
        .columns
        .iter()
        .find(|c| c.ty.starts_with("vector("))
        .ok_or_else(|| {
            ControlPlaneError::Backend(
                format!(
                    "no vector column in schema for {}.{}",
                    table.schema, table.name
                )
                .into(),
            )
        })?;
    let vector_col = vec_def.name.clone();
    let vec_ty = resolve_logical(&vec_def.ty).ok_or_else(|| {
        ControlPlaneError::Backend(
            format!(
                "unresolvable vector type {:?} for {}.{}",
                vec_def.ty, table.schema, table.name
            )
            .into(),
        )
    })?;
    let id_ty = schema
        .columns
        .iter()
        .find(|c| c.name == identity_col)
        .and_then(|c| resolve_logical(&c.ty))
        .ok_or_else(|| {
            ControlPlaneError::Backend(
                format!(
                    "identity column '{identity_col}' missing or unresolvable in schema for {}.{}",
                    table.schema, table.name
                )
                .into(),
            )
        })?;

    // Runtime query: select only the identity + vector columns with the delta
    // MVCC predicate. Tombstones (`loom_tombstone` — set by BOTH a non-CDC delete
    // and a CDC `-D`), CDC `-U` before-images (audit rows, mirroring the inline
    // provider's base predicate, serving.rs), and rows without a vector are
    // EXCLUDED: none is scoreable, and a tombstone's NULL vector would fail the
    // non-nullable output schema's `RecordBatch` validation (the `/search` 500 of
    // iss-search-vector-merge-view-nullable). A tombstoned identity's stale COLD
    // hit is suppressed by query-api's survivor post-filter, not here.
    let id_quoted = quote_ident(&identity_col);
    let vec_quoted = quote_ident(&vector_col);
    let rows = sqlx::query(AssertSqlSafe(format!(
        "select {id_quoted}, {vec_quoted} \
         from {} \
         where begin_snapshot > {born_after} \
           and {} \
           and not loom_tombstone \
           and (loom_change_kind is null or loom_change_kind <> '-U') \
           and {vec_quoted} is not null \
         order by loom_row_id",
        inline_table_name(tid),
        mvcc_live_pred(at),
    )))
    .fetch_all(&mut *conn)
    .await
    .map_err(backend)?;

    if rows.is_empty() {
        return Ok(None);
    }

    // Decode through THE shared PG-row → Arrow bridge (`column_array`): the
    // identity per its declared BaseType, the vector as List<Float32> with the
    // canonical "item" child. Field data types come from the same BaseType map.
    use arrow_schema::{Field, Schema};

    let id_array = column_array(&rows, 0, id_ty)?;
    let vec_array = column_array(&rows, 1, vec_ty)?;
    let out_schema = Arc::new(Schema::new(vec![
        Field::new(&identity_col, id_ty.arrow_data_type(), false),
        Field::new(&vector_col, vec_ty.arrow_data_type(), false),
    ]));
    let batch = RecordBatch::try_new(out_schema, vec![id_array, vec_array]).map_err(backend)?;
    Ok(Some(batch))
}

/// The pre-read inputs of a vector-index build: the MVCC anchor snapshot S
/// (captured BEFORE any data read so cold and hot reads are consistent as of
/// S), the named declaration's column/metric/spec (authoritative), and the
/// ontology-declared identity column.
struct BuildInputs {
    at: SnapshotId,
    column: String,
    metric: Metric,
    spec: IndexSpec,
    identity_col: String,
}

/// Jobs 1-2 of the build: snapshot anchor, declaration resolution, identity
/// column. Resolution order (snapshot -> declaration -> identity) is
/// load-bearing for error precedence and preserved from the inline code.
async fn resolve_build_inputs(
    ice: &crate::iceberg_catalog::IcebergCatalog,
    pool: &PgPool,
    table: &TableRef,
    index_name: &str,
) -> Result<BuildInputs> {
    let at = ice.current_snapshot(table).await?.id;
    let type_name = type_name_for(pool, table).await?;
    let def = crate::ontology::vector_index_def_row(pool, &type_name, index_name)
        .await?
        .ok_or_else(|| {
            ControlPlaneError::NotFound(format!(
                "no vector index definition `{index_name}` on type `{type_name}`"
            ))
        })?;
    let identity_col = identity_column_for(pool, table).await?;
    Ok(BuildInputs {
        at,
        column: def.property,
        metric: def.metric,
        spec: def.spec,
        identity_col,
    })
}

/// Jobs 3-5: read the cold Parquet files and hot inline rows live at `at`,
/// and extract `(VectorKey, vector)` rows from both tiers.
async fn collect_vectors(
    catalog: &crate::iceberg_sql_catalog::SqlCatalog,
    ice: &crate::iceberg_catalog::IcebergCatalog,
    table: &TableRef,
    at: SnapshotId,
    column: &str,
    identity_col: &str,
) -> Result<Vec<(VectorKey, Vec<f32>)>> {
    let files = ice.files_with_stats(table, at).await?;
    let paths: Vec<String> = files.iter().map(|f| f.path.clone()).collect();
    let (_, cold_batches) = crate::read_files_as_batches(catalog, table, &paths).await?;
    let hot_batch: Option<RecordBatch> = ice.inline_live_batch(table, at).await?.map(|(_, _, b)| b);

    let mut all_rows: Vec<(VectorKey, Vec<f32>)> = Vec::new();
    for batch in cold_batches.iter().chain(hot_batch.iter()) {
        all_rows.extend(extract_rows(batch, column, identity_col)?);
    }
    Ok(all_rows)
}

/// The declared `vector(N)` dimension of `column` in `schema`, or 0 when the
/// column is missing or its type is not a well-formed `vector(N)` — the
/// build's fallback when the table has no rows to infer from (job 6's
/// legacy `unwrap_or(0)`).
#[must_use]
pub fn declared_dim(schema: &TableSchema, column: &str) -> u32 {
    schema
        .columns
        .iter()
        .find(|c| c.name == column)
        .and_then(|c| {
            // ty is e.g. "vector(4)"
            c.ty.strip_prefix("vector(")
                .and_then(|s| s.strip_suffix(')'))
                .and_then(|s| s.parse::<u32>().ok())
        })
        .unwrap_or(0)
}

/// Jobs 8-9: resolve the vector column's Iceberg field id (informational),
/// mint a fresh sidecar path under the table's metadata location, and write
/// the Puffin file. The object-store write happens BEFORE the Postgres tx —
/// a failed build leaves an orphan sidecar, never a dangling mirror row.
/// `write_vector_index` is the single source of the 7-key property map.
async fn write_sidecar(
    catalog: &crate::iceberg_sql_catalog::SqlCatalog,
    table: &TableRef,
    index: &dyn control_plane_core::VectorIndex,
    covered_snapshot: i64,
    column: &str,
    identity_col: &str,
) -> Result<String> {
    let ident =
        TableIdent::from_strs([table.schema.as_str(), table.name.as_str()]).map_err(backend)?;
    let tbl = catalog.load_table(&ident).await.map_err(backend)?;
    // Use the Schema::field_id_by_name accessor (available on the iceberg-rust
    // pinned main commit). Falls back to 0 if the accessor returns None (e.g.
    // if the Iceberg schema uses a different field name than expected — purely
    // informational for Puffin footer decode in slice 1).
    let field_id: i32 = tbl
        .metadata()
        .current_schema()
        .field_id_by_name(column)
        .unwrap_or(0);

    let puffin_path = format!(
        "{}/metadata/loom-vector-index-{}.puffin",
        tbl.metadata().location(),
        uuid::Uuid::new_v4()
    );
    let file_io = tbl.file_io().clone();
    crate::puffin::write_vector_index(
        &file_io,
        &puffin_path,
        index,
        covered_snapshot,
        field_id,
        column,
        identity_col,
    )
    .await?;
    Ok(puffin_path)
}

/// The build's completion lineage event: input = the CANONICAL loom dataset
/// ref for the source table (`DatasetRef::from(table)` — the same node the
/// landing/flush emitters use; iss-vector-build-lineage-ref, #295), output =
/// the Puffin sidecar under the "loom-vector-index" namespace.
#[must_use]
pub fn build_lineage_event(
    run_id: RunId,
    table: &TableRef,
    column: &str,
    covered_snapshot: i64,
    row_count: i64,
    puffin_path: &str,
) -> LineageEvent {
    LineageEvent {
        run_id,
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![DatasetRef::from(table)],
        outputs: vec![DatasetRef {
            namespace: "loom-vector-index".to_string(),
            name: puffin_path.to_string(),
        }],
        payload: serde_json::json!({
            "column": column,
            "covered_snapshot": covered_snapshot,
            "row_count": row_count,
        }),
    }
}

/// The mirror-row fields of a completed build: `VectorIndexRow` before the
/// `table_id` is known — it is resolved INSIDE `bind_index_and_emit`'s
/// transaction (moving it earlier would change failure semantics under a
/// concurrent table drop/re-create).
struct IndexBinding {
    index_name: String,
    column: String,
    covered_snapshot: i64,
    metric: String,
    index_kind: String,
    dim: i32,
    row_count: i64,
    puffin_path: String,
}

/// Job 10: ONE Postgres tx — resolve the live mirror table id, upsert the
/// `vector_index` binding row, emit the lineage event, commit.
async fn bind_index_and_emit(
    pool: &PgPool,
    table: &TableRef,
    binding: IndexBinding,
    lineage: &LineageEvent,
) -> Result<()> {
    use crate::iceberg_mirror::live_table_id;
    use crate::lineage::pg_emit;

    let mut tx = pool.begin().await.map_err(backend)?;
    let conn: &mut PgConnection = &mut tx;

    let table_id = live_table_id(conn, &table.schema, &table.name)
        .await?
        .ok_or_else(|| {
            ControlPlaneError::NotFound(format!(
                "no live mirror table for {}.{}",
                table.schema, table.name
            ))
        })?;

    insert_vector_index(
        conn,
        &VectorIndexRow {
            table_id,
            column: binding.column,
            index_name: binding.index_name,
            covered_snapshot: binding.covered_snapshot,
            metric: binding.metric,
            index_kind: binding.index_kind,
            dim: binding.dim,
            row_count: binding.row_count,
            puffin_path: binding.puffin_path,
        },
    )
    .await?;

    pg_emit(conn, lineage).await?;
    tx.commit().await.map_err(backend)
}

/// Build a flat vector index over all vectors live at the table's current
/// snapshot, write a Puffin sidecar to object storage, and commit the
/// `vector_index` mirror row plus a lineage event in one Postgres transaction.
///
/// The build reads:
/// - **cold** data: Parquet files from the Iceberg mirror (via `read_files_as_batches`)
/// - **hot** data: live inline rows (via `IcebergCatalog::inline_live_batch`)
///
/// The covered snapshot `S` is captured before any data read so both cold and
/// hot reads are MVCC-consistent as of `S`.
pub async fn build_vector_index(
    catalog: &crate::iceberg_sql_catalog::SqlCatalog,
    pool: &PgPool,
    table: &TableRef,
    index_name: &str,
    run_id: RunId,
) -> Result<BuiltIndex> {
    use crate::iceberg_catalog::IcebergCatalog;

    // 1-2. Snapshot anchor S + declaration + identity.
    let ice = IcebergCatalog::new(pool.clone());
    let inputs = resolve_build_inputs(&ice, pool, table, index_name).await?;
    let at = inputs.at;
    let s: i64 = at.0;

    // 3-5. Cold Parquet + hot inline rows, extracted to (VectorKey, vector).
    let all_rows = collect_vectors(
        catalog,
        &ice,
        table,
        at,
        &inputs.column,
        &inputs.identity_col,
    )
    .await?;
    let row_count = all_rows.len() as i64;

    // 6. Infer dim from the first row; an empty table falls back to the
    //    declared vector(N) (schema fetched only on this arm, as before).
    let dim: u32 = match all_rows.first() {
        Some((_, v)) => v.len() as u32,
        None => declared_dim(&ice.schema(table, at).await?, &inputs.column),
    };

    // 7. Build the chosen index via the core spec routing. The `VectorIndex`
    //    trait is `Send`, so the box may be held across `.await` points.
    let index: Box<dyn control_plane_core::VectorIndex> =
        inputs.spec.build(dim, inputs.metric, all_rows)?;
    // dim may have been inferred as 0 for empty tables; prefer index's own dim.
    let dim = if index.dim() > 0 { index.dim() } else { dim };

    // 8-9. Puffin sidecar (object-store write BEFORE the Postgres tx).
    let puffin_path = write_sidecar(
        catalog,
        table,
        index.as_ref(),
        s,
        &inputs.column,
        &inputs.identity_col,
    )
    .await?;

    // 10. One Postgres tx: binding row + lineage event.
    let lineage = build_lineage_event(run_id, table, &inputs.column, s, row_count, &puffin_path);
    bind_index_and_emit(
        pool,
        table,
        IndexBinding {
            index_name: index_name.to_string(),
            column: inputs.column.clone(),
            covered_snapshot: s,
            metric: inputs.metric.as_str().to_string(),
            index_kind: index.index_kind().as_str().to_string(),
            dim: dim as i32,
            row_count,
            puffin_path: puffin_path.clone(),
        },
        &lineage,
    )
    .await?;

    Ok(BuiltIndex {
        covered_snapshot: s,
        puffin_path,
        row_count,
    })
}
