//! The `iceberg_mirror.vector_index` binding (row type + insert/lookup) and the
//! build primitive (Task 6). loom records `(table, column, covered_snapshot) ->
//! puffin_path` as a mirror row in lieu of a REST catalog.

use std::sync::Arc;

use arrow_array::{Float32Array, Int32Array, Int64Array, ListArray, RecordBatch, StringArray};
use control_plane_core::{
    Catalog, ControlPlaneError, DatasetRef, EventType, FlatIndex, LineageEvent, Metric, Result,
    RunId, SnapshotId, TableRef, VectorKey,
};
use iceberg::{Catalog as IceCatalog, TableIdent};
use sqlx::{AssertSqlSafe, PgConnection, PgPool, Row};
use time::OffsetDateTime;

fn backend<E: std::fmt::Display>(e: E) -> ControlPlaneError {
    ControlPlaneError::Backend(e.to_string().into())
}

/// A bound vector index: the metadata loom needs to find and decode the sidecar.
#[derive(Clone, Debug)]
pub struct VectorIndexRow {
    pub table_id: i64,
    pub column: String,
    pub covered_snapshot: i64,
    pub metric: String,
    pub index_kind: String,
    pub dim: i32,
    pub row_count: i64,
    pub puffin_path: String,
}

// SQL-STYLE (env-forced runtime, see plan "Decisions"): this cloud session cannot
// regenerate the .sqlx cache (Postgres won't boot as root; libxml2 egress is
// policy-blocked), so the new vector_index queries use RUNTIME
// `sqlx::query(AssertSqlSafe(...))` + bind params instead of compile-time
// `query!`/`query_scalar!`. The SQL is a fixed literal (no interpolation — every
// value is a bound `$n` param), so AssertSqlSafe carries no injection risk. This
// mirrors the runtime pattern already used in `iceberg_inline.rs`/`fixture.rs`.
// (Promotable to compile-time `query!` in a follow-up when a Postgres-capable env
// is available — tracked as a FUTURE item.)

/// Insert a `vector_index` binding row in the caller's transaction.
pub async fn insert_vector_index(tx: &mut PgConnection, row: &VectorIndexRow) -> Result<()> {
    sqlx::query(AssertSqlSafe(
        "insert into iceberg_mirror.vector_index \
         (table_id, column_name, covered_snapshot, metric, index_kind, dim, row_count, puffin_path) \
         values ($1, $2, $3, $4, $5, $6, $7, $8)",
    ))
    .bind(row.table_id)
    .bind(&row.column)
    .bind(row.covered_snapshot)
    .bind(&row.metric)
    .bind(&row.index_kind)
    .bind(row.dim)
    .bind(row.row_count)
    .bind(&row.puffin_path)
    .execute(&mut *tx)
    .await
    .map_err(backend)?;
    Ok(())
}

/// The newest bound index for `(table_id, column)` with `covered_snapshot <= at`,
/// or `None` if none is bound.
pub async fn lookup_vector_index(
    pool: &PgPool,
    table_id: i64,
    column: &str,
    at: i64,
) -> Result<Option<VectorIndexRow>> {
    let row = sqlx::query(AssertSqlSafe(
        "select table_id, column_name, covered_snapshot, metric, index_kind, dim, \
                row_count, puffin_path \
         from iceberg_mirror.vector_index \
         where table_id = $1 and column_name = $2 and covered_snapshot <= $3 \
         order by covered_snapshot desc limit 1",
    ))
    .bind(table_id)
    .bind(column)
    .bind(at)
    .fetch_optional(pool)
    .await
    .map_err(backend)?;
    row.map(|r| {
        Ok(VectorIndexRow {
            table_id: r.try_get("table_id").map_err(backend)?,
            column: r.try_get("column_name").map_err(backend)?,
            covered_snapshot: r.try_get("covered_snapshot").map_err(backend)?,
            metric: r.try_get("metric").map_err(backend)?,
            index_kind: r.try_get("index_kind").map_err(backend)?,
            dim: r.try_get("dim").map_err(backend)?,
            row_count: r.try_get("row_count").map_err(backend)?,
            puffin_path: r.try_get("puffin_path").map_err(backend)?,
        })
    })
    .transpose()
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

/// Resolve the ontology's declared `identity` column for `(table.schema, table.name)`.
///
/// Returns an error if the type row is absent or the identity field is `NULL`
/// (the build requires a declared identity to populate `VectorKey`).
async fn identity_column_for(pool: &PgPool, table: &TableRef) -> Result<String> {
    let row = sqlx::query(AssertSqlSafe(
        "select identity from ontology.object_type \
         where table_schema = $1 and table_name = $2"
            .to_string(),
    ))
    .bind(&table.schema)
    .bind(&table.name)
    .fetch_optional(pool)
    .await
    .map_err(backend)?;
    let id: Option<String> = match row {
        Some(r) => r.try_get("identity").map_err(backend)?,
        None => None,
    };
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
    let vec_idx = batch
        .schema()
        .index_of(vector_col)
        .map_err(|e| ControlPlaneError::Backend(e.to_string().into()))?;
    let id_idx = batch
        .schema()
        .index_of(identity_col)
        .map_err(|e| ControlPlaneError::Backend(e.to_string().into()))?;

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

/// Read the live inline rows for `table` that were born AFTER `born_after` and
/// are still alive at snapshot `at`, returning only the `identity_col` and
/// `vector_col` columns. Returns `None` if the inline table does not exist or
/// has no matching rows.
///
/// Used by Task 7/8 (hot-delta path) to fetch the rows appended between S and Q
/// so the serving layer can score them alongside the cold Puffin index.
pub async fn inline_delta_batch(
    pool: &PgPool,
    table: &TableRef,
    born_after: i64,
    at: i64,
) -> Result<Option<RecordBatch>> {
    use crate::iceberg_inline::inline_table_name;
    use crate::iceberg_mirror::live_table_id;

    let mut conn = pool.acquire().await.map_err(backend)?;
    let Some(tid) = live_table_id(&mut conn, &table.schema, &table.name).await? else {
        return Ok(None);
    };

    // Check the inline table exists.
    let exists: Option<String> = sqlx::query_scalar(AssertSqlSafe(format!(
        "select to_regclass('{}')::text",
        inline_table_name(tid)
    )))
    .fetch_one(&mut *conn)
    .await
    .map_err(backend)?;
    if exists.is_none() {
        return Ok(None);
    }

    // Resolve identity + vector column names from the ontology.
    let identity_col = identity_column_for(pool, table).await?;
    let vector_col = {
        // Find which column of this table's mirror schema is a vector type.
        // We look for a column whose iceberg_type starts with "vector(" in the
        // current mirror snapshot (at SnapshotId(at)).
        let ice = crate::iceberg_catalog::IcebergCatalog::new(pool.clone());
        use control_plane_core::Catalog;
        let schema = ice.schema(table, SnapshotId(at)).await?;
        schema
            .columns
            .into_iter()
            .find(|c| c.ty.starts_with("vector("))
            .map(|c| c.name)
            .ok_or_else(|| {
                ControlPlaneError::Backend(
                    format!(
                        "no vector column in schema for {}.{}",
                        table.schema, table.name
                    )
                    .into(),
                )
            })?
    };

    // Runtime query: select only the identity + vector columns with the delta MVCC predicate.
    let id_quoted = format!("\"{}\"", identity_col.replace('"', "\"\""));
    let vec_quoted = format!("\"{}\"", vector_col.replace('"', "\"\""));
    let rows = sqlx::query(AssertSqlSafe(format!(
        "select {id_quoted}, {vec_quoted} \
         from {} \
         where begin_snapshot > {born_after} \
           and begin_snapshot <= {at} \
           and (end_snapshot is null or end_snapshot > {at}) \
         order by loom_row_id",
        inline_table_name(tid),
    )))
    .fetch_all(&mut *conn)
    .await
    .map_err(backend)?;

    if rows.is_empty() {
        return Ok(None);
    }

    // Build Arrow arrays: identity (Int64) + vector (List<Float32>).
    use arrow_array::builder::{Float32Builder, Int64Builder, ListBuilder};
    use arrow_schema::{DataType, Field, Schema};

    let mut id_builder = Int64Builder::new();
    let item_field = Arc::new(Field::new("item", DataType::Float32, false));
    let mut vec_builder = ListBuilder::new(Float32Builder::new()).with_field(item_field.clone());

    for r in &rows {
        // Identity
        let id_val: i64 = r.try_get(0).map_err(backend)?;
        id_builder.append_value(id_val);

        // Vector: stored as jsonb array of floats in the inline table.
        // The vector column is stored as a jsonb array in the inline table.
        let json_val: serde_json::Value = r.try_get(1).map_err(backend)?;
        let floats: Vec<f32> = json_val
            .as_array()
            .ok_or_else(|| {
                ControlPlaneError::Backend("inline vector column is not a JSON array".into())
            })?
            .iter()
            .map(|v| {
                v.as_f64()
                    .ok_or_else(|| {
                        ControlPlaneError::Backend("inline vector element is not a float".into())
                    })
                    .map(|f| f as f32)
            })
            .collect::<Result<Vec<_>>>()?;
        vec_builder.values().append_slice(&floats);
        vec_builder.append(true);
    }

    let id_array = Arc::new(id_builder.finish());
    let vec_array = Arc::new(vec_builder.finish());

    let schema = Arc::new(Schema::new(vec![
        Field::new(&identity_col, DataType::Int64, false),
        Field::new(&vector_col, DataType::List(item_field), false),
    ]));
    let batch = RecordBatch::try_new(schema, vec![id_array, vec_array])
        .map_err(|e| ControlPlaneError::Backend(e.to_string().into()))?;
    Ok(Some(batch))
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
#[allow(
    clippy::too_many_arguments,
    reason = "build_vector_index needs catalog, pool, table, column, metric and run_id — \
              no sensible grouping; mirrors land/append_parquet_snapshot pattern"
)]
pub async fn build_vector_index(
    catalog: &crate::iceberg_sql_catalog::SqlCatalog,
    pool: &PgPool,
    table: &TableRef,
    column: &str,
    metric: Metric,
    run_id: RunId,
) -> Result<BuiltIndex> {
    use crate::iceberg_catalog::IcebergCatalog;
    use crate::iceberg_mirror::live_table_id;
    use crate::lineage::pg_emit;
    use crate::puffin::write_flat_index;
    use crate::read_files_as_batches;

    // 1. Snapshot S: the catalog snapshot we build as-of (MVCC anchor).
    let ice = IcebergCatalog::new(pool.clone());
    let s: i64 = ice.current_snapshot(table).await?.id.0;
    let at = SnapshotId(s);

    // 2. Resolve identity column from the ontology.
    let identity_col = identity_column_for(pool, table).await?;

    // 3. Cold data: Parquet files live at S.
    let files = ice.files_with_stats(table, at).await?;
    let paths: Vec<String> = files.iter().map(|f| f.path.clone()).collect();
    let (_, cold_batches) = read_files_as_batches(catalog, table, &paths).await?;

    // 4. Hot data: live inline rows at S.
    let hot_batch_opt = ice.inline_live_batch(table, at).await?;
    let hot_batch: Option<RecordBatch> = hot_batch_opt.map(|(_, _, b)| b);

    // 5. Extract (VectorKey, Vec<f32>) rows from all batches.
    let mut all_rows: Vec<(VectorKey, Vec<f32>)> = Vec::new();
    for batch in &cold_batches {
        let rows = extract_rows(batch, column, &identity_col)?;
        all_rows.extend(rows);
    }
    if let Some(ref batch) = hot_batch {
        let rows = extract_rows(batch, column, &identity_col)?;
        all_rows.extend(rows);
    }

    let row_count = all_rows.len() as i64;

    // 6. Infer dim from the first row (or from the schema).
    let dim: u32 = if let Some((_, v)) = all_rows.first() {
        v.len() as u32
    } else {
        // No rows: look up the declared dim from the column spec.
        let schema = ice.schema(table, at).await?;
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
    };

    // 7. Build the FlatIndex.
    let index = FlatIndex::build(dim, metric, all_rows)?;

    // 8. Resolve the Iceberg field id for the vector column (informational).
    let ident = TableIdent::from_strs([table.schema.as_str(), table.name.as_str()])
        .map_err(|e| ControlPlaneError::Backend(e.to_string().into()))?;
    let tbl = catalog
        .load_table(&ident)
        .await
        .map_err(|e| ControlPlaneError::Backend(e.to_string().into()))?;
    // Use the Schema::field_id_by_name accessor (available on the iceberg-rust
    // pinned main commit). Falls back to 0 if the accessor returns None (e.g.
    // if the Iceberg schema uses a different field name than expected — purely
    // informational for Puffin footer decode in slice 1).
    let field_id: i32 = tbl
        .metadata()
        .current_schema()
        .field_id_by_name(column)
        .unwrap_or(0);

    // 9. Write the Puffin sidecar (object-store write BEFORE the Postgres tx).
    let puffin_path = format!(
        "{}/metadata/loom-vector-index-{}.puffin",
        tbl.metadata().location(),
        uuid::Uuid::new_v4()
    );
    let file_io = tbl.file_io().clone();
    write_flat_index(
        &file_io,
        &puffin_path,
        &index,
        s,
        field_id,
        column,
        &identity_col,
    )
    .await?;

    // 10. One Postgres tx: insert vector_index mirror row + lineage event.
    let lineage = LineageEvent {
        run_id,
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![DatasetRef {
            namespace: table.schema.clone(),
            name: table.name.clone(),
        }],
        outputs: vec![DatasetRef {
            namespace: "loom-vector-index".to_string(),
            name: puffin_path.clone(),
        }],
        payload: serde_json::json!({
            "column": column,
            "covered_snapshot": s,
            "row_count": row_count,
        }),
    };

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
            column: column.to_string(),
            covered_snapshot: s,
            metric: metric.as_str().to_string(),
            index_kind: control_plane_core::IndexKind::Flat.as_str().to_string(),
            dim: dim as i32,
            row_count,
            puffin_path: puffin_path.clone(),
        },
    )
    .await?;

    pg_emit(conn, &lineage).await?;
    tx.commit().await.map_err(backend)?;

    Ok(BuiltIndex {
        covered_snapshot: s,
        puffin_path,
        row_count,
    })
}
