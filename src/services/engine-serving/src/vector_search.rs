//! Cold/hot k-NN merge seam for the engine serving layer.
//!
//! `vector_search` looks up the bound Puffin flat index (cold path), reads the
//! inline delta (hot path — now live for vector tables: inline rows born after
//! the index's covered snapshot S and alive at query snapshot Q are scored and
//! merged with the cold Puffin results), merges via `merge_topk`, and returns a
//! 2-column `RecordBatch` (identity + `_distance`).

use std::sync::Arc;

use arrow::array::{Float32Array, Int64Array, ListArray, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{Metric, TableRef, VectorKey, distance};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_mirror::live_table_id;
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use control_plane_postgres::puffin::read_vector_index;
use control_plane_postgres::vector_index::{inline_delta_batch, lookup_vector_index};
use iceberg::{Catalog as IceCatalog, TableIdent};
use sqlx::PgPool;

use crate::serving::{EngineServingError, to_serving};

/// Pure merge combiner: concatenate `cold` and `hot`, stable-sort ascending by
/// distance, take at most `k` results. The single combiner `vector_search` uses;
/// with an empty `hot` (this slice) it returns the cold top-k unchanged.
pub fn merge_topk(
    cold: Vec<(VectorKey, f32)>,
    hot: Vec<(VectorKey, f32)>,
    k: usize,
) -> Vec<(VectorKey, f32)> {
    let mut combined: Vec<(VectorKey, f32)> = cold.into_iter().chain(hot).collect();
    combined.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    combined.truncate(k);
    combined
}

/// Exact k-NN search over the table's bound vector index, merging cold (Puffin)
/// and hot (inline delta) results.
///
/// Returns a 2-column `RecordBatch`:
/// - column 0: identity (`Int64` or `Utf8`, matching the index's identity kind)
/// - column 1: `_distance` (`Float32`), ascending
///
/// Errors with `EngineServingError::NoIndex` when no index has been built for
/// `(table, column)` at the current snapshot.
pub async fn vector_search(
    catalog: &SqlCatalog,
    pool: &PgPool,
    table: &TableRef,
    index_name: &str,
    query: &[f32],
    k: usize,
) -> Result<RecordBatch, EngineServingError> {
    use control_plane_core::Catalog;

    // 1. Snapshot Q: MVCC anchor for the search.
    let ice = IcebergCatalog::new(pool.clone());
    let q: i64 = ice.current_snapshot(table).await.map_err(to_serving)?.id.0;

    // 2. Resolve the mirror table_id.
    let mut conn = pool.acquire().await.map_err(to_serving)?;
    let table_id = live_table_id(&mut conn, &table.schema, &table.name)
        .await
        .map_err(to_serving)?
        .ok_or_else(|| {
            EngineServingError::Engine(format!(
                "no live mirror table for {}.{}",
                table.schema, table.name
            ))
        })?;
    drop(conn);

    // 3. Look up the bound index (NoIndex error if none).
    let row = lookup_vector_index(pool, table_id, index_name, q)
        .await
        .map_err(to_serving)?
        .ok_or_else(|| {
            EngineServingError::NoIndex(format!(
                "no vector index `{}` on {}.{} at snapshot {}",
                index_name, table.schema, table.name, q
            ))
        })?;
    let column: &str = &row.column;

    // 4. Cold path: read the Puffin vector index (polymorphic: Flat or IVF) and search it.
    let ident =
        TableIdent::from_strs([table.schema.as_str(), table.name.as_str()]).map_err(to_serving)?;
    let tbl = catalog.load_table(&ident).await.map_err(to_serving)?;
    let file_io = tbl.file_io().clone();
    let idx = read_vector_index(&file_io, &row.puffin_path)
        .await
        .map_err(to_serving)?;
    let cold: Vec<(VectorKey, f32)> = idx.search(query, k);

    // 5. Hot path: `inline_delta_batch` returns the inline vector rows born after
    //    the index's covered snapshot S and alive at Q; they are brute-force scored
    //    and merged with the cold results.
    let metric: Metric = row.metric.parse().map_err(to_serving)?;
    let hot: Vec<(VectorKey, f32)> = match inline_delta_batch(pool, table, row.covered_snapshot, q)
        .await
        .map_err(to_serving)?
    {
        None => vec![],
        Some(batch) => score_inline_batch(&batch, query, metric, column)?,
    };

    // 6. Merge cold + hot, ascending distance, top-k.
    let merged = merge_topk(cold, hot, k);

    // 7. Build the output RecordBatch.
    build_result_batch(merged)
}

/// Brute-force score the inline delta batch, returning (VectorKey, distance) pairs.
/// Mirrors `extract_rows` from the build path to stay score-identical.
fn score_inline_batch(
    batch: &RecordBatch,
    query: &[f32],
    metric: Metric,
    vector_col: &str,
) -> Result<Vec<(VectorKey, f32)>, EngineServingError> {
    let schema = batch.schema();
    let id_idx = 0;
    let vec_idx = schema.index_of(vector_col).map_err(|e| {
        EngineServingError::Engine(format!("vector column '{vector_col}' not in schema: {e}"))
    })?;

    let id_col = batch.column(id_idx);
    let vec_col = batch.column(vec_idx);

    let list_arr = vec_col
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| {
            EngineServingError::Engine(format!("vector column '{vector_col}' is not a List array"))
        })?;

    let n = batch.num_rows();
    let mut out = Vec::with_capacity(n);

    for row in 0..n {
        let key = if let Some(i64arr) = id_col.as_any().downcast_ref::<Int64Array>() {
            VectorKey::Int(i64arr.value(row))
        } else if let Some(sarr) = id_col.as_any().downcast_ref::<StringArray>() {
            VectorKey::Str(sarr.value(row).to_string())
        } else {
            return Err(EngineServingError::Engine(format!(
                "identity column has unsupported arrow type {:?}",
                id_col.data_type()
            )));
        };

        let elem_arr = list_arr.value(row);
        let f32arr = elem_arr
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| {
                EngineServingError::Engine(format!(
                    "vector column '{vector_col}' child is not Float32 at row {row}"
                ))
            })?;
        let vec_slice: Vec<f32> = f32arr.values().to_vec();
        let d = distance(metric, query, &vec_slice);
        out.push((key, d));
    }
    Ok(out)
}

/// Build the 2-column result `RecordBatch` from merged (VectorKey, f32) pairs.
/// Column 0: identity (`Int64` if all `Int`; `Utf8` if all `Str`; empty → `Int64`).
/// Column 1: `_distance` (`Float32`).
fn build_result_batch(merged: Vec<(VectorKey, f32)>) -> Result<RecordBatch, EngineServingError> {
    let distances: Vec<f32> = merged.iter().map(|(_, d)| *d).collect();
    let dist_array: Arc<Float32Array> = Arc::new(Float32Array::from(distances));

    let all_str = !merged.is_empty() && merged.iter().all(|(k, _)| matches!(k, VectorKey::Str(_)));

    let (id_field, id_array): (Field, Arc<dyn arrow::array::Array>) = if all_str {
        let values: Vec<&str> = merged
            .iter()
            .map(|(k, _)| match k {
                VectorKey::Str(s) => s.as_str(),
                VectorKey::Int(_) => "",
            })
            .collect();
        (
            Field::new("id", DataType::Utf8, false),
            Arc::new(StringArray::from(values)),
        )
    } else {
        let values: Vec<i64> = merged
            .iter()
            .map(|(k, _)| match k {
                VectorKey::Int(i) => *i,
                VectorKey::Str(_) => 0,
            })
            .collect();
        (
            Field::new("id", DataType::Int64, false),
            Arc::new(Int64Array::from(values)),
        )
    };

    let schema = Arc::new(Schema::new(vec![
        id_field,
        Field::new("_distance", DataType::Float32, false),
    ]));

    RecordBatch::try_new(schema, vec![id_array, dist_array]).map_err(to_serving)
}
