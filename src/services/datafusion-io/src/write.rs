//! Arrow batches -> Snappy Parquet bytes + the `DataFile` stats the snapshot-commit
//! primitive needs. The load-bearing fidelity unit: the Parquet write/read-back tests
//! (tests/write.rs, tests/single_file_write.rs) are its executable oracle.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use bytes::Bytes;
use control_plane_core::{ColumnStat, DataFile, FileFormat, StatValue};
use datafusion::common::config::TableParquetOptions;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::datasource::MemTable;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::execution::object_store::ObjectStoreUrl;
// object_store is unified to DataFusion's bundled 0.13, so the top-level crate and
// DataFusion's re-export are the same types; register/list/get all agree on one version.
use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::statistics::Statistics;

/// Tunables for the DataFusion write. Defaults target ~128 MiB Snappy files.
#[derive(Clone, Debug)]
pub struct WriteConfig {
    /// Desired size of each output Parquet file, in bytes.
    pub target_file_size_bytes: u64,
    /// Hard upper bound on the number of output files (= partitions).
    pub max_files: usize,
    /// In-memory Arrow bytes are larger than compressed Parquet; this factor maps
    /// estimated in-memory size to estimated on-disk size.
    pub compression_factor: f64,
}

impl Default for WriteConfig {
    fn default() -> Self {
        Self {
            target_file_size_bytes: 128 * 1024 * 1024,
            max_files: 64,
            compression_factor: 0.3,
        }
    }
}

/// Map total in-memory Arrow size to a partition (file) count: estimate compressed
/// size, divide by the target file size, clamp to `[1, max_files]`. Pure — no I/O.
pub fn estimate_partitions(in_memory_bytes: u64, cfg: &WriteConfig) -> usize {
    let est_compressed = (in_memory_bytes as f64 * cfg.compression_factor).ceil() as u64;
    let target = cfg.target_file_size_bytes.max(1);
    let n = est_compressed.div_ceil(target) as usize;
    n.clamp(1, cfg.max_files.max(1))
}

#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    #[error("parquet write failed: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("datafusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),
    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),
    #[error("not a valid parquet buffer (len {0}, missing PAR1 magic or too short)")]
    NotParquet(usize),
}

/// The loom object-store URL DataFusion writes through. The authority is arbitrary;
/// it only keys the registered store.
pub(crate) const LOOM_STORE_URL: &str = "loom://data";

/// Write `batches` as N size-targeted Snappy Parquet files directly into `store`,
/// under `dir_prefix` (e.g. "main/customer/<file_prefix>"). Returns one `WrittenFile`
/// per output file, with paths relative to the table directory.
///
/// File count is governed by `minimum_parallel_output_files = estimate_partitions`: small
/// data lands as a single file, while large data splits toward the size target across that
/// many writers (see the in-body comment). Empty input (no batches, or batches with no rows)
/// yields zero files — the caller's `append_files(&[])` then registers an empty, row-less
/// snapshot rather than an empty file.
pub async fn write_dataset(
    store: Arc<dyn ObjectStore>,
    dir_prefix: &str,
    schema: Arc<Schema>,
    batches: &[RecordBatch],
    cfg: &WriteConfig,
) -> Result<Vec<WrittenFile>, WriteError> {
    let in_memory: u64 = batches
        .iter()
        .map(|b| b.get_array_memory_size() as u64)
        .sum();
    let partitions = estimate_partitions(in_memory, cfg);

    // Produce EXACTLY `partitions` files.
    //
    // File count here is NOT controlled by the DataFrame's partitioning: DataFusion's
    // `DataSinkExec` declares `required_input_distribution = SinglePartition`, so the
    // optimizer coalesces every partition into one stream and the parquet sink then
    // *dynamically* re-splits it at write time. That dynamic split is governed solely by
    // `minimum_parallel_output_files` (the sink opens up to this many files, round-robining
    // batches across them) and `soft_max_rows_per_output_file`. The old code left this at
    // its default of 4, so the output file count tracked the upstream batch count — a join
    // that emitted 2 batches silently produced 2 files even though `estimate_partitions`
    // asked for 1. That historically mis-read a multi-file table under a pushed-down `LIMIT`
    // (the DuckDB-era scan reconstructed `id` values incorrectly, e.g. 10 -> 266), corrupting
    // reads — so a stray split was never cosmetic. Pinning the sink's file count to exactly
    // `partitions` (and leaving the high soft row cap) makes small results a single file
    // while still letting size-targeted large results split. See tests/single_file_write.rs.
    let mut config = SessionConfig::new();
    config.options_mut().execution.minimum_parallel_output_files = partitions;
    let ctx = SessionContext::new_with_config(config);
    let url = ObjectStoreUrl::parse(LOOM_STORE_URL)?;
    ctx.register_object_store(url.as_ref(), store.clone());

    let provider = MemTable::try_new(schema.clone(), vec![batches.to_vec()])?;
    let df = ctx.read_table(Arc::new(provider))?;

    let mut parquet_opts = TableParquetOptions::default();
    parquet_opts.global.compression = Some("snappy".to_string());

    let write_path = format!("{LOOM_STORE_URL}/{dir_prefix}/");
    df.write_parquet(
        &write_path,
        DataFrameWriteOptions::new(),
        Some(parquet_opts),
    )
    .await?;

    // Discover the written files by listing the prefix in the store.
    let list_prefix = ObjectPath::from(dir_prefix);
    let mut objects: Vec<_> = store
        .list(Some(&list_prefix))
        .try_collect::<Vec<_>>()
        .await?;
    objects.sort_by(|a, b| a.location.cmp(&b.location));

    // Strip the "<schema>/<table>/" prefix so the recorded path is relative to the
    // table dir. dir_prefix is "<schema>/<table>/<file_prefix>"; keep "<file_prefix>/...".
    let table_dir = dir_prefix.rsplit_once('/').map(|(p, _)| p).unwrap_or("");
    let strip = if table_dir.is_empty() {
        String::new()
    } else {
        format!("{table_dir}/")
    };

    let mut files = Vec::with_capacity(objects.len());
    for obj in objects {
        let key = obj.location.as_ref();
        if !key.ends_with(".parquet") {
            continue;
        }
        let rel = key.strip_prefix(&strip).unwrap_or(key).to_string();
        let bytes = store.get(&obj.location).await?.bytes().await?;
        files.push(file_stats_from_bytes(rel, &bytes, &schema)?);
    }
    Ok(files)
}

/// A written Parquet file plus the metadata `append_files` registers. One per output file.
#[derive(Clone, Debug)]
pub struct WrittenFile {
    /// Path to register in `append_files`, relative to the table directory
    /// (e.g. "<file_prefix>/part-0.parquet"). The catalog resolves it under data_path.
    pub path: String,
    pub record_count: i64,
    pub file_size_bytes: i64,
    pub footer_size: i64,
    pub column_stats: Vec<ColumnStat>,
}

/// Promote the table-relative `WrittenFile`s from [`write_dataset`] into absolute
/// [`DataFile`]s for a snapshot commit. `write_dataset` returns each file's path
/// relative to the table directory (e.g. `"<run_id>/part-0.parquet"`); the snapshot
/// mirror — and the DataFusion serving path that reads it (`IcebergMirrorTableProvider`,
/// which resolves `iceberg_mirror.data_file.path` as an ABSOLUTE URL) — needs the full
/// `{root_url}/{schema}/{table}/{rel}` path with `path_is_relative = false`.
///
/// This is the single source of truth for that promotion, shared by the transform
/// (`run.rs`), Iceberg compaction (`compact.rs`), and matching the worker compaction
/// path (`worker/src/compact.rs`) — so a relative path can never leak into the mirror
/// and silently make a transform-derived dataset unreadable through serving.
pub fn absolute_data_files(
    written: Vec<WrittenFile>,
    root_url: &str,
    schema: &str,
    table: &str,
) -> Vec<DataFile> {
    written
        .into_iter()
        .map(|w| DataFile {
            path: format!("{root_url}/{schema}/{table}/{}", w.path),
            path_is_relative: false,
            file_format: FileFormat::Parquet,
            record_count: w.record_count,
            file_size_bytes: w.file_size_bytes,
            column_stats: w.column_stats,
            parquet_footer_size: Some(w.footer_size),
        })
        .collect()
}

fn min_stat(stats: &Statistics) -> Option<StatValue> {
    match stats {
        Statistics::Boolean(s) => s.min_opt().map(|v| StatValue::Bool(*v)),
        Statistics::Int32(s) => s.min_opt().map(|v| StatValue::I32(*v)),
        Statistics::Int64(s) => s.min_opt().map(|v| StatValue::I64(*v)),
        Statistics::Float(s) => s.min_opt().map(|v| StatValue::F32(*v)),
        Statistics::Double(s) => s.min_opt().map(|v| StatValue::F64(*v)),
        Statistics::ByteArray(s) => s
            .min_opt()
            .and_then(|v| v.as_utf8().ok().map(|s| StatValue::Str(s.to_string()))),
        _ => None,
    }
}

fn max_stat(stats: &Statistics) -> Option<StatValue> {
    match stats {
        Statistics::Boolean(s) => s.max_opt().map(|v| StatValue::Bool(*v)),
        Statistics::Int32(s) => s.max_opt().map(|v| StatValue::I32(*v)),
        Statistics::Int64(s) => s.max_opt().map(|v| StatValue::I64(*v)),
        Statistics::Float(s) => s.max_opt().map(|v| StatValue::F32(*v)),
        Statistics::Double(s) => s.max_opt().map(|v| StatValue::F64(*v)),
        Statistics::ByteArray(s) => s
            .max_opt()
            .and_then(|v| v.as_utf8().ok().map(|s| StatValue::Str(s.to_string()))),
        _ => None,
    }
}

fn stat_partial_cmp(a: &StatValue, b: &StatValue) -> Option<std::cmp::Ordering> {
    use StatValue::*;
    match (a, b) {
        (Bool(x), Bool(y)) => x.partial_cmp(y),
        (I32(x), I32(y)) => x.partial_cmp(y),
        (I64(x), I64(y)) => x.partial_cmp(y),
        (F32(x), F32(y)) => x.partial_cmp(y),
        (F64(x), F64(y)) => x.partial_cmp(y),
        (Str(x), Str(y)) => x.partial_cmp(y),
        _ => None,
    }
}

/// Extract the `DataFile` stats from a complete Parquet byte buffer,
/// merging typed min/max across ALL row groups (preserves pruning on multi-row-group
/// files). `path` is the relative DataFile path to record.
pub fn file_stats_from_bytes(
    path: String,
    bytes: &[u8],
    schema: &Schema,
) -> Result<WrittenFile, WriteError> {
    let file_size_bytes = bytes.len() as i64;
    let footer_size = parquet_footer_size(bytes).ok_or(WriteError::NotParquet(bytes.len()))?;

    let reader = SerializedFileReader::new(Bytes::from(bytes.to_vec()))?;
    let meta = reader.metadata();
    let record_count: i64 = meta.file_metadata().num_rows();

    let mut column_stats = Vec::with_capacity(schema.fields().len());
    for (i, field) in schema.fields().iter().enumerate() {
        let mut null_count: i64 = 0;
        let mut column_size_bytes: i64 = 0;
        let mut min: Option<StatValue> = None;
        let mut max: Option<StatValue> = None;
        for rg in meta.row_groups() {
            let col = rg.column(i);
            debug_assert!(
                col.statistics().is_some(),
                "expected ArrowWriter to emit column statistics"
            );
            column_size_bytes += col.compressed_size();
            if let Some(stats) = col.statistics() {
                null_count += stats.null_count_opt().unwrap_or(0) as i64;
                if let Some(b) = min_stat(stats) {
                    min = match min {
                        Some(cur)
                            if stat_partial_cmp(&cur, &b) != Some(std::cmp::Ordering::Greater) =>
                        {
                            Some(cur)
                        }
                        _ => Some(b),
                    };
                }
                if let Some(b) = max_stat(stats) {
                    max = match max {
                        Some(cur)
                            if stat_partial_cmp(&cur, &b) != Some(std::cmp::Ordering::Less) =>
                        {
                            Some(cur)
                        }
                        _ => Some(b),
                    };
                }
            }
        }
        column_stats.push(ColumnStat {
            column_name: field.name().clone(),
            null_count,
            column_size_bytes,
            min,
            max,
        });
    }

    Ok(WrittenFile {
        path,
        record_count,
        file_size_bytes,
        footer_size,
        column_stats,
    })
}

/// The 4 bytes before the trailing `PAR1` magic are the little-endian footer
/// length recorded as `footer_size`. Returns `None` when the buffer is not a
/// well-formed Parquet file (callers propagate as a `WriteError`).
fn parquet_footer_size(bytes: &[u8]) -> Option<i64> {
    let n = bytes.len();
    if n < 8 || bytes.get(n - 4..) != Some(b"PAR1") {
        return None;
    }
    let footer_bytes: [u8; 4] = bytes
        .get(n - 8..n - 4)?
        .try_into()
        .ok()?;
    Some(u32::from_le_bytes(footer_bytes) as i64)
}
