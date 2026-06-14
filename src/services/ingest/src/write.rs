//! Arrow batches -> Snappy Parquet bytes + the DuckLake `DataFile` stats the
//! snapshot-commit primitive needs. The load-bearing fidelity unit: the DuckDB
//! read-back interop test (tests/ducklake_interop.rs) is its executable oracle.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use bytes::Bytes;
use control_plane_core::ColumnStat;
use datafusion::common::config::TableParquetOptions;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::Partitioning;
// object_store is unified to DataFusion's bundled 0.13, so the top-level crate and
// DataFusion's re-export are the same types; register/list/get all agree on one version.
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use futures::TryStreamExt;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::statistics::Statistics;

/// Tunables for the DataFusion ingest write. Defaults target ~128 MiB Snappy files.
#[derive(Clone, Debug)]
pub struct IngestWriteConfig {
    /// Desired size of each output Parquet file, in bytes.
    pub target_file_size_bytes: u64,
    /// Hard upper bound on the number of output files (= partitions).
    pub max_files: usize,
    /// In-memory Arrow bytes are larger than compressed Parquet; this factor maps
    /// estimated in-memory size to estimated on-disk size.
    pub compression_factor: f64,
}

impl Default for IngestWriteConfig {
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
pub fn estimate_partitions(in_memory_bytes: u64, cfg: &IngestWriteConfig) -> usize {
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
}

/// A written Parquet file plus the metadata `append_files` registers.
#[derive(Clone, Debug)]
pub struct WrittenParquet {
    pub bytes: Vec<u8>,
    pub record_count: i64,
    pub file_size_bytes: i64,
    pub footer_size: i64,
    pub column_stats: Vec<ColumnStat>,
}

/// Write `batches` as one Snappy Parquet file and extract its DuckLake stats.
pub fn write_parquet(
    schema: Arc<Schema>,
    batches: &[RecordBatch],
) -> Result<WrittenParquet, WriteError> {
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut buf: Vec<u8> = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), Some(props))?;
    for b in batches {
        writer.write(b)?;
    }
    writer.close()?;

    let file_size_bytes = buf.len() as i64;
    let footer_size = parquet_footer_size(&buf);

    let reader = SerializedFileReader::new(Bytes::from(buf.clone()))?;
    let meta = reader.metadata();
    let record_count: i64 = meta.file_metadata().num_rows();
    // best-effort: min/max are only emitted for single-row-group files (merging typed
    // ranges across row groups is not worth it here; absent min/max just disables pruning).
    let single_rg = meta.num_row_groups() == 1;

    let mut column_stats = Vec::with_capacity(schema.fields().len());
    for (i, field) in schema.fields().iter().enumerate() {
        let mut null_count: i64 = 0;
        let mut column_size_bytes: i64 = 0;
        let mut min: Option<String> = None;
        let mut max: Option<String> = None;
        for rg in meta.row_groups() {
            let col = rg.column(i);
            // ArrowWriter always emits column statistics, so null_count/value_count below are
            // faithful. If a column chunk ever lacked stats, null_count would default to 0 and
            // value_count to record_count (asserting "no nulls"), which could mislead DuckDB
            // pruning — guard the assumption rather than silently understate nulls.
            debug_assert!(
                col.statistics().is_some(),
                "expected ArrowWriter to emit column statistics"
            );
            column_size_bytes += col.compressed_size();
            if let Some(stats) = col.statistics() {
                null_count += stats.null_count_opt().unwrap_or(0) as i64;
                if single_rg {
                    min = stat_min_string(stats);
                    max = stat_max_string(stats);
                }
            }
        }
        column_stats.push(ColumnStat {
            column_name: field.name().clone(),
            min,
            max,
            null_count,
            value_count: record_count - null_count,
            column_size_bytes,
        });
    }

    Ok(WrittenParquet {
        bytes: buf,
        record_count,
        file_size_bytes,
        footer_size,
        column_stats,
    })
}

/// The loom object-store URL DataFusion writes through. The authority is arbitrary;
/// it only keys the registered store.
const LOOM_STORE_URL: &str = "loom://data";

/// Write `batches` as N size-targeted Snappy Parquet files directly into `store`,
/// under `dir_prefix` (e.g. "main/customer/<file_prefix>"). Returns one `WrittenFile`
/// per output file, with paths relative to the table directory.
///
/// Note: DataFusion's RoundRobinBatch distributes whole batches, so the file count is
/// bounded by min(estimated partitions, batch count). A single huge batch yields one file.
pub async fn write_dataset(
    store: Arc<dyn ObjectStore>,
    dir_prefix: &str,
    schema: Arc<Schema>,
    batches: &[RecordBatch],
    cfg: &IngestWriteConfig,
) -> Result<Vec<WrittenFile>, WriteError> {
    let in_memory: u64 = batches
        .iter()
        .map(|b| b.get_array_memory_size() as u64)
        .sum();
    let partitions = estimate_partitions(in_memory, cfg);

    let ctx = SessionContext::new();
    let url = ObjectStoreUrl::parse(LOOM_STORE_URL)?;
    ctx.register_object_store(url.as_ref(), store.clone());

    let provider = MemTable::try_new(schema.clone(), vec![batches.to_vec()])?;
    let df = ctx
        .read_table(Arc::new(provider))?
        .repartition(Partitioning::RoundRobinBatch(partitions))?;

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
    /// (e.g. "<file_prefix>/part-0.parquet"). DuckLake resolves it under data_path.
    pub path: String,
    pub record_count: i64,
    pub file_size_bytes: i64,
    pub footer_size: i64,
    pub column_stats: Vec<ColumnStat>,
}

/// A typed min/max bound, so we compare numerically (not lexically) when folding
/// across row groups, then stringify once at the end.
#[derive(Clone)]
enum Bound {
    Bool(bool),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Str(String),
}

impl Bound {
    fn to_stat_string(&self) -> String {
        match self {
            Bound::Bool(v) => v.to_string(),
            Bound::I32(v) => v.to_string(),
            Bound::I64(v) => v.to_string(),
            Bound::F32(v) => v.to_string(),
            Bound::F64(v) => v.to_string(),
            Bound::Str(v) => v.clone(),
        }
    }
    fn partial_cmp(&self, other: &Bound) -> Option<std::cmp::Ordering> {
        use Bound::*;
        match (self, other) {
            (Bool(a), Bool(b)) => a.partial_cmp(b),
            (I32(a), I32(b)) => a.partial_cmp(b),
            (I64(a), I64(b)) => a.partial_cmp(b),
            (F32(a), F32(b)) => a.partial_cmp(b),
            (F64(a), F64(b)) => a.partial_cmp(b),
            (Str(a), Str(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

fn min_bound(stats: &Statistics) -> Option<Bound> {
    match stats {
        Statistics::Boolean(s) => s.min_opt().map(|v| Bound::Bool(*v)),
        Statistics::Int32(s) => s.min_opt().map(|v| Bound::I32(*v)),
        Statistics::Int64(s) => s.min_opt().map(|v| Bound::I64(*v)),
        Statistics::Float(s) => s.min_opt().map(|v| Bound::F32(*v)),
        Statistics::Double(s) => s.min_opt().map(|v| Bound::F64(*v)),
        Statistics::ByteArray(s) => s
            .min_opt()
            .and_then(|v| v.as_utf8().ok().map(|s| Bound::Str(s.to_string()))),
        _ => None,
    }
}

fn max_bound(stats: &Statistics) -> Option<Bound> {
    match stats {
        Statistics::Boolean(s) => s.max_opt().map(|v| Bound::Bool(*v)),
        Statistics::Int32(s) => s.max_opt().map(|v| Bound::I32(*v)),
        Statistics::Int64(s) => s.max_opt().map(|v| Bound::I64(*v)),
        Statistics::Float(s) => s.max_opt().map(|v| Bound::F32(*v)),
        Statistics::Double(s) => s.max_opt().map(|v| Bound::F64(*v)),
        Statistics::ByteArray(s) => s
            .max_opt()
            .and_then(|v| v.as_utf8().ok().map(|s| Bound::Str(s.to_string()))),
        _ => None,
    }
}

/// Extract the DuckLake `DataFile` stats from a complete Parquet byte buffer,
/// merging typed min/max across ALL row groups (preserves pruning on multi-row-group
/// files). `path` is the relative DataFile path to record.
pub fn file_stats_from_bytes(
    path: String,
    bytes: &[u8],
    schema: &Schema,
) -> Result<WrittenFile, WriteError> {
    let file_size_bytes = bytes.len() as i64;
    let footer_size = parquet_footer_size(bytes);

    let reader = SerializedFileReader::new(Bytes::from(bytes.to_vec()))?;
    let meta = reader.metadata();
    let record_count: i64 = meta.file_metadata().num_rows();

    let mut column_stats = Vec::with_capacity(schema.fields().len());
    for (i, field) in schema.fields().iter().enumerate() {
        let mut null_count: i64 = 0;
        let mut column_size_bytes: i64 = 0;
        let mut min: Option<Bound> = None;
        let mut max: Option<Bound> = None;
        for rg in meta.row_groups() {
            let col = rg.column(i);
            debug_assert!(
                col.statistics().is_some(),
                "expected ArrowWriter to emit column statistics"
            );
            column_size_bytes += col.compressed_size();
            if let Some(stats) = col.statistics() {
                null_count += stats.null_count_opt().unwrap_or(0) as i64;
                if let Some(b) = min_bound(stats) {
                    min = match min {
                        Some(cur) if cur.partial_cmp(&b) != Some(std::cmp::Ordering::Greater) => {
                            Some(cur)
                        }
                        _ => Some(b),
                    };
                }
                if let Some(b) = max_bound(stats) {
                    max = match max {
                        Some(cur) if cur.partial_cmp(&b) != Some(std::cmp::Ordering::Less) => {
                            Some(cur)
                        }
                        _ => Some(b),
                    };
                }
            }
        }
        column_stats.push(ColumnStat {
            column_name: field.name().clone(),
            min: min.map(|b| b.to_stat_string()),
            max: max.map(|b| b.to_stat_string()),
            null_count,
            value_count: record_count - null_count,
            column_size_bytes,
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
/// length DuckLake records as `footer_size`.
fn parquet_footer_size(bytes: &[u8]) -> i64 {
    assert!(
        bytes.len() >= 8 && &bytes[bytes.len() - 4..] == b"PAR1",
        "not a well-formed parquet buffer (len {}, missing PAR1 magic)",
        bytes.len()
    );
    let len = &bytes[bytes.len() - 8..bytes.len() - 4];
    u32::from_le_bytes(len.try_into().unwrap()) as i64
}

fn stat_min_string(stats: &Statistics) -> Option<String> {
    match stats {
        Statistics::Boolean(s) => s.min_opt().map(|v| v.to_string()),
        Statistics::Int32(s) => s.min_opt().map(|v| v.to_string()),
        Statistics::Int64(s) => s.min_opt().map(|v| v.to_string()),
        Statistics::Float(s) => s.min_opt().map(|v| v.to_string()),
        Statistics::Double(s) => s.min_opt().map(|v| v.to_string()),
        Statistics::ByteArray(s) => s
            .min_opt()
            .and_then(|v| v.as_utf8().ok().map(|s| s.to_string())),
        _ => None,
    }
}

fn stat_max_string(stats: &Statistics) -> Option<String> {
    match stats {
        Statistics::Boolean(s) => s.max_opt().map(|v| v.to_string()),
        Statistics::Int32(s) => s.max_opt().map(|v| v.to_string()),
        Statistics::Int64(s) => s.max_opt().map(|v| v.to_string()),
        Statistics::Float(s) => s.max_opt().map(|v| v.to_string()),
        Statistics::Double(s) => s.max_opt().map(|v| v.to_string()),
        Statistics::ByteArray(s) => s
            .max_opt()
            .and_then(|v| v.as_utf8().ok().map(|s| s.to_string())),
        _ => None,
    }
}
