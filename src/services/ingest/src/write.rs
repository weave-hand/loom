//! Arrow batches -> Snappy Parquet bytes + the DuckLake `DataFile` stats the
//! snapshot-commit primitive needs. The load-bearing fidelity unit: the DuckDB
//! read-back interop test (tests/ducklake_interop.rs) is its executable oracle.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use bytes::Bytes;
use control_plane_core::ColumnStat;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::statistics::Statistics;

#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    #[error("parquet write failed: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
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
