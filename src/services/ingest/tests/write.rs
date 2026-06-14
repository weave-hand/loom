use ingest::write::{IngestWriteConfig, estimate_partitions};

fn cfg(target: u64, max_files: usize) -> IngestWriteConfig {
    IngestWriteConfig {
        target_file_size_bytes: target,
        max_files,
        compression_factor: 0.3,
    }
}

#[test]
fn empty_input_is_one_partition() {
    assert_eq!(estimate_partitions(0, &cfg(1, 8)), 1);
}

#[test]
fn small_input_fits_one_file() {
    // 100 in-memory bytes * 0.3 = 30 est compressed; target 128 MiB -> 1 file.
    assert_eq!(estimate_partitions(100, &cfg(128 * 1024 * 1024, 8)), 1);
}

#[test]
fn large_input_splits_up_to_target() {
    // 1000 bytes * 0.3 = 300 est; target 100 -> ceil(300/100) = 3 files.
    assert_eq!(estimate_partitions(1000, &cfg(100, 8)), 3);
}

#[test]
fn partition_count_is_clamped_to_max_files() {
    // 1_000_000 * 0.3 = 300_000 est; target 1 -> 300_000, clamped to max_files = 4.
    assert_eq!(estimate_partitions(1_000_000, &cfg(1, 4)), 4);
}

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use ingest::write::{WrittenFile, file_stats_from_bytes};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

/// Write a 5-row file with `max_row_group_size = 2` so it has THREE row groups —
/// the case the old single-row-group-only code could not produce min/max for.
fn multi_row_group_bytes() -> (Arc<Schema>, Vec<u8>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![5, 3, 9, 1, 7])),
            Arc::new(StringArray::from(vec![
                Some("e"),
                None,
                Some("z"),
                Some("a"),
                Some("m"),
            ])),
        ],
    )
    .unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_row_count(Some(2))
        .build();
    let mut buf = Vec::new();
    let mut w = ArrowWriter::try_new(&mut buf, schema.clone(), Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    (schema, buf)
}

#[test]
fn stats_merge_min_max_across_row_groups() {
    let (schema, bytes) = multi_row_group_bytes();
    let stats: WrittenFile =
        file_stats_from_bytes("p/part-0.parquet".into(), &bytes, &schema).unwrap();

    assert_eq!(&bytes[bytes.len() - 4..], b"PAR1");
    assert_eq!(stats.path, "p/part-0.parquet");
    assert_eq!(stats.file_size_bytes, bytes.len() as i64);
    assert!(stats.footer_size > 0 && stats.footer_size < stats.file_size_bytes);
    assert_eq!(stats.record_count, 5);

    let id = &stats.column_stats[0];
    assert_eq!(id.column_name, "id");
    assert_eq!(id.null_count, 0);
    assert_eq!(id.value_count, 5);
    assert!(id.column_size_bytes > 0);
    // Merged across all 3 row groups — NOT just the first.
    assert_eq!(id.min.as_deref(), Some("1"));
    assert_eq!(id.max.as_deref(), Some("9"));

    let name = &stats.column_stats[1];
    assert_eq!(name.column_name, "name");
    assert_eq!(name.null_count, 1);
    assert_eq!(name.value_count, 4);
    assert_eq!(name.min.as_deref(), Some("a"));
    assert_eq!(name.max.as_deref(), Some("z"));
}
