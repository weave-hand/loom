//! Unit: column_stats merges typed min/max, null counts and compressed sizes across
//! ALL row groups of a real Parquet buffer. parquet/arrow 58.

use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryArray, Float32Array, Float64Array,
    Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use control_plane_core::snapshot::StatValue;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet_stats::column_stats;

/// Write each batch as its own row group (`flush` closes the open group) and return
/// the finished Parquet buffer.
fn write_groups(
    schema: &Arc<Schema>,
    batches: &[RecordBatch],
    props: Option<WriterProperties>,
) -> Bytes {
    let mut buf = Vec::new();
    {
        let mut w = ArrowWriter::try_new(&mut buf, Arc::clone(schema), props).unwrap();
        for b in batches {
            w.write(b).unwrap();
            w.flush().unwrap();
        }
        w.close().unwrap();
    }
    Bytes::from(buf)
}

fn batch(schema: &Arc<Schema>, cols: Vec<ArrayRef>) -> RecordBatch {
    RecordBatch::try_new(Arc::clone(schema), cols).unwrap()
}

fn names(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| (*s).to_string()).collect()
}

#[test]
fn folds_min_max_across_row_groups() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("i", DataType::Int64, false),
        Field::new("s", DataType::Utf8, false),
    ]));
    let bytes = write_groups(
        &schema,
        &[
            batch(
                &schema,
                vec![
                    Arc::new(Int64Array::from(vec![5i64, 9])),
                    Arc::new(StringArray::from(vec!["m", "z"])),
                ],
            ),
            batch(
                &schema,
                vec![
                    Arc::new(Int64Array::from(vec![2i64, 7])),
                    Arc::new(StringArray::from(vec!["a", "c"])),
                ],
            ),
        ],
        None,
    );
    let reader = SerializedFileReader::new(bytes).unwrap();
    assert_eq!(reader.metadata().num_row_groups(), 2);

    let stats = column_stats(reader.metadata(), &names(&["i", "s"]));
    assert_eq!(stats.len(), 2);
    assert_eq!(stats[0].column_name, "i");
    assert_eq!(stats[0].min, Some(StatValue::I64(2)));
    assert_eq!(stats[0].max, Some(StatValue::I64(9)));
    assert_eq!(stats[1].min, Some(StatValue::Str("a".to_string())));
    assert_eq!(stats[1].max, Some(StatValue::Str("z".to_string())));
}

#[test]
fn folds_bool_i32_f32_f64_bounds() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("b", DataType::Boolean, false),
        Field::new("i32", DataType::Int32, false),
        Field::new("f32", DataType::Float32, false),
        Field::new("f64", DataType::Float64, false),
    ]));
    let bytes = write_groups(
        &schema,
        &[
            batch(
                &schema,
                vec![
                    Arc::new(BooleanArray::from(vec![true, true])),
                    Arc::new(Int32Array::from(vec![10i32, 20])),
                    Arc::new(Float32Array::from(vec![2.5f32, 4.5])),
                    Arc::new(Float64Array::from(vec![2.5f64, 4.5])),
                ],
            ),
            batch(
                &schema,
                vec![
                    Arc::new(BooleanArray::from(vec![false, true])),
                    Arc::new(Int32Array::from(vec![-1i32, 3])),
                    Arc::new(Float32Array::from(vec![-1.5f32, 0.5])),
                    Arc::new(Float64Array::from(vec![-1.5f64, 0.5])),
                ],
            ),
        ],
        None,
    );
    let reader = SerializedFileReader::new(bytes).unwrap();
    let stats = column_stats(reader.metadata(), &names(&["b", "i32", "f32", "f64"]));

    assert_eq!(stats[0].min, Some(StatValue::Bool(false)));
    assert_eq!(stats[0].max, Some(StatValue::Bool(true)));
    assert_eq!(stats[1].min, Some(StatValue::I32(-1)));
    assert_eq!(stats[1].max, Some(StatValue::I32(20)));
    assert_eq!(stats[2].min, Some(StatValue::F32(-1.5)));
    assert_eq!(stats[2].max, Some(StatValue::F32(4.5)));
    assert_eq!(stats[3].min, Some(StatValue::F64(-1.5)));
    assert_eq!(stats[3].max, Some(StatValue::F64(4.5)));
}

#[test]
fn accumulates_null_counts_and_compressed_size_across_row_groups() {
    let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
    let bytes = write_groups(
        &schema,
        &[
            batch(
                &schema,
                vec![Arc::new(StringArray::from(vec![Some("a"), None, None]))],
            ),
            batch(
                &schema,
                vec![Arc::new(StringArray::from(vec![None, Some("b")]))],
            ),
        ],
        None,
    );
    let reader = SerializedFileReader::new(bytes).unwrap();
    let meta = reader.metadata();
    let expected_size: i64 = meta
        .row_groups()
        .iter()
        .map(|rg| rg.column(0).compressed_size())
        .sum();

    let stats = column_stats(meta, &names(&["s"]));
    assert_eq!(stats[0].null_count, 3);
    assert!(expected_size > 0);
    assert_eq!(stats[0].column_size_bytes, expected_size);
}

#[test]
fn column_without_statistics_has_no_bounds_but_keeps_size() {
    let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, false)]));
    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::None)
        .build();
    let bytes = write_groups(
        &schema,
        &[batch(
            &schema,
            vec![Arc::new(Int64Array::from(vec![1i64, 2]))],
        )],
        Some(props),
    );
    let reader = SerializedFileReader::new(bytes).unwrap();
    let stats = column_stats(reader.metadata(), &names(&["i"]));

    assert_eq!(stats[0].min, None);
    assert_eq!(stats[0].max, None);
    assert_eq!(stats[0].null_count, 0);
    assert!(stats[0].column_size_bytes > 0);
}

#[test]
fn unsupported_statistics_variant_yields_no_bounds() {
    // FixedSizeBinary -> parquet FIXED_LEN_BYTE_ARRAY -> Statistics::FixedLenByteArray,
    // which loom does not prune on. Binary -> ByteArray whose bytes are not UTF-8,
    // so the as_utf8 decode fails and the bound is dropped.
    let schema = Arc::new(Schema::new(vec![
        Field::new("fb", DataType::FixedSizeBinary(2), false),
        Field::new("bin", DataType::Binary, false),
    ]));
    let fb =
        FixedSizeBinaryArray::try_from_iter(vec![vec![1u8, 2], vec![3u8, 4]].into_iter()).unwrap();
    let bin = BinaryArray::from(vec![&[0xffu8, 0xfe][..], &[0xfdu8, 0xfc][..]]);
    let bytes = write_groups(
        &schema,
        &[batch(&schema, vec![Arc::new(fb), Arc::new(bin)])],
        None,
    );
    let reader = SerializedFileReader::new(bytes).unwrap();
    let stats = column_stats(reader.metadata(), &names(&["fb", "bin"]));

    assert_eq!(stats[0].min, None);
    assert_eq!(stats[0].max, None);
    assert_eq!(stats[1].min, None);
    assert_eq!(stats[1].max, None);
}

#[test]
fn column_names_drive_the_output_length_and_labels() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("i", DataType::Int64, false),
        Field::new("s", DataType::Utf8, false),
    ]));
    let bytes = write_groups(
        &schema,
        &[batch(
            &schema,
            vec![
                Arc::new(Int64Array::from(vec![1i64])),
                Arc::new(StringArray::from(vec!["a"])),
            ],
        )],
        None,
    );
    let reader = SerializedFileReader::new(bytes).unwrap();

    // Empty request -> empty result, file untouched.
    assert!(column_stats(reader.metadata(), &[]).is_empty());

    // A prefix of the columns is stats'd, under the caller's names.
    let stats = column_stats(reader.metadata(), &names(&["renamed"]));
    assert_eq!(stats.len(), 1);
    assert_eq!(stats[0].column_name, "renamed");
    assert_eq!(stats[0].min, Some(StatValue::I64(1)));
}
