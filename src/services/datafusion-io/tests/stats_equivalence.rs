//! Cross-crate: the Iceberg mirror reader and the DataFusion write-path reader now
//! share one implementation, so they must agree on every `ColumnStat` field for the
//! same Parquet buffer. This is the test that would have caught the two hand-copied
//! merges drifting apart.

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_postgres::iceberg_stats::column_stats_from_parquet;
use datafusion_io::write::file_stats_from_bytes;
use parquet::arrow::ArrowWriter;

#[test]
fn both_readers_agree_on_the_same_parquet_buffer() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
    ]));
    let first = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![5i64, 9])),
            Arc::new(StringArray::from(vec![Some("m"), None])),
            Arc::new(Float64Array::from(vec![Some(2.5f64), Some(4.5)])),
        ],
    )
    .unwrap();
    let second = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![2i64, 7])),
            Arc::new(StringArray::from(vec![Some("a"), Some("c")])),
            Arc::new(Float64Array::from(vec![None, Some(-1.5f64)])),
        ],
    )
    .unwrap();

    let mut buf = Vec::new();
    {
        let mut w = ArrowWriter::try_new(&mut buf, Arc::clone(&schema), None).unwrap();
        w.write(&first).unwrap();
        w.flush().unwrap(); // close row group 1
        w.write(&second).unwrap();
        w.close().unwrap();
    }

    let names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let from_mirror = column_stats_from_parquet(buf.clone().into(), &names).unwrap();
    let from_write = file_stats_from_bytes("p/part-0.parquet".to_string(), &buf, &schema)
        .unwrap()
        .column_stats;

    assert_eq!(from_mirror, from_write);
    // …and the shared result is the merged one, not a single row group's.
    assert_eq!(from_mirror.len(), 3);
    assert_eq!(from_mirror[0].column_name, "id");
    assert_eq!(from_mirror[1].null_count, 1);
    assert_eq!(from_mirror[2].null_count, 1);
    assert!(from_mirror[0].column_size_bytes > 0);
}
