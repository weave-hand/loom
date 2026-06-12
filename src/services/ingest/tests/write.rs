use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use ingest::write::write_parquet;

fn sample() -> (Arc<Schema>, RecordBatch) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
        ],
    )
    .unwrap();
    (schema, batch)
}

#[test]
fn writes_parquet_and_extracts_load_bearing_stats() {
    let (schema, batch) = sample();
    let w = write_parquet(schema, &[batch]).unwrap();

    assert_eq!(&w.bytes[w.bytes.len() - 4..], b"PAR1");
    assert_eq!(w.file_size_bytes, w.bytes.len() as i64);
    assert!(w.footer_size > 0 && w.footer_size < w.file_size_bytes);
    assert_eq!(w.record_count, 3);

    assert_eq!(w.column_stats.len(), 2);
    let id = &w.column_stats[0];
    assert_eq!(id.column_name, "id");
    assert_eq!(id.null_count, 0);
    assert_eq!(id.value_count, 3);
    assert!(id.column_size_bytes > 0);
    assert_eq!(id.min.as_deref(), Some("1"));
    assert_eq!(id.max.as_deref(), Some("3"));

    let name = &w.column_stats[1];
    assert_eq!(name.column_name, "name");
    assert_eq!(name.null_count, 1);
    assert_eq!(name.value_count, 2);
    assert_eq!(name.min.as_deref(), Some("a"));
    assert_eq!(name.max.as_deref(), Some("c"));
}
