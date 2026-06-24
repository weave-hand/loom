//! Unit: column_stats_from_parquet merges typed min/max across row groups, and the
//! StatValue<->text codec round-trips per iceberg type. parquet/arrow 58.

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::snapshot::StatValue;
use control_plane_postgres::iceberg_stats::{
    column_stats_from_parquet, stat_from_text, stat_to_text,
};
use parquet::arrow::ArrowWriter;
use std::sync::Arc;

#[test]
fn merges_min_max_and_null_counts() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![3i64, 7, 1])),
            Arc::new(StringArray::from(vec![Some("b"), None, Some("a")])),
        ],
    )
    .unwrap();
    let mut buf = Vec::new();
    {
        let mut w = ArrowWriter::try_new(&mut buf, schema.clone(), None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }
    let names = vec!["id".to_string(), "name".to_string()];
    let stats = column_stats_from_parquet(bytes::Bytes::from(buf), &names).unwrap();
    let id = stats.iter().find(|s| s.column_name == "id").unwrap();
    assert_eq!(id.min, Some(StatValue::I64(1)));
    assert_eq!(id.max, Some(StatValue::I64(7)));
    assert_eq!(id.null_count, 0);
    let name = stats.iter().find(|s| s.column_name == "name").unwrap();
    assert_eq!(name.min, Some(StatValue::Str("a".into())));
    assert_eq!(name.max, Some(StatValue::Str("b".into())));
    assert_eq!(name.null_count, 1);
}

#[test]
fn codec_round_trips_clean_types_and_drops_others() {
    assert_eq!(stat_to_text(&StatValue::I64(42)), "42");
    assert_eq!(stat_from_text("42", "long"), Some(StatValue::I64(42)));
    assert_eq!(stat_from_text("7", "int"), Some(StatValue::I32(7)));
    assert_eq!(
        stat_from_text("a", "string"),
        Some(StatValue::Str("a".into()))
    );
    assert_eq!(
        stat_from_text("true", "boolean"),
        Some(StatValue::Bool(true))
    );
    // date/timestamp/other -> no bound (never pruned on)
    assert_eq!(stat_from_text("19000", "date"), None);
    assert_eq!(stat_from_text("123", "timestamp"), None);
}
