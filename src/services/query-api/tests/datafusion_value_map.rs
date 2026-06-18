//! arrow_to_sqlvalue / batches_to_rows: Arrow result -> engine-neutral Rows.
//! `rust_test` integration target (pure logic, no DB).

use std::sync::Arc;

use arrow::array::{
    BooleanArray, Date32Array, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use query_api::serving::{Rows, SqlValue};
use query_api::serving_datafusion::batches_to_rows;

#[test]
fn maps_each_scalar_type_and_nulls() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("i", DataType::Int64, true),
        Field::new("f", DataType::Float64, true),
        Field::new("b", DataType::Boolean, true),
        Field::new("s", DataType::Utf8, true),
        Field::new("d", DataType::Date32, true),
        Field::new("t", DataType::Timestamp(TimeUnit::Microsecond, None), true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(7), None])),
            Arc::new(Float64Array::from(vec![Some(1.5), None])),
            Arc::new(BooleanArray::from(vec![Some(true), None])),
            Arc::new(StringArray::from(vec![Some("hi"), None])),
            // 1970-01-02 = 1 day after epoch.
            Arc::new(Date32Array::from(vec![Some(1), None])),
            // 1970-01-01T00:00:01 = 1_000_000 microseconds.
            Arc::new(TimestampMicrosecondArray::from(vec![Some(1_000_000), None])),
        ],
    )
    .unwrap();

    let rows: Rows = batches_to_rows(vec![batch]);

    assert_eq!(rows.columns, vec!["i", "f", "b", "s", "d", "t"]);
    assert_eq!(rows.rows.len(), 2);
    assert_eq!(
        rows.rows[0],
        vec![
            SqlValue::Int(7),
            SqlValue::Double(1.5),
            SqlValue::Bool(true),
            SqlValue::Text("hi".into()),
            SqlValue::Date(time::macros::date!(1970 - 01 - 02)),
            SqlValue::Timestamp(time::macros::datetime!(1970 - 01 - 01 00:00:01)),
        ]
    );
    assert!(
        rows.rows[1].iter().all(|v| *v == SqlValue::Null),
        "row 2 is all nulls"
    );
}

#[test]
fn empty_batches_yield_no_rows() {
    let rows = batches_to_rows(vec![]);
    assert!(rows.columns.is_empty());
    assert!(rows.rows.is_empty());
}
