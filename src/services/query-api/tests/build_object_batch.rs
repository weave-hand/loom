//! build_object_batch: a one-row Arrow batch + schema + ColumnSpec list from an
//! aligned (columns, values, logical_types) triple. Pure logic (no DB).

use arrow::array::{Array, Int64Array, StringArray};
use query_api::serving::{SqlValue, build_object_batch};

#[test]
fn builds_one_row_batch_with_typed_null_and_specs() {
    let (schema, batch, specs) = build_object_batch(
        &["id".to_string(), "name".to_string()],
        &[SqlValue::Int(7), SqlValue::Null],
        &["Long".to_string(), "String".to_string()],
    )
    .expect("builds");

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 2);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(1).name(), "name");
    // ColumnSpec.ty is the loom LOGICAL canonical name (not the physical type).
    assert_eq!(
        specs.iter().map(|s| s.ty.as_str()).collect::<Vec<_>>(),
        vec!["long", "string"]
    );
    assert!(
        specs.iter().all(|s| s.nullable),
        "action columns are nullable"
    );

    let id = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(id.value(0), 7);
    let name = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(name.is_null(0), "unset property is a typed null");
}

#[test]
fn rejects_unknown_logical_type() {
    let err = build_object_batch(
        &["x".to_string()],
        &[SqlValue::Int(1)],
        &["Bogus".to_string()],
    )
    .unwrap_err();
    assert!(
        format!("{err}").contains("unknown logical type"),
        "got {err}"
    );
}

#[test]
fn rejects_value_type_mismatch() {
    // A Long column handed a Text value is a fault, not a silent coercion.
    let err = build_object_batch(
        &["id".to_string()],
        &[SqlValue::Text("nope".into())],
        &["Long".to_string()],
    )
    .unwrap_err();
    assert!(format!("{err}").contains("does not match"), "got {err}");
}

#[test]
fn rejects_length_mismatch() {
    let err = build_object_batch(&["x".to_string()], &[], &["Long".to_string()]).unwrap_err();
    assert!(format!("{err}").contains("columns"), "got {err}");
}
