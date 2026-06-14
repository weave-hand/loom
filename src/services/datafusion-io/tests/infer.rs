use arrow::datatypes::{DataType, Field, Schema};
use datafusion_io::infer::{InferError, duck_type, infer_columns};

#[test]
fn maps_supported_arrow_types_to_ducklake_strings() {
    assert_eq!(duck_type(&DataType::Int64), Some("int64"));
    assert_eq!(duck_type(&DataType::Utf8), Some("varchar"));
    assert_eq!(duck_type(&DataType::LargeUtf8), Some("varchar"));
    assert_eq!(duck_type(&DataType::Boolean), Some("boolean"));
    assert_eq!(duck_type(&DataType::Float64), Some("float64"));
    assert_eq!(duck_type(&DataType::Int32), Some("int32"));
}

#[test]
fn infer_columns_carries_name_and_nullability_in_order() {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]);
    let cols = infer_columns(&schema).unwrap();
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0].name, "id");
    assert_eq!(cols[0].ty, "int64");
    assert!(!cols[0].nullable);
    assert_eq!(cols[1].name, "name");
    assert_eq!(cols[1].ty, "varchar");
    assert!(cols[1].nullable);
}

#[test]
fn unsupported_arrow_type_is_an_error_not_a_guess() {
    let schema = Schema::new(vec![Field::new("blob", DataType::Binary, false)]);
    match infer_columns(&schema) {
        Err(InferError::Unsupported(dt)) => assert_eq!(dt, DataType::Binary),
        other => panic!("expected Unsupported, got {other:?}"),
    }
}
