use arrow::datatypes::DataType;
use control_plane_core::ColumnSpec;
use datafusion_io::infer::{
    InferError, arrow_logical_type, logical_arrow_schema, logical_arrow_type,
};

fn col(name: &str, ty: &str, nullable: bool) -> ColumnSpec {
    ColumnSpec {
        name: name.into(),
        ty: ty.into(),
        nullable,
    }
}

#[test]
fn maps_supported_logical_names_to_arrow_types() {
    assert_eq!(logical_arrow_type("boolean"), Some(DataType::Boolean));
    assert_eq!(logical_arrow_type("integer"), Some(DataType::Int32));
    assert_eq!(logical_arrow_type("long"), Some(DataType::Int64));
    assert_eq!(logical_arrow_type("double"), Some(DataType::Float64));
    assert_eq!(logical_arrow_type("string"), Some(DataType::Utf8));
    assert_eq!(logical_arrow_type("date"), None);
}

#[test]
fn round_trips_against_arrow_logical_type() {
    // Every supported logical name maps to an Arrow type that maps back to the
    // same name — the inverse the empty-input path relies on.
    for name in ["boolean", "integer", "long", "double", "string"] {
        let dt = logical_arrow_type(name).unwrap();
        assert_eq!(arrow_logical_type(&dt), Some(name));
    }
}

#[test]
fn builds_schema_in_column_order_with_nullability() {
    let cols = vec![
        col("id", "long", false),
        col("name", "string", true),
        col("active", "boolean", false),
    ];
    let schema = logical_arrow_schema(&cols).unwrap();
    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert!(!schema.field(0).is_nullable());
    assert_eq!(schema.field(1).name(), "name");
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
    assert!(schema.field(1).is_nullable());
    assert_eq!(schema.field(2).name(), "active");
    assert_eq!(schema.field(2).data_type(), &DataType::Boolean);
    assert!(!schema.field(2).is_nullable());
}

#[test]
fn unsupported_logical_type_is_an_error_not_a_guess() {
    let cols = vec![col("when", "timestamp", true)];
    match logical_arrow_schema(&cols) {
        Err(InferError::UnsupportedLogical(t)) => assert_eq!(t, "timestamp"),
        other => panic!("expected UnsupportedLogical, got {other:?}"),
    }
}
