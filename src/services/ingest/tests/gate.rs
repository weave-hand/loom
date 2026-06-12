use arrow::datatypes::{DataType, Field, Schema};
use ingest::gate::{ColumnShape, ModelShape, ViolationReason, validate};

fn customer_shape() -> ModelShape {
    ModelShape {
        columns: vec![
            ColumnShape {
                name: "id".into(),
                ty: "int64".into(),
                required: true,
            },
            ColumnShape {
                name: "email".into(),
                ty: "varchar".into(),
                required: true,
            },
        ],
    }
}

#[test]
fn conforming_batch_passes() {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("email", DataType::Utf8, true),
    ]);
    assert!(validate(&customer_shape(), &schema).is_ok());
}

#[test]
fn missing_required_column_is_a_violation() {
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let v = validate(&customer_shape(), &schema).unwrap_err();
    assert!(
        v.iter()
            .any(|x| x.column == "email" && matches!(x.reason, ViolationReason::MissingRequired))
    );
}

#[test]
fn type_mismatch_is_a_violation() {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Utf8, false), // wrong: varchar, model wants int64
        Field::new("email", DataType::Utf8, true),
    ]);
    let v = validate(&customer_shape(), &schema).unwrap_err();
    assert!(
        v.iter()
            .any(|x| x.column == "id" && matches!(x.reason, ViolationReason::TypeMismatch { .. }))
    );
}
