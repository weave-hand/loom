use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{LengthConstraint, PropertyConstraints, RangeConstraint};
use ingest::gate::{ColumnShape, ModelShape, ViolationReason, validate, validate_values};

fn customer_shape() -> ModelShape {
    ModelShape {
        columns: vec![
            ColumnShape {
                name: "id".into(),
                ty: "long".into(),
                required: true,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
            ColumnShape {
                name: "email".into(),
                ty: "string".into(),
                required: true,
                constraints: control_plane_core::PropertyConstraints::default(),
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
        Field::new("id", DataType::Utf8, false), // wrong: string, model wants long
        Field::new("email", DataType::Utf8, true),
    ]);
    let v = validate(&customer_shape(), &schema).unwrap_err();
    assert!(
        v.iter()
            .any(|x| x.column == "id" && matches!(x.reason, ViolationReason::TypeMismatch { .. }))
    );
}

/// A `code String [^[A-Z]+$, len 2..=4]` + `score Double [0..=100]` shape.
fn constrained_shape() -> ModelShape {
    ModelShape {
        columns: vec![
            ColumnShape {
                name: "code".into(),
                ty: "string".into(),
                required: true,
                constraints: PropertyConstraints {
                    pattern: Some("^[A-Z]+$".into()),
                    length: Some(LengthConstraint {
                        min: Some(2),
                        max: Some(4),
                    }),
                    one_of: None,
                    range: None,
                },
            },
            ColumnShape {
                name: "score".into(),
                ty: "double".into(),
                required: false,
                constraints: PropertyConstraints {
                    range: Some(RangeConstraint {
                        min: Some(0.0),
                        max: Some(100.0),
                    }),
                    ..PropertyConstraints::default()
                },
            },
        ],
    }
}

fn batch(codes: Vec<Option<&str>>, scores: Vec<Option<f64>>) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("code", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
    ]);
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(StringArray::from(codes)),
            Arc::new(Float64Array::from(scores)),
        ],
    )
    .unwrap()
}

#[test]
fn conforming_values_pass() {
    let b = batch(vec![Some("AB"), Some("XYZ")], vec![Some(10.0), Some(99.5)]);
    assert!(validate_values(&constrained_shape(), &[b]).is_ok());
}

#[test]
fn pattern_violation_is_reported() {
    let b = batch(vec![Some("ab")], vec![Some(5.0)]);
    let v = validate_values(&constrained_shape(), &[b]).unwrap_err();
    assert!(v.iter().any(|x| x.column == "code"
        && matches!(&x.reason, ViolationReason::Constraint { rule } if rule == "pattern")));
}

#[test]
fn length_violation_is_reported() {
    let b = batch(vec![Some("A")], vec![None]); // too short
    let v = validate_values(&constrained_shape(), &[b]).unwrap_err();
    assert!(v.iter().any(|x| x.column == "code"
        && matches!(&x.reason, ViolationReason::Constraint { rule } if rule == "length")));
}

#[test]
fn range_violation_on_numeric_is_reported() {
    let b = batch(vec![Some("AB")], vec![Some(250.0)]); // above 100
    let v = validate_values(&constrained_shape(), &[b]).unwrap_err();
    assert!(v.iter().any(|x| x.column == "score"
        && matches!(&x.reason, ViolationReason::Constraint { rule } if rule == "range")));
}

#[test]
fn nulls_skip_value_validation() {
    // A null cell carries no value to validate (presence is a separate `required` rule).
    let b = batch(vec![None], vec![None]);
    assert!(validate_values(&constrained_shape(), &[b]).is_ok());
}

#[test]
fn unconstrained_shape_is_a_noop() {
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let b = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
    )
    .unwrap();
    assert!(validate_values(&customer_shape(), &[b]).is_ok());
}
