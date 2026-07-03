use std::sync::Arc;

use arrow::array::{
    Float64Array, Int32Array, Int64Array, LargeStringArray, RecordBatch, StringArray,
};
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

/// A `count Integer [0..=10]` + `note String (LargeUtf8) [len 2..=4]` + `big Long
/// [0..=1000]` shape — characterization coverage for the Int32/LargeUtf8/Int64 arms of
/// `validate_values`'s per-array-type dispatch (added ahead of the `AsArray` rewrite;
/// these were the arms not otherwise exercised by `constrained_shape`, which only covers
/// Utf8 and Float64).
fn wide_constrained_shape() -> ModelShape {
    ModelShape {
        columns: vec![
            ColumnShape {
                name: "count".into(),
                ty: "integer".into(),
                required: false,
                constraints: PropertyConstraints {
                    range: Some(RangeConstraint {
                        min: Some(0.0),
                        max: Some(10.0),
                    }),
                    ..PropertyConstraints::default()
                },
            },
            ColumnShape {
                name: "note".into(),
                ty: "string".into(),
                required: false,
                constraints: PropertyConstraints {
                    length: Some(LengthConstraint {
                        min: Some(2),
                        max: Some(4),
                    }),
                    ..PropertyConstraints::default()
                },
            },
            ColumnShape {
                name: "big".into(),
                ty: "long".into(),
                required: false,
                constraints: PropertyConstraints {
                    range: Some(RangeConstraint {
                        min: Some(0.0),
                        max: Some(1000.0),
                    }),
                    ..PropertyConstraints::default()
                },
            },
        ],
    }
}

fn wide_batch(
    counts: Vec<Option<i32>>,
    notes: Vec<Option<&str>>,
    bigs: Vec<Option<i64>>,
) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("count", DataType::Int32, true),
        Field::new("note", DataType::LargeUtf8, true),
        Field::new("big", DataType::Int64, true),
    ]);
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int32Array::from(counts)),
            Arc::new(LargeStringArray::from(notes)),
            Arc::new(Int64Array::from(bigs)),
        ],
    )
    .unwrap()
}

#[test]
fn int32_range_violation_is_reported() {
    let b = wide_batch(vec![Some(50)], vec![Some("ok")], vec![Some(5)]); // count above 10
    let v = validate_values(&wide_constrained_shape(), &[b]).unwrap_err();
    assert!(v.iter().any(|x| x.column == "count"
        && matches!(&x.reason, ViolationReason::Constraint { rule } if rule == "range")));
}

#[test]
fn large_utf8_length_violation_is_reported() {
    let b = wide_batch(vec![Some(1)], vec![Some("x")], vec![Some(5)]); // note too short
    let v = validate_values(&wide_constrained_shape(), &[b]).unwrap_err();
    assert!(v.iter().any(|x| x.column == "note"
        && matches!(&x.reason, ViolationReason::Constraint { rule } if rule == "length")));
}

#[test]
fn int64_range_violation_is_reported() {
    let b = wide_batch(vec![Some(1)], vec![Some("ok")], vec![Some(5000)]); // big above 1000
    let v = validate_values(&wide_constrained_shape(), &[b]).unwrap_err();
    assert!(v.iter().any(|x| x.column == "big"
        && matches!(&x.reason, ViolationReason::Constraint { rule } if rule == "range")));
}

#[test]
fn wide_shape_nulls_skip_value_validation() {
    // Nulls on the Int32/LargeUtf8/Int64 columns carry no value to validate, matching
    // the existing `nulls_skip_value_validation` coverage for Utf8/Float64.
    let b = wide_batch(vec![None], vec![None], vec![None]);
    assert!(validate_values(&wide_constrained_shape(), &[b]).is_ok());
}
