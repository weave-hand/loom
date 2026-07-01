//! The optional model-conformance gate ("this data is this model"). Takes a plain
//! `ModelShape` value — NOT the ontology — so this gate stays ontology-free; the
//! `model` module (`model_shape_from_type`) derives a `ModelShape` from an
//! `ObjectType` for the `POST /models/{type}` path. This ships the seam plus a
//! minimal check (required columns present, types match). Richer constraints
//! (ranges, regex, coercion) extend `ViolationReason`.
//!
//! Note: this validates schema STRUCTURE (column presence + type), not runtime
//! values — a column declared non-nullable can still carry nulls in the batch;
//! value/null-constraint enforcement is a later slice.

use arrow::array::{
    Array, Float64Array, Int32Array, Int64Array, LargeStringArray, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Schema};

use control_plane_core::{PropertyConstraints, PropertyValidator};
use datafusion_io::arrow_logical_type;

/// One expected column of a model. `ty` is a loom logical type string. `constraints`
/// (default empty) carries the property's declared per-value rules for value validation.
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnShape {
    pub name: String,
    pub ty: String,
    pub required: bool,
    pub constraints: PropertyConstraints,
}

/// The physical shape a batch must satisfy to be "this model".
#[derive(Clone, Debug, PartialEq)]
pub struct ModelShape {
    pub columns: Vec<ColumnShape>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Violation {
    pub column: String,
    pub reason: ViolationReason,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ViolationReason {
    MissingRequired,
    /// The column is present but its inferred logical type does not match the model.
    TypeMismatch {
        expected: String,
        found: String,
    },
    /// The column has an Arrow type loom cannot land at all.
    Unsupported,
    /// A present value violates the property's declared constraint. `rule` is the failed
    /// rule's stable token (`range`|`length`|`pattern`|`one_of`).
    Constraint {
        rule: String,
    },
}

/// Validate a batch schema against a model. `Ok(())` if every required column is
/// present and every present model column's inferred type matches. Returns ALL
/// violations (not just the first) so callers can report them together.
pub fn validate(shape: &ModelShape, batch: &Schema) -> Result<(), Vec<Violation>> {
    let mut violations = Vec::new();
    for col in &shape.columns {
        match batch.fields().iter().find(|f| f.name() == &col.name) {
            None => {
                if col.required {
                    violations.push(Violation {
                        column: col.name.clone(),
                        reason: ViolationReason::MissingRequired,
                    });
                }
            }
            Some(field) => match arrow_logical_type(field.data_type()) {
                None => violations.push(Violation {
                    column: col.name.clone(),
                    reason: ViolationReason::Unsupported,
                }),
                Some(found) if found != col.ty => violations.push(Violation {
                    column: col.name.clone(),
                    reason: ViolationReason::TypeMismatch {
                        expected: col.ty.clone(),
                        found: found.to_string(),
                    },
                }),
                Some(_) => {}
            },
        }
    }
    if violations.is_empty() {
        Ok(())
    } else {
        Err(violations)
    }
}

/// Validate the runtime VALUES of `batches` against each column's declared constraints
/// (run AFTER the schema-shape [`validate`]). Column-wise: a string column drives
/// `check_str`, a numeric column drives `check_num`; nulls and unconstrained columns are
/// skipped. Returns ALL violations (one per offending cell/rule) so callers report them
/// together. The same `core` validator drives the query-api action path.
pub fn validate_values(shape: &ModelShape, batches: &[RecordBatch]) -> Result<(), Vec<Violation>> {
    let mut violations = Vec::new();
    for col in &shape.columns {
        if col.constraints.is_empty() {
            continue;
        }
        // define-time validated; a bad regex cannot reach here, so skip defensively.
        let Ok(validator) = PropertyValidator::from_parts(&col.name, &col.constraints) else {
            continue;
        };
        if validator.is_noop() {
            continue;
        }
        for batch in batches {
            let Ok(idx) = batch.schema().index_of(&col.name) else {
                // absent optional column — already gated by `validate`.
                continue;
            };
            let array = batch.column(idx);
            let mut cv = Vec::new();
            match array.data_type() {
                DataType::Utf8 => {
                    if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
                        for i in 0..a.len() {
                            if !a.is_null(i) {
                                validator.check_str(a.value(i), &mut cv);
                            }
                        }
                    }
                }
                DataType::LargeUtf8 => {
                    if let Some(a) = array.as_any().downcast_ref::<LargeStringArray>() {
                        for i in 0..a.len() {
                            if !a.is_null(i) {
                                validator.check_str(a.value(i), &mut cv);
                            }
                        }
                    }
                }
                DataType::Int32 => {
                    if let Some(a) = array.as_any().downcast_ref::<Int32Array>() {
                        for i in 0..a.len() {
                            if !a.is_null(i) {
                                validator.check_num(f64::from(a.value(i)), &mut cv);
                            }
                        }
                    }
                }
                DataType::Int64 => {
                    if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
                        for i in 0..a.len() {
                            if !a.is_null(i) {
                                #[expect(
                                    clippy::cast_precision_loss,
                                    reason = "i64->f64 acceptable for range validation"
                                )]
                                let v = a.value(i) as f64;
                                validator.check_num(v, &mut cv);
                            }
                        }
                    }
                }
                DataType::Float64 => {
                    if let Some(a) = array.as_any().downcast_ref::<Float64Array>() {
                        for i in 0..a.len() {
                            if !a.is_null(i) {
                                validator.check_num(a.value(i), &mut cv);
                            }
                        }
                    }
                }
                _ => {}
            }
            for v in cv {
                violations.push(Violation {
                    column: v.property,
                    reason: ViolationReason::Constraint {
                        rule: v.rule.as_str().to_string(),
                    },
                });
            }
        }
    }
    if violations.is_empty() {
        Ok(())
    } else {
        Err(violations)
    }
}
