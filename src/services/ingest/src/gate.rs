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

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::{DataType, Float64Type, Int32Type, Int64Type, Schema};

use control_plane_core::{PropertyConstraints, PropertyValidator, satisfies};
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
                // Case-insensitive logical-type match via core's `satisfies` (the
                // seam conform.rs/bind.rs already use), not raw string equality —
                // so a property declared `Long` accepts the landing map's lowercase
                // `long`. `satisfies` errs only when the DECLARED type is itself
                // unrecognized, which is a mismatch to report here too.
                Some(found) if !satisfies(&col.ty, found).unwrap_or(false) => {
                    violations.push(Violation {
                        column: col.name.clone(),
                        reason: ViolationReason::TypeMismatch {
                            expected: col.ty.clone(),
                            found: found.to_string(),
                        },
                    });
                }
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
                    for s in array.as_string::<i32>().iter().flatten() {
                        validator.check_str(s, &mut cv);
                    }
                }
                DataType::LargeUtf8 => {
                    for s in array.as_string::<i64>().iter().flatten() {
                        validator.check_str(s, &mut cv);
                    }
                }
                DataType::Int32 => {
                    for v in array.as_primitive::<Int32Type>().iter().flatten() {
                        validator.check_num(f64::from(v), &mut cv);
                    }
                }
                DataType::Int64 => {
                    for v in array.as_primitive::<Int64Type>().iter().flatten() {
                        #[expect(
                            clippy::cast_precision_loss,
                            reason = "i64->f64 acceptable for range validation"
                        )]
                        let f = v as f64;
                        validator.check_num(f, &mut cv);
                    }
                }
                DataType::Float64 => {
                    for v in array.as_primitive::<Float64Type>().iter().flatten() {
                        validator.check_num(v, &mut cv);
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
