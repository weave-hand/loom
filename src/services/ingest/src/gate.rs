//! The optional model-conformance gate ("this data is this model"). Takes a plain
//! `ModelShape` value — NOT the ontology — so this crate stays ontology-free; a
//! later slice derives a `ModelShape` from an `ObjectType`. This slice ships the
//! seam plus a minimal check (required columns present, types match). Richer
//! constraints (ranges, regex, coercion) extend `ViolationReason`.
//!
//! Note: this validates schema STRUCTURE (column presence + type), not runtime
//! values — a column declared non-nullable can still carry nulls in the batch;
//! value/null-constraint enforcement is a later slice.

use arrow::datatypes::Schema;

use crate::infer::duck_type;

/// One expected column of a model. `ty` is a DuckLake type string.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnShape {
    pub name: String,
    pub ty: String,
    pub required: bool,
}

/// The physical shape a batch must satisfy to be "this model".
#[derive(Clone, Debug, PartialEq, Eq)]
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
    /// The column is present but its inferred DuckLake type does not match the model.
    TypeMismatch {
        expected: String,
        found: String,
    },
    /// The column has an Arrow type loom cannot land at all.
    Unsupported,
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
            Some(field) => match duck_type(field.data_type()) {
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
