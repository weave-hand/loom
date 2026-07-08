//! Derive a conformance [`ModelShape`](crate::gate::ModelShape) from an ontology
//! `ObjectType`. This fills the seam `gate` documents: the gate stays ontology-free
//! (it takes a plain `ModelShape`); the `ObjectType` -> `ModelShape` derivation lives
//! here so the conversion's ontology dependency does not leak into the gate.

use control_plane_core::{ObjectType, PropertyDef, TableRef, TypeName};

use arrow::datatypes::Schema;
use datafusion_io::arrow_logical_type;

use crate::gate::{ColumnShape, ModelShape, Violation, ViolationReason};

/// One `ColumnShape` per declared property, in declaration order. A property is
/// `required` if it is non-nullable **or** it is the type's declared `identity`
/// (the identity must always be present for the row to be addressable).
#[must_use]
pub fn model_shape_from_type(ty: &ObjectType) -> ModelShape {
    let identity = ty.identity.as_deref();
    ModelShape {
        columns: ty
            .properties
            .iter()
            .map(|p| ColumnShape {
                name: p.name.clone(),
                ty: p.ty.clone(),
                required: p.required || identity == Some(p.name.as_str()),
                constraints: p.constraints.clone(),
            })
            .collect(),
    }
}

/// Why an Arrow batch schema could not be turned into an `ObjectType`. Each variant
/// names the offending column so the HTTP layer can render a precise client error:
/// `UnsupportedColumns` -> 422 (a `violations`-shaped body), `IdentityNotFound` -> 400.
#[derive(Debug)]
pub enum InferTypeError {
    /// One or more Arrow columns have no loom logical-type mapping. Carries one
    /// `Violation` per offending column (reason `Unsupported`).
    UnsupportedColumns(Vec<Violation>),
    /// The caller-declared `?identity=` column is not present in the batch schema.
    IdentityNotFound(String),
}

/// Infer an `ObjectType` from an Arrow batch `schema` — the reverse of
/// [`model_shape_from_type`]. One ordered [`PropertyDef`] per Arrow field: `name` is the
/// field name, `ty` is the loom logical type via the shared landing mapping
/// ([`arrow_logical_type`]), and `required` mirrors the field's non-nullability. The
/// inferred type is bound to the conventional `main.<name>` table.
///
/// `identity`, if `Some`, must name a field in the batch: that property is forced
/// `required` and recorded as the type's `identity` (a wrong identity is hard to undo,
/// so it is caller-declared, never guessed). An Arrow type with no logical mapping is an
/// `UnsupportedColumns` error naming every offending column; a `?identity=` naming an
/// absent column is `IdentityNotFound`.
pub fn infer_object_type(
    name: &TypeName,
    schema: &Schema,
    identity: Option<&str>,
) -> Result<ObjectType, InferTypeError> {
    let mut properties = Vec::with_capacity(schema.fields().len());
    let mut violations = Vec::new();
    for field in schema.fields() {
        match arrow_logical_type(field.data_type()) {
            Some(ty) => properties.push(PropertyDef {
                name: field.name().clone(),
                ty: ty.to_string(),
                required: !field.is_nullable(),
                constraints: control_plane_core::PropertyConstraints::default(),
            }),
            None => violations.push(Violation {
                column: field.name().clone(),
                reason: ViolationReason::Unsupported,
            }),
        }
    }
    if !violations.is_empty() {
        return Err(InferTypeError::UnsupportedColumns(violations));
    }

    if let Some(id) = identity {
        match properties.iter_mut().find(|p| p.name == id) {
            Some(p) => p.required = true,
            None => return Err(InferTypeError::IdentityNotFound(id.to_string())),
        }
    }

    Ok(ObjectType {
        name: name.clone(),
        properties,
        derived: vec![],
        // Conventional landing target for an inferred type: the default `main` schema,
        // table named for the type. Serving resolves type -> this table at read time.
        table: TableRef {
            schema: "main".to_string(),
            name: name.0.clone(),
        },
        identity: identity.map(str::to_string),
        version: None,
    })
}
