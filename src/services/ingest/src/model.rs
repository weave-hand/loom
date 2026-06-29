//! Derive a conformance [`ModelShape`](crate::gate::ModelShape) from an ontology
//! `ObjectType`. This fills the seam `gate` documents: the gate stays ontology-free
//! (it takes a plain `ModelShape`); the `ObjectType` -> `ModelShape` derivation lives
//! here so the conversion's ontology dependency does not leak into the gate.

use control_plane_core::ObjectType;

use crate::gate::{ColumnShape, ModelShape};

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
            })
            .collect(),
    }
}
