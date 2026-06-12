//! dataset->model binding: validate a landed DuckLake table's physical schema
//! against a declared ontology type, then persist it (define_type). A bound type is
//! guaranteed-serveable by the query read path. See the part-2b design doc.

use control_plane_core::{
    Catalog, ControlPlaneError, ObjectType, Ontology, TableRef, UnknownLogicalType, satisfies,
};

#[derive(Debug, thiserror::Error)]
pub enum BindError {
    #[error("table not found in catalog: {0:?}")]
    TableNotFound(TableRef),
    #[error("type does not conform to the landed table: {} violation(s)", .0.len())]
    DoesNotConform(Vec<BindViolation>),
    #[error(transparent)]
    ControlPlane(#[from] ControlPlaneError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindViolation {
    pub property: String,
    pub reason: BindViolationReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindViolationReason {
    MissingColumn,
    UnknownLogicalType(String),
    TypeMismatch { logical: String, physical: String },
    NullabilityViolation, // a required property backed by a nullable column
}

/// Validate `type_def` against the physical schema of its target table, then persist
/// it via `define_type`. Collects ALL violations; persists nothing on rejection.
pub async fn bind(
    catalog: &dyn Catalog,
    ontology: &dyn Ontology,
    type_def: ObjectType,
) -> Result<(), BindError> {
    // 1. The table must be live in the catalog.
    let snap = match catalog.current_snapshot(&type_def.table).await {
        Ok(s) => s,
        Err(ControlPlaneError::NotFound(_)) => {
            return Err(BindError::TableNotFound(type_def.table.clone()));
        }
        Err(e) => return Err(BindError::ControlPlane(e)),
    };

    // 2. Its physical columns at that snapshot.
    let schema = catalog.schema(&type_def.table, snap.id).await?;

    // 3. Validate every declared property against its same-named physical column.
    //    Extra physical columns are fine — a type is a view over the table.
    let mut violations = Vec::new();
    for p in &type_def.properties {
        let Some(col) = schema.columns.iter().find(|c| c.name == p.name) else {
            violations.push(BindViolation {
                property: p.name.clone(),
                reason: BindViolationReason::MissingColumn,
            });
            continue;
        };
        match satisfies(&p.ty, &col.ty) {
            Err(UnknownLogicalType(t)) => violations.push(BindViolation {
                property: p.name.clone(),
                reason: BindViolationReason::UnknownLogicalType(t),
            }),
            Ok(false) => violations.push(BindViolation {
                property: p.name.clone(),
                reason: BindViolationReason::TypeMismatch {
                    logical: p.ty.clone(),
                    physical: col.ty.clone(),
                },
            }),
            Ok(true) => {}
        }
        if p.required && col.nullable {
            violations.push(BindViolation {
                property: p.name.clone(),
                reason: BindViolationReason::NullabilityViolation,
            });
        }
    }

    if !violations.is_empty() {
        return Err(BindError::DoesNotConform(violations));
    }

    // 4. Persist — the type is now serveable by the governed read path.
    ontology.define_type(type_def).await?;
    Ok(())
}
