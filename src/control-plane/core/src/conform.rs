//! Exact-match conformance: the SQL result of a typed transform must produce precisely
//! the output type's properties. Pure logic, no I/O. Parallels `ingest::bind`'s
//! validation but ALSO rejects extra columns — a typed transform materializes the
//! table, it is not a view over a wider one.

use crate::{ColumnSpec, PropertyDef, UnknownLogicalType, satisfies};

/// One way a result schema fails to conform to the output type. Collected, not fatal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Violation {
    /// A declared property has no same-named result column.
    MissingColumn { property: String, logical: String },
    /// A result column's physical type does not satisfy the property's logical type.
    TypeMismatch {
        property: String,
        logical: String,
        physical: String,
    },
    /// The property's logical type is not in loom's vocabulary.
    UnknownLogicalType { property: String, logical: String },
    /// A required property is backed by a nullable result column.
    NullabilityViolation { property: String },
    /// A result column has no matching property (the exact-match half).
    UnexpectedColumn { column: String },
}

/// Exact-match: `result` columns must be precisely `properties`. Collects ALL violations
/// (never short-circuits) so an author fixes everything in one pass.
pub fn check_conformance(
    result: &[ColumnSpec],
    properties: &[PropertyDef],
) -> Result<(), Vec<Violation>> {
    let mut violations = Vec::new();

    // Every property must have a conforming, same-named result column.
    for p in properties {
        let Some(col) = result.iter().find(|c| c.name == p.name) else {
            violations.push(Violation::MissingColumn {
                property: p.name.clone(),
                logical: p.ty.clone(),
            });
            continue;
        };
        match satisfies(&p.ty, &col.ty) {
            Err(UnknownLogicalType(t)) => violations.push(Violation::UnknownLogicalType {
                property: p.name.clone(),
                logical: t,
            }),
            Ok(false) => violations.push(Violation::TypeMismatch {
                property: p.name.clone(),
                logical: p.ty.clone(),
                physical: col.ty.clone(),
            }),
            Ok(true) => {}
        }
        if p.required && col.nullable {
            violations.push(Violation::NullabilityViolation {
                property: p.name.clone(),
            });
        }
    }

    // Exact-match half: no result column may lack a matching property.
    for c in result {
        if !properties.iter().any(|p| p.name == c.name) {
            violations.push(Violation::UnexpectedColumn {
                column: c.name.clone(),
            });
        }
    }

    if violations.is_empty() {
        Ok(())
    } else {
        Err(violations)
    }
}
