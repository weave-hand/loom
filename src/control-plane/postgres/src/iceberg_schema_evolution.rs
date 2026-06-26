//! The single home of loom's additive-vs-unsupported Iceberg schema-evolution
//! policy. Pure logic over `iceberg_mirror::ProjectedColumn` — no I/O — so it is
//! unit-testable in isolation and cannot drift between the two mirror-projection
//! sites (`register_files`, `write_mirror`) that call it via `reconcile_and_project`.

use crate::iceberg_mirror::ProjectedColumn;

/// The outcome of comparing the live mirror columns to an incoming write's columns.
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaPlan {
    /// Incoming equals live — no column rows to write.
    Identical,
    /// Incoming equals live plus one or more new nullable columns appended at the end.
    Additive { new_columns: Vec<ProjectedColumn> },
}

/// A non-additive (therefore rejected) schema change. Every `Display` begins
/// `schema evolution unsupported:` so callers can surface a stable, matchable marker.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum SchemaEvolutionError {
    #[error("schema evolution unsupported: column {name:?} was dropped")]
    ColumnDropped { name: String },
    #[error(
        "schema evolution unsupported: column at position {position} changed from {was:?} to {now:?} (rename or reorder)"
    )]
    ColumnChangedAtPosition {
        position: i64,
        was: String,
        now: String,
    },
    #[error("schema evolution unsupported: column {name:?} type changed from {from:?} to {to:?}")]
    ColumnTypeChanged {
        name: String,
        from: String,
        to: String,
    },
    #[error("schema evolution unsupported: column {name:?} nullability changed")]
    ColumnNullabilityChanged { name: String },
    #[error("schema evolution unsupported: new column {name:?} is not nullable")]
    NonNullableColumnAdded { name: String },
}

/// Classify `incoming` against the `live` mirror columns. Comparison is POSITIONAL on
/// `(name, iceberg_type, nullable)` — the `order` field value is intentionally ignored.
pub fn classify_schema_change(
    live: &[ProjectedColumn],
    incoming: &[ProjectedColumn],
) -> Result<SchemaPlan, SchemaEvolutionError> {
    for (i, (l, n)) in live.iter().zip(incoming.iter()).enumerate() {
        if l.name != n.name {
            return Err(SchemaEvolutionError::ColumnChangedAtPosition {
                position: i as i64,
                was: l.name.clone(),
                now: n.name.clone(),
            });
        }
        if l.iceberg_type != n.iceberg_type {
            return Err(SchemaEvolutionError::ColumnTypeChanged {
                name: l.name.clone(),
                from: l.iceberg_type.clone(),
                to: n.iceberg_type.clone(),
            });
        }
        if l.nullable != n.nullable {
            return Err(SchemaEvolutionError::ColumnNullabilityChanged {
                name: l.name.clone(),
            });
        }
    }
    if incoming.len() < live.len() {
        // `incoming.len() < live.len()` guarantees `incoming.len()` is a valid index into `live`.
        let name = live
            .get(incoming.len())
            .map_or_else(|| "<schema evolution: index out of bounds>".to_string(), |c| c.name.clone());
        return Err(SchemaEvolutionError::ColumnDropped { name });
    }
    // `live.len() <= incoming.len()` is guaranteed by the early return above.
    let new_columns: Vec<ProjectedColumn> = incoming
        .get(live.len()..)
        .map_or_else(Vec::new, <[ProjectedColumn]>::to_vec);
    if let Some(req) = new_columns.iter().find(|c| !c.nullable) {
        return Err(SchemaEvolutionError::NonNullableColumnAdded {
            name: req.name.clone(),
        });
    }
    if new_columns.is_empty() {
        Ok(SchemaPlan::Identical)
    } else {
        Ok(SchemaPlan::Additive { new_columns })
    }
}
