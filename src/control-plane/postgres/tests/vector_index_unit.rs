//! Pure-seam unit tests for the decomposed build path: `declared_dim` (the
//! empty-table dim fallback — previously buried in build_vector_index's job 6
//! with no cheap fixture reaching it) and, from Task 5, `build_lineage_event`
//! (the canonical-DatasetRef event assembly).

use control_plane_core::{ColumnDef, TableSchema};
use control_plane_postgres::vector_index::declared_dim;

fn schema(cols: &[(&str, &str)]) -> TableSchema {
    TableSchema {
        columns: cols
            .iter()
            .enumerate()
            .map(|(i, (name, ty))| ColumnDef {
                order: i as i64,
                name: (*name).to_string(),
                ty: (*ty).to_string(),
                nullable: false,
            })
            .collect(),
    }
}

#[test]
fn declared_dim_parses_vector_n() {
    let s = schema(&[("id", "long"), ("embedding", "vector(4)")]);
    assert_eq!(declared_dim(&s, "embedding"), 4);
}

#[test]
fn declared_dim_zero_when_column_missing() {
    let s = schema(&[("id", "long")]);
    assert_eq!(declared_dim(&s, "embedding"), 0);
}

#[test]
fn declared_dim_zero_when_not_a_vector() {
    assert_eq!(
        declared_dim(&schema(&[("embedding", "long")]), "embedding"),
        0
    );
    assert_eq!(
        declared_dim(&schema(&[("embedding", "vector()")]), "embedding"),
        0,
        "malformed vector type falls back to 0, matching the legacy unwrap_or(0)"
    );
}
