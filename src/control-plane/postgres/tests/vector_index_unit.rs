//! Pure-seam unit tests for the decomposed build path: `declared_dim` (the
//! empty-table dim fallback — previously buried in build_vector_index's job 6
//! with no cheap fixture reaching it) and, from Task 5, `build_lineage_event`
//! (the canonical-DatasetRef event assembly).

use control_plane_core::{ColumnDef, DatasetRef, EventType, RunId, TableRef, TableSchema};
use control_plane_postgres::vector_index::{build_lineage_event, declared_dim};

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

#[test]
fn lineage_event_uses_canonical_dataset_ref() {
    let table = TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    };
    let run = RunId(uuid::Uuid::new_v4());
    let evt = build_lineage_event(run, &table, "embedding", 7, 42, "s3://x/y.puffin");
    assert_eq!(evt.run_id, run);
    assert!(matches!(evt.event_type, EventType::Complete));
    // Canonical loom dataset ref — the SAME lineage node the landing/flush
    // emitters use (iss-vector-build-lineage-ref, fixed in #295); this unit pin
    // keeps the fix structural through the decomposition.
    assert_eq!(evt.inputs, vec![DatasetRef::from(&table)]);
    assert_eq!(evt.outputs.len(), 1);
    assert_eq!(evt.outputs[0].namespace, "loom-vector-index");
    assert_eq!(evt.outputs[0].name, "s3://x/y.puffin");
    assert_eq!(evt.payload["column"], "embedding");
    assert_eq!(evt.payload["covered_snapshot"], 7_i64);
    assert_eq!(evt.payload["row_count"], 42_i64);
}
