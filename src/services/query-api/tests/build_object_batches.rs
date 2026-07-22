//! build_object_batches: an N-row Arrow batch + schema + ColumnSpec list from
//! aligned (columns, rows, logical_types). Pure logic (no DB).

use query_api::serving::{SqlValue, build_object_batches};

#[test]
fn builds_two_rows() {
    let cols = vec!["sku".to_string(), "qty".to_string()];
    let types = vec!["String".to_string(), "Long".to_string()];
    let rows = vec![
        vec![SqlValue::Text("a".into()), SqlValue::Int(1)],
        vec![SqlValue::Text("b".into()), SqlValue::Int(2)],
    ];
    // sku non-nullable (e.g. an identity), qty nullable — the per-column flags are honored.
    let (schema, batch, specs) =
        build_object_batches(&cols, &rows, &types, &[false, true]).unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.num_columns(), 2);
    assert_eq!(specs.len(), 2);
    assert!(
        !schema.field(0).is_nullable(),
        "sku field non-nullable (#359)"
    );
    assert!(schema.field(1).is_nullable(), "qty field nullable");
    assert!(!specs[0].nullable && specs[1].nullable);
}

#[test]
fn rejects_empty_rows() {
    let err = build_object_batches(&["sku".into()], &[], &["String".into()], &[true]);
    assert!(err.is_err());
}
