//! Serde round-trip + lookup for the governed-catalog wire payloads.

use control_plane_core::{
    CompareOp, GovernedCatalog, GovernedTable, RowFilter, ScalarValue, TableRef,
};

#[test]
fn governed_catalog_json_roundtrips_and_looks_up() {
    let t = TableRef {
        schema: "s".into(),
        name: "t".into(),
    };
    let cat = GovernedCatalog {
        tables: vec![GovernedTable {
            table: t.clone(),
            row_filters: vec![RowFilter::Compare {
                property: "a".into(),
                op: CompareOp::Gt,
                value: ScalarValue::Int(5),
            }],
            denied: vec!["secret".into()],
            masked: vec!["email".into()],
        }],
    };
    let json = serde_json::to_vec(&cat).expect("encode");
    let back: GovernedCatalog = serde_json::from_slice(&json).expect("decode");
    assert_eq!(back, cat);
    assert!(back.table_for(&t).is_some());
    let absent = TableRef {
        schema: "s".into(),
        name: "missing".into(),
    };
    assert!(back.table_for(&absent).is_none());
}

#[test]
fn governed_catalog_defaults_missing_lists() {
    // Only `table` provided; list fields default to empty.
    let json = br#"{"tables":[{"table":{"schema":"s","name":"t"}}]}"#;
    let cat: GovernedCatalog = serde_json::from_slice(json).expect("decode");
    let gt = &cat.tables[0];
    assert!(gt.row_filters.is_empty() && gt.denied.is_empty() && gt.masked.is_empty());
}
