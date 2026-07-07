use loom_ui_core::{
    DatasetDetail, PropRow, SchemaCol, TableRef, TypeDetail, schema_from_dataset_details,
    schema_from_types,
};

#[test]
fn physical_schema_carries_schema_name_and_columns() {
    let inputs = vec![(
        TableRef {
            schema: "sales".into(),
            name: "orders".into(),
        },
        DatasetDetail {
            snapshot_time: "t".into(),
            columns: vec![
                SchemaCol {
                    name: "id".into(),
                    ty: "int64".into(),
                    nullable: false,
                },
                SchemaCol {
                    name: "total".into(),
                    ty: "float64".into(),
                    nullable: true,
                },
            ],
        },
    )];
    let schema = schema_from_dataset_details(&inputs);
    assert_eq!(schema.tables.len(), 1);
    let t = &schema.tables[0];
    assert_eq!(t.schema.as_deref(), Some("sales"));
    assert_eq!(t.name, "orders");
    assert_eq!(t.columns.len(), 2);
    assert_eq!(t.columns[0].name, "id");
    assert_eq!(t.columns[0].ty, "int64");
}

#[test]
fn typed_schema_uses_type_name_and_properties() {
    let types = vec![(
        "Customer".to_string(),
        TypeDetail {
            properties: vec![
                PropRow {
                    name: "id".into(),
                    ty: "int64".into(),
                    required: true,
                },
                PropRow {
                    name: "email".into(),
                    ty: "utf8".into(),
                    required: false,
                },
            ],
            ..TypeDetail::default()
        },
    )];
    let schema = schema_from_types(&types);
    assert_eq!(schema.tables.len(), 1);
    let t = &schema.tables[0];
    assert_eq!(t.schema, None);
    assert_eq!(t.name, "Customer");
    assert_eq!(t.columns.len(), 2);
    assert_eq!(t.columns[1].name, "email");
    assert_eq!(t.columns[1].ty, "utf8");
}

#[test]
fn empty_inputs_yield_empty_schema() {
    assert!(schema_from_dataset_details(&[]).tables.is_empty());
    assert!(schema_from_types(&[]).tables.is_empty());
}
