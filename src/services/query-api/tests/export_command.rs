use arrow::datatypes::DataType;
use query_api::flight_export::{ExportCommand, export_arrow_schema};

#[test]
fn export_command_json_round_trips() {
    let cmd = ExportCommand {
        type_name: "Chunk".to_string(),
        filters: vec![("sourcebook".to_string(), "PHB".to_string())],
        ids: vec!["c1".to_string(), "c2".to_string()],
    };
    let bytes = cmd.encode();
    let back = ExportCommand::decode(&bytes).expect("decode");
    assert_eq!(back, cmd);
}

#[test]
fn export_command_decodes_minimal() {
    let bytes = br#"{"type":"Chunk"}"#;
    let cmd = ExportCommand::decode(bytes).expect("decode");
    assert_eq!(cmd.type_name, "Chunk");
    assert!(cmd.filters.is_empty());
    assert!(cmd.ids.is_empty());
}

#[test]
fn export_schema_maps_scalars_and_vector() {
    let cols = vec!["id".to_string(), "embedding".to_string()];
    let types = vec!["string".to_string(), "vector(4)".to_string()];
    let schema = export_arrow_schema(&cols, &types, &[]).expect("schema");
    assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    match schema.field(1).data_type() {
        DataType::List(f) => {
            assert_eq!(f.data_type(), &DataType::Float32);
            assert!(!f.is_nullable());
        }
        other => panic!("expected List<Float32>, got {other:?}"),
    }
}

#[test]
fn export_schema_advertises_masked_columns_as_utf8() {
    let cols = vec!["embedding".to_string()];
    let types = vec!["vector(4)".to_string()];
    let schema = export_arrow_schema(&cols, &types, &["embedding".to_string()]).expect("schema");
    assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
}

#[test]
fn export_schema_rejects_unknown_logical_type() {
    let err = export_arrow_schema(&["x".to_string()], &["nonsense".to_string()], &[]);
    assert!(err.is_err());
}
