//! The new compact RPC messages encode/serialize as expected (no server needed).
use control_plane_core::{ColumnStat, DataFile, FileFormat, StatValue};

#[test]
fn data_file_json_round_trips_for_wire() {
    // The client puts each DataFile in write_json as a JSON string; the engine reads
    // it back. Guard that contract here (the engine handler depends on it).
    let f = DataFile {
        path: "file:///wh/main/t/c/part-0.parquet".into(),
        path_is_relative: false,
        file_format: FileFormat::Parquet,
        record_count: 3,
        file_size_bytes: 100,
        column_stats: vec![ColumnStat {
            column_name: "id".into(),
            null_count: 0,
            column_size_bytes: 8,
            min: Some(StatValue::I64(1)),
            max: Some(StatValue::I64(3)),
        }],
        parquet_footer_size: Some(20),
    };
    let s = serde_json::to_string(&f).unwrap();
    let back: DataFile = serde_json::from_str(&s).unwrap();
    assert_eq!(back, f);
}

#[test]
fn compact_request_message_constructs() {
    // Proof the generated pb types exist with the expected fields.
    let req = engine_wire::pb::CompactTableRequest {
        schema: "main".into(),
        name: "t".into(),
        expire: vec!["file:///a.parquet".into()],
        write_json: vec!["{}".into()],
    };
    assert_eq!(req.expire.len(), 1);
    let lf = engine_wire::pb::ListFilesResponse {
        files: vec![engine_wire::pb::FileMeta {
            path: "p".into(),
            record_count: 1,
            file_size_bytes: 2,
        }],
        columns_json: None,
    };
    assert_eq!(lf.files[0].record_count, 1);
}
