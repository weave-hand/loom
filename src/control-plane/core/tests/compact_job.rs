use control_plane_core::{
    COMPACT_JOB_KIND, ColumnStat, CompactJob, DataFile, FileFormat, StatValue,
};

#[test]
fn compact_job_round_trips_json() {
    assert_eq!(COMPACT_JOB_KIND, "compact_table");
    let j = CompactJob {
        schema: "main".into(),
        name: "orders".into(),
    };
    let v = serde_json::to_value(&j).unwrap();
    let back: CompactJob = serde_json::from_value(v).unwrap();
    assert_eq!(back.schema, "main");
    assert_eq!(back.name, "orders");
}

#[test]
fn data_file_round_trips_json() {
    let f = DataFile {
        path: "s3://b/main/orders/c/part-0.parquet".into(),
        path_is_relative: false,
        file_format: FileFormat::Parquet,
        record_count: 7,
        file_size_bytes: 1234,
        column_stats: vec![ColumnStat {
            column_name: "id".into(),
            null_count: 0,
            column_size_bytes: 64,
            min: Some(StatValue::I64(1)),
            max: Some(StatValue::I64(7)),
        }],
        parquet_footer_size: Some(40),
    };
    let s = serde_json::to_string(&f).unwrap();
    let back: DataFile = serde_json::from_str(&s).unwrap();
    assert_eq!(back, f);
}
