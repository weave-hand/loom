use control_plane_core::{ColumnStat, DataFile, FileFormat, StatValue};

#[test]
fn column_stat_holds_typed_bounds_without_value_count() {
    let s = ColumnStat {
        column_name: "id".into(),
        null_count: 1,
        column_size_bytes: 64,
        min: Some(StatValue::I64(10)),
        max: Some(StatValue::I64(99)),
    };
    assert_eq!(s.min, Some(StatValue::I64(10)));
    assert_eq!(s.max, Some(StatValue::I64(99)));
}

#[test]
fn data_file_records_format_and_optional_footer() {
    let f = DataFile {
        path: "p/part-0.parquet".into(),
        path_is_relative: true,
        file_format: FileFormat::Parquet,
        record_count: 3,
        file_size_bytes: 200,
        column_stats: vec![],
        parquet_footer_size: Some(50),
    };
    assert_eq!(f.file_format, FileFormat::Parquet);
    assert_eq!(f.parquet_footer_size, Some(50));
}
