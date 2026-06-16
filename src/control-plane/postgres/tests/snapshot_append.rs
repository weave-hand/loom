use control_plane_core::{
    Catalog, ColumnStat, ControlPlane, DataFile, FileFormat, PageReq, StatValue, TableRef,
};
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};

// Bootstrap a DuckLake catalog + a DuckDB-created table (no loom-written files
// yet), then have loom's native append writer register one data file via
// `append_files` + `commit`, and assert the existing `Catalog` reads see it.
#[tokio::test]
async fn append_files_writes_visible_data_file() {
    let fixture = PgFixture::start();
    let (cp, db) = fixture.fresh_db().await;
    let writer = DuckLakeWriter::new(fixture.socket_path(), &db);

    // ATTACH + CREATE TABLE main.t (id BIGINT), no INSERTs -> no data files.
    let snaps = writer
        .seed("main", "t", &[("id".into(), "BIGINT".into(), false)], &[])
        .await;
    assert!(snaps.is_empty(), "no data files seeded by DuckDB");

    let t = TableRef {
        schema: "main".into(),
        name: "t".into(),
    };
    let mut tx = cp.begin().await.unwrap();
    tx.append_files(
        &t,
        &[DataFile {
            path: "ducklake-loom-0.parquet".into(),
            path_is_relative: true,
            file_format: FileFormat::Parquet,
            record_count: 10,
            file_size_bytes: 444,
            column_stats: vec![ColumnStat {
                column_name: "id".into(),
                null_count: 0,
                column_size_bytes: 88,
                min: Some(StatValue::I64(0)),
                max: Some(StatValue::I64(9)),
            }],
            parquet_footer_size: Some(249),
        }],
    )
    .await
    .unwrap();
    let snap = tx.commit().await.unwrap();
    assert!(snap.is_some(), "append commit yields a snapshot id");

    let latest = cp.current_snapshot(&t).await.unwrap();
    assert_eq!(latest.id, snap.unwrap());
    let files = cp.files(&t, latest.id, PageReq::unbounded()).await.unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files.items[0].record_count, 10);
    assert_eq!(files.items[0].path, "ducklake-loom-0.parquet");
    assert_eq!(files.items[0].file_size_bytes, 444);
}
