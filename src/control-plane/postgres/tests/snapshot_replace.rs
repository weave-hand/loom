use control_plane_core::{
    Catalog, ColumnStat, ControlPlane, DataFile, FileFormat, PageReq, StatValue, TableRef,
};
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};

fn data_file(path: &str, rows: i64) -> DataFile {
    DataFile {
        path: path.into(),
        path_is_relative: true,
        file_format: FileFormat::Parquet,
        record_count: rows,
        file_size_bytes: rows * 16,
        column_stats: vec![ColumnStat {
            column_name: "id".into(),
            null_count: 0,
            column_size_bytes: rows * 8,
            min: Some(StatValue::I64(0)),
            max: Some(StatValue::I64(rows - 1)),
        }],
        parquet_footer_size: Some(120),
    }
}

// Append a data file, then replace_files: at the new snapshot only the replacement is
// live and table stats reflect it alone; at the prior snapshot the original file is
// still live (time travel via end_snapshot).
#[tokio::test]
async fn replace_files_expires_old_and_preserves_time_travel() {
    let fixture = PgFixture::start();
    let (cp, db) = fixture.fresh_db().await;
    let writer = DuckLakeWriter::new(fixture.socket_path(), &db);
    writer
        .seed("main", "t", &[("id".into(), "BIGINT".into(), false)], &[])
        .await;

    let t = TableRef {
        schema: "main".into(),
        name: "t".into(),
    };

    // append a.parquet (10 rows).
    let mut tx = cp.begin().await.unwrap();
    tx.append_files(&t, &[data_file("a.parquet", 10)])
        .await
        .unwrap();
    let s1 = tx.commit().await.unwrap().expect("append snapshot");

    // replace with b.parquet (4 rows).
    let mut tx = cp.begin().await.unwrap();
    tx.replace_files(&t, &[data_file("b.parquet", 4)])
        .await
        .unwrap();
    let s2 = tx.commit().await.unwrap().expect("replace snapshot");

    // current snapshot: only b.parquet, 4 rows.
    let cur = cp.current_snapshot(&t).await.unwrap();
    assert_eq!(cur.id, s2);
    let now = cp.files(&t, s2, PageReq::unbounded()).await.unwrap();
    assert_eq!(now.len(), 1, "only the replacement is live");
    assert_eq!(now.items[0].path, "b.parquet");
    assert_eq!(now.items[0].record_count, 4);

    // prior snapshot: a.parquet still live (time travel).
    let before = cp.files(&t, s1, PageReq::unbounded()).await.unwrap();
    assert_eq!(before.len(), 1, "prior snapshot retains the original file");
    assert_eq!(before.items[0].path, "a.parquet");
    assert_eq!(before.items[0].record_count, 10);
}
