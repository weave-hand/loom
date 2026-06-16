use control_plane_core::{
    Catalog, ColumnSpec, ColumnStat, ControlPlane, DataFile, FileFormat, PageReq, StatValue,
    TableRef,
};
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};

// Bootstrap a bare DuckLake catalog (no DuckDB-created table), then have loom's
// native writer CREATE a table and APPEND a data file to it in one transaction,
// and assert the existing `Catalog` reads see both the columns and the file.
#[tokio::test]
async fn create_table_then_append_in_one_tx() {
    let fixture = PgFixture::start();
    let (cp, db) = fixture.fresh_db().await;
    let writer = DuckLakeWriter::new(fixture.socket_path(), &db);

    // Bare ATTACH: 27 ducklake_* tables, snapshot 0, `main` schema, no table.
    writer.bootstrap().await;

    let t = TableRef {
        schema: "main".into(),
        name: "made_by_loom".into(),
    };
    let mut tx = cp.begin().await.unwrap();
    tx.create_table(
        &t,
        &[
            ColumnSpec {
                name: "id".into(),
                ty: "long".into(),
                nullable: false,
            },
            ColumnSpec {
                name: "name".into(),
                ty: "string".into(),
                nullable: true,
            },
        ],
    )
    .await
    .unwrap();
    tx.append_files(
        &t,
        &[DataFile {
            path: "ducklake-loom-0.parquet".into(),
            path_is_relative: true,
            file_format: FileFormat::Parquet,
            record_count: 2,
            file_size_bytes: 100,
            column_stats: vec![
                ColumnStat {
                    column_name: "id".into(),
                    null_count: 0,
                    column_size_bytes: 16,
                    min: Some(StatValue::I64(1)),
                    max: Some(StatValue::I64(2)),
                },
                ColumnStat {
                    column_name: "name".into(),
                    null_count: 0,
                    column_size_bytes: 20,
                    min: Some(StatValue::Str("a".into())),
                    max: Some(StatValue::Str("b".into())),
                },
            ],
            parquet_footer_size: Some(50),
        }],
    )
    .await
    .unwrap();
    let snap = tx.commit().await.unwrap();
    assert!(snap.is_some(), "create+append commit yields a snapshot id");

    // Columns visible via the existing Catalog::schema read.
    let latest = cp.current_snapshot(&t).await.unwrap();
    assert_eq!(latest.id, snap.unwrap());
    let schema = cp.schema(&t, latest.id).await.unwrap();
    assert_eq!(schema.columns.len(), 2);
    assert_eq!(schema.columns[0].name, "id");
    assert_eq!(schema.columns[0].ty, "long");
    assert!(!schema.columns[0].nullable);
    assert_eq!(schema.columns[1].name, "name");
    assert_eq!(schema.columns[1].ty, "string");
    assert!(schema.columns[1].nullable);

    let files = cp.files(&t, latest.id, PageReq::unbounded()).await.unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files.items[0].record_count, 2);
    assert_eq!(files.items[0].path, "ducklake-loom-0.parquet");
}
