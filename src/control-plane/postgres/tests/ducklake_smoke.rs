use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};

// The hermetic DuckDB CLI + offline extensions can drive DuckLake against the
// fixture's Postgres and produce one data file per (inlining-disabled) batch.
#[tokio::test]
async fn ducklake_writer_produces_catalog() {
    let fixture = PgFixture::start();
    let (_cp, db) = fixture.fresh_db().await;
    let writer = DuckLakeWriter::new(fixture.socket_path(), &db);

    let snaps = writer
        .seed(
            "main",
            "events",
            &[
                ("id".into(), "BIGINT".into(), false),
                ("name".into(), "VARCHAR".into(), true),
            ],
            &[10, 20],
        )
        .await;

    assert_eq!(snaps.len(), 2, "one snapshot per batch");
    assert!(snaps[0] < snaps[1], "snapshots ascending");
}
