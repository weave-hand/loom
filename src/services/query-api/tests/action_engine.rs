//! EmbeddedDuckDbWriter inserts a row inline; it reads back through the read engine.

use control_plane_core::TableRef;
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use query_api::serving::{
    ActionEngine, EmbeddedDuckDb, EmbeddedDuckDbWriter, ServingEngine, SqlValue,
};

#[tokio::test(flavor = "multi_thread")]
async fn writer_inserts_a_row_read_back_by_the_reader() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let writer_fx = DuckLakeWriter::new(fx.socket_path(), &db);
    writer_fx.bootstrap().await;
    writer_fx
        .seed(
            "main",
            "widget",
            &[
                ("id".into(), "BIGINT".into(), false),
                ("name".into(), "VARCHAR".into(), true),
            ],
            &[],
        )
        .await;

    let pg_conn = format!(
        "dbname={db} host={} user=postgres",
        fx.socket_path().display()
    );
    let data_path = writer_fx.data_path();

    let engine = EmbeddedDuckDbWriter::attach(&pg_conn, data_path)
        .await
        .unwrap();
    engine
        .insert_row(
            &TableRef {
                schema: "main".into(),
                name: "widget".into(),
            },
            &["id".to_string(), "name".to_string()],
            &[SqlValue::Int(7), SqlValue::Text("hi".into())],
        )
        .await
        .unwrap();

    let reader = EmbeddedDuckDb::attach(&pg_conn, data_path).await.unwrap();
    let rows = reader
        .fetch_rows("SELECT id, name FROM main.widget", &[])
        .await
        .unwrap();
    assert_eq!(rows.rows.len(), 1, "the inline-written row reads back");
    assert_eq!(rows.rows[0][0], SqlValue::Int(7));
    assert_eq!(rows.rows[0][1], SqlValue::Text("hi".into()));
}
