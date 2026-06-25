//! `execute_query_stream` yields exactly the same rows as the collected
//! `execute_query` path — proving the streaming sibling is behaviour-preserving.

use arrow::array::{Array, Int64Array, RecordBatch};
use control_plane_postgres::fixture::{IcebergWriter, PgFixture};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use futures::TryStreamExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_matches_collected() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);

    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), false),
    ];
    let writer = IcebergWriter::new(pool.clone(), dsn);
    writer.seed("sales", "orders", &cols, &[3]).await;
    writer
        .inline(
            "sales",
            "orders",
            &cols,
            &[(100, "row100")],
            uuid::Uuid::new_v4(),
        )
        .await;

    let catalog = IcebergCatalog::new(pool);
    let sql = r#"SELECT "id" FROM "sales"."orders" ORDER BY "id""#;

    let collected = engine_serving::execute_query(&catalog, sql, None)
        .await
        .expect("execute_query");
    let collected_ids = ids(&collected);

    let stream = engine_serving::execute_query_stream(&catalog, sql, None)
        .await
        .expect("execute_query_stream");
    let streamed: Vec<RecordBatch> = stream.try_collect().await.expect("collect stream");
    let streamed_ids = ids(&streamed);

    assert_eq!(collected_ids, vec![0, 1, 2, 100]);
    assert_eq!(streamed_ids, collected_ids, "stream must match collected");
}

fn ids(batches: &[RecordBatch]) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let col = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id col is i64");
        for i in 0..col.len() {
            out.push(col.value(i));
        }
    }
    out
}
