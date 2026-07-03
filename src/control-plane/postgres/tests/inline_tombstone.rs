//! Verifies the inline tier carries a `loom_tombstone` column after an append.
//! loom_fixture_test (Postgres).

use control_plane_core::{ColumnSpec, EventType, LineageEvent, RunId, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_inline;
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_table_has_tombstone_column() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let table = TableRef {
        schema: "sales".to_string(),
        name: "orders".to_string(),
    };
    let columns = vec![ColumnSpec {
        name: "id".to_string(),
        ty: "long".to_string(),
        nullable: false,
    }];
    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let batch = arrow_array::RecordBatch::try_new(
        schema,
        vec![Arc::new(arrow_array::Int64Array::from(vec![1]))],
    )
    .expect("test batch");
    let lineage = LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![],
        payload: serde_json::json!({ "source": "test" }),
    };

    iceberg_inline::inline_append(&pool, &table, &columns, &batch, lineage, None)
        .await
        .expect("inline append succeeds");

    // Resolve the internal table_id the same way the other inline tests do.
    let tid: i64 = sqlx::query_scalar(
        "select table_id from iceberg_mirror.table \
         where table_namespace='sales' and table_name='orders' and end_snapshot is null",
    )
    .fetch_one(&pool)
    .await
    .expect("table_id");

    let exists: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "select exists(select 1 from information_schema.columns \
         where table_schema='iceberg_mirror' and table_name='inline_{tid}' \
           and column_name='loom_tombstone')"
    )))
    .fetch_one(&pool)
    .await
    .expect("column existence check");
    assert!(exists, "inline table must carry loom_tombstone");
}
