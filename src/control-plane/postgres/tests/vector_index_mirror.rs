//! The `iceberg_mirror.vector_index` binding: insert a row, then look up the
//! latest covered_snapshot <= Q.

use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::vector_index::{
    VectorIndexRow, insert_vector_index, lookup_vector_index,
};
use sqlx::AssertSqlSafe;

#[tokio::test]
async fn insert_then_lookup_latest_le_q() {
    let fx = PgFixture::start();
    let (cp, _db) = fx.fresh_db().await;
    let pool = cp.pool().clone();

    // Create a real table row via the mirror so the FK resolves. Insert a
    // snapshot and a table row directly so we get a valid table_id. These must be
    // separate statements: sqlx prepares each query, and Postgres rejects multiple
    // commands in one prepared statement (error 42601).
    sqlx::query(AssertSqlSafe(
        "insert into iceberg_mirror.snapshot (snapshot_id) values (1) on conflict do nothing",
    ))
    .execute(&pool)
    .await
    .unwrap();
    let table_id: i64 = sqlx::query_scalar(AssertSqlSafe(
        "insert into iceberg_mirror.\"table\" (table_namespace, table_name, begin_snapshot) \
         values ('wh','docs',1) returning table_id",
    ))
    .fetch_one(&pool)
    .await
    .unwrap();

    let mut conn = pool.acquire().await.unwrap();
    let row = VectorIndexRow {
        table_id,
        column: "embedding".into(),
        index_name: "default".into(),
        covered_snapshot: 5,
        metric: "cosine".into(),
        index_kind: "flat".into(),
        dim: 4,
        row_count: 3,
        puffin_path: "file:///tmp/x.puffin".into(),
    };
    insert_vector_index(&mut conn, &row).await.unwrap();

    // Q below the covered snapshot → no binding.
    assert!(
        lookup_vector_index(&pool, table_id, "default", 4)
            .await
            .unwrap()
            .is_none()
    );
    // Q at/after → the row.
    let got = lookup_vector_index(&pool, table_id, "default", 9)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.covered_snapshot, 5);
    assert_eq!(got.dim, 4);
    assert_eq!(got.metric, "cosine");

    // A newer index at covered 8: lookup at Q=9 returns the newest <= Q.
    let mut row2 = row.clone();
    row2.covered_snapshot = 8;
    row2.row_count = 7;
    insert_vector_index(&mut conn, &row2).await.unwrap();
    let got = lookup_vector_index(&pool, table_id, "default", 9)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.covered_snapshot, 8);
    assert_eq!(got.row_count, 7);
}
