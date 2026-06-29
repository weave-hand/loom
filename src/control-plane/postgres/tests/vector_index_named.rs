//! Two named index rows coexist per column; lookup_vector_index resolves by name.

use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::vector_index::{
    VectorIndexRow, insert_vector_index, lookup_vector_index,
};
use sqlx::{AssertSqlSafe, PgPool};

/// Seed a live mirror table row and return its `table_id`.
/// Copied verbatim from tests/vector_index_mirror.rs.
async fn seed_live_table(pool: &PgPool, namespace: &str, name: &str) -> i64 {
    sqlx::query(AssertSqlSafe(
        "insert into iceberg_mirror.snapshot (snapshot_id) values (1) on conflict do nothing",
    ))
    .execute(pool)
    .await
    .unwrap();
    sqlx::query_scalar(AssertSqlSafe(format!(
        "insert into iceberg_mirror.\"table\" (table_namespace, table_name, begin_snapshot) \
         values ('{namespace}','{name}',1) returning table_id",
    )))
    .fetch_one(pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn lookup_resolves_by_index_name() {
    let fx = PgFixture::start();
    let (_, _db) = fx.fresh_db().await;
    let pool = fx.pool_for(&_db).await;
    let table_id = seed_live_table(&pool, "main", "document").await;
    let mut conn = pool.acquire().await.unwrap();
    for (name, kind) in [("by_sim", "hnsw"), ("by_cluster", "ivf_flat")] {
        insert_vector_index(
            &mut conn,
            &VectorIndexRow {
                table_id,
                column: "embedding".into(),
                index_name: name.into(),
                covered_snapshot: 1,
                metric: "cosine".into(),
                index_kind: kind.into(),
                dim: 8,
                row_count: 10,
                puffin_path: format!("/p/{name}.puffin"),
            },
        )
        .await
        .unwrap();
    }
    drop(conn);
    assert_eq!(
        lookup_vector_index(&pool, table_id, "by_sim", 1)
            .await
            .unwrap()
            .unwrap()
            .index_kind,
        "hnsw"
    );
    assert_eq!(
        lookup_vector_index(&pool, table_id, "by_cluster", 1)
            .await
            .unwrap()
            .unwrap()
            .index_kind,
        "ivf_flat"
    );
    assert!(
        lookup_vector_index(&pool, table_id, "nope", 1)
            .await
            .unwrap()
            .is_none()
    );
}
