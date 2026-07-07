//! The `bucket_count >= 1` CHECK on `stream.stream_table` rejects a non-positive
//! bucket count at the database layer (defense-in-depth under the in-code guard).
use control_plane_postgres::fixture::PgFixture;

#[tokio::test]
async fn declare_zero_buckets_is_rejected_by_check() {
    let fixture = PgFixture::shared();
    let (_cp, db) = fixture.fresh_db().await;
    let pool = fixture.pool_for(&db).await;

    let err =
        sqlx::query("insert into stream.stream_table (table_id, bucket_count) values ($1, $2)")
            .bind(4242_i64)
            .bind(0_i32)
            .execute(&pool)
            .await;

    assert!(
        err.is_err(),
        "bucket_count = 0 must be rejected by the CHECK"
    );
}
