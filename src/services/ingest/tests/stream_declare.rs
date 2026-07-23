//! `?mode=stream&buckets=N` on `POST /datasets/{schema}/{table}`: the first write
//! declares a log table (row in `stream.stream_table`, `bucket_count = N`); a later
//! bucket-count mismatch or a batch->stream conversion attempt is rejected with 400;
//! a plain (no `mode`) write stays byte-identical to today (batch table, no
//! `stream.stream_table` row). Hermetic Postgres fixture + a temp file warehouse;
//! tower oneshot, no socket. Mirrors `tests/http_land.rs`'s harness.

use axum::http::StatusCode;
use control_plane_postgres::fixture::PgFixture;
use e2e_support::{ingest_router, ipc_bytes, post_ipc, sample_batch};
use sqlx::PgPool;

/// The declared bucket count for `schema.table`, or `None` if it has no
/// `stream.stream_table` row (i.e. it is a batch table, or does not exist).
async fn stream_bucket_count(pool: &PgPool, schema: &str, table: &str) -> Option<i32> {
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "select st.bucket_count from stream.stream_table st \
         join iceberg_mirror.table t on t.table_id = st.table_id \
         where t.table_namespace = '{schema}' and t.table_name = '{table}' \
         and t.end_snapshot is null"
    )))
    .fetch_optional(pool)
    .await
    .expect("stream_table lookup")
}

#[tokio::test(flavor = "multi_thread")]
async fn first_write_declares_stream_with_bucket_count() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (app, _pg, pool, _wh) = ingest_router(fx, &db).await;

    let res = post_ipc(
        app.clone(),
        "/datasets/main/events?mode=stream&buckets=2",
        ipc_bytes(&sample_batch()),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);

    let bucket_count = stream_bucket_count(&pool, "main", "events").await;
    assert_eq!(
        bucket_count,
        Some(2),
        "the first write declares the log table with the requested bucket count"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn bucket_count_mismatch_on_second_write_is_400() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (app, _pg, pool, _wh) = ingest_router(fx, &db).await;

    let res = post_ipc(
        app.clone(),
        "/datasets/main/events?mode=stream&buckets=2",
        ipc_bytes(&sample_batch()),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);

    let res = post_ipc(
        app.clone(),
        "/datasets/main/events?mode=stream&buckets=3",
        ipc_bytes(&sample_batch()),
    )
    .await;
    assert_eq!(
        res.status(),
        StatusCode::BAD_REQUEST,
        "a conflicting bucket count on an already-declared stream table is rejected"
    );

    // The declared bucket count is unchanged by the rejected write.
    let bucket_count = stream_bucket_count(&pool, "main", "events").await;
    assert_eq!(bucket_count, Some(2));
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_mode_on_existing_batch_table_is_400() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (app, _pg, pool, _wh) = ingest_router(fx, &db).await;

    // Land as a plain batch table first.
    let res = post_ipc(
        app.clone(),
        "/datasets/main/customer",
        ipc_bytes(&sample_batch()),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);

    let res = post_ipc(
        app.clone(),
        "/datasets/main/customer?mode=stream&buckets=2",
        ipc_bytes(&sample_batch()),
    )
    .await;
    assert_eq!(
        res.status(),
        StatusCode::BAD_REQUEST,
        "converting an existing batch table to a stream table is rejected"
    );

    let bucket_count = stream_bucket_count(&pool, "main", "customer").await;
    assert_eq!(
        bucket_count, None,
        "the rejected conversion attempt leaves the table as batch"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn plain_write_stays_batch_with_no_stream_table_row() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (app, _pg, pool, _wh) = ingest_router(fx, &db).await;

    let res = post_ipc(
        app.clone(),
        "/datasets/main/customer",
        ipc_bytes(&sample_batch()),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);

    let bucket_count = stream_bucket_count(&pool, "main", "customer").await;
    assert_eq!(
        bucket_count, None,
        "a plain write (no mode) never creates a stream.stream_table row"
    );
}
