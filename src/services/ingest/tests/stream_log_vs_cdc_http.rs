//! Cross-surface repro for iss-stream-log-vs-cdc-declare: declare a CDC table
//! via POST /models/{type}?mode=cdc, then attempt a matching-count
//! POST /datasets/{schema}/{table}?mode=stream against the SAME physical table.
//! Must 400 with the kind-mismatch message; the registry row stays kind='cdc'.
//! Hermetic Postgres fixture; tower oneshot, no socket. Auth + raw-SQL readback
//! harness comes from `tests/e2e_support.rs`.

use axum::http::StatusCode;
use control_plane_postgres::fixture::PgFixture;
use e2e_support::{
    app_state, grant_write_absent_type, ipc_bytes, post_dataset_q, post_model_q, protected,
    sample_batch, session_token, stream_meta_row,
};

#[tokio::test(flavor = "multi_thread")]
async fn mode_stream_matching_count_against_cdc_table_is_400() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, _wh, state) = app_state(fx, &db).await;
    grant_write_absent_type(&pg, &pool, "alice", "widget").await;
    let token = session_token(&pg, "alice").await;

    // Surface 1: declare main.widget as CDC (buckets=2) via the models path.
    let app = protected(state.clone(), pg.clone());
    let status = post_model_q(
        app,
        "widget",
        "identity=id&mode=cdc&buckets=2",
        &token,
        ipc_bytes(&sample_batch()),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "mode=cdc declare lands");
    assert_eq!(
        stream_meta_row(&pool, "main", "widget").await,
        Some((2, "cdc".to_string(), Some("id".to_string())))
    );

    // Surface 2: the SAME physical table via the datasets path, mode=stream
    // with the MATCHING bucket count — the silently-accepted case before the
    // fix. Must 400 with the kind-mismatch message.
    let app = protected(state, pg.clone());
    let (status, body) = post_dataset_q(
        app,
        "main",
        "widget",
        "mode=stream&buckets=2",
        &token,
        ipc_bytes(&sample_batch()),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "mode=stream against a cdc table is rejected (body: {body})"
    );
    assert!(
        body.contains("different stream kind"),
        "the kind-mismatch message is echoed to the client, got: {body}"
    );

    // The rejected write changed nothing: still kind='cdc', count 2, key id.
    assert_eq!(
        stream_meta_row(&pool, "main", "widget").await,
        Some((2, "cdc".to_string(), Some("id".to_string()))),
        "the rejected log declare leaves the cdc registry row untouched"
    );
}
