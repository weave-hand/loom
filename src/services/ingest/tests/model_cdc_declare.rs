//! `?mode=cdc&buckets=N` on `POST /models/{type}`: declares an identity-bearing
//! type's table as a PK/CDC stream table (a `kind='cdc'` `stream.stream_table` row
//! keyed on the type's identity), in the SAME transaction as the first write. A
//! type with no declared identity, or a `mode=cdc` request against a table already
//! landed as batch, is rejected with 400. Hermetic Postgres fixture; tower
//! oneshot, no socket. Auth/ACL + raw-SQL readback harness comes from
//! `tests/e2e_support.rs`.

use axum::http::StatusCode;
use control_plane_postgres::fixture::PgFixture;
use e2e_support::{
    app_state, grant_write_absent_type, ipc_bytes, post_model_q, protected, sample_batch,
    session_token, stream_meta_row,
};

#[tokio::test(flavor = "multi_thread")]
async fn mode_cdc_with_identity_declares_a_cdc_table() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, _wh, state) = app_state(fx, &db).await;
    grant_write_absent_type(&pg, &pool, "alice", "widget").await;
    let token = session_token(&pg, "alice").await;

    let app = protected(state, pg.clone());
    let status = post_model_q(
        app,
        "widget",
        "identity=id&mode=cdc&buckets=2",
        &token,
        ipc_bytes(&sample_batch()),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "mode=cdc on an identity-bearing type lands"
    );

    let meta = stream_meta_row(&pool, "main", "widget")
        .await
        .expect("a stream_table row was declared");
    assert_eq!(meta, (2, "cdc".to_string(), Some("id".to_string())));
}

#[tokio::test(flavor = "multi_thread")]
async fn mode_cdc_without_identity_is_400() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, _wh, state) = app_state(fx, &db).await;
    grant_write_absent_type(&pg, &pool, "alice", "gizmo").await;
    let token = session_token(&pg, "alice").await;

    let app = protected(state, pg.clone());
    // No `identity=` — the inferred type has no declared identity, so `mode=cdc`
    // must be rejected before any write.
    let status = post_model_q(
        app,
        "gizmo",
        "mode=cdc&buckets=2",
        &token,
        ipc_bytes(&sample_batch()),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "mode=cdc without a declared identity is rejected"
    );
    assert_eq!(
        stream_meta_row(&pool, "main", "gizmo").await,
        None,
        "the rejected declare leaves no stream_table row"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn mode_cdc_on_existing_batch_table_is_400() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, _wh, state) = app_state(fx, &db).await;
    grant_write_absent_type(&pg, &pool, "alice", "doohickey").await;
    let token = session_token(&pg, "alice").await;

    // First write: declares the identity but no stream intent -> lands as a plain
    // batch table (no stream_table row).
    let app = protected(state.clone(), pg.clone());
    let status = post_model_q(
        app,
        "doohickey",
        "identity=id",
        &token,
        ipc_bytes(&sample_batch()),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(stream_meta_row(&pool, "main", "doohickey").await, None);

    // Second write, now requesting mode=cdc against the already-landed batch table
    // -> rejected (cannot convert an existing batch table to a stream table).
    let app = protected(state, pg.clone());
    let status = post_model_q(
        app,
        "doohickey",
        "mode=cdc&buckets=2",
        &token,
        ipc_bytes(&sample_batch()),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "mode=cdc against an existing batch table is rejected"
    );
    assert_eq!(
        stream_meta_row(&pool, "main", "doohickey").await,
        None,
        "the rejected conversion attempt leaves the table as batch"
    );
}
