//! End-to-end governed `POST /search/{type}/{index}` kNN matrix.
//!
//! Drives the full wire path (router → auth gate → governed `vector_search` →
//! in-process Iceberg/DataFusion serving engine over a freshly seeded `Docs`
//! vector type) and asserts ranking, the governance gates (no-grant / unknown
//! type / no-index), request validation (k bounds / malformed / dim mismatch),
//! row-filter post-filtering, and the index-tuning knobs.

use std::sync::Arc;

use control_plane_core::{
    Acl, Action, CompareOp, Policy, PolicyTarget, RowFilter, ScalarValue, TypeName,
};
use control_plane_postgres::fixture::PgFixture;
use e2e_support::{
    grant_read, grant_read_columns, grant_read_filtered, post_search, seed_vector_type,
    subject_with_role,
};

use axum::http::StatusCode;

/// Pull the `results` array, asserting a 200.
fn results(status: StatusCode, body: &serde_json::Value) -> Vec<serde_json::Value> {
    assert_eq!(status, StatusCode::OK, "body: {body}");
    body["results"].as_array().expect("results array").clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_returns_ranked_ids_for_permitted_subject() {
    let fx = PgFixture::start();
    let (_init, db) = fx.fresh_db().await;
    let (cp, serving, _writer) = seed_vector_type(&fx, &db).await;
    let (_subj, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Docs").await;
    let cp = Arc::new(cp);

    let (status, body) = post_search(
        cp.clone(),
        serving.clone(),
        "/search/Docs/by_sim",
        &serde_json::json!({ "query": [1.0, 0.0, 0.0, 0.0], "k": 2 }),
        "alice",
    )
    .await;

    let res = results(status, &body);
    assert_eq!(res.len(), 2, "k=2 hits");
    assert_eq!(res[0]["id"], serde_json::json!(1), "exact match id=1 first");
    let d0 = res[0]["distance"].as_f64().expect("distance 0");
    let d1 = res[1]["distance"].as_f64().expect("distance 1");
    assert!(d0 <= d1, "distances ascending: {d0} <= {d1}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_forbidden_without_read_grant() {
    let fx = PgFixture::start();
    let (_init, db) = fx.fresh_db().await;
    let (cp, serving, _writer) = seed_vector_type(&fx, &db).await;
    // role exists but no grant_read.
    let (_subj, _role) = subject_with_role(&cp, "mallory").await;
    let cp = Arc::new(cp);

    let (status, _body) = post_search(
        cp.clone(),
        serving.clone(),
        "/search/Docs/by_sim",
        &serde_json::json!({ "query": [1.0, 0.0, 0.0, 0.0], "k": 2 }),
        "mallory",
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_unknown_type_is_forbidden_no_leak() {
    let fx = PgFixture::start();
    let (_init, db) = fx.fresh_db().await;
    let (cp, serving, _writer) = seed_vector_type(&fx, &db).await;
    // Granted on Docs, but probing a type that does not exist.
    let (_subj, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Docs").await;
    let cp = Arc::new(cp);

    let (status, _body) = post_search(
        cp.clone(),
        serving.clone(),
        "/search/Nope/by_sim",
        &serde_json::json!({ "query": [1.0, 0.0, 0.0, 0.0], "k": 2 }),
        "alice",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "unknown type must not leak as 404"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_missing_index_is_not_found() {
    let fx = PgFixture::start();
    let (_init, db) = fx.fresh_db().await;
    let (cp, serving, _writer) = seed_vector_type(&fx, &db).await;
    let (_subj, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Docs").await;
    let cp = Arc::new(cp);

    let (status, _body) = post_search(
        cp.clone(),
        serving.clone(),
        "/search/Docs/missing",
        &serde_json::json!({ "query": [1.0, 0.0, 0.0, 0.0], "k": 2 }),
        "alice",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "no built index named `missing`"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_bad_requests_are_rejected() {
    let fx = PgFixture::start();
    let (_init, db) = fx.fresh_db().await;
    let (cp, serving, _writer) = seed_vector_type(&fx, &db).await;
    let (_subj, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Docs").await;
    let cp = Arc::new(cp);

    // (1) malformed body: missing required `k`.
    let (status, _b) = post_search(
        cp.clone(),
        serving.clone(),
        "/search/Docs/by_sim",
        &serde_json::json!({ "query": [1.0, 0.0, 0.0, 0.0] }),
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "missing k -> 400");

    // (2) k = 0.
    let (status, _b) = post_search(
        cp.clone(),
        serving.clone(),
        "/search/Docs/by_sim",
        &serde_json::json!({ "query": [1.0, 0.0, 0.0, 0.0], "k": 0 }),
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "k=0 -> 400");

    // (3) k > K_MAX (1000).
    let (status, _b) = post_search(
        cp.clone(),
        serving.clone(),
        "/search/Docs/by_sim",
        &serde_json::json!({ "query": [1.0, 0.0, 0.0, 0.0], "k": 1001 }),
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "k=1001 -> 400");

    // (4) dim mismatch: index dim is 4, query has length 3 -> engine DimMismatch -> 400.
    let (status, _b) = post_search(
        cp.clone(),
        serving.clone(),
        "/search/Docs/by_sim",
        &serde_json::json!({ "query": [1.0, 0.0, 0.0], "k": 2 }),
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "dim mismatch -> 400");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn row_filter_drops_nearest_hit() {
    let fx = PgFixture::start();
    let (_init, db) = fx.fresh_db().await;
    let (cp, serving, _writer) = seed_vector_type(&fx, &db).await;
    let (_subj, role) = subject_with_role(&cp, "carol").await;
    // Read on Docs but with a row filter that excludes the exact match (id = 1).
    grant_read_filtered(
        &cp,
        &role,
        "Docs",
        RowFilter::Compare {
            property: "id".into(),
            op: CompareOp::Gt,
            value: ScalarValue::Int(1),
        },
    )
    .await;
    let cp = Arc::new(cp);

    let (status, body) = post_search(
        cp.clone(),
        serving.clone(),
        "/search/Docs/by_sim",
        &serde_json::json!({ "query": [1.0, 0.0, 0.0, 0.0], "k": 4 }),
        "carol",
    )
    .await;

    let res = results(status, &body);
    assert!(
        res.iter().all(|h| h["id"] != serde_json::json!(1)),
        "row filter id>1 must drop the nearest hit id=1: {body}"
    );
    assert!(!res.is_empty(), "surviving rows remain after the filter");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_accepts_tuning_knobs() {
    let fx = PgFixture::start();
    let (_init, db) = fx.fresh_db().await;
    let (cp, serving, _writer) = seed_vector_type(&fx, &db).await;
    let (_subj, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Docs").await;
    let cp = Arc::new(cp);

    // nprobe knob (Flat ignores it; a 200 with a valid ranked hit is the assertion).
    let (status, body) = post_search(
        cp.clone(),
        serving.clone(),
        "/search/Docs/by_sim",
        &serde_json::json!({ "query": [1.0, 0.0, 0.0, 0.0], "k": 1, "nprobe": 2 }),
        "alice",
    )
    .await;
    let res = results(status, &body);
    assert_eq!(res.len(), 1);
    assert_eq!(
        res[0]["id"],
        serde_json::json!(1),
        "nprobe knob still ranks"
    );

    // ef_search knob.
    let (status, body) = post_search(
        cp.clone(),
        serving.clone(),
        "/search/Docs/by_sim",
        &serde_json::json!({ "query": [1.0, 0.0, 0.0, 0.0], "k": 1, "ef_search": 64 }),
        "alice",
    )
    .await;
    let res = results(status, &body);
    assert_eq!(res.len(), 1);
    assert_eq!(
        res[0]["id"],
        serde_json::json!(1),
        "ef_search knob still ranks"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_forbidden_when_identity_denied_no_row_filter() {
    let fx = PgFixture::start();
    let (_init, db) = fx.fresh_db().await;
    let (cp, serving, _writer) = seed_vector_type(&fx, &db).await;
    let (_subj, role) = subject_with_role(&cp, "dave").await;
    // Coarse Read + deny the identity column `id`, no row filter.
    grant_read_columns(&cp, &role, "Docs", vec!["id".into()], vec![]).await;
    let cp = Arc::new(cp);

    let (status, _body) = post_search(
        cp.clone(),
        serving.clone(),
        "/search/Docs/by_sim",
        &serde_json::json!({ "query": [1.0, 0.0, 0.0, 0.0], "k": 2 }),
        "dave",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "denied identity column must fail closed, not leak ids"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_forbidden_when_identity_masked_no_row_filter() {
    let fx = PgFixture::start();
    let (_init, db) = fx.fresh_db().await;
    let (cp, serving, _writer) = seed_vector_type(&fx, &db).await;
    let (_subj, role) = subject_with_role(&cp, "erin").await;
    // Coarse Read + mask the identity column `id`, no row filter.
    grant_read_columns(&cp, &role, "Docs", vec![], vec!["id".into()]).await;
    let cp = Arc::new(cp);

    let (status, _body) = post_search(
        cp.clone(),
        serving.clone(),
        "/search/Docs/by_sim",
        &serde_json::json!({ "query": [1.0, 0.0, 0.0, 0.0], "k": 2 }),
        "erin",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "masked identity column must fail closed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_forbidden_when_identity_governed_with_row_filter() {
    let fx = PgFixture::start();
    let (_init, db) = fx.fresh_db().await;
    let (cp, serving, _writer) = seed_vector_type(&fx, &db).await;
    let (_subj, role) = subject_with_role(&cp, "frank").await;
    // Coarse Read first, then a policy that BOTH denies `id` AND carries a row filter.
    // Previously this path returned an incidental BadFilter/500; now a deliberate 403.
    grant_read(&cp, &role, "Docs").await;
    cp.set_policy(
        &role,
        Action::Read,
        Policy {
            target: PolicyTarget::Type(TypeName("Docs".into())),
            row_filter: Some(RowFilter::Compare {
                property: "id".into(),
                op: CompareOp::Gt,
                value: ScalarValue::Int(0),
            }),
            deny_columns: vec!["id".into()],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();
    let cp = Arc::new(cp);

    let (status, _body) = post_search(
        cp.clone(),
        serving.clone(),
        "/search/Docs/by_sim",
        &serde_json::json!({ "query": [1.0, 0.0, 0.0, 0.0], "k": 2 }),
        "frank",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "governed identity + row filter must also be a deliberate 403 (symmetry)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_ok_when_identity_ungoverned_regression() {
    let fx = PgFixture::start();
    let (_init, db) = fx.fresh_db().await;
    let (cp, serving, _writer) = seed_vector_type(&fx, &db).await;
    let (_subj, role) = subject_with_role(&cp, "grace").await;
    // Plain Read, identity column not governed → unchanged behavior, value-exact hits.
    grant_read(&cp, &role, "Docs").await;
    let cp = Arc::new(cp);

    let (status, body) = post_search(
        cp.clone(),
        serving.clone(),
        "/search/Docs/by_sim",
        &serde_json::json!({ "query": [1.0, 0.0, 0.0, 0.0], "k": 2 }),
        "grace",
    )
    .await;
    let res = results(status, &body);
    assert_eq!(
        res.len(),
        2,
        "ungoverned identity returns kNN hits unchanged"
    );
    assert_eq!(res[0]["id"], serde_json::json!(1), "exact match id=1 first");
}
