//! e2e: `/datasets/{schema}/{table}/preview` under type-fallback governance, served by
//! the real in-process Iceberg/DataFusion engine — row filters exclude rows, masked
//! columns render the mask marker, denied columns leave the projection, and a direct
//! Table grant still samples raw. Complements the route-level SQL-shape tests
//! (`datasets_routes.rs`) by proving the compiled SQL actually executes.

use std::sync::Arc;

use axum::http::StatusCode;
use control_plane_core::{Acl, Action, CompareOp, Effect, PolicyTarget, RowFilter, ScalarValue};
use control_plane_postgres::fixture::PgFixture;
use e2e_support::{
    get, grant_read_columns, grant_read_filtered, setup_iceberg, subject_with_role, tref,
};

/// Sort the string-rendered rows for order-independent assertion (preview has no
/// ORDER BY; the engine's scan order is not part of the contract).
fn sorted_rows(body: &serde_json::Value) -> Vec<Vec<String>> {
    let mut rows: Vec<Vec<String>> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| {
            r.as_array()
                .unwrap()
                .iter()
                .map(|c| c.as_str().unwrap().to_string())
                .collect()
        })
        .collect();
    rows.sort();
    rows
}

#[tokio::test(flavor = "multi_thread")]
async fn row_filtered_type_grant_limits_preview_rows() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup_iceberg(fx).await;
    let (_subj, role) = subject_with_role(&cp, "analyst").await;
    grant_read_filtered(
        &cp,
        &role,
        "Customer",
        RowFilter::Compare {
            property: "region".into(),
            op: CompareOp::Eq,
            value: ScalarValue::Text("CA".into()),
        },
    )
    .await;
    let cp = Arc::new(cp);
    let (status, body) = get(
        cp,
        eng,
        "/datasets/main/customer/preview?limit=10",
        "analyst",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["columns"], serde_json::json!(["id", "region"]));
    assert_eq!(
        sorted_rows(&body),
        vec![vec!["1".to_string(), "CA".to_string()]]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn masked_column_previews_as_marker() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup_iceberg(fx).await;
    let (_subj, role) = subject_with_role(&cp, "analyst").await;
    grant_read_columns(&cp, &role, "Customer", vec![], vec!["region".into()]).await;
    let cp = Arc::new(cp);
    let (status, body) = get(
        cp,
        eng,
        "/datasets/main/customer/preview?limit=10",
        "analyst",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["columns"], serde_json::json!(["id", "region"]));
    assert_eq!(
        sorted_rows(&body),
        vec![
            vec!["1".to_string(), "***".to_string()],
            vec!["2".to_string(), "***".to_string()],
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn denied_column_leaves_preview_projection() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup_iceberg(fx).await;
    let (_subj, role) = subject_with_role(&cp, "analyst").await;
    grant_read_columns(&cp, &role, "Customer", vec!["region".into()], vec![]).await;
    let cp = Arc::new(cp);
    let (status, body) = get(
        cp,
        eng,
        "/datasets/main/customer/preview?limit=10",
        "analyst",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["columns"], serde_json::json!(["id"]));
    assert_eq!(
        sorted_rows(&body),
        vec![vec!["1".to_string()], vec!["2".to_string()]]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn table_grant_previews_raw_rows() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup_iceberg(fx).await;
    let (_subj, role) = subject_with_role(&cp, "analyst").await;
    cp.grant(
        &role,
        Action::Read,
        PolicyTarget::Table(tref("main", "customer")),
        Effect::Allow,
    )
    .await
    .unwrap();
    let cp = Arc::new(cp);
    let (status, body) = get(
        cp,
        eng,
        "/datasets/main/customer/preview?limit=10",
        "analyst",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["columns"], serde_json::json!(["id", "region"]));
    assert_eq!(
        sorted_rows(&body),
        vec![
            vec!["1".to_string(), "CA".to_string()],
            vec!["2".to_string(), "NY".to_string()],
        ]
    );
}
