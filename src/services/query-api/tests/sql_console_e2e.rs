//! Acceptance e2e for the governed SQL console (`POST /sql`, #621): drives the HTTP
//! route through the auth gate against a REAL engine (landing -> engine over UDS ->
//! `EngineServingClient::execute_governed`), asserting the caller's own arbitrary SQL
//! is governed row-for-row and column-for-column, that a table the subject cannot see
//! (ungranted type) is a 400 (the engine's closed-world catalog makes it
//! indistinguishable from a nonexistent table), that malformed SQL is a 400, that the
//! row cap truncates with a flag, and that an unauthenticated request is a 401.
//!
//! The seed/ACL/engine fixture lives in `e2e_support::governed_sql_harness` — it is
//! shared with `sql_validate_e2e`, which needs the identical governed catalog.

use control_plane_postgres::fixture::PgFixture;
use e2e_support::{GovernedSqlHarness, governed_sql_harness};

/// Drive `POST /sql` as `reader` with the given SQL (+ optional limit).
async fn run(
    h: &GovernedSqlHarness,
    sql: &str,
    limit: Option<u32>,
) -> (axum::http::StatusCode, serde_json::Value) {
    let mut body = serde_json::json!({ "sql": sql });
    if let Some(n) = limit {
        body["limit"] = serde_json::json!(n);
    }
    e2e_support::post_search(h.cp.clone(), h.serving.clone(), "/sql", &body, "reader").await
}

fn str_col(body: &serde_json::Value, col_idx: usize) -> Vec<String> {
    body["rows"]
        .as_array()
        .expect("rows array")
        .iter()
        .map(|r| r[col_idx].as_str().expect("string cell").to_string())
        .collect()
}

/// The row filter (id >= 2) and the email mask both survive an arbitrary client SELECT.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn governed_select_filters_rows_and_masks_columns() {
    let fx = PgFixture::shared();
    let h = governed_sql_harness(fx).await;

    let (status, body) = run(
        &h,
        r#"SELECT "id", "email" FROM "wh"."orders" ORDER BY "id""#,
        None,
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "body: {body}");
    assert_eq!(body["columns"], serde_json::json!(["id", "email"]));
    assert_eq!(body["truncated"], serde_json::json!(false));
    assert_eq!(str_col(&body, 0), vec!["2", "3", "4"], "row filter id >= 2");
    assert_eq!(
        str_col(&body, 1),
        vec!["***", "***", "***"],
        "email column is masked"
    );
}

/// A denied column never appears in `SELECT *`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn governed_select_star_omits_denied_column() {
    let fx = PgFixture::shared();
    let h = governed_sql_harness(fx).await;

    let (status, body) = run(&h, r#"SELECT * FROM "wh"."customers" ORDER BY "id""#, None).await;
    assert_eq!(status, axum::http::StatusCode::OK, "body: {body}");
    let cols: Vec<String> = body["columns"]
        .as_array()
        .expect("columns")
        .iter()
        .map(|c| c.as_str().expect("str").to_string())
        .collect();
    assert!(cols.contains(&"id".to_string()));
    assert!(cols.contains(&"name".to_string()));
    assert!(
        !cols.contains(&"ssn".to_string()),
        "denied ssn column must be absent, got {cols:?}"
    );
}

/// A table the subject holds no grant on is unresolvable — a 400, not an existence oracle.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ungranted_table_is_bad_request() {
    let fx = PgFixture::shared();
    let h = governed_sql_harness(fx).await;

    let (status, _body) = run(&h, r#"SELECT * FROM "wh"."secrets""#, None).await;
    assert_eq!(
        status,
        axum::http::StatusCode::BAD_REQUEST,
        "an ungranted type's table is a closed-world planning error"
    );
}

/// Malformed SQL is a 400 (the engine's plan message is the client's own vocabulary).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_sql_is_bad_request() {
    let fx = PgFixture::shared();
    let h = governed_sql_harness(fx).await;

    let (status, _body) = run(&h, "SELCT 1", None).await;
    assert_eq!(status, axum::http::StatusCode::BAD_REQUEST);
}

/// A `COPY … TO` write (which bypasses the governed table providers) is rejected at
/// planning — the console is read-only by construction, not merely by provider shape.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copy_to_write_is_rejected() {
    let fx = PgFixture::shared();
    let h = governed_sql_harness(fx).await;

    let (status, _body) = run(
        &h,
        "COPY (SELECT 1 AS x) TO 'file:///tmp/loom-sql-console-should-not-exist.csv' STORED AS CSV",
        None,
    )
    .await;
    assert_eq!(
        status,
        axum::http::StatusCode::BAD_REQUEST,
        "COPY ... TO must be rejected as a write, never executed"
    );
}

/// An empty SQL body is a 400.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_sql_is_bad_request() {
    let fx = PgFixture::shared();
    let h = governed_sql_harness(fx).await;

    let (status, _body) = run(&h, "   ", None).await;
    assert_eq!(status, axum::http::StatusCode::BAD_REQUEST);
}

/// A `limit` below the result size truncates and flags it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn limit_below_result_truncates_with_flag() {
    let fx = PgFixture::shared();
    let h = governed_sql_harness(fx).await;

    // The row filter yields 3 rows; a cap of 2 truncates.
    let (status, body) = run(
        &h,
        r#"SELECT "id" FROM "wh"."orders" ORDER BY "id""#,
        Some(2),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "body: {body}");
    assert_eq!(body["rows"].as_array().expect("rows").len(), 2);
    assert_eq!(body["truncated"], serde_json::json!(true));
}

/// An unauthenticated request is a 401 (the auth gate runs before the route).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unauthenticated_is_401() {
    let fx = PgFixture::shared();
    let h = governed_sql_harness(fx).await;

    let status = e2e_support::get_unauth(h.cp.clone(), h.serving.clone(), "/sql").await;
    assert_eq!(status, axum::http::StatusCode::UNAUTHORIZED);
}
