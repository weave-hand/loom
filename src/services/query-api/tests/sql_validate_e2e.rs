//! Acceptance e2e for governed SQL validation (`POST /sql/validate`, #657): drives the
//! HTTP route through the auth gate against a REAL engine, asserting that valid SQL
//! yields no diagnostics, that an unknown *column* yields exactly one carrying
//! DataFusion's own message (the case no client-side heuristic can get right, and the
//! reason this slice exists), that a parse error resolves a line/column, that a table
//! the subject cannot see is a diagnostic rather than a leak or a 500, and that the
//! response never carries plan text.

use control_plane_postgres::fixture::PgFixture;
use e2e_support::{GovernedSqlHarness, governed_sql_harness};

/// Drive `POST /sql/validate` as `reader`.
async fn validate(
    h: &GovernedSqlHarness,
    sql: &str,
) -> (axum::http::StatusCode, serde_json::Value) {
    let body = serde_json::json!({ "sql": sql });
    e2e_support::post_search(
        h.cp.clone(),
        h.serving.clone(),
        "/sql/validate",
        &body,
        "reader",
    )
    .await
}

fn diagnostics(body: &serde_json::Value) -> &Vec<serde_json::Value> {
    body["diagnostics"].as_array().expect("diagnostics array")
}

/// Assert `body` carries no fragment of an `EXPLAIN` plan. The endpoint plans with
/// `EXPLAIN`, whose output names file paths and storage layout; returning any of it
/// would be a governance leak, so the refusal is asserted on every response shape.
fn assert_no_plan_text(body: &serde_json::Value) {
    let raw = body.to_string();
    for marker in [
        "ExecutionPlan",
        "DataSourceExec",
        "logical_plan",
        "physical_plan",
        "parquet",
    ] {
        assert!(
            !raw.contains(marker),
            "plan text leaked into the response ({marker}): {raw}"
        );
    }
    // Structural, not just substring: the body has exactly one key.
    assert_eq!(
        body.as_object().expect("object body").len(),
        1,
        "the response carries `diagnostics` and nothing else: {raw}"
    );
}

/// Valid SQL over a granted table plans cleanly: 200, no diagnostics.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn valid_sql_has_no_diagnostics() {
    let fx = PgFixture::shared();
    let h = governed_sql_harness(fx).await;

    let (status, body) = validate(&h, r#"SELECT "id" FROM "wh"."orders""#).await;
    assert_eq!(status, axum::http::StatusCode::OK, "body: {body}");
    assert!(
        diagnostics(&body).is_empty(),
        "expected no diagnostics, got {body}"
    );
}

/// An unknown COLUMN is caught — the authoritative check a client-side heuristic
/// cannot do, since it needs full alias/scope resolution.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_column_yields_one_diagnostic() {
    let fx = PgFixture::shared();
    let h = governed_sql_harness(fx).await;

    let (status, body) = validate(&h, r#"SELECT "nope" FROM "wh"."orders""#).await;
    assert_eq!(
        status,
        axum::http::StatusCode::OK,
        "invalid SQL is a valid request: {body}"
    );
    let ds = diagnostics(&body);
    assert_eq!(ds.len(), 1, "expected exactly one diagnostic, got {body}");
    let msg = ds[0]["message"].as_str().expect("message");
    assert!(
        msg.contains("nope"),
        "the engine's own message must name the unknown column, got {msg}"
    );
    assert_eq!(ds[0]["severity"], serde_json::json!("error"));
    // The diagnostic-bearing arm must not leak plan text either — this is the arm
    // `response_never_contains_plan_text` cannot reach, since that one plans cleanly.
    assert_no_plan_text(&body);
}

/// A parse error resolves a typed line/column out of the engine's message — proving
/// the extractor, not just the passthrough.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parse_error_resolves_a_position() {
    let fx = PgFixture::shared();
    let h = governed_sql_harness(fx).await;

    let (status, body) = validate(&h, "SELCT 1").await;
    assert_eq!(status, axum::http::StatusCode::OK, "body: {body}");
    let ds = diagnostics(&body);
    assert_eq!(ds.len(), 1, "expected one diagnostic, got {body}");
    assert_eq!(ds[0]["line"], serde_json::json!(1), "body: {body}");
    // 1, not 9: the endpoint plans `EXPLAIN SELCT 1`, where the offending token sits at
    // column 9, and shifts the position back into the caller's own coordinates. An
    // assertion of `is_some()` here would pass with the un-shifted 9 and leave every
    // squiggle in the editor 8 columns to the right of the problem.
    assert_eq!(
        ds[0]["start_col"],
        serde_json::json!(1),
        "the reported column must be the caller's, not the wrapped statement's: {body}"
    );
}

/// A table the subject holds no grant on is unresolvable — a diagnostic, NOT a 200
/// with an empty list (which would confirm the table exists) and NOT a 500.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ungranted_table_is_a_diagnostic_not_a_leak() {
    let fx = PgFixture::shared();
    let h = governed_sql_harness(fx).await;

    let (status, body) = validate(&h, r#"SELECT * FROM "wh"."secrets""#).await;
    assert_eq!(
        status,
        axum::http::StatusCode::OK,
        "closed-world miss is a diagnostic, not a server fault: {body}"
    );
    assert_eq!(
        diagnostics(&body).len(),
        1,
        "an ungranted table must be indistinguishable from a nonexistent one: {body}"
    );

    // And a table that genuinely does not exist produces the same shape, so the
    // response cannot be used as an existence oracle.
    let (nonexistent_status, nonexistent) =
        validate(&h, r#"SELECT * FROM "wh"."no_such_table_at_all""#).await;
    assert_eq!(nonexistent_status, axum::http::StatusCode::OK);
    assert_eq!(diagnostics(&nonexistent).len(), 1);
}

/// The response NEVER carries plan text. `EXPLAIN` returns the physical plan, which
/// names file paths and storage layout; this endpoint returns diagnostics only.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn response_never_contains_plan_text() {
    let fx = PgFixture::shared();
    let h = governed_sql_harness(fx).await;

    let (status, body) = validate(&h, r#"SELECT "id" FROM "wh"."orders""#).await;
    assert_eq!(status, axum::http::StatusCode::OK);
    assert!(diagnostics(&body).is_empty(), "body: {body}");
    assert_no_plan_text(&body);
}

/// An empty buffer is nothing to say, not a fault — the debounced editor sends it
/// routinely.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_sql_has_no_diagnostics() {
    let fx = PgFixture::shared();
    let h = governed_sql_harness(fx).await;

    let (status, body) = validate(&h, "   ").await;
    assert_eq!(status, axum::http::StatusCode::OK, "body: {body}");
    assert!(diagnostics(&body).is_empty());
}

/// An unauthenticated request is a 401 (the auth gate runs before the route).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unauthenticated_is_401() {
    let fx = PgFixture::shared();
    let h = governed_sql_harness(fx).await;

    let status = e2e_support::get_unauth(h.cp.clone(), h.serving.clone(), "/sql/validate").await;
    assert_eq!(status, axum::http::StatusCode::UNAUTHORIZED);
}
