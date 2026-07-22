//! The GET /objects/:type filter path renders an uncoercible value as a structured 400
//! body { error, column, expected, value }, while a visibility denial (a denied filter
//! column) stays a bare-column 400 that echoes no value. In-memory: the coercion / column
//! check fires before any serving call, so a stub engine suffices (no fixture).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use control_plane_core::{
    Acl, Action, ControlPlane, Effect, ObjectType, Ontology, Policy, PolicyTarget, PropertyDef,
    RoleId, SnapshotId, SubjectId, TableRef, TypeName,
};
use control_plane_memory::MemoryControlPlane;
use http_body_util::BodyExt;
use query_api::http::{AppState, router};
use query_api::serving::{ActionEngine, Rows, ServingEngine, ServingError, SqlValue};
use service_runtime::Subject;
use tower::ServiceExt;

struct StubServing;

#[async_trait]
impl ServingEngine for StubServing {
    async fn fetch_rows(
        &self,
        _sql: &str,
        _params: &[SqlValue],
        _at: Option<control_plane_core::SnapshotId>,
    ) -> Result<Rows, ServingError> {
        Ok(Rows {
            columns: vec![],
            rows: vec![],
        })
    }
}

struct StubAction;

#[async_trait]
impl ActionEngine for StubAction {
    async fn write_object(
        &self,
        _table: &TableRef,
        _columns: &[String],
        _values: &[SqlValue],
        _logical_types: &[String],
        _nullable: &[bool],
        _event: control_plane_core::LineageEvent,
        _jobs: &[control_plane_core::NewJob],
    ) -> Result<SnapshotId, ServingError> {
        Err(ServingError::Engine("unused".into()))
    }

    async fn overwrite_table(
        &self,
        _table: &TableRef,
        _columns: &[String],
        _rows: &[Vec<SqlValue>],
        _logical_types: &[String],
        _nullable: &[bool],
        _event: control_plane_core::LineageEvent,
        _jobs: &[control_plane_core::NewJob],
    ) -> Result<SnapshotId, ServingError> {
        Err(ServingError::Engine("unused".into()))
    }
}

fn prop(name: &str, ty: &str) -> PropertyDef {
    PropertyDef::new(name, ty)
}

/// Seed `Order(id long identity, amount double)` and grant coarse Read to `analyst`.
/// When `deny_amount` is set, also attach a Read policy denying the `amount` column.
async fn seed(deny_amount: bool) -> MemoryControlPlane {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    cp.define_type(
        ObjectType::build("Order", ("main", "order"))
            .add_prop(prop("id", "long"))
            .add_prop(prop("amount", "double"))
            .identity("id")
            .done(),
    )
    .await
    .unwrap();

    let analyst = SubjectId("analyst".into());
    let reader = RoleId("reader".into());
    cp.define_subject(&analyst).await.unwrap();
    cp.define_role(&reader).await.unwrap();
    cp.assign_role(&analyst, &reader).await.unwrap();
    cp.grant(
        &reader,
        Action::Read,
        PolicyTarget::Type(TypeName("Order".into())),
        Effect::Allow,
    )
    .await
    .unwrap();
    if deny_amount {
        cp.set_policy(
            &reader,
            Action::Read,
            Policy {
                target: PolicyTarget::Type(TypeName("Order".into())),
                row_filter: None,
                deny_columns: vec!["amount".into()],
                mask_columns: vec![],
            },
        )
        .await
        .unwrap();
    }
    cp
}

async fn get(cp: MemoryControlPlane, subject: &str, uri: &str) -> (StatusCode, String) {
    let state = AppState {
        cp: Arc::new(cp) as Arc<dyn ControlPlane>,
        serving: Arc::new(StubServing),
        action_engine: Arc::new(StubAction),
        default_limit: 1000,
        gc_retention: std::time::Duration::from_secs(7 * 24 * 3600),
        naming: query_api::lineage_filter::local_naming(),
    };
    let app = router(state);
    let mut req = Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    req.extensions_mut()
        .insert(Subject(SubjectId(subject.into())));
    let res = app.oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&bytes).to_string())
}

#[tokio::test(flavor = "multi_thread")]
async fn uncoercible_value_is_structured_400() {
    // Regression guard for iss-qa-search-badfiltervalue-classification: this caller-value
    // coercion fault must stay a 400 — the reclassification only touches the two
    // ENGINE-derived seams in `vector_search`'s post-filter, not this caller-filter path.
    let cp = seed(false).await;
    let (status, body) = get(cp, "analyst", "/objects/Order?amount=gt:abc").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
    let json: serde_json::Value = serde_json::from_str(&body).expect("structured JSON body");
    assert_eq!(json["error"], "bad_filter_value");
    assert_eq!(json["column"], "amount");
    assert_eq!(json["expected"], "double");
    assert_eq!(json["value"], "abc");
}

#[tokio::test(flavor = "multi_thread")]
async fn valid_filter_is_unaffected() {
    let cp = seed(false).await;
    let (status, body) = get(cp, "analyst", "/objects/Order?amount=gt:5").await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
}

#[tokio::test(flavor = "multi_thread")]
async fn denied_column_filter_is_400_without_value_echo() {
    // Filtering on a denied column is a visibility signal (BadFilter), NOT a coercion error:
    // it stays a bare-column 400 and never echoes the caller's value.
    let cp = seed(true).await;
    let (status, body) = get(cp, "analyst", "/objects/Order?amount=gt:99").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
    assert_eq!(body, "amount");
    assert!(!body.contains("99"), "must not echo the filtered value");
    assert!(
        !body.contains("bad_filter_value"),
        "a visibility denial is not a coercion error"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn denied_column_uncoercible_value_stays_bad_filter() {
    // The visibility check runs BEFORE coercion, so a denied column with an *uncoercible*
    // value is still a bare-column BadFilter 400 — it must NOT leak the column's type via a
    // structured `bad_filter_value` body, and must not echo the caller's value.
    let cp = seed(true).await;
    let (status, body) = get(cp, "analyst", "/objects/Order?amount=gt:abc").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
    assert_eq!(body, "amount");
    assert!(
        !body.contains("bad_filter_value"),
        "must not leak a coercion body"
    );
    assert!(!body.contains("abc"), "must not echo the caller value");
    assert!(
        !body.contains("double"),
        "must not leak the column's declared type"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn ids_uncoercible_value_is_400() {
    // Caller-supplied `_ids` failing coercion against the declared identity type is the same
    // caller-value fault class as an uncoercible filter value (see
    // `uncoercible_value_is_structured_400` above) — a caller-facing 400, unaffected by the
    // engine-derived-value reclassification in `vector_search`'s post-filter (iss-qa-search-
    // badfiltervalue-classification), which only wraps the two ENGINE-derived seams.
    let cp = seed(false).await;
    let (status, body) = get(cp, "analyst", "/objects/Order?_ids=notanint").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
    let json: serde_json::Value = serde_json::from_str(&body).expect("structured JSON body");
    assert_eq!(json["error"], "bad_filter_value");
}

#[tokio::test(flavor = "multi_thread")]
async fn grammar_fault_is_structured_400_column_only() {
    // A predicate-grammar fault (empty set operand) is `FilterError::BadValue` — a structured
    // 400 carrying {error, column} but no expected/value (there is no single offending value).
    let cp = seed(false).await;
    let (status, body) = get(cp, "analyst", "/objects/Order?amount=in:1,,3").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
    let json: serde_json::Value = serde_json::from_str(&body).expect("structured JSON body");
    assert_eq!(json["error"], "bad_filter_value");
    assert_eq!(json["column"], "amount");
    assert!(
        json.get("expected").is_none(),
        "grammar fault has no expected type"
    );
    assert!(
        json.get("value").is_none(),
        "grammar fault has no offending value"
    );
}
