//! Route tests: GET /datasets lists the live mirror tables and
//! GET /datasets/{schema}/{table} composes the current snapshot with the schema at it;
//! an unknown table 404s. No socket is bound (tower oneshot); the catalog is seeded via
//! the memory fake's `seed_catalog` (the pg `list_tables` path is contract-tested).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use control_plane_core::{
    Acl, Action, CompareOp, ControlPlane, Effect, ObjectType, PolicyTarget, RoleId, RowFilter,
    ScalarValue, SubjectId, TableRef, TypeName, ViewDef,
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
        _event: control_plane_core::LineageEvent,
        _jobs: &[control_plane_core::NewJob],
    ) -> std::result::Result<control_plane_core::SnapshotId, ServingError> {
        Ok(control_plane_core::SnapshotId(0))
    }

    async fn overwrite_table(
        &self,
        _table: &TableRef,
        _columns: &[String],
        _rows: &[Vec<SqlValue>],
        _logical_types: &[String],
        _event: control_plane_core::LineageEvent,
        _jobs: &[control_plane_core::NewJob],
    ) -> std::result::Result<control_plane_core::SnapshotId, ServingError> {
        Err(ServingError::Engine("overwrite_table unsupported".into()))
    }
}

fn app(cp: MemoryControlPlane) -> axum::Router {
    router(AppState {
        cp: Arc::new(cp),
        serving: Arc::new(StubServing),
        action_engine: Arc::new(StubAction),
        default_limit: 1000,
        gc_retention: std::time::Duration::from_secs(7 * 24 * 3600),
        naming: query_api::lineage_filter::local_naming(),
    })
}

struct CannedServing;

#[async_trait]
impl ServingEngine for CannedServing {
    async fn fetch_rows(
        &self,
        _sql: &str,
        _params: &[SqlValue],
        _at: Option<control_plane_core::SnapshotId>,
    ) -> Result<Rows, ServingError> {
        Ok(Rows {
            columns: vec!["id".into(), "note".into()],
            rows: vec![
                vec![SqlValue::Int(1), SqlValue::Text("a".into())],
                vec![SqlValue::Int(2), SqlValue::Null],
            ],
        })
    }
}

fn app_canned(cp: MemoryControlPlane) -> axum::Router {
    router(AppState {
        cp: Arc::new(cp),
        serving: Arc::new(CannedServing),
        action_engine: Arc::new(StubAction),
        default_limit: 1000,
        gc_retention: std::time::Duration::from_secs(7 * 24 * 3600),
        naming: query_api::lineage_filter::local_naming(),
    })
}

async fn get(app: &axum::Router, uri: &str) -> (StatusCode, serde_json::Value) {
    let mut req = Request::builder().uri(uri).body(Body::empty()).unwrap();
    req.extensions_mut()
        .insert(Subject(SubjectId("analyst".into())));
    let res = app.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let json = serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
    (status, json)
}

/// Like `get`, but returns the raw response body text instead of JSON-decoding it —
/// needed for the 404-oracle tests below, which must assert byte-identical bodies
/// rather than two bodies that both happen to fail JSON decoding to `Null`.
async fn get_raw(app: &axum::Router, uri: &str) -> (StatusCode, String) {
    let mut req = Request::builder().uri(uri).body(Body::empty()).unwrap();
    req.extensions_mut()
        .insert(Subject(SubjectId("analyst".into())));
    let res = app.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8(body.to_vec()).expect("404 body must be valid UTF-8");
    (status, text)
}

/// Seed `main.events` (two columns, two append snapshots); returns the cp and the
/// latest (second) snapshot id.
fn seeded() -> (MemoryControlPlane, i64) {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    let table = TableRef {
        schema: "main".into(),
        name: "events".into(),
    };
    let cols = vec![
        ("id".to_string(), "Long".to_string(), false),
        ("note".to_string(), "String".to_string(), true),
    ];
    let snapshots = cp.seed_catalog(&table, &cols, &[3, 2]);
    let latest = snapshots.last().expect("seeded snapshots").0;
    (cp, latest)
}

/// Give `analyst` a role and a Table Read grant on main.events so the positive
/// route tests still see the dataset once gating is enforced.
async fn grant_analyst_table(cp: &MemoryControlPlane) {
    let subj = SubjectId("analyst".into());
    let role = RoleId("analyst-role".into());
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    cp.grant(
        &role,
        Action::Read,
        PolicyTarget::Table(TableRef {
            schema: "main".into(),
            name: "events".into(),
        }),
        Effect::Allow,
    )
    .await
    .unwrap();
}

/// Drive a request as an arbitrary named subject (the module-level `get` hardcodes
/// `analyst`). Needed to distinguish role A (view grant) from role B (base grant).
async fn get_as(app: &axum::Router, uri: &str, subject: &str) -> (StatusCode, serde_json::Value) {
    let mut req = Request::builder().uri(uri).body(Body::empty()).unwrap();
    req.extensions_mut()
        .insert(Subject(SubjectId(subject.into())));
    let res = app.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let json = serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
    (status, json)
}

/// Give `subject` a fresh role with a Table Read grant on `table`.
async fn grant_table(cp: &MemoryControlPlane, subject: &str, table: TableRef) {
    let subj = SubjectId(subject.into());
    let role = RoleId(format!("{subject}-role"));
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    cp.grant(
        &role,
        Action::Read,
        PolicyTarget::Table(table),
        Effect::Allow,
    )
    .await
    .unwrap();
}

/// Seed a base `main.customers(id, region, amount)` with two append snapshots, then
/// define the view `gov.customers_eu = main.customers WHERE region='EU'` projecting
/// `(id, region)`. Returns the cp and the base's latest snapshot id.
async fn seeded_with_view() -> (MemoryControlPlane, i64) {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    let base = TableRef {
        schema: "main".into(),
        name: "customers".into(),
    };
    let cols = vec![
        ("id".to_string(), "Long".to_string(), false),
        ("region".to_string(), "String".to_string(), true),
        ("amount".to_string(), "Long".to_string(), true),
    ];
    let snapshots = cp.seed_catalog(&base, &cols, &[3, 2]);
    let latest = snapshots.last().expect("seeded snapshots").0;
    cp.catalog()
        .define_view(ViewDef {
            view: TableRef {
                schema: "gov".into(),
                name: "customers_eu".into(),
            },
            base: base.clone(),
            predicate: Some(RowFilter::Compare {
                property: "region".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Text("EU".into()),
            }),
            columns: Some(vec!["id".into(), "region".into()]),
        })
        .await
        .expect("define_view");
    (cp, latest)
}

#[tokio::test(flavor = "multi_thread")]
async fn views_list_with_kind_and_base_under_their_own_grant() {
    let (cp, _) = seeded_with_view().await;
    // Role A: Table grant on the VIEW only. Role B: Table grant on the BASE only.
    grant_table(
        &cp,
        "alice",
        TableRef {
            schema: "gov".into(),
            name: "customers_eu".into(),
        },
    )
    .await;
    grant_table(
        &cp,
        "bob",
        TableRef {
            schema: "main".into(),
            name: "customers".into(),
        },
    )
    .await;
    let app = app(cp);

    // Alice sees exactly the view entry; the base is NOT listed.
    let (status, json) = get_as(&app, "/datasets", "alice").await;
    assert_eq!(status, StatusCode::OK);
    let ds = json["datasets"].as_array().unwrap();
    assert_eq!(ds.len(), 1, "view-grantee sees only the view: {json}");
    assert_eq!(ds[0]["schema"], "gov");
    assert_eq!(ds[0]["name"], "customers_eu");
    assert_eq!(ds[0]["kind"], "view");
    assert_eq!(
        ds[0]["base"],
        serde_json::json!({ "schema": "main", "name": "customers" })
    );
    assert!(
        ds[0]["updated"].as_str().is_some_and(|t| !t.is_empty()),
        "view updated delegates to the base snapshot time: {json}"
    );

    // Bob sees exactly the base table; the view is NOT listed.
    let (status, json) = get_as(&app, "/datasets", "bob").await;
    assert_eq!(status, StatusCode::OK);
    let ds = json["datasets"].as_array().unwrap();
    assert_eq!(ds.len(), 1, "base-grantee sees only the base: {json}");
    assert_eq!(ds[0]["schema"], "main");
    assert_eq!(ds[0]["name"], "customers");
    assert_eq!(ds[0]["kind"], "table");
    assert!(
        ds[0].get("base").is_none(),
        "a table entry has no base: {json}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn view_get_dataset_returns_projected_columns() {
    let (cp, latest) = seeded_with_view().await;
    grant_table(
        &cp,
        "alice",
        TableRef {
            schema: "gov".into(),
            name: "customers_eu".into(),
        },
    )
    .await;
    let app = app(cp);

    let (status, json) = get_as(&app, "/datasets/gov/customers_eu", "alice").await;
    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(json["table"]["schema"], "gov");
    assert_eq!(json["table"]["name"], "customers_eu");
    assert_eq!(json["kind"], "view");
    assert_eq!(
        json["base"],
        serde_json::json!({ "schema": "main", "name": "customers" })
    );
    // Snapshot delegates to the base's current; columns are the projected subset.
    assert_eq!(json["snapshot_id"], latest);
    assert_eq!(
        json["columns"],
        serde_json::json!([
            { "name": "id", "ty": "Long", "nullable": false },
            { "name": "region", "ty": "String", "nullable": true },
        ]),
        "projected to (id, region): {json}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn datasets_lists_the_seeded_table() {
    let (cp, _) = seeded();
    grant_analyst_table(&cp).await;
    let app = app(cp);
    let (status, json) = get(&app, "/datasets").await;
    assert_eq!(status, StatusCode::OK);
    let ds = &json["datasets"][0];
    assert_eq!(ds["schema"], "main");
    assert_eq!(ds["name"], "events");
    assert_eq!(ds["project"], "main");
    assert!(
        ds["updated"].as_str().is_some_and(|t| !t.is_empty()),
        "updated must be a non-empty RFC3339 string, got {json}"
    );
    assert_eq!(json["datasets"].as_array().unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn dataset_detail_composes_snapshot_and_columns() {
    let (cp, latest) = seeded();
    grant_analyst_table(&cp).await;
    let app = app(cp);
    let (status, json) = get(&app, "/datasets/main/events").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["table"]["schema"], "main");
    assert_eq!(json["table"]["name"], "events");
    assert_eq!(json["snapshot_id"], latest);
    assert!(
        json["snapshot_time"]
            .as_str()
            .is_some_and(|t| !t.is_empty()),
        "snapshot_time must be a non-empty RFC3339 string, got {json}"
    );
    assert_eq!(
        json["columns"],
        serde_json::json!([
            { "name": "id", "ty": "Long", "nullable": false },
            { "name": "note", "ty": "String", "nullable": true },
        ])
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_dataset_is_404() {
    let (cp, _) = seeded();
    let app = app(cp);
    let (status, _) = get(&app, "/datasets/main/no-such-table").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn get_dataset_404_body_is_identical_for_unreadable_and_nonexistent() {
    // Ungranted `analyst`: an existing dataset and a nonexistent one must return the
    // byte-identical 404 (no existence oracle). Compare raw body bytes/text — not
    // JSON-decoded — since the plaintext 404 body decodes to `Value::Null` either way,
    // which would mask two genuinely different bodies.
    let (cp1, _) = seeded();
    let app_existing = app(cp1);
    let (s_existing, b_existing) = get_raw(&app_existing, "/datasets/main/events").await;

    let (cp2, _) = seeded();
    let app_missing = app(cp2);
    let (s_missing, b_missing) = get_raw(&app_missing, "/datasets/main/no-such-table").await;

    assert_eq!(s_existing, StatusCode::NOT_FOUND);
    assert_eq!(s_missing, StatusCode::NOT_FOUND);
    assert_eq!(b_existing, b_missing);
    assert_eq!(b_existing, "dataset not found");
}

#[tokio::test(flavor = "multi_thread")]
async fn preview_404_body_is_identical_for_unreadable_and_nonexistent() {
    // As above: compare raw body text, not JSON-decoded, so this genuinely proves
    // byte-identical 404 bodies rather than two bodies that both fail to parse as JSON.
    let (cp1, _) = seeded();
    let app_existing = app_canned(cp1);
    let (s_existing, b_existing) = get_raw(&app_existing, "/datasets/main/events/preview").await;

    let (cp2, _) = seeded();
    let app_missing = app_canned(cp2);
    let (s_missing, b_missing) =
        get_raw(&app_missing, "/datasets/main/no-such-table/preview").await;

    assert_eq!(s_existing, StatusCode::NOT_FOUND);
    assert_eq!(s_missing, StatusCode::NOT_FOUND);
    assert_eq!(b_existing, b_missing);
    assert_eq!(b_existing, "dataset not found");
}

#[tokio::test(flavor = "multi_thread")]
async fn granted_subject_reads_get_and_preview() {
    let (cp, _) = seeded();
    grant_analyst_table(&cp).await;
    let app = app_canned(cp);
    let (s_get, _) = get(&app, "/datasets/main/events").await;
    assert_eq!(s_get, StatusCode::OK);
    let (s_prev, prev) = get(&app, "/datasets/main/events/preview?limit=5").await;
    assert_eq!(s_prev, StatusCode::OK);
    assert_eq!(prev["sampled"], serde_json::json!(true));
}

#[tokio::test(flavor = "multi_thread")]
async fn dataset_preview_returns_sampled_rows() {
    let (cp, _) = seeded();
    grant_analyst_table(&cp).await;
    let app = app_canned(cp);
    let (status, json) = get(&app, "/datasets/main/events/preview?limit=5").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["columns"], serde_json::json!(["id", "note"]));
    assert_eq!(json["sampled"], serde_json::json!(true));
    assert_eq!(json["rows"], serde_json::json!([["1", "a"], ["2", ""]]));
}

#[tokio::test(flavor = "multi_thread")]
async fn dataset_preview_rejects_bad_limit() {
    let (cp, _) = seeded();
    let app = app_canned(cp);
    let (status, _) = get(&app, "/datasets/main/events/preview?limit=nope").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread")]
async fn datasets_list_is_empty_for_ungranted_subject() {
    let (cp, _) = seeded();
    let app = app(cp);
    let (status, json) = get(&app, "/datasets").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["datasets"].as_array().unwrap().len(), 0);
}

/// Spec Testing "Type grant (backing table listed)" at the route level: a Read grant
/// on a TYPE backed by main.events makes the backing dataset appear in the list — the
/// lineage-symmetry case, exercised through the fallback in `is_table_readable`.
#[tokio::test(flavor = "multi_thread")]
async fn type_grant_lists_backing_table() {
    let (cp, _) = seeded();
    cp.ontology()
        .define_type(ObjectType::build("Event", ("main", "events")).done())
        .await
        .unwrap();
    let subj = SubjectId("analyst".into());
    let role = RoleId("analyst-role".into());
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    cp.grant(
        &role,
        Action::Read,
        PolicyTarget::Type(TypeName("Event".into())),
        Effect::Allow,
    )
    .await
    .unwrap();

    let app = app(cp);
    let (status, json) = get(&app, "/datasets").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["datasets"].as_array().unwrap().len(), 1);
    assert_eq!(json["datasets"][0]["name"], "events");
}
