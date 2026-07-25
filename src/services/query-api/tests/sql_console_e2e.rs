//! Acceptance e2e for the governed SQL console (`POST /sql`, #621): drives the HTTP
//! route through the auth gate against a REAL engine (landing -> engine over UDS ->
//! `EngineServingClient::execute_governed`), asserting the caller's own arbitrary SQL
//! is governed row-for-row and column-for-column, that a table the subject cannot see
//! (ungranted type) is a 400 (the engine's closed-world catalog makes it
//! indistinguishable from a nonexistent table), that malformed SQL is a 400, that the
//! row cap truncates with a flag, and that an unauthenticated request is a 401.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    Acl, Action, ColumnSpec, CompareOp, DatasetId, EventType, LineageEvent, ObjectType, Ontology,
    Policy, PolicyTarget, PropertyDef, RowFilter, RunId, ScalarValue, TableRef, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use loom_test_seed::local_sql_catalog;
use query_api::engine_client::EngineServingClient;
use query_api::serving::ServingEngine;

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef {
        schema: schema.into(),
        name: name.into(),
    }
}

fn col(name: &str, ty: &str) -> ColumnSpec {
    ColumnSpec {
        name: name.into(),
        ty: ty.into(),
        nullable: false,
    }
}

fn prop(name: &str, ty: &str) -> PropertyDef {
    PropertyDef::new(name, ty).required()
}

fn lineage(schema: &str, name: &str) -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(&tref(schema, name)).dataset_ref()],
        payload: serde_json::json!({ "source": "sql-console-e2e" }),
    }
}

/// Everything a test needs kept alive: the control plane (for the driver), the real
/// engine serving client (the `eng` the router uses), and the guards that must not drop.
struct Harness {
    cp: Arc<PgControlPlane>,
    serving: Arc<dyn ServingEngine>,
    _wh: tempfile::TempDir,
    _eng: e2e_support::EngineGuard,
}

/// Land `wh.orders`/`wh.customers`/`wh.secrets` through the real landing path, bind
/// `Order`/`Customer`/`Secret` ontology types, grant `reader` a row-filtered + masked
/// Read on `Order`, a column-denied Read on `Customer`, and NOTHING on `Secret`, then
/// boot the engine over a UDS and connect a real `EngineServingClient`.
async fn setup(fx: &PgFixture) -> Harness {
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let wh = tempfile::tempdir().expect("warehouse");
    let warehouse = wh.path().display().to_string();
    let catalog = local_sql_catalog(dsn, &warehouse).await;
    let limits = InlineLimits {
        inline_byte_limit: 0,
        flush_byte_threshold: i64::MAX,
    };

    // wh.orders(id, customer_id, email): 4 rows.
    let orders_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("email", DataType::Utf8, false),
    ]));
    let orders = RecordBatch::try_new(
        orders_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2, 3, 4])),
            Arc::new(Int64Array::from(vec![1i64, 1, 2, 2])),
            Arc::new(StringArray::from(vec![
                "a1@x.com", "a2@x.com", "a3@x.com", "a4@x.com",
            ])),
        ],
    )
    .expect("orders batch");
    land(
        &pool,
        &catalog,
        &tref("wh", "orders"),
        &[col("id", "long"), col("customer_id", "long"), col("email", "string")],
        orders_schema,
        vec![orders],
        limits,
        lineage("wh", "orders"),
        None,
    )
    .await
    .expect("land orders");

    // wh.customers(id, name, ssn): 2 rows.
    let cust_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("ssn", DataType::Utf8, false),
    ]));
    let customers = RecordBatch::try_new(
        cust_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            Arc::new(StringArray::from(vec!["111-11-1111", "222-22-2222"])),
        ],
    )
    .expect("customers batch");
    land(
        &pool,
        &catalog,
        &tref("wh", "customers"),
        &[col("id", "long"), col("name", "string"), col("ssn", "string")],
        cust_schema,
        vec![customers],
        limits,
        lineage("wh", "customers"),
        None,
    )
    .await
    .expect("land customers");

    // wh.secrets(id, value): 1 row — reader holds NO grant on its type.
    let sec_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let secrets = RecordBatch::try_new(
        sec_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1i64])),
            Arc::new(StringArray::from(vec!["top-secret"])),
        ],
    )
    .expect("secrets batch");
    land(
        &pool,
        &catalog,
        &tref("wh", "secrets"),
        &[col("id", "long"), col("value", "string")],
        sec_schema,
        vec![secrets],
        limits,
        lineage("wh", "secrets"),
        None,
    )
    .await
    .expect("land secrets");

    cp.define_type(
        ObjectType::build("Order", ("wh".to_string(), "orders".to_string()))
            .add_prop(prop("id", "long"))
            .add_prop(prop("customer_id", "long"))
            .add_prop(prop("email", "string"))
            .identity("id")
            .done(),
    )
    .await
    .expect("define Order");
    cp.define_type(
        ObjectType::build("Customer", ("wh".to_string(), "customers".to_string()))
            .add_prop(prop("id", "long"))
            .add_prop(prop("name", "string"))
            .add_prop(prop("ssn", "string"))
            .identity("id")
            .done(),
    )
    .await
    .expect("define Customer");
    cp.define_type(
        ObjectType::build("Secret", ("wh".to_string(), "secrets".to_string()))
            .add_prop(prop("id", "long"))
            .add_prop(prop("value", "string"))
            .identity("id")
            .done(),
    )
    .await
    .expect("define Secret");

    // ACL: reader gets a row-filtered (id >= 2) + email-masked Read on Order, a
    // ssn-denied Read on Customer, and NOTHING on Secret.
    let (_subj, role) = e2e_support::subject_with_role(&cp, "reader").await;
    e2e_support::grant_read(&cp, &role, "Order").await;
    cp.set_policy(
        &role,
        Action::Read,
        Policy {
            target: PolicyTarget::Type(TypeName("Order".into())),
            row_filter: Some(RowFilter::Compare {
                property: "id".into(),
                op: CompareOp::Ge,
                value: ScalarValue::Int(2),
            }),
            deny_columns: vec![],
            mask_columns: vec!["email".into()],
        },
    )
    .await
    .expect("set Order policy");
    e2e_support::grant_read_columns(&cp, &role, "Customer", vec!["ssn".into()], vec![]).await;

    // Boot the engine over a UDS and connect a real EngineServingClient (control+flight).
    let (sock, eng) = e2e_support::spawn_engine_full(fx, &db, wh.path(), 0, i64::MAX).await;
    let serving: Arc<dyn ServingEngine> =
        Arc::new(EngineServingClient::connect(sock).await.expect("engine connect"));
    let cp = Arc::new(cp);

    Harness {
        cp,
        serving,
        _wh: wh,
        _eng: eng,
    }
}

/// Drive `POST /sql` as `reader` with the given SQL (+ optional limit).
async fn run(h: &Harness, sql: &str, limit: Option<u32>) -> (axum::http::StatusCode, serde_json::Value) {
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
    let h = setup(fx).await;

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
    let h = setup(fx).await;

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
    let h = setup(fx).await;

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
    let h = setup(fx).await;

    let (status, _body) = run(&h, "SELCT 1", None).await;
    assert_eq!(status, axum::http::StatusCode::BAD_REQUEST);
}

/// An empty SQL body is a 400.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_sql_is_bad_request() {
    let fx = PgFixture::shared();
    let h = setup(fx).await;

    let (status, _body) = run(&h, "   ", None).await;
    assert_eq!(status, axum::http::StatusCode::BAD_REQUEST);
}

/// A `limit` below the result size truncates and flags it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn limit_below_result_truncates_with_flag() {
    let fx = PgFixture::shared();
    let h = setup(fx).await;

    // The row filter yields 3 rows; a cap of 2 truncates.
    let (status, body) = run(&h, r#"SELECT "id" FROM "wh"."orders" ORDER BY "id""#, Some(2)).await;
    assert_eq!(status, axum::http::StatusCode::OK, "body: {body}");
    assert_eq!(body["rows"].as_array().expect("rows").len(), 2);
    assert_eq!(body["truncated"], serde_json::json!(true));
}

/// An unauthenticated request is a 401 (the auth gate runs before the route).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unauthenticated_is_401() {
    let fx = PgFixture::shared();
    let h = setup(fx).await;

    let status = e2e_support::get_unauth(h.cp.clone(), h.serving.clone(), "/sql").await;
    assert_eq!(status, axum::http::StatusCode::UNAUTHORIZED);
}
