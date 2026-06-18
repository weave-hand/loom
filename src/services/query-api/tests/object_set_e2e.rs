//! Object-set input (`?_ids=`) e2e over the real HTTP router backed by a DuckDB serving
//! engine reading a DuckLake-on-Postgres catalog. Proves `?_ids=1,2` scopes a plain read
//! to those source objects by declared identity; a traversal `…/links/orders?_ids=1`
//! scopes the source before the hop; `?_ids=` + `?_shape=association` scopes the
//! association's source; `?_ids=` on a no-identity type -> HTTP 400; and a present-but-empty
//! `?_ids=` -> HTTP 400.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use control_plane_core::{
    Acl, Action, Cardinality, ControlPlane, DatasetRef, Effect, EventType, LineageEvent,
    LinkBacking, LinkDef, ObjectType, Ontology, PolicyTarget, PropertyDef, RoleId, RunId,
    SubjectId, TableRef, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use http_body_util::BodyExt;
use ingest::{MaterializeRequest, materialize};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use query_api::http::{AppState, router};
use query_api::serving::{ActionEngine, EmbeddedDuckDb, ServingError, SqlValue};
use time::OffsetDateTime;
use tower::ServiceExt;
use uuid::Uuid;

fn tref(s: &str, n: &str) -> TableRef {
    TableRef {
        schema: s.into(),
        name: n.into(),
    }
}

fn prop(name: &str, ty: &str, required: bool) -> PropertyDef {
    PropertyDef {
        name: name.into(),
        ty: ty.into(),
        required,
    }
}

/// No-op write engine: the read-only routes never touch it, but `AppState` requires one.
struct StubAction;

#[async_trait]
impl ActionEngine for StubAction {
    async fn insert_row(
        &self,
        _table: &TableRef,
        _columns: &[String],
        _values: &[SqlValue],
    ) -> std::result::Result<(), ServingError> {
        Ok(())
    }
}

async fn land(
    cp: &PgControlPlane,
    store: &Arc<dyn ObjectStore>,
    table: &TableRef,
    schema: Arc<Schema>,
    batch: RecordBatch,
) {
    let lineage = LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetRef::from(table)],
        payload: serde_json::json!({}),
    };
    materialize(
        cp,
        store.clone(),
        MaterializeRequest {
            table,
            schema,
            batches: &[batch],
            file_prefix: "run-1",
            gate: None,
            lineage,
        },
    )
    .await
    .unwrap();
}

/// Seed customer(id, region) ids {1,2,3} and orders(id, customer_id, status) FK-linked.
/// Define Customer (identity `id`), Order (identity `order_id`... here `id`), the FK link
/// `orders`, and a `Plain` type backed by the same table but with NO declared identity (to
/// drive the no-identity -> 400 path). Caller MUST keep the `DuckLakeWriter` alive.
async fn setup(fx: &PgFixture) -> (PgControlPlane, EmbeddedDuckDb, DuckLakeWriter) {
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(writer.data_path()).unwrap());

    // customer(id, region): (1,'CA'), (2,'CA'), (3,'NY')
    let cust = tref("main", "customer");
    let cust_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, true),
    ]));
    let cust_batch = RecordBatch::try_new(
        cust_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("CA"), Some("CA"), Some("NY")])),
        ],
    )
    .unwrap();
    land(&cp, &store, &cust, cust_schema, cust_batch).await;

    // orders(order_id, customer_id, status):
    //   (10,1,'shipped'),(11,1,'pending'),(20,2,'shipped'),(30,3,'shipped')
    let ord = tref("main", "orders");
    let ord_schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, true),
    ]));
    let ord_batch = RecordBatch::try_new(
        ord_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![10, 11, 20, 30])),
            Arc::new(Int64Array::from(vec![1, 1, 2, 3])),
            Arc::new(StringArray::from(vec![
                Some("shipped"),
                Some("pending"),
                Some("shipped"),
                Some("shipped"),
            ])),
        ],
    )
    .unwrap();
    land(&cp, &store, &ord, ord_schema, ord_batch).await;

    // Customer declares identity `id`.
    cp.define_type(ObjectType {
        name: TypeName("Customer".into()),
        properties: vec![prop("id", "Long", true), prop("region", "String", false)],
        derived: vec![],
        table: cust.clone(),
        identity: Some("id".into()),
    })
    .await
    .unwrap();
    // Order declares identity `order_id`.
    cp.define_type(ObjectType {
        name: TypeName("Order".into()),
        properties: vec![
            prop("order_id", "Long", true),
            prop("customer_id", "Long", true),
            prop("status", "String", false),
        ],
        derived: vec![],
        table: ord.clone(),
        identity: Some("order_id".into()),
    })
    .await
    .unwrap();
    // Plain: a type with NO declared identity, backed by the customer table.
    cp.define_type(ObjectType {
        name: TypeName("Plain".into()),
        properties: vec![prop("id", "Long", true), prop("region", "String", false)],
        derived: vec![],
        table: cust.clone(),
        identity: None,
    })
    .await
    .unwrap();

    // Customer -> Order over the FK (customer.id = orders.customer_id).
    cp.define_link(LinkDef {
        name: "orders".into(),
        from: TypeName("Customer".into()),
        to: TypeName("Order".into()),
        cardinality: Cardinality::Many,
        backing: LinkBacking::ForeignKey {
            from_column: "id".into(),
            to_column: "customer_id".into(),
        },
    })
    .await
    .unwrap();

    let eng = EmbeddedDuckDb::attach(
        &format!(
            "dbname={} host={} user=postgres",
            db,
            fx.socket_path().display()
        ),
        writer.data_path(),
    )
    .await
    .unwrap();
    (cp, eng, writer)
}

async fn subject_with_role(cp: &PgControlPlane, name: &str) -> (SubjectId, RoleId) {
    let subj = SubjectId(name.into());
    let role = RoleId(format!("{name}-role"));
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    (subj, role)
}

async fn grant_read(cp: &PgControlPlane, role: &RoleId, type_name: &str) {
    cp.grant(
        role,
        Action::Read,
        PolicyTarget::Type(TypeName(type_name.into())),
        Effect::Allow,
    )
    .await
    .unwrap();
}

/// Drive the HTTP router and return (status, parsed JSON body).
async fn get(
    cp: Arc<PgControlPlane>,
    eng: Arc<EmbeddedDuckDb>,
    uri: &str,
    subject: &str,
) -> (StatusCode, serde_json::Value) {
    let app = router(AppState {
        cp: cp as Arc<dyn ControlPlane>,
        serving: eng,
        action_engine: Arc::new(StubAction),
    });
    let res = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Loom-Subject", subject)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let json = if bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
    };
    (status, json)
}

/// Sorted values of the given identity key from an objects body. `Long` identities render
/// as JSON strings (NumericString repr), so read each as a string and parse to i64.
fn id_values(body: &serde_json::Value, key: &str) -> Vec<i64> {
    let mut out: Vec<i64> = body["objects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o[key].as_str().unwrap().parse::<i64>().unwrap())
        .collect();
    out.sort();
    out
}

/// Sorted {from,to} string pairs from an association body.
fn pairs(body: &serde_json::Value) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = body["associations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| {
            (
                a["from"].as_str().unwrap().to_string(),
                a["to"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    out.sort();
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn ids_scopes_a_plain_read() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Customer").await;

    // ?_ids=1,2 returns exactly customers 1 and 2 (id 3 excluded).
    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Customer?_ids=1,2",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        id_values(&body, "id"),
        vec![1, 2],
        "scoped to the given identities"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn ids_scopes_a_traversal_source() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "Order").await;

    // ?_ids=1 scopes the SOURCE customer to id 1 before the hop -> only customer 1's orders.
    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Customer/links/orders?_ids=1",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        id_values(&body, "order_id"),
        vec![10, 11],
        "only orders of the scoped source customer 1"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn ids_scopes_an_association_source() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "Order").await;

    // ?_ids=1 + association: pairs only from source customer 1.
    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Customer/links/orders?_ids=1&_shape=association",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        pairs(&body),
        vec![("1".into(), "10".into()), ("1".into(), "11".into())],
        "association pairs scoped to source customer 1"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn ids_on_a_no_identity_type_is_400() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Plain").await;

    // Plain has identity: None -> ?_ids= -> NoIdentity -> HTTP 400.
    let (status, _body) = get(cp.clone(), eng.clone(), "/objects/Plain?_ids=1", "alice").await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "?_ids= on a type with no declared identity -> 400"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn present_but_empty_ids_is_400() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Customer").await;

    // ?_ids= present but empty (zero non-empty values) -> HTTP 400.
    let (status, _body) = get(cp.clone(), eng.clone(), "/objects/Customer?_ids=", "alice").await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "present-but-empty ?_ids= -> 400"
    );
}
