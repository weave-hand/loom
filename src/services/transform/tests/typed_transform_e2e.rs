//! Typed transform e2e: enqueue a "typed-transform" job; the worker resolves input
//! TYPES to tables, runs type-name SQL, validates the result conforms to the output
//! TYPE, commits, and emits first-class type-named lineage. The output reads back
//! through query-api as the typed object. Real Postgres + DuckDB.
//!
//! The positive case uses non-required output properties: an inner join can widen
//! column nullability in DataFusion, which would make a required-over-nullable check
//! flaky. The deterministic `required` semantics are covered by the pure `conform`
//! unit tests; the negative case here uses a single-table select (no join, no widening)
//! so its MissingColumn violation is deterministic.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{
    Acl, Action, ControlPlane, ControlPlaneError, DatasetRef, Effect, EventType, LineageEvent,
    NewJob, ObjectType, PageReq, PolicyTarget, PropertyDef, Queue, RoleId, RunId, SubjectId,
    TableRef, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use control_plane_worker::Worker;
use ingest::{MaterializeRequest, materialize};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use query_api::handler::{ObjectQuery, QueryDeps, Subject, read_object};
use query_api::render::objects_to_json;
use query_api::serving::EmbeddedDuckDb;
use serde_json::json;
use time::OffsetDateTime;
use tokio_util::sync::CancellationToken;
use transform::{transform_handler, typed_transform_handler};
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
        payload: json!({}),
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

/// Spawn the worker on BOTH transform kinds; return a cancel token + join handle.
fn spawn_worker(
    cp: &PgControlPlane,
    store: &Arc<dyn ObjectStore>,
) -> (CancellationToken, tokio::task::JoinHandle<()>) {
    let token = CancellationToken::new();
    let t = token.clone();
    let store_h = store.clone();
    let cp_h: Arc<dyn ControlPlane> = Arc::new(cp.clone());
    let worker = Worker::new(
        cp.clone(),
        "typed-transform-test",
        Duration::from_millis(300),
    )
    .with_poll_interval(Duration::from_millis(50));
    let handle = tokio::spawn(async move {
        worker
            .run(
                &["transform".to_string(), "typed-transform".to_string()],
                t,
                move |job| {
                    let cp = cp_h.clone();
                    let store = store_h.clone();
                    async move {
                        match job.kind.as_str() {
                            "typed-transform" => {
                                typed_transform_handler(cp.as_ref(), store, job).await
                            }
                            _ => transform_handler(cp.as_ref(), store, job).await,
                        }
                    }
                },
            )
            .await
            .unwrap();
    });
    (token, handle)
}

#[tokio::test(flavor = "multi_thread")]
async fn typed_transform_materializes_and_governs_the_output_model() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(writer.data_path()).unwrap());

    // 1. LAND two input tables.
    let customers = tref("main", "customers");
    let cust_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, true),
    ]));
    land(
        &cp,
        &store,
        &customers,
        cust_schema.clone(),
        RecordBatch::try_new(
            cust_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("CA"), Some("NY")])),
            ],
        )
        .unwrap(),
    )
    .await;

    let orders = tref("main", "orders");
    let ord_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    land(
        &cp,
        &store,
        &orders,
        ord_schema.clone(),
        RecordBatch::try_new(
            ord_schema,
            vec![
                Arc::new(Int64Array::from(vec![10, 11, 12])),
                Arc::new(Int64Array::from(vec![1, 1, 2])),
                Arc::new(Float64Array::from(vec![5.5, 7.5, 2.5])),
            ],
        )
        .unwrap(),
    )
    .await;

    // 2. DEFINE the input types (so resolve() finds their tables) and the OUTPUT type
    //    (its backing table main.order_enriched does NOT exist yet).
    cp.ontology()
        .define_type(ObjectType {
            name: TypeName("Customer".into()),
            properties: vec![prop("id", "Long", true), prop("region", "String", false)],
            derived: vec![],
            table: customers.clone(),
            identity: None,
        })
        .await
        .unwrap();
    cp.ontology()
        .define_type(ObjectType {
            name: TypeName("Order".into()),
            properties: vec![
                prop("id", "Long", true),
                prop("customer_id", "Long", true),
                prop("amount", "Double", true),
            ],
            derived: vec![],
            table: orders.clone(),
            identity: None,
        })
        .await
        .unwrap();
    let enriched = tref("main", "order_enriched");
    cp.ontology()
        .define_type(ObjectType {
            name: TypeName("OrderEnriched".into()),
            // Non-required to stay robust against inner-join nullability widening; see
            // the module doc comment.
            properties: vec![
                prop("id", "Long", false),
                prop("region", "String", false),
                prop("amount", "Double", false),
            ],
            derived: vec![],
            table: enriched.clone(),
            identity: None,
        })
        .await
        .unwrap();

    // 3. ENQUEUE a typed transform: SQL references inputs by TYPE name ("Order" is a
    //    reserved word, so quote both identifiers; aliases produce the property names).
    cp.enqueue(NewJob {
        kind: "typed-transform".into(),
        payload: json!({
            "inputs": ["Customer", "Order"],
            "output": "OrderEnriched",
            "sql": "SELECT \"Order\".id AS id, \"Customer\".region AS region, \
                    \"Order\".amount AS amount \
                    FROM \"Order\" JOIN \"Customer\" ON \"Order\".customer_id = \"Customer\".id"
        }),
        run_at: None,
        priority: 0,
    })
    .await
    .unwrap();

    // 4. RUN the worker until the job drains.
    let (token, handle) = spawn_worker(&cp, &store);
    tokio::time::sleep(Duration::from_millis(900)).await;
    token.cancel();
    handle.await.unwrap();

    assert!(
        cp.dequeue(&["typed-transform".to_string()], "probe")
            .await
            .unwrap()
            .is_none(),
        "typed-transform job completed"
    );

    // 5a. Output rows landed (DuckDB).
    let count = writer
        .query_scalar("SELECT count(*) FROM lake.main.order_enriched;")
        .await;
    assert_eq!(count, "3", "DuckDB reads the typed transform output");
    let regions = writer
        .query_scalar("SELECT string_agg(region, ',' ORDER BY id) FROM lake.main.order_enriched;")
        .await;
    assert_eq!(regions, "CA,CA,NY", "join produced the right regions");

    // 5b. First-class TYPE-named lineage: upstream(OrderEnriched) == {Customer, Order}.
    let out_ds: DatasetRef = (&TypeName("OrderEnriched".into())).into();
    let ups = cp
        .lineage()
        .upstream(&out_ds, PageReq::unbounded())
        .await
        .unwrap();
    let up: std::collections::HashSet<String> = ups.items.iter().map(|d| d.name.clone()).collect();
    assert_eq!(
        up,
        std::collections::HashSet::from(["Customer".to_string(), "Order".to_string()]),
        "type-named lineage upstream, got {up:?}"
    );
    assert!(
        ups.items.iter().all(|d| d.namespace == "loom:type"),
        "lineage nodes are type-namespaced"
    );

    // 5c. The Object Model round-trips: read OrderEnriched through query-api.
    let subj = SubjectId("analyst".into());
    let role = RoleId("analysts".into());
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    cp.grant(
        &role,
        Action::Read,
        PolicyTarget::Type(TypeName("OrderEnriched".into())),
        Effect::Allow,
    )
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
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &eng,
    };
    let rows = read_object(
        &ObjectQuery {
            type_name: "OrderEnriched".into(),
            eq_filters: vec![],
            ids: vec![],
        },
        &Subject(subj),
        &deps,
    )
    .await
    .unwrap();
    assert_eq!(
        rows.columns,
        vec!["id".to_string(), "region".to_string(), "amount".to_string()]
    );
    assert_eq!(
        rows.rows.len(),
        3,
        "all three enriched rows are governed-readable"
    );

    let body = objects_to_json(&rows);
    let mut objs: Vec<serde_json::Value> = body["objects"].as_array().unwrap().clone();
    objs.sort_by_key(|o| o["id"].as_str().unwrap().to_string());
    assert_eq!(
        objs,
        vec![
            json!({ "id": "10", "region": "CA", "amount": 5.5 }),
            json!({ "id": "11", "region": "CA", "amount": 7.5 }),
            json!({ "id": "12", "region": "NY", "amount": 2.5 }),
        ],
        "OrderEnriched round-trips as typed JSON (Long id as string, Double amount as number)"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn non_conforming_typed_transform_commits_nothing() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(writer.data_path()).unwrap());

    // One landed input + its type.
    let customers = tref("main", "customers");
    let cust_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, true),
    ]));
    land(
        &cp,
        &store,
        &customers,
        cust_schema.clone(),
        RecordBatch::try_new(
            cust_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("CA"), Some("NY")])),
            ],
        )
        .unwrap(),
    )
    .await;
    cp.ontology()
        .define_type(ObjectType {
            name: TypeName("Customer".into()),
            properties: vec![prop("id", "Long", true), prop("region", "String", false)],
            derived: vec![],
            table: customers.clone(),
            identity: None,
        })
        .await
        .unwrap();

    // Output type requires `region`, but the SQL omits it -> DoesNotConform. Single-table
    // select (no join) keeps `id` deterministically non-null.
    let bad = tref("main", "customer_bad");
    cp.ontology()
        .define_type(ObjectType {
            name: TypeName("CustomerBad".into()),
            properties: vec![prop("id", "Long", true), prop("region", "String", false)],
            derived: vec![],
            table: bad.clone(),
            identity: None,
        })
        .await
        .unwrap();

    cp.enqueue(NewJob {
        kind: "typed-transform".into(),
        payload: json!({
            "inputs": ["Customer"],
            "output": "CustomerBad",
            "sql": "SELECT \"Customer\".id AS id FROM \"Customer\""
        }),
        run_at: None,
        priority: 0,
    })
    .await
    .unwrap();

    let (token, handle) = spawn_worker(&cp, &store);
    tokio::time::sleep(Duration::from_millis(700)).await;
    token.cancel();
    handle.await.unwrap();

    // The job was abandoned (deterministic), and NOTHING was committed.
    assert!(
        cp.dequeue(&["typed-transform".to_string()], "probe")
            .await
            .unwrap()
            .is_none(),
        "non-conforming job is not left runnable (abandoned)"
    );
    assert!(
        matches!(
            cp.catalog().current_snapshot(&bad).await,
            Err(ControlPlaneError::NotFound(_))
        ),
        "non-conforming transform committed no snapshot for the output table"
    );
}
