//! Typed transform e2e: enqueue a "typed-transform" job; the worker resolves input
//! TYPES to tables, runs type-name SQL, validates the result conforms to the output
//! TYPE, commits, and emits first-class type-named lineage. The output reads back
//! through query-api as the typed object. Real Postgres + Iceberg serving — NO DuckDB.
//!
//! The positive case uses non-required output properties: an inner join can widen
//! column nullability in DataFusion, which would make a required-over-nullable check
//! flaky. The deterministic `required` semantics are covered by the pure `conform`
//! unit tests; the negative case here uses a single-table select (no join, no widening)
//! so its MissingColumn violation is deterministic.

mod transform_e2e_support;
mod transform_serving_support;

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{
    Acl, Action, ControlPlane, ControlPlaneError, DatasetRef, Effect, NewJob, ObjectType, PageReq,
    PropertyDef, Queue, RoleId, SubjectId, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_control_plane::IcebergControlPlane;
use control_plane_worker::Worker;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use query_api::handler::{ObjectQuery, QueryDeps, Subject, read_object};
use query_api::render::objects_to_json;
use serde_json::json;
use tokio_util::sync::CancellationToken;
use transform::{transform_handler, typed_transform_handler};

use transform_e2e_support::{col_csv, cols, make_catalog, scalar_i64, seed_table, tref};
use transform_serving_support::InProcessServingEngine;

fn prop(name: &str, ty: &str, required: bool) -> PropertyDef {
    PropertyDef {
        name: name.into(),
        ty: ty.into(),
        required,
    }
}

/// Spawn the worker on BOTH transform kinds; the queue dequeues through `pg`, the
/// handler commits through a fresh Iceberg control plane over `warehouse`.
#[allow(
    clippy::too_many_arguments,
    reason = "spawn_worker needs pg, cp_h, store, and root_url to fully configure the test worker"
)]
fn spawn_worker(
    pg: &PgControlPlane,
    cp_h: Arc<dyn ControlPlane>,
    store: &Arc<dyn ObjectStore>,
    root_url: &str,
) -> (CancellationToken, tokio::task::JoinHandle<()>) {
    let token = CancellationToken::new();
    let t = token.clone();
    let store_h = store.clone();
    let root_url = root_url.to_string();
    let worker = Worker::new(
        pg.clone(),
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
                    let root_url = root_url.clone();
                    async move {
                        match job.kind.as_str() {
                            "typed-transform" => {
                                typed_transform_handler(cp.as_ref(), store, &root_url, job).await
                            }
                            _ => transform_handler(cp.as_ref(), store, &root_url, job).await,
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
    let (pg, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let warehouse = wh.path().display().to_string();
    let root_url = format!("file://{warehouse}");
    let catalog = make_catalog(fx.pg_dsn(&db), &warehouse).await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(wh.path()).expect("store"));
    let cp = IcebergControlPlane::new(pg.clone(), catalog);

    // 1. SEED two input tables (real Iceberg Parquet, mirror-registered).
    let customers = tref("main", "customers");
    let cust_cols = cols(&[("id", "long", false), ("region", "string", true)]);
    let cust_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, true),
    ]));
    seed_table(
        &cp,
        &store,
        &customers,
        &cust_cols,
        cust_schema.clone(),
        RecordBatch::try_new(
            cust_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("CA"), Some("NY")])),
            ],
        )
        .unwrap(),
        "seed-c",
    )
    .await;

    let orders = tref("main", "orders");
    let ord_cols = cols(&[
        ("id", "long", false),
        ("customer_id", "long", false),
        ("amount", "double", false),
    ]);
    let ord_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    seed_table(
        &cp,
        &store,
        &orders,
        &ord_cols,
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
        "seed-o",
    )
    .await;

    // 2. DEFINE the input types (so resolve() finds their tables) and the OUTPUT type
    //    (its backing table main.order_enriched does NOT exist yet).
    pg.ontology()
        .define_type(ObjectType {
            name: TypeName("Customer".into()),
            properties: vec![prop("id", "Long", true), prop("region", "String", false)],
            derived: vec![],
            table: customers.clone(),
            identity: None,
        })
        .await
        .unwrap();
    pg.ontology()
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
    pg.ontology()
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
    pg.enqueue(NewJob {
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

    // 4. RUN the worker until the job drains (handler commits through Iceberg).
    let cp_h: Arc<dyn ControlPlane> = Arc::new(IcebergControlPlane::new(
        pg.clone(),
        make_catalog(fx.pg_dsn(&db), &warehouse).await,
    ));
    let (token, handle) = spawn_worker(&pg, cp_h, &store, &root_url);
    tokio::time::sleep(Duration::from_millis(900)).await;
    token.cancel();
    handle.await.unwrap();

    assert!(
        pg.dequeue(&["typed-transform".to_string()], "probe")
            .await
            .unwrap()
            .is_none(),
        "typed-transform job completed"
    );

    // 5a. Output rows landed — read back through the serving engine.
    let serving = IcebergCatalog::new(fx.pool_for(&db).await);
    let count = engine_serving::execute_query(
        &serving,
        "SELECT count(*) FROM \"main\".\"order_enriched\"",
        None,
    )
    .await
    .expect("serving count");
    assert_eq!(
        scalar_i64(&count),
        3,
        "serving reads the typed transform output"
    );
    let regions = engine_serving::execute_query(
        &serving,
        "SELECT \"id\", \"region\" FROM \"main\".\"order_enriched\" ORDER BY \"id\"",
        None,
    )
    .await
    .expect("serving regions");
    assert_eq!(
        col_csv(&regions),
        "CA,CA,NY",
        "join produced the right regions"
    );

    // 5b. First-class TYPE-named lineage: upstream(OrderEnriched) == {Customer, Order}.
    let out_ds: DatasetRef = (&TypeName("OrderEnriched".into())).into();
    let ups = pg
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

    // 5c. The Object Model round-trips: read OrderEnriched through query-api over the
    //     in-process Iceberg serving engine (no DuckDB).
    let subj = SubjectId("analyst".into());
    let role = RoleId("analysts".into());
    pg.define_subject(&subj).await.unwrap();
    pg.define_role(&role).await.unwrap();
    pg.assign_role(&subj, &role).await.unwrap();
    pg.grant(
        &role,
        Action::Read,
        control_plane_core::PolicyTarget::Type(TypeName("OrderEnriched".into())),
        Effect::Allow,
    )
    .await
    .unwrap();

    let eng = InProcessServingEngine::new(IcebergCatalog::new(fx.pool_for(&db).await));
    let deps = QueryDeps {
        ontology: &pg,
        acl: &pg,
        serving: &eng,
        default_limit: 1000,
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
    let (pg, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let warehouse = wh.path().display().to_string();
    let root_url = format!("file://{warehouse}");
    let catalog = make_catalog(fx.pg_dsn(&db), &warehouse).await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(wh.path()).expect("store"));
    let cp = IcebergControlPlane::new(pg.clone(), catalog);

    // One seeded input + its type.
    let customers = tref("main", "customers");
    let cust_cols = cols(&[("id", "long", false), ("region", "string", true)]);
    let cust_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, true),
    ]));
    seed_table(
        &cp,
        &store,
        &customers,
        &cust_cols,
        cust_schema.clone(),
        RecordBatch::try_new(
            cust_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("CA"), Some("NY")])),
            ],
        )
        .unwrap(),
        "seed-c",
    )
    .await;
    pg.ontology()
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
    pg.ontology()
        .define_type(ObjectType {
            name: TypeName("CustomerBad".into()),
            properties: vec![prop("id", "Long", true), prop("region", "String", false)],
            derived: vec![],
            table: bad.clone(),
            identity: None,
        })
        .await
        .unwrap();

    pg.enqueue(NewJob {
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

    let cp_h: Arc<dyn ControlPlane> = Arc::new(IcebergControlPlane::new(
        pg.clone(),
        make_catalog(fx.pg_dsn(&db), &warehouse).await,
    ));
    let (token, handle) = spawn_worker(&pg, cp_h, &store, &root_url);
    tokio::time::sleep(Duration::from_millis(700)).await;
    token.cancel();
    handle.await.unwrap();

    // The job was abandoned (deterministic), and NOTHING was committed.
    assert!(
        pg.dequeue(&["typed-transform".to_string()], "probe")
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
