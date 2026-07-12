//! query-api's EngineActionClient drives the governed-write RPCs end-to-end:
//! spawn an engine over a UDS, write via the ActionEngine trait, read back via
//! the governed read path. Proves that `EngineActionClient` builds the Arrow
//! batch client-side and that the committed row is visible in the Iceberg mirror.

use control_plane_core::{
    ControlPlane, DatasetRef, EventType, LineageEvent, RunId, SnapshotId, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use e2e_support::{InProcessServingEngine, define_widget, grant_writer, spawn_engine_writer};
use query_api::handler::{ObjectQuery, QueryDeps, Subject, read_object};
use query_api::render::objects_to_json;
use query_api::serving::{ActionEngine, SqlValue};
use serde_json::json;
use uuid::Uuid;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_object_through_wire_client() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    // define_widget creates Widget(id Long identity, name String, qty Long)
    // plus createWidget/updateWidget/deleteWidget actions.
    let widget = define_widget(&cp).await;
    let subj = grant_writer(&cp, &widget).await;

    let warehouse = tempfile::tempdir().expect("warehouse");
    let (engine, _guard) =
        spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX).await;

    let table = TableRef {
        schema: "main".into(),
        name: "widget".into(),
    };
    let event = LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetRef {
            namespace: "loom".into(),
            name: "main.widget".into(),
        }],
        payload: json!({ "action": "createWidget" }),
    };

    // Write all 3 columns so the Iceberg schema includes qty; read_object will
    // generate `SELECT id, name, qty FROM main.widget` and would fail if qty is absent.
    let snap: SnapshotId = engine
        .write_object(
            &table,
            &["id".to_string(), "name".to_string(), "qty".to_string()],
            &[
                SqlValue::Int(7),
                SqlValue::Text("hi".into()),
                SqlValue::Null,
            ],
            &["Long".to_string(), "String".to_string(), "Long".to_string()],
            event,
            &[],
        )
        .await
        .expect("write_object via EngineActionClient");
    assert!(snap.0 > 0, "snapshot id must be positive");

    // Read back via the in-process serving engine over the same pool / IcebergCatalog.
    let catalog = IcebergCatalog::new(pool.clone());
    let eng = InProcessServingEngine::new(catalog);
    let qdeps = QueryDeps {
        ontology: cp.ontology(),
        acl: cp.acl(),
        catalog: cp.catalog(),
        serving: &eng,
        default_limit: 1000,
        gc_retention: e2e_support::TEST_GC_RETENTION,
    };
    let rows = read_object(
        &ObjectQuery {
            type_name: "Widget".into(),
            filters: vec![],
            ids: vec![],
            or_raw: Vec::new(),
            as_of: None,
        },
        &Subject(subj),
        &qdeps,
    )
    .await
    .expect("read_object must succeed after wire write");

    assert_eq!(
        objects_to_json(&rows, None)["objects"][0],
        json!({ "id": "7", "name": "hi", "qty": null }),
        "row round-trips through EngineActionClient wire"
    );

    drop(warehouse);
}
