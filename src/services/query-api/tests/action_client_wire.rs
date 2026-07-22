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
            &[false, true, true],
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

/// Regression for #359: an Insert action APPENDING into a table whose identity column
/// is stored non-nullable must succeed. The first write creates the table (no schema
/// reconciliation); the SECOND write appends into the existing table and is compared,
/// positionally, against the live schema. Before the fix the writer declared every
/// column nullable, so the required `id` disagreed with the stored non-null `id` and the
/// engine's schema-evolution guard rejected the append ("nullability changed") with a 500.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn append_into_existing_nonnull_id_table_succeeds() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let widget = define_widget(&cp).await;
    let subj = grant_writer(&cp, &widget).await;

    let warehouse = tempfile::tempdir().expect("warehouse");
    let (engine, _guard) =
        spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX).await;

    let table = TableRef {
        schema: "main".into(),
        name: "widget".into(),
    };
    // Widget(id Long identity, name String, qty Long): id is non-nullable, the others
    // nullable — exactly the schema `full_row_nullability` derives for an insert.
    let cols = ["id".to_string(), "name".to_string(), "qty".to_string()];
    let logical = ["Long".to_string(), "String".to_string(), "Long".to_string()];
    let nullable = [false, true, true];
    let event = |run: Uuid| LineageEvent {
        run_id: RunId(run),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetRef {
            namespace: "loom".into(),
            name: "main.widget".into(),
        }],
        payload: json!({ "action": "createWidget" }),
    };

    // 1. First write CREATES the table (id stored non-nullable).
    engine
        .write_object(
            &table,
            &cols,
            &[
                SqlValue::Int(1),
                SqlValue::Text("first".into()),
                SqlValue::Null,
            ],
            &logical,
            &nullable,
            event(Uuid::new_v4()),
            &[],
        )
        .await
        .expect("create write");

    // 2. Second write APPENDS into the existing table — the #359 path. The append is
    //    reconciled against the live non-null `id`; the honest nullability makes it Identical.
    engine
        .write_object(
            &table,
            &cols,
            &[
                SqlValue::Int(2),
                SqlValue::Text("second".into()),
                SqlValue::Null,
            ],
            &logical,
            &nullable,
            event(Uuid::new_v4()),
            &[],
        )
        .await
        .expect("append into existing non-null id table must not 500 (#359)");

    // Both rows are visible.
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
    .expect("read_object after append");
    assert_eq!(
        objects_to_json(&rows, None)["objects"]
            .as_array()
            .map(Vec::len),
        Some(2),
        "both the created and appended rows are visible"
    );

    drop(warehouse);
}
