//! Shared test-support helpers for query-api end-to-end tests.
//!
//! Provides the common `tref` / `land` / `setup` / `ids` functions used by
//! `multi_hop_traversal_e2e` and `inverse_hops_e2e`.  Direction-specific
//! helpers (`inv`, `hopf`, `srcf`, …) stay local to their respective test
//! files.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{
    Cardinality, DatasetRef, EventType, LineageEvent, LinkBacking, LinkDef, ObjectType, Ontology,
    PropertyDef, RunId, TableRef, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use ingest::{MaterializeRequest, materialize};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use query_api::render::objects_to_json;
use query_api::serving::EmbeddedDuckDb;
use time::OffsetDateTime;
use uuid::Uuid;

/// Convenience constructor for a `TableRef`.
pub fn tref(s: &str, n: &str) -> TableRef {
    TableRef {
        schema: s.into(),
        name: n.into(),
    }
}

/// Materialize a single `RecordBatch` into the DuckLake-backed control plane.
pub async fn land(
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

/// Seed three tables forming the FK chain `customer -> orders -> line_items`,
/// define the three ontology types and the two FK links (`orders`, `lineItems`),
/// and attach the serving engine.
///
/// The caller **must** keep the returned `DuckLakeWriter` alive for the duration
/// of the test — its `TempDir` holds the Parquet files that DuckDB reads;
/// dropping it removes them out from under the engine.
pub async fn setup(fx: &PgFixture) -> (PgControlPlane, EmbeddedDuckDb, DuckLakeWriter) {
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(writer.data_path()).unwrap());

    // customer(id, region): (1,'CA'), (2,'NY')
    let cust = tref("main", "customer");
    let cust_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, true),
    ]));
    let cust_batch = RecordBatch::try_new(
        cust_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("CA"), Some("NY")])),
        ],
    )
    .unwrap();
    land(&cp, &store, &cust, cust_schema, cust_batch).await;

    // orders(id, customer_id, status): (10,1,'shipped'),(11,1,'pending'),(20,2,'shipped')
    let ord = tref("main", "orders");
    let ord_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, true),
    ]));
    let ord_batch = RecordBatch::try_new(
        ord_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![10, 11, 20])),
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(StringArray::from(vec![
                Some("shipped"),
                Some("pending"),
                Some("shipped"),
            ])),
        ],
    )
    .unwrap();
    land(&cp, &store, &ord, ord_schema, ord_batch).await;

    // line_items(id, order_id, sku): (100,10,'A'),(101,10,'B'),(102,11,'C'),(200,20,'D')
    let li = tref("main", "line_items");
    let li_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("order_id", DataType::Int64, false),
        Field::new("sku", DataType::Utf8, true),
    ]));
    let li_batch = RecordBatch::try_new(
        li_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![100, 101, 102, 200])),
            Arc::new(Int64Array::from(vec![10, 10, 11, 20])),
            Arc::new(StringArray::from(vec![
                Some("A"),
                Some("B"),
                Some("C"),
                Some("D"),
            ])),
        ],
    )
    .unwrap();
    land(&cp, &store, &li, li_schema, li_batch).await;

    cp.define_type(ObjectType {
        name: TypeName("Customer".into()),
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
            },
            PropertyDef {
                name: "region".into(),
                ty: "String".into(),
                required: false,
            },
        ],
        derived: vec![],
        table: cust.clone(),
        identity: None,
    })
    .await
    .unwrap();
    cp.define_type(ObjectType {
        name: TypeName("Order".into()),
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
            },
            PropertyDef {
                name: "customer_id".into(),
                ty: "Long".into(),
                required: true,
            },
            PropertyDef {
                name: "status".into(),
                ty: "String".into(),
                required: false,
            },
        ],
        derived: vec![],
        table: ord.clone(),
        identity: None,
    })
    .await
    .unwrap();
    cp.define_type(ObjectType {
        name: TypeName("LineItem".into()),
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
            },
            PropertyDef {
                name: "order_id".into(),
                ty: "Long".into(),
                required: true,
            },
            PropertyDef {
                name: "sku".into(),
                ty: "String".into(),
                required: false,
            },
        ],
        derived: vec![],
        table: li.clone(),
        identity: None,
    })
    .await
    .unwrap();

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
    cp.define_link(LinkDef {
        name: "lineItems".into(),
        from: TypeName("Order".into()),
        to: TypeName("LineItem".into()),
        cardinality: Cardinality::Many,
        backing: LinkBacking::ForeignKey {
            from_column: "id".into(),
            to_column: "order_id".into(),
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

/// Extract sorted `id` values from a served object set.
///
/// loom renders a `Long` property as a JSON *string* (not a number), so `id`
/// is read via `as_str()`.
pub fn ids(rows: &query_api::handler::ObjectRows) -> Vec<String> {
    let body = objects_to_json(rows);
    let mut out: Vec<String> = body["objects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["id"].as_str().unwrap().to_string())
        .collect();
    out.sort_unstable();
    out
}
