//! Engine wire test for the governed-write RPCs: drive WriteObject / OverwriteTable
//! over the UDS and assert committed snapshots.

use loom_test_seed::local_sql_catalog;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{
    ColumnSpec, ControlPlane, DatasetRef, EventType, LineageEvent, ObjectType, PropertyDef, RunId,
    TableRef, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use engine::service::EngineControlService;
use engine_serving::IcebergActionWriter;
use engine_wire::client::GrpcQueueClient;
use engine_wire::convert::LineageWire;
use engine_wire::pb::engine_control_server::EngineControlServer;
use tonic::transport::Server;
use uuid::Uuid;

// ---- helpers ---------------------------------------------------------------

fn one_row_ipc(id: i64, name: &str) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(StringArray::from(vec![name])),
        ],
    )
    .expect("batch");
    let mut buf = Vec::new();
    {
        let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &schema).expect("writer");
        w.write(&batch).expect("write");
        w.finish().expect("finish");
    }
    buf
}

fn cols() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: true,
        },
        ColumnSpec {
            name: "name".into(),
            ty: "string".into(),
            nullable: true,
        },
    ]
}

fn event(op: &str) -> LineageEvent {
    let ds = DatasetRef {
        namespace: "loom".into(),
        name: "main.widget".into(),
    };
    LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("ts"),
        inputs: vec![],
        outputs: vec![ds],
        payload: serde_json::json!({ "action": "test", "op": op }),
    }
}

/// Define the `Widget` type in the ontology so write_object / overwrite_table
/// succeed on a fresh table.
async fn seed_widget_table(cp: &PgControlPlane) {
    let widget = TypeName("Widget".into());
    cp.ontology()
        .define_type(ObjectType {
            name: widget.clone(),
            table: TableRef {
                schema: "main".into(),
                name: "widget".into(),
            },
            properties: vec![
                PropertyDef {
                    name: "id".into(),
                    ty: "Long".into(),
                    required: true,
                    constraints: control_plane_core::PropertyConstraints::default(),
                },
                PropertyDef {
                    name: "name".into(),
                    ty: "String".into(),
                    required: false,
                    constraints: control_plane_core::PropertyConstraints::default(),
                },
            ],
            derived: vec![],
            identity: None,
            version: None,
        })
        .await
        .expect("define_type");
}

// ---- test ------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_object_over_wire() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    // Seed the main.widget mirror table (same as engine-serving test).
    seed_widget_table(&cp).await;

    let wh = tempfile::tempdir().expect("wh");
    let sock_dir = tempfile::tempdir().expect("sock");
    let sock = sock_dir.path().join("engine.sock");
    let sock_str = sock.to_string_lossy().to_string();
    let wh_str = wh.path().display().to_string();

    let pool = fx.pool_for(&db).await;
    let cp2 =
        control_plane_postgres::PgControlPlane::new(pool.clone(), Duration::from_millis(5000));
    let catalog = Arc::new(local_sql_catalog(fx.pg_dsn(&db), &wh_str).await);
    let writer_catalog = Arc::new(local_sql_catalog(fx.pg_dsn(&db), &wh_str).await);
    let writer = IcebergActionWriter::new(writer_catalog, pool.clone(), 16 * 1024 * 1024, i64::MAX);

    let svc = EngineControlService {
        cp: cp2,
        catalog,
        pool,
        retention: Duration::from_secs(7 * 24 * 3600),
        writer,
    };
    let listener = tokio::net::UnixListener::bind(&sock).expect("bind");
    let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);
    tokio::spawn(async move {
        let _wh = wh;
        drop(
            Server::builder()
                .add_service(EngineControlServer::new(svc))
                .serve_with_incoming(incoming)
                .await,
        );
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    let client = GrpcQueueClient::connect(&sock_str).await.expect("connect");
    let columns_json = serde_json::to_string(&cols()).expect("cols");
    let lineage_json = serde_json::to_string(&LineageWire::from(&event("insert"))).expect("ev");

    let s1 = client
        .write_object(
            "main".into(),
            "widget".into(),
            one_row_ipc(1, "a"),
            columns_json.clone(),
            lineage_json.clone(),
        )
        .await
        .expect("write_object");
    assert!(s1 > 0);

    let s2 = client
        .overwrite_table(
            "main".into(),
            "widget".into(),
            one_row_ipc(2, "b"),
            columns_json,
            lineage_json,
        )
        .await
        .expect("overwrite_table");
    assert!(s2 > s1);

    // truncate
    let empty_cols = serde_json::to_string::<Vec<ColumnSpec>>(&vec![]).expect("empty");
    let s3 = client
        .overwrite_table(
            "main".into(),
            "widget".into(),
            Vec::new(),
            empty_cols,
            serde_json::to_string(&LineageWire::from(&event("delete"))).expect("ev"),
        )
        .await
        .expect("truncate");
    assert!(s3 > s2);
}
