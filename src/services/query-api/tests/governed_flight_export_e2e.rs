//! Acceptance e2e (road-governed-flight-export): proves the governed Arrow Flight export
//! end-to-end. Boots a real engine `FlightDataService` over a UDS, lands a 1500-row
//! `vector(4)` typed object through the real Iceberg landing path, stands the
//! `FlightExportService` up on a TCP port, and drives it with a REAL Arrow Flight client.
//! Asserts: (1) all 1500 rows stream back with the embedding carried natively as
//! `List<Float32>` (value-exact, NO `LIMIT 1000`, no `SqlValue` flattening to Utf8);
//! (2) an authenticated token without a Read grant on the type is `PermissionDenied`;
//! (3) a request with no bearer token is `Unauthenticated`;
//! (4) a column-masked scalar (`id`) is advertised AND streamed as `Utf8` with every value the
//! literal `'***'`, while the unmasked `embedding` survives value-exact as `List<Float32>`.

use loom_test_flight::{EngineGuard, spawn_flight_uds};
use loom_test_seed::local_sql_catalog;
use std::sync::Arc;

use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_array::{Array, Float32Array, Int64Array, ListArray, RecordBatch, StringArray};
use arrow_flight::FlightDescriptor;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    Auth, ColumnSpec, ControlPlane, DatasetId, EventType, LineageEvent, ObjectType, Ontology,
    PropertyDef, RunId, TableRef, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use engine_wire::flight::FlightSqlClient;
use futures::TryStreamExt;
use query_api::flight_export::{ExportCommand, FlightExportService};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

/// The `vector(4)` dataset: `id` (long) + `embedding` (vector(4)), both non-null.
fn columns() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        },
        ColumnSpec {
            name: "embedding".into(),
            ty: "vector(4)".into(),
            nullable: false,
        },
    ]
}

/// Arrow IPC body: `id: long` + `embedding: list<float>` (non-null element). Builds `rows`
/// rows of width-4 embeddings. Row 0 is the deterministic `[0.1, 0.2, 0.3, 0.4]` used for the
/// value-exact assertion; the rest are derived from the row index (none can collide with row
/// 0). 1500 rows proves the export carries past any 1000-row cap.
/// `land` now takes pre-decoded batches; build the schema + batch directly
/// rather than round-tripping through an Arrow IPC encode/decode.
fn ipc_body(rows: usize) -> (Arc<Schema>, Vec<RecordBatch>) {
    let element = Arc::new(Field::new("item", DataType::Float32, false));
    let mut lb = ListBuilder::new(Float32Builder::new()).with_field(element.clone());
    for r in 0..rows {
        let v: [f32; 4] = if r == 0 {
            [0.1, 0.2, 0.3, 0.4]
        } else {
            let b = r as f32;
            [b, b + 0.25, b + 0.5, b + 0.75]
        };
        lb.values().append_slice(&v);
        lb.append(true);
    }
    let embedding = lb.finish();
    let id = Int64Array::from((0..rows as i64).collect::<Vec<i64>>());
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("embedding", DataType::List(element), false),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(id), Arc::new(embedding)])
        .expect("batch");
    (schema, vec![batch])
}

fn lineage(run: RunId, schema: &str, name: &str) -> LineageEvent {
    let out = TableRef {
        schema: schema.into(),
        name: name.into(),
    };
    LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(&out).dataset_ref()],
        payload: serde_json::json!({ "source": "test" }),
    }
}

/// Wrap a Flight message in a `tonic::Request` carrying a `Bearer` authorization header.
fn authed<T>(msg: T, token: &str) -> tonic::Request<T> {
    let mut req = tonic::Request::new(msg);
    req.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("metadata value"),
    );
    req
}

/// The export command for the seeded `Chunk` type with no filters / ids.
fn chunk_cmd() -> ExportCommand {
    ExportCommand {
        type_name: "Chunk".into(),
        filters: vec![],
        ids: vec![],
    }
}

/// Full harness: land the `vector(4)` dataset, register the `Chunk` ontology type, grant the
/// `reader` role Read, boot the engine over a UDS, and stand the `FlightExportService` up on
/// an ephemeral TCP port. Returns `(tcp addr, reader bearer token, control-plane handle, wh
/// tempdir, engine guard)`. The tempdir and guard MUST stay alive for the test's duration — the
/// engine reads the warehouse Parquet through the socket; dropping either pulls the files /
/// listener out from under it.
async fn setup(
    fx: &PgFixture,
) -> (
    std::net::SocketAddr,
    String,
    Arc<PgControlPlane>,
    tempfile::TempDir,
    EngineGuard,
) {
    setup_with_cap(fx, 100_000).await
}

/// Like [`setup`] but with a configurable `max_rows` export cap, for the row-cap test.
async fn setup_with_cap(
    fx: &PgFixture,
    max_rows: u32,
) -> (
    std::net::SocketAddr,
    String,
    Arc<PgControlPlane>,
    tempfile::TempDir,
    EngineGuard,
) {
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let wh = tempfile::tempdir().expect("warehouse");
    let warehouse = wh.path().display().to_string();

    // Land the vector dataset through the REAL landing path. limit 0 forces the Parquet write
    // path (the inline path is scalar-only and cannot store vectors).
    let table = TableRef {
        schema: "wh".into(),
        name: "chunks".into(),
    };
    let catalog = local_sql_catalog(dsn.clone(), &warehouse).await;
    let (schema, batches) = ipc_body(1500);
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "chunks"),
    )
    .await
    .expect("land vector");

    // Register the ontology type — its table MUST equal the landed `TableRef`.
    cp.define_type(ObjectType {
        name: TypeName("Chunk".into()),
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "long".into(),
                required: true,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
            PropertyDef {
                name: "embedding".into(),
                ty: "vector(4)".into(),
                required: true,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
        ],
        derived: vec![],
        table: table.clone(),
        identity: Some("id".into()),
    })
    .await
    .expect("define Chunk type");

    // ACL: a `reader` subject/role granted Read on Chunk, plus a live session token.
    let (_subj, role) = e2e_support::subject_with_role(&cp, "reader").await;
    e2e_support::grant_read(&cp, &role, "Chunk").await;
    let token = e2e_support::session_token(&cp, "reader").await;

    // Boot the engine over a UDS, then build the export service over a Flight-SQL client to it.
    let eng = spawn_flight_uds(fx, &db, &warehouse).await;
    let cp = Arc::new(cp);
    let auth: Arc<dyn Auth + Send + Sync> = cp.clone();
    let cp_dyn: Arc<dyn ControlPlane> = cp.clone();
    let flight_engine = FlightSqlClient::connect(eng.sock.clone())
        .await
        .expect("engine connect");
    let export = FlightExportService::new(auth, cp_dyn, flight_engine, max_rows);

    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind tcp");
    let addr = tcp.local_addr().expect("addr");
    let incoming = TcpListenerStream::new(tcp);
    tokio::spawn(async move {
        let _serve = Server::builder()
            .add_service(FlightServiceServer::new(export))
            .serve_with_incoming(incoming)
            .await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    (addr, token, cp, wh, eng)
}

/// Like [`setup`] but masks the scalar `id` column via a column policy, for the masked-export
/// case. Identical landing / engine / export wiring; only the ACL grant differs (coarse Read
/// Allow + a `mask_columns: ["id"]` policy). The `vector(4)` `embedding` stays unmasked.
async fn setup_with_mask(
    fx: &PgFixture,
) -> (
    std::net::SocketAddr,
    String,
    Arc<PgControlPlane>,
    tempfile::TempDir,
    EngineGuard,
) {
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let wh = tempfile::tempdir().expect("warehouse");
    let warehouse = wh.path().display().to_string();

    let table = TableRef {
        schema: "wh".into(),
        name: "chunks".into(),
    };
    let catalog = local_sql_catalog(dsn.clone(), &warehouse).await;
    let (schema, batches) = ipc_body(1500);
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "chunks"),
    )
    .await
    .expect("land vector");

    cp.define_type(ObjectType {
        name: TypeName("Chunk".into()),
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "long".into(),
                required: true,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
            PropertyDef {
                name: "embedding".into(),
                ty: "vector(4)".into(),
                required: true,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
        ],
        derived: vec![],
        table: table.clone(),
        identity: Some("id".into()),
    })
    .await
    .expect("define Chunk type");

    // ACL: Read Allow on Chunk refined by a column policy that MASKS the scalar `id`.
    let (_subj, role) = e2e_support::subject_with_role(&cp, "reader").await;
    e2e_support::grant_read_columns(&cp, &role, "Chunk", vec![], vec!["id".into()]).await;
    let token = e2e_support::session_token(&cp, "reader").await;

    let eng = spawn_flight_uds(fx, &db, &warehouse).await;
    let cp = Arc::new(cp);
    let auth: Arc<dyn Auth + Send + Sync> = cp.clone();
    let cp_dyn: Arc<dyn ControlPlane> = cp.clone();
    let flight_engine = FlightSqlClient::connect(eng.sock.clone())
        .await
        .expect("engine connect");
    let export = FlightExportService::new(auth, cp_dyn, flight_engine, 100_000);

    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind tcp");
    let addr = tcp.local_addr().expect("addr");
    let incoming = TcpListenerStream::new(tcp);
    tokio::spawn(async move {
        let _serve = Server::builder()
            .add_service(FlightServiceServer::new(export))
            .serve_with_incoming(incoming)
            .await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    (addr, token, cp, wh, eng)
}

/// Authed get_flight_info + do_get: all 1500 rows arrive, the embedding is carried natively
/// as `List<Float32>` (not flattened to Utf8 through `SqlValue`), and some row is value-exact.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn export_streams_vectors_value_exact() {
    let fx = PgFixture::shared();
    let (addr, token, _cp, _wh, _sock) = setup(fx).await;

    let url = format!("http://{addr}");
    let channel = tonic::transport::Endpoint::try_from(url)
        .expect("endpoint")
        .connect()
        .await
        .expect("connect");
    let mut client = FlightServiceClient::new(channel);
    let desc = FlightDescriptor::new_cmd(chunk_cmd().encode());
    let info = client
        .get_flight_info(authed(desc, &token))
        .await
        .expect("get_flight_info")
        .into_inner();
    let ticket = info
        .endpoint
        .into_iter()
        .next()
        .and_then(|e| e.ticket)
        .expect("ticket");
    let stream = client
        .do_get(authed(ticket, &token))
        .await
        .expect("do_get")
        .into_inner();
    let data = stream.map_err(FlightError::from);
    let batches: Vec<RecordBatch> = FlightRecordBatchStream::new_from_flight_data(data)
        .try_collect()
        .await
        .expect("collect batches");

    // All 1500 rows arrive — no LIMIT 1000.
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 1500, "all 1500 rows must export (no LIMIT 1000)");

    // The embedding is carried natively as List<Float32>, NOT flattened to Utf8 via SqlValue.
    assert!(!batches.is_empty(), "export produced at least one batch");
    let emb = batches[0]
        .column_by_name("embedding")
        .expect("embedding column");
    assert!(
        matches!(emb.data_type(), DataType::List(_)),
        "embedding must be List<Float32>, not Utf8 (a SqlValue regression would Utf8-ify it)"
    );

    // Some row is value-exact (row 0 = [0.1, 0.2, 0.3, 0.4]).
    let mut embeddings: Vec<Vec<f32>> = Vec::new();
    for batch in &batches {
        let list = batch
            .column_by_name("embedding")
            .expect("embedding column")
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("list array");
        for row in list.iter().flatten() {
            let f = row
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("float32 element");
            embeddings.push(f.values().to_vec());
        }
    }
    assert!(
        embeddings.contains(&vec![0.1f32, 0.2, 0.3, 0.4]),
        "expected a row with embedding [0.1, 0.2, 0.3, 0.4] carried value-exact"
    );
}

/// A second authenticated subject WITHOUT a Read grant on Chunk is `PermissionDenied`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn export_denied_without_grant() {
    let fx = PgFixture::shared();
    let (addr, _token, cp, _wh, _sock) = setup(fx).await;

    // A valid token for a subject with a role but no Read grant on Chunk.
    let (_subj, _role) = e2e_support::subject_with_role(cp.as_ref(), "intruder").await;
    let token = e2e_support::session_token(cp.as_ref(), "intruder").await;

    let url = format!("http://{addr}");
    let channel = tonic::transport::Endpoint::try_from(url)
        .expect("endpoint")
        .connect()
        .await
        .expect("connect");
    let mut client = FlightServiceClient::new(channel);
    let desc = FlightDescriptor::new_cmd(chunk_cmd().encode());
    let result = client.get_flight_info(authed(desc, &token)).await;
    assert_eq!(
        result.unwrap_err().code(),
        tonic::Code::PermissionDenied,
        "an authenticated subject without a Read grant must be denied"
    );
}

/// A request with no bearer token at all is `Unauthenticated`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn export_requires_bearer_token() {
    let fx = PgFixture::shared();
    let (addr, _token, _cp, _wh, _sock) = setup(fx).await;

    let url = format!("http://{addr}");
    let channel = tonic::transport::Endpoint::try_from(url)
        .expect("endpoint")
        .connect()
        .await
        .expect("connect");
    let mut client = FlightServiceClient::new(channel);
    let desc = FlightDescriptor::new_cmd(chunk_cmd().encode());
    let result = client.get_flight_info(tonic::Request::new(desc)).await;
    assert_eq!(
        result.unwrap_err().code(),
        tonic::Code::Unauthenticated,
        "a request with no bearer token must be unauthenticated"
    );
}

/// A well-formed but unrecognised bearer token is `Unauthenticated` (not just a missing one).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn export_rejects_invalid_token() {
    let fx = PgFixture::shared();
    let (addr, _token, _cp, _wh, _sock) = setup(fx).await;

    let url = format!("http://{addr}");
    let channel = tonic::transport::Endpoint::try_from(url)
        .expect("endpoint")
        .connect()
        .await
        .expect("connect");
    let mut client = FlightServiceClient::new(channel);
    let desc = FlightDescriptor::new_cmd(chunk_cmd().encode());
    // A syntactically valid bearer header carrying a token that resolves to no session.
    let result = client
        .get_flight_info(authed(desc, "not-a-real-session-token"))
        .await;
    assert_eq!(
        result.unwrap_err().code(),
        tonic::Code::Unauthenticated,
        "an invalid/unknown bearer token must be unauthenticated"
    );
}

/// The `LOOM_EXPORT_MAX_ROWS` cap is a real guard: an export whose governed slice exceeds the
/// cap ends the `do_get` stream with an ERROR (compiled with `LIMIT max_rows+1`, the stream
/// errors past `max_rows`) rather than silently truncating. Cap 1000 against the 1500-row
/// dataset must fail the stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn export_cap_exceeded_errors_stream() {
    let fx = PgFixture::shared();
    let (addr, token, _cp, _wh, _sock) = setup_with_cap(fx, 1000).await;

    let url = format!("http://{addr}");
    let channel = tonic::transport::Endpoint::try_from(url)
        .expect("endpoint")
        .connect()
        .await
        .expect("connect");
    let mut client = FlightServiceClient::new(channel);
    let desc = FlightDescriptor::new_cmd(chunk_cmd().encode());
    let info = client
        .get_flight_info(authed(desc, &token))
        .await
        .expect("get_flight_info")
        .into_inner();
    let ticket = info
        .endpoint
        .into_iter()
        .next()
        .and_then(|e| e.ticket)
        .expect("ticket");
    let stream = client
        .do_get(authed(ticket, &token))
        .await
        .expect("do_get")
        .into_inner();
    let data = stream.map_err(FlightError::from);
    let collected: Result<Vec<RecordBatch>, _> =
        FlightRecordBatchStream::new_from_flight_data(data)
            .try_collect()
            .await;
    assert!(
        collected.is_err(),
        "an export exceeding LOOM_EXPORT_MAX_ROWS must fail the stream, not truncate silently"
    );
}

/// A column-masked scalar (`id`) exports end-to-end through the live engine with the advertised
/// `get_flight_info` schema and the streamed `do_get` data schema in lockstep: both report the
/// masked column as `Utf8`, every masked value is the literal `"***"`, and the UNmasked
/// `embedding` survives value-exact as `List<Float32>` (no over-masking).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn export_masked_scalar_schema_and_values() {
    let fx = PgFixture::shared();
    let (addr, token, _cp, _wh, _sock) = setup_with_mask(fx).await;

    let url = format!("http://{addr}");
    let channel = tonic::transport::Endpoint::try_from(url)
        .expect("endpoint")
        .connect()
        .await
        .expect("connect");
    let mut client = FlightServiceClient::new(channel);
    let desc = FlightDescriptor::new_cmd(chunk_cmd().encode());
    let info = client
        .get_flight_info(authed(desc, &token))
        .await
        .expect("get_flight_info")
        .into_inner();

    // (1) Advertised schema: the masked `id` field is Utf8 (the '***' constant's type), not Int64.
    let advertised = info
        .clone()
        .try_decode_schema()
        .expect("decode advertised schema");
    let adv_id = advertised
        .field_with_name("id")
        .expect("advertised id field");
    assert_eq!(
        adv_id.data_type(),
        &DataType::Utf8,
        "the masked `id` must be advertised as Utf8 (the '***' constant), not its declared Int64"
    );

    let ticket = info
        .endpoint
        .into_iter()
        .next()
        .and_then(|e| e.ticket)
        .expect("ticket");
    let stream = client
        .do_get(authed(ticket, &token))
        .await
        .expect("do_get")
        .into_inner();
    let data = stream.map_err(FlightError::from);
    let batches: Vec<RecordBatch> = FlightRecordBatchStream::new_from_flight_data(data)
        .try_collect()
        .await
        .expect("collect batches");
    assert!(!batches.is_empty(), "export produced at least one batch");

    // (2) Data-schema lockstep: the streamed `id` column is ALSO Utf8 (advertised == data).
    let data_id = batches[0]
        .schema()
        .field_with_name("id")
        .expect("data id field")
        .data_type()
        .clone();
    assert_eq!(
        data_id,
        DataType::Utf8,
        "the streamed `id` must be Utf8 — the divergence guard (advertised schema == data schema)"
    );

    // (3) Masked values: every `id` across all batches is the literal "***".
    for batch in &batches {
        let ids = batch
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("id is a Utf8/StringArray");
        for i in 0..ids.len() {
            assert_eq!(
                ids.value(i),
                "***",
                "every masked id value must be the '***' literal"
            );
        }
    }

    // Spot check (no over-masking): the UNmasked `embedding` survives as List<Float32>, and the
    // deterministic seed vector [0.1, 0.2, 0.3, 0.4] is still present (order-independent — the
    // export SQL has no ORDER BY) — only `id` was replaced.
    let emb = batches[0]
        .column_by_name("embedding")
        .expect("embedding column");
    assert!(
        matches!(emb.data_type(), DataType::List(_)),
        "the unmasked embedding must stay List<Float32>, not be masked to Utf8"
    );
    let mut embeddings: Vec<Vec<f32>> = Vec::new();
    for batch in &batches {
        let list = batch
            .column_by_name("embedding")
            .expect("embedding column")
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("list array");
        for row in list.iter().flatten() {
            let f = row
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("float32 element");
            embeddings.push(f.values().to_vec());
        }
    }
    assert!(
        embeddings.contains(&vec![0.1f32, 0.2, 0.3, 0.4]),
        "the unmasked embedding must carry the seed vector [0.1, 0.2, 0.3, 0.4] value-exact"
    );
}
