//! Pins for `do_get`'s ticket-decode error contract — the statuses and messages
//! the four-stage decode emits for malformed tickets. No test previously sent a
//! garbage or non-utf8 ticket, so these behaviors were unpinned; they must stay
//! byte-identical when the dispatch moves onto `engine_wire::flight::EngineTicket`
//! (road-engine-wire-dedup). Green against the pre-refactor code.

use std::sync::Arc;
use std::time::Duration;

use arrow_flight::Ticket;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt, TicketStatementQuery};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalogBuilder,
};
use engine::flight::FlightDataService;
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;
use prost::Message;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::{Endpoint, Server, Uri};

/// Boot a `FlightDataService` on a UDS and return a RAW `FlightServiceClient`
/// (the wrapper clients only send well-formed tickets; these pins need to put
/// arbitrary bytes in `Ticket.ticket` and read the tonic `Status` directly).
async fn spawn_raw(
    fx: &PgFixture,
    db: &str,
    warehouse: &str,
) -> (
    tempfile::TempDir,
    FlightServiceClient<tonic::transport::Channel>,
) {
    let sock_dir = tempfile::tempdir().expect("socket dir");
    let sock_path = sock_dir.path().join("engine.sock");
    let sock = sock_path.to_string_lossy().to_string();

    let pool = fx.pool_for(db).await;
    let mut props = std::collections::HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), fx.pg_dsn(db));
    props.insert(
        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
        format!("file://{warehouse}"),
    );
    let catalog = SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("loom", props)
        .await
        .expect("catalog");
    let svc = FlightDataService {
        catalog,
        serving_catalog: IcebergCatalog::new(pool.clone()),
        serving_store: None,
        pool,
    };

    let listener = tokio::net::UnixListener::bind(&sock_path).expect("bind uds");
    let incoming = UnixListenerStream::new(listener);
    tokio::spawn(async move {
        drop(
            Server::builder()
                .add_service(FlightServiceServer::new(svc))
                .serve_with_incoming(incoming)
                .await,
        );
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    let channel = Endpoint::try_from("http://[::]:50051")
        .expect("endpoint")
        .connect_with_connector(tower::service_fn(move |_: Uri| {
            let sock = sock.clone();
            async move {
                let stream = tokio::net::UnixStream::connect(sock).await?;
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
            }
        }))
        .await
        .expect("connect uds");
    (sock_dir, FlightServiceClient::new(channel))
}

async fn do_get_err(
    client: &mut FlightServiceClient<tonic::transport::Channel>,
    ticket: Vec<u8>,
) -> tonic::Status {
    // match, not expect_err: the Ok side (Response<Streaming<..>>) has no
    // useful Debug and must never be printed anyway.
    match client
        .do_get(Ticket {
            ticket: ticket.into(),
        })
        .await
    {
        Ok(_) => panic!("malformed ticket must be rejected"),
        Err(s) => s,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ticket_decode_error_contract() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let (_sock_dir, mut client) = spawn_raw(fx, &db, &wh.path().display().to_string()).await;

    // Stage 4 terminal: bytes that are no known ticket -> the FILE plane's decode
    // error (the last stage in the fall-through chain), never a schema/path leak.
    let err = do_get_err(&mut client, b"not json".to_vec()).await;
    assert_eq!(err.code(), tonic::Code::InvalidArgument, "got: {err}");
    assert!(
        err.message().starts_with("bad flight ticket: "),
        "terminal error is the file-plane decode, got: {}",
        err.message()
    );

    // Stage 1, no fall-through: a matched TicketStatementQuery whose handle is
    // not UTF-8 errors immediately (it must NOT be retried as JSON).
    let tsq = TicketStatementQuery {
        statement_handle: vec![0xff, 0xfe, 0xfd].into(),
    };
    let err = do_get_err(&mut client, tsq.as_any().encode_to_vec()).await;
    assert_eq!(err.code(), tonic::Code::InvalidArgument, "got: {err}");
    assert!(
        err.message().starts_with("non-utf8 sql: "),
        "matched flight-sql ticket fails in place, got: {}",
        err.message()
    );

    // Stage 1 -> 4 fall-through: a VALID protobuf Any of the WRONG type is not a
    // flight-sql ticket; it falls through the JSON stages to the terminal error.
    let cmd = CommandStatementQuery {
        query: "SELECT 1".into(),
        transaction_id: None,
    };
    let err = do_get_err(&mut client, cmd.as_any().encode_to_vec()).await;
    assert_eq!(err.code(), tonic::Code::InvalidArgument, "got: {err}");
    assert!(
        err.message().starts_with("bad flight ticket: "),
        "wrong Any type falls through to the file-plane decode, got: {}",
        err.message()
    );
}
