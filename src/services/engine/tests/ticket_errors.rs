//! Pins for `do_get`'s ticket-decode error contract — the statuses and messages
//! the four-stage decode emits for malformed tickets. No test previously sent a
//! garbage or non-utf8 ticket, so these behaviors were unpinned; they must stay
//! byte-identical when the dispatch moves onto `engine_wire::flight::EngineTicket`
//! (road-engine-wire-dedup). Green against the pre-refactor code.

use arrow_flight::Ticket;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt, TicketStatementQuery};
use control_plane_postgres::fixture::PgFixture;
use loom_test_flight::{EngineGuard, spawn_flight_uds};
use prost::Message;
use tonic::transport::{Endpoint, Uri};

/// Boot a `FlightDataService` on a UDS and return a RAW `FlightServiceClient`
/// (the wrapper clients only send well-formed tickets; these pins need to put
/// arbitrary bytes in `Ticket.ticket` and read the tonic `Status` directly).
async fn spawn_raw(
    fx: &PgFixture,
    db: &str,
    warehouse: &str,
) -> (EngineGuard, FlightServiceClient<tonic::transport::Channel>) {
    let eng = spawn_flight_uds(fx, db, warehouse).await;
    let sock = eng.sock.clone();

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
    (eng, FlightServiceClient::new(channel))
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
