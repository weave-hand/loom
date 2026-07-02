//! Unit pins for `EngineTicket::decode` — the four-plane ticket dispatch that
//! previously lived inline in the engine's `do_get`. Pins the decode ORDER
//! (protobuf `Any` first, then governed/kNN/file JSON fall-through), the
//! disjointness the ticket types' `deny_unknown_fields` guarantees, and the
//! exact error statuses/messages the engine's Status conversion emits.
//! Pure logic — no running engine, runs on RE.

use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt, TicketStatementQuery};
use control_plane_core::GovernedCatalog;
use engine_wire::flight::{
    EngineTicket, FlightTicket, GovernedStatementQuery, TicketError, VectorSearchTicket,
};
use prost::Message;

fn tsq_bytes(handle: Vec<u8>) -> Vec<u8> {
    TicketStatementQuery {
        statement_handle: handle.into(),
    }
    .as_any()
    .encode_to_vec()
}

#[test]
fn flight_sql_ticket_decodes_to_sql() {
    let t = EngineTicket::decode(&tsq_bytes(b"SELECT 1".to_vec())).expect("decode");
    assert!(matches!(t, EngineTicket::Sql(s) if s == "SELECT 1"));
}

#[test]
fn governed_ticket_decodes_to_governed() {
    let q = GovernedStatementQuery {
        sql: "SELECT 1".into(),
        catalog: GovernedCatalog { tables: vec![] },
    };
    let t = EngineTicket::decode(&q.encode()).expect("decode");
    assert!(matches!(t, EngineTicket::GovernedSql(g) if g == q));
}

#[test]
fn vector_ticket_decodes_to_vector_search() {
    let v = VectorSearchTicket {
        schema: "wh".into(),
        name: "docs".into(),
        index_name: "by_flat".into(),
        query: vec![1.0, 0.0],
        k: 2,
        nprobe: None,
        ef_search: None,
    };
    let t = EngineTicket::decode(&v.encode()).expect("decode");
    assert!(matches!(t, EngineTicket::VectorSearch(got) if got == v));
}

#[test]
fn file_ticket_decodes_to_files() {
    let f = FlightTicket {
        schema: "wh".into(),
        name: "orders".into(),
        files: vec!["data/loom-abc.parquet".into()],
    };
    let t = EngineTicket::decode(&f.encode()).expect("decode");
    assert!(matches!(t, EngineTicket::Files(got) if got == f));
}

// --- error contract (must match the pre-refactor do_get statuses verbatim) ---

#[test]
fn garbage_is_the_terminal_file_ticket_error() {
    let err = EngineTicket::decode(b"not json").expect_err("garbage must fail");
    assert!(
        matches!(&err, TicketError::BadFileTicket(_)),
        "got: {err:?}"
    );
    assert!(
        err.to_string().starts_with("bad flight ticket: "),
        "got: {err}"
    );
    let s = tonic::Status::from(err);
    assert_eq!(s.code(), tonic::Code::InvalidArgument);
    assert!(s.message().starts_with("bad flight ticket: "));
}

#[test]
fn non_utf8_flight_sql_handle_fails_in_place() {
    // A MATCHED TicketStatementQuery must not fall through to the JSON stages.
    let err =
        EngineTicket::decode(&tsq_bytes(vec![0xff, 0xfe, 0xfd])).expect_err("non-utf8 must fail");
    assert!(matches!(&err, TicketError::NonUtf8Sql(_)), "got: {err:?}");
    assert!(err.to_string().starts_with("non-utf8 sql: "), "got: {err}");
    let s = tonic::Status::from(err);
    assert_eq!(s.code(), tonic::Code::InvalidArgument);
    assert!(s.message().starts_with("non-utf8 sql: "));
}

#[test]
fn wrong_any_type_falls_through_to_the_file_plane() {
    // A valid protobuf Any of a DIFFERENT type is not a flight-sql ticket; it
    // must fall through the JSON stages and fail as a file ticket.
    let cmd = CommandStatementQuery {
        query: "SELECT 1".into(),
        transaction_id: None,
    };
    let err = EngineTicket::decode(&cmd.as_any().encode_to_vec())
        .expect_err("wrong Any type must fall through and fail");
    assert!(
        matches!(&err, TicketError::BadFileTicket(_)),
        "got: {err:?}"
    );
}

#[test]
fn unpack_none_maps_to_internal() {
    // FlightSqlEmpty is an arrow-flight invariant violation (is::<T>() matched but
    // unpack returned None) — the server's fault: internal, not invalid_argument.
    let s = tonic::Status::from(TicketError::FlightSqlEmpty);
    assert_eq!(s.code(), tonic::Code::Internal);
    assert_eq!(s.message(), "flight-sql ticket unpack returned None");
}

#[test]
fn json_planes_stay_disjoint() {
    // deny_unknown_fields keeps the three JSON ticket shapes mutually exclusive —
    // the property the fall-through decode order depends on.
    let f = FlightTicket {
        schema: "s".into(),
        name: "t".into(),
        files: vec![],
    };
    assert!(matches!(
        EngineTicket::decode(&f.encode()),
        Ok(EngineTicket::Files(_))
    ));
    let v = VectorSearchTicket {
        schema: "s".into(),
        name: "t".into(),
        index_name: "i".into(),
        query: vec![],
        k: 1,
        nprobe: None,
        ef_search: None,
    };
    assert!(matches!(
        EngineTicket::decode(&v.encode()),
        Ok(EngineTicket::VectorSearch(_))
    ));
}
