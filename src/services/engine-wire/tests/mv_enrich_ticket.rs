//! MvEnrichTicket round-trip + EngineTicket decode-chain routing. The required
//! enrich_schema/enrich_name fields keep it deny_unknown_fields-disjoint from
//! every other JSON ticket shape.

use engine_wire::flight::{EngineTicket, FlightTicket, MvEnrichTicket};

#[test]
fn mv_enrich_ticket_round_trips_keyed_and_unkeyed() {
    for ticket in [
        MvEnrichTicket {
            enrich_schema: "s".into(),
            enrich_name: "customers".into(),
            key: Some("id".into()),
            keys: vec![serde_json::json!(1), serde_json::json!(2)],
        },
        MvEnrichTicket {
            enrich_schema: "s".into(),
            enrich_name: "customers".into(),
            key: None,
            keys: vec![],
        },
    ] {
        let bytes = ticket.encode();
        match EngineTicket::decode(&bytes).expect("decodes") {
            EngineTicket::MvEnrich(back) => assert_eq!(back, ticket),
            other => panic!("misrouted: {other:?}"),
        }
    }
}

#[test]
fn keys_field_defaults_empty() {
    let bytes = br#"{"enrich_schema":"s","enrich_name":"t","key":null}"#;
    match EngineTicket::decode(bytes).expect("decodes") {
        EngineTicket::MvEnrich(t) => assert!(t.keys.is_empty() && t.key.is_none()),
        other => panic!("misrouted: {other:?}"),
    }
}

#[test]
fn existing_tickets_still_route_unchanged() {
    // The terminal file ticket must not be shadowed by the new arm.
    let files = FlightTicket {
        schema: "s".into(),
        name: "t".into(),
        files: vec!["f".into()],
    };
    match EngineTicket::decode(&files.encode()).expect("decodes") {
        EngineTicket::Files(back) => assert_eq!(back, files),
        other => panic!("misrouted: {other:?}"),
    }
}
