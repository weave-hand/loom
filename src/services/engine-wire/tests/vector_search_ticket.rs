use engine_wire::flight::{FlightTicket, VectorSearchTicket};

#[test]
fn vector_search_ticket_json_roundtrips() {
    let t = VectorSearchTicket {
        schema: "wh".into(),
        name: "docs".into(),
        column: "embedding".into(),
        query: vec![1.0, 0.0, 0.5, 0.25],
        k: 5,
    };
    let bytes = t.encode();
    let back = VectorSearchTicket::decode(&bytes).unwrap();
    assert_eq!(back, t);
}

#[test]
fn file_ticket_is_not_a_vector_ticket() {
    // A file FlightTicket must NOT decode as a VectorSearchTicket (disjoint fields).
    let ft = FlightTicket { schema: "wh".into(), name: "docs".into(), files: vec!["a".into()] };
    assert!(VectorSearchTicket::decode(&ft.encode()).is_err());
}

#[test]
fn vector_ticket_is_not_a_file_ticket() {
    // Symmetric: a VectorSearchTicket must NOT decode as a FlightTicket, so the
    // engine's file-path branch never swallows a k-NN ticket.
    let vt = VectorSearchTicket {
        schema: "wh".into(), name: "docs".into(), column: "embedding".into(),
        query: vec![1.0], k: 1,
    };
    assert!(FlightTicket::decode(&vt.encode()).is_err());
}
