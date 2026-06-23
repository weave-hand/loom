use engine_wire::flight::FlightTicket;

#[test]
fn ticket_json_round_trips() {
    let t = FlightTicket {
        schema: "wh".into(),
        name: "orders".into(),
        files: vec![
            "data/loom-abc.parquet".into(),
            "data/loom-def.parquet".into(),
        ],
    };
    let bytes = t.encode();
    let back = FlightTicket::decode(&bytes).expect("decode");
    assert_eq!(t, back);
}

#[test]
fn decode_rejects_garbage() {
    assert!(FlightTicket::decode(b"not json").is_err());
}
