//! Proves the codegen pipe: the generated prost types encode/decode.
use engine_wire::pb::{PingRequest, PingResponse};
use prost::Message;

#[test]
fn ping_request_roundtrips() {
    let req = PingRequest { note: "hi".into() };
    let bytes = req.encode_to_vec();
    let back = PingRequest::decode(bytes.as_slice()).unwrap();
    assert_eq!(back.note, "hi");
}

#[test]
fn ping_response_roundtrips() {
    let resp = PingResponse { note: "ok".into() };
    let back = PingResponse::decode(resp.encode_to_vec().as_slice()).unwrap();
    assert_eq!(back.note, "ok");
}
