//! The BuildVectorIndexRequest carries `(schema, name, index_name)` — the build
//! resolves kind/metric/params from the named ontology declaration, so the request
//! itself carries no build knobs.

use engine_wire::pb;
use prost::Message;

#[test]
fn request_roundtrips_index_name() {
    let req = pb::BuildVectorIndexRequest {
        schema: "wh".into(),
        name: "docs".into(),
        index_name: "by_sim".into(),
    };
    let bytes = req.encode_to_vec();
    let back = pb::BuildVectorIndexRequest::decode(bytes.as_slice()).unwrap();
    assert_eq!(back.schema, "wh");
    assert_eq!(back.name, "docs");
    assert_eq!(back.index_name, "by_sim");
}

#[test]
fn request_defaults_are_empty_strings() {
    // proto3 scalar defaults: "".
    let req = pb::BuildVectorIndexRequest::default();
    let back = pb::BuildVectorIndexRequest::decode(req.encode_to_vec().as_slice()).unwrap();
    assert!(back.schema.is_empty());
    assert!(back.name.is_empty());
    assert!(back.index_name.is_empty());
}
