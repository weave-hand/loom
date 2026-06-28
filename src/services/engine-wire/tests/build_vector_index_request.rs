//! The BuildVectorIndexRequest carries the optional IVF selector fields.

use engine_wire::pb;
use prost::Message;

#[test]
fn request_roundtrips_ivf_fields() {
    let req = pb::BuildVectorIndexRequest {
        schema: "wh".into(),
        name: "docs".into(),
        column: "embedding".into(),
        index_kind: "ivf_flat".into(),
        nlist: 32,
    };
    let bytes = req.encode_to_vec();
    let back = pb::BuildVectorIndexRequest::decode(bytes.as_slice()).unwrap();
    assert_eq!(back.index_kind, "ivf_flat");
    assert_eq!(back.nlist, 32);
}

#[test]
fn request_defaults_are_empty_kind_zero_nlist() {
    // proto3 scalar defaults: "" and 0 — the "flat / auto" sentinel.
    let req = pb::BuildVectorIndexRequest {
        schema: "wh".into(),
        name: "docs".into(),
        column: "embedding".into(),
        index_kind: String::new(),
        nlist: 0,
    };
    let back = pb::BuildVectorIndexRequest::decode(req.encode_to_vec().as_slice()).unwrap();
    assert!(back.index_kind.is_empty());
    assert_eq!(back.nlist, 0);
}
