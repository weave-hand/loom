//! The BuildVectorIndexRequest carries the optional IVF and HNSW selector fields.

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
        m: 0,
        ef_construction: 0,
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
        m: 0,
        ef_construction: 0,
    };
    let back = pb::BuildVectorIndexRequest::decode(req.encode_to_vec().as_slice()).unwrap();
    assert!(back.index_kind.is_empty());
    assert_eq!(back.nlist, 0);
}

#[test]
fn request_roundtrips_hnsw_fields() {
    let req = pb::BuildVectorIndexRequest {
        schema: "wh".into(),
        name: "docs".into(),
        column: "embedding".into(),
        index_kind: "hnsw".into(),
        nlist: 0,
        m: 16,
        ef_construction: 200,
    };
    let bytes = req.encode_to_vec();
    let back = pb::BuildVectorIndexRequest::decode(bytes.as_slice()).unwrap();
    assert_eq!(back.index_kind, "hnsw");
    assert_eq!(back.m, 16);
    assert_eq!(back.ef_construction, 200);
}
