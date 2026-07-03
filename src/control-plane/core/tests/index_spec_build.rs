//! `IndexSpec::build` — the single authoritative spec→index constructor
//! routing (moved from the postgres adapter's build_vector_index, job 7).
//! Byte-identity with the direct constructors is asserted so the routing can
//! never drift from the codec goldens (tests/vector_index_codec.rs) — the
//! constructors are deterministic (fixed-seed SplitMix64).

use control_plane_core::{
    FlatIndex, HnswIndex, IndexKind, IndexSpec, IvfFlatIndex, Metric, VectorIndex, VectorKey,
};

fn rows(n: i64) -> Vec<(VectorKey, Vec<f32>)> {
    (0..n)
        .map(|i| (VectorKey::Int(i), vec![i as f32, 1.0, 2.0]))
        .collect()
}

#[test]
fn flat_routes_and_bytes_match_direct_constructor() {
    let built = IndexSpec::Flat
        .build(3, Metric::Cosine, rows(8))
        .expect("spec build");
    assert_eq!(built.index_kind(), IndexKind::Flat);
    assert_eq!(built.dim(), 3);
    assert_eq!(built.row_count(), 8);
    let direct = FlatIndex::build(3, Metric::Cosine, rows(8)).expect("direct build");
    assert_eq!(
        built.serialize(),
        direct.serialize(),
        "routing adds nothing to the bytes"
    );
}

#[test]
fn ivf_routes_with_and_without_nlist() {
    let built = IndexSpec::IvfFlat { nlist: Some(2) }
        .build(3, Metric::L2, rows(16))
        .expect("spec build");
    assert_eq!(built.index_kind(), IndexKind::IvfFlat);
    let direct = IvfFlatIndex::build(3, Metric::L2, rows(16), Some(2)).expect("direct build");
    assert_eq!(built.serialize(), direct.serialize());

    let defaulted = IndexSpec::IvfFlat { nlist: None }
        .build(3, Metric::L2, rows(16))
        .expect("default nlist");
    assert_eq!(defaulted.index_kind(), IndexKind::IvfFlat);
    let direct_default = IvfFlatIndex::build(3, Metric::L2, rows(16), None).expect("direct");
    assert_eq!(defaulted.serialize(), direct_default.serialize());
}

#[test]
fn hnsw_routes_with_params() {
    let built = IndexSpec::Hnsw {
        m: Some(4),
        ef_construction: Some(32),
    }
    .build(3, Metric::Cosine, rows(16))
    .expect("spec build");
    assert_eq!(built.index_kind(), IndexKind::Hnsw);
    let direct =
        HnswIndex::build(3, Metric::Cosine, rows(16), Some(4), Some(32)).expect("direct build");
    assert_eq!(
        built.serialize(),
        direct.serialize(),
        "same params, same deterministic bytes"
    );
}

#[test]
fn constructor_errors_propagate() {
    // rows are 3-wide, declared dim 4: pack_rows' dim-mismatch error surfaces.
    assert!(IndexSpec::Flat.build(4, Metric::Cosine, rows(4)).is_err());
}
