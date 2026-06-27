use control_plane_core::{FlatIndex, Metric, VectorIndex, VectorKey};

fn rows() -> Vec<(VectorKey, Vec<f32>)> {
    vec![
        (VectorKey::Int(1), vec![1.0, 0.0, 0.0, 0.0]),
        (VectorKey::Int(2), vec![0.0, 1.0, 0.0, 0.0]),
        (VectorKey::Int(3), vec![0.9, 0.1, 0.0, 0.0]),
    ]
}

#[test]
fn cosine_topk_is_exact_and_ascending() {
    let idx = FlatIndex::build(4, Metric::Cosine, rows()).unwrap();
    let res = idx.search(&[1.0, 0.0, 0.0, 0.0], 2);
    assert_eq!(res.len(), 2);
    // Nearest by cosine to [1,0,0,0] is key 1 (distance 0), then key 3.
    assert_eq!(res[0].0, VectorKey::Int(1));
    assert!(res[0].1 <= res[1].1, "ascending distance");
    assert_eq!(res[1].0, VectorKey::Int(3));
    assert!((res[0].1 - 0.0).abs() < 1e-6);
}

#[test]
fn l2_topk_is_exact() {
    let idx = FlatIndex::build(4, Metric::L2, rows()).unwrap();
    let res = idx.search(&[0.0, 1.0, 0.0, 0.0], 1);
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].0, VectorKey::Int(2));
    assert!((res[0].1 - 0.0).abs() < 1e-6);
}

#[test]
fn k_larger_than_rows_returns_all() {
    let idx = FlatIndex::build(4, Metric::Cosine, rows()).unwrap();
    assert_eq!(idx.search(&[1.0, 0.0, 0.0, 0.0], 99).len(), 3);
}

#[test]
fn serialize_deserialize_is_value_exact() {
    let idx = FlatIndex::build(4, Metric::L2, rows()).unwrap();
    let bytes = idx.serialize();
    let back = FlatIndex::deserialize(&bytes).unwrap();
    assert_eq!(back.dim(), 4);
    assert_eq!(back.metric(), Metric::L2);
    assert_eq!(back.row_count(), 3);
    // Same search result after a round-trip.
    let a = idx.search(&[0.0, 1.0, 0.0, 0.0], 3);
    let b = back.search(&[0.0, 1.0, 0.0, 0.0], 3);
    assert_eq!(a, b);
}

#[test]
fn string_keys_round_trip() {
    let r = vec![
        (VectorKey::Str("a".into()), vec![1.0, 0.0]),
        (VectorKey::Str("b".into()), vec![0.0, 1.0]),
    ];
    let idx = FlatIndex::build(2, Metric::Cosine, r).unwrap();
    let back = FlatIndex::deserialize(&idx.serialize()).unwrap();
    let res = back.search(&[1.0, 0.0], 1);
    assert_eq!(res[0].0, VectorKey::Str("a".into()));
}

#[test]
fn build_rejects_dim_mismatch() {
    let r = vec![(VectorKey::Int(1), vec![1.0, 0.0, 0.0])];
    assert!(FlatIndex::build(4, Metric::Cosine, r).is_err());
}

#[test]
fn distance_fn_matches_metrics() {
    use control_plane_core::distance;
    // L2 of identical vectors is 0; cosine of identical (non-zero) is ~0.
    assert!((distance(Metric::L2, &[1.0, 2.0], &[1.0, 2.0]) - 0.0).abs() < 1e-6);
    assert!((distance(Metric::Cosine, &[1.0, 0.0], &[1.0, 0.0]) - 0.0).abs() < 1e-6);
    // Orthogonal cosine distance is 1.
    assert!((distance(Metric::Cosine, &[1.0, 0.0], &[0.0, 1.0]) - 1.0).abs() < 1e-6);
}
