use control_plane_core::{FlatIndex, HnswIndex, IvfFlatIndex, Metric, VectorIndex, VectorKey};

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

#[test]
fn index_kind_as_str_and_parse() {
    use control_plane_core::IndexKind;
    use std::str::FromStr;
    assert_eq!(IndexKind::Flat.as_str(), "flat");
    assert_eq!(IndexKind::IvfFlat.as_str(), "ivf_flat");
    assert_eq!(IndexKind::Hnsw.as_str(), "hnsw");
    assert_eq!(IndexKind::from_str("flat").unwrap(), IndexKind::Flat);
    assert_eq!(IndexKind::from_str("ivf_flat").unwrap(), IndexKind::IvfFlat);
    assert_eq!(IndexKind::from_str("hnsw").unwrap(), IndexKind::Hnsw);
    assert!(IndexKind::from_str("nope").is_err());
}

#[test]
fn metric_as_str_and_parse() {
    use control_plane_core::Metric;
    use std::str::FromStr;
    assert_eq!(Metric::Cosine.as_str(), "cosine");
    assert_eq!(Metric::L2.as_str(), "l2");
    // FromStr round-trips the labels and errors (not None) on an unknown one.
    assert_eq!(Metric::from_str("cosine").unwrap(), Metric::Cosine);
    assert_eq!(Metric::from_str("l2").unwrap(), Metric::L2);
    assert!(Metric::from_str("nope").is_err());
}

#[test]
fn flat_index_reports_kind_and_serializes_via_trait() {
    // Exercise the new trait methods through a trait object.
    let idx = FlatIndex::build(4, Metric::L2, rows()).unwrap();
    let dynidx: &dyn VectorIndex = &idx;
    assert_eq!(dynidx.index_kind(), control_plane_core::IndexKind::Flat);
    assert_eq!(dynidx.row_count(), 3);
    // Trait serialize must equal the inherent serialize (same bytes).
    assert_eq!(dynidx.serialize(), idx.serialize());
}

// A small deterministic generator for clustered test data (LCG — test-local).
struct Lcg(u64);
impl Lcg {
    #[expect(
        clippy::unreadable_literal,
        reason = "LCG magic constants from Knuth; separators would break their canonical form"
    )]
    fn next_f32(&mut self) -> f32 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((self.0 >> 33) as f32 / (1u64 << 31) as f32) - 1.0 // in [-1, 1)
    }
}

/// 10 clusters × 20 points in 8-dim, each point = center + small jitter.
#[expect(
    clippy::type_complexity,
    reason = "test helper; extracting a type alias would add more noise than it saves"
)]
fn clustered_rows() -> (Vec<(VectorKey, Vec<f32>)>, Vec<Vec<f32>>) {
    let d = 8;
    let mut rng = Lcg(0x1234_5678);
    let mut centers = Vec::new();
    for _ in 0..10 {
        centers.push(
            std::iter::repeat_with(|| rng.next_f32() * 10.0)
                .take(d)
                .collect::<Vec<f32>>(),
        );
    }
    let mut rows = Vec::new();
    let mut id = 0i64;
    for c in &centers {
        for _ in 0..20 {
            let v: Vec<f32> = c.iter().map(|x| x + rng.next_f32() * 0.1).collect();
            rows.push((VectorKey::Int(id), v));
            id += 1;
        }
    }
    (rows, centers)
}

#[test]
fn ivf_nprobe_equals_nlist_is_exact_oracle_cosine() {
    let (rows, _) = clustered_rows();
    let flat = FlatIndex::build(8, Metric::Cosine, rows.clone()).unwrap();
    let ivf = IvfFlatIndex::build(8, Metric::Cosine, rows, None).unwrap();
    let n = ivf.dim(); // sanity
    assert_eq!(n, 8);
    // Probe every cluster -> IVF scores all rows -> identical to exact flat.
    let ivf_all = ivf.with_nprobe(u32::MAX); // clamps to nlist
    let q = vec![1.0f32, 2.0, 3.0, 4.0, 0.0, 0.0, 0.0, 0.0];
    assert_eq!(ivf_all.search(&q, 10), flat.search(&q, 10));
}

#[test]
fn ivf_nprobe_equals_nlist_is_exact_oracle_l2() {
    let (rows, _) = clustered_rows();
    let flat = FlatIndex::build(8, Metric::L2, rows.clone()).unwrap();
    let ivf = IvfFlatIndex::build(8, Metric::L2, rows, None)
        .unwrap()
        .with_nprobe(u32::MAX);
    let q = vec![0.0f32, 0.0, 0.0, 0.0, 1.0, 2.0, 3.0, 4.0];
    assert_eq!(ivf.search(&q, 10), flat.search(&q, 10));
}

#[test]
fn ivf_recall_meets_threshold_with_small_nprobe() {
    let (rows, centers) = clustered_rows();
    let flat = FlatIndex::build(8, Metric::L2, rows.clone()).unwrap();
    let ivf = IvfFlatIndex::build(8, Metric::L2, rows, None)
        .unwrap()
        .with_nprobe(3);
    // Query each cluster center; measure top-10 recall vs exact.
    let mut hits = 0usize;
    let mut total = 0usize;
    for c in &centers {
        let exact: std::collections::HashSet<_> =
            flat.search(c, 10).into_iter().map(|(k, _)| k).collect();
        let approx: std::collections::HashSet<_> =
            ivf.search(c, 10).into_iter().map(|(k, _)| k).collect();
        hits += exact.intersection(&approx).count();
        total += exact.len();
    }
    let recall = hits as f32 / total as f32;
    assert!(recall >= 0.9, "recall {recall} below 0.9");
}

#[test]
fn ivf_empty_is_searchable() {
    let ivf = IvfFlatIndex::build(4, Metric::Cosine, vec![], None).unwrap();
    assert_eq!(ivf.row_count(), 0);
    assert_eq!(ivf.search(&[1.0, 0.0, 0.0, 0.0], 5), vec![]);
}

#[test]
fn ivf_fewer_rows_than_nlist_clamps() {
    let rows = vec![
        (VectorKey::Int(1), vec![1.0, 0.0]),
        (VectorKey::Int(2), vec![0.0, 1.0]),
    ];
    // Request 16 clusters but only 2 rows: must clamp, build, and search exactly.
    let ivf = IvfFlatIndex::build(2, Metric::L2, rows, Some(16))
        .unwrap()
        .with_nprobe(u32::MAX);
    let res = ivf.search(&[1.0, 0.0], 1);
    assert_eq!(res[0].0, VectorKey::Int(1));
}

#[test]
fn ivf_build_rejects_dim_mismatch() {
    let rows = vec![(VectorKey::Int(1), vec![1.0, 0.0, 0.0])];
    assert!(IvfFlatIndex::build(4, Metric::Cosine, rows, None).is_err());
}

#[test]
fn ivf_serialize_roundtrip_is_search_exact() {
    let (rows, _) = clustered_rows();
    let ivf = IvfFlatIndex::build(8, Metric::Cosine, rows, None)
        .unwrap()
        .with_nprobe(3);
    let bytes = ivf.serialize();
    let back = IvfFlatIndex::deserialize(&bytes).unwrap();
    let q = vec![1.0f32, 2.0, 3.0, 4.0, 0.0, 0.0, 0.0, 0.0];
    assert_eq!(ivf.search(&q, 10), back.search(&q, 10));
}

#[test]
fn ivf_build_is_byte_deterministic() {
    let (rows, _) = clustered_rows();
    let a = IvfFlatIndex::build(8, Metric::L2, rows.clone(), None).unwrap();
    let b = IvfFlatIndex::build(8, Metric::L2, rows, None).unwrap();
    assert_eq!(
        a.serialize(),
        b.serialize(),
        "same input order -> identical bytes"
    );
}

#[test]
fn decode_routes_on_kind_byte() {
    use control_plane_core::{IndexKind, decode};
    let (rows, _) = clustered_rows();
    let flat = FlatIndex::build(8, Metric::L2, rows.clone()).unwrap();
    let ivf = IvfFlatIndex::build(8, Metric::L2, rows, None).unwrap();

    let dflat = decode(&flat.serialize()).unwrap();
    assert_eq!(dflat.index_kind(), IndexKind::Flat);
    let divf = decode(&ivf.serialize()).unwrap();
    assert_eq!(divf.index_kind(), IndexKind::IvfFlat);

    // Boxed trait object searches; the default nprobe recorded in the blob is used.
    let q = vec![0.0f32, 0.0, 0.0, 0.0, 1.0, 2.0, 3.0, 4.0];
    assert_eq!(divf.search(&q, 5).len(), 5);

    // Truncated header: fewer than 7 bytes -> bytes.get(6) is None -> error.
    assert!(control_plane_core::decode(&[0u8; 6]).is_err());
    // Unknown kind byte: magic ok, version 1, metric 0, kind byte = 3 -> error.
    assert!(control_plane_core::decode(&[b'L', b'V', b'I', b'X', 1, 0, 3]).is_err());
}

#[test]
fn hnsw_recall_meets_threshold_cosine() {
    let (rows, centers) = clustered_rows();
    let flat = FlatIndex::build(8, Metric::Cosine, rows.clone()).unwrap();
    let hnsw = HnswIndex::build(8, Metric::Cosine, rows, None, None).unwrap();
    let mut hits = 0usize;
    let mut total = 0usize;
    for c in &centers {
        let exact: std::collections::HashSet<_> =
            flat.search(c, 10).into_iter().map(|(k, _)| k).collect();
        let approx: std::collections::HashSet<_> =
            hnsw.search(c, 10).into_iter().map(|(k, _)| k).collect();
        hits += exact.intersection(&approx).count();
        total += exact.len();
    }
    let recall = hits as f32 / total as f32;
    assert!(recall >= 0.9, "cosine recall {recall} below 0.9");
}

#[test]
fn hnsw_recall_meets_threshold_l2() {
    let (rows, centers) = clustered_rows();
    let flat = FlatIndex::build(8, Metric::L2, rows.clone()).unwrap();
    let hnsw = HnswIndex::build(8, Metric::L2, rows, None, None).unwrap();
    let mut hits = 0usize;
    let mut total = 0usize;
    for c in &centers {
        let exact: std::collections::HashSet<_> =
            flat.search(c, 10).into_iter().map(|(k, _)| k).collect();
        let approx: std::collections::HashSet<_> =
            hnsw.search(c, 10).into_iter().map(|(k, _)| k).collect();
        hits += exact.intersection(&approx).count();
        total += exact.len();
    }
    let recall = hits as f32 / total as f32;
    assert!(recall >= 0.9, "l2 recall {recall} below 0.9");
}

#[test]
fn hnsw_build_is_byte_deterministic() {
    let (rows, _) = clustered_rows();
    let a = HnswIndex::build(8, Metric::L2, rows.clone(), None, None).unwrap();
    let b = HnswIndex::build(8, Metric::L2, rows, None, None).unwrap();
    assert_eq!(
        a.serialize(),
        b.serialize(),
        "same input order -> identical bytes"
    );
}

#[test]
fn hnsw_serialize_roundtrip_is_search_exact() {
    let (rows, _) = clustered_rows();
    let hnsw = HnswIndex::build(8, Metric::Cosine, rows, None, None).unwrap();
    let bytes = hnsw.serialize();
    let back = HnswIndex::deserialize(&bytes).unwrap();
    assert_eq!(bytes, back.serialize(), "round-trip is byte-exact");
    let q = vec![1.0f32, 2.0, 3.0, 4.0, 0.0, 0.0, 0.0, 0.0];
    assert_eq!(hnsw.search(&q, 10), back.search(&q, 10));
}

#[test]
fn hnsw_string_keys_round_trip() {
    let r = vec![
        (VectorKey::Str("a".into()), vec![1.0, 0.0]),
        (VectorKey::Str("b".into()), vec![0.0, 1.0]),
    ];
    let hnsw = HnswIndex::build(2, Metric::Cosine, r, None, None).unwrap();
    let back = HnswIndex::deserialize(&hnsw.serialize()).unwrap();
    let res = back.search(&[1.0, 0.0], 1);
    assert_eq!(res[0].0, VectorKey::Str("a".into()));
}

#[test]
fn hnsw_empty_is_searchable() {
    let hnsw = HnswIndex::build(4, Metric::Cosine, vec![], None, None).unwrap();
    assert_eq!(hnsw.row_count(), 0);
    assert_eq!(hnsw.search(&[1.0, 0.0, 0.0, 0.0], 5), vec![]);
    // Empty round-trips too.
    let back = HnswIndex::deserialize(&hnsw.serialize()).unwrap();
    assert_eq!(back.row_count(), 0);
}

#[test]
fn hnsw_single_row_returns_it() {
    let r = vec![(VectorKey::Int(7), vec![1.0, 0.0, 0.0, 0.0])];
    let hnsw = HnswIndex::build(4, Metric::L2, r, None, None).unwrap();
    let res = hnsw.search(&[1.0, 0.0, 0.0, 0.0], 3);
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].0, VectorKey::Int(7));
}

#[test]
fn hnsw_fewer_rows_than_m_builds_and_searches() {
    // N < M (default 16): every node simply links to all reachable neighbors.
    let rows = vec![
        (VectorKey::Int(1), vec![1.0, 0.0]),
        (VectorKey::Int(2), vec![0.0, 1.0]),
        (VectorKey::Int(3), vec![0.9, 0.1]),
    ];
    let hnsw = HnswIndex::build(2, Metric::L2, rows, None, None).unwrap();
    let res = hnsw.search(&[1.0, 0.0], 1);
    assert_eq!(res[0].0, VectorKey::Int(1));
}

#[test]
fn hnsw_build_rejects_dim_mismatch() {
    let rows = vec![(VectorKey::Int(1), vec![1.0, 0.0, 0.0])];
    assert!(HnswIndex::build(4, Metric::Cosine, rows, None, None).is_err());
}

#[test]
fn hnsw_reports_kind_via_trait() {
    let (rows, _) = clustered_rows();
    let hnsw = HnswIndex::build(8, Metric::L2, rows, None, None).unwrap();
    let dynidx: &dyn VectorIndex = &hnsw;
    assert_eq!(dynidx.index_kind(), control_plane_core::IndexKind::Hnsw);
    assert_eq!(dynidx.serialize(), hnsw.serialize());
}

#[test]
fn hnsw_with_ef_search_clamps_and_searches() {
    let (rows, _) = clustered_rows();
    let hnsw = HnswIndex::build(8, Metric::L2, rows, None, None)
        .unwrap()
        .with_ef_search(0); // clamps to >= 1
    let q = vec![0.0f32, 0.0, 0.0, 0.0, 1.0, 2.0, 3.0, 4.0];
    assert_eq!(hnsw.search(&q, 5).len(), 5);
}

#[test]
fn decode_routes_hnsw_kind_byte() {
    use control_plane_core::{IndexKind, decode};
    let (rows, _) = clustered_rows();
    let hnsw = HnswIndex::build(8, Metric::L2, rows, None, None).unwrap();
    let boxed = decode(&hnsw.serialize()).unwrap();
    assert_eq!(boxed.index_kind(), IndexKind::Hnsw);
    let q = vec![0.0f32, 0.0, 0.0, 0.0, 1.0, 2.0, 3.0, 4.0];
    assert_eq!(boxed.search(&q, 5).len(), 5);
}
