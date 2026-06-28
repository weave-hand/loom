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

// --- Malformed-blob hardening (iss-hnsw-deserialize-bounds) ---------------
//
// `HnswIndex::deserialize` must reject a corrupt-but-structurally-readable blob
// with a `bad(...)` error instead of decoding it into an index that panics (or
// over-allocates) at search time. Layout reminder for the fixed-size header:
//   [0..4]   "LVIX"
//   [4] version  [5] metric  [6] kind
//   [7..11]  dim          [11..15] m         [15..19] ef_construction
//   [19..23] ef_search    [23..27] entry_point  [27..31] max_layer
//   [31..35] row_count    [35..]   data (row_count*dim f32) then adjacency...

fn read_u32(b: &[u8], off: usize) -> u32 {
    u32::from_le_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]])
}

fn write_u32(b: &mut [u8], off: usize, v: u32) {
    b[off..off + 4].copy_from_slice(&v.to_le_bytes());
}

#[test]
fn hnsw_deserialize_rejects_oversized_row_count() {
    // A header claiming far more rows than the buffer can hold must error rather
    // than drive a giant speculative `Vec::with_capacity` on the data section.
    let (rows, _) = clustered_rows();
    let mut bytes = HnswIndex::build(8, Metric::L2, rows, None, None)
        .unwrap()
        .serialize();
    write_u32(&mut bytes, 31, 100_000_000); // row_count
    assert!(HnswIndex::deserialize(&bytes).is_err());
}

#[test]
fn hnsw_deserialize_rejects_out_of_range_neighbor() {
    // Point node 0's first layer-0 neighbor at an index == row_count (out of
    // range). Decode must reject it; otherwise `row_slice`/`layers[c]` panics at
    // search time.
    let (rows, _) = clustered_rows();
    let mut bytes = HnswIndex::build(8, Metric::L2, rows, None, None)
        .unwrap()
        .serialize();
    let dim = read_u32(&bytes, 7) as usize;
    let rc = read_u32(&bytes, 31) as usize;
    let adj = 35 + rc * dim * 4; // node 0: [nml u8][layer0: cnt u32][nbr u32...]
    let cnt0 = read_u32(&bytes, adj + 1);
    assert!(
        cnt0 >= 1,
        "node 0 should have a layer-0 neighbor to corrupt"
    );
    let nbr_off = adj + 1 + 4;
    write_u32(&mut bytes, nbr_off, rc as u32); // == row_count => out of range
    assert!(HnswIndex::deserialize(&bytes).is_err());
}

#[test]
fn hnsw_deserialize_rejects_out_of_range_entry_point() {
    let (rows, _) = clustered_rows();
    let mut bytes = HnswIndex::build(8, Metric::L2, rows, None, None)
        .unwrap()
        .serialize();
    let rc = read_u32(&bytes, 31);
    write_u32(&mut bytes, 23, rc); // entry_point == row_count => out of range
    assert!(HnswIndex::deserialize(&bytes).is_err());
}

/// Build a minimal 2-node HNSW blob (dim 2, L2, int keys). `layer1_nbr` is the
/// single neighbor listed in node 0's layer 1; node 1 has only layer 0. With
/// `layer1_nbr == 0` the graph is layer-consistent (node 0 has layer 1); with
/// `layer1_nbr == 1` it references node 1's absent layer 1.
fn two_node_blob(layer1_nbr: u32) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(b"LVIX");
    b.push(1); // version
    b.push(1); // metric: L2
    b.push(2); // kind: hnsw
    b.extend_from_slice(&2u32.to_le_bytes()); // dim
    b.extend_from_slice(&16u32.to_le_bytes()); // m
    b.extend_from_slice(&200u32.to_le_bytes()); // ef_construction
    b.extend_from_slice(&16u32.to_le_bytes()); // ef_search
    b.extend_from_slice(&0u32.to_le_bytes()); // entry_point = node 0
    b.extend_from_slice(&1u32.to_le_bytes()); // max_layer = 1
    b.extend_from_slice(&2u32.to_le_bytes()); // row_count = 2
    for f in [1.0f32, 0.0, 0.0, 1.0] {
        b.extend_from_slice(&f.to_le_bytes()); // data: node0=[1,0], node1=[0,1]
    }
    // node 0: nml=1 (layers 0 and 1)
    b.push(1);
    b.extend_from_slice(&1u32.to_le_bytes()); // layer0 cnt
    b.extend_from_slice(&1u32.to_le_bytes()); // layer0 -> node 1
    b.extend_from_slice(&1u32.to_le_bytes()); // layer1 cnt
    b.extend_from_slice(&layer1_nbr.to_le_bytes()); // layer1 -> param
    // node 1: nml=0 (layer 0 only)
    b.push(0);
    b.extend_from_slice(&1u32.to_le_bytes()); // layer0 cnt
    b.extend_from_slice(&0u32.to_le_bytes()); // layer0 -> node 0
    b.push(0); // key_kind = int
    b.extend_from_slice(&1i64.to_le_bytes()); // key 0
    b.extend_from_slice(&2i64.to_le_bytes()); // key 1
    b
}

#[test]
fn two_node_blob_consistent_decodes_and_searches() {
    // Sanity-anchor the synthetic builder: a layer-consistent blob still decodes
    // and searches, so the rejection test below isolates the inconsistency.
    let back = HnswIndex::deserialize(&two_node_blob(0)).unwrap();
    assert_eq!(back.row_count(), 2);
    let res = back.search(&[1.0, 0.0], 1);
    assert_eq!(res[0].0, VectorKey::Int(1));
}

#[test]
fn hnsw_deserialize_rejects_neighbor_referencing_absent_layer() {
    // Node 0's layer 1 references node 1, which has no layer 1. Decode must
    // reject it; otherwise `layers[c][lc]` panics at search time.
    assert!(HnswIndex::deserialize(&two_node_blob(1)).is_err());
}
