//! Golden characterization of the LVIX wire format (road-vector-index-codec).
//!
//! Pins the EXACT serialized bytes of representative Flat/IVF/HNSW indexes so the
//! vector_index module split (codec extraction, per-index files) is provably
//! byte-identical. If any assertion here fails after a refactor commit, the wire
//! format moved: revert the refactor — never update a golden.

use control_plane_core::{
    FlatIndex, HnswIndex, IndexKind, IvfFlatIndex, Metric, VectorIndex, VectorKey, decode,
};

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut s, b| {
        write!(s, "{b:02x}").unwrap();
        s
    })
}

/// FNV-1a 64 — test-local, deterministic; no new deps.
fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

fn u32_at(b: &[u8], off: usize) -> u32 {
    u32::from_le_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]])
}

fn small_rows() -> Vec<(VectorKey, Vec<f32>)> {
    vec![
        (VectorKey::Int(1), vec![1.0, 0.0]),
        (VectorKey::Int(-7), vec![0.25, -1.0]),
        (VectorKey::Int(i64::MAX), vec![0.5, 0.5]),
    ]
}

fn str_rows() -> Vec<(VectorKey, Vec<f32>)> {
    vec![
        (VectorKey::Str("a".into()), vec![1.0, 0.0]),
        (VectorKey::Str("naïve".into()), vec![0.0, 1.0]), // multi-byte utf8
        (VectorKey::Str(String::new()), vec![0.5, 0.5]),  // empty string key
    ]
}

// A small deterministic generator for clustered test data (LCG — test-local;
// copied from tests/vector_index.rs, which must stay verbatim).
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

/// 10 clusters × 20 points in 8-dim — same shape as tests/vector_index.rs's
/// clustered_rows (copied, not shared: that file must stay verbatim).
fn clustered_rows() -> Vec<(VectorKey, Vec<f32>)> {
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
    rows
}

// --- golden constants (captured from the pre-refactor code) --------------------
// FlatIndex::build(2, Cosine, small_rows())
const GOLDEN_FLAT_INT_COSINE: &str = "4c56495801000002000000030000000000803f000000000000803e000080bf0000003f0000003f000100000000000000f9ffffffffffffffffffffffffffff7f";
// FlatIndex::build(2, L2, str_rows())
const GOLDEN_FLAT_STR_L2: &str = "4c56495801010002000000030000000000803f00000000000000000000803f0000003f0000003f010100000061060000006e61c3af766500000000";
// IvfFlatIndex::build(2, L2, small_rows(), Some(2))
const GOLDEN_IVF_SMALL_L2: &str = "4c564958010101020000000200000001000000030000000000403f0000803e0000803e000080bf0000000001000000000000000000803f000000000000803e000080bf0000003f0000003f000100000000000000f9ffffffffffffffffffffffffffff7f";
// HnswIndex::build(2, Cosine, small_rows(), None, None)
const GOLDEN_HNSW_SMALL_COSINE: &str = "4c5649580100020200000010000000c8000000c80000000000000000000000030000000000803f000000000000803e000080bf0000003f0000003f00020000000100000002000000000100000000000000000100000000000000000100000000000000f9ffffffffffffffffffffffffffff7f";
// HnswIndex::build(2, L2, str_rows(), Some(2), Some(8))
const GOLDEN_HNSW_STR_L2: &str = "4c564958010102020000000200000008000000080000000100000001000000030000000000803f00000000000000000000803f0000003f0000003f00020000000100000002000000010200000000000000020000000000000000020000000000000001000000010100000061060000006e61c3af766500000000";
// build(8, L2, clustered, None)
const GOLDEN_IVF_CLUSTERED_L2: (usize, u64) = (9272, 12_791_955_903_904_257_543);
// build(8, Cosine, clustered, None) — pins the kmeans++ init path for the metric
// where distances can round negative on self-comparison.
const GOLDEN_IVF_CLUSTERED_COSINE: (usize, u64) = (9272, 2_216_060_053_611_786_600);
// build(8, Cosine, clustered, None, None)
const GOLDEN_HNSW_CLUSTERED_COSINE: (usize, u64) = (13980, 7_678_422_750_193_869_183);

/// Structural spot-checks shared by every fixture: magic "LVIX", version 1,
/// the kind byte at offset 6 — catches offset drift independent of the goldens.
fn assert_header(bytes: &[u8], kind: u8) {
    assert_eq!(&bytes[0..4], b"LVIX", "magic");
    assert_eq!(bytes[4], 1, "version");
    assert_eq!(bytes[6], kind, "kind byte at offset 6");
}

/// decode() round-trip: kind routing, byte re-serialization through the boxed
/// trait, and search equivalence on one query.
fn assert_decode_roundtrip(idx: &dyn VectorIndex, bytes: &[u8], kind: IndexKind, q: &[f32]) {
    let back = decode(bytes).unwrap();
    assert_eq!(back.index_kind(), kind);
    assert_eq!(back.serialize(), bytes, "decode->serialize is byte-exact");
    assert_eq!(idx.search(q, 3), back.search(q, 3), "search equivalence");
}

#[test]
fn golden_flat_int_cosine() {
    let idx = FlatIndex::build(2, Metric::Cosine, small_rows()).unwrap();
    let bytes = idx.serialize();
    assert_eq!(hex(&bytes), GOLDEN_FLAT_INT_COSINE);
    assert_header(&bytes, 0);
    assert_decode_roundtrip(&idx, &bytes, IndexKind::Flat, &[1.0, 0.25]);
}

#[test]
fn golden_flat_str_l2() {
    let idx = FlatIndex::build(2, Metric::L2, str_rows()).unwrap();
    let bytes = idx.serialize();
    assert_eq!(hex(&bytes), GOLDEN_FLAT_STR_L2);
    assert_header(&bytes, 0);
    assert_decode_roundtrip(&idx, &bytes, IndexKind::Flat, &[0.5, 0.5]);
}

#[test]
fn golden_ivf_small_l2() {
    let idx = IvfFlatIndex::build(2, Metric::L2, small_rows(), Some(2)).unwrap();
    let bytes = idx.serialize();
    assert_eq!(hex(&bytes), GOLDEN_IVF_SMALL_L2);
    assert_header(&bytes, 1);
    assert!(u32_at(&bytes, 11) >= 2, "nlist >= 2");
    assert_decode_roundtrip(&idx, &bytes, IndexKind::IvfFlat, &[1.0, 0.25]);
}

#[test]
fn golden_hnsw_small_cosine() {
    let idx = HnswIndex::build(2, Metric::Cosine, small_rows(), None, None).unwrap();
    let bytes = idx.serialize();
    assert_eq!(hex(&bytes), GOLDEN_HNSW_SMALL_COSINE);
    assert_header(&bytes, 2);
    assert_decode_roundtrip(&idx, &bytes, IndexKind::Hnsw, &[1.0, 0.25]);
}

#[test]
fn golden_hnsw_str_l2() {
    let idx = HnswIndex::build(2, Metric::L2, str_rows(), Some(2), Some(8)).unwrap();
    let bytes = idx.serialize();
    assert_eq!(hex(&bytes), GOLDEN_HNSW_STR_L2);
    assert_header(&bytes, 2);
    assert_decode_roundtrip(&idx, &bytes, IndexKind::Hnsw, &[0.5, 0.5]);
}

#[test]
fn golden_ivf_clustered_l2() {
    let idx = IvfFlatIndex::build(8, Metric::L2, clustered_rows(), None).unwrap();
    let bytes = idx.serialize();
    assert_eq!((bytes.len(), fnv1a64(&bytes)), GOLDEN_IVF_CLUSTERED_L2);
    assert_header(&bytes, 1);
    assert!(u32_at(&bytes, 11) >= 2, "nlist >= 2");
    let q = [1.0f32, 2.0, 3.0, 4.0, 0.0, 0.0, 0.0, 0.0];
    assert_decode_roundtrip(&idx, &bytes, IndexKind::IvfFlat, &q);
}

#[test]
fn golden_ivf_clustered_cosine() {
    let idx = IvfFlatIndex::build(8, Metric::Cosine, clustered_rows(), None).unwrap();
    let bytes = idx.serialize();
    assert_eq!((bytes.len(), fnv1a64(&bytes)), GOLDEN_IVF_CLUSTERED_COSINE);
    assert_header(&bytes, 1);
    assert!(u32_at(&bytes, 11) >= 2, "nlist >= 2");
    let q = [1.0f32, 2.0, 3.0, 4.0, 0.0, 0.0, 0.0, 0.0];
    assert_decode_roundtrip(&idx, &bytes, IndexKind::IvfFlat, &q);
}

#[test]
fn golden_hnsw_clustered_cosine() {
    let idx = HnswIndex::build(8, Metric::Cosine, clustered_rows(), None, None).unwrap();
    let bytes = idx.serialize();
    assert_eq!((bytes.len(), fnv1a64(&bytes)), GOLDEN_HNSW_CLUSTERED_COSINE);
    assert_header(&bytes, 2);
    // max_layer >= 1 — the multi-layer requirement for this fixture.
    assert!(u32_at(&bytes, 27) >= 1, "max_layer >= 1 (multi-layer)");
    let q = [1.0f32, 2.0, 3.0, 4.0, 0.0, 0.0, 0.0, 0.0];
    assert_decode_roundtrip(&idx, &bytes, IndexKind::Hnsw, &q);
}
