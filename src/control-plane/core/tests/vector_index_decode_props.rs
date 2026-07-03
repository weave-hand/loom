//! Property tests for the vector-index binary codec. Two invariants at the seam
//! where loom has demonstrably had the bug class (the HNSW/Flat/IVF deserialize
//! -bounds fixes): (1) `decode(arbitrary bytes)` never panics or over-allocates —
//! it returns Ok or a graceful Err; (2) `decode(encode(x)) == x` round-trips
//! byte-exactly for every buildable index with homogeneous keys (the codec's
//! one-kind contract). See
//! docs/superpowers/specs/2026-07-02-pillar-idioms-audit-design.md.

use control_plane_core::{
    FlatIndex, HnswIndex, IvfFlatIndex, Metric, VectorIndex, VectorKey, decode,
};
use proptest::prelude::*;

fn metric() -> impl Strategy<Value = Metric> {
    prop_oneof![Just(Metric::Cosine), Just(Metric::L2)]
}

/// Finite f32 in a bounded range — avoids NaN/inf ordering hazards in kmeans/HNSW
/// build while still round-tripping byte-exactly.
fn coord() -> impl Strategy<Value = f32> {
    -1000.0f32..1000.0f32
}

/// `(dim, rows)` where every vector has length == dim (build's precondition) and
/// all identity keys within one index share a kind. Homogeneous keys are the
/// codec's documented contract (the identity column is a single logical type —
/// see `codec.rs::write_keys`); the invariant is now enforced at build
/// (`pack_rows` rejects mixed kinds with a `Validation` error) and exercised by
/// `mixed_keys_rejected_at_build` below.
fn dim_and_rows() -> impl Strategy<Value = (u32, Vec<(VectorKey, Vec<f32>)>)> {
    (1usize..=8, any::<bool>()).prop_flat_map(|(dim, str_keys)| {
        let key = if str_keys {
            ".{0,8}".prop_map(VectorKey::Str).boxed()
        } else {
            any::<i64>().prop_map(VectorKey::Int).boxed()
        };
        let row = (key, prop::collection::vec(coord(), dim..=dim));
        prop::collection::vec(row, 0..6).prop_map(move |rows| (dim as u32, rows))
    })
}

/// Generator: rows with at least one Int AND one Str key (dim fixed small).
/// Built on `dim_and_rows()`'s row shape with the payload length pinned to 1
/// (matching the tests' `dim=1`, so the homogeneity check — not the dim check —
/// is what fires): draw one guaranteed row of each key kind plus a mixed-kind
/// tail, then shuffle.
fn mixed_rows() -> impl Strategy<Value = Vec<(VectorKey, Vec<f32>)>> {
    let key = prop_oneof![
        any::<i64>().prop_map(VectorKey::Int),
        ".{0,8}".prop_map(VectorKey::Str),
    ];
    let payload = || prop::collection::vec(coord(), 1..=1);
    (
        (any::<i64>().prop_map(VectorKey::Int), payload()),
        (".{0,8}".prop_map(VectorKey::Str), payload()),
        prop::collection::vec((key, payload()), 0..6),
    )
        .prop_map(|(int_row, str_row, mut rows)| {
            rows.push(int_row);
            rows.push(str_row);
            rows
        })
        .prop_shuffle()
}

proptest! {
    /// Property 1a: decode of arbitrary bytes never panics; on the rare Ok, the
    /// decoded index re-serializes to bytes that decode again (idempotent).
    #[test]
    fn decode_arbitrary_bytes_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..2048)) {
        if let Ok(idx) = decode(&bytes) {
            let re = idx.serialize();
            prop_assert!(decode(&re).is_ok(), "re-decode of a decoded index failed");
        }
    }

    /// Property 1b: the per-type deserializers also never panic on garbage; any
    /// bytes that parse must re-serialize to something that re-parses.
    #[test]
    fn per_type_deserialize_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..2048)) {
        if let Ok(idx) = FlatIndex::deserialize(&bytes) {
            prop_assert!(FlatIndex::deserialize(&idx.serialize()).is_ok());
        }
        if let Ok(idx) = IvfFlatIndex::deserialize(&bytes) {
            prop_assert!(IvfFlatIndex::deserialize(&idx.serialize()).is_ok());
        }
        if let Ok(idx) = HnswIndex::deserialize(&bytes) {
            prop_assert!(HnswIndex::deserialize(&idx.serialize()).is_ok());
        }
    }

    /// Property 1c (over-allocation guard): a valid Flat header whose row_count is
    /// poisoned to a huge value must be rejected (Err), not over-allocate/panic.
    #[test]
    fn flat_oversized_row_count_is_rejected(
        (dim, rows) in dim_and_rows(),
        huge in 1_000_000u32..=u32::MAX,
    ) {
        let idx = FlatIndex::build(dim, Metric::Cosine, rows).expect("build");
        let mut bytes = idx.serialize();
        // Flat layout: header (7 bytes, kind at 6) | dim: u32 @7..11 | row_count: u32 @11..15.
        prop_assume!(bytes.len() >= 15);
        bytes[11..15].copy_from_slice(&huge.to_le_bytes());
        prop_assert!(decode(&bytes).is_err(), "oversized row_count was not rejected");
    }

    /// Property 2: byte-exact round-trip for FlatIndex.
    #[test]
    fn flat_round_trips((dim, rows) in dim_and_rows(), m in metric()) {
        let idx = FlatIndex::build(dim, m, rows).expect("build");
        let bytes = idx.serialize();
        let back = decode(&bytes).expect("decode of our own bytes");
        prop_assert_eq!(back.serialize(), bytes);
    }

    /// Property 2: byte-exact round-trip for IvfFlatIndex.
    #[test]
    fn ivf_round_trips((dim, rows) in dim_and_rows(), m in metric()) {
        let idx = IvfFlatIndex::build(dim, m, rows, None).expect("build");
        let bytes = idx.serialize();
        let back = decode(&bytes).expect("decode of our own bytes");
        prop_assert_eq!(back.serialize(), bytes);
    }

    /// Property 2: byte-exact round-trip for HnswIndex.
    #[test]
    fn hnsw_round_trips((dim, rows) in dim_and_rows(), m in metric()) {
        let idx = HnswIndex::build(dim, m, rows, None, None).expect("build");
        let bytes = idx.serialize();
        let back = decode(&bytes).expect("decode of our own bytes");
        prop_assert_eq!(back.serialize(), bytes);
    }

    /// Key-kind homogeneity: every build rejects a mixed-kind row set loudly.
    #[test]
    fn mixed_keys_rejected_at_build(rows in mixed_rows()) {
        // Assert the MESSAGE, not just is_err(): a mis-built generator (e.g.
        // payload len != 1) would otherwise pass vacuously via dim-mismatch.
        for e in [
            FlatIndex::build(1, Metric::Cosine, rows.clone()).unwrap_err(),
            IvfFlatIndex::build(1, Metric::Cosine, rows.clone(), None).unwrap_err(),
            HnswIndex::build(1, Metric::Cosine, rows, None, None).unwrap_err(),
        ] {
            prop_assert!(e.to_string().contains("mixed identity key kinds"), "wrong error: {e}");
        }
    }

    /// Full consumption: any non-empty suffix appended to a valid blob fails decode.
    #[test]
    fn trailing_suffix_never_decodes((dim, rows) in dim_and_rows(), m in metric(), suffix in prop::collection::vec(any::<u8>(), 1..16)) {
        let mut blob = FlatIndex::build(dim, m, rows).expect("build").serialize();
        blob.extend_from_slice(&suffix);
        prop_assert!(decode(&blob).is_err());
    }
}
