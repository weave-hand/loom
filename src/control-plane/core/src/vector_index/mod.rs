//! A swappable vector-index abstraction and an exact (flat/brute-force) first
//! implementation. Pure: no iceberg, no arrow, no object store. The serialized
//! form is a compact self-describing binary written into a Puffin blob by the
//! postgres adapter.

use crate::error::{ControlPlaneError, Result};

mod codec;
mod flat;
mod hnsw;
mod ivf;
mod kmeans;

pub use flat::FlatIndex;
pub use hnsw::HnswIndex;
pub use ivf::IvfFlatIndex;

use codec::{KIND_FLAT, KIND_HNSW, KIND_IVF_FLAT, KIND_OFFSET, bad};

/// Which index to build, chosen at build time. `Flat` is the default (exact);
/// `IvfFlat` is the approximate IVF index with an optional `nlist` override;
/// `Hnsw` is the approximate HNSW graph index with optional `m`/`ef_construction`.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum IndexSpec {
    Flat,
    IvfFlat {
        nlist: Option<u32>,
    },
    Hnsw {
        m: Option<u32>,
        ef_construction: Option<u32>,
    },
}

impl IndexSpec {
    /// The persisted `(index_kind, nlist, m, ef_construction)` column tuple for a
    /// declaration row. Inverse of [`IndexSpec::from_label`].
    #[must_use]
    pub fn as_cols(&self) -> (&'static str, Option<u32>, Option<u32>, Option<u32>) {
        match self {
            IndexSpec::Flat => ("flat", None, None, None),
            IndexSpec::IvfFlat { nlist } => ("ivf_flat", *nlist, None, None),
            IndexSpec::Hnsw { m, ef_construction } => ("hnsw", None, *m, *ef_construction),
        }
    }

    /// Map a `(kind, nlist, m, ef_construction)` tuple (e.g. from a job payload or
    /// RPC request) to a spec. `None`/`"flat"` → `Flat`; `"ivf_flat"` → `IvfFlat`;
    /// `"hnsw"` → `Hnsw`; else error.
    pub fn from_label(
        kind: Option<&str>,
        nlist: Option<u32>,
        m: Option<u32>,
        ef_construction: Option<u32>,
    ) -> Result<IndexSpec> {
        match kind {
            None | Some("flat") => Ok(IndexSpec::Flat),
            Some("ivf_flat") => Ok(IndexSpec::IvfFlat { nlist }),
            Some("hnsw") => Ok(IndexSpec::Hnsw { m, ef_construction }),
            Some(other) => Err(ControlPlaneError::Backend(
                format!("unknown index kind '{other}'").into(),
            )),
        }
    }
}

/// Distance metric, declared at build time and recorded with the index.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub enum Metric {
    #[default]
    Cosine,
    L2,
}

impl Metric {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Metric::Cosine => "cosine",
            Metric::L2 => "l2",
        }
    }
}

impl std::str::FromStr for Metric {
    type Err = ControlPlaneError;

    /// Parse the on-the-wire label (e.g. from a query parameter or the mirror
    /// row) into a `Metric`, erroring on an unknown label.
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "cosine" => Ok(Metric::Cosine),
            "l2" => Ok(Metric::L2),
            other => Err(ControlPlaneError::Backend(
                format!("unknown metric '{other}'").into(),
            )),
        }
    }
}

/// The index algorithm family.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexKind {
    Flat,
    IvfFlat,
    Hnsw,
}

impl IndexKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            IndexKind::Flat => "flat",
            IndexKind::IvfFlat => "ivf_flat",
            IndexKind::Hnsw => "hnsw",
        }
    }
}

impl std::str::FromStr for IndexKind {
    type Err = ControlPlaneError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "flat" => Ok(IndexKind::Flat),
            "ivf_flat" => Ok(IndexKind::IvfFlat),
            "hnsw" => Ok(IndexKind::Hnsw),
            other => Err(ControlPlaneError::Backend(
                format!("unknown index kind '{other}'").into(),
            )),
        }
    }
}

/// The object identity value carried alongside each indexed vector so k-NN
/// results map back to objects. Covers the realistic identity logical types
/// (`Integer`/`Long` → `Int`, `String` → `Str`).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum VectorKey {
    Int(i64),
    Str(String),
}

/// Exact-or-approximate top-k nearest-neighbour index.
///
/// `Send + Sync` are required so that `Box<dyn VectorIndex>` and `&dyn VectorIndex`
/// can cross `await` points in multi-threaded async contexts (e.g. the engine gRPC
/// handler). Both concrete implementations (`FlatIndex`, `IvfFlatIndex`) are pure
/// data with no interior mutability, so the bounds hold trivially.
pub trait VectorIndex: Send + Sync {
    fn metric(&self) -> Metric;
    fn dim(&self) -> u32;
    /// The algorithm family — recorded in the mirror row and Puffin properties.
    fn index_kind(&self) -> IndexKind;
    /// Number of indexed vectors.
    fn row_count(&self) -> u32;
    /// The compact self-describing binary written into the Puffin blob.
    fn serialize(&self) -> Vec<u8>;
    /// Top-k by `metric`, ascending distance. Ties broken by insertion order.
    fn search(&self, query: &[f32], k: usize) -> Vec<(VectorKey, f32)>;
    /// Apply per-query tuning knobs to a decoded index before searching.
    /// `nprobe` tunes IVF-Flat probe count; `ef_search` tunes HNSW candidate width.
    /// The default is a no-op (e.g. `FlatIndex`); each implementation applies only
    /// the knob relevant to its kind and ignores the other.
    fn apply_query_knobs(&mut self, nprobe: Option<u32>, ef_search: Option<u32>) {
        let _ = (nprobe, ef_search);
    }
}

/// Deterministic SplitMix64 — fixed seed makes k-means (hence the serialized
/// blob) reproducible for a given input row order.
struct SplitMix64 {
    state: u64,
}
impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }
    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
    fn next_usize(&mut self, bound: usize) -> usize {
        // bound > 0 guaranteed by callers.
        (self.next_u64() % bound as u64) as usize
    }
}

#[expect(
    clippy::indexing_slicing,
    reason = "callers guarantee i*d + d <= data.len(); packed row access"
)]
fn row_slice(data: &[f32], d: usize, i: usize) -> &[f32] {
    &data[i * d..i * d + d]
}

#[expect(
    clippy::indexing_slicing,
    reason = "callers guarantee i*d + d <= data.len(); packed row access"
)]
fn row_slice_mut(data: &mut [f32], d: usize, i: usize) -> &mut [f32] {
    &mut data[i * d..i * d + d]
}

/// The scoring function for `metric`, ascending = nearer. Public so the engine's
/// hot-delta brute-force (Task 7) scores identically to the cold index.
#[must_use]
pub fn distance(metric: Metric, a: &[f32], b: &[f32]) -> f32 {
    match metric {
        Metric::Cosine => cosine_distance(a, b),
        Metric::L2 => l2_distance(a, b),
    }
}

fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0.0f32;
    let mut na = 0.0f32;
    let mut nb = 0.0f32;
    for (x, y) in a.iter().zip(b.iter()) {
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    let denom = na.sqrt() * nb.sqrt();
    if denom == 0.0 {
        return 1.0; // maximal distance for a zero vector
    }
    1.0 - dot / denom
}

fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
    let mut s = 0.0f32;
    for (x, y) in a.iter().zip(b.iter()) {
        let d = x - y;
        s += d * d;
    }
    s.sqrt()
}

/// Decode any serialized loom vector index into a boxed `VectorIndex`, routing on
/// the `kind` byte (offset 6: magic[4] + version + metric). Used by the engine
/// serving + postgres read paths.
pub fn decode(bytes: &[u8]) -> Result<Box<dyn VectorIndex>> {
    let kind = *bytes
        .get(KIND_OFFSET)
        .ok_or_else(|| bad("truncated index header"))?;
    match kind {
        KIND_FLAT => Ok(Box::new(FlatIndex::deserialize(bytes)?)),
        KIND_IVF_FLAT => Ok(Box::new(IvfFlatIndex::deserialize(bytes)?)),
        KIND_HNSW => Ok(Box::new(HnswIndex::deserialize(bytes)?)),
        _ => Err(bad("unknown index kind")),
    }
}
