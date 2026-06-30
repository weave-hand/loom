//! A swappable vector-index abstraction and an exact (flat/brute-force) first
//! implementation. Pure: no iceberg, no arrow, no object store. The serialized
//! form is a compact self-describing binary written into a Puffin blob by the
//! postgres adapter.

use crate::error::{ControlPlaneError, Result};

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

/// Exact brute-force index: packed `f32` rows + a parallel identity column.
#[derive(Clone, Debug)]
pub struct FlatIndex {
    dim: u32,
    metric: Metric,
    keys: Vec<VectorKey>,
    /// Row-major packed vectors; `data[i*dim .. (i+1)*dim]` is row `i`.
    data: Vec<f32>,
}

impl FlatIndex {
    /// Build from `(identity, vector)` rows. Errors if any vector length != `dim`.
    pub fn build(dim: u32, metric: Metric, rows: Vec<(VectorKey, Vec<f32>)>) -> Result<FlatIndex> {
        let d = dim as usize;
        let mut keys = Vec::with_capacity(rows.len());
        let mut data = Vec::with_capacity(rows.len() * d);
        for (key, v) in rows {
            if v.len() != d {
                return Err(ControlPlaneError::Backend(
                    format!("vector dim mismatch: expected {d}, got {}", v.len()).into(),
                ));
            }
            keys.push(key);
            data.extend_from_slice(&v);
        }
        Ok(FlatIndex {
            dim,
            metric,
            keys,
            data,
        })
    }

    #[must_use]
    #[expect(
        clippy::same_name_method,
        reason = "inherent kept for callers that do not go through the VectorIndex trait object"
    )]
    pub fn row_count(&self) -> u32 {
        self.keys.len() as u32
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "i < keys.len(); data is keys.len()*dim"
    )]
    fn row(&self, i: usize) -> &[f32] {
        let d = self.dim as usize;
        &self.data[i * d..(i + 1) * d]
    }

    // --- compact binary format -------------------------------------------------
    // magic "LVIX" | u8 version=1 | u8 metric | u8 kind=flat | u32 dim |
    // u32 row_count | for each row: vector (dim * f32 LE) ;
    // then identity block: u8 key_kind (0=int,1=str) |
    //   if int: row_count * i64 LE ; if str: row_count * (u32 len LE + utf8 bytes)
    //
    // All vectors share one key_kind (the identity column's logical type).

    #[must_use]
    #[expect(
        clippy::same_name_method,
        reason = "inherent kept for callers that do not go through the VectorIndex trait object"
    )]
    pub fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(b"LVIX");
        out.push(1); // version
        out.push(match self.metric {
            Metric::Cosine => 0,
            Metric::L2 => 1,
        });
        out.push(0); // kind: flat
        out.extend_from_slice(&self.dim.to_le_bytes());
        out.extend_from_slice(&self.row_count().to_le_bytes());
        for &f in &self.data {
            out.extend_from_slice(&f.to_le_bytes());
        }
        let key_kind: u8 = match self.keys.first() {
            Some(VectorKey::Str(_)) => 1,
            _ => 0, // empty or Int
        };
        out.push(key_kind);
        for key in &self.keys {
            match key {
                VectorKey::Int(i) => out.extend_from_slice(&i.to_le_bytes()),
                VectorKey::Str(s) => {
                    out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    out.extend_from_slice(s.as_bytes());
                }
            }
        }
        out
    }

    pub fn deserialize(bytes: &[u8]) -> Result<FlatIndex> {
        let mut c = Cursor { b: bytes, p: 0 };
        let magic = c.take(4)?;
        if magic != b"LVIX" {
            return Err(bad("bad magic"));
        }
        if c.u8()? != 1 {
            return Err(bad("unsupported version"));
        }
        let metric = match c.u8()? {
            0 => Metric::Cosine,
            1 => Metric::L2,
            _ => return Err(bad("bad metric")),
        };
        if c.u8()? != 0 {
            return Err(bad("bad index kind"));
        }
        let dim = c.u32()?;
        let row_count = c.u32()?;
        let d = dim as usize;
        // Bound the data section against the buffer before allocating, so a
        // corrupt header cannot drive a huge speculative `Vec::with_capacity`.
        let data_len = (row_count as usize)
            .checked_mul(d)
            .ok_or_else(|| bad("row_count * dim overflow"))?;
        if data_len > c.remaining() {
            return Err(bad("data section exceeds buffer"));
        }
        let mut data = Vec::with_capacity(data_len);
        for _ in 0..data_len {
            data.push(c.f32()?);
        }
        let key_kind = c.u8()?;
        let mut keys = Vec::with_capacity((row_count as usize).min(c.remaining()));
        for _ in 0..row_count {
            match key_kind {
                0 => keys.push(VectorKey::Int(c.i64()?)),
                1 => {
                    let len = c.u32()? as usize;
                    let raw = c.take(len)?;
                    let s = std::str::from_utf8(raw).map_err(|e| bad(&e.to_string()))?;
                    keys.push(VectorKey::Str(s.to_string()));
                }
                _ => return Err(bad("bad key kind")),
            }
        }
        Ok(FlatIndex {
            dim,
            metric,
            keys,
            data,
        })
    }
}

fn bad(m: &str) -> ControlPlaneError {
    ControlPlaneError::Backend(format!("vector index decode: {m}").into())
}

// =============================================================================
// IvfFlatIndex — approximate IVF-Flat index with deterministic k-means build
// =============================================================================

const KMEANS_SEED: u64 = 0x6C6F_6F6D_7665_6331; // "loomvec1"
const KMEANS_MAX_ITERS: usize = 20;

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

fn default_nlist(n: usize) -> u32 {
    ((n as f64).sqrt().round() as u32).max(1)
}

fn default_nprobe(nlist: u32) -> u32 {
    ((f64::from(nlist)).sqrt().round() as u32).max(1)
}

/// Approximate IVF-Flat index: k-means centroids + per-row cluster assignment.
/// Cold-tier index; the engine merges its results with the exact inline hot delta.
#[derive(Clone, Debug)]
pub struct IvfFlatIndex {
    dim: u32,
    metric: Metric,
    nlist: u32,
    nprobe: u32,
    centroids: Vec<f32>,   // nlist * dim
    assignments: Vec<u32>, // row_count; cluster id per row
    keys: Vec<VectorKey>,  // row_count
    data: Vec<f32>,        // row_count * dim
}

impl IvfFlatIndex {
    /// Build from `(identity, vector)` rows. `nlist` defaults to ~sqrt(N), clamped
    /// to `[1, N]`. Errors on any vector length != `dim`.
    pub fn build(
        dim: u32,
        metric: Metric,
        rows: Vec<(VectorKey, Vec<f32>)>,
        nlist: Option<u32>,
    ) -> Result<IvfFlatIndex> {
        let d = dim as usize;
        let n = rows.len();
        let mut keys = Vec::with_capacity(n);
        let mut data = Vec::with_capacity(n * d);
        for (key, v) in rows {
            if v.len() != d {
                return Err(ControlPlaneError::Backend(
                    format!("vector dim mismatch: expected {d}, got {}", v.len()).into(),
                ));
            }
            keys.push(key);
            data.extend_from_slice(&v);
        }

        if n == 0 {
            return Ok(IvfFlatIndex {
                dim,
                metric,
                nlist: 0,
                nprobe: 0,
                centroids: Vec::new(),
                assignments: Vec::new(),
                keys,
                data,
            });
        }

        let nlist = nlist.unwrap_or_else(|| default_nlist(n)).clamp(1, n as u32);
        let mut rng = SplitMix64::new(KMEANS_SEED);
        let (centroids, assignments) = kmeans(&data, d, n, nlist as usize, metric, &mut rng);
        let nprobe = default_nprobe(nlist);

        Ok(IvfFlatIndex {
            dim,
            metric,
            nlist,
            nprobe,
            centroids,
            assignments,
            keys,
            data,
        })
    }

    /// Override the query-time probe count in place (clamped to `[1, nlist]`). No-op when empty.
    pub fn set_nprobe(&mut self, nprobe: u32) {
        if self.nlist > 0 {
            self.nprobe = nprobe.clamp(1, self.nlist);
        }
    }

    /// Override the query-time probe count (clamped to `[1, nlist]`). No-op when empty.
    #[must_use]
    pub fn with_nprobe(mut self, nprobe: u32) -> IvfFlatIndex {
        self.set_nprobe(nprobe);
        self
    }

    fn centroid(&self, c: usize) -> &[f32] {
        row_slice(&self.centroids, self.dim as usize, c)
    }
    fn row(&self, i: usize) -> &[f32] {
        row_slice(&self.data, self.dim as usize, i)
    }

    // --- compact binary format -------------------------------------------------
    // magic "LVIX" | u8 version=1 | u8 metric | u8 kind=1 |
    // u32 dim | u32 nlist | u32 nprobe | u32 row_count |
    // centroids (nlist*dim f32 LE) | assignments (row_count u32 LE) |
    // data (row_count*dim f32 LE) | u8 key_kind | keys (as FlatIndex)
    fn serialize_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(b"LVIX");
        out.push(1); // version
        out.push(match self.metric {
            Metric::Cosine => 0,
            Metric::L2 => 1,
        });
        out.push(1); // kind: ivf_flat
        out.extend_from_slice(&self.dim.to_le_bytes());
        out.extend_from_slice(&self.nlist.to_le_bytes());
        out.extend_from_slice(&self.nprobe.to_le_bytes());
        out.extend_from_slice(&self.row_count().to_le_bytes());
        for &f in &self.centroids {
            out.extend_from_slice(&f.to_le_bytes());
        }
        for &a in &self.assignments {
            out.extend_from_slice(&a.to_le_bytes());
        }
        for &f in &self.data {
            out.extend_from_slice(&f.to_le_bytes());
        }
        let key_kind: u8 = match self.keys.first() {
            Some(VectorKey::Str(_)) => 1,
            _ => 0,
        };
        out.push(key_kind);
        for key in &self.keys {
            match key {
                VectorKey::Int(i) => out.extend_from_slice(&i.to_le_bytes()),
                VectorKey::Str(s) => {
                    out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    out.extend_from_slice(s.as_bytes());
                }
            }
        }
        out
    }

    pub fn deserialize(bytes: &[u8]) -> Result<IvfFlatIndex> {
        let mut c = Cursor { b: bytes, p: 0 };
        if c.take(4)? != b"LVIX" {
            return Err(bad("bad magic"));
        }
        if c.u8()? != 1 {
            return Err(bad("unsupported version"));
        }
        let metric = match c.u8()? {
            0 => Metric::Cosine,
            1 => Metric::L2,
            _ => return Err(bad("bad metric")),
        };
        if c.u8()? != 1 {
            return Err(bad("not an ivf_flat index"));
        }
        let dim = c.u32()?;
        let nlist = c.u32()?;
        let nprobe = c.u32()?;
        let row_count = c.u32()?;
        let d = dim as usize;
        // Bound each header product against the buffer before allocating, so a
        // corrupt header cannot drive a huge speculative `Vec::with_capacity`.
        let cent_len = (nlist as usize)
            .checked_mul(d)
            .ok_or_else(|| bad("nlist * dim overflow"))?;
        if cent_len > c.remaining() {
            return Err(bad("centroid section exceeds buffer"));
        }
        let mut centroids = Vec::with_capacity(cent_len);
        for _ in 0..cent_len {
            centroids.push(c.f32()?);
        }
        let mut assignments = Vec::with_capacity((row_count as usize).min(c.remaining()));
        for _ in 0..row_count {
            assignments.push(c.u32()?);
        }
        let data_len = (row_count as usize)
            .checked_mul(d)
            .ok_or_else(|| bad("row_count * dim overflow"))?;
        if data_len > c.remaining() {
            return Err(bad("data section exceeds buffer"));
        }
        let mut data = Vec::with_capacity(data_len);
        for _ in 0..data_len {
            data.push(c.f32()?);
        }
        let key_kind = c.u8()?;
        let mut keys = Vec::with_capacity((row_count as usize).min(c.remaining()));
        for _ in 0..row_count {
            match key_kind {
                0 => keys.push(VectorKey::Int(c.i64()?)),
                1 => {
                    let len = c.u32()? as usize;
                    let raw = c.take(len)?;
                    let s = std::str::from_utf8(raw).map_err(|e| bad(&e.to_string()))?;
                    keys.push(VectorKey::Str(s.to_string()));
                }
                _ => return Err(bad("bad key kind")),
            }
        }
        Ok(IvfFlatIndex {
            dim,
            metric,
            nlist,
            nprobe,
            centroids,
            assignments,
            keys,
            data,
        })
    }
}

/// Deterministic k-means: k-means++ seeded init (metric-consistent) + Lloyd
/// iterations. Returns `(centroids [k*d], assignments [n])`. Requires
/// `1 <= k <= n` and `n >= 1`.
#[expect(
    clippy::needless_range_loop,
    reason = "stride loops over parallel packed arrays (data/centroids/sums/counts) \
              are clearer indexed than zipped"
)]
#[expect(
    clippy::indexing_slicing,
    reason = "loop bounds (i<n, c<k) guarantee every index is valid"
)]
fn kmeans(
    data: &[f32],
    d: usize,
    n: usize,
    k: usize,
    metric: Metric,
    rng: &mut SplitMix64,
) -> (Vec<f32>, Vec<u32>) {
    let mut centroids = vec![0.0f32; k * d];
    // --- k-means++ init ---
    let first = rng.next_usize(n);
    row_slice_mut(&mut centroids, d, 0).copy_from_slice(row_slice(data, d, first));
    let mut dist2 = vec![f32::INFINITY; n];
    for c in 1..k {
        let prev = row_slice(&centroids, d, c - 1);
        for i in 0..n {
            let dd = distance(metric, row_slice(data, d, i), prev);
            let dd2 = dd * dd;
            if dd2 < dist2[i] {
                dist2[i] = dd2;
            }
        }
        let sum: f64 = dist2.iter().map(|&x| f64::from(x)).sum();
        let mut target = rng.next_f64() * sum;
        let mut chosen = n - 1;
        for i in 0..n {
            target -= f64::from(dist2[i]);
            if target <= 0.0 {
                chosen = i;
                break;
            }
        }
        row_slice_mut(&mut centroids, d, c).copy_from_slice(row_slice(data, d, chosen));
    }

    // --- Lloyd iterations ---
    let mut assignments = vec![0u32; n];
    for iter in 0..KMEANS_MAX_ITERS {
        let mut changed = false;
        for i in 0..n {
            let p = row_slice(data, d, i);
            let mut best = 0u32;
            let mut bestd = f32::INFINITY;
            for c in 0..k {
                let dd = distance(metric, p, row_slice(&centroids, d, c));
                if dd < bestd {
                    bestd = dd;
                    best = c as u32;
                }
            }
            if assignments[i] != best {
                changed = true;
                assignments[i] = best;
            }
        }
        if !changed && iter > 0 {
            break;
        }
        // Recompute centroids as the mean of assigned points; empty clusters keep
        // their previous centroid.
        let mut sums = vec![0.0f32; k * d];
        let mut counts = vec![0u32; k];
        for i in 0..n {
            let c = assignments[i] as usize;
            counts[c] += 1;
            let p = row_slice(data, d, i);
            let dst = row_slice_mut(&mut sums, d, c);
            for (s, &x) in dst.iter_mut().zip(p.iter()) {
                *s += x;
            }
        }
        for c in 0..k {
            if counts[c] > 0 {
                let cnt = counts[c] as f32;
                let src_sum = row_slice(&sums, d, c).to_vec();
                let dst = row_slice_mut(&mut centroids, d, c);
                for (cv, s) in dst.iter_mut().zip(src_sum.iter()) {
                    *cv = s / cnt;
                }
            }
        }
    }
    (centroids, assignments)
}

impl VectorIndex for IvfFlatIndex {
    fn metric(&self) -> Metric {
        self.metric
    }
    fn dim(&self) -> u32 {
        self.dim
    }
    fn index_kind(&self) -> IndexKind {
        IndexKind::IvfFlat
    }
    fn row_count(&self) -> u32 {
        self.keys.len() as u32
    }
    fn serialize(&self) -> Vec<u8> {
        // Implemented in Task 3.
        self.serialize_bytes()
    }
    fn apply_query_knobs(&mut self, nprobe: Option<u32>, _ef_search: Option<u32>) {
        if let Some(n) = nprobe {
            self.set_nprobe(n);
        }
    }
    fn search(&self, query: &[f32], k: usize) -> Vec<(VectorKey, f32)> {
        if self.keys.is_empty() || k == 0 {
            return Vec::new();
        }
        let nlist = self.nlist as usize;
        // 1. Score the query against every centroid; take the nprobe nearest.
        let mut cdist: Vec<(usize, f32)> = (0..nlist)
            .map(|c| (c, distance(self.metric, query, self.centroid(c))))
            .collect();
        cdist.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        let probe: std::collections::HashSet<u32> = cdist
            .into_iter()
            .take(self.nprobe as usize)
            .map(|(c, _)| c as u32)
            .collect();
        // 2. Brute-force rows in the probed clusters. Tie-break by original index
        //    so that nprobe>=nlist reproduces FlatIndex's ordering exactly.
        let mut scored: Vec<(usize, f32)> = (0..self.keys.len())
            .filter(|&i| self.assignments.get(i).is_some_and(|c| probe.contains(c)))
            .map(|i| (i, distance(self.metric, query, self.row(i))))
            .collect();
        scored.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.0.cmp(&b.0))
        });
        scored
            .into_iter()
            .take(k)
            .filter_map(|(i, dd)| self.keys.get(i).map(|key| (key.clone(), dd)))
            .collect()
    }
}

struct Cursor<'a> {
    b: &'a [u8],
    p: usize,
}
impl<'a> Cursor<'a> {
    /// Bytes left to read. An untrusted element count exceeding this cannot be
    /// satisfied (every element occupies at least one byte), so it bounds
    /// speculative `Vec::with_capacity` against a corrupt header.
    fn remaining(&self) -> usize {
        self.b.len().saturating_sub(self.p)
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.p.checked_add(n).ok_or_else(|| bad("overflow"))?;
        let s = self.b.get(self.p..end).ok_or_else(|| bad("truncated"))?;
        self.p = end;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(*self.take(1)?.first().ok_or_else(|| bad("truncated"))?)
    }
    fn u32(&mut self) -> Result<u32> {
        let s = self.take(4)?;
        let arr: [u8; 4] = s.try_into().map_err(|e| bad(&format!("u32: {e}")))?;
        Ok(u32::from_le_bytes(arr))
    }
    fn i64(&mut self) -> Result<i64> {
        let s = self.take(8)?;
        let arr: [u8; 8] = s.try_into().map_err(|e| bad(&format!("i64: {e}")))?;
        Ok(i64::from_le_bytes(arr))
    }
    fn f32(&mut self) -> Result<f32> {
        let s = self.take(4)?;
        let arr: [u8; 4] = s.try_into().map_err(|e| bad(&format!("f32: {e}")))?;
        Ok(f32::from_le_bytes(arr))
    }
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

// =============================================================================
// HnswIndex — approximate HNSW graph index with deterministic build
// =============================================================================

const HNSW_SEED: u64 = 0x6C6F_6F6D_7665_6331; // "loomvec1"
const HNSW_DEFAULT_M: u32 = 16;
const HNSW_DEFAULT_EF_CONSTRUCTION: u32 = 200;
/// Cap node levels so `node_max_layer` always fits the serialized `u8` and the
/// graph height stays bounded even for a pathologically small level-draw `u`.
/// Far above any realistic height (~log_M(N)).
const HNSW_MAX_LEVEL: usize = 64;

/// Total order over `(distance, node_index)`: ascending distance, ties broken by
/// node index (deterministic, matching the other impls' insertion-order tie-break).
fn cmp_dist(a: (f32, u32), b: (f32, u32)) -> std::cmp::Ordering {
    a.0.partial_cmp(&b.0)
        .unwrap_or(std::cmp::Ordering::Equal)
        .then(a.1.cmp(&b.1))
}

/// Best-first beam search within a single graph layer. Returns up to `ef` nearest
/// nodes to `query`, ascending by `cmp_dist`. Visited-set guarded; no recursion.
#[expect(
    clippy::indexing_slicing,
    reason = "node indices come from the graph's own adjacency and layer counts; \
              build/search invariants guarantee `c < n` and `lc <= node_level[c]`"
)]
#[expect(
    clippy::too_many_arguments,
    reason = "beam-search signature mirrors HNSW paper; all args are needed; \
              wrapping them in a struct buys nothing in a private helper"
)]
fn hnsw_search_layer(
    query: &[f32],
    entry: &[u32],
    data: &[f32],
    d: usize,
    metric: Metric,
    layers: &[Vec<Vec<u32>>],
    lc: usize,
    ef: usize,
) -> Vec<(f32, u32)> {
    let mut visited: std::collections::HashSet<u32> = std::collections::HashSet::new();
    let mut frontier: Vec<(f32, u32)> = Vec::new();
    let mut results: Vec<(f32, u32)> = Vec::new();
    for &e in entry {
        if visited.insert(e) {
            let de = distance(metric, query, row_slice(data, d, e as usize));
            frontier.push((de, e));
            results.push((de, e));
        }
    }
    while !frontier.is_empty() {
        // Pop the nearest frontier element (linear min-scan; `ef` is bounded).
        let mut best = 0usize;
        for j in 1..frontier.len() {
            if cmp_dist(frontier[j], frontier[best]) == std::cmp::Ordering::Less {
                best = j;
            }
        }
        let (cd, c) = frontier.swap_remove(best);
        let worst = results
            .iter()
            .map(|&(dd, _)| dd)
            .fold(f32::NEG_INFINITY, f32::max);
        if results.len() >= ef && cd > worst {
            break;
        }
        for &nbr in &layers[c as usize][lc] {
            if visited.insert(nbr) {
                let dn = distance(metric, query, row_slice(data, d, nbr as usize));
                let worst = results
                    .iter()
                    .map(|&(dd, _)| dd)
                    .fold(f32::NEG_INFINITY, f32::max);
                if results.len() < ef || dn < worst {
                    frontier.push((dn, nbr));
                    results.push((dn, nbr));
                    if results.len() > ef {
                        // Evict the current worst result.
                        let mut w = 0usize;
                        for j in 1..results.len() {
                            if cmp_dist(results[j], results[w]) == std::cmp::Ordering::Greater {
                                w = j;
                            }
                        }
                        results.swap_remove(w);
                    }
                }
            }
        }
    }
    results.sort_by(|a, b| cmp_dist(*a, *b));
    results
}

/// Diversity heuristic (Malkov & Yashunin Algorithm 4): from `candidates`
/// (ascending by distance `dq` to the base row the neighbor list belongs to),
/// keep a candidate only if it is closer to the base than to every
/// already-selected neighbor. The base distance is the precomputed `dq` in each
/// `(dq, node)` candidate, so the base vector itself is not needed here.
fn hnsw_select_neighbors(
    candidates: &[(f32, u32)],
    mmax: usize,
    data: &[f32],
    d: usize,
    metric: Metric,
) -> Vec<u32> {
    let mut selected: Vec<u32> = Vec::new();
    for &(dq, c) in candidates {
        if selected.len() >= mmax {
            break;
        }
        let crow = row_slice(data, d, c as usize);
        let mut keep = true;
        for &s in &selected {
            if distance(metric, crow, row_slice(data, d, s as usize)) < dq {
                keep = false;
                break;
            }
        }
        if keep {
            selected.push(c);
        }
    }
    selected
}

/// Approximate HNSW graph index: hierarchical navigable small world graph.
/// Deterministic build (fixed seed) → byte-identical serialization for a given
/// input row order.
#[derive(Clone, Debug)]
pub struct HnswIndex {
    dim: u32,
    metric: Metric,
    m: u32,
    ef_construction: u32,
    ef_search: u32,
    entry_point: u32,
    max_layer: u32,
    keys: Vec<VectorKey>,
    data: Vec<f32>,
    /// `layers[node][layer]` = neighbor list for `node` at `layer`.
    layers: Vec<Vec<Vec<u32>>>,
}

impl HnswIndex {
    /// Build from `(identity, vector)` rows. `m` defaults to 16, `ef_construction`
    /// to 200. Errors on any vector length != `dim`. Single-threaded + fixed seed
    /// ⇒ byte-identical serialization for a given input row order.
    #[expect(
        clippy::indexing_slicing,
        reason = "insertion-order node indices `i` and per-node layer vectors are \
                  in-range by construction (each node's `layers[i]` is sized to its level)"
    )]
    pub fn build(
        dim: u32,
        metric: Metric,
        rows: Vec<(VectorKey, Vec<f32>)>,
        m: Option<u32>,
        ef_construction: Option<u32>,
    ) -> Result<HnswIndex> {
        let d = dim as usize;
        let mut keys = Vec::with_capacity(rows.len());
        let mut data = Vec::with_capacity(rows.len() * d);
        for (key, v) in rows {
            if v.len() != d {
                return Err(ControlPlaneError::Backend(
                    format!("vector dim mismatch: expected {d}, got {}", v.len()).into(),
                ));
            }
            keys.push(key);
            data.extend_from_slice(&v);
        }
        let n = keys.len();
        let m = m.unwrap_or(HNSW_DEFAULT_M).max(1);
        let ef_construction = ef_construction
            .unwrap_or(HNSW_DEFAULT_EF_CONSTRUCTION)
            .max(1);
        let ef_search = ef_construction;

        if n == 0 {
            return Ok(HnswIndex {
                dim,
                metric,
                m,
                ef_construction,
                ef_search,
                entry_point: 0,
                max_layer: 0,
                keys,
                data,
                layers: Vec::new(),
            });
        }

        // mL = 1/ln(M); M < 2 forces every node to layer 0 (avoids ln(1)=0).
        let mml = if m >= 2 { 1.0 / f64::from(m).ln() } else { 0.0 };
        let mut rng = SplitMix64::new(HNSW_SEED);
        let mut layers: Vec<Vec<Vec<u32>>> = Vec::with_capacity(n);
        let mut entry_point: usize = 0;
        let mut max_layer: usize = 0;

        for i in 0..n {
            // Draw this node's top level; nudge u off 0 so ln is finite, cap height.
            let u = {
                let x = rng.next_f64();
                if x <= 0.0 { f64::MIN_POSITIVE } else { x }
            };
            let level = ((-(u.ln()) * mml).floor() as usize).min(HNSW_MAX_LEVEL);
            layers.push(vec![Vec::new(); level + 1]);

            if i == 0 {
                entry_point = 0;
                max_layer = level;
                continue;
            }

            let q = row_slice(&data, d, i).to_vec();
            let mut ep = entry_point;

            // 1. Greedy descent (ef=1) from max_layer down to level+1.
            let mut lc = max_layer;
            while lc > level {
                ep = hnsw_search_layer(&q, &[ep as u32], &data, d, metric, &layers, lc, 1)
                    .first()
                    .map_or(ep, |&(_, nd)| nd as usize);
                lc -= 1;
            }

            // 2. For layers min(level, max_layer)..=0: ef_construction search,
            //    diversity-select neighbors, add bidirectional links, re-prune.
            let top = level.min(max_layer);
            let mut lc_i = top as isize;
            while lc_i >= 0 {
                let lc = lc_i as usize;
                let found = hnsw_search_layer(
                    &q,
                    &[ep as u32],
                    &data,
                    d,
                    metric,
                    &layers,
                    lc,
                    ef_construction as usize,
                );
                let mmax = if lc == 0 {
                    (2 * m) as usize
                } else {
                    m as usize
                };
                let selected = hnsw_select_neighbors(&found, mmax, &data, d, metric);
                for &nbr in &selected {
                    layers[i][lc].push(nbr);
                    layers[nbr as usize][lc].push(i as u32);
                    if layers[nbr as usize][lc].len() > mmax {
                        let nrow = row_slice(&data, d, nbr as usize).to_vec();
                        let mut conns: Vec<(f32, u32)> = layers[nbr as usize][lc]
                            .iter()
                            .map(|&x| (distance(metric, &nrow, row_slice(&data, d, x as usize)), x))
                            .collect();
                        conns.sort_by(|a, b| cmp_dist(*a, *b));
                        layers[nbr as usize][lc] =
                            hnsw_select_neighbors(&conns, mmax, &data, d, metric);
                    }
                }
                ep = found.first().map_or(ep, |&(_, nd)| nd as usize);
                lc_i -= 1;
            }

            // 3. Promote entry point if this node is taller than the graph.
            if level > max_layer {
                max_layer = level;
                entry_point = i;
            }
        }

        Ok(HnswIndex {
            dim,
            metric,
            m,
            ef_construction,
            ef_search,
            entry_point: entry_point as u32,
            max_layer: max_layer as u32,
            keys,
            data,
            layers,
        })
    }

    /// Override the query-time candidate width in place (clamped to `>= 1`). No-op when empty.
    pub fn set_ef_search(&mut self, ef_search: u32) {
        if !self.keys.is_empty() {
            self.ef_search = ef_search.max(1);
        }
    }

    /// Override the query-time candidate width (clamped to `>= 1`). No-op when empty.
    #[must_use]
    pub fn with_ef_search(mut self, ef_search: u32) -> HnswIndex {
        self.set_ef_search(ef_search);
        self
    }

    // --- compact binary format -------------------------------------------------
    // magic "LVIX" | u8 version=1 | u8 metric | u8 kind=2 |
    // u32 dim | u32 m | u32 ef_construction | u32 ef_search |
    // u32 entry_point | u32 max_layer | u32 row_count |
    // data (row_count*dim f32 LE) |
    // per node: u8 node_max_layer | for layer 0..=node_max_layer: u32 nbr_count | nbr_count u32 LE |
    // u8 key_kind | keys (as FlatIndex)
    fn serialize_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(b"LVIX");
        out.push(1); // version
        out.push(match self.metric {
            Metric::Cosine => 0,
            Metric::L2 => 1,
        });
        out.push(2); // kind: hnsw
        out.extend_from_slice(&self.dim.to_le_bytes());
        out.extend_from_slice(&self.m.to_le_bytes());
        out.extend_from_slice(&self.ef_construction.to_le_bytes());
        out.extend_from_slice(&self.ef_search.to_le_bytes());
        out.extend_from_slice(&self.entry_point.to_le_bytes());
        out.extend_from_slice(&self.max_layer.to_le_bytes());
        out.extend_from_slice(&self.row_count().to_le_bytes());
        for &f in &self.data {
            out.extend_from_slice(&f.to_le_bytes());
        }
        for node in &self.layers {
            // node.len() == node_level + 1, always >= 1 and <= HNSW_MAX_LEVEL + 1 <= 65.
            let nml = node.len().saturating_sub(1) as u8;
            out.push(nml);
            for layer in node {
                out.extend_from_slice(&(layer.len() as u32).to_le_bytes());
                for &nb in layer {
                    out.extend_from_slice(&nb.to_le_bytes());
                }
            }
        }
        let key_kind: u8 = match self.keys.first() {
            Some(VectorKey::Str(_)) => 1,
            _ => 0,
        };
        out.push(key_kind);
        for key in &self.keys {
            match key {
                VectorKey::Int(i) => out.extend_from_slice(&i.to_le_bytes()),
                VectorKey::Str(s) => {
                    out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    out.extend_from_slice(s.as_bytes());
                }
            }
        }
        out
    }

    pub fn deserialize(bytes: &[u8]) -> Result<HnswIndex> {
        let mut c = Cursor { b: bytes, p: 0 };
        if c.take(4)? != b"LVIX" {
            return Err(bad("bad magic"));
        }
        if c.u8()? != 1 {
            return Err(bad("unsupported version"));
        }
        let metric = match c.u8()? {
            0 => Metric::Cosine,
            1 => Metric::L2,
            _ => return Err(bad("bad metric")),
        };
        if c.u8()? != 2 {
            return Err(bad("not an hnsw index"));
        }
        let dim = c.u32()?;
        let m = c.u32()?;
        let ef_construction = c.u32()?;
        let ef_search = c.u32()?;
        let entry_point = c.u32()?;
        let max_layer = c.u32()?;
        let row_count = c.u32()?;
        let d = dim as usize;
        let rc = row_count as usize;
        // Bound the data section against the buffer before allocating: a
        // corrupt-but-self-consistent header otherwise drives a huge speculative
        // allocation. Each f32 is 4 bytes, so `rc * d` elements cannot exceed the
        // bytes left to read (conservative: never rejects a well-formed blob).
        let data_len = rc
            .checked_mul(d)
            .ok_or_else(|| bad("row_count * dim overflow"))?;
        if data_len > c.remaining() {
            return Err(bad("data section exceeds buffer"));
        }
        if rc > 0 && entry_point as usize >= rc {
            return Err(bad("entry_point out of range"));
        }
        let mut data = Vec::with_capacity(data_len);
        for _ in 0..data_len {
            data.push(c.f32()?);
        }
        let mut layers: Vec<Vec<Vec<u32>>> = Vec::with_capacity(rc.min(c.remaining()));
        for _ in 0..row_count {
            let nml = c.u8()? as usize;
            let mut node: Vec<Vec<u32>> = Vec::with_capacity(nml + 1);
            for _ in 0..=nml {
                let cnt = c.u32()? as usize;
                if cnt > c.remaining() {
                    return Err(bad("neighbor list exceeds buffer"));
                }
                let mut nbrs = Vec::with_capacity(cnt);
                for _ in 0..cnt {
                    let nbr = c.u32()?;
                    if nbr as usize >= rc {
                        return Err(bad("neighbor index out of range"));
                    }
                    nbrs.push(nbr);
                }
                node.push(nbrs);
            }
            layers.push(node);
        }
        // Layer consistency: a node listed in another node's layer-`l` adjacency
        // must itself have a layer `l` (its own height >= l), or the search-time
        // `layers[c][lc]` access panics. `layers[nbr]` exists (nbr < rc above).
        for node in &layers {
            for (l, layer) in node.iter().enumerate() {
                for &nbr in layer {
                    if layers.get(nbr as usize).map_or(0, Vec::len) <= l {
                        return Err(bad("neighbor references absent layer"));
                    }
                }
            }
        }
        // The greedy descent enters at `layers[entry_point][max_layer]`, so the
        // entry node must reach that height.
        if rc > 0 && max_layer as usize >= layers.get(entry_point as usize).map_or(0, Vec::len) {
            return Err(bad("max_layer exceeds entry point height"));
        }
        let key_kind = c.u8()?;
        let mut keys = Vec::with_capacity(rc.min(c.remaining()));
        for _ in 0..row_count {
            match key_kind {
                0 => keys.push(VectorKey::Int(c.i64()?)),
                1 => {
                    let len = c.u32()? as usize;
                    let raw = c.take(len)?;
                    let s = std::str::from_utf8(raw).map_err(|e| bad(&e.to_string()))?;
                    keys.push(VectorKey::Str(s.to_string()));
                }
                _ => return Err(bad("bad key kind")),
            }
        }
        Ok(HnswIndex {
            dim,
            metric,
            m,
            ef_construction,
            ef_search,
            entry_point,
            max_layer,
            keys,
            data,
            layers,
        })
    }
}

impl VectorIndex for HnswIndex {
    fn metric(&self) -> Metric {
        self.metric
    }
    fn dim(&self) -> u32 {
        self.dim
    }
    fn index_kind(&self) -> IndexKind {
        IndexKind::Hnsw
    }
    fn row_count(&self) -> u32 {
        self.keys.len() as u32
    }
    fn serialize(&self) -> Vec<u8> {
        self.serialize_bytes()
    }
    fn apply_query_knobs(&mut self, _nprobe: Option<u32>, ef_search: Option<u32>) {
        if let Some(e) = ef_search {
            self.set_ef_search(e);
        }
    }
    fn search(&self, query: &[f32], k: usize) -> Vec<(VectorKey, f32)> {
        if self.keys.is_empty() || k == 0 {
            return Vec::new();
        }
        let d = self.dim as usize;
        let ef = (self.ef_search as usize).max(k);
        // 1. Greedy descent (ef=1) from the entry point through layers max_layer..1.
        let mut ep = self.entry_point as usize;
        let mut lc = self.max_layer as usize;
        while lc > 0 {
            ep = hnsw_search_layer(
                query,
                &[ep as u32],
                &self.data,
                d,
                self.metric,
                &self.layers,
                lc,
                1,
            )
            .first()
            .map_or(ep, |&(_, nd)| nd as usize);
            lc -= 1;
        }
        // 2. ef-width search at layer 0; top-k ascending (already sorted with index tie-break).
        let found = hnsw_search_layer(
            query,
            &[ep as u32],
            &self.data,
            d,
            self.metric,
            &self.layers,
            0,
            ef,
        );
        found
            .into_iter()
            .take(k)
            .filter_map(|(dd, i)| self.keys.get(i as usize).map(|key| (key.clone(), dd)))
            .collect()
    }
}

/// Decode any serialized loom vector index into a boxed `VectorIndex`, routing on
/// the `kind` byte (offset 6: magic[4] + version + metric). Used by the engine
/// serving + postgres read paths.
pub fn decode(bytes: &[u8]) -> Result<Box<dyn VectorIndex>> {
    let kind = *bytes.get(6).ok_or_else(|| bad("truncated index header"))?;
    match kind {
        0 => Ok(Box::new(FlatIndex::deserialize(bytes)?)),
        1 => Ok(Box::new(IvfFlatIndex::deserialize(bytes)?)),
        2 => Ok(Box::new(HnswIndex::deserialize(bytes)?)),
        _ => Err(bad("unknown index kind")),
    }
}

impl VectorIndex for FlatIndex {
    fn metric(&self) -> Metric {
        self.metric
    }
    fn dim(&self) -> u32 {
        self.dim
    }
    fn index_kind(&self) -> IndexKind {
        IndexKind::Flat
    }
    fn row_count(&self) -> u32 {
        // Delegates to the inherent method (kept for non-trait callers).
        FlatIndex::row_count(self)
    }
    fn serialize(&self) -> Vec<u8> {
        FlatIndex::serialize(self)
    }
    fn search(&self, query: &[f32], k: usize) -> Vec<(VectorKey, f32)> {
        let mut scored: Vec<(usize, f32)> = (0..self.keys.len())
            .map(|i| (i, distance(self.metric, query, self.row(i))))
            .collect();
        // Stable sort by distance; ties keep insertion order.
        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        scored
            .into_iter()
            .take(k)
            .filter_map(|(i, d)| self.keys.get(i).map(|key| (key.clone(), d)))
            .collect()
    }
}
