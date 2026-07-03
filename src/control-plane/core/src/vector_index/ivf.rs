//! IvfFlatIndex — approximate IVF-Flat index with deterministic k-means build.

use super::codec::{
    ByteReader, F32Section, KIND_IVF_FLAT, expect_eof, pack_rows, read_f32_section, read_header,
    read_keys, write_f32s, write_header, write_keys,
};
use super::kmeans;
use super::{IndexKind, Metric, SplitMix64, VectorIndex, VectorKey, distance, row_slice};
use crate::error::Result;

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
        let (keys, data) = pack_rows(dim, rows)?;
        let n = keys.len();

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
        let mut rng = SplitMix64::new(kmeans::KMEANS_SEED);
        let (centroids, assignments) =
            kmeans::kmeans(&data, d, n, nlist as usize, metric, &mut rng);
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
        write_header(&mut out, self.metric, KIND_IVF_FLAT);
        out.extend_from_slice(&self.dim.to_le_bytes());
        out.extend_from_slice(&self.nlist.to_le_bytes());
        out.extend_from_slice(&self.nprobe.to_le_bytes());
        out.extend_from_slice(&self.row_count().to_le_bytes());
        write_f32s(&mut out, &self.centroids);
        for &a in &self.assignments {
            out.extend_from_slice(&a.to_le_bytes());
        }
        write_f32s(&mut out, &self.data);
        write_keys(&mut out, &self.keys);
        out
    }

    pub fn deserialize(bytes: &[u8]) -> Result<IvfFlatIndex> {
        let mut r = ByteReader::new(bytes);
        let metric = read_header(&mut r, KIND_IVF_FLAT, "not an ivf_flat index")?;
        let dim = r.u32()?;
        let nlist = r.u32()?;
        let nprobe = r.u32()?;
        let row_count = r.u32()?;
        let d = dim as usize;
        let centroids = read_f32_section(&mut r, nlist as usize, d, F32Section::Centroids)?;
        // The u32 assignments section is IVF-only, so it stays local; the
        // `min(remaining)` cap bounds the speculative allocation the same way
        // the codec's shared sections do.
        let mut assignments = Vec::with_capacity((row_count as usize).min(r.remaining()));
        for _ in 0..row_count {
            assignments.push(r.u32()?);
        }
        let data = read_f32_section(&mut r, row_count as usize, d, F32Section::Data)?;
        let keys = read_keys(&mut r, row_count as usize)?;
        expect_eof(&r)?;
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
