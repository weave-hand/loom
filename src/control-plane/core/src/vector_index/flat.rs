//! Exact (flat/brute-force) vector index.

use super::codec::{
    ByteReader, F32Section, KIND_FLAT, pack_rows, read_f32_section, read_header, read_keys,
    write_f32s, write_header, write_keys,
};
use super::{IndexKind, Metric, VectorIndex, VectorKey, distance};
use crate::error::Result;

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
        let (keys, data) = pack_rows(dim, rows)?;
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
        write_header(&mut out, self.metric, KIND_FLAT);
        out.extend_from_slice(&self.dim.to_le_bytes());
        out.extend_from_slice(&self.row_count().to_le_bytes());
        write_f32s(&mut out, &self.data);
        write_keys(&mut out, &self.keys);
        out
    }

    pub fn deserialize(bytes: &[u8]) -> Result<FlatIndex> {
        let mut r = ByteReader::new(bytes);
        let metric = read_header(&mut r, KIND_FLAT, "bad index kind")?;
        let dim = r.u32()?;
        let row_count = r.u32()?;
        let data = read_f32_section(&mut r, row_count as usize, dim as usize, F32Section::Data)?;
        let keys = read_keys(&mut r, row_count as usize)?;
        Ok(FlatIndex {
            dim,
            metric,
            keys,
            data,
        })
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
