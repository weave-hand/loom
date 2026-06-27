//! A swappable vector-index abstraction and an exact (flat/brute-force) first
//! implementation. Pure: no iceberg, no arrow, no object store. The serialized
//! form is a compact self-describing binary written into a Puffin blob by the
//! postgres adapter.

use crate::error::{ControlPlaneError, Result};

/// Distance metric, declared at build time and recorded with the index.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
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

    #[must_use]
    pub fn from_label(s: &str) -> Option<Metric> {
        match s {
            "cosine" => Some(Metric::Cosine),
            "l2" => Some(Metric::L2),
            _ => None,
        }
    }
}

/// The index algorithm family. Slice 1 ships only `Flat`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexKind {
    Flat,
}

impl IndexKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            IndexKind::Flat => "flat",
        }
    }
}

/// The object identity value carried alongside each indexed vector so k-NN
/// results map back to objects. Covers the realistic identity logical types
/// (`Integer`/`Long` → `Int`, `String` → `Str`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VectorKey {
    Int(i64),
    Str(String),
}

/// Exact-or-approximate top-k nearest-neighbour index. Slice-1 impl is exact.
pub trait VectorIndex {
    fn metric(&self) -> Metric;
    fn dim(&self) -> u32;
    /// Top-k by `metric`, ascending distance. Ties broken by insertion order.
    fn search(&self, query: &[f32], k: usize) -> Vec<(VectorKey, f32)>;
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
        let mut data = Vec::with_capacity(row_count as usize * d);
        for _ in 0..(row_count as usize * d) {
            data.push(c.f32()?);
        }
        let key_kind = c.u8()?;
        let mut keys = Vec::with_capacity(row_count as usize);
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
    ControlPlaneError::Backend(format!("FlatIndex decode: {m}").into())
}

struct Cursor<'a> {
    b: &'a [u8],
    p: usize,
}
impl<'a> Cursor<'a> {
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

impl VectorIndex for FlatIndex {
    fn metric(&self) -> Metric {
        self.metric
    }
    fn dim(&self) -> u32 {
        self.dim
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
