//! Shared binary primitives of the LVIX wire format — the header, bounded f32
//! sections, key section, and row packing that all three index (de)serializers
//! (`flat`, `ivf`, `hnsw`) build on. Everything is `pub(super)`: siblings need
//! it; nothing leaves the `vector_index` tree.

use super::{Metric, VectorKey};
use crate::error::{ControlPlaneError, Result};

/// Kind byte for `FlatIndex`.
pub(super) const KIND_FLAT: u8 = 0;
/// Kind byte for `IvfFlatIndex`.
pub(super) const KIND_IVF_FLAT: u8 = 1;
/// Kind byte for `HnswIndex`.
pub(super) const KIND_HNSW: u8 = 2;
/// Byte offset of the kind byte `decode()` peeks: magic[4] + version + metric.
pub(super) const KIND_OFFSET: usize = 6;

/// The decode-error constructor shared by every decoder and `decode()`.
pub(super) fn bad(m: &str) -> ControlPlaneError {
    ControlPlaneError::Backend(format!("vector index decode: {m}").into())
}

/// Final-step decode guard: the identity block ends every LVIX format, so any
/// unread byte means a corrupt/padded blob (or a mixed-kind blob written by a
/// pre-fix loom whose two encodings happened to sum compatibly).
pub(super) fn expect_eof(r: &ByteReader<'_>) -> Result<()> {
    if r.remaining() > 0 {
        return Err(bad("trailing bytes after identity block"));
    }
    Ok(())
}

/// Bounds-checked little-endian reader over a serialized index blob.
///
/// (Formerly the module-private `Cursor` — renamed because it shadowed
/// `crate::page::Cursor`, which IS re-exported at the crate root.)
pub(super) struct ByteReader<'a> {
    b: &'a [u8],
    p: usize,
}

impl<'a> ByteReader<'a> {
    pub(super) fn new(b: &'a [u8]) -> ByteReader<'a> {
        ByteReader { b, p: 0 }
    }
    /// Bytes left to read. An untrusted element count exceeding this cannot be
    /// satisfied (every element occupies at least one byte), so it bounds
    /// speculative `Vec::with_capacity` against a corrupt header.
    pub(super) fn remaining(&self) -> usize {
        self.b.len().saturating_sub(self.p)
    }
    pub(super) fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.p.checked_add(n).ok_or_else(|| bad("overflow"))?;
        let s = self.b.get(self.p..end).ok_or_else(|| bad("truncated"))?;
        self.p = end;
        Ok(s)
    }
    pub(super) fn u8(&mut self) -> Result<u8> {
        Ok(*self.take(1)?.first().ok_or_else(|| bad("truncated"))?)
    }
    pub(super) fn u32(&mut self) -> Result<u32> {
        let s = self.take(4)?;
        let arr: [u8; 4] = s.try_into().map_err(|e| bad(&format!("u32: {e}")))?;
        Ok(u32::from_le_bytes(arr))
    }
    pub(super) fn i64(&mut self) -> Result<i64> {
        let s = self.take(8)?;
        let arr: [u8; 8] = s.try_into().map_err(|e| bad(&format!("i64: {e}")))?;
        Ok(i64::from_le_bytes(arr))
    }
    pub(super) fn f32(&mut self) -> Result<f32> {
        let s = self.take(4)?;
        let arr: [u8; 4] = s.try_into().map_err(|e| bad(&format!("f32: {e}")))?;
        Ok(f32::from_le_bytes(arr))
    }
}

/// Write the 7 fixed header bytes: magic `"LVIX"` | u8 version=1 | u8 metric |
/// u8 kind.
pub(super) fn write_header(out: &mut Vec<u8>, metric: Metric, kind: u8) {
    out.extend_from_slice(b"LVIX");
    out.push(1); // version
    out.push(match metric {
        Metric::Cosine => 0,
        Metric::L2 => 1,
    });
    out.push(kind);
}

/// Validate magic/version/metric, then the kind byte. `kind_mismatch` preserves
/// each decoder's exact error text ("bad index kind" / "not an ivf_flat index" /
/// "not an hnsw index").
pub(super) fn read_header(
    r: &mut ByteReader<'_>,
    expected_kind: u8,
    kind_mismatch: &'static str,
) -> Result<Metric> {
    if r.take(4)? != b"LVIX" {
        return Err(bad("bad magic"));
    }
    if r.u8()? != 1 {
        return Err(bad("unsupported version"));
    }
    let metric = match r.u8()? {
        0 => Metric::Cosine,
        1 => Metric::L2,
        _ => return Err(bad("bad metric")),
    };
    if r.u8()? != expected_kind {
        return Err(bad(kind_mismatch));
    }
    Ok(metric)
}

/// Write a packed f32 section, little-endian per element.
pub(super) fn write_f32s(out: &mut Vec<u8>, xs: &[f32]) {
    for &f in xs {
        out.extend_from_slice(&f.to_le_bytes());
    }
}

/// Which f32 section is being read — selects the exact error strings so the
/// refactor is message-identical.
pub(super) enum F32Section {
    Data,
    Centroids,
}

/// Read an `n * d` f32 section. THE bounded-`with_capacity` corrupt-header
/// guard, once (closes `iss-hnsw-deserialize-bounds` and
/// `iss-flat-ivf-deserialize-bounds`): `n.checked_mul(d)` (overflow →
/// "row_count * dim overflow" / "nlist * dim overflow"), then
/// `len > r.remaining()` → "data section exceeds buffer" /
/// "centroid section exceeds buffer" — each f32 is 4 bytes, so `n * d` elements
/// cannot exceed the bytes left to read (conservative: never rejects a
/// well-formed blob) — then the element loop.
pub(super) fn read_f32_section(
    r: &mut ByteReader<'_>,
    n: usize,
    d: usize,
    section: F32Section,
) -> Result<Vec<f32>> {
    let (overflow_msg, exceeds_msg) = match section {
        F32Section::Data => ("row_count * dim overflow", "data section exceeds buffer"),
        F32Section::Centroids => ("nlist * dim overflow", "centroid section exceeds buffer"),
    };
    let len = n.checked_mul(d).ok_or_else(|| bad(overflow_msg))?;
    if len > r.remaining() {
        return Err(bad(exceeds_msg));
    }
    let mut xs = Vec::with_capacity(len);
    for _ in 0..len {
        xs.push(r.f32()?);
    }
    Ok(xs)
}

/// Write the identity block: u8 key_kind (0=int from `keys.first()` — empty ⇒
/// 0 — 1=str), then per key: i64 LE, or u32 len LE + utf8 bytes. All keys share
/// one key_kind (the identity column's logical type) — enforced by `pack_rows`,
/// the single rows→columns seam every build goes through, so a mixed-kind
/// `keys` slice can never reach this writer.
pub(super) fn write_keys(out: &mut Vec<u8>, keys: &[VectorKey]) {
    let key_kind: u8 = match keys.first() {
        Some(VectorKey::Str(_)) => 1,
        _ => 0, // empty or Int
    };
    out.push(key_kind);
    for key in keys {
        match key {
            VectorKey::Int(i) => out.extend_from_slice(&i.to_le_bytes()),
            VectorKey::Str(s) => {
                out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                out.extend_from_slice(s.as_bytes());
            }
        }
    }
}

/// Read the identity block: key_kind byte +
/// `Vec::with_capacity(row_count.min(r.remaining()))` (the corrupt-header
/// capacity cap) + int/str loops + "bad key kind" / utf8-error arms.
pub(super) fn read_keys(r: &mut ByteReader<'_>, row_count: usize) -> Result<Vec<VectorKey>> {
    let key_kind = r.u8()?;
    let mut keys = Vec::with_capacity(row_count.min(r.remaining()));
    for _ in 0..row_count {
        match key_kind {
            0 => keys.push(VectorKey::Int(r.i64()?)),
            1 => {
                let len = r.u32()? as usize;
                let raw = r.take(len)?;
                let s = std::str::from_utf8(raw).map_err(|e| bad(&e.to_string()))?;
                keys.push(VectorKey::Str(s.to_string()));
            }
            _ => return Err(bad("bad key kind")),
        }
    }
    Ok(keys)
}

/// Pack `(identity, vector)` rows into the parallel `(keys, row-major data)`
/// columns all three builds use, validating every vector length against `dim`
/// (error: "vector dim mismatch: expected {d}, got {n}") and that every
/// identity key shares the first row's kind (error: "mixed identity key kinds:
/// all index keys must be Int or all Str") — the codec's one-key-kind wire
/// invariant (`write_keys` encodes a single key_kind byte for the whole index).
pub(super) fn pack_rows(
    dim: u32,
    rows: Vec<(VectorKey, Vec<f32>)>,
) -> Result<(Vec<VectorKey>, Vec<f32>)> {
    let d = dim as usize;
    let expected_kind = rows.first().map(|(k, _)| std::mem::discriminant(k));
    let mut keys = Vec::with_capacity(rows.len());
    let mut data = Vec::with_capacity(rows.len() * d);
    for (key, v) in rows {
        if Some(std::mem::discriminant(&key)) != expected_kind {
            return Err(ControlPlaneError::Validation(
                "mixed identity key kinds: all index keys must be Int or all Str".to_string(),
            ));
        }
        if v.len() != d {
            return Err(ControlPlaneError::Validation(format!(
                "vector dim mismatch: expected {d}, got {}",
                v.len()
            )));
        }
        keys.push(key);
        data.extend_from_slice(&v);
    }
    Ok((keys, data))
}
