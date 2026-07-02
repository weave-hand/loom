//! HnswIndex — approximate HNSW graph index with deterministic build.

use super::codec::{
    ByteReader, F32Section, KIND_HNSW, bad, pack_rows, read_f32_section, read_header, read_keys,
    write_f32s, write_header, write_keys,
};
use super::{IndexKind, Metric, SplitMix64, VectorIndex, VectorKey, distance, row_slice};
use crate::error::Result;

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

// The two beam-search maintenance helpers below keep their linear scans
// deliberately: both are O(len) with len bounded by `ef` (+M fan-out), where a
// heap's constant factors and code weight buy nothing. The manual first-wins
// scans (strict `Less` / strict `Greater`) are load-bearing: an
// `iter().min_by`/`max_by` keeps the LAST extremum on ties (reachable via NaN
// distances → `cmp_dist` Equal), which would change node selection.

/// Pop the frontier element nearest by `cmp_dist` (linear min-scan,
/// `swap_remove`); `None` when the frontier is exhausted.
#[expect(
    clippy::indexing_slicing,
    reason = "j and best are both < frontier.len() by the loop bound"
)]
fn pop_nearest(frontier: &mut Vec<(f32, u32)>) -> Option<(f32, u32)> {
    if frontier.is_empty() {
        return None;
    }
    let mut best = 0usize;
    for j in 1..frontier.len() {
        if cmp_dist(frontier[j], frontier[best]) == std::cmp::Ordering::Less {
            best = j;
        }
    }
    Some(frontier.swap_remove(best))
}

/// Evict the current worst result by `cmp_dist` (linear max-scan, `swap_remove`).
/// No-op on an empty vec (callers only invoke it when `results.len() > ef >= 1`).
#[expect(
    clippy::indexing_slicing,
    reason = "j and w are both < results.len() by the loop bound"
)]
fn evict_worst(results: &mut Vec<(f32, u32)>) {
    if results.is_empty() {
        return;
    }
    let mut w = 0usize;
    for j in 1..results.len() {
        if cmp_dist(results[j], results[w]) == std::cmp::Ordering::Greater {
            w = j;
        }
    }
    results.swap_remove(w);
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
    while let Some((cd, c)) = pop_nearest(&mut frontier) {
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
                        evict_worst(&mut results);
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
        let (keys, data) = pack_rows(dim, rows)?;
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
        write_header(&mut out, self.metric, KIND_HNSW);
        out.extend_from_slice(&self.dim.to_le_bytes());
        out.extend_from_slice(&self.m.to_le_bytes());
        out.extend_from_slice(&self.ef_construction.to_le_bytes());
        out.extend_from_slice(&self.ef_search.to_le_bytes());
        out.extend_from_slice(&self.entry_point.to_le_bytes());
        out.extend_from_slice(&self.max_layer.to_le_bytes());
        out.extend_from_slice(&self.row_count().to_le_bytes());
        write_f32s(&mut out, &self.data);
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
        write_keys(&mut out, &self.keys);
        out
    }

    pub fn deserialize(bytes: &[u8]) -> Result<HnswIndex> {
        let mut r = ByteReader::new(bytes);
        let metric = read_header(&mut r, KIND_HNSW, "not an hnsw index")?;
        let dim = r.u32()?;
        let m = r.u32()?;
        let ef_construction = r.u32()?;
        let ef_search = r.u32()?;
        let entry_point = r.u32()?;
        let max_layer = r.u32()?;
        let row_count = r.u32()?;
        let d = dim as usize;
        let rc = row_count as usize;
        // The graph-invariant guards below are HNSW's decode-time soundness —
        // what makes the search-time `layers[c][lc]`/`row_slice` indexing
        // sound — not a wire-format concern, so they stay here. The `rc > 0`
        // gates are load-bearing: an empty blob (rc=0, max_layer=0) must stay
        // decodable. This header-only check runs BEFORE the data reads to
        // preserve the pre-split guard ordering.
        if rc > 0 && entry_point as usize >= rc {
            return Err(bad("entry_point out of range"));
        }
        let data = read_f32_section(&mut r, rc, d, F32Section::Data)?;
        let mut layers: Vec<Vec<Vec<u32>>> = Vec::with_capacity(rc.min(r.remaining()));
        for _ in 0..row_count {
            let nml = r.u8()? as usize;
            let mut node: Vec<Vec<u32>> = Vec::with_capacity(nml + 1);
            for _ in 0..=nml {
                let cnt = r.u32()? as usize;
                if cnt > r.remaining() {
                    return Err(bad("neighbor list exceeds buffer"));
                }
                let mut nbrs = Vec::with_capacity(cnt);
                for _ in 0..cnt {
                    let nbr = r.u32()?;
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
        let keys = read_keys(&mut r, rc)?;
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
