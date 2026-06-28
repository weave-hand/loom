# HNSW Graph Vector Index Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a third, approximate `VectorIndex` implementation — a hand-rolled, deterministic HNSW graph index (`kind = 2`) — behind the existing trait, selectable at build time and decoded polymorphically at serve time, mirroring the IVF-Flat slice.

**Architecture:** `HnswIndex` lives entirely in `control-plane-core` (pure: no iceberg/arrow/object-store deps; reuses the existing `SplitMix64`, `row_slice`, `distance`). It serializes into the same self-describing `LVIX` blob with a new `kind = 2` discriminator; `decode` gains a `2 =>` arm so the engine serve path picks it up transparently. Build is single-threaded with a fixed PRNG seed, so the serialized blob is byte-identical for a given input row order. The cold∪hot merge in `engine-serving` is unchanged — HNSW covers the **cold tier only**; the inline hot delta is still scored exactly, so graph approximation never drops freshly-landed rows.

**Tech Stack:** Rust 2024, buck2, the loom control-plane crates, tonic/prost gRPC (engine wire), sqlx-backed Postgres fixture tests.

## Global Constraints

- **Spec:** `docs/superpowers/specs/2026-06-28-hnsw-vector-index-design.md` — every task implicitly includes its requirements.
- **No inline tests.** Tests are `rust_test` integration targets in sibling `tests/<name>.rs` files wired in the crate `BUCK`. Inline `#[cfg(test)]`/`#[test]` in `src/**.rs` is rejected by the `no-inline-tests` hook and never runs.
- **Fixture (Postgres-booting) tests use the `loom_fixture_test` macro**, never a bare `rust_test`, or they route to remote execution and fail as root.
- **Strict clippy (pedantic + restriction).** Production lib/bin code may not `unwrap`/`expect`/`panic`/`todo`/index a slice without a justifying `#[expect(lint, reason = "…")]`. Mirror the existing `#[expect(clippy::indexing_slicing, reason = "…")]` annotations on `kmeans`/`row_slice`. Test code is exempted from the panic-safety lints via the `loom_rust_test`/`loom_fixture_test` wrappers, so `.unwrap()` in tests is fine.
- **Determinism is a hard requirement.** Single-threaded build + the fixed `SplitMix64` seed `0x6C6F_6F6D_7665_6331` + index-based tie-breaking ⇒ byte-identical `serialize()` for a fixed input order. No `Math.random`, no threads, no `HashMap` iteration order in the serialized output.
- **Default index kind stays `Flat`.** Absent job/primitive params ⇒ exact `Flat`. HNSW is opt-in (`index_kind = "hnsw"`).
- **Don't pipe `buck2 test` through `tail`/`head`.** Redirect to a file and grep: `buck2 test //… > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/t.log`.
- **buck2 commands:** build a crate with `buck2 build //src/control-plane/core:core`; run a single test target with `buck2 test //src/control-plane/core:vector-index`.

## File Structure

| File | Responsibility | Task |
|------|----------------|------|
| `src/control-plane/core/src/vector_index.rs` | `HnswIndex` struct + algorithm + serialize/deserialize + `VectorIndex` impl; `IndexKind::Hnsw`; `decode` arm | 1 |
| `src/control-plane/core/src/lib.rs` | export `HnswIndex` | 1 |
| `src/control-plane/core/tests/vector_index.rs` | core HNSW unit tests (recall, determinism, round-trip, decode dispatch, edges) | 1 |
| `src/control-plane/core/src/vector_index.rs` | `IndexSpec::Hnsw` + extended `from_label` | 2 |
| `src/control-plane/core/src/vector_index_job.rs` | `BuildVectorIndexJob` `m`/`ef_construction` fields + `index_spec()` | 2 |
| `src/control-plane/postgres/src/vector_index.rs` | `IndexSpec::Hnsw` dispatch arm → `HnswIndex::build` | 2 |
| `src/services/engine/src/service.rs` | forced `from_label` arity fix (pass `None, None` until Task 3) | 2 |
| `src/services/worker/src/handler.rs` | forced struct-pattern fix (`..` until Task 3) | 2 |
| `src/control-plane/core/tests/vector_index_job.rs` | `from_label` table + HNSW payload unit tests | 2 |
| `src/services/engine-wire/proto/engine_control.proto` | `m`/`ef_construction` request fields | 3 |
| `src/services/engine-wire/src/client.rs` | thread `m`/`ef_construction` params | 3 |
| `src/services/engine/src/service.rs` | read `req.m`/`req.ef_construction` → `from_label` | 3 |
| `src/services/worker/src/handler.rs` | destructure + thread `m`/`ef_construction` | 3 |
| `src/control-plane/postgres/tests/vector_index_hnsw.rs` (new) + `BUCK` | postgres fixture test: HNSW build → kind=2 blob + mirror row | 4 |
| `src/services/worker/tests/build_vector_index.rs` | worker e2e: HNSW job over the wire → mirror `index_kind = "hnsw"` | 4 |
| `src/services/engine-serving/tests/vector_search.rs` | engine freshness fixture test: HNSW cold + exact hot delta | 5 |

---

### Task 1: Core `HnswIndex` — algorithm, serialization, trait impl, decode dispatch

**Files:**
- Modify: `src/control-plane/core/src/vector_index.rs`
- Modify: `src/control-plane/core/src/lib.rs`
- Test: `src/control-plane/core/tests/vector_index.rs`

**Interfaces:**
- Consumes (existing, in this file): `SplitMix64` (with `next_f64`), `row_slice(data, d, i) -> &[f32]`, `distance(metric, a, b) -> f32`, `Cursor`, `bad(msg)`, `Metric`, `VectorKey`, `IndexKind`, the `VectorIndex` trait, `ControlPlaneError`/`Result`.
- Produces:
  - `IndexKind::Hnsw` (with `as_str` → `"hnsw"`, `FromStr` arm for `"hnsw"`).
  - `pub struct HnswIndex` with fields `dim: u32, metric: Metric, m: u32, ef_construction: u32, ef_search: u32, entry_point: u32, max_layer: u32, keys: Vec<VectorKey>, data: Vec<f32>, layers: Vec<Vec<Vec<u32>>>`.
  - `HnswIndex::build(dim: u32, metric: Metric, rows: Vec<(VectorKey, Vec<f32>)>, m: Option<u32>, ef_construction: Option<u32>) -> Result<HnswIndex>`.
  - `HnswIndex::with_ef_search(self, ef_search: u32) -> HnswIndex` (clamped to ≥ 1; no-op when empty).
  - `HnswIndex::deserialize(bytes: &[u8]) -> Result<HnswIndex>`.
  - `impl VectorIndex for HnswIndex` (`metric`/`dim`/`index_kind`/`row_count`/`serialize`/`search`).
  - `decode` routes `2 => HnswIndex::deserialize`.
  - `lib.rs` re-exports `HnswIndex`.

- [ ] **Step 1: Write the failing core HNSW tests**

Append to `src/control-plane/core/tests/vector_index.rs`. (The `clustered_rows()` / `Lcg` helpers already exist in this file from the IVF tests — reuse them.) Add `HnswIndex` to the top `use` line:

```rust
// change the first line of the file from:
//   use control_plane_core::{FlatIndex, IvfFlatIndex, Metric, VectorIndex, VectorKey};
// to:
use control_plane_core::{FlatIndex, HnswIndex, IvfFlatIndex, Metric, VectorIndex, VectorKey};
```

Then append these tests at the end of the file:

```rust
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
    assert_eq!(a.serialize(), b.serialize(), "same input order -> identical bytes");
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
```

Also update the existing `index_kind_as_str_and_parse` test (around line 81) to cover the new variant — replace its body with:

```rust
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
```

And update the unknown-kind probe in the existing `decode_routes_on_kind_byte` test (the final two assertions, ~line 273-275) so the "unknown kind" case uses a byte that is still unknown after this task (`3`, not `2`):

```rust
    // Truncated header: fewer than 7 bytes -> bytes.get(6) is None -> error.
    assert!(control_plane_core::decode(&[0u8; 6]).is_err());
    // Unknown kind byte: magic ok, version 1, metric 0, kind byte = 3 -> error.
    assert!(control_plane_core::decode(&[b'L', b'V', b'I', b'X', 1, 0, 3]).is_err());
```

- [ ] **Step 2: Run the tests to verify they fail to compile**

Run: `buck2 test //src/control-plane/core:vector-index > /tmp/t.log 2>&1; grep -E "error\[|cannot find|Tests finished|FAIL" /tmp/t.log`
Expected: compile errors — `cannot find … HnswIndex`, `no variant … Hnsw`.

- [ ] **Step 3: Add `IndexKind::Hnsw`**

In `src/control-plane/core/src/vector_index.rs`, extend the `IndexKind` enum and its two impls:

```rust
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
```

- [ ] **Step 4: Add the `HnswIndex` struct, constants, and graph helpers**

Append a new section to `src/control-plane/core/src/vector_index.rs` (after the `IvfFlatIndex` block, before or after `Cursor` — anywhere in the module is fine). Constants and the deterministic graph helpers:

```rust
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
        let worst = results.iter().map(|&(dd, _)| dd).fold(f32::NEG_INFINITY, f32::max);
        if results.len() >= ef && cd > worst {
            break;
        }
        for &nbr in &layers[c as usize][lc] {
            if visited.insert(nbr) {
                let dn = distance(metric, query, row_slice(data, d, nbr as usize));
                let worst = results.iter().map(|&(dd, _)| dd).fold(f32::NEG_INFINITY, f32::max);
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
#[expect(
    clippy::indexing_slicing,
    reason = "candidate/selected node indices are valid graph rows by construction"
)]
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
```

Note: `dq` in `candidates` is already the distance from the candidate to the base row (the caller computes candidates relative to that base), so the heuristic compares `dist(c, s) < dist(c, base)` without needing the base vector as a parameter.

- [ ] **Step 5: Add `HnswIndex::build`**

```rust
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
        let ef_construction = ef_construction.unwrap_or(HNSW_DEFAULT_EF_CONSTRUCTION).max(1);
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
                    &q, &[ep as u32], &data, d, metric, &layers, lc, ef_construction as usize,
                );
                let mmax = if lc == 0 { (2 * m) as usize } else { m as usize };
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

    /// Override the query-time candidate width (clamped to ≥ 1). No-op when empty.
    #[must_use]
    pub fn with_ef_search(mut self, ef_search: u32) -> HnswIndex {
        if !self.keys.is_empty() {
            self.ef_search = ef_search.max(1);
        }
        self
    }
}
```

- [ ] **Step 6: Add `serialize_bytes` + `deserialize` (kind = 2)**

Inside the same `impl HnswIndex` block (or a second `impl` block), per the spec's serialized form:

```rust
impl HnswIndex {
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
        let mut data = Vec::with_capacity(row_count as usize * d);
        for _ in 0..(row_count as usize * d) {
            data.push(c.f32()?);
        }
        let mut layers: Vec<Vec<Vec<u32>>> = Vec::with_capacity(row_count as usize);
        for _ in 0..row_count {
            let nml = c.u8()? as usize;
            let mut node: Vec<Vec<u32>> = Vec::with_capacity(nml + 1);
            for _ in 0..=nml {
                let cnt = c.u32()? as usize;
                let mut nbrs = Vec::with_capacity(cnt);
                for _ in 0..cnt {
                    nbrs.push(c.u32()?);
                }
                node.push(nbrs);
            }
            layers.push(node);
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
```

- [ ] **Step 7: Add `impl VectorIndex for HnswIndex` + `decode` arm + lib export**

`impl VectorIndex`:

```rust
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
            ep = hnsw_search_layer(query, &[ep as u32], &self.data, d, self.metric, &self.layers, lc, 1)
                .first()
                .map_or(ep, |&(_, nd)| nd as usize);
            lc -= 1;
        }
        // 2. ef-width search at layer 0; top-k ascending (already sorted with index tie-break).
        let found =
            hnsw_search_layer(query, &[ep as u32], &self.data, d, self.metric, &self.layers, 0, ef);
        found
            .into_iter()
            .take(k)
            .filter_map(|(dd, i)| self.keys.get(i as usize).map(|key| (key.clone(), dd)))
            .collect()
    }
}
```

`decode` (add the `2 =>` arm to the existing `match kind`):

```rust
pub fn decode(bytes: &[u8]) -> Result<Box<dyn VectorIndex>> {
    let kind = *bytes.get(6).ok_or_else(|| bad("truncated index header"))?;
    match kind {
        0 => Ok(Box::new(FlatIndex::deserialize(bytes)?)),
        1 => Ok(Box::new(IvfFlatIndex::deserialize(bytes)?)),
        2 => Ok(Box::new(HnswIndex::deserialize(bytes)?)),
        _ => Err(bad("unknown index kind")),
    }
}
```

`src/control-plane/core/src/lib.rs` — add `HnswIndex` to the `vector_index` re-export:

```rust
pub use vector_index::{
    FlatIndex, HnswIndex, IndexKind, IndexSpec, IvfFlatIndex, Metric, VectorIndex, VectorKey,
    decode, distance,
};
```

- [ ] **Step 8: Run the tests + clippy to verify they pass**

Run: `buck2 test //src/control-plane/core:vector-index > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|PASS|error\[" /tmp/t.log`
Expected: all HNSW tests PASS, existing Flat/IVF tests still PASS.

Run: `buck2 build '//src/control-plane/core:core[clippy.txt]' > /tmp/c.log 2>&1; cat $(buck2 build '//src/control-plane/core:core[clippy.txt]' --show-output 2>/dev/null | awk '{print $2}')` — or simply `./tools/clippy-all.sh 2>&1 | grep -iE "warn|error|core" | head`.
Expected: no clippy warnings for `core` (empty `clippy.txt`).

- [ ] **Step 9: Commit**

```bash
git add src/control-plane/core/src/vector_index.rs src/control-plane/core/src/lib.rs src/control-plane/core/tests/vector_index.rs
git commit -m "feat(vector): add deterministic HNSW index (kind=2) behind VectorIndex"
```

---

### Task 2: Make HNSW selectable through the build primitive

**Files:**
- Modify: `src/control-plane/core/src/vector_index.rs` (`IndexSpec` + `from_label`)
- Modify: `src/control-plane/core/src/vector_index_job.rs`
- Modify: `src/control-plane/postgres/src/vector_index.rs` (dispatch arm + import)
- Modify: `src/services/engine/src/service.rs` (forced `from_label` arity)
- Modify: `src/services/worker/src/handler.rs` (forced struct-pattern fix)
- Test: `src/control-plane/core/tests/vector_index_job.rs`

**Interfaces:**
- Consumes: `HnswIndex::build` (Task 1), `IndexKind`, `Result`, `ControlPlaneError`.
- Produces:
  - `IndexSpec::Hnsw { m: Option<u32>, ef_construction: Option<u32> }`.
  - `IndexSpec::from_label(kind: Option<&str>, nlist: Option<u32>, m: Option<u32>, ef_construction: Option<u32>) -> Result<IndexSpec>` (new 4-arg signature).
  - `BuildVectorIndexJob` gains `#[serde(default)] m: Option<u32>` and `#[serde(default)] ef_construction: Option<u32>`; `index_spec()` calls the 4-arg `from_label`.
  - postgres `build_vector_index` handles `IndexSpec::Hnsw`.

- [ ] **Step 1: Write the failing `from_label`/job tests**

Replace the `index_spec_from_label_table` test in `src/control-plane/core/tests/vector_index_job.rs` (it has the old 2-arg signature) and replace `unknown_index_kind_is_error` (which currently treats `"hnsw"` as the unknown), then add an HNSW payload test:

```rust
#[test]
fn index_spec_from_label_table() {
    use control_plane_core::IndexSpec;
    assert!(matches!(
        IndexSpec::from_label(None, None, None, None).unwrap(),
        IndexSpec::Flat
    ));
    assert!(matches!(
        IndexSpec::from_label(Some("flat"), None, None, None).unwrap(),
        IndexSpec::Flat
    ));
    assert!(matches!(
        IndexSpec::from_label(Some("ivf_flat"), Some(8), None, None).unwrap(),
        IndexSpec::IvfFlat { nlist: Some(8) }
    ));
    assert!(matches!(
        IndexSpec::from_label(Some("hnsw"), None, Some(32), Some(128)).unwrap(),
        IndexSpec::Hnsw { m: Some(32), ef_construction: Some(128) }
    ));
    assert!(IndexSpec::from_label(Some("bogus"), None, None, None).is_err());
}

#[test]
fn unknown_index_kind_is_error() {
    let v = serde_json::json!({
        "schema": "wh", "name": "docs", "column": "embedding", "index_kind": "bogus"
    });
    let job: BuildVectorIndexJob = serde_json::from_value(v).unwrap();
    assert!(job.index_spec().is_err());
}

#[test]
fn hnsw_payload_maps_to_hnsw_spec() {
    use control_plane_core::IndexSpec;
    let v = serde_json::json!({
        "schema": "wh", "name": "docs", "column": "embedding",
        "index_kind": "hnsw", "m": 24, "ef_construction": 100
    });
    let job: BuildVectorIndexJob = serde_json::from_value(v).unwrap();
    match job.index_spec().unwrap() {
        IndexSpec::Hnsw { m, ef_construction } => {
            assert_eq!(m, Some(24));
            assert_eq!(ef_construction, Some(100));
        }
        other => panic!("expected Hnsw, got {other:?}"),
    }
}
```

The existing `legacy_payload_without_index_fields_deserializes_to_flat` and `ivf_payload_maps_to_ivf_spec` tests stay as-is — they must still pass (the new `#[serde(default)]` fields keep old payloads valid).

- [ ] **Step 2: Run to verify failure**

Run: `buck2 test //src/control-plane/core:vector-index-job > /tmp/t.log 2>&1; grep -E "error\[|Tests finished|FAIL" /tmp/t.log`
Expected: compile error — `from_label` takes 2 args / no variant `Hnsw`.

- [ ] **Step 3: Extend `IndexSpec` + `from_label`**

In `src/control-plane/core/src/vector_index.rs`:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IndexSpec {
    Flat,
    IvfFlat { nlist: Option<u32> },
    Hnsw { m: Option<u32>, ef_construction: Option<u32> },
}

impl IndexSpec {
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
```

- [ ] **Step 4: Extend `BuildVectorIndexJob`**

`src/control-plane/core/src/vector_index_job.rs`:

```rust
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct BuildVectorIndexJob {
    pub schema: String,
    pub name: String,
    pub column: String,
    #[serde(default)]
    pub index_kind: Option<String>,
    #[serde(default)]
    pub nlist: Option<u32>,
    #[serde(default)]
    pub m: Option<u32>,
    #[serde(default)]
    pub ef_construction: Option<u32>,
}

impl BuildVectorIndexJob {
    /// Resolve the payload's `(index_kind, nlist, m, ef_construction)` into an `IndexSpec`.
    pub fn index_spec(&self) -> Result<IndexSpec> {
        IndexSpec::from_label(self.index_kind.as_deref(), self.nlist, self.m, self.ef_construction)
    }
}
```

The existing struct-literal in `src/control-plane/core/tests/vector_index_job.rs` (the `build_vector_index_job_serde_roundtrip_and_kind` test, ~line 6) constructs `BuildVectorIndexJob { … index_kind: None, nlist: None }` — add the two new fields:

```rust
    let j = BuildVectorIndexJob {
        schema: "wh".into(),
        name: "docs".into(),
        column: "embedding".into(),
        index_kind: None,
        nlist: None,
        m: None,
        ef_construction: None,
    };
```

- [ ] **Step 5: Add the postgres dispatch arm**

`src/control-plane/postgres/src/vector_index.rs` — extend the import (line ~9, add `HnswIndex`) and the `match index_spec` (lines ~410-415):

```rust
// import line — add HnswIndex:
//   Catalog, ControlPlaneError, DatasetRef, EventType, FlatIndex, HnswIndex, IndexSpec, IvfFlatIndex,

    let index: Box<dyn control_plane_core::VectorIndex> = match index_spec {
        IndexSpec::Flat => Box::new(FlatIndex::build(dim, metric, all_rows)?),
        IndexSpec::IvfFlat { nlist } => {
            Box::new(IvfFlatIndex::build(dim, metric, all_rows, nlist)?)
        }
        IndexSpec::Hnsw { m, ef_construction } => {
            Box::new(HnswIndex::build(dim, metric, all_rows, m, ef_construction)?)
        }
    };
```

- [ ] **Step 6: Fix the two compile-forced call sites (no behavior change yet)**

`src/services/engine/src/service.rs` (~line 183) — the `from_label` call now needs 4 args. HNSW params arrive over the wire in Task 3; pass `None, None` for now:

```rust
        let nlist = if r.nlist == 0 { None } else { Some(r.nlist) };
        // m / ef_construction are threaded from the proto in a later task; default for now.
        let index_spec = IndexSpec::from_label(kind, nlist, None, None).map_err(status)?;
```

`src/services/worker/src/handler.rs` (~line 55) — the destructure must account for the new job fields. Use `..` to ignore them for now (Task 3 consumes them):

```rust
    let BuildVectorIndexJob {
        schema,
        name,
        column,
        index_kind,
        nlist,
        ..
    } = serde_json::from_value(job.payload).map_err(|e| JobFailure {
        error: format!("bad build_vector_index payload: {e}"),
        policy: RetryPolicy::Abandon,
    })?;
```

The `worker/tests/build_vector_index.rs` struct-literal (~line 158) also constructs `BuildVectorIndexJob { … index_kind: None, nlist: None }` — add `m: None, ef_construction: None`:

```rust
        payload: serde_json::to_value(BuildVectorIndexJob {
            schema: schema.into(),
            name: name.into(),
            column: column.into(),
            index_kind: None,
            nlist: None,
            m: None,
            ef_construction: None,
        })
```

(If the field names in that literal differ, match the file; the rule is: every `BuildVectorIndexJob { … }` literal in the tree gains `m` and `ef_construction`.)

- [ ] **Step 7: Build the affected crates + run the unit tests**

Run: `buck2 build //src/control-plane/core:core //src/control-plane/postgres:postgres //src/services/engine:engine //src/services/worker:worker > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|FAILED|error\[" /tmp/b.log`
Expected: BUILD SUCCEEDED.

Run: `buck2 test //src/control-plane/core:vector-index-job //src/control-plane/core:vector-index > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/t.log`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add src/control-plane/core/src/vector_index.rs src/control-plane/core/src/vector_index_job.rs src/control-plane/core/tests/vector_index_job.rs src/control-plane/postgres/src/vector_index.rs src/services/engine/src/service.rs src/services/worker/src/handler.rs src/services/worker/tests/build_vector_index.rs
git commit -m "feat(vector): select HNSW via IndexSpec/BuildVectorIndexJob + primitive dispatch"
```

---

### Task 3: Thread `m`/`ef_construction` over the engine wire

**Files:**
- Modify: `src/services/engine-wire/proto/engine_control.proto`
- Modify: `src/services/engine-wire/src/client.rs`
- Modify: `src/services/engine/src/service.rs`
- Modify: `src/services/worker/src/handler.rs`

**Interfaces:**
- Consumes: `IndexSpec::from_label` (4-arg, Task 2), `BuildVectorIndexJob` `m`/`ef_construction` (Task 2).
- Produces: `EngineWireClient::build_vector_index(schema, name, column, index_kind: Option<String>, nlist: Option<u32>, m: Option<u32>, ef_construction: Option<u32>)` (the exact client type name is whatever `client.rs` already exports; keep it).

- [ ] **Step 1: Add proto fields**

`src/services/engine-wire/proto/engine_control.proto` (line ~50) — extend the message with fields 6 and 7 (append; never renumber existing fields):

```proto
message BuildVectorIndexRequest  { string schema = 1; string name = 2; string column = 3; string index_kind = 4; uint32 nlist = 5; uint32 m = 6; uint32 ef_construction = 7; }
```

- [ ] **Step 2: Thread params in the engine-wire client**

`src/services/engine-wire/src/client.rs` (the `build_vector_index` method, ~line 85) — add the two params and set the new proto fields (`0` sentinel ⇒ `None`, matching the `nlist` convention):

```rust
    /// Build (or rebuild) the vector index for `(schema, name, column)`.
    /// `index_kind` selects the variant: `"ivf_flat"` uses `nlist`; `"hnsw"` uses
    /// `m`/`ef_construction`; `None`/`"flat"` is exact. Returns
    /// `(covered_snapshot, puffin_path, row_count)`.
    pub async fn build_vector_index(
        &self,
        schema: String,
        name: String,
        column: String,
        index_kind: Option<String>,
        nlist: Option<u32>,
        m: Option<u32>,
        ef_construction: Option<u32>,
    ) -> Result<(i64, String, i64)> {
        let resp = self
            .inner
            .clone()
            .build_vector_index(pb::BuildVectorIndexRequest {
                schema,
                name,
                column,
                index_kind: index_kind.unwrap_or_default(),
                nlist: nlist.unwrap_or(0),
                m: m.unwrap_or(0),
                ef_construction: ef_construction.unwrap_or(0),
            })
            .await
            .map_err(be)?
            .into_inner();
        Ok((resp.covered_snapshot, resp.puffin_path, resp.row_count))
    }
```

- [ ] **Step 3: Read the new fields in the engine service**

`src/services/engine/src/service.rs` (the `build_vector_index` RPC handler, ~line 168-183) — decode `m`/`ef_construction` and pass them to the 4-arg `from_label` (replace the `None, None` placeholder from Task 2):

```rust
        let kind = if r.index_kind.is_empty() {
            None
        } else {
            Some(r.index_kind.as_str())
        };
        let nlist = if r.nlist == 0 { None } else { Some(r.nlist) };
        let m = if r.m == 0 { None } else { Some(r.m) };
        let ef_construction = if r.ef_construction == 0 { None } else { Some(r.ef_construction) };
        let index_spec = IndexSpec::from_label(kind, nlist, m, ef_construction).map_err(status)?;
```

- [ ] **Step 4: Thread the job fields in the worker handler**

`src/services/worker/src/handler.rs` (~line 55-66) — destructure `m`/`ef_construction` (replace the `..` from Task 2) and pass them to the client:

```rust
    let BuildVectorIndexJob {
        schema,
        name,
        column,
        index_kind,
        nlist,
        m,
        ef_construction,
    } = serde_json::from_value(job.payload).map_err(|e| JobFailure {
        error: format!("bad build_vector_index payload: {e}"),
        policy: RetryPolicy::Abandon,
    })?;
    client
        .build_vector_index(schema, name, column, index_kind, nlist, m, ef_construction)
        .await
        .map_err(|e| JobFailure {
            error: e.to_string(),
            policy: RetryPolicy::Retry {
                delay: tuning.backoff(job.attempts),
            },
        })?;
    Ok(())
```

- [ ] **Step 5: Build the wire/engine/worker crates**

Run: `buck2 build //src/services/engine-wire:engine-wire //src/services/engine:engine //src/services/worker:worker > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|FAILED|error\[" /tmp/b.log`
Expected: BUILD SUCCEEDED. (The exact `engine-wire` target name is in `src/services/engine-wire/BUCK`; adjust if it differs.)

- [ ] **Step 6: Commit**

```bash
git add src/services/engine-wire/proto/engine_control.proto src/services/engine-wire/src/client.rs src/services/engine/src/service.rs src/services/worker/src/handler.rs
git commit -m "feat(vector): thread HNSW m/ef_construction over the engine wire"
```

---

### Task 4: Fixture coverage — postgres build + worker e2e

**Files:**
- Create: `src/control-plane/postgres/tests/vector_index_hnsw.rs`
- Modify: `src/control-plane/postgres/BUCK`
- Modify: `src/services/worker/tests/build_vector_index.rs`

**Interfaces:**
- Consumes: `build_vector_index` primitive (now handles `IndexSpec::Hnsw`), `read_vector_index`, `lookup_vector_index`, `handle_build_vector_index`.

- [ ] **Step 1: Create the postgres HNSW fixture test**

Create `src/control-plane/postgres/tests/vector_index_hnsw.rs` by copying `tests/vector_index_ivf.rs` verbatim and changing (a) the doc comment, (b) the `IndexSpec` passed to `build_vector_index`, (c) the asserted `index_kind`, (d) the asserted `IndexKind`. The full file:

```rust
//! HNSW build: select IndexSpec::Hnsw, assert the Puffin blob decodes to an hnsw
//! index, the mirror row records index_kind = "hnsw", and a search over the
//! decoded index returns the nearest match.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_array::{Int64Array, RecordBatch};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    ColumnSpec, ControlPlane, DatasetId, EventType, IndexSpec, LineageEvent, Metric, ObjectType,
    PropertyDef, RunId, TableRef, TypeName, VectorKey,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::land;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use control_plane_postgres::puffin::read_vector_index;
use control_plane_postgres::vector_index::{build_vector_index, lookup_vector_index};
use iceberg::CatalogBuilder;
use iceberg::io::{FileIO, LocalFsStorageFactory};

fn columns() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        },
        ColumnSpec {
            name: "embedding".into(),
            ty: "vector(4)".into(),
            nullable: false,
        },
    ]
}

fn ipc_body(rows: &[(i64, [f32; 4])]) -> Vec<u8> {
    let element = Arc::new(Field::new("item", DataType::Float32, false));
    let mut lb = ListBuilder::new(Float32Builder::new()).with_field(element.clone());
    let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
    for (_, emb) in rows {
        lb.values().append_slice(emb);
        lb.append(true);
    }
    let id_array = Int64Array::from(ids);
    let emb_array = lb.finish();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("embedding", DataType::List(element), false),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(id_array), Arc::new(emb_array)])
        .expect("batch");
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &schema).expect("writer");
        w.write(&batch).expect("write");
        w.finish().expect("finish");
    }
    buf
}

fn lineage(run: RunId, table: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(table).dataset_ref()],
        payload: serde_json::json!({ "source": "test" }),
    }
}

async fn make_catalog(dsn: String, warehouse: &str) -> SqlCatalog {
    let mut props = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), dsn);
    props.insert(
        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
        format!("file://{warehouse}"),
    );
    SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("loom", props)
        .await
        .expect("catalog")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hnsw_build_writes_decodable_blob_and_mirror_kind() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let table = TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    };

    cp.ontology()
        .define_type(ObjectType {
            name: TypeName("Docs".into()),
            table: table.clone(),
            properties: vec![
                PropertyDef {
                    name: "id".into(),
                    ty: "Long".into(),
                    required: true,
                },
                PropertyDef {
                    name: "embedding".into(),
                    ty: "Vector".into(),
                    required: true,
                },
            ],
            derived: vec![],
            identity: Some("id".into()),
        })
        .await
        .expect("define_type");

    let run = RunId(uuid::Uuid::new_v4());
    let rows: &[(i64, [f32; 4])] = &[
        (1, [1.0, 0.0, 0.0, 0.0]),
        (2, [0.0, 1.0, 0.0, 0.0]),
        (3, [0.0, 0.0, 1.0, 0.0]),
        (4, [0.0, 0.0, 0.0, 1.0]),
    ];
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        &ipc_body(rows),
        0,
        i64::MAX,
        lineage(run, &table),
    )
    .await
    .expect("land rows");

    // Build with HNSW (defaults: m=16, ef_construction=200).
    let build_run = RunId(uuid::Uuid::new_v4());
    let built = build_vector_index(
        &catalog,
        &pool,
        &table,
        "embedding",
        Metric::Cosine,
        IndexSpec::Hnsw { m: None, ef_construction: None },
        build_run,
    )
    .await
    .expect("build hnsw");
    assert_eq!(built.row_count, 4);

    // Mirror row records the HNSW kind.
    let mut conn = pool.acquire().await.expect("acquire");
    let table_id: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(
        "select table_id from iceberg_mirror.\"table\" \
         where table_namespace = 'wh' and table_name = 'docs' and end_snapshot is null",
    ))
    .fetch_one(&mut *conn)
    .await
    .expect("table_id");
    let found = lookup_vector_index(&pool, table_id, "embedding", built.covered_snapshot)
        .await
        .expect("lookup")
        .expect("Some");
    assert_eq!(found.index_kind, "hnsw");

    // The blob decodes polymorphically to an hnsw index that searches.
    let file_io = FileIO::new_with_fs();
    let idx = read_vector_index(&file_io, &built.puffin_path)
        .await
        .expect("read");
    assert_eq!(idx.index_kind(), control_plane_core::IndexKind::Hnsw);
    assert_eq!(idx.dim(), 4);
    assert_eq!(idx.row_count(), 4);
    let res = idx.search(&[1.0, 0.0, 0.0, 0.0], 1);
    assert_eq!(res[0].0, VectorKey::Int(1));
}
```

- [ ] **Step 2: Wire the new target in the postgres `BUCK`**

In `src/control-plane/postgres/BUCK`, append a target mirroring `vector-index-ivf` exactly (it uses the `loom_fixture_test` macro). Add after the `vector-index-ivf` block:

```python
loom_fixture_test(
    name = "vector-index-hnsw",
    crate = "vector_index_hnsw",
    srcs = ["tests/vector_index_hnsw.rs"],
    crate_root = "tests/vector_index_hnsw.rs",
    deps = [
        ":postgres",
        "//src/control-plane/core:core",
        "//third-party:arrow-array",
        "//third-party:arrow-ipc",
        "//third-party:arrow-schema",
        "//third-party:iceberg",
        "//third-party:serde_json",
        "//third-party:sqlx",
        "//third-party:tempfile",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```

- [ ] **Step 3: Run the postgres fixture test**

Run: `buck2 test //src/control-plane/postgres:vector-index-hnsw > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/t.log`
Expected: PASS (`hnsw_build_writes_decodable_blob_and_mirror_kind`).

- [ ] **Step 4: Extend the worker e2e to drive an HNSW job over the wire**

In `src/services/worker/tests/build_vector_index.rs`, generalize `make_build_vector_index_job` to accept an index kind, then add an HNSW assertion. Replace the helper (~line 154) and add a focused test. First the helper:

```rust
fn make_build_vector_index_job(schema: &str, name: &str, column: &str) -> Job {
    make_build_vector_index_job_kind(schema, name, column, None)
}

fn make_build_vector_index_job_kind(
    schema: &str,
    name: &str,
    column: &str,
    index_kind: Option<&str>,
) -> Job {
    Job {
        // ... keep the existing Job fields from the original helper ...
        payload: serde_json::to_value(BuildVectorIndexJob {
            schema: schema.into(),
            name: name.into(),
            column: column.into(),
            index_kind: index_kind.map(Into::into),
            nlist: None,
            m: None,
            ef_construction: None,
        })
        .expect("payload"),
        // ... remaining Job fields unchanged ...
    }
}
```

(Copy the surrounding `Job { … }` field initialization from the existing `make_build_vector_index_job` body — only the `payload` and the new `index_kind` arg change.)

Then, in the existing `worker_builds_vector_index_over_the_wire` test, after it asserts the default (flat) mirror, add a second build driven with `Some("hnsw")` and assert the mirror kind. The minimal addition (place near the end of the test, reusing the already-seeded table):

```rust
    // Build again as HNSW over the wire; the mirror records index_kind = "hnsw".
    handle_build_vector_index(
        client.clone(),
        tuning.clone(),
        make_build_vector_index_job_kind("main", "vectors", "embedding", Some("hnsw")),
    )
    .await
    .expect("handle_build_vector_index hnsw");

    let hnsw_row = lookup_vector_index(&pool, table_id, "embedding", snap.id.0)
        .await
        .expect("lookup_vector_index hnsw")
        .expect("Some");
    assert_eq!(hnsw_row.index_kind, "hnsw");
```

If `client`/`tuning`/`table_id`/`snap` are not already bound at that point in the test, reuse the exact bindings the existing assertions use (the test already looks up `table_id` and `snap` to read the flat mirror row — extend from there). Keep `client.clone()`/`tuning.clone()` only if those values are not `Copy`.

- [ ] **Step 5: Run the worker e2e**

Run: `buck2 test //src/services/worker:build-vector-index > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/t.log`
(The exact target name is in `src/services/worker/BUCK` — find it with `grep -n build_vector_index src/services/worker/BUCK`.)
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/control-plane/postgres/tests/vector_index_hnsw.rs src/control-plane/postgres/BUCK src/services/worker/tests/build_vector_index.rs
git commit -m "test(vector): HNSW postgres fixture build + worker e2e over the wire"
```

---

### Task 5: Engine-serving freshness fixture test (cold HNSW ∪ exact hot delta)

**Files:**
- Modify: `src/services/engine-serving/tests/vector_search.rs`

**Interfaces:**
- Consumes: the existing engine-serving `vector_search` path + `build_vector_index` primitive (HNSW-capable). No production change — this proves the freshness invariant holds with an HNSW cold index.

- [ ] **Step 1: Read the existing merge test to mirror it**

Read `src/services/engine-serving/tests/vector_search.rs` end to end. It already has a cold∪hot merge test (`knn_cold_hot_merge_*`) that: lands rows up to snapshot S, builds the index over S, lands a *new* inline row in (S, Q], runs `vector_search`, and asserts the new row is found and counted exactly once. Identify the helper that builds the cold index (it calls `build_vector_index` with an `IndexSpec`) and the query/assert helper.

- [ ] **Step 2: Add an HNSW freshness test**

Append a test that mirrors the existing `knn_cold_hot_merge` flow but builds the cold index with `IndexSpec::Hnsw { m: None, ef_construction: None }` instead of `Flat`/`IvfFlat`. The assertions: (a) a row landed inline *after* the covered snapshot S appears in the result exactly once (cold ∪ hot dedup holds), and (b) that hot row is returned even though it is not in the cold HNSW graph (proving the hot path bypasses graph pruning). Use the exact same seed/land/query helpers the existing test uses — only the `IndexSpec` argument changes. Skeleton (fill the helper names from Step 1):

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hnsw_cold_hot_merge_counts_fresh_row_once_cosine() {
    // ... identical setup to knn_cold_hot_merge_cosine: fixture, catalog, define_type,
    //     land the cold rows, capture covered snapshot S ...

    // Build the COLD index as HNSW (the only change from the flat/ivf merge test).
    let built = build_vector_index(
        &catalog,
        &pool,
        &table,
        "embedding",
        Metric::Cosine,
        control_plane_core::IndexSpec::Hnsw { m: None, ef_construction: None },
        RunId(uuid::Uuid::new_v4()),
    )
    .await
    .expect("build hnsw cold");

    // ... land a NEW inline row in (S, Q] that is a true top-k neighbor of the query ...
    // ... run vector_search at Q ...

    // The fresh inline row is present exactly once across cold ∪ hot.
    let ids: Vec<i64> = result_ids(&hits); // reuse the test's id extractor
    let fresh = /* the inline row's id */;
    assert_eq!(ids.iter().filter(|&&x| x == fresh).count(), 1, "fresh row counted once");
    assert!(ids.contains(&fresh), "fresh hot row returned despite cold HNSW pruning");
}
```

Add an L2 variant (`hnsw_cold_hot_merge_counts_fresh_row_once_l2`) the same way if the existing test has both Cosine and L2 variants — mirror whichever metrics the existing `knn_cold_hot_merge_*` tests cover.

No `BUCK` change is needed — these tests live in the already-wired `vector-search` target.

- [ ] **Step 3: Run the engine-serving test**

Run: `buck2 test //src/services/engine-serving:vector-search > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/t.log`
Expected: PASS (new `hnsw_cold_hot_merge_*` tests + existing tests still green).

- [ ] **Step 4: Commit**

```bash
git add src/services/engine-serving/tests/vector_search.rs
git commit -m "test(vector): engine HNSW cold-hot freshness merge fixture"
```

---

## Final Verification (run after Task 5, before the PR)

- [ ] **Full sweep:** `buck2 build //src/... > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|FAILED" /tmp/b.log` then `buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`. Both green. (Fixture tests can flake under cloud resource contention — re-run a clean sweep to confirm any failure is real, per the pre-push-flakes note.)
- [ ] **Clippy:** `./tools/clippy-all.sh 2>&1 | tail -5` — clean.
- [ ] **Determinism spot-check is covered** by `hnsw_build_is_byte_deterministic` (Task 1) — confirm it ran.
- [ ] **Docs register:** run `loom-docs-update` to close `road-hnsw-vector-index` (`- [ ]` → `- [x]`, terminal status, add `pr:#N`) in the same PR.

## Self-Review Notes (plan author)

- **Spec coverage:** `IndexKind::Hnsw` (T1) · `HnswIndex` struct/build/with_ef_search/serialize/deserialize/search (T1) · `decode` `2 =>` arm (T1) · lib export (T1) · `IndexSpec::Hnsw` + 4-arg `from_label` (T2) · `BuildVectorIndexJob` `m`/`ef_construction` + `index_spec` (T2) · postgres dispatch (T2) · proto/client/service/worker threading (T3) · core recall/determinism/round-trip/decode/edges tests (T1) · postgres fixture (T4) · worker e2e (T4) · engine freshness incl. "hot row returned despite cold pruning" (T5). `engine-serving/src/vector_search.rs` is intentionally unchanged (spec §Components item 5).
- **Cosine handling:** the implementation uses `distance(metric, …)` uniformly (which computes true cosine distance via internal normalization) rather than a separate normalization pass; `data` is stored raw exactly as `FlatIndex`. This is functionally equivalent to the spec's "L2-normalize during construction" for ranking purposes and keeps `data` consistent with the exact path. Flagged here so the spec-compliance reviewer accepts the equivalence rather than reading it as a gap.
- **Level cap:** `HNSW_MAX_LEVEL = 64` guarantees `node_max_layer` fits the serialized `u8` even for a pathologically small level-draw `u` (not explicit in the spec, but required for the spec's `u8 node_max_layer` field to be safe). Documented in code.
- **Type consistency:** `from_label` is 4-arg everywhere (T2 changes the def + all callers in the same commit). `BuildVectorIndexJob` struct literals gain `m`/`ef_construction` in every test in the tree (T2). `cmp_dist`/`hnsw_search_layer`/`hnsw_select_neighbors` signatures are fixed in T1 and reused unchanged by `search`.
```
