# IVF-Flat Approximate Vector Index Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a hand-rolled IVF-Flat approximate vector index as a second `VectorIndex` implementation behind the existing trait, selectable at build time and decoded polymorphically at serve time, with the cold-only-approximation freshness guarantee.

**Architecture:** `IvfFlatIndex` lives in `control-plane-core` (pure: no iceberg/arrow/object-store/third-party deps; hand-rolled deterministic k-means). The `VectorIndex` trait gains `index_kind()`, `row_count()`, and `serialize()` so the Puffin write/read path and the engine serving path become polymorphic over `Box<dyn VectorIndex>`. The build primitive picks Flat vs IVF from an `IndexSpec`; IVF covers the **cold** tier only — the inline hot delta keeps its exact brute-force scoring and the existing `merge_topk` seam, so freshly-landed rows are never dropped by cluster pruning.

**Tech Stack:** Rust (edition 2024), buck2 build, `arrow`/`iceberg` (postgres + serving layers only), Apache Puffin sidecars, hermetic-Postgres fixture tests.

## Global Constraints

- **Tests are `rust_test` integration targets only — NEVER inline `#[cfg(test)]`.** Each test file is a sibling `tests/<name>.rs` wired as its own target in the crate `BUCK`. The `no-inline-tests` prek hook fails the build on any `#[test]`/`#[tokio::test]` inside `src/**`.
- **Fixture (hermetic-Postgres) tests use `loom_fixture_test`, not bare `rust_test`** (it pins the test to local execution + injects the test lint allows). Pure-logic tests use `rust_test` (already loaded in each BUCK via `load("//src:loom_test.bzl", "rust_test")`).
- **Clippy is strict (pedantic + restriction enforced).** `clippy::indexing_slicing` and `clippy::needless_range_loop` are ACTIVE on production `src/**` code. The numeric kernels in this plan use stride indexing; confine raw indexing to the `row_slice`/`row_slice_mut` helpers and annotate them with `#[expect(clippy::indexing_slicing, reason = "...")]` (mirroring `FlatIndex::row` at `core/src/vector_index.rs:105`). Where a `for i in 0..n` stride loop over parallel arrays is unavoidable, add `#[expect(clippy::needless_range_loop, reason = "...")]`. Every `#[expect]`/`#[allow]` MUST carry a `reason`. Test code is exempt from the panic-safety lints via the wrapper, so `.unwrap()`/`.expect()` in tests are fine.
- **Run tests with buck2, never piped through `tail`/`head`:** `buck2 test //path:target > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/t.log`.
- **Blob format:** the on-disk Puffin blob `type` string stays `loom-vector-index-v1` (`postgres/src/puffin.rs:14`). The Flat-vs-IVF discriminator is the **`kind` byte inside the payload** (offset 6: after `magic[4]` + `version` + `metric`), NOT the blob-type string. `version` stays `1`.
- **Default index kind is `Flat`.** Absent job/proto fields ⇒ `IndexSpec::Flat` ⇒ today's exact behavior (non-breaking). IVF is opt-in.
- **Determinism:** k-means uses a fixed seed constant and a fixed iteration cap so a build over a given input row order yields byte-identical serialization (same-process / same-platform).
- **Spec:** `docs/superpowers/specs/2026-06-28-ivf-vector-index-design.md`.

---

### Task 1: Extend the `VectorIndex` trait + `IndexKind::IvfFlat`

Make the trait polymorphic-write-ready and add the IVF kind, **without** `IvfFlatIndex` yet. This is the trait-shape change everything else builds on.

**Files:**
- Modify: `src/control-plane/core/src/vector_index.rs` (trait at lines 60-65; `IndexKind` at 37-48; `impl VectorIndex for FlatIndex` at 271-290)
- Test: `src/control-plane/core/tests/vector_index.rs` (existing target `//src/control-plane/core:vector-index`)

**Interfaces:**
- Produces:
  - `IndexKind::IvfFlat` with `as_str()` → `"ivf_flat"` and a new `IndexKind::from_str(&str) -> Option<IndexKind>` (covers `"flat"` and `"ivf_flat"`).
  - `VectorIndex` trait now also requires: `fn index_kind(&self) -> IndexKind;`, `fn row_count(&self) -> u32;`, `fn serialize(&self) -> Vec<u8>;`.
  - `FlatIndex` implements all three (kind → `Flat`, the other two delegate to existing inherent methods).

- [ ] **Step 1: Write the failing test**

Append to `src/control-plane/core/tests/vector_index.rs`:

```rust
#[test]
fn index_kind_string_roundtrip() {
    use control_plane_core::IndexKind;
    assert_eq!(IndexKind::Flat.as_str(), "flat");
    assert_eq!(IndexKind::IvfFlat.as_str(), "ivf_flat");
    assert_eq!(IndexKind::from_str("flat"), Some(IndexKind::Flat));
    assert_eq!(IndexKind::from_str("ivf_flat"), Some(IndexKind::IvfFlat));
    assert_eq!(IndexKind::from_str("nope"), None);
}

#[test]
fn flat_index_reports_kind_and_serializes_via_trait() {
    // Exercise the new trait methods through a trait object.
    let idx = FlatIndex::build(4, Metric::L2, rows()).unwrap();
    let dynidx: &dyn VectorIndex = &idx;
    assert_eq!(dynidx.index_kind(), control_plane_core::IndexKind::Flat);
    assert_eq!(dynidx.row_count(), 3);
    // Trait serialize must equal the inherent serialize (same bytes).
    assert_eq!(dynidx.serialize(), idx.serialize());
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `buck2 test //src/control-plane/core:vector-index > /tmp/t.log 2>&1; grep -E "error\[|cannot find|FAIL|Tests finished" /tmp/t.log`
Expected: compile error — `IndexKind::IvfFlat` / `from_str` / trait methods not found.

- [ ] **Step 3: Add `IndexKind::IvfFlat` + `from_str`**

In `src/control-plane/core/src/vector_index.rs`, replace the `IndexKind` enum and impl (lines 35-48) with:

```rust
/// The index algorithm family.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexKind {
    Flat,
    IvfFlat,
}

impl IndexKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            IndexKind::Flat => "flat",
            IndexKind::IvfFlat => "ivf_flat",
        }
    }

    #[must_use]
    pub fn from_str(s: &str) -> Option<IndexKind> {
        match s {
            "flat" => Some(IndexKind::Flat),
            "ivf_flat" => Some(IndexKind::IvfFlat),
            _ => None,
        }
    }
}
```

- [ ] **Step 4: Add the three trait methods + FlatIndex conformance**

Replace the `VectorIndex` trait (lines 59-65) with:

```rust
/// Exact-or-approximate top-k nearest-neighbour index.
pub trait VectorIndex {
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
}
```

In `impl VectorIndex for FlatIndex` (currently lines 271-290), add the three methods alongside the existing `metric`/`dim`/`search`:

```rust
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
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `buck2 test //src/control-plane/core:vector-index //src/control-plane/core:vector-index-job > /tmp/t.log 2>&1; grep -E "FAIL|Tests finished" /tmp/t.log`
Expected: PASS. Then confirm no other crate broke by the trait change:
Run: `buck2 build //src/control-plane/postgres:postgres //src/services/engine-serving:engine-serving > /tmp/b.log 2>&1; grep -E "error|BUILD SUCCEEDED|Build ID" /tmp/b.log`
Expected: builds clean (FlatIndex still satisfies the trait; puffin.rs's `index.metric()`/`index.dim()` calls unaffected).

- [ ] **Step 6: Commit**

```bash
git add src/control-plane/core/src/vector_index.rs src/control-plane/core/tests/vector_index.rs
git commit -m "feat(core): extend VectorIndex trait (index_kind/row_count/serialize) + IndexKind::IvfFlat"
```

---

### Task 2: `IvfFlatIndex` — struct, deterministic k-means build, search

The core algorithm. Pure, in `control-plane-core`. Serialization is Task 3; this task makes the index buildable and searchable in memory, with the exactness oracle (`nprobe = nlist` ⇒ identical to `FlatIndex`).

**Files:**
- Modify: `src/control-plane/core/src/vector_index.rs` (add after the `FlatIndex` block + its `impl VectorIndex`, before `fn bad` if you like — anywhere in the file is fine; keep `Cursor`/`bad`/`distance` reachable)
- Modify: `src/control-plane/core/src/lib.rs:45` (export `IvfFlatIndex`)
- Test: `src/control-plane/core/tests/vector_index.rs`

**Interfaces:**
- Consumes: `Metric`, `VectorKey`, `IndexKind` (Task 1), `distance()` (`core/src/vector_index.rs:239`).
- Produces:
  - `pub struct IvfFlatIndex { dim: u32, metric: Metric, nlist: u32, nprobe: u32, centroids: Vec<f32>, assignments: Vec<u32>, keys: Vec<VectorKey>, data: Vec<f32> }`
  - `IvfFlatIndex::build(dim: u32, metric: Metric, rows: Vec<(VectorKey, Vec<f32>)>, nlist: Option<u32>) -> Result<IvfFlatIndex>`
  - `IvfFlatIndex::with_nprobe(self, nprobe: u32) -> IvfFlatIndex` (clamps to `[1, nlist]`; no-op when `nlist == 0`)
  - `impl VectorIndex for IvfFlatIndex` (`metric`/`dim`/`index_kind`→`IvfFlat`/`row_count`/`search`; `serialize` added in Task 3 — see note in Step 4)

- [ ] **Step 1: Write the failing tests**

Append to `src/control-plane/core/tests/vector_index.rs`:

```rust
use control_plane_core::IvfFlatIndex;

// A small deterministic generator for clustered test data (LCG — test-local).
struct Lcg(u64);
impl Lcg {
    fn next_f32(&mut self) -> f32 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        ((self.0 >> 33) as f32 / (1u64 << 31) as f32) - 1.0 // in [-1, 1)
    }
}

/// 10 clusters × 20 points in 8-dim, each point = center + small jitter.
fn clustered_rows() -> (Vec<(VectorKey, Vec<f32>)>, Vec<Vec<f32>>) {
    let d = 8;
    let mut rng = Lcg(0x1234_5678);
    let mut centers = Vec::new();
    for _ in 0..10 {
        centers.push((0..d).map(|_| rng.next_f32() * 10.0).collect::<Vec<f32>>());
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
    (rows, centers)
}

#[test]
fn ivf_nprobe_equals_nlist_is_exact_oracle_cosine() {
    let (rows, _) = clustered_rows();
    let flat = FlatIndex::build(8, Metric::Cosine, rows.clone()).unwrap();
    let ivf = IvfFlatIndex::build(8, Metric::Cosine, rows, None).unwrap();
    let n = ivf.dim(); // sanity
    assert_eq!(n, 8);
    // Probe every cluster -> IVF scores all rows -> identical to exact flat.
    let ivf_all = ivf.with_nprobe(u32::MAX); // clamps to nlist
    let q = vec![1.0f32, 2.0, 3.0, 4.0, 0.0, 0.0, 0.0, 0.0];
    assert_eq!(ivf_all.search(&q, 10), flat.search(&q, 10));
}

#[test]
fn ivf_nprobe_equals_nlist_is_exact_oracle_l2() {
    let (rows, _) = clustered_rows();
    let flat = FlatIndex::build(8, Metric::L2, rows.clone()).unwrap();
    let ivf = IvfFlatIndex::build(8, Metric::L2, rows, None).unwrap().with_nprobe(u32::MAX);
    let q = vec![0.0f32, 0.0, 0.0, 0.0, 1.0, 2.0, 3.0, 4.0];
    assert_eq!(ivf.search(&q, 10), flat.search(&q, 10));
}

#[test]
fn ivf_recall_meets_threshold_with_small_nprobe() {
    let (rows, centers) = clustered_rows();
    let flat = FlatIndex::build(8, Metric::L2, rows.clone()).unwrap();
    let ivf = IvfFlatIndex::build(8, Metric::L2, rows, None).unwrap().with_nprobe(3);
    // Query each cluster center; measure top-10 recall vs exact.
    let mut hits = 0usize;
    let mut total = 0usize;
    for c in &centers {
        let exact: std::collections::HashSet<_> =
            flat.search(c, 10).into_iter().map(|(k, _)| k).collect();
        let approx: std::collections::HashSet<_> =
            ivf.search(c, 10).into_iter().map(|(k, _)| k).collect();
        hits += exact.intersection(&approx).count();
        total += exact.len();
    }
    let recall = hits as f32 / total as f32;
    assert!(recall >= 0.9, "recall {recall} below 0.9");
}

#[test]
fn ivf_empty_is_searchable() {
    let ivf = IvfFlatIndex::build(4, Metric::Cosine, vec![], None).unwrap();
    assert_eq!(ivf.row_count(), 0);
    assert_eq!(ivf.search(&[1.0, 0.0, 0.0, 0.0], 5), vec![]);
}

#[test]
fn ivf_fewer_rows_than_nlist_clamps() {
    let rows = vec![
        (VectorKey::Int(1), vec![1.0, 0.0]),
        (VectorKey::Int(2), vec![0.0, 1.0]),
    ];
    // Request 16 clusters but only 2 rows: must clamp, build, and search exactly.
    let ivf = IvfFlatIndex::build(2, Metric::L2, rows, Some(16)).unwrap().with_nprobe(u32::MAX);
    let res = ivf.search(&[1.0, 0.0], 1);
    assert_eq!(res[0].0, VectorKey::Int(1));
}

#[test]
fn ivf_build_rejects_dim_mismatch() {
    let rows = vec![(VectorKey::Int(1), vec![1.0, 0.0, 0.0])];
    assert!(IvfFlatIndex::build(4, Metric::Cosine, rows, None).is_err());
}
```

- [ ] **Step 2: Run to verify failure**

Run: `buck2 test //src/control-plane/core:vector-index > /tmp/t.log 2>&1; grep -E "cannot find|error\[|FAIL" /tmp/t.log`
Expected: `cannot find type IvfFlatIndex`.

- [ ] **Step 3: Implement `IvfFlatIndex` + deterministic k-means**

Add to `src/control-plane/core/src/vector_index.rs` (constants near the top of the file, the rest below the `FlatIndex` impls). Note `bad`, `Cursor`, and `distance` already exist in this module and are reused.

```rust
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

    /// Override the query-time probe count (clamped to `[1, nlist]`). No-op when empty.
    #[must_use]
    pub fn with_nprobe(mut self, nprobe: u32) -> IvfFlatIndex {
        if self.nlist > 0 {
            self.nprobe = nprobe.clamp(1, self.nlist);
        }
        self
    }

    fn centroid(&self, c: usize) -> &[f32] {
        row_slice(&self.centroids, self.dim as usize, c)
    }
    fn row(&self, i: usize) -> &[f32] {
        row_slice(&self.data, self.dim as usize, i)
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
            .filter(|&i| probe.contains(&self.assignments[i]))
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
```

Note the `search` filter uses `self.assignments[i]` — that is `clippy::indexing_slicing`. Add `#[expect(clippy::indexing_slicing, reason = "i < keys.len() == assignments.len()")]` on the `search` method, or rewrite the `.filter` to `self.assignments.get(i).is_some_and(|c| probe.contains(c))`. **Prefer the `.get()` rewrite** (no attribute needed):

```rust
            .filter(|&i| self.assignments.get(i).is_some_and(|c| probe.contains(c)))
```

For Step 3 to compile before Task 3, add a temporary `serialize_bytes` stub that Task 3 replaces:

```rust
impl IvfFlatIndex {
    fn serialize_bytes(&self) -> Vec<u8> {
        // Replaced with the real format in Task 3.
        unimplemented!("IvfFlatIndex serialization lands in Task 3")
    }
}
```

- [ ] **Step 4: Export and build**

In `src/control-plane/core/src/lib.rs:45`, add `IvfFlatIndex` to the `vector_index` re-export:

```rust
pub use vector_index::{
    FlatIndex, IndexKind, IvfFlatIndex, Metric, VectorIndex, VectorKey, distance,
};
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `buck2 test //src/control-plane/core:vector-index > /tmp/t.log 2>&1; grep -E "FAIL|Tests finished" /tmp/t.log`
Expected: PASS (the 6 new IVF tests + all existing). The `serialize_bytes` stub is never hit by these tests.

- [ ] **Step 6: Commit**

```bash
git add src/control-plane/core/src/vector_index.rs src/control-plane/core/src/lib.rs src/control-plane/core/tests/vector_index.rs
git commit -m "feat(core): IvfFlatIndex — deterministic k-means build + nprobe search"
```

---

### Task 3: IVF serialization (`kind=1`) + `decode` dispatcher

Replace the `serialize_bytes` stub with the real format, add `deserialize`, and add the free `decode(bytes) -> Box<dyn VectorIndex>` that routes on the `kind` byte.

**Files:**
- Modify: `src/control-plane/core/src/vector_index.rs`
- Modify: `src/control-plane/core/src/lib.rs:45` (export `decode`)
- Test: `src/control-plane/core/tests/vector_index.rs`

**Interfaces:**
- Produces:
  - `IvfFlatIndex::serialize` (real, `kind=1`) + `IvfFlatIndex::deserialize(bytes: &[u8]) -> Result<IvfFlatIndex>`
  - `pub fn decode(bytes: &[u8]) -> Result<Box<dyn VectorIndex>>`

- [ ] **Step 1: Write the failing tests**

Append to `src/control-plane/core/tests/vector_index.rs`:

```rust
#[test]
fn ivf_serialize_roundtrip_is_search_exact() {
    let (rows, _) = clustered_rows();
    let ivf = IvfFlatIndex::build(8, Metric::Cosine, rows, None).unwrap().with_nprobe(3);
    let bytes = ivf.serialize();
    let back = IvfFlatIndex::deserialize(&bytes).unwrap();
    let q = vec![1.0f32, 2.0, 3.0, 4.0, 0.0, 0.0, 0.0, 0.0];
    assert_eq!(ivf.search(&q, 10), back.search(&q, 10));
}

#[test]
fn ivf_build_is_byte_deterministic() {
    let (rows, _) = clustered_rows();
    let a = IvfFlatIndex::build(8, Metric::L2, rows.clone(), None).unwrap();
    let b = IvfFlatIndex::build(8, Metric::L2, rows, None).unwrap();
    assert_eq!(a.serialize(), b.serialize(), "same input order -> identical bytes");
}

#[test]
fn decode_routes_on_kind_byte() {
    use control_plane_core::{decode, IndexKind};
    let (rows, _) = clustered_rows();
    let flat = FlatIndex::build(8, Metric::L2, rows.clone()).unwrap();
    let ivf = IvfFlatIndex::build(8, Metric::L2, rows, None).unwrap();

    let dflat = decode(&flat.serialize()).unwrap();
    assert_eq!(dflat.index_kind(), IndexKind::Flat);
    let divf = decode(&ivf.serialize()).unwrap();
    assert_eq!(divf.index_kind(), IndexKind::IvfFlat);

    // Boxed trait object searches; the default nprobe recorded in the blob is used.
    let q = vec![0.0f32, 0.0, 0.0, 0.0, 1.0, 2.0, 3.0, 4.0];
    assert_eq!(divf.search(&q, 5).len(), 5);
}
```

- [ ] **Step 2: Run to verify failure**

Run: `buck2 test //src/control-plane/core:vector-index > /tmp/t.log 2>&1; grep -E "cannot find|unimplemented|error\[|FAIL" /tmp/t.log`
Expected: `cannot find function decode` / `deserialize` not found.

- [ ] **Step 3: Implement IVF serialize/deserialize + `decode`**

Replace the temporary `serialize_bytes` stub (from Task 2) and add `deserialize`. The IVF block sits next to the format comment for Flat:

```rust
impl IvfFlatIndex {
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
        let mut centroids = Vec::with_capacity(nlist as usize * d);
        for _ in 0..(nlist as usize * d) {
            centroids.push(c.f32()?);
        }
        let mut assignments = Vec::with_capacity(row_count as usize);
        for _ in 0..row_count {
            assignments.push(c.u32()?);
        }
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

/// Decode any serialized loom vector index into a boxed `VectorIndex`, routing on
/// the `kind` byte (offset 6: magic[4] + version + metric). Used by the engine
/// serving + postgres read paths.
pub fn decode(bytes: &[u8]) -> Result<Box<dyn VectorIndex>> {
    let kind = *bytes.get(6).ok_or_else(|| bad("truncated index header"))?;
    match kind {
        0 => Ok(Box::new(FlatIndex::deserialize(bytes)?)),
        1 => Ok(Box::new(IvfFlatIndex::deserialize(bytes)?)),
        _ => Err(bad("unknown index kind")),
    }
}
```

Change the `serialize` trait method body in `impl VectorIndex for IvfFlatIndex` from the stub call to delegate (it already calls `self.serialize_bytes()`, which is now real — no change needed). The `c.u32()` cursor helper already exists; `Cursor` reads `u32` LE (lines 219-223). Confirm `u32` assignments decode via the existing `c.u32()`.

- [ ] **Step 4: Export `decode`**

`src/control-plane/core/src/lib.rs:45`:

```rust
pub use vector_index::{
    FlatIndex, IndexKind, IvfFlatIndex, Metric, VectorIndex, VectorKey, decode, distance,
};
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `buck2 test //src/control-plane/core:vector-index > /tmp/t.log 2>&1; grep -E "FAIL|Tests finished" /tmp/t.log`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/control-plane/core/src/vector_index.rs src/control-plane/core/src/lib.rs src/control-plane/core/tests/vector_index.rs
git commit -m "feat(core): IVF serialization (kind=1) + decode() trait-object dispatcher"
```

---

### Task 4: `IndexSpec` + `BuildVectorIndexJob` IVF fields

A build-time index selector in `core`, plus the two optional job-payload fields and a mapping helper. Pure-logic; no postgres/proto yet.

**Files:**
- Modify: `src/control-plane/core/src/vector_index.rs` (add `IndexSpec`)
- Modify: `src/control-plane/core/src/vector_index_job.rs`
- Modify: `src/control-plane/core/src/lib.rs:45-46` (exports)
- Test: `src/control-plane/core/tests/vector_index_job.rs`

**Interfaces:**
- Produces:
  - `pub enum IndexSpec { Flat, IvfFlat { nlist: Option<u32> } }`
  - `IndexSpec::from_label(kind: Option<&str>, nlist: Option<u32>) -> Result<IndexSpec>` (None/`"flat"` → Flat; `"ivf_flat"` → IvfFlat; unknown → Err)
  - `BuildVectorIndexJob { schema, name, column, index_kind: Option<String>, nlist: Option<u32> }` (both new fields `#[serde(default)]`)
  - `BuildVectorIndexJob::index_spec(&self) -> Result<IndexSpec>`

- [ ] **Step 1: Write the failing tests**

Append to `src/control-plane/core/tests/vector_index_job.rs`:

```rust
#[test]
fn legacy_payload_without_index_fields_deserializes_to_flat() {
    use control_plane_core::IndexSpec;
    // A payload written before IVF existed (no index_kind / nlist keys).
    let v = serde_json::json!({ "schema": "wh", "name": "docs", "column": "embedding" });
    let job: BuildVectorIndexJob = serde_json::from_value(v).unwrap();
    assert_eq!(job.index_kind, None);
    assert_eq!(job.nlist, None);
    assert!(matches!(job.index_spec().unwrap(), IndexSpec::Flat));
}

#[test]
fn ivf_payload_maps_to_ivf_spec() {
    use control_plane_core::IndexSpec;
    let v = serde_json::json!({
        "schema": "wh", "name": "docs", "column": "embedding",
        "index_kind": "ivf_flat", "nlist": 32
    });
    let job: BuildVectorIndexJob = serde_json::from_value(v).unwrap();
    match job.index_spec().unwrap() {
        IndexSpec::IvfFlat { nlist } => assert_eq!(nlist, Some(32)),
        other => panic!("expected IvfFlat, got {other:?}"),
    }
}

#[test]
fn unknown_index_kind_is_error() {
    let v = serde_json::json!({
        "schema": "wh", "name": "docs", "column": "embedding", "index_kind": "hnsw"
    });
    let job: BuildVectorIndexJob = serde_json::from_value(v).unwrap();
    assert!(job.index_spec().is_err());
}

#[test]
fn index_spec_from_label_table() {
    use control_plane_core::IndexSpec;
    assert!(matches!(IndexSpec::from_label(None, None).unwrap(), IndexSpec::Flat));
    assert!(matches!(IndexSpec::from_label(Some("flat"), None).unwrap(), IndexSpec::Flat));
    assert!(matches!(
        IndexSpec::from_label(Some("ivf_flat"), Some(8)).unwrap(),
        IndexSpec::IvfFlat { nlist: Some(8) }
    ));
    assert!(IndexSpec::from_label(Some("bogus"), None).is_err());
}
```

Also update the existing `build_vector_index_job_serde_roundtrip_and_kind` test's struct literal (it constructs `BuildVectorIndexJob { schema, name, column }`) to add the two new fields:

```rust
    let j = BuildVectorIndexJob {
        schema: "wh".into(),
        name: "docs".into(),
        column: "embedding".into(),
        index_kind: None,
        nlist: None,
    };
```

- [ ] **Step 2: Run to verify failure**

Run: `buck2 test //src/control-plane/core:vector-index-job > /tmp/t.log 2>&1; grep -E "cannot find|missing field|error\[|FAIL" /tmp/t.log`
Expected: `cannot find type IndexSpec` and/or missing-field errors.

- [ ] **Step 3: Add `IndexSpec` to `vector_index.rs`**

```rust
/// Which index to build, chosen at build time. `Flat` is the default (exact);
/// `IvfFlat` is the approximate IVF index with an optional `nlist` override.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IndexSpec {
    Flat,
    IvfFlat { nlist: Option<u32> },
}

impl IndexSpec {
    /// Map a `(kind, nlist)` pair (e.g. from a job payload or RPC request) to a
    /// spec. `None` or `"flat"` → `Flat`; `"ivf_flat"` → `IvfFlat`; else error.
    pub fn from_label(kind: Option<&str>, nlist: Option<u32>) -> Result<IndexSpec> {
        match kind {
            None | Some("flat") => Ok(IndexSpec::Flat),
            Some("ivf_flat") => Ok(IndexSpec::IvfFlat { nlist }),
            Some(other) => Err(ControlPlaneError::Backend(
                format!("unknown index kind '{other}'").into(),
            )),
        }
    }
}
```

- [ ] **Step 4: Add the job fields + helper**

Replace `src/control-plane/core/src/vector_index_job.rs` body (the struct) with:

```rust
use crate::vector_index::IndexSpec;
use crate::error::Result;

/// The queue `kind` for a vector-index build. Protocol invariant, not a tunable.
pub const BUILD_VECTOR_INDEX_JOB_KIND: &str = "build_vector_index";

/// Payload of a `build_vector_index` job. `index_kind`/`nlist` are optional and
/// default to absent (⇒ exact `Flat`) so payloads written before IVF existed
/// still deserialize.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct BuildVectorIndexJob {
    pub schema: String,
    pub name: String,
    pub column: String,
    #[serde(default)]
    pub index_kind: Option<String>,
    #[serde(default)]
    pub nlist: Option<u32>,
}

impl BuildVectorIndexJob {
    /// Resolve the payload's `(index_kind, nlist)` into an `IndexSpec`.
    pub fn index_spec(&self) -> Result<IndexSpec> {
        IndexSpec::from_label(self.index_kind.as_deref(), self.nlist)
    }
}
```

Check the existing module doc comment / imports at the top of the file are preserved (lines 1-3). Confirm `crate::error::Result` is the right path (the crate's `Result` alias — `FlatIndex::build` uses `crate::error::Result` per `vector_index.rs:6`).

- [ ] **Step 5: Export + run**

`src/control-plane/core/src/lib.rs:45`: add `IndexSpec`:

```rust
pub use vector_index::{
    FlatIndex, IndexKind, IndexSpec, IvfFlatIndex, Metric, VectorIndex, VectorKey, decode, distance,
};
```

Run: `buck2 test //src/control-plane/core:vector-index-job //src/control-plane/core:vector-index > /tmp/t.log 2>&1; grep -E "FAIL|Tests finished" /tmp/t.log`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/control-plane/core/src/vector_index.rs src/control-plane/core/src/vector_index_job.rs src/control-plane/core/src/lib.rs src/control-plane/core/tests/vector_index_job.rs
git commit -m "feat(core): IndexSpec + BuildVectorIndexJob index_kind/nlist fields"
```

---

### Task 5: Postgres — polymorphic Puffin write/read + `build_vector_index(index_spec)`

Generalize the Puffin helpers to any `VectorIndex`, and thread `IndexSpec` through the build primitive so it can build + persist an IVF index.

**Files:**
- Modify: `src/control-plane/postgres/src/puffin.rs`
- Modify: `src/control-plane/postgres/src/vector_index.rs` (`build_vector_index` at line 344; the index-build at step 7, line ~408; the puffin write at step 9, line ~434; the mirror-row insert at step 10, line ~484)
- Modify: `src/control-plane/postgres/tests/vector_index_build.rs` (existing fixture target)
- Test (new): `src/control-plane/postgres/tests/vector_index_ivf.rs` + a `loom_fixture_test` target in `src/control-plane/postgres/BUCK`

**Interfaces:**
- Consumes: `IndexSpec`, `IvfFlatIndex`, `FlatIndex`, `decode`, `VectorIndex` (core).
- Produces:
  - `puffin::write_vector_index(file_io, path, index: &dyn VectorIndex, covered_snapshot, field_id, column, identity_column) -> Result<()>`
  - `puffin::read_vector_index(file_io, path) -> Result<Box<dyn VectorIndex>>`
  - `vector_index::build_vector_index(catalog, pool, table, column, metric, index_spec: IndexSpec, run_id) -> Result<BuiltIndex>` (new `index_spec` param, inserted before `run_id`)

- [ ] **Step 1: Write the failing fixture test**

Create `src/control-plane/postgres/tests/vector_index_ivf.rs`. Reuse the proven helpers from `vector_index_build.rs` (copy `columns`, `ipc_body`, `lineage`, `make_catalog` verbatim — per-test fixtures of a different shape are intentional divergence, not duplication to collapse).

```rust
//! IVF build: select IndexSpec::IvfFlat, assert the Puffin blob decodes to an
//! ivf_flat index, the mirror row records index_kind = "ivf_flat", and a search
//! over the decoded index returns the exact match when every cluster is probed.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_array::{Int64Array, RecordBatch};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    ColumnSpec, ControlPlane, DatasetId, EventType, IndexSpec, LineageEvent, Metric, ObjectType,
    PropertyDef, RunId, TableRef, TypeName, VectorIndex, VectorKey,
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
        ColumnSpec { name: "id".into(), ty: "long".into(), nullable: false },
        ColumnSpec { name: "embedding".into(), ty: "vector(4)".into(), nullable: false },
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
    props.insert(SQL_CATALOG_PROP_WAREHOUSE.to_string(), format!("file://{warehouse}"));
    SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("loom", props)
        .await
        .expect("catalog")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ivf_build_writes_decodable_blob_and_mirror_kind() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let table = TableRef { schema: "wh".into(), name: "docs".into() };

    cp.ontology()
        .define_type(ObjectType {
            name: TypeName("Docs".into()),
            table: table.clone(),
            properties: vec![
                PropertyDef { name: "id".into(), ty: "Long".into(), required: true },
                PropertyDef { name: "embedding".into(), ty: "Vector".into(), required: true },
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
    land(&pool, &catalog, &table, &columns(), &ipc_body(rows), 0, i64::MAX, lineage(run, &table))
        .await
        .expect("land rows");

    // Build with IVF (nlist=2 over 4 rows).
    let build_run = RunId(uuid::Uuid::new_v4());
    let built = build_vector_index(
        &catalog,
        &pool,
        &table,
        "embedding",
        Metric::Cosine,
        IndexSpec::IvfFlat { nlist: Some(2) },
        build_run,
    )
    .await
    .expect("build ivf");
    assert_eq!(built.row_count, 4);

    // Mirror row records the IVF kind.
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
    assert_eq!(found.index_kind, "ivf_flat");

    // The blob decodes polymorphically to an ivf_flat index that searches.
    let file_io = FileIO::new_with_fs();
    let idx = read_vector_index(&file_io, &built.puffin_path).await.expect("read");
    assert_eq!(idx.index_kind(), control_plane_core::IndexKind::IvfFlat);
    assert_eq!(idx.dim(), 4);
    assert_eq!(idx.row_count(), 4);
    // With 2 clusters, probing default nprobe may miss; but id=1 is its own
    // cluster's nearest, and nprobe>=1 always probes the query's own centroid.
    let res = idx.search(&[1.0, 0.0, 0.0, 0.0], 1);
    assert_eq!(res[0].0, VectorKey::Int(1));
}
```

Wire the target — add to `src/control-plane/postgres/BUCK` (mirror the `vector-index-build` block at line 803, but `loom_fixture_test`; confirm `loom_fixture_test` is imported at the top of the BUCK — it is used by the other vector fixture targets):

```python
loom_fixture_test(
    name = "vector-index-ivf",
    crate = "vector_index_ivf",
    srcs = ["tests/vector_index_ivf.rs"],
    crate_root = "tests/vector_index_ivf.rs",
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

(If the existing `vector-index-build` target is a bare `rust_test`, switch nothing there; just confirm `loom_fixture_test` is already `load`-ed near the top of the postgres BUCK. The other fixture targets in that file use it.)

- [ ] **Step 2: Run to verify failure**

Run: `buck2 test //src/control-plane/postgres:vector-index-ivf > /tmp/t.log 2>&1; grep -E "cannot find|error\[|expected .* arguments|FAIL" /tmp/t.log`
Expected: compile error — `read_vector_index` not found and `build_vector_index` takes 6 args not 7.

- [ ] **Step 3: Generalize the Puffin helpers**

In `src/control-plane/postgres/src/puffin.rs`, change the import (line 8) to drop the `FlatIndex`/`IndexKind`-only coupling and add `decode`:

```rust
use control_plane_core::{ControlPlaneError, FlatIndex, Result, VectorIndex, decode};
```

Add the two polymorphic helpers (keep `write_flat_index`/`read_flat_index` for existing callers/tests):

```rust
/// Serialize any `VectorIndex` into a `loom-vector-index-v1` Puffin blob with the
/// self-describing properties. The blob's payload `kind` byte distinguishes Flat
/// vs IVF; the blob-type string is unchanged.
pub async fn write_vector_index(
    file_io: &FileIO,
    path: &str,
    index: &dyn VectorIndex,
    covered_snapshot: i64,
    field_id: i32,
    column: &str,
    identity_column: &str,
) -> Result<()> {
    let mut props = HashMap::new();
    props.insert("dim".to_string(), index.dim().to_string());
    props.insert("metric".to_string(), index.metric().as_str().to_string());
    props.insert("index-kind".to_string(), index.index_kind().as_str().to_string());
    props.insert("column".to_string(), column.to_string());
    props.insert("identity-column".to_string(), identity_column.to_string());
    props.insert("row-count".to_string(), index.row_count().to_string());
    props.insert("covered-snapshot".to_string(), covered_snapshot.to_string());
    let payload = index.serialize();
    write_index_blob(file_io, path, &payload, covered_snapshot, field_id, props).await
}

/// Read and decode any loom vector index from a Puffin file, routing on the
/// payload `kind` byte. Returns a boxed `VectorIndex` for the serving/build paths.
pub async fn read_vector_index(file_io: &FileIO, path: &str) -> Result<Box<dyn VectorIndex>> {
    let loaded = read_index_blob(file_io, path).await?;
    decode(&loaded.payload)
}
```

(`FlatIndex`/`IndexKind` imports: `write_flat_index` still uses `FlatIndex` + `IndexKind::Flat`. Keep `IndexKind` in the `use` if `write_flat_index` references it — it does at line 99. So the import line should be `use control_plane_core::{ControlPlaneError, FlatIndex, IndexKind, Result, VectorIndex, decode};`.)

- [ ] **Step 4: Thread `IndexSpec` through `build_vector_index`**

In `src/control-plane/postgres/src/vector_index.rs`:

1. Imports — ensure `IndexSpec`, `IvfFlatIndex` are available. The current `use control_plane_core::{...}` at lines 8-11 imports `FlatIndex`. Add `IndexSpec, IvfFlatIndex` and drop the now-unused `IndexKind` import if the function no longer references it directly (it will get the kind from the built index — see below).

2. Signature (line 344) — add `index_spec: IndexSpec` before `run_id`:

```rust
pub async fn build_vector_index(
    catalog: &crate::iceberg_sql_catalog::SqlCatalog,
    pool: &PgPool,
    table: &TableRef,
    column: &str,
    metric: Metric,
    index_spec: control_plane_core::IndexSpec,
    run_id: RunId,
) -> Result<BuiltIndex> {
```

Update the `#[allow(clippy::too_many_arguments, reason = ...)]` reason text to mention `index_spec`.

3. Replace step 7 (line ~407-408, `let index = FlatIndex::build(dim, metric, all_rows)?;`) with a polymorphic build:

```rust
    // 7. Build the chosen index (Flat exact, or IVF approximate).
    let index: Box<dyn control_plane_core::VectorIndex> = match index_spec {
        control_plane_core::IndexSpec::Flat => {
            Box::new(FlatIndex::build(dim, metric, all_rows)?)
        }
        control_plane_core::IndexSpec::IvfFlat { nlist } => {
            Box::new(IvfFlatIndex::build(dim, metric, all_rows, nlist)?)
        }
    };
    let index_kind = index.index_kind();
```

4. Replace step 9's `write_flat_index(...)` call (line ~434) with `write_vector_index`:

```rust
    write_vector_index(
        &file_io,
        &puffin_path,
        index.as_ref(),
        s,
        field_id,
        column,
        &identity_col,
    )
    .await?;
```

And change the `use crate::puffin::write_flat_index;` (line 355) to `use crate::puffin::write_vector_index;`.

5. In step 10's `insert_vector_index` call (line ~484), replace the hardcoded `control_plane_core::IndexKind::Flat.as_str().to_string()` with the built index's kind:

```rust
            index_kind: index_kind.as_str().to_string(),
```

(`index_kind` was bound in step 7. `dim`/`row_count` locals are still valid; `index` is still in scope for nothing else, which is fine.)

- [ ] **Step 5: Fix the existing build test's call site + run**

The existing `vector_index_build.rs` calls `build_vector_index(&catalog, &pool, &table, "embedding", Metric::Cosine, build_run)` — add the spec arg:

```rust
    let built = build_vector_index(
        &catalog,
        &pool,
        &table,
        "embedding",
        Metric::Cosine,
        control_plane_core::IndexSpec::Flat,
        build_run,
    )
```

(Add `IndexSpec` to that file's `control_plane_core::{...}` import, or use the fully-qualified path as above.)

Run both postgres vector tests:
Run: `buck2 test //src/control-plane/postgres:vector-index-ivf //src/control-plane/postgres:vector-index-build > /tmp/t.log 2>&1; grep -E "FAIL|Tests finished" /tmp/t.log`
Expected: PASS. (These are fixture tests — they route local automatically via `loom_fixture_test`.)

- [ ] **Step 6: Commit**

```bash
git add src/control-plane/postgres/src/puffin.rs src/control-plane/postgres/src/vector_index.rs src/control-plane/postgres/tests/vector_index_ivf.rs src/control-plane/postgres/tests/vector_index_build.rs src/control-plane/postgres/BUCK
git commit -m "feat(postgres): polymorphic Puffin write/read + build_vector_index(IndexSpec)"
```

---

### Task 6: Wire IVF selection through the gRPC build path

Thread `index_kind`/`nlist` from the job payload → worker handler → engine-wire client → proto → engine service → primitive. The engine still hardcodes `Metric::Cosine` (unchanged); only the index spec is added.

**Files:**
- Modify: `src/services/engine-wire/proto/engine_control.proto:50` (request message)
- Modify: `src/services/engine-wire/src/client.rs:84-101` (`build_vector_index`)
- Modify: `src/services/engine/src/service.rs:168-191` (service handler)
- Modify: `src/services/worker/src/handler.rs:50-73` (`handle_build_vector_index`)
- Test (new): `src/services/engine-wire/tests/build_vector_index_request.rs` + BUCK target

**Interfaces:**
- Consumes: `IndexSpec::from_label`, `BuildVectorIndexJob` fields.
- Produces:
  - proto `BuildVectorIndexRequest { string schema=1; string name=2; string column=3; string index_kind=4; uint32 nlist=5; }`
  - `client.build_vector_index(schema, name, column, index_kind: Option<String>, nlist: Option<u32>)`

- [ ] **Step 1: Write the failing test (proto fields survive encode/decode)**

Create `src/services/engine-wire/tests/build_vector_index_request.rs`:

```rust
//! The BuildVectorIndexRequest carries the optional IVF selector fields.

use engine_wire::pb;
use prost::Message;

#[test]
fn request_roundtrips_ivf_fields() {
    let req = pb::BuildVectorIndexRequest {
        schema: "wh".into(),
        name: "docs".into(),
        column: "embedding".into(),
        index_kind: "ivf_flat".into(),
        nlist: 32,
    };
    let bytes = req.encode_to_vec();
    let back = pb::BuildVectorIndexRequest::decode(bytes.as_slice()).unwrap();
    assert_eq!(back.index_kind, "ivf_flat");
    assert_eq!(back.nlist, 32);
}

#[test]
fn request_defaults_are_empty_kind_zero_nlist() {
    // proto3 scalar defaults: "" and 0 — the "flat / auto" sentinel.
    let req = pb::BuildVectorIndexRequest {
        schema: "wh".into(),
        name: "docs".into(),
        column: "embedding".into(),
        index_kind: String::new(),
        nlist: 0,
    };
    let back = pb::BuildVectorIndexRequest::decode(req.encode_to_vec().as_slice()).unwrap();
    assert!(back.index_kind.is_empty());
    assert_eq!(back.nlist, 0);
}
```

Add the BUCK target (mirror `vector-search-ticket` in `src/services/engine-wire/BUCK`):

```python
rust_test(
    name = "build-vector-index-request",
    crate = "build_vector_index_request",
    srcs = ["tests/build_vector_index_request.rs"],
    crate_root = "tests/build_vector_index_request.rs",
    deps = [":engine-wire", "//third-party:prost"],
)
```

- [ ] **Step 2: Run to verify failure**

Run: `buck2 test //src/services/engine-wire:build-vector-index-request > /tmp/t.log 2>&1; grep -E "missing field|no field|error\[|FAIL" /tmp/t.log`
Expected: error — `index_kind`/`nlist` not fields of `BuildVectorIndexRequest`.

- [ ] **Step 3: Add the proto fields**

`src/services/engine-wire/proto/engine_control.proto:50` — replace:

```proto
message BuildVectorIndexRequest  { string schema = 1; string name = 2; string column = 3; string index_kind = 4; uint32 nlist = 5; }
```

(The `pb-gen` genrule regenerates the Rust on the next build — no manual codegen.)

- [ ] **Step 4: Thread through client, service, worker**

**engine-wire client** (`src/services/engine-wire/src/client.rs:84`):

```rust
    pub async fn build_vector_index(
        &self,
        schema: String,
        name: String,
        column: String,
        index_kind: Option<String>,
        nlist: Option<u32>,
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
            })
            .await
            .map_err(be)?
            .into_inner();
        Ok((resp.covered_snapshot, resp.puffin_path, resp.row_count))
    }
```

**engine service** (`src/services/engine/src/service.rs:168`) — map request → `IndexSpec`, pass to primitive. Add `IndexSpec` to the core import at line 5:

```rust
use control_plane_core::{Catalog, IndexSpec, Queue, RetryPolicy, RunId, TableRef};
```

Then in the handler body:

```rust
    async fn build_vector_index(
        &self,
        req: Request<pb::BuildVectorIndexRequest>,
    ) -> std::result::Result<Response<pb::BuildVectorIndexResponse>, Status> {
        let r = req.into_inner();
        let table = TableRef { schema: r.schema, name: r.name };
        let kind = if r.index_kind.is_empty() { None } else { Some(r.index_kind.as_str()) };
        let nlist = if r.nlist == 0 { None } else { Some(r.nlist) };
        let index_spec = IndexSpec::from_label(kind, nlist).map_err(status)?;
        let built = control_plane_postgres::vector_index::build_vector_index(
            &self.catalog,
            &self.pool,
            &table,
            &r.column,
            control_plane_core::Metric::Cosine,
            index_spec,
            RunId(uuid::Uuid::new_v4()),
        )
        .await
        .map_err(status)?;
        Ok(Response::new(pb::BuildVectorIndexResponse {
            covered_snapshot: built.covered_snapshot,
            puffin_path: built.puffin_path,
            row_count: built.row_count,
        }))
    }
```

**worker handler** (`src/services/worker/src/handler.rs:50`) — destructure the new fields and pass them:

```rust
    let BuildVectorIndexJob {
        schema,
        name,
        column,
        index_kind,
        nlist,
    } = serde_json::from_value(job.payload).map_err(|e| JobFailure {
        error: format!("bad build_vector_index payload: {e}"),
        policy: RetryPolicy::Abandon,
    })?;
    client
        .build_vector_index(schema, name, column, index_kind, nlist)
        .await
        .map_err(|e| JobFailure {
            error: e.to_string(),
            policy: RetryPolicy::Retry {
                delay: tuning.backoff(job.attempts),
            },
        })?;
```

- [ ] **Step 5: Run the new test + rebuild the touched binaries**

Run: `buck2 test //src/services/engine-wire:build-vector-index-request > /tmp/t.log 2>&1; grep -E "FAIL|Tests finished" /tmp/t.log`
Expected: PASS.
Run: `buck2 build //src/services/engine-wire:engine-wire //src/services/engine:engine //src/services/worker:worker > /tmp/b.log 2>&1; grep -E "error|Build ID|SUCCEEDED" /tmp/b.log`
Expected: all three build clean (new client signature + service mapping + worker wiring all consistent).

- [ ] **Step 6: Commit**

```bash
git add src/services/engine-wire/proto/engine_control.proto src/services/engine-wire/src/client.rs src/services/engine-wire/tests/build_vector_index_request.rs src/services/engine-wire/BUCK src/services/engine/src/service.rs src/services/worker/src/handler.rs
git commit -m "feat(engine): thread IVF index_kind/nlist through the build gRPC path"
```

---

### Task 7: Engine serving — decode polymorphically + IVF freshness tests

Switch the cold read from the concrete `read_flat_index` to the polymorphic `read_vector_index`, and prove the freshness invariant end-to-end with an IVF cold index.

**Files:**
- Modify: `src/services/engine-serving/src/vector_search.rs:17` (import) and `:90-93` (cold read + search)
- Modify: `src/services/engine-serving/tests/vector_search.rs` (existing `loom_fixture_test` target `vector-search`)

**Interfaces:**
- Consumes: `puffin::read_vector_index` (Task 5), `build_vector_index(IndexSpec)` (Task 5).
- Produces: no new public surface — `vector_search` behavior is unchanged for Flat; gains IVF support transparently.

- [ ] **Step 1: Write the failing tests**

The existing `seed_and_build` helper in `tests/vector_search.rs` hardcodes the Flat build. Add an IVF variant and three tests. Append to `src/services/engine-serving/tests/vector_search.rs`:

```rust
/// Like `seed_and_build` but builds an IVF index (nlist=2 over the 4 cold rows).
async fn seed_and_build_ivf(
    fx: &PgFixture,
    db: &str,
    metric: Metric,
) -> (
    SqlCatalog,
    sqlx::PgPool,
    control_plane_postgres::PgControlPlane,
    tempfile::TempDir,
) {
    use control_plane_postgres::PgControlPlane;
    use std::time::Duration;

    let wh = tempfile::tempdir().expect("wh");
    let pool = fx.pool_for(db).await;
    let catalog = make_catalog(fx.pg_dsn(db), &wh.path().display().to_string()).await;
    let cp = PgControlPlane::new(pool.clone(), Duration::from_secs(5));
    let table = TableRef { schema: "wh".into(), name: "docs".into() };

    cp.ontology()
        .define_type(ObjectType {
            name: TypeName("Docs".into()),
            table: table.clone(),
            properties: vec![
                PropertyDef { name: "id".into(), ty: "Long".into(), required: true },
                PropertyDef { name: "embedding".into(), ty: "Vector".into(), required: true },
            ],
            derived: vec![],
            identity: Some("id".into()),
        })
        .await
        .expect("define_type");

    let run = RunId(uuid::Uuid::new_v4());
    let rows_1_2: &[(i64, [f32; 4])] = &[(1, [1.0, 0.0, 0.0, 0.0]), (2, [0.0, 1.0, 0.0, 0.0])];
    land(&pool, &catalog, &table, &columns(), &ipc_body(rows_1_2), 0, i64::MAX, lineage_evt(run, &table))
        .await
        .expect("land rows 1-2");
    let rows_3_4: &[(i64, [f32; 4])] = &[(3, [0.0, 0.0, 1.0, 0.0]), (4, [0.0, 0.0, 0.0, 1.0])];
    land(&pool, &catalog, &table, &columns(), &ipc_body(rows_3_4), 0, i64::MAX, lineage_evt(run, &table))
        .await
        .expect("land rows 3-4");

    let build_run = RunId(uuid::Uuid::new_v4());
    build_vector_index(
        &catalog,
        &pool,
        &table,
        "embedding",
        metric,
        control_plane_core::IndexSpec::IvfFlat { nlist: Some(2) },
        build_run,
    )
    .await
    .expect("build ivf");

    (catalog, pool, cp, wh)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ivf_cold_search_returns_exact_match() {
    let fx = PgFixture::start();
    let (_cp_init, db) = fx.fresh_db().await;
    let table = TableRef { schema: "wh".into(), name: "docs".into() };
    let (catalog, pool, _cp, _wh) = seed_and_build_ivf(&fx, &db, Metric::Cosine).await;

    // Query id=1's own embedding: its centroid is always probed (nearest), so the
    // exact match is found even though the index is approximate.
    let batch = engine_serving::vector_search(&catalog, &pool, &table, "embedding",
        &[1.0_f32, 0.0, 0.0, 0.0], 1).await.expect("ivf cold search");
    assert_eq!(ids(&batch)[0], 1, "exact match found via IVF cold index");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ivf_hot_delta_row_is_never_pruned_cosine() {
    // The freshness invariant: a row landed inline after S is scored EXACTLY and
    // wins, regardless of IVF cluster pruning on the cold side.
    let fx = PgFixture::start();
    let (_cp_init, db) = fx.fresh_db().await;
    let table = TableRef { schema: "wh".into(), name: "docs".into() };
    let (catalog, pool, _cp, _wh) = seed_and_build_ivf(&fx, &db, Metric::Cosine).await;

    // Row 5 inline (born after S): the unique nearest to the query.
    let run = RunId(uuid::Uuid::new_v4());
    let inline: &[(i64, [f32; 4])] = &[(5, [0.95, 0.05, 0.0, 0.0])];
    land(&pool, &catalog, &table, &columns(), &ipc_body(inline), usize::MAX, i64::MAX, lineage_evt(run, &table))
        .await
        .expect("land inline row 5");

    let batch = engine_serving::vector_search(&catalog, &pool, &table, "embedding",
        &[0.9_f32, 0.1, 0.0, 0.0], 2).await.expect("ivf cold+hot search");
    let id_vec = ids(&batch);
    assert_eq!(id_vec[0], 5, "hot inline row is nearest — never pruned by IVF");
    assert_eq!(id_vec.iter().filter(|&&x| x == 5).count(), 1, "counted once");
    let dists = distances(&batch);
    assert!(dists[0] <= dists[1], "ascending");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ivf_hot_delta_row_is_never_pruned_l2() {
    let fx = PgFixture::start();
    let (_cp_init, db) = fx.fresh_db().await;
    let table = TableRef { schema: "wh".into(), name: "docs".into() };
    let (catalog, pool, _cp, _wh) = seed_and_build_ivf(&fx, &db, Metric::L2).await;

    let run = RunId(uuid::Uuid::new_v4());
    let inline: &[(i64, [f32; 4])] = &[(5, [0.95, 0.05, 0.0, 0.0])];
    land(&pool, &catalog, &table, &columns(), &ipc_body(inline), usize::MAX, i64::MAX, lineage_evt(run, &table))
        .await
        .expect("land inline row 5");

    let batch = engine_serving::vector_search(&catalog, &pool, &table, "embedding",
        &[0.9_f32, 0.1, 0.0, 0.0], 2).await.expect("ivf cold+hot l2");
    let id_vec = ids(&batch);
    assert_eq!(id_vec[0], 5, "hot inline row is nearest (L2) — never pruned");
    assert_eq!(id_vec.iter().filter(|&&x| x == 5).count(), 1, "counted once");
}
```

- [ ] **Step 2: Run to verify failure**

Run: `buck2 test //src/services/engine-serving:vector-search > /tmp/t.log 2>&1; grep -E "cannot find|error\[|FAIL" /tmp/t.log`
Expected: the new tests fail to compile/route because serving still uses `read_flat_index` (the IVF blob would error in `FlatIndex::deserialize` with "bad index kind"). Specifically `ivf_cold_search_returns_exact_match` would hit the decode error at runtime.

- [ ] **Step 3: Switch serving to `read_vector_index`**

`src/services/engine-serving/src/vector_search.rs:17` — change the import:

```rust
use control_plane_postgres::puffin::read_vector_index;
```

Lines 90-93 — replace the concrete read + search:

```rust
    let idx = read_vector_index(&file_io, &row.puffin_path)
        .await
        .map_err(to_serving)?;
    let cold: Vec<(VectorKey, f32)> = idx.search(query, k);
```

(`idx` is now `Box<dyn VectorIndex>`; `.search` is the trait method — the hot-delta fetch, `score_inline_batch`, `merge_topk`, and `build_result_batch` at lines 95-112 are unchanged. The `metric` for the hot path still comes from `row.metric`.)

- [ ] **Step 4: Run the full serving vector suite**

Run: `buck2 test //src/services/engine-serving:vector-search //src/services/engine-serving:vector-merge > /tmp/t.log 2>&1; grep -E "FAIL|Tests finished" /tmp/t.log`
Expected: PASS — the 3 new IVF tests plus the 5 existing Flat tests (`knn_cold_exact_*`, `knn_cold_hot_merge_*`, `no_bound_index_*`) all green, proving the polymorphic read is backward-compatible.

- [ ] **Step 5: Commit**

```bash
git add src/services/engine-serving/src/vector_search.rs src/services/engine-serving/tests/vector_search.rs
git commit -m "feat(engine-serving): decode vector index polymorphically; IVF cold+exact-hot freshness tests"
```

---

### Task 8: Docs registers — promote the IVF slice

Record the slice as shipped and narrow the parent ANN future item. Per CLAUDE.md's documentation-registers workflow.

**Files:**
- Modify: `docs/ROADMAP.md` (add a `done` item for the IVF slice)
- Modify: `docs/FUTURE.md:86-87` (`fut-puffin-vector-index-ann` — strike the now-shipped "approximate indexes" clause, leave the rest deferred)

- [ ] **Step 1: Add the ROADMAP entry**

Append under the `## query` section of `docs/ROADMAP.md` (mirror the grammar of the `road-puffin-vector-index` entry at line 67):

```markdown
- [x] **IVF-Flat approximate vector index (engine-side)** `{#road-ivf-vector-index area:query status:done from:2026-06-28-ivf-vector-index-design pr:- spec:2026-06-28-ivf-vector-index-design}`
  Slice 2 of the Puffin vector index. A hand-rolled **IVF-Flat** approximate index as a second `VectorIndex` impl behind the trait (deterministic k-means centroids + `nprobe` cluster probing), serialized into the `loom-vector-index-v1` Puffin blob with a `kind=1` payload discriminator and decoded polymorphically (`decode → Box<dyn VectorIndex>`). Selectable at build time via `IndexSpec` (threaded through `BuildVectorIndexJob` + the build gRPC), `Flat` stays the default. **Cold-only approximation:** the inline hot delta is still scored exactly and merged, so freshly-landed rows are never dropped by cluster pruning (`nprobe=nlist` ⇒ exact, the test oracle). Engine-side only. Remaining ANN slices stay in [[fut-puffin-vector-index-ann]].
```

- [ ] **Step 2: Narrow the parent future item**

In `docs/FUTURE.md`, edit the `fut-puffin-vector-index-ann` prose (line 87) to remove the shipped "approximate indexes (HNSW/Vamana/DiskANN) behind the `VectorIndex` trait, replacing the flat/exact first impl" clause and note IVF-Flat shipped, leaving the genuinely-deferred slices (auto-rebuild, clustering, distributed, disaggregated, external endpoint, ACL pruning, delete-vector maintenance, more types/metrics, GC, **and HNSW/Vamana graph indexes**) intact, with a `[[road-ivf-vector-index]]` cross-link.

- [ ] **Step 3: Validate the registers**

Run: `bash tools/docs.sh validate > /tmp/d.log 2>&1; cat /tmp/d.log`
Expected: no errors (ids/vocab/links valid; `spec:` slug resolves to the on-disk design file).

- [ ] **Step 4: Lint + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/l.log 2>&1; grep -E "Failed|Passed|error" /tmp/l.log | tail -20`
Expected: hooks pass (fix any markdown EOF/whitespace the hooks rewrite, then re-stage).

```bash
git add docs/ROADMAP.md docs/FUTURE.md
git commit -m "docs(registers): ship road-ivf-vector-index; narrow fut-puffin-vector-index-ann"
```

---

## Self-Review

**Spec coverage:**
- IVF-Flat behind the trait → Tasks 1-3. ✓
- `kind=1` self-describing blob + `decode → Box<dyn VectorIndex>` → Task 3. ✓
- Auto-derived `nlist`/`nprobe`, recorded, overridable (`with_nprobe`, `nlist: Option`) → Tasks 2, 4. ✓
- `IndexSpec`, `Flat` default, opt-in, no config plumbing → Tasks 4-6. ✓
- Cold-only approximation + exact hot merge (freshness invariant) → Task 7 (serving unchanged except polymorphic read; freshness tests). ✓
- Edge cases (N=0, N<nlist, nprobe≥nlist exact, dim mismatch, cosine normalize) → Task 2 tests; cosine handled by reusing `distance`. ✓
- Tests: oracle, recall, determinism, round-trip+dispatch, edges (core); build+decode (postgres); end-to-end freshness incl. nprobe-pruning (serving) → Tasks 2, 3, 5, 7. ✓
- Out-of-scope items untouched; docs narrowed → Task 8. ✓

**Type consistency:** `IvfFlatIndex::build(dim, metric, rows, nlist: Option<u32>)`, `with_nprobe(self, u32) -> Self`, `decode(&[u8]) -> Result<Box<dyn VectorIndex>>`, `IndexSpec::{Flat, IvfFlat{nlist: Option<u32>}}`, `IndexSpec::from_label(Option<&str>, Option<u32>)`, `write_vector_index(file_io, path, &dyn VectorIndex, i64, i32, &str, &str)`, `read_vector_index(file_io, path) -> Box<dyn VectorIndex>`, `build_vector_index(..., metric, index_spec, run_id)` — names/signatures are consistent across Tasks 2-7. Trait method set (`metric/dim/index_kind/row_count/serialize/search`) defined once in Task 1 and implemented for both impls.

**Placeholder scan:** No `TBD`/`handle edge cases`/bare "similar to" remain; every code step carries full, transcribable code.

**Clippy note carried in Global Constraints:** `indexing_slicing`/`needless_range_loop` confined to annotated helpers + the one `kmeans` function; the `search` filter uses `.get()` to avoid an attribute.
