# IVF-Flat approximate vector index — design

**Status:** approved (brainstorm)
**Date:** 2026-06-28
**Register item:** carves the first slice out of `fut-puffin-vector-index-ann`
(approximate index behind the `VectorIndex` trait). The remaining slices of that
item — auto-rebuild/staleness-on-flush, clustering surface, distributed build,
disaggregated query, external `/search` endpoint, ACL/predicate pruning,
delete-vector maintenance, more element types/metrics, and Puffin-sidecar GC —
stay deferred to their own specs.
**Builds on:** `road-puffin-vector-index` (PR #207, the exact flat index + Puffin
blob + mirror binding + engine-side cold∪hot merge) and `fut-inline-vector-hot-delta`
(PR #209, real inline `real[]` hot delta).

## Goal

Add an **approximate** vector index — IVF-Flat (inverted file with flat
per-cluster lists) — as a second implementation behind the existing
`VectorIndex` trait, selectable at build time and decoded polymorphically at
serve time. The exact `FlatIndex` stays as the default; IVF is opt-in. The
engine-side cold∪hot k-NN merge is unchanged: **IVF covers the cold tier only**,
and the live inline hot delta is scored exactly and merged, so approximation
never drops freshly-landed rows.

This remains **engine-side only** — no external query-api `/search` endpoint
(that is a separate deferred slice).

## Background: what slice 1 built

- **`control-plane-core`** (`src/control-plane/core/src/vector_index.rs`): a pure
  `VectorIndex` trait (`metric()`, `dim()`, `search(query, k) -> Vec<(VectorKey,
  f32)>`), an exact `FlatIndex` impl (packed `f32` rows + parallel `VectorKey`
  column), a `Metric` enum (`Cosine`/`L2`), an `IndexKind` enum (only `Flat`), a
  `VectorKey` enum (`Int`/`Str`), and a `distance(metric, a, b)` helper. `FlatIndex`
  serializes to a compact self-describing binary:
  `magic "LVIX" | u8 version=1 | u8 metric | u8 kind | u32 dim | u32 row_count |
  data (row_count*dim f32 LE) | u8 key_kind | keys`. The `kind` byte is written as
  `0` and `deserialize` asserts `== 0` — **it is the extension point this slice
  uses.**
- **`control-plane-postgres`**: `puffin.rs` writes/reads the index as a single
  `loom-vector-index-v1` Puffin blob (`write_flat_index`/`read_flat_index`).
  `vector_index.rs` holds the `vector_index` mirror row (insert/lookup, with an
  `index_kind` text column already present), and the `build_vector_index`
  primitive — reads cold Parquet + hot inline rows **as of covered snapshot S**,
  builds the index, writes the Puffin blob, and commits the mirror row + lineage
  in one transaction. It currently hardcodes `FlatIndex::build` / `IndexKind::Flat`.
- **`control-plane-core`** `vector_index_job.rs`: `BuildVectorIndexJob { schema,
  name, column }` + the `build_vector_index` queue kind, threaded through the
  worker (`handler.rs` → engine RPC).
- **`engine-serving`** (`vector_search.rs`): `vector_search` looks up the bound
  index, reads the Puffin `FlatIndex` (`read_flat_index`), runs `idx.search`,
  fetches the inline hot delta (`inline_delta_batch`, rows born in (S, Q] alive at
  Q), scores it exactly (`score_inline_batch`), and merges via `merge_topk`.

## Architecture

`IvfFlatIndex` is a second `VectorIndex` implementation living **entirely in
`control-plane-core`** (pure — no iceberg, arrow, object-store, or third-party
deps; hand-rolled k-means). The build and serve paths become **polymorphic over
the trait** rather than hardcoding `FlatIndex`:

- **Self-describing blob, kind dispatch.** IVF serializes with `kind = 1`. A new
  free function `core::vector_index::decode(bytes) -> Result<Box<dyn
  VectorIndex>>` reads `magic | version | metric | kind` and returns the matching
  boxed impl (`0` → `FlatIndex`, `1` → `IvfFlatIndex`). The blob self-describes,
  so serving dispatches off the bytes — the mirror `index_kind` column stays
  observability/filtering only.
- **Cold-only approximation (the freshness invariant).** IVF replaces `FlatIndex`
  on the **cold** side only. The inline hot delta keeps its exact brute-force
  scoring and the `merge_topk` seam is unchanged. Therefore:

  > IVF approximation applies **only** to the cold tier (rows flushed at or before
  > the index's covered snapshot S). Inline rows born in (S, Q] are scored exactly
  > and merged. A row landed inline after S is counted exactly once and is **never**
  > dropped by cluster pruning — guaranteed for any `nprobe`.

  We deliberately do **not** mutate the loaded IVF index per query (no in-memory
  insertion of live rows into clusters): that would subject fresh rows to IVF's
  cluster pruning and could silently drop the most-relevant data. The data flow is
  identical to slice 1 — the only swap is `FlatIndex` → `IvfFlatIndex` on the cold
  side.

## IVF-Flat algorithm

**Build (over the cold+hot rows live at S, as slice 1 already gathers them):**
1. Choose `nlist` (number of clusters): caller override, else heuristic
   `round(sqrt(N))` clamped to `[1, N]` (`N` = row count).
2. Run **k-means** over the `N` vectors with `nlist` centroids:
   - **Deterministic** k-means++ initialization seeded by a **fixed constant**
     (a small reproducible PRNG, e.g. SplitMix64 seeded with a compile-time
     constant), then a fixed cap of Lloyd iterations (e.g. `max_iters = 20`, early
     stop on no reassignment). Determinism makes the serialized blob byte-identical
     for a given input row order — and the build already fixes input order
     (`order by loom_row_id` / Parquet file order).
   - Distance uses the index `metric`. For `Cosine`, vectors are L2-normalized
     before assignment (spherical k-means), consistent with how `FlatIndex` /
     `distance` treat cosine.
3. Assign each row to its nearest centroid (`assignments: Vec<u32>`, one cluster id
   per row, parallel to `data`/`keys`).
4. Record a default `nprobe` = heuristic of `nlist` (e.g. `max(1, round(sqrt(nlist)))`),
   overridable via `with_nprobe`.

**Search (`search(query, k)`):**
1. Score `query` against all `nlist` centroids via `distance(metric, query, centroid)`.
2. Take the `nprobe` nearest centroids (the probe set).
3. Brute-force `distance(metric, query, row)` over **only** the rows whose
   `assignments[i]` is in the probe set (postings are rebuilt in memory at decode
   for efficient cluster→rows iteration), keep the running top-k.
4. Return top-k ascending by distance, ties broken by insertion order (matches
   `FlatIndex`).

**Exactness corollary (the test oracle):** with `nprobe >= nlist` every cluster is
probed, so IVF brute-forces all rows and returns results **identical** to
`FlatIndex` (same keys, same order). Recall degrades as `nprobe` shrinks.

## Serialized form (`kind = 1`)

```
magic "LVIX" | u8 version=1 | u8 metric | u8 kind=1 |
u32 dim | u32 nlist | u32 nprobe | u32 row_count |
centroids: nlist*dim f32 LE |
assignments: row_count * u32 LE |
data: row_count*dim f32 LE |
u8 key_kind | keys (as in FlatIndex: int → i64 LE; str → u32 len + utf8)
```

The version byte stays `1`; the `kind` byte discriminates. `FlatIndex`'s existing
`serialize`/`deserialize` are unchanged. `decode(bytes)` peeks `magic | version |
metric | kind` and routes to the concrete `deserialize`.

## Components (exact files)

1. **`src/control-plane/core/src/vector_index.rs`**
   - `IndexKind::IvfFlat`; `as_str` → `"ivf_flat"`; add `from_str(&str) ->
     Option<IndexKind>` covering both kinds.
   - `pub struct IvfFlatIndex { dim, metric, nlist, nprobe, centroids: Vec<f32>,
     assignments: Vec<u32>, keys: Vec<VectorKey>, data: Vec<f32> }`.
   - `IvfFlatIndex::build(dim: u32, metric: Metric, rows: Vec<(VectorKey,
     Vec<f32>)>, nlist: Option<u32>) -> Result<IvfFlatIndex>` (errors on dim
     mismatch, as `FlatIndex::build`).
   - `IvfFlatIndex::with_nprobe(self, nprobe: u32) -> Self` (clamped to `[1,
     nlist]`).
   - `impl VectorIndex for IvfFlatIndex` (`metric`/`dim`/`search` per the algorithm
     above).
   - `serialize(&self) -> Vec<u8>` / `deserialize(bytes) -> Result<IvfFlatIndex>`
     for `kind = 1`.
   - `pub fn decode(bytes: &[u8]) -> Result<Box<dyn VectorIndex>>` — the kind
     dispatcher.
   - Private deterministic k-means helper (k-means++ seeded init + Lloyd iters)
     with a fixed seed constant and `max_iters` constant.
   - `lib.rs`: export `IvfFlatIndex` (and `decode` if not already path-public).

2. **`src/control-plane/postgres/src/puffin.rs`**
   - `write_vector_index(file_io, path, bytes: &[u8], covered_snapshot, field_id,
     column, identity_col)` — writes any index's pre-serialized bytes as the
     `loom-vector-index-v1` blob (the existing `write_flat_index` becomes a thin
     caller, or call sites move to the generalized fn).
   - `read_vector_index(file_io, path) -> Result<Box<dyn VectorIndex>>` — reads the
     blob bytes and calls `core::decode`. `read_flat_index` may stay for existing
     callers/tests but serving moves to `read_vector_index`.

3. **`src/control-plane/postgres/src/vector_index.rs`**
   - `build_vector_index` gains an `index_spec: IndexSpec` parameter, where
     `pub enum IndexSpec { Flat, IvfFlat { nlist: Option<u32> } }` (define in core
     or postgres — core, next to `IndexKind`, so the job/worker can construct it).
   - Dispatch: `IndexSpec::Flat` → `FlatIndex::build` (today's path);
     `IndexSpec::IvfFlat { nlist }` → `IvfFlatIndex::build(dim, metric, rows,
     nlist)`. Serialize the chosen index, write via `write_vector_index`, set the
     mirror row `index_kind` to the matching `as_str()`. Mirror schema unchanged.

4. **`src/control-plane/core/src/vector_index_job.rs`**
   - `BuildVectorIndexJob` gains `#[serde(default)] index_kind: Option<String>` and
     `#[serde(default)] nlist: Option<u32>`. `#[serde(default)]` keeps existing
     payloads deserializing (both `None` → `IndexSpec::Flat`, today's behavior). A
     helper maps `(index_kind, nlist)` → `IndexSpec` (unknown `index_kind` → error).

5. **`src/services/worker/src/handler.rs`** (+ the engine RPC it calls)
   - `handle_build_vector_index` reads the two new fields, maps to `IndexSpec`,
     and threads it through to `build_vector_index`. Default (absent fields) =
     `IndexSpec::Flat`.

6. **`src/services/engine-serving/src/vector_search.rs`**
   - Replace `read_flat_index` + concrete `idx.search` with `read_vector_index` →
     `Box<dyn VectorIndex>` → `.search(query, k)`. Steps 5–7 (hot-delta fetch,
     exact scoring, `merge_topk`, result batch) are **unchanged**.

**Default index kind:** `IndexSpec::Flat` whenever neither the job nor the
primitive specifies a kind — non-breaking for existing enqueuers and tests, which
keep exact results. IVF is selected explicitly. No `LayeredConfig`/`QueryApiConfig`
plumbing (engine-side only; no external caller forces a default flip yet).

## Edge cases

- **`N = 0`** → `nlist` clamps to `0`/`1`; empty index; `search` returns `[]`.
- **`N < nlist`** → clamp `nlist = min(nlist, N)` so k-means has no empty-by-
  construction clusters.
- **`nprobe >= nlist`** → all clusters probed ⟹ exact (oracle).
- **dim mismatch in a build row** → error, as `FlatIndex::build`.
- **Cosine** → L2-normalize in both k-means assignment and query scoring,
  consistent with the existing exact path.
- **Reproducibility** → fixed seed + fixed `max_iters` ⟹ byte-identical blob for a
  given input order (build order is already fixed).

## Testing

All tests are `rust_test` integration targets in sibling `tests/<name>.rs` files
wired in the crate `BUCK` (per CLAUDE.md — **no inline `#[cfg(test)]`**; the
`no-inline-tests` hook enforces it). Fixture-backed tests use `loom_fixture_test`.

**`control-plane-core` (pure, RE-eligible):**
- **Oracle / exactness:** IVF with `nprobe = nlist` returns identical top-k (keys
  **and** order) to `FlatIndex` on a synthetic set, for both `Cosine` and `L2`.
- **Recall:** on a synthetic clustered set, IVF with `nprobe < nlist` achieves
  recall ≥ a fixed threshold (e.g. 0.9) for top-k vs. exact.
- **Determinism:** building twice over the same row order yields byte-identical
  `serialize()` output.
- **Round-trip + dispatch:** `serialize`→`deserialize` is bit-exact; `decode`
  returns an `IvfFlatIndex` for `kind = 1` bytes and a `FlatIndex` for `kind = 0`
  bytes; both then `search` correctly.
- **Edges:** `N = 0`, `N < nlist`, dim-mismatch error.

**`control-plane-postgres` (fixture):**
- `build_vector_index` with `IndexSpec::IvfFlat { .. }` writes a `kind = 1` Puffin
  blob and a mirror row with `index_kind = "ivf_flat"`; `read_vector_index`
  decodes it back to a working index.

**`engine-serving` (fixture):**
- **End-to-end freshness (extends the slice-1 acceptance-4 fixture):** with an IVF
  cold index and a row landed inline in (S, Q], the row is counted exactly once
  across cold ∪ hot, Cosine and L2.
- **Freshness under maximal pruning:** a hot-delta row that is a true top-k
  neighbor is returned even with `nprobe = 1` (cold side maximally pruned),
  proving the hot path bypasses approximation.
- **Cold exactness:** with `nprobe = nlist` the end-to-end result equals the exact
  `FlatIndex` end-to-end result on the same data.

## Out of scope (stay in `fut-puffin-vector-index-ann` or sibling items)

External `/search` query-api endpoint; automatic rebuild / staleness-on-flush;
clustering surface; distributed (coordinator/executor) build + routing-blob
codebook; disaggregated tiered-probe + beam-search query; ACL/predicate pruning of
the cold index before k-NN; deletion-vector / update-delete index maintenance;
non-`f32` element types and metrics beyond Cosine/L2; Puffin-sidecar GC; HNSW/Vamana
graph indexes; config-seam plumbing of `nlist`/`nprobe`.
