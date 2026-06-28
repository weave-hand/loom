# HNSW graph vector index — design

**Status:** approved (brainstorm)
**Date:** 2026-06-28
**Register item:** carves the next slice out of `fut-puffin-vector-index-ann` (the
**HNSW graph index** behind the `VectorIndex` trait). The remaining slices of that
item — Vamana, auto-rebuild/staleness-on-flush, clustering surface, distributed
build, disaggregated query, external `/search` endpoint, ACL/predicate pruning,
delete-vector maintenance, more element types/metrics, and Puffin-sidecar GC —
stay deferred to their own specs.
**Builds on:** `road-puffin-vector-index` (PR #207, the exact flat index + Puffin
blob + mirror binding + engine-side cold∪hot merge), `fut-inline-vector-hot-delta`
(PR #209, real inline `real[]` hot delta), and `road-ivf-vector-index` (PR #210,
the second `VectorIndex` impl + `kind` discriminator + polymorphic `decode`).

## Goal

Add a third **approximate** vector index — **HNSW** (Hierarchical Navigable Small
World graphs) — as a third implementation behind the existing `VectorIndex` trait,
selectable at build time and decoded polymorphically at serve time. The exact
`FlatIndex` stays the default; HNSW is opt-in like `IvfFlatIndex`. The engine-side
cold∪hot k-NN merge is unchanged: **HNSW covers the cold tier only**, and the live
inline hot delta is scored exactly and merged, so graph approximation never drops
freshly-landed rows.

This remains **engine-side only** — no external query-api `/search` endpoint (that
is a separate deferred slice).

**Why HNSW (not Vamana/DiskANN):** HNSW is an in-memory graph index, which fits
loom's serve model — the whole Puffin blob is read into memory and searched.
Vamana/DiskANN is designed around on-disk adjacency and a beam search tuned for SSD
page reads; it would add complexity that loom's "load the blob, search in memory"
path does not need. Vamana stays deferred in `fut-puffin-vector-index-ann`.

## Background: what slices 1–2 built

- **`control-plane-core`** (`src/control-plane/core/src/vector_index.rs`): the pure
  `VectorIndex` trait (`metric()`, `dim()`, `index_kind()`, `row_count()`,
  `serialize()`, `search(query, k) -> Vec<(VectorKey, f32)>`), an exact `FlatIndex`
  (`kind=0`), an approximate `IvfFlatIndex` (`kind=1`), a `Metric` enum
  (`Cosine`/`L2`), an `IndexKind` enum (`Flat`/`IvfFlat`, with `as_str`/`FromStr`),
  an `IndexSpec` enum (`Flat`/`IvfFlat { nlist }`, with `from_label`), a `VectorKey`
  enum (`Int`/`Str`), a `distance(metric, a, b)` helper, a deterministic
  `SplitMix64` PRNG, and the kind dispatcher `decode(bytes) -> Box<dyn
  VectorIndex>` (routes on the `kind` byte at offset 6). All indexes serialize into
  one self-describing binary: `magic "LVIX" | u8 version=1 | u8 metric | u8 kind |
  …`.
- **`control-plane-postgres`**: `puffin.rs` writes/reads the index as a single
  `loom-vector-index-v1` Puffin blob (`write_vector_index`/`read_vector_index`;
  `read_vector_index` calls `core::decode`). `vector_index.rs` holds the
  `vector_index` mirror row (insert/lookup, `index_kind` text column) and the
  `build_vector_index` primitive — reads cold Parquet + hot inline rows **as of
  covered snapshot S**, builds the index per an `IndexSpec`, writes the Puffin blob,
  and commits the mirror row + lineage in one transaction.
- **`control-plane-core`** `vector_index_job.rs`: `BuildVectorIndexJob { schema,
  name, column, index_kind, nlist }` + the `build_vector_index` queue kind, threaded
  through the worker (`handler.rs` → engine RPC).
- **`engine-serving`** (`vector_search.rs`): `vector_search` looks up the bound
  index, reads it via `read_vector_index` → `Box<dyn VectorIndex>`, runs
  `.search(query, k)`, fetches the inline hot delta (rows born in (S, Q] alive at
  Q), scores it exactly, and merges via `merge_topk`. **Already polymorphic over the
  trait — untouched by this slice.**

## Architecture

`HnswIndex` is a third `VectorIndex` implementation living **entirely in
`control-plane-core`** (pure — no iceberg, arrow, object-store, or third-party
deps; hand-rolled graph + the existing `SplitMix64`). The build and serve paths are
already polymorphic over the trait (slice 2 did that work); this slice adds the
third concrete impl and its `kind=2` discriminator.

- **Self-describing blob, kind dispatch.** HNSW serializes with `kind = 2`.
  `decode(bytes)` gains a `2 => HnswIndex::deserialize` arm. The blob self-describes,
  so serving dispatches off the bytes — the mirror `index_kind` column stays
  observability/filtering only.
- **Cold-only approximation (the freshness invariant, unchanged).** HNSW replaces
  the cold-side index only. The inline hot delta keeps its exact brute-force scoring
  and the `merge_topk` seam is unchanged. Therefore:

  > HNSW approximation applies **only** to the cold tier (rows flushed at or before
  > the index's covered snapshot S). Inline rows born in (S, Q] are scored exactly
  > and merged. A row landed inline after S is counted exactly once and is **never**
  > dropped by graph pruning — guaranteed for any `efSearch`.

  We deliberately do **not** insert live rows into the loaded graph per query: that
  would subject fresh rows to graph pruning and could silently drop the
  most-relevant data. The data flow is identical to slices 1–2 — the only swap is
  `IvfFlatIndex` → `HnswIndex` on the cold side.

## HNSW algorithm

A textbook HNSW (Malkov & Yashunin, 2016), made **deterministic** so the serialized
blob is byte-identical for a given input row order (matching `IvfFlatIndex`'s
reproducibility guarantee; build order is already fixed by `order by loom_row_id` /
Parquet file order).

**Parameters:**
- `M` — max neighbors per node on layers ≥ 1 (`Mmax`); layer 0 uses `Mmax0 = 2*M`.
  Default `M = 16`.
- `efConstruction` — candidate-list width during build. Default `200`.
- `efSearch` — candidate-list width at query time. Default `max(efSearch, k)` so the
  search always considers at least `k` candidates. Overridable via `with_ef_search`
  (clamped to ≥ 1); **not wired to an external caller this slice** (mirrors
  `IvfFlatIndex::with_nprobe`).
- `mL = 1 / ln(M)` — level-generation normalizer.

**Build (over the cold+hot rows live at S, as slices 1–2 already gather them),
single-threaded, in fixed row order:**
1. Pack rows into `data` (`row_count*dim` f32) + parallel `keys`, erroring on any
   `v.len() != dim` (as `FlatIndex::build`).
2. For each node `i` in insertion order, draw its top layer
   `L = floor(-ln(u) * mL)` where `u ∈ (0,1]` from the fixed-seed `SplitMix64`
   (`next_f64`, with `u` nudged off 0). The first inserted node becomes the entry
   point at its layer.
3. Insert: greedy descent (ef=1) from the current entry point down to layer `L+1`;
   then for layers `min(L, max_layer)`..0 run an `efConstruction`-width search,
   select up to `Mmax`(`Mmax0` at layer 0) neighbors via the **diversity heuristic**
   (Algorithm 4 — keep a candidate only if it is closer to the query than to every
   already-selected neighbor), and add **bidirectional** links. When a neighbor’s
   connection list exceeds its `Mmax`, re-prune it with the same heuristic.
4. If `L > max_layer`, the new node becomes the entry point and `max_layer = L`.
5. **Cosine** → operate on L2-normalized vectors during graph construction
   (consistent with how `FlatIndex` / `IvfFlatIndex` / `distance` treat cosine).

**Search (`search(query, k)`):**
1. Greedy descent (ef=1) from the entry point through layers `max_layer`..1.
2. At layer 0, run an `efSearch`-width search (best-first expansion over the graph,
   visited-set guarded), collecting the closest candidates.
3. Return top-k ascending by `distance(metric, query, row)`, ties broken by node
   index (deterministic, matching the other impls’ insertion-order tie-break).

**Determinism:** single-threaded build + the fixed `SplitMix64` seed + index
tie-breaking ⇒ byte-identical `serialize()` for a fixed input order.

**No exact oracle (unlike IVF).** HNSW has no parameterization that *provably*
equals `FlatIndex` (the graph may not be fully connected, and beam search is
heuristic). The cold-side correctness test is therefore a **recall threshold**, not
bit-exact equality — see Testing.

## Serialized form (`kind = 2`)

```
magic "LVIX" | u8 version=1 | u8 metric | u8 kind=2 |
u32 dim | u32 M | u32 ef_construction | u32 ef_search |
u32 entry_point | u32 max_layer | u32 row_count |
data: row_count*dim f32 LE |
per node i in 0..row_count:
    u8 node_max_layer |
    for layer in 0..=node_max_layer: u32 nbr_count | nbr_count * u32 LE (neighbor node indices) |
u8 key_kind | keys (as FlatIndex: int → i64 LE; str → u32 len + utf8)
```

The version byte stays `1`; the `kind` byte discriminates. `FlatIndex`'s and
`IvfFlatIndex`'s existing `serialize`/`deserialize` are unchanged. `decode(bytes)`
peeks `magic | version | metric | kind` and routes to the concrete `deserialize`.
For an empty index (`row_count = 0`) `entry_point`/`max_layer` are written as `0`
and `search` returns `[]`.

## Components (exact files)

1. **`src/control-plane/core/src/vector_index.rs`**
   - `IndexKind::Hnsw`; `as_str` → `"hnsw"`; `FromStr` arm for `"hnsw"`.
   - `IndexSpec::Hnsw { m: Option<u32>, ef_construction: Option<u32> }`. Extend
     `IndexSpec::from_label` to accept the HNSW params (new signature
     `from_label(kind, nlist, m, ef_construction)`); `Some("hnsw")` →
     `IndexSpec::Hnsw { m, ef_construction }`, unknown kind still errors.
   - `pub struct HnswIndex { dim, metric, m, ef_construction, ef_search,
     entry_point: u32, max_layer: u32, keys: Vec<VectorKey>, data: Vec<f32>,
     layers: Vec<Vec<Vec<u32>>> }` (per-node, per-layer adjacency).
   - `HnswIndex::build(dim, metric, rows, m: Option<u32>, ef_construction:
     Option<u32>) -> Result<HnswIndex>` (errors on dim mismatch).
   - `HnswIndex::with_ef_search(self, ef_search: u32) -> Self` (clamped to ≥ 1).
   - `impl VectorIndex for HnswIndex` (`metric`/`dim`/`index_kind`/`row_count`/
     `serialize`/`search` per the algorithm above).
   - `serialize`/`deserialize` for `kind = 2`.
   - `decode`: add the `2 => Box::new(HnswIndex::deserialize(bytes)?)` arm.
   - Private deterministic graph helpers (level draw, layer search, diversity
     heuristic) reusing the existing `SplitMix64`, `row_slice`, `distance`.
   - `lib.rs`: export `HnswIndex`.

2. **`src/control-plane/postgres/src/vector_index.rs`**
   - `build_vector_index`’s `IndexSpec` dispatch gains `IndexSpec::Hnsw { m,
     ef_construction }` → `HnswIndex::build(dim, metric, rows, m, ef_construction)`.
     Serialize the chosen index, write via the existing `write_vector_index`, set
     the mirror row `index_kind` to `"hnsw"`. Mirror schema unchanged.

3. **`src/control-plane/core/src/vector_index_job.rs`**
   - `BuildVectorIndexJob` gains `#[serde(default)] m: Option<u32>` and
     `#[serde(default)] ef_construction: Option<u32>`. `#[serde(default)]` keeps
     existing payloads deserializing. The `(index_kind, nlist, m, ef_construction)`
     → `IndexSpec` mapping goes through the extended `from_label`.

4. **`src/services/worker/src/handler.rs`** (+ the engine RPC it calls)
   - `handle_build_vector_index` reads the new fields and threads them through to
     `build_vector_index` via `IndexSpec::from_label`. Default (absent fields) =
     `IndexSpec::Flat`.

5. **`src/services/engine-serving/src/vector_search.rs`** — **no change.** Already
   reads `read_vector_index` → `Box<dyn VectorIndex>` → `.search(query, k)`; HNSW is
   picked up via `decode`'s new arm transparently.

**Default index kind:** `IndexSpec::Flat` whenever neither the job nor the primitive
specifies a kind — non-breaking for existing enqueuers and tests. HNSW is selected
explicitly (`index_kind = "hnsw"`). No `LayeredConfig`/`QueryApiConfig` plumbing of
`M`/`efConstruction`/`efSearch` (engine-side only; no external caller forces a
default yet).

## Edge cases

- **`N = 0`** → empty graph; `entry_point`/`max_layer` = 0; `search` returns `[]`.
- **`N = 1`** → single node, entry point, no edges; `search` returns it.
- **`N < M`** → graph builds with fewer than `M` neighbors per node (the heuristic
  simply selects all available); no special-casing needed.
- **dim mismatch in a build row** → error, as `FlatIndex::build`.
- **Cosine** → L2-normalize during construction and query scoring, consistent with
  the existing exact path.
- **Disconnected components / `k > reachable`** → `search` returns as many as the
  beam reaches; the cold result is unioned with the exact hot delta upstream, so the
  freshness invariant still holds.
- **Reproducibility** → fixed seed + single-threaded build ⇒ byte-identical blob for
  a given input order (build order is already fixed).

## Testing

All tests are `rust_test` integration targets in sibling `tests/<name>.rs` files
wired in the crate `BUCK` (per CLAUDE.md — **no inline `#[cfg(test)]`**; the
`no-inline-tests` hook enforces it). Fixture-backed tests use `loom_fixture_test`.

**`control-plane-core` (pure, RE-eligible):**
- **Recall:** on a synthetic clustered set, HNSW top-k recall ≥ a fixed threshold
  (e.g. 0.9) vs. exact `FlatIndex`, for both `Cosine` and `L2`. (The cold-side
  correctness oracle — HNSW has no exact parameterization.)
- **Determinism:** building twice over the same row order yields byte-identical
  `serialize()` output.
- **Round-trip + dispatch:** `serialize`→`deserialize` is bit-exact; `decode`
  returns an `HnswIndex` for `kind = 2` bytes (and still a `FlatIndex`/`IvfFlatIndex`
  for `kind = 0`/`1`); each then `search`es correctly.
- **Edges:** `N = 0`, `N = 1`, `N < M`, dim-mismatch error.

**`control-plane-postgres` (fixture):**
- `build_vector_index` with `IndexSpec::Hnsw { .. }` writes a `kind = 2` Puffin blob
  and a mirror row with `index_kind = "hnsw"`; `read_vector_index` decodes it back
  to a working index.

**`engine-serving` (fixture):**
- **End-to-end freshness (extends the slice-1/2 acceptance fixture):** with an HNSW
  cold index and a row landed inline in (S, Q], the row is counted exactly once
  across cold ∪ hot, Cosine and L2.
- **Freshness under approximation:** a hot-delta row that is a true top-k neighbor
  is returned even when the cold HNSW graph would not surface it (proving the hot
  path bypasses graph pruning).

## Out of scope (stay in `fut-puffin-vector-index-ann` or sibling items)

Vamana/DiskANN; external `/search` query-api endpoint; automatic rebuild /
staleness-on-flush; clustering surface; distributed (coordinator/executor) build +
routing-blob codebook; disaggregated tiered-probe + beam-search query; ACL/predicate
pruning of the cold index before k-NN; deletion-vector / update-delete index
maintenance; non-`f32` element types and metrics beyond Cosine/L2; Puffin-sidecar
GC; config-seam plumbing of `M`/`efConstruction`/`efSearch`.
