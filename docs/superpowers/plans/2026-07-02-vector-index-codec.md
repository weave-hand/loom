# road-vector-index-codec Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split `src/control-plane/core/src/vector_index.rs` (1373 lines; the three
(de)serializers are cc 28/37/49) into
`src/control-plane/core/src/vector_index/{mod.rs, codec.rs, flat.rs, ivf.rs, hnsw.rs, kmeans.rs}`
with **zero public-API change and byte-identical wire format**. `codec.rs` owns the
shared binary primitives the three near-identical (de)serializers hand-roll today
(header, bounded f32 sections, keys section, row packing, the `decode()` kind-byte
offset); each `deserialize` collapses to ~15 lines of format-specific fields. The
cognitive hotspots get named helpers: `kmeans` extracts `nearest_centroid`,
`hnsw_search_layer` extracts `pop_nearest`/`evict_worst` (linear scans stay —
deliberate, bounded by `ef`). Byte-identity is *proven*, not assumed: Task 1 pins
golden bytes for representative Flat/IVF/HNSW blobs **before any restructuring**, and
that test passes unchanged through every subsequent commit.

**Architecture:**

- **Directory-module layout is `vector_index/mod.rs`** (not `vector_index.rs` + a
  sibling dir): clippy's whole-`restriction` group is enabled on the toolchain and
  `CLIPPY_ALLOWS` (`toolchains/BUCK:220`) allows `mod_module_files` but NOT
  `self_named_module_files` — so the self-named layout would redden the gate. This
  also matches the tree's one existing precedent
  (`src/control-plane/postgres/src/iceberg_sql_catalog/mod.rs`). The `:core`
  `rust_library` globs `src/**/*.rs` (`core/BUCK:5`), so **no BUCK change** is needed
  for the split itself.
- **Module boundaries:**
  - `mod.rs` — the shared vocabulary + cross-impl utilities: `IndexSpec`, `Metric`,
    `IndexKind`, `VectorKey`, the `VectorIndex` trait, `decode()`, `distance` (+
    private `cosine_distance`/`l2_distance`), `row_slice`/`row_slice_mut`, and
    `SplitMix64` (the deterministic RNG both the k-means and HNSW builds seed).
    Private items in `mod.rs` are visible to the child modules (Rust privacy:
    an item is visible in its defining module and all descendants), so no visibility
    churn for these. Plus `mod codec; mod flat; mod hnsw; mod ivf; mod kmeans;` and
    `pub use flat::FlatIndex; pub use hnsw::HnswIndex; pub use ivf::IvfFlatIndex;`.
  - `codec.rs` — `ByteReader` (today's private `Cursor` at `vector_index.rs:727-762`,
    renamed because it shadows `core::page::Cursor`, which IS re-exported at the crate
    root), `write_header`/`read_header`, `write_f32s`/`read_f32_section`,
    `write_keys`/`read_keys`, `pack_rows`, `bad()` (the decode-error constructor,
    used by every decoder and `decode()`), `KIND_OFFSET: usize = 6`, and the kind-byte
    constants `KIND_FLAT = 0` / `KIND_IVF_FLAT = 1` / `KIND_HNSW = 2` (today hard-coded
    literals in three writers, three readers, and `decode()`'s match). Everything
    `pub(super)` — siblings need it; nothing leaves the `vector_index` tree.
  - `flat.rs` — `FlatIndex` (struct, `build`, inherent `row_count`/`serialize` with
    their `#[expect(clippy::same_name_method)]`, `deserialize`, `row`, the
    `VectorIndex` impl currently at `:1344-1373`, and the format doc comment).
  - `ivf.rs` — `IvfFlatIndex` (struct, `build`, `set_nprobe`/`with_nprobe`,
    `centroid`/`row`, `serialize_bytes`, `deserialize`, `VectorIndex` impl) +
    `default_nlist`/`default_nprobe`.
  - `kmeans.rs` — `kmeans()`, `KMEANS_SEED`, `KMEANS_MAX_ITERS`, and (Task 6) the new
    `nearest_centroid`.
  - `hnsw.rs` — `HnswIndex` (struct, `build`, `set_ef_search`/`with_ef_search`,
    `serialize_bytes`, `deserialize`, `VectorIndex` impl), `hnsw_search_layer`,
    `hnsw_select_neighbors`, `cmp_dist`, `HNSW_SEED`/`HNSW_DEFAULT_M`/
    `HNSW_DEFAULT_EF_CONSTRUCTION`/`HNSW_MAX_LEVEL`, and (Task 6) the new
    `pop_nearest`/`evict_worst`.
- **The codec API** (all `pub(super)`, all in `codec.rs`):

  ```rust
  pub(super) const KIND_FLAT: u8 = 0;
  pub(super) const KIND_IVF_FLAT: u8 = 1;
  pub(super) const KIND_HNSW: u8 = 2;
  /// Byte offset of the kind byte `decode()` peeks: magic[4] + version + metric.
  pub(super) const KIND_OFFSET: usize = 6;

  pub(super) fn bad(m: &str) -> ControlPlaneError;

  pub(super) struct ByteReader<'a> { /* b: &'a [u8], p: usize — today's Cursor */ }
  impl ByteReader<'_> {
      pub(super) fn new(b: &[u8]) -> ByteReader<'_>;
      pub(super) fn remaining(&self) -> usize;      // saturating_sub — the alloc bound
      pub(super) fn take(&mut self, n: usize) -> Result<&[u8]>; // checked_add + get
      pub(super) fn u8(&mut self) -> Result<u8>;
      pub(super) fn u32(&mut self) -> Result<u32>;
      pub(super) fn i64(&mut self) -> Result<i64>;
      pub(super) fn f32(&mut self) -> Result<f32>;
  }

  /// magic "LVIX" | u8 version=1 | u8 metric | u8 kind — the 7 fixed header bytes.
  pub(super) fn write_header(out: &mut Vec<u8>, metric: Metric, kind: u8);
  /// Validates magic/version/metric, then the kind byte; `kind_mismatch` preserves
  /// each decoder's exact error text ("bad index kind" / "not an ivf_flat index" /
  /// "not an hnsw index").
  pub(super) fn read_header(
      r: &mut ByteReader<'_>,
      expected_kind: u8,
      kind_mismatch: &'static str,
  ) -> Result<Metric>;

  pub(super) fn write_f32s(out: &mut Vec<u8>, xs: &[f32]);
  /// Which f32 section is being read — selects the exact error strings so the
  /// refactor is message-identical.
  pub(super) enum F32Section { Data, Centroids }
  /// THE bounded-`with_capacity` corrupt-header guard, once: `n.checked_mul(d)`
  /// (overflow → "row_count * dim overflow" / "nlist * dim overflow"), then
  /// `len > r.remaining()` → "data section exceeds buffer" /
  /// "centroid section exceeds buffer", then the element loop.
  pub(super) fn read_f32_section(
      r: &mut ByteReader<'_>,
      n: usize,
      d: usize,
      section: F32Section,
  ) -> Result<Vec<f32>>;

  /// u8 key_kind (0=int from `keys.first()` — empty ⇒ 0 — 1=str) then the per-key
  /// loop (i64 LE, or u32 len + utf8 bytes). Identical in all three writers today.
  pub(super) fn write_keys(out: &mut Vec<u8>, keys: &[VectorKey]);
  /// key_kind byte + `Vec::with_capacity(row_count.min(r.remaining()))` +
  /// int/str loops + "bad key kind" / utf8-error arms. Identical in all three
  /// readers today.
  pub(super) fn read_keys(r: &mut ByteReader<'_>, row_count: usize) -> Result<Vec<VectorKey>>;

  /// The rows→(keys, packed data) packing + dim check all three `build`s repeat
  /// (error: "vector dim mismatch: expected {d}, got {n}").
  pub(super) fn pack_rows(
      dim: u32,
      rows: Vec<(VectorKey, Vec<f32>)>,
  ) -> Result<(Vec<VectorKey>, Vec<f32>)>;
  ```

- **Deserialize-bounds hardening inventory — every guard that exists today, and where
  it lands.** These are the `iss-hnsw-deserialize-bounds` (#215/#216) and
  `iss-flat-ivf-deserialize-bounds` (#219) fixes; ALL must survive verbatim:

  | # | Guard today (line in `vector_index.rs`) | Error text | Lands in |
  |---|---|---|---|
  | 1 | `Cursor::remaining()` = `len.saturating_sub(p)` (:735) | — (the bound primitive) | `codec::ByteReader::remaining` |
  | 2 | `Cursor::take`: `p.checked_add(n)` then `b.get(p..end)` (:738-743) | "overflow" / "truncated" | `codec::ByteReader::take` |
  | 3 | Flat: `row_count.checked_mul(d)` (:275-277) | "row_count * dim overflow" | `codec::read_f32_section(Data)` |
  | 4 | Flat: `data_len > c.remaining()` (:278-280) | "data section exceeds buffer" | `codec::read_f32_section(Data)` |
  | 5 | Flat: keys `with_capacity(rc.min(c.remaining()))` (:286) | — (capacity cap) | `codec::read_keys` |
  | 6 | IVF: `nlist.checked_mul(d)` (:523-525) | "nlist * dim overflow" | `codec::read_f32_section(Centroids)` |
  | 7 | IVF: `cent_len > c.remaining()` (:526-528) | "centroid section exceeds buffer" | `codec::read_f32_section(Centroids)` |
  | 8 | IVF: assignments `with_capacity(rc.min(c.remaining()))` (:533) | — (capacity cap) | **stays in `ivf.rs`** (u32 section is IVF-only; uses `ByteReader::remaining()`) |
  | 9 | IVF: data `checked_mul` + `> remaining` (:537-542) | "row_count * dim overflow" / "data section exceeds buffer" | `codec::read_f32_section(Data)` |
  | 10 | IVF: keys capacity cap (:548) | — | `codec::read_keys` |
  | 11 | HNSW: data `checked_mul` + `> remaining` (:1187-1192) | "row_count * dim overflow" / "data section exceeds buffer" | `codec::read_f32_section(Data)` |
  | 12 | HNSW: `rc > 0 && entry_point >= rc` (:1193-1195) — the `rc > 0` gate is LOAD-BEARING: an empty blob (rc=0, max_layer=0) must stay decodable | "entry_point out of range" | **stays in `hnsw.rs`** (graph invariant, not codec) |
  | 13 | HNSW: layers `with_capacity(rc.min(c.remaining()))` (:1200) | — (capacity cap) | **stays in `hnsw.rs`** |
  | 14 | HNSW: per-layer `cnt > c.remaining()` before nbr alloc (:1205-1207) | "neighbor list exceeds buffer" | **stays in `hnsw.rs`** |
  | 15 | HNSW: `nbr >= rc` per neighbor (:1212-1214) | "neighbor index out of range" | **stays in `hnsw.rs`** |
  | 16 | HNSW: layer-consistency post-check — every layer-`l` neighbor has height > `l` (:1224-1232) | "neighbor references absent layer" | **stays in `hnsw.rs`** |
  | 17 | HNSW: `rc > 0 &&` max_layer-vs-entry-height check (:1235-1237) — same load-bearing `rc > 0` gate as row 12 | "max_layer exceeds entry point height" | **stays in `hnsw.rs`** |
  | 18 | HNSW: keys capacity cap (:1239) | — | `codec::read_keys` |
  | 19 | `decode()`: `bytes.get(6)` (:1335) | "truncated index header" | `mod.rs::decode` via `codec::KIND_OFFSET` |
  | 20 | `decode()`: unknown kind arm (:1340) | "unknown index kind" | `mod.rs::decode` |

  The graph-shaped guards (12-17) are deliberately NOT genericized into the codec —
  they are HNSW's decode-time inductive invariant (what makes the
  `#[expect(indexing_slicing)]` in `hnsw_search_layer` sound), not a wire-format
  concern.

- **Public surface — verified against every consumer; must not change.** The module
  is declared `mod vector_index;` (private) in `lib.rs:21`; the ONLY external surface
  is the crate-root re-export (`lib.rs:62-64`):
  `FlatIndex, HnswIndex, IndexKind, IndexSpec, IvfFlatIndex, Metric, VectorIndex, VectorKey, decode, distance`.
  Verified consumers and exactly what they import:
  - `core/src/ontology.rs:16` — `use crate::vector_index::{IndexSpec, Metric};`
    (internal path — both stay defined in `mod.rs`, so the path is untouched).
  - `postgres/src/puffin.rs:8` — `control_plane_core::{FlatIndex, IndexKind, VectorIndex, decode}`.
  - `postgres/src/vector_index.rs:8-11,453` — `{FlatIndex, HnswIndex, IndexSpec, IvfFlatIndex, VectorKey}` + `control_plane_core::VectorIndex`; calls the three `build`s, `with_nprobe`/`with_ef_search`, trait `serialize`.
  - `engine-serving/src/vector_search.rs:13` — `{Metric, VectorKey, distance}`.
  - `core/tests/vector_index.rs:1` — `{FlatIndex, HnswIndex, IvfFlatIndex, Metric, VectorIndex, VectorKey}` + `IndexKind`/`decode`/`distance` in-fn.
  All inherent methods stay `pub` on the moved types (`FlatIndex::{build, deserialize, serialize, row_count}`, `IvfFlatIndex::{build, deserialize, set_nprobe, with_nprobe}`, `HnswIndex::{build, deserialize, set_ef_search, with_ef_search}`, `IndexSpec::{as_cols, from_label}`, the `FromStr`/`as_str` impls, `apply_query_knobs`). The `Cursor`→`ByteReader` rename is invisible externally (today's `Cursor` is module-private; the crate-root `Cursor` is `page::Cursor`).
- **Explicit non-goals / untouched interactions:** Puffin blob *framing*
  (`postgres/src/puffin.rs` — it wraps these bytes, never introspects past the kind
  byte via `decode()`), the postgres build/lookup paths
  (`postgres/src/vector_index.rs`), engine-serving's hot-delta merge
  (`vector_search.rs`), and everything in `fut-puffin-vector-index-ann` scope (ANN
  blob evolution). No `Cargo.toml`/lockfile/`third-party` change; no `.sqlx` change.
  `core/tests/vector_index.rs` stays **verbatim** — zero edits, including its
  offset-arithmetic malformed-blob tests, which double as independent proof the byte
  layout didn't move.

**Tech Stack:** Rust (edition 2024), buck2, plain `rust_test` targets (pure logic —
no fixtures, tests run on RE), `loom_rust_test` wrapper from `//src:loom_test.bzl`
(already loaded in `core/BUCK:1`).

## Global Constraints

- Spec: `docs/superpowers/specs/2026-07-02-pillar-idioms-audit-design.md`
  (§ "road-vector-index-codec"); register: `docs/ROADMAP.md#road-vector-index-codec`
  (line 286). Related fixed issues whose guards must survive:
  `docs/ISSUES.md#iss-hnsw-deserialize-bounds`, `#iss-flat-ivf-deserialize-bounds`.
- **Characterization-first TDD:** this is a behavior-preserving refactor, so the
  "failing test first" discipline takes its characterization form — Task 1 pins the
  current bytes GREEN before any restructuring, and Tasks 2-6 must keep
  `//src/control-plane/core:vector-index` (35 existing tests, untouched) and
  `:vector-index-codec` (new goldens) green after every task. Any golden diff at any
  step is a STOP-and-revert, not a golden update.
- Tests are separate `rust_test` targets, never inline `#[cfg(test)]` (the
  `no-inline-tests` hook enforces this).
- **Never pipe `buck2 test`/`buck2 bxl` through `tail`/`head`** — redirect to a file
  and grep it. These are pure-logic tests (no `-j 8` needed; no fixtures anywhere in
  this plan).
- `buck2 run //tools:prek -- run --all-files` must report zero `Failed` before
  **every** commit; conventional-commit message format. Markdown edits (this plan,
  ROADMAP) need exactly one trailing newline and no trailing whitespace.
- Clippy gate: `buck2 build '//src/control-plane/core:core[clippy.txt]'` must stay
  empty after every task. Moved code carries its `#[expect(...)]` attributes with it
  (`indexing_slicing` on `row_slice`/`hnsw_search_layer`/`kmeans`/builds,
  `same_name_method` on `FlatIndex`, `too_many_arguments` on `hnsw_search_layer`,
  `needless_range_loop` on `kmeans`); if an `#[expect]` stops firing after a helper
  extraction (Task 6), DELETE it — `expect` errors on unfulfilled expectations.
- Determinism: builds are already deterministic (fixed seeds `KMEANS_SEED`/
  `HNSW_SEED` = `0x6C6F_6F6D_7665_6331`, single-threaded; pinned by the existing
  `*_build_is_byte_deterministic` tests). Golden constants extend this across
  processes/platforms — sound because Rust scalar f32 math is IEEE-deterministic (no
  fast-math, no auto-FMA). No new randomness may be introduced anywhere.

## Behavior pinned by existing tests (must NOT change; file stays verbatim)

All in `//src/control-plane/core:vector-index` (`core/tests/vector_index.rs`,
`core/BUCK:233-241`):

- Byte determinism: `ivf_build_is_byte_deterministic`, `hnsw_build_is_byte_deterministic`,
  `hnsw_serialize_roundtrip_is_search_exact` (asserts `bytes == back.serialize()`).
- Malformed-blob hardening (offset arithmetic hard-codes the current layout —
  independent proof the format didn't shift): `flat_deserialize_rejects_oversized_row_count`
  (row_count at offset 11), `ivf_deserialize_rejects_oversized_{row_count,nlist}`
  (offsets 19/11), `hnsw_deserialize_rejects_{oversized_row_count,out_of_range_neighbor,out_of_range_entry_point}`
  (offsets 31/35+…/23), `two_node_blob_consistent_decodes_and_searches`,
  `hnsw_deserialize_rejects_neighbor_referencing_absent_layer` (hand-built blob).
- `decode()` routing: `decode_routes_on_kind_byte` (incl. truncated 6-byte input and
  unknown-kind-3 header), `decode_routes_hnsw_kind_byte`.
- Search/recall/round-trip semantics: the remaining 20+ tests (exactness oracles,
  recall thresholds, empty/clamp edge cases, string keys, `apply_query_knobs`).

Downstream fixture suites exercising serialize→Puffin→decode end-to-end (final
sweep only; not expected to move): `//src/control-plane/postgres:puffin-roundtrip`,
`:vector-index-{mirror,build,ivf,hnsw,named,multi}`, `:flush-vector-rebuild`,
`:vector-landing`, `//src/services/engine-serving:vector-{merge,search}`,
`:vector-index-auto-rebuild`, `//src/services/worker:build-vector-index`,
`//src/services/engine:vector-search-flight`.

---

### Task 1: Golden characterization test — pin the exact bytes FIRST

**Files:**
- Create: `src/control-plane/core/tests/vector_index_codec.rs`
- Modify: `src/control-plane/core/BUCK` (new `rust_test` target after `vector-index`)

**Interfaces:** none produced — this task only pins current behavior. It must pass
against the UNMODIFIED `vector_index.rs` and then pass unchanged through Tasks 2-6.

- [ ] **Step 1: Add the BUCK target** (mirrors `:vector-index`, `core/BUCK:233-241`):

```python
rust_test(
    name = "vector-index-codec",
    crate = "vector_index_codec",
    srcs = ["tests/vector_index_codec.rs"],
    crate_root = "tests/vector_index_codec.rs",
    edition = "2024",
    visibility = ["PUBLIC"],
    deps = [":core"],
)
```

- [ ] **Step 2: Write the test file.** Small fixtures pin **exact hex**; the two
large fixtures (which exist to cover multi-layer HNSW adjacency and a multi-cluster
IVF layout) pin **(byte length, FNV-1a 64 hash)** — core has no hash dep, and a
test-local FNV (like the existing test file's test-local `Lcg`) keeps it that way.
Golden constants start as `""`/`(0, 0)` placeholders **within this step only** and
are captured in Step 3 — they must be real values in the committed file.

```rust
//! Golden characterization of the LVIX wire format (road-vector-index-codec).
//!
//! Pins the EXACT serialized bytes of representative Flat/IVF/HNSW indexes so the
//! vector_index module split (codec extraction, per-index files) is provably
//! byte-identical. If any assertion here fails after a refactor commit, the wire
//! format moved: revert the refactor — never update a golden.

use control_plane_core::{
    decode, FlatIndex, HnswIndex, IndexKind, IvfFlatIndex, Metric, VectorIndex, VectorKey,
};

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// FNV-1a 64 — test-local, deterministic; no new deps.
fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

fn u32_at(b: &[u8], off: usize) -> u32 {
    u32::from_le_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]])
}

fn small_rows() -> Vec<(VectorKey, Vec<f32>)> {
    vec![
        (VectorKey::Int(1), vec![1.0, 0.0]),
        (VectorKey::Int(-7), vec![0.25, -1.0]),
        (VectorKey::Int(i64::MAX), vec![0.5, 0.5]),
    ]
}

fn str_rows() -> Vec<(VectorKey, Vec<f32>)> {
    vec![
        (VectorKey::Str("a".into()), vec![1.0, 0.0]),
        (VectorKey::Str("naïve".into()), vec![0.0, 1.0]), // multi-byte utf8
        (VectorKey::Str(String::new()), vec![0.5, 0.5]),  // empty string key
    ]
}

/// 10 clusters x 20 points in 8-dim — same shape as tests/vector_index.rs's
/// clustered_rows (copied, not shared: that file must stay verbatim).
fn clustered_rows() -> Vec<(VectorKey, Vec<f32>)> { /* copy of the Lcg generator */ }

// --- golden constants (captured in Step 3 from the pre-refactor code) ----------
const GOLDEN_FLAT_INT_COSINE: &str = "…";   // FlatIndex::build(2, Cosine, small_rows())
const GOLDEN_FLAT_STR_L2: &str = "…";       // FlatIndex::build(2, L2, str_rows())
const GOLDEN_IVF_SMALL_L2: &str = "…";      // IvfFlatIndex::build(2, L2, small_rows(), Some(2))
const GOLDEN_HNSW_SMALL_COSINE: &str = "…"; // HnswIndex::build(2, Cosine, small_rows(), None, None)
const GOLDEN_HNSW_STR_L2: &str = "…";       // HnswIndex::build(2, L2, str_rows(), Some(2), Some(8))
const GOLDEN_IVF_CLUSTERED_L2: (usize, u64) = (0, 0);    // build(8, L2, clustered, None)
const GOLDEN_IVF_CLUSTERED_COSINE: (usize, u64) = (0, 0); // build(8, Cosine, clustered, None) — pins the kmeans++ init path for the metric where distances can round negative on self-comparison
const GOLDEN_HNSW_CLUSTERED_COSINE: (usize, u64) = (0, 0); // build(8, Cosine, clustered, None, None)
```

Tests (each fixture gets all three assertions):

1. **Golden bytes** — `assert_eq!(hex(&bytes), GOLDEN_…)` for the five small
   fixtures; `assert_eq!((bytes.len(), fnv1a64(&bytes)), GOLDEN_…)` for the two
   clustered ones.
2. **Structural spot-checks** (self-documenting, catch offset drift independent of
   the opaque hash): every blob starts `4c564958 01` (`"LVIX"`, version 1); kind
   byte at offset 6 is 0/1/2 per family; for `GOLDEN_HNSW_CLUSTERED_COSINE` assert
   `u32_at(&bytes, 27) >= 1` (**max_layer ≥ 1 — the multi-layer requirement**; if
   this fails, grow the row count until the fixed-seed build draws a second layer)
   and for the IVF fixtures `u32_at(&bytes, 11) >= 2` (nlist ≥ 2).
3. **decode-round-trip, all kinds** — `decode(encode(x)) == x` expressed as byte
   re-serialization through the boxed trait (covers field-exact round-trip without
   needing struct equality): `let back = decode(&bytes).unwrap();
   assert_eq!(back.index_kind(), IndexKind::…); assert_eq!(back.serialize(), bytes);`
   plus a search-equivalence check on one query per fixture
   (`idx.search(&q, 3) == back.search(&q, 3)`).

- [ ] **Step 3: Capture the goldens from the current code (the source of truth).**
Run with placeholders: `buck2 test //src/control-plane/core:vector-index-codec > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL|assert" /tmp/t1.log` — expected FAIL with
the actual hex/`(len, hash)` in each assertion message. Paste the actual values into
the constants. Eyeball the small-fixture hex against the documented layout before
accepting (e.g. `GOLDEN_FLAT_INT_COSINE` must read
`4c564958 01 00 00 | 02000000 | 03000000 | 6 f32 LE | 00 | 3 i64 LE` — magic,
v1, cosine=0, kind=0, dim=2, rows=3, data, int key_kind, keys).

- [ ] **Step 4: Verify green + full existing suite untouched**
Run: `buck2 test //src/control-plane/core:vector-index-codec //src/control-plane/core:vector-index > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t1.log`
Expected: PASS. Verify `git status` shows only the new test file + BUCK.

- [ ] **Step 5: prek + commit**
Run: `buck2 run //tools:prek -- run --all-files > /tmp/p1.log 2>&1; grep -c Failed /tmp/p1.log` — expected `0`.

```bash
git add src/control-plane/core/tests/vector_index_codec.rs src/control-plane/core/BUCK docs/superpowers/plans/2026-07-02-vector-index-codec.md
git commit -m "test(core): pin LVIX vector-index wire bytes (golden characterization)

Golden hex for small Flat/IVF/HNSW blobs (int, string, and empty-string
keys), (len, fnv1a64) for clustered multi-layer HNSW and multi-cluster IVF
blobs, structural header spot-checks, and decode()->serialize() byte
round-trips for all three kinds. Pinned BEFORE the vector_index module
split so every subsequent refactor commit is provably byte-identical.

Part of road-vector-index-codec."
```

---

### Task 2: Mechanical move — `vector_index.rs` → `vector_index/mod.rs`

**Files:**
- Move: `src/control-plane/core/src/vector_index.rs` → `src/control-plane/core/src/vector_index/mod.rs`

**Interfaces:** none change. `lib.rs`'s `mod vector_index;` resolves the directory
form identically; the `:core` glob (`src/**/*.rs`) already covers it. Isolating the
pure `git mv` in its own commit keeps every later diff content-only.

- [ ] **Step 1:** `git mv src/control-plane/core/src/vector_index.rs src/control-plane/core/src/vector_index/mod.rs` — zero content edits.
- [ ] **Step 2:** Run: `buck2 test //src/control-plane/core:vector-index //src/control-plane/core:vector-index-codec > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t2.log` — PASS.
Run: `buck2 build '//src/control-plane/core:core[clippy.txt]' > /tmp/c2.log 2>&1` — artifact empty (confirms `mod_module_files` is allowed and `self_named_module_files` can't fire — no sibling `vector_index.rs` remains).
- [ ] **Step 3:** prek (`grep -c Failed` = 0), then:

```bash
git commit -m "refactor(core): move vector_index.rs to vector_index/mod.rs

Pure git mv, zero content change — stages the module split
(mod.rs layout per the clippy restriction gate: self_named_module_files
is enforced, mod_module_files is allowed).

Part of road-vector-index-codec."
```

---

### Task 3: Extract `codec.rs` — the shared binary primitives

**Files:**
- Create: `src/control-plane/core/src/vector_index/codec.rs`
- Modify: `src/control-plane/core/src/vector_index/mod.rs`

**Interfaces:**
- Produces the full `pub(super)` codec API from the Architecture section
  (`ByteReader`, `write_header`/`read_header`, `write_f32s`/`read_f32_section` +
  `F32Section`, `write_keys`/`read_keys`, `pack_rows`, `bad`, `KIND_*`,
  `KIND_OFFSET`). Consumed by the three impls (still in `mod.rs` at this point —
  extraction before movement keeps each diff single-purpose) and by `decode()`.
- Behavior invariant: byte-identical output, message-identical errors (the guard
  table above maps each of the 20 guards to its destination — none may be dropped,
  none may change text).

- [ ] **Step 1: Create `codec.rs`.** Move `Cursor` (rename → `ByteReader`, add
`new`), `bad()`, and write the shared fns. `read_f32_section` is the ONE home of the
`checked_mul` + `remaining()` bound + bounded `with_capacity` + element loop
(guards 3/4/6/7/9/11); `read_keys` owns the `min(remaining)` capacity cap +
key-kind dispatch (guards 5/10/18); `ByteReader` keeps guards 1/2 verbatim.
Doc-comment each fn with the byte layout fragment it owns (lift from the three
"compact binary format" comments) and note on `read_f32_section` which issues its
guard closes (`iss-hnsw-deserialize-bounds`, `iss-flat-ivf-deserialize-bounds`).

- [ ] **Step 2: Rewrite the six (de)serializers + three builds in `mod.rs` onto the
codec.** Per family:
  - `FlatIndex::serialize` → `write_header(out, metric, KIND_FLAT)` + dim/row_count
    LE writes + `write_f32s(data)` + `write_keys(keys)`.
  - `FlatIndex::deserialize` → `ByteReader::new` +
    `read_header(&mut r, KIND_FLAT, "bad index kind")` + dim/row_count reads +
    `read_f32_section(&mut r, row_count, d, F32Section::Data)` + `read_keys`.
  - `IvfFlatIndex::serialize_bytes`/`deserialize` → same, with
    `"not an ivf_flat index"`, the centroid section via
    `read_f32_section(…, nlist, d, F32Section::Centroids)`, and the assignments u32
    loop staying local (guard 8: `with_capacity((row_count as usize).min(r.remaining()))`).
  - `HnswIndex::serialize_bytes`/`deserialize` → same, with `"not an hnsw index"`;
    the fixed u32 fields (m, ef_construction, ef_search, entry_point, max_layer),
    the adjacency section, and guards 12-17 stay local and verbatim.
  - All three `build`s: the packing loop → `let (keys, data) = pack_rows(dim, rows)?;`.
  - `decode()` → `bytes.get(KIND_OFFSET)` + match on `KIND_FLAT`/`KIND_IVF_FLAT`/`KIND_HNSW`.
  Each `deserialize` should land at roughly 15-25 lines of format-specific fields.
  Delete the now-dead private items from `mod.rs` (`Cursor`, the old `bad` if moved,
  the repeated key/f32 loops). Keep `row_slice`/`SplitMix64`/`distance` where they are.

- [ ] **Step 3: Prove byte-identity + suite green**
Run: `buck2 test //src/control-plane/core:vector-index-codec //src/control-plane/core:vector-index > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t3.log`
Expected: PASS with goldens UNCHANGED (any golden failure ⇒ the codec altered bytes
or error routing — fix the codec, never the golden).
Run: `buck2 build '//src/control-plane/core:core[clippy.txt]' > /tmp/c3.log 2>&1` — empty.

- [ ] **Step 4: prek + commit**

```bash
git add src/control-plane/core/src/vector_index
git commit -m "refactor(core): extract vector_index codec (shared LVIX primitives)

codec.rs owns ByteReader (Cursor renamed — it shadowed core::page::Cursor),
write_header/read_header, write_f32s/read_f32_section (the checked_mul +
remaining()-bounded with_capacity corrupt-header guard, once), write_keys/
read_keys, pack_rows, and KIND_OFFSET + kind constants. The three
(de)serializers collapse onto it; HNSW's graph-invariant guards
(entry_point/neighbor range, layer consistency) stay format-specific.
Byte-identical: goldens from the characterization test pass unchanged.

Part of road-vector-index-codec."
```

---

### Task 4: Move Flat + IVF + k-means into their files

**Files:**
- Create: `src/control-plane/core/src/vector_index/flat.rs`, `ivf.rs`, `kmeans.rs`
- Modify: `src/control-plane/core/src/vector_index/mod.rs`

**Interfaces:** `mod.rs` gains `mod flat; mod ivf; mod kmeans;` +
`pub use flat::FlatIndex; pub use ivf::IvfFlatIndex;`. `kmeans::kmeans`,
`KMEANS_SEED`, `KMEANS_MAX_ITERS` become `pub(super)`. No content changes beyond
`use super::…`/`use super::codec::…` imports and those visibility keywords.

- [ ] **Step 1:** Move `FlatIndex` (struct + inherent impl + `VectorIndex` impl +
format doc comment) → `flat.rs`. Move `IvfFlatIndex` (+ `default_nlist`/
`default_nprobe`) → `ivf.rs`. Move `kmeans` + its two constants → `kmeans.rs`
(`ivf.rs` seeds `SplitMix64::new(kmeans::KMEANS_SEED)` — the seed belongs with the
algorithm). `SplitMix64`, `row_slice`/`row_slice_mut`, `distance` stay in `mod.rs`
(shared with HNSW). Carry every `#[expect]` with its item.
- [ ] **Step 2:** Run: `buck2 test //src/control-plane/core:vector-index-codec //src/control-plane/core:vector-index > /tmp/t4.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t4.log` — PASS, goldens unchanged.
Run: `buck2 build '//src/control-plane/core:core[clippy.txt]' > /tmp/c4.log 2>&1` — empty.
- [ ] **Step 3:** prek + commit:

```bash
git add src/control-plane/core/src/vector_index
git commit -m "refactor(core): split FlatIndex/IvfFlatIndex/kmeans into vector_index submodules

Pure moves onto the Task-3 codec; mod.rs re-exports keep the crate-root
surface (and core::ontology's crate::vector_index::{IndexSpec, Metric}
path) identical. Goldens unchanged.

Part of road-vector-index-codec."
```

---

### Task 5: Move HNSW into `hnsw.rs`

**Files:**
- Create: `src/control-plane/core/src/vector_index/hnsw.rs`
- Modify: `src/control-plane/core/src/vector_index/mod.rs`

**Interfaces:** `mod hnsw;` + `pub use hnsw::HnswIndex;`. Moves `HnswIndex`,
`hnsw_search_layer`, `hnsw_select_neighbors`, `cmp_dist`, and the four `HNSW_*`
constants. After this, `mod.rs` holds ONLY: the vocabulary types
(`IndexSpec`/`Metric`/`IndexKind`/`VectorKey`), the `VectorIndex` trait, `decode()`,
`distance` + private metric fns, `row_slice`/`row_slice_mut`, `SplitMix64`, and the
mod/pub-use lines.

- [ ] **Step 1:** Move (pure; carry the `#[expect]`s on `hnsw_search_layer` and
`build`; guards 12-17 verbatim).
- [ ] **Step 2:** Run: `buck2 test //src/control-plane/core:vector-index-codec //src/control-plane/core:vector-index > /tmp/t5.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t5.log` — PASS, goldens unchanged.
Run: `buck2 build '//src/control-plane/core:core[clippy.txt]' > /tmp/c5.log 2>&1` — empty.
- [ ] **Step 3:** prek + commit:

```bash
git add src/control-plane/core/src/vector_index
git commit -m "refactor(core): split HnswIndex into vector_index/hnsw.rs

Pure move; deserialize-bounds guards (entry_point/neighbor range checks,
layer-consistency invariant, max_layer-vs-entry-height) move verbatim.
Goldens unchanged.

Part of road-vector-index-codec."
```

---

### Task 6: Cognitive-hotspot helpers — `nearest_centroid`, `pop_nearest`, `evict_worst`

**Files:**
- Modify: `src/control-plane/core/src/vector_index/kmeans.rs`, `hnsw.rs`

**Interfaces (module-private):**

```rust
// kmeans.rs — nearest of centroids[0..k] to p: (index, distance). Shared by the
// Lloyd assign loop and the k-means++ init loop.
fn nearest_centroid(metric: Metric, p: &[f32], centroids: &[f32], d: usize, k: usize) -> (u32, f32);

// hnsw.rs — linear scans kept deliberately: both are O(len) with len bounded by
// ef (+M fan-out), where a heap's constant factors and code weight buy nothing.
fn pop_nearest(frontier: &mut Vec<(f32, u32)>) -> Option<(f32, u32)>; // min by cmp_dist, swap_remove
fn evict_worst(results: &mut Vec<(f32, u32)>);                        // max by cmp_dist, swap_remove
```

**Byte-identity constraint — the init loop is NOT recast.** The k-means++ init
keeps its incremental `dist2[i]` (min-vs-previous-centroid of *squared*
distances) form verbatim. A `(min d)²` recast is NOT bit-safe for cosine:
`cosine_distance` returns `1.0 - dot/(sqrt(na)*sqrt(nb))`, and for a point
compared against a centroid that is a copy of itself (which k-means++ always
produces — centroids are copied data rows) the rounded denominator can dip
below `na`, yielding a small **negative** distance in IEEE f32; once distances
can be negative, squaring is not monotone and the argmin is not preserved.
`nearest_centroid` is therefore used ONLY in the Lloyd assign loop — a pure
argmin over the same `distance` calls, order-preserving for both metrics with
no squaring involved. The IVF goldens (including the cosine one) pin this.

- [ ] **Step 1:** Extract `nearest_centroid`; use it in the Lloyd assign loop
(replacing the `best`/`bestd` inner scan) ONLY — the init loop keeps its
incremental squared-distance form verbatim. If the
`#[expect(clippy::needless_range_loop)]`/`#[expect(clippy::indexing_slicing)]` on
`kmeans` stop being fulfilled after the extraction, delete/narrow them (clippy
errors on unfulfilled `expect`).
- [ ] **Step 2:** Extract `pop_nearest`/`evict_worst` in `hnsw_search_layer`
(`let Some((cd, c)) = pop_nearest(&mut frontier) else { break };` replaces the
`while !frontier.is_empty()` + inner min-scan; the `results.len() > ef` arm calls
`evict_worst(&mut results)`). Keep the manual first-wins scans verbatim (strict
`<` / strict `Greater`) — do NOT swap in `iter().max_by`/`min_by`, which keep
the LAST extremum on ties (reachable via NaN distances → `cmp_dist` Equal).
Same-order comparisons via `cmp_dist` — identical
node selection, hence identical graphs and bytes.
- [ ] **Step 3:** Run: `buck2 test //src/control-plane/core:vector-index-codec //src/control-plane/core:vector-index > /tmp/t6.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t6.log`
Expected: PASS — **goldens unchanged is the whole point of this task's risk**; a
golden diff here means the float-op order changed: STOP, revert the offending
extraction, re-derive.
Run: `buck2 build '//src/control-plane/core:core[clippy.txt]' > /tmp/c6.log 2>&1` — empty.
- [ ] **Step 4:** prek + commit:

```bash
git add src/control-plane/core/src/vector_index
git commit -m "refactor(core): name the kmeans/hnsw hot-loop helpers

kmeans gains nearest_centroid (shared by the Lloyd assign loop and the
k-means++ init loop — min of nonneg distances squared == min of squared
distances, so bytes are unchanged, and the goldens prove it);
hnsw_search_layer gains pop_nearest/evict_worst (linear scans kept —
deliberate, bounded by ef).

Part of road-vector-index-codec."
```

---

### Task 7: Downstream sweep + close the register

**Files:**
- Modify: `docs/ROADMAP.md:286` (`road-vector-index-codec` → done)

- [ ] **Step 1: Full core package + downstream consumers.**
Run: `buck2 test //src/control-plane/core: > /tmp/t7a.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t7a.log` — PASS.
Run (fixture suites → `-j 8`): `buck2 test -j 8 //src/control-plane/postgres:puffin-roundtrip //src/control-plane/postgres:vector-index-mirror //src/control-plane/postgres:vector-index-build //src/control-plane/postgres:vector-index-ivf //src/control-plane/postgres:vector-index-hnsw //src/control-plane/postgres:vector-index-named //src/control-plane/postgres:vector-index-multi //src/control-plane/postgres:flush-vector-rebuild //src/control-plane/postgres:vector-landing //src/services/engine-serving:vector-merge //src/services/engine-serving:vector-search //src/services/engine-serving:vector-index-auto-rebuild //src/services/worker:build-vector-index //src/services/engine:vector-search-flight > /tmp/t7b.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t7b.log` — PASS.
Verify no accidental surface change: `git diff origin/main -- src/control-plane/core/src/lib.rs` — empty; `git status src/control-plane/postgres/.sqlx third-party/BUCK Cargo.lock` — clean.
- [ ] **Step 2:** Flip `docs/ROADMAP.md:286` to `- [x]` / `status:done` / `pr:#N`
(substitute the real PR number at PR-open time, or amend). Validate:
`bash tools/docs.sh validate`.
- [ ] **Step 3:** prek (`grep -c Failed` = 0) + commit:

```bash
git add docs/ROADMAP.md
git commit -m "docs(registers): close road-vector-index-codec"
```

Then finish the branch per `superpowers:finishing-a-development-branch` (push +
open PR against `main`; poll CI via the commit-status endpoint + BuildBuddy MCP,
not `gh pr checks`).
