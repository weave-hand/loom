# Puffin-backed Vector Index (flat/exact, engine-side) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give loom the ability to precompute an exact vector index over a table's `vector(N)` column, store it as an Apache Puffin sidecar bound to a snapshot via a Postgres-mirror row, and answer an engine-side exact k-NN query that merges the cold precomputed index with the table's un-flushed (hot) inline rows.

**Architecture:** A swappable `VectorIndex` trait + a `FlatIndex` (exact brute-force) first impl live in `core` (pure). The serialized index rides an Apache **Puffin** blob (`loom-vector-index-v1`) written to the object store via iceberg-rust's `puffin` module. A new `iceberg_mirror.vector_index` row binds `(table, column, covered_snapshot) → puffin_path` and is written in the same transaction as a lineage event. A **build primitive** in `postgres` reads all vectors live at snapshot `S` over the serving read path (cold Parquet ∪ hot inline), builds the index, writes the sidecar, and commits the mirror row + lineage. The build is triggered by a `build_vector_index` queue job whose worker handler calls a new `EngineControl` gRPC RPC (the worker is zero-pool; the engine owns Postgres + object store). The **query** is a new loom-native Flight `do_get` ticket handled engine-side: read the bound index (cold top-k) + the inline rows born after `S` (hot top-k), merge to the global top-k.

**Tech Stack:** Rust 2024, buck2, DataFusion, Arrow 58, iceberg-rust (pinned git rev `148afc50…`, ships `iceberg::puffin`), sqlx compile-time queries, tonic/Arrow Flight, hermetic Postgres fixtures.

## Global Constraints

- **Tests are `rust_test` integration targets only** — never inline `#[cfg(test)]`. Put each test in a sibling `tests/<name>.rs` wired as its own target in the crate's `BUCK`. The `no-inline-tests` prek hook fails the build otherwise.
- **Fixture tests (hermetic Postgres / object store) MUST use `loom_fixture_test`** (loaded from the crate's `defs.bzl`), not a bare `rust_test`, or they route to RE and fail as root. Pure-logic tests use `rust_test` (loaded from `//src:loom_test.bzl`).
- **Clippy is strict** (`pedantic` + `restriction` groups). Enforced panic-safety lints include `unwrap_used`, `expect_used`, `indexing_slicing`, `panic`, `todo`, `get_unwrap`. Production lib/bin code must avoid these; use `?`, `.get(i).ok_or(...)`, slice patterns. Test code is exempt from panic-safety lints (the `loom_rust_test`/`loom_fixture_test` wrappers inject the allows). Local silencing uses `#[expect(lint, reason = "...")]`.
- **Element type is fixed `f32`.** `vector(N)` is stored as Arrow `List<Float32>` / Iceberg `list<float>`. Dimension `N` is fixed per column.
- **Metrics: Cosine (default) and L2.** The metric is a property of the built index (recorded in blob metadata + mirror row), not a per-query choice.
- **Blob type string is exactly `loom-vector-index-v1`.**
- **Append-only correctness invariant:** the index covers everything live at `S`; the hot delta is exactly inline rows with `begin_snapshot > S` and live at `Q`. A row flushed between `S` and `Q` is in the index (live at `S`) and excluded from the delta (`begin > S`) — counted once. (Updates/deletes are deferred; holds because loom is append-only.)
- **Compile-time SQL:** new production queries in `postgres` use `sqlx::query!`/`query_scalar!`; after adding/changing them run `tools/sqlx-prepare.sh` and commit the `.sqlx/` change. Dynamic-relation queries (`inline_<tid>`) use runtime `sqlx::query(AssertSqlSafe(...))` exactly as `iceberg_inline.rs` already does (no `.sqlx` entry).
- **`buck2 test //src/...` must stay green;** all existing serving / inline-union behaviour unchanged.
- **Commit messages: Conventional Commits** (`feat:`, `test:`, `fix:`, `chore:`, `docs:`) — the `conventional-commit` commit-msg hook enforces this.
- **Markdown** (if any `.md` touched): exactly one trailing newline, no trailing whitespace.
- **Do not run instrumented coverage builds manually**; do not pipe `buck2 test`/`bxl` through `tail`/`head` (redirect to a file and grep).

## File Structure

**New files**

- `src/control-plane/core/src/vector_index.rs` — `Metric`, `VectorKey`, `VectorIndex` trait, `FlatIndex` (build + exact `search` + `serialize`/`deserialize`), `IndexKind`. Pure, no iceberg/arrow.
- `src/control-plane/core/src/vector_index_job.rs` — `BUILD_VECTOR_INDEX_JOB_KIND` const + `BuildVectorIndexJob` payload.
- `src/control-plane/core/tests/vector_index.rs` — pure FlatIndex tests (Cosine/L2/serde).
- `src/control-plane/core/tests/vector_index_job.rs` — payload serde test.
- `src/control-plane/postgres/migrations/0018_vector_index.sql` — `iceberg_mirror.vector_index` table.
- `src/control-plane/postgres/src/puffin.rs` — raw Puffin blob write/read helpers over iceberg-rust + the `FlatIndex ↔ loom-vector-index-v1` (de)serialization wrapper.
- `src/control-plane/postgres/src/vector_index.rs` — the `vector_index` mirror row type, insert + lookup queries (compile-time), and the build primitive `build_vector_index(...)`; plus `inline_delta_batch` query-time hot read.
- `src/control-plane/postgres/tests/puffin_roundtrip.rs` — de-risk fixture/io test.
- `src/control-plane/postgres/tests/vector_index_mirror.rs` — mirror insert/lookup fixture test.
- `src/control-plane/postgres/tests/vector_index_build.rs` — build-primitive fixture test.
- `src/services/engine-serving/src/vector_search.rs` — the cold/hot k-NN merge `vector_search(...)`.
- `src/services/engine-serving/tests/vector_search.rs` — headline merge + no-index fixture test.
- `src/services/engine-wire/tests/vector_search_ticket.rs` — `VectorSearchTicket` JSON round-trip test.
- `src/services/worker/tests/build_vector_index.rs` — job-wiring fixture test.

**Modified files**

- `src/control-plane/core/src/lib.rs` — declare + re-export the two new modules.
- `src/control-plane/core/BUCK` — two new pure `rust_test` targets.
- `src/control-plane/postgres/src/lib.rs` — `pub mod puffin; pub mod vector_index;`.
- `src/control-plane/postgres/BUCK` — new `loom_fixture_test` targets.
- `src/control-plane/postgres/.sqlx/` — regenerated cache (committed).
- `src/services/engine-wire/proto/engine_control.proto` — `BuildVectorIndex` RPC + messages.
- `src/services/engine-wire/src/client.rs` — `build_vector_index` client method.
- `src/services/engine-wire/src/flight.rs` — `VectorSearchTicket` type + `FlightTableClient::vector_search` method.
- `src/services/engine-wire/BUCK` — new `rust_test` target.
- `src/services/engine-serving/src/lib.rs` — `pub mod vector_search; pub use ...`.
- `src/services/engine-serving/BUCK` — new `loom_fixture_test`, add deps if needed.
- `src/services/engine/src/service.rs` — `BuildVectorIndex` RPC impl.
- `src/services/engine/src/flight.rs` — `do_get` vector-search ticket branch + `do_get_vector_search`.
- `src/services/engine/BUCK` — extend a fixture test / add one.
- `src/services/worker/src/handler.rs` — `handle_build_vector_index`.
- `src/services/worker/src/main.rs` — register `BUILD_VECTOR_INDEX_JOB_KIND` + dispatch arm.
- `src/services/worker/BUCK` — new `loom_fixture_test`.
- `docs/ROADMAP.md` — close the item at finish (via `loom-docs-update`).

---

### Task 1: Puffin raw blob round-trip (de-risk)

The one load-bearing external dependency: confirm the pinned iceberg-rust `puffin` module reads/writes a `loom-vector-index-v1` blob byte-exact, footer metadata included. Do this first.

**Files:**
- Create: `src/control-plane/postgres/src/puffin.rs`
- Modify: `src/control-plane/postgres/src/lib.rs` (add `pub mod puffin;`)
- Modify: `src/control-plane/postgres/BUCK` (add `puffin-roundtrip` fixture test)
- Test: `src/control-plane/postgres/tests/puffin_roundtrip.rs`

**Interfaces:**
- Produces:
  - `pub const LOOM_VECTOR_INDEX_BLOB_TYPE: &str = "loom-vector-index-v1";`
  - `pub async fn write_index_blob(file_io: &iceberg::io::FileIO, path: &str, payload: &[u8], snapshot_id: i64, field_id: i32, properties: std::collections::HashMap<String, String>) -> control_plane_core::Result<()>`
  - `pub struct LoadedBlob { pub payload: Vec<u8>, pub properties: std::collections::HashMap<String, String>, pub snapshot_id: i64, pub fields: Vec<i32> }`
  - `pub async fn read_index_blob(file_io: &iceberg::io::FileIO, path: &str) -> control_plane_core::Result<LoadedBlob>`

- [ ] **Step 1: Write the failing test**

`src/control-plane/postgres/tests/puffin_roundtrip.rs`:

```rust
//! De-risk: a `loom-vector-index-v1` Puffin blob round-trips byte-exact
//! (payload + footer metadata) through the pinned iceberg-rust `puffin` module.

use std::collections::HashMap;

use control_plane_postgres::puffin::{
    LOOM_VECTOR_INDEX_BLOB_TYPE, read_index_blob, write_index_blob,
};
use iceberg::io::FileIOBuilder;

#[tokio::test]
async fn puffin_blob_roundtrips_byte_exact() {
    // A local-filesystem FileIO over a tempdir — no Postgres/S3 needed.
    let dir = tempfile::tempdir().unwrap();
    let file_io = FileIOBuilder::new_fs_io().build().unwrap();
    let path = format!("{}/idx.puffin", dir.path().display());

    let payload: Vec<u8> = (0u8..200).collect();
    let mut props = HashMap::new();
    props.insert("index-kind".to_string(), "flat".to_string());
    props.insert("dim".to_string(), "4".to_string());
    props.insert("metric".to_string(), "cosine".to_string());

    write_index_blob(&file_io, &path, &payload, 7, 3, props.clone())
        .await
        .unwrap();

    let loaded = read_index_blob(&file_io, &path).await.unwrap();
    assert_eq!(loaded.payload, payload, "payload must round-trip byte-exact");
    assert_eq!(loaded.snapshot_id, 7);
    assert_eq!(loaded.fields, vec![3]);
    assert_eq!(loaded.properties.get("index-kind").map(String::as_str), Some("flat"));
    assert_eq!(loaded.properties.get("dim").map(String::as_str), Some("4"));
    assert_eq!(loaded.properties.get("metric").map(String::as_str), Some("cosine"));
}
```

> NOTE for implementer: confirm the exact local-FileIO constructor on the pinned rev. `FileIOBuilder::new_fs_io()` is the documented helper; if it differs, use `FileIOBuilder::new("file").build()` or the `LocalFsStorageFactory` path used in `service_runtime::build_storage_factory`. Grep iceberg-rust for `new_fs_io` to confirm.

- [ ] **Step 2: Write the puffin module**

`src/control-plane/postgres/src/puffin.rs`:

```rust
//! Apache Puffin sidecar helpers for loom's vector index. A Puffin file is the
//! standardized Iceberg container for index/stat blobs; loom stores a serialized
//! `FlatIndex` as a single `loom-vector-index-v1` blob. Read/write go through
//! iceberg-rust's `puffin` module over a `FileIO` (object store handle).

use std::collections::HashMap;

use control_plane_core::{ControlPlaneError, Result};
use iceberg::io::FileIO;
use iceberg::puffin::{Blob, CompressionCodec, PuffinReader, PuffinWriter};

/// The custom Puffin blob `type` string for loom's flat vector index. Versioned:
/// a future on-disk format bump becomes `loom-vector-index-v2`.
pub const LOOM_VECTOR_INDEX_BLOB_TYPE: &str = "loom-vector-index-v1";

fn be<E: std::fmt::Display>(e: E) -> ControlPlaneError {
    ControlPlaneError::Backend(e.to_string().into())
}

/// Write `payload` as a single `loom-vector-index-v1` blob to a fresh Puffin file
/// at `path`. `field_id` is the Iceberg field id of the indexed vector column;
/// `snapshot_id` is the covered snapshot; `properties` carry self-describing
/// metadata (dim, metric, index-kind, column, identity-column, row-count, …).
pub async fn write_index_blob(
    file_io: &FileIO,
    path: &str,
    payload: &[u8],
    snapshot_id: i64,
    field_id: i32,
    properties: HashMap<String, String>,
) -> Result<()> {
    let output = file_io.new_output(path).map_err(be)?;
    // Footer kept uncompressed; the blob itself uncompressed (the payload is
    // already a compact packed-f32 format — Lz4/Zstd would add a dep surface for
    // little gain in slice 1).
    let mut writer = PuffinWriter::new(&output, HashMap::new(), false)
        .await
        .map_err(be)?;
    let blob = Blob::builder()
        .r#type(LOOM_VECTOR_INDEX_BLOB_TYPE.to_string())
        .fields(vec![field_id])
        .snapshot_id(snapshot_id)
        .sequence_number(0)
        .data(payload.to_vec())
        .properties(properties)
        .build();
    writer
        .add(blob, CompressionCodec::None)
        .await
        .map_err(be)?;
    writer.close().await.map_err(be)?;
    Ok(())
}

/// A blob read back from a Puffin file: payload bytes + the footer metadata loom
/// cares about.
pub struct LoadedBlob {
    pub payload: Vec<u8>,
    pub properties: HashMap<String, String>,
    pub snapshot_id: i64,
    pub fields: Vec<i32>,
}

/// Read the single `loom-vector-index-v1` blob from the Puffin file at `path`.
/// Errors if the file has no such blob.
pub async fn read_index_blob(file_io: &FileIO, path: &str) -> Result<LoadedBlob> {
    let input = file_io.new_input(path).map_err(be)?;
    let reader = PuffinReader::new(input);
    let meta = reader.file_metadata().await.map_err(be)?;
    let bm = meta
        .blobs()
        .iter()
        .find(|b| b.blob_type() == LOOM_VECTOR_INDEX_BLOB_TYPE)
        .ok_or_else(|| {
            ControlPlaneError::NotFound(format!("no {LOOM_VECTOR_INDEX_BLOB_TYPE} blob in {path}"))
        })?;
    let blob = reader.blob(bm).await.map_err(be)?;
    Ok(LoadedBlob {
        payload: blob.data().to_vec(),
        properties: blob.properties().clone(),
        snapshot_id: blob.snapshot_id(),
        fields: blob.fields().to_vec(),
    })
}
```

> NOTE for implementer: `Blob` uses `TypedBuilder` (`Blob::builder()…build()`). Confirm the builder field method names against the pinned rev (`r#type`, `fields`, `snapshot_id`, `sequence_number`, `data`, `properties`). If `new_output` returns a value whose lifetime must outlive the writer, bind it to a local (as shown).

- [ ] **Step 3: Wire the module + BUCK target**

In `src/control-plane/postgres/src/lib.rs`, add alongside the other `pub mod iceberg_*` lines:

```rust
pub mod puffin;
```

In `src/control-plane/postgres/BUCK`, add (mirror the `iceberg-landing` fixture test; this one needs only iceberg + tempfile + tokio):

```python
loom_fixture_test(
    name = "puffin-roundtrip",
    crate = "puffin_roundtrip",
    srcs = ["tests/puffin_roundtrip.rs"],
    crate_root = "tests/puffin_roundtrip.rs",
    deps = [
        ":postgres",
        "//third-party:iceberg",
        "//third-party:tempfile",
        "//third-party:tokio",
    ],
)
```

> NOTE: this test touches no Postgres, but it is filesystem/IO and `loom_fixture_test` keeps it on local execution (safe + consistent with other IO tests). A bare `rust_test` would route to RE; keep it `loom_fixture_test`.

- [ ] **Step 4: Run the test**

Run: `buck2 test //src/control-plane/postgres:puffin-roundtrip > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t1.log`
Expected: PASS (`Tests finished: Pass 1`). If it fails to compile on a puffin API name, fix against the pinned rev and re-run.

- [ ] **Step 5: Commit**

```bash
git add src/control-plane/postgres/src/puffin.rs src/control-plane/postgres/src/lib.rs \
        src/control-plane/postgres/BUCK src/control-plane/postgres/tests/puffin_roundtrip.rs
git commit -m "feat(postgres): puffin loom-vector-index-v1 blob read/write helpers"
```

---

### Task 2: `core` — `VectorIndex` trait + `FlatIndex` (pure)

**Files:**
- Create: `src/control-plane/core/src/vector_index.rs`
- Modify: `src/control-plane/core/src/lib.rs`
- Modify: `src/control-plane/core/BUCK`
- Test: `src/control-plane/core/tests/vector_index.rs`

**Interfaces:**
- Produces (all in `control_plane_core`):
  - `pub enum Metric { Cosine, L2 }` — `Default` = `Cosine`; `as_str()`/`from_str()` for `"cosine"`/`"l2"`.
  - `pub enum IndexKind { Flat }` — `as_str()` = `"flat"`.
  - `pub enum VectorKey { Int(i64), Str(String) }` — the object identity value; `Eq` + `Clone` + `Debug`.
  - `pub trait VectorIndex { fn metric(&self) -> Metric; fn dim(&self) -> u32; fn search(&self, query: &[f32], k: usize) -> Vec<(VectorKey, f32)>; }`
  - `pub fn distance(metric: Metric, a: &[f32], b: &[f32]) -> f32` — the single scoring function shared by the cold index (`search`) and the hot delta (Task 7), so cold and hot are scored identically (guards Acceptance 4 against a metric mismatch).
  - `pub struct FlatIndex { … }` with:
    - `pub fn build(dim: u32, metric: Metric, rows: Vec<(VectorKey, Vec<f32>)>) -> Result<FlatIndex>` (errors if any row's len != dim)
    - `pub fn serialize(&self) -> Vec<u8>`
    - `pub fn deserialize(bytes: &[u8]) -> Result<FlatIndex>`
    - `pub fn row_count(&self) -> u32`
    - impls `VectorIndex`

- [ ] **Step 1: Write the failing test**

`src/control-plane/core/tests/vector_index.rs`:

```rust
use control_plane_core::{FlatIndex, Metric, VectorIndex, VectorKey};

fn rows() -> Vec<(VectorKey, Vec<f32>)> {
    vec![
        (VectorKey::Int(1), vec![1.0, 0.0, 0.0, 0.0]),
        (VectorKey::Int(2), vec![0.0, 1.0, 0.0, 0.0]),
        (VectorKey::Int(3), vec![0.9, 0.1, 0.0, 0.0]),
    ]
}

#[test]
fn cosine_topk_is_exact_and_ascending() {
    let idx = FlatIndex::build(4, Metric::Cosine, rows()).unwrap();
    let res = idx.search(&[1.0, 0.0, 0.0, 0.0], 2);
    assert_eq!(res.len(), 2);
    // Nearest by cosine to [1,0,0,0] is key 1 (distance 0), then key 3.
    assert_eq!(res[0].0, VectorKey::Int(1));
    assert!(res[0].1 <= res[1].1, "ascending distance");
    assert_eq!(res[1].0, VectorKey::Int(3));
    assert!((res[0].1 - 0.0).abs() < 1e-6);
}

#[test]
fn l2_topk_is_exact() {
    let idx = FlatIndex::build(4, Metric::L2, rows()).unwrap();
    let res = idx.search(&[0.0, 1.0, 0.0, 0.0], 1);
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].0, VectorKey::Int(2));
    assert!((res[0].1 - 0.0).abs() < 1e-6);
}

#[test]
fn k_larger_than_rows_returns_all() {
    let idx = FlatIndex::build(4, Metric::Cosine, rows()).unwrap();
    assert_eq!(idx.search(&[1.0, 0.0, 0.0, 0.0], 99).len(), 3);
}

#[test]
fn serialize_deserialize_is_value_exact() {
    let idx = FlatIndex::build(4, Metric::L2, rows()).unwrap();
    let bytes = idx.serialize();
    let back = FlatIndex::deserialize(&bytes).unwrap();
    assert_eq!(back.dim(), 4);
    assert_eq!(back.metric(), Metric::L2);
    assert_eq!(back.row_count(), 3);
    // Same search result after a round-trip.
    let a = idx.search(&[0.0, 1.0, 0.0, 0.0], 3);
    let b = back.search(&[0.0, 1.0, 0.0, 0.0], 3);
    assert_eq!(a, b);
}

#[test]
fn string_keys_round_trip() {
    let r = vec![
        (VectorKey::Str("a".into()), vec![1.0, 0.0]),
        (VectorKey::Str("b".into()), vec![0.0, 1.0]),
    ];
    let idx = FlatIndex::build(2, Metric::Cosine, r).unwrap();
    let back = FlatIndex::deserialize(&idx.serialize()).unwrap();
    let res = back.search(&[1.0, 0.0], 1);
    assert_eq!(res[0].0, VectorKey::Str("a".into()));
}

#[test]
fn build_rejects_dim_mismatch() {
    let r = vec![(VectorKey::Int(1), vec![1.0, 0.0, 0.0])];
    assert!(FlatIndex::build(4, Metric::Cosine, r).is_err());
}

#[test]
fn distance_fn_matches_metrics() {
    use control_plane_core::distance;
    // L2 of identical vectors is 0; cosine of identical (non-zero) is ~0.
    assert!((distance(Metric::L2, &[1.0, 2.0], &[1.0, 2.0]) - 0.0).abs() < 1e-6);
    assert!((distance(Metric::Cosine, &[1.0, 0.0], &[1.0, 0.0]) - 0.0).abs() < 1e-6);
    // Orthogonal cosine distance is 1.
    assert!((distance(Metric::Cosine, &[1.0, 0.0], &[0.0, 1.0]) - 1.0).abs() < 1e-6);
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `buck2 test //src/control-plane/core:vector-index > /tmp/t2.log 2>&1; grep -E "FAIL|error|no target" /tmp/t2.log`
Expected: FAIL — target/symbols not defined yet.

- [ ] **Step 3: Implement `vector_index.rs`**

`src/control-plane/core/src/vector_index.rs`:

```rust
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
    pub fn from_str(s: &str) -> Option<Metric> {
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
        Ok(FlatIndex { dim, metric, keys, data })
    }

    #[must_use]
    pub fn row_count(&self) -> u32 {
        self.keys.len() as u32
    }

    fn row(&self, i: usize) -> &[f32] {
        let d = self.dim as usize;
        // i is always < keys.len(); data is keys.len()*d long.
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
        Ok(FlatIndex { dim, metric, keys, data })
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
        let arr: [u8; 4] = s.try_into().map_err(|_| bad("u32"))?;
        Ok(u32::from_le_bytes(arr))
    }
    fn i64(&mut self) -> Result<i64> {
        let s = self.take(8)?;
        let arr: [u8; 8] = s.try_into().map_err(|_| bad("i64"))?;
        Ok(i64::from_le_bytes(arr))
    }
    fn f32(&mut self) -> Result<f32> {
        let s = self.take(4)?;
        let arr: [u8; 4] = s.try_into().map_err(|_| bad("f32"))?;
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
```

> NOTE: `indexing_slicing` is an enforced lint. `row()` uses a slice index on `self.data`; wrap it defensively or add `#[expect(clippy::indexing_slicing, reason = "i < keys.len(); data is keys.len()*dim")]` on the `row` method. The `Cursor` uses `.get(..)` everywhere (lint-clean). Prefer the `#[expect]` on `row` over restructuring.

- [ ] **Step 4: Wire the module + re-exports + BUCK**

In `src/control-plane/core/src/lib.rs`: add `mod vector_index;` in the module block and to the re-exports:

```rust
pub use vector_index::{FlatIndex, IndexKind, Metric, VectorIndex, VectorKey, distance};
```

In `src/control-plane/core/BUCK`, add (mirror the `page` test target):

```python
rust_test(
    name = "vector-index",
    crate = "vector_index",
    srcs = ["tests/vector_index.rs"],
    crate_root = "tests/vector_index.rs",
    edition = "2024",
    visibility = ["PUBLIC"],
    deps = [":core"],
)
```

- [ ] **Step 5: Run the tests**

Run: `buck2 test //src/control-plane/core:vector-index > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t2.log`
Expected: PASS (6 tests).

- [ ] **Step 6: Commit**

```bash
git add src/control-plane/core/src/vector_index.rs src/control-plane/core/src/lib.rs \
        src/control-plane/core/BUCK src/control-plane/core/tests/vector_index.rs
git commit -m "feat(core): VectorIndex trait + exact FlatIndex (cosine/l2, serde)"
```

---

### Task 3: `core` — `build_vector_index` job kind + payload

**Files:**
- Create: `src/control-plane/core/src/vector_index_job.rs`
- Modify: `src/control-plane/core/src/lib.rs`
- Modify: `src/control-plane/core/BUCK`
- Test: `src/control-plane/core/tests/vector_index_job.rs`

**Interfaces:**
- Produces:
  - `pub const BUILD_VECTOR_INDEX_JOB_KIND: &str = "build_vector_index";`
  - `pub struct BuildVectorIndexJob { pub schema: String, pub name: String, pub column: String }` (`serde::Serialize + Deserialize + Debug + Clone`)

- [ ] **Step 1: Write the failing test**

`src/control-plane/core/tests/vector_index_job.rs`:

```rust
use control_plane_core::{BUILD_VECTOR_INDEX_JOB_KIND, BuildVectorIndexJob};

#[test]
fn build_vector_index_job_serde_roundtrip_and_kind() {
    assert_eq!(BUILD_VECTOR_INDEX_JOB_KIND, "build_vector_index");
    let j = BuildVectorIndexJob {
        schema: "wh".into(),
        name: "docs".into(),
        column: "embedding".into(),
    };
    let v = serde_json::to_value(&j).unwrap();
    assert_eq!(v["schema"], "wh");
    assert_eq!(v["name"], "docs");
    assert_eq!(v["column"], "embedding");
    let back: BuildVectorIndexJob = serde_json::from_value(v).unwrap();
    assert_eq!(back.schema, "wh");
    assert_eq!(back.name, "docs");
    assert_eq!(back.column, "embedding");
}
```

- [ ] **Step 2: Implement**

`src/control-plane/core/src/vector_index_job.rs`:

```rust
//! The build-vector-index job contract, shared by the producer (enqueue) and the
//! consumer (worker → engine RPC). Mirrors `flush.rs`. Lives in core so a
//! zero-pool worker can read it without the postgres adapter.

/// The queue `kind` for a vector-index build. Protocol invariant, not a tunable.
pub const BUILD_VECTOR_INDEX_JOB_KIND: &str = "build_vector_index";

/// Payload of a `build_vector_index` job: which `(schema, name)` table and which
/// `vector(N)` column to index.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct BuildVectorIndexJob {
    pub schema: String,
    pub name: String,
    pub column: String,
}
```

In `src/control-plane/core/src/lib.rs`: add `mod vector_index_job;` and:

```rust
pub use vector_index_job::{BUILD_VECTOR_INDEX_JOB_KIND, BuildVectorIndexJob};
```

In `src/control-plane/core/BUCK`, add (mirror `flush-job`):

```python
rust_test(
    name = "vector-index-job",
    crate = "vector_index_job",
    srcs = ["tests/vector_index_job.rs"],
    crate_root = "tests/vector_index_job.rs",
    edition = "2024",
    visibility = ["PUBLIC"],
    deps = [":core", "//third-party:serde_json"],
)
```

- [ ] **Step 3: Run**

Run: `buck2 test //src/control-plane/core:vector-index-job > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t3.log`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add src/control-plane/core/src/vector_index_job.rs src/control-plane/core/src/lib.rs \
        src/control-plane/core/BUCK src/control-plane/core/tests/vector_index_job.rs
git commit -m "feat(core): build_vector_index job kind + payload"
```

---

### Task 4: `postgres` — `vector_index` mirror table + insert/lookup queries

**Files:**
- Create: `src/control-plane/postgres/migrations/0018_vector_index.sql`
- Create: `src/control-plane/postgres/src/vector_index.rs` (row type + insert + lookup; build primitive added in Task 6)
- Modify: `src/control-plane/postgres/src/lib.rs` (`pub mod vector_index;`)
- Modify: `src/control-plane/postgres/BUCK`
- Regenerate + commit: `src/control-plane/postgres/.sqlx/`
- Test: `src/control-plane/postgres/tests/vector_index_mirror.rs`

**Interfaces:**
- Produces (in `control_plane_postgres::vector_index`):
  - `pub struct VectorIndexRow { pub table_id: i64, pub column: String, pub covered_snapshot: i64, pub metric: String, pub index_kind: String, pub dim: i32, pub row_count: i64, pub puffin_path: String }`
  - `pub async fn insert_vector_index(tx: &mut sqlx::PgConnection, row: &VectorIndexRow) -> control_plane_core::Result<()>`
  - `pub async fn lookup_vector_index(pool: &sqlx::PgPool, table_id: i64, column: &str, at: i64) -> control_plane_core::Result<Option<VectorIndexRow>>`
- Consumes: nothing new.

- [ ] **Step 1: Write the migration**

`src/control-plane/postgres/migrations/0018_vector_index.sql`:

```sql
-- Binds a precomputed vector index (a Puffin sidecar) to a covered snapshot.
-- loom has no REST catalog; this mirror row is the binding pointer the paper
-- expresses via the REST catalog's statistics-file summary property.
create table iceberg_mirror.vector_index (
    table_id         bigint not null references iceberg_mirror.table(table_id),
    column_name      text   not null,
    covered_snapshot bigint not null,
    metric           text   not null,
    index_kind       text   not null,
    dim              integer not null,
    row_count        bigint not null,
    puffin_path      text   not null,
    created_at       timestamptz not null default now(),
    primary key (table_id, column_name, covered_snapshot)
);
create index iceberg_vector_index_lookup_idx
    on iceberg_mirror.vector_index (table_id, column_name, covered_snapshot);
```

- [ ] **Step 2: Write the failing test**

`src/control-plane/postgres/tests/vector_index_mirror.rs`:

```rust
//! The `iceberg_mirror.vector_index` binding: insert a row, then look up the
//! latest covered_snapshot <= Q.

use control_plane_postgres::fixture;
use control_plane_postgres::iceberg_inline::inline_append; // ensures a table exists
use control_plane_postgres::vector_index::{
    VectorIndexRow, insert_vector_index, lookup_vector_index,
};

#[tokio::test]
async fn insert_then_lookup_latest_le_q() {
    let fx = fixture::Fixture::new().await.unwrap();
    let pool = fx.pool();

    // Create a real table row via the mirror so the FK resolves. The simplest
    // path: insert an iceberg_mirror.table row directly for the test.
    let table_id: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(
        "insert into iceberg_mirror.snapshot (snapshot_id) values (1) on conflict do nothing; \
         insert into iceberg_mirror.\"table\" (table_namespace, table_name, begin_snapshot) \
         values ('wh','docs',1) returning table_id",
    ))
    .fetch_one(pool)
    .await
    .unwrap();

    let mut conn = pool.acquire().await.unwrap();
    let row = VectorIndexRow {
        table_id,
        column: "embedding".into(),
        covered_snapshot: 5,
        metric: "cosine".into(),
        index_kind: "flat".into(),
        dim: 4,
        row_count: 3,
        puffin_path: "file:///tmp/x.puffin".into(),
    };
    insert_vector_index(&mut conn, &row).await.unwrap();

    // Q below the covered snapshot → no binding.
    assert!(lookup_vector_index(pool, table_id, "embedding", 4).await.unwrap().is_none());
    // Q at/after → the row.
    let got = lookup_vector_index(pool, table_id, "embedding", 9).await.unwrap().unwrap();
    assert_eq!(got.covered_snapshot, 5);
    assert_eq!(got.dim, 4);
    assert_eq!(got.metric, "cosine");

    // A newer index at covered 8: lookup at Q=9 returns the newest <= Q.
    let mut row2 = row.clone();
    row2.covered_snapshot = 8;
    row2.row_count = 7;
    insert_vector_index(&mut conn, &row2).await.unwrap();
    let got = lookup_vector_index(pool, table_id, "embedding", 9).await.unwrap().unwrap();
    assert_eq!(got.covered_snapshot, 8);
    assert_eq!(got.row_count, 7);
}
```

> NOTE: confirm `fixture::Fixture`'s exact constructor/accessor names by reading `src/control-plane/postgres/src/fixture.rs` and an existing fixture test (e.g. `tests/iceberg_landing.rs`). Adapt `Fixture::new()/pool()` to the real API. The `VectorIndexRow` must derive `Clone`.

- [ ] **Step 3: Implement the row + queries**

`src/control-plane/postgres/src/vector_index.rs` (this task's portion; Task 6 appends the build primitive + `inline_delta_batch` to the same file):

```rust
//! The `iceberg_mirror.vector_index` binding (row type + insert/lookup) and the
//! build primitive (Task 6). loom records `(table, column, covered_snapshot) ->
//! puffin_path` as a mirror row in lieu of a REST catalog.

use control_plane_core::{ControlPlaneError, Result};
use sqlx::{AssertSqlSafe, PgConnection, PgPool, Row};

fn backend<E: std::fmt::Display>(e: E) -> ControlPlaneError {
    ControlPlaneError::Backend(e.to_string().into())
}

/// A bound vector index: the metadata loom needs to find and decode the sidecar.
#[derive(Clone, Debug)]
pub struct VectorIndexRow {
    pub table_id: i64,
    pub column: String,
    pub covered_snapshot: i64,
    pub metric: String,
    pub index_kind: String,
    pub dim: i32,
    pub row_count: i64,
    pub puffin_path: String,
}

// SQL-STYLE (env-forced runtime, see plan "Decisions"): this cloud session cannot
// regenerate the .sqlx cache (Postgres won't boot as root; libxml2 egress is
// policy-blocked), so the new vector_index queries use RUNTIME
// `sqlx::query(AssertSqlSafe(...))` + bind params instead of compile-time
// `query!`/`query_scalar!`. The SQL is a fixed literal (no interpolation — every
// value is a bound `$n` param), so AssertSqlSafe carries no injection risk. This
// mirrors the runtime pattern already used in `iceberg_inline.rs`/`fixture.rs`.
// (Promotable to compile-time `query!` in a follow-up when a Postgres-capable env
// is available — tracked as a FUTURE item.)

/// Insert a `vector_index` binding row in the caller's transaction.
pub async fn insert_vector_index(tx: &mut PgConnection, row: &VectorIndexRow) -> Result<()> {
    sqlx::query(AssertSqlSafe(
        "insert into iceberg_mirror.vector_index \
         (table_id, column_name, covered_snapshot, metric, index_kind, dim, row_count, puffin_path) \
         values ($1, $2, $3, $4, $5, $6, $7, $8)"
            .to_string(),
    ))
    .bind(row.table_id)
    .bind(&row.column)
    .bind(row.covered_snapshot)
    .bind(&row.metric)
    .bind(&row.index_kind)
    .bind(row.dim)
    .bind(row.row_count)
    .bind(&row.puffin_path)
    .execute(&mut *tx)
    .await
    .map_err(backend)?;
    Ok(())
}

/// The newest bound index for `(table_id, column)` with `covered_snapshot <= at`,
/// or `None` if none is bound.
pub async fn lookup_vector_index(
    pool: &PgPool,
    table_id: i64,
    column: &str,
    at: i64,
) -> Result<Option<VectorIndexRow>> {
    let row = sqlx::query(AssertSqlSafe(
        "select table_id, column_name, covered_snapshot, metric, index_kind, dim, \
                row_count, puffin_path \
         from iceberg_mirror.vector_index \
         where table_id = $1 and column_name = $2 and covered_snapshot <= $3 \
         order by covered_snapshot desc limit 1"
            .to_string(),
    ))
    .bind(table_id)
    .bind(column)
    .bind(at)
    .fetch_optional(pool)
    .await
    .map_err(backend)?;
    row.map(|r| {
        Ok(VectorIndexRow {
            table_id: r.try_get("table_id").map_err(backend)?,
            column: r.try_get("column_name").map_err(backend)?,
            covered_snapshot: r.try_get("covered_snapshot").map_err(backend)?,
            metric: r.try_get("metric").map_err(backend)?,
            index_kind: r.try_get("index_kind").map_err(backend)?,
            dim: r.try_get("dim").map_err(backend)?,
            row_count: r.try_get("row_count").map_err(backend)?,
            puffin_path: r.try_get("puffin_path").map_err(backend)?,
        })
    })
    .transpose()
}
```

In `src/control-plane/postgres/src/lib.rs`: add `pub mod vector_index;`.

> NOTE (verify AssertSqlSafe form): confirm the exact `AssertSqlSafe(...)` call shape and the `Row::try_get` import against `iceberg_inline.rs` (it uses `sqlx::query_scalar(AssertSqlSafe(format!(...)))`). If `AssertSqlSafe` wraps a `&str` rather than `String` on this sqlx version, drop the `.to_string()`. The library MUST compile with `buck2 build //src/control-plane/postgres:postgres` (no `.sqlx` change, since there are no compile-time macros here).

- [ ] **Step 4: Confirm the library compiles (no `.sqlx` regen needed)**

These are runtime queries, so there is NO `query!` macro and NO `.sqlx` cache entry to regenerate. Just confirm the production library still builds:

Run: `buck2 build //src/control-plane/postgres:postgres > /tmp/b4.log 2>&1; grep -E "BUILD (SUCCEEDED|FAILED)|error\[" /tmp/b4.log`
Expected: BUILD SUCCEEDED. (The `sqlx-cache-check` test stays green automatically — no new cache entries.)

- [ ] **Step 5: Add the BUCK test target**

In `src/control-plane/postgres/BUCK`, mirror `iceberg-landing`:

```python
loom_fixture_test(
    name = "vector-index-mirror",
    crate = "vector_index_mirror",
    srcs = ["tests/vector_index_mirror.rs"],
    crate_root = "tests/vector_index_mirror.rs",
    deps = [
        ":postgres",
        "//src/control-plane/core:core",
        "//third-party:sqlx",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 6: Build the test target (run is CI-only in this env)**

The fixture test boots hermetic Postgres and needs the `:libxml2` http_archive — both impossible in this cloud session (root + egress-blocked vault.centos.org). So you CANNOT run it here; it is verified by the PR's BuildBuddy CI. Do confirm the production library + pure code compile (Step 4). Do NOT attempt to build the `vector-index-mirror` fixture target locally (the libxml2 download will 403). Write the test correctly per the brief so CI can run it.

> If you have any way to sanity-check the SQL/decoding logic without Postgres, do so; otherwise rely on CI. Note in your report that the fixture test is unrun-locally-by-design.

- [ ] **Step 7: Commit** (no `.sqlx` to add — runtime queries)

```bash
git add src/control-plane/postgres/migrations/0018_vector_index.sql \
        src/control-plane/postgres/src/vector_index.rs src/control-plane/postgres/src/lib.rs \
        src/control-plane/postgres/BUCK src/control-plane/postgres/tests/vector_index_mirror.rs
git commit -m "feat(postgres): vector_index mirror table + insert/lookup binding"
```

---

### Task 5: `postgres` — `FlatIndex ↔ loom-vector-index-v1` payload wrapper

Bridge `core::FlatIndex` to the Puffin blob payload + the self-describing properties map. Builds on Task 1's `puffin.rs` and Task 2's `FlatIndex::serialize/deserialize`.

**Files:**
- Modify: `src/control-plane/postgres/src/puffin.rs` (add the FlatIndex-aware functions)
- Test: extend `src/control-plane/postgres/tests/puffin_roundtrip.rs`

**Interfaces:**
- Produces (in `control_plane_postgres::puffin`):
  - `pub async fn write_flat_index(file_io: &iceberg::io::FileIO, path: &str, index: &control_plane_core::FlatIndex, covered_snapshot: i64, field_id: i32, column: &str, identity_column: &str) -> control_plane_core::Result<()>`
  - `pub async fn read_flat_index(file_io: &iceberg::io::FileIO, path: &str) -> control_plane_core::Result<control_plane_core::FlatIndex>`
- Consumes: `core::{FlatIndex, Metric, IndexKind}`, Task 1's `write_index_blob`/`read_index_blob`.

- [ ] **Step 1: Add the failing test (append to `puffin_roundtrip.rs`)**

```rust
use control_plane_core::{FlatIndex, Metric, VectorKey, VectorIndex};
use control_plane_postgres::puffin::{read_flat_index, write_flat_index};

#[tokio::test]
async fn flat_index_round_trips_through_puffin() {
    let dir = tempfile::tempdir().unwrap();
    let file_io = iceberg::io::FileIOBuilder::new_fs_io().build().unwrap();
    let path = format!("{}/flat.puffin", dir.path().display());

    let idx = FlatIndex::build(
        4,
        Metric::Cosine,
        vec![
            (VectorKey::Int(10), vec![1.0, 0.0, 0.0, 0.0]),
            (VectorKey::Int(20), vec![0.0, 1.0, 0.0, 0.0]),
        ],
    )
    .unwrap();

    write_flat_index(&file_io, &path, &idx, 5, 3, "embedding", "id")
        .await
        .unwrap();
    let back = read_flat_index(&file_io, &path).await.unwrap();
    assert_eq!(back.dim(), 4);
    assert_eq!(back.metric(), Metric::Cosine);
    assert_eq!(
        back.search(&[1.0, 0.0, 0.0, 0.0], 1)[0].0,
        VectorKey::Int(10)
    );
}
```

- [ ] **Step 2: Implement (append to `puffin.rs`)**

```rust
use control_plane_core::{FlatIndex, IndexKind, Metric, VectorIndex};

/// Serialize a `FlatIndex` into a `loom-vector-index-v1` Puffin blob with the
/// self-describing properties the spec mandates.
pub async fn write_flat_index(
    file_io: &FileIO,
    path: &str,
    index: &FlatIndex,
    covered_snapshot: i64,
    field_id: i32,
    column: &str,
    identity_column: &str,
) -> Result<()> {
    let mut props = HashMap::new();
    props.insert("dim".to_string(), index.dim().to_string());
    props.insert("metric".to_string(), index.metric().as_str().to_string());
    props.insert("index-kind".to_string(), IndexKind::Flat.as_str().to_string());
    props.insert("column".to_string(), column.to_string());
    props.insert("identity-column".to_string(), identity_column.to_string());
    props.insert("row-count".to_string(), index.row_count().to_string());
    props.insert("covered-snapshot".to_string(), covered_snapshot.to_string());
    let payload = index.serialize();
    write_index_blob(file_io, path, &payload, covered_snapshot, field_id, props).await
}

/// Read and decode the `FlatIndex` from a `loom-vector-index-v1` Puffin file.
pub async fn read_flat_index(file_io: &FileIO, path: &str) -> Result<FlatIndex> {
    let loaded = read_index_blob(file_io, path).await?;
    FlatIndex::deserialize(&loaded.payload)
}
```

> NOTE: ensure the `puffin-roundtrip` BUCK target's deps include `//src/control-plane/core:core` (the new test code uses `control_plane_core`). Update the Task 1 target's `deps` to add `"//src/control-plane/core:core"`.

- [ ] **Step 3: Run**

Run: `buck2 test //src/control-plane/postgres:puffin-roundtrip > /tmp/t5.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t5.log`
Expected: PASS (2 tests).

- [ ] **Step 4: Commit**

```bash
git add src/control-plane/postgres/src/puffin.rs src/control-plane/postgres/BUCK \
        src/control-plane/postgres/tests/puffin_roundtrip.rs
git commit -m "feat(postgres): FlatIndex <-> loom-vector-index-v1 puffin payload"
```

---

### Task 6: `postgres` — the build primitive

Read all vectors live at `S` over the serving read path (cold Parquet ∪ hot inline), build the `FlatIndex`, write the Puffin sidecar, and commit the `vector_index` mirror row + a lineage event in one transaction.

**Files:**
- Modify: `src/control-plane/postgres/src/vector_index.rs` (append the primitive + a vector/identity extractor + `inline_delta_batch`)
- Test: `src/control-plane/postgres/tests/vector_index_build.rs`
- Modify: `src/control-plane/postgres/BUCK`

**Interfaces:**
- Produces:
  - `pub struct BuiltIndex { pub covered_snapshot: i64, pub puffin_path: String, pub row_count: i64 }`
  - `pub async fn build_vector_index(catalog: &SqlCatalog, pool: &PgPool, table: &TableRef, column: &str, metric: control_plane_core::Metric, run_id: RunId) -> Result<BuiltIndex>` — `metric` is recorded in the blob + mirror row; slice-1 callers (RPC/worker) pass `Metric::Cosine`.
  - `pub async fn inline_delta_batch(pool: &PgPool, table: &TableRef, born_after: i64, at: i64) -> Result<Option<arrow_array::RecordBatch>>` (used by Task 8)
- Consumes: `IcebergCatalog::{current_snapshot, files_with_stats, inline_live_batch}`, `read_files_as_batches`, `puffin::write_flat_index`, `vector_index::insert_vector_index`, `pg_emit` (lineage), `core::{FlatIndex, Metric, VectorKey, ObjectType-identity via ontology}`.

**Design notes for the implementer (read before coding):**
- **Snapshot/Catalog visibility (BLOCKER fix).** `IcebergCatalog::current_snapshot` is a method of the `control_plane_core::Catalog` **trait**, not an inherent method, so the caller MUST have `use control_plane_core::Catalog;` in scope (exactly as `inline_live_batch` does internally). `current_snapshot` returns a `Snapshot` whose `id` is a `SnapshotId(pub i64)` newtype — extract the raw `i64` as `.id.0`, never `.id`. So: the covered snapshot `S: i64 = IcebergCatalog::new(pool.clone()).current_snapshot(table).await?.id.0;`.
- **`table_id` resolver (BLOCKER fix — unify across Tasks 4/6/7).** Use the existing public helper `crate::iceberg_mirror::live_table_id(conn: &mut PgConnection, ns: &str, name: &str) -> Result<Option<i64>>` (defined at `iceberg_mirror.rs:177`, already reused by inline/flush/gc). Acquire a conn from the pool, call `live_table_id(&mut conn, &table.schema, &table.name).await?`, and map `None` → `ControlPlaneError::NotFound`. Do NOT invent a new resolver; Task 7 uses this same helper for its lookup.
- **Identity column resolver (BLOCKER fix — concrete query, runtime SQL).** The build needs the object's identity property name to populate `VectorKey`. The ontology stores it in `ontology.object_type(name, table_schema, table_name, identity)` where `identity text` is **nullable** (opt-in). Resolve it with a RUNTIME query (env-forced; see Task 4's SQL-style note — no `.sqlx` regen possible here):
  ```rust
  async fn identity_column_for(pool: &PgPool, table: &TableRef) -> Result<String> {
      let row = sqlx::query(AssertSqlSafe(
          "select identity from ontology.object_type \
           where table_schema = $1 and table_name = $2"
              .to_string(),
      ))
      .bind(&table.schema)
      .bind(&table.name)
      .fetch_optional(pool)
      .await
      .map_err(backend)?;
      // `identity` is nullable: try_get yields Option<String>; row-missing also -> None.
      let id: Option<String> = match row {
          Some(r) => r.try_get("identity").map_err(backend)?,
          None => None,
      };
      id.ok_or_else(|| {
          ControlPlaneError::Backend(
              format!("no identity column declared for {}.{}", table.schema, table.name).into(),
          )
      })
  }
  ```
  (Runtime query → no `.sqlx` entry. `Row`/`AssertSqlSafe` are already imported for Task 4's queries in this module.)
- Extract vectors: the vector column is Arrow `List<Float32>`. For each row, downcast the column to `ListArray`, get the child `Float32Array` slice for that row → `Vec<f32>`. The identity column downcasts to `Int64Array`/`Int32Array` (→ `VectorKey::Int`) or `StringArray` (→ `VectorKey::Str`). Mirror the arrow downcast patterns already in `engine-serving/src/provider.rs` (`pg_rows_to_arrays`) and `iceberg_inline.rs` (`cell_from_arrow`). Use `.as_any().downcast_ref::<ListArray>().ok_or_else(...)` (no `unwrap`).
- Field id of the vector column: obtain from the loaded iceberg `Table`'s current schema (prefer the real id via the schema's field-id-by-name accessor on this rev — grep iceberg-rust `spec::Schema` for `field_id_by_name`). If the accessor name differs, a `0` fallback is acceptable: the footer `fields` is informational for decode in slice 1.
- Puffin path: `format!("{}/metadata/loom-vector-index-{}.puffin", tbl.metadata().location(), uuid::Uuid::new_v4())`. `tbl.metadata().location()` is absolute (file://… or s3://…) so the FileIO resolves it.
- FileIO: `let tbl = catalog.load_table(&ident).await.map_err(backend)?; let file_io = tbl.file_io().clone();`.
- Transaction: `write_flat_index` happens **before** the tx (it is an object-store write, mirroring how `append_parquet_snapshot` does object-store reads before its tx). Then `pool.begin()`, resolve `table_id` via `live_table_id(&mut tx, …)`, `insert_vector_index(&mut tx, …)`, `pg_emit(&mut tx, &lineage)`, `tx.commit()`. (`pg_emit(conn, &LineageEvent)` is reused exactly as `iceberg_control_plane.rs` does.)
- Lineage: `LineageEvent { run_id, event_type: EventType::Complete, event_time: OffsetDateTime::now_utc(), inputs: vec![DatasetRef{namespace: schema, name}], outputs: vec![DatasetRef{namespace:"loom-vector-index", name: puffin_path}], payload: json!({"column": column, "covered_snapshot": S, "row_count": n}) }`. Confirm `pg_emit`'s signature in `src/control-plane/postgres/src/lineage.rs` (`pg_emit(conn, &LineageEvent)`).

- [ ] **Step 1: Write the failing fixture test**

`src/control-plane/postgres/tests/vector_index_build.rs`:

```rust
//! Build primitive: land a typed object with an `embedding: vector(4)` column
//! (some flushed, some inline), run the build as of S, assert a Puffin sidecar
//! exists and a vector_index mirror row + lineage event were written in one tx,
//! covering all rows live at S.

// Use the crate's existing landing/seed helpers. The implementer should mirror
// the setup in tests/iceberg_landing.rs to: create the ontology type with an
// `id: Long` identity + `embedding: vector(4)` property, land a few rows
// (forcing some to Parquet via the byte limit and leaving some inline), then:

#[tokio::test]
async fn build_covers_all_rows_live_at_s() {
    // 1. fixture + seed type `wh.docs` (id: Long identity, embedding: vector(4)).
    // 2. land rows {id:1..=4, embedding:…}; flush ids 1..=2 to Parquet, leave 3..=4 inline.
    // 3. let run = build_vector_index(&catalog, pool, &table, "embedding", Metric::Cosine, RunId(uuid)).await.unwrap();
    // 4. assert run.row_count == 4 (cold 2 + hot 2).
    // 5. lookup_vector_index(pool, table_id, "embedding", run.covered_snapshot) is Some.
    // 6. read_flat_index over run.puffin_path returns an index whose search finds id 1
    //    for embedding[1]'s query vector.
    // 7. assert exactly one lineage event with output namespace "loom-vector-index".
    todo!("flesh out using tests/iceberg_landing.rs seed helpers");
}
```

> The `todo!()` is a scaffold marker for the implementer only — it MUST be replaced with the real assertions before the task's commit (the `todo` lint is allowed in test code, but a `todo!()` left in a committed test is a task failure). Read `tests/iceberg_landing.rs` to copy the exact seed/land/flush helper calls, then write the concrete body.

- [ ] **Step 2: Run it to verify it fails**

Run: `buck2 test //src/control-plane/postgres:vector-index-build > /tmp/t6.log 2>&1; grep -E "FAIL|error\[|panicked" /tmp/t6.log`
Expected: FAIL.

- [ ] **Step 3: Implement the primitive (append to `vector_index.rs`)**

Provide the full `build_vector_index`, `inline_delta_batch`, the arrow extractor `fn extract_rows(batch, vector_col, identity_col, dim) -> Result<Vec<(VectorKey, Vec<f32>)>>`, and `identity_column_for`. Use `?` everywhere; no `unwrap`/`expect`/`indexing_slicing` in this production module (downcasts via `as_any().downcast_ref::<ListArray>().ok_or_else(...)`). Concatenate cold + hot batches by iterating each batch's rows. `inline_delta_batch` mirrors `IcebergCatalog::inline_live_batch` but adds `begin_snapshot > $born_after` to the MVCC predicate and selects only the vector + identity columns; build it as a runtime `sqlx::query(AssertSqlSafe(...))` over `inline_<tid>` (dynamic relation name — cannot be compile-time).

> Implementer: keep `inline_delta_batch` returning a `RecordBatch` with just the identity + vector columns to keep arrow assembly small; reuse the `cell`/arrow-builder helpers from `iceberg_inline.rs` if accessible, else build `Int64Builder`/`Float32`-list builders inline.

- [ ] **Step 4: Confirm the library compiles (no `.sqlx` regen — all runtime queries)**

All new queries in this task are runtime `sqlx::query(AssertSqlSafe(...))`, so there is no `.sqlx` cache to regenerate. Confirm the production lib builds:
Run: `buck2 build //src/control-plane/postgres:postgres > /tmp/b6.log 2>&1; grep -E "BUILD (SUCCEEDED|FAILED)|error\[" /tmp/b6.log`
Expected: BUILD SUCCEEDED.

- [ ] **Step 5: Add BUCK target (run is CI-only in this env)**

Add `vector-index-build` `loom_fixture_test` (deps mirror `iceberg-landing`: arrow-array, arrow-schema, arrow-ipc, iceberg, serde_json, tempfile, time, tokio, uuid, core, postgres). The fixture test boots Postgres + needs `:libxml2` → it CANNOT build/run in this cloud session (root + egress-blocked); it is verified by the PR's BuildBuddy CI. Do NOT attempt to build the fixture target locally. Confirm only the library build (Step 4).

- [ ] **Step 6: Commit** (no `.sqlx` — runtime queries)

```bash
git add src/control-plane/postgres/src/vector_index.rs src/control-plane/postgres/BUCK \
        src/control-plane/postgres/tests/vector_index_build.rs
git commit -m "feat(postgres): build_vector_index primitive (serving-read -> puffin + mirror + lineage)"
```

---

### Task 7: `engine-serving` — the cold/hot k-NN merge

**Files:**
- Create: `src/services/engine-serving/src/vector_search.rs`
- Modify: `src/services/engine-serving/src/lib.rs`
- Modify: `src/services/engine-serving/BUCK`
- Test: `src/services/engine-serving/tests/vector_search.rs`

**Interfaces:**
- Produces (in `engine_serving`):
  - `pub async fn vector_search(catalog: &SqlCatalog, pool: &PgPool, table: &TableRef, column: &str, query: &[f32], k: usize) -> Result<RecordBatch, EngineServingError>` — returns a 2-column batch: identity (`Int64` or `Utf8`, matching the index's identity kind) + `_distance` (`Float32`), ascending distance, at most `k` rows.
  - Add an `EngineServingError::NoIndex(String)` variant for the deterministic no-index error.
- Consumes: `vector_index::{lookup_vector_index}`, `puffin::read_flat_index`, `vector_index::inline_delta_batch`, `IcebergCatalog::current_snapshot`, `core::{FlatIndex, VectorIndex, VectorKey}`.

**Design notes:**
- `Q` (BLOCKER fix, same as Task 6): `use control_plane_core::Catalog;` in scope, then `let q: i64 = IcebergCatalog::new(pool.clone()).current_snapshot(table).await.map_err(to_serving)?.id.0;` — `.id.0`, not `.id`.
- Resolve `table_id` with the SAME helper as Tasks 4/6: `crate::iceberg_mirror::live_table_id` (acquire a conn from `pool`). Do not introduce a new resolver.
- `lookup_vector_index(pool, table_id, column, q)` → `None` ⇒ `Err(EngineServingError::NoIndex(...))`.
- Cold: load the table via `catalog`, `read_flat_index(&tbl.file_io(), &row.puffin_path)`, `idx.search(query, k)`.
- Hot: `inline_delta_batch(pool, table, row.covered_snapshot, q)` → if `Some(batch)`, brute-force top-k over its rows using `control_plane_core::distance(metric, query, row_vec)` with `metric = Metric::from_str(&row.metric).ok_or(...)` — the SAME scoring function the cold index uses (added as `pub` in Task 2), so cold and hot can never diverge. Pull identity + vector from the batch with the same downcasts as Task 6.
- Merge: concatenate cold `(VectorKey, f32)` + hot `(VectorKey, f32)`, stable-sort ascending by distance, take `k`.
- Build the output `RecordBatch`: identity column typed from the `VectorKey` variants (all `Int` ⇒ `Int64Array`; all `Str` ⇒ `StringArray`), `_distance` ⇒ `Float32Array`.

- [ ] **Step 1: Write the failing fixture test (headline)**

`src/services/engine-serving/tests/vector_search.rs`:

```rust
//! Headline: with an index built at S, insert further inline rows (born after S),
//! query at Q > S; assert the exact global nearest set across cold-index + hot
//! inline, no double-count and none missed — including a row flushed between S
//! and Q. Cosine and L2 both verified. Plus the no-index deterministic error.

// Shared seed: extract the (id, distance) pairs from a vector_search result batch.
// `ids` downcasts the identity column (Int64Array) and returns Vec<i64> in row order.
fn ids(batch: &arrow_array::RecordBatch) -> Vec<i64> {
    use arrow_array::{Array, Int64Array};
    let col = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    (0..col.len()).map(|i| col.value(i)).collect()
}

#[tokio::test]
async fn knn_merges_cold_and_hot_exactly_cosine() {
    // 1. seed wh.docs (id: Long identity, embedding: vector(4)); land ids 1,2 and FLUSH
    //    them to Parquet; build_vector_index @S so the index covers {1,2}. record S.
    // 2. land ids 3,4 INLINE (begin_snapshot > S). FLUSH id 3 between S and Q (it moves
    //    to Parquet but keeps begin_snapshot > S, so it is hot-delta, not in the index).
    // 3. query nearest to id 1's embedding, k=3:
    //    let batch = vector_search(&catalog, pool, &table, "embedding", &q, 3).await.unwrap();
    //    let got = ids(&batch);
    // 4. EXACT assertions (the load-bearing proof):
    //    assert_eq!(got.len(), 3);
    //    let set: std::collections::HashSet<i64> = got.iter().copied().collect();
    //    assert_eq!(set.len(), got.len(), "no id double-counted");          // no dup
    //    assert_eq!(set, [1, 3, 4].into_iter().collect());                  // exact membership
    //    // distances ascending:
    //    let d = distances(&batch); assert!(d.windows(2).all(|w| w[0] <= w[1]));
    //    Choose embeddings so the true cosine top-3 to q is exactly {1,3,4} (2 is farthest),
    //    including id 3 which was flushed between S and Q — counted once via the hot delta,
    //    NOT via the index. This is Acceptance 4's "flushed-between row counted exactly once".
    panic!("REPLACE: implement using tests/iceberg_landing.rs + vector_index_build.rs seed helpers");
}

#[tokio::test]
async fn knn_l2_exact() {
    // Same seed shape, but build the index with Metric::L2 (the build primitive must let
    // the metric be chosen — default Cosine; for L2 either add a metric arg to the queue
    // payload later or build the FlatIndex with L2 directly in this test via the primitive's
    // metric parameter). Assert the L2 nearest top-k matches the hand-computed euclidean order.
    panic!("REPLACE: L2 variant of the merge proof");
}

#[tokio::test]
async fn no_bound_index_is_deterministic_error() {
    // seed a table with a vector column but DO NOT build an index; assert
    // vector_search returns Err(EngineServingError::NoIndex(_)), never panics.
    panic!("REPLACE: assert matches!(err, EngineServingError::NoIndex(_))");
}
```

> The three bodies above are spelled out as concrete assertion plans (exact id set, no-dup, ascending distance, the flushed-between case). Replace each `panic!("REPLACE…")` with the real body before committing Task 7 — `panic!` (not `todo!`) is used deliberately so a forgotten scaffold fails the test run loudly rather than compiling green. Also add a `distances(&batch) -> Vec<f32>` helper (downcast column 1 to `Float32Array`). **Metric decision for `knn_l2_exact`:** the build primitive (Task 6) builds with `Metric::Cosine` by default; to test L2, give `build_vector_index` an explicit `metric: Metric` parameter (thread it through, default Cosine at the job/RPC layer for slice 1). Update Task 6's `build_vector_index` signature to `build_vector_index(catalog, pool, table, column, metric, run_id)` and the RPC/worker to pass `Metric::Cosine` — record this as a cross-task signature so Task 9's RPC impl matches.

- [ ] **Step 2: Implement `vector_search.rs` + lib wiring**

Add `pub mod vector_search;` and `pub use vector_search::vector_search;` to `engine-serving/src/lib.rs`. Implement per the design notes. Add the `NoIndex` error variant to `EngineServingError` in `serving.rs`.

- [ ] **Step 3: BUCK + run**

**Definite (not conditional):** the engine-serving lib currently has NO `iceberg` dep, but `vector_search` takes `&SqlCatalog` and calls `tbl.file_io()` (an `iceberg::io::FileIO`). Add `iceberg = { git = "https://github.com/apache/iceberg-rust", rev = "148afc50ee950b2cd8d99c0243242ef45f3948c5" }` to `src/services/engine-serving/Cargo.toml`, run `cargo generate-lockfile` + `./tools/buckify.sh` (the `reindeer-check` hook enforces Cargo/BUCK sync), and add `"//third-party:iceberg"` to the `engine-serving` `rust_library` `deps` in `BUCK`. Then add a `vector-search` `loom_fixture_test` (deps: `:engine-serving`, `//src/control-plane/core:core`, `//src/control-plane/postgres:postgres`, arrow, iceberg, sqlx, tokio, serde_json, tempfile, time, uuid).

> NOTE: adding iceberg pins a 4th consumer of the git dep — that is fine (it shares the existing pinned rev). Do NOT bump the rev. After `buckify.sh`, `git diff third-party/BUCK` should be empty (the crate is already in the graph via postgres/engine/worker).

Run: `buck2 test //src/services/engine-serving:vector-search > /tmp/t7.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t7.log`
Expected: PASS (3 tests).

- [ ] **Step 4: Commit**

```bash
git add src/services/engine-serving/src/vector_search.rs src/services/engine-serving/src/lib.rs \
        src/services/engine-serving/src/serving.rs src/services/engine-serving/BUCK \
        src/services/engine-serving/tests/vector_search.rs
git commit -m "feat(engine-serving): exact k-NN cold/hot merge (vector_search)"
```

---

### Task 8: `engine-wire` — `BuildVectorIndex` RPC + `VectorSearchTicket`

**Files:**
- Modify: `src/services/engine-wire/proto/engine_control.proto`
- Modify: `src/services/engine-wire/src/client.rs`
- Modify: `src/services/engine-wire/src/flight.rs`
- Modify: `src/services/engine-wire/BUCK`
- Test: `src/services/engine-wire/tests/vector_search_ticket.rs`

**Interfaces:**
- Produces:
  - proto: `rpc BuildVectorIndex (BuildVectorIndexRequest) returns (BuildVectorIndexResponse);` with `BuildVectorIndexRequest { string schema=1; string name=2; string column=3; }` and `BuildVectorIndexResponse { int64 covered_snapshot=1; string puffin_path=2; int64 row_count=3; }`.
  - `GrpcQueueClient::build_vector_index(&self, schema: String, name: String, column: String) -> Result<(i64, String, i64)>`.
  - `engine_wire::flight::VectorSearchTicket { schema, name, column, query: Vec<f32>, k: u32 }` with `encode()`/`decode()` (JSON) — `#[serde(deny_unknown_fields)]` so it never aliases a `FlightTicket`.
  - `FlightTableClient::vector_search(&self, ticket: VectorSearchTicket) -> Result<Vec<RecordBatch>>`.

- [ ] **Step 1: Edit the proto**

Add the RPC line to `service EngineControl` and the two messages at the end of `engine_control.proto`:

```proto
  rpc BuildVectorIndex (BuildVectorIndexRequest) returns (BuildVectorIndexResponse);
```

```proto
message BuildVectorIndexRequest  { string schema = 1; string name = 2; string column = 3; }
message BuildVectorIndexResponse { int64 covered_snapshot = 1; string puffin_path = 2; int64 row_count = 3; }
```

(The `pb-gen` genrule regenerates the tonic stubs automatically — no manual codegen.)

- [ ] **Step 2: Write the failing ticket test**

`src/services/engine-wire/tests/vector_search_ticket.rs`:

```rust
use engine_wire::flight::{FlightTicket, VectorSearchTicket};

#[test]
fn vector_search_ticket_json_roundtrips() {
    let t = VectorSearchTicket {
        schema: "wh".into(),
        name: "docs".into(),
        column: "embedding".into(),
        query: vec![1.0, 0.0, 0.5, 0.25],
        k: 5,
    };
    let bytes = t.encode();
    let back = VectorSearchTicket::decode(&bytes).unwrap();
    assert_eq!(back, t);
}

#[test]
fn file_ticket_is_not_a_vector_ticket() {
    // A file FlightTicket must NOT decode as a VectorSearchTicket (disjoint fields).
    let ft = FlightTicket { schema: "wh".into(), name: "docs".into(), files: vec!["a".into()] };
    assert!(VectorSearchTicket::decode(&ft.encode()).is_err());
}

#[test]
fn vector_ticket_is_not_a_file_ticket() {
    // Symmetric: a VectorSearchTicket must NOT decode as a FlightTicket, so the
    // engine's file-path branch never swallows a k-NN ticket.
    let vt = VectorSearchTicket {
        schema: "wh".into(), name: "docs".into(), column: "embedding".into(),
        query: vec![1.0], k: 1,
    };
    assert!(FlightTicket::decode(&vt.encode()).is_err());
}
```

- [ ] **Step 3: Implement**

In `engine-wire/src/flight.rs`, add (next to `FlightTicket`):

```rust
/// A loom-native Flight `do_get` ticket requesting an engine-side k-NN search.
/// JSON-encoded; `deny_unknown_fields` guarantees it never aliases a `FlightTicket`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VectorSearchTicket {
    pub schema: String,
    pub name: String,
    pub column: String,
    pub query: Vec<f32>,
    pub k: u32,
}

impl VectorSearchTicket {
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("VectorSearchTicket is always serializable")
    }
    pub fn decode(bytes: &[u8]) -> std::result::Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}
```

**REQUIRED (verified necessary):** `FlightTicket` (`engine-wire/src/flight.rs:22`) currently has **no** `#[serde(deny_unknown_fields)]`. Add it to `FlightTicket`'s derive in this task. Without it, a `VectorSearchTicket` JSON could be silently accepted by `FlightTicketReq::decode` in the engine's `do_get` (the file-path branch), misrouting the query. Both tickets carry `schema`/`name`; the disjoint extra fields (`files` vs `column`/`query`/`k`) make decode unambiguous **only** when both have `deny_unknown_fields`. The Task 8 disjointness test (`file_ticket_is_not_a_vector_ticket`) plus a symmetric `vector_ticket_is_not_a_file_ticket` assertion guard this — add the second assertion too.

> NOTE (encode `.expect`): mirror `FlightTicket::encode` verbatim — it uses an unannotated `serde_json::to_vec(...).expect(...)` and already passes the clippy gate, so `VectorSearchTicket::encode` needs no extra `#[expect(clippy::expect_used)]`. If clippy unexpectedly flags it, add the annotation, but the existing twin shows it will not.

Add `FlightTableClient::vector_search` (mirror `fetch`, but encode a `VectorSearchTicket`):

```rust
pub async fn vector_search(&self, ticket: VectorSearchTicket) -> Result<Vec<RecordBatch>> {
    let resp = self
        .inner
        .clone()
        .do_get(Ticket { ticket: ticket.encode().into() })
        .await
        .map_err(crate::client::be)?;
    let stream = FlightRecordBatchStream::new_from_flight_data(
        resp.into_inner().map_err(arrow_flight::error::FlightError::from),
    );
    stream.try_collect().await.map_err(crate::client::be)
}
```

In `engine-wire/src/client.rs`, add to `impl GrpcQueueClient`:

```rust
/// Build (or rebuild) the flat vector index for `(schema, name, column)`.
/// Returns `(covered_snapshot, puffin_path, row_count)`.
pub async fn build_vector_index(
    &self,
    schema: String,
    name: String,
    column: String,
) -> Result<(i64, String, i64)> {
    let resp = self
        .inner
        .clone()
        .build_vector_index(pb::BuildVectorIndexRequest { schema, name, column })
        .await
        .map_err(be)?
        .into_inner();
    Ok((resp.covered_snapshot, resp.puffin_path, resp.row_count))
}
```

- [ ] **Step 4: BUCK + run**

Add a `vector-search-ticket` `rust_test` to `engine-wire/BUCK` (mirror `flight-ticket`: deps `:engine-wire`, `//third-party:serde_json`).

Run: `buck2 test //src/services/engine-wire:vector-search-ticket //src/services/engine-wire:compact-rpc > /tmp/t8.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t8.log`
Expected: PASS (the proto change must still compile the existing `compact-rpc` test + the whole engine-wire lib).

- [ ] **Step 5: Commit**

```bash
git add src/services/engine-wire/proto/engine_control.proto src/services/engine-wire/src/client.rs \
        src/services/engine-wire/src/flight.rs src/services/engine-wire/BUCK \
        src/services/engine-wire/tests/vector_search_ticket.rs
git commit -m "feat(engine-wire): BuildVectorIndex RPC + VectorSearchTicket"
```

---

### Task 9: `engine` — wire the RPC + the Flight query branch

**Files:**
- Modify: `src/services/engine/src/service.rs` (the `BuildVectorIndex` RPC impl)
- Modify: `src/services/engine/src/flight.rs` (`do_get` vector branch + `do_get_vector_search`)
- Modify: `src/services/engine/BUCK`
- Test: `src/services/engine/tests/vector_search_flight.rs`

**Interfaces:**
- Consumes: `control_plane_postgres::vector_index::build_vector_index`, `engine_serving::vector_search`, `engine_wire::flight::VectorSearchTicket`.

- [ ] **Step 1: Implement the control RPC (in `service.rs`)**

Add to `impl pb::engine_control_server::EngineControl for EngineControlService`:

```rust
async fn build_vector_index(
    &self,
    req: Request<pb::BuildVectorIndexRequest>,
) -> std::result::Result<Response<pb::BuildVectorIndexResponse>, Status> {
    let r = req.into_inner();
    let table = TableRef { schema: r.schema, name: r.name };
    let built = control_plane_postgres::vector_index::build_vector_index(
        &self.catalog,
        &self.pool,
        &table,
        &r.column,
        control_plane_core::Metric::Cosine, // slice 1: Cosine default; metric arg reserved
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

- [ ] **Step 2: Implement the Flight query branch (in `flight.rs`)**

Add a `do_get_vector_search` helper on `FlightDataService` and a branch in `do_get`, inserted **after** the `TicketStatementQuery` block and **before** `FlightTicketReq::decode`:

```rust
// loom-native k-NN ticket (JSON). Disjoint fields from FlightTicket
// (deny_unknown_fields on both) make this unambiguous.
if let Ok(vs) = engine_wire::flight::VectorSearchTicket::decode(&ticket.ticket) {
    return self.do_get_vector_search(vs).await;
}
```

```rust
async fn do_get_vector_search(
    &self,
    vs: engine_wire::flight::VectorSearchTicket,
) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
    let table = TableRef { schema: vs.schema, name: vs.name };
    let batch = engine_serving::vector_search(
        &self.catalog,
        &self.pool,
        &table,
        &vs.column,
        &vs.query,
        vs.k as usize,
    )
    .await
    .map_err(|e| Status::internal(e.to_string()))?;
    let input = futures::stream::iter(std::iter::once(Ok(batch)));
    let stream = FlightDataEncoderBuilder::new()
        .build(input)
        .map_err(|e| Status::internal(e.to_string()));
    Ok(Response::new(Box::pin(stream)))
}
```

> NOTE: map `EngineServingError::NoIndex` to `Status::not_found` (not `internal`) so the no-index case is a clean, distinguishable error over the wire. Add a small match in `do_get_vector_search` instead of the blanket `to_string()`/`internal`. Add `engine-serving` + `engine-wire` to the engine lib's deps if not already present (they are).

- [ ] **Step 3: Write the fixture test (engine-level e2e)**

`src/services/engine/tests/vector_search_flight.rs` — boot the engine Flight server over a UDS against a fixture DB + seeded table, build an index (call the build RPC via `GrpcQueueClient` OR the primitive directly), then `FlightTableClient::vector_search(...)` and assert the returned batch's identities are the exact top-k. Mirror the existing `flight-sql` fixture test's server/socket bootstrap. Also assert a no-index ticket yields a `not_found`-mapped error.

> Read `src/services/engine/tests/` (the `flight-sql` test) for the exact UDS server bootstrap + client dial; reuse it verbatim.

- [ ] **Step 4: BUCK + run**

Add a `vector-search-flight` `loom_fixture_test` to `engine/BUCK` (mirror `flight-sql` deps + core/postgres/engine-serving/engine-wire/arrow-flight/iceberg/tokio/uuid).

Run: `buck2 test //src/services/engine:vector-search-flight //src/services/engine:flight-sql > /tmp/t9.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t9.log`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/services/engine/src/service.rs src/services/engine/src/flight.rs \
        src/services/engine/BUCK src/services/engine/tests/vector_search_flight.rs
git commit -m "feat(engine): BuildVectorIndex RPC + Flight k-NN do_get branch"
```

---

### Task 10: `worker` — the `build_vector_index` job handler

**Files:**
- Modify: `src/services/worker/src/handler.rs`
- Modify: `src/services/worker/src/main.rs`
- Modify: `src/services/worker/BUCK`
- Test: `src/services/worker/tests/build_vector_index.rs`

**Interfaces:**
- Produces: `pub async fn handle_build_vector_index(client: GrpcQueueClient, tuning: WorkerTuning, job: Job) -> std::result::Result<(), JobFailure>`.
- Consumes: `core::{BUILD_VECTOR_INDEX_JOB_KIND, BuildVectorIndexJob}`, `GrpcQueueClient::build_vector_index`.

- [ ] **Step 1: Implement the handler (in `handler.rs`)**

```rust
pub async fn handle_build_vector_index(
    client: GrpcQueueClient,
    tuning: WorkerTuning,
    job: Job,
) -> std::result::Result<(), JobFailure> {
    let BuildVectorIndexJob { schema, name, column } =
        serde_json::from_value(job.payload).map_err(|e| JobFailure {
            error: format!("bad build_vector_index payload: {e}"),
            policy: RetryPolicy::Abandon,
        })?;
    client
        .build_vector_index(schema, name, column)
        .await
        .map_err(|e| JobFailure {
            error: e.to_string(),
            policy: RetryPolicy::Retry { delay: tuning.backoff(job.attempts) },
        })?;
    Ok(())
}
```

Add the imports (`BUILD_VECTOR_INDEX_JOB_KIND` only needed in `main.rs`; `BuildVectorIndexJob` here).

- [ ] **Step 2: Register the kind + dispatch (in `main.rs`)**

Add `BUILD_VECTOR_INDEX_JOB_KIND` to the `worker.run(&[...])` kinds slice, and add a match arm in the dispatch closure:

```rust
k if k == BUILD_VECTOR_INDEX_JOB_KIND => {
    worker::handler::handle_build_vector_index(flush.clone(), worker_tuning, job).await
}
```

(Import `control_plane_core::BUILD_VECTOR_INDEX_JOB_KIND` at the top of `main.rs`.)

- [ ] **Step 3: Write the job-wiring fixture test**

`src/services/worker/tests/build_vector_index.rs` — enqueue a `build_vector_index` job; run the worker (or invoke `handle_build_vector_index` against a live engine over UDS); assert the produced artifact (mirror row + puffin sidecar) matches a direct `build_vector_index` primitive call. Mirror `worker/tests/compact-e2e`'s engine+worker bootstrap.

> Read the `compact-e2e` test for the exact engine-server + worker bootstrap and reuse it; the assertion is "job path produces the same `vector_index` mirror row as the direct primitive."

- [ ] **Step 4: BUCK + run**

Add `build-vector-index` `loom_fixture_test` to `worker/BUCK` (mirror `compact-e2e` deps).

Run: `buck2 test //src/services/worker:build-vector-index //src/services/worker:compact-e2e > /tmp/t10.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t10.log`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/services/worker/src/handler.rs src/services/worker/src/main.rs \
        src/services/worker/BUCK src/services/worker/tests/build_vector_index.rs
git commit -m "feat(worker): build_vector_index job handler + dispatch"
```

---

### Task 11: Full-suite green + clippy + docs register

**Files:**
- Modify: `docs/ROADMAP.md` (via `loom-docs-update` at finish)

- [ ] **Step 1: Full test sweep**

Run: `buck2 test //src/... > /tmp/all.log 2>&1; grep -E "Tests finished|FAIL" /tmp/all.log`
Expected: all pass, no FAIL. Investigate any regression (especially the postgres fixture suite — a shared-schema or migration ordering issue surfaces here).

- [ ] **Step 2: Clippy across first-party Rust**

Run: `./tools/clippy-all.sh > /tmp/clippy.log 2>&1; grep -E "warning|error" /tmp/clippy.log | head`
Expected: clean (empty `[clippy.txt]` per target). Fix any pedantic/restriction hits in the new production modules with `?`/`.get`/`#[expect(..., reason=...)]`.

- [ ] **Step 3: prek hooks (formatting, EOF, inline-test guard, sqlx-in-sync)**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/prek.log 2>&1; tail -20 /tmp/prek.log`
Expected: all hooks pass. Commit any in-place fixes the hooks make.

- [ ] **Step 4: Close the register item** — run `loom-docs-update` to flip `road-puffin-vector-index` `- [ ]`→`- [x]`, set `status:done`, add `pr:#<n>` once the PR exists. Stage `docs/ROADMAP.md`.

- [ ] **Step 5: Final commit**

```bash
git add -A
git commit -m "chore: full-suite green, clippy clean, close road-puffin-vector-index"
```

---

## Self-Review

**1. Spec coverage** (each Decision/Surface/Testing/Acceptance item → task):

- VectorIndex trait + Metric + VectorKey + FlatIndex (exact, Cosine+L2, serde) → **Task 2**. ✓ (Acceptance 4 metrics)
- `loom-vector-index-v1` Puffin read/write byte-exact + footer → **Task 1** (raw) + **Task 5** (FlatIndex). ✓ (Acceptance 1)
- `vector_index` mirror table + migration + binding lookup → **Task 4**. ✓
- Build primitive (serving-read → FlatIndex → Puffin → mirror row + lineage, one tx) → **Task 6**. ✓ (Acceptance 2)
- `build_vector_index` job kind + payload → **Task 3**; worker handler → **Task 10**. ✓ (Acceptance 3)
- Engine-side hot/cold exact-kNN merge over internal Flight SQL → **Task 7** (merge) + **Task 8** (ticket) + **Task 9** (Flight branch). ✓ (Acceptance 4)
- No-index deterministic error → **Task 7** (`NoIndex`) + **Task 9** (`not_found` mapping). ✓ (Acceptance 5)
- Append-only invariant (born-after-S delta, flushed-between counted once) → `inline_delta_batch` (**Task 6**) + headline test (**Task 7**). ✓
- Governance (lineage event + mirror artifact + same read-path ACL) → **Task 6** lineage; query rides the engine read path. ✓
- `buck2 test //src/...` green, defaults unchanged → **Task 11**. ✓ (Acceptance 6)
- Tests: Puffin round-trip (T1/T5), FlatIndex pure (T2), build primitive fixture (T6), query merge fixture (T7), no-index (T7/T9), job wiring (T10), defaults (T11). ✓

**2. Placeholder scan:** Fixture-test bodies (T6, T7, T10) use `panic!("REPLACE…")` markers (not `todo!()`, so a forgotten scaffold fails loudly at runtime rather than compiling green). Each carries a concrete assertion plan — for the headline merge test (T7) the exact id-set / no-dup / ascending-distance / flushed-between-S-and-Q assertions are spelled out; only the seed/bootstrap boilerplate is delegated to "mirror `iceberg_landing.rs`/`flight-sql`/`compact-e2e`", which the implementing subagent reads in-context. (Task 6's build-primitive test still notes a `todo!()` scaffold for its seed boilerplate only — replace before commit.) All production code is complete.

**3. Type consistency:** `VectorKey`/`Metric`/`FlatIndex`/`IndexKind`/`distance` names are consistent across core (T2), the puffin wrapper (T5), the build primitive (T6), and the merge (T7). `VectorIndexRow` fields match between insert/lookup (T4) and the primitive (T6). The proto `BuildVectorIndexResponse {covered_snapshot, puffin_path, row_count}` matches `BuiltIndex` (T6), the client tuple `(i64, String, i64)` (T8), and the RPC impl (T9). `VectorSearchTicket {schema,name,column,query,k}` matches between engine-wire (T8) and the engine branch (T9). `vector_search(catalog, pool, table, column, query, k)` matches between T7 (def) and T9 (call). `build_vector_index(catalog, pool, table, column, **metric**, run_id)` matches between T6 (def), the T9 RPC call (passes `Metric::Cosine`), and the T7 L2 test.

**Blockers from the plan review — all resolved in-plan:** (1) `current_snapshot` is a `Catalog` *trait* method and `Snapshot.id` is `SnapshotId(pub i64)` → T6/T7 now specify `use control_plane_core::Catalog;` and `.id.0`. (2) identity resolver → T6 now carries the concrete `query_scalar!` against `ontology.object_type(table_schema, table_name, identity)` with the nullable-`identity` `.flatten()` + deterministic error. (3) `table_id` resolver → unified on the existing `pub crate::iceberg_mirror::live_table_id` across T4/T6/T7. (4) engine-serving `iceberg` dep → T7 makes adding it to `Cargo.toml` + `BUCK` (+ `buckify.sh`) a definite step. (5) `FlightTicket` `deny_unknown_fields` → T8 required action + symmetric disjointness tests.

**Residual risks for subagents** (resolve by reading the named files, not guessing): (a) exact iceberg-rust puffin builder field-method + local-FileIO constructor names on the pinned rev (T1); (b) the exact field-id-by-name accessor on `iceberg::spec::Schema` (T6; `0` fallback acceptable); (c) the exact fixture bootstrap APIs (`Fixture`, engine UDS server) reused across T4/T6/T7/T9/T10.
