# road-test-wire-harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Collapse the cross-crate vector/wire test seed-prologue clones (the
register's `road-test-wire-harness` cluster) into a shared test-support home —
a new `//src/testing` package with two `rust_library` targets (`:seed` for the
vector/catalog seed fixtures, `:flight` for the engine-UDS spawn harness with
connect-retry readiness) — then migrate the cloned test files onto it,
package by package, with every test assertion preserved.

**Architecture:** TEST-ONLY. The harness is the cross-crate analog of
query-api's `:e2e-support` (a `rust_library` of pub helpers that `rust_test`
targets dep on) and of `testkit` (PUBLIC visibility, consumed across
packages). Two targets, not one, so dependency blast radius stays tiered:
`:seed` deps only `core` + `postgres` + arrow (usable by control-plane tests
without building any service crate); `:flight` deps `engine`/`engine-serving`/
`engine-wire` on top (usable by the wire-side tests, which already carry those
deps). Migration is mechanical per test file: swap the hand-rolled prologue
helpers for the shared ones; deliberately divergent seeds (different
topologies, id kinds, dims) STAY LOCAL, per the e2e-support precedent.

**Tech Stack:** Rust (edition 2024), buck2 `rust_library` + `loom_fixture_test`,
`PgFixture::shared()`, the #304 `ObjectType::build` seed DSL, tonic UDS
serving, lucidshark-duplo for the before/after proof.

## Scope notes and verified drift

(Fresh census on branch `work/road-test-wire-harness`, 2026-07-03 — all
register/spec line numbers are stale; everything below is re-anchored by
symbol.)

- **"engine-wire" appears in the item's mental model but its tests carry ZERO
  prologues.** All 9 files under `src/services/engine-wire/tests/` are
  pure-logic conversion/serialization tests (no PgFixture, no `make_catalog`,
  no seed). The actual cluster spans **control-plane/postgres, engine-serving,
  engine, worker, and query-api** (the e2e-support private copies). "Wire" in
  the item name refers to the Flight-over-UDS side, which this plan serves via
  `:flight`. Recorded here as a prompt/spec-vs-tree correction; the migration
  tasks target the five crates that actually hold clones.
- **The spec's `spawn_engine_uds(fx, db, EngineOpts)` sketch omits the
  warehouse parameter** — every real call site passes one. Added:
  `spawn_engine_uds(fx, db, warehouse, opts)`.
- **Two spec bullets are OUT OF SCOPE for this TEST-ONLY item and are
  deferred at close** (Task 9 records a FUTURE item): (a) *fixture boot
  telemetry* — it edits `src/control-plane/postgres/src/fixture.rs`, a `src/`
  module compiled into the production `:postgres` library, which the
  zero-production-code ground rule forbids touching (also `#[traced_test]`
  standardization rides with it); (b) *ingest router support*
  (`ingest_router`/`sample_batch`/`post_ipc`) — the ingest package is outside
  this cluster and its `sample_batch` has exactly 2 copies (low value; rides
  the `loom-duplication-fix` routine or the deferred item).
- **Everything the harness needs is already pub** — `FlightDataService` /
  `EngineControlService` (pub fields), `fixture::{PgFixture, IcebergWriter}`,
  `iceberg_landing::{land, InlineLimits}`, `vector_index::build_vector_index`,
  the `SqlCatalogBuilder` surface, `IcebergActionWriter::new`. **No
  pub-ification findings; zero production edits required.**
- **First-party BUCK is handwritten and all deps are already vendored** — the
  new package needs no `Cargo.toml`, no reindeer run, no `third-party/BUCK`
  delta.

## Fresh duplication census (2026-07-03, this branch)

Method: `buck2 run //tools:lucidshark-duplo -- <file-list> --json -m 20` over
all 188 `tests/*.rs` files in
`control-plane/postgres`, `engine-serving`, `engine`, `engine-wire`, `worker`,
`query-api` (the exact command is re-run as the Task 9 proof). Result: **180
duplicate pairs** (92 distinct cross-file file-pairs, 11 intra-file).
Cluster breakdown:

| Cluster | Pairs | Dup lines | Crates | What is cloned |
| --- | --- | --- | --- | --- |
| **Vector seed/search** (16 files) | 102 | 3168 | postgres, engine-serving, engine, worker, query-api | `columns()` (id long + embedding vector(4)), `ipc_body(&[(i64, [f32; 4])])`, `lineage_evt`, `make_catalog`, `ids`/`distances`, and the `seed_and_build*` composites (`vector_search.rs` self-duplicates the composite 3× — 234 intra-file dup lines) |
| **Wire/iceberg prologue** | 18 | 480 | postgres, engine, worker | `make_catalog` (byte-identical in 25+ files — zero variation, verified), spawn blocks, id-column `ipc_body` variants |
| Cross vector↔wire | 7 | — | — | mostly `make_catalog` + `columns` overlap |
| query-api-internal (typed_filter/update_delete/graph/action families) | remainder | — | query-api only | intentional divergence or already-tracked #304 residuals — **NOT this item** |

Vector-cluster membership and per-file variance (from side-by-side reads):

| File | Shares canonical helpers | Variance (STAYS LOCAL) |
| --- | --- | --- |
| engine-serving `vector_search.rs` | ALL + `seed_and_build`/`_ivf`/`_hnsw` composites + `ids`/`distances` | — (this is the canonical copy) |
| engine-serving `vector_index_auto_rebuild.rs` | ALL incl. an identical `seed_and_build` + `ids` | rebuild-specific steps local |
| engine-serving `inline_vector_sql.rs` | `columns`, `ipc_body`, `make_catalog` | local `seed()` (file+inline rows split), `read_vectors`, table-arg `lineage_evt(table)` |
| engine-serving `vector_search_identity_kinds.rs` | `make_catalog` only | id-kind parametrized `columns(id_ty)`/`ipc_from`/`ids_str` — deliberate divergence |
| postgres `vector_index_build.rs` / `_hnsw.rs` / `_ivf.rs` | `columns`, `ipc_body`, `make_catalog`, `lineage` (renamed `lineage_evt`, body identical incl. `{"source": "test"}` payload) | assert against build/lookup primitives, no composite |
| postgres `vector_index_multi.rs` | `make_catalog`, `lineage` | `[f32; 8]` dim-8 `columns`/`ipc_body` — deliberate divergence |
| postgres `vector_index_inline_delta.rs` | `make_catalog` | id-kind parametrized seed — deliberate divergence |
| postgres `flush_vector_rebuild.rs` | `columns`, `ipc_body`, `make_catalog`, `lineage` | local `setup()` (defines type, lands nothing) |
| postgres `vector_landing.rs` | `columns`, `make_catalog` | width-parametrized `ipc_body(width)`, `(schema, name)` lineage — local |
| postgres `vector_index_named.rs` / `vector_index_mirror.rs` | none (direct mirror inserts) | **not migrated** |
| engine `vector_search_flight.rs` | ALL + `ids`/`distances` + `spawn_flight` | Flight-client assertions local |
| worker `build_vector_index.rs` | `columns`, `ipc_body`, `make_catalog`, `spawn_server` (control+flight UDS) | `lineage` payload differs (`"build-vector-index-e2e-test"`) — stays local |
| query-api `e2e_support.rs` | `vector_columns`/`vector_ipc_body`/`vector_lineage_evt` (private copies; the file itself says "Copied from `engine-serving/tests/vector_search.rs::ipc_body` (the canonical recipe)") + `spawn_engine`/`spawn_engine_writer` | `seed_vector_type` is pub API of the support lib — kept as a facade |

Spawn-harness census: `spawn_flight(fx, db, warehouse) -> (TempDir, String)`
exists 5× (engine `flight_sql.rs`, `flight_ticket_membership.rs`,
`vector_search_flight.rs`; query-api `engine_wire_serving_e2e.rs`,
`governed_flight_export_e2e.rs`) — bodies identical except two trivial
`let _ = …` vs `drop(…)` captures. Inline UDS spawn blocks additionally in
engine `wire.rs`(2×), `compact_wire.rs`, `write_wire.rs`, `ticket_errors.rs`
and worker `build_vector_index.rs` (`spawn_server`, control+flight),
`e2e.rs`, `compact_e2e.rs`, `flight_roundtrip.rs`. **All readiness syncs are
`sleep(20ms)`-and-hope: 13 fixed-delay readiness sleeps tree-wide** (plus 3
deliberate time-passage sleeps, which stay). `make_catalog` is byte-identical
in every one of its 25+ copies (20 in postgres tests alone).

Existing helpers the harness REUSES rather than duplicates: the #304
`ObjectType::build(…).prop_req(…).identity(…).done()` DSL (type definition
inside `seed_docs_table`), `PgFixture::shared()`/`fresh_db()`/`pool_for()`/
`pg_dsn()` (all boot goes through the shared fixture), and
`fixture::IcebergWriter` (the wire tests' row-seeding helper — already shared
in `postgres::fixture`; NOT reimplemented here, `spawn_flight_uds` slots in
beside it).

## Global Constraints

- Spec: `docs/superpowers/specs/2026-07-02-pillar-idioms-audit-design.md`,
  § "road-test-wire-harness — cross-crate fixtures for the vector/wire
  clusters". Register: `docs/ROADMAP.md` `#road-test-wire-harness` (locate by
  id, never line number).
- **TEST-ONLY: zero production code changes.** Nothing under any crate's
  `src/` directory is edited except the two NEW files in `src/testing/`
  (which is a test-support package, compiled into no production binary). If a
  migration wants a production symbol that isn't pub, the file is NOT
  migrated and the finding is recorded in the Task 9 close prose.
- **Assertions stay byte-identical.** No test function's assertion predicates
  or message strings change. Exactly two mechanical renames are permitted
  inside assertions, mapping 1:1 with no semantic delta: `ids(` →
  `ids_i64(` and `distances(` → `distances_f32(`. `assert_knn` may be adopted
  ONLY where a test's local block asserts exactly the helper's four
  predicates (row count, nearest id, nearest-counted-once, distances
  ascending) or a strict subset of them; any additional local assert stays,
  verbatim, after the helper call.
- **Deliberate seed divergence stays local** (different graph/dataset
  topologies, id kinds, vector dims, payloads) — the e2e-support precedent.
  The per-file variance table above is the authority: only cells in the
  "Shares canonical helpers" column are swapped.
- **Fixture discipline:** every harness path boots PG via
  `PgFixture::shared()` — the harness takes `&PgFixture` and never calls
  `start()`. New smoke tests are `loom_fixture_test` targets.
- Tests are `rust_test`/`loom_fixture_test` BUCK targets, never inline
  `#[cfg(test)]`. The two new library sources are non-`rust_test` support
  sources and carry the crate-level `#![allow(...reason)]` block (byte-copied
  from `e2e_support.rs`), per the lint policy.
- No new third-party deps; no `Cargo.toml`, no reindeer, no `.sqlx` churn.
- **Never pipe `buck2 test`/`buck2 bxl` through `tail`/`head`** — redirect to
  a file and grep it. Multi-target fixture runs use `-j 8`. Whole-tree builds
  only as `buck2 build -M none //src/...`.
- `buck2 run //tools:prek -- run --all-files` must report zero `Failed`
  before **every** commit. Conventional Commits; one commit per task.

## Deliberate behavior changes (the whitelist)

**EMPTY.** No production behavior and no test assertion changes. Two
test-infrastructure notes that are NOT whitelist entries (they change no
asserted behavior):

1. Readiness syncs move from fixed `sleep(20ms)` to **connect-retry** (poll
   `UnixStream::connect` up to 5s). Strictly more reliable; asserts nothing
   new; removes the tree's classic flake source.
2. Seed lineage run-ids are minted inside the harness exactly as the local
   copies minted them (fresh `RunId(uuid)` per seed; one shared run across
   the two cold lands, matching the canonical `seed_and_build`). No test in
   the migrated set asserts on lineage payload/run identity; any file that
   does (none found) would keep its local lineage fn per the divergence rule.

---

### Task 1: `//src/testing:seed` — vector/catalog seed fixtures + smoke

**Files:**
- Create: `src/testing/BUCK`
- Create: `src/testing/seed.rs`
- Create: `src/testing/tests/seed_smoke.rs`

**Interfaces:**
- Produces (all `pub` in crate `loom_test_seed`):
  - `fn vec4_columns() -> Vec<ColumnSpec>`
  - `fn vec4_ipc(rows: &[(i64, [f32; 4])]) -> Vec<u8>`
  - `fn test_lineage(run: RunId, table: &TableRef) -> LineageEvent`
  - `async fn local_sql_catalog(dsn: String, warehouse: &str) -> SqlCatalog`
  - `fn ids_i64(batch: &RecordBatch) -> Vec<i64>`
  - `fn distances_f32(batch: &RecordBatch) -> Vec<f32>`
  - `fn assert_knn(batch: &RecordBatch, nearest: i64, k: usize)`
  - `fn cold_limits() -> InlineLimits` / `fn hot_limits() -> InlineLimits`
  - `struct VectorSeed { pub catalog: SqlCatalog, pub pool: sqlx::PgPool, pub cp: PgControlPlane, pub table: TableRef, pub wh: tempfile::TempDir }`
  - `async fn seed_docs_table(fx: &PgFixture, db: &str) -> VectorSeed`
  - `async fn seed_docs_vector(fx: &PgFixture, db: &str, index: &str, metric: Metric, spec: IndexSpec, build: bool) -> VectorSeed`
  - `async fn land_vec4(s: &VectorSeed, rows: &[(i64, [f32; 4])], limits: InlineLimits)`
- Consumes: `PgFixture::shared()/pool_for/pg_dsn`, `iceberg_landing::{land, InlineLimits}`, `vector_index::build_vector_index`, the #304 `ObjectType::build` DSL.

- [x] **Step 1: Write the BUCK file and the failing smoke test.** Create
  `src/testing/BUCK`:

```python
load("//src/control-plane/postgres:defs.bzl", "loom_fixture_test")

# Cross-crate vector/wire test harness (road-test-wire-harness): the shared
# home for the seed prologues previously hand-rolled per test file. `:seed`
# stays service-free (deps only core/postgres/arrow) so control-plane tests
# can use it without building any service crate; the engine-UDS spawn harness
# lives in the sibling `:flight` target.
rust_library(
    name = "seed",
    crate = "loom_test_seed",
    srcs = ["seed.rs"],
    crate_root = "seed.rs",
    edition = "2024",
    visibility = ["PUBLIC"],
    deps = [
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
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

loom_fixture_test(
    name = "seed-smoke",
    crate = "seed_smoke",
    srcs = ["tests/seed_smoke.rs"],
    crate_root = "tests/seed_smoke.rs",
    deps = [
        ":seed",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:arrow-array",
        "//third-party:arrow-ipc",
        "//third-party:arrow-schema",
        "//third-party:sqlx",
        "//third-party:tokio",
    ],
)
```

Create `src/testing/tests/seed_smoke.rs`:

```rust
//! Smoke for `loom_test_seed`: the shared vector seed prologue produces the
//! same wh.docs world the per-file copies did — type defined, 4 cold rows
//! landed, flat index built — and the IPC/extractor helpers round-trip.

use arrow_ipc::reader::StreamReader;
use control_plane_core::{IndexSpec, Metric};
use control_plane_postgres::fixture::PgFixture;
use loom_test_seed::{
    assert_knn, hot_limits, ids_i64, land_vec4, seed_docs_vector, vec4_columns, vec4_ipc,
};

#[test]
fn vec4_ipc_round_trips_ids() {
    let body = vec4_ipc(&[(1, [1.0, 0.0, 0.0, 0.0]), (2, [0.0, 1.0, 0.0, 0.0])]);
    let reader = StreamReader::try_new(body.as_slice(), None).expect("reader");
    let batches: Vec<_> = reader.collect::<Result<_, _>>().expect("batches");
    assert_eq!(batches.len(), 1);
    assert_eq!(ids_i64(&batches[0]), vec![1, 2]);
    assert_eq!(vec4_columns().len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seed_docs_vector_seeds_and_builds() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    // build = true exercises define_type + 2 cold lands + define_vector_index
    // + build_vector_index end-to-end (each step expects internally).
    let s = seed_docs_vector(fx, &db, "by_flat", Metric::Cosine, IndexSpec::Flat, true).await;
    assert_eq!(s.table.schema, "wh");
    assert_eq!(s.table.name, "docs");
    // Hot landing through the same seed works too.
    land_vec4(&s, &[(5, [0.9, 0.1, 0.0, 0.0])], hot_limits()).await;
    // The pool is live and points at the seeded db.
    let one: i64 = sqlx::query_scalar("select 1")
        .fetch_one(&s.pool)
        .await
        .expect("select 1");
    assert_eq!(one, 1);
}

#[test]
fn assert_knn_checks_the_four_shared_predicates() {
    use std::sync::Arc;
    let batch = arrow_array::RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("_distance", arrow_schema::DataType::Float32, false),
        ])),
        vec![
            Arc::new(arrow_array::Int64Array::from(vec![5, 1])),
            Arc::new(arrow_array::Float32Array::from(vec![0.1_f32, 0.2])),
        ],
    )
    .expect("batch");
    assert_knn(&batch, 5, 2);
}
```

- [x] **Step 2: Run — expect RED** (build failure: `seed.rs` does not exist)

```bash
buck2 test //src/testing:seed-smoke -j 8 > /tmp/t1red.log 2>&1; \
  grep -E "error|Tests finished|FAIL" /tmp/t1red.log | head -5
```

- [x] **Step 3: Write the library.** Create `src/testing/seed.rs`. Every body
  below is the canonical copy from
  `src/services/engine-serving/tests/vector_search.rs` (verified byte-identical
  in the census), renamed to the spec's names:

```rust
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::let_underscore_must_use,
    clippy::unused_result_ok,
    clippy::map_err_ignore,
    clippy::unreachable,
    reason = "cross-crate test/fixture harness code, not a production path"
)]
//! Cross-crate vector/wire test seed fixtures (`road-test-wire-harness`).
//!
//! THE shared home for the seed prologue previously hand-rolled per test
//! file across control-plane/postgres, engine-serving, engine, worker, and
//! query-api: the vec4 docs table (`vec4_columns`/`vec4_ipc`), the local-fs
//! `SqlCatalog` (`local_sql_catalog` — 25+ byte-identical `make_catalog`
//! copies collapsed), the seed/build composite (`seed_docs_table`/
//! `seed_docs_vector`, built on the #304 `ObjectType::build` DSL), and the
//! k-NN result extractors/asserts (`ids_i64`/`distances_f32`/`assert_knn`).
//!
//! Deliberately divergent seeds (id-kind parametrized, dim-8, width-swept)
//! stay local to their test files — divergence is intent, not duplication.
//! The engine-UDS spawn harness lives in the sibling `loom_test_flight`
//! crate so this one never pulls service deps into control-plane tests.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_array::{Float32Array, Int64Array, RecordBatch};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    ColumnSpec, ControlPlane, DatasetId, EventType, IndexSpec, LineageEvent, Metric, ObjectType,
    RunId, TableRef, TypeName, VectorIndexDef,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use control_plane_postgres::vector_index::build_vector_index;
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;

/// The canonical vec4 landing columns: `id: long` + `embedding: vector(4)`.
pub fn vec4_columns() -> Vec<ColumnSpec> {
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

/// Build an Arrow IPC body with `id: long` + `embedding: list<float32>` (4 elements).
pub fn vec4_ipc(rows: &[(i64, [f32; 4])]) -> Vec<u8> {
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
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(id_array), Arc::new(emb_array)],
    )
    .expect("batch");
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &schema).expect("writer");
        w.write(&batch).expect("write");
        w.finish().expect("finish");
    }
    buf
}

/// A minimal Complete lineage event targeting `table` (payload
/// `{"source": "test"}` — the canonical test payload).
pub fn test_lineage(run: RunId, table: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(table).dataset_ref()],
        payload: serde_json::json!({ "source": "test" }),
    }
}

/// Build a local-filesystem `SqlCatalog` over `dsn` with `warehouse` as the
/// `file://` warehouse root — the single home for the tree's 25+
/// byte-identical `make_catalog` copies.
pub async fn local_sql_catalog(dsn: String, warehouse: &str) -> SqlCatalog {
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

/// Downcast the identity column (column 0, `Int64Array`) → `Vec<i64>` in row order.
pub fn ids_i64(batch: &RecordBatch) -> Vec<i64> {
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("identity column is Int64");
    (0..col.len()).map(|i| col.value(i)).collect()
}

/// Downcast column 1 (`Float32Array`, the `_distance` column) → `Vec<f32>` in row order.
pub fn distances_f32(batch: &RecordBatch) -> Vec<f32> {
    let col = batch
        .column(1)
        .as_any()
        .downcast_ref::<Float32Array>()
        .expect("_distance column is Float32");
    (0..col.len()).map(|i| col.value(i)).collect()
}

/// The four shared k-NN result predicates, exactly as the per-file assert
/// blocks stated them: `k` rows returned, `nearest` is first, `nearest`
/// appears exactly once (cold∪hot dedup), distances ascending. Call sites
/// keep any FURTHER local assertions verbatim after this call.
pub fn assert_knn(batch: &RecordBatch, nearest: i64, k: usize) {
    assert_eq!(batch.num_rows(), k, "k rows returned");
    let ids = ids_i64(batch);
    assert_eq!(ids[0], nearest, "nearest id first");
    assert_eq!(
        ids.iter().filter(|&&x| x == nearest).count(),
        1,
        "nearest counted exactly once"
    );
    let dists = distances_f32(batch);
    for w in dists.windows(2) {
        assert!(w[0] <= w[1], "distances ascending");
    }
}

/// `InlineLimits` forcing every landed row to Parquet (the "cold" seed shape).
pub fn cold_limits() -> InlineLimits {
    InlineLimits {
        inline_byte_limit: 0,
        flush_byte_threshold: i64::MAX,
    }
}

/// `InlineLimits` forcing every landed row inline (the "hot delta" shape).
pub fn hot_limits() -> InlineLimits {
    InlineLimits {
        inline_byte_limit: usize::MAX,
        flush_byte_threshold: i64::MAX,
    }
}

/// Everything a seeded vector test needs, by name (the old per-file
/// composites returned an anonymous 4-tuple). `wh` is the warehouse TempDir
/// guard — hold the whole struct alive across search calls.
pub struct VectorSeed {
    pub catalog: SqlCatalog,
    pub pool: sqlx::PgPool,
    pub cp: PgControlPlane,
    pub table: TableRef,
    pub wh: tempfile::TempDir,
}

/// Seed the canonical `wh.docs` world: register the `Docs` type (identity
/// `id`, `embedding vector(4)`, via the #304 seed DSL) and land cold rows
/// 1..=4 (forced to Parquet) in the canonical two batches sharing one run id.
pub async fn seed_docs_table(fx: &PgFixture, db: &str) -> VectorSeed {
    let wh = tempfile::tempdir().expect("wh");
    let pool = fx.pool_for(db).await;
    let catalog = local_sql_catalog(fx.pg_dsn(db), &wh.path().display().to_string()).await;
    let cp = PgControlPlane::new(pool.clone(), Duration::from_secs(5));
    let table = TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    };

    cp.ontology()
        .define_type(
            ObjectType::build("Docs", ("wh", "docs"))
                .prop_req("id", "Long")
                .prop_req("embedding", "vector(4)")
                .identity("id")
                .done(),
        )
        .await
        .expect("define_type");

    let run = RunId(uuid::Uuid::new_v4());
    let rows_1_2: &[(i64, [f32; 4])] = &[(1, [1.0, 0.0, 0.0, 0.0]), (2, [0.0, 1.0, 0.0, 0.0])];
    let rows_3_4: &[(i64, [f32; 4])] = &[(3, [0.0, 0.0, 1.0, 0.0]), (4, [0.0, 0.0, 0.0, 1.0])];
    for rows in [rows_1_2, rows_3_4] {
        land(
            &pool,
            &catalog,
            &table,
            &vec4_columns(),
            &vec4_ipc(rows),
            cold_limits(),
            test_lineage(run, &table),
        )
        .await
        .expect("land cold rows");
    }

    VectorSeed {
        catalog,
        pool,
        cp,
        table,
        wh,
    }
}

/// `seed_docs_table` + declare the named vector index over `embedding`
/// (metric/spec supplied) and, when `build`, run `build_vector_index` —
/// the full canonical `seed_and_build`/`_ivf`/`_hnsw` composite, index
/// shape now a parameter.
pub async fn seed_docs_vector(
    fx: &PgFixture,
    db: &str,
    index: &str,
    metric: Metric,
    spec: IndexSpec,
    build: bool,
) -> VectorSeed {
    let s = seed_docs_table(fx, db).await;
    s.cp
        .ontology()
        .define_vector_index(VectorIndexDef {
            name: index.into(),
            type_name: TypeName("Docs".into()),
            property: "embedding".into(),
            metric,
            spec,
        })
        .await
        .expect("define_vector_index");
    if build {
        let build_run = RunId(uuid::Uuid::new_v4());
        build_vector_index(&s.catalog, &s.pool, &s.table, index, build_run)
            .await
            .expect("build_vector_index");
    }
    s
}

/// Land more vec4 rows into a seeded world (fresh run id, canonical lineage) —
/// `hot_limits()` for the classic "row 5 inline after the covered snapshot".
pub async fn land_vec4(s: &VectorSeed, rows: &[(i64, [f32; 4])], limits: InlineLimits) {
    land(
        &s.pool,
        &s.catalog,
        &s.table,
        &vec4_columns(),
        &vec4_ipc(rows),
        limits,
        test_lineage(RunId(uuid::Uuid::new_v4()), &s.table),
    )
    .await
    .expect("land vec4 rows");
}
```

If the #304 DSL's builder methods differ in name (verify against
`control_plane_core::ontology` — `ObjectType::build(name, (schema, table))`,
`.prop_req(name, ty)`, `.identity(col)`, `.done()` per the seed-DSL register
entry and the cp-adapter-hygiene plan's usage), adjust the `define_type` call
to the actual DSL surface; the DSL's parity tests guarantee output identical
to the old handwritten `ObjectType` literal, which is what the migrated files
constructed.

- [x] **Step 4: Run — GREEN**

```bash
buck2 test //src/testing:seed-smoke -j 8 > /tmp/t1.log 2>&1; \
  grep -E "Tests finished|FAIL" /tmp/t1.log
```

Expected: `Fail 0` (3 tests). If `define_type` via the DSL rejects
`vector(4)` (it must not — postgres vector tests define the same property
type), STOP and check the DSL parity tests before proceeding.

- [x] **Step 5: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files > /tmp/prek1.log 2>&1; grep -E "Failed" /tmp/prek1.log || echo CLEAN
git add src/testing
git commit -m "test(testing): loom_test_seed shared vector seed fixtures"
```

---

### Task 2: `//src/testing:flight` — engine UDS spawn harness with connect-retry

**Files:**
- Create: `src/testing/flight.rs`
- Create: `src/testing/tests/flight_smoke.rs`
- Modify: `src/testing/BUCK` (add `:flight`, `:flight-smoke`)

**Interfaces:**
- Produces (all `pub` in crate `loom_test_flight`):
  - `struct EngineOpts { pub control: bool, pub flight: bool, pub inline_byte_limit: usize, pub flush_byte_threshold: i64 }` (+ `Default`: `{ control: false, flight: true, inline_byte_limit: 16 * 1024 * 1024, flush_byte_threshold: i64::MAX }`)
  - `struct EngineGuard { pub sock: String, pub handle: tokio::task::JoinHandle<()>, /* private */ _sock_dir: tempfile::TempDir }`
  - `async fn spawn_engine_uds(fx: &PgFixture, db: &str, warehouse: &str, opts: EngineOpts) -> EngineGuard`
  - `async fn spawn_flight_uds(fx: &PgFixture, db: &str, warehouse: &str) -> EngineGuard`
- Consumes: `loom_test_seed::local_sql_catalog`; `engine::flight::FlightDataService`, `engine::service::EngineControlService`, `engine_serving::IcebergActionWriter`, `engine_wire::pb::engine_control_server::EngineControlServer`, `control_plane_postgres::iceberg_catalog::IcebergCatalog`.

- [x] **Step 1: Write the failing smoke test.** Create
  `src/testing/tests/flight_smoke.rs`:

```rust
//! Smoke for `loom_test_flight`: `spawn_engine_uds` is READY on return —
//! a client connects immediately, no post-spawn sleep (the connect-retry
//! readiness contract that replaces the tree's 13 `sleep(20ms)` syncs).

use control_plane_postgres::fixture::PgFixture;
use loom_test_flight::{EngineOpts, spawn_engine_uds, spawn_flight_uds};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_plane_is_ready_on_return() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let eng = spawn_engine_uds(
        fx,
        &db,
        &wh.path().display().to_string(),
        EngineOpts {
            control: true,
            ..EngineOpts::default()
        },
    )
    .await;
    // No sleep: the readiness contract is that this connects first try.
    let client = engine_wire::client::GrpcQueueClient::connect(&eng.sock).await;
    assert!(client.is_ok(), "control reachable on return: {:?}", client.err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flight_only_spawn_accepts_connections() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let eng = spawn_flight_uds(fx, &db, &wh.path().display().to_string()).await;
    let stream = tokio::net::UnixStream::connect(&eng.sock).await;
    assert!(stream.is_ok(), "flight socket accepts: {:?}", stream.err());
}
```

Append to `src/testing/BUCK`:

```python
# Engine-over-UDS spawn harness. Separate target so `:seed` consumers
# (control-plane tests) never build the service crates.
rust_library(
    name = "flight",
    crate = "loom_test_flight",
    srcs = ["flight.rs"],
    crate_root = "flight.rs",
    edition = "2024",
    visibility = ["PUBLIC"],
    deps = [
        ":seed",
        "//src/control-plane/postgres:postgres",
        "//src/services/engine:engine",
        "//src/services/engine-serving:engine-serving",
        "//src/services/engine-wire:engine-wire",
        "//third-party:arrow-flight",
        "//third-party:tempfile",
        "//third-party:tokio",
        "//third-party:tokio-stream",
        "//third-party:tonic",
    ],
)

loom_fixture_test(
    name = "flight-smoke",
    crate = "flight_smoke",
    srcs = ["tests/flight_smoke.rs"],
    crate_root = "tests/flight_smoke.rs",
    deps = [
        ":flight",
        "//src/control-plane/postgres:postgres",
        "//src/services/engine-wire:engine-wire",
        "//third-party:tempfile",
        "//third-party:tokio",
    ],
)
```

- [x] **Step 2: Run — expect RED** (`flight.rs` missing)

```bash
buck2 test //src/testing:flight-smoke -j 8 > /tmp/t2red.log 2>&1; \
  grep -E "error|Tests finished|FAIL" /tmp/t2red.log | head -5
```

- [x] **Step 3: Write the library.** Create `src/testing/flight.rs`. The
  service construction is the union of the tree's verified spawn bodies
  (engine `spawn_flight`, worker `spawn_server`, e2e-support `spawn_engine` —
  identical modulo which services they add); only the readiness sync is new:

```rust
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::let_underscore_must_use,
    clippy::unused_result_ok,
    clippy::map_err_ignore,
    clippy::unreachable,
    reason = "cross-crate test/fixture harness code, not a production path"
)]
//! Engine-over-UDS spawn harness (`road-test-wire-harness`).
//!
//! One `spawn_engine_uds(fx, db, warehouse, EngineOpts)` replaces the tree's
//! copies of `spawn_flight` (engine + query-api tests), `spawn_server`
//! (worker tests), and e2e-support's `spawn_engine` — with **connect-retry
//! readiness** instead of the `sleep(20ms)`-and-hope sync those copies used
//! (the tree's classic flake source). The guard owns the socket dir and the
//! serve task; hold it alive for the duration of the test.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arrow_flight::flight_service_server::FlightServiceServer;
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use engine::flight::FlightDataService;
use engine::service::EngineControlService;
use engine_serving::IcebergActionWriter;
use engine_wire::pb::engine_control_server::EngineControlServer;
use loom_test_seed::local_sql_catalog;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;

/// Which services the spawned engine serves, and the action-writer limits.
/// Defaults mirror the tree's most common spawn: Flight only, 16 MiB inline
/// limit, no flush threshold.
pub struct EngineOpts {
    /// Serve `EngineControl` (queue/commit RPCs).
    pub control: bool,
    /// Serve Arrow Flight (`FlightDataService`).
    pub flight: bool,
    /// `IcebergActionWriter` inline byte limit (control plane writer).
    pub inline_byte_limit: usize,
    /// `IcebergActionWriter` flush byte threshold.
    pub flush_byte_threshold: i64,
}

impl Default for EngineOpts {
    fn default() -> Self {
        Self {
            control: false,
            flight: true,
            inline_byte_limit: 16 * 1024 * 1024,
            flush_byte_threshold: i64::MAX,
        }
    }
}

/// Keep-alive guard for a spawned engine: socket path, serve-task handle,
/// and the owned socket TempDir. Dropping it tears the socket dir down.
pub struct EngineGuard {
    /// Filesystem path of the bound unix socket.
    pub sock: String,
    /// The tokio task running the tonic server.
    pub handle: tokio::task::JoinHandle<()>,
    _sock_dir: tempfile::TempDir,
}

/// Spawn an engine on a fresh UDS serving the services `opts` selects,
/// backed by `db` + `warehouse`. Returns only after a client can connect
/// (connect-retry readiness — no fixed sleep).
pub async fn spawn_engine_uds(
    fx: &PgFixture,
    db: &str,
    warehouse: &str,
    opts: EngineOpts,
) -> EngineGuard {
    assert!(opts.control || opts.flight, "spawn at least one service");
    let sock_dir = tempfile::tempdir().expect("socket dir");
    let sock_path = sock_dir.path().join("engine.sock");
    let sock = sock_path.to_string_lossy().to_string();
    let pool = fx.pool_for(db).await;

    let control = if opts.control {
        let cp = PgControlPlane::new(pool.clone(), Duration::from_millis(5000));
        let catalog = local_sql_catalog(fx.pg_dsn(db), warehouse).await;
        let writer_catalog = local_sql_catalog(fx.pg_dsn(db), warehouse).await;
        let writer = IcebergActionWriter::new(
            Arc::new(writer_catalog),
            pool.clone(),
            opts.inline_byte_limit,
            opts.flush_byte_threshold,
        );
        Some(EngineControlServer::new(EngineControlService {
            cp,
            catalog,
            pool: pool.clone(),
            retention: Duration::from_secs(7 * 24 * 3600),
            writer,
        }))
    } else {
        None
    };
    let flight = if opts.flight {
        let catalog = local_sql_catalog(fx.pg_dsn(db), warehouse).await;
        Some(FlightServiceServer::new(FlightDataService {
            catalog,
            serving_catalog: IcebergCatalog::new(pool.clone()),
            serving_store: None,
            pool,
        }))
    } else {
        None
    };

    let listener = tokio::net::UnixListener::bind(&sock_path).expect("bind uds");
    let incoming = UnixListenerStream::new(listener);
    let handle = tokio::spawn(async move {
        drop(
            Server::builder()
                .add_optional_service(control)
                .add_optional_service(flight)
                .serve_with_incoming(incoming)
                .await,
        );
    });
    await_uds_ready(&sock_path).await;

    EngineGuard {
        sock,
        handle,
        _sock_dir: sock_dir,
    }
}

/// The classic Flight-only spawn (the 5 `spawn_flight` copies).
pub async fn spawn_flight_uds(fx: &PgFixture, db: &str, warehouse: &str) -> EngineGuard {
    spawn_engine_uds(fx, db, warehouse, EngineOpts::default()).await
}

/// Poll-connect until the UDS accepts (≤ 5s), replacing fixed-sleep syncs.
async fn await_uds_ready(path: &Path) {
    for _ in 0..250 {
        if tokio::net::UnixStream::connect(path).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("engine UDS not ready after 5s: {}", path.display());
}
```

- [x] **Step 4: Run — GREEN**

```bash
buck2 test //src/testing:flight-smoke //src/testing:seed-smoke -j 8 \
  > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t2.log
```

Expected: `Fail 0`. If `add_optional_service` is unavailable on the vendored
tonic (it has existed since tonic 0.6 — it will be), fall back to a match on
`(control, flight)` building the three concrete server shapes; do NOT add a
sleep back.

- [x] **Step 5: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files > /tmp/prek2.log 2>&1; grep -E "Failed" /tmp/prek2.log || echo CLEAN
git add src/testing
git commit -m "test(testing): loom_test_flight engine-UDS spawn harness with connect-retry readiness"
```

---

### Task 3: Migrate engine-serving tests

**Files:**
- Modify: `src/services/engine-serving/tests/vector_search.rs`
- Modify: `src/services/engine-serving/tests/vector_index_auto_rebuild.rs`
- Modify: `src/services/engine-serving/tests/inline_vector_sql.rs`
- Modify: `src/services/engine-serving/tests/vector_search_identity_kinds.rs`
- Modify: `src/services/engine-serving/BUCK` (add `//src/testing:seed` to the
  four targets' deps)

**Interfaces:**
- Consumes (Task 1): `seed_docs_table`, `seed_docs_vector`, `land_vec4`,
  `vec4_columns`, `vec4_ipc`, `test_lineage`, `local_sql_catalog`, `ids_i64`,
  `distances_f32`, `hot_limits`, `cold_limits`, `VectorSeed`.

- [x] **Step 1: Baseline — run the package suite green**

```bash
buck2 test //src/services/engine-serving: -j 8 > /tmp/t3pre.log 2>&1; \
  grep -E "Tests finished|FAIL" /tmp/t3pre.log
```

- [x] **Step 2: Migrate `vector_search.rs`.** Deletions: fns `columns`,
  `ipc_body`, `lineage_evt`, `make_catalog`, `ids`, `distances`,
  `seed_and_build`, `seed_and_build_ivf`, `seed_and_build_hnsw` (the whole
  hand-rolled prologue). New imports replace the now-unused ones:

```rust
use control_plane_core::{IndexSpec, Metric, RunId, TableRef};
use control_plane_postgres::fixture::PgFixture;
use engine_serving::EngineServingError;
use loom_test_seed::{
    distances_f32, hot_limits, ids_i64, land_vec4, local_sql_catalog, seed_docs_vector,
    test_lineage, vec4_columns, vec4_ipc,
};
```

(Keep whatever of the old import list the remaining bodies still use — e.g.
`control_plane_postgres::iceberg_landing::{InlineLimits, land}` is still
needed by the `no_bound_index_is_deterministic_error` local seed; prune the
rest. `rustc` unused-import warnings are the checklist.)

Per-test call-site mapping (assertion lines untouched except the two
sanctioned renames):

- `let (catalog, pool, _cp, _wh) = seed_and_build(fx, &db, M).await;`
  → `let s = seed_docs_vector(fx, &db, "by_flat", M, IndexSpec::Flat, true).await;`
- `… = seed_and_build_ivf(fx, &db, M).await;`
  → `let s = seed_docs_vector(fx, &db, "by_ivf", M, IndexSpec::IvfFlat { nlist: Some(2) }, true).await;`
- `… = seed_and_build_hnsw(fx, &db, M).await;`
  → `let s = seed_docs_vector(fx, &db, "by_hnsw", M, IndexSpec::Hnsw { m: None, ef_construction: None }, true).await;`
- Every subsequent `&catalog` → `&s.catalog`, `&pool` → `&s.pool`; the local
  `let table = TableRef { … "docs" … }` bindings may stay (they equal
  `s.table`) or become `&s.table` — pick one per file and be consistent.
- Each inline "land row 5" block (`let run = …; let inline: &[…] = &[(5, …)];
  land(&pool, &catalog, &table, &columns(), &ipc_body(inline),
  InlineLimits { inline_byte_limit: usize::MAX, … }, lineage_evt(run,
  &table)).await.expect("land inline row 5");`) →
  `land_vec4(&s, &[(5, [0.95, 0.05, 0.0, 0.0])], hot_limits()).await;`
  (values copied verbatim from each site — they are all `(5, [0.95, 0.05,
  0.0, 0.0])`).
- `ids(&batch)` → `ids_i64(&batch)`; `distances(&batch)` →
  `distances_f32(&batch)` (assertion predicates and messages otherwise
  byte-identical — do NOT adopt `assert_knn` here in the first pass; the
  blocks carry extra per-test asserts like `id_vec[1] == 1` and distinct
  messages, and keeping them verbatim is the safer mechanical move).
- `no_bound_index_is_deterministic_error` keeps its LOCAL hand-rolled seed
  (different type/table `NoDocs`/`nodocs` — deliberate divergence) but swaps
  helper names: `columns()` → `vec4_columns()`, `ipc_body(rows)` →
  `vec4_ipc(rows)`, `make_catalog(…)` → `local_sql_catalog(…)`,
  `lineage_evt(run, &table)` → `test_lineage(run, &table)`.

- [x] **Step 3: Migrate `vector_index_auto_rebuild.rs`** — its
  `seed_and_build` is verbatim-identical to the canonical: same deletions
  (`columns`/`ipc_body`/`lineage_evt`/`make_catalog`/`ids`/`seed_and_build`),
  same call-site mapping (`seed_docs_vector(fx, &db, "by_flat", M,
  IndexSpec::Flat, true)` — confirm the index name/spec each call used and
  carry them verbatim), `ids(` → `ids_i64(`. Rebuild-specific steps
  (auto-rebuild triggers, second search) stay untouched.

- [x] **Step 4: Migrate `inline_vector_sql.rs`** — swap `columns()` →
  `vec4_columns()`, `ipc_body` → `vec4_ipc`, `make_catalog` →
  `local_sql_catalog` (delete the three local fns). The local `seed()`,
  `read_vectors()`, and table-arg `lineage_evt(table)` STAY (divergent
  shapes); if `lineage_evt(table)`'s body is `test_lineage(RunId(uuid…),
  table)` with the identical `{"source": "test"}` payload, reimplement it as
  that one-liner; if its payload differs, leave it entirely alone.

- [x] **Step 5: Migrate `vector_search_identity_kinds.rs`** — swap ONLY
  `make_catalog` → `local_sql_catalog`. Everything else (id-kind
  parametrized `columns(id_ty)`, `ipc_from`, `object_type`, local
  `seed_and_build`, `ids_str`) is deliberate divergence and stays.

- [x] **Step 6: BUCK deps.** In `src/services/engine-serving/BUCK`, add
  `"//src/testing:seed",` to the `deps` of the four `loom_fixture_test`
  targets (`vector-search`, the auto-rebuild target, the inline-vector-sql
  target, the identity-kinds target — locate by `srcs`). Remove any
  third-party dep a file no longer imports (e.g. `arrow-ipc` where the local
  `ipc_body` was the only user) — check with the build, not by guessing.

- [x] **Step 7: Run — package green; diff shows prologue-only changes**

```bash
buck2 test //src/services/engine-serving: -j 8 > /tmp/t3.log 2>&1; \
  grep -E "Tests finished|FAIL" /tmp/t3.log
git diff --stat src/services/engine-serving
git diff src/services/engine-serving/tests | grep -E "^[-+].*assert" | sort | uniq -c | sort -rn | head
```

Expected: `Fail 0`; net-negative diff; the assert-line grep shows ONLY
`ids(`→`ids_i64(`/`distances(`→`distances_f32(` renames (each `-` line has a
matching `+` line differing only in the helper name). Any other assert delta
is a bug — revert it.

- [x] **Step 8: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files > /tmp/prek3.log 2>&1; grep -E "Failed" /tmp/prek3.log || echo CLEAN
git add src/services/engine-serving
git commit -m "test(engine-serving): migrate vector suites onto loom_test_seed"
```

---

### Task 4: Migrate control-plane/postgres vector tests

**Files:**
- Modify: `src/control-plane/postgres/tests/vector_index_build.rs`
- Modify: `src/control-plane/postgres/tests/vector_index_hnsw.rs`
- Modify: `src/control-plane/postgres/tests/vector_index_ivf.rs`
- Modify: `src/control-plane/postgres/tests/vector_index_multi.rs`
- Modify: `src/control-plane/postgres/tests/vector_index_inline_delta.rs`
- Modify: `src/control-plane/postgres/tests/flush_vector_rebuild.rs`
- Modify: `src/control-plane/postgres/tests/vector_landing.rs`
- Modify: `src/control-plane/postgres/BUCK` (add `"//src/testing:seed"` to
  those 7 targets)

**Interfaces:**
- Consumes (Task 1): `vec4_columns`, `vec4_ipc`, `test_lineage`,
  `local_sql_catalog`.

NOT touched: `vector_index_named.rs`, `vector_index_mirror.rs`,
`vector_index_unit.rs` (no prologue clones). This task is a **helper swap
only** — postgres tests assert against the build/lookup primitives with
per-file seed sequencing, so the `seed_docs_*` composites are NOT adopted
here (their per-test land/define ordering is part of what each test pins).

- [x] **Step 1: Baseline green** (scope to the 7 targets; find their names by
  `srcs` in the BUCK file — e.g. `vector-index-build` for
  `tests/vector_index_build.rs`):

```bash
buck2 test //src/control-plane/postgres:vector-index-build \
  //src/control-plane/postgres:vector-index-hnsw \
  //src/control-plane/postgres:vector-index-ivf \
  //src/control-plane/postgres:vector-index-multi \
  //src/control-plane/postgres:vector-index-inline-delta \
  //src/control-plane/postgres:flush-vector-rebuild \
  //src/control-plane/postgres:vector-landing -j 8 \
  > /tmp/t4pre.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t4pre.log
```

(If any of these target names differs, read
`src/control-plane/postgres/BUCK` and use the target whose `srcs` matches the
file; do not guess.)

- [x] **Step 2: Per-file swaps** (delete the local fn; import the shared one;
  rename call sites — nothing else changes):

| File | Swaps |
| --- | --- |
| `vector_index_build.rs`, `vector_index_hnsw.rs`, `vector_index_ivf.rs` | `columns()` → `vec4_columns()`, `ipc_body` → `vec4_ipc`, `make_catalog` → `local_sql_catalog`, local `lineage(run, &table)` → `test_lineage(run, &table)` (verified: body identical incl. `{"source": "test"}` payload — BYTE-CHECK before deleting; if a payload differs, keep that file's local fn) |
| `vector_index_multi.rs` | `make_catalog` → `local_sql_catalog`, `lineage` → `test_lineage` (same byte-check); the `[f32; 8]` `columns`/`ipc_body` STAY LOCAL |
| `vector_index_inline_delta.rs` | `make_catalog` → `local_sql_catalog` only (id-kind parametrized seed stays) |
| `flush_vector_rebuild.rs` | `columns()` → `vec4_columns()`, `ipc_body` → `vec4_ipc`, `make_catalog` → `local_sql_catalog`, `lineage` → `test_lineage` (byte-check); local `setup()` stays |
| `vector_landing.rs` | `columns()` → `vec4_columns()`, `make_catalog` → `local_sql_catalog`; width-param `ipc_body(width)` and `(schema, name)` `lineage` STAY LOCAL |

Byte-check recipe (run per file before deleting a local `lineage`/helper):

```bash
sed -n '/fn lineage/,/^}/p' src/control-plane/postgres/tests/vector_index_build.rs
```

and compare field-for-field against `test_lineage` in
`src/testing/seed.rs` (only the fn name may differ).

- [x] **Step 3: BUCK deps** — add `"//src/testing:seed",` to each of the 7
  targets in `src/control-plane/postgres/BUCK`; prune deps the file no longer
  imports (the build's unused-crate output is the checklist).

- [x] **Step 4: Run — the 7 targets green; assert-diff discipline**

```bash
buck2 test //src/control-plane/postgres:vector-index-build \
  //src/control-plane/postgres:vector-index-hnsw \
  //src/control-plane/postgres:vector-index-ivf \
  //src/control-plane/postgres:vector-index-multi \
  //src/control-plane/postgres:vector-index-inline-delta \
  //src/control-plane/postgres:flush-vector-rebuild \
  //src/control-plane/postgres:vector-landing -j 8 \
  > /tmp/t4.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t4.log
git diff src/control-plane/postgres/tests | grep -E "^[-+].*assert" | head
```

Expected: `Fail 0`; ZERO assert-line changes in this task (postgres files
had no local `ids`/`distances`).

- [x] **Step 5: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files > /tmp/prek4.log 2>&1; grep -E "Failed" /tmp/prek4.log || echo CLEAN
git add src/control-plane/postgres
git commit -m "test(control-plane): migrate postgres vector tests onto loom_test_seed"
```

---

### Task 5: Postgres `make_catalog` sweep (wire/iceberg prologue cluster)

**Files:**
- Modify (13 files, `make_catalog` → `local_sql_catalog` ONLY):
  `src/control-plane/postgres/tests/iceberg_landing.rs`,
  `iceberg_flush.rs`, `iceberg_gc.rs`, `iceberg_compact.rs`,
  `iceberg_read.rs`, `iceberg_overwrite.rs`, `iceberg_tx_compact.rs`,
  `iceberg_control_plane.rs`, `iceberg_write_roundtrip.rs`,
  `inline_flush_trigger.rs`, `overwrite_end_caps_inline.rs`,
  `sqlcatalog_execute_commit.rs`, `iceberg_schema_evolution_land.rs`
- Modify: `src/control-plane/postgres/BUCK` (add `"//src/testing:seed"` to
  the 13 matching targets)

This is the "tree-wide literal sweep" the #304 close deferred to this item,
scoped to the one helper that is byte-identical in every copy. The files'
`columns()`/`ipc_body()` variants (id-only, id+name, reordered) STAY LOCAL —
they encode per-test schemas.

- [x] **Step 1: Verify byte-identity, then swap.** For each file:

```bash
grep -A13 "async fn make_catalog" src/control-plane/postgres/tests/iceberg_landing.rs
```

must match the `local_sql_catalog` body (modulo fn name and an optional
`std::collections::HashMap` path form). Then delete the local fn, add
`use loom_test_seed::local_sql_catalog;`, rename call sites
(`make_catalog(` → `local_sql_catalog(`). If ANY copy differs materially
(none is expected to — 20/20 postgres copies verified identical), leave that
file alone and note it for the Task 9 close prose.

- [x] **Step 2: BUCK deps** — add `"//src/testing:seed",` to each target;
  build is the unused-dep checklist.

- [x] **Step 3: Run the affected targets green** (list assembled from the
  BUCK `srcs` of the 13 files):

```bash
buck2 test //src/control-plane/postgres: -j 8 > /tmp/t5.log 2>&1; \
  grep -E "Tests finished|FAIL" /tmp/t5.log
```

(The whole-package run also re-proves Task 4; local runs keep `-j 8` per the
fixture-slot guidance. In a cloud session scope to the 13 targets instead of
the whole package if disk pressure appears.)

- [x] **Step 4: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files > /tmp/prek5.log 2>&1; grep -E "Failed" /tmp/prek5.log || echo CLEAN
git add src/control-plane/postgres
git commit -m "test(control-plane): collapse make_catalog copies onto local_sql_catalog"
```

---

### Task 6: Migrate engine tests

**Files:**
- Modify: `src/services/engine/tests/vector_search_flight.rs`,
  `flight_sql.rs`, `flight_ticket_membership.rs`, `wire.rs`,
  `compact_wire.rs`, `write_wire.rs`, `ticket_errors.rs`,
  `governed_flight.rs`
- Modify: `src/services/engine/BUCK` (deps: `"//src/testing:seed"` for all
  eight; `"//src/testing:flight"` for those whose spawn migrates)

**Interfaces:**
- Consumes (Tasks 1-2): `local_sql_catalog`, `vec4_columns`, `vec4_ipc`,
  `test_lineage`, `ids_i64`, `distances_f32`, `spawn_flight_uds`,
  `EngineGuard` (`.sock`).

- [x] **Step 1: Baseline green**

```bash
buck2 test //src/services/engine: -j 8 > /tmp/t6pre.log 2>&1; \
  grep -E "Tests finished|FAIL" /tmp/t6pre.log
```

- [x] **Step 2: `make_catalog` → `local_sql_catalog` in all eight files**
  (same byte-check + swap recipe as Task 5).

- [x] **Step 3: `spawn_flight` → `spawn_flight_uds`** in `flight_sql.rs`,
  `flight_ticket_membership.rs`, `vector_search_flight.rs` (the three
  verified-identical copies). Call-site mapping:

```rust
// before
let (_sock_dir, sock) = spawn_flight(fx, &db, &wh.path().display().to_string()).await;
// after
let eng = spawn_flight_uds(fx, &db, &wh.path().display().to_string()).await;
```

then `sock` uses become `eng.sock.clone()` (or `&eng.sock`); the `eng`
binding replaces `_sock_dir` as the keep-alive. Delete the local
`spawn_flight` fn and its now-unused imports
(`FlightServiceServer`/`UnixListenerStream`/`Server`, the `Duration` sleep).

- [x] **Step 4: Inline spawn blocks in `wire.rs` (2×), `compact_wire.rs`,
  `write_wire.rs`, `ticket_errors.rs`** — migrate each to `spawn_flight_uds`
  ONLY if its service construction is exactly
  `FlightDataService { catalog: <local make_catalog result>, serving_catalog:
  IcebergCatalog::new(pool.clone()), serving_store: None, pool }` over a plain
  UDS bind + readiness sleep. Any block that differs (extra services, a
  `serving_store: Some(…)`, custom service fields) STAYS LOCAL — record it in
  the Task 9 close prose. `governed_flight.rs` has no spawn (catalog swap
  only). Time-passage sleeps (e.g. `wire.rs`'s 100ms job-enqueue wait) are
  NOT readiness syncs — leave them.

- [x] **Step 5: `vector_search_flight.rs` vector swaps** — `columns()` →
  `vec4_columns()`, `ipc_body` → `vec4_ipc`, `lineage_evt` → `test_lineage`
  (verified identical), `ids(` → `ids_i64(`, `distances(` →
  `distances_f32(` (the sanctioned renames; all other assert text
  byte-identical).

- [x] **Step 6: BUCK deps + run green + assert-diff check**

```bash
buck2 test //src/services/engine: -j 8 > /tmp/t6.log 2>&1; \
  grep -E "Tests finished|FAIL" /tmp/t6.log
git diff src/services/engine/tests | grep -E "^[-+].*assert" | head
```

Expected: `Fail 0`; assert deltas only the two sanctioned renames.

- [x] **Step 7: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files > /tmp/prek6.log 2>&1; grep -E "Failed" /tmp/prek6.log || echo CLEAN
git add src/services/engine
git commit -m "test(engine): migrate wire/vector suites onto the shared harness"
```

---

### Task 7: Migrate worker tests

**Files:**
- Modify: `src/services/worker/tests/build_vector_index.rs`, `e2e.rs`,
  `compact_e2e.rs`, `flight_roundtrip.rs`
- Modify: `src/services/worker/BUCK` (deps: `"//src/testing:seed"` ×4,
  `"//src/testing:flight"` where the spawn migrates)

**Interfaces:**
- Consumes: `local_sql_catalog`, `vec4_columns`, `vec4_ipc`,
  `spawn_engine_uds`, `EngineOpts`, `EngineGuard`.

- [ ] **Step 1: Baseline green**

```bash
buck2 test //src/services/worker: -j 8 > /tmp/t7pre.log 2>&1; \
  grep -E "Tests finished|FAIL" /tmp/t7pre.log
```

- [ ] **Step 2: `build_vector_index.rs`** — swap `columns`/`ipc_body`/
  `make_catalog` to the shared trio; the local `lineage` STAYS (its payload is
  `"build-vector-index-e2e-test"` — deliberate divergence). Its
  `spawn_server(fx, db, wh_path)` (verified: control+flight, writer
  `16 * 1024 * 1024` / `i64::MAX`, retention 7d — exactly the harness default
  shapes) becomes:

```rust
// before
let (_sock_dir, sock) = spawn_server(fx, &db, wh_path).await;
// after
let eng = spawn_engine_uds(
    fx,
    &db,
    wh_path,
    EngineOpts {
        control: true,
        ..EngineOpts::default()
    },
)
.await;
```

with `sock` uses → `eng.sock`. Delete `spawn_server` + its unused imports.

- [ ] **Step 3: `e2e.rs`, `compact_e2e.rs`, `flight_roundtrip.rs`** —
  `make_catalog` swap always. For each file's spawn block: compare against
  the `spawn_engine_uds` construction (which services, writer limits,
  retention). Migrate only exact structural matches — pass differing writer
  limits through `EngineOpts { inline_byte_limit, flush_byte_threshold, … }`;
  if the block differs beyond what `EngineOpts` expresses (e.g. a custom
  retention driving a GC assertion), it STAYS LOCAL and is recorded for the
  close prose. Their id-column `columns`/`ipc_body(ids)` variants STAY LOCAL.

- [ ] **Step 4: BUCK deps + run green**

```bash
buck2 test //src/services/worker: -j 8 > /tmp/t7.log 2>&1; \
  grep -E "Tests finished|FAIL" /tmp/t7.log
git diff src/services/worker/tests | grep -E "^[-+].*assert" | head
```

Expected: `Fail 0`; ZERO assert-line changes.

- [ ] **Step 5: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files > /tmp/prek7.log 2>&1; grep -E "Failed" /tmp/prek7.log || echo CLEAN
git add src/services/worker
git commit -m "test(worker): migrate e2e suites onto the shared harness"
```

---

### Task 8: Migrate query-api (e2e-support facades + the two flight e2es)

**Files:**
- Modify: `src/services/query-api/tests/e2e_support.rs`
- Modify: `src/services/query-api/tests/engine_wire_serving_e2e.rs`
- Modify: `src/services/query-api/tests/governed_flight_export_e2e.rs`
- Modify: `src/services/query-api/BUCK` (`e2e-support` deps +=
  `"//src/testing:seed"`, `"//src/testing:flight"`; the two e2e targets +=
  both as needed)

**Interfaces:**
- Consumes: everything from Tasks 1-2.
- Produces: `e2e_support`'s PUBLIC surface is UNCHANGED
  (`seed_vector_type`, `spawn_engine`, `spawn_engine_writer`, `EngineGuard`)
  — its consumers (`vector_search_e2e.rs` and the wire e2es) keep compiling
  with zero edits.

- [ ] **Step 1: Baseline green**

```bash
buck2 test //src/services/query-api: -j 8 > /tmp/t8pre.log 2>&1; \
  grep -E "Tests finished|FAIL" /tmp/t8pre.log
```

- [ ] **Step 2: Retire e2e-support's private fourth copy.** In
  `e2e_support.rs`: delete `vector_columns` and `vector_ipc_body` (the
  file's own comment marks them "Copied from
  `engine-serving/tests/vector_search.rs::ipc_body` (the canonical recipe)")
  and, inside `seed_vector_type` (public signature unchanged), replace their
  uses with `loom_test_seed::{vec4_columns, vec4_ipc}`. For
  `vector_lineage_evt`, byte-check the payload first: the census read it as
  `{"source": "e2e"}` — if so it STAYS LOCAL (a payload is seed content; do
  NOT let it silently change to `"test"`); if it actually reads
  `{"source": "test"}`, delete it and use `test_lineage`. The columns/ipc
  swaps are the duplication win here either way.

- [ ] **Step 3: Delegate the spawn fns.** Replace the bodies of
  `spawn_engine` / `spawn_engine_writer` (signatures unchanged) and the local
  `EngineGuard`:

```rust
pub use loom_test_flight::EngineGuard;

/// Spawn an `EngineControlService` on a UDS and return its socket path +
/// keep-alive guard. Now a facade over `loom_test_flight::spawn_engine_uds`
/// (control-only), which adds connect-retry readiness.
pub async fn spawn_engine(
    fx: &control_plane_postgres::fixture::PgFixture,
    db: &str,
    warehouse: &std::path::Path,
    inline_byte_limit: usize,
    flush_byte_threshold: i64,
) -> (String, EngineGuard) {
    let eng = loom_test_flight::spawn_engine_uds(
        fx,
        db,
        &warehouse.display().to_string(),
        loom_test_flight::EngineOpts {
            control: true,
            flight: false,
            inline_byte_limit,
            flush_byte_threshold,
        },
    )
    .await;
    (eng.sock.clone(), eng)
}
```

(`spawn_engine_writer` already delegates to `spawn_engine` — it needs no body
change beyond the type re-export.) Precondition check: `grep -rn
"EngineGuard {" src/services/query-api/tests --include=*.rs` must show no
construction outside `e2e_support.rs`; if a test constructs the struct,
keep the local type instead and convert.

- [ ] **Step 4: The two flight e2es.** In `engine_wire_serving_e2e.rs` and
  `governed_flight_export_e2e.rs`: `make_catalog` → `local_sql_catalog`;
  local `spawn_flight` → `spawn_flight_uds` (their bodies differ from the
  canonical only in a `let _ = …` capture — semantics identical, migrate);
  `governed_flight_export_e2e.rs`'s `columns()`/`ipc_body(rows: usize)`
  variants STAY LOCAL; its non-readiness sleeps STAY.

- [ ] **Step 5: BUCK deps + run green + assert-diff check**

```bash
buck2 test //src/services/query-api: -j 8 > /tmp/t8.log 2>&1; \
  grep -E "Tests finished|FAIL" /tmp/t8.log
git diff src/services/query-api/tests | grep -E "^[-+].*assert" | head
```

Expected: `Fail 0` (including `vector_search_e2e`, `wire_harness_smoke`, and
every `update_delete*`/action e2e that deps `:e2e-support` — the public
surface didn't move); ZERO assert-line changes.

- [ ] **Step 6: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files > /tmp/prek8.log 2>&1; grep -E "Failed" /tmp/prek8.log || echo CLEAN
git add src/services/query-api
git commit -m "test(query-api): e2e-support delegates to the shared vector/wire harness"
```

---

### Task 9: Duplication proof, affected-package sweep, register close

**Files:**
- Modify: `docs/ROADMAP.md` (`#road-test-wire-harness` — locate by id)
- Modify: `docs/FUTURE.md` (new deferral item)

- [ ] **Step 1: Re-run the census — prove the drop.** Same command as the
  fresh census (file list = all `tests/*.rs` under the six packages):

```bash
find src/control-plane/postgres/tests src/services/engine-serving/tests \
  src/services/engine/tests src/services/engine-wire/tests \
  src/services/worker/tests src/services/query-api/tests -name '*.rs' \
  > /tmp/duplo-files.txt
buck2 run //tools:lucidshark-duplo -- /tmp/duplo-files.txt --json -m 20 \
  > /tmp/duplo-after.json 2>/tmp/duplo-after.err || true
python3 - <<'EOF'
import json
d = json.load(open('/tmp/duplo-after.json'))['duplicates']
vec = {"vector_search.rs","vector_index_auto_rebuild.rs","vector_search_identity_kinds.rs",
"inline_vector_sql.rs","vector_search_flight.rs","build_vector_index.rs","vector_index_build.rs",
"vector_index_hnsw.rs","vector_index_ivf.rs","vector_index_multi.rs","vector_index_named.rs",
"vector_index_inline_delta.rs","vector_index_mirror.rs","flush_vector_rebuild.rs",
"vector_landing.rs","e2e_support.rs"}
import os
b = os.path.basename
vp = [p for p in d if b(p['file1']['path']) in vec and b(p['file2']['path']) in vec]
print("total pairs:", len(d), "| vector-cluster pairs:", len(vp),
      "| vector dup lines:", sum(p['line_count'] for p in vp))
EOF
```

Baseline (this plan's census): **180 total pairs; 102 vector-cluster pairs /
3168 dup lines**. Expected after: the vector-cluster count drops to the
residue of the deliberate variants (the dim-8/id-kind/width copies) — record
the exact before/after numbers for the PR body. **Investigate any surviving
pair whose block contains a byte-copy of a harness helper** (`make_catalog`,
`vec4`-shaped `columns`/`ipc_body`, a spawn body) — that means a migration
step was missed.

- [ ] **Step 2: Affected-package sweep**

```bash
buck2 build -M none //src/... > /tmp/build9.log 2>&1; tail -3 /tmp/build9.log
buck2 test //src/testing: //src/control-plane/postgres: \
  //src/services/engine-serving: //src/services/engine: \
  //src/services/worker: //src/services/query-api: -j 8 \
  > /tmp/sweep9.log 2>&1; grep -E "Tests finished|FAIL" /tmp/sweep9.log
```

Expected: build success; `Fail 0`. (Cloud sessions: keep `-M none`, keep the
test list scoped exactly as above, and `buck2 clean` between heavy phases if
disk pressure appears.)

- [ ] **Step 3: Close the ROADMAP item** — replace the
  `road-test-wire-harness` entry (checkbox, status, prose) with:

```markdown
- [x] **Cross-crate vector/wire test harness** `{#road-test-wire-harness area:test status:done from:2026-07-02-pillar-idioms-audit-design pr:- spec:2026-07-02-pillar-idioms-audit-design}`
  Done. New `//src/testing` package, two PUBLIC `rust_library` targets (the cross-crate e2e-support/testkit pattern): `:seed` (`loom_test_seed` — `vec4_columns`/`vec4_ipc`/`test_lineage`/`local_sql_catalog`/`ids_i64`/`distances_f32`/`assert_knn`/`cold_limits`/`hot_limits` + the `seed_docs_table`/`seed_docs_vector`/`land_vec4` composites over `VectorSeed`, built on `PgFixture::shared()` and the #304 seed DSL; deps only core/postgres/arrow so control-plane tests pull no service crate) and `:flight` (`loom_test_flight` — `spawn_engine_uds(fx, db, warehouse, EngineOpts) -> EngineGuard` + `spawn_flight_uds`, with **connect-retry readiness** replacing the tree's `sleep(20ms)` syncs). Migrated (assertions byte-identical; deliberate seed divergence kept local): engine-serving vector suites (the `seed_and_build`/`_ivf`/`_hnsw` triple deleted), postgres vector tests + the 13-file `make_catalog` sweep, engine wire/vector suites (5 identical `spawn_flight` copies deleted), worker e2es (`spawn_server` deleted), and query-api's e2e-support (private vector copy retired; `spawn_engine`/`spawn_engine_writer` now facades with signatures unchanged). Vector-cluster duplication: 102 pairs / 3168 lines → <record post-migration numbers> (duplo, -m 20, same 188-file list). NOT in scope (TEST-ONLY ground rule / outside the cluster), deferred to [[fut-test-harness-residuals]]: fixture boot telemetry (edits the production-crate `fixture.rs` module), `#[traced_test]` standardization, and the ingest router support trio. engine-wire's own tests carried no prologues (pure-logic conversion tests) — the cluster's fifth crate was query-api.
```

Replace `<record post-migration numbers>` with the Step 1 output before
committing.

- [ ] **Step 4: Record the deferral in `docs/FUTURE.md`** (under the test
  area, matching the register grammar):

```markdown
- [ ] **Test-harness residuals: fixture telemetry, traced_test, ingest router support** `{#fut-test-harness-residuals area:test status:deferred from:2026-07-02-pillar-idioms-audit-design pr:- spec:2026-07-02-pillar-idioms-audit-design}`
  The spec bullets of [[road-test-wire-harness]] that its TEST-ONLY ground rule excluded: `tracing` spans in `PgFixture::start()`/`fresh_db()` (slot-wait/initdb/migrate durations) + `LOOM_FIXTURE_TIMING=1` (edits `src/control-plane/postgres/src/fixture.rs`, a module of the production `:postgres` library); adopting `#[traced_test]`/`logs_contain` as the standard for error-path logging assertions; and the ingest router support trio (`ingest_router(fx, db)`, `sample_batch`, `post_ipc` — 2 copies today in `src/services/ingest/tests/`). Also any spawn blocks the migration left local for structural divergence (recorded in the road item's close prose).
```

- [ ] **Step 5: Validate + prek + commit**

```bash
bash tools/docs.sh validate
buck2 run //tools:prek -- run --all-files > /tmp/prek9.log 2>&1; grep -E "Failed" /tmp/prek9.log || echo CLEAN
git add docs/ROADMAP.md docs/FUTURE.md
git commit -m "docs(registers): close road-test-wire-harness, defer telemetry/ingest residuals"
```

If the PR number is known when this runs (`gh pr view --json number` on the
pushed branch), replace `pr:-` with `pr:#<N>` in both entries; otherwise
leave it for the finishing flow.

---

## Self-review notes (performed at plan time)

- **Spec coverage:** the spec section's four bullets → vector fixtures
  (Tasks 1, 3-8: every named helper lands under its spec name —
  `vec4_columns`, `vec4_ipc`, `test_lineage`, `local_sql_catalog`,
  `seed_docs_vector` (+`build` flag), `ids_i64`/`distances_f32`/`assert_knn`;
  the e2e-support "private fourth copy" retires in Task 8); engine UDS
  harness (Task 2, with the spec's connect-retry requirement; `warehouse`
  param added vs the spec sketch — every call site needs it); ingest router
  support and fixture telemetry (both DEFERRED with an explicit register
  item, Task 9 — they conflict with the item's TEST-ONLY ground rule /
  four-crate cluster scope; this is the plan's one deliberate spec
  deviation, recorded in three places).
- **Placement decision:** new top-level `src/testing` package with a
  two-target split, over (a) expanding `postgres::fixture` (that module is
  compiled into the production `:postgres` crate — the TEST-ONLY rule and
  the engine-dep cycle both forbid it), (b) `testkit` (backend-neutral
  contract library; giving it postgres+engine deps inverts its purpose),
  (c) an e2e-support-style library inside one service package (control-plane
  tests dep'ing into `src/services/query-api` is the wrong layering optics,
  and e2e-support deps `:query-api` itself). PUBLIC visibility per the
  testkit/e2e-support convention.
- **Type consistency:** `VectorSeed` fields (`catalog`/`pool`/`cp`/`table`/
  `wh`) match every Task 3-8 call site; `seed_docs_vector(fx, db, index,
  metric, spec, build)` matches the spec sketch's arity; `EngineGuard.sock:
  String` matches the `eng.sock` uses in Tasks 6-8; `spawn_engine`'s facade
  returns `(String, EngineGuard)` — its downstream consumers unchanged;
  `land_vec4` takes `&VectorSeed` + `InlineLimits` from
  `cold_limits`/`hot_limits`, matching `iceberg_landing::InlineLimits`'s pub
  fields as used in every current test literal.
- **Assertion-preservation mechanics:** exactly two sanctioned renames
  (`ids`→`ids_i64`, `distances`→`distances_f32`), each verified by the
  per-task `git diff … grep assert` step; `assert_knn` ships in the library
  (spec-named, smoke-tested) but is NOT adopted in the mechanical first pass
  — the local blocks carry per-test extra asserts and message strings that
  must survive byte-identical.
- **Verified-vs-conditional migrations:** byte-identical helpers
  (`make_catalog` 25+ copies, the three `spawn_flight`s, `vector_search`'s
  and `auto_rebuild`'s `seed_and_build`, `vector_search_flight`'s
  `lineage_evt`) migrate unconditionally; everything else carries an explicit
  byte-check step and a keep-local escape hatch recorded at close. The plan
  never migrates a file the census marked "deliberate divergence".
- **Known judgment calls, recorded:** worker `build_vector_index.rs` keeps
  its local `lineage` (payload string differs); e2e-support's
  `vector_lineage_evt` payload (`"e2e"`) is preserved rather than unified;
  postgres files do NOT adopt the `seed_docs_*` composites (their per-test
  land/define ordering is pinned behavior); `EngineOpts` defaults mirror the
  most common spawn (Flight-only, 16 MiB / `i64::MAX`).
