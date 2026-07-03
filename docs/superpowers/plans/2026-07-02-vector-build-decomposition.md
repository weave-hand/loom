# road-vector-build-decomposition Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Behavior-preserving decomposition of the postgres adapter's
vector-index build path plus ONE whitelisted defect fix.
`build_vector_index` (`src/control-plane/postgres/src/vector_index.rs:376-554`,
~179 lines, 10 numbered jobs, census cc 41) decomposes onto named seams —
`resolve_build_inputs` / `collect_vectors` / `declared_dim` (pure) /
`write_sidecar` / `build_lineage_event` (pure) / `bind_index_and_emit` — and
the `Box<dyn VectorIndex>` construction match moves to core as
`IndexSpec::build(dim, metric, rows)`, the single authoritative
spec→constructor routing. The whitelisted fix closes
`iss-inline-delta-string-identity`: `inline_delta_batch` (the hot-delta leg of
k-NN search) hardcodes the identity column as `Int64`
(`Int64Builder` at `:339`, `try_get::<i64>` at `:345`, `DataType::Int64` field
at `:358`) while the cold path (`extract_rows`, `:193-255`) supports
`Int64`/`Int32`/`Utf8`; the fix decodes identity + vector through the shared
`column_array` PG→Arrow bridge (PR #301) keyed by the mirror schema's declared
`BaseType`, with red-first tests. **The core codec is untouched; the golden
characterization tests (`core/tests/vector_index_codec.rs` — exact hex + FNV)
must pass byte-identical.**

**Register drift (corrections to the ROADMAP/spec prose, verified against the
tree 2026-07-02, branch `work/road-vector-build-decomposition` off current
`main` — Wave 1 PRs #295/#301/#303 and road-vector-index-auto-rebuild (#240)
all predate this branch and are absorbed below; locate by symbol, register
line numbers are stale):**

- **The ROADMAP item says it "Fixes [[iss-vector-build-lineage-ref]] +
  [[iss-inline-delta-string-identity]]" — the lineage half is ALREADY FIXED.**
  PR #295 landed `DatasetRef::from(table)` as the canonical lineage input
  (`vector_index.rs:506`), the ISSUES entry is `status:fixed pr:#295`, and the
  fix is pinned by `postgres/tests/vector_index_build.rs:270-274`. This plan
  does NOT re-fix or re-close it; only `iss-inline-delta-string-identity`
  remains, and `build_lineage_event` (Task 5) keeps the canonical ref
  structural with a new unit pin.
- **The spec's "sharing an `identity_array` helper with `extract_rows`" is
  superseded by #301's `column_array`.** `iceberg_inline::column_array(rows,
  i, ty)` is now THE single PG-row→Arrow decoder (its doc comment says so;
  `inline_live_batch` and engine-serving's `PgTableProvider` already delegate
  to it). `extract_rows` reads *Arrow* batches, not PG rows, so a helper
  shared between it and `inline_delta_batch` is a type mismatch — the right
  shared seam for the fix is `column_array`, which makes the delta batch agree
  with `inline_live_batch` by construction. `extract_rows` is untouched.
- **The ISSUES claim "silently breaks the hot-delta search path" is imprecise
  (mechanism verified).** With a String identity, the inline table column is
  PG `text` (`pg_type_for("string")`, `iceberg_type.rs:65`), and sqlx's strict
  typing makes `r.try_get::<i64>(0)` (`vector_index.rs:345`) fail with a
  `ColumnDecode`/mismatched-types error → `ControlPlaneError::Backend` →
  `EngineServingError::Engine` (`vector_search.rs:120-122`) — the search
  request errors loudly at query time. The "silent" part is that
  `define_vector_index`/`build_vector_index` accept String identities without
  complaint (the cold path fully supports them), so the fault surfaces only
  when unflushed inline rows exist. **There is no string-form identity
  comparison anywhere in the merge** — `merge_topk`
  (`engine-serving/src/vector_search.rs:27-36`) compares distances only and
  performs no identity dedup — so no "non-canonical string form" value (e.g.
  `"07"` vs `"7"`) is constructible as a red test; the red test constructs a
  String-**typed** identity instead, which is the defect the ISSUES entry
  actually claims.
- **Integer identities break the same way (same defect class, verified).** The
  inline column for a `"integer"` identity is PG `integer` (int4), which sqlx
  also refuses to decode as `i64`. `extract_rows` handles `Int32Array`
  (`:226-227`, widening to `VectorKey::Int`), but engine-serving's
  `score_inline_batch` (`vector_search.rs:163-172`) has no `Int32Array` arm
  either — the fix adds it, mirroring `extract_rows`, so the hot leg supports
  exactly the identity kinds the cold leg does. One whitelist entry covers the
  class.
- **The spec's `:339, :345, :358` line refs for the hardcode are still
  current** (verified this branch), as is the 10-numbered-job shape of
  `build_vector_index` (179 lines today; the census "cc 41, 182 sloc" predates
  #295/#301 but the shape is unchanged).
- **The spec's `infer_dim` seam lands as `declared_dim` + a two-arm match.**
  The current dim inference (`:431-448`) does an *async* schema lookup only
  when the table is empty; a pure `infer_dim(rows, schema, column)` would
  force the schema fetch on every build. The pure, unit-testable part is the
  `vector(N)` fallback — extracted as `pub fn declared_dim(schema, column) ->
  u32` (parse moved verbatim) — and the orchestrator keeps the two-arm match
  with the schema fetch on the empty arm only. The empty-table branch is not
  reachable in a cheap fixture (a mirror table only exists once something
  landed), which is exactly why the seam must be pure to be testable — the
  unit tests in Task 4 are its first coverage.

**Architecture (key decisions, verified against the tree):**

- **`IndexSpec::build` lives in `core/src/vector_index/mod.rs`** beside
  `IndexSpec` — NOT in `codec.rs` (sacred: byte-identical goldens). It is a
  pure routing match over the existing constructors
  (`FlatIndex::build(dim, metric, rows)`,
  `IvfFlatIndex::build(dim, metric, rows, nlist)`,
  `HnswIndex::build(dim, metric, rows, m, ef_construction)` — signatures
  verified), moved verbatim from the adapter's job 7. Its unit tests assert
  `spec.build(...).serialize() == DirectConstructor::build(...).serialize()`
  (all three constructors are deterministic — fixed-seed SplitMix64), which
  chains the routing to the codec goldens without touching them.
- **The decomposition stays inside `vector_index.rs`** (554 lines today; no
  file split — core's #303 split was for a 1000+ line file). New seams are
  private except the two pure fns that need direct unit tests
  (`declared_dim`, `build_lineage_event`) — buck2 `rust_test` targets are
  external crates, so unit-tested items must be `pub`.
- **`bind_index_and_emit` takes a private `IndexBinding` struct**
  (`VectorIndexRow` minus `table_id`): `live_table_id` is resolved INSIDE the
  final transaction today (`:521`), and moving it earlier would change
  failure semantics under a concurrent table drop/re-create — so the tx keeps
  the resolution and the caller cannot hand over a complete `VectorIndexRow`.
- **`write_sidecar` keeps the object-store write BEFORE the Postgres tx**
  (current job 9 comment moved verbatim): a failed build leaves an orphan
  Puffin file, never a dangling mirror row.
- **The fix decodes per the mirror schema's declared logical type.**
  `Catalog::schema()` returns canonical logical names
  (`iceberg_catalog.rs:263-264` maps through
  `logical_from_iceberg(..).map(BaseType::canonical_name)`), so
  `resolve_logical(&c.ty)` resolves directly — `inline_delta_batch` already
  fetches this schema for vector-column detection (`:298`), so the identity
  lookup adds no query. Output arrays and `Field` data types both come from
  `BaseType` (`arrow_data_type()` / `column_array`), so the Long case is
  byte-identical to today (same `Int64Array`, same non-nullable `Int64`
  field, same `"item"` list child) — pinned by Task 1 before the change.
- **Pins before refactor:** the build path is pinned by five postgres fixture
  suites (`vector-index-build`, `vector-index-ivf`, `vector-index-hnsw`,
  `vector-index-multi`, `flush-vector-rebuild`) plus engine-serving's
  `vector-search` (cold/hot merge ×3 index kinds — all Long identity) and
  `vector-index-auto-rebuild`; `vector-index-mirror`/`vector-index-named` pin
  insert/lookup. **Unpinned:** the `inline_delta_batch` batch *contract*
  (field names/types/nullability/MVCC window — only exercised indirectly),
  the String-identity **cold** path, and the empty-table dim fallback. Task 1
  adds the first two pins green-first; the third becomes `declared_dim`'s
  unit tests (see drift note above).

**Tech Stack:** Rust (edition 2024), buck2. New test targets: postgres
`vector-index-inline-delta` (`loom_fixture_test`) + `vector-index-unit`
(pure `rust_test`), engine-serving `vector-search-identity-kinds`
(`loom_fixture_test`), core `index-spec-build` (pure `rust_test`). No
`Cargo.toml`/lockfile changes, no new deps. **No `.sqlx` changes**: every
compile-time query in `vector_index.rs` is untouched (the delta query is
runtime `AssertSqlSafe` and its SQL text does not change), so
`tools/sqlx-prepare.sh` is NOT needed — if any task finds itself editing a
`query!`/`query_scalar!` string, stop and re-plan.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-07-02-pillar-idioms-audit-design.md`
  (§ "road-vector-build-decomposition" + Wave 0 item 4), with the drift
  corrections above. Registers: `docs/ROADMAP.md`
  `#road-vector-build-decomposition`, `docs/ISSUES.md`
  `#iss-inline-delta-string-identity` (locate by id, line numbers stale).
- **Behavior-preserving except the single whitelisted change below.** In
  particular: the serialized index bytes (codec goldens byte-identical — the
  item must NOT touch `core/src/vector_index/codec.rs`), the Puffin property
  map, the mirror `vector_index` row values, the lineage event (inputs =
  canonical `DatasetRef::from(table)` per #295, outputs, payload), the SQL
  text of every query, the build's MVCC anchoring (S captured before any data
  read), the write ordering (Puffin before tx; `live_table_id` inside the
  tx), and the Long-identity delta batch bytes.
- **Existing tests pass unmodified.** No existing test function or assertion
  is edited. The only test files touched are the four NEW files
  (`postgres/tests/vector_index_inline_delta.rs`,
  `postgres/tests/vector_index_unit.rs`, `core/tests/index_spec_build.rs`,
  `engine-serving/tests/vector_search_identity_kinds.rs`) plus their BUCK
  targets.
- **TDD:** every new fn lands with its test written first and observed red
  (for pure extractions: red = compile failure on the missing symbol);
  pure-refactor tasks run their pinning suites green before AND after.
- Tests are separate `rust_test`/`loom_fixture_test` targets wired in BUCK —
  never inline `#[cfg(test)]` (`no-inline-tests` hook). Anything booting
  postgres uses `loom_fixture_test` and `PgFixture::shared()`.
- Clippy pedantic+restriction on prod code: no
  unwrap/expect/panic/indexing/print in `src/**` production code; `map_err`
  closures use named bindings (`|e| ...`, never `|_|` — `map_err_ignore`);
  **no new `#[expect]`**; pub fns returning values get `#[must_use]` where
  applicable.
- **Never pipe `buck2 test`/`buck2 bxl` through `tail`/`head`** — redirect to
  a file and grep it. Multi-target fixture runs use `-j 8` (postgres
  boot-slot starvation). Whole-tree builds only as `buck2 build -M none
  //src/...`.
- `buck2 run //tools:prek -- run --all-files` must report zero `Failed`
  before **every** commit (rustfmt is a separate hook from clippy; markdown
  files need one trailing newline, no trailing whitespace). Conventional
  Commits; one commit per task.

## Current build-path anatomy (jobs → seams; all locations current)

`build_vector_index` (`vector_index.rs:376-554`), the 10 numbered jobs and
where each one lands:

| Job | Lines | Today | Lands in |
| --- | --- | --- | --- |
| 1. Snapshot S (MVCC anchor) | `:388-391` | `IcebergCatalog::current_snapshot` | `resolve_build_inputs` (Task 4) |
| — Declaration resolution | `:393-404` | `type_name_for` + `ontology::vector_index_def_row` → column/metric/spec | `resolve_build_inputs` (Task 4) |
| 2. Identity column | `:406-407` | `identity_column_for` | `resolve_build_inputs` (Task 4) |
| 3. Cold data | `:409-412` | `files_with_stats` → `read_files_as_batches` | `collect_vectors` (Task 4) |
| 4. Hot data | `:414-416` | `inline_live_batch` (uses shared `column_array` — String-identity clean) | `collect_vectors` (Task 4) |
| 5. Row extraction | `:418-427` | `extract_rows` per batch (Int64/Int32/Utf8 identity) | `collect_vectors` (Task 4) |
| 6. Dim inference | `:429-448` | first-row len, else parse `vector(N)` from schema, else 0 | two-arm match + pure `declared_dim` (Task 4) |
| 7. Index construction | `:450-463` | 3-arm match over `FlatIndex`/`IvfFlatIndex`/`HnswIndex` | core `IndexSpec::build` (Task 3) |
| 8. Field id | `:465-480` | `load_table` + `field_id_by_name` fallback 0 | `write_sidecar` (Task 5) |
| 9. Puffin write | `:482-499` | path mint + `puffin::write_vector_index`, BEFORE tx | `write_sidecar` (Task 5) |
| — Lineage assembly | `:501-516` | `LineageEvent` literal (canonical input ref since #295) | pure `build_lineage_event` (Task 5) |
| 10. One tx | `:518-547` | `live_table_id` + `insert_vector_index` + `pg_emit` + commit | `bind_index_and_emit` (Task 5) |

The defect lives NEXT DOOR, not in the build: `inline_delta_batch`
(`:264-364`, the search hot leg consumed by
`engine-serving/src/vector_search.rs:120`) hand-rolls `Int64Builder` /
`try_get::<i64>` / `DataType::Int64` (`:336-360`) instead of delegating to
`column_array`.

## Verified claim inventory (register/spec claim → current evidence)

| Claim | Verdict | Current evidence |
| --- | --- | --- |
| `build_vector_index` cc 41, 182 sloc, 10 numbered jobs | CONFIRMED (shape; 179 lines today) | `vector_index.rs:376-554`; numbered comments 1-10 present |
| decomposes into `resolve_build_inputs`/`collect_vectors`/`infer_dim`/`bind_index_and_emit` | CONFIRMED feasible, two adjustments | `infer_dim` → pure `declared_dim` + two-arm match (async fetch only on empty); jobs 8-9 + lineage assembly get their own seams (`write_sidecar`, `build_lineage_event`) — the spec's 4-name list is a sketch |
| `IndexSpec::build(dim, metric, rows)` moves to core | CONFIRMED | constructor signatures verified (`flat.rs:22`, `ivf.rs:36-41`, `hnsw.rs:191-197`); `IndexSpec` at `core/src/vector_index/mod.rs:24`; `pack_rows` validates dim (`codec.rs:200-204`) so `build` error-propagation is testable |
| "Fixes iss-vector-build-lineage-ref" | STALE — already fixed | #295; `vector_index.rs:506` = `DatasetRef::from(table)`; pinned `vector_index_build.rs:270-274`; ISSUES entry `[x] pr:#295` |
| `inline_delta_batch` hardcodes Int64 at `:339, :345, :358` | CONFIRMED at exactly those lines | `Int64Builder::new()` `:339`; `let id_val: i64 = r.try_get(0)` `:345`; `Field::new(&identity_col, DataType::Int64, false)` `:358` |
| `extract_rows` supports Utf8 identities too | CONFIRMED (+ Int32) | `:224-238` — Int64, Int32 (widened), Utf8 arms |
| string identity "silently breaks" hot-delta search | CONFIRMED mechanism, loud not silent | PG column is `text` (`pg_type_for`, `iceberg_type.rs:65`); sqlx strict decode fails → `Backend` → `vector_search` errors (`vector_search.rs:120-122`); silent only at define/build time |
| "string-typed identity comparison in the inline-delta merge" (task-prompt phrasing) | REFUTED | `merge_topk` (`vector_search.rs:27-36`) compares f32 distances only; no identity comparison or cold/hot dedup exists anywhere on the path |
| shared `identity_array` helper with `extract_rows` | SUPERSEDED by #301 | `column_array` (`iceberg_inline.rs:392-473`) is THE PG→Arrow decoder; `inline_live_batch` + engine-serving provider already use it; `extract_rows` reads Arrow, not PG rows |
| auto-rebuild (#240) changed this file | CONFIRMED, no conflict | git log: #240 added `declared_vector_index_names` (`:133-149`), consumed by `iceberg_flush.rs:104` — untouched by this plan |
| build path pinned by fixture tests | CONFIRMED (inventory in Architecture) | callers of `build_vector_index`: `engine/src/service.rs:191` (RPC), engine-serving tests, postgres tests; worker goes through the engine RPC (`worker/src/handler.rs:64`) — the orchestrator signature does not change, so no caller changes |

## Deliberate behavior changes (the whitelist — everything else is byte-identical)

1. **Hot-delta identity decode widens from hardcoded Int64 to the declared
   identity `BaseType`** (fixes `iss-inline-delta-string-identity`). Old: a
   String- (or Integer-) identity table with live inline rows born after the
   index's covered snapshot makes `inline_delta_batch` fail
   (`ControlPlaneError::Backend`, sqlx mismatched-types) and therefore the
   whole `vector_search` call error. New: the delta batch carries a
   `Utf8`/`Int32` identity column (decoded by the shared `column_array`,
   field type from `BaseType::arrow_data_type()`), and
   `score_inline_batch` gains the `Int32Array` arm mirroring `extract_rows`,
   so hot rows merge into results for every identity kind the cold path
   supports. The Long-identity batch is byte-identical (Task 1 pin).
   Red-first proof: `string_identity_cold_hot_merge`,
   `integer_identity_cold_hot_merge` (e2e) +
   `string_identity_delta_is_utf8`, `integer_identity_delta_is_int32`
   (seam-level). Two same-class error-path notes (both are `Backend` errors
   today and after; only the message/site changes, no test pins them): a NULL
   identity now fails at `RecordBatch::try_new` (non-nullable field) instead
   of inside `try_get`; an ontology identity column missing from the mirror
   schema now fails with a named `Backend` error before the SQL instead of a
   PG unknown-column error from it.

There is no second item. The build orchestration, lineage, Puffin bytes,
mirror rows, codec, and every existing test's observable behavior are
unchanged.

---

### Task 1: Pin the unpinned behaviors (green against the CURRENT code)

Two refactor-relevant behaviors have no direct coverage: the
`inline_delta_batch` batch contract (Long identity — the fix must keep it
byte-identical) and the String-identity **cold** path (which the defect fix
must not regress). Add pins FIRST; they must pass against the unmodified
tree — a failure here means a mis-written pin: fix the TEST, never the
production code.

**Files:**
- Create: `src/control-plane/postgres/tests/vector_index_inline_delta.rs`
- Create: `src/services/engine-serving/tests/vector_search_identity_kinds.rs`
- Modify: `src/control-plane/postgres/BUCK` (new `vector-index-inline-delta`
  fixture target)
- Modify: `src/services/engine-serving/BUCK` (new
  `vector-search-identity-kinds` fixture target)

**Interfaces:**
- Consumes: `control_plane_postgres::vector_index::{inline_delta_batch,
  build_vector_index}`, `iceberg_landing::{InlineLimits, land}` (returns
  `Result<SnapshotId>`), `PgFixture::shared() -> &'static PgFixture`,
  `PgControlPlane::new(pool, Duration)`, `engine_serving::vector_search`.
- Produces: the seed helpers and test files Task 2 appends its red tests to.

- [x] **Step 1: Create `vector_index_inline_delta.rs`** (pin only; Task 2
  appends the red tests)

```rust
//! Seam-level pins for `inline_delta_batch` — the hot-delta read the k-NN
//! search path merges over. The engine-serving merge e2es exercise it only
//! indirectly (and only with a Long identity); these tests pin the batch
//! CONTRACT at the seam (field names, arrow types, nullability, MVCC window)
//! so the road-vector-build-decomposition fix can prove the Long path
//! byte-identical. `int_identity_delta_batch_shape` is GREEN pre-fix.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_array::{Array, Float32Array, Int64Array, ListArray, RecordBatch};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    ColumnSpec, ControlPlane, DatasetId, EventType, LineageEvent, ObjectType, PropertyDef, RunId,
    TableRef, TypeName,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use control_plane_postgres::vector_index::inline_delta_batch;
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;

fn columns(id_ty: &str) -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "id".into(),
            ty: id_ty.into(),
            nullable: false,
        },
        ColumnSpec {
            name: "embedding".into(),
            ty: "vector(4)".into(),
            nullable: false,
        },
    ]
}

/// IPC body from a prebuilt identity array + embeddings (shared by the
/// per-identity-kind wrappers below and Task 2's appends).
fn ipc_from(id_field: Field, id_array: Arc<dyn Array>, embs: &[[f32; 4]]) -> Vec<u8> {
    let element = Arc::new(Field::new("item", DataType::Float32, false));
    let mut lb = ListBuilder::new(Float32Builder::new()).with_field(element.clone());
    for emb in embs {
        lb.values().append_slice(emb);
        lb.append(true);
    }
    let schema = Arc::new(Schema::new(vec![
        id_field,
        Field::new("embedding", DataType::List(element), false),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![id_array, Arc::new(lb.finish())])
        .expect("batch");
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &schema).expect("writer");
        w.write(&batch).expect("write");
        w.finish().expect("finish");
    }
    buf
}

fn ipc_long(rows: &[(i64, [f32; 4])]) -> Vec<u8> {
    let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
    let embs: Vec<[f32; 4]> = rows.iter().map(|(_, e)| *e).collect();
    ipc_from(
        Field::new("id", DataType::Int64, false),
        Arc::new(Int64Array::from(ids)),
        &embs,
    )
}

fn object_type(name: &str, table: &TableRef, id_ty: &str) -> ObjectType {
    ObjectType {
        name: TypeName(name.into()),
        table: table.clone(),
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: id_ty.into(),
                required: true,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
            PropertyDef {
                name: "embedding".into(),
                ty: "vector(4)".into(),
                required: true,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
        ],
        derived: vec![],
        identity: Some("id".into()),
    }
}

fn lineage_evt(table: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
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

/// Land `cold_ipc` as Parquet (limit 0), then `hot_ipc` INLINE (limit
/// usize::MAX). Returns (pool, s_cold, s_hot): the delta window is
/// (s_cold, s_hot].
async fn seed(
    fx: &PgFixture,
    db: &str,
    table: &TableRef,
    type_name: &str,
    id_ty_logical: &str,
    id_ty_property: &str,
    cold_ipc: Vec<u8>,
    hot_ipc: Vec<u8>,
) -> (sqlx::PgPool, i64, i64) {
    let pool = fx.pool_for(db).await;
    let cp = control_plane_postgres::PgControlPlane::new(
        pool.clone(),
        std::time::Duration::from_secs(5),
    );
    cp.ontology()
        .define_type(object_type(type_name, table, id_ty_property))
        .await
        .expect("define_type");
    let wh = tempfile::tempdir().expect("wh");
    let catalog = make_catalog(fx.pg_dsn(db), &wh.path().display().to_string()).await;
    let s_cold = land(
        &pool,
        &catalog,
        table,
        &columns(id_ty_logical),
        &cold_ipc,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage_evt(table),
    )
    .await
    .expect("land cold");
    let s_hot = land(
        &pool,
        &catalog,
        table,
        &columns(id_ty_logical),
        &hot_ipc,
        InlineLimits {
            inline_byte_limit: usize::MAX,
            flush_byte_threshold: i64::MAX,
        },
        lineage_evt(table),
    )
    .await
    .expect("land hot");
    (pool, s_cold.0, s_hot.0)
}

/// GREEN pre-fix: the Long-identity delta batch contract the fix must keep
/// byte-identical — identity field named after the ontology identity column,
/// Int64, non-nullable; vector field List<Float32> with the canonical "item"
/// child; only rows born in (born_after, at]; None outside the window.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn int_identity_delta_batch_shape() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let table = TableRef {
        schema: "wh".into(),
        name: "ldocs".into(),
    };
    let cold: &[(i64, [f32; 4])] = &[(1, [1.0, 0.0, 0.0, 0.0]), (2, [0.0, 1.0, 0.0, 0.0])];
    let hot: &[(i64, [f32; 4])] = &[(5, [0.9, 0.1, 0.0, 0.0])];
    let (pool, s_cold, s_hot) = seed(
        fx, &db, &table, "LDocs", "long", "Long", ipc_long(cold), ipc_long(hot),
    )
    .await;

    let batch = inline_delta_batch(&pool, &table, s_cold, s_hot)
        .await
        .expect("delta")
        .expect("Some: one row born after s_cold");
    let schema = batch.schema();
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert!(!schema.field(0).is_nullable());
    assert_eq!(schema.field(1).name(), "embedding");
    let DataType::List(child) = schema.field(1).data_type() else {
        panic!("vector column is a List");
    };
    assert_eq!(child.name(), "item");
    assert_eq!(child.data_type(), &DataType::Float32);
    assert!(!schema.field(1).is_nullable());

    assert_eq!(batch.num_rows(), 1);
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 ids");
    assert_eq!(ids.value(0), 5);
    let vecs = batch
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("list");
    let elems = vecs.value(0);
    let f = elems
        .as_any()
        .downcast_ref::<Float32Array>()
        .expect("f32 child");
    assert_eq!(f.values(), &[0.9, 0.1, 0.0, 0.0]);

    // MVCC window: nothing born after s_hot -> None.
    assert!(
        inline_delta_batch(&pool, &table, s_hot, s_hot)
            .await
            .expect("delta at s_hot")
            .is_none()
    );
}
```

(If `PgControlPlane::new`'s second parameter differs on this branch, mirror
the construction in `engine-serving/tests/vector_search.rs:306` verbatim.)

- [x] **Step 2: Create `vector_search_identity_kinds.rs`** (cold pin only;
  Task 2 appends the hot red tests)

```rust
//! Identity-kind coverage for the cold+hot k-NN path. The ontology permits
//! String and Integer identity columns; `extract_rows` (build) supports them
//! on the cold tier, but the hot-delta leg hardcoded Int64
//! (iss-inline-delta-string-identity). The cold pin here is GREEN pre-fix;
//! Task 2 appends the *_cold_hot_merge red tests.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_array::{Array, RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    ColumnSpec, ControlPlane, DatasetId, EventType, IndexSpec, LineageEvent, Metric, ObjectType,
    PropertyDef, RunId, TableRef, TypeName, VectorIndexDef,
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

fn columns(id_ty: &str) -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "id".into(),
            ty: id_ty.into(),
            nullable: false,
        },
        ColumnSpec {
            name: "embedding".into(),
            ty: "vector(4)".into(),
            nullable: false,
        },
    ]
}

fn ipc_from(id_field: Field, id_array: Arc<dyn Array>, embs: &[[f32; 4]]) -> Vec<u8> {
    let element = Arc::new(Field::new("item", DataType::Float32, false));
    let mut lb = ListBuilder::new(Float32Builder::new()).with_field(element.clone());
    for emb in embs {
        lb.values().append_slice(emb);
        lb.append(true);
    }
    let schema = Arc::new(Schema::new(vec![
        id_field,
        Field::new("embedding", DataType::List(element), false),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![id_array, Arc::new(lb.finish())])
        .expect("batch");
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &schema).expect("writer");
        w.write(&batch).expect("write");
        w.finish().expect("finish");
    }
    buf
}

fn ipc_str(rows: &[(&str, [f32; 4])]) -> Vec<u8> {
    let ids: Vec<&str> = rows.iter().map(|(id, _)| *id).collect();
    let embs: Vec<[f32; 4]> = rows.iter().map(|(_, e)| *e).collect();
    ipc_from(
        Field::new("id", DataType::Utf8, false),
        Arc::new(StringArray::from(ids)),
        &embs,
    )
}

fn object_type(name: &str, table: &TableRef, id_ty: &str) -> ObjectType {
    ObjectType {
        name: TypeName(name.into()),
        table: table.clone(),
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: id_ty.into(),
                required: true,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
            PropertyDef {
                name: "embedding".into(),
                ty: "vector(4)".into(),
                required: true,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
        ],
        derived: vec![],
        identity: Some("id".into()),
    }
}

fn lineage_evt(table: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
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

/// Define the type + a flat cosine index, land `cold_ipc` as Parquet, build.
/// The warehouse TempDir is returned — the cold search reads its Parquet.
async fn seed_and_build(
    fx: &PgFixture,
    db: &str,
    table: &TableRef,
    type_name: &str,
    id_ty_logical: &str,
    id_ty_property: &str,
    cold_ipc: Vec<u8>,
) -> (SqlCatalog, sqlx::PgPool, tempfile::TempDir) {
    let pool = fx.pool_for(db).await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = make_catalog(fx.pg_dsn(db), &wh.path().display().to_string()).await;
    let cp = PgControlPlane::new(pool.clone(), Duration::from_secs(5));
    cp.ontology()
        .define_type(object_type(type_name, table, id_ty_property))
        .await
        .expect("define_type");
    cp.ontology()
        .define_vector_index(VectorIndexDef {
            name: "by_flat".into(),
            type_name: TypeName(type_name.into()),
            property: "embedding".into(),
            metric: Metric::Cosine,
            spec: IndexSpec::Flat,
        })
        .await
        .expect("define_vector_index");
    land(
        &pool,
        &catalog,
        table,
        &columns(id_ty_logical),
        &cold_ipc,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage_evt(table),
    )
    .await
    .expect("land cold");
    build_vector_index(&catalog, &pool, table, "by_flat", RunId(uuid::Uuid::new_v4()))
        .await
        .expect("build_vector_index");
    (catalog, pool, wh)
}

/// Identity column of the result batch as strings (Utf8 output).
fn ids_str(batch: &RecordBatch) -> Vec<String> {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("identity column is Utf8")
        .iter()
        .map(|v| v.expect("non-null id").to_string())
        .collect()
}

const COLD_STR: &[(&str, [f32; 4])] = &[
    ("a", [1.0, 0.0, 0.0, 0.0]),
    ("b", [0.0, 1.0, 0.0, 0.0]),
    ("c", [0.0, 0.0, 1.0, 0.0]),
    ("d", [0.0, 0.0, 0.0, 1.0]),
];

/// GREEN pre-fix: the COLD path already supports String identities end to end
/// (extract_rows Utf8 arm -> VectorKey::Str -> Utf8 result column). The defect
/// is hot-only; this pin proves the fix does not regress the cold tier.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn string_identity_cold_search() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let table = TableRef {
        schema: "wh".into(),
        name: "sdocs".into(),
    };
    let (catalog, pool, _wh) = seed_and_build(
        fx, &db, &table, "SDocs", "string", "String", ipc_str(COLD_STR),
    )
    .await;

    let batch = engine_serving::vector_search(
        &catalog,
        &pool,
        &table,
        "by_flat",
        &[1.0_f32, 0.0, 0.0, 0.0],
        1,
        None,
        None,
    )
    .await
    .expect("cold search over string identity");
    assert_eq!(ids_str(&batch), vec!["a".to_string()], "nearest is 'a'");
}
```

- [x] **Step 3: Wire both BUCK targets**

Append to `src/control-plane/postgres/BUCK` (after `vector-index-multi`,
mirroring its shape):

```python
loom_fixture_test(
    name = "vector-index-inline-delta",
    crate = "vector_index_inline_delta",
    srcs = ["tests/vector_index_inline_delta.rs"],
    crate_root = "tests/vector_index_inline_delta.rs",
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

Append to `src/services/engine-serving/BUCK` (after
`vector-index-auto-rebuild`, mirroring `vector-search`'s deps minus `arrow`,
which this file does not use):

```python
loom_fixture_test(
    name = "vector-search-identity-kinds",
    crate = "vector_search_identity_kinds",
    srcs = ["tests/vector_search_identity_kinds.rs"],
    crate_root = "tests/vector_search_identity_kinds.rs",
    deps = [
        ":engine-serving",
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
```

- [x] **Step 4: Run both pins — expect PASS against unmodified production code**

```bash
buck2 test //src/control-plane/postgres:vector-index-inline-delta \
  //src/services/engine-serving:vector-search-identity-kinds -j 8 \
  > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t1.log
```

Expected: `Tests finished: Pass 2. Fail 0. ...`. If either pin is red, the
pin misstates current behavior — fix the test, never the production code.

- [x] **Step 5: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files > /tmp/prek1.log 2>&1; grep -E "Failed" /tmp/prek1.log || echo CLEAN
git add src/control-plane/postgres/tests/vector_index_inline_delta.rs \
  src/control-plane/postgres/BUCK \
  src/services/engine-serving/tests/vector_search_identity_kinds.rs \
  src/services/engine-serving/BUCK
git commit -m "test(vector): pin inline-delta batch contract + string-identity cold path"
```

---

### Task 2: Fix `iss-inline-delta-string-identity` (red first — the whitelisted change)

**Files:**
- Test (append): `src/control-plane/postgres/tests/vector_index_inline_delta.rs`
- Test (append): `src/services/engine-serving/tests/vector_search_identity_kinds.rs`
- Modify: `src/control-plane/postgres/src/vector_index.rs` (`inline_delta_batch`,
  `:264-364`)
- Modify: `src/services/engine-serving/src/vector_search.rs`
  (`score_inline_batch` `:163-172` + the `Int32Array` import)

**Interfaces:**
- Consumes: `crate::iceberg_inline::column_array(rows, i, ty) ->
  Result<ArrayRef>` (pub, #301), `control_plane_core::resolve_logical(&str) ->
  Option<BaseType>`, `BaseType::arrow_data_type()`.
- Produces: `inline_delta_batch` (signature UNCHANGED: `(pool, table,
  born_after, at) -> Result<Option<RecordBatch>>`) now emits the identity
  column typed per the declared identity `BaseType`.

- [x] **Step 1: Append the seam-level red tests to `vector_index_inline_delta.rs`**

```rust
fn ipc_str(rows: &[(&str, [f32; 4])]) -> Vec<u8> {
    let ids: Vec<&str> = rows.iter().map(|(id, _)| *id).collect();
    let embs: Vec<[f32; 4]> = rows.iter().map(|(_, e)| *e).collect();
    ipc_from(
        Field::new("id", DataType::Utf8, false),
        Arc::new(arrow_array::StringArray::from(ids)),
        &embs,
    )
}

fn ipc_int(rows: &[(i32, [f32; 4])]) -> Vec<u8> {
    let ids: Vec<i32> = rows.iter().map(|(id, _)| *id).collect();
    let embs: Vec<[f32; 4]> = rows.iter().map(|(_, e)| *e).collect();
    ipc_from(
        Field::new("id", DataType::Int32, false),
        Arc::new(arrow_array::Int32Array::from(ids)),
        &embs,
    )
}

/// RED pre-fix: a String identity makes `inline_delta_batch` fail with a sqlx
/// mismatched-types Backend error (`try_get::<i64>` on a PG `text` column).
/// Desired: a Utf8 identity column, mirroring the cold path's Utf8 support
/// (iss-inline-delta-string-identity).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn string_identity_delta_is_utf8() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let table = TableRef {
        schema: "wh".into(),
        name: "sdocs".into(),
    };
    let cold: &[(&str, [f32; 4])] = &[("a", [1.0, 0.0, 0.0, 0.0])];
    let hot: &[(&str, [f32; 4])] = &[("hot", [0.9, 0.1, 0.0, 0.0])];
    let (pool, s_cold, s_hot) = seed(
        fx, &db, &table, "SDocs", "string", "String", ipc_str(cold), ipc_str(hot),
    )
    .await;

    let batch = inline_delta_batch(&pool, &table, s_cold, s_hot)
        .await
        .expect("delta over string identity")
        .expect("Some: one row born after s_cold");
    assert_eq!(batch.schema().field(0).data_type(), &DataType::Utf8);
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .expect("Utf8 ids");
    assert_eq!(ids.value(0), "hot");
}

/// RED pre-fix: an Integer identity fails the same way (PG `integer` never
/// decodes as i64 under sqlx's strict typing). Desired: an Int32 identity
/// column, mirroring extract_rows' Int32 arm.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integer_identity_delta_is_int32() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let table = TableRef {
        schema: "wh".into(),
        name: "idocs".into(),
    };
    let cold: &[(i32, [f32; 4])] = &[(1, [1.0, 0.0, 0.0, 0.0])];
    let hot: &[(i32, [f32; 4])] = &[(5, [0.9, 0.1, 0.0, 0.0])];
    let (pool, s_cold, s_hot) = seed(
        fx, &db, &table, "IDocs", "integer", "Integer", ipc_int(cold), ipc_int(hot),
    )
    .await;

    let batch = inline_delta_batch(&pool, &table, s_cold, s_hot)
        .await
        .expect("delta over integer identity")
        .expect("Some: one row born after s_cold");
    assert_eq!(batch.schema().field(0).data_type(), &DataType::Int32);
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int32Array>()
        .expect("Int32 ids");
    assert_eq!(ids.value(0), 5);
}
```

- [x] **Step 2: Append the e2e red tests to `vector_search_identity_kinds.rs`**

First extend the Task 1 file's arrow import (the integer tests need the two
extra array types — do NOT add them earlier; they would be unused imports and
fail the clippy hook at Task 1's commit):

```rust
use arrow_array::{Array, Int32Array, Int64Array, RecordBatch, StringArray};
```

Then append:

```rust
fn ipc_int(rows: &[(i32, [f32; 4])]) -> Vec<u8> {
    let ids: Vec<i32> = rows.iter().map(|(id, _)| *id).collect();
    let embs: Vec<[f32; 4]> = rows.iter().map(|(_, e)| *e).collect();
    ipc_from(
        Field::new("id", DataType::Int32, false),
        Arc::new(Int32Array::from(ids)),
        &embs,
    )
}

/// Identity column of the result batch as i64 (Int64 output — Integer
/// identities widen through VectorKey::Int).
fn ids_i64(batch: &RecordBatch) -> Vec<i64> {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("identity column is Int64")
        .iter()
        .map(|v| v.expect("non-null id"))
        .collect()
}

/// RED pre-fix: with a String identity, an inline row born after the covered
/// snapshot makes the WHOLE search error (the hot leg's Int64 hardcode).
/// Desired: the hot row merges in first.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn string_identity_cold_hot_merge() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let table = TableRef {
        schema: "wh".into(),
        name: "sdocs".into(),
    };
    let (catalog, pool, _wh) = seed_and_build(
        fx, &db, &table, "SDocs", "string", "String", ipc_str(COLD_STR),
    )
    .await;

    let hot: &[(&str, [f32; 4])] = &[("hot", [0.95, 0.05, 0.0, 0.0])];
    land(
        &pool,
        &catalog,
        &table,
        &columns("string"),
        &ipc_str(hot),
        InlineLimits {
            inline_byte_limit: usize::MAX,
            flush_byte_threshold: i64::MAX,
        },
        lineage_evt(&table),
    )
    .await
    .expect("land inline hot row");

    let batch = engine_serving::vector_search(
        &catalog,
        &pool,
        &table,
        "by_flat",
        &[0.9_f32, 0.1, 0.0, 0.0],
        2,
        None,
        None,
    )
    .await
    .expect("cold+hot search over string identity");
    assert_eq!(
        ids_str(&batch),
        vec!["hot".to_string(), "a".to_string()],
        "hot inline row merges in first, cold 'a' second"
    );
}

const COLD_INT: &[(i32, [f32; 4])] = &[
    (1, [1.0, 0.0, 0.0, 0.0]),
    (2, [0.0, 1.0, 0.0, 0.0]),
    (3, [0.0, 0.0, 1.0, 0.0]),
    (4, [0.0, 0.0, 0.0, 1.0]),
];

/// RED pre-fix: same defect class for Integer identities (int4 never decodes
/// as i64). Desired: hot row merges; result ids widen to Int64 exactly as the
/// cold tier's VectorKey::Int does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integer_identity_cold_hot_merge() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let table = TableRef {
        schema: "wh".into(),
        name: "idocs".into(),
    };
    let (catalog, pool, _wh) = seed_and_build(
        fx, &db, &table, "IDocs", "integer", "Integer", ipc_int(COLD_INT),
    )
    .await;

    let hot: &[(i32, [f32; 4])] = &[(5, [0.95, 0.05, 0.0, 0.0])];
    land(
        &pool,
        &catalog,
        &table,
        &columns("integer"),
        &ipc_int(hot),
        InlineLimits {
            inline_byte_limit: usize::MAX,
            flush_byte_threshold: i64::MAX,
        },
        lineage_evt(&table),
    )
    .await
    .expect("land inline hot row");

    let batch = engine_serving::vector_search(
        &catalog,
        &pool,
        &table,
        "by_flat",
        &[0.9_f32, 0.1, 0.0, 0.0],
        2,
        None,
        None,
    )
    .await
    .expect("cold+hot search over integer identity");
    assert_eq!(ids_i64(&batch), vec![5, 1], "hot row first, cold row 1 second");
}
```

- [x] **Step 3: Run and observe RED**

```bash
buck2 test //src/control-plane/postgres:vector-index-inline-delta \
  //src/services/engine-serving:vector-search-identity-kinds -j 8 \
  > /tmp/t2red.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t2red.log
```

Expected: `Fail 2` per target (the four new tests), each panicking inside an
`expect(...)` with a `Backend`/`Engine` error containing sqlx's
mismatched-types text; `int_identity_delta_batch_shape` and
`string_identity_cold_search` stay green. If a "red" test unexpectedly
passes, STOP — the defect claim is wrong; re-verify before touching
production code.

- [x] **Step 4: Rewrite `inline_delta_batch`'s decode over `column_array`**

Replace `inline_delta_batch` (`vector_index.rs:264-364`) with (signature,
SQL text, and the three early-`None` returns unchanged; the doc comment
gains one sentence):

```rust
/// Read the live inline rows for `table` that were born AFTER `born_after` and
/// are still alive at snapshot `at`, returning only the `identity_col` and
/// `vector_col` columns. Returns `None` if the inline table does not exist or
/// has no matching rows.
///
/// Used by Task 7/8 (hot-delta path) to fetch the rows appended between S and Q
/// so the serving layer can score them alongside the cold Puffin index.
///
/// The identity column is decoded per its DECLARED logical type (Long, Integer,
/// or String — the same kinds the cold path's `extract_rows` accepts), through
/// the shared `column_array` PG→Arrow bridge, so the delta batch can never
/// drift from `inline_live_batch` (iss-inline-delta-string-identity).
pub async fn inline_delta_batch(
    pool: &PgPool,
    table: &TableRef,
    born_after: i64,
    at: i64,
) -> Result<Option<RecordBatch>> {
    use crate::iceberg_inline::{column_array, inline_table_name};
    use crate::iceberg_mirror::live_table_id;
    use control_plane_core::resolve_logical;

    let mut conn = pool.acquire().await.map_err(backend)?;
    let Some(tid) = live_table_id(&mut conn, &table.schema, &table.name).await? else {
        return Ok(None);
    };

    // Check the inline table exists.
    let exists: Option<String> = sqlx::query_scalar(AssertSqlSafe(format!(
        "select to_regclass('{}')::text",
        inline_table_name(tid)
    )))
    .fetch_one(&mut *conn)
    .await
    .map_err(backend)?;
    if exists.is_none() {
        return Ok(None);
    }

    // Resolve identity + vector column names AND logical types from the
    // ontology/mirror. We look for a column whose type starts with "vector("
    // in the current mirror snapshot (at SnapshotId(at)); the identity's
    // BaseType drives the decode below.
    let identity_col = identity_column_for(pool, table).await?;
    let ice = crate::iceberg_catalog::IcebergCatalog::new(pool.clone());
    let schema = ice.schema(table, SnapshotId(at)).await?;
    let vec_def = schema
        .columns
        .iter()
        .find(|c| c.ty.starts_with("vector("))
        .ok_or_else(|| {
            ControlPlaneError::Backend(
                format!(
                    "no vector column in schema for {}.{}",
                    table.schema, table.name
                )
                .into(),
            )
        })?;
    let vector_col = vec_def.name.clone();
    let vec_ty = resolve_logical(&vec_def.ty).ok_or_else(|| {
        ControlPlaneError::Backend(
            format!(
                "unresolvable vector type {:?} for {}.{}",
                vec_def.ty, table.schema, table.name
            )
            .into(),
        )
    })?;
    let id_ty = schema
        .columns
        .iter()
        .find(|c| c.name == identity_col)
        .and_then(|c| resolve_logical(&c.ty))
        .ok_or_else(|| {
            ControlPlaneError::Backend(
                format!(
                    "identity column '{identity_col}' missing or unresolvable in schema for {}.{}",
                    table.schema, table.name
                )
                .into(),
            )
        })?;

    // Runtime query: select only the identity + vector columns with the delta MVCC predicate.
    let id_quoted = format!("\"{}\"", identity_col.replace('"', "\"\""));
    let vec_quoted = format!("\"{}\"", vector_col.replace('"', "\"\""));
    let rows = sqlx::query(AssertSqlSafe(format!(
        "select {id_quoted}, {vec_quoted} \
         from {} \
         where begin_snapshot > {born_after} \
           and begin_snapshot <= {at} \
           and (end_snapshot is null or end_snapshot > {at}) \
         order by loom_row_id",
        inline_table_name(tid),
    )))
    .fetch_all(&mut *conn)
    .await
    .map_err(backend)?;

    if rows.is_empty() {
        return Ok(None);
    }

    // Decode through THE shared PG-row → Arrow bridge (`column_array`): the
    // identity per its declared BaseType, the vector as List<Float32> with the
    // canonical "item" child. Field data types come from the same BaseType map.
    use arrow_schema::{Field, Schema};

    let id_array = column_array(&rows, 0, id_ty)?;
    let vec_array = column_array(&rows, 1, vec_ty)?;
    let out_schema = Arc::new(Schema::new(vec![
        Field::new(&identity_col, id_ty.arrow_data_type(), false),
        Field::new(&vector_col, vec_ty.arrow_data_type(), false),
    ]));
    let batch = RecordBatch::try_new(out_schema, vec![id_array, vec_array])
        .map_err(|e| ControlPlaneError::Backend(e.to_string().into()))?;
    Ok(Some(batch))
}
```

Notes: `Catalog` is already in the file's top-level imports (`:8-11`), so
`ice.schema` resolves; the old fn-local
`use arrow_array::builder::{Float32Builder, Int64Builder, ListBuilder};` and
the `control_plane_core::vector_list_field()` call are deleted (the SQL-STYLE
comment block at `:34-43` still describes the runtime query accurately). The
existing `// SQL-STYLE` rationale and MVCC predicate are byte-identical.

- [x] **Step 5: Add the `Int32Array` arm to `score_inline_batch`**

In `src/services/engine-serving/src/vector_search.rs`, extend the import at
`:11`:

```rust
use arrow::array::{Float32Array, Int32Array, Int64Array, ListArray, RecordBatch, StringArray};
```

and insert between the `Int64Array` and `StringArray` arms (`:163-165`):

```rust
        let key = if let Some(i64arr) = id_col.as_any().downcast_ref::<Int64Array>() {
            VectorKey::Int(i64arr.value(row))
        } else if let Some(i32arr) = id_col.as_any().downcast_ref::<Int32Array>() {
            // Mirrors extract_rows (the build path): Integer identities widen to
            // VectorKey::Int so hot scoring agrees with the cold index keys.
            VectorKey::Int(i64::from(i32arr.value(row)))
        } else if let Some(sarr) = id_col.as_any().downcast_ref::<StringArray>() {
```

- [x] **Step 6: Run — reds green, pins still green, adjacent suites unmodified-green**

```bash
buck2 test //src/control-plane/postgres:vector-index-inline-delta \
  //src/services/engine-serving:vector-search-identity-kinds \
  //src/services/engine-serving:vector-search \
  //src/services/engine-serving:inline-vector-sql \
  //src/services/engine-serving:vector-index-auto-rebuild \
  //src/control-plane/postgres:vector-index-build -j 8 \
  > /tmp/t2green.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t2green.log
```

Expected: `Fail 0`. The `vector-search` suite (Long-identity hot merges ×3
index kinds) passing unmodified is the byte-identity proof for the Long path,
alongside Task 1's shape pin.

- [x] **Step 7: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files > /tmp/prek2.log 2>&1; grep -E "Failed" /tmp/prek2.log || echo CLEAN
git add src/control-plane/postgres/src/vector_index.rs \
  src/control-plane/postgres/tests/vector_index_inline_delta.rs \
  src/services/engine-serving/src/vector_search.rs \
  src/services/engine-serving/tests/vector_search_identity_kinds.rs
git commit -m "fix(query): hot-delta identity decode via shared column_array (Long/Integer/String)"
```

---

### Task 3: `IndexSpec::build` in core (job 7 moves out of the adapter)

**Files:**
- Create: `src/control-plane/core/tests/index_spec_build.rs`
- Modify: `src/control-plane/core/src/vector_index/mod.rs` (append to
  `impl IndexSpec`, after `from_label`)
- Modify: `src/control-plane/core/BUCK` (new `index-spec-build` target)
- Modify: `src/control-plane/postgres/src/vector_index.rs`
  (`build_vector_index` job 7 `:453-461` + imports `:9-10`)

**Interfaces:**
- Consumes: `FlatIndex::build`, `IvfFlatIndex::build`, `HnswIndex::build`
  (existing, unchanged), `crate::error::Result`.
- Produces: `IndexSpec::build(&self, dim: u32, metric: Metric, rows:
  Vec<(VectorKey, Vec<f32>)>) -> Result<Box<dyn VectorIndex>>` — Tasks 4-5's
  orchestrator calls `inputs.spec.build(dim, inputs.metric, all_rows)?`.

- [x] **Step 1: Write the failing test `core/tests/index_spec_build.rs`**

```rust
//! `IndexSpec::build` — the single authoritative spec→index constructor
//! routing (moved from the postgres adapter's build_vector_index, job 7).
//! Byte-identity with the direct constructors is asserted so the routing can
//! never drift from the codec goldens (tests/vector_index_codec.rs) — the
//! constructors are deterministic (fixed-seed SplitMix64).

use control_plane_core::{
    FlatIndex, HnswIndex, IndexKind, IndexSpec, IvfFlatIndex, Metric, VectorKey,
};

fn rows(n: i64) -> Vec<(VectorKey, Vec<f32>)> {
    (0..n)
        .map(|i| (VectorKey::Int(i), vec![i as f32, 1.0, 2.0]))
        .collect()
}

#[test]
fn flat_routes_and_bytes_match_direct_constructor() {
    let built = IndexSpec::Flat
        .build(3, Metric::Cosine, rows(8))
        .expect("spec build");
    assert_eq!(built.index_kind(), IndexKind::Flat);
    assert_eq!(built.dim(), 3);
    assert_eq!(built.row_count(), 8);
    let direct = FlatIndex::build(3, Metric::Cosine, rows(8)).expect("direct build");
    assert_eq!(
        built.serialize(),
        direct.serialize(),
        "routing adds nothing to the bytes"
    );
}

#[test]
fn ivf_routes_with_and_without_nlist() {
    let built = IndexSpec::IvfFlat { nlist: Some(2) }
        .build(3, Metric::L2, rows(16))
        .expect("spec build");
    assert_eq!(built.index_kind(), IndexKind::IvfFlat);
    let direct = IvfFlatIndex::build(3, Metric::L2, rows(16), Some(2)).expect("direct build");
    assert_eq!(built.serialize(), direct.serialize());

    let defaulted = IndexSpec::IvfFlat { nlist: None }
        .build(3, Metric::L2, rows(16))
        .expect("default nlist");
    assert_eq!(defaulted.index_kind(), IndexKind::IvfFlat);
    let direct_default = IvfFlatIndex::build(3, Metric::L2, rows(16), None).expect("direct");
    assert_eq!(defaulted.serialize(), direct_default.serialize());
}

#[test]
fn hnsw_routes_with_params() {
    let built = IndexSpec::Hnsw {
        m: Some(4),
        ef_construction: Some(32),
    }
    .build(3, Metric::Cosine, rows(16))
    .expect("spec build");
    assert_eq!(built.index_kind(), IndexKind::Hnsw);
    let direct =
        HnswIndex::build(3, Metric::Cosine, rows(16), Some(4), Some(32)).expect("direct build");
    assert_eq!(
        built.serialize(),
        direct.serialize(),
        "same params, same deterministic bytes"
    );
}

#[test]
fn constructor_errors_propagate() {
    // rows are 3-wide, declared dim 4: pack_rows' dim-mismatch error surfaces.
    assert!(IndexSpec::Flat.build(4, Metric::Cosine, rows(4)).is_err());
}
```

- [x] **Step 2: Wire the BUCK target and observe RED (compile failure)**

Append to `src/control-plane/core/BUCK` after `vector-index-codec`:

```python
rust_test(
    name = "index-spec-build",
    crate = "index_spec_build",
    srcs = ["tests/index_spec_build.rs"],
    crate_root = "tests/index_spec_build.rs",
    edition = "2024",
    visibility = ["PUBLIC"],
    deps = [":core"],
)
```

```bash
buck2 test //src/control-plane/core:index-spec-build > /tmp/t3red.log 2>&1; \
  grep -E "no method named|Tests finished|FAIL" /tmp/t3red.log
```

Expected: build failure — `no method named `build` found for enum IndexSpec`.

- [x] **Step 3: Implement `IndexSpec::build`** (append inside `impl IndexSpec`
  in `core/src/vector_index/mod.rs`, after `from_label`; the match bodies are
  the adapter's job 7 verbatim)

```rust
    /// Build the index this spec describes over `rows` — THE single
    /// authoritative spec→constructor routing (previously an inline match in
    /// the postgres adapter's `build_vector_index`). Codec-neutral: each
    /// constructor serializes through the unchanged codec, so built bytes stay
    /// pinned by the codec goldens.
    pub fn build(
        &self,
        dim: u32,
        metric: Metric,
        rows: Vec<(VectorKey, Vec<f32>)>,
    ) -> Result<Box<dyn VectorIndex>> {
        Ok(match self {
            IndexSpec::Flat => Box::new(FlatIndex::build(dim, metric, rows)?),
            IndexSpec::IvfFlat { nlist } => {
                Box::new(IvfFlatIndex::build(dim, metric, rows, *nlist)?)
            }
            IndexSpec::Hnsw { m, ef_construction } => {
                Box::new(HnswIndex::build(dim, metric, rows, *m, *ef_construction)?)
            }
        })
    }
```

- [x] **Step 4: Run — new target green AND the goldens byte-identical**

```bash
buck2 test //src/control-plane/core:index-spec-build \
  //src/control-plane/core:vector-index-codec \
  //src/control-plane/core:vector-index > /tmp/t3a.log 2>&1; \
  grep -E "Tests finished|FAIL" /tmp/t3a.log
```

Expected: `Fail 0`. A golden failure means the codec was touched — revert;
never update a golden.

- [x] **Step 5: Switch the adapter's job 7 to the core routing**

In `build_vector_index` (`vector_index.rs:450-461`), replace:

```rust
    // 7. Build the chosen index (Flat exact, or IVF approximate). The `VectorIndex`
    //    trait is `Send`, so the box may be held across `.await` points without
    //    extracting fields early.
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

with:

```rust
    // 7. Build the chosen index via the core spec routing (`IndexSpec::build` —
    //    Flat exact, IVF/HNSW approximate). The `VectorIndex` trait is `Send`,
    //    so the box may be held across `.await` points without extracting
    //    fields early.
    let index: Box<dyn control_plane_core::VectorIndex> =
        index_spec.build(dim, metric, all_rows)?;
```

and trim the now-unused imports at `:9-10`: remove `FlatIndex`, `HnswIndex`,
`IvfFlatIndex` from the `control_plane_core::{...}` list (`IndexSpec` stays —
it is the routing's receiver type via `def.spec`).

- [x] **Step 6: Run the build-path fixture suites**

```bash
buck2 test //src/control-plane/postgres:vector-index-build \
  //src/control-plane/postgres:vector-index-ivf \
  //src/control-plane/postgres:vector-index-hnsw \
  //src/control-plane/postgres:vector-index-multi -j 8 \
  > /tmp/t3b.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t3b.log
```

Expected: `Fail 0` — the three kind-specific suites prove the routing arms
(flat/ivf/hnsw mirror `index_kind`, decodable blobs) byte-identically.

- [x] **Step 7: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files > /tmp/prek3.log 2>&1; grep -E "Failed" /tmp/prek3.log || echo CLEAN
git add src/control-plane/core/src/vector_index/mod.rs \
  src/control-plane/core/tests/index_spec_build.rs \
  src/control-plane/core/BUCK \
  src/control-plane/postgres/src/vector_index.rs
git commit -m "refactor(core): IndexSpec::build owns the spec->index construction routing"
```

---

### Task 4: Extract `resolve_build_inputs` / `collect_vectors` / `declared_dim`

**Files:**
- Create: `src/control-plane/postgres/tests/vector_index_unit.rs`
- Modify: `src/control-plane/postgres/src/vector_index.rs`
- Modify: `src/control-plane/postgres/BUCK` (new pure `vector-index-unit`
  target)

**Interfaces:**
- Consumes: existing `type_name_for`, `identity_column_for`,
  `ontology::vector_index_def_row`, `IcebergCatalog`, `read_files_as_batches`,
  `extract_rows`, `inline_live_batch`.
- Produces (used by Task 5's orchestrator):
  - `struct BuildInputs { at: SnapshotId, column: String, metric: Metric,
    spec: IndexSpec, identity_col: String }` (private)
  - `async fn resolve_build_inputs(ice: &IcebergCatalog, pool: &PgPool,
    table: &TableRef, index_name: &str) -> Result<BuildInputs>` (private)
  - `async fn collect_vectors(catalog: &SqlCatalog, ice: &IcebergCatalog,
    table: &TableRef, at: SnapshotId, column: &str, identity_col: &str) ->
    Result<Vec<(VectorKey, Vec<f32>)>>` (private)
  - `pub fn declared_dim(schema: &TableSchema, column: &str) -> u32`

- [x] **Step 1: Write the failing unit test `tests/vector_index_unit.rs`**

```rust
//! Pure-seam unit tests for the decomposed build path: `declared_dim` (the
//! empty-table dim fallback — previously buried in build_vector_index's job 6
//! with no cheap fixture reaching it) and, from Task 5, `build_lineage_event`
//! (the canonical-DatasetRef event assembly).

use control_plane_core::{ColumnDef, TableSchema};
use control_plane_postgres::vector_index::declared_dim;

fn schema(cols: &[(&str, &str)]) -> TableSchema {
    TableSchema {
        columns: cols
            .iter()
            .enumerate()
            .map(|(i, (name, ty))| ColumnDef {
                order: i as i64,
                name: (*name).to_string(),
                ty: (*ty).to_string(),
                nullable: false,
            })
            .collect(),
    }
}

#[test]
fn declared_dim_parses_vector_n() {
    let s = schema(&[("id", "long"), ("embedding", "vector(4)")]);
    assert_eq!(declared_dim(&s, "embedding"), 4);
}

#[test]
fn declared_dim_zero_when_column_missing() {
    let s = schema(&[("id", "long")]);
    assert_eq!(declared_dim(&s, "embedding"), 0);
}

#[test]
fn declared_dim_zero_when_not_a_vector() {
    assert_eq!(declared_dim(&schema(&[("embedding", "long")]), "embedding"), 0);
    assert_eq!(
        declared_dim(&schema(&[("embedding", "vector()")]), "embedding"),
        0,
        "malformed vector type falls back to 0, matching the legacy unwrap_or(0)"
    );
}
```

- [x] **Step 2: Wire the BUCK target (pure `rust_test`, mirroring
  `iceberg-type`) and observe RED (unresolved import)**

Append to `src/control-plane/postgres/BUCK` near the other pure tests:

```python
rust_test(
    name = "vector-index-unit",
    crate = "vector_index_unit",
    srcs = ["tests/vector_index_unit.rs"],
    crate_root = "tests/vector_index_unit.rs",
    edition = "2024",
    deps = [
        ":postgres",
        "//src/control-plane/core:core",
        "//third-party:time",
        "//third-party:uuid",
    ],
)
```

(`time`/`uuid` are for Task 5's `build_lineage_event` appends.)

```bash
buck2 test //src/control-plane/postgres:vector-index-unit > /tmp/t4red.log 2>&1; \
  grep -E "unresolved import|cannot find|Tests finished|FAIL" /tmp/t4red.log
```

Expected: compile failure — `declared_dim` does not exist yet.

- [x] **Step 3: Add the three seams to `vector_index.rs`** (above
  `build_vector_index`; the bodies are today's jobs 1-6 moved verbatim, with
  `use control_plane_core::{Metric, TableSchema}` added to the top-level
  import list)

```rust
/// The pre-read inputs of a vector-index build: the MVCC anchor snapshot S
/// (captured BEFORE any data read so cold and hot reads are consistent as of
/// S), the named declaration's column/metric/spec (authoritative), and the
/// ontology-declared identity column.
struct BuildInputs {
    at: SnapshotId,
    column: String,
    metric: Metric,
    spec: IndexSpec,
    identity_col: String,
}

/// Jobs 1-2 of the build: snapshot anchor, declaration resolution, identity
/// column. Resolution order (snapshot -> declaration -> identity) is
/// load-bearing for error precedence and preserved from the inline code.
async fn resolve_build_inputs(
    ice: &crate::iceberg_catalog::IcebergCatalog,
    pool: &PgPool,
    table: &TableRef,
    index_name: &str,
) -> Result<BuildInputs> {
    let at = ice.current_snapshot(table).await?.id;
    let type_name = type_name_for(pool, table).await?;
    let def = crate::ontology::vector_index_def_row(pool, &type_name, index_name)
        .await?
        .ok_or_else(|| {
            ControlPlaneError::NotFound(format!(
                "no vector index definition `{index_name}` on type `{type_name}`"
            ))
        })?;
    let identity_col = identity_column_for(pool, table).await?;
    Ok(BuildInputs {
        at,
        column: def.property,
        metric: def.metric,
        spec: def.spec,
        identity_col,
    })
}

/// Jobs 3-5: read the cold Parquet files and hot inline rows live at `at`,
/// and extract `(VectorKey, vector)` rows from both tiers.
async fn collect_vectors(
    catalog: &crate::iceberg_sql_catalog::SqlCatalog,
    ice: &crate::iceberg_catalog::IcebergCatalog,
    table: &TableRef,
    at: SnapshotId,
    column: &str,
    identity_col: &str,
) -> Result<Vec<(VectorKey, Vec<f32>)>> {
    let files = ice.files_with_stats(table, at).await?;
    let paths: Vec<String> = files.iter().map(|f| f.path.clone()).collect();
    let (_, cold_batches) = crate::read_files_as_batches(catalog, table, &paths).await?;
    let hot_batch: Option<RecordBatch> = ice
        .inline_live_batch(table, at)
        .await?
        .map(|(_, _, b)| b);

    let mut all_rows: Vec<(VectorKey, Vec<f32>)> = Vec::new();
    for batch in cold_batches.iter().chain(hot_batch.iter()) {
        all_rows.extend(extract_rows(batch, column, identity_col)?);
    }
    Ok(all_rows)
}

/// The declared `vector(N)` dimension of `column` in `schema`, or 0 when the
/// column is missing or its type is not a well-formed `vector(N)` — the
/// build's fallback when the table has no rows to infer from (job 6's
/// legacy `unwrap_or(0)`).
#[must_use]
pub fn declared_dim(schema: &TableSchema, column: &str) -> u32 {
    schema
        .columns
        .iter()
        .find(|c| c.name == column)
        .and_then(|c| {
            // ty is e.g. "vector(4)"
            c.ty.strip_prefix("vector(")
                .and_then(|s| s.strip_suffix(')'))
                .and_then(|s| s.parse::<u32>().ok())
        })
        .unwrap_or(0)
}
```

- [x] **Step 4: Rewire `build_vector_index` jobs 1-6 onto the seams**

Replace `:388-448` (jobs 1-6; everything from `// 1. Snapshot S` through the
dim inference) with:

```rust
    // 1-2. Snapshot anchor S + declaration + identity (see resolve_build_inputs).
    let ice = IcebergCatalog::new(pool.clone());
    let inputs = resolve_build_inputs(&ice, pool, table, index_name).await?;
    let at = inputs.at;
    let s: i64 = at.0;
    let column: &str = &inputs.column;
    let metric = inputs.metric;
    let index_spec = inputs.spec.clone();
    let identity_col = inputs.identity_col.clone();

    // 3-5. Cold Parquet + hot inline rows, extracted to (VectorKey, vector).
    let all_rows = collect_vectors(catalog, &ice, table, at, column, &identity_col).await?;
    let row_count = all_rows.len() as i64;

    // 6. Infer dim from the first row; an empty table falls back to the
    //    declared vector(N) (schema fetched only on this arm, as before).
    let dim: u32 = match all_rows.first() {
        Some((_, v)) => v.len() as u32,
        None => declared_dim(&ice.schema(table, at).await?, column),
    };
```

Everything from `// 7.` down is untouched in this task (the local names
`s`, `column`, `metric`, `index_spec`, `identity_col`, `all_rows`,
`row_count`, `dim` are preserved exactly, so jobs 7-10 compile unmodified).
Delete the now-dead `use crate::iceberg_mirror::live_table_id;` ONLY if the
compiler flags it — job 10 still uses it in this task.

- [x] **Step 5: Run — unit test green, fixture suites green**

```bash
buck2 test //src/control-plane/postgres:vector-index-unit > /tmp/t4a.log 2>&1; \
  grep -E "Tests finished|FAIL" /tmp/t4a.log
buck2 test //src/control-plane/postgres:vector-index-build \
  //src/control-plane/postgres:vector-index-ivf \
  //src/control-plane/postgres:vector-index-hnsw \
  //src/control-plane/postgres:vector-index-multi \
  //src/control-plane/postgres:flush-vector-rebuild -j 8 \
  > /tmp/t4b.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t4b.log
```

Expected: `Fail 0` in both.

- [x] **Step 6: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files > /tmp/prek4.log 2>&1; grep -E "Failed" /tmp/prek4.log || echo CLEAN
git add src/control-plane/postgres/src/vector_index.rs \
  src/control-plane/postgres/tests/vector_index_unit.rs \
  src/control-plane/postgres/BUCK
git commit -m "refactor(vector): extract resolve_build_inputs/collect_vectors/declared_dim seams"
```

---

### Task 5: Extract `write_sidecar` / `build_lineage_event` / `bind_index_and_emit`; slim orchestrator

**Files:**
- Test (append): `src/control-plane/postgres/tests/vector_index_unit.rs`
- Modify: `src/control-plane/postgres/src/vector_index.rs`

**Interfaces:**
- Consumes: Task 4's seams; existing `puffin::write_vector_index`,
  `lineage::pg_emit`, `insert_vector_index`, `live_table_id`.
- Produces:
  - `async fn write_sidecar(catalog: &SqlCatalog, table: &TableRef, index:
    &dyn control_plane_core::VectorIndex, covered_snapshot: i64, column: &str,
    identity_col: &str) -> Result<String>` (private; returns the puffin path)
  - `pub fn build_lineage_event(run_id: RunId, table: &TableRef, column: &str,
    covered_snapshot: i64, row_count: i64, puffin_path: &str) -> LineageEvent`
  - `struct IndexBinding { index_name: String, column: String,
    covered_snapshot: i64, metric: String, index_kind: String, dim: i32,
    row_count: i64, puffin_path: String }` (private)
  - `async fn bind_index_and_emit(pool: &PgPool, table: &TableRef, binding:
    IndexBinding, lineage: &LineageEvent) -> Result<()>` (private)

- [x] **Step 1: Append the failing `build_lineage_event` unit test**

Append to `tests/vector_index_unit.rs` (imports extend to
`control_plane_core::{ColumnDef, DatasetRef, EventType, RunId, TableRef,
TableSchema}` and `control_plane_postgres::vector_index::{build_lineage_event,
declared_dim}`):

```rust
#[test]
fn lineage_event_uses_canonical_dataset_ref() {
    let table = TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    };
    let run = RunId(uuid::Uuid::new_v4());
    let evt = build_lineage_event(run, &table, "embedding", 7, 42, "s3://x/y.puffin");
    assert_eq!(evt.run_id, run);
    assert!(matches!(evt.event_type, EventType::Complete));
    // Canonical loom dataset ref — the SAME lineage node the landing/flush
    // emitters use (iss-vector-build-lineage-ref, fixed in #295); this unit pin
    // keeps the fix structural through the decomposition.
    assert_eq!(evt.inputs, vec![DatasetRef::from(&table)]);
    assert_eq!(evt.outputs.len(), 1);
    assert_eq!(evt.outputs[0].namespace, "loom-vector-index");
    assert_eq!(evt.outputs[0].name, "s3://x/y.puffin");
    assert_eq!(evt.payload["column"], "embedding");
    assert_eq!(evt.payload["covered_snapshot"], 7_i64);
    assert_eq!(evt.payload["row_count"], 42_i64);
}
```

Run: `buck2 test //src/control-plane/postgres:vector-index-unit >
/tmp/t5red.log 2>&1; grep -E "cannot find|Tests finished|FAIL" /tmp/t5red.log`
— expected: compile failure (`build_lineage_event` missing).

- [x] **Step 2: Add the three seams** (below `declared_dim`; bodies are jobs
  8-10 + the lineage literal moved verbatim, comments included)

```rust
/// Jobs 8-9: resolve the vector column's Iceberg field id (informational),
/// mint a fresh sidecar path under the table's metadata location, and write
/// the Puffin file. The object-store write happens BEFORE the Postgres tx —
/// a failed build leaves an orphan sidecar, never a dangling mirror row.
/// `write_vector_index` is the single source of the 7-key property map.
async fn write_sidecar(
    catalog: &crate::iceberg_sql_catalog::SqlCatalog,
    table: &TableRef,
    index: &dyn control_plane_core::VectorIndex,
    covered_snapshot: i64,
    column: &str,
    identity_col: &str,
) -> Result<String> {
    let ident = TableIdent::from_strs([table.schema.as_str(), table.name.as_str()])
        .map_err(|e| ControlPlaneError::Backend(e.to_string().into()))?;
    let tbl = catalog
        .load_table(&ident)
        .await
        .map_err(|e| ControlPlaneError::Backend(e.to_string().into()))?;
    // Use the Schema::field_id_by_name accessor (available on the iceberg-rust
    // pinned main commit). Falls back to 0 if the accessor returns None (e.g.
    // if the Iceberg schema uses a different field name than expected — purely
    // informational for Puffin footer decode in slice 1).
    let field_id: i32 = tbl
        .metadata()
        .current_schema()
        .field_id_by_name(column)
        .unwrap_or(0);

    let puffin_path = format!(
        "{}/metadata/loom-vector-index-{}.puffin",
        tbl.metadata().location(),
        uuid::Uuid::new_v4()
    );
    let file_io = tbl.file_io().clone();
    crate::puffin::write_vector_index(
        &file_io,
        &puffin_path,
        index,
        covered_snapshot,
        field_id,
        column,
        identity_col,
    )
    .await?;
    Ok(puffin_path)
}

/// The build's completion lineage event: input = the CANONICAL loom dataset
/// ref for the source table (`DatasetRef::from(table)` — the same node the
/// landing/flush emitters use; iss-vector-build-lineage-ref, #295), output =
/// the Puffin sidecar under the "loom-vector-index" namespace.
#[must_use]
pub fn build_lineage_event(
    run_id: RunId,
    table: &TableRef,
    column: &str,
    covered_snapshot: i64,
    row_count: i64,
    puffin_path: &str,
) -> LineageEvent {
    LineageEvent {
        run_id,
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![DatasetRef::from(table)],
        outputs: vec![DatasetRef {
            namespace: "loom-vector-index".to_string(),
            name: puffin_path.to_string(),
        }],
        payload: serde_json::json!({
            "column": column,
            "covered_snapshot": covered_snapshot,
            "row_count": row_count,
        }),
    }
}

/// The mirror-row fields of a completed build: `VectorIndexRow` before the
/// `table_id` is known — it is resolved INSIDE `bind_index_and_emit`'s
/// transaction (moving it earlier would change failure semantics under a
/// concurrent table drop/re-create).
struct IndexBinding {
    index_name: String,
    column: String,
    covered_snapshot: i64,
    metric: String,
    index_kind: String,
    dim: i32,
    row_count: i64,
    puffin_path: String,
}

/// Job 10: ONE Postgres tx — resolve the live mirror table id, upsert the
/// `vector_index` binding row, emit the lineage event, commit.
async fn bind_index_and_emit(
    pool: &PgPool,
    table: &TableRef,
    binding: IndexBinding,
    lineage: &LineageEvent,
) -> Result<()> {
    use crate::iceberg_mirror::live_table_id;
    use crate::lineage::pg_emit;

    let mut tx = pool.begin().await.map_err(backend)?;
    let conn: &mut PgConnection = &mut tx;

    let table_id = live_table_id(conn, &table.schema, &table.name)
        .await?
        .ok_or_else(|| {
            ControlPlaneError::NotFound(format!(
                "no live mirror table for {}.{}",
                table.schema, table.name
            ))
        })?;

    insert_vector_index(
        conn,
        &VectorIndexRow {
            table_id,
            column: binding.column,
            index_name: binding.index_name,
            covered_snapshot: binding.covered_snapshot,
            metric: binding.metric,
            index_kind: binding.index_kind,
            dim: binding.dim,
            row_count: binding.row_count,
            puffin_path: binding.puffin_path,
        },
    )
    .await?;

    pg_emit(conn, lineage).await?;
    tx.commit().await.map_err(backend)
}
```

- [x] **Step 3: Slim the orchestrator** — `build_vector_index` becomes (doc
  comment `:366-375` unchanged; signature unchanged):

```rust
pub async fn build_vector_index(
    catalog: &crate::iceberg_sql_catalog::SqlCatalog,
    pool: &PgPool,
    table: &TableRef,
    index_name: &str,
    run_id: RunId,
) -> Result<BuiltIndex> {
    use crate::iceberg_catalog::IcebergCatalog;

    // 1-2. Snapshot anchor S + declaration + identity.
    let ice = IcebergCatalog::new(pool.clone());
    let inputs = resolve_build_inputs(&ice, pool, table, index_name).await?;
    let at = inputs.at;
    let s: i64 = at.0;

    // 3-5. Cold Parquet + hot inline rows, extracted to (VectorKey, vector).
    let all_rows =
        collect_vectors(catalog, &ice, table, at, &inputs.column, &inputs.identity_col).await?;
    let row_count = all_rows.len() as i64;

    // 6. Infer dim from the first row; an empty table falls back to the
    //    declared vector(N) (schema fetched only on this arm, as before).
    let dim: u32 = match all_rows.first() {
        Some((_, v)) => v.len() as u32,
        None => declared_dim(&ice.schema(table, at).await?, &inputs.column),
    };

    // 7. Build the chosen index via the core spec routing. The `VectorIndex`
    //    trait is `Send`, so the box may be held across `.await` points.
    let index: Box<dyn control_plane_core::VectorIndex> =
        inputs.spec.build(dim, inputs.metric, all_rows)?;
    // dim may have been inferred as 0 for empty tables; prefer index's own dim.
    let dim = if index.dim() > 0 { index.dim() } else { dim };

    // 8-9. Puffin sidecar (object-store write BEFORE the Postgres tx).
    let puffin_path =
        write_sidecar(catalog, table, index.as_ref(), s, &inputs.column, &inputs.identity_col)
            .await?;

    // 10. One Postgres tx: binding row + lineage event.
    let lineage = build_lineage_event(run_id, table, &inputs.column, s, row_count, &puffin_path);
    bind_index_and_emit(
        pool,
        table,
        IndexBinding {
            index_name: index_name.to_string(),
            column: inputs.column.clone(),
            covered_snapshot: s,
            metric: inputs.metric.as_str().to_string(),
            index_kind: index.index_kind().as_str().to_string(),
            dim: dim as i32,
            row_count,
            puffin_path: puffin_path.clone(),
        },
        &lineage,
    )
    .await?;

    Ok(BuiltIndex {
        covered_snapshot: s,
        puffin_path,
        row_count,
    })
}
```

Housekeeping: with jobs 8-10 moved out, drop the orchestrator's now-unused
fn-local `use` lines and any top-level imports the compiler flags as unused
(`TableIdent`/`IceCatalog` move into `write_sidecar`'s scope via the existing
top-level `use iceberg::{Catalog as IceCatalog, TableIdent};` — keep that
line, `write_sidecar` uses both; `OffsetDateTime` stays for
`build_lineage_event`). Task 4's temporary local aliases (`column`, `metric`,
`index_spec`, `identity_col`) disappear — `inputs.*` is read directly.

- [x] **Step 4: Run — unit tests green, ALL vector fixture suites green**

```bash
buck2 test //src/control-plane/postgres:vector-index-unit > /tmp/t5a.log 2>&1; \
  grep -E "Tests finished|FAIL" /tmp/t5a.log
buck2 test //src/control-plane/postgres:vector-index-build \
  //src/control-plane/postgres:vector-index-ivf \
  //src/control-plane/postgres:vector-index-hnsw \
  //src/control-plane/postgres:vector-index-multi \
  //src/control-plane/postgres:vector-index-mirror \
  //src/control-plane/postgres:vector-index-named \
  //src/control-plane/postgres:vector-index-inline-delta \
  //src/control-plane/postgres:flush-vector-rebuild \
  //src/control-plane/postgres:puffin-roundtrip -j 8 \
  > /tmp/t5b.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t5b.log
```

Expected: `Fail 0` in both. The `vector-index-build` suite's lineage
assertions (exactly one event, canonical input ref, puffin-path output) prove
`build_lineage_event` + `bind_index_and_emit` emit identically.

- [x] **Step 5: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files > /tmp/prek5.log 2>&1; grep -E "Failed" /tmp/prek5.log || echo CLEAN
git add src/control-plane/postgres/src/vector_index.rs \
  src/control-plane/postgres/tests/vector_index_unit.rs
git commit -m "refactor(vector): extract write_sidecar/build_lineage_event/bind_index_and_emit"
```

---

### Task 6: Affected-package sweep + register closes

**Files:**
- Modify: `docs/ROADMAP.md` (`#road-vector-build-decomposition` — locate by
  id)
- Modify: `docs/ISSUES.md` (`#iss-inline-delta-string-identity` — locate by
  id)

- [ ] **Step 1: Whole-tree build + affected-package test sweep**

Core changed, so everything downstream rebuilds; test the consuming packages
(engine/worker/query-api call `build_vector_index`/`vector_search` through
unchanged signatures but must still be swept):

```bash
buck2 build -M none //src/... > /tmp/build6.log 2>&1; tail -3 /tmp/build6.log
buck2 test //src/control-plane/core: //src/control-plane/postgres: \
  //src/services/engine-serving: //src/services/engine: \
  //src/services/worker: //src/services/query-api: -j 8 \
  > /tmp/sweep6.log 2>&1; grep -E "Tests finished|FAIL" /tmp/sweep6.log
```

Expected: build success; `Fail 0`. (Locally the `-j 8` cap avoids postgres
boot-slot starvation; in a cloud session do NOT widen this to a bare
`buck2 test //src/...`.)

- [ ] **Step 2: Close the ROADMAP item** — replace the
  `road-vector-build-decomposition` entry (checkbox, status, prose) with:

```markdown
- [x] **Vector-index build decomposition** `{#road-vector-build-decomposition area:quality status:done from:2026-07-02-pillar-idioms-audit-design pr:- spec:2026-07-02-pillar-idioms-audit-design}`
  Done. `build_vector_index` (cc 41, 179 lines, 10 numbered jobs) decomposed onto named seams: `resolve_build_inputs` (snapshot anchor + declaration + identity), `collect_vectors` (cold Parquet + hot inline extraction), `declared_dim` (the spec's `infer_dim` — pure empty-table dim fallback, first-ever coverage via unit tests), `write_sidecar` (field-id + Puffin write, still BEFORE the tx), `build_lineage_event` (pure; the canonical `DatasetRef::from(table)` input now pinned at the unit level), and `bind_index_and_emit` (the one tx, `live_table_id` still resolved inside it). The `Box<dyn VectorIndex>` construction match moved to core as `IndexSpec::build(dim, metric, rows)` with byte-identity tests against the direct constructors — codec untouched, goldens byte-identical. Fixes [[iss-inline-delta-string-identity]] as the ONE whitelisted behavior change (see that entry); the [[iss-vector-build-lineage-ref]] half was pre-closed by #295. The spec's `identity_array` sketch was superseded by #301's `column_array`, which the fix delegates to instead. Everything else byte-identical; every pre-existing vector fixture suite passed unmodified.
```

- [ ] **Step 3: Close the ISSUES defect** — replace the
  `iss-inline-delta-string-identity` entry with:

```markdown
- [x] **Vector hot-delta path hardcodes Int64 identity** `{#iss-inline-delta-string-identity area:query status:fixed from:2026-07-02-pillar-idioms-audit-design pr:- spec:2026-07-02-pillar-idioms-audit-design}`
  `postgres/src/vector_index.rs::inline_delta_batch` read the identity column as `Int64` only (`Int64Builder` + `try_get::<i64>` + a hardcoded `DataType::Int64` field) while `extract_rows` (the cold path) supports `Utf8` and `Int32` identities — a String (or Integer) identity made the hot-delta leg of vector search over unflushed inline rows fail with a sqlx mismatched-types decode error (loud at query time, silent at define/build time: the cold tier fully supports such identities, so the fault surfaced only once post-build inline rows existed). Fixed by [[road-vector-build-decomposition]]: the delta batch decodes identity and vector through the shared `column_array` PG→Arrow bridge keyed by the mirror schema's declared `BaseType`, and engine-serving's `score_inline_batch` gained the `Int32` arm mirroring `extract_rows`. Pinned by string/integer cold+hot merge e2es (`engine-serving/tests/vector_search_identity_kinds.rs`) and seam-level delta-batch contract tests (`postgres/tests/vector_index_inline_delta.rs`, incl. a Long-identity byte-shape pin added green-first).
```

- [ ] **Step 4: Validate the registers**

```bash
bash tools/docs.sh validate
```

Expected: no errors. If the PR number is already known when this task runs
(`gh pr view --json number` on the pushed branch), replace both `pr:-` with
`pr:#<N>`; otherwise leave the placeholders for the finishing flow.

- [ ] **Step 5: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files > /tmp/prek6.log 2>&1; grep -E "Failed" /tmp/prek6.log || echo CLEAN
git add docs/ROADMAP.md docs/ISSUES.md
git commit -m "docs(registers): close road-vector-build-decomposition + iss-inline-delta-string-identity"
```

---

## Self-review notes (performed at plan time)

- **Spec coverage:** all four named seams land (`infer_dim` as
  `declared_dim` + match, deviation recorded); `IndexSpec::build` lands with
  the spec's exact `(dim, metric, rows)` signature; both defects the register
  names are accounted for (one fixed here, one verified pre-fixed by #295);
  codec untouched.
- **Type consistency:** `BuildInputs.at: SnapshotId` / `s: i64 = at.0`;
  `IndexBinding` field names match `VectorIndexRow` minus `table_id`;
  `column_array(&rows, i, BaseType)` matches `iceberg_inline.rs:392`;
  `ipc_from(Field, Arc<dyn Array>, &[[f32; 4]])` is used identically in both
  new test files; `ids_str`/`ids_i64` return types match their assertions.
- **Red-test honesty:** the reds depend on sqlx's strict decode (`text`/`int4`
  never decode as `i64`). Step 3 of Task 2 instructs STOP-and-reverify if a
  red unexpectedly passes.
- **No placeholders:** every step carries complete code or an exact command
  with expected output. Task-1 files import only what their green pins use;
  the integer-identity helpers (`ipc_int`, `ids_i64`, the `Int32Array`/
  `Int64Array` imports) arrive with Task 2's appends so no commit carries
  unused imports past the clippy hook.
