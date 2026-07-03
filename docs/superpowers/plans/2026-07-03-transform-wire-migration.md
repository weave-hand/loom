# Transform Wire Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run both transform job kinds (`transform`, `typed-transform`) on the zero-pool worker over the engine wire (Flight inputs, `CommitTransform` commit RPC), then delete the pool-owning `src/services/transform/` crate.

**Architecture:** Follows the spec's four-phase sequencing: (1) wire plumbing — job structs + `OutputMode` + `conform` into core, `register_batches` into datafusion-io, `CommitTransform` proto/client/engine impl, `columns_json` on `ListFilesResponse`; (2) physical `transform` handler on the worker + e2e; (3) `typed-transform` + e2e; (4) delete the transform crate. Payload JSON and job-kind strings are unchanged, so both binaries can drain the same kinds during cutover — every phase lands green.

**Tech Stack:** Rust, tonic/prost (protox codegen, no protoc), DataFusion, buck2.

**Spec:** `docs/superpowers/specs/2026-07-03-transform-wire-migration-design.md`

## Global Constraints

- Payload JSON shapes and the kind strings `"transform"` / `"typed-transform"` are wire-frozen — byte-compatible with today's `TransformPayload`/`TypedTransformPayload`.
- Lineage payloads byte-identical to today's: physical `{"sql": ...}`; typed `{"sql", "input_tables", "output_table"}`.
- Retry taxonomy preserved: parse/unknown-type/unknown-input/ambiguous/DataFusion/infer/conformance/NoSnapshot ⇒ Abandon; wire + object-store-write ⇒ Retry with `WorkerTuning::backoff(attempts)`. gRPC `NotFound` from resolution RPCs is deterministic ⇒ Abandon (survives the wire via `cp_status` — `engine-wire/src/client.rs:27`). **Three deliberate deviations from today's classes (state them in the PR body):** (1) registration errors (`register_empty_table`/`register_batches`, formerly `Scan` ⇒ Retry) become Abandon — on the wire path they are local, deterministic MemTable/schema errors, not I/O; (2) the mid-handler drop race (`run_unknown_input.rs`'s class: NotFound *after* a successful existence check) now surfaces as a Retry that converges to Abandon on the clean re-list, because engine `list_files` propagates a mid-read NotFound as a `Backend` wire error; (3) backoff cap 64s → 60s (`WorkerTuning` ceiling vs today's `2^6`).
- The worker **binary/library** dep closure stays Postgres-free (no `sqlx`, no `//src/control-plane/postgres`); fixture-test targets may depend on postgres (compact-e2e already does).
- Builds `buck2 build -M none`; test runs carry `--unstable-allow-all-tests-on-re` (root host); never pipe buck2 test/bxl output — redirect to a file.
- prek clean before every commit: `buck2 run -v0 //tools:prek -- run --all-files > /tmp/p.log 2>&1; grep -c Failed /tmp/p.log` must print 0.
- Commit trailers: `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>` + `Claude-Session: https://claude.ai/code/session_01KrF8cSY7q1odAy9AW6aAnd`.

## Key pre-verified signatures (read from code)

- `IcebergControlPlane::new(pg: PgControlPlane, catalog: SqlCatalog)` — `postgres/src/iceberg_control_plane.rs:44`; internally wraps catalog in `Arc`. Engine holds `cp: PgControlPlane` (Clone) + `catalog: Arc<SqlCatalog>` (`engine/src/service.rs:46-55`) — Task 3 widens the ctor to `impl Into<Arc<SqlCatalog>>`.
- `LineageWire` — `engine-wire/src/convert.rs:69`, `From<&LineageEvent>` (:117) + `TryFrom<LineageWire> for LineageEvent` (:132). Engine decodes it in `write_object`/`overwrite_table` (`service.rs:249`).
- `GrpcQueueClient::gov_resolve(&TypeName) -> Result<TableRef>` (client.rs:372), `gov_get_type(&TypeName) -> Result<ObjectType>` (:364) — both map status via `cp_status`, so `ControlPlaneError::NotFound` survives.
- `GrpcQueueClient::list_files(schema, name) -> Result<Vec<FileRef>>` (client.rs:150) — return type changes in Task 3. **Three existing call/shape sites must be updated:** `worker/src/compact.rs:29` (caller), `engine/tests/compact_wire.rs:163-170,192-197` (callers using the result as a `Vec` — append `.files`), and `engine-wire/tests/compact_rpc.rs:38-44` (constructs the `pb::ListFilesResponse` struct literal — gains `columns_json: None`).
- `Catalog::schema(&TableRef, SnapshotId) -> Result<TableSchema>` (core/src/catalog.rs:83); `TableSchema { columns: Vec<ColumnDef> }`; ColumnDef has `name`/`ty`/`nullable` (mapped to `ColumnSpec` at `transform/src/run.rs:141-149`).
- Engine `list_files` (service.rs:156-184): `current_snapshot` NotFound ⇒ `Ok` empty vec today — Task 3 keeps that shape and adds `columns_json` presence as the table-exists discriminator.
- `write_dataset(Arc<dyn ObjectStore>, dir_prefix, Arc<Schema>, &[RecordBatch], &WriteConfig) -> Result<Vec<WrittenFile>, WriteError>` (datafusion-io/src/write.rs:123); `absolute_data_files(Vec<WrittenFile>, root_url, schema, table) -> Vec<DataFile>` (:224); `infer_columns(&Schema)` (infer.rs:66); `logical_arrow_schema(&[ColumnSpec]) -> Result<SchemaRef, InferError>` (infer.rs:52); `register_empty_table(&SessionContext, &str, SchemaRef)` (scan.rs:113, uses `MemTable::try_new(schema, vec![vec![]])` + `TableReference::bare`).
- Worker e2e harness: `loom_test_flight::{EngineOpts, spawn_engine_uds}` (src/testing/flight.rs:84 — set `control: true, flight: true`), `loom_test_seed::local_sql_catalog` (seed.rs:174), `iceberg_landing::land` seeding + `Job`/`JobId` hand-construction — mirror `worker/tests/compact_e2e.rs`.
- Ontology seeding for typed e2e: `pg.ontology().define_type(ObjectType{...})` — mirror `transform/tests/typed_transform_e2e.rs:182-215`.
- Empty-table fixture: port `create_empty_table` from `transform/tests/transform_e2e.rs:188-197` **verbatim** — it is `begin_table` + `create_table` + **`append_files(table, &[])`** + `commit`; the empty `append_files` is load-bearing (a create-only commit leaves no live mirror row, so `current_snapshot` would return NotFound and the table would read as unknown).
- Lineage payload read-back (for e2e asserts): today's e2es only check `Lineage::upstream` dataset names, never payloads. To pin the byte-identical-payload constraint, query Postgres directly in the test: `lineage.event` has `run_id, event_type, event_time, payload`; `lineage.event_dataset` has `event_id, direction, ordinal, namespace, name` (`postgres/src/lineage.rs:18-20`). E.g. `sqlx::query_scalar::<_, serde_json::Value>("select e.payload from lineage.event e join lineage.event_dataset d on d.event_id = e.event_id and d.direction = 'output' where d.name = $1")` — verify the exact `direction` literal against `postgres/src/lineage.rs` when writing the test. Requires `//third-party:sqlx` in the worker e2e test targets (NOT in `compact-e2e`'s dep list, which they otherwise mirror; the worker `e2e` target has it).
- `TableRef` derives Serialize/Deserialize as `{schema, name}` (core/src/catalog.rs:19) — byte-compatible with today's private `TableSpec` wire form (`transform/src/handler.rs:19-24`).

---

### Task 1: Core moves — `OutputMode`, job structs, `conform` (transform re-exports, stays green)

**Files:**
- Create: `src/control-plane/core/src/transform_job.rs`
- Create: `src/control-plane/core/src/conform.rs` (moved verbatim from `src/services/transform/src/conform.rs`, imports adjusted to `crate::`)
- Modify: `src/control-plane/core/src/lib.rs` (module decls + re-exports next to `compact_job`)
- Modify: `src/services/transform/src/run.rs:21-30` (delete `OutputMode` def, `pub use control_plane_core::OutputMode;`)
- Modify: `src/services/transform/src/conform.rs` (body replaced by `pub use control_plane_core::{Violation, check_conformance};` — keep the module so `crate::conform::` paths keep compiling)
- Create: `src/control-plane/core/tests/transform_job.rs` + `src/control-plane/core/tests/conform.rs` (ported test content)
- Modify: `src/control-plane/core/BUCK` (two new `rust_test` targets), `src/services/transform/BUCK` (delete `output-mode` + `conform` test targets)
- Delete: `src/services/transform/tests/output_mode.rs`, `src/services/transform/tests/conform.rs`

**Interfaces (produced):**

```rust
// core/src/transform_job.rs
use crate::TableRef;

pub const TRANSFORM_JOB_KIND: &str = "transform";
pub const TYPED_TRANSFORM_JOB_KIND: &str = "typed-transform";

/// How a transform's result lands in the output table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OutputMode {
    /// Add the result's files to the table (today's behavior).
    #[default]
    Append,
    /// Replace the table's live contents with the result (older snapshots time-travel).
    Overwrite,
}

/// Payload of a `"transform"` job: physical SQL over table-named inputs.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct TransformJob {
    pub inputs: Vec<TableRef>,
    pub output: TableRef,
    pub sql: String,
    #[serde(default)]
    pub output_mode: OutputMode,
}

/// Payload of a `"typed-transform"` job: SQL over ontology-type-named inputs.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct TypedTransformJob {
    pub inputs: Vec<String>,
    pub output: String,
    pub sql: String,
    #[serde(default)]
    pub output_mode: OutputMode,
}
```

`OutputMode` moves verbatim except it **gains `serde::Serialize`** (jobs are now also constructed, not just parsed). Wire shape unchanged (`"append"`/`"overwrite"`, absent ⇒ Append).

- [x] **Step 1 (red):** wire the two `rust_test` BUCK targets (`:transform-job`, `:conform` — mirror `:page`) AND write `core/tests/transform_job.rs` — port the four `output_mode.rs` cases against `control_plane_core::OutputMode`, plus payload-compat pins:

```rust
// today's exact wire JSON parses into the core structs
let j: TransformJob = serde_json::from_value(serde_json::json!({
    "inputs": [{"schema": "main", "name": "a"}],
    "output": {"schema": "main", "name": "out"},
    "sql": "select 1",
})).expect("parse");
assert_eq!(j.output_mode, OutputMode::Append);
// and serializes back byte-compatibly (output_mode always present when serialized is fine;
// pin the field VALUES, not absence)
let v = serde_json::to_value(&j).expect("ser");
assert_eq!(v["output"]["name"], "out");
assert_eq!(v["output_mode"], "append");
let tj: TypedTransformJob = serde_json::from_value(serde_json::json!({
    "inputs": ["Customer"], "output": "Enriched", "sql": "select 1", "output_mode": "overwrite",
})).expect("parse typed");
assert_eq!(tj.output_mode, OutputMode::Overwrite);
assert_eq!(TRANSFORM_JOB_KIND, "transform");
assert_eq!(TYPED_TRANSFORM_JOB_KIND, "typed-transform");
```

`core/tests/conform.rs`: move `transform/tests/conform.rs` content, imports → `control_plane_core::{check_conformance, Violation, ColumnSpec, PropertyDef}`.

- [x] **Step 2:** run the new targets, verify FAIL red (compile error — module absent): `buck2 test //src/control-plane/core:transform-job //src/control-plane/core:conform --unstable-allow-all-tests-on-re > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL|Error" /tmp/t1.log`
- [x] **Step 3:** create `core/src/transform_job.rs` + `core/src/conform.rs` (conform body verbatim from transform, `use crate::{ColumnSpec, PropertyDef, UnknownLogicalType, satisfies};`); wire `lib.rs`: `mod conform; mod transform_job; pub use conform::{Violation, check_conformance}; pub use transform_job::{OutputMode, TransformJob, TypedTransformJob, TRANSFORM_JOB_KIND, TYPED_TRANSFORM_JOB_KIND};`.
- [x] **Step 4:** re-point transform: `run.rs` deletes its `OutputMode` and re-exports core's; `transform/src/conform.rs` becomes the re-export shim; delete the two transform test files + BUCK targets.
- [x] **Step 5:** `buck2 build -M none //src/... > /tmp/b1.log 2>&1; grep -c "BUILD FAILED" /tmp/b1.log` (0) and re-run Step 2's tests (PASS) plus transform's remaining unit targets: `buck2 test //src/control-plane/core:transform-job //src/control-plane/core:conform //src/services/transform:run-unknown-input --unstable-allow-all-tests-on-re > /tmp/t1b.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t1b.log`
- [x] **Step 6:** prek; commit `refactor(core): move OutputMode, transform job payloads, and conformance into core`

### Task 2: `datafusion_io::register_batches`

**Files:**
- Modify: `src/services/datafusion-io/src/scan.rs` (new fn; `register_empty_table` delegates), `src/services/datafusion-io/src/lib.rs` (export)
- Modify: `src/services/datafusion-io/tests/scan.rs` (new cases)

**Interfaces (produced):**

```rust
/// Register in-memory `batches` as table `name` (a `MemTable`). All batches must
/// share `schema`. Sibling of `register_empty_table`, which is the zero-batch case.
pub fn register_batches(
    ctx: &SessionContext,
    name: &str,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> Result<(), ScanError>
```

- [x] **Step 1 (red):** in `tests/scan.rs` add `register_batches_serves_rows_for_sql` (build a two-batch `id: Int64` table, `SELECT count(*)`/`sum(id)` over it, assert values) and `register_batches_empty_matches_register_empty_table` (empty vec ⇒ `count(*) == 0`). Run: `buck2 test //src/services/datafusion-io:scan --unstable-allow-all-tests-on-re > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t2.log` — FAIL (fn missing).
- [x] **Step 2:** implement in `scan.rs`:

```rust
pub fn register_batches(
    ctx: &SessionContext,
    name: &str,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> Result<(), ScanError> {
    let provider = MemTable::try_new(schema, vec![batches])?;
    ctx.register_table(TableReference::bare(name), Arc::new(provider))?;
    Ok(())
}
```

and rewrite `register_empty_table`'s body as `register_batches(ctx, name, schema, Vec::new())` (keeping its doc). Export from `lib.rs` next to `register_empty_table`.

- [x] **Step 3:** rerun Step 1 (PASS, incl. the existing `register_empty_table` case).
- [x] **Step 4:** prek; commit `feat(datafusion-io): register_batches — MemTable registration for wire-fetched inputs`

### Task 3: `CommitTransform` RPC + `ListFilesResponse.columns_json` (proto, engine, client)

**Files:**
- Modify: `src/services/engine-wire/proto/engine_control.proto`
- Modify: `src/services/engine-wire/src/client.rs` (`TableFiles` struct, `list_files` return change, `commit_transform` method)
- Modify: `src/services/engine/src/service.rs` (`commit_transform` handler, `list_files` columns; the `use` list gains `control_plane_core::{TableControlPlane, TableTx, Tx}` as needed for `begin_table`/staging/`emit`/`commit` method resolution), `src/control-plane/postgres/src/iceberg_control_plane.rs:44` (ctor widening)
- Modify: `src/services/worker/src/compact.rs:29-37` (`list_files` caller — append `.files`)
- Modify: `src/services/engine/tests/compact_wire.rs:163-170,192-197` (`list_files` callers — append `.files`)
- Modify: `src/services/engine-wire/tests/compact_rpc.rs:38-44` (`pb::ListFilesResponse` struct literal — add `columns_json: None`)
- Create: `src/services/engine/tests/transform_wire.rs` + `loom_fixture_test` target in engine BUCK. Dep list = the `write-wire` target's deps PLUS `//src/testing:flight`, `//src/testing:seed`, `//src/services/datafusion-io:datafusion-io`, `//src/services/store-config:store-config` (case 1 writes real parquet via `write_dataset` + `absolute_data_files` and boots the engine via `spawn_engine_uds`, which no existing engine test target does — enumerate, don't assume).

**Interfaces (produced):**

Proto (mirror `CompactTable` conventions at proto lines 62-63):

```proto
rpc CommitTransform (CommitTransformRequest) returns (CommitTransformResponse);

message ListFilesResponse {
  repeated FileMeta files = 1;
  // Declared schema (serde_json Vec<ColumnSpec>) at the current snapshot.
  // ABSENT <=> the table does not exist — the table-exists discriminator that
  // lets a zero-file table register as an empty relation.
  optional string columns_json = 2;
}

message CommitTransformRequest {
  string schema = 1;              // output TableRef
  string name = 2;
  string columns_json = 3;        // serde_json Vec<ColumnSpec> — inferred output schema
  repeated string write_json = 4; // one serde_json DataFile per written file
  string lineage_json = 5;        // serde_json LineageWire
  bool replace = 6;               // false = append_files; true = replace_files
}
message CommitTransformResponse { optional int64 snapshot_id = 1; }
```

Client (`engine-wire/src/client.rs`):

```rust
/// A table's live file set plus its declared schema. `columns` is `None` iff the
/// table does not exist (the wire's `columns_json` was absent).
#[derive(Debug, Clone, PartialEq)]
pub struct TableFiles {
    pub files: Vec<control_plane_core::FileRef>,
    pub columns: Option<Vec<control_plane_core::ColumnSpec>>,
}

pub async fn list_files(&self, schema: String, name: String) -> Result<TableFiles>
// columns_json decode failure => be(...)

pub async fn commit_transform(
    &self,
    schema: String,
    name: String,
    columns: &[control_plane_core::ColumnSpec],
    write: &[control_plane_core::DataFile],
    lineage: &control_plane_core::LineageEvent,
    replace: bool,
) -> Result<Option<i64>>
// serializes columns (serde_json), each DataFile (mirror compact_table), and
// LineageWire::from(lineage); maps transport/status via be (mirror compact_table)
```

Engine (`service.rs`), after widening the ctor to `pub fn new(pg: PgControlPlane, catalog: impl Into<Arc<SqlCatalog>>) -> Self` (field becomes `catalog: catalog.into()`; existing owned-`SqlCatalog` callers compile unchanged via `impl Into`):

```rust
async fn commit_transform(
    &self,
    req: Request<pb::CommitTransformRequest>,
) -> std::result::Result<Response<pb::CommitTransformResponse>, Status> {
    let r = req.into_inner();
    let table = TableRef { schema: r.schema, name: r.name };
    let columns: Vec<control_plane_core::ColumnSpec> = serde_json::from_str(&r.columns_json)
        .map_err(|e| Status::invalid_argument(format!("bad columns_json: {e}")))?;
    let write: Vec<control_plane_core::DataFile> = r.write_json.iter()
        .map(|s| serde_json::from_str(s)
            .map_err(|e| Status::invalid_argument(format!("bad write DataFile json: {e}"))))
        .collect::<std::result::Result<_, _>>()?;
    let wire: engine_wire::convert::LineageWire = serde_json::from_str(&r.lineage_json)
        .map_err(|e| Status::invalid_argument(format!("bad lineage_json: {e}")))?;
    let lineage = control_plane_core::LineageEvent::try_from(wire)
        .map_err(|e| Status::invalid_argument(format!("bad lineage: {e}")))?;
    let icp = control_plane_postgres::iceberg_control_plane::IcebergControlPlane::new(
        self.cp.clone(), self.catalog.clone(),
    );
    let mut tx = icp.begin_table().await.map_err(status)?;
    tx.create_table(&table, &columns).await.map_err(status)?;
    if r.replace {
        tx.replace_files(&table, &write).await.map_err(status)?;
    } else {
        tx.append_files(&table, &write).await.map_err(status)?;
    }
    tx.emit(lineage).await.map_err(status)?;
    let snap = tx.commit().await.map_err(status)?;
    Ok(Response::new(pb::CommitTransformResponse { snapshot_id: snap.map(|s| s.0) }))
}
```

(`status` at service.rs:19 already maps `Conflict => aborted`, `NotFound => not_found`, rest `internal` — exactly the spec's mapping. `TableControlPlane`/`TableTx` come from `control_plane_core` — engine BUCK already deps core + postgres.)

Engine `list_files` gains columns: in the `Ok(snap)` arm also fetch `ice.schema(&table, snap.id)`, map `ColumnDef -> ColumnSpec` (name/ty/nullable — the `transform/src/run.rs:141` mapping), serialize to `columns_json: Some(json)`; the `NotFound` arm returns `files: vec![], columns_json: None`.

- [x] **Step 1 (red):** write `src/services/engine/tests/transform_wire.rs` (mirror `engine/tests/write_wire.rs` boot pattern / `spawn_engine_uds` with `control: true`), four cases:
  1. `commit_transform_appends_and_emits_lineage` — seed nothing; call `client.commit_transform("main", "t_out", &cols, &files, &event, false)` where `files` come from `write_dataset` + `absolute_data_files` against a real warehouse tempdir; assert `Some(snapshot)`, `IcebergCatalog::current_snapshot` resolves, `files_with_stats` names the committed paths, and the lineage event round-trips (query via the pool-backed lineage concern, mirroring `transform_e2e.rs:173`).
  2. `commit_transform_replace_expires_prior_live_set` — append once, then `replace=true` with a new file; assert live set == new file only and the prior snapshot still time-travels (files at old snapshot id unchanged).
  3. `commit_transform_bad_lineage_json_is_invalid_argument` — raw `EngineControlClient` call with garbage `lineage_json`; assert `Code::InvalidArgument`.
  4. `list_files_reports_columns_and_absence` — for a seeded table: `columns == Some(declared)`; for an unknown table: `files` empty and `columns == None`; for a zero-file table (port `create_empty_table` from `transform/tests/transform_e2e.rs:188`): `columns == Some(declared)`, `files` empty.
  Run: `buck2 test //src/services/engine:transform-wire --unstable-allow-all-tests-on-re > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t3.log` — FAIL (rpc absent).
- [x] **Step 2:** proto + regen (build does it — `pb-gen` genrule), engine ctor widening, engine handler + list_files columns, client `TableFiles`/`list_files`/`commit_transform`, and all three pre-existing shape sites: compact.rs caller (`let live = ctx.control.list_files(...).await.map_err(...)?.files;`), `compact_wire.rs`'s two callers (`.files`), `compact_rpc.rs`'s response literal (`columns_json: None`).
- [x] **Step 3:** rerun Step 1 (PASS) + the untouched wire suites: `buck2 test //src/services/engine: //src/services/engine-wire: //src/services/worker:compact-e2e --unstable-allow-all-tests-on-re > /tmp/t3b.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t3b.log`
- [x] **Step 4:** prek; commit `feat(engine): CommitTransform RPC + declared columns on ListFiles`

### Task 4: Physical `transform` on the worker

**Files:**
- Create: `src/services/worker/src/transform.rs`
- Modify: `src/services/worker/src/lib.rs` (`pub mod transform;`), `src/services/worker/src/main.rs` (kinds + dispatch + ctx), `src/services/worker/BUCK` (lib gains `//third-party:datafusion` + `//third-party:time`; new `transform-e2e` fixture target mirroring `compact-e2e` deps)
- Create: `src/services/worker/tests/transform_e2e.rs`

**Interfaces (produced, `worker/src/transform.rs`):**

```rust
#[derive(Clone)]
pub struct TransformCtx {
    pub control: GrpcQueueClient,
    pub flight: FlightTableClient,
    pub write: Arc<WriteStore>,
    pub write_cfg: WriteConfig,
    pub worker_tuning: WorkerTuning,
}

pub async fn handle_transform(ctx: &TransformCtx, job: Job) -> Result<(), JobFailure>;
// Task 5 adds: pub async fn handle_typed_transform(ctx: &TransformCtx, job: Job) -> Result<(), JobFailure>;

// private shared core both handlers call (Task 5 threads conform/lineage through it):
struct WireTransform<'a> {
    inputs: Vec<(String, TableRef)>, // (register_as, table)
    output: &'a TableRef,
    sql: &'a str,
    conform: Option<&'a [PropertyDef]>,
    output_mode: OutputMode,
    lineage: LineageEvent,
}
async fn run_wire_transform(ctx: &TransformCtx, attempts: i32, req: WireTransform<'_>)
    -> Result<(), JobFailure>;
```

`handle_transform` body (exact taxonomy, mirroring `compact.rs` error style):

```rust
pub async fn handle_transform(ctx: &TransformCtx, job: Job) -> std::result::Result<(), JobFailure> {
    let attempts = job.attempts;
    let parsed: TransformJob = serde_json::from_value(job.payload)
        .map_err(|e| JobFailure::abandon(format!("bad transform payload: {e}")))?;
    let inputs: Vec<(String, TableRef)> = parsed.inputs.iter()
        .map(|t| (t.name.clone(), t.clone()))
        .collect();
    let lineage = LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: parsed.inputs.iter().map(DatasetRef::from).collect(),
        outputs: vec![DatasetRef::from(&parsed.output)],
        payload: serde_json::json!({ "sql": parsed.sql }),
    };
    run_wire_transform(ctx, attempts, WireTransform {
        inputs,
        output: &parsed.output,
        sql: &parsed.sql,
        conform: None,
        output_mode: parsed.output_mode,
        lineage,
    }).await
}
```

`run_wire_transform` steps (each with its outcome class):

1. **Ambiguity** — `HashSet` over `register_as`; duplicate ⇒ `JobFailure::abandon(format!("ambiguous input table name {n}: two inputs would register under it"))`.
2. **Per input:** `ctx.control.list_files(t.schema.clone(), t.name.clone())` — wire error ⇒ `JobFailure::retry(ctx.worker_tuning.backoff(attempts), format!("list_files: {e}"))`. `columns: None` ⇒ `JobFailure::abandon(format!("unknown input table {}.{}", t.schema, t.name))`.
3. **Register:** files empty ⇒ `logical_arrow_schema(&columns)` (Err ⇒ abandon `infer:`) + `register_empty_table(&ctx_df, register_as, schema)` (Err ⇒ abandon). Non-empty ⇒ `ctx.flight.fetch(FlightTicket { schema, name, files: paths })` (Err ⇒ retry `flight fetch:` — the live-set race converges on re-list); empty batch vec ⇒ fall back to the declared-columns empty registration; else `register_batches(&ctx_df, register_as, batches[0].schema(), batches)` (Err ⇒ abandon).
4. **Compute:** fresh `SessionContext::new()` built before step 3; `ctx_df.sql(req.sql).await` ⇒ abandon on Err (`sql:`); `infer_columns(df.schema().as_arrow())` ⇒ abandon on Err; if `Some(props) = req.conform`, `check_conformance(&columns, props)` ⇒ abandon with the violation debug list **before any `collect()`**.
5. **Collect + write:** `df.collect().await` ⇒ abandon on Err (DataFusion class); `write_dataset(ctx.write.store.clone(), &format!("{}/{}/{}", output.schema, output.name, run_id), <schema from logical_arrow_schema(&columns)>, &batches, &ctx.write_cfg)` ⇒ retry on Err; `absolute_data_files(written, &ctx.write.root_url, &output.schema, &output.name)`. (`run_id` = `uuid::Uuid::new_v4().to_string()`, minted at step 5, exactly like `compact.rs:61`.)
6. **Commit:** `ctx.control.commit_transform(output.schema.clone(), output.name.clone(), &columns, &files, &req.lineage, matches!(req.output_mode, OutputMode::Overwrite))` — Err ⇒ retry (`commit_transform:`); `Ok(None)` ⇒ `JobFailure::abandon("commit produced no snapshot id")`.

(Write-schema note: the parquet files must carry the SQL result schema — use `df.schema().as_arrow().clone().into()` (`SchemaRef`) captured at step 4 for `write_dataset`, NOT a re-derivation; this matches `run.rs:182-189` which writes with the result schema.)

`main.rs`: kinds array gains `TRANSFORM_JOB_KIND.to_string()` (Task 5 adds the typed kind); build `let tctx = TransformCtx { control: client.clone(), flight: flight.clone(), write: write.clone(), write_cfg: wcfg.write.clone(), worker_tuning };` (note: `flight`/`write` currently move into `CompactCtx` — clone before) and add the dispatch arm `k if k == TRANSFORM_JOB_KIND => handle_transform(&tctx, job).await,`.

- [x] **Step 1 (red):** write `worker/tests/transform_e2e.rs` (mirror `compact_e2e.rs` scaffolding: `PgFixture::shared()`, shared warehouse tempdir, `local_sql_catalog` for seeding, `spawn_engine_uds(fx, &db, &wh_str, EngineOpts { control: true, flight: true, ..EngineOpts::default() })`). BUCK target deps = `compact-e2e`'s list PLUS `//third-party:sqlx` (lineage payload assert). Five cases:
  1. `transform_runs_over_the_wire` — seed `main.src` via `land` with ids `[1,2,3]`; **enqueue** a `NewJob { kind: TRANSFORM_JOB_KIND, payload: serde_json::to_value(TransformJob{...}), .. }` through the fixture's `PgControlPlane::queue()`, then **dequeue it via `GrpcQueueClient::dequeue(&[TRANSFORM_JOB_KIND.to_string()], "e2e-worker")`** (pins the kind string through the real queue, per the spec's "enqueue a transform job" acceptance wording); job SQL `SELECT id FROM src WHERE id >= 2` into `main.dst`; `handle_transform` returns Ok; read `main.dst` rows back via `FlightTableClient::fetch` of its listed files (assert ids `{2,3}`); assert the lineage event via raw sqlx against `lineage.event`/`lineage.event_dataset` (see the payload read-back note in Key pre-verified signatures): output dataset row names `main.dst`, input row names `main.src`, and `payload["sql"]` equals the job SQL; assert a snapshot id exists via `IcebergCatalog::current_snapshot`.
  2. `unknown_input_abandons` — job referencing `main.nope`; assert `Err(JobFailure { policy: RetryPolicy::Abandon, .. })` and error contains `unknown input table`.
  3. `ambiguous_register_as_abandons` — two inputs both named `dup` (different schemas); Abandon before any RPC.
  4. `empty_input_counts_zero` — port `create_empty_table` (transform_e2e.rs:188-197) **verbatim, including the empty `append_files(table, &[])`** against a directly-constructed `IcebergControlPlane` (test dep on postgres is fine); job `SELECT count(*) AS n FROM empty_in`; assert output table's single row is `0` (fetch + downcast).
  5. `overwrite_replaces_live_set_and_time_travels` — run an append transform, capture snapshot; run again with `output_mode: Overwrite` and different predicate; assert live files serve only the new result and the captured older snapshot's file list is unchanged (`ice.files(...)` at the old id — the `overwrite_e2e.rs` assertion pattern).
  Run: `buck2 test //src/services/worker:transform-e2e --unstable-allow-all-tests-on-re > /tmp/t4.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t4.log` — FAIL (module absent).
- [x] **Step 2:** implement `transform.rs` + `lib.rs` + `main.rs` + BUCK (lib deps += `//third-party:datafusion`, `//third-party:time`; `transform-e2e` target = `compact-e2e` dep list).
- [x] **Step 3:** rerun Step 1 (PASS) + `buck2 test //src/services/worker: --unstable-allow-all-tests-on-re > /tmp/t4b.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t4b.log`.
- [x] **Step 4:** structural acceptance probe: `grep -n "sqlx\|control-plane/postgres" src/services/worker/BUCK` must hit only test-target dep lists (the `worker`/`worker-bin` blocks stay clean).
- [x] **Step 5:** prek; commit `feat(worker): physical transform jobs over the engine wire`

### Task 5: `typed-transform` on the worker

**Files:**
- Modify: `src/services/worker/src/transform.rs` (add `handle_typed_transform`), `src/services/worker/src/main.rs` (kind + arm)
- Create: `src/services/worker/tests/typed_transform_e2e.rs` (+ BUCK fixture target)

**Interfaces:** `handle_typed_transform(ctx: &TransformCtx, job: Job) -> Result<(), JobFailure>`:

```rust
pub async fn handle_typed_transform(ctx: &TransformCtx, job: Job) -> std::result::Result<(), JobFailure> {
    let attempts = job.attempts;
    let parsed: TypedTransformJob = serde_json::from_value(job.payload)
        .map_err(|e| JobFailure::abandon(format!("bad typed-transform payload: {e}")))?;
    // Resolve inputs: NotFound is deterministic (Abandon), other errors transient (Retry).
    let mut inputs = Vec::new();
    let mut input_types = Vec::new();
    for name in &parsed.inputs {
        let ty = TypeName(name.clone());
        let table = ctx.control.gov_resolve(&ty).await.map_err(|e| match e {
            ControlPlaneError::NotFound(_) =>
                JobFailure::abandon(format!("unknown ontology type {name}")),
            other => JobFailure::retry(ctx.worker_tuning.backoff(attempts), format!("resolve: {other}")),
        })?;
        inputs.push((name.clone(), table));
        input_types.push(ty);
    }
    let out_ty = TypeName(parsed.output.clone());
    let out_type = ctx.control.gov_get_type(&out_ty).await.map_err(|e| match e {
        ControlPlaneError::NotFound(_) =>
            JobFailure::abandon(format!("unknown ontology type {}", parsed.output)),
        other => JobFailure::retry(ctx.worker_tuning.backoff(attempts), format!("get_type: {other}")),
    })?;
    let lineage = LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: input_types.iter().map(DatasetRef::from).collect(),
        outputs: vec![DatasetRef::from(&out_ty)],
        payload: serde_json::json!({
            "sql": parsed.sql,
            "input_tables": inputs.iter().map(|(_, t)| format!("{}.{}", t.schema, t.name)).collect::<Vec<_>>(),
            "output_table": format!("{}.{}", out_type.table.schema, out_type.table.name),
        }),
    };
    run_wire_transform(ctx, attempts, WireTransform {
        inputs,
        output: &out_type.table,
        sql: &parsed.sql,
        conform: Some(&out_type.properties),
        output_mode: parsed.output_mode,
        lineage,
    }).await
}
```

(Byte-identical lineage payload to `typed.rs:70-84`; register_as = type name, matching `typed.rs:61-67`.)

- [ ] **Step 1 (red):** `worker/tests/typed_transform_e2e.rs` — seed backing tables via `land`, define types via `pg.ontology().define_type(ObjectType{...})` — **copy the literal from `typed_transform_e2e.rs:182-190` verbatim; it also needs `derived: vec![]` and `identity: None`**, don't paraphrase the struct. BUCK target deps = `compact-e2e`'s list PLUS `//third-party:sqlx`. Three cases:
  1. `typed_transform_commits_with_type_named_lineage` — SQL in type terms; assert rows land in the output type's backing table, and — via the same raw-sqlx lineage read as Task 4 — the event's input/output dataset rows are TYPE refs (`loom:type` namespace) and `payload["input_tables"]`/`["output_table"]` name the physical tables (pins the byte-identical typed payload).
  2. `nonconforming_result_abandons_without_commit` — SQL yielding an extra column; assert Abandon mentioning `violation`, output table has no snapshot (`current_snapshot` ⇒ NotFound), and no lineage event for the output.
  3. `unknown_type_abandons` — input type not defined ⇒ Abandon `unknown ontology type`.
  Run: `buck2 test //src/services/worker:typed-transform-e2e --unstable-allow-all-tests-on-re > /tmp/t5.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t5.log` — FAIL.
- [ ] **Step 2:** implement handler + main.rs kind/arm + BUCK target.
- [ ] **Step 3:** rerun (PASS) + `buck2 test //src/services/worker: --unstable-allow-all-tests-on-re > /tmp/t5b.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t5b.log`.
- [ ] **Step 4:** prek; commit `feat(worker): typed transform jobs over the engine wire`

### Task 6: Delete the pool-owning transform crate

**Files:**
- Delete: `src/services/transform/` (whole directory: src, tests, BUCK. **There is NO `Cargo.toml` in the crate and it is NOT a workspace member** — the workspace `Cargo.toml`, `Cargo.lock`, and `third-party/BUCK` are untouched; do not run `cargo generate-lockfile`/`buckify.sh`.)

Coverage accounting before deletion (all already ported): `output_mode`/`conform` → core (Task 1); `run_unknown_input`'s table-absent classification → worker e2e case 2 (Task 4) — its mid-handler drop-RACE class is deliberately reclassified to converging-Retry (see Global Constraints deviation 2; name it in the PR body); `transform_e2e` happy/empty → Task 4 cases 1/4; `overwrite_e2e` → Task 4 case 5; `typed_transform_e2e` → Task 5 (its query-api governed read-back of the typed output is dropped — governed reads over typed tables are pinned by query-api's own e2e suite; note in the PR body); `iceberg_backend_e2e`'s commit-sequence coverage → engine `transform_wire.rs` (Task 3). `transform_chain_e2e`'s absolute-path re-read: add the explicit chain as case 6 in `transform_e2e.rs`: second job `SELECT * FROM dst` into `main.dst2`, assert rows (input registered from absolute-path files exercises the wire read of an absolute live set).

- [ ] **Step 1:** add the chain case (it should pass immediately; it pins the ported behavior): `buck2 test //src/services/worker:transform-e2e --unstable-allow-all-tests-on-re > /tmp/t6a.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t6a.log`
- [ ] **Step 2:** `git rm -r src/services/transform`; `grep -rn "services/transform" src/ deploy/ buildbuddy.yaml .github/ third-party/BUCK` — expect zero hits (docs handled in Task 7).
- [ ] **Step 3:** whole-suite sweep (spec acceptance): `buck2 build -M none //src/... > /tmp/b6.log 2>&1; grep -c "BUILD FAILED" /tmp/b6.log` (0), then `buck2 test //src/... --unstable-allow-all-tests-on-re > /tmp/t6.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t6.log` — PASS.
- [ ] **Step 4:** prek; commit `refactor(worker)!: delete the pool-owning transform service — transforms run on the zero-pool worker`

### Task 7: Register close + capability docs

**Files:**
- Modify: `docs/ROADMAP.md` (delete the `road-transform-wire-migration` entry block), `docs/system-capabilities/transform.md` (rewrite: transforms execute on the zero-pool worker over the engine wire; `CommitTransform`; `columns_json`; crate deleted — keep the compute-pipeline prose, repoint paths to `worker/src/transform.rs` + core; `(#PRNUM)` placeholder), `docs/system-capabilities/engine.md` (CommitTransform + ListFiles columns under the EngineControl surface), `docs/system-capabilities/build-and-test.md` (only if it names transform targets — grep)
- Check: `grep -rn 'road-transform-wire-migration\|fut-transform-wire-migration' docs/ .claude/ src/` — rewrite surviving `[[...]]` links as `` `#id` `` code spans; delete matching Known-gaps bullets. FUTURE's `fut-transform-followups` prose references stay (they name deferred follow-ups, not this item).
- Grep stale paths: `grep -rn 'services/transform' docs/` — repoint or delete every hit (ROADMAP/FUTURE prose included; `docs/system-capabilities/transform.md:7,17,55,117` are known hits). The generated code-health registers (`docs/code-health/complexity.md:137-141`, `duplication.md:37-54`) will also hit — delete the dead rows but don't hand-reconcile beyond that; the scheduled census routines rebuild them.

- [ ] **Step 1:** register + docs edits; `bash tools/docs.sh validate` → OK.
- [ ] **Step 2:** prek; commit `docs: close road-transform-wire-migration — transforms on the zero-pool worker`
