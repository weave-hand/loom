# Orphaned-Parquet GC Sweep Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a warehouse-scoped, schedulable `sweep_orphans` job that LISTs the Iceberg warehouse, diffs pattern-scoped data objects (`*.parquet` + `*.puffin`) against every mirror-referenced path, and deletes the unreferenced remainder older than a write-race grace window — the third and final GC source.

**Architecture:** A new `sweep_orphans` primitive in the postgres adapter does the LIST → read-references → diff → grace-filter → delete pipeline (never inside a Postgres tx). It is exposed as a new `EngineControl::SweepOrphans` RPC, driven by a new `sweep_orphans` queue kind that the zero-pool worker dispatches over the wire, and enqueued by the existing `/admin/schedules` cron surface. Grace is a global env knob threaded through the engine config exactly like GC retention.

**Tech Stack:** Rust 2024, buck2, tonic/prost (engine wire), sqlx compile-time macros (postgres), object_store (S3/local LIST+delete), tokio, `loom_fixture_test` (MinIO+Postgres fixture).

## Global Constraints

- **Strict clippy (pedantic + restriction).** No `unwrap`/`expect`/`panic`/`indexing_slicing`/`todo` in **production** lib/bin code; silence locally only with `#[expect(lint, reason = "...")]` (bare `#[allow]` needs a `reason`). **Test** code is exempted from the panic-safety lints by the `loom_rust_test`/`loom_fixture_test` wrappers, so tests may `unwrap`/`expect` freely.
- **Tests are `rust_test`/`loom_fixture_test` integration targets only** — a sibling `tests/<name>.rs` wired in the crate's `BUCK`. **NEVER** inline `#[cfg(test)] mod tests` / `#[test]` in `src/**.rs` (the `no-inline-tests` prek hook fails the build). **New fixture tests MUST use `loom_fixture_test`**, not a bare `rust_test`.
- **No object-store I/O inside a Postgres transaction.** The sweep reads references with plain `SELECT`s, then deletes objects outside any tx.
- **Compile-time SQL:** new `sqlx::query_scalar!` macros are verified against the committed `.sqlx` cache. After adding/altering SQL, run **`tools/sqlx-prepare.sh`** and commit `src/control-plane/postgres/.sqlx/`. The `sqlx-cache-check` test (in the normal test sweep) fails on a stale cache.
- **Before every commit:** `buck2 run //tools:prek -- run --all-files` (rustfmt, clippy, eof/trailing-whitespace, reindeer-in-sync). Commit whatever the hooks fix.
- **Commit messages** follow Conventional Commits (the commit-msg hook enforces it).
- **Build / test commands** (single clean invocations):
  - Build: `buck2 build -v0 --console none //src/...` (silent on success).
  - Test: `buck2 test --console none //src/...` (prints only the pass/fail summary).
  - In a **cloud** session, mind the ~38 GiB disk cap: build with `buck2 build -M none //src/...` and scope tests to the touched targets — never a bare whole-tree `buck2 build`.
- **No dry-run mode, no HTTP enqueue endpoint** in v1 (operator decisions 2026-07-12) — schedules are the only surface; safety is pattern-scoping + grace + per-deletion logging.

---

## File Structure

New files:
- `src/control-plane/core/src/orphan_sweep.rs` — the `sweep_orphans` queue-kind contract (`ORPHAN_SWEEP_JOB_KIND` + `OrphanSweepJob {}`).
- `src/control-plane/core/tests/orphan_sweep_job.rs` — kind membership + schedule-validation unit tests.
- `src/control-plane/postgres/src/orphan_sweep.rs` — the `sweep_orphans` primitive (`SweepSummary` + the LIST/diff/grace/delete pipeline).
- `src/control-plane/postgres/tests/orphan_sweep.rs` — the fixture suite proving every safety class.

Modified files:
- `src/control-plane/core/src/{lib.rs,queue.rs,job_schedule.rs}` — declare/re-export the module; add the kind to `KNOWN_JOB_KINDS`, `SCHEDULABLE_JOB_KINDS`, and the `validate_job_schedule` decode arm.
- `src/control-plane/core/BUCK` — the new core test target.
- `src/control-plane/postgres/src/lib.rs` — `pub mod orphan_sweep;`.
- `src/control-plane/postgres/BUCK` — the new fixture-test target.
- `src/control-plane/postgres/.sqlx/` — regenerated cache (two new queries).
- `src/services/engine-wire/proto/engine_control.proto` — the `SweepOrphans` RPC + messages.
- `src/services/engine-wire/src/client.rs` — `GrpcQueueClient::sweep_orphans`.
- `src/services/engine/src/service.rs` — two new `EngineControlService` fields + the `sweep_orphans` RPC method.
- `src/services/engine/src/run.rs` — build the `WriteStore`, thread grace into the service.
- `src/services/runtime/src/lib.rs` — `Config.orphan_sweep_grace` + its `from_map` parse.
- `src/services/runtime/tests/config.rs` — grace default/override test.
- `src/services/runtime/tests/admin_management.rs` — pin: a `sweep_orphans` schedule is accepted with no table.
- `src/testing/flight.rs` + `src/testing/BUCK` — `EngineOpts.orphan_sweep_grace`, build a `WriteStore` for the spawned engine, pass the two new fields.
- `src/services/worker/src/handler.rs` — `handle_sweep_orphans`.
- `src/services/worker/src/main.rs` — dequeue-kind + dispatch arm.
- `src/services/worker/tests/scheduled_maintenance_e2e.rs` — the `sweep_orphans` schedule→worker→delete leg.

**Task ordering rationale.** The proto RPC and the engine's server-side `impl EngineControl` are atomically coupled — adding an `rpc` to the `.proto` makes the trait impl incomplete until the server method exists, so the proto edit, the server method, the config knob, the harness wiring, and the client method **must land in one task** (Task 3), and that task depends on the postgres primitive (Task 2) already existing. Hence: core (1) → postgres primitive (2) → proto+engine+config+harness+client (3) → worker handler (4) → worker e2e (5) → docs close (6).

---

## Task 1: Core `orphan_sweep` job contract + allowlist wiring

**Files:**
- Create: `src/control-plane/core/src/orphan_sweep.rs`
- Modify: `src/control-plane/core/src/lib.rs` (module decl + re-export)
- Modify: `src/control-plane/core/src/queue.rs:32-41` (`KNOWN_JOB_KINDS`)
- Modify: `src/control-plane/core/src/job_schedule.rs` (`SCHEDULABLE_JOB_KINDS` + decode arm)
- Create/Test: `src/control-plane/core/tests/orphan_sweep_job.rs`
- Modify: `src/control-plane/core/BUCK` (new `rust_test` target)

**Interfaces:**
- Produces:
  - `pub const ORPHAN_SWEEP_JOB_KIND: &str = "sweep_orphans"` (re-exported at crate root as `control_plane_core::ORPHAN_SWEEP_JOB_KIND`).
  - `pub struct OrphanSweepJob {}` — `Serialize + Deserialize + Debug + Clone`; round-trips the empty JSON object `{}` (re-exported as `control_plane_core::OrphanSweepJob`).
- Consumes: existing `JobSchedule`, `validate_job_schedule`, `SCHEDULABLE_JOB_KINDS`, `KNOWN_JOB_KINDS`.

- [ ] **Step 1: Write the failing test** — `src/control-plane/core/tests/orphan_sweep_job.rs`

```rust
//! The `sweep_orphans` queue kind: it is a known, schedulable kind, its empty
//! payload round-trips `{}`, and `validate_job_schedule` accepts/rejects it.

use control_plane_core::{
    JobSchedule, KNOWN_JOB_KINDS, ORPHAN_SWEEP_JOB_KIND, OrphanSweepJob, SCHEDULABLE_JOB_KINDS,
    validate_job_schedule,
};

#[test]
fn kind_is_known_and_schedulable() {
    assert!(KNOWN_JOB_KINDS.contains(&ORPHAN_SWEEP_JOB_KIND));
    assert!(SCHEDULABLE_JOB_KINDS.contains(&ORPHAN_SWEEP_JOB_KIND));
}

#[test]
fn empty_payload_round_trips_as_object() {
    let v = serde_json::to_value(OrphanSweepJob {}).expect("serialize");
    assert_eq!(v, serde_json::json!({}), "payload is the empty JSON object");
    let _back: OrphanSweepJob = serde_json::from_value(v).expect("deserialize");
}

#[test]
fn schedule_validates_with_empty_payload() {
    let s = JobSchedule {
        name: "nightly-sweep".into(),
        kind: ORPHAN_SWEEP_JOB_KIND.into(),
        payload: serde_json::json!({}),
        cron: "0 4 * * *".into(),
    };
    validate_job_schedule(&s).expect("sweep_orphans schedule must validate");
}

#[test]
fn schedule_rejects_non_object_payload() {
    let s = JobSchedule {
        name: "bad-sweep".into(),
        kind: ORPHAN_SWEEP_JOB_KIND.into(),
        payload: serde_json::json!("not-an-object"),
        cron: "0 4 * * *".into(),
    };
    assert!(
        validate_job_schedule(&s).is_err(),
        "a non-object sweep payload must fail decode validation"
    );
}
```

- [ ] **Step 2: Add the core test target** — `src/control-plane/core/BUCK` (mirror the `gc-job` target at `:227`)

```python
rust_test(
    name = "orphan-sweep-job",
    crate = "orphan_sweep_job",
    srcs = ["tests/orphan_sweep_job.rs"],
    crate_root = "tests/orphan_sweep_job.rs",
    edition = "2024",
    visibility = ["PUBLIC"],
    deps = [
        ":core",
        "//third-party:serde_json",
    ],
)
```

- [ ] **Step 3: Run the test to verify it fails to compile**

Run: `buck2 test --console none //src/control-plane/core:orphan-sweep-job`
Expected: FAIL — `ORPHAN_SWEEP_JOB_KIND` / `OrphanSweepJob` unresolved.

- [ ] **Step 4: Create the module** — `src/control-plane/core/src/orphan_sweep.rs`

```rust
//! The orphan-sweep job contract, shared by the producer (a schedule firing) and
//! the consumer (the worker). Lives in core so a zero-pool worker can read it
//! without depending on the postgres adapter. Unlike `gc_table` / `compact_table`,
//! the sweep is **warehouse-scoped**: it names no table — it diffs the whole
//! warehouse — so its payload carries no fields.

/// The queue `kind` for an orphaned-object sweep job.
pub const ORPHAN_SWEEP_JOB_KIND: &str = "sweep_orphans";

/// The payload of a `sweep_orphans` job. A braced (not unit) struct so it
/// round-trips the empty JSON object `{}` that schedules and the queue serialize
/// it as — a unit struct would (de)serialize as `null` instead.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
#[expect(
    clippy::empty_structs_with_brackets,
    reason = "the schedule/queue payload is the empty JSON object `{}`; a braced struct deserializes it, a unit struct would require `null`"
)]
pub struct OrphanSweepJob {}
```

- [ ] **Step 5: Declare + re-export in `lib.rs`** — `src/control-plane/core/src/lib.rs`

Add the module declaration next to `mod gc;` (`:13`):

```rust
mod orphan_sweep;
```

Add the re-export next to `pub use gc::{GC_JOB_KIND, GcJob};` (`:53`):

```rust
pub use orphan_sweep::{ORPHAN_SWEEP_JOB_KIND, OrphanSweepJob};
```

- [ ] **Step 6: Add to `KNOWN_JOB_KINDS`** — `src/control-plane/core/src/queue.rs:32-41`

Append inside the `&[ ... ]` literal (after `crate::STREAM_MV_JOB_KIND,`):

```rust
    crate::ORPHAN_SWEEP_JOB_KIND,
```

- [ ] **Step 7: Add to `SCHEDULABLE_JOB_KINDS` + decode arm** — `src/control-plane/core/src/job_schedule.rs`

Extend the `use` at the top (after the `gc` import at `:9`):

```rust
use crate::orphan_sweep::{ORPHAN_SWEEP_JOB_KIND, OrphanSweepJob};
```

Change `SCHEDULABLE_JOB_KINDS` (`:15`) to include the new kind:

```rust
pub const SCHEDULABLE_JOB_KINDS: &[&str] =
    &[GC_JOB_KIND, COMPACT_JOB_KIND, ORPHAN_SWEEP_JOB_KIND];
```

Add a decode arm in `validate_job_schedule`'s `match` (after the `COMPACT_JOB_KIND` arm at `:52-56`, before the `other =>` arm):

```rust
        ORPHAN_SWEEP_JOB_KIND => {
            serde_json::from_value::<OrphanSweepJob>(s.payload.clone()).map_err(|e| {
                ControlPlaneError::Validation(format!("invalid sweep_orphans payload: {e}"))
            })?;
        }
```

- [ ] **Step 8: Run the test to verify it passes**

Run: `buck2 test --console none //src/control-plane/core:orphan-sweep-job`
Expected: `Tests finished: Pass 4. Fail 0`.

- [ ] **Step 9: Lint + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/control-plane/core/
git commit -m "feat(core): sweep_orphans queue kind + schedulable payload contract"
```

---

## Task 2: Postgres `sweep_orphans` primitive + `.sqlx` + fixture suite

**Files:**
- Create: `src/control-plane/postgres/src/orphan_sweep.rs`
- Modify: `src/control-plane/postgres/src/lib.rs:28` (add `pub mod orphan_sweep;`)
- Create/Test: `src/control-plane/postgres/tests/orphan_sweep.rs`
- Modify: `src/control-plane/postgres/BUCK` (new `loom_fixture_test` target)
- Modify: `src/control-plane/postgres/.sqlx/` (regenerate)

**Interfaces:**
- Produces:
  - `pub struct SweepSummary { pub objects_deleted: u64, pub bytes_deleted: u64, pub candidates_skipped_grace: u64 }` — `Debug + Clone + Default + PartialEq + Eq`.
  - `pub async fn sweep_orphans(store: &Arc<dyn object_store::ObjectStore>, root_url: &str, pool: &sqlx::PgPool, grace: std::time::Duration) -> control_plane_core::Result<SweepSummary>`.
- Consumes: `control_plane_core::{Result, ControlPlaneError}`; `crate::backend` (sqlx-error → `ControlPlaneError`); `object_store::ObjectStore::list`; `iceberg_mirror.data_file.path` + `iceberg_mirror.vector_index.puffin_path` (both `NOT NULL`).

> **Deviation from the spec's stated signature (justified):** the spec lists `sweep_orphans(catalog: &SqlCatalog, pool, store: &WriteStore, grace)`. This plan (a) **drops `catalog`** — deletion goes straight through the object store handle (`ObjectMeta.location` is already a store key), so `catalog` would be an unused param that trips clippy; and (b) takes **`(store: &Arc<dyn ObjectStore>, root_url: &str)` instead of `&WriteStore`** so the control-plane crate does **not** gain a dependency on the `store-config` (services-layer) crate — a layering inversion. `WriteStore` is just `{ store, root_url }`, so the engine passes `&ws.store, &ws.root_url`. The spec explicitly leaves module/interface placement open ("or a sibling module"); behavior is unchanged.

- [ ] **Step 1: Write the failing fixture tests** — `src/control-plane/postgres/tests/orphan_sweep.rs`

```rust
//! Fixture tests for the orphaned-object sweep (`orphan_sweep::sweep_orphans`):
//! planted orphans older than grace are deleted; every mirror-referenced object
//! (live, historical-in-window, dropped-in-window, puffin) survives; Iceberg
//! metadata is out of scope and untouchable; young orphans are held by grace; a
//! second immediate sweep is a clean no-op.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    Catalog, ColumnSpec, DatasetId, EventType, LineageEvent, RunId, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_landing::{InlineLimits, land, overwrite_parquet_snapshot};
use control_plane_postgres::orphan_sweep::{SweepSummary, sweep_orphans};
use control_plane_postgres::vector_index::{VectorIndexRow, insert_vector_index};
use iceberg::{Catalog as _, NamespaceIdent, TableIdent};
use loom_test_seed::local_sql_catalog;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use time::OffsetDateTime;

const ZERO_GRACE: Duration = Duration::ZERO;

fn columns() -> Vec<ColumnSpec> {
    vec![ColumnSpec { name: "id".into(), ty: "long".into(), nullable: false }]
}

fn batch(rows: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>()))],
    )
    .expect("batch")
}

fn ipc_body(rows: i64) -> (Arc<Schema>, Vec<RecordBatch>) {
    let b = batch(rows);
    (b.schema(), vec![b])
}

fn lineage(run: RunId, schema: &str, name: &str) -> LineageEvent {
    let out = TableRef { schema: schema.into(), name: name.into() };
    LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(&out).dataset_ref()],
        payload: serde_json::json!({ "source": "orphan-sweep-test" }),
    }
}

fn small_limits() -> InlineLimits {
    InlineLimits { inline_byte_limit: 0, flush_byte_threshold: i64::MAX }
}

/// A writable local object store rooted at `wh`, plus the matching `root_url`
/// prefix — the same shape `store_config::build_write_store` produces for a
/// `file://` warehouse, but without pulling the services-layer crate into a
/// control-plane test.
fn store_for(wh: &std::path::Path) -> (Arc<dyn ObjectStore>, String) {
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(wh).expect("local store"));
    (store, format!("file://{}", wh.display()))
}

/// Strip a `file://` URL to a local filesystem path.
fn local_path(file_url: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(file_url.strip_prefix("file://").unwrap_or(file_url))
}

/// Planted orphans older than grace are deleted; the referenced data file survives.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sweep_deletes_orphans_keeps_referenced() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let ice = IcebergCatalog::new(pool.clone());
    let t = TableRef { schema: "wh".into(), name: "kept".into() };

    let (schema, batches) = ipc_body(4);
    let s1 = land(
        &pool, &catalog, &t, &columns(), schema, batches, small_limits(),
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "kept"), None,
    )
    .await
    .expect("land");
    let kept = local_path(&ice.files_with_stats(&t, s1).await.expect("files")[0].path);
    assert!(kept.exists(), "referenced file present before sweep");

    // Two planted orphans (a stray parquet + a stray puffin), referenced by no row.
    let orphan_parquet = wh.path().join("orphan-abc.parquet");
    let orphan_puffin = wh.path().join("orphan-abc.puffin");
    std::fs::write(&orphan_parquet, b"orphan-parquet").expect("write orphan parquet");
    std::fs::write(&orphan_puffin, b"orphan-puffin").expect("write orphan puffin");

    let (store, root_url) = store_for(wh.path());
    let summary = sweep_orphans(&store, &root_url, &pool, ZERO_GRACE).await.expect("sweep");

    assert!(!orphan_parquet.exists(), "orphan parquet deleted");
    assert!(!orphan_puffin.exists(), "orphan puffin deleted");
    assert!(kept.exists(), "referenced data file survived");
    assert_eq!(summary.objects_deleted, 2, "both orphans deleted: {summary:?}");
    assert_eq!(summary.candidates_skipped_grace, 0);
}

/// A young orphan (freshly written) is HELD when grace exceeds its age.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sweep_holds_young_orphan_under_grace() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let _catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;

    let orphan = wh.path().join("young.parquet");
    std::fs::write(&orphan, b"young").expect("write");

    let (store, root_url) = store_for(wh.path());
    let summary = sweep_orphans(&store, &root_url, &pool, Duration::from_secs(3600))
        .await
        .expect("sweep");

    assert!(orphan.exists(), "young orphan held by the 1h grace window");
    assert_eq!(summary.objects_deleted, 0);
    assert_eq!(summary.candidates_skipped_grace, 1, "held young orphan counted: {summary:?}");
}

/// Iceberg metadata/manifest objects are out of scope: never candidates, even
/// when unreferenced. A control orphan parquet in the same dir IS deleted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sweep_never_touches_metadata() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let _catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;

    let meta_json = wh.path().join("v3.metadata.json");
    let manifest = wh.path().join("snap-42.avro");
    let control = wh.path().join("stray.parquet");
    std::fs::write(&meta_json, b"{}").expect("meta");
    std::fs::write(&manifest, b"avro").expect("manifest");
    std::fs::write(&control, b"parquet").expect("control");

    let (store, root_url) = store_for(wh.path());
    let summary = sweep_orphans(&store, &root_url, &pool, ZERO_GRACE).await.expect("sweep");

    assert!(meta_json.exists(), "metadata JSON out of scope, untouched");
    assert!(manifest.exists(), "manifest .avro out of scope, untouched");
    assert!(!control.exists(), "the in-scope orphan parquet was deleted");
    assert_eq!(summary.objects_deleted, 1, "only the parquet: {summary:?}");
}

/// A historical, end-capped-but-in-window data file (its `data_file` row still
/// present with `end_snapshot` set — not yet GC'd) is still referenced, so it
/// survives the sweep.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sweep_keeps_historical_in_window_file() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let ice = IcebergCatalog::new(pool.clone());
    let t = TableRef { schema: "wh".into(), name: "hist".into() };

    let (schema, batches) = ipc_body(4);
    let s1 = land(
        &pool, &catalog, &t, &columns(), schema, batches, small_limits(),
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "hist"), None,
    )
    .await
    .expect("land");
    let a_path = local_path(&ice.files_with_stats(&t, s1).await.expect("files@s1")[0].path);

    // Overwrite end-caps file A@s2, but A's data_file row (end_snapshot set) remains.
    overwrite_parquet_snapshot(
        &pool, &catalog, &t, &columns(), vec![batch(2)],
        Some(&lineage(RunId(uuid::Uuid::new_v4()), "wh", "hist")), &[],
    )
    .await
    .expect("overwrite");
    assert!(a_path.exists(), "end-capped file A on disk before sweep");

    let (store, root_url) = store_for(wh.path());
    let summary = sweep_orphans(&store, &root_url, &pool, ZERO_GRACE).await.expect("sweep");

    assert!(a_path.exists(), "historical in-window file survives (still referenced)");
    assert_eq!(summary.objects_deleted, 0, "nothing unreferenced: {summary:?}");
}

/// A dropped-but-in-window table's data file is still referenced (its `data_file`
/// rows survive until `gc_table` ages out the drop snapshot), so the sweep keeps it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sweep_keeps_dropped_incarnation_file() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let ice = IcebergCatalog::new(pool.clone());
    let t = TableRef { schema: "wh".into(), name: "dropped".into() };

    let (schema, batches) = ipc_body(4);
    let s1 = land(
        &pool, &catalog, &t, &columns(), schema, batches, small_limits(),
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "dropped"), None,
    )
    .await
    .expect("land");
    let path = local_path(&ice.files_with_stats(&t, s1).await.expect("files")[0].path);

    let ident = TableIdent::new(NamespaceIdent::new("wh".into()), "dropped".into());
    catalog.drop_table(&ident).await.expect("drop");
    assert!(path.exists(), "dropped table's file on disk before sweep");

    let (store, root_url) = store_for(wh.path());
    let summary = sweep_orphans(&store, &root_url, &pool, ZERO_GRACE).await.expect("sweep");

    assert!(path.exists(), "dropped-in-window file survives (data_file row still present)");
    assert_eq!(summary.objects_deleted, 0, "nothing unreferenced: {summary:?}");
}

/// A puffin sidecar referenced by a `vector_index` row survives; a stray puffin dies.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sweep_keeps_referenced_puffin() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let t = TableRef { schema: "wh".into(), name: "indexed".into() };

    let (schema, batches) = ipc_body(4);
    let s1 = land(
        &pool, &catalog, &t, &columns(), schema, batches, small_limits(),
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "indexed"), None,
    )
    .await
    .expect("land");
    let tid: i64 = sqlx::query_scalar::<_, i64>(
        "select table_id from iceberg_mirror.table \
         where table_namespace = $1 and table_name = $2 and end_snapshot is null",
    )
    .bind("wh")
    .bind("indexed")
    .fetch_one(&pool)
    .await
    .expect("tid");

    let referenced = wh.path().join("indexed.idx.puffin");
    std::fs::write(&referenced, b"puffin").expect("write referenced puffin");
    let mut conn = pool.acquire().await.expect("conn");
    insert_vector_index(
        &mut conn,
        &VectorIndexRow {
            table_id: tid,
            column: "id".into(),
            index_name: "idx".into(),
            covered_snapshot: s1.0,
            metric: "l2".into(),
            index_kind: "flat".into(),
            dim: 4,
            row_count: 4,
            puffin_path: format!("file://{}", referenced.display()),
        },
    )
    .await
    .expect("insert vector_index");
    drop(conn);

    let stray = wh.path().join("stray.puffin");
    std::fs::write(&stray, b"stray").expect("write stray");

    let (store, root_url) = store_for(wh.path());
    let summary = sweep_orphans(&store, &root_url, &pool, ZERO_GRACE).await.expect("sweep");

    assert!(referenced.exists(), "referenced puffin survives");
    assert!(!stray.exists(), "stray puffin deleted");
    assert_eq!(summary.objects_deleted, 1, "only the stray puffin: {summary:?}");
}

/// A second immediate sweep deletes nothing and errors nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sweep_is_idempotent() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let _catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;

    let orphan = wh.path().join("once.parquet");
    std::fs::write(&orphan, b"once").expect("write");

    let (store, root_url) = store_for(wh.path());
    let first = sweep_orphans(&store, &root_url, &pool, ZERO_GRACE).await.expect("first");
    assert_eq!(first.objects_deleted, 1);

    let second = sweep_orphans(&store, &root_url, &pool, ZERO_GRACE).await.expect("second");
    assert_eq!(second, SweepSummary::default(), "second sweep is a clean no-op: {second:?}");
}
```

- [ ] **Step 2: Add the fixture-test target** — `src/control-plane/postgres/BUCK` (mirror `:iceberg-gc` at `:552`, add `object_store`)

```python
loom_fixture_test(
    name = "orphan-sweep",
    crate = "orphan_sweep",
    srcs = ["tests/orphan_sweep.rs"],
    crate_root = "tests/orphan_sweep.rs",
    deps = [
        "//src/testing:seed",
        ":postgres",
        "//src/control-plane/core:core",
        "//third-party:arrow-array",
        "//third-party:arrow-schema",
        "//third-party:iceberg",
        "//third-party:object_store",
        "//third-party:serde_json",
        "//third-party:sqlx",
        "//third-party:tempfile",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```

- [ ] **Step 3: Run the test to verify it fails to compile**

Run: `buck2 build -v0 --console none //src/control-plane/postgres:orphan-sweep`
Expected: FAIL — `control_plane_postgres::orphan_sweep` unresolved.

- [ ] **Step 4: Create the primitive** — `src/control-plane/postgres/src/orphan_sweep.rs`

```rust
//! The third GC source: an orphaned-object sweep. `gc_table` reclaims only
//! MIRROR-REFERENCED dead bytes; nothing else ever asks "what object does no
//! mirror row reference?". Orphans accrue from write-then-commit crashes (every
//! landing path writes Parquet pre-tx) and commit-then-delete degradations (a
//! failed post-commit object delete is logged and left behind — the
//! "already-deferred orphaned-Parquet class" `iceberg_gc` names).
//!
//! `sweep_orphans` LISTs the warehouse, diffs the **pattern-scoped** data objects
//! (`*.parquet` + `*.puffin` only — Iceberg metadata/manifests are excluded by
//! scope, never diffed) against every mirror-referenced path, and deletes the
//! unreferenced remainder older than a write-race grace window.
//!
//! ## Safety (why this cannot delete a live file)
//! 1. **Pattern scoping** — metadata/manifest `.json`/`.avro` are structurally
//!    unreachable (never listed as candidates).
//! 2. **Reference over-approximation** — the reference set is EVERY `data_file.path`
//!    (any `end_snapshot`, live and historical-in-window) + every
//!    `vector_index.puffin_path`, across all tables incl. dropped-but-unreclaimed
//!    incarnations, so a still-retained file is always referenced.
//! 3. **LIST-before-read ordering** — a commit racing the sweep lands its
//!    reference row before the diff reads it (LIST happens first; the later
//!    reference read sees the new row).
//! 4. **Grace window** — an uncommitted in-flight file is younger than the grace.
//!
//! ## No transaction around object I/O
//! Reference reads are plain `SELECT`s; deletes go straight to the object store
//! outside any Postgres tx (the same no-store-I/O-in-a-tx rule `gc_table` follows).

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use control_plane_core::{ControlPlaneError, Result};
use futures::StreamExt;
use object_store::ObjectStore;
use sqlx::PgPool;
use time::OffsetDateTime;

use crate::backend;

/// Counts of what a `sweep_orphans` run reclaimed, for observability and tests.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweepSummary {
    pub objects_deleted: u64,
    pub bytes_deleted: u64,
    /// Unreferenced candidates younger than the grace window, held this run (they
    /// reclaim on a later sweep once aged past grace).
    pub candidates_skipped_grace: u64,
}

/// The sweep's data-pattern scope: `*.parquet` and `*.puffin` only. Everything
/// else under the warehouse — Iceberg metadata JSON, manifest lists, manifest
/// `.avro`, version hints, unknown files — is excluded by scope and can never be
/// flagged as an orphan.
fn in_scope(key: &str) -> bool {
    key.ends_with(".parquet") || key.ends_with(".puffin")
}

/// Normalize an absolute mirror path (`s3://bucket/key` or `file:///abs/key`) to
/// the store-relative key that `ObjectMeta.location` yields: strip the store's
/// `root_url` prefix, then any leading `/`.
fn to_store_key(root_url: &str, abs: &str) -> String {
    abs.strip_prefix(root_url)
        .unwrap_or(abs)
        .trim_start_matches('/')
        .to_string()
}

/// Object-store error → opaque backend error (transport/IO faults have no more
/// specific class here).
fn store_err<E: std::fmt::Display>(e: E) -> ControlPlaneError {
    ControlPlaneError::Backend(e.to_string().into())
}

/// Sweep orphaned data objects from the warehouse `store` (rooted at `root_url`).
///
/// LIST-then-read ordering + the `grace` window make concurrent writers/GC safe
/// (see module docs). Never opens a Postgres transaction around the object
/// deletes. Idempotent: a missing object deletes as success, and a failed delete
/// is retried by the next scheduled run.
pub async fn sweep_orphans(
    store: &Arc<dyn ObjectStore>,
    root_url: &str,
    pool: &PgPool,
    grace: Duration,
) -> Result<SweepSummary> {
    // 1. LIST the warehouse; keep in-scope data objects with their key/age/size.
    let mut listed: Vec<(object_store::path::Path, i64, u64)> = Vec::new();
    let mut stream = store.list(None);
    while let Some(item) = stream.next().await {
        let meta = item.map_err(store_err)?;
        if in_scope(meta.location.as_ref()) {
            // `ObjectMeta.size` is already `u64` in object_store 0.13 — no cast
            // (an `as u64` here trips `clippy::unnecessary_cast`, which is enforced).
            listed.push((
                meta.location.clone(),
                meta.last_modified.timestamp_millis(),
                meta.size,
            ));
        }
    }

    // 2. Read the reference set: ALL data_file paths + ALL puffin paths (no
    //    end_snapshot filter — historical-in-window and dropped-in-window rows
    //    included). Normalize both to store-relative keys before comparison.
    let mut referenced: HashSet<String> = HashSet::new();
    for p in sqlx::query_scalar!("select path from iceberg_mirror.data_file")
        .fetch_all(pool)
        .await
        .map_err(backend)?
    {
        referenced.insert(to_store_key(root_url, &p));
    }
    for p in sqlx::query_scalar!("select puffin_path from iceberg_mirror.vector_index")
        .fetch_all(pool)
        .await
        .map_err(backend)?
    {
        referenced.insert(to_store_key(root_url, &p));
    }

    // 3+4. Diff (listed − referenced) + grace filter, then 5. delete survivors
    //      outside any tx. Grace is applied at ms precision so a just-written file
    //      is deterministically past a zero grace.
    let now_ms: i64 = (OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64;
    let cutoff_ms = now_ms - grace.as_millis() as i64;
    let mut summary = SweepSummary::default();
    for (loc, modified_ms, size) in listed {
        let key = loc.as_ref();
        if referenced.contains(key) {
            continue; // referenced — never a candidate
        }
        if modified_ms >= cutoff_ms {
            summary.candidates_skipped_grace += 1;
            continue; // orphan, but younger than grace — hold
        }
        match store.delete(&loc).await {
            Ok(()) => {
                summary.objects_deleted += 1;
                summary.bytes_deleted += size;
                tracing::info!(
                    path = %key,
                    size,
                    age_ms = now_ms - modified_ms,
                    "orphan-sweep: deleted unreferenced object"
                );
            }
            // A concurrent sweep/GC already removed it — idempotent success.
            Err(object_store::Error::NotFound { .. }) => {}
            Err(e) => tracing::warn!(
                error = %e,
                path = %key,
                "orphan-sweep: failed to delete; leaving for next run"
            ),
        }
    }
    tracing::info!(
        objects_deleted = summary.objects_deleted,
        bytes_deleted = summary.bytes_deleted,
        candidates_skipped_grace = summary.candidates_skipped_grace,
        "orphan-sweep: complete"
    );
    Ok(summary)
}
```

> **Clippy note:** do **not** cast `meta.size` — it is already `u64` in object_store 0.13, and an `as u64` trips the enforced `clippy::unnecessary_cast` (a default complexity lint, NOT in `CLIPPY_ALLOWS`). The two `as i64` casts (`grace.as_millis() as i64`, `... / 1_000_000 as i64`) are `cast_possible_wrap`/`cast_possible_truncation`, both of which ARE globally allowed via `CLIPPY_ALLOWS` (mirroring `iceberg_gc::gc_locked`'s `retention.as_secs() as i64`). If any cast lint fires on your toolchain, add a local `#[expect(clippy::<lint>, reason = "...")]`; do **not** widen `CLIPPY_ALLOWS`.

- [ ] **Step 5: Expose the module** — `src/control-plane/postgres/src/lib.rs` (after `pub mod iceberg_gc;` at `:28`)

```rust
pub mod orphan_sweep;
```

- [ ] **Step 6: Regenerate the `.sqlx` cache**

```bash
./tools/sqlx-prepare.sh
```

Expected: two new `.sqlx/query-*.json` files appear (for the `data_file` + `vector_index` scalar selects). Stage them.

- [ ] **Step 7: Run the fixture suite to verify it passes**

Run: `buck2 test --console none //src/control-plane/postgres:orphan-sweep`
Expected: `Tests finished: Pass 7. Fail 0`.

- [ ] **Step 8: Confirm the sqlx cache freshness check still passes**

Run: `buck2 test --console none //src/control-plane/postgres:sqlx-cache-check`
Expected: Pass (the committed cache matches the live schema).

- [ ] **Step 9: Lint + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/control-plane/postgres/
git commit -m "feat(iceberg): sweep_orphans primitive — LIST/diff/grace-delete unreferenced warehouse objects"
```

---

## Task 3: Proto RPC + engine server method + grace config + wire client + harness

**Files:**
- Modify: `src/services/engine-wire/proto/engine_control.proto`
- Modify: `src/services/engine-wire/src/client.rs`
- Modify: `src/services/engine/src/service.rs`
- Modify: `src/services/engine/src/run.rs:169-184`
- Modify: `src/services/runtime/src/lib.rs` (`Config` field + `from_map`)
- Modify: `src/services/runtime/tests/config.rs`
- Modify: `src/services/runtime/tests/migrate_managed.rs` (a whole-`Config` struct literal — needs the new field)
- Modify: `src/services/runtime/tests/admin_management.rs`
- Modify: `src/testing/flight.rs` + `src/testing/BUCK`

**Interfaces:**
- Consumes: `control_plane_postgres::orphan_sweep::sweep_orphans` + `SweepSummary` (Task 2); `service_runtime::{WriteStore, build_write_store}`; `service_runtime::parse_var`.
- Produces:
  - proto: `rpc SweepOrphans (SweepOrphansRequest) returns (SweepOrphansResponse)`; `SweepOrphansRequest {}`; `SweepOrphansResponse { uint64 objects_deleted = 1; uint64 bytes_deleted = 2; uint64 candidates_skipped_grace = 3; }`.
  - `GrpcQueueClient::sweep_orphans(&self) -> Result<(u64, u64, u64)>` (`(objects_deleted, bytes_deleted, candidates_skipped_grace)`).
  - `EngineControlService` gains `pub write_store: service_runtime::WriteStore` and `pub orphan_sweep_grace: std::time::Duration`.
  - `Config.orphan_sweep_grace: Duration` (from `LOOM_ORPHAN_SWEEP_GRACE_SECS`, default 86400).
  - `EngineOpts.orphan_sweep_grace: Duration` (test harness; default 86400).

- [ ] **Step 1: Write the failing config test** — `src/services/runtime/tests/config.rs` (mirror `gc_retention_defaults_...` at `:46`)

```rust
#[test]
fn orphan_sweep_grace_defaults_to_24h_and_parses_override() {
    let mut v = full();
    v.remove("LOOM_ORPHAN_SWEEP_GRACE_SECS");
    assert_eq!(
        Config::from_map(&v).unwrap().orphan_sweep_grace,
        Duration::from_secs(24 * 3600)
    );
    v.insert("LOOM_ORPHAN_SWEEP_GRACE_SECS".into(), "60".into());
    assert_eq!(
        Config::from_map(&v).unwrap().orphan_sweep_grace,
        Duration::from_secs(60)
    );
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `buck2 test --console none //src/services/runtime:config`
Expected: FAIL — no field `orphan_sweep_grace` on `Config`.

- [ ] **Step 3: Add the `Config` field + parse** — `src/services/runtime/src/lib.rs`

Add the field to `struct Config` (after `gc_retention` at `:247`):

```rust
    /// Grace window for the orphaned-object sweep: an unreferenced warehouse
    /// object younger than this is held (guards the write-then-commit race). From
    /// `LOOM_ORPHAN_SWEEP_GRACE_SECS` (default 24h). Mirrors `gc_retention`'s
    /// `_SECS` unit convention.
    pub orphan_sweep_grace: Duration,
```

Parse it in `from_map` (after the `gc_retention` let at `:277-281`):

```rust
        let orphan_sweep_grace = Duration::from_secs(parse_var(
            vars,
            "LOOM_ORPHAN_SWEEP_GRACE_SECS",
            24 * 3600_u64,
        )?);
```

Add it to the `Config { ... }` literal (after `gc_retention,` at `:290`):

```rust
            orphan_sweep_grace,
```

- [ ] **Step 3b: Fix the whole-`Config` struct literal in the runtime tests** — `src/services/runtime/tests/migrate_managed.rs:43`

`Config` derives no `Default`, and `external_config` builds it as a full struct literal (NOT via `from_map`), so the new required field is an `E0063` here. Add it to that `Config { ... }` literal (after `gc_retention: ...,`):

```rust
        orphan_sweep_grace: Duration::from_secs(24 * 3600),
```

(The other `Config` helpers — `embedded_client_only.rs`, `build_pool_managed.rs`, `standalone/tests/composite_e2e.rs` — all go through `Config::from_map`, so they need no change.)

- [ ] **Step 4: Verify the config test passes + the runtime tests compile**

Run: `buck2 test --console none //src/services/runtime:config`
Then: `buck2 build -v0 --console none //src/services/runtime/... //src/services/standalone/...`
Expected: config tests Pass; both build clean (proving the `migrate_managed.rs` literal is fixed).

- [ ] **Step 5: Add the proto RPC + messages** — `src/services/engine-wire/proto/engine_control.proto`

Add the RPC to `service EngineControl` (after the `GcTable` line at `:11`):

```proto
  rpc SweepOrphans (SweepOrphansRequest) returns (SweepOrphansResponse);
```

Add the messages near the `GcTable` messages (after `:64`):

```proto
message SweepOrphansRequest  {}
message SweepOrphansResponse { uint64 objects_deleted = 1; uint64 bytes_deleted = 2; uint64 candidates_skipped_grace = 3; }
```

(The `:pb-gen` genrule regenerates the tonic/prost stubs from the `.proto` on the next build — no manual codegen.)

- [ ] **Step 6: Add the two new `EngineControlService` fields** — `src/services/engine/src/service.rs`

Add to `pub struct EngineControlService` (after `flush_byte_threshold` at `:79`):

```rust
    /// The warehouse object store + root URL, for the orphan sweep's LIST/delete.
    pub write_store: service_runtime::WriteStore,
    /// Grace window for the orphan sweep (from `LOOM_ORPHAN_SWEEP_GRACE_SECS`).
    pub orphan_sweep_grace: std::time::Duration,
```

- [ ] **Step 7: Implement the RPC method** — `src/services/engine/src/service.rs` (after the `gc_table` method at `:179`, inside the `impl EngineControl for EngineControlService` block)

```rust
    async fn sweep_orphans(
        &self,
        _req: Request<pb::SweepOrphansRequest>,
    ) -> std::result::Result<Response<pb::SweepOrphansResponse>, Status> {
        let summary = control_plane_postgres::orphan_sweep::sweep_orphans(
            &self.write_store.store,
            &self.write_store.root_url,
            &self.pool,
            self.orphan_sweep_grace,
        )
        .await
        .map_err(status)?;
        Ok(Response::new(pb::SweepOrphansResponse {
            objects_deleted: summary.objects_deleted,
            bytes_deleted: summary.bytes_deleted,
            candidates_skipped_grace: summary.candidates_skipped_grace,
        }))
    }
```

- [ ] **Step 8: Wire it in production `run.rs`** — `src/services/engine/src/run.rs`

Build the write store just before constructing `control` (after the `writer` at `:175`):

```rust
    let write_store = service_runtime::build_write_store(&cfg.object_store)?;
```

Add the two fields to the `EngineControlService { ... }` literal (after `flush_byte_threshold: tuning.flush_byte_threshold,` at `:183`):

```rust
        write_store,
        orphan_sweep_grace: cfg.orphan_sweep_grace,
```

- [ ] **Step 9: Add the client method** — `src/services/engine-wire/src/client.rs` (after `gc_table` at `:157`)

```rust
    /// Sweep orphaned warehouse objects (no mirror row references them and older
    /// than the engine's grace window). Returns
    /// `(objects_deleted, bytes_deleted, candidates_skipped_grace)`.
    pub async fn sweep_orphans(&self) -> Result<(u64, u64, u64)> {
        let resp = self
            .inner
            .clone()
            .sweep_orphans(pb::SweepOrphansRequest {})
            .await
            .map_err(be)?
            .into_inner();
        Ok((
            resp.objects_deleted,
            resp.bytes_deleted,
            resp.candidates_skipped_grace,
        ))
    }
```

- [ ] **Step 10: Wire the test harness** — `src/testing/flight.rs`

Add to `struct EngineOpts` (after `flush_byte_threshold` at `:50`):

```rust
    /// Grace window for the engine's orphan sweep RPC.
    pub orphan_sweep_grace: Duration,
```

Add to its `Default` impl (after `flush_byte_threshold: i64::MAX,` at `:59`):

```rust
            orphan_sweep_grace: Duration::from_secs(24 * 3600),
```

Add imports at the top of the file (near the other `use`s):

```rust
use std::collections::HashMap;
use store_config::{ObjectStoreConfig, build_write_store};
```

Inside the `opts.control.then(|| { ... })` closure (`:102`), build the write store before the `EngineControlServer::new(...)` and pass the two fields. Note this introduces a new precondition on the harness: `build_write_store` → `LocalFileSystem::new_with_prefix(path)` canonicalizes and errors if the warehouse dir is missing — fine because every `control: true` caller already passes an existing `tempfile::tempdir()` warehouse, but keep it in mind for new callers. Replace the closure body's service construction so it reads:

```rust
    let control = opts.control.then(|| {
        let cp = cp.clone();
        let writer = IcebergActionWriter::new(
            catalog.clone(),
            pool.clone(),
            opts.inline_byte_limit,
            opts.flush_byte_threshold,
        );
        let mut env = HashMap::new();
        env.insert("LOOM_WAREHOUSE_URI".to_string(), format!("file://{warehouse}"));
        let store_cfg = ObjectStoreConfig::parse_from_env(&env).expect("store config");
        let write_store = build_write_store(&store_cfg).expect("write store");
        EngineControlServer::new(EngineControlService {
            cp,
            catalog: catalog.clone(),
            pool: pool.clone(),
            retention: Duration::from_secs(7 * 24 * 3600),
            writer,
            flush_byte_threshold: opts.flush_byte_threshold,
            write_store,
            orphan_sweep_grace: opts.orphan_sweep_grace,
        })
    });
```

- [ ] **Step 11: Add the harness store-config dep** — `src/testing/BUCK`

Add to the `flight` target's `deps` list (`:29-41`):

```python
        "//src/services/store-config:store-config",
```

- [ ] **Step 12: Add the admin-schedule pin test** — `src/services/runtime/tests/admin_management.rs` (after `schedule_crud_roundtrip`, mirroring its `send`/`req_json` form)

```rust
/// A warehouse-scoped `sweep_orphans` schedule is accepted with an empty payload
/// and NO seeded table — pinning that `schedule_table_check` only gates the
/// table-scoped kinds (`gc_table`/`compact_table`) and lets `sweep_orphans` fall
/// through untouched.
#[tokio::test]
async fn sweep_orphans_schedule_accepted_without_table() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    // Deliberately no seed_table(..): a warehouse-scoped kind needs no table.

    let body = r#"{"name":"nightly-sweep","kind":"sweep_orphans",
        "payload":{},"cron":"0 4 * * *"}"#;
    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/schedules", &token, body),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "sweep_orphans schedule accepted with no table");

    let (status, listed) = send(app(cp), req_empty("GET", "/admin/schedules", &token)).await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&listed).unwrap();
    let schedules = v["schedules"].as_array().unwrap();
    assert_eq!(schedules.len(), 1, "{listed}");
    assert_eq!(schedules[0]["kind"], "sweep_orphans");
    assert_eq!(schedules[0]["payload"], serde_json::json!({}));
}
```

- [ ] **Step 13: Build the whole tree + run the touched suites**

Run: `buck2 build -v0 --console none //src/...` (proves the proto regen, both `EngineControlService` construction sites, and the client all compile together).
Then:
- `buck2 test --console none //src/services/runtime:config`
- `buck2 test --console none //src/services/runtime:admin-management` (find the exact target name with `grep -n 'admin_management\|admin-management' src/services/runtime/BUCK`)

Expected: build clean; both suites Pass.

- [ ] **Step 14: Lint + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/services/engine-wire/ src/services/engine/ src/services/runtime/ src/testing/
git commit -m "feat(engine): SweepOrphans RPC + orphan-sweep grace config + harness wiring"
```

---

## Task 4: Worker `handle_sweep_orphans` + dispatch

**Files:**
- Modify: `src/services/worker/src/handler.rs`
- Modify: `src/services/worker/src/main.rs`

**Interfaces:**
- Consumes: `control_plane_core::{OrphanSweepJob, ORPHAN_SWEEP_JOB_KIND}` (Task 1); `GrpcQueueClient::sweep_orphans` (Task 3); the existing `run_wire_job` helper.
- Produces: `pub async fn handle_sweep_orphans(engine: GrpcQueueClient, tuning: WorkerTuning, job: Job) -> Result<(), JobFailure>`.

- [ ] **Step 1: Add the handler** — `src/services/worker/src/handler.rs`

Extend the `control_plane_core` import (`:7`) to add `OrphanSweepJob`:

```rust
use control_plane_core::{BuildVectorIndexJob, FlushJob, GcJob, Job, JobFailure, OrphanSweepJob};
```

Add the handler (after `handle_gc` at `:54`):

```rust
pub async fn handle_sweep_orphans(
    engine: GrpcQueueClient,
    tuning: WorkerTuning,
    job: Job,
) -> std::result::Result<(), JobFailure> {
    run_wire_job(job, tuning, "sweep_orphans", |OrphanSweepJob {}| async move {
        engine.sweep_orphans().await.map(|_| ())
    })
    .await
}
```

- [ ] **Step 2: Wire dispatch** — `src/services/worker/src/main.rs`

Add `ORPHAN_SWEEP_JOB_KIND` to the `control_plane_core` import (`:14-17`):

```rust
use control_plane_core::{
    BUILD_VECTOR_INDEX_JOB_KIND, COMPACT_JOB_KIND, FLUSH_JOB_KIND, GC_JOB_KIND, JobFailure,
    ORPHAN_SWEEP_JOB_KIND, STREAM_CONSOLIDATE_JOB_KIND, STREAM_MV_JOB_KIND, TRANSFORM_JOB_KIND,
    TYPED_TRANSFORM_JOB_KIND,
};
```

Add the kind to the dequeue array (`:91-100`, after `STREAM_MV_JOB_KIND.to_string(),`):

```rust
                ORPHAN_SWEEP_JOB_KIND.to_string(),
```

Add the dispatch arm (`:108-128`, after the `GC_JOB_KIND` arm):

```rust
                        k if k == ORPHAN_SWEEP_JOB_KIND => {
                            worker::handler::handle_sweep_orphans(flush, worker_tuning, job).await
                        }
```

- [ ] **Step 3: Build the worker**

Run: `buck2 build -v0 --console none //src/services/worker/...`
Expected: build clean (behavioral proof lands in Task 5's e2e).

- [ ] **Step 4: Lint + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/services/worker/src/
git commit -m "feat(worker): dispatch sweep_orphans jobs over the engine wire"
```

---

## Task 5: Worker e2e — schedule fires → worker drains → orphan deleted

**Files:**
- Modify: `src/services/worker/tests/scheduled_maintenance_e2e.rs`

**Interfaces:**
- Consumes: `control_plane_core::ORPHAN_SWEEP_JOB_KIND`; `worker::handler::handle_sweep_orphans`; `loom_test_flight::EngineOpts { orphan_sweep_grace }`; the file's existing `columns`/`ipc_body`/`lineage`/`small_limits` helpers.

- [ ] **Step 1: Extend imports** — `src/services/worker/tests/scheduled_maintenance_e2e.rs`

Add `ORPHAN_SWEEP_JOB_KIND` to the `control_plane_core` import (`:17-20`) and `handle_sweep_orphans` to the `worker::handler` import (`:31`):

```rust
use control_plane_core::{
    COMPACT_JOB_KIND, Catalog, ColumnSpec, DatasetId, EventType, GC_JOB_KIND, JobSchedule,
    LineageEvent, ORPHAN_SWEEP_JOB_KIND, Queue, RunId, TableRef,
};
```
```rust
use worker::handler::{handle_gc, handle_sweep_orphans};
```

- [ ] **Step 2: Write the failing e2e leg** — append to `src/services/worker/tests/scheduled_maintenance_e2e.rs`

```rust
// ---- sweep_orphans leg -------------------------------------------------------

/// A `sweep_orphans` schedule fires exactly once at its due probe and the
/// enqueued job drains end-to-end through `handle_sweep_orphans` over the engine
/// wire: a planted orphan `.parquet` is deleted while a referenced (landed) file
/// survives. Grace is 0 (via `EngineOpts`) so the freshly-planted orphan is
/// immediately reclaimable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sweep_orphans_schedule_fires_and_worker_drains_over_the_wire() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let cp = PgControlPlane::new(pool.clone(), Duration::from_millis(5000));

    let wh = tempfile::tempdir().expect("warehouse dir");
    let wh_str = wh.path().display().to_string();
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh_str).await;

    let eng = spawn_engine_uds(
        fx,
        &db,
        &wh_str,
        EngineOpts {
            control: true,
            orphan_sweep_grace: Duration::ZERO,
            ..EngineOpts::default()
        },
    )
    .await;

    // A referenced (landed) file that MUST survive the sweep.
    let table = TableRef { schema: "main".into(), name: "kept".into() };
    let (schema, batches) = ipc_body(&[1, 2, 3]);
    land(
        &pool, &catalog, &table, &columns(), schema, batches, small_limits(),
        lineage(RunId(uuid::Uuid::new_v4()), &table), None,
    )
    .await
    .expect("land kept");
    let ice = IcebergCatalog::new(pool.clone());
    let cur = ice.current_snapshot(&table).await.expect("snapshot");
    let kept = std::path::PathBuf::from(
        ice.files_with_stats(&table, cur.id).await.expect("files")[0]
            .path
            .strip_prefix("file://")
            .expect("file:// path"),
    );
    assert!(kept.exists(), "referenced file present before sweep");

    // A planted orphan under the warehouse, referenced by NO mirror row.
    let orphan = wh.path().join("orphan-xyz.parquet");
    std::fs::write(&orphan, b"orphan-bytes").expect("write orphan");

    let now = time::OffsetDateTime::now_utc();
    cp.define_job_schedule(JobSchedule {
        name: "nightly-sweep-e2e".into(),
        kind: ORPHAN_SWEEP_JOB_KIND.into(),
        payload: serde_json::json!({}),
        cron: "0 4 * * *".into(),
    })
    .await
    .expect("define sweep schedule");

    assert!(
        cp.fire_due_job_schedules(now, 32)
            .await
            .expect("fire (not due)")
            .is_empty(),
        "a freshly defined daily schedule is not due at define-time"
    );

    let probe = now + time::Duration::days(2);
    let fired = cp.fire_due_job_schedules(probe, 32).await.expect("fire (due)");
    assert_eq!(fired.len(), 1, "exactly one schedule fires at the probe");
    assert_eq!(fired[0].name, "nightly-sweep-e2e");
    let job_id = fired[0].job.expect("due fire enqueues a job");

    let client = GrpcQueueClient::connect(&eng.sock).await.expect("connect");
    let job = client
        .dequeue(&[ORPHAN_SWEEP_JOB_KIND.to_string()], "sched-e2e")
        .await
        .expect("dequeue")
        .expect("the fired sweep_orphans job must be present");
    assert_eq!(job.id, job_id, "dequeued job matches the fired schedule's job id");
    assert_eq!(job.kind, ORPHAN_SWEEP_JOB_KIND);
    assert_eq!(job.payload, serde_json::json!({}), "empty warehouse-scoped payload");

    handle_sweep_orphans(client.clone(), loom_config::WorkerTuning::default(), job)
        .await
        .expect("handle_sweep_orphans must succeed over the wire");
    client.complete(job_id).await.expect("complete sweep job");

    assert!(!orphan.exists(), "planted orphan deleted by the scheduled sweep");
    assert!(kept.exists(), "referenced file survived the sweep");

    let again = client
        .dequeue(&[ORPHAN_SWEEP_JOB_KIND.to_string()], "sched-e2e")
        .await
        .expect("dequeue after complete");
    assert!(again.is_none(), "queue empty after the scheduled sweep completed");
}
```

- [ ] **Step 3: Run the e2e suite**

Run: `buck2 test --console none //src/services/worker:scheduled-maintenance-e2e`
Expected: `Tests finished: Pass 3. Fail 0` (the existing gc + compact legs plus the new sweep leg).

- [ ] **Step 4: Lint + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/services/worker/tests/scheduled_maintenance_e2e.rs
git commit -m "test(worker): e2e sweep_orphans schedule drains over the wire and reclaims orphans"
```

---

## Task 6: Register close + capability doc (at branch finish)

**Files:**
- Modify: `docs/ROADMAP.md` (remove the `road-iceberg-gc-orphan-sweep` entry)
- Modify: `docs/system-capabilities/` (fold the landed capability into the iceberg/GC subsystem doc)

This is done as part of `superpowers:finishing-a-development-branch` via the `loom-docs-update` skill, in the PR that lands the work — registers carry **open** work only. Name the id (`#road-iceberg-gc-orphan-sweep`) and the PR number in the PR body.

- [ ] **Step 1:** Run `loom-docs-update` to remove the ROADMAP entry and document the sweep under `docs/system-capabilities/` (its README says where the iceberg/GC capabilities live).
- [ ] **Step 2:** `buck2 run //tools:prek -- run --all-files` (markdown eof/trailing-whitespace hooks police `.md` too), then commit the doc changes.

---

## Final Verification (whole branch)

- [ ] `buck2 build -v0 --console none //src/...` — clean.
- [ ] `buck2 test --console none //src/...` — all green (in a cloud session, scope to the btd-affected targets and use `-M none`; the CI `affected` job runs the full impacted set).
- [ ] `buck2 run //tools:prek -- run --all-files` — clean (rustfmt, clippy-all, eof, reindeer-in-sync).
- [ ] **Metric gate** (part of the final review): `loom-complexity diff` and `loom-duplication diff` on the branch — report any NEW hotspot over the census thresholds (cc > 15, cognitive > 15, MI < 20, SLOC > 100) or any NEW cross-file duplication pair ≥ 20 lines as a finding, and either fix or justify it in the PR description. The new e2e leg intentionally mirrors the gc/compact legs' schedule-fire scaffold (a known, accepted duplication family in `scheduled_maintenance_e2e.rs`) — expect and justify that if flagged.

## Self-Review

**Spec coverage:**
- Job kind + plumbing (spec §"Job kind + plumbing" 1–5): Task 1 (core kind, KNOWN/SCHEDULABLE, decode arm), Task 3 (proto RPC, engine RPC, grace on config), Task 4 (worker dispatch), Task 3 Step 12 (`schedule_table_check` pinned by test — no code change, per spec item 5). ✓
- Sweep algorithm (spec §"The sweep algorithm" 1–5): Task 2 primitive — LIST+scope, reference read (no `end_snapshot` filter, all tables), diff, grace filter (ms precision), delete-outside-tx with per-deletion logging + NotFound-as-success. ✓
- Safety posture (spec 5 guards): pattern scoping (`in_scope`), reference over-approximation (unfiltered reads), LIST-before-read ordering (LIST then reference read), grace window, per-deletion logs + RPC counts. ✓ — each proven by a Task 2 fixture case.
- Grace knob `LOOM_ORPHAN_SWEEP_GRACE_SECS` default 24h on engine config: Task 3 (runtime `Config` + test). ✓
- Non-regression: no existing path changes (new kind/module); no migration; new SQL → `.sqlx` regen (Task 2 Step 6). ✓
- Testing (spec §"Testing" cases): live survive, historical-in-window survive, metadata untouchable, grace holds young, dropped-incarnation survive, schedule e2e, idempotence — Tasks 2 + 5 cover all seven. ✓
- Out-of-scope items (dry-run, HTTP enqueue, manifest walking, sharding) — none built. ✓

**Placeholder scan:** every code step carries full source; no TBD/"add error handling"/"similar to Task N". ✓

**Type consistency:** `SweepSummary { objects_deleted, bytes_deleted, candidates_skipped_grace }` is used identically in the postgres primitive (Task 2), the engine RPC mapping (Task 3 Step 7), and the proto response (Task 3 Step 5); `sweep_orphans` client returns the tuple in that field order (Task 3 Step 9), consumed as `.map(|_| ())` by the worker (Task 4). `EngineControlService` gains the same two fields at both construction sites (Task 3 Steps 8 + 10). `OrphanSweepJob {}` / `ORPHAN_SWEEP_JOB_KIND` are defined in Task 1 and consumed unchanged in Tasks 3–5. ✓
