# Worker Deployment Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the worker a deployment path on both shipped surfaces — composed in-process into the `loom` standalone binary, and as an image + Deployment in the Helm chart — so queued jobs actually drain.

**Architecture:** Slice 1 of `road-deploy-worker`, UDS-only. The worker's process composition (connect clients → build job contexts → run the dispatch loop) is **extracted into the `worker` library first**, so `worker-bin` and the standalone composite share one code path by construction rather than duplicating ~80 lines. Standalone then spawns that shared entry point as a task after the existing engine-ready gate, reusing the already-resolved `cfg.object_store`. The chart gets a worker image plus a Deployment carrying its own engine sidecar over a shared `engine-sock` emptyDir. No transport change, no `store-config` change.

**Tech Stack:** Rust 2024, buck2, tokio/tonic (UDS), sqlx, apko/Wolfi OCI images, Helm 3.

**Spec:** `docs/superpowers/specs/2026-07-16-worker-deployment-path-design.md` — read it before Task 1.

## Global Constraints

- **Tests are `rust_test` integration targets only** — never inline `#[cfg(test)] mod tests`. The `no-inline-tests` prek hook fails the build if any `src/**.rs` outside a `tests/` dir contains `#[test]`/`#[tokio::test]`.
- **Fixture tests must use `loom_fixture_test`**, not bare `rust_test`, or they run without the PG/MinIO env and fail to boot.
- **The zero-pool guard is asserted on `worker-bin`.** Never add `//src/control-plane/postgres` to `//src/services/worker:worker` or `:worker-bin`. Adding `//src/control-plane/worker:worker` (the generic loop — no postgres) to the `worker` *library* is fine and is required by Task 2. Adding the `worker` library to `standalone` is fine.
- **Clippy is strict** (`pedantic` + `restriction` with an allowlist). `unwrap_used`/`expect_used`/`panic`/`indexing_slicing` are enforced in lib/bin code. Test code is exempted via the `loom_rust_test`/`loom_fixture_test` wrappers. `too_many_lines` and `cognitive_complexity` are in `CLIPPY_ALLOWS` (`toolchains/BUCK`), so a long dispatch match is not a gate risk.
- **Every commit message must be Conventional Commits** (`conventional-commit` prek hook, commit-msg stage).
- **Markdown must end with exactly one trailing newline and no trailing whitespace.**
- Build: `buck2 build -v0 --console none //src/...`. Test: `buck2 test --console none //src/...`. Never pipe a superconsole `buck2 test` through `tail`/`head`.
- The `deploy//` cell is **off** the `//src` sweep and needs the `homelab` external cell. `buck2 test //src/...` builds nothing under `deploy/`.

---

### Task 1: `StandaloneTuning` carries the worker's job config

The standalone worker needs `WorkerTuning` (poll interval) and `WriteConfig` (write path) — together `datafusion_io::JobConfig` — plus the compaction threshold. The composite deliberately **never reads the live environment** (its own doc comment), so these are parsed once in `from_map` and handed in.

**The derive problem — the crux of this task.** `StandaloneTuning` derives `Clone, Copy, Debug, PartialEq, Eq` (`src/services/standalone/src/lib.rs:22`). `datafusion_io::WriteConfig` derives `Clone, Debug, serde::Serialize, serde::Deserialize` (`src/services/datafusion-io/src/write.rs:30`) — **not** `Copy`, **not** `PartialEq`/`Eq` (it holds an `f64`). So carrying `JobConfig` forces `StandaloneTuning` to drop `Copy`, `PartialEq`, `Eq`. And `JobConfig` derives only `Default, serde::Deserialize` (`src/services/datafusion-io/src/job_config.rs:8`), so it needs `Clone, Debug` before it can sit inside a `Clone, Debug` struct. Both its members (`WorkerTuning`, `WriteConfig`) are already `Clone + Debug`, so the derive is sound.

**Files:**
- Modify: `src/services/datafusion-io/src/job_config.rs:8`
- Modify: `src/services/standalone/src/lib.rs:22-43`, and `:86` / `:120` (the `Copy` fallout)
- Modify: `src/services/standalone/BUCK` (`standalone` library deps)
- Modify: `src/services/standalone/tests/tuning.rs`

**Interfaces:**
- Consumes: `datafusion_io::JobConfig { worker: loom_config::WorkerTuning, write: WriteConfig }`; `service_runtime::load` / `overlay_opt` (re-exported from `loom_config` at `src/services/runtime/src/lib.rs:39-47`).
- Produces: `StandaloneTuning { session_ttl, max_ttl, lockout, engine, jobs: datafusion_io::JobConfig, compact_threshold_bytes: i64 }`, deriving `Clone, Debug` **only**. Task 3 reads `tuning.jobs` and `tuning.compact_threshold_bytes`.

- [ ] **Step 1: Write the failing test**

Append to `src/services/standalone/tests/tuning.rs`:

```rust
#[test]
fn defaults_cover_worker_job_config() {
    let t = StandaloneTuning::from_map(&HashMap::new()).unwrap();
    // Mirrors `loom_config::WorkerTuning::default()` (poll_interval_ms: 5000).
    assert_eq!(t.jobs.worker.poll_interval(), Duration::from_millis(5000));
    // Mirrors `worker-bin`'s default in `src/services/worker/src/main.rs:53`.
    assert_eq!(t.compact_threshold_bytes, 128 * 1024 * 1024);
}

#[test]
fn worker_job_config_overlays_from_env() {
    let mut vars: HashMap<String, String> = HashMap::new();
    vars.insert("LOOM_WORKER_POLL_INTERVAL_MS".into(), "250".into());
    vars.insert("LOOM_COMPACT_THRESHOLD_BYTES".into(), "4096".into());
    let t = StandaloneTuning::from_map(&vars).unwrap();
    assert_eq!(t.jobs.worker.poll_interval(), Duration::from_millis(250));
    assert_eq!(t.compact_threshold_bytes, 4096);
}

#[test]
fn malformed_compact_threshold_is_startup_error() {
    let mut vars: HashMap<String, String> = HashMap::new();
    vars.insert("LOOM_COMPACT_THRESHOLD_BYTES".into(), "big".into());
    assert!(StandaloneTuning::from_map(&vars).is_err());
}
```

`LOOM_WORKER_POLL_INTERVAL_MS` is verified at `src/loom-config/src/worker.rs:61`.

- [ ] **Step 2: Run test to verify it fails**

Run: `buck2 test --console none //src/services/standalone:tuning`
Expected: FAIL — compile error, `no field 'jobs' on type 'StandaloneTuning'`.

- [ ] **Step 3: Add `Clone, Debug` to `JobConfig`**

`src/services/datafusion-io/src/job_config.rs` line 8:

```rust
#[derive(Default, Clone, Debug, serde::Deserialize)]
```

- [ ] **Step 4: Widen `StandaloneTuning`**

Replace `src/services/standalone/src/lib.rs:18-43`:

```rust
/// Env-derived tunables the composite passes to its services: the auth TTLs, the
/// engine write-path byte thresholds, and the in-process worker's job config.
/// Parsed once from the main's env snapshot (fail-loud on malformed values) and
/// handed into [`run`]; the composite itself never reads the live environment.
///
/// Not `Copy`/`Eq`: `jobs.write` is a `WriteConfig`, which holds an `f64` and is
/// `Clone`-only.
#[derive(Clone, Debug)]
pub struct StandaloneTuning {
    pub session_ttl: std::time::Duration,
    pub max_ttl: std::time::Duration,
    pub lockout: service_runtime::LockoutPolicy,
    pub engine: engine::EngineTuning,
    /// Worker loop + write-path config for the in-process worker. Composed
    /// defaults < file < env, exactly as `worker-bin` does it.
    pub jobs: datafusion_io::JobConfig,
    /// Compaction size threshold, `LOOM_COMPACT_THRESHOLD_BYTES` (default 128 MiB).
    /// Mirrors `src/services/worker/src/main.rs:53-54`.
    pub compact_threshold_bytes: i64,
}

impl StandaloneTuning {
    /// Parse from the env snapshot. Absent keys take the documented defaults
    /// (24h session TTL, 90-day token cap, 16 MiB inline / 64 MiB flush,
    /// 5s worker poll, 128 MiB compaction threshold).
    pub fn from_map(
        vars: &std::collections::HashMap<String, String>,
    ) -> Result<Self, service_runtime::ConfigError> {
        let mut compact_threshold_bytes: i64 = 128 * 1024 * 1024;
        service_runtime::overlay_opt(
            vars,
            "LOOM_COMPACT_THRESHOLD_BYTES",
            &mut compact_threshold_bytes,
        )?;
        Ok(StandaloneTuning {
            session_ttl: service_runtime::session_ttl(vars)?,
            max_ttl: service_runtime::service_token_max_ttl(vars)?,
            lockout: service_runtime::login_lockout(vars)?,
            engine: engine::EngineTuning::from_map(vars)?,
            jobs: service_runtime::load(vars)?,
            compact_threshold_bytes,
        })
    }
}
```

- [ ] **Step 5: Add the library dep**

In `src/services/standalone/BUCK`, add to the **`standalone` `rust_library`** `deps`:

```python
        "//src/services/datafusion-io:datafusion-io",
```

The list is **not** strictly sorted today (`runtime` precedes `managed-postgres` at `:12-19`) — match the surrounding grouping, don't impose a new order.

Do **not** add this dep to the `tuning` test target: the test only calls `t.jobs.worker.poll_interval()` and never names `datafusion_io::JobConfig`, so it needs no direct edge.

- [ ] **Step 6: Fix the `Copy` fallout in `serve_composite`**

`serve_composite` reads `tuning.engine` inside an `async move` at `src/services/standalone/src/lib.rs:120`. Extract it to a local **before** the engine spawn — immediately after `let max_ttl = tuning.max_ttl;` (:86):

```rust
    let engine_tuning = tuning.engine;
```

and change `:120` from `tuning.engine,` to `engine_tuning,`.

`engine::EngineTuning` derives `Clone, Copy, Debug, PartialEq, Eq` (`src/services/engine/src/run.rs:33`), so this is a copy. (Strictly, RFC-2229 precise capture would make the original compile anyway by capturing only the `Copy` field — but the explicit local is clearer and removes the dependency on capture subtleties.)

- [ ] **Step 7: Run the tests**

Run: `buck2 test --console none //src/services/standalone/...`
Expected: `Fail 0`.

**Blast radius of dropping `Copy`/`PartialEq`/`Eq`:** `StandaloneTuning::from_map` has **five** call sites — `standalone/src/main.rs`, the three `standalone/tests/*`, and **`src/ui/e2e/src/lib.rs:104`**, which this command does not cover. Each moves `tuning` exactly once into `standalone::run`, so none should break — but the Final Gate's full `//src/...` sweep is what actually proves it. (`src/ui/e2e:login` fails deterministically in cloud on a missing `libnspr4.so`, unrelated to this diff.)

- [ ] **Step 8: Commit**

```bash
git add src/services/datafusion-io/src/job_config.rs src/services/standalone/src/lib.rs src/services/standalone/BUCK src/services/standalone/tests/tuning.rs
git commit -m "feat(standalone): carry worker job config in StandaloneTuning"
```

---

### Task 2: Extract the worker's process composition into the `worker` library

**Why this task exists.** `worker-bin`'s `main` (`src/services/worker/src/main.rs:46-138`) is ~90 lines of composition: connect three clients, build three job contexts, build the `Worker`, run the dispatch loop over nine job kinds. Task 3 needs exactly that inside the standalone composite. Copying it would create an ~80-line cross-file duplicate — over the metric gate's ≥20-line threshold, and genuinely bad code. Extract **first**; then both callers are thin.

This is a pure refactor: no behaviour change, and `//src/services/worker/...`'s existing tests must stay green.

**Files:**
- Create: `src/services/worker/src/runtime.rs`
- Modify: `src/services/worker/src/lib.rs` (add `pub mod runtime;`)
- Modify: `src/services/worker/BUCK` (move two deps from the binary onto the library)
- Modify: `src/services/worker/src/main.rs` (call the new entry point)

**Interfaces:**
- Consumes: `control_plane_worker::Worker::new(q, impl Into<String>, Duration)` → `.with_poll_interval(Duration)` → `.run(&[String], CancellationToken, F)` returning `control_plane_core::Result<()>` (`src/control-plane/worker/src/lib.rs:47,57,67`). `store_config::WriteStore`. `datafusion_io::JobConfig`.
- Produces:
  ```rust
  pub struct WorkerRuntime {
      pub socket: String,
      pub worker_id: String,
      pub lease: Duration,
      pub write: Arc<store_config::WriteStore>,
      pub jobs: datafusion_io::JobConfig,
      pub compact_threshold_bytes: i64,
  }
  pub async fn run_worker(rt: WorkerRuntime, shutdown: CancellationToken)
      -> control_plane_core::Result<()>;
  ```
  Task 3 calls `worker::runtime::run_worker`.

**Note:** the ctx field types are verified — `CompactCtx { control, flight, write: Arc<WriteStore>, threshold_bytes: i64, write_cfg, worker_tuning }`, `TransformCtx { control, sql, write: Arc<WriteStore>, write_cfg, worker_tuning }`, `StreamMvCtx { control, table: FlightTableClient, worker_tuning }`. `build_write_store` returns `WriteStore` (which itself wraps `Arc<dyn ObjectStore>` + `root_url`), so the contexts want `Arc<WriteStore>`, **not** `Arc<dyn ObjectStore>`.

- [ ] **Step 1: Move the deps onto the library**

In `src/services/worker/BUCK`, add to the **`worker` `rust_library`** `deps` (they currently sit only on `worker-bin` at `:42` and `:52`):

```python
        "//src/control-plane/worker:worker",
        "//third-party:tokio-util",
```

Leave them on `worker-bin` too (it still constructs the `CancellationToken` for `ctrl_c`). Neither is postgres, so the zero-pool guard is unaffected — but do not touch the guard comment block at `:28-31`.

- [ ] **Step 2: Create the runtime module**

Create `src/services/worker/src/runtime.rs`. Move the composition out of `main.rs:46-138` verbatim — same clients, same contexts, same nine kinds, same dispatch match:

```rust
//! The worker's process composition: connect to an engine over a UDS, build the
//! job contexts, and run the dispatch loop. Shared by the `worker-bin` binary and
//! the standalone composite so the two cannot drift — the composition is the
//! contract ("which kinds does a loom worker drain?"), not a per-binary detail.

use std::sync::Arc;
use std::time::Duration;

use control_plane_core::{
    BUILD_VECTOR_INDEX_JOB_KIND, COMPACT_JOB_KIND, FLUSH_JOB_KIND, GC_JOB_KIND, JobFailure,
    ORPHAN_SWEEP_JOB_KIND, STREAM_CONSOLIDATE_JOB_KIND, STREAM_MV_JOB_KIND, TRANSFORM_JOB_KIND,
    TYPED_TRANSFORM_JOB_KIND,
};
use control_plane_worker::Worker;
use engine_wire::client::GrpcQueueClient;
use engine_wire::flight::{FlightSqlClient, FlightTableClient};
use store_config::WriteStore;
use tokio_util::sync::CancellationToken;

use crate::compact::{CompactCtx, handle_compact};
use crate::consolidate::handle_stream_consolidate;
use crate::stream_mv::{StreamMvCtx, handle_stream_mv};
use crate::transform::{TransformCtx, handle_transform, handle_typed_transform};

/// Everything the worker loop needs, independent of how the host process
/// obtained it. `worker-bin` reads these from the environment; the standalone
/// composite takes them from its already-resolved `Config`/`StandaloneTuning`.
pub struct WorkerRuntime {
    /// Path of the engine's unix-domain socket to dial.
    pub socket: String,
    /// Queue-lease identity. Must be unique per running worker.
    pub worker_id: String,
    /// Queue lock lease (`LOOM_LOCK_TIMEOUT_MS` in both hosts).
    pub lease: Duration,
    pub write: Arc<WriteStore>,
    pub jobs: datafusion_io::JobConfig,
    pub compact_threshold_bytes: i64,
}

/// Every job kind a loom worker drains. One list, so a kind added here reaches
/// both the binary and the composite.
fn job_kinds() -> Vec<String> {
    vec![
        FLUSH_JOB_KIND.to_string(),
        GC_JOB_KIND.to_string(),
        COMPACT_JOB_KIND.to_string(),
        BUILD_VECTOR_INDEX_JOB_KIND.to_string(),
        TRANSFORM_JOB_KIND.to_string(),
        TYPED_TRANSFORM_JOB_KIND.to_string(),
        STREAM_CONSOLIDATE_JOB_KIND.to_string(),
        STREAM_MV_JOB_KIND.to_string(),
        ORPHAN_SWEEP_JOB_KIND.to_string(),
    ]
}

/// Connect to the engine, build the contexts, and run the dispatch loop until
/// `shutdown` is cancelled.
pub async fn run_worker(rt: WorkerRuntime, shutdown: CancellationToken) -> control_plane_core::Result<()> {
    let control = GrpcQueueClient::connect(&rt.socket).await?;
    let flight = FlightTableClient::connect(&rt.socket).await?;
    let sql = FlightSqlClient::connect(&rt.socket).await?;

    let worker_tuning = rt.jobs.worker;
    let cctx = CompactCtx {
        control: control.clone(),
        flight: flight.clone(),
        write: rt.write.clone(),
        threshold_bytes: rt.compact_threshold_bytes,
        write_cfg: rt.jobs.write.clone(),
        worker_tuning,
    };
    let tctx = TransformCtx {
        control: control.clone(),
        sql,
        write: rt.write,
        write_cfg: rt.jobs.write.clone(),
        worker_tuning,
    };
    let mctx = StreamMvCtx {
        control: control.clone(),
        table: flight,
        worker_tuning,
    };
    let flush = control.clone();
    let worker =
        Worker::new(control, rt.worker_id, rt.lease).with_poll_interval(worker_tuning.poll_interval());

    worker
        .run(&job_kinds(), shutdown, move |job| {
            let flush = flush.clone();
            let cctx = cctx.clone();
            let tctx = tctx.clone();
            let mctx = mctx.clone();
            async move {
                match job.kind.as_str() {
                    k if k == FLUSH_JOB_KIND => {
                        crate::handler::handle_flush(flush, worker_tuning, job).await
                    }
                    k if k == GC_JOB_KIND => {
                        crate::handler::handle_gc(flush, worker_tuning, job).await
                    }
                    k if k == ORPHAN_SWEEP_JOB_KIND => {
                        crate::handler::handle_sweep_orphans(flush, worker_tuning, job).await
                    }
                    k if k == COMPACT_JOB_KIND => handle_compact(&cctx, job).await,
                    k if k == TRANSFORM_JOB_KIND => handle_transform(&tctx, job).await,
                    k if k == TYPED_TRANSFORM_JOB_KIND => handle_typed_transform(&tctx, job).await,
                    k if k == BUILD_VECTOR_INDEX_JOB_KIND => {
                        crate::handler::handle_build_vector_index(flush, worker_tuning, job).await
                    }
                    k if k == STREAM_CONSOLIDATE_JOB_KIND => {
                        handle_stream_consolidate(flush, worker_tuning, job).await
                    }
                    k if k == STREAM_MV_JOB_KIND => handle_stream_mv(&mctx, job).await,
                    other => Err(JobFailure::abandon(format!("unknown job kind: {other}"))),
                }
            }
        })
        .await
}
```

- [ ] **Step 3: Register the module**

In `src/services/worker/src/lib.rs`, add `pub mod runtime;` to the existing list (keep it alphabetical — it goes after `handler`):

```rust
pub mod compact;
pub mod consolidate;
pub mod handler;
pub mod runtime;
pub mod stream_mv;
pub mod transform;
```

- [ ] **Step 4: Rewrite `worker-bin`'s main to call it**

Replace `src/services/worker/src/main.rs` body from line 46 (`// Compose worker config …`) to the end with:

```rust
    // Compose worker config as defaults < file < env (see `JobConfig`'s `LayeredConfig`).
    let wcfg: datafusion_io::JobConfig = loom_config::load(&env)?;

    let store_cfg = store_config::ObjectStoreConfig::parse_from_env(&env)?;
    let write = Arc::new(store_config::build_write_store(&store_cfg)?);
    let mut threshold_bytes: i64 = 128 * 1024 * 1024;
    loom_config::overlay_opt(&env, "LOOM_COMPACT_THRESHOLD_BYTES", &mut threshold_bytes)?;

    let shutdown = CancellationToken::new();
    let sig = shutdown.clone();
    tokio::spawn(async move {
        drop(tokio::signal::ctrl_c().await);
        sig.cancel();
    });

    worker::runtime::run_worker(
        worker::runtime::WorkerRuntime {
            socket,
            worker_id,
            lease,
            write,
            jobs: wcfg,
            compact_threshold_bytes: threshold_bytes,
        },
        shutdown,
    )
    .await?;
    Ok(())
}
```

Delete the now-unused imports (the job-kind consts, `Worker`, the client types, the ctx types and handlers) — `unused_imports` is a hard error under this toolchain's warn set. Keep `use std::sync::Arc;`, `use std::time::Duration;`, `use tokio_util::sync::CancellationToken;`. Keep the module doc at `:1-9` accurate: it still describes exactly this binary.

- [ ] **Step 5: Verify the refactor is behaviour-neutral**

Run: `buck2 test --console none //src/services/worker/...`
Expected: `Fail 0` — every existing worker test green, unchanged.

- [ ] **Step 6: Verify the zero-pool guard still holds**

Run: `buck2 uquery "deps(//src/services/worker:worker-bin)" | grep -i "control-plane/postgres" | head`
Expected: **empty output**.

- [ ] **Step 7: Commit**

```bash
git add src/services/worker/src/runtime.rs src/services/worker/src/lib.rs src/services/worker/src/main.rs src/services/worker/BUCK
git commit -m "refactor(worker): extract the process composition into worker::runtime"
```

---

### Task 3: Standalone composes the worker in-process

The acceptance task. **This test fails on `main` today** — nothing drains a queued job in any composed deployment.

**Why extend `composite_e2e.rs` rather than add a new test file:** the drain assertion needs a **landed table** *and* a booted composite with a direct control-plane pool, and `composite_e2e.rs` is the only test with both. There is no shared `embedded_config` helper (query-api's `e2e_support` exports seeding/HTTP helpers only; `composite_error_path.rs:22-44` carries its own divergent near-copy). The accepted cost: `composite_e2e` becomes a four-concern test, so its module doc must say so.

**Files:**
- Modify: `src/services/standalone/src/lib.rs` (spawn the worker task in `serve_composite`)
- Modify: `src/services/standalone/BUCK`
- Test: `src/services/standalone/tests/composite_e2e.rs`

**Interfaces:**
- Consumes: `worker::runtime::{WorkerRuntime, run_worker}` from Task 2; `StandaloneTuning { jobs, compact_threshold_bytes }` from Task 1; `service_runtime::build_write_store(&cfg.object_store)`; `cfg.object_store` (`src/services/runtime/src/lib.rs:242`) and `cfg.lock_timeout` (`:243`).
- Produces: a `"worker"` entry in the composite's `JoinSet`.

- [ ] **Step 1: Write the failing test**

In `src/services/standalone/tests/composite_e2e.rs`:

Add the import:

```rust
use control_plane_core::{NewJob, Queue};
```

Keep a pool handle for raw SQL — the current line at `:118-121` moves `pool` into `control_plane`:

```rust
    let pool = service_runtime::build_pool(&cfg_direct.db)
        .await
        .expect("direct pool");
    let cp = service_runtime::control_plane(pool.clone(), cfg_direct.lock_timeout);
```

Insert after step (3)'s `assert_eq!(objects.len(), 2, ...)` and before step (4)'s shutdown:

```rust
    // (3.5) The composite runs a worker in-process, so a queued job drains.
    //       Only `Queue::complete` deletes the row (postgres/src/queue.rs:108);
    //       Retry leaves state='available' and Abandon leaves state='failed'
    //       (:120-140), so "row gone" means the handler returned Ok — an
    //       abandoned or retry-looping job fails this test rather than passing it.
    let job = cp
        .enqueue(NewJob {
            kind: control_plane_core::FLUSH_JOB_KIND.to_string(),
            payload: serde_json::json!({ "schema": "main", "name": "widget" }),
            run_at: None,
            priority: 0,
        })
        .await
        .expect("enqueue flush_table job");

    let drained = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let n: i64 = sqlx::query_scalar("select count(*) from queue.jobs where id = $1")
                .bind(job.0)
                .fetch_one(&pool)
                .await
                .expect("count queued job");
            if n == 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await;
    if drained.is_err() {
        let row: Option<(String, i32, Option<String>)> =
            sqlx::query_as("select state, attempts, last_error from queue.jobs where id = $1")
                .bind(job.0)
                .fetch_optional(&pool)
                .await
                .expect("read job state");
        panic!(
            "the flush_table job was never drained in 60s; (state, attempts, last_error) = {row:?} \
             — state='available' with attempts=0 means nothing dequeued it (no worker composed); \
             state='failed' means a worker ran it and abandoned it"
        );
    }
```

Update the module doc at `:1-3` to say the composite also drains queued jobs.

**Verified facts behind this test — do not re-litigate them:**
- `JobId(pub Uuid)` (`core/src/queue.rs:15`), so `job.0` is a `Uuid`; sqlx has the `uuid` feature, so `.bind(job.0)` works and the test needs no `//third-party:uuid` edge (it never names the type).
- Columns exist: `state text not null`, `attempts int not null default 0`, `last_error text` (`migrations/0001_queue.sql:3-17`).
- **The flush is a no-op, and that is fine.** `POST /datasets/{schema}/{table}` goes through the landing materializer (Arrow → Iceberg schema → **Parquet** → snapshot commit) — it never writes the inline PG tier, so `main.widget` has a snapshot and **zero live inline rows**. `flush_locked` maps that to the empty case (`Err(ControlPlaneError::NotFound(_)) => return Ok(None)`, `iceberg_flush.rs:89-93`) and the RPC passes the `Option` through (`engine/src/service.rs:379-381`), so the handler returns `Ok` and the job completes. A no-op flush still proves the whole loop: dequeue → dispatch → complete → row deleted. That is exactly the hole.
- **Nothing competes.** `inline_append` auto-enqueues a flush only above `flush_byte_threshold`, defaulted to 64 MiB (`ingest/src/config.rs:65`; trigger at `iceberg_inline.rs:772-802`). A two-row batch is nowhere near it, so there is no pre-existing undrained job on `main` and no racing job afterwards. `enqueue` is a plain insert with no dedup.

- [ ] **Step 2: Add the test-target deps and watch it fail correctly**

In `src/services/standalone/BUCK`, add to the **`composite-e2e`** `loom_fixture_test` `deps`:

```python
        "//src/control-plane/core:core",
        "//third-party:sqlx",
```

Run: `buck2 test --console none //src/services/standalone:composite-e2e`
Expected: FAIL with the panic `the flush_table job was never drained in 60s; (state, attempts, last_error) = Some(("available", 0, None))`.

**This exact failure is the point of the task.** `state='available', attempts=0` proves nothing ever dequeued it — i.e. no worker exists. If you instead see an enqueue error or a compile error, fix that first; if you see `state='failed'`, stop and diagnose with `superpowers:systematic-debugging` — that would mean a handler is erroring, not that a worker is missing.

- [ ] **Step 3: Add the library deps**

In `src/services/standalone/BUCK`, add to the **`standalone` `rust_library`** `deps`:

```python
        "//src/services/worker:worker",
        "//third-party:tokio-util",
        "//third-party:uuid",
```

Note what is **not** needed: `engine-wire` and `control-plane/worker` are internal to `worker::runtime::run_worker` after Task 2, so standalone never names them.

- [ ] **Step 4: Spawn the worker in `serve_composite`**

In `src/services/standalone/src/lib.rs`, add:

```rust
use tokio_util::sync::CancellationToken;
```

Insert this **after** the engine-ready gate (after the `if eng_ready_rx.await.is_err() { … }` block ending at `:136`) and **before** the ingest/query-api listener binds at `:138-140`. The engine must be serving before the worker dials it.

```rust
    // Worker: dials the engine we just brought up, over the same UDS query-api uses.
    // The write store comes from `cfg.object_store` — already resolved by
    // `Config::from_map` with its LOOM_DATA_PATH fallback — rather than
    // `ObjectStoreConfig::parse_from_env`, which requires LOOM_WAREHOUSE_URI. The
    // deployed `loom` binary's embedded mode sets only LOOM_DATA_PATH, so a
    // re-parse here would fail on exactly the default single-binary configuration.
    let worker_rt = worker::runtime::WorkerRuntime {
        socket: addrs.engine_socket.clone(),
        // A fresh id per process: the composite never reads the live environment,
        // so LOOM_WORKER_ID is deliberately not consulted.
        worker_id: uuid::Uuid::new_v4().to_string(),
        lease: cfg.lock_timeout,
        write: Arc::new(service_runtime::build_write_store(&cfg.object_store)?),
        jobs: tuning.jobs.clone(),
        compact_threshold_bytes: tuning.compact_threshold_bytes,
    };

    // Bridge the composite's watch-channel shutdown to the worker's CancellationToken.
    let worker_cancel = CancellationToken::new();
    let cancel_src = worker_cancel.clone();
    let worker_sd = sub(sd_rx.clone());
    tokio::spawn(async move {
        worker_sd.await;
        cancel_src.cancel();
    });

    tasks.spawn(async move {
        (
            "worker",
            worker::runtime::run_worker(worker_rt, worker_cancel)
                .await
                .map_err(Into::into),
        )
    });
```

**Verified:** `sub` is `|rx: Receiver<bool>| async move {…}` and captures no upvalues, so the returned future is `Send + 'static` — `lib.rs:112` already does exactly this shape. The insertion point is before `:167`'s `sub(sd_rx)` moves the receiver, so `sd_rx.clone()` is still available. `run_worker` returns `control_plane_core::Result<()>`, whose error is `ControlPlaneError: Error + Send + Sync + 'static`, so the blanket `From` makes `.map_err(Into::into)` into `BoxErr` valid.

Update the three now-inaccurate comments: the crate doc (`:1-2`, "engine … + ingest … + query-api … as tasks"), `serve_composite`'s doc (`:65-67`, "Run the three services"), and the `JoinSet` comment (`:101-103`, "All three serve loops").

- [ ] **Step 5: Run the test to verify it passes**

Run: `buck2 test --console none //src/services/standalone:composite-e2e`
Expected: PASS.

If it still times out, do **not** raise the timeout — the panic message now tells you which failure it is. Use `superpowers:systematic-debugging`.

- [ ] **Step 6: Run the standalone + worker suites**

Run: `buck2 test --console none //src/services/standalone/... //src/services/worker/...`
Expected: `Fail 0`.

- [ ] **Step 7: Commit**

```bash
git add src/services/standalone/src/lib.rs src/services/standalone/BUCK src/services/standalone/tests/composite_e2e.rs
git commit -m "feat(standalone): drain queued jobs by composing the worker in-process"
```

---

### Task 4: Worker OCI image

**Files:**
- Create: `deploy/images/worker/apko.yaml`
- Create: `deploy/images/worker/apko.lock.json`
- Create: `deploy/images/worker/BUCK`

**Interfaces:**
- Consumes: `root//src/services/worker:worker-bin` (`visibility = ["PUBLIC"]`, `worker/BUCK:26`).
- Produces: `deploy//images/worker:image` (+ `:image.info`, `:image.push`), repository `ghcr.io/weave-hand/loom-worker`. Task 5 uses the repository string; Task 6 uses `:image.info` and `:image.push`.

`worker-bin` is a glibc, dynamically-linked Rust binary with the same DataFusion/Arrow/Flight stack as the engine and no embedded C++, so the engine's package set applies unchanged.

- [ ] **Step 1: Create the apko base config**

Create `deploy/images/worker/apko.yaml`:

```yaml
# apko/Wolfi base for the loom worker image. Like the engine it's a glibc
# (x86_64-unknown-linux-gnu, dynamically linked) Rust binary — its
# DataFusion/Arrow/Flight stack is pure Rust, no embedded C++ — so the base ships
# only glibc + libgcc + CA certs (outbound TLS to the object store), identical to
# the engine base. The worker binary is layered on at /usr/local/bin/worker by
# oci_image; the entrypoint points there so no separate mutate step is needed.
# Runs as a non-root user. Refresh the lockfile with `apko lock apko.yaml` when
# this changes.
contents:
  repositories:
    - https://packages.wolfi.dev/os
  keyring:
    - https://packages.wolfi.dev/os/wolfi-signing.rsa.pub
  packages:
    - ca-certificates-bundle
    - glibc
    - libgcc
accounts:
  users:
    - username: nonroot
      uid: 65532
      gid: 65532
  groups:
    - groupname: nonroot
      gid: 65532
  run-as: 65532
entrypoint:
  command: /usr/local/bin/worker
archs:
  - x86_64
```

- [ ] **Step 2: Create the lockfile**

Run `apko lock deploy/images/worker/apko.yaml` if `apko` is on PATH. If not, copy `deploy/images/engine/apko.lock.json` — the package set above is byte-identical to the engine's (`ca-certificates-bundle`, `glibc`, `libgcc`), so the resolved lock is the same. Verify the copied lock's `contents.packages` matches the engine's; if the engine's lock has drifted from its own `apko.yaml`, regenerate instead of copying.

- [ ] **Step 3: Create the BUCK file**

Create `deploy/images/worker/BUCK`:

```python
# loom worker service image: the buck2-built `worker-bin` (x86_64 glibc, pure
# Rust, no embedded C++) layered onto an apko/Wolfi base — identical package set to the
# engine base. Kept under //deploy (not //src) so it stays off the normal CI
# build sweep — apko fetches packages over the network and builds local-only; the
# release workflow builds/pushes it explicitly. The worker runs as its own
# Deployment alongside an engine sidecar, draining queue jobs over the shared
# LOOM_ENGINE_SOCKET unix-domain socket.
load("@homelab//buck2/apko:defs.bzl", "apko_image")
load("@homelab//buck2/oci:defs.bzl", "oci_image", "tar_layer")

# Wolfi base (glibc + libgcc + CA certs, nonroot, entrypoint = /usr/local/bin/worker).
apko_image(
    name = "base",
    config = "apko.yaml",
    lock = "apko.lock.json",
)

# Stage the worker binary at the base's entrypoint path.
tar_layer(
    name = "bin_layer",
    binary = "root//src/services/worker:worker-bin",
    path = "/usr/local/bin/worker",
)

# Deployable image + `.info` (digest pin) + `.push` (crane push to ghcr).
oci_image(
    name = "image",
    base = ":base",
    layers = [":bin_layer"],
    repository = "ghcr.io/weave-hand/loom-worker",
)
```

- [ ] **Step 4: Verify the target graph resolves**

Run: `buck2 uquery 'deploy//images/worker:image'`
Expected: prints `deploy//images/worker:image`.

Do **not** `buck2 build` it unless the `homelab` cell is available and you have network — apko fetches packages and builds local-only. A resolving uquery is the gate.

- [ ] **Step 5: Commit**

```bash
git add deploy/images/worker/
git commit -m "feat(deploy): add the loom-worker OCI image"
```

---

### Task 5: Chart worker Deployment

**Files:**
- Create: `deploy/chart/chart/templates/worker.yaml`
- Modify: `deploy/chart/chart/templates/_helpers.tpl`
- Modify: `deploy/chart/chart/values.yaml`
- Test: `deploy/chart/tests/render_assertions.sh`

**Interfaces:**
- Consumes: `loom.fullname`, `loom.labels`, `loom.selectorLabels`, `loom.serviceAccountName`, `loom.image`, `loom.dbEnv`, `loom.objectStoreEnv`, `loom.migrateOnBootEnv`; `.Values.engine.{image,port,socketDir,socketPath,resources}`; `.Values.objectStore.{mountPath,s3.enabled}`.
- Produces: `loom.workerWarehouseEnv`; `.Values.worker.*`.

**The load-bearing detail.** `loom.objectStoreEnv` emits `LOOM_WAREHOUSE_URI` **only when `objectStore.s3.enabled`**; the default file:// path emits `LOOM_DATA_PATH` alone. `worker-bin` calls `store_config::ObjectStoreConfig::parse_from_env` eagerly (`worker/src/main.rs:49`), which **requires** `LOOM_WAREHOUSE_URI` and returns `Missing` without it (`store-config/src/lib.rs:153-156`). A worker wired to the bare helper **crash-loops on a default install**. The new helper closes that and must **not** change `loom.objectStoreEnv` — the three existing services' rendered env must stay byte-identical.

- [ ] **Step 1: Write the failing assertions**

Append to `deploy/chart/tests/render_assertions.sh`, matching its `echo "== … =="` idiom. Container names sit at 8 spaces of indent and `volumeMounts` entries at 12, so the container-scoping awk below is unambiguous.

```bash
echo "== worker: Deployment renders with worker + engine containers =="
OUT="$(helm template loom "$CHART")"
worker_deploy() { echo "$1" | awk '/^kind: Deployment$/{d=1} d && /component: worker/{p=1} p; /^---$/{if(p)exit}'; }
worker_container() { worker_deploy "$1" | awk '/^        - name: worker$/{f=1;next} f && /^        - name: /{exit} f'; }
worker_deploy "$OUT" | has 'name: worker'
worker_deploy "$OUT" | has 'name: engine'

echo "== worker: LOOM_WAREHOUSE_URI is set in BOTH warehouse modes =="
# Default (file:// PVC): objectStoreEnv omits LOOM_WAREHOUSE_URI, so the worker
# needs it injected or worker-bin's eager parse_from_env crash-loops.
worker_container "$OUT" | has 'LOOM_WAREHOUSE_URI'
worker_container "$OUT" | has 'file:///var/lib/loom/data'
S3OUT="$(helm template loom "$CHART" \
  --set objectStore.s3.enabled=true \
  --set objectStore.s3.bucket=b \
  --set objectStore.s3.credentialsSecret=s)"
worker_container "$S3OUT" | has 's3://b'

echo "== worker: container carries NO DB credentials (zero-pool) =="
# Only the engine sidecar talks to PG.
worker_container "$OUT" | hasnt 'LOOM_DB_'

echo "== worker: no probes (binary-only image, no port to probe) =="
worker_container "$OUT" | hasnt 'Probe'

echo "== worker: data mount + podAffinity only when !s3 =="
worker_deploy "$OUT" | has 'persistentVolumeClaim'
worker_deploy "$OUT" | has 'podAffinity'
worker_deploy "$S3OUT" | hasnt 'persistentVolumeClaim'
worker_deploy "$S3OUT" | hasnt 'podAffinity'

echo "== the new helper did NOT change the three existing services' env =="
# The spec's hard constraint: loom.workerWarehouseEnv is worker-only, so
# ingest/query-api/engine must NOT gain LOOM_WAREHOUSE_URI in the non-S3 render.
qapi_deploy "$OUT" | hasnt 'LOOM_WAREHOUSE_URI'
echo "$OUT" | awk '/^kind: Deployment$/{d=1} d && /component: ingest/{p=1} p; /^---$/{if(p)exit}' \
  | hasnt 'LOOM_WAREHOUSE_URI'
```

`qapi_deploy` is already defined near the top of the file — reuse it, don't redefine it.

- [ ] **Step 2: Run the assertions to verify they fail**

Run: `bash deploy/chart/tests/render_assertions.sh`
Expected: FAIL at `ASSERT FAIL: expected to find: name: worker`.

Requires `helm` 3.x on PATH. **If `helm` is unavailable here, say so plainly in the PR** rather than claiming the assertions passed.

- [ ] **Step 3: Add the warehouse helper**

Append to `deploy/chart/chart/templates/_helpers.tpl`:

```
{{/*
Worker-only warehouse env. `loom.objectStoreEnv` emits LOOM_WAREHOUSE_URI only in
the S3 case; the zero-pool worker's `ObjectStoreConfig::parse_from_env` REQUIRES it
(it has no Config/data_path fallback — see src/services/store-config/src/lib.rs),
so a file://-warehouse worker must be handed it explicitly or it crash-loops at
startup. Emitted only when S3 is off — with S3 on, objectStoreEnv already set it.
Deliberately a separate helper: folding this into loom.objectStoreEnv would change
the rendered env of ingest/query-api/engine.
*/}}
{{- define "loom.workerWarehouseEnv" -}}
{{- if not .Values.objectStore.s3.enabled }}
- name: LOOM_WAREHOUSE_URI
  value: {{ printf "file://%s" .Values.objectStore.mountPath | quote }}
{{- end }}
{{- end -}}
```

- [ ] **Step 4: Add the values block**

In `deploy/chart/chart/values.yaml`, insert after the `queryApi:` block and before the `# Engine: runs as a SIDECAR …` comment:

```yaml
# Worker: drains the queue (flush, GC, compaction, orphan sweep, transforms,
# stream consolidate, micro-batch MVs). Runs as its OWN Deployment with its own
# engine sidecar — the worker is a zero-pool wire client and reaches the engine
# only over a pod-local unix socket, so it must be co-located with one. Its
# replicas are therefore independent of queryApi.replicas. Without this workload
# nothing drains the queue and jobs accumulate forever.
worker:
  image:
    repository: ghcr.io/weave-hand/loom-worker
    tag: latest
    digest: ""
    pullPolicy: IfNotPresent
  replicas: 1
  resources: {}
  nodeSelector: {}
  tolerations: []
  # Like query-api, the worker is co-scheduled onto ingest's node by default so it
  # can mount the shared ReadWriteOnce object-store PVC. Setting this overrides
  # that default (use with RWX or S3).
  affinity: {}
```

- [ ] **Step 5: Create the worker template**

Create `deploy/chart/chart/templates/worker.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "loom.fullname" . }}-worker
  labels:
    {{- include "loom.labels" . | nindent 4 }}
    app.kubernetes.io/component: worker
spec:
  replicas: {{ .Values.worker.replicas }}
  selector:
    matchLabels:
      {{- include "loom.selectorLabels" . | nindent 6 }}
      app.kubernetes.io/component: worker
  template:
    metadata:
      labels:
        {{- include "loom.labels" . | nindent 8 }}
        app.kubernetes.io/component: worker
    spec:
      serviceAccountName: {{ include "loom.serviceAccountName" . }}
      securityContext:
        {{- toYaml .Values.podSecurityContext | nindent 8 }}
      containers:
        # The worker is a zero-pool wire client: it drains the queue over the
        # engine sidecar's control + Flight services on the shared unix socket,
        # and deliberately carries NO database credentials (the engine owns PG).
        - name: worker
          image: {{ include "loom.image" .Values.worker.image | quote }}
          imagePullPolicy: {{ .Values.worker.image.pullPolicy | default "IfNotPresent" }}
          # CWD on the writable /tmp emptyDir (read-only rootfs otherwise).
          workingDir: /tmp
          securityContext:
            {{- toYaml .Values.containerSecurityContext | nindent 12 }}
          # No probes: the image is binary-only (no shell, no coreutils) so an exec
          # probe has nothing to run (#356), and the worker exposes no port to
          # tcpSocket-probe. It fails loud and restarts if the socket is absent.
          env:
            - name: LOOM_ENGINE_SOCKET
              value: {{ .Values.engine.socketPath | quote }}
            # Stable per-pod queue-lease identity; the binary otherwise mints a
            # fresh uuid on every restart.
            - name: LOOM_WORKER_ID
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            {{- include "loom.objectStoreEnv" . | nindent 12 }}
            {{- include "loom.workerWarehouseEnv" . | nindent 12 }}
            - name: LOOM_LOCK_TIMEOUT_MS
              value: {{ .Values.lockTimeoutMs | quote }}
            - name: HOME
              value: /tmp
            - name: TMPDIR
              value: /tmp
          volumeMounts:
            {{- if not .Values.objectStore.s3.enabled }}
            - name: data
              mountPath: {{ .Values.objectStore.mountPath }}
            {{- end }}
            - name: tmp
              mountPath: /tmp
            - name: engine-sock
              mountPath: {{ .Values.engine.socketDir | quote }}
          {{- with .Values.worker.resources }}
          resources:
            {{- toYaml . | nindent 12 }}
          {{- end }}
        # Engine sidecar: same role as in the query-api pod — serves the control
        # plane + Flight over the shared UDS and owns the Postgres connection.
        - name: engine
          image: {{ include "loom.image" .Values.engine.image | quote }}
          imagePullPolicy: {{ .Values.engine.image.pullPolicy | default "IfNotPresent" }}
          workingDir: /tmp
          securityContext:
            {{- toYaml .Values.containerSecurityContext | nindent 12 }}
          env:
            # The engine serves on the unix socket, not TCP, but service_runtime's
            # Config::from_env still parses LOOM_BIND_ADDR — give it a placeholder.
            - name: LOOM_BIND_ADDR
              value: "127.0.0.1:{{ .Values.engine.port }}"
            {{- include "loom.objectStoreEnv" . | nindent 12 }}
            - name: LOOM_LOCK_TIMEOUT_MS
              value: {{ .Values.lockTimeoutMs | quote }}
            - name: HOME
              value: /tmp
            - name: TMPDIR
              value: /tmp
            - name: LOOM_ENGINE_SOCKET
              value: {{ .Values.engine.socketPath | quote }}
            {{- include "loom.dbEnv" . | nindent 12 }}
            {{- include "loom.migrateOnBootEnv" . | nindent 12 }}
          volumeMounts:
            {{- if not .Values.objectStore.s3.enabled }}
            - name: data
              mountPath: {{ .Values.objectStore.mountPath }}
            {{- end }}
            - name: tmp
              mountPath: /tmp
            - name: engine-sock
              mountPath: {{ .Values.engine.socketDir | quote }}
          {{- with .Values.engine.resources }}
          resources:
            {{- toYaml . | nindent 12 }}
          {{- end }}
      volumes:
        {{- if not .Values.objectStore.s3.enabled }}
        - name: data
          persistentVolumeClaim:
            claimName: {{ include "loom.fullname" . }}-data
        {{- end }}
        - name: tmp
          emptyDir: {}
        # Shared socket dir for the worker ⇄ engine-sidecar UDS.
        - name: engine-sock
          emptyDir: {}
      {{- with .Values.worker.nodeSelector }}
      nodeSelector:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- if .Values.worker.affinity }}
      affinity:
        {{- toYaml .Values.worker.affinity | nindent 8 }}
      {{- else if not .Values.objectStore.s3.enabled }}
      # Default: co-schedule onto the same node as ingest so both pods can mount
      # the shared ReadWriteOnce object-store PVC. Override worker.affinity (and
      # use an RWX storageClassName) to spread across nodes. Skipped under S3 —
      # each pod reaches the warehouse over egress, so no co-scheduling is needed.
      affinity:
        podAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            - labelSelector:
                matchLabels:
                  {{- include "loom.selectorLabels" . | nindent 18 }}
                  app.kubernetes.io/component: ingest
              topologyKey: kubernetes.io/hostname
      {{- end }}
      {{- with .Values.worker.tolerations }}
      tolerations:
        {{- toYaml . | nindent 8 }}
      {{- end }}
```

**No Service.** `query-api.yaml` continues past this point with a `---` and a `ClusterIP` Service; the worker exposes no port and needs none. End the file here.

**NetworkPolicy needs no change** — every policy selects on `loom.selectorLabels`, which this pod carries.

- [ ] **Step 6: Run the assertions to verify they pass**

Run: `bash deploy/chart/tests/render_assertions.sh`
Expected: all assertions pass, including the pre-existing ones (`helm lint`, migrations modes, query-api affinity, allow-s3).

- [ ] **Step 7: Commit**

```bash
git add deploy/chart/chart/templates/worker.yaml deploy/chart/chart/templates/_helpers.tpl deploy/chart/chart/values.yaml deploy/chart/tests/render_assertions.sh
git commit -m "feat(deploy): run the worker as its own Deployment in the chart"
```

---

### Task 6: Release wiring and digest pin

Without this the chart references an image that is never built or pushed.

**Files:**
- Modify: `.github/workflows/release.yml`
- Modify: `deploy/chart/BUCK`

- [ ] **Step 1: Add the image env**

In `.github/workflows/release.yml`, after the `ENGINE_IMAGE` line (~42):

```yaml
  WORKER_IMAGE: ghcr.io/weave-hand/loom-worker
```

- [ ] **Step 2: Add the push lines**

Search for `images/engine:image.push` — there are **three** hits (~88, ~163, ~215). Add a worker line beside each, matching the surrounding alignment:

```bash
          buck2 run deploy//images/worker:image.push    -- "sha-$short"
```
```bash
          buck2 run deploy//images/worker:image.push    -- "$V" latest
```
```bash
          buck2 run deploy//images/worker:image.push    -- "$dev" "$moving"
```

**Read the file** — those line numbers are from `main` at time of writing and may have drifted. If any block also references images elsewhere (a digest-collection step, a summary), add the worker there too.

- [ ] **Step 3: Add the digest pin**

In `deploy/chart/BUCK`, add to `helm_chart`'s `images` map:

```python
        "worker.image": "//images/worker:image.info",
```

Update the rule's header comment — it says "digest-pinning the two service images into values.yaml (ingest.image / queryApi.image)", already stale at three and now wrong at four. Make it name the four.

- [ ] **Step 4: Verify the chart target resolves**

Run: `buck2 uquery 'deploy//chart:chart'`
Expected: prints `deploy//chart:chart`.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/release.yml deploy/chart/BUCK
git commit -m "build(deploy): publish and digest-pin the loom-worker image"
```

---

### Task 7: Close the register item

**Files:**
- Modify: `docs/ROADMAP.md`, `docs/FUTURE.md`, `docs/system-capabilities/`

- [ ] **Step 1: Run the docs-update skill**

Use the `loom-docs-update` skill.

- [ ] **Step 2: Remove the ROADMAP entry**

Registers carry **open work only** — the closing PR removes the entry; git history is the record. Delete the `road-deploy-worker` bullet. If `## deploy` is left empty, keep the empty heading (ROADMAP already keeps empty sections, e.g. `## catalog`).

- [ ] **Step 3: Update `fut-worker-lazy-compact-ctx`**

It stays **open** — lazy `CompactCtx` is still wanted for a flush-only worker. Update its prose: the manifest now exists and sets `LOOM_WAREHOUSE_URI`, discharging the "when the worker Helm manifest is authored" conditional; what remains is the lazy-init idea itself.

- [ ] **Step 4: Record the capability**

Add the worker deployment path under `docs/system-capabilities/` (read its `README.md` for the per-subsystem convention first).

- [ ] **Step 5: Validate**

Run: `bash tools/docs.sh validate`
Expected: `docs.sh validate: OK (3 files)`

- [ ] **Step 6: Commit**

```bash
git add docs/
git commit -m "docs(deploy): close road-deploy-worker; record the worker deployment path"
```

---

## Final gate — before opening the PR

- [ ] **Full sweep:** `buck2 test --console none //src/...` → `Fail 0`. Not a scoped subset — Task 1 changed a shared type (`JobConfig`) and a public one (`StandaloneTuning`, five call sites incl. `src/ui/e2e/src/lib.rs:104`), and Task 2 moved code between the worker library and binary.
- [ ] **Zero-pool guard:** `buck2 uquery "deps(//src/services/worker:worker-bin)" | grep -i "control-plane/postgres"` → empty.
- [ ] **Metric gate (a FIX step, not a reporting step):** run `loom-complexity diff` and `loom-duplication diff` against the **merge-base** (`git merge-base HEAD origin/main`), not the committed register. Fast-forward local `main` to `origin/main` first, or the diff base sweeps unrelated files. `loom-complexity diff` needs `-p` repeated per changed path. If the branch worsened any hotspot on any axis, or introduced any cross-file duplication pair ≥ 20 lines, **fix it in this PR** — put before/after numbers in the PR body.
  - Task 2 exists precisely to pre-empt the predictable finding here (`serve_composite`'s worker block vs `worker-bin`'s main, ~80 near-identical lines). If duplication still flags that pair, the extraction is incomplete — finish it rather than reporting it.
- [ ] **Lint:** `buck2 run //tools:prek -- run --all-files` and commit anything the hooks rewrite. `--no-verify` skips rustfmt and CI lint will fail.
