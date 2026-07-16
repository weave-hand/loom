# Worker Deployment Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the worker a deployment path on both shipped surfaces — composed in-process into the `loom` standalone binary, and as an image + Deployment in the Helm chart — so queued jobs actually drain.

**Architecture:** Slice 1 of `road-deploy-worker`, UDS-only. Standalone spawns the `worker` library as a task in `serve_composite` after the existing engine-ready gate, dialing the internal engine socket and reusing the already-resolved `cfg.object_store`. The chart gets a worker image plus a Deployment carrying its own engine sidecar over a shared `engine-sock` emptyDir. No transport change, no `store-config` change.

**Tech Stack:** Rust 2024, buck2, tokio/tonic (UDS), sqlx, apko/Wolfi OCI images, Helm 3.

**Spec:** `docs/superpowers/specs/2026-07-16-worker-deployment-path-design.md` — read it before Task 1.

## Global Constraints

- **Tests are `rust_test` integration targets only** — never inline `#[cfg(test)] mod tests`. The `no-inline-tests` prek hook fails the build if any `src/**.rs` outside a `tests/` dir contains `#[test]`/`#[tokio::test]`.
- **Fixture tests must use `loom_fixture_test`**, not bare `rust_test`, or they run without the PG/MinIO env and fail to boot.
- **The zero-pool guard is asserted on `worker-bin`, not the `worker` library.** Do not add `//src/control-plane/postgres` to `//src/services/worker:worker-bin`'s deps. Adding the `worker` *library* to `standalone` is fine — the library has no postgres dep.
- **Clippy is strict** (`pedantic` + `restriction` with an allowlist). `unwrap_used`/`expect_used`/`panic`/`indexing_slicing` are enforced in lib/bin code. Test code is exempted via the `loom_rust_test`/`loom_fixture_test` wrappers.
- **Every commit message must be Conventional Commits** (`conventional-commit` prek hook, commit-msg stage).
- **Markdown must end with exactly one trailing newline and no trailing whitespace** (`end-of-file-fixer`, `trim trailing whitespace` hooks run on all files).
- Build: `buck2 build -v0 --console none //src/...`. Test: `buck2 test --console none //src/...`. Never pipe a superconsole `buck2 test` through `tail`/`head`.
- The `deploy//` cell is **off** the `//src` sweep and needs the `homelab` external cell. `buck2 test //src/...` will not build or test anything under `deploy/`.

---

### Task 1: `StandaloneTuning` carries the worker's job config

The standalone worker needs `WorkerTuning` (poll interval) and `WriteConfig` (write path) — together `datafusion_io::JobConfig` — plus the compaction threshold. The composite deliberately **never reads the live environment** (see the doc comment on `StandaloneTuning`), so these must be parsed once in `from_map` and handed in.

**The derive problem — this is the crux of the task.** `StandaloneTuning` currently derives `Clone, Copy, Debug, PartialEq, Eq` (`src/services/standalone/src/lib.rs:22`). `datafusion_io::WriteConfig` derives only `Clone, Debug` (`src/services/datafusion-io/src/write.rs:30`) — it is **not** `Copy`, **not** `PartialEq`/`Eq` (it holds an `f64`). So carrying `JobConfig` forces `StandaloneTuning` to drop `Copy`, `PartialEq`, and `Eq`. And `JobConfig` itself derives only `Default, serde::Deserialize` (`src/services/datafusion-io/src/job_config.rs:8`), so it needs `Clone, Debug` added before it can sit inside a `Clone, Debug` struct.

**Files:**
- Modify: `src/services/datafusion-io/src/job_config.rs:8` (add `Clone, Debug` derives)
- Modify: `src/services/standalone/src/lib.rs:22-43` (derives + two new fields + `from_map`)
- Modify: `src/services/standalone/BUCK:5-21` (add `datafusion-io` dep to the `standalone` library)
- Modify: `src/services/standalone/tests/tuning.rs` (new assertions)
- Modify: `src/services/standalone/BUCK:38-48` (add `datafusion-io` dep to the `tuning` test target)

**Interfaces:**
- Consumes: `datafusion_io::JobConfig { worker: loom_config::WorkerTuning, write: WriteConfig }`; `loom_config::load(&HashMap<String,String>) -> Result<T, ConfigError>`; `loom_config::overlay_opt(&map, key, &mut T) -> Result<(), ConfigError>`.
- Produces: `StandaloneTuning { session_ttl, max_ttl, lockout, engine, jobs: datafusion_io::JobConfig, compact_threshold_bytes: i64 }`, deriving `Clone, Debug` **only**. Task 2 reads `tuning.jobs` and `tuning.compact_threshold_bytes`.

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

`LOOM_WORKER_POLL_INTERVAL_MS` is the correct var — verified at `src/loom-config/src/worker.rs:61`, where `WorkerTuning::overlay_env` reads it into `poll_interval_ms`.

- [ ] **Step 2: Run test to verify it fails**

Run: `buck2 test --console none //src/services/standalone:tuning`
Expected: FAIL — compile error, `no field 'jobs' on type 'StandaloneTuning'`.

- [ ] **Step 3: Add `Clone, Debug` to `JobConfig`**

In `src/services/datafusion-io/src/job_config.rs`, change line 8:

```rust
#[derive(Default, Clone, Debug, serde::Deserialize)]
```

- [ ] **Step 4: Widen `StandaloneTuning`**

In `src/services/standalone/src/lib.rs`, replace the derive line and struct (lines 22-43):

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
        service_runtime::overlay_opt(vars, "LOOM_COMPACT_THRESHOLD_BYTES", &mut compact_threshold_bytes)?;
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

`service_runtime` re-exports `load` and `overlay_opt` from `loom_config` (`src/services/runtime/src/lib.rs:44-45`), so no new import is needed.

- [ ] **Step 5: Add the buck deps**

In `src/services/standalone/BUCK`, add to the `standalone` `rust_library` `deps` (the list is **not** strictly sorted today — `runtime` precedes `managed-postgres` at `:12-19` — so match the surrounding grouping rather than imposing a new order):

```python
        "//src/services/datafusion-io:datafusion-io",
```

And to the `tuning` `rust_test` `deps`:

```python
        "//src/services/datafusion-io:datafusion-io",
```

- [ ] **Step 6: Fix the `Copy` fallout in `serve_composite`**

`serve_composite` reads `tuning.engine` inside an `async move` block at `src/services/standalone/src/lib.rs:120`. That compiles today only because `StandaloneTuning` is `Copy`. With `Copy` gone, the block **moves** `tuning`. Extract the engine tuning into a local **before** the spawn (immediately after the `let max_ttl = tuning.max_ttl;` line at :86):

```rust
    let engine_tuning = tuning.engine;
```

and change line 120 from `tuning.engine,` to `engine_tuning,`.

`engine::EngineTuning` is `Copy` (it is a field of the current `Copy` `StandaloneTuning`), so this is a copy, not a move. Leave `tuning` otherwise intact — Task 2 reads `tuning.jobs` and `tuning.compact_threshold_bytes` from it.

- [ ] **Step 7: Run tests to verify they pass**

Run: `buck2 test --console none //src/services/standalone/...`
Expected: `Tests finished: Pass N. Fail 0` — `tuning`, `composite-e2e`, `composite-error-path`, `create-admin` all green.

If `composite-e2e` or `composite-error-path` fail to compile on a `StandaloneTuning` derive (e.g. an `assert_eq!` on the whole struct), fix the call site to compare fields rather than re-adding `PartialEq`.

- [ ] **Step 8: Commit**

```bash
git add src/services/datafusion-io/src/job_config.rs src/services/standalone/src/lib.rs src/services/standalone/BUCK src/services/standalone/tests/tuning.rs
git commit -m "feat(standalone): carry worker job config in StandaloneTuning"
```

---

### Task 2: Standalone composes the worker in-process

The acceptance task. **This is the test that fails on `main` today** — nothing drains a queued job in any composed deployment.

**Why extend `composite_e2e.rs` rather than add a new test file:** the drain assertion needs a **landed table** *and* a booted composite with a direct control-plane pool, and `composite_e2e.rs` is the only test that has both. There is no shared `embedded_config` helper to reach for (query-api's `e2e_support` exports seeding/HTTP helpers only, and `composite_error_path.rs:22-44` already carries its own divergent near-copy). The cost, which the plan accepts: `composite_e2e` becomes a four-concern test, so its module doc must be updated to say so (Step 1).

**Files:**
- Modify: `src/services/standalone/src/lib.rs` (spawn the worker task in `serve_composite`)
- Modify: `src/services/standalone/BUCK` (worker deps on the `standalone` library; test deps on `composite-e2e`)
- Test: `src/services/standalone/tests/composite_e2e.rs`

**Interfaces:**
- Consumes: `StandaloneTuning { jobs, compact_threshold_bytes }` from Task 1. `service_runtime::build_write_store(&ObjectStoreConfig) -> Result<WriteStore, StoreConfigError>`; `cfg.object_store: ObjectStoreConfig` (`src/services/runtime/src/lib.rs:242`). `engine_wire::client::GrpcQueueClient::connect(&str)`, `engine_wire::flight::{FlightTableClient, FlightSqlClient}::connect(&str)`. `control_plane_worker::Worker::new(client, worker_id: String, lease: Duration).with_poll_interval(Duration)`, then `.run(&[String], CancellationToken, handler) -> Result<()>`.
- Produces: nothing new public — the composite gains a `"worker"` `JoinSet` entry.

**Reference implementation:** `src/services/worker/src/main.rs:46-138` is the exact composition to mirror (contexts, kind list, dispatch match). Read it in full before Step 3. The only deviations are: build the write store from `cfg.object_store` instead of `parse_from_env`, take tuning from `StandaloneTuning` instead of `loom_config::load(&env)`, and drive cancellation from the composite's watch channel instead of `ctrl_c`.

- [ ] **Step 1: Write the failing test**

In `src/services/standalone/tests/composite_e2e.rs`, add these imports at the top:

```rust
use control_plane_core::{NewJob, Queue};
```

Change the direct-pool line so the test keeps a pool handle for raw SQL (the current line moves `pool` into `control_plane`):

```rust
    let pool = service_runtime::build_pool(&cfg_direct.db)
        .await
        .expect("direct pool");
    let cp = service_runtime::control_plane(pool.clone(), cfg_direct.lock_timeout);
```

Then insert this block **after** step (3)'s `assert_eq!(objects.len(), 2, ...)` and **before** step (4)'s shutdown:

```rust
    // (3.5) The composite runs a worker in-process, so a queued job drains.
    //       `Queue::complete` is `delete from queue.jobs where id = $1`
    //       (src/control-plane/postgres/src/queue.rs:108), so "row gone" == "drained".
    //       Without a composed worker nothing dequeues and this times out — which is
    //       exactly the hole this test guards.
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
        // Distinguish "nothing dequeued it" from "a worker took it and abandoned it":
        // Retry leaves state='available', Abandon leaves state='failed', and only
        // `complete` deletes the row (src/control-plane/postgres/src/queue.rs:108-140).
        let state: Option<(String, i32, Option<String>)> =
            sqlx::query_as("select state, attempts, last_error from queue.jobs where id = $1")
                .bind(job.0)
                .fetch_optional(&pool)
                .await
                .expect("read job state");
        panic!(
            "the flush_table job was never drained in 60s; (state, attempts, last_error) = {state:?} \
             (state='available' with attempts=0 => nothing dequeued it, i.e. no worker is composed; \
             state='failed' => a worker ran it and abandoned it; \
             attempts>0 with state='available' => it is retry-looping)"
        );
    }
```

Columns verified against `src/control-plane/postgres/migrations/0001_queue.sql:3-17`: `state text not null`, `attempts int not null default 0`, `last_error text` (nullable).

Also update the module doc at the top of the file (line 1-3) to mention that the composite drains queued jobs.

**On `job.0`:** `JobId(pub Uuid)` (`src/control-plane/core/src/queue.rs:15`) — the field is public, and `src/control-plane/postgres/src/queue.rs:108` binds `id.0` the same way.

**On the job kind — verified, no hedge needed.** `main.widget` is landed by step (1) via ingest with a two-row payload, far under the 16 MiB `inline_byte_limit` (`src/services/ingest/src/config.rs:64`), so it is inline-backed and `flush_table` does real work. And `handle_flush` **cannot** error on this table even if it were empty: `flush_locked` maps a missing snapshot to the empty case (`Err(ControlPlaneError::NotFound(_)) => return Ok(None)`, `iceberg_flush.rs:89-93`) and the RPC passes the `Option` straight through (`engine/src/service.rs:379-381`) — no inline rows ⇒ `Ok(None)` ⇒ the job completes.

**No competing job.** `inline_append` auto-enqueues a flush only when a write crosses `flush_byte_threshold`, defaulted to 64 MiB (`ingest/src/config.rs:65`; trigger at `iceberg_inline.rs:772-802`). The test's batch is nowhere near it, so there is no pre-existing undrained job on `main` and nothing races the test's job. `enqueue` is a plain insert with no dedup, so the test's job always gets its own uuid.

- [ ] **Step 2: Run test to verify it fails**

Run: `buck2 test --console none //src/services/standalone:composite-e2e`
Expected: FAIL — either a compile error on the missing `control_plane_core`/`sqlx` deps (add them in Step 3), or, once compiling, the assertion `the flush_table job was never drained in 60s`. **The timeout failure is the point** — confirm you see it before implementing, and that it is a timeout rather than an enqueue error.

- [ ] **Step 3: Add the buck deps**

In `src/services/standalone/BUCK`, add to the `standalone` `rust_library` `deps`:

```python
        "//src/control-plane/worker:worker",
        "//src/services/engine-wire:engine-wire",
        "//src/services/worker:worker",
        "//third-party:tokio-util",
        "//third-party:uuid",
```

and add to the `composite-e2e` `loom_fixture_test` `deps`:

```python
        "//src/control-plane/core:core",
        "//third-party:sqlx",
```

**Do not** add `//src/control-plane/postgres` anywhere in `src/services/worker/BUCK`. The `worker` library carries no postgres dep; `standalone` already has postgres transitively via `engine`, which is fine and does not weaken the guard (the guard's uquery targets `worker-bin`).

- [ ] **Step 4: Spawn the worker in `serve_composite`**

In `src/services/standalone/src/lib.rs`, add these imports at the top of the file:

```rust
use control_plane_core::{
    BUILD_VECTOR_INDEX_JOB_KIND, COMPACT_JOB_KIND, FLUSH_JOB_KIND, GC_JOB_KIND, JobFailure,
    ORPHAN_SWEEP_JOB_KIND, STREAM_CONSOLIDATE_JOB_KIND, STREAM_MV_JOB_KIND, TRANSFORM_JOB_KIND,
    TYPED_TRANSFORM_JOB_KIND,
};
use control_plane_worker::Worker;
use engine_wire::client::GrpcQueueClient;
use engine_wire::flight::{FlightSqlClient, FlightTableClient};
use tokio_util::sync::CancellationToken;
use worker::compact::{CompactCtx, handle_compact};
use worker::consolidate::handle_stream_consolidate;
use worker::stream_mv::{StreamMvCtx, handle_stream_mv};
use worker::transform::{TransformCtx, handle_transform, handle_typed_transform};
```

Insert this block **after** the engine-ready gate (after the `if eng_ready_rx.await.is_err() { … }` block ending at line 136) and **before** the ingest/query-api listener binds at line 138-140. The engine must be serving before the worker dials it.

```rust
    // Worker: dials the engine we just brought up, over the same UDS query-api uses.
    // Built from `cfg.object_store` (already resolved with the LOOM_DATA_PATH fallback
    // by `Config::from_map`) rather than `ObjectStoreConfig::parse_from_env`, which
    // requires LOOM_WAREHOUSE_URI — unset in embedded mode, the default here.
    let write = Arc::new(service_runtime::build_write_store(&cfg.object_store)?);
    let control = GrpcQueueClient::connect(&addrs.engine_socket).await?;
    let flight = FlightTableClient::connect(&addrs.engine_socket).await?;
    let sql = FlightSqlClient::connect(&addrs.engine_socket).await?;
    let worker_tuning = tuning.jobs.worker;
    let cctx = CompactCtx {
        control: control.clone(),
        flight: flight.clone(),
        write: write.clone(),
        threshold_bytes: tuning.compact_threshold_bytes,
        write_cfg: tuning.jobs.write.clone(),
        worker_tuning,
    };
    let tctx = TransformCtx {
        control: control.clone(),
        sql,
        write,
        write_cfg: tuning.jobs.write.clone(),
        worker_tuning,
    };
    let mctx = StreamMvCtx {
        control: control.clone(),
        table: flight,
        worker_tuning,
    };
    let flush = control.clone();
    // A fresh id per process: the composite never reads the live environment, so
    // LOOM_WORKER_ID is deliberately not consulted here.
    let worker_id = uuid::Uuid::new_v4().to_string();
    let worker = Worker::new(control, worker_id, cfg.lock_timeout)
        .with_poll_interval(worker_tuning.poll_interval());

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
            worker
                .run(
                    &[
                        FLUSH_JOB_KIND.to_string(),
                        GC_JOB_KIND.to_string(),
                        COMPACT_JOB_KIND.to_string(),
                        BUILD_VECTOR_INDEX_JOB_KIND.to_string(),
                        TRANSFORM_JOB_KIND.to_string(),
                        TYPED_TRANSFORM_JOB_KIND.to_string(),
                        STREAM_CONSOLIDATE_JOB_KIND.to_string(),
                        STREAM_MV_JOB_KIND.to_string(),
                        ORPHAN_SWEEP_JOB_KIND.to_string(),
                    ],
                    worker_cancel,
                    move |job| {
                        let flush = flush.clone();
                        let cctx = cctx.clone();
                        let tctx = tctx.clone();
                        let mctx = mctx.clone();
                        async move {
                            match job.kind.as_str() {
                                k if k == FLUSH_JOB_KIND => {
                                    worker::handler::handle_flush(flush, worker_tuning, job).await
                                }
                                k if k == GC_JOB_KIND => {
                                    worker::handler::handle_gc(flush, worker_tuning, job).await
                                }
                                k if k == ORPHAN_SWEEP_JOB_KIND => {
                                    worker::handler::handle_sweep_orphans(flush, worker_tuning, job)
                                        .await
                                }
                                k if k == COMPACT_JOB_KIND => handle_compact(&cctx, job).await,
                                k if k == TRANSFORM_JOB_KIND => handle_transform(&tctx, job).await,
                                k if k == TYPED_TRANSFORM_JOB_KIND => {
                                    handle_typed_transform(&tctx, job).await
                                }
                                k if k == BUILD_VECTOR_INDEX_JOB_KIND => {
                                    worker::handler::handle_build_vector_index(
                                        flush,
                                        worker_tuning,
                                        job,
                                    )
                                    .await
                                }
                                k if k == STREAM_CONSOLIDATE_JOB_KIND => {
                                    handle_stream_consolidate(flush, worker_tuning, job).await
                                }
                                k if k == STREAM_MV_JOB_KIND => handle_stream_mv(&mctx, job).await,
                                other => {
                                    Err(JobFailure::abandon(format!("unknown job kind: {other}")))
                                }
                            }
                        }
                    },
                )
                .await
                .map_err(Into::into),
        )
    });
```

Update the crate doc at `lib.rs:1-2` — it says "run engine (tonic/UDS) + ingest (HTTP) + query-api (HTTP) as tasks in one runtime". Add the worker. Also update `serve_composite`'s doc comment at :65-67 ("Run the three services…") and the `JoinSet` comment at :101-103 ("All three serve loops…"), both of which now understate the composition.

**On the `.map_err(Into::into)`:** the `JoinSet` entry is `Result<(), BoxErr>`. `Worker::run` returns `control_plane_core::Result<()>`; convert its error into `BoxErr`. If the error type does not implement `Into<BoxErr>`, map it explicitly (`.map_err(|e| -> BoxErr { Box::new(e) })`) rather than swallowing it.

**On `cfg.lock_timeout` as the lease:** `worker-bin` derives the lease from `LOOM_LOCK_TIMEOUT_MS` (default 5000ms). `service_runtime::Config::lock_timeout` is parsed from that same var, so reusing it keeps one source of truth and needs no new tuning field.

- [ ] **Step 5: Run the test to verify it passes**

Run: `buck2 test --console none //src/services/standalone:composite-e2e`
Expected: PASS.

If it still times out, do not raise the timeout. Diagnose: is the worker task spawning at all? Is it erroring immediately (check whether the composite returns a `"worker: …"` error)? Use `superpowers:systematic-debugging`.

- [ ] **Step 6: Run the whole standalone + worker suite**

Run: `buck2 test --console none //src/services/standalone/... //src/services/worker/...`
Expected: `Fail 0`.

- [ ] **Step 7: Verify the zero-pool guard still holds**

Run: `buck2 uquery "deps(//src/services/worker:worker-bin)" | grep -i "control-plane/postgres" | head`
Expected: **empty output**. If not empty, you added a postgres edge to the binary — revert it.

- [ ] **Step 8: Commit**

```bash
git add src/services/standalone/src/lib.rs src/services/standalone/BUCK src/services/standalone/tests/composite_e2e.rs
git commit -m "feat(standalone): drain queued jobs by composing the worker in-process"
```

---

### Task 3: Worker OCI image

**Files:**
- Create: `deploy/images/worker/apko.yaml`
- Create: `deploy/images/worker/apko.lock.json`
- Create: `deploy/images/worker/BUCK`

**Interfaces:**
- Consumes: `root//src/services/worker:worker-bin` (exists, `visibility = ["PUBLIC"]`).
- Produces: `deploy//images/worker:image` (+ `:image.info`, `:image.push`), repository `ghcr.io/weave-hand/loom-worker`. Task 4 references the repository string; Task 5 references `:image.info` and `:image.push`.

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

Run `apko lock deploy/images/worker/apko.yaml` if `apko` is on PATH. If it is not, copy `deploy/images/engine/apko.lock.json` verbatim — the package set is byte-identical to the engine's, so the resolved lock is the same. Verify the copied lock's `contents.packages` matches the engine's exactly; if the engine's lock has drifted from its `apko.yaml`, regenerate rather than copy.

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

Do **not** run `buck2 build deploy//images/worker:image` unless the `homelab` cell is available and you have network — apko fetches packages and builds local-only. A resolving uquery is the gate here.

- [ ] **Step 5: Commit**

```bash
git add deploy/images/worker/
git commit -m "feat(deploy): add the loom-worker OCI image"
```

---

### Task 4: Chart worker Deployment

**Files:**
- Create: `deploy/chart/chart/templates/worker.yaml`
- Modify: `deploy/chart/chart/templates/_helpers.tpl` (new `loom.workerWarehouseEnv` helper)
- Modify: `deploy/chart/chart/values.yaml` (new `worker:` block)
- Test: `deploy/chart/tests/render_assertions.sh`

**Interfaces:**
- Consumes: `loom.fullname`, `loom.labels`, `loom.selectorLabels`, `loom.serviceAccountName`, `loom.image`, `loom.dbEnv`, `loom.objectStoreEnv`, `loom.migrateOnBootEnv` (all in `_helpers.tpl`); `.Values.engine.{image,port,socketDir,socketPath,resources}`; `.Values.objectStore.{mountPath,s3.enabled}`.
- Produces: `loom.workerWarehouseEnv` (emits `LOOM_WAREHOUSE_URI` in the non-S3 case only); `.Values.worker.*`.

**The load-bearing detail.** `loom.objectStoreEnv` emits `LOOM_WAREHOUSE_URI` **only when `objectStore.s3.enabled`** (`_helpers.tpl`, the `{{- if .Values.objectStore.s3.enabled }}` branch); the default file:// path emits `LOOM_DATA_PATH` alone. `worker-bin` calls `store_config::ObjectStoreConfig::parse_from_env` eagerly at `src/services/worker/src/main.rs:49`, which **requires** `LOOM_WAREHOUSE_URI` and returns `Missing` without it (`src/services/store-config/src/lib.rs:153-156`). A worker container wired to the bare helper therefore **crash-loops on a default install**. The new helper closes that, and must **not** change `loom.objectStoreEnv` — the three existing services' rendered env must stay byte-identical.

- [ ] **Step 1: Write the failing assertions**

Append to `deploy/chart/tests/render_assertions.sh`, before its final summary line (match the file's existing `echo "== … =="` idiom):

```bash
echo "== worker: Deployment renders with worker + engine containers =="
OUT="$(helm template loom "$CHART")"
worker_deploy() { echo "$1" | awk '/^kind: Deployment$/{d=1} d && /component: worker/{p=1} p; /^---$/{if(p)exit}'; }
worker_deploy "$OUT" | has 'name: worker'
worker_deploy "$OUT" | has 'name: engine'

echo "== worker: LOOM_WAREHOUSE_URI is set in BOTH warehouse modes =="
# Default (file:// PVC): objectStoreEnv omits LOOM_WAREHOUSE_URI, so the worker
# needs it injected or worker-bin's eager parse_from_env crash-loops.
worker_deploy "$OUT" | has 'LOOM_WAREHOUSE_URI'
worker_deploy "$OUT" | has 'file:///var/lib/loom/data'
S3OUT="$(helm template loom "$CHART" \
  --set objectStore.s3.enabled=true \
  --set objectStore.s3.bucket=b \
  --set objectStore.s3.credentialsSecret=s)"
worker_deploy "$S3OUT" | has 's3://b'

echo "== worker: container carries NO DB credentials (zero-pool) =="
# Only the engine sidecar talks to PG. Scope to the worker container: from
# `- name: worker` to the next container boundary.
worker_container() { worker_deploy "$1" | awk '/^        - name: worker$/{f=1;next} f && /^        - name: /{exit} f'; }
worker_container "$OUT" | hasnt 'LOOM_DB_'

echo "== worker: data mount + affinity only when !s3 =="
worker_deploy "$OUT" | has 'persistentVolumeClaim'
worker_deploy "$S3OUT" | hasnt 'persistentVolumeClaim'
```

- [ ] **Step 2: Run the assertions to verify they fail**

Run: `bash deploy/chart/tests/render_assertions.sh`
Expected: FAIL at `ASSERT FAIL: expected to find: name: worker` — no worker template exists yet.

Requires `helm` 3.x on PATH. If `helm` is unavailable in this environment, say so plainly in the PR rather than claiming the assertions passed.

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

Create `deploy/chart/chart/templates/worker.yaml`. Model it directly on `deploy/chart/chart/templates/query-api.yaml` — copy that file and adapt. Read it first; the structure below must match its idiom (labels, securityContext, volumes, the `{{- if .Values.queryApi.affinity }}…{{- else if not .Values.objectStore.s3.enabled }}` affinity ladder, and the podAffinity block at its tail).

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

**No Service.** `query-api.yaml` continues past this point with a `---` and a `ClusterIP` Service; the worker exposes no port and needs none. End the file after the tolerations block.

- [ ] **Step 6: Run the assertions to verify they pass**

Run: `bash deploy/chart/tests/render_assertions.sh`
Expected: all assertions pass, including the pre-existing ones (`helm lint` clean, migrations modes, query-api affinity, allow-s3).

- [ ] **Step 7: Commit**

```bash
git add deploy/chart/chart/templates/worker.yaml deploy/chart/chart/templates/_helpers.tpl deploy/chart/chart/values.yaml deploy/chart/tests/render_assertions.sh
git commit -m "feat(deploy): run the worker as its own Deployment in the chart"
```

---

### Task 5: Release wiring and digest pin

Without this the chart references an image that is never built or pushed.

**Files:**
- Modify: `.github/workflows/release.yml`
- Modify: `deploy/chart/BUCK`

**Interfaces:**
- Consumes: `deploy//images/worker:image.push` and `:image.info` from Task 3; the `worker.image` values key from Task 4.

- [ ] **Step 1: Add the image env**

In `.github/workflows/release.yml`, after the `ENGINE_IMAGE` line (~line 42):

```yaml
  WORKER_IMAGE: ghcr.io/weave-hand/loom-worker
```

- [ ] **Step 2: Add the push lines**

There are **three** push blocks. Add a worker line to **each**, matching the surrounding alignment:

After line ~88 (`buck2 run deploy//images/engine:image.push    -- "sha-$short"`):
```bash
          buck2 run deploy//images/worker:image.push    -- "sha-$short"
```

After line ~163 (`… engine:image.push -- "$V" latest`):
```bash
          buck2 run deploy//images/worker:image.push    -- "$V" latest
```

After line ~215 (`… engine:image.push -- "$dev" "$moving"`):
```bash
          buck2 run deploy//images/worker:image.push    -- "$dev" "$moving"
```

**Verify by reading the file** — the line numbers above are from `main` at the time of writing and may have drifted. Search for `images/engine:image.push` and add a worker line beside each of the three hits. If any block also references images elsewhere (a digest-collection step, a summary), add the worker there too.

- [ ] **Step 3: Add the digest pin**

In `deploy/chart/BUCK`, add to `helm_chart`'s `images` map:

```python
        "worker.image": "//images/worker:image.info",
```

and update the rule's header comment — it says "digest-pinning the two service images into values.yaml (ingest.image / queryApi.image)", which was already stale at three and is now wrong at four. Make it name the four.

- [ ] **Step 4: Verify the chart target resolves**

Run: `buck2 uquery 'deploy//chart:chart'`
Expected: prints `deploy//chart:chart`.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/release.yml deploy/chart/BUCK
git commit -m "build(deploy): publish and digest-pin the loom-worker image"
```

---

### Task 6: Close the register item

**Files:**
- Modify: `docs/ROADMAP.md` (remove the `road-deploy-worker` entry)
- Modify: `docs/FUTURE.md` (update `fut-worker-lazy-compact-ctx` prose)
- Modify/Create: `docs/system-capabilities/` (record the landed capability)

- [ ] **Step 1: Run the docs-update skill**

Use the `loom-docs-update` skill. It closes resolved items and records new deferrals, staged alongside the work.

- [ ] **Step 2: Remove the ROADMAP entry**

Registers carry **open work only** — the closing PR removes the entry; git history is the record. Delete the `road-deploy-worker` bullet. If it leaves `## deploy` empty, leave the empty section heading (the other registers keep empty sections — see `## catalog` in ROADMAP today).

- [ ] **Step 3: Update `fut-worker-lazy-compact-ctx`**

It stays **open** — lazy `CompactCtx` is still wanted for a flush-only worker. Update its prose: the manifest now exists and sets `LOOM_WAREHOUSE_URI`, so the "when the worker Helm manifest is authored" conditional is discharged; what remains is the lazy-init idea itself.

- [ ] **Step 4: Record the capability**

Add the worker deployment path to the relevant file under `docs/system-capabilities/` (read its `README.md` for the per-subsystem convention first).

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

- [ ] **Full sweep:** `buck2 test --console none //src/...` → `Fail 0`. Not a scoped subset; Task 1 changed a shared type (`JobConfig`) and Task 2 changed `standalone`'s dep closure.
- [ ] **Zero-pool guard:** `buck2 uquery "deps(//src/services/worker:worker-bin)" | grep -i "control-plane/postgres"` → empty.
- [ ] **Metric gate (a FIX step, not a reporting step):** run `loom-complexity diff` and `loom-duplication diff` against the **merge-base** (`git merge-base HEAD origin/main`), not the committed register. If the branch worsened any hotspot on any axis, or introduced any cross-file duplication pair ≥ 20 lines, **fix it in this PR**. The most likely finding is duplication between `serve_composite`'s worker composition and `worker-bin`'s `main` — they are near-identical by design. If the detector flags it, the honest fix is to extract the shared composition (contexts + kind list + dispatch match) into the `worker` library and have both call it; that is very likely the abstraction that should have existed already. Put before/after numbers in the PR body.
  - Note: `loom-complexity diff` needs `-p` repeated per changed path when more than one file changed.
  - Fast-forward local `main` to `origin/main` first, or the diff base sweeps unrelated files.
- [ ] **Lint:** `buck2 run //tools:prek -- run --all-files` and commit anything the hooks rewrite. `--no-verify` skips rustfmt and CI lint will fail.
