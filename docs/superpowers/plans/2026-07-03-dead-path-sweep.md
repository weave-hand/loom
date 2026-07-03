# Dead-Path Sweep Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Delete superseded/dead pipelines, collapse duplicated sibling paths, and fix one shipped defect across the ingest, transform/worker, and engine service clusters — all behaviour-preserving except the two named bug fixes.

**Architecture:** Three independent phases, one per cluster. Each phase is self-contained, independently committable, and verified green on its own affected targets before the next begins. Within a phase, tasks are ordered so each ends at a compiling, test-passing state. Nothing here changes wire formats or endpoint semantics observable to a correct client; the only behaviour changes are `iss-transform-catalog-local-only` (transform can now write to S3) and deleting a dead footer-inference code path.

**Tech Stack:** Rust, buck2, DataFusion, Iceberg (single arrow major = 58), Postgres control plane, Arrow Flight/gRPC wire.

## Global Constraints

- **Register item:** delivers `road-dead-path-sweep`; closes `iss-transform-catalog-local-only`. Spec: `docs/superpowers/specs/2026-07-02-pillar-idioms-audit-design.md` (§ "road-dead-path-sweep", Wave-0 defect #6).
- **Tests are `rust_test` integration targets only** — never inline `#[cfg(test)]` (the `no-inline-tests` prek hook fails the build otherwise). Put unit tests in a sibling `tests/<name>.rs` wired as its own `rust_test`/`loom_rust_test`/`loom_fixture_test` target.
- **Fixture (hermetic-Postgres) tests use `loom_fixture_test`**, pure-logic tests use `loom_rust_test` — never a bare `rust_test`.
- **Cloud disk cap ~38 GiB.** Build with `buck2 build -M none <targets>`; scope tests to the touched/affected targets — never a bare whole-tree `buck2 build //src/...`. `buck2 clean` between phases reclaims space. Do not pipe `buck2 test`/`bxl` through `tail`/`head` — redirect to a file and grep it.
- **Clippy is strict** (pedantic + restriction groups enforced): no new `unwrap`/`expect`/`panic`/`todo`/`indexing_slicing`/`dbg` in production code; use `#[expect(lint, reason = "...")]` locally only when unavoidable. Test code is exempted from panic-safety lints via the `loom_rust_test`/`loom_fixture_test` wrappers.
- **Conventional Commits** enforced on commit messages (`type(scope): summary`). Use `refactor`/`fix`/`test`/`chore` as appropriate.
- **`.sqlx` cache:** none of these tasks change SQL — no `tools/sqlx-prepare.sh` run is required. (If any task unexpectedly touches a `query!`/`query_scalar!`, refresh the cache and commit it.)
- **Do not put the model identifier anywhere in committed artifacts** (commit messages, code comments, PR body).

---

## Phase A — Transform/Worker cluster

Order: A1 → A2 → A3 → A4 → A5 → A6. A1–A2 are worker-only; A3 removes the worker→transform edge; A4 is the bug fix; A5 deletes the dead compaction library; A6 dedups the config struct.

### Task A1: `JobFailure::abandon` / `JobFailure::retry` constructors

The three worker handlers and `worker/src/compact.rs` and both binaries' dispatchers all build `JobFailure { error, policy }` struct literals inline. Add two constructors in core so every site names its intent.

**Files:**
- Modify: `src/control-plane/core/src/queue.rs` (add an `impl JobFailure` block after the struct at `:49-53`)
- Test: `src/control-plane/core/tests/job_failure.rs` (new)
- Modify: `src/control-plane/core/BUCK` (new `loom_rust_test` target)

**Interfaces:**
- Produces:
  - `impl JobFailure { pub fn abandon(error: impl Into<String>) -> Self; pub fn retry(delay: std::time::Duration, error: impl Into<String>) -> Self; }`

- [ ] **Step 1: Write the failing test.** Create `src/control-plane/core/tests/job_failure.rs`:

```rust
use std::time::Duration;

use control_plane_core::queue::{JobFailure, RetryPolicy};

#[test]
fn abandon_sets_abandon_policy() {
    let f = JobFailure::abandon("bad payload");
    assert_eq!(f.error, "bad payload");
    assert!(matches!(f.policy, RetryPolicy::Abandon));
}

#[test]
fn retry_carries_delay() {
    let f = JobFailure::retry(Duration::from_millis(250), format!("rpc: {}", "boom"));
    assert_eq!(f.error, "rpc: boom");
    match f.policy {
        RetryPolicy::Retry { delay } => assert_eq!(delay, Duration::from_millis(250)),
        RetryPolicy::Abandon => panic!("expected Retry"),
    }
}
```

  Confirm `queue` and its items are reachable as `control_plane_core::queue::{JobFailure, RetryPolicy}` — check `src/control-plane/core/src/lib.rs` for whether `queue` is `pub mod queue` (it is; `JobFailure`/`RetryPolicy` are also re-exported at the crate root, so `control_plane_core::{JobFailure, RetryPolicy}` works too — use whichever the existing tests use).

- [ ] **Step 2: Add the `loom_rust_test` target** to `src/control-plane/core/BUCK`, mirroring an existing pure-logic test target in that file (e.g. the `page`/`compact_job` test). It needs `deps = [":core"]` (plus nothing else). Name it `job-failure`, `srcs = ["tests/job_failure.rs"]`, `crate = "job_failure"`.

- [ ] **Step 3: Run to verify it fails.** Run: `buck2 build -M none //src/control-plane/core:job-failure 2>&1 | tail -20` — expect a compile error `no function or associated item named 'abandon'`.

- [ ] **Step 4: Implement the constructors.** In `src/control-plane/core/src/queue.rs`, immediately after the `JobFailure` struct (`:53`), add:

```rust
impl JobFailure {
    /// A terminal failure: move the job to `failed`, retained for inspection.
    pub fn abandon(error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            policy: RetryPolicy::Abandon,
        }
    }

    /// A retryable failure: make the job available again after `delay`.
    pub fn retry(delay: std::time::Duration, error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            policy: RetryPolicy::Retry { delay },
        }
    }
}
```

- [ ] **Step 5: Run to verify it passes.** Run: `buck2 test //src/control-plane/core:job-failure > /tmp/a1.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/a1.log` — expect pass.

- [ ] **Step 6: Commit.**

```bash
git add src/control-plane/core/src/queue.rs src/control-plane/core/tests/job_failure.rs src/control-plane/core/BUCK
git commit -m "feat(queue): JobFailure::abandon/retry constructors"
```

### Task A2: `run_wire_job` wrapper collapsing the three clone-shaped handlers

`handle_flush`, `handle_gc`, `handle_build_vector_index` (`src/services/worker/src/handler.rs`) are identical modulo the payload type, the error-message prefix, and which RPC runs. Extract a generic wrapper and rewrite all three; convert `worker/src/compact.rs`'s local `retry` and the two mains' `Abandon` literals to the A1 constructors.

**Files:**
- Modify: `src/services/worker/src/handler.rs` (add `run_wire_job`; rewrite the three handlers)
- Modify: `src/services/worker/src/compact.rs` (use `JobFailure::retry`/`abandon`)
- Modify: `src/services/worker/src/main.rs` and `src/services/transform/src/main.rs` (use `JobFailure::abandon` for the unknown-kind arm)
- Test: `src/services/worker/tests/run_wire_job.rs` (new) + `src/services/worker/BUCK` target

**Interfaces:**
- Consumes: `JobFailure::abandon`, `JobFailure::retry` (Task A1); `WorkerTuning::backoff(attempts) -> Duration`.
- Produces:
  - `pub async fn run_wire_job<P, F, Fut, E>(job: Job, tuning: WorkerTuning, what: &str, rpc: F) -> Result<(), JobFailure> where P: serde::de::DeserializeOwned, F: FnOnce(P) -> Fut, Fut: Future<Output = Result<(), E>>, E: std::fmt::Display`

- [ ] **Step 1: Write the failing test.** Create `src/services/worker/tests/run_wire_job.rs`:

```rust
use std::time::Duration;

use control_plane_core::queue::RetryPolicy;
use control_plane_core::{Job, JobId};
use loom_config::WorkerTuning;
use worker::handler::run_wire_job;

fn job(payload: serde_json::Value) -> Job {
    Job {
        id: JobId(uuid::Uuid::nil()),
        kind: "test".into(),
        payload,
        attempts: 1,
        run_at: time::OffsetDateTime::UNIX_EPOCH,
    }
}

#[derive(serde::Deserialize)]
struct P {
    n: i64,
}

#[tokio::test]
async fn bad_payload_abandons() {
    let out = run_wire_job::<P, _, _, String>(
        job(serde_json::json!({ "wrong": 1 })),
        WorkerTuning::default(),
        "test",
        |_p| async { Ok(()) },
    )
    .await;
    let err = out.expect_err("should fail to parse");
    assert!(matches!(err.policy, RetryPolicy::Abandon));
    assert!(err.error.contains("bad test payload"));
}

#[tokio::test]
async fn rpc_error_retries() {
    let out = run_wire_job::<P, _, _, &str>(
        job(serde_json::json!({ "n": 7 })),
        WorkerTuning::default(),
        "test",
        |p| async move {
            assert_eq!(p.n, 7);
            Err("boom")
        },
    )
    .await;
    let err = out.expect_err("rpc failed");
    assert!(matches!(err.policy, RetryPolicy::Retry { .. }));
    assert!(err.error.contains("boom"));
}

#[tokio::test]
async fn ok_passes_through() {
    let out = run_wire_job::<P, _, _, String>(
        job(serde_json::json!({ "n": 1 })),
        WorkerTuning::default(),
        "test",
        |_p| async { Ok(()) },
    )
    .await;
    assert!(out.is_ok());
}
```

  Add a `run-wire-job` `loom_rust_test` target to `src/services/worker/BUCK`, mirroring an existing worker pure-logic test target's deps but adding `//third-party:tokio`, `//third-party:serde`, `//third-party:serde_json`, `//third-party:uuid`, `//third-party:time`, `//src/control-plane/core:core`, `//src/loom-config:loom-config`, and `:worker`. Confirm `Job`/`JobId`/`WorkerTuning::default` field names against `src/control-plane/core/src/queue.rs:26-36` and `src/loom-config/src/worker.rs` before running (adjust the `Job` literal if a field differs). If `WorkerTuning` has no `Default`, construct it explicitly from its fields instead.

- [ ] **Step 2: Run to verify it fails.** Run: `buck2 build -M none //src/services/worker:run-wire-job 2>&1 | tail -20` — expect `unresolved import ... run_wire_job`.

- [ ] **Step 3: Implement `run_wire_job` and rewrite the handlers.** Replace the body of `src/services/worker/src/handler.rs` with:

```rust
//! The worker's job handlers: parse a flush_table / gc_table / build_vector_index
//! job and run it over the wire. All three are one shape — deserialize the typed
//! payload (parse error => Abandon), run one RPC (RPC error => Retry with backoff) —
//! captured by `run_wire_job`.
use std::future::Future;

use control_plane_core::{BuildVectorIndexJob, FlushJob, GcJob, Job, JobFailure};
use engine_wire::client::GrpcQueueClient;
use loom_config::WorkerTuning;

/// Run a single-RPC wire job: parse `job.payload` as `P`, then call `rpc`.
/// A parse failure is terminal (`Abandon`); an RPC failure is retried with the
/// tuning's backoff. `what` names the job kind for the parse-error message.
pub async fn run_wire_job<P, F, Fut, E>(
    job: Job,
    tuning: WorkerTuning,
    what: &str,
    rpc: F,
) -> std::result::Result<(), JobFailure>
where
    P: serde::de::DeserializeOwned,
    F: FnOnce(P) -> Fut,
    Fut: Future<Output = std::result::Result<(), E>>,
    E: std::fmt::Display,
{
    let payload: P = serde_json::from_value(job.payload)
        .map_err(|e| JobFailure::abandon(format!("bad {what} payload: {e}")))?;
    rpc(payload)
        .await
        .map_err(|e| JobFailure::retry(tuning.backoff(job.attempts), e.to_string()))?;
    Ok(())
}

pub async fn handle_flush(
    flush: GrpcQueueClient,
    tuning: WorkerTuning,
    job: Job,
) -> std::result::Result<(), JobFailure> {
    run_wire_job(job, tuning, "flush", |FlushJob { schema, name }| {
        flush.flush_table(schema, name)
    })
    .await
}

pub async fn handle_gc(
    engine: GrpcQueueClient,
    tuning: WorkerTuning,
    job: Job,
) -> std::result::Result<(), JobFailure> {
    run_wire_job(job, tuning, "gc", |GcJob { schema, name }| {
        engine.gc_table(schema, name)
    })
    .await
}

pub async fn handle_build_vector_index(
    client: GrpcQueueClient,
    tuning: WorkerTuning,
    job: Job,
) -> std::result::Result<(), JobFailure> {
    run_wire_job(
        job,
        tuning,
        "build_vector_index",
        |BuildVectorIndexJob {
             schema,
             name,
             index_name,
         }| client.build_vector_index(schema, name, index_name),
    )
    .await
}
```

  Note: the error-message prefix changes from e.g. `"bad gc payload"` to `"bad gc payload"` (identical) — verify each `what` string reproduces the old prefix exactly (`flush`, `gc`, `build_vector_index`). If any `GrpcQueueClient` RPC method borrows `&self` rather than consuming, the closure still works because it moves the owned client in; leave as written and let the build confirm.

- [ ] **Step 4: Convert `compact.rs` and the mains to the constructors.**
  - In `src/services/worker/src/compact.rs`, delete the local `fn retry(...)` (`:24-31`) and replace its call sites `retry(&ctx.worker_tuning, attempts, msg)` with `JobFailure::retry(ctx.worker_tuning.backoff(attempts), msg)`; replace the `Abandon` literal at `:36-39` with `JobFailure::abandon(format!("bad compact payload: {e}"))`. Update the `use` line (`:6`) to drop `RetryPolicy` if now unused.
  - In `src/services/worker/src/main.rs` (`:122-125`) and `src/services/transform/src/main.rs` (the analogous unknown-kind arm ~`:88-91`), replace `JobFailure { error: format!("unknown job kind: {other}"), policy: RetryPolicy::Abandon }` with `JobFailure::abandon(format!("unknown job kind: {other}"))`; drop the now-unused `RetryPolicy` import if the file has no other use of it.

- [ ] **Step 5: Run tests.** Run:

```bash
buck2 test //src/services/worker:run-wire-job //src/services/worker:worker \
  //src/services/worker/... > /tmp/a2.log 2>&1; grep -E "Tests finished|FAIL" /tmp/a2.log
```

  Also build the transform binary to catch the main edit: `buck2 build -M none //src/services/transform:transform-bin //src/services/worker:worker-bin 2>&1 | tail -5`. Expect all pass/clean.

- [ ] **Step 6: Commit.**

```bash
git add src/services/worker src/services/transform/src/main.rs
git commit -m "refactor(worker): run_wire_job wrapper + JobFailure constructors"
```

### Task A3: Relocate `small_files` to core; drop the worker→transform dep

`small_files` (pure, `src/services/transform/src/compact.rs:38-43`) is the *only* symbol the worker imports from transform. Move it next to `FileRef` in `control_plane_core` so the zero-pool worker drops its transform dep.

**Files:**
- Modify: `src/control-plane/core/src/catalog.rs` (add `small_files` next to `FileRef`) + re-export in `src/control-plane/core/src/lib.rs` if the crate re-exports catalog items
- Modify: `src/services/transform/src/compact.rs` (delete the local `small_files`), `src/services/transform/src/lib.rs` (drop it from the re-export)
- Modify: `src/services/worker/src/compact.rs` (`use control_plane_core::small_files;`)
- Move: `src/services/transform/tests/compact_unit.rs` → `src/control-plane/core/tests/small_files.rs` (or add an equivalent core test)
- Modify BUCK: `src/services/worker/BUCK` (drop `//src/services/transform:transform` from `worker` lib `:16`, `worker-bin` `:43`, `compact-e2e` `:173`); `src/control-plane/core/BUCK` (new test target); `src/services/transform/BUCK` (remove `compact-unit` target if the test moved)

**Interfaces:**
- Produces: `control_plane_core::small_files(files: &[FileRef], threshold_bytes: i64) -> Vec<&FileRef>` (re-exported at crate root as `control_plane_core::small_files`).
- Consumes: `control_plane_core::catalog::FileRef` (fields `path`, `file_size_bytes` — confirm against `catalog.rs:35`).

- [ ] **Step 1: Move the function + test.** In `src/control-plane/core/src/catalog.rs`, after the `FileRef` definition, add:

```rust
/// The subset of `files` smaller than `threshold_bytes` — the compaction candidate set.
pub fn small_files(files: &[FileRef], threshold_bytes: i64) -> Vec<&FileRef> {
    files
        .iter()
        .filter(|f| f.file_size_bytes < threshold_bytes)
        .collect()
}
```

  Ensure it is reachable as `control_plane_core::small_files` — if `lib.rs` does `pub use catalog::...`, add `small_files` there; else reference `control_plane_core::catalog::small_files` at call sites. Create `src/control-plane/core/tests/small_files.rs` porting `transform/tests/compact_unit.rs`'s assertions verbatim but importing from core; wire a `small-files` `loom_rust_test` target in `src/control-plane/core/BUCK` (`deps = [":core"]`, plus whatever the test constructs `FileRef` with).

- [ ] **Step 2: Delete the transform copy and re-export.** In `src/services/transform/src/compact.rs`, delete `pub fn small_files` (`:38-43`). In `src/services/transform/src/lib.rs:11`, change `pub use compact::{CompactConfig, CompactError, compact_table, small_files};` to drop `small_files` (it will change again in A5). Update `compact.rs`'s own call at `:64` — but note `compact_table` is deleted in A5; for now, if compact.rs still references `small_files`, import it: `use control_plane_core::small_files;` at the top of `compact.rs`. Remove the `compact-unit` target from `src/services/transform/BUCK:96-106` (the test moved to core).

- [ ] **Step 3: Point the worker at core.** In `src/services/worker/src/compact.rs`, change `use transform::small_files;` (`:12`) to `use control_plane_core::small_files;` (or fold into the existing `control_plane_core::{...}` import at `:6`).

- [ ] **Step 4: Drop the worker→transform BUCK edges.** Remove `//src/services/transform:transform` from the three worker targets (`worker` lib, `worker-bin`, `compact-e2e`) in `src/services/worker/BUCK`. The zero-pool guard comment (`worker/BUCK:25-28`) stays.

- [ ] **Step 5: Build + test.** Run:

```bash
buck2 build -M none //src/control-plane/core:core //src/services/transform:transform \
  //src/services/worker:worker //src/services/worker:worker-bin 2>&1 | tail -5
buck2 test //src/control-plane/core:small-files //src/services/worker:compact-e2e \
  > /tmp/a3.log 2>&1; grep -E "Tests finished|FAIL" /tmp/a3.log
```

  Expect clean build and passing tests. `compact-e2e` is a fixture/wire test — it routes local automatically via its macro.

- [ ] **Step 6: Commit.**

```bash
git add src/control-plane/core src/services/transform src/services/worker
git commit -m "refactor(core): relocate small_files to core; worker drops transform dep"
```

### Task A4: Fix `build_iceberg_catalog` to honour `cfg.object_store` (iss-transform-catalog-local-only)

`transform/src/main.rs::build_iceberg_catalog` hardcodes `LocalFsStorageFactory` + `file://{data_path}`, so the transform binary cannot commit to an S3 warehouse. Mirror the ingest/engine pattern exactly.

**Files:**
- Modify: `src/services/transform/src/main.rs` (the `build_iceberg_catalog` fn + drop the `use iceberg::io::LocalFsStorageFactory;` import at `:15`)

**Interfaces:**
- Consumes: `service_runtime::build_storage_factory(&cfg.object_store) -> Result<Arc<dyn iceberg::io::StorageFactory>, ConfigError>`; `cfg.object_store.warehouse_uri: String`.

- [ ] **Step 1: Apply the fix.** In `src/services/transform/src/main.rs`, change the two hardcoded lines in `build_iceberg_catalog` (`:102-116`) to mirror `src/services/ingest/src/serve.rs:53-65`:
  - warehouse prop value `format!("file://{}", cfg.data_path.display())` → `cfg.object_store.warehouse_uri.clone()`
  - `.with_storage_factory(Arc::new(LocalFsStorageFactory))` → `.with_storage_factory(service_runtime::build_storage_factory(&cfg.object_store)?)`

  Then delete the now-unused `use iceberg::io::LocalFsStorageFactory;` at `:15`. Keep the other `iceberg` imports (`SqlCatalogBuilder`, `SQL_CATALOG_PROP_*`).

- [ ] **Step 2: Build.** Run: `buck2 build -M none //src/services/transform:transform-bin 2>&1 | tail -5` — expect clean (an unused-import warning would fail clippy, so confirm the `LocalFsStorageFactory` import is gone).

- [ ] **Step 3: Verify against the S3 path.** A full S3 write test needs MinIO, which may be unavailable in this environment. The fix is byte-for-byte the ingest/engine idiom already covered by `src/control-plane/postgres/tests/iceberg_s3_roundtrip.rs`; run the transform e2e (local backend) to confirm no regression on the local path:

```bash
buck2 test //src/services/transform/... > /tmp/a4.log 2>&1; grep -E "Tests finished|FAIL" /tmp/a4.log
```

  Expect pass. Note in the commit body that S3 coverage rides on the existing roundtrip idiom.

- [ ] **Step 4: Close the issue register entry.** This is handled at PR time via `loom-docs-update` (mark `iss-transform-catalog-local-only` fixed, add `pr:#N`). Do not edit `docs/ISSUES.md` here.

- [ ] **Step 5: Commit.**

```bash
git add src/services/transform/src/main.rs
git commit -m "fix(transform): build_iceberg_catalog honours cfg.object_store (S3 warehouses)"
```

### Task A5: Delete `compact_table`/`CompactConfig`/`CompactError`; port the unique test

`transform::compact_table` (`src/services/transform/src/compact.rs`) is superseded by the worker path and used only by its own e2e. **Before deleting**, port the one assertion the worker e2e lacks — `compact_leaves_large_files_untouched` (`transform/tests/compact_e2e.rs:149-284`) — into the worker e2e.

**Files:**
- Modify: `src/services/worker/tests/compact_e2e.rs` (add a `worker_leaves_large_files_untouched` test)
- Delete: `src/services/transform/tests/compact_e2e.rs` and the `compact-e2e` target (`src/services/transform/BUCK:175-195`)
- Delete: `compact_table`, `CompactConfig`, `CompactError` from `src/services/transform/src/compact.rs`; if `compact.rs` is now empty (only held those + the moved `small_files`), delete the file and its `mod compact;`/re-exports in `lib.rs`
- Modify: `src/services/transform/src/lib.rs` (drop the `compact::{CompactConfig, CompactError, compact_table}` re-export)

**Interfaces:**
- Consumes: the worker compaction wire path (`worker::compact::handle_compact` via the engine UDS harness the worker e2e already builds).

- [ ] **Step 1: Read both e2es.** Read `src/services/transform/tests/compact_e2e.rs` (the `compact_leaves_large_files_untouched` fn, `:149-284`) and `src/services/worker/tests/compact_e2e.rs` (the existing `worker_compacts_small_files_over_the_wire`). Identify the worker e2e's setup harness (engine UDS spawn, seed helpers, threshold knob).

- [ ] **Step 2: Write the ported test.** Add a new test fn to `src/services/worker/tests/compact_e2e.rs` that reproduces the transform test's scenario over the worker/wire path: seed a table with a mix of small files plus one large file, set the threshold at the large file's exact size (so the large file is *not* a candidate), run the worker compaction job, and assert (a) the small files coalesced and (b) the large file remained live and untouched. Reuse the existing worker-e2e harness/seed helpers; do not copy transform-specific plumbing. Model the assertions on the transform version but express them through the worker e2e's existing readback (serving query / `list_files`).

- [ ] **Step 3: Run the ported test.** Run: `buck2 test //src/services/worker:compact-e2e > /tmp/a5a.log 2>&1; grep -E "Tests finished|FAIL" /tmp/a5a.log` — expect pass (both the pre-existing and the new test).

- [ ] **Step 4: Delete the dead library + transform e2e.** Remove `compact_table`, `CompactConfig`, `CompactError` from `compact.rs`. If only `small_files` remained (moved in A3) and it is now gone from transform, delete `compact.rs` entirely, remove `mod compact;` from `lib.rs`, and delete the `compact::{...}` re-export line. Delete `src/services/transform/tests/compact_e2e.rs` and its `compact-e2e` BUCK target (`:175-195`). Check for any dangling `use` of the deleted types across transform (`run.rs`, `handler.rs`, `main.rs`) and remove them.

- [ ] **Step 5: Build + test transform + worker.** Run:

```bash
buck2 build -M none //src/services/transform/... 2>&1 | tail -5
buck2 test //src/services/transform/... //src/services/worker:compact-e2e \
  > /tmp/a5b.log 2>&1; grep -E "Tests finished|FAIL" /tmp/a5b.log
```

  Expect clean build and green tests, with the transform `compact-e2e`/`compact-unit` targets no longer present.

- [ ] **Step 6: Commit.**

```bash
git add src/services/transform src/services/worker
git commit -m "refactor(transform): delete superseded compact_table; port large-file case to worker e2e"
```

### Task A6: Dedup `TransformConfig`/`WorkerConfig` into a shared struct in `datafusion-io`

Both binaries define a byte-identical `{ worker: WorkerTuning, write: WriteConfig }` + `LayeredConfig` impl. It **cannot** live in `loom-config` (that would cycle: `loom-config → datafusion-io → loom-config`, since `WriteConfig` lives in datafusion-io). The correct home is `datafusion-io`, which already sees both `loom_config::WorkerTuning` and its own `WriteConfig`, and is already a dep of both binaries.

**Files:**
- Modify: `src/services/datafusion-io/src/lib.rs` (add `pub mod job_config;` or inline; export `JobConfig`) + a new `src/services/datafusion-io/src/job_config.rs`
- Modify: `src/services/worker/src/main.rs` (delete local `WorkerConfig`; use `datafusion_io::JobConfig`)
- Modify: `src/services/transform/src/main.rs` (delete local `TransformConfig`; use `datafusion_io::JobConfig`)
- Test: `src/services/datafusion-io/tests/job_config.rs` (new) + BUCK target

**Interfaces:**
- Produces: `datafusion_io::JobConfig` — `#[derive(Default, serde::Deserialize)] #[serde(default)] pub struct JobConfig { pub worker: loom_config::WorkerTuning, pub write: crate::WriteConfig }` with `impl loom_config::LayeredConfig for JobConfig`.

- [ ] **Step 1: Write the failing test.** Create `src/services/datafusion-io/tests/job_config.rs`:

```rust
use std::collections::HashMap;

use datafusion_io::JobConfig;
use loom_config::LayeredConfig;

#[test]
fn overlays_env_onto_defaults() {
    let mut cfg = JobConfig::default();
    let mut env = HashMap::new();
    env.insert("LOOM_WORKER_POLL_INTERVAL_MS".to_string(), "42".to_string());
    cfg.overlay_env(&env).expect("overlay");
    cfg.validate().expect("valid");
    assert_eq!(cfg.worker.poll_interval().as_millis(), 42);
}
```

  Confirm the exact env var name and accessor against `src/loom-config/src/worker.rs` (`WorkerTuning::overlay_env` / `poll_interval`) and adjust the key/assertion to a real overlayable field. Wire a `job-config` `loom_rust_test` target in `src/services/datafusion-io/BUCK` (`deps = [":datafusion-io", "//src/loom-config:loom-config"]`).

- [ ] **Step 2: Run to verify it fails.** Run: `buck2 build -M none //src/services/datafusion-io:job-config 2>&1 | tail -20` — expect `unresolved import datafusion_io::JobConfig`.

- [ ] **Step 3: Implement `JobConfig`.** Create `src/services/datafusion-io/src/job_config.rs`:

```rust
//! Composed config for the job-processing service binaries (worker, transform):
//! worker tuning + Parquet write config, loaded via `loom_config::load` as
//! defaults < file < env through the `LayeredConfig` impl below. Lives here (not
//! `loom-config`) because `write` is this crate's `WriteConfig` and `datafusion-io`
//! already depends on `loom-config` — the reverse edge would cycle.
use crate::WriteConfig;

#[derive(Default, serde::Deserialize)]
#[serde(default)]
pub struct JobConfig {
    pub worker: loom_config::WorkerTuning,
    pub write: WriteConfig,
}

impl loom_config::LayeredConfig for JobConfig {
    fn overlay_env(
        &mut self,
        env: &std::collections::HashMap<String, String>,
    ) -> Result<(), loom_config::ConfigError> {
        self.worker.overlay_env(env)?;
        self.write.overlay_env(env)?;
        Ok(())
    }

    fn validate(&self) -> Result<(), loom_config::ConfigError> {
        self.worker.validate()?;
        self.write.validate()?;
        Ok(())
    }
}
```

  In `src/services/datafusion-io/src/lib.rs`, add `mod job_config;` and `pub use job_config::JobConfig;` alongside the existing `pub use write::{...}`. Confirm `WriteConfig` implements `overlay_env`/`validate` (it must, since the binaries call them today) — it lives at `src/services/datafusion-io/src/write.rs:32`.

- [ ] **Step 4: Swap both binaries onto it.** In `src/services/worker/src/main.rs`, delete the local `struct WorkerConfig` + its `LayeredConfig` impl (`:23-47`) and change `let wcfg: WorkerConfig = loom_config::load(&env)?;` to `let wcfg: datafusion_io::JobConfig = loom_config::load(&env)?;`. Do the same in `src/services/transform/src/main.rs` for `TransformConfig`. Confirm both binaries already dep `//src/services/datafusion-io` (they do) — no BUCK dep change needed.

- [ ] **Step 5: Build + test.** Run:

```bash
buck2 test //src/services/datafusion-io:job-config > /tmp/a6.log 2>&1; grep -E "Tests finished|FAIL" /tmp/a6.log
buck2 build -M none //src/services/worker:worker-bin //src/services/transform:transform-bin 2>&1 | tail -5
```

  Expect green test + clean build.

- [ ] **Step 6: Commit + Phase A verification.**

```bash
git add src/services/datafusion-io src/services/worker/src/main.rs src/services/transform/src/main.rs
git commit -m "refactor(config): hoist shared JobConfig into datafusion-io"
```

  Then run the whole Phase A affected surface once and confirm clippy is clean:

```bash
buck2 test //src/services/worker/... //src/services/transform/... //src/control-plane/core/... \
  //src/services/datafusion-io/... > /tmp/phaseA.log 2>&1; grep -E "Tests finished|FAIL" /tmp/phaseA.log
./tools/clippy-all.sh > /tmp/phaseA-clippy.log 2>&1; tail -5 /tmp/phaseA-clippy.log
buck2 clean
```

---

## Phase B — Engine cluster

Order: B1 → B2 → B3 → B4 → B5 → B6. All within `engine`, `engine-serving`, `store-config` (+ two query-api tests). Behaviour-preserving except deleting the dead `try_new` path.

### Task B1: Build one `Arc<SqlCatalog>` instead of three

`engine::run::run` (`src/services/engine/src/run.rs:75-87`) builds `catalog`, `flight_catalog`, `writer_catalog` — three `SqlCatalog`s, each opening its own PG pool (default 10 conns) and each calling `build_storage_factory` separately. All consumers take `&SqlCatalog`, so one shared `Arc<SqlCatalog>` deref-coerces everywhere.

**Files:**
- Modify: `src/services/engine/src/run.rs` (build one `Arc<SqlCatalog>`, clone into all three holders)
- Modify: `src/services/engine/src/service.rs:45` (`catalog: SqlCatalog` → `Arc<SqlCatalog>`)
- Modify: `src/services/engine/src/flight.rs:46` (`catalog: SqlCatalog` → `Arc<SqlCatalog>`)

**Interfaces:**
- `IcebergActionWriter::new` already takes `Arc<SqlCatalog>` (`action_writer.rs:43`) — unchanged.
- Consumers call `flush_table(&self.catalog, …)` etc. — `&Arc<SqlCatalog>` deref-coerces to `&SqlCatalog`; no call-site change.

- [ ] **Step 1: Change the field types.** In `service.rs`, change `EngineControlService.catalog` to `Arc<SqlCatalog>`; in `flight.rs`, change `FlightDataService.catalog` to `Arc<SqlCatalog>`. Add `use std::sync::Arc;` if absent. Update each struct's constructor/literal to accept/store the `Arc`.

- [ ] **Step 2: Build one catalog in `run.rs`.** Replace the three `SqlCatalogBuilder::default()....load(...)` blocks (`:75-87`) with a single:

```rust
let catalog = Arc::new(
    SqlCatalogBuilder::default()
        .with_storage_factory(service_runtime::build_storage_factory(&cfg.object_store)?)
        .load("loom", props)
        .await?,
);
```

  Delete `flight_catalog`, `writer_catalog`, `props.clone()`, and `props_for_writer` (build `props` once, move it into the single `.load`). Clone the single `Arc` into each holder: `EngineControlService`'s `catalog: catalog.clone()`, `FlightDataService`'s `catalog: catalog.clone()`, and `IcebergActionWriter::new(catalog.clone(), …)`. Verify `props_for_writer` carried no distinct props vs `props` (`run.rs:74`) — if it set something extra (e.g. a writer-only prop), preserve that on the single catalog; if identical, drop it.

- [ ] **Step 3: Build + test.** Run:

```bash
buck2 build -M none //src/services/engine:engine //src/services/engine:engine-bin 2>&1 | tail -5
buck2 test //src/services/engine/... > /tmp/b1.log 2>&1; grep -E "Tests finished|FAIL" /tmp/b1.log
```

  Expect clean build and green engine fixture tests.

- [ ] **Step 4: Commit.**

```bash
git add src/services/engine
git commit -m "refactor(engine): share one Arc<SqlCatalog> across the three services"
```

### Task B2: Delete `IcebergMirrorTableProvider::try_new` (dead footer-inference path)

`try_new` (`src/services/engine-serving/src/serving.rs:437-455`) infers schema from Parquet footers, contradicting the mirror-authoritative invariant; production uses only `try_new_with_schema`. Its two callers are query-api tests.

**Files:**
- Modify: `src/services/engine-serving/src/serving.rs` (delete `try_new`; drop now-unused imports: `ParquetFormat`, `ListingOptions`, `ListingTableConfig`, `ListingTable`, and `SessionContext` if unused elsewhere — check first, `:18-24`)
- Modify: `src/services/query-api/tests/iceberg_mirror_provider.rs` (rewrite over `try_new_with_schema`)
- Modify: `src/services/query-api/tests/iceberg_pruning_e2e.rs` (rewrite over `try_new_with_schema`)

**Interfaces:**
- Consumes: `IcebergMirrorTableProvider::try_new_with_schema(files: Vec<FileWithStats>, schema: SchemaRef) -> Self` (sync, no ctx).

- [ ] **Step 1: Rewrite the two tests first.** In each of `iceberg_mirror_provider.rs:104` and `iceberg_pruning_e2e.rs:88`, replace `IcebergMirrorTableProvider::try_new(&ctx, files).await?` with `IcebergMirrorTableProvider::try_new_with_schema(files, schema)`, constructing `schema: SchemaRef` from the mirror columns the test already seeds (the test knows the table's arrow schema — build it directly rather than inferring from Parquet). Drop the `SessionContext`/local-FS-store registration lines that only existed to feed `try_new`. Keep the pruning assertions unchanged.

- [ ] **Step 2: Run the tests (should still pass against the still-present `try_new_with_schema`).** Run: `buck2 test //src/services/query-api:iceberg-mirror-provider //src/services/query-api:iceberg-pruning-e2e > /tmp/b2a.log 2>&1; grep -E "Tests finished|FAIL" /tmp/b2a.log` — expect pass.

- [ ] **Step 3: Delete `try_new`.** Remove the `pub async fn try_new` (`serving.rs:437-455`) and the doc-comment tension note above it if it only described `try_new`. Remove now-unused imports. Build to confirm nothing else referenced it.

- [ ] **Step 4: Build + test.** Run:

```bash
buck2 build -M none //src/services/engine-serving:engine-serving 2>&1 | tail -5
buck2 test //src/services/query-api:iceberg-mirror-provider //src/services/query-api:iceberg-pruning-e2e \
  > /tmp/b2b.log 2>&1; grep -E "Tests finished|FAIL" /tmp/b2b.log
```

  Expect clean build (no unused-import warnings — those fail clippy) and green tests.

- [ ] **Step 5: Commit.**

```bash
git add src/services/engine-serving/src/serving.rs src/services/query-api/tests/iceberg_mirror_provider.rs src/services/query-api/tests/iceberg_pruning_e2e.rs
git commit -m "refactor(engine-serving): delete dead footer-inference try_new; tests use try_new_with_schema"
```

### Task B3: `ServingStore` tuple alias → named struct

`type ServingStore = (String, Arc<dyn ObjectStore>)` (`store-config/src/lib.rs:169`) is spelled inline `(String, Arc<dyn object_store::ObjectStore>)` at six engine hops and destructured `(bucket, store)` twice. Make it a named struct with `bucket`/`store`.

**Files:**
- Modify: `src/services/store-config/src/lib.rs` (`ServingStore` struct; `build_serving_object_store` construction; `build_write_store` destructure)
- Modify: `src/services/runtime/src/lib.rs:38` (re-export unchanged — still `service_runtime::ServingStore`)
- Modify: the six hops — `engine-serving/src/serving.rs` (`build_serving_provider :75`, `register_iceberg_table :154`, `execute_query :471`, `execute_query_stream :491`), `engine-serving/src/governed.rs:292`, `engine/src/flight.rs:51`
- Modify destructure site: `engine-serving/src/serving.rs:86`

**Interfaces:**
- Produces: `pub struct ServingStore { pub bucket: String, pub store: Arc<dyn ObjectStore> }`. Threaded params become `Option<&ServingStore>`; the flight field becomes `Option<ServingStore>`.

- [ ] **Step 1: Define the struct + fix construction.** In `store-config/src/lib.rs`, replace the `type ServingStore = ...` alias (`:168-169`) with:

```rust
/// Bucket name + object store handle returned by [`build_serving_object_store`].
pub struct ServingStore {
    pub bucket: String,
    pub store: Arc<dyn ObjectStore>,
}
```

  Change `build_serving_object_store`'s S3 return (`:189`) from `Ok(Some((s.bucket.clone(), Arc::new(store))))` to `Ok(Some(ServingStore { bucket: s.bucket.clone(), store: Arc::new(store) }))`. Change `build_write_store`'s S3 arm (`:216-217`) destructure to field access (this line changes again in B6; for now: `let ss = build_serving_object_store(cfg)?.expect(...); ... store: ss.store, root_url: format!("s3://{}", ss.bucket)`).

- [ ] **Step 2: Update the six hops.** Change every `&(String, Arc<dyn object_store::ObjectStore>)` param to `&ServingStore` (import `store_config::ServingStore` or `service_runtime::ServingStore` as the file already imports store types) and the flight owned field to `Option<ServingStore>`. At the destructure `if let Some((bucket, store)) = serving_store` (`serving.rs:86`), change to `if let Some(ServingStore { bucket, store }) = serving_store` (borrowed: `if let Some(ss) = serving_store { ... ss.bucket ... ss.store ... }`, whichever borrows cleanly).

- [ ] **Step 3: Build + test.** Run:

```bash
buck2 build -M none //src/services/store-config:store-config //src/services/engine-serving:engine-serving //src/services/engine:engine 2>&1 | tail -5
buck2 test //src/services/store-config/... //src/services/engine-serving/... > /tmp/b3.log 2>&1; grep -E "Tests finished|FAIL" /tmp/b3.log
```

  Expect clean build + green.

- [ ] **Step 4: Commit.**

```bash
git add src/services/store-config src/services/runtime src/services/engine-serving src/services/engine
git commit -m "refactor(store-config): ServingStore named struct in place of the tuple alias"
```

### Task B4: `VectorQuery<'a>` params struct (delete the `too_many_arguments` expect)

`engine_serving::vector_search` (`vector_search.rs:47-60`) has 8 args under an `#[expect(clippy::too_many_arguments)]`. Fold the six query-describing args into `VectorQuery<'a>`, leaving `catalog`/`pool`.

**Files:**
- Modify: `src/services/engine-serving/src/vector_search.rs` (add `VectorQuery<'a>`; change `vector_search` signature; delete the `#[expect]`)
- Modify callers: `src/services/engine/src/flight.rs:119-128`; test callers in `engine-serving/tests/{vector_search.rs, vector_search_identity_kinds.rs, vector_index_auto_rebuild.rs}` and `query-api/tests/e2e_support.rs:174`

**Interfaces:**
- Produces:
  - `pub struct VectorQuery<'a> { pub table: &'a TableRef, pub index_name: &'a str, pub query: &'a [f32], pub k: usize, pub nprobe: Option<u32>, pub ef_search: Option<u32> }`
  - `pub async fn vector_search(catalog: &SqlCatalog, pool: &PgPool, q: VectorQuery<'_>) -> Result<RecordBatch, EngineServingError>`

- [ ] **Step 1: Add the struct + change the signature.** In `vector_search.rs`, add the `VectorQuery<'a>` struct above the function, delete the `#[expect(clippy::too_many_arguments, ...)]` attribute, and change `vector_search` to take `(catalog, pool, q: VectorQuery<'_>)`, replacing the six loose args with `q.table`, `q.index_name`, `q.query`, `q.k`, `q.nprobe`, `q.ef_search` in the body.

- [ ] **Step 2: Update the production caller.** In `engine/src/flight.rs:119-128` (`do_get_vector_search`), build a `VectorQuery { table, index_name, query, k, nprobe, ef_search }` and pass it. Import `engine_serving::VectorQuery` (add `pub use vector_search::VectorQuery;` to engine-serving's `lib.rs` if the crate re-exports symbols there — check how `vector_search` itself is exported and mirror it).

- [ ] **Step 3: Update the test callers.** In each test call site (14 in `vector_search.rs`, 3 in `vector_search_identity_kinds.rs`, 3 in `vector_index_auto_rebuild.rs`, 1 in `e2e_support.rs:174`), wrap the six query args in `VectorQuery { ... }`. A local helper in each test file (e.g. `fn vq<'a>(...) -> VectorQuery<'a>`) is acceptable to keep call sites terse, but a plain struct literal is fine.

- [ ] **Step 4: Build + test.** Run:

```bash
buck2 build -M none //src/services/engine-serving:engine-serving //src/services/engine:engine 2>&1 | tail -5
buck2 test //src/services/engine-serving:vector-search //src/services/engine-serving:vector-search-identity-kinds \
  //src/services/engine-serving:vector-index-auto-rebuild //src/services/engine:vector-search-flight \
  > /tmp/b4.log 2>&1; grep -E "Tests finished|FAIL" /tmp/b4.log
```

  Expect clean build (the `#[expect]` is gone and the arg count is now under threshold) and green tests. If any target name differs, list with `buck2 targets //src/services/engine-serving: 2>/dev/null | grep vector`.

- [ ] **Step 5: Commit.**

```bash
git add src/services/engine-serving src/services/engine/src/flight.rs src/services/query-api/tests/e2e_support.rs
git commit -m "refactor(engine-serving): VectorQuery params struct replaces the too_many_arguments expect"
```

### Task B5: `register_qualified` helper + `execute_query = collect(execute_query_stream)`

Two dedups in `engine-serving/src/serving.rs` (+ `governed.rs`): the schema-qualified registration block (serving.rs:162-173 ≡ governed.rs:306-317) and the two `execute_query`/`execute_query_stream` bodies that differ by one line.

**Files:**
- Modify: `src/services/engine-serving/src/serving.rs` (extract `register_qualified`; rewrite `execute_query`)
- Modify: `src/services/engine-serving/src/governed.rs` (call `register_qualified`)
- Modify: `src/services/engine-serving/BUCK` (add `//third-party:futures` to the lib deps if `TryStreamExt` is used for collection)

**Interfaces:**
- Produces: `fn register_qualified(ctx: &SessionContext, schema: &str, name: &str, provider: Arc<dyn TableProvider>) -> Result<(), EngineServingError>` (visibility `pub(crate)` or module-private as needed).

- [ ] **Step 1: Extract `register_qualified`.** In `serving.rs`, factor the four statements at `:162-173` into a function taking `(ctx, schema, name, provider)`; call it from `register_iceberg_table`. In `governed.rs`, replace the mirrored block at `:306-317` with a call to it (import the helper). Confirm the provider type parameter matches both call sites (`Arc<dyn TableProvider>` — check the concrete types `IcebergMirrorTableProvider`/`GovernedTableProvider` coerce).

- [ ] **Step 2: Rewrite `execute_query` over the stream.** Change `execute_query` (`serving.rs:468-479`) to delegate:

```rust
pub async fn execute_query(
    catalog: &IcebergCatalog,
    sql: &str,
    serving_store: Option<&ServingStore>,
) -> Result<Vec<RecordBatch>, EngineServingError> {
    let stream = execute_query_stream(catalog, sql, serving_store).await?;
    datafusion::physical_plan::common::collect(stream)
        .await
        .map_err(to_serving)
}
```

  (Prefer `datafusion::physical_plan::common::collect` — no new dep — over `TryStreamExt::try_collect`. If you use `try_collect`, add `//third-party:futures` to the engine-serving **lib** deps.) The `ServingStore` param type reflects B3.

- [ ] **Step 3: Build + test.** Run:

```bash
buck2 build -M none //src/services/engine-serving:engine-serving 2>&1 | tail -5
buck2 test //src/services/engine-serving:execute-query-e2e //src/services/engine-serving:execute-query-stream \
  //src/services/engine-serving:governed-sql > /tmp/b5.log 2>&1; grep -E "Tests finished|FAIL" /tmp/b5.log
```

  Expect green (the stream-equivalence test already pins row-for-row parity).

- [ ] **Step 4: Commit.**

```bash
git add src/services/engine-serving
git commit -m "refactor(engine-serving): register_qualified helper; execute_query via execute_query_stream"
```

### Task B6: store-config — restructure `build_write_store`'s `expect`; hide `for_s3_test`

`build_write_store`'s S3 arm calls `build_serving_object_store(...)?.expect(...)` (`store-config/src/lib.rs:216-217`); `for_s3_test` (`:69-89`) is a `pub fn` test helper that ships in the public API.

**Files:**
- Modify: `src/services/store-config/src/lib.rs`

- [ ] **Step 1: Extract a non-optional S3 helper.** Add a private `fn s3_serving_store(s: &S3Backend) -> Result<ServingStore, StoreConfigError>` containing the `AmazonS3Builder` body currently in `build_serving_object_store`'s S3 arm (`:179-189`). Rewrite `build_serving_object_store`'s S3 arm to `Ok(Some(s3_serving_store(s)?))`. Rewrite `build_write_store`'s S3 arm (`:215-222`) to `ObjectStoreBackend::S3(s) => { let ss = s3_serving_store(s)?; Ok(WriteStore { store: ss.store, root_url: format!("s3://{}", ss.bucket) }) }` — no `Option`, no `expect`.

- [ ] **Step 2: Hide `for_s3_test`.** Add `#[doc(hidden)]` immediately above `pub fn for_s3_test` (`:71`). Leave it `pub` (cross-crate test callers need it: `postgres/tests/iceberg_s3_roundtrip.rs:34`, `lineage-naming/tests/naming.rs:10`).

- [ ] **Step 3: Build + test.** Run:

```bash
buck2 build -M none //src/services/store-config:store-config 2>&1 | tail -5
buck2 test //src/services/store-config/... > /tmp/b6.log 2>&1; grep -E "Tests finished|FAIL" /tmp/b6.log
```

  Expect clean build (no `expect_used` lint firing now) + green.

- [ ] **Step 4: Commit + Phase B verification.**

```bash
git add src/services/store-config/src/lib.rs
git commit -m "refactor(store-config): drop build_write_store expect; hide for_s3_test from docs"
```

  Then:

```bash
buck2 test //src/services/engine/... //src/services/engine-serving/... //src/services/store-config/... \
  //src/services/query-api:iceberg-mirror-provider //src/services/query-api:iceberg-pruning-e2e \
  > /tmp/phaseB.log 2>&1; grep -E "Tests finished|FAIL" /tmp/phaseB.log
./tools/clippy-all.sh > /tmp/phaseB-clippy.log 2>&1; tail -5 /tmp/phaseB-clippy.log
buck2 clean
```

---

## Phase C — Ingest cluster (highest blast radius; do last)

Order: C1 (safe deletion) → C2 (add shared decode) → C3 (the `land` signature change). C3 touches ~18 fixture tests in postgres + engine-serving, so verify with the full affected fixture set.

### Task C1: Delete the production-dead `materialize` pipeline; keep `resolve_columns`

`ingest::materialize` (`materialize.rs`) exposes `materialize()`, `MaterializeRequest`, and a module-internal `land()` whose only consumer is `tests/materialize.rs`. `resolve_columns` (same file, `:33`) is production-live (called by `http.rs:325, 431`) and must be kept.

**Files:**
- Modify: `src/services/ingest/src/materialize.rs` (delete `MaterializeRequest`, `land`, `materialize`; keep `resolve_columns`; fix the stale module doc)
- Modify: `src/services/ingest/src/lib.rs:21` (drop the `materialize`/`MaterializeRequest` re-export; keep the module for `resolve_columns` if `http.rs` imports `crate::materialize::resolve_columns`)
- Delete: `src/services/ingest/tests/materialize.rs` and the `materialize` BUCK target (`ingest/BUCK:159-178`)
- Modify: `src/services/ingest/src/http.rs:1-4` (correct the stale module doc that claims all landing logic lives in `materialize`)

- [ ] **Step 1: Delete the dead symbols.** In `materialize.rs`, remove `MaterializeRequest` (`:17`), the module-internal `land` (`:61`), and `materialize` (`:100`). Keep `resolve_columns` (`:33`) and whatever imports it still needs (`gate::validate`, `datafusion_io::infer_columns`, `ColumnSpec`, `ModelShape`). Delete now-unused imports (`ControlPlane`, `ObjectStore`, `WriteConfig`, `write_dataset`, `SnapshotId`, `LineageEvent`, `RecordBatch`, `TableRef` — whichever were only used by the deleted fns). Rewrite the module doc (`:1`) to describe the surviving responsibility (physical-column resolution / model-gate validation), not "the orchestrator".

- [ ] **Step 2: Fix the re-export + http doc.** In `lib.rs:21`, drop `materialize`/`MaterializeRequest` from the re-export; keep `mod materialize;` (or `pub(crate) mod materialize;`) so `crate::materialize::resolve_columns` still resolves for `http.rs`. Fix the stale `http.rs:1-4` module doc to say landing dispatches through the `LandingMaterializer` port, not `materialize`.

- [ ] **Step 3: Delete the test + BUCK target.** Remove `src/services/ingest/tests/materialize.rs` and the `materialize` `rust_test` target (`ingest/BUCK:159-178`).

- [ ] **Step 4: Build + test.** Run:

```bash
buck2 build -M none //src/services/ingest:ingest //src/services/ingest:ingest-bin 2>&1 | tail -5
buck2 test //src/services/ingest/... > /tmp/c1.log 2>&1; grep -E "Tests finished|FAIL" /tmp/c1.log
```

  Expect clean build (no unused-import warnings) + green (minus the deleted `materialize` target).

- [ ] **Step 5: Commit.**

```bash
git add src/services/ingest
git commit -m "refactor(ingest): delete production-dead materialize pipeline; keep resolve_columns"
```

### Task C2: Add `datafusion_io::decode_ipc`; adopt it in the two umbrella-arrow callers

There are three `decode_ipc` copies. `datafusion-io` has none yet. Add one there and adopt it in the two crates that already dep datafusion-io + umbrella `arrow` (ingest, engine-serving). (Copy C in the postgres crate is deleted in C3 by removing decoding from `land`, not by adding a dep.)

**Files:**
- Create/modify: `src/services/datafusion-io/src/ipc.rs` (new) + export in `lib.rs`
- Modify: `src/services/ingest/src/http.rs` (replace local `decode_ipc` `:164` with `datafusion_io::decode_ipc`)
- Modify: `src/services/engine-serving/src/action_writer.rs` (replace local `decode_ipc` `:19`, preserving the empty-body → empty-vec behaviour)
- Test: `src/services/datafusion-io/tests/ipc.rs` (new) + BUCK target

**Interfaces:**
- Produces: `pub fn decode_ipc(body: &[u8]) -> Result<(SchemaRef, Vec<RecordBatch>), IpcError>` — returns `(schema, batches)`; empty `body` → an empty batch vec with a valid (empty) schema is **not** required (callers that need the empty-signal handle it themselves). Define an `IpcError` (thiserror) or reuse `datafusion-io`'s existing error style.

- [ ] **Step 1: Write the failing test.** Create `src/services/datafusion-io/tests/ipc.rs` that encodes a small `RecordBatch` to an Arrow IPC stream (via `arrow::ipc::writer::StreamWriter`), decodes it with `datafusion_io::decode_ipc`, and asserts the schema and row count round-trip; and that decoding random/truncated bytes returns `Err` (not panic). Wire an `ipc` `loom_rust_test` target in `datafusion-io/BUCK` with `//third-party:arrow` in deps.

- [ ] **Step 2: Run to verify it fails.** Run: `buck2 build -M none //src/services/datafusion-io:ipc 2>&1 | tail -20` — expect `unresolved import datafusion_io::decode_ipc`.

- [ ] **Step 3: Implement the helper.** Create `src/services/datafusion-io/src/ipc.rs`:

```rust
//! Single authoritative Arrow-IPC stream decode. The three per-crate copies
//! (ingest HTTP, engine-serving action writer, postgres iceberg_landing) collapse
//! to this — callers on the umbrella `arrow` crate call it directly; the postgres
//! crate stops decoding entirely (it now takes pre-decoded batches).
use std::io::Cursor;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow::ipc::reader::StreamReader;

/// Decode an Arrow IPC stream into its schema and batches.
pub fn decode_ipc(body: &[u8]) -> Result<(Arc<Schema>, Vec<RecordBatch>), arrow::error::ArrowError> {
    let reader = StreamReader::try_new(Cursor::new(body), None)?;
    let schema = reader.schema();
    let batches = reader.collect::<Result<Vec<_>, _>>()?;
    Ok((schema, batches))
}
```

  (Return the raw `arrow::error::ArrowError` — Copy A already did, and both adopting callers map arrow errors to their own error types at the call boundary.) Add `mod ipc; pub use ipc::decode_ipc;` to `datafusion-io/src/lib.rs`.

- [ ] **Step 4: Adopt in ingest.** In `src/services/ingest/src/http.rs`, delete the local `fn decode_ipc` (`:164`) and its `use arrow::ipc::reader::StreamReader;` import; call `datafusion_io::decode_ipc(&body)` at `:282` and `:405`, mapping its `ArrowError` to the same `IngestError` the local copy's callers expected (check how the two call sites `match`/`map_err` today and preserve the exact error mapping).

- [ ] **Step 5: Adopt in engine-serving.** In `src/services/engine-serving/src/action_writer.rs`, delete the local `fn decode_ipc` (`:19`). At its one caller (`overwrite_table :90`), preserve the empty-body special case explicitly: `if ipc.is_empty() { Vec::new() } else { datafusion_io::decode_ipc(ipc).map_err(|e| EngineServingError::Engine(e.to_string()))?.1 }` (take `.1` for batches-only). Keep the exact `EngineServingError::Engine(...)` mapping.

- [ ] **Step 6: Build + test.** Run:

```bash
buck2 test //src/services/datafusion-io:ipc > /tmp/c2.log 2>&1; grep -E "Tests finished|FAIL" /tmp/c2.log
buck2 build -M none //src/services/ingest:ingest //src/services/engine-serving:engine-serving 2>&1 | tail -5
buck2 test //src/services/engine-serving:action-writer > /tmp/c2b.log 2>&1; grep -E "Tests finished|FAIL" /tmp/c2b.log
```

  Expect green + clean.

- [ ] **Step 7: Commit.**

```bash
git add src/services/datafusion-io src/services/ingest/src/http.rs src/services/engine-serving/src/action_writer.rs
git commit -m "refactor(datafusion-io): one decode_ipc; ingest + action-writer adopt it"
```

### Task C3: `iceberg_landing::land` takes `(SchemaRef, Vec<RecordBatch>)`; delete the double decode

`land` (`control-plane/postgres/src/iceberg_landing.rs:67`) currently takes `ipc_body: &[u8]` and re-decodes (`:76`) bytes the ingest/engine-serving caller already decoded. Change it to accept pre-decoded batches, delete Copy C (`iceberg_landing.rs:43`), and drop `LandRequest.ipc_body`.

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_landing.rs` (`land` signature; delete `decode_ipc` `:43`, drop `arrow_ipc::reader::StreamReader` import `:15`)
- Modify: `src/services/ingest/src/landing.rs` (`LandRequest` drops `ipc_body`; `IcebergMaterializer::land` passes `req.schema`/`req.batches`)
- Modify: `src/services/ingest/src/http.rs` (`LandRequest` construction at `:345, :434` drops `ipc_body`)
- Modify: `src/services/engine-serving/src/action_writer.rs` (`write_object :64` decodes once, passes batches)
- Modify: the direct-`land` test callers in `control-plane/postgres/tests/*` (~14 files) and `engine-serving/tests/*` (~4 files)

**Interfaces:**
- Produces: `pub async fn land(pool: &PgPool, catalog: &SqlCatalog, table: &TableRef, columns: &[ColumnSpec], schema: SchemaRef, batches: Vec<RecordBatch>, limits: InlineLimits, lineage: LineageEvent) -> Result<SnapshotId>` (replaces `ipc_body: &[u8]` with `schema` + `batches`; `align_to_columns` runs on the passed batches).

- [ ] **Step 1: Change `land`'s signature + body.** In `iceberg_landing.rs`, replace the `ipc_body: &[u8]` param with `schema: SchemaRef, batches: Vec<RecordBatch>` (use the split-arrow `arrow_schema::SchemaRef` / `arrow_array::RecordBatch` types the crate already imports). Delete the `let (schema, batches) = decode_ipc(ipc_body)?;` line (`:76`) — use the params directly. Delete `fn decode_ipc` (`:43`) and the `use arrow_ipc::reader::StreamReader;` import (`:15`) and the `Cursor` import if now unused. Everything downstream (`align_to_columns`, `inline_append`, `land_parquet`) already works on batches — leave it.

  **Cross-crate arrow type note (de-risks the whole task):** the umbrella `arrow` crate (used by ingest/engine-serving) re-exports the *same* split `arrow-schema`/`arrow-array` rlibs the postgres crate deps directly (single arrow major = 58 tree-wide), so `Arc<arrow::datatypes::Schema>` and `arrow_schema::SchemaRef` are the identical type and unify at the call boundary. This is already proven in-tree: `engine-serving/src/action_writer.rs:91` passes a `Vec<RecordBatch>` (umbrella arrow) into `iceberg_landing::overwrite_parquet_snapshot` (split arrow) today. If the build ever complains of a type mismatch here, it means the tree drifted off a single arrow major — stop and escalate, don't paper over it with a conversion.

- [ ] **Step 2: Update `LandRequest` + the live materializer.** In `landing.rs`, delete the `ipc_body` field from `LandRequest` (`:21`). In `IcebergMaterializer::land` (`:58`), pass `req.schema.clone()` (or move) + `req.batches.to_vec()` into `iceberg_land(...)` instead of `req.ipc_body`. Confirm the `schema`/`batches` field types match `land`'s new params (convert `&[RecordBatch]` → `Vec<RecordBatch>` via `.to_vec()`; `Arc<Schema>` matches `SchemaRef`).

- [ ] **Step 3: Update the two ingest construction sites.** In `http.rs:345` and `:434`, drop `ipc_body: body.as_ref()` from the `LandRequest` literal. The `schema`/`batches` already come from the C2 `datafusion_io::decode_ipc(&body)` call — no extra decode.

- [ ] **Step 4: Update engine-serving `write_object`.** In `action_writer.rs`, `write_object` (`:64`) currently forwards raw `ipc: &[u8]` into `iceberg_landing::land`. Decode once at the top of `write_object` with `datafusion_io::decode_ipc(ipc).map_err(|e| EngineServingError::Engine(e.to_string()))?` and pass `(schema, batches)` to `land`. Handle empty `ipc` if `write_object` can receive it (mirror `overwrite_table`'s empty handling if applicable — check whether an empty insert is possible; if not, no special case needed).

- [ ] **Step 5: Update the direct-`land` test callers.** Each test in `control-plane/postgres/tests/{iceberg_landing, iceberg_schema_evolution_land, iceberg_compact, iceberg_tx_compact, iceberg_overwrite, iceberg_gc, vector_landing, vector_index_build, vector_index_multi, vector_index_hnsw, vector_index_ivf, vector_index_inline_delta, iceberg_read, flush_vector_rebuild}.rs` and `engine-serving/tests/{inline_vector_sql, vector_search, vector_search_identity_kinds, vector_index_auto_rebuild}.rs` calls `land(..., ipc_body, ...)`. Introduce a tiny per-file (or shared) helper that decodes the test's IPC bytes once and passes `(schema, batches)`, OR change each call to build batches directly. Prefer a shared test helper: these tests already construct IPC bodies with a common pattern — add a `decode_ipc`-based shim so the diff is mechanical. (The `road-test-wire-harness` item will later hoist these; here, keep the change minimal and local.)

- [ ] **Step 6: Build + test the full affected fixture set.** This is the blast-radius step. Run (fixture tests route local automatically):

```bash
buck2 build -M none //src/control-plane/postgres:postgres //src/services/ingest:ingest \
  //src/services/engine-serving:engine-serving 2>&1 | tail -5
buck2 test //src/control-plane/postgres/... //src/services/ingest/... \
  //src/services/engine-serving/... > /tmp/c3.log 2>&1; grep -E "Tests finished|FAIL" /tmp/c3.log
```

  Expect clean build + all green. If disk pressure appears, `buck2 clean` and test the postgres and engine-serving subtrees separately.

- [ ] **Step 7: Commit + Phase C verification.**

```bash
git add src/control-plane/postgres/src/iceberg_landing.rs src/control-plane/postgres/tests \
  src/services/ingest src/services/engine-serving
git commit -m "refactor(landing): land takes decoded batches; delete the double IPC decode"
./tools/clippy-all.sh > /tmp/phaseC-clippy.log 2>&1; tail -5 /tmp/phaseC-clippy.log
buck2 clean
```

---

## Final verification (all phases)

- [ ] Run the union of affected subtrees once more, green:

```bash
buck2 test //src/control-plane/core/... //src/services/worker/... //src/services/transform/... \
  //src/services/datafusion-io/... //src/services/engine/... //src/services/engine-serving/... \
  //src/services/store-config/... //src/services/ingest/... //src/control-plane/postgres/... \
  //src/services/query-api:iceberg-mirror-provider //src/services/query-api:iceberg-pruning-e2e \
  > /tmp/final.log 2>&1; grep -E "Tests finished|FAIL" /tmp/final.log
```

- [ ] `./tools/clippy-all.sh` clean.
- [ ] `buck2 run //tools:prek -- run --all-files` clean (markdown/formatting hooks on the plan + any touched files).
- [ ] `loom-docs-update`: mark `road-dead-path-sweep` done (`- [ ]`→`- [x]`, `status:done`, `pr:#N`) and `iss-transform-catalog-local-only` fixed (`status:fixed`, `pr:#N`).
- [ ] Open the PR with head `work/road-dead-path-sweep`.

## Notes on descoping (if the session cannot complete all three phases)

Each phase is independently valuable and independently green. If time/space forces a stop, commit and PR the completed phases, and **do not** mark `road-dead-path-sweep` done — leave it `- [ ]` with a PR note listing the remaining phase(s). A partially-delivered sweep is still a net win; a half-compiled tree is not. Never leave the branch in a non-building state at a phase boundary.
