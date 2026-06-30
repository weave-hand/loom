# Apply `LOOM_WRITE_*` to transform output Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Thread the existing `datafusion_io::WriteConfig` (the `LOOM_WRITE_*` seam) through the transform binary so derived-dataset output honors the operator's write-tuning knobs instead of a hardcoded `WriteConfig::default()`.

**Architecture:** Mirror `IngestConfig` (`src/services/ingest/src/config.rs`): the transform binary's `TransformConfig` composes a `write: datafusion_io::WriteConfig` field (defaults < file < env, validated at startup), and a borrowed `&WriteConfig` is threaded down through `transform_handler` / `typed_transform_handler` → `run_transform` / `run_typed_transform` → the single `write_dataset` call. Behavior is byte-identical when no `LOOM_WRITE_*` is set (the threaded value equals the previous hardcoded default).

**Tech Stack:** Rust 2024, buck2 (`rust_test` integration targets — NEVER inline `#[cfg(test)]`), `datafusion_io::WriteConfig`, `loom_config::LayeredConfig`, the `loom_fixture_test` macro for Postgres-backed e2e tests.

## Global Constraints

- **Tests are `rust_test` integration targets only** — put any new test in a `tests/<name>.rs` file wired in `BUCK`, never an inline `#[cfg(test)] mod tests`. The `no-inline-tests` prek hook fails on a first-party `src/**.rs` `#[test]`.
- **Fixture (Postgres-backed) tests use `loom_fixture_test`, not bare `rust_test`** — they boot `initdb`/`postgres` and must route to local execution.
- **Behavior-preserving by default:** with no `LOOM_WRITE_*` set, the composed `WriteConfig` equals `WriteConfig::default()`, so existing transforms write byte-identically. This is NOT a default-value change.
- **Clippy is strict (pedantic + restriction enforced).** No new `#[allow]` is needed for argument count: `run_transform` goes 5→6 args (under the 7 threshold), and `run_typed_transform` already carries `#[allow(clippy::too_many_arguments)]`.
- **Out of scope:** the compaction path (`compact_table`/`CompactConfig` already threads `cfg.write`); `fut-transform-backoff-unify`; any new `LOOM_WRITE_*` knob; changing `WriteConfig`'s own defaults.

## Canonical argument order

To keep the test call-site ripple mechanical, the new `write: &WriteConfig`
parameter is inserted at a **single consistent position: immediately after
`root_url`** (grouping the four shared deps `cp, store, root_url, write` first,
then the per-call request args). Every signature below follows this rule:

- `run_transform(cp, store, root_url, write, run_id, req)`
- `run_typed_transform(cp, store, root_url, write, run_id, inputs, output, sql, output_mode)`
- `transform_handler(cp, store, root_url, write, job)`
- `typed_transform_handler(cp, store, root_url, write, job)`

## File Structure

**Production source (the threaded seam):**
- `src/services/transform/src/run.rs` — `run_transform` gains `write: &WriteConfig`; the `write_dataset` call at `run.rs:182` passes `write` instead of `&WriteConfig::default()`.
- `src/services/transform/src/typed.rs` — `run_typed_transform` gains `write: &WriteConfig`, forwards it to `run_transform`.
- `src/services/transform/src/handler.rs` — both handlers gain `write: &WriteConfig`, forward it down.
- `src/services/transform/src/main.rs` — `TransformConfig` gains `write: datafusion_io::WriteConfig` (overlay + validate chained); the worker closure captures `tcfg.write` and passes `&write` to both handler calls.
- `src/services/transform/BUCK` — `transform-bin` gains a `//src/services/datafusion-io:datafusion-io` dep (so `main.rs` can name `datafusion_io::WriteConfig`); `run-unknown-input` test target gains the same dep.

**Test call-site ripple (mechanical — pass `&WriteConfig::default()`):**
- `src/services/transform/tests/transform_e2e.rs` (2 `run_transform` + 1 `transform_handler` call) — and the **new tuning test** added here.
- `src/services/transform/tests/overwrite_e2e.rs` (1 `transform_handler`).
- `src/services/transform/tests/run_unknown_input.rs` (1 `run_transform`).
- `src/services/transform/tests/iceberg_backend_e2e.rs` (3 `run_transform`).
- `src/services/transform/tests/typed_transform_e2e.rs` (1 `typed_transform_handler` + 1 `transform_handler`).

---

### Task 1: Thread `&WriteConfig` through the transform library primitives

This task changes the four library functions' signatures and the single
`write_dataset` call, then fixes the in-crate test call sites so
`buck2 build`/`test` of the `transform` crate compiles. It is one task because
the signature change and its call-site ripple cannot compile independently.

**Files:**
- Modify: `src/services/transform/src/run.rs`
- Modify: `src/services/transform/src/typed.rs`
- Modify: `src/services/transform/src/handler.rs`
- Modify: `src/services/transform/tests/transform_e2e.rs` (call-site ripple only; the new test is Task 3)
- Modify: `src/services/transform/tests/overwrite_e2e.rs`
- Modify: `src/services/transform/tests/run_unknown_input.rs`
- Modify: `src/services/transform/tests/iceberg_backend_e2e.rs`
- Modify: `src/services/transform/tests/typed_transform_e2e.rs`
- Modify: `src/services/transform/BUCK` (add `datafusion-io` dep to `run-unknown-input`)

**Interfaces:**
- Produces (new signatures other code consumes):
  - `pub async fn run_transform(cp: &dyn ControlPlane, store: Arc<dyn ObjectStore>, root_url: &str, write: &WriteConfig, run_id: &str, req: TransformRequest<'_>) -> Result<SnapshotId, TransformError>`
  - `pub async fn run_typed_transform(cp: &dyn ControlPlane, store: Arc<dyn ObjectStore>, root_url: &str, write: &WriteConfig, run_id: &str, inputs: &[TypeName], output: &TypeName, sql: &str, output_mode: crate::run::OutputMode) -> Result<SnapshotId, TypedTransformError>`
  - `pub async fn transform_handler(cp: &dyn ControlPlane, store: Arc<dyn ObjectStore>, root_url: &str, write: &WriteConfig, job: Job) -> Result<(), JobFailure>`
  - `pub async fn typed_transform_handler(cp: &dyn ControlPlane, store: Arc<dyn ObjectStore>, root_url: &str, write: &WriteConfig, job: Job) -> Result<(), JobFailure>`
- Consumes: `datafusion_io::WriteConfig` (already a transitive dep of the `transform` lib via `datafusion-io`).

- [ ] **Step 1: `run.rs` — add the parameter and use it**

In `src/services/transform/src/run.rs`, change the `run_transform` signature
(currently `pub async fn run_transform(cp, store, root_url, run_id, req)`) to
insert `write: &WriteConfig` after `root_url`:

```rust
pub async fn run_transform(
    cp: &dyn ControlPlane,
    store: Arc<dyn ObjectStore>,
    root_url: &str,
    write: &WriteConfig,
    run_id: &str,
    req: TransformRequest<'_>,
) -> Result<SnapshotId, TransformError> {
```

Then replace the hardcoded default at the `write_dataset` call (currently
`run.rs:182`):

```rust
    let written = write_dataset(store, &dir_prefix, schema, &batches, write).await?;
```

`WriteConfig` is already imported in `run.rs` (the `use datafusion_io::{WriteConfig, ...}` line), so no import change is needed.

- [ ] **Step 2: `typed.rs` — add the parameter and forward it**

In `src/services/transform/src/typed.rs`, add `use datafusion_io::WriteConfig;`
to the imports, insert `write: &WriteConfig` after `root_url` in
`run_typed_transform`, and forward it to the `run_transform` call:

```rust
pub async fn run_typed_transform(
    cp: &dyn ControlPlane,
    store: Arc<dyn ObjectStore>,
    root_url: &str,
    write: &WriteConfig,
    run_id: &str,
    inputs: &[TypeName],
    output: &TypeName,
    sql: &str,
    output_mode: crate::run::OutputMode,
) -> Result<SnapshotId, TypedTransformError> {
```

The existing `#[allow(clippy::too_many_arguments, reason = "...")]` on this
function stays. In its body, change the delegating call:

```rust
    run_transform(
        cp,
        store,
        root_url,
        write,
        run_id,
        TransformRequest {
            inputs: &specs,
            output: &out_type.table,
            sql,
            conform: Some(&out_type.properties),
            output_mode,
            lineage,
        },
    )
    .await
    .map_err(TypedTransformError::Transform)
```

- [ ] **Step 3: `handler.rs` — add the parameter to both handlers and forward it**

In `src/services/transform/src/handler.rs`, add `use datafusion_io::WriteConfig;`
to the imports. For `transform_handler`, insert `write: &WriteConfig` after
`root_url`:

```rust
pub async fn transform_handler(
    cp: &dyn ControlPlane,
    store: Arc<dyn ObjectStore>,
    root_url: &str,
    write: &WriteConfig,
    job: Job,
) -> Result<(), JobFailure> {
```

and forward it in the `run_transform` call (insert `write` after `root_url`):

```rust
    let res = run_transform(
        cp,
        store,
        root_url,
        write,
        &run_id,
        TransformRequest {
            inputs: &inputs,
            output: &output,
            sql: &payload.sql,
            conform: None,
            output_mode: payload.output_mode,
            lineage,
        },
    )
    .await;
```

For `typed_transform_handler`, insert `write: &WriteConfig` after `root_url`:

```rust
pub async fn typed_transform_handler(
    cp: &dyn ControlPlane,
    store: Arc<dyn ObjectStore>,
    root_url: &str,
    write: &WriteConfig,
    job: Job,
) -> Result<(), JobFailure> {
```

and forward it in the `run_typed_transform` call (insert `write` after `root_url`):

```rust
    let res = run_typed_transform(
        cp,
        store,
        root_url,
        write,
        &run_id,
        &inputs,
        &output,
        &payload.sql,
        payload.output_mode,
    )
    .await;
```

- [ ] **Step 4: Ripple the in-crate test call sites — `transform_e2e.rs`**

In `src/services/transform/tests/transform_e2e.rs`, add the import near the top
(after the existing `use` block, e.g. below `use transform::transform_handler;`):

```rust
use datafusion_io::WriteConfig;
```

At the `transform_handler` call inside the worker closure (currently
`transform_e2e.rs:122`), insert `&WriteConfig::default()` after `&root_url`:

```rust
                async move {
                    transform_handler(cp.as_ref(), store, &root_url, &WriteConfig::default(), job)
                        .await
                }
```

At both `transform::run_transform(...)` calls (currently `transform_e2e.rs:223`
and `:274`), insert `&WriteConfig::default()` after the `&format!("file://{warehouse}")`
`root_url` argument and before the `run_id` string. Example for the first:

```rust
    transform::run_transform(
        &cp,
        store.clone(),
        &format!("file://{warehouse}"),
        &WriteConfig::default(),
        "run-empty-count",
        transform::TransformRequest {
```

Apply the identical edit to the second call (its `run_id` is `"run-empty-star"`).

- [ ] **Step 5: Ripple `overwrite_e2e.rs`**

In `src/services/transform/tests/overwrite_e2e.rs`, add `use datafusion_io::WriteConfig;`
and insert `&WriteConfig::default()` after `&root_url` in the `transform_handler`
call (currently `overwrite_e2e.rs:53`):

```rust
                async move {
                    transform_handler(cp.as_ref(), store, &root_url, &WriteConfig::default(), job)
                        .await
                }
```

- [ ] **Step 6: Ripple `run_unknown_input.rs` (+ its BUCK dep)**

In `src/services/transform/tests/run_unknown_input.rs`, add `use datafusion_io::WriteConfig;`
to the imports and insert `&WriteConfig::default()` after the `root_url`
argument in the `run_transform` call (currently `run_unknown_input.rs:107`).
Inspect the existing argument list and place `&WriteConfig::default()`
immediately after the `root_url` string/expression and before the `run_id`.

This test target does NOT currently depend on `datafusion-io`, so in
`src/services/transform/BUCK`, add the dep to the `run-unknown-input`
`rust_test` target's `deps` list:

```python
        "//src/services/datafusion-io:datafusion-io",
```

(Add it in alphabetical-ish position, e.g. after `"//src/control-plane/core:core",`.)

- [ ] **Step 7: Ripple `iceberg_backend_e2e.rs`**

In `src/services/transform/tests/iceberg_backend_e2e.rs`, ensure
`use datafusion_io::WriteConfig;` is present (add it if not — this target
already depends on `//src/services/datafusion-io:datafusion-io`). Insert
`&WriteConfig::default()` after the `root_url` argument in all THREE
`run_transform` calls (currently `:138`, `:163`, `:257`). Read each call's
argument list and place the new arg immediately after its `root_url` and
before its `run_id`.

- [ ] **Step 8: Ripple `typed_transform_e2e.rs`**

In `src/services/transform/tests/typed_transform_e2e.rs`, add
`use datafusion_io::WriteConfig;` and insert `&WriteConfig::default()` after
`&root_url` in BOTH calls inside the worker closure (currently
`typed_transform_e2e.rs:82` `typed_transform_handler` and `:84`
`transform_handler`):

```rust
                                typed_transform_handler(cp.as_ref(), store, &root_url, &WriteConfig::default(), job).await
```
```rust
                            _ => transform_handler(cp.as_ref(), store, &root_url, &WriteConfig::default(), job).await,
```

- [ ] **Step 9: Build the transform crate + run the existing tests to verify the ripple compiles and stays green**

Run (redirect to a file — never pipe `buck2 test` through `tail`/`head`):

```bash
buck2 build //src/services/transform/... > /tmp/t1-build.log 2>&1; grep -iE "error|BUILD SUCCEEDED|Build ID" /tmp/t1-build.log | tail -20
buck2 test //src/services/transform/... > /tmp/t1-test.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/t1-test.log | tail -20
```

Expected: build succeeds; all existing transform tests pass (behavior unchanged
because every call passes `&WriteConfig::default()`, identical to the prior
hardcoded value).

- [ ] **Step 10: Commit**

```bash
git add src/services/transform/src/run.rs src/services/transform/src/typed.rs \
        src/services/transform/src/handler.rs src/services/transform/BUCK \
        src/services/transform/tests/transform_e2e.rs \
        src/services/transform/tests/overwrite_e2e.rs \
        src/services/transform/tests/run_unknown_input.rs \
        src/services/transform/tests/iceberg_backend_e2e.rs \
        src/services/transform/tests/typed_transform_e2e.rs
git commit -m "refactor(transform): thread &WriteConfig through run_transform and handlers"
```

---

### Task 2: Compose `WriteConfig` in the transform binary

Wire the seam at the binary's edge: `TransformConfig` gains the `write` field so
`LOOM_WRITE_*` is composed and validated at startup, and the worker closure
passes it to the handlers.

**Files:**
- Modify: `src/services/transform/src/main.rs`
- Modify: `src/services/transform/BUCK` (add `datafusion-io` dep to `transform-bin`)

**Interfaces:**
- Consumes: the Task 1 handler signatures (`transform_handler`/`typed_transform_handler` now take `write: &WriteConfig` after `root_url`).
- Produces: a `transform-bin` whose output honors `LOOM_WRITE_TARGET_FILE_BYTES` / `LOOM_WRITE_MAX_FILES` / `LOOM_WRITE_COMPRESSION_FACTOR` and fails startup on a malformed value.

- [ ] **Step 1: Add the `datafusion-io` dep to `transform-bin`**

In `src/services/transform/BUCK`, add to the `transform-bin` `rust_binary`'s
`deps` list:

```python
        "//src/services/datafusion-io:datafusion-io",
```

(e.g. after `":transform",`.) This lets `main.rs` name `datafusion_io::WriteConfig`.

- [ ] **Step 2: Add the `write` field to `TransformConfig` and chain it**

In `src/services/transform/src/main.rs`, extend the struct and its
`LayeredConfig` impl (mirroring `IngestConfig`):

```rust
#[derive(Default, serde::Deserialize)]
#[serde(default)]
struct TransformConfig {
    worker: loom_config::WorkerTuning,
    write: datafusion_io::WriteConfig,
}

impl loom_config::LayeredConfig for TransformConfig {
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

- [ ] **Step 3: Capture `tcfg.write` and pass `&write` to both handlers**

In `main()`, after `let tcfg: TransformConfig = service_runtime::load(&env)?;`
is available and the closure deps are set up, bind the composed write config
alongside `store`/`root_url` so it can be moved into the worker closure. Change
the closure block (currently capturing `store`, `root_url`) to also capture the
write config and pass `&write` to each handler call:

```rust
    let store: Arc<dyn ObjectStore> = write.store.clone();
    let root_url = write.root_url.clone();
    let write_cfg = tcfg.write;
```

Note the existing binding `let write = service_runtime::build_write_store(...)`
already uses the name `write` for the **object store**; to avoid a name clash,
name the composed config `write_cfg`. Then in the worker closure:

```rust
            move |job: Job| {
                let cp = cp_for_handler.clone();
                let store = store.clone();
                let root_url = root_url.clone();
                let write_cfg = write_cfg.clone();
                async move {
                    match job.kind.as_str() {
                        "typed-transform" => {
                            typed_transform_handler(cp.as_ref(), store, &root_url, &write_cfg, job)
                                .await
                        }
                        "transform" => {
                            transform_handler(cp.as_ref(), store, &root_url, &write_cfg, job).await
                        }
                        other => Err(JobFailure {
                            error: format!("unknown job kind: {other}"),
                            policy: RetryPolicy::Abandon,
                        }),
                    }
                }
            },
```

(`WriteConfig` derives `Clone`, so `write_cfg.clone()` per job is fine and
mirrors the existing `store.clone()` / `root_url.clone()` pattern.)

- [ ] **Step 4: Build the binary**

```bash
buck2 build //src/services/transform:transform-bin > /tmp/t2-build.log 2>&1; grep -iE "error|BUILD SUCCEEDED|Build ID" /tmp/t2-build.log | tail -20
```

Expected: build succeeds.

- [ ] **Step 5: Clippy-check the binary and library**

```bash
buck2 build '//src/services/transform:transform-bin[clippy.txt]' '//src/services/transform:transform[clippy.txt]' > /tmp/t2-clippy.log 2>&1; grep -iE "error|warning|FAILED|SUCCEEDED" /tmp/t2-clippy.log | tail -20
```

Expected: clean (the `[clippy.txt]` sub-target output is empty on success).

- [ ] **Step 6: Commit**

```bash
git add src/services/transform/src/main.rs src/services/transform/BUCK
git commit -m "feat(transform): compose LOOM_WRITE_* WriteConfig in the transform binary"
```

---

### Task 3: Tuning fixture test — prove the knob reaches `write_dataset`

Add the integration test the spec requires: a transform run with a **non-default**
`WriteConfig` produces a different committed output-file layout than the same
transform under `WriteConfig::default()` — the observable that was silently fixed
at the default before.

**Files:**
- Modify: `src/services/transform/tests/transform_e2e.rs` (add one `#[tokio::test]` fn; this file is already wired in the `transform-e2e` `loom_fixture_test` target with the `datafusion-io` dep and the `WriteConfig` import from Task 1).

**Interfaces:**
- Consumes: `transform::run_transform` (Task 1 signature, `write: &WriteConfig` after `root_url`); the `transform_e2e_support` helpers `seed_table`, `cols`, `tref`, `lineage`; `control_plane_core::PageReq`.
- Produces: nothing downstream — a leaf test.

**Test design rationale (why it is deterministic — CPU-count independent):**
`write_dataset` builds its OWN `SessionContext` and wraps the collected `batches`
in a single-partition `MemTable` (`vec![batches.to_vec()]`, `write.rs:157`), then
sets `minimum_parallel_output_files = estimate_partitions(in_memory_bytes, cfg)`
and lets the parquet sink round-robin **whole batches** across that many writers
(`write.rs:136-169`). So the output file count is
`min(estimate_partitions, number_of_batches_run_transform_collected)`. Two levers:

- (a) `estimate_partitions > 1` — achieved with a tuned
  `WriteConfig { target_file_size_bytes: 1, max_files: 8, compression_factor: 1.0 }`
  (tiny target ⇒ clamps to `max_files = 8`); under `WriteConfig::default()` the
  same input has `estimate_partitions == 1` ⇒ exactly one output file.
- (b) `run_transform`'s `df.collect()` yielding **≥2 batches** — guaranteed *by
  construction, not by host CPU count*, by seeding the input with **more than
  `batch_size` (8192) rows**. DataFusion's scan emits at most `batch_size` rows
  per `RecordBatch` and nothing in a `SELECT *` plan merges batches beyond
  `batch_size`, so `collect()` always returns ≥ `ceil(rows / 8192)` batches
  regardless of `target_partitions`/core count. Seeding 20 000 rows ⇒ ≥3 batches
  on any executor.

This is **stronger than** the sibling `write_dataset_splits_into_multiple_files`
test (which hand-builds batches) and deliberately avoids the CPU-coupled
"4 files ⇒ 4 scan partitions ⇒ 4 batches" assumption (a single-core runner would
collapse a multi-file scan to one batch). Result, on any runner:
default ⇒ exactly 1 file, tuned ⇒ ≥2 files.

- [ ] **Step 1: Write the failing test**

Append this test to `src/services/transform/tests/transform_e2e.rs`. It seeds one
input table as four single-row files, runs the same `SELECT *` transform twice
(tuned vs default) into two output tables, and asserts the committed file counts
differ as designed. `WriteConfig` is already imported (Task 1, Step 4).

```rust
/// The transform's write tuning reaches `write_dataset`: a non-default
/// `WriteConfig` (tiny target size, many files) splits the output across
/// multiple data files, while the SAME transform under `WriteConfig::default()`
/// commits a single file. This is the observable that was hardcoded to the
/// default before `road-transform-write-tuning`.
#[tokio::test(flavor = "multi_thread")]
async fn write_config_controls_transform_output_file_count() {
    let fx = PgFixture::start();
    let (pg, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let warehouse = wh.path().display().to_string();
    let root_url = format!("file://{warehouse}");
    let catalog = make_catalog(fx.pg_dsn(&db), &warehouse).await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(wh.path()).expect("store"));
    let cp = IcebergControlPlane::new(pg.clone(), catalog);

    // Seed the input with > batch_size (8192) rows so `run_transform`'s
    // `df.collect()` yields >= 2 record batches BY CONSTRUCTION (DataFusion caps a
    // batch at `batch_size` rows; nothing in a `SELECT *` plan merges beyond it),
    // independent of host CPU count / `target_partitions`. The parquet sink in
    // `write_dataset` round-robins those batches across up to
    // `minimum_parallel_output_files` writers, so a tuned tiny-target config splits
    // the output while the default coalesces it to one file.
    let src = tref("main", "tuning_src");
    let src_cols = cols(&[("id", "long", false)]);
    let src_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let ids: Vec<i64> = (0..20_000_i64).collect();
    seed_table(
        &cp,
        &store,
        &src,
        &src_cols,
        src_schema.clone(),
        RecordBatch::try_new(src_schema.clone(), vec![Arc::new(Int64Array::from(ids))]).unwrap(),
        "seed-0",
    )
    .await;

    // Helper: run `SELECT * FROM tuning_src` into `out` with `write`, return the
    // committed output data-file count.
    async fn run_into(
        cp: &IcebergControlPlane,
        store: &Arc<dyn ObjectStore>,
        root_url: &str,
        src: &TableRef,
        out: &TableRef,
        write: &WriteConfig,
        run_id: &str,
    ) -> usize {
        let mut ev = lineage(out);
        ev.inputs = vec![DatasetRef::from(src)];
        transform::run_transform(
            cp,
            store.clone(),
            root_url,
            write,
            run_id,
            transform::TransformRequest {
                inputs: &[transform::TransformInput {
                    table: src,
                    register_as: "tuning_src",
                }],
                output: out,
                sql: "SELECT * FROM tuning_src",
                conform: None,
                output_mode: transform::OutputMode::Append,
                lineage: ev,
            },
        )
        .await
        .expect("transform commits");
        let snap = cp.catalog().current_snapshot(out).await.unwrap().id;
        cp.catalog()
            .files(out, snap, PageReq::unbounded())
            .await
            .unwrap()
            .items
            .len()
    }

    let tuned = WriteConfig {
        target_file_size_bytes: 1,
        max_files: 8,
        compression_factor: 1.0,
    };
    let tuned_out = tref("main", "tuned_out");
    let tuned_count =
        run_into(&cp, &store, &root_url, &src, &tuned_out, &tuned, "run-tuned").await;

    let default_out = tref("main", "default_out");
    let default_count = run_into(
        &cp,
        &store,
        &root_url,
        &src,
        &default_out,
        &WriteConfig::default(),
        "run-default",
    )
    .await;

    assert_eq!(
        default_count, 1,
        "the default WriteConfig coalesces the input to a single file"
    );
    assert!(
        tuned_count >= 2,
        "the tuned WriteConfig splits the output across multiple files (got {tuned_count})"
    );
    assert!(
        tuned_count > default_count,
        "tuning changed the layout (tuned={tuned_count}, default={default_count})"
    );
}
```

This test references `DatasetRef`, `TableRef`, `PageReq`, `Field`, `DataType`,
`Int64Array`, `RecordBatch`, `Schema`, `LocalFileSystem`, `IcebergControlPlane`,
`PgFixture` — all already imported at the top of `transform_e2e.rs` and re-used
from `transform_e2e_support` (`cols`, `lineage`, `make_catalog`, `seed_table`,
`tref`). If the compiler reports any of these unresolved, add the matching
`use` (e.g. `control_plane_core::PageReq` is already imported; `DatasetRef` is
imported via the existing `use control_plane_core::{... DatasetRef ...}` line).

- [ ] **Step 2: Run the new test to verify it passes**

```bash
buck2 test //src/services/transform:transform-e2e > /tmp/t3-test.log 2>&1; grep -E "Tests finished|FAIL|PASS|write_config_controls" /tmp/t3-test.log | tail -20
```

Expected: the suite passes, including `write_config_controls_transform_output_file_count`.

> **TDD note:** the "failing" state for this task is established before Task 1
> lands — without the threaded `write`, `run_transform` would not accept the
> tuned config and the assertion (`tuned_count > default_count`) could not even
> be expressed. Since Tasks 1–2 are a prerequisite for the signature, this test
> is written against the new signature and verified green here. If implementing
> strictly test-first within this task, first stub the assertion to expect the
> OLD behavior (both counts equal 1) to confirm the harness runs, then flip to
> the real assertion above.

- [ ] **Step 3: Commit**

```bash
git add src/services/transform/tests/transform_e2e.rs
git commit -m "test(transform): prove LOOM_WRITE_* reaches transform output file layout"
```

---

### Task 4: Full-suite verification + register update

**Files:**
- Modify: `docs/ROADMAP.md` (close the register item via `loom-docs-update`).

- [ ] **Step 1: Run the whole transform suite + a broad build to catch any missed call site**

```bash
buck2 build //src/services/transform/... //src/services/ingest/... > /tmp/t4-build.log 2>&1; grep -iE "error|SUCCEEDED|Build ID" /tmp/t4-build.log | tail -20
buck2 test //src/services/transform/... > /tmp/t4-test.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t4-test.log | tail -20
```

Expected: build succeeds; tests finish with zero failures.

- [ ] **Step 2: Clippy across first-party Rust**

```bash
buck2 build '//src/services/transform:transform[clippy.txt]' '//src/services/transform:transform-bin[clippy.txt]' > /tmp/t4-clippy.log 2>&1; cat /tmp/t4-clippy.log | tail -20
```

Expected: clean.

- [ ] **Step 3: Run the prek hooks (rustfmt, trailing-whitespace, EOF, no-inline-tests, clippy) and commit any fixups**

```bash
buck2 run //tools:prek -- run --all-files > /tmp/t4-prek.log 2>&1; tail -40 /tmp/t4-prek.log
```

Expected: all hooks pass. If a hook rewrote a file, `git add` + commit the fixups.

- [ ] **Step 4: Close the register item**

Use the `loom-docs-update` skill (or edit directly) to flip
`docs/ROADMAP.md`'s `road-transform-write-tuning` entry from `- [ ]` to `- [x]`,
set `status:done`, and record `pr:#<N>` once the PR number is known. Validate:

```bash
bash tools/docs.sh validate > /tmp/t4-docs.log 2>&1; tail -5 /tmp/t4-docs.log
```

Expected: validation passes.

- [ ] **Step 5: Commit the register update**

```bash
git add docs/ROADMAP.md
git commit -m "docs(transform): mark road-transform-write-tuning done"
```

---

## Self-Review

**1. Spec coverage:**
- Spec §1 "Compose `WriteConfig` in the transform binary" → Task 2 (struct field + overlay/validate chaining, byte-for-byte the `IngestConfig` pattern). ✓
- Spec §2 "Thread `&WriteConfig` to the write call" (`run_transform`, `transform_handler`, `typed_transform_handler`) → Task 1 (and `run_typed_transform`, which the spec implies via the typed handler delegating through it). ✓
- Spec §3 "Ripple to test call sites" → Task 1 Steps 4–8 (all eight non-doc call sites enumerated: 2+1 in transform_e2e, 1 overwrite, 1 run_unknown_input, 3 iceberg_backend, 2 typed). ✓
- Spec "Testing" (tuning fixture test, file-count observable) → Task 3. ✓
- Spec scope: compaction untouched ✓; no new knob ✓; defaults unchanged ✓.

**2. Placeholder scan:** No TBD/TODO; every code step shows the actual code; the test body is complete. The only "inspect and place" instructions (run_unknown_input/iceberg_backend call sites) are necessary because those exact lines aren't quoted here — but the rule (insert `&WriteConfig::default()` immediately after `root_url`) is unambiguous and the canonical argument order is fixed up front.

**3. Type consistency:** `write: &WriteConfig` placed immediately after `root_url` in all four signatures consistently; `WriteConfig` (not `&WriteConfig`) stored in `TransformConfig`; `write_cfg` chosen to avoid clashing with the existing `write` object-store binding in `main.rs`; `run_into` helper returns `usize` (matches `Vec::len`); `tuned_count`/`default_count` typed `usize`. The new BUCK dep (`datafusion-io`) is added to exactly the two targets that newly name `WriteConfig` directly: `transform-bin` (Task 2) and `run-unknown-input` (Task 1) — the other test targets already carry it.
