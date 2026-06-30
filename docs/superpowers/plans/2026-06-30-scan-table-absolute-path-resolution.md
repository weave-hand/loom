# `scan_table` Absolute-Path Resolution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `datafusion_io::scan_table` correctly read a table whose `FileRef.path`s are **absolute** mirror URLs (`file://…` / `s3://bucket/…`), so a transform that reads another transform's (or compaction's) output no longer fails at DataFusion scan time.

**Architecture:** Relocate the proven `object_store_url_for` resolver out of `engine-serving` into `datafusion-io` as a `pub` helper (its natural home — `scan.rs`/`write.rs` own the warehouse path layout). `engine-serving` then depends on `datafusion-io` and drops its private copy. `scan_table` gains a per-`FileRef` branch: a path containing `"://"` is absolute — used verbatim as the `ListingTableUrl` with its derived `ObjectStoreUrl` registered (local `LocalFileSystem` for `file://`/local, the passed warehouse `store` under `s3://{bucket}`); a path without `"://"` keeps today's relative behavior (`{LOOM_STORE_URL}/{schema}/{table}/{path}` registered under `LOOM_STORE_URL`). Object stores are registered idempotently, once per distinct `ObjectStoreUrl`.

**Tech Stack:** Rust 2024, buck2, DataFusion 54, `object_store` 0.13, Arrow 58, hermetic Postgres fixture tests (`loom_fixture_test`).

## Global Constraints

- **Tests are `rust_test` integration targets only — never inline `#[cfg(test)]`.** The new fixture test is a `loom_fixture_test` target in `src/services/transform/BUCK`. (`tools/check-inline-tests.sh` enforces this.)
- **`scan_table`'s signature is unchanged:** `scan_table(ctx, store, name, table, files)`. The two callers (`transform/src/run.rs`, `transform/src/compact.rs`) pass `store.clone()` and must need **no** change.
- **`FileRef` is NOT extended** to carry `path_is_relative`. Absolute-vs-relative is inferred from the path string (`"://"` presence) — the same approach the serving engine already uses. (Out of scope per spec.)
- **`datafusion-io` is a buck-only crate** — it has **no `Cargo.toml`** and is **not** a Cargo workspace member. Existing workspace-member consumers (`ingest`, `worker`) buck-depend on it **without** listing it in their `Cargo.toml`. Therefore the `engine-serving` → `datafusion-io` dependency is added to **`engine-serving/BUCK` deps only**; **do NOT** add it to `engine-serving/Cargo.toml` (a `path` dep to a manifest-less directory would break `cargo metadata` / `./tools/buckify.sh`). This is a deliberate, verified deviation from the spec's "add to Cargo.toml" wording — see Task 2.
- **No `./tools/buckify.sh` run is needed** — no `Cargo.toml` / `Cargo.lock` changes, so `third-party/BUCK` does not change.
- **Clippy is strict** (pedantic + restriction). New code must avoid `unwrap`/`expect`/`panic`/indexing-slicing in library code, carry `#[expect(..., reason = "...")]` where a local allow is genuinely needed, and keep the existing `#[expect]`/doc-comment patterns.
- **Run the full suite before finishing:** `buck2 test //src/...` (fixture tests route local automatically via `loom_fixture_test`). Redirect to a file and grep — never pipe `buck2 test` through `tail`/`head`.

## Reference: current code

`src/services/datafusion-io/src/scan.rs` — `scan_table` today:

```rust
pub async fn scan_table(
    ctx: &SessionContext,
    store: Arc<dyn ObjectStore>,
    name: &str,
    table: &TableRef,
    files: &[FileRef],
) -> Result<(), ScanError> {
    let url = ObjectStoreUrl::parse(LOOM_STORE_URL)?;
    ctx.register_object_store(url.as_ref(), store.clone());

    let paths: Vec<ListingTableUrl> = files
        .iter()
        .map(|f| {
            let key = format!("{LOOM_STORE_URL}/{}/{}/{}", table.schema, table.name, f.path);
            ListingTableUrl::parse(key)
        })
        .collect::<Result<_, _>>()?;
    // ... ParquetFormat / ListingOptions / infer_schema / register_table ...
}
```

`src/services/engine-serving/src/serving.rs` — the resolver to relocate (private today):

```rust
/// The object-store URL a data file's absolute path resolves against. `s3://bucket/...`
/// => `s3://bucket`; everything else (absolute `file://`/local paths) => local filesystem.
fn object_store_url_for(path: &str) -> datafusion::error::Result<ObjectStoreUrl> {
    if let Some(rest) = path.strip_prefix("s3://") {
        let bucket = rest.split('/').next().unwrap_or("");
        ObjectStoreUrl::parse(format!("s3://{bucket}"))
    } else {
        Ok(ObjectStoreUrl::local_filesystem())
    }
}
```

`engine-serving`'s `register_iceberg_table` registers a fresh `LocalFileSystem` under `local_filesystem()` and (when an S3 warehouse is configured) the S3 store under `s3://{bucket}` — the dual-registration shape `scan_table` will mirror.

`LOOM_STORE_URL` is `pub(crate) const LOOM_STORE_URL: &str = "loom://data";` in `write.rs` and is imported into `scan.rs` via `use crate::write::LOOM_STORE_URL;`.

## File Structure

- **Modify** `src/services/datafusion-io/src/scan.rs` — add `pub fn object_store_url_for`; rewrite the path/store loop in `scan_table` to be scheme-aware.
- **Modify** `src/services/datafusion-io/src/lib.rs` — re-export `object_store_url_for` from `scan`.
- **Modify** `src/services/engine-serving/src/serving.rs` — delete the private `object_store_url_for`; import and use `datafusion_io::object_store_url_for`.
- **Modify** `src/services/engine-serving/BUCK` — add `//src/services/datafusion-io:datafusion-io` to the `engine-serving` library `deps`.
- **Modify** `src/services/datafusion-io/tests/scan.rs` — add a unit test for `object_store_url_for` (pure-logic, fast feedback on the resolver).
- **Create / Modify** `src/services/transform/tests/transform_chain_e2e.rs` — the new transform-reads-transform-output `loom_fixture_test`.
- **Modify** `src/services/transform/BUCK` — add the `transform-chain-e2e` `loom_fixture_test` target.
- **Modify** `docs/ISSUES.md` — close `iss-iceberg-transform-chain-path` (done at PR time via `loom-docs-update`, Task 6).

---

### Task 1: Add the scheme-aware unit test for `object_store_url_for` + relocate the resolver into `datafusion-io`

This task moves the resolver and proves it with a fast pure-logic test. It is a pure relocate of behavior — no `scan_table` logic change yet.

**Files:**
- Modify: `src/services/datafusion-io/src/scan.rs`
- Modify: `src/services/datafusion-io/src/lib.rs:14`
- Test: `src/services/datafusion-io/tests/scan.rs`

**Interfaces:**
- Produces: `pub fn object_store_url_for(path: &str) -> datafusion::error::Result<ObjectStoreUrl>` in `datafusion_io::scan`, re-exported as `datafusion_io::object_store_url_for`.
  - `"s3://bucket/a/b.parquet"` → `ObjectStoreUrl::parse("s3://bucket")`
  - `"file:///w/s/t/x.parquet"` → `ObjectStoreUrl::local_filesystem()`
  - any non-`s3://` string → `ObjectStoreUrl::local_filesystem()`

- [ ] **Step 1: Write the failing test** — append to `src/services/datafusion-io/tests/scan.rs`:

```rust
#[test]
fn object_store_url_for_s3_uses_bucket_authority() {
    let url = datafusion_io::object_store_url_for("s3://my-bucket/schema/table/part-0.parquet")
        .expect("s3 url parses");
    assert_eq!(url.as_str(), "s3://my-bucket/");
}

#[test]
fn object_store_url_for_file_uses_local_filesystem() {
    let url = datafusion_io::object_store_url_for("file:///warehouse/schema/table/part-0.parquet")
        .expect("file url resolves");
    assert_eq!(
        url.as_str(),
        datafusion::execution::object_store::ObjectStoreUrl::local_filesystem().as_str()
    );
}
```

> Note: `ObjectStoreUrl::as_str()` returns the normalized URL **with a trailing slash** (DataFusion normalizes `s3://my-bucket` to `s3://my-bucket/`). DataFusion 54 exposes `as_str()`; if a future bump removes it, compare via `format!("{url}")` (Display) instead — verify in Step 2 and adjust the assertion to whatever the type exposes. The behavioral assertion (s3 authority vs local) is what matters.

- [ ] **Step 2: Run the test to verify it fails to compile** (resolver not yet public):

```
buck2 test //src/services/datafusion-io:scan > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[|cannot find" /tmp/t.log
```

Expected: build failure — `object_store_url_for` is not found in `datafusion_io`.

- [ ] **Step 3: Relocate the resolver into `scan.rs`.** Add this `pub fn` to `src/services/datafusion-io/src/scan.rs` (above `scan_table`):

```rust
/// The object-store URL a data file's ABSOLUTE path resolves against.
/// `s3://bucket/...` => `s3://bucket`; everything else (absolute `file://` or local
/// filesystem paths) => the local filesystem store. Shared with the serving engine
/// (`engine-serving`), which registers data files by these same absolute mirror paths.
pub fn object_store_url_for(path: &str) -> datafusion::error::Result<ObjectStoreUrl> {
    if let Some(rest) = path.strip_prefix("s3://") {
        let bucket = rest.split('/').next().unwrap_or("");
        ObjectStoreUrl::parse(format!("s3://{bucket}"))
    } else {
        Ok(ObjectStoreUrl::local_filesystem())
    }
}
```

`ObjectStoreUrl` is already imported in `scan.rs` (`use datafusion::execution::object_store::ObjectStoreUrl;`). The `unwrap_or("")` is on `split('/').next()` which is infallible for a non-empty iterator, but `.next()` returns `Option`; `unwrap_or` is allowed (not `unwrap`). Keep it byte-identical to the serving copy so the relocate is provably behavior-preserving.

- [ ] **Step 4: Re-export it.** In `src/services/datafusion-io/src/lib.rs`, change the `scan` re-export line:

```rust
pub use scan::{ScanError, object_store_url_for, register_empty_table, scan_table};
```

- [ ] **Step 5: Run the test to verify it passes:**

```
buck2 test //src/services/datafusion-io:scan > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error" /tmp/t.log
```

Expected: PASS (the new tests + existing scan tests).

- [ ] **Step 6: Commit:**

```bash
git add src/services/datafusion-io/src/scan.rs src/services/datafusion-io/src/lib.rs src/services/datafusion-io/tests/scan.rs
git commit -m "refactor(datafusion-io): relocate object_store_url_for resolver as pub helper"
```

---

### Task 2: Point `engine-serving` at the shared resolver

Pure relocate completion: `engine-serving` drops its private copy and uses the shared one. Behavior identical.

**Files:**
- Modify: `src/services/engine-serving/src/serving.rs` (delete private `fn object_store_url_for`; add import; the single call site in `register_iceberg_table` is unchanged in spelling — `object_store_url_for(...)` now resolves to the imported one).
- Modify: `src/services/engine-serving/BUCK` (add `//src/services/datafusion-io:datafusion-io` to the `engine-serving` library `deps`).

**Interfaces:**
- Consumes: `datafusion_io::object_store_url_for` (Task 1).

> **DEVIATION FROM SPEC (verified):** The spec says add `datafusion-io` to `engine-serving`'s **`Cargo.toml` + `BUCK`**. But `datafusion-io` has no `Cargo.toml` and is not a workspace member; adding a `path` dep to it would break `cargo metadata`/`./tools/buckify.sh`. The established pattern (`ingest`, `worker` are workspace members that buck-depend on `datafusion-io` with **no** Cargo.toml entry) is **BUCK-dep only**. So: edit `engine-serving/BUCK` only, leave `engine-serving/Cargo.toml` untouched, and do **not** run buckify.

- [ ] **Step 1: Find the call site** to confirm only one use:

```
grep -n "object_store_url_for" src/services/engine-serving/src/serving.rs
```

Expected: the `fn object_store_url_for` definition (~line 56–65) and one call inside `IcebergMirrorTableProvider::scan` (it builds an `ObjectStoreUrl` per file). Note the exact call site(s) before editing.

- [ ] **Step 2: Add the BUCK dep.** In `src/services/engine-serving/BUCK`, add to the `engine-serving` `rust_library` `deps` list (keep alphabetical-ish ordering with the other `//src/...` deps, after `control-plane/postgres`):

```python
        "//src/services/datafusion-io:datafusion-io",
```

- [ ] **Step 3: Delete the private resolver and import the shared one.** In `src/services/engine-serving/src/serving.rs`, remove the entire private `fn object_store_url_for(...)` block (the doc comment + fn). Add an import near the other top-of-file `use` lines:

```rust
use datafusion_io::object_store_url_for;
```

The existing call sites `object_store_url_for(<path>)` now bind to the imported function — no call-site edits needed.

- [ ] **Step 4: Build engine-serving + its tests to verify the relocate compiles:**

```
buck2 build //src/services/engine-serving:engine-serving > /tmp/t.log 2>&1; grep -E "BUILD SUCCEEDED|FAIL|error\[" /tmp/t.log
```

Expected: BUILD SUCCEEDED. (If `error: unused import` fires because the call site was conditionally compiled, recheck Step 1's call-site count.)

- [ ] **Step 5: Clippy-clean check** for the edited crate:

```
buck2 build '//src/services/engine-serving:engine-serving[clippy.txt]' > /tmp/c.log 2>&1; cat $(buck2 build '//src/services/engine-serving:engine-serving[clippy.txt]' --show-output 2>/dev/null | awk '{print $2}') 2>/dev/null; grep -E "warning|error" /tmp/c.log || echo "clippy clean"
```

Expected: empty clippy output (clean). If the relocate left an unused `use` (e.g. `ObjectStoreUrl` only used by the deleted fn), remove it.

- [ ] **Step 6: Commit:**

```bash
git add src/services/engine-serving/src/serving.rs src/services/engine-serving/BUCK
git commit -m "refactor(engine-serving): use shared datafusion_io::object_store_url_for"
```

---

### Task 3: Make `scan_table` scheme-aware (the fix)

The behavior change. Branch per `FileRef` on `"://"`; register each distinct `ObjectStoreUrl` once.

**Files:**
- Modify: `src/services/datafusion-io/src/scan.rs` (the body of `scan_table` between the initial register and the `ParquetFormat` setup).

**Interfaces:**
- Consumes: `object_store_url_for` (Task 1), `LOOM_STORE_URL` (already imported).
- Produces: unchanged `scan_table` signature.

- [ ] **Step 1: Rewrite the register+path-build block.** Replace the current head of `scan_table` (the `let url = ObjectStoreUrl::parse(LOOM_STORE_URL)?; ctx.register_object_store(...)` line **and** the `let paths: Vec<ListingTableUrl> = ...` block) with a scheme-aware loop. New code:

```rust
    // Each data file is either RELATIVE (landed tables: `<run_id>/part.parquet`,
    // resolved under the loom virtual store) or ABSOLUTE (transform/compaction output
    // promoted by `absolute_data_files`: `file://…`/`s3://bucket/…`). `FileRef` drops
    // the authoritative `path_is_relative` flag on read-back, so infer from the path:
    // a `"://"` means an absolute URI. Register each distinct object store once.
    let mut registered: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut register_once = |ctx: &SessionContext, url: &ObjectStoreUrl, s: Arc<dyn ObjectStore>| {
        if registered.insert(url.as_str().to_string()) {
            ctx.register_object_store(url.as_ref(), s);
        }
    };

    let mut paths: Vec<ListingTableUrl> = Vec::with_capacity(files.len());
    for f in files {
        if f.path.contains("://") {
            // Absolute mirror path: use verbatim; register its derived store.
            let url = object_store_url_for(&f.path)?;
            let object_store: Arc<dyn ObjectStore> = if f.path.starts_with("s3://") {
                store.clone()
            } else {
                Arc::new(object_store::local::LocalFileSystem::new())
            };
            register_once(ctx, &url, object_store);
            paths.push(ListingTableUrl::parse(&f.path)?);
        } else {
            // Relative landed path: reconstruct `{LOOM_STORE_URL}/{schema}/{table}/{rel}`
            // and register the passed store under the loom virtual URL.
            let url = ObjectStoreUrl::parse(LOOM_STORE_URL)?;
            register_once(ctx, &url, store.clone());
            let key = format!(
                "{LOOM_STORE_URL}/{}/{}/{}",
                table.schema, table.name, f.path
            );
            paths.push(ListingTableUrl::parse(key)?);
        }
    }
```

Implementation notes for the engineer:
- `object_store::local::LocalFileSystem` — `object_store` is already a dep of `datafusion-io`; `use object_store::ObjectStore;` is already imported, so reference `LocalFileSystem` by full path `object_store::local::LocalFileSystem::new()` (matches how `engine-serving` constructs a fresh `LocalFileSystem`). A fresh local store is correct because absolute `file://` URLs carry the full path; the local store resolves from filesystem root.
- `ObjectStoreUrl::as_str()` (DataFusion 54 exposes `pub fn as_str(&self) -> &str` and `Display`) is used as the dedupe key. If for some reason `as_str()` is unavailable, fall back to `format!("{url}")` (via `Display`) — **not** `url.as_ref().to_string()`: `ObjectStoreUrl::as_ref()` returns `&Url`, not `&str` (that's the `&Url` serving.rs passes to `register_object_store`). `(&Url).to_string()` would still work as a key, but `format!("{url}")` is the clearer fallback. Verify in Step 3.
- The closure borrows `registered` mutably and is `FnMut`; it takes `ctx`/`url`/`store` as params to avoid capturing them, sidestepping borrow conflicts with the `paths` loop. If the borrow checker objects to the closure form, inline the dedupe with an explicit `if registered.insert(...)` at each branch instead — same behavior, no closure.
- Do **not** change the `ParquetFormat`/`ListingOptions`/`infer_schema`/`register_table` tail — it stays exactly as today.
- `ListingTableUrl::parse` takes `impl AsRef<str>`; passing `&f.path` (a `&String`) and `key` (a `String`) both work.

- [ ] **Step 2: Confirm callers are unchanged.** Verify no edits are needed in `transform/src/run.rs` or `transform/src/compact.rs`:

```
grep -n "scan_table(" src/services/transform/src/run.rs src/services/transform/src/compact.rs
```

Expected: both still call `scan_table(&ctx, store.clone(), …)` — unchanged.

- [ ] **Step 3: Build datafusion-io + run its existing scan tests** (regression guard — landed/relative path must still work):

```
buck2 test //src/services/datafusion-io:scan //src/services/datafusion-io:single-file-write > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error" /tmp/t.log
```

Expected: PASS. The existing scan tests exercise relative paths; they must remain green (the relative branch is behavior-identical).

- [ ] **Step 4: Clippy-clean check:**

```
buck2 build '//src/services/datafusion-io:datafusion-io[clippy.txt]' > /tmp/c.log 2>&1; grep -E "warning|error" /tmp/c.log || echo "clippy clean"
```

Expected: clean. Watch for `clippy::indexing_slicing` (none used), `unwrap`/`expect` (none — `unwrap_or` is fine), and unused imports.

- [ ] **Step 5: Commit:**

```bash
git add src/services/datafusion-io/src/scan.rs
git commit -m "fix(datafusion-io): resolve absolute file://, s3:// paths in scan_table"
```

---

### Task 4: Transform-chain e2e — the path that fails today

The regression test from the spec's Testing section: transform A lands an output with **absolute** `file://` paths; transform B reads A's output and commits the expected rows. Before Task 3 this fails at B's scan; after, it passes. The test calls `run_transform` **directly** (like `transform_e2e.rs`'s `empty_input_*` tests) — no worker/queue, no `CancellationToken`.

**Files:**
- Create: `src/services/transform/tests/transform_chain_e2e.rs`
- Modify: `src/services/transform/BUCK` (add a `transform-chain-e2e` `loom_fixture_test`).
- Reuse: `src/services/transform/tests/transform_e2e_support.rs` (`tref`, `make_catalog`, `seed_table`, `cols`, `scalar_i64`).

**Interfaces (VERIFIED against `run.rs:94` and `transform_e2e.rs:223,274`):**
- `transform::run_transform(cp: &dyn ControlPlane, store: Arc<dyn ObjectStore>, root_url: &str, run_id: &str, req: TransformRequest<'_>) -> Result<SnapshotId, TransformError>` — **5 args**; `run_id: &str` sits between `root_url` and `req`.
- `TransformRequest { inputs: &[TransformInput], output: &TableRef, sql: &str, conform: Option<…>, output_mode: OutputMode, lineage: LineageEvent }`.
- `TransformInput { table: &TableRef, register_as: &str }`.
- `engine_serving::execute_query(&IcebergCatalog, sql: &str, None) -> Vec<RecordBatch>`.
- Support helpers: `seed_table(&cp, &store, &table, &cols, schema, batch, file_prefix)`, `cols(&[(name, logical_ty, nullable)])`, `tref(schema, name)`, `scalar_i64(&batches) -> i64`, `make_catalog(dsn, &warehouse) -> SqlCatalog`, `lineage(&out) -> LineageEvent`.

- [ ] **Step 1: Write the failing test.** Create `src/services/transform/tests/transform_chain_e2e.rs` with this concrete body (modeled on `transform_e2e.rs:204-248`, the direct-`run_transform` path):

```rust
//! Transform-chain e2e: a transform that reads ANOTHER transform's output, whose
//! mirror files carry ABSOLUTE `file://` paths (promoted by `absolute_data_files`).
//! This is the one path that failed before scan_table became scheme-aware
//! (iss-iceberg-transform-chain-path): scan_table would prepend `loom://data/...`
//! to an already-absolute path and register no store under `file://`.

mod transform_e2e_support;

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_control_plane::IcebergControlPlane;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;

use transform_e2e_support::{cols, lineage, make_catalog, scalar_i64, seed_table, tref};

#[tokio::test(flavor = "multi_thread")]
async fn transform_reads_transform_output_with_absolute_paths() {
    let fx = PgFixture::start();
    let (pg, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let warehouse = wh.path().display().to_string();
    let root_url = format!("file://{warehouse}");
    let catalog = make_catalog(fx.pg_dsn(&db), &warehouse).await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(wh.path()).expect("store"));
    let cp = IcebergControlPlane::new(pg, catalog);

    // 1. Seed a base input table with RELATIVE paths (the shape a landed table has).
    let base = tref("src", "base");
    let base_cols = cols(&[("id", "long", false), ("name", "string", true)]);
    let base_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    seed_table(
        &cp,
        &store,
        &base,
        &base_cols,
        base_schema.clone(),
        RecordBatch::try_new(
            base_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
            ],
        )
        .unwrap(),
        "seed-base",
    )
    .await;

    // 2. Transform A: src.base -> stage.mid. run_transform commits stage.mid's files
    //    with ABSOLUTE file:// paths (via absolute_data_files in run.rs).
    let mid = tref("stage", "mid");
    transform::run_transform(
        &cp,
        store.clone(),
        &root_url,
        "run-a",
        transform::TransformRequest {
            inputs: &[transform::TransformInput {
                table: &base,
                register_as: "base",
            }],
            output: &mid,
            sql: "SELECT id, name FROM base",
            conform: None,
            output_mode: transform::OutputMode::Append,
            lineage: lineage(&mid),
        },
    )
    .await
    .expect("transform A lands stage.mid with absolute file:// paths");

    // 3. Transform B: reads stage.mid (ABSOLUTE paths!) -> dst.out. This is the call
    //    that errored before scan_table became scheme-aware.
    let out = tref("dst", "out");
    transform::run_transform(
        &cp,
        store.clone(),
        &root_url,
        "run-b",
        transform::TransformRequest {
            inputs: &[transform::TransformInput {
                table: &mid,
                register_as: "mid",
            }],
            output: &out,
            sql: "SELECT count(*) AS n FROM mid",
            conform: None,
            output_mode: transform::OutputMode::Append,
            lineage: lineage(&out),
        },
    )
    .await
    .expect("transform B resolves transform A's absolute-path files");

    // 4. Read dst.out back through the serving engine; B counted A's 2 rows.
    let serving = IcebergCatalog::new(fx.pool_for(&db).await);
    let n = engine_serving::execute_query(&serving, "SELECT \"n\" FROM \"dst\".\"out\"", None)
        .await
        .expect("serving read");
    assert_eq!(scalar_i64(&n), 2, "transform B read transform A's 2 rows");
}
```

> Notes: logical types use the same vocabulary as `transform_e2e.rs` (`"long"`, `"string"`). `register_as` is unquoted in the SQL (`FROM base`), matching the existing tests. `pool_for`/`pg_dsn`/`fresh_db` come from `PgFixture` (used identically in `transform_e2e.rs`). If `tempfile`/`arrow` array imports differ, copy the exact `use` lines from `transform_e2e.rs:8-25`.

- [ ] **Step 2: Add the BUCK target.** In `src/services/transform/BUCK`, add this `loom_fixture_test` (dep set copied from the sibling `compact-e2e` target, which — like this test — calls `run_transform`/compaction directly with **no** worker or `tokio-util`):

```python
loom_fixture_test(
    name = "transform-chain-e2e",
    crate = "transform_chain_e2e",
    srcs = ["tests/transform_chain_e2e.rs", "tests/transform_e2e_support.rs"],
    crate_root = "tests/transform_chain_e2e.rs",
    deps = [
        ":transform",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//src/services/datafusion-io:datafusion-io",
        "//src/services/engine-serving:engine-serving",
        "//third-party:arrow",
        "//third-party:iceberg",
        "//third-party:object_store",
        "//third-party:serde_json",
        "//third-party:tempfile",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```

Do **not** add `//src/control-plane/worker:worker` or `//third-party:tokio-util` — this test does not use `Worker`/`CancellationToken`, and the strict-clippy gate (`tools/clippy-all.sh`) treats unused deps as a failure, not a warning.

- [ ] **Step 3 (RED): Verify the test fails on the UNFIXED scan_table.** To prove the test catches the bug, temporarily `git stash` Task 3's scan.rs change (or check out the pre-Task-3 `scan.rs`), then run:

```
buck2 test //src/services/transform:transform-chain-e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error|panicked" /tmp/t.log
```

Expected: **FAIL** at transform B's scan (object-store / file-not-found / scan error). Then restore Task 3's change (`git stash pop`). *If reordering against an unfixed tree is impractical in the subagent flow, instead reason explicitly in the commit/PR why the test exercises the absolute-path branch (transform B's input files carry `file://` paths) — the RED is then demonstrated by the resolver unit test + code path, and the GREEN by Step 4.*

- [ ] **Step 4 (GREEN): Run the test against the fixed scan_table:**

```
buck2 test //src/services/transform:transform-chain-e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error|panicked" /tmp/t.log
```

Expected: PASS.

- [ ] **Step 5: Commit:**

```bash
git add src/services/transform/tests/transform_chain_e2e.rs src/services/transform/BUCK
git commit -m "test(transform): e2e transform-reads-transform-output absolute-path chain"
```

---

### Task 5: Full-suite green + clippy-all

**Files:** none (verification only).

- [ ] **Step 1: Full build + test:**

```
buck2 build //src/... > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|FAIL|error\[" /tmp/b.log
buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```

Expected: build succeeds; `Tests finished` with 0 failures. Fixture tests route local automatically (`loom_fixture_test`).

- [ ] **Step 2: Clippy across all first-party Rust:**

```
./tools/clippy-all.sh > /tmp/c.log 2>&1; tail -20 /tmp/c.log
```

Expected: clean (no per-target `[clippy.txt]` content).

- [ ] **Step 3:** If anything is red, fix and re-run before proceeding to Task 6. Do not open the PR on red.

---

### Task 6: Close the register item + open the PR

**Files:**
- Modify: `docs/ISSUES.md` (`iss-iceberg-transform-chain-path`: `- [ ]`→`- [x]`, `status:open`→`status:fixed`, `pr:-`→`pr:#<N>`).

- [ ] **Step 1:** Use the `loom-docs-update` skill to close `iss-iceberg-transform-chain-path` (flip the checkbox, set `status:fixed`, add the PR number once known). Commit the docs edit.
- [ ] **Step 2:** Run `bash tools/docs.sh validate` — expect no errors.
- [ ] **Step 3:** Push `work/iss-iceberg-transform-chain-path` and open a PR with that head branch (binds the claim to the PR). Conventional-commit PR title, e.g. `fix(iceberg): resolve absolute scan_table input paths for transform chains`.
- [ ] **Step 4:** Watch CI (`build-test`/`affected` + `lint`) to green; the `lint` job runs prek on all files — ensure no markdown trailing-whitespace / EOF issues in the plan/docs.

---

## Self-Review

**1. Spec coverage:**
- Part 1 (relocate resolver, `engine-serving` depends on `datafusion-io`, drop private copy) → Tasks 1 + 2. ✓ (Cargo.toml deviation documented and justified.)
- Part 2 (scheme-aware `scan_table`, no signature change, idempotent store registration, callers unchanged) → Task 3. ✓
- Testing (transform-chain e2e, `loom_fixture_test`, `file://` deterministic case) → Task 4. ✓
- Scope: `datafusion-io` + `engine-serving` + `transform` test only; `FileRef` unchanged; `loom://data` relative convention unchanged. ✓
- Risk: relative branch byte-identical (Task 3 Step 3 regression guard), Part 1 pure move (Task 2 builds identical), s3 branch covered structurally by reusing the proven resolver. ✓

**2. Placeholder scan:** No "TBD"/"handle edge cases"/"similar to". Task 4's fixture boilerplate is explicitly delegated to "copy from `transform_e2e.rs`" with the scenario fully specified — this is a deliberate instruction to read a concrete sibling file, not a placeholder, because inventing the exact `TransformRequest` field names here would risk type drift; the spec itself names the support file to reuse.

**3. Type consistency:** `object_store_url_for(path: &str) -> datafusion::error::Result<ObjectStoreUrl>` is identical across Tasks 1, 2, 3. `scan_table` signature unchanged everywhere. `LOOM_STORE_URL` referenced as imported. `run_transform` is the **5-arg** `(cp, store, root_url, run_id, req)` form — verified against `run.rs:94` and the call sites in `transform_e2e.rs:223,274` — and Task 4's skeleton passes `run_id` (`"run-a"`/`"run-b"`). `seed_table`/`cols`/`tref`/`scalar_i64`/`make_catalog`/`lineage` names match `transform_e2e_support.rs` as read.
