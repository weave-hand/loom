# Serving Empty-Table Not-Found Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A live-but-empty table (no inline rows, no cold Parquet files) registers in the serving context and reads as **zero rows with the mirror's authoritative schema** everywhere (SQL reads, previews, governed SQL, transform inputs), instead of `table '…' not found`; a genuinely-unknown table still not-founds. Then drop the transform worker's compensating `Validation`-means-empty guard.

**Architecture:** Replace both `(None, None) => return Ok(None)` arms in `build_serving_provider` (`src/services/engine-serving/src/serving.rs:201`, `:218`) with a zero-row `MemTable` over the in-scope mirror schema (`arrow_schema_from_mirror` result, `serving.rs:122`). `Ok(None)` then means exactly "not live at the requested snapshot" (the as-of arm, `serving.rs:109-115`). The worker's `Err(Validation) => empty` arm (`worker/src/transform.rs:278`) becomes dead code and is deleted. Spec: `docs/superpowers/specs/2026-07-09-serving-empty-table-not-found-design.md`.

**Tech Stack:** Rust, DataFusion (`MemTable`), buck2 `loom_fixture_test`, hermetic Postgres fixture, Arrow Flight (worker e2e).

## Global Constraints

Carried from the spec; every task implicitly includes these:

- **Tests are `rust_test` / `loom_fixture_test` integration targets only — never inline `#[cfg(test)]`.** The new fixture test MUST use `loom_fixture_test` (`src/control-plane/postgres/defs.bzl`), wired in `src/services/engine-serving/BUCK` mirroring the `serving_as_of` target. The `no-inline-tests` prek hook fails the build on any inline `#[test]`.
- **No `.sqlx` change expected** — no SQL is added or changed. If that changes, run `tools/sqlx-prepare.sh` and commit `.sqlx/`.
- **Run prek before every commit:** `buck2 run //tools:prek -- run --all-files` (stage new files with `git add` first — prek skips untracked files). Markdown files end with exactly one trailing newline, no trailing whitespace.
- **Clippy is strict (pedantic + restriction):** no `unwrap`/`expect`/`indexing_slicing`/`panic`/`todo` in production lib/bin code — the production change is `?`-based throughout. Test code is exempted from the panic-safety lints via `loom_fixture_test`.
- **Non-empty tables stay byte-identical.** The only production serving change is inside the two `(None, None)` arms, unreachable when either tier exists. Existing suites (`serving_as_of`, `merge_on_read`, `execute_query_e2e`, `governed_sql`, query-api e2e, worker `transform_e2e`) must stay green.
- **No framing leak:** the empty provider presents the mirror **data** schema only — never `loom_*` reserved columns.
- **Task order is load-bearing:** Task 3 (drop the worker guard) MUST land after Task 2 (the serving fix) — before it, the worker's empty-input e2e tests fail without the guard, which is exactly the coupling being removed.
- **Build/test commands** (CLAUDE.md): build `buck2 build -v0 --console none //src/...`; test `buck2 test --console none <targets>` (cloud: add `-M none` to builds, scope tests, `buck2 clean` between heavy phases; full local suite needs `-j 8` to avoid starving the 8 PG boot-slots).

---

## File Structure

**Create:**
- `src/services/engine-serving/tests/serving_empty_table.rs` — the empty-table serving fixture test (all boundary cases).

**Modify (production):**
- `src/services/engine-serving/src/serving.rs` — `empty_provider` helper; both `(None, None)` arms; doc comments.
- `src/services/worker/src/transform.rs` — drop the `Validation`-means-empty arm + rewrite the comment block.

**Modify (wiring/docs):**
- `src/services/engine-serving/BUCK` — new `loom_fixture_test` target `serving-empty-table`.
- `docs/system-capabilities/engine.md`, `docs/system-capabilities/transform.md` — document the new invariant / remove the guard note.
- `docs/ISSUES.md` — remove the closed `iss-serving-empty-table-not-found` entry.

---

## Task 1: Failing fixture test — empty live table serves zero rows; the not-found boundary is pinned

**Files:**
- Create: `src/services/engine-serving/tests/serving_empty_table.rs`
- Modify: `src/services/engine-serving/BUCK`

**Interfaces:**
- Consumes: `build_serving_provider`/`execute_query`/`EngineServingError` (exported at `engine-serving/src/lib.rs:21`), `execute_governed_sql_stream` (`engine_serving::governed`), `IcebergControlPlane` + `TableControlPlane::begin_table` (`iceberg_control_plane.rs:35`/`:88`), the empty-table recipe (`worker/tests/transform_e2e.rs:145-154`), `local_sql_catalog` (`//src/testing:seed`), `PgFixture`.
- Produces: test target `//src/services/engine-serving:serving-empty-table` — the acceptance gate for Task 2.

- [ ] **Step 1: Write the failing test**

Create `src/services/engine-serving/tests/serving_empty_table.rs`:

```rust
//! A live-but-empty table (no inline rows, no cold Parquet files) must register
//! and read as ZERO ROWS with the mirror's authoritative schema — never as
//! `table not found`. Also pins the boundary: an unknown table still
//! plan-errors, and an as-of read of a table not live at the pinned snapshot
//! still yields no provider. Harness: `IcebergControlPlane`
//! create+empty-append (the live-zero-file fixture from
//! `worker/tests/transform_e2e.rs`). loom_fixture_test (Postgres).
//! Spec: docs/superpowers/specs/2026-07-09-serving-empty-table-not-found-design.md

use std::sync::Arc;

use control_plane_core::{
    ColumnSpec, ControlPlane, GovernedCatalog, ObjectType, SnapshotId, TableControlPlane, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_control_plane::IcebergControlPlane;
use datafusion::arrow::datatypes::DataType;
use datafusion::catalog::TableProvider;
use datafusion::prelude::SessionContext;
use engine_serving::governed::execute_governed_sql_stream;
use engine_serving::{EngineServingError, build_serving_provider, execute_query};
use futures::TryStreamExt;
use loom_test_seed::local_sql_catalog;

fn cols() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        },
        ColumnSpec {
            name: "name".into(),
            ty: "string".into(),
            nullable: true,
        },
    ]
}

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef {
        schema: schema.into(),
        name: name.into(),
    }
}

/// Create `table` live in the mirror with declared columns and an EMPTY file
/// list: `current_snapshot`/`schema` resolve, zero data files, no inline
/// storage — the exact `(None, None)` serving case. Mirrors
/// `transform_e2e::create_empty_table` (a create-only commit would allocate a
/// snapshot without a mirror `table` row, so the empty append is required).
async fn create_empty_table(icp: &IcebergControlPlane, table: &TableRef) {
    let mut tx = icp.begin_table().await.expect("begin");
    tx.create_table(table, &cols()).await.expect("create");
    tx.append_files(table, &[]).await.expect("append empty");
    tx.commit().await.expect("commit");
}

/// Total rows across the batches of a full scan of `provider`.
async fn count_rows(provider: Arc<dyn TableProvider>) -> usize {
    let ctx = SessionContext::new();
    let df = ctx.read_table(provider).expect("read_table");
    let batches = df.collect().await.expect("collect");
    batches.iter().map(|b| b.num_rows()).sum()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_live_table_serves_zero_rows_with_mirror_schema() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("wh");
    let icp = IcebergControlPlane::new(
        cp,
        local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await,
    );
    let table = tref("s", "empty");
    create_empty_table(&icp, &table).await;

    let catalog = IcebergCatalog::new(pool);
    let ctx = SessionContext::new();
    let provider = build_serving_provider(&ctx, &catalog, &table, None, None)
        .await
        .expect("build")
        .expect("a live-but-empty table must yield a provider, not None");

    // The provider presents the mirror's authoritative schema exactly.
    let schema = provider.schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, vec!["id", "name"], "mirror column order preserved");
    assert_eq!(*schema.field(0).data_type(), DataType::Int64);
    assert!(!schema.field(0).is_nullable(), "id is non-null in the mirror");
    assert_eq!(*schema.field(1).data_type(), DataType::Utf8);
    assert!(schema.field(1).is_nullable(), "name is nullable in the mirror");

    assert_eq!(count_rows(provider).await, 0, "an empty table reads as zero rows");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_live_table_select_star_returns_empty_not_not_found() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("wh");
    let icp = IcebergControlPlane::new(
        cp,
        local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await,
    );
    create_empty_table(&icp, &tref("s", "empty")).await;

    let catalog = IcebergCatalog::new(pool);
    // The exact path previews and worker input reads consume.
    let batches = execute_query(&catalog, "SELECT * FROM \"s\".\"empty\"", None)
        .await
        .expect("SELECT * over a live-but-empty table must succeed");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 0, "empty result, not an error");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_table_still_plan_errors() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("wh");
    let icp = IcebergControlPlane::new(
        cp,
        local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await,
    );
    // A live table exists so the serving context is non-trivially populated…
    create_empty_table(&icp, &tref("s", "empty")).await;

    let catalog = IcebergCatalog::new(pool);
    // …but an undeclared table must STILL be a planning fault (the boundary).
    let err = execute_query(&catalog, "SELECT * FROM \"s\".\"never_declared\"", None)
        .await
        .expect_err("an unknown table must stay not-found");
    assert!(
        matches!(err, EngineServingError::Plan(_)),
        "unknown tables keep the Plan (client-fault / not-found) class, got: {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_identity_table_serves_zero_rows() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("wh");
    let icp = IcebergControlPlane::new(
        cp,
        local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await,
    );
    let table = tref("s", "empty_typed");
    create_empty_table(&icp, &table).await;
    // Bind an identity-bearing type to the table: the identity-bearing
    // `(None, None)` arm (serving.rs:218) must also yield the empty provider.
    let ty = ObjectType::build("EmptyTyped", ("s", "empty_typed"))
        .prop_req("id", "Long")
        .prop("name", "String")
        .identity("id")
        .done();
    icp.ontology().define_type(ty).await.expect("define type");

    let catalog = IcebergCatalog::new(pool);
    let ctx = SessionContext::new();
    let provider = build_serving_provider(&ctx, &catalog, &table, None, None)
        .await
        .expect("build")
        .expect("an identity-bearing empty table must also yield a provider");
    // Data schema ONLY — no loom_* framing columns may leak.
    let names: Vec<&str> = provider
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(names, vec!["id", "name"], "no framing columns on the empty provider");
    assert_eq!(count_rows(provider).await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn as_of_before_table_existed_stays_none() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("wh");
    let icp = IcebergControlPlane::new(
        cp,
        local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await,
    );
    let table = tref("s", "empty");
    create_empty_table(&icp, &table).await;

    let catalog = IcebergCatalog::new(pool);
    let ctx = SessionContext::new();
    // Snapshot 0 predates every mirror row (begin_snapshot >= 1): the table is
    // NOT live at it, so the as-of skip (serving.rs:109-115) must still yield
    // None — the one remaining meaning of None after this change.
    let provider = build_serving_provider(&ctx, &catalog, &table, None, Some(SnapshotId(0)))
        .await
        .expect("build");
    assert!(provider.is_none(), "not-live-at-snapshot keeps yielding no provider");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn governed_read_of_empty_table_returns_zero_rows() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("wh");
    let icp = IcebergControlPlane::new(
        cp,
        local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await,
    );
    create_empty_table(&icp, &tref("s", "empty")).await;

    let catalog = IcebergCatalog::new(pool);
    // Empty policy = full visibility (the governed_sql.rs convention): the
    // governed loop must register the empty table instead of `continue`ing.
    let cat = GovernedCatalog { tables: vec![] };
    let stream = execute_governed_sql_stream(&catalog, "SELECT * FROM \"s\".\"empty\"", &cat, None)
        .await
        .expect("governed SELECT * over a live-but-empty table must plan");
    let batches: Vec<_> = stream.try_collect().await.expect("collect");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 0);
}
```

(If `GovernedCatalog`'s construction differs — check `governed_sql.rs`'s `GovernedCatalog { tables: vec![…] }` literal — mirror that file exactly; the intent is "no policy rows".)

- [ ] **Step 2: Wire the BUCK target**

In `src/services/engine-serving/BUCK`, add a `loom_fixture_test` mirroring the `serving_as_of` target:

```python
loom_fixture_test(
    name = "serving-empty-table",
    crate = "serving_empty_table",
    srcs = ["tests/serving_empty_table.rs"],
    crate_root = "tests/serving_empty_table.rs",
    deps = [
        ":engine-serving",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//src/testing:seed",
        "//third-party:datafusion",
        "//third-party:futures",
        "//third-party:tempfile",
        "//third-party:tokio",
    ],
)
```

(Arrow types come through `datafusion::arrow`, so no separate arrow dep is needed; add `//third-party:arrow` only if the compiler asks.)

- [ ] **Step 3: Run the test to verify it fails for the right reason**

Run: `buck2 test --console none //src/services/engine-serving:serving-empty-table`
Expected: FAIL —
- `empty_live_table_serves_zero_rows_with_mirror_schema` / `empty_identity_table_serves_zero_rows` panic at `.expect("a live-but-empty table must yield a provider…")` (today `Ok(None)`);
- `empty_live_table_select_star_returns_empty_not_not_found` / `governed_read_of_empty_table_returns_zero_rows` fail with a `Plan` "table … not found" error;
- `unknown_table_still_plan_errors` and `as_of_before_table_existed_stays_none` PASS (they pin existing behavior — if either fails, stop and re-read the harness).

- [ ] **Step 4: Commit the failing test**

```bash
git add src/services/engine-serving/tests/serving_empty_table.rs src/services/engine-serving/BUCK
buck2 run //tools:prek -- run --all-files
git commit -m "test(serving): pin empty-live-table reads + the not-found boundary

A live-but-empty table must serve zero rows with the mirror schema (direct
provider, SELECT *, governed SQL, identity-bearing); an unknown table must
stay a Plan not-found; as-of not-live stays None. Fails on main: the
(None, None) arms in build_serving_provider return Ok(None), so the table is
never registered (iss-serving-empty-table-not-found)."
```

---

## Task 2: Implement — `empty_provider` + replace both `(None, None)` arms

**Files:**
- Modify: `src/services/engine-serving/src/serving.rs:69-73` (doc comment), `:188-231` (the combine match — two arms), new helper after `build_serving_provider` (near `:236`).

**Interfaces:**
- Consumes: the in-scope `schema` (`serving.rs:122`), `to_serving` (`serving.rs:65`), `datafusion::datasource::MemTable`.
- Produces: `fn empty_provider(schema: &SchemaRef) -> Result<Arc<dyn TableProvider>, EngineServingError>`; `build_serving_provider` returning `Ok(Some(_))` for every table live at the requested snapshot.

- [ ] **Step 1: Add the helper**

In `src/services/engine-serving/src/serving.rs`, after `build_serving_provider` (before `with_cdc_framing_fields`, ~line 236), add:

```rust
/// A zero-row provider presenting the mirror's authoritative `schema`. Used by
/// [`build_serving_provider`] for a table that is live at the requested
/// snapshot but holds no data in either tier (no cold Parquet files, no live
/// inline rows), so the table still REGISTERS in the serving context — and a
/// `SELECT *`/preview of a legitimately-empty table reads as zero rows with a
/// schema instead of `table not found`. Data columns only: no `loom_*` framing
/// (with zero rows there is nothing to fold or tombstone).
fn empty_provider(schema: &SchemaRef) -> Result<Arc<dyn TableProvider>, EngineServingError> {
    // One EMPTY partition, not zero partitions — `MemTable::try_new` rejects an
    // empty partition list ("No partitions provided"); same note as
    // `datafusion_io::scan::register_batches`.
    let mem = datafusion::datasource::MemTable::try_new(schema.clone(), vec![Vec::new()])
        .map_err(to_serving)?;
    Ok(Arc::new(mem))
}
```

- [ ] **Step 2: Replace both arms**

In the combine match (`serving.rs:188-231`):

- Identity-less arm (`:201`):
  `(None, None) => return Ok(None), // a live table with no data; nothing to register`
  becomes
  `(None, None) => empty_provider(&schema)?, // live but empty: zero rows, mirror schema`
- Identity-bearing arm (`:218`):
  `(None, None) => return Ok(None),`
  becomes
  `(None, None) => empty_provider(&schema)?,`
  (with zero physical rows there is nothing to fold — the plain data-schema
  empty provider is exactly what `build_merge_view` would project back to).

Both arms already sit in a `match` whose arms evaluate to `Arc<dyn TableProvider>` bound to `provider`, so `empty_provider(&schema)?` slots in with no other restructuring.

- [ ] **Step 3: Update the doc contract**

- `build_serving_provider`'s doc comment (`serving.rs:69-73`): replace "(either alone, or `None` when the table has no live data)" with wording per the spec — a live-but-empty table yields a zero-row provider over the mirror schema; `Ok(None)` means exactly "not live at the requested snapshot" (the as-of skip).
- In `governed.rs`, the `else { continue; }` after the `build_serving_provider` call (`governed.rs:299-301`) now only skips as-of-not-live tables — since this loop passes `at: None`, add/adjust a one-line comment noting the `None` case is unreachable here in practice (current-snapshot path) but kept as a harmless skip.

- [ ] **Step 4: Verify the Task 1 gate passes**

Run: `buck2 test --console none //src/services/engine-serving:serving-empty-table`
Expected: PASS — all six tests.

- [ ] **Step 5: Non-regression sweep (serving + its consumers)**

Run: `buck2 build -v0 --console none //src/...`
Run: `buck2 test --console none //src/services/engine-serving/... //src/services/engine/...`
Run: `buck2 test --console none //src/services/query-api/... //src/services/worker/...`
Expected: PASS everywhere. The worker's `transform-e2e` empty-input tests (`empty_input_counts_zero`, `empty_input_select_star_commits_empty_output`) now take the `Ok`-empty path (the engine registers the table; `execute` returns an empty result) — they must stay green with the guard still present (the `Validation` arm is now dead code, removed in Task 3).

- [ ] **Step 6: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "fix(serving): register live-but-empty tables as zero-row relations

build_serving_provider's (None, None) arms now yield an empty MemTable over
the mirror's authoritative schema (arrow_schema_from_mirror) instead of
Ok(None), so a legitimately-empty table reads as zero rows with a schema on
every serving path (SELECT *, previews, governed SQL, transform inputs).
Ok(None) now means exactly 'not live at the requested snapshot'; unknown
tables still plan-error not-found. Closes the serving half of
iss-serving-empty-table-not-found."
```

---

## Task 3: Drop the worker's `Validation`-means-empty guard

**Files:**
- Modify: `src/services/worker/src/transform.rs:247-255` (comment block), `:273-285` (the read + guard).

**Interfaces:**
- Consumes: the Task 2 serving invariant (an existing input's `SELECT *` never plan-errors).
- Produces: `run_wire_transform` whose input read is `Ok(batches)` / `Err(e) ⇒ Retry` — no `Validation` special case.

- [ ] **Step 1: Delete the guard arm**

In `src/services/worker/src/transform.rs`, replace the read (`:273-285`):

```rust
        let batches = match ctx.sql.execute(select_all_sql(table)).await {
            Ok(batches) => batches,
            // The table exists (columns is Some) but the serving catalog holds no
            // provider for it => it is empty (no inline, no cold files). Register an
            // empty relation with the declared schema below.
            Err(ControlPlaneError::Validation(_)) => Vec::new(),
            Err(e) => {
                return Err(JobFailure::retry(
                    ctx.worker_tuning.backoff(attempts),
                    format!("read input {}.{}: {e}", table.schema, table.name),
                ));
            }
        };
```

with:

```rust
        let batches = ctx.sql.execute(select_all_sql(table)).await.map_err(|e| {
            JobFailure::retry(
                ctx.worker_tuning.backoff(attempts),
                format!("read input {}.{}: {e}", table.schema, table.name),
            )
        })?;
```

If `ControlPlaneError` is now unused in this file, remove it from the imports (the compiler will say).

Behavior note (deliberate): a *genuine* `Validation` on an input read — e.g. the input dropped between `list_files` and the read — now retries loudly instead of silently registering an empty relation. The serving engine registers every live table (Task 2), so `Validation` no longer means "empty"; it means something is actually wrong.

- [ ] **Step 2: Rewrite the step-2/3 comment block**

Replace the comment at `:247-255` (which ends "…that is treated as an empty input (register the DECLARED schema with no rows) so the SQL runs over an empty relation — matching the prior semantics.") so it reads, in substance:

```rust
    // 2+3. Resolve + register each input. ListFiles is the existence + declared-schema
    //      oracle: an absent `columns` means the table does not exist — deterministically
    //      bad (Abandon). The ROWS come from the engine's SQL serving path (`SELECT *`),
    //      which merges the hot inline PG tier with the cold Parquet files. The engine
    //      registers every live table — a live-but-empty input answers an EMPTY result
    //      (zero batches), which registers below under the DECLARED schema; a read error
    //      is a real fault and retries.
```

The zero-batch arm (`:286-292`, `batches.first()` is `None` → `register_empty_table(logical_arrow_schema(&columns))`) **stays unchanged** — a Flight-decoded empty result can legitimately arrive as zero `RecordBatch`es, and the declared-schema registration is still the right recovery for that shape.

- [ ] **Step 3: Verify with the existing worker suite (the test evidence)**

Run: `buck2 test --console none //src/services/worker:transform-e2e //src/services/worker:typed-transform-e2e //src/services/worker:run-wire-job`
Expected: PASS. In particular `empty_input_counts_zero` (`transform_e2e.rs:474` — `count(*)` over an empty input commits a single `0` row) and `empty_input_select_star_commits_empty_output` (`:642` — `SELECT *` over an empty input commits an empty live output) — the two tests that exercised the deleted arm before Task 2 — prove the serving path now feeds `Ok(empty)` end-to-end over Flight with no worker-side compensation. (Evidence the coupling existed: this task run against main *without* Task 2 fails both tests — that is why task order is load-bearing.)

Also run: `buck2 build -v0 --console none //src/services/worker/...` (import cleanup compiles).

- [ ] **Step 4: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "fix(worker): drop the Validation-means-empty transform-input guard

The engine now registers live-but-empty tables (zero-row MemTable over the
mirror schema), so an existing input's SELECT * never plan-errors: the
Err(Validation) => empty arm is dead. A genuine Validation on an input read
now retries loudly instead of silently registering an empty relation. The
zero-batch declared-schema registration stays (an empty Flight result can
arrive as zero batches). Closes the worker half of
iss-serving-empty-table-not-found."
```

---

## Task 4: Documentation — capability docs + close the register item

**Files:**
- Modify: `docs/system-capabilities/engine.md` — the serving read path section gains the invariant.
- Modify: `docs/system-capabilities/transform.md:160-163` — remove the guard note.
- Modify: `docs/ISSUES.md` — remove the closed `iss-serving-empty-table-not-found` entry (`ISSUES.md:20-21`).

- [ ] **Step 1: Update `docs/system-capabilities/engine.md`**

In the serving section, add one sentence stating the invariant: every table live in the mirror registers in the serving context — a live-but-empty table (no inline rows, no data files) serves zero rows under its authoritative mirror schema (`empty_provider` in `serving.rs`), while an undeclared/dropped table (and, for as-of reads, a table not live at the pinned snapshot) still answers a not-found planning error. Note also (if the `ListFiles` sentence at `engine.md:170-173` implies wire consumers must self-register empty relations) that internal SQL readers no longer need to.

- [ ] **Step 2: Update `docs/system-capabilities/transform.md`**

Rewrite the clause at `transform.md:160-163` ("…a live-but-empty input (no inline, no files) is not registered in the serving catalog, so the worker treats a planning error on an existing input as an empty relation (see `#iss-serving-empty-table-not-found`).") to state the current behavior: the serving catalog registers every live table, so a live-but-empty input reads as an empty result over the same merged path; the worker registers the declared schema when the result carries no batches.

- [ ] **Step 3: Close the register item**

Remove the `- [ ] **Serving path returns \`table not found\` for a live zero-row table** {#iss-serving-empty-table-not-found …}` entry (title + prose lines) from `docs/ISSUES.md` — the registers carry open work only; git history keeps the record. Run the `loom-docs-update` skill if operating under it, or edit directly.

Validate: `bash tools/docs.sh validate`
Expected: clean (no dangling `[[iss-serving-empty-table-not-found]]` cross-links — `transform.md`'s prose reference was rewritten in Step 2; grep to be sure: `grep -rn "iss-serving-empty-table-not-found" docs/ src/` should hit only this spec/plan pair).

- [ ] **Step 4: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "docs(serving): empty live tables read as zero rows; close iss-serving-empty-table-not-found

Document the serving invariant (every live mirror table registers; empty
means zero rows + schema; unknown still not-found) in the engine capability,
rewrite the transform capability's guard note, and remove the closed ISSUES
entry."
```

---

## Task 5: Final verification

- [ ] **Step 1: Full build + focused suites**

Run: `buck2 build -v0 --console none //src/...`
Run: `buck2 test --console none //src/services/engine-serving/... //src/services/engine/... //src/services/worker/... //src/services/query-api/...`
(Locally, a full `buck2 test //src/... -j 8` is the gold standard; in a cloud session scope to the above + btd-affected targets and build with `-M none`.)
Expected: PASS everywhere.

- [ ] **Step 2: Lint gate**

Run: `buck2 run //tools:prek -- run --all-files`
Expected: clean (rustfmt, clippy strict, markdown trailing-newline/whitespace, docs-validate).

- [ ] **Step 3: Grep the dead pattern is gone**

Run: `grep -rn "Validation(_)) => Vec::new()" src/services/worker/` — no hits.
Run: `grep -rn "(None, None) => return Ok(None)" src/services/engine-serving/` — no hits.

---

## Self-Review (run after writing the plan)

**1. Spec coverage.** Every spec section maps to a task: the `empty_provider` helper + both arm replacements + doc contract ⇒ Task 2; the not-found boundary pins (unknown-table `Plan`, as-of `None`) ⇒ Task 1 tests 3/5; the identity-bearing arm + no-framing-leak ⇒ Task 1 test 4; the governed path ⇒ Task 1 test 6; the worker guard drop + comment rewrite + retry-on-real-Validation semantics ⇒ Task 3; non-regression (worker empty-input tests green with guard present, then absent) ⇒ Task 2 Step 5 + Task 3 Step 3; docs/register closure ⇒ Task 4. ✓

**2. Type consistency.** `empty_provider(&schema)` consumes the `SchemaRef` built at `serving.rs:122`; its `Arc<dyn TableProvider>` return matches the combine-match arm type bound to `provider` (`serving.rs:188`); `build_serving_provider`'s signature is unchanged so no caller (governed.rs:298, serving.rs:493, serving_as_of tests) needs a signature edit. ✓

**3. Order dependency stated.** Task 3 after Task 2 (Global Constraints + Task 3 Step 3 explain why). ✓

**4. Placeholder scan.** No TBD/TODO; the one conditional instruction (arrow dep, `GovernedCatalog` literal shape) names the exact file to mirror. ✓
