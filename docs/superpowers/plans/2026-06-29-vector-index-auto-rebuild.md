# Vector Index Auto-Rebuild on Flush — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When a flush drains a table's live inline rows into a cold Parquet snapshot and end-caps them, automatically enqueue a `build_vector_index` rebuild — atomically with the snapshot commit — for every declared vector index on that table, so just-flushed vectors do not silently disappear from k-NN results.

**Architecture:** Thread a new optional `jobs: &[NewJob]` field through the existing `CommitExtras` seam (the same path `lineage`/`end_cap` already ride) so the enqueue lands in the one Postgres transaction that commits the flush snapshot inside the vendored catalog's `do_update_table`. The enqueue is a generic **atomic insert-if-absent** primitive (`queue::pg_insert_if_absent`) that skips when an unstarted (`state = 'available'`) job with the same `kind` + `payload` already exists — pending-only dedup, evaluated inside the commit tx so there is no check-then-insert race. `flush_locked` resolves the declared vector-index names for the flushed table and builds one `BuildVectorIndexJob`-shaped `NewJob` per index; the catalog commit stays free of vector-index knowledge.

**Tech Stack:** Rust (edition 2024), buck2, sqlx compile-time `query!`/`query_scalar!` (offline cache at `src/control-plane/postgres/.sqlx/`), Postgres (hermetic fixture), Arrow/Iceberg, `loom_fixture_test` integration tests.

## Global Constraints

- **Tests are `rust_test` integration targets only** — never inline `#[cfg(test)]`. New fixture tests MUST use the `loom_fixture_test` macro (`src/control-plane/postgres/defs.bzl`), not a bare `rust_test`, or they route to RE and fail as root.
- **Run the suite with:** `buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`. Never pipe `buck2 test` through `tail`/`head`.
- **Any new/changed compile-time SQL requires refreshing the committed `.sqlx` cache** via `tools/sqlx-prepare.sh`, and committing the result. The `//src/control-plane/postgres:sqlx-cache-check` test enforces freshness in the normal sweep.
- **Clippy is strict** (pedantic + restriction groups enforced on production code: `unwrap_used`, `expect_used`, `indexing_slicing`, `panic`, `map_err_ignore`, etc.). Carry source errors (`.map_err(backend)`), no bare `unwrap`/`expect` in lib code. Test code is exempt from panic-safety lints via `loom_fixture_test`.
- **Behavior-preserving for tables without vector indexes:** the declared-index set is empty → no enqueue → byte-identical flush.
- **No new job kind, no proto/wire change, no migration, no ontology change.** `queue.jobs`, `ontology.vector_index_definition`, `iceberg_mirror.vector_index` all already exist.

---

## Reference: exact current code (read before editing)

These are the functions the plan modifies, with current signatures verified against the tree:

- `src/control-plane/postgres/src/queue.rs:10` — `pub(crate) async fn pg_insert<'e, E: sqlx::PgExecutor<'e>>(ex: E, job: &NewJob) -> Result<JobId>`
- `src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs:~219` — `CommitExtras<'a>` struct (`lineage`, `end_cap`, `overwrite`)
- `src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs:460` — `do_update_table(&self, commit, extras: CommitExtras<'_>) -> Result<Table>`; commit tx is `begin()` at line 497, `tx.commit()` at line 568; `end_cap` block at 546-560, `lineage` block at 562-566.
- `src/control-plane/postgres/src/iceberg_writer.rs:144` — `CommitExtrasCatalog<'a>` struct + its `Debug` impl (151) + `update_table` (165) rebuilding `CommitExtras` (169-176).
- `src/control-plane/postgres/src/iceberg_writer.rs:245` — `append_batches_with_extras(catalog, table, batches, lineage, end_cap, overwrite)`; `append_batches_with_lineage` (279) calls it with `None, None, false`.
- `src/control-plane/postgres/src/iceberg_landing.rs:149` — `append_parquet_snapshot(pool, catalog, table, columns, batches, lineage, end_cap, overwrite)`; calls `land_additive(...)` at 200 and `append_batches_with_extras(...)` at 226.
- `src/control-plane/postgres/src/iceberg_landing.rs:308` — `land_additive(pool, catalog, table, columns, batches, lineage, end_cap)`; its own commit tx `begin()` at 365, `tx.commit()` at 389, `end_cap` block 368-385, `lineage` block 386-388.
- `src/control-plane/postgres/src/iceberg_flush.rs:56` — `flush_locked`; `end_cap` built at 96-99, `append_parquet_snapshot(...)` call at 101-111.
- `src/control-plane/postgres/src/vector_index.rs:131` — `pub async fn type_name_for(pool: &PgPool, table: &TableRef) -> Result<String>` (returns `ControlPlaneError::NotFound` when the table has no ontology type).
- `control_plane_core` exports `NewJob { kind: String, payload: serde_json::Value, run_at: Option<OffsetDateTime>, priority: i32 }`, `BUILD_VECTOR_INDEX_JOB_KIND: &str = "build_vector_index"`, and `BuildVectorIndexJob { schema: String, name: String, index_name: String }` (derives `Serialize`, `Deserialize`, `Debug`, `Clone`). Confirm the exact import paths with `grep -rn "BUILD_VECTOR_INDEX_JOB_KIND\|BuildVectorIndexJob" src/` (the worker tests already import them).

**Critical invariant — pending-only dedup:** a `build_vector_index` job fixes its covered snapshot `S` at the *start* of execution (`vector_index.rs:367-370`). A *running* build cannot cover rows from a flush that commits after it started, so the dedup MUST skip only on `state = 'available'` (unstarted) jobs, never on running ones. This is the whole point of the slice; do not "optimize" it to `state in ('available','running')`.

---

## Task 1: Atomic auto-rebuild enqueue on flush (postgres)

Deliver: a flush that end-caps inline rows enqueues exactly one `build_vector_index` job per declared vector index, atomic with the snapshot commit, deduped pending-only; tables with no declared index and no-op flushes enqueue nothing.

**Files:**
- Modify: `src/control-plane/postgres/src/queue.rs` (add `pg_insert_if_absent`)
- Modify: `src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs` (`CommitExtras.jobs` field + insert in `do_update_table`)
- Modify: `src/control-plane/postgres/src/iceberg_writer.rs` (`CommitExtrasCatalog.jobs` + thread through `append_batches_with_extras`, `append_batches_with_lineage`)
- Modify: `src/control-plane/postgres/src/iceberg_landing.rs` (thread `jobs` through `append_parquet_snapshot` + `land_additive`)
- Modify: `src/control-plane/postgres/src/vector_index.rs` (add `declared_vector_index_names`)
- Modify: `src/control-plane/postgres/src/iceberg_flush.rs` (`flush_locked` resolves indexes, builds jobs, passes them)
- Create: `src/control-plane/postgres/tests/flush_vector_rebuild.rs`
- Modify: `src/control-plane/postgres/BUCK` (new `loom_fixture_test` target)
- Refresh: `src/control-plane/postgres/.sqlx/` (two new compile-time queries)

**Interfaces:**
- Produces: `queue::pg_insert_if_absent<'e, E: sqlx::PgExecutor<'e>>(ex: E, job: &NewJob) -> Result<Option<JobId>>` (`Some` if inserted, `None` if a matching `available` job already existed).
- Produces: `vector_index::declared_vector_index_names(pool: &PgPool, table: &TableRef) -> Result<Vec<String>>` (empty when the table has no bound ontology type).
- Produces: `CommitExtras.jobs: &'a [NewJob]` threaded through `append_parquet_snapshot`/`append_batches_with_extras`/`land_additive` (each gains a `jobs: &[NewJob]` parameter, appended last).
- Consumes: `control_plane_core::{NewJob, BUILD_VECTOR_INDEX_JOB_KIND, BuildVectorIndexJob}`, `vector_index::type_name_for`, `crate::backend`.

---

- [ ] **Step 1: Write the failing test (auto-enqueue)**

Create `src/control-plane/postgres/tests/flush_vector_rebuild.rs`. **Copy these helpers verbatim from `tests/vector_index_build.rs`** (they are local, intentionally duplicated per loom's test-helper convention): `columns()`, `ipc_body(rows: &[(i64,[f32;4])])`, `lineage(run, table)`, and `make_catalog(dsn, warehouse)`. Add a local `setup` helper that boots the fixture and declares the type + flat index (the bootstrap/define idiom below is verified against `tests/vector_search.rs:120-208`).

**Use the REAL fixture API** — `PgFixture::start()` is **sync**; `fresh_db()` returns `(PgControlPlane, db)`; the warehouse is a `tempfile::tempdir()` whose guard must outlive the catalog:

```rust
use std::time::Duration;
use control_plane_core::{
    ControlPlane, IndexSpec, Metric, ObjectType, PropertyDef, RunId, TableRef, TypeName,
    VectorIndexDef,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_flush::flush_table;
use control_plane_postgres::iceberg_landing::land;

/// Boot the fixture, define the `Docs` type (identity = "id"), and — when
/// `with_index` — declare a flat vector index `by_flat`. Returns everything the
/// tests need; keep `_wh` alive for the whole test.
async fn setup(
    fx: &PgFixture,
    db: &str,
    with_index: bool,
) -> (SqlCatalog, sqlx::PgPool, TableRef, tempfile::TempDir) {
    let wh = tempfile::tempdir().expect("wh");
    let pool = fx.pool_for(db).await;
    let catalog = make_catalog(fx.pg_dsn(db), &wh.path().display().to_string()).await;
    let cp = PgControlPlane::new(pool.clone(), Duration::from_secs(5));
    let table = TableRef { schema: "wh".into(), name: "docs".into() };

    cp.ontology()
        .define_type(ObjectType {
            name: TypeName("Docs".into()),
            table: table.clone(),
            properties: vec![
                PropertyDef { name: "id".into(), ty: "Long".into(), required: true },
                PropertyDef { name: "embedding".into(), ty: "vector(4)".into(), required: true },
            ],
            derived: vec![],
            identity: Some("id".into()),
        })
        .await
        .expect("define_type");

    if with_index {
        cp.ontology()
            .define_vector_index(VectorIndexDef {
                name: "by_flat".into(),
                type_name: TypeName("Docs".into()),
                property: "embedding".into(),
                metric: Metric::Cosine,
                spec: IndexSpec::Flat,
            })
            .await
            .expect("define_vector_index");
    }
    (catalog, pool, table, wh)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_enqueues_one_build_job_per_declared_index() {
    let fx = PgFixture::start();
    let (_cp_init, db) = fx.fresh_db().await;
    let (catalog, pool, table, _wh) = setup(&fx, &db, true).await;

    // Land vector rows INLINE (inline_byte_limit = usize::MAX) so the flush has live
    // inline rows to drain. (The "vectors can't inline" comment in vector_index_build.rs
    // is stale — vector_search.rs:388 lands inline with usize::MAX.)
    let run = RunId(uuid::Uuid::new_v4());
    let rows: &[(i64, [f32; 4])] = &[(1, [1.0, 0.0, 0.0, 0.0]), (2, [0.0, 1.0, 0.0, 0.0])];
    land(&pool, &catalog, &table, &columns(), &ipc_body(rows), usize::MAX, i64::MAX, lineage(run, &table))
        .await.expect("land inline");

    let before: i64 = sqlx::query_scalar("select count(*) from queue.jobs where kind = $1")
        .bind(control_plane_core::BUILD_VECTOR_INDEX_JOB_KIND)
        .fetch_one(&pool).await.expect("count before");
    assert_eq!(before, 0);

    flush_table(&catalog, &pool, &table, RunId(uuid::Uuid::new_v4())).await.expect("flush");

    let payloads: Vec<String> = sqlx::query_scalar(
        "select payload::text from queue.jobs where kind = $1")
        .bind(control_plane_core::BUILD_VECTOR_INDEX_JOB_KIND)
        .fetch_all(&pool).await.expect("jobs");
    assert_eq!(payloads.len(), 1, "exactly one rebuild enqueued");
    let p: serde_json::Value = serde_json::from_str(&payloads[0]).expect("payload json");
    assert_eq!(p["schema"], "wh");
    assert_eq!(p["name"], "docs");
    assert_eq!(p["index_name"], "by_flat");
}
```

(Import `SqlCatalog` + `SqlCatalogBuilder` + the `SQL_CATALOG_PROP_*` consts exactly as `tests/vector_index_build.rs` does, since `make_catalog` is copied from there.)

Wire the BUCK target (in `src/control-plane/postgres/BUCK`, mirror the `vector-index-build` target):

```bzl
loom_fixture_test(
    name = "flush-vector-rebuild",
    crate = "flush_vector_rebuild",
    srcs = ["tests/flush_vector_rebuild.rs"],
    crate_root = "tests/flush_vector_rebuild.rs",
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

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test //src/control-plane/postgres:flush-vector-rebuild > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: FAIL — `assert_eq!(rows.len(), 1)` fails with `0` (no enqueue exists yet on `main`).

- [ ] **Step 3: Add the atomic insert-if-absent primitive**

In `src/control-plane/postgres/src/queue.rs`, add below `pg_insert`:

```rust
/// Enqueue `job` only if no job with the same `kind` + `payload` is already
/// **pending** (`state = 'available'`). The absence test and the insert are one
/// statement, so there is no check-then-insert race with a concurrent worker:
/// a matching job that transitions to `running` between two callers no longer
/// suppresses the insert (only `available` rows match). `pg_notify` fires only on
/// an actual insert (the CTE produces no row when deduped), so a coalesced enqueue
/// wakes no worker. Returns `Some(id)` when inserted, `None` when deduped.
pub(crate) async fn pg_insert_if_absent<'e, E: sqlx::PgExecutor<'e>>(
    ex: E,
    job: &NewJob,
) -> Result<Option<JobId>> {
    let id = Uuid::new_v4();
    // `.execute()` (not `.fetch_*`) deliberately mirrors `pg_insert`: the final
    // SELECT carries a `pg_notify(...)` `void` column, and execute never decodes the
    // returned rows — it only reports the row count. The top-level SELECT yields one
    // row per inserted `ins` row (1 on insert, 0 when the NOT EXISTS dedups), so
    // `rows_affected() > 0` ⇔ a job was enqueued, and `pg_notify` fires exactly once
    // per actual insert (never on a dedup).
    let result = sqlx::query!(
        "with ins as ( \
             insert into queue.jobs (id, kind, payload, state, run_at, priority) \
             select $1, $2, $3, 'available', coalesce($4, now()), $5 \
             where not exists ( \
                 select 1 from queue.jobs \
                 where kind = $2 and payload = $3 and state = 'available') \
             returning id, kind) \
         select id, pg_notify('loom_queue:' || kind, '') from ins",
        id,
        &job.kind,
        &job.payload,
        job.run_at,
        job.priority,
    )
    .execute(ex)
    .await
    .map_err(backend)?;
    Ok((result.rows_affected() > 0).then_some(JobId(id)))
}
```

(If `Uuid`/`backend`/`NewJob`/`JobId` are not already in scope in `queue.rs`, they are — `pg_insert` uses all four; match its imports. `pg_insert` already proves a `query!` macro compiles with a `pg_notify` `void` column, so the macro will accept this SELECT.)

- [ ] **Step 4: Add `jobs` to `CommitExtras` and insert inside the commit tx**

In `src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs`, add a field to `CommitExtras` (after `overwrite`):

```rust
    /// Enqueue these jobs in the same commit tx, deduped pending-only via
    /// `queue::pg_insert_if_absent`. Empty for every path except the flush's
    /// vector-index auto-rebuild. Re-presented on each commit-retry attempt and
    /// only persisted by the winning, committed attempt.
    pub jobs: &'a [control_plane_core::NewJob],
```

In `do_update_table`, between the `lineage` block (ends line ~566) and `tx.commit()` (line ~568), add:

```rust
        for job in extras.jobs {
            crate::queue::pg_insert_if_absent(&mut *tx, job)
                .await
                .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;
        }
```

Update the doc comment on `do_update_table` to mention the jobs leg alongside lineage/end-cap.

- [ ] **Step 5: Thread `jobs` through the writer decorator**

In `src/control-plane/postgres/src/iceberg_writer.rs`:

Add to `CommitExtrasCatalog<'a>` (after `overwrite`): `jobs: &'a [control_plane_core::NewJob],`

In its `Debug` impl, add `.field("jobs", &self.jobs.len())`.

In `update_table`, pass the field through:

```rust
                CommitExtras {
                    lineage: self.lineage,
                    end_cap: self.end_cap.as_ref().map(|c| InlineEndCap {
                        table_id: c.table_id,
                        row_ids: c.row_ids,
                    }),
                    overwrite: self.overwrite,
                    jobs: self.jobs,
                },
```

Change `append_batches_with_extras` to accept `jobs` (append last) and set it on the wrapper:

```rust
pub async fn append_batches_with_extras(
    catalog: &SqlCatalog,
    table: &Table,
    batches: Vec<RecordBatch>,
    lineage: Option<&LineageEvent>,
    end_cap: Option<InlineEndCap<'_>>,
    overwrite: bool,
    jobs: &[control_plane_core::NewJob],
) -> Result<Vec<WrittenFile>> {
    // ... unchanged body ...
    let wrapper = CommitExtrasCatalog { inner: catalog, lineage, end_cap, overwrite, jobs };
    // ... unchanged ...
}
```

Update `append_batches_with_lineage` (the other caller) to pass `&[]`:

```rust
    append_batches_with_extras(catalog, table, batches, Some(lineage), None, false, &[]).await
```

Add `use control_plane_core::NewJob;` if cleaner, or reference the full path as above (match the file's existing import style — it already imports `control_plane_core::LineageEvent`).

- [ ] **Step 6: Thread `jobs` through the landing path**

In `src/control-plane/postgres/src/iceberg_landing.rs`:

`append_parquet_snapshot` gains a `jobs: &[control_plane_core::NewJob]` parameter (append last). Pass it to both downstream calls:

```rust
                return land_additive(pool, catalog, table, columns, batches, lineage, end_cap, jobs)
                    .await;
```
```rust
    append_batches_with_extras(catalog, &ice_table, batches, lineage, end_cap, overwrite, jobs)
        .await
        .map_err(be)?;
```

`land_additive` gains the same `jobs: &[control_plane_core::NewJob]` parameter (append last). In its own commit tx, after the `lineage` block (line ~388) and before `tx.commit()` (line ~389), add:

```rust
    for job in jobs {
        crate::queue::pg_insert_if_absent(&mut *tx, job).await?;
    }
```

- [ ] **Step 7: Update the landing caller(s) of `append_parquet_snapshot`**

Find every caller: `grep -rn "append_parquet_snapshot(" src/control-plane/postgres/src/`. There are **three**: `iceberg_flush.rs` (the flush, passes `&rebuild_jobs`), and **two** non-flush landing callers that each pass `&[]` (they do not auto-rebuild) — `land_parquet` (`iceberg_landing.rs:~540`) **and** `overwrite_parquet_snapshot` (`iceberg_landing.rs:~579`). Do not miss the overwrite path. Also re-grep callers of `append_batches_with_extras` (`grep -rn "append_batches_with_extras(" src/`) and pass `&[]` to any not already updated (the writer's `append_batches_with_lineage` and the landing call at `iceberg_landing.rs:226` are the known ones; both handled in Steps 5-6).

- [ ] **Step 8: Add the declared-index-names resolver**

In `src/control-plane/postgres/src/vector_index.rs`, add:

```rust
/// The names of every declared vector index on `table`, or an empty vec when the
/// table has no bound ontology type (a landed-but-unbound dataset has no indexes).
/// Same set a build resolves (`ontology.vector_index_definition` by `type_name`).
pub(crate) async fn declared_vector_index_names(
    pool: &PgPool,
    table: &TableRef,
) -> Result<Vec<String>> {
    let type_name = match type_name_for(pool, table).await {
        Ok(t) => t,
        Err(ControlPlaneError::NotFound(_)) => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    sqlx::query_scalar!(
        "select name from ontology.vector_index_definition where type_name = $1",
        type_name,
    )
    .fetch_all(pool)
    .await
    .map_err(backend)
}
```

(Confirm `ControlPlaneError`/`backend`/`TableRef`/`PgPool` imports are present — `type_name_for` in the same file uses them.)

- [ ] **Step 9: Wire the enqueue into `flush_locked`**

In `src/control-plane/postgres/src/iceberg_flush.rs`, after the `end_cap` is built (line ~99) and before `append_parquet_snapshot`, resolve the declared indexes and build one job per index:

```rust
    // Schedule a rebuild of every vector index this flush is about to stale: the
    // end-cap below removes these rows from the hot inline delta, and the cold
    // Puffin index was built at an older snapshot, so without a rebuild the
    // just-flushed vectors vanish from k-NN until the next build. Enqueued inside
    // the snapshot-commit tx (via CommitExtras.jobs) so a flush that commits can
    // never forget its rebuild; deduped pending-only by pg_insert_if_absent. The
    // per-table pg_advisory_xact_lock held by flush_table for this whole call
    // serializes same-table flushes (the only same-payload inserters), so the
    // INSERT ... WHERE NOT EXISTS cannot race a second concurrent flush of this table.
    let index_names = crate::vector_index::declared_vector_index_names(pool, table).await?;
    let rebuild_jobs: Vec<control_plane_core::NewJob> = index_names
        .iter()
        .map(|index_name| {
            let payload = serde_json::to_value(control_plane_core::BuildVectorIndexJob {
                schema: table.schema.clone(),
                name: table.name.clone(),
                index_name: index_name.clone(),
            })
            .map_err(|e| ControlPlaneError::Backend(e.to_string().into()))?;
            Ok(control_plane_core::NewJob {
                kind: control_plane_core::BUILD_VECTOR_INDEX_JOB_KIND.to_string(),
                payload,
                run_at: None,
                priority: 0,
            })
        })
        .collect::<Result<Vec<_>>>()?;
```

Then pass `&rebuild_jobs` as the new last argument to `append_parquet_snapshot`:

```rust
    let snap = append_parquet_snapshot(
        pool,
        catalog,
        table,
        &columns,
        vec![batch],
        Some(&lineage),
        Some(end_cap),
        false,
        &rebuild_jobs,
    )
    .await?;
```

(`ControlPlaneError` and `Result` are already imported in this file; `control_plane_core` is the `use` at the top.)

- [ ] **Step 10: Refresh the `.sqlx` cache and build**

Run: `tools/sqlx-prepare.sh` then commit the `.sqlx/` change.
Run: `buck2 build //src/control-plane/postgres:postgres > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|error\[|error:" /tmp/b.log`
Expected: build succeeds; **one** new `query-*.json` file appears under `.sqlx/` — only the `pg_insert_if_absent` CTE is new. `declared_vector_index_names`'s SQL (`select name from ontology.vector_index_definition where type_name = $1`) is byte-identical to the existing query in `ontology.rs` (`vector_indexes_for`), and sqlx keys cache files on the SQL hash, so it reuses that file. Do not be alarmed by only one new file.

- [ ] **Step 11: Run the auto-enqueue test — expect PASS**

Run: `buck2 test //src/control-plane/postgres:flush-vector-rebuild > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS.

- [ ] **Step 12: Add the remaining mechanics tests (no-index, no-op, dedup, pending-only)**

Append to `tests/flush_vector_rebuild.rs`. A small local helper keeps the bodies tight:

```rust
async fn land_inline(catalog: &SqlCatalog, pool: &sqlx::PgPool, table: &TableRef, rows: &[(i64, [f32; 4])]) {
    let run = RunId(uuid::Uuid::new_v4());
    land(pool, catalog, table, &columns(), &ipc_body(rows), usize::MAX, i64::MAX, lineage(run, table))
        .await.expect("land inline");
}
async fn build_job_count(pool: &sqlx::PgPool) -> i64 {
    sqlx::query_scalar("select count(*) from queue.jobs where kind = $1")
        .bind(control_plane_core::BUILD_VECTOR_INDEX_JOB_KIND)
        .fetch_one(pool).await.expect("count")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_without_declared_index_enqueues_nothing() {
    let fx = PgFixture::start();
    let (_cp_init, db) = fx.fresh_db().await;
    let (catalog, pool, table, _wh) = setup(&fx, &db, false).await; // type, but NO index
    land_inline(&catalog, &pool, &table, &[(1, [1.0, 0.0, 0.0, 0.0])]).await;

    flush_table(&catalog, &pool, &table, RunId(uuid::Uuid::new_v4())).await.expect("flush");

    assert_eq!(build_job_count(&pool).await, 0, "no declared index -> no rebuild enqueued");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn noop_flush_enqueues_nothing() {
    let fx = PgFixture::start();
    let (_cp_init, db) = fx.fresh_db().await;
    let (catalog, pool, table, _wh) = setup(&fx, &db, true).await;
    // Land rows straight to PARQUET (inline_byte_limit = 0): a snapshot exists but there
    // are NO live inline rows, so the flush hits the `inline_live_batch` == None no-op branch.
    let run = RunId(uuid::Uuid::new_v4());
    land(&pool, &catalog, &table, &columns(), &ipc_body(&[(1, [1.0, 0.0, 0.0, 0.0])]),
        0, i64::MAX, lineage(run, &table)).await.expect("land parquet");

    flush_table(&catalog, &pool, &table, RunId(uuid::Uuid::new_v4())).await.expect("flush");

    assert_eq!(build_job_count(&pool).await, 0, "no-op flush enqueues no rebuild");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_flushes_with_pending_build_enqueue_one() {
    let fx = PgFixture::start();
    let (_cp_init, db) = fx.fresh_db().await;
    let (catalog, pool, table, _wh) = setup(&fx, &db, true).await;

    land_inline(&catalog, &pool, &table, &[(1, [1.0, 0.0, 0.0, 0.0])]).await;
    flush_table(&catalog, &pool, &table, RunId(uuid::Uuid::new_v4())).await.expect("flush 1");
    assert_eq!(build_job_count(&pool).await, 1);

    // Second flush while the first build is still `available` (pending) -> deduped.
    land_inline(&catalog, &pool, &table, &[(2, [0.0, 1.0, 0.0, 0.0])]).await;
    flush_table(&catalog, &pool, &table, RunId(uuid::Uuid::new_v4())).await.expect("flush 2");
    assert_eq!(build_job_count(&pool).await, 1, "pending build dedups the second rebuild");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_while_build_running_enqueues_a_fresh_pending() {
    let fx = PgFixture::start();
    let (_cp_init, db) = fx.fresh_db().await;
    let (catalog, pool, table, _wh) = setup(&fx, &db, true).await;

    land_inline(&catalog, &pool, &table, &[(1, [1.0, 0.0, 0.0, 0.0])]).await;
    flush_table(&catalog, &pool, &table, RunId(uuid::Uuid::new_v4())).await.expect("flush 1");
    assert_eq!(build_job_count(&pool).await, 1);

    // Simulate the build dequeued and running (its covered snapshot now fixed < the next flush).
    sqlx::query("update queue.jobs set state = 'running' where kind = $1 and state = 'available'")
        .bind(control_plane_core::BUILD_VECTOR_INDEX_JOB_KIND)
        .execute(&pool).await.expect("mark running");

    // A flush now MUST enqueue a fresh pending build (the running one can't cover these rows).
    land_inline(&catalog, &pool, &table, &[(2, [0.0, 1.0, 0.0, 0.0])]).await;
    flush_table(&catalog, &pool, &table, RunId(uuid::Uuid::new_v4())).await.expect("flush 2");
    assert_eq!(build_job_count(&pool).await, 2, "running build does NOT suppress (pending-only dedup)");
}
```

- [ ] **Step 13: Run the full new test file — expect PASS**

Run: `buck2 test //src/control-plane/postgres:flush-vector-rebuild > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: all four/five tests PASS.

- [ ] **Step 14: Run the postgres crate's existing fixture suite (behavior-preservation)**

Run: `buck2 test //src/control-plane/postgres/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: all green — landing, inline-flush-trigger, sqlx-cache-check, and the existing vector-index tests unaffected (jobs default to `&[]` everywhere except the flush path).

- [ ] **Step 15: Commit**

```bash
git add src/control-plane/postgres/src/queue.rs \
        src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs \
        src/control-plane/postgres/src/iceberg_writer.rs \
        src/control-plane/postgres/src/iceberg_landing.rs \
        src/control-plane/postgres/src/vector_index.rs \
        src/control-plane/postgres/src/iceberg_flush.rs \
        src/control-plane/postgres/tests/flush_vector_rebuild.rs \
        src/control-plane/postgres/BUCK \
        src/control-plane/postgres/.sqlx
git commit -m "feat(query): auto-enqueue vector-index rebuild on flush (atomic, pending-only dedup)"
```

---

## Task 2: Freshness end-to-end (engine-serving)

Deliver: the spec's headline red test — a just-flushed vector goes missing from k-NN at the post-flush snapshot, then becomes visible again once the auto-enqueued rebuild runs.

**Files:**
- Create: `src/services/engine-serving/tests/vector_index_auto_rebuild.rs`
- Modify: `src/services/engine-serving/BUCK` (new `loom_fixture_test` target, mirror `vector-search` at lines 65-87)

**Interfaces:**
- Consumes: `engine_serving::vector_search(catalog, pool, table, index_name, query, k, nprobe, ef_search)`, `postgres::iceberg_flush::flush_table`, `postgres::vector_index::build_vector_index`, the `seed_and_build`-style helpers from `tests/vector_search.rs`.

- [ ] **Step 1: Write the failing freshness test**

Create `src/services/engine-serving/tests/vector_index_auto_rebuild.rs`. **Copy these helpers verbatim from `tests/vector_search.rs`:** `columns()`, `ipc_body(...)`, `lineage_evt(...)`, `make_catalog(...)`, `ids(...)`, and the whole `seed_and_build(fx, db, metric)` (`vector_search.rs:120-215` — it defines the type, lands cold rows 1-4 to Parquet, declares `by_flat`, and builds the index at covered snapshot `S`). This test then mirrors `knn_cold_hot_merge_cosine` (`vector_search.rs:366-423`) for the land-inline + query setup — using its **proven non-colliding vectors** (row 5 `[0.95,0.05,0,0]`, query `[0.9,0.1,0,0]`, `k=2`, where row 5 is strictly nearest) — and adds the flush → gap → rebuild → restored arc.

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flushed_vector_is_missing_then_restored_by_auto_rebuild() {
    use control_plane_core::{Metric, RunId, TableRef};
    use control_plane_postgres::iceberg_flush::flush_table;
    use control_plane_postgres::vector_index::build_vector_index;

    let fx = PgFixture::start();
    let (_cp_init, db) = fx.fresh_db().await;
    let table = TableRef { schema: "wh".into(), name: "docs".into() };

    // 1. Cold rows 1-4 + flat index built at covered_snapshot S.
    let (catalog, pool, _cp, _wh) = seed_and_build(&fx, &db, Metric::Cosine).await;

    // 2. Land row 5 INLINE (born after S) — the strictly-nearest vector to the query,
    //    living only in the hot delta. (Mirrors knn_cold_hot_merge_cosine.)
    let run = RunId(uuid::Uuid::new_v4());
    let inline: &[(i64, [f32; 4])] = &[(5, [0.95, 0.05, 0.0, 0.0])];
    land(&pool, &catalog, &table, &columns(), &ipc_body(inline), usize::MAX, i64::MAX, lineage_evt(run, &table))
        .await.expect("land inline row 5");

    let q = &[0.9_f32, 0.1, 0.0, 0.0];

    // Sanity (hot-delta merge): row 5 is the nearest and visible BEFORE the flush.
    let hot = engine_serving::vector_search(&catalog, &pool, &table, "by_flat", q, 2, None, None)
        .await.expect("knn pre-flush");
    assert_eq!(ids(&hot)[0], 5, "inline row is nearest before flush");

    // 3. Flush: drains row 5 to cold Parquet, end-caps it, AND auto-enqueues a rebuild.
    flush_table(&catalog, &pool, &table, RunId(uuid::Uuid::new_v4())).await.expect("flush");

    // 4. GAP (the bug this slice fixes): row 5 left the hot delta (end-capped) and is not
    //    in the cold index (built at the older S) -> missing from k-NN.
    let gap = engine_serving::vector_search(&catalog, &pool, &table, "by_flat", q, 2, None, None)
        .await.expect("knn post-flush");
    assert!(!ids(&gap).contains(&5), "just-flushed row is in the visibility gap");

    // 5. Drain the auto-enqueued rebuild (simulate the worker): read the enqueued job's
    //    index_name and run the build. The fetch_one FAILS without this slice (no job).
    let index_name: String = sqlx::query_scalar(
        "select payload->>'index_name' from queue.jobs where kind = $1")
        .bind(control_plane_core::BUILD_VECTOR_INDEX_JOB_KIND)
        .fetch_one(&pool).await.expect("a rebuild job was auto-enqueued by the flush");
    assert_eq!(index_name, "by_flat");
    build_vector_index(&catalog, &pool, &table, &index_name, RunId(uuid::Uuid::new_v4()))
        .await.expect("rebuild");

    // 6. Fresh again: the rebuilt cold index (covered_snapshot advanced past the flush)
    //    once more makes row 5 the nearest.
    let fresh = engine_serving::vector_search(&catalog, &pool, &table, "by_flat", q, 2, None, None)
        .await.expect("knn post-rebuild");
    assert_eq!(ids(&fresh)[0], 5, "auto-rebuild restored the flushed row");
}
```

`ids` returns the identity column in row order (`vector_search.rs:97`); `ids(&batch)[0]` is the nearest. Keep `_wh` alive for the whole test (the `seed_and_build` tuple's 4th element).

Wire the BUCK target (mirror `vector-search` at `src/services/engine-serving/BUCK:65-87`, name `vector-index-auto-rebuild`, crate_root the new file; deps identical to `vector-search`).

- [ ] **Step 2: Run it to verify it fails (on the implemented Task 1 tree)**

Run: `buck2 test //src/services/engine-serving:vector-index-auto-rebuild > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|panicked" /tmp/t.log`
Expected (if Task 1 is committed): PASS. To prove the slice is what turns it green, also confirm the `fetch_one(...).expect("a rebuild job was auto-enqueued")` line is the assertion that would fail against pre-Task-1 code — note this in the PR (you can temporarily `git stash` Task 1's `iceberg_flush.rs` change to demonstrate the red, then restore).

> Rationale for the apparent ordering: the freshness behavior depends on Task 1's enqueue. We keep Task 1 (the mechanism, with its own fail-first unit test) before Task 2 (the end-to-end proof). Task 2's value is the user-visible guarantee, exercised through the real k-NN read path that the unit tests cannot reach (engine-serving does not link from the postgres crate's tests).

- [ ] **Step 3: Run the engine-serving suite (no regressions)**

Run: `buck2 test //src/services/engine-serving/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: all green, including the existing `vector-search` tests.

- [ ] **Step 4: Commit**

```bash
git add src/services/engine-serving/tests/vector_index_auto_rebuild.rs \
        src/services/engine-serving/BUCK
git commit -m "test(query): end-to-end vector-index freshness across flush + auto-rebuild"
```

---

## Task 3: Close the register item

**Files:**
- Modify: `docs/ROADMAP.md` (mark the item done)

- [ ] **Step 1: Update the ROADMAP entry**

Invoke the `loom-docs-update` skill (it runs as part of finishing the branch). It flips `road-vector-index-auto-rebuild` from `- [ ]` to `- [x]`, sets `status:done`, and adds `pr:#N` once the PR number is known. The line at `docs/ROADMAP.md:75` currently reads:

```
- [ ] **Vector index auto-rebuild on flush (freshness-on-staleness)** `{#road-vector-index-auto-rebuild area:query status:planned from:2026-06-29-vector-index-auto-rebuild-design pr:- spec:2026-06-29-vector-index-auto-rebuild-design}`
```

becomes (with the real PR number):

```
- [x] **Vector index auto-rebuild on flush (freshness-on-staleness)** `{#road-vector-index-auto-rebuild area:query status:done from:2026-06-29-vector-index-auto-rebuild-design pr:#N spec:2026-06-29-vector-index-auto-rebuild-design}`
```

- [ ] **Step 2: Validate the registers**

Run: `bash tools/docs.sh validate`
Expected: passes (grammar, ids, links, spec-on-disk all resolve).

- [ ] **Step 3: Run the lint hooks (markdown EOF/whitespace)**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/lint.log 2>&1; grep -E "Passed|Failed" /tmp/lint.log`
Expected: pass; commit any in-place fixes the hooks make (the spec/plan/ROADMAP edits must end with exactly one trailing newline and no trailing whitespace).

---

## Whole-implementation verification (before finishing)

- [ ] Full sweep: `buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log` — all green (note: fixture tests can flake on cloud resource contention; re-run a clean sweep to confirm any failure is real).
- [ ] Clippy: `tools/clippy-all.sh` — clean.
- [ ] `.sqlx` committed and `sqlx-cache-check` green (it runs in the sweep above).
- [ ] Confirm behavior-preservation: every non-flush caller passes `&[]` for `jobs`; the inline-flush trigger and landing paths are byte-identical in effect.
