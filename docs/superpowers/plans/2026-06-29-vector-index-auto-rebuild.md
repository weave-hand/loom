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

Create `src/control-plane/postgres/tests/flush_vector_rebuild.rs`. Mirror the setup idioms in `tests/inline_flush_trigger.rs` (hermetic PG via `PgFixture`, landing inline rows, calling `flush_table`) and `tests/vector_index_build.rs` (declaring a vector index via `cp.ontology().define_vector_index(...)`, building a `RecordBatch` with a `vector(N)` column). Read both of those files first and reuse their helper patterns (do not invent new seed plumbing).

The first test seeds a table with **live inline vector rows** and a **declared vector index**, flushes, and asserts exactly one `build_vector_index` job is enqueued with the right payload:

```rust
#[tokio::test]
async fn flush_enqueues_one_build_job_per_declared_index() {
    let fx = PgFixture::start().await;
    let pool = fx.fresh_db().await;
    let (catalog, _tmp) = make_catalog(&fx, &pool).await; // helper mirrored from vector_index_build.rs
    let cp = PgControlPlane::new(pool.clone());
    let table = TableRef { schema: "wh".into(), name: "docs".into() };

    // Define the ontology type + a flat vector index, and land inline vector rows
    // (inline_byte_limit large enough that the rows stay inline, i.e. NOT auto-flushed).
    seed_type_and_vector_rows(&cp, &catalog, &pool, &table).await; // helper: defines type, lands inline rows
    define_flat_index(&cp, &table, "by_flat").await;               // helper: cp.ontology().define_vector_index(...)

    // Sanity: no build job yet.
    let before: i64 = sqlx::query_scalar("select count(*) from queue.jobs where kind = $1")
        .bind(control_plane_core::BUILD_VECTOR_INDEX_JOB_KIND)
        .fetch_one(&pool).await.unwrap();
    assert_eq!(before, 0);

    flush_table(&catalog, &pool, &table, RunId::new()).await.unwrap();

    let rows: Vec<(String,)> = sqlx::query_as(
        "select payload::text from queue.jobs where kind = $1")
        .bind(control_plane_core::BUILD_VECTOR_INDEX_JOB_KIND)
        .fetch_all(&pool).await.unwrap();
    assert_eq!(rows.len(), 1, "exactly one rebuild enqueued");
    let payload: serde_json::Value = serde_json::from_str(&rows[0].0).unwrap();
    assert_eq!(payload["schema"], "wh");
    assert_eq!(payload["name"], "docs");
    assert_eq!(payload["index_name"], "by_flat");
}
```

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
    let row = sqlx::query!(
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
    .fetch_optional(ex)
    .await
    .map_err(backend)?;
    Ok(row.map(|_| JobId(id)))
}
```

(If `Uuid`/`backend`/`NewJob`/`JobId` are not already in scope in `queue.rs`, they are — `pg_insert` uses all four; match its imports.)

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

Find every caller: `grep -rn "append_parquet_snapshot(" src/control-plane/postgres/src/`. The non-flush caller is the landing `land(...)` Parquet path (in `iceberg_landing.rs`). Pass `&[]` there (landing does not auto-rebuild). Also re-grep callers of `append_batches_with_extras` (`grep -rn "append_batches_with_extras(" src/`) and pass `&[]` to any not already updated (e.g. transform/register paths).

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
    // never forget its rebuild; deduped pending-only by pg_insert_if_absent.
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
Expected: build succeeds; two new `query-*.json` files appear under `.sqlx/` (the insert-if-absent and the index-names query).

- [ ] **Step 11: Run the auto-enqueue test — expect PASS**

Run: `buck2 test //src/control-plane/postgres:flush-vector-rebuild > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS.

- [ ] **Step 12: Add the remaining mechanics tests (no-index, no-op, dedup, pending-only)**

Append to `tests/flush_vector_rebuild.rs`:

```rust
#[tokio::test]
async fn flush_without_declared_index_enqueues_nothing() {
    // seed inline rows + ontology type but NO define_flat_index; flush; assert 0 build jobs.
}

#[tokio::test]
async fn noop_flush_enqueues_nothing() {
    // declare index but land NO inline rows (or flush twice: the second flush is a no-op);
    // assert the no-op flush adds 0 build jobs.
}

#[tokio::test]
async fn two_flushes_with_pending_build_enqueue_one() {
    // declare index; land inline rows; flush -> 1 pending build job.
    // land more inline rows; flush again (the first build job is still `available`);
    // assert still exactly 1 build job (deduped pending-only).
}

#[tokio::test]
async fn flush_while_build_running_enqueues_a_fresh_pending() {
    // declare index; land inline rows; flush -> 1 pending job J.
    // Simulate J running: `update queue.jobs set state = 'running' where kind = $1`.
    // land more inline rows; flush again; assert TWO build jobs now exist
    // (the running J does not suppress — pending-only dedup).
}
```

Fill each with the concrete seed/flush/assert body, reusing the Step-1 helpers. For the running-state simulation use a raw `sqlx::query("update queue.jobs set state = 'running' where kind = $1 and state = 'available'").bind(BUILD_VECTOR_INDEX_JOB_KIND)`.

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

Create `src/services/engine-serving/tests/vector_index_auto_rebuild.rs`. Reuse the catalog/seed helpers from `tests/vector_search.rs` (read it first; copy the `make_catalog`/seed idioms — landing with `inline_byte_limit = 0` forces Parquet, but here we need rows to land **inline** first, so use a large inline limit / the inline landing path, then flush).

```rust
#[tokio::test]
async fn flushed_vector_is_missing_then_restored_by_auto_rebuild() {
    let fx = PgFixture::start().await;
    let pool = fx.fresh_db().await;
    let (catalog, _tmp) = make_catalog(&fx, &pool).await;
    let cp = PgControlPlane::new(pool.clone());
    let table = TableRef { schema: "wh".into(), name: "docs".into() };

    // 1. Define type, land an initial cold batch, declare + build the flat index.
    seed_initial_and_build(&cp, &catalog, &pool, &table, "by_flat").await;

    // 2. Land MORE vector rows INLINE (born after the index's covered_snapshot),
    //    incl. a distinctive query-target vector q = [1,0,0,0].
    land_inline_vectors(&catalog, &pool, &table, &[(99_i64, [1.0_f32,0.0,0.0,0.0])]).await;

    // The hot-delta merge means k-NN currently DOES find row 99 (inline). Sanity:
    let hot = engine_serving::vector_search(&catalog, &pool, &table, "by_flat",
        &[1.0_f32,0.0,0.0,0.0], 1, None, None).await.unwrap();
    assert!(ids(&hot).contains(&99), "inline row visible before flush");

    // 3. Flush: drains row 99 to cold Parquet, end-caps it, auto-enqueues a rebuild.
    flush_table(&catalog, &pool, &table, RunId::new()).await.unwrap();

    // 4. GAP (red on main): row 99 left the hot delta (end-capped) and is not in the
    //    cold index (built at the older covered_snapshot) -> missing from k-NN.
    let gap = engine_serving::vector_search(&catalog, &pool, &table, "by_flat",
        &[1.0_f32,0.0,0.0,0.0], 1, None, None).await.unwrap();
    assert!(!ids(&gap).contains(&99), "just-flushed row is in the visibility gap");

    // 5. Drain the auto-enqueued rebuild (simulate the worker): read the enqueued
    //    job's index_name and run the build directly. FAILS ON MAIN: no job enqueued.
    let index_name: String = sqlx::query_scalar(
        "select payload->>'index_name' from queue.jobs where kind = $1")
        .bind(control_plane_core::BUILD_VECTOR_INDEX_JOB_KIND)
        .fetch_one(&pool).await.expect("a rebuild job was auto-enqueued by the flush");
    build_vector_index(&catalog, &pool, &table, &index_name, RunId::new()).await.unwrap();

    // 6. Fresh again: the rebuilt cold index (covered_snapshot advanced past the flush)
    //    now contains row 99.
    let fresh = engine_serving::vector_search(&catalog, &pool, &table, "by_flat",
        &[1.0_f32,0.0,0.0,0.0], 1, None, None).await.unwrap();
    assert!(ids(&fresh).contains(&99), "auto-rebuild restored the flushed row");
}
```

Provide the local helpers (`seed_initial_and_build`, `land_inline_vectors`, `ids`) by adapting `tests/vector_search.rs` — `ids` extracts the identity column from the returned `RecordBatch` (copy its existing id-extraction helper).

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
