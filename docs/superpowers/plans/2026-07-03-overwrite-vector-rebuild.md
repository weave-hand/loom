# Overwrite Vector-Index Rebuild Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The governed UPDATE/DELETE overwrite/replace commit (both the non-empty and the zero-file truncate branch) enqueues one deduped `build_vector_index` job per declared index, via a `rebuild_jobs_for` helper shared with the flush path.

**Architecture:** Extract flush's job construction (`iceberg_flush.rs:119-137`) into `vector_index::rebuild_jobs_for(pool, table)`; `overwrite_parquet_snapshot` computes it internally — non-empty branch rides `CommitExtras.jobs`, `overwrite_truncate` loops `queue::pg_insert_if_absent` inside its own tx. `IcebergActionWriter::overwrite_table` calls `overwrite_parquet_snapshot` directly (`action_writer.rs:90`), so the engine-serving seam is covered without signature changes.

**Tech Stack:** Rust, buck2 `loom_fixture_test` (hermetic Postgres — on this root host run tests with `--unstable-allow-all-tests-on-re`).

**Spec:** `docs/superpowers/specs/2026-07-03-overwrite-vector-rebuild-design.md`

## Global Constraints

- Payloads byte-identical to flush's (`BuildVectorIndexJob { schema, name, index_name }`, kind `BUILD_VECTOR_INDEX_JOB_KIND`) so pending-dedup collides across paths.
- No `.sqlx` change expected (`rebuild_jobs_for` reuses `declared_vector_index_names`'s existing compile-time query). If the build demands a refresh, STOP and re-check — that means a query changed.
- No public-signature change to `overwrite_parquet_snapshot` or `overwrite_table`.
- Fixture tests use `loom_fixture_test` in BUCK, never bare `rust_test`; prek clean before every commit; test runs redirected to a file, never piped.

---

### Task 1: Extract `rebuild_jobs_for`; flush swaps to it (behavior-preserving)

**Files:**
- Modify: `src/control-plane/postgres/src/vector_index.rs` (new fn beside `declared_vector_index_names`, line ~128), `src/control-plane/postgres/src/iceberg_flush.rs:117-137`

**Interfaces:**
- Produces: `pub(crate) async fn rebuild_jobs_for(pool: &PgPool, table: &TableRef) -> Result<Vec<NewJob>>` in `vector_index.rs`.

- [x] **Step 1: Implement the helper** — move the construction verbatim:

```rust
/// One `build_vector_index` NewJob per vector index declared on the ontology
/// type backing `table` (empty when the table has no type or no indexes) —
/// the shared enqueue source for the flush AND overwrite/replace commit
/// paths, so their pending-dedup keys always collide.
pub(crate) async fn rebuild_jobs_for(pool: &PgPool, table: &TableRef) -> Result<Vec<NewJob>> {
    let index_names = declared_vector_index_names(pool, table).await?;
    index_names
        .iter()
        .map(|index_name| {
            let payload = serde_json::to_value(BuildVectorIndexJob {
                schema: table.schema.clone(),
                name: table.name.clone(),
                index_name: index_name.clone(),
            })
            .map_err(backend)?;
            Ok(NewJob {
                kind: BUILD_VECTOR_INDEX_JOB_KIND.to_string(),
                payload,
                run_at: None,
                priority: 0,
            })
        })
        .collect::<Result<Vec<_>>>()
}
```

Add the needed imports to `vector_index.rs` (`BUILD_VECTOR_INDEX_JOB_KIND`, `BuildVectorIndexJob`, `NewJob` from `control_plane_core` — mirror `iceberg_flush.rs:7-8`). In `iceberg_flush.rs`, replace lines 120-137 (the construction ONLY — keep the whole "atomic + deduped" comment at 117-119, wording adjusted to point at the shared helper) with `let rebuild_jobs = crate::vector_index::rebuild_jobs_for(pool, table).await?;` and keep the comment about atomic+deduped enqueue; drop now-unused imports.

- [x] **Step 2: Prove behavior preserved**

Run: `buck2 test //src/control-plane/postgres:flush-vector-rebuild --unstable-allow-all-tests-on-re > /tmp/o1.log 2>&1; grep -E "Tests finished|FAIL" /tmp/o1.log`
(Target name per BUCK — verify with `grep -n 'flush_vector_rebuild' src/control-plane/postgres/BUCK` and use the actual `name =`.)
Expected: PASS, 0 failures.

- [x] **Step 3: prek + commit**

```bash
buck2 run -v0 //tools:prek -- run --all-files > /tmp/p.log 2>&1; grep -c Failed /tmp/p.log  # 0
git add src/control-plane/postgres/src/vector_index.rs src/control-plane/postgres/src/iceberg_flush.rs
git commit -m "refactor(vector-index): extract rebuild_jobs_for from the flush enqueue"
```

### Task 2: Non-empty overwrite branch enqueues (red-first)

**Files:**
- Create: `src/control-plane/postgres/tests/overwrite_vector_rebuild.rs`
- Modify: `src/control-plane/postgres/BUCK` (new `loom_fixture_test` target `overwrite-vector-rebuild`, mirroring the `flush-vector-rebuild` stanza's deps exactly), `src/control-plane/postgres/src/iceberg_landing.rs:536-560`

**Interfaces:**
- Consumes: Task 1's `rebuild_jobs_for`.

- [x] **Step 1: Write the failing test.** New file `tests/overwrite_vector_rebuild.rs`: copy `flush_vector_rebuild.rs`'s header docs pattern, its `setup` fixture (adapt: declare **two** indexes — call `define_vector_index` twice, e.g. `by_flat` + `by_flat2`, both `IndexKind::Flat`/`Metric::Cosine` over the same column — mirror how `setup(fx, true)` declares one), and its `job_count`/`job_count_by_state` helpers verbatim. First test:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overwrite_enqueues_one_rebuild_per_declared_index() {
    let fx = PgFixture::shared(); // sync, returns &'static PgFixture
    let (_cp, catalog, pool, table, _wh) = setup(fx, true).await;
    // Seed rows exactly as flush_vector_rebuild.rs's tests do — copy its
    // shortest green test's land(...) sequence verbatim (it inlines `land`
    // with InlineLimits; do the same — loom_test_seed::land_vec4 has a
    // different signature and is NOT what that file uses).
    assert_eq!(job_count(&pool, BUILD_VECTOR_INDEX_JOB_KIND).await, 0, "seed must not enqueue");

    let (_schema, batches) = vec4_batches(&[(1, [0.1, 0.2, 0.3, 0.4])]);
    let ev = test_lineage(RunId("overwrite-1".into()), &table);
    overwrite_parquet_snapshot(&pool, &catalog, &table, &vec4_columns(), batches, Some(&ev))
        .await
        .expect("overwrite");

    assert_eq!(job_count_by_state(&pool, BUILD_VECTOR_INDEX_JOB_KIND, "available").await, 2);
    // payloads name each declared index exactly once
    let names: Vec<String> = sqlx::query_scalar(
        "select payload->>'index_name' from queue.jobs where kind = $1 order by 1",
    )
    .bind(BUILD_VECTOR_INDEX_JOB_KIND)
    .fetch_all(&pool)
    .await
    .expect("names");
    assert_eq!(names, vec!["by_flat", "by_flat2"]);
}
```

(Verified signatures: `PgFixture::shared()` is sync; `vec4_batches(rows: &[(i64, [f32; 4])]) -> (SchemaRef, Vec<RecordBatch>)`; `test_lineage(run: RunId, table: &TableRef)`; setup declares the SECOND index by a second `define_vector_index` call — legal, PK is `(type_name, name)`, precedent `vector_index_multi.rs`. Note: flush's seed lands INLINE (`inline_byte_limit: usize::MAX`); that is fine — overwrite end-caps inline rows too — but if file-backed seeding is wanted use `cold_limits()` from loom_test_seed.) Wire the BUCK target (copy the `flush-vector-rebuild` stanza, rename target/crate/srcs/crate_root).

- [x] **Step 2: Run to verify it fails**

Run: `buck2 test //src/control-plane/postgres:overwrite-vector-rebuild --unstable-allow-all-tests-on-re > /tmp/o2.log 2>&1; grep -E "Tests finished|FAIL" /tmp/o2.log`
Expected: FAIL — `job_count_by_state == 0` today (empty job set).

- [x] **Step 3: Implement** — in `overwrite_parquet_snapshot`, compute jobs before the branch and pass them:

```rust
    let rebuild_jobs = crate::vector_index::rebuild_jobs_for(pool, table).await?;
    if batches.iter().all(|b| b.num_rows() == 0) {
        return overwrite_truncate(pool, table, lineage, &rebuild_jobs).await;
    }
    append_parquet_snapshot(
        pool,
        catalog,
        table,
        columns,
        batches,
        CommitExtras {
            lineage,
            overwrite: true,
            jobs: &rebuild_jobs,
            ..CommitExtras::default()
        },
    )
    .await
```

(`overwrite_truncate` gains the param now but only *uses* it in Task 3 — name it `_jobs: &[NewJob]` in THIS task so clippy/prek stays clean (unused-variable would fail the hook), and rename to `jobs` when Task 3 adds the loop. Add `NewJob` to `iceberg_landing.rs`'s `control_plane_core` import list — it is not imported today.) Update `overwrite_parquet_snapshot`'s doc comment: overwrite commits now enqueue deduped index rebuilds like flush.

- [x] **Step 4: Run to verify pass** (same command). Expected: PASS.
- [x] **Step 5: prek + commit**

```bash
git add src/control-plane/postgres/src/iceberg_landing.rs src/control-plane/postgres/tests/overwrite_vector_rebuild.rs src/control-plane/postgres/BUCK
git commit -m "fix(vector-index): overwrite commits enqueue declared index rebuilds"
```

### Task 3: Truncate branch + cross-path dedup (red-first)

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_landing.rs:567-586` (`overwrite_truncate`), `tests/overwrite_vector_rebuild.rs`

- [x] **Step 1: Write the failing tests** — append to the test file:

```rust
#[tokio::test]
async fn truncate_overwrite_enqueues_rebuilds() {
    // same setup + seed as above, then a zero-row overwrite:
    overwrite_parquet_snapshot(&pool, &catalog, &table, &vec4_columns(), vec4_batches(&[]).1, Some(&ev)) // 0-row batch trips the truncate branch
        .await
        .expect("truncate");
    assert_eq!(job_count_by_state(&pool, BUILD_VECTOR_INDEX_JOB_KIND, "available").await, 2);
}

#[tokio::test]
async fn pending_rebuild_dedupes_across_overwrites() {
    // setup with ONE declared index; seed; first overwrite -> 1 available job
    // second overwrite while still available -> count stays 1
    // mark it running (copy flush_vector_rebuild.rs:345's update statement),
    // third overwrite -> a NEW available job appears (count 2 total, 1 available)
}
```

(Assemble the dedup test from flush_vector_rebuild.rs's TWO existing tests: `two_flushes_with_pending_build_enqueue_one` (:254, phases 1-2) and `flush_while_build_running_enqueues_a_fresh_pending` (:317, the running-state UPDATE at :345) — mirror with overwrite calls. Empty batch = `vec4_batches(&[]).1`.)

- [x] **Step 2: Run to verify the truncate test fails** (dedup test red only on its third phase if Task 2 shipped). Expected: `truncate_overwrite_enqueues_rebuilds` FAILS with 0 jobs.
- [x] **Step 3: Implement** — in `overwrite_truncate` (signature from Task 2: `jobs: &[NewJob]`), after the lineage emit and before `tx.commit()`:

```rust
    for job in jobs {
        // Same pending-dedup as CommitExtras.jobs; pg_notify is buffered until
        // this tx commits, so a rolled-back truncate enqueues nothing.
        crate::queue::pg_insert_if_absent(&mut *conn, job).await?;
    }
```

- [x] **Step 4: Run to verify pass** — the file's three tests green. Expected: PASS.
- [x] **Step 5: prek + commit**

```bash
git add src/control-plane/postgres/src/iceberg_landing.rs src/control-plane/postgres/tests/overwrite_vector_rebuild.rs
git commit -m "fix(vector-index): truncate overwrites enqueue deduped index rebuilds"
```

### Task 4: Engine-serving seam assertion + register close + sweep

**Files:**
- Modify: `src/services/engine-serving/tests/action_writer.rs` (+ its BUCK deps if the queue count needs sqlx — it already depends on `control-plane-postgres` fixture; check the stanza), `docs/ISSUES.md`, `docs/system-capabilities/vector-search.md`, `docs/system-capabilities/engine.md`

- [x] **Step 1: Seam test** — extend `action_writer.rs`: the seeded `Widget` type (:91-119) has NO vector property, so first extend its `define_type` seed with an `embedding vector(4)` property (landing derives schema from the caller's `columns`, so existing tests are unaffected), then a new test that declares TWO vector indexes (matching the spec's criterion-1 shape), drives `IcebergActionWriter::overwrite_table`, and asserts two `available` `build_vector_index` jobs (copy the `job_count` helper from `flush_vector_rebuild.rs:83-89`; add `//third-party:sqlx` to the `action-writer` BUCK stanza — it lacks it). Run the engine-serving `action-writer` target with `--unstable-allow-all-tests-on-re`. Expected: PASS (wiring proven at the postgres layer; this pins the seam). NOTE for the PR body: the spec's criterion 1 wanted the red-first two-index case driven through this seam; the plan does red-first at the postgres layer and pins the seam green-first — equivalent coverage since `overwrite_table` is a 10-line pure delegation (action_writer.rs:76-100).
- [x] **Step 2: Register close** per loom-docs-update: remove the `iss-overwrite-vector-index-staleness` entry from `docs/ISSUES.md`; `bash tools/docs.sh validate` OK. Sweep for dangling refs: `grep -rn 'iss-overwrite-vector-index-staleness' docs/ .claude/ src/` — rewrite the `[[...]]` link inside `docs/FUTURE.md`'s `fut-cow-arrow-native` entry (its "folds in [[iss-overwrite-vector-index-staleness]]" acceptance note) to state the rebuild-enqueue now exists (plain `` `#id` `` span + "fixed" phrasing), delete the Known-gaps bullet in `docs/system-capabilities/vector-search.md` (line ~41), AND fix the prose at vector-search.md:37 ("…the governed UPDATE/DELETE replace path is a known gap (below)") which the grep sweep will NOT catch — rewrite it to state the replace path now enqueues rebuilds.
- [x] **Step 3: Capability docs** — vector-search.md "Rebuild/staleness" theme + engine.md's overwrite paragraph: one sentence each — overwrite/truncate commits now enqueue deduped `build_vector_index` rebuilds via the shared `rebuild_jobs_for` seam `(#PRNUM)`.
- [x] **Step 4: Sweep** — affected targets: `buck2 test //src/control-plane/postgres:flush-vector-rebuild //src/control-plane/postgres:overwrite-vector-rebuild //src/control-plane/postgres:iceberg-overwrite //src/control-plane/postgres:overwrite-end-caps-inline //src/services/engine-serving:action-writer --unstable-allow-all-tests-on-re > /tmp/o4.log 2>&1; grep -E "Tests finished|FAIL" /tmp/o4.log` — expect PASS. (Deliberate narrowing of the spec's "full //src/... green" — reviewer verified no other test declares a vector index or counts build_vector_index jobs; CI's btd-affected job covers the remainder. Say so in the PR body.)
- [x] **Step 5: prek + commit**

```bash
git add -A
git commit -m "docs: close iss-overwrite-vector-index-staleness"
```
