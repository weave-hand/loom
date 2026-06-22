# Flush Idempotency (Duplicate-Dispatch) Over-the-Wire Test Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add one over-the-wire regression test proving that two concurrent `flush_table` dispatches of the same table (modeling at-least-once redelivery of one job to two workers) write the rows exactly once.

**Architecture:** Test-only. `flush_table` (`src/control-plane/postgres/src/iceberg_flush.rs`) already takes a transaction-scoped `pg_advisory_xact_lock` on a per-table key before reading any state and holds it across the whole flush, so the second dispatch blocks, then re-reads `current_snapshot`/`inline_live_batch`, finds the inline rows already end-capped, and returns `Ok(None)`. The new test pins this exactly-once guarantee at the dispatch boundary (`Some(snap)` XOR `None`) and the storage layer (3 rows, not 6; inline retired). No production code changes.

**Tech Stack:** Rust, `tokio` multi-thread test, `tonic`/UDS engine wire, hermetic Postgres+DuckDB fixture (`loom_fixture_test` — the `worker:e2e` target already is one).

## Global Constraints

- **No production code change** — `flush_table`, the engine service, the worker, and the wire stay unmodified; the test asserts existing behavior. (Spec: "Test-only; no production code changes.")
- **No new helpers, no new BUCK target** — reuse the existing `src/services/worker/tests/e2e.rs` harness (`spawn_server`, `make_catalog`, `columns`, `inline_batch`, `inline_lineage`) and the imports already in that file. The `worker:e2e` `rust_test`/`loom_fixture_test` target already depends on `engine`, `engine-wire`, `control-plane-postgres`, `iceberg`, `tokio`, `uuid`.
- **Tests are `rust_test` integration targets only** — the new test goes in the existing `tests/e2e.rs`; no inline `#[cfg(test)]` module (the `no-inline-tests` hook enforces this).
- **Markdown lint** — `docs/ISSUES.md` must end with exactly one trailing newline and no trailing whitespace.

---

### Task 1: Add the concurrent-dispatch idempotency test

**Files:**
- Modify: `src/services/worker/tests/e2e.rs` — append one test function (after the existing `inline_threshold_enqueues_and_worker_flushes`).
- Modify: `docs/ISSUES.md` — close `iss-flush-at-least-once-idempotency`.

**Interfaces (all already in scope — verified against the codebase):**
- Consumes from the existing `e2e.rs` harness: `spawn_server(&fx, &db) -> (tempfile::TempDir, String)`, `columns() -> Vec<ColumnSpec>`, `inline_batch(&[i64]) -> RecordBatch`, `inline_lineage(RunId, &TableRef) -> LineageEvent`.
- Consumes (already imported in `e2e.rs`): `PgFixture`, `control_plane_postgres::iceberg_inline::inline_append`, `IcebergCatalog::{new, current_snapshot, files, inline_parquet}`, `GrpcQueueClient::{connect, clone, flush_table}`, `TableRef`, `RunId`, `PageReq`, `uuid`.
- API shapes (verified): `inline_append(&pool, &TableRef, &[ColumnSpec], &RecordBatch, LineageEvent, Option<i64>) -> Result<...>`; `GrpcQueueClient::flush_table(&self, schema: String, name: String) -> Result<Option<i64>>` (`&self`, type is `Clone`); file entry from `files(...).items` has `record_count: i64`; `inline_parquet(...) -> Result<Option<...>>`.
- Produces: nothing other tasks rely on (terminal task).

- [ ] **Step 1: Add the test function**

Append to `src/services/worker/tests/e2e.rs` (after the existing test):

```rust
/// At-least-once redelivery: two workers dispatch the SAME flush_table job
/// concurrently. The per-table advisory lock makes the duplicate a safe no-op —
/// the rows are written exactly once. Holds under any interleaving, so non-flaky.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_dispatch_flush_is_idempotent_over_the_wire() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let (_sock_dir, sock) = spawn_server(&fx, &db).await;

    let table = TableRef {
        schema: "wh".into(),
        name: "t".into(),
    };
    let run = RunId(uuid::Uuid::new_v4());

    // Land three inline rows directly (threshold None — we drive the duplicate
    // dispatch ourselves rather than through the queue; two flush RPCs of the same
    // table *is* the same job delivered twice).
    inline_append(
        &pool,
        &table,
        &columns(),
        &inline_batch(&[1, 2, 3]),
        inline_lineage(run, &table),
        None,
    )
    .await
    .expect("inline_append");

    // Two concurrent flush_table RPCs for the same table (one cloned client,
    // tonic multiplexes; the engine runs each as its own handler task).
    let c1 = GrpcQueueClient::connect(&sock).await.expect("connect");
    let c2 = c1.clone();
    let (a, b) = tokio::join!(
        c1.flush_table("wh".into(), "t".into()),
        c2.flush_table("wh".into(), "t".into()),
    );
    let a = a.expect("rpc a ok");
    let b = b.expect("rpc b ok");

    // (1) Exactly one dispatch did the work; the other was a no-op.
    assert!(
        a.is_some() ^ b.is_some(),
        "exactly one flush wrote a snapshot (got {a:?}, {b:?})"
    );
    assert!(
        a.is_none() || b.is_none(),
        "the duplicate dispatch is a no-op"
    );

    // (2) Durable state == a single flush: one fileset holding exactly the 3 rows
    //     (not 6), and the inline rows retired.
    let ice = IcebergCatalog::new(pool.clone());
    let cur = ice.current_snapshot(&table).await.expect("current");
    let files = ice
        .files(&table, cur.id, PageReq::unbounded())
        .await
        .expect("files");
    let rows: i64 = files.items.iter().map(|f| f.record_count).sum();
    assert_eq!(
        rows, 3,
        "rows written exactly once across both dispatches (no double-write)"
    );
    let inline = ice.inline_parquet(&table, cur.id).await.expect("inline");
    assert!(inline.is_none(), "inline rows retired exactly once");
}
```

- [ ] **Step 2: Run the test and verify it PASSES**

This is a regression that pins existing behavior — there is no production change to make, so the test must pass against the current code on first run (the advisory lock already provides exactly-once). It is the inverse of normal red-green TDD: the deliverable is the assertion, and a *failure* here would mean the idempotency guarantee is actually broken (a real finding, not an expected red).

Run (redirect to a file — never pipe `buck2 test` through `tail`/`head`):

```bash
buck2 test //src/services/worker:e2e > /tmp/flush_idem.log 2>&1; grep -E "Tests finished|FAIL|PASS|duplicate_dispatch" /tmp/flush_idem.log
```

Expected: `duplicate_dispatch_flush_is_idempotent_over_the_wire` PASSES, and the existing `inline_threshold_enqueues_and_worker_flushes` stays green.

If it fails, do NOT weaken the assertions — debug the idempotency guarantee (systematic-debugging); a genuine failure is a defect in `flush_table`, which would change the scope of this work and must be surfaced.

- [ ] **Step 3: Close the register item in `docs/ISSUES.md`**

Change the `iss-flush-at-least-once-idempotency` entry from `- [ ]` to `- [x]`, set `status:fixed`, and set `pr:#<n>` (the PR number once known — leave a placeholder to fill at finish; it can also be done in the finishing step). Add a one-line "Fixed (PR #<n>):" note to the body summarizing the added test. Use `loom-docs-update` at finish to apply this consistently.

- [ ] **Step 4: Commit**

```bash
git add src/services/worker/tests/e2e.rs docs/ISSUES.md docs/superpowers/plans/2026-06-22-flush-idempotency-test.md
git commit -m "test(iceberg): assert flush_table idempotency under duplicate over-the-wire dispatch"
```

---

## Self-Review

**1. Spec coverage:**
- Spec "The test (over the wire)" → Task 1 Step 1 (verbatim test, multi-thread flavor, reuses harness). ✓
- Spec assertions `a.is_some() ^ b.is_some()`, no-op loser, `rows == 3`, `inline is_none` → Step 1 assertions. ✓
- Spec "Testing / validation" (`buck2 test //src/services/worker:e2e`, single target) → Step 2. ✓
- Spec "Files" (modify `e2e.rs`, modify `docs/ISSUES.md`, no production/BUCK/Cargo change) → Task 1 files + Global Constraints. ✓
- Spec "What this does NOT do" (no production change, no lease-lapse simulation, primitive tests untouched) → captured in Architecture + Global Constraints; nothing in the plan violates it. ✓

**2. Placeholder scan:** The only `<n>` placeholders are the PR number (genuinely unknown until the PR opens) in Step 3 — resolved at finish via `loom-docs-update`. No TBD/TODO/"handle edge cases" in code steps; the test body is complete. ✓

**3. Type consistency:** `flush_table(String, String) -> Result<Option<i64>>`, `record_count: i64`, `inline_parquet -> Result<Option<_>>`, `files(...).items` — all match the verified codebase signatures and are used identically to the existing `inline_threshold_enqueues_and_worker_flushes` test. ✓
