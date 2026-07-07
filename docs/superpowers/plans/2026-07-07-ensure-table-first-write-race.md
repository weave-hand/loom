# `ensure_table` first-write race — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `ensure_table` race-safe so two concurrent FIRST writes to the same brand-new `schema.table` both resolve to the winner's `table_id` (via a savepoint retry) instead of the loser surfacing a raw Postgres `23505` unique-violation that poisons its transaction.

**Architecture:** `ensure_table` does SELECT-live-row-then-INSERT with no conflict handling; the loser of a first-write race trips the `iceberg_table_one_live_idx` partial unique index and gets a raw `Backend(23505)`. Wrap the INSERT in a `SAVEPOINT` (reusing the exact shape of the existing `run_idempotent_ddl` guard): on a duplicate-race error, roll back to the savepoint and re-SELECT the now-committed live row, returning the winner's `table_id`. Under READ COMMITTED the conflicting INSERT blocks until the winner commits, so the re-SELECT is guaranteed to find exactly one live row.

**Tech Stack:** Rust, sqlx (runtime `AssertSqlSafe` for savepoint control; the INSERT/SELECT reuse the existing compile-time `query_scalar!` macros — **no `.sqlx` regen**), buck2 `loom_fixture_test` (hermetic Postgres, `multi_thread` flavor for the race).

## Global Constraints

- **No schema/index change** — `iceberg_table_one_live_idx` (`migrations/0012_iceberg_mirror.sql:27-29`) stays exactly as is; the fix *relies* on it.
- **No `.sqlx` regen** — the INSERT and the re-SELECT reuse the existing `query_scalar!` text verbatim; savepoint statements are runtime `AssertSqlSafe`. The `//src/control-plane/postgres:sqlx-cache-check` test must stay green.
- **No signature change** to `ensure_table` — it keeps `(conn: &mut PgConnection, ns: &str, name: &str, at: SnapshotId) -> Result<i64>`; all callers are untouched.
- **No change to the already-live path** (the leading SELECT that returns an existing live row) — only the INSERT branch is guarded.
- **Reuse `is_duplicate_object_race`** (`iceberg_inline.rs:310`) rather than re-deriving the `23505` check — promote it to `pub(crate)`.
- **Liveness rests on READ COMMITTED** (loom sets no other isolation level). Do not add a new transaction — `ensure_table` always runs inside the caller's outer tx.
- **Tests are `rust_test` integration targets** — the changes land in the existing `//src/control-plane/postgres:stream-inline` target, not an inline module.
- **Clippy strict** (pedantic + restriction) on production code — no `unwrap`/`expect`/indexing in `ensure_table`; errors go through `map_err(backend)`.
- Commit messages end with the two required trailers; subjects follow Conventional Commits.

---

### Task 1: Savepoint-retry `ensure_table`, pinned by a concurrent first-write test

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_inline.rs:310` (promote `is_duplicate_object_race` to `pub(crate)`)
- Modify: `src/control-plane/postgres/src/iceberg_mirror.rs:83-114` (savepoint retry in `ensure_table`; add imports)
- Test: `src/control-plane/postgres/tests/stream_inline.rs` (new focused race test; tighten the existing differing-count test)

**Interfaces:**
- Consumes: `is_duplicate_object_race(e: &sqlx::Error) -> bool` (`iceberg_inline.rs:310`, made `pub(crate)`); `sqlx::AssertSqlSafe`; `crate::backend`; `iceberg_mirror::next_snapshot(conn, Option<i64>) -> Result<SnapshotId>`; `iceberg_mirror::ensure_table(conn, ns, name, at) -> Result<i64>`; test helpers already in `stream_inline.rs` (`PgFixture`, `table()`, `id_spec()`, `id_batch_n`, `lin()`).
- Produces: no new public surface — behavior change only (the losing first-writer resolves to the winner's `table_id`).

- [ ] **Step 1: Write the failing focused race test**

In `src/control-plane/postgres/tests/stream_inline.rs`, add a new test. It races two concurrent transactions each issuing the FIRST `ensure_table` for the same brand-new `(ns, name)`:

```rust
/// Two concurrent FIRST writes to the same brand-new `(ns, name)` both resolve to
/// the single live `iceberg_mirror.table` row: the loser of the unique-index race
/// absorbs the winner's `table_id` via the savepoint retry, instead of surfacing a
/// raw `23505`. Exactly one live table row exists afterward.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_first_ensure_table_resolves_to_one_id() {
    use control_plane_postgres::iceberg_mirror;

    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    async fn first_write(pool: sqlx::PgPool) -> control_plane_core::Result<i64> {
        let mut tx = pool.begin().await.expect("begin");
        let snap = iceberg_mirror::next_snapshot(&mut tx, None)
            .await
            .expect("allocate snapshot");
        let tid = iceberg_mirror::ensure_table(&mut tx, "ns", "brand_new", snap).await?;
        tx.commit().await.expect("commit");
        Ok(tid)
    }

    let handle_a = tokio::spawn(first_write(pool.clone()));
    let handle_b = tokio::spawn(first_write(pool.clone()));
    let tid_a = handle_a.await.expect("join a").expect("writer a resolves ensure_table");
    let tid_b = handle_b.await.expect("join b").expect("writer b resolves ensure_table");

    assert_eq!(tid_a, tid_b, "both first-writers resolve to one table_id");

    let live: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(
        "select count(*) from iceberg_mirror.\"table\" where end_snapshot is null",
    ))
    .fetch_one(&pool)
    .await
    .expect("live row count");
    assert_eq!(live, 1, "exactly one live table row after concurrent first writes");
}
```

- [ ] **Step 2: Run it to verify it fails (RED)**

Run: `buck2 test --console none //src/control-plane/postgres:stream-inline`
Expected: FAIL — the losing writer's `ensure_table` returns `Err(Backend(23505))`, so `.expect("writer … resolves ensure_table")` panics (or, depending on scheduling, the run is flaky pre-fix — the raw `23505` path is exactly what the fix removes). If instead the race does not trigger on a given run, note it; the fix makes the outcome deterministic.

- [ ] **Step 3: Promote the duplicate-race helper**

In `src/control-plane/postgres/src/iceberg_inline.rs:310`, change the visibility so `iceberg_mirror` can reuse it:

```rust
pub(crate) fn is_duplicate_object_race(e: &sqlx::Error) -> bool {
```

(The body and doc comment are unchanged.)

- [ ] **Step 4: Add the savepoint retry to `ensure_table`**

In `src/control-plane/postgres/src/iceberg_mirror.rs`, add the two imports near the top (after the existing `use crate::backend;` at line 12):

```rust
use crate::iceberg_inline::is_duplicate_object_race;
use sqlx::AssertSqlSafe;
```

Then replace the INSERT tail of `ensure_table` (currently lines 103-113, the `let tid = sqlx::query_scalar!( "insert …") … Ok(tid)`) with the savepoint-guarded version. The leading live-row SELECT (lines 91-102) is unchanged; only the fall-through INSERT is replaced:

```rust
    // No live row yet: insert one, guarded by a savepoint so a lost first-write
    // race (a concurrent writer inserted the live row for (ns, name) between our
    // SELECT and INSERT) resolves to the winner's table_id instead of surfacing a
    // raw 23505 that would poison the caller's outer transaction. Mirrors
    // `run_idempotent_ddl`'s savepoint shape, but re-SELECTs the winner's id.
    sqlx::query(AssertSqlSafe("savepoint loom_ensure_table"))
        .execute(&mut *conn)
        .await
        .map_err(backend)?;
    match sqlx::query_scalar!(
        "insert into iceberg_mirror.table (table_namespace, table_name, begin_snapshot) \
         values ($1, $2, $3) returning table_id as \"id!\"",
        ns,
        name,
        at.0,
    )
    .fetch_one(&mut *conn)
    .await
    {
        Ok(tid) => {
            sqlx::query(AssertSqlSafe("release savepoint loom_ensure_table"))
                .execute(&mut *conn)
                .await
                .map_err(backend)?;
            Ok(tid)
        }
        // A concurrent first-writer won the unique-index race. Undo our aborted
        // INSERT, then re-SELECT the now-committed live row: under READ COMMITTED
        // our INSERT blocked until the winner committed, and the partial unique
        // index permits exactly one live (ns, name), so precisely one row is found.
        Err(e) if is_duplicate_object_race(&e) => {
            sqlx::query(AssertSqlSafe("rollback to savepoint loom_ensure_table"))
                .execute(&mut *conn)
                .await
                .map_err(backend)?;
            sqlx::query(AssertSqlSafe("release savepoint loom_ensure_table"))
                .execute(&mut *conn)
                .await
                .map_err(backend)?;
            let tid = sqlx::query_scalar!(
                "select table_id as \"id!\" from iceberg_mirror.table \
                 where table_namespace = $1 and table_name = $2 and end_snapshot is null",
                ns,
                name,
            )
            .fetch_one(&mut *conn)
            .await
            .map_err(backend)?;
            Ok(tid)
        }
        // A genuinely different failure: restore the pre-INSERT state so the outer
        // transaction is usable, then surface it.
        Err(e) => {
            sqlx::query(AssertSqlSafe("rollback to savepoint loom_ensure_table"))
                .execute(&mut *conn)
                .await
                .map_err(backend)?;
            Err(backend(e))
        }
    }
```

Note: `ensure_table`'s body no longer ends with a bare `Ok(tid)` — the `match` is the trailing expression. Ensure the function's closing brace placement is correct (the `match` replaces the old `let tid = …; Ok(tid)`).

- [ ] **Step 5: Run the focused test to verify it passes (GREEN)**

Run: `buck2 test --console none //src/control-plane/postgres:stream-inline`
Expected: `concurrent_first_ensure_table_resolves_to_one_id` passes deterministically (both writers `Ok`, same id, one live row).

- [ ] **Step 6: Tighten the existing differing-count race test**

The fix makes `ensure_table` resolve both writers to the same `table_id`, so in `concurrent_first_declare_with_differing_counts_does_not_desync` (`stream_inline.rs:379`) the loser now ALWAYS reaches the stream-declare and is rejected there with `Conflict` — the `23505` ensure_table shape can no longer reach the caller. Update the test:

- Delete the `is_benign_ensure_table_race` helper fn (`stream_inline.rs:410-419`).
- Change the `clean_rejection_count` filter (`stream_inline.rs:427-434`) so the losing arm accepts only `Err(ControlPlaneError::Conflict(_)) => true` (drop the `Err(e) => is_benign_ensure_table_race(e)` arm; keep `Ok(_) => false`).
- Update the assertion message on `clean_rejection_count` to drop the "or the benign ensure_table race" clause — the loser must now be exactly `Conflict`.
- Revise the doc comment (`stream_inline.rs:360-377`) that narrates the "either shape" ensure_table race: with the race fixed, both writers reach the declare with the SAME resolved `table_id`, so the loser is deterministically the stream-declare `Conflict`. Keep the `loom_bucket < bucket_count` desync invariant description intact.

- [ ] **Step 7: Run the full postgres + touched-crate sweep**

Run: `buck2 test --console none //src/control-plane/postgres/...`
Expected: `Pass N. Fail 0` — both race tests pass, the `sqlx-cache-check` test stays green (no SQL text changed), and no other stream/inline test regressed.

- [ ] **Step 8: Commit**

```bash
git add src/control-plane/postgres/src/iceberg_inline.rs \
        src/control-plane/postgres/src/iceberg_mirror.rs \
        src/control-plane/postgres/tests/stream_inline.rs
git commit -m "fix(iceberg): resolve ensure_table first-write race to the winner's table_id"
```

(Commit body carries the two required trailers.)
