# First-declare arm — kind-symmetric post-race re-read Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close `iss-stream-first-declare-race-kind` — make `reconcile_stream_mode`'s
first-declare arm `(Some(n), None)` compare the winner's stream **kind** (not just its
bucket count) after a lost `on conflict do nothing` declare race, and stop a losing CDC
declare from stamping `changelog_table_id` onto the winner's log row.

**Architecture:** Three layers, innermost first.
1. **The guard** (`postgres/src/stream.rs`): swap the post-declare re-read from
   `pg_stream_bucket_count` (count only) to `pg_stream_meta` (count + kind), raise a
   *distinctly worded* `Validation` on a kind disagreement, and move the CDC sub-branch's
   changelog writes **after** that check. No new SQL — `pg_stream_meta`'s query is already
   in the committed `.sqlx` cache, so `sqlx-cache-check` stays green.
2. **Testability**: `reconcile_stream_mode` and `StreamDecl` are `pub(crate)`; loom forbids
   inline `#[cfg(test)]`, so they are widened to `pub` and driven directly from a `tests/`
   target. The race is reproduced deterministically with a **`pg_stat_activity` barrier** —
   no sleeps: connection A holds an uncommitted conflicting `stream.stream_table` insert,
   connection B calls `reconcile_stream_mode` and blocks inside its `on conflict do nothing`
   insert; the test polls until B's backend shows `wait_event_type='Lock' AND
   wait_event='transactionid'`, then commits A.
3. **The root cause of the arm's reachability** (`postgres/src/iceberg_mirror.rs`,
   `iceberg_landing.rs`, `iceberg_inline.rs`): the `pre_existing` witness that gates the
   batch→stream conversion guard is read *before* `ensure_table` (and, on the Parquet path,
   on a **separate pooled connection before the transaction**). `ensure_table` silently
   resolves a lost unique-index race to the winner's `table_id`, so the caller's witness can
   say "brand new" about a table another transaction just created. A new
   `ensure_table_witnessed` returns whether **this call** inserted the row; the two reconcile
   call sites derive `pre_existing` from that instead of from the stale probe. This is the
   spec's "out of scope — file it" item; per the checkout skill's *fix-don't-file* rule it is
   fixed here, because the fix is one delegating function plus two call sites and it is
   testable with the very barrier machinery tasks 1–3 build.

**Tech Stack:** Rust, sqlx 0.9 (compile-time `query!` against the committed `.sqlx` cache),
Postgres 17.9 via `PgFixture`, buck2 (`loom_fixture_test`), tokio multi-thread test runtime.

## Global Constraints

- **Tests are `rust_test` / `loom_fixture_test` targets only** — never inline
  `#[cfg(test)] mod tests` (the `no-inline-tests` prek hook fails the commit). Every new
  fixture test gets a **`loom_fixture_test`** target in `src/control-plane/postgres/BUCK`,
  never a bare `rust_test`.
- **No new SQL.** The whole change reuses existing `query!` statements. Do **not** invent a
  `select … for update`, a conditional insert, or any new statement — that would require
  `tools/sqlx-prepare.sh` (which cannot run in this environment) and would redden
  `//src/control-plane/postgres:sqlx-cache-check`.
- **Do not change `pg_stream_bucket_count`.** It is deliberately kind-agnostic — an "is this
  a stream table?" probe used by `iceberg_inline.rs:481`, `iceberg_landing.rs:858,1224`,
  `iceberg_flush.rs:169` (any kind ⇒ framing).
- **Strict clippy** (pedantic + restriction) on production code: no `unwrap`/`expect`/
  `panic`/indexing in `src/**`; silence locally only with `#[expect(lint, reason = "…")]`.
  Test code is exempt from the panic-safety lints via `loom_fixture_test`.
- **Build/test commands** (console-quiet forms, from CLAUDE.md):
  - `buck2 build -v0 --console none //src/control-plane/postgres:postgres`
  - `buck2 test --console none //src/control-plane/postgres:<target>`
- **`buck2 run //tools:prek -- run --all-files` before every commit** (rustfmt + clippy +
  file hooks). Commit whatever the hooks rewrite.
- **Conventional Commits** are enforced by the `commit-msg` hook.

---

### Task 1: Make the reconcile seam drivable from a test target

Widens the two `pub(crate)` items to `pub` and lands a new fixture test that drives
`reconcile_stream_mode` directly, asserting the **current, correct** behavior (a clean
first-declare on an uncontended table returns `Ok(Some(n))`). This task is green end to end:
it is the harness the next two tasks put under a race.

**Files:**
- Modify: `src/control-plane/postgres/src/stream.rs:17` (`enum StreamDecl` — `pub(crate)` → `pub`)
- Modify: `src/control-plane/postgres/src/stream.rs:56` (`async fn reconcile_stream_mode` — `pub(crate)` → `pub`)
- Create: `src/control-plane/postgres/tests/stream_first_declare_race.rs`
- Modify: `src/control-plane/postgres/BUCK` (new `loom_fixture_test` target)

**Interfaces:**
- Consumes: `control_plane_postgres::stream::{reconcile_stream_mode, StreamDecl}` (widened
  here); `control_plane_postgres::fixture::PgFixture` (`shared()`, `fresh_db() -> (PgControlPlane, String)`,
  `pool_for(&db) -> sqlx::PgPool`); `control_plane_core::{SnapshotId, TableRef, MergeEngine, ControlPlaneError}`.
- Produces: the test module's private helpers used by tasks 2–3, with these exact
  signatures:
  - `async fn insert_stream_row(pool: &sqlx::PgPool, tid: i64, buckets: i32, kind: &str, bucket_key: Option<&str>) -> sqlx::Transaction<'static, sqlx::Postgres>`
    — begins a transaction, inserts the row, returns the **still-open** transaction.
    (`sqlx::PgPool::begin` returns a `Transaction<'static, Postgres>`, so it can outlive the
    borrow and be committed later by the test body.)
  - `fn tref() -> TableRef` — the `s.t` table the tests name in error messages.
  - `const TID: i64` — the synthetic mirror table id both racers declare against.

  The `pg_stat_activity` barrier (`await_declare_blocked`) is **not** written in this task —
  it arrives in Task 2, where the first racing test needs it. Same for the `MergeEngine`
  import. Writing them early would be dead code and trip the lint gate.

- [ ] **Step 1: Widen the two items to `pub`**

`src/control-plane/postgres/src/stream.rs` — the module is already `pub mod stream;` in
`lib.rs`, so only the item visibility changes. Keep both doc comments exactly as they are and
append one sentence to each explaining the widening, because a reviewer will ask why a
crate-internal seam is public:

```rust
/// A write's REQUESTED stream-mode declaration, threaded from `LandRequest` down
/// to [`reconcile_stream_mode`]. `Log`/`Cdc` carry the requested bucket count;
/// `Cdc` additionally carries the identity column to bucket on (the "bucket
/// key"). The ingest-facing boundary (`land`'s public signature / `LandRequest`)
/// instead carries the plain `Option<i32>`/`Option<CdcDecl>` pair the brief
/// specifies, combined into this enum once inside `land`.
///
/// `pub` (not `pub(crate)`) only so the reconcile seam is drivable from a
/// `tests/` target: loom forbids inline `#[cfg(test)]` tests, and the
/// concurrent first-declare race
/// (`postgres/tests/stream_first_declare_race.rs`) cannot be reproduced through
/// the `land` API — it needs a caller-controlled `tid`/`pre_existing`.
#[derive(Clone, Debug)]
pub enum StreamDecl {
```

and, on the function (keep the whole existing doc block, add the final paragraph):

```rust
/// `pub` (not `pub(crate)`) only so a `tests/` target can drive this seam
/// directly; production callers remain `iceberg_inline::inline_append` and
/// `iceberg_landing::land_parquet_stream`.
pub async fn reconcile_stream_mode(
```

- [ ] **Step 2: Write the harness test (control case)**

Create `src/control-plane/postgres/tests/stream_first_declare_race.rs`. This file grows in
tasks 2 and 3; write it now with the helpers and the uncontended control test only.

```rust
//! The first-declare arm's post-race re-read must compare stream KIND, not just
//! bucket count (iss-stream-first-declare-race-kind).
//!
//! `pg_declare_stream`/`pg_declare_cdc` are `insert … on conflict (table_id) do
//! nothing`, so a first-declare that loses a race to a concurrent first-declare
//! no-ops and then re-reads what the winner recorded. Re-reading only the COUNT
//! lets a log declare proceed against a `kind='cdc'` row (and vice versa).
//!
//! The race is reproduced deterministically — NO sleeps — with a
//! `pg_stat_activity` barrier: connection A holds an uncommitted conflicting
//! `stream.stream_table` row, connection B's declare blocks on A's
//! `transactionid` lock, the test polls until B is observably blocked, then
//! commits A. Without the barrier, A could commit before B's `pg_stream_meta`
//! read, routing B into the (already-fixed) count-equal arm — a green test for
//! the wrong reason. Each test therefore asserts the DISTINCT first-declare
//! message to prove which arm fired.
//!
//! `stream.stream_table.table_id` is a bare `bigint primary key` with no FK
//! (migrations/0035), and `reconcile_stream_mode` takes `tid`/`pre_existing` as
//! parameters, so these tests need no mirror table and land no data.
//! loom_fixture_test (Postgres).

use control_plane_core::{ControlPlaneError, SnapshotId, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::stream::{StreamDecl, reconcile_stream_mode};
use sqlx::{PgPool, Postgres, Transaction};

/// The table the reconcile seam names in its error messages. No mirror row is
/// created for it — the first-declare arm never reads one.
fn tref() -> TableRef {
    TableRef {
        schema: "s".into(),
        name: "t".into(),
    }
}

/// The synthetic mirror table id both racers declare against.
const TID: i64 = 4242;

/// Begin a transaction, insert the WINNER's `stream.stream_table` row, and return
/// the transaction still OPEN (uncommitted) — the loser's `on conflict do nothing`
/// insert blocks on it until the caller commits.
async fn insert_stream_row(
    pool: &PgPool,
    tid: i64,
    buckets: i32,
    kind: &str,
    bucket_key: Option<&str>,
) -> Transaction<'static, Postgres> {
    let mut tx = pool.begin().await.expect("begin winner tx");
    sqlx::query(
        "insert into stream.stream_table (table_id, bucket_count, kind, bucket_key) \
         values ($1, $2, $3, $4)",
    )
    .bind(tid)
    .bind(buckets)
    .bind(kind)
    .bind(bucket_key)
    .execute(&mut *tx)
    .await
    .expect("winner insert");
    tx
}

/// Control / non-regression: an UNCONTENDED first declare (no concurrent winner)
/// still returns the declared bucket count. Pins the happy path the race tests
/// perturb.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uncontended_first_declare_returns_its_bucket_count() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let mut tx = pool.begin().await.expect("begin");
    let effective = reconcile_stream_mode(
        &mut tx,
        TID,
        &StreamDecl::Log(2),
        false,
        &tref(),
        SnapshotId(1),
    )
    .await
    .expect("uncontended log first-declare");
    assert_eq!(
        effective,
        Some(2),
        "an uncontended first declare records and returns its own bucket count"
    );
    tx.commit().await.expect("commit");
}

/// Control / non-regression: the COUNT mismatch on the first-declare arm stays a
/// `Conflict` (count precedence is unchanged by the kind guard).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn first_declare_losing_on_count_stays_conflict() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    // Winner: a log table with a DIFFERENT count, committed before the loser runs
    // (no barrier needed — this test pins the count arm, not the race).
    let tx = insert_stream_row(&pool, TID, 4, "log", None).await;
    tx.commit().await.expect("commit winner");

    let mut tx = pool.begin().await.expect("begin");
    // `pre_existing = false` and a stream row that already exists => the
    // (Some, Some) count-mismatch arm fires first. Kept as a guard that the
    // kind check never overtakes count precedence.
    let res = reconcile_stream_mode(
        &mut tx,
        TID,
        &StreamDecl::Log(2),
        false,
        &tref(),
        SnapshotId(1),
    )
    .await;
    assert!(
        matches!(res, Err(ControlPlaneError::Conflict(_))),
        "a bucket-count disagreement stays a Conflict, got {res:?}"
    );
    drop(tx.rollback().await);
}
```

That is the whole file for this task — two green tests, no barrier helper, no `MergeEngine`
import (both arrive in Task 2 with the first test that uses them).

- [ ] **Step 3: Add the buck2 target**

`src/control-plane/postgres/BUCK` — add next to `stream-log-vs-cdc-declare`. A slim dep set:
this test lands no data, so no arrow/iceberg/tempfile/seed.

```python
loom_fixture_test(
    name = "stream-first-declare-race",
    crate = "stream_first_declare_race",
    srcs = ["tests/stream_first_declare_race.rs"],
    crate_root = "tests/stream_first_declare_race.rs",
    deps = [
        ":postgres",
        "//src/control-plane/core:core",
        "//third-party:sqlx",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 4: Run the test — it must PASS**

Run: `buck2 test --console none //src/control-plane/postgres:stream-first-declare-race`
Expected: `Tests finished: Pass 2. Fail 0.`

If it fails to compile with "private item", the visibility change in Step 1 was not applied.
If `sqlx::query(...)` fails to compile, note the crate uses sqlx 0.9 — a `&'static str`
literal is accepted by `sqlx::query`; a non-literal needs `sqlx::AssertSqlSafe(...)`.

- [ ] **Step 5: Prove nothing else broke**

Run: `buck2 build -v0 --console none //src/control-plane/postgres:postgres`
Expected: exit 0, silent (the widened visibility must not trip clippy).

- [ ] **Step 6: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/control-plane/postgres/src/stream.rs \
        src/control-plane/postgres/tests/stream_first_declare_race.rs \
        src/control-plane/postgres/BUCK
git commit -m "test(stream): drive reconcile_stream_mode from a fixture test target"
```

---

### Task 2: The kind-aware first-declare re-read

TDD the defect itself: a **log** first-declare that loses the race to a **CDC** first-declare
with the same bucket count is silently accepted today. Make it a distinctly-worded
`Validation`, in both directions.

**Files:**
- Modify: `src/control-plane/postgres/tests/stream_first_declare_race.rs` (add two race tests + the barrier helper)
- Modify: `src/control-plane/postgres/src/stream.rs:232-249` (the post-declare re-read)

**Interfaces:**
- Consumes: `pg_stream_meta(&mut *conn, tid) -> Result<Option<StreamMeta>>`
  (`stream.rs:423`, already used at `stream.rs:125`; its query is already in the `.sqlx`
  cache — **no new SQL**); `control_plane_core::{StreamMeta, StreamKind}`.
- Produces: the distinct error string **`"declared concurrently with a different stream
  kind"`** — the substring both race tests assert on. It MUST differ from the count-equal
  arm's `"already declared with a different stream kind"`, or a green test cannot prove which
  arm fired.

- [ ] **Step 1: Write the two failing race tests**

Append to `src/control-plane/postgres/tests/stream_first_declare_race.rs`. Add
`MergeEngine` to the `control_plane_core` import line, and add this ONE barrier helper next to
`insert_stream_row`. The loser's backend is identified by the statement it is stuck in, so no
pid needs to be plumbed out of the spawned task (whose `JoinHandle` only resolves *after* the
race is over — too late to be a barrier):

```rust
/// Barrier: block until some backend in this database is waiting on another
/// transaction's lock inside a `stream.stream_table` insert — i.e. the loser's
/// `insert … on conflict do nothing` is blocked on the uncommitted winner.
/// `wait_event_type='Lock' / wait_event='transactionid'` is exactly what that
/// insert waits on against an uncommitted conflicting row (verified against the
/// pinned PG 17.9). Polls `pg_stat_activity`; panics rather than hanging.
///
/// Load-bearing: only once the loser is observably blocked may the caller commit
/// the winner. Commit it any earlier and the loser's `pg_stream_meta` read at the
/// top of `reconcile_stream_mode` sees the winner's row, routing it into the
/// (already-guarded) count-equal arm — a green test for the wrong reason.
async fn await_declare_blocked(pool: &PgPool) {
    for _ in 0..600 {
        let blocked: i64 = sqlx::query_scalar(
            "select count(*) from pg_stat_activity \
             where datname = current_database() \
               and wait_event_type = 'Lock' \
               and wait_event = 'transactionid' \
               and query like 'insert into stream.stream_table%'",
        )
        .fetch_one(pool)
        .await
        .expect("pg_stat_activity barrier probe");
        if blocked >= 1 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("the losing declare never blocked on the winner's transactionid lock");
}

/// THE DEFECT: a log first-declare that loses the `on conflict do nothing` race
/// to a CDC first-declare with the SAME bucket count must be rejected. Before the
/// fix the re-read compares only the count, so the log write proceeds against a
/// `kind='cdc'` row — log framing stamped into CDC storage.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn log_first_declare_losing_to_cdc_winner_is_validation_error() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    // A: the CDC winner, held UNCOMMITTED.
    let winner = insert_stream_row(&pool, TID, 2, "cdc", Some("id")).await;

    // B: the log loser. Its `pg_stream_meta` read returns None (A is uncommitted), so
    // it takes the (Some, None) first-declare arm, and its `pg_declare_stream` insert
    // then BLOCKS on A's transactionid lock.
    let pool_b = pool.clone();
    let loser = tokio::spawn(async move {
        let mut tx = pool_b.begin().await.expect("begin loser tx");
        let res = reconcile_stream_mode(
            &mut tx,
            TID,
            &StreamDecl::Log(2),
            false,
            &tref(),
            SnapshotId(1),
        )
        .await;
        drop(tx.rollback().await);
        res
    });

    await_declare_blocked(&pool).await;
    winner.commit().await.expect("commit winner");

    let res = loser.await.expect("join loser");
    assert!(
        matches!(&res, Err(ControlPlaneError::Validation(msg))
                 if msg.contains("declared concurrently with a different stream kind")),
        "a log first-declare losing to a cdc winner with the same count must be rejected \
         by the FIRST-DECLARE arm, got {res:?}"
    );
}

/// Symmetric: a CDC first-declare losing to a LOG winner with the same count.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cdc_first_declare_losing_to_log_winner_is_validation_error() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let winner = insert_stream_row(&pool, TID, 2, "log", None).await;

    let pool_b = pool.clone();
    let loser = tokio::spawn(async move {
        let mut tx = pool_b.begin().await.expect("begin loser tx");
        let res = reconcile_stream_mode(
            &mut tx,
            TID,
            &StreamDecl::Cdc {
                buckets: 2,
                bucket_key: "id".into(),
                merge_engine: MergeEngine::LastRow,
            },
            false,
            &tref(),
            SnapshotId(1),
        )
        .await;
        drop(tx.rollback().await);
        res
    });

    await_declare_blocked(&pool).await;
    winner.commit().await.expect("commit winner");

    let res = loser.await.expect("join loser");
    assert!(
        matches!(&res, Err(ControlPlaneError::Validation(msg))
                 if msg.contains("declared concurrently with a different stream kind")),
        "a cdc first-declare losing to a log winner with the same count must be rejected \
         by the FIRST-DECLARE arm, got {res:?}"
    );
}
```

- [ ] **Step 2: Run the tests — they must FAIL, for the right reason**

Run: `buck2 test --console none //src/control-plane/postgres:stream-first-declare-race`
Expected: the two new tests FAIL with a message showing `Ok(Some(2))` — i.e. the losing
declare was **accepted**. That is the defect. (If instead they fail with a `Conflict`, the
barrier did not hold and the test took the count arm — fix the barrier before touching
`stream.rs`.)

- [ ] **Step 3: Implement the kind-aware re-read**

`src/control-plane/postgres/src/stream.rs` — replace the count-only re-read (the block at
`:232-249`, which currently calls `pg_stream_bucket_count`) with a `pg_stream_meta` re-read
that checks count **and** kind. Leave `pg_stream_bucket_count` itself untouched.

```rust
            // A concurrent first-writer may have won the declare (our ON CONFLICT DO
            // NOTHING then no-ops). Re-read what was ACTUALLY recorded and honour it,
            // so the rows we stamp always agree with the registry — checking KIND as
            // well as count. Re-reading only the count (as this arm did before
            // iss-stream-first-declare-race-kind) let a log declare that lost the race
            // to a same-count cdc declare proceed against a `kind='cdc'` row, stamping
            // log framing into CDC storage — exactly what the count-equal arm above
            // rejects. `pg_stream_meta` is the same statement read at the top of this
            // function, so this adds no new SQL.
            let stored = pg_stream_meta(&mut *conn, tid).await?.ok_or_else(|| {
                ControlPlaneError::Backend(
                    "stream_table row missing immediately after declare".into(),
                )
            })?;
            if stored.bucket_count != n {
                return Err(ControlPlaneError::Conflict(format!(
                    "stream bucket count mismatch for {}.{}: requested {n}, table has {}",
                    table.schema, table.name, stored.bucket_count
                )));
            }
            let requested_kind = match decl {
                StreamDecl::Cdc { .. } => StreamKind::Cdc,
                // `Log` and `None` alike declare a log table here — `None` cannot
                // reach this arm (it has no `requested` count), so this is the Log
                // case. A future StreamDecl variant must extend this mapping.
                StreamDecl::Log(_) | StreamDecl::None => StreamKind::Log,
            };
            if stored.kind != requested_kind {
                // Distinct wording from the count-equal arm's "already declared with a
                // different stream kind": these two rejections are otherwise
                // indistinguishable, and the race test asserts on the message to prove
                // the FIRST-DECLARE arm fired.
                return Err(ControlPlaneError::Validation(format!(
                    "cannot declare {}.{}: declared concurrently with a different stream kind",
                    table.schema, table.name
                )));
            }
            Some(stored.bucket_count)
```

`StreamKind` is already imported at the top of `stream.rs` (line 3).

- [ ] **Step 4: Run the tests — all four must PASS**

Run: `buck2 test --console none //src/control-plane/postgres:stream-first-declare-race`
Expected: `Tests finished: Pass 4. Fail 0.`

- [ ] **Step 5: Prove the `.sqlx` cache and the neighbours are untouched**

Run: `buck2 test --console none //src/control-plane/postgres:sqlx-cache-check //src/control-plane/postgres:stream-log-vs-cdc-declare //src/control-plane/postgres:stream-merge-declare`
Expected: `Fail 0`. (`sqlx-cache-check` green proves no new SQL was introduced;
`git status` must show **no** change under `src/control-plane/postgres/.sqlx/`.)

- [ ] **Step 6: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/control-plane/postgres/src/stream.rs \
        src/control-plane/postgres/tests/stream_first_declare_race.rs
git commit -m "fix(stream): compare kind, not just bucket count, in the first-declare re-read"
```

---

### Task 3: A losing CDC declare must not stamp the winner's row

The CDC sub-branch does two **writes** before the re-read — it creates the changelog table's
mirror row (`iceberg_mirror::ensure_table`) and issues an unconditional
`pg_set_changelog_table_id` UPDATE. A CDC declare that loses to a **log** winner therefore
stamps `changelog_table_id` onto the winner's log row before Task 2's kind check can fire.
The caller's rollback hides this today; it is still an ordering bug, and the fix is to move
the checks ahead of the writes.

**Files:**
- Modify: `src/control-plane/postgres/tests/stream_first_declare_race.rs` (extend the CDC-loser test)
- Modify: `src/control-plane/postgres/src/stream.rs:204-231` (the `(Some(n), None)` arm's `match decl`)

**Interfaces:**
- Consumes: `pg_declare_cdc` / `pg_declare_stream` (`stream.rs:332,401`);
  `crate::iceberg_landing::changelog_table_ref(table) -> TableRef`;
  `crate::iceberg_mirror::ensure_table(&mut *conn, ns, name, at) -> Result<i64>`;
  `pg_set_changelog_table_id(&mut *conn, tid, clog_tid)`.
- Produces: no new names. The arm's *order* becomes: declare (ON CONFLICT DO NOTHING) →
  re-read + validate count/kind → **then** the CDC-only changelog writes.

- [ ] **Step 1: Extend the CDC-loser test to assert the winner's row is unstamped**

The loser's transaction is rolled back when the test drops it, so the stamp is invisible from
outside. Probe it **inside the loser's own transaction**, immediately after
`reconcile_stream_mode` returns — the loser's uncommitted UPDATE is visible to itself.

In `cdc_first_declare_losing_to_log_winner_is_validation_error`, change the spawned task to
return the reconcile result **and** a tx-local read of the winner's `changelog_table_id`:

```rust
    let pool_b = pool.clone();
    let loser = tokio::spawn(async move {
        let mut tx = pool_b.begin().await.expect("begin loser tx");
        let res = reconcile_stream_mode(
            &mut tx,
            TID,
            &StreamDecl::Cdc {
                buckets: 2,
                bucket_key: "id".into(),
                merge_engine: MergeEngine::LastRow,
            },
            false,
            &tref(),
            SnapshotId(1),
        )
        .await;
        // Read the winner's row on the LOSER's still-open transaction: any UPDATE the
        // loser issued before bailing out is visible to itself here, and nowhere else
        // (the rollback below erases it). This is the only way to observe the
        // changelog stamp the pre-fix ordering performs.
        let stamped: Option<i64> = sqlx::query_scalar(
            "select changelog_table_id from stream.stream_table where table_id = $1",
        )
        .bind(TID)
        .fetch_one(&mut *tx)
        .await
        .expect("read winner changelog_table_id on the loser tx");
        drop(tx.rollback().await);
        (res, stamped)
    });

    await_declare_blocked(&pool).await;
    winner.commit().await.expect("commit winner");

    let (res, stamped) = loser.await.expect("join loser");
    assert!(
        matches!(&res, Err(ControlPlaneError::Validation(msg))
                 if msg.contains("declared concurrently with a different stream kind")),
        "a cdc first-declare losing to a log winner with the same count must be rejected \
         by the FIRST-DECLARE arm, got {res:?}"
    );
    assert_eq!(
        stamped, None,
        "a losing cdc declare must bail out BEFORE writing changelog_table_id onto the \
         winner's log row — the kind check has to precede the changelog writes"
    );
```

- [ ] **Step 2: Run the test — the new assertion must FAIL**

Run: `buck2 test --console none //src/control-plane/postgres:stream-first-declare-race`
Expected: `cdc_first_declare_losing_to_log_winner_is_validation_error` FAILS on
`assert_eq!(stamped, None)` — it reads `Some(<clog tid>)`, proving the pre-fix ordering
stamps the winner's row. The `Validation` assertion above it already passes (Task 2).

- [ ] **Step 3: Reorder the arm — checks before the CDC writes**

`src/control-plane/postgres/src/stream.rs`, the `(Some(n), None)` arm. Issue the declare
(both variants), then run Task 2's re-read + count/kind validation, and only then do the
CDC-only changelog work:

```rust
        (Some(n), None) => {
            if pre_existing {
                return Err(ControlPlaneError::Validation(format!(
                    "cannot convert existing batch table {}.{} to a stream table",
                    table.schema, table.name
                )));
            }
            // The declare itself is `insert … on conflict (table_id) do nothing`: it
            // blocks against a concurrent uncommitted first-declare and no-ops if that
            // writer won.
            match decl {
                StreamDecl::Cdc {
                    bucket_key,
                    merge_engine,
                    ..
                } => pg_declare_cdc(&mut *conn, tid, n, bucket_key, *merge_engine).await?,
                _ => pg_declare_stream(&mut *conn, tid, n).await?,
            }

            // Re-read what was ACTUALLY recorded (ours, or a concurrent winner's) and
            // validate count AND kind BEFORE any further write. Ordering is
            // load-bearing: the CDC changelog writes below are UNCONDITIONAL updates to
            // this table_id's registry row, so a cdc declare that lost the race to a log
            // winner would otherwise stamp `changelog_table_id` onto the winner's log
            // row before the kind check could reject it (undone by the caller's
            // rollback today — but that is the caller's discipline, not this seam's).
            let stored = pg_stream_meta(&mut *conn, tid).await?.ok_or_else(|| {
                ControlPlaneError::Backend(
                    "stream_table row missing immediately after declare".into(),
                )
            })?;
            if stored.bucket_count != n {
                return Err(ControlPlaneError::Conflict(format!(
                    "stream bucket count mismatch for {}.{}: requested {n}, table has {}",
                    table.schema, table.name, stored.bucket_count
                )));
            }
            let requested_kind = match decl {
                StreamDecl::Cdc { .. } => StreamKind::Cdc,
                StreamDecl::Log(_) | StreamDecl::None => StreamKind::Log,
            };
            if stored.kind != requested_kind {
                return Err(ControlPlaneError::Validation(format!(
                    "cannot declare {}.{}: declared concurrently with a different stream kind",
                    table.schema, table.name
                )));
            }

            // Winner (or an agreeing redeclare of our own kind): register the changelog
            // table's mirror row (its Iceberg metadata was created by `land_cdc` before
            // this tx, outside any commit) and point the registry at it, reusing this
            // write's `at` snapshot — the changelog table's genesis shares the same
            // snapshot as the declaring write. Its columns are projected on first flush
            // append; an empty mirror row is a valid never-written table.
            if matches!(decl, StreamDecl::Cdc { .. }) {
                let clog = crate::iceberg_landing::changelog_table_ref(table);
                let clog_tid =
                    crate::iceberg_mirror::ensure_table(&mut *conn, &clog.schema, &clog.name, at)
                        .await?;
                pg_set_changelog_table_id(&mut *conn, tid, clog_tid).await?;
            }
            Some(stored.bucket_count)
        }
```

- [ ] **Step 4: Run the tests — all four must PASS**

Run: `buck2 test --console none //src/control-plane/postgres:stream-first-declare-race`
Expected: `Tests finished: Pass 4. Fail 0.`

- [ ] **Step 5: Prove the CDC declare path still works end to end**

The reorder moved the changelog writes; the CDC *winner* must still get its changelog mirror
row and pointer. These suites cover the full `land_cdc` declare path:

Run: `buck2 test --console none //src/control-plane/postgres:stream-merge-declare //src/control-plane/postgres:stream-log-vs-cdc-declare //src/control-plane/postgres:stream-cdc-emission //src/control-plane/postgres:stream-cdc-dual-flush //src/control-plane/postgres:stream-changelog-notify`
Expected: `Fail 0`.

- [ ] **Step 6: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/control-plane/postgres/src/stream.rs \
        src/control-plane/postgres/tests/stream_first_declare_race.rs
git commit -m "fix(stream): validate the first-declare re-read before the cdc changelog writes"
```

---

### Task 4: Root cause — an honest `pre_existing` witness from `ensure_table`

The batch→stream conversion guard (`stream.rs:198`) is only as good as the `pre_existing`
witness its caller passes. Both callers compute that witness from a read taken **before**
`ensure_table` — and `land_parquet` takes it on a **separate pooled connection, before the
transaction even opens** (`iceberg_landing.rs:853-862`). `ensure_table` resolves a lost
unique-index race by rolling back to its savepoint and re-`SELECT`ing the *winner's*
`table_id` — so a caller can be handed another transaction's brand-new table while its own
witness still says "this table did not exist". A stream-declaring write then converts a table
a concurrent batch writer just created.

This is the spec's "out of scope — file it" item. It is fixed here instead: it is the root
cause of the very window Tasks 1–3 guard, the fix is one delegating function plus two call
sites, and it is testable with the barrier the earlier tasks already built.

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_mirror.rs:92` (add `ensure_table_witnessed`; `ensure_table` delegates)
- Modify: `src/control-plane/postgres/src/iceberg_landing.rs:853-862,878,926,963` (drop the pre-tx `pre_existing` probe; witness from `ensure_table_witnessed`)
- Modify: `src/control-plane/postgres/src/iceberg_inline.rs:465-473` (witness from `ensure_table_witnessed`)
- Create: `src/control-plane/postgres/tests/stream_declare_vs_concurrent_create.rs`
- Modify: `src/control-plane/postgres/BUCK` (new `loom_fixture_test` target)

**Interfaces:**
- Produces: `pub async fn ensure_table_witnessed(conn: &mut PgConnection, ns: &str, name: &str, at: SnapshotId) -> Result<(i64, bool)>`
  — `(table_id, created_by_this_call)`. `created_by_this_call == false` for BOTH the
  "already live when we looked" case and the "we lost the insert race" case; that is exactly
  the `pre_existing` the conversion guard wants.
- Consumes: `ensure_table` keeps its signature (`-> Result<i64>`) and its 43 call sites — it
  becomes `Ok(ensure_table_witnessed(conn, ns, name, at).await?.0)`.

- [ ] **Step 1: Write the failing race test**

Create `src/control-plane/postgres/tests/stream_declare_vs_concurrent_create.rs`.

```rust
//! A stream-declaring `land` must not convert a table a CONCURRENT writer created
//! as a batch table (root cause behind iss-stream-first-declare-race-kind's
//! reachability).
//!
//! `land_parquet` probes `pre_existing` on a separate pooled connection BEFORE its
//! transaction; `ensure_table` then silently resolves a lost unique-index race to
//! the winner's table_id. The witness therefore says "brand new" about a table
//! another transaction just created, and the batch→stream conversion guard
//! (`reconcile_stream_mode`'s "cannot convert existing batch table") never fires.
//!
//! Deterministic, no sleeps — the same `pg_stat_activity` barrier as
//! `stream_first_declare_race.rs`: connection A holds an uncommitted
//! `iceberg_mirror.table` insert for `s.t`; the landing writer's `ensure_table`
//! blocks on it; once it is observably blocked we commit A, so the writer takes
//! exactly the lost-race path.
//! loom_fixture_test (Postgres).

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{ColumnSpec, ControlPlaneError, LineageEvent, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use control_plane_postgres::iceberg_mirror::next_snapshot;
use loom_test_seed::local_sql_catalog;
use sqlx::PgPool;

fn columns() -> Vec<ColumnSpec> {
    vec![ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: false,
    }]
}

fn batch() -> (Arc<Schema>, Vec<RecordBatch>) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let b = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))])
        .expect("batch");
    (schema, vec![b])
}

fn lineage() -> LineageEvent {
    LineageEvent::completed(vec![], serde_json::json!({ "source": "test" }))
}

/// Direct-write (Parquet) limits: never inline, so the write takes `land_parquet`
/// — the path whose `pre_existing` probe runs on a separate connection before the
/// transaction.
fn never_inline() -> InlineLimits {
    InlineLimits {
        inline_byte_limit: 0,
        flush_byte_threshold: 0,
    }
}

/// Barrier: block until some backend is waiting on another transaction's lock
/// inside an `iceberg_mirror.table` insert — i.e. the lander's `ensure_table` is
/// blocked on our uncommitted row. Panics rather than hanging.
async fn await_ensure_table_blocked(pool: &PgPool) {
    for _ in 0..600 {
        let blocked: i64 = sqlx::query_scalar(
            "select count(*) from pg_stat_activity \
             where datname = current_database() \
               and wait_event_type = 'Lock' \
               and query like 'insert into iceberg_mirror.table%'",
        )
        .fetch_one(pool)
        .await
        .expect("pg_stat_activity barrier probe");
        if blocked >= 1 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("the lander's ensure_table never blocked on the concurrent creator");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_declare_losing_the_create_race_cannot_convert_the_winners_table() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("warehouse");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;

    let table = TableRef {
        schema: "s".into(),
        name: "t".into(),
    };

    // A: a concurrent BATCH writer that has created the mirror row for s.t but has
    // not committed. Held open.
    let mut winner = pool.begin().await.expect("begin winner tx");
    let at = next_snapshot(&mut winner, None)
        .await
        .expect("winner snapshot");
    sqlx::query(
        "insert into iceberg_mirror.table (table_namespace, table_name, begin_snapshot) \
         values ($1, $2, $3)",
    )
    .bind(&table.schema)
    .bind(&table.name)
    .bind(at.0)
    .execute(&mut *winner)
    .await
    .expect("winner creates the mirror row");

    // B: a stream-declaring land. Its pre-transaction probe sees NO table (A is
    // uncommitted) => pre_existing = false. Its `ensure_table` then blocks on A.
    let (schema, batches) = batch();
    let pool_b = pool.clone();
    let cat_b = catalog.clone();
    let table_b = table.clone();
    let lander = tokio::spawn(async move {
        land(
            &pool_b,
            &cat_b,
            &table_b,
            &columns(),
            schema,
            batches,
            never_inline(),
            lineage(),
            Some(2),
        )
        .await
    });

    await_ensure_table_blocked(&pool).await;
    winner.commit().await.expect("commit winner");

    let res = lander.await.expect("join lander");
    assert!(
        matches!(&res, Err(ControlPlaneError::Validation(msg))
                 if msg.contains("cannot convert existing batch table")),
        "a stream declare that LOSES the create race must see the winner's table as \
         pre-existing and refuse the batch->stream conversion, got {res:?}"
    );

    drop(wh);
    drop(catalog);
}
```

**If `SqlCatalog` is not `Clone`,** build the catalog inside the spawned task from
`fx.pg_dsn(&db)` + the warehouse path instead of cloning it (`local_sql_catalog(...)` is
async and takes owned `String`s), and keep the `tempfile::TempDir` alive in the outer test.

- [ ] **Step 2: Add the buck2 target**

`src/control-plane/postgres/BUCK`, next to the Task 1 target:

```python
loom_fixture_test(
    name = "stream-declare-vs-concurrent-create",
    crate = "stream_declare_vs_concurrent_create",
    srcs = ["tests/stream_declare_vs_concurrent_create.rs"],
    crate_root = "tests/stream_declare_vs_concurrent_create.rs",
    deps = [
        "//src/testing:seed",
        ":postgres",
        "//src/control-plane/core:core",
        "//third-party:arrow-array",
        "//third-party:arrow-schema",
        "//third-party:serde_json",
        "//third-party:sqlx",
        "//third-party:tempfile",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 3: Run it — it must FAIL**

Run: `buck2 test --console none //src/control-plane/postgres:stream-declare-vs-concurrent-create`
Expected: FAIL — the land returns `Ok(SnapshotId(..))`. The stream declare converted the
concurrent batch writer's table. That is the defect.

- [ ] **Step 4: Add the witnessing primitive**

`src/control-plane/postgres/src/iceberg_mirror.rs` — rename the existing body to
`ensure_table_witnessed`, returning `(table_id, created)`, and keep `ensure_table` as a thin
delegate so all 43 existing call sites are untouched.

```rust
/// Ensure a live mirror row for `(ns, name)` and return its `table_id`.
/// See [`ensure_table_witnessed`] when the caller needs to know whether THIS call
/// created the row.
pub async fn ensure_table(
    conn: &mut PgConnection,
    ns: &str,
    name: &str,
    at: SnapshotId,
) -> Result<i64> {
    Ok(ensure_table_witnessed(conn, ns, name, at).await?.0)
}

/// [`ensure_table`], plus an honest witness: `true` iff THIS call inserted the live
/// row. `false` covers both "already live when we looked" and "we lost the
/// unique-index race to a concurrent creator" — the two cases a caller must treat
/// alike, because in both the table is somebody else's.
///
/// The witness exists because the batch→stream conversion guard
/// (`stream::reconcile_stream_mode`'s `pre_existing`) cannot be computed by a read
/// taken before this call: the lost-race path below resolves to the WINNER's
/// `table_id`, so a pre-read says "brand new" about a table another transaction
/// just created, and a stream declare would silently convert it.
pub async fn ensure_table_witnessed(
    conn: &mut PgConnection,
    ns: &str,
    name: &str,
    at: SnapshotId,
) -> Result<(i64, bool)> {
    // …the existing body, verbatim, with each `Ok(tid)` / `return Ok(tid)` becoming
    // the corresponding `(tid, created)` pair:
    //   * the opening "already live" SELECT hit          -> Ok((tid, false))
    //   * the INSERT that succeeded                       -> Ok((tid, true))
    //   * the 23505 lost-race re-SELECT                   -> Ok((tid, false))
    // The catalog-view guard and the savepoint/rollback handling are unchanged.
}
```

Copy the existing body across exactly — the view guard, the savepoint, the
`is_duplicate_object_race` arm and its re-SELECT, and the "genuinely different failure" arm.
Only the three success returns change shape.

- [ ] **Step 5: Use the witness at the two reconcile call sites**

`src/control-plane/postgres/src/iceberg_inline.rs` — delete the pre-`ensure_table` probe
(`:465-469`) and take the witness from the ensure:

```rust
    // 1. Snapshot (no Iceberg backing) + ensure mirror table/columns exist. The
    //    ensure's WITNESS — did THIS call create the row? — is the batch->stream
    //    conversion guard's `pre_existing`. A read taken before the ensure cannot
    //    serve: `ensure_table` resolves a lost create race to the winner's table_id,
    //    so a pre-read would call a concurrent writer's brand-new table "not
    //    existing" and let a stream declare convert it.
    let at = next_snapshot(conn, None).await?;
    let (tid, created) = ensure_table_witnessed(conn, &table.schema, &table.name, at).await?;
    let pre_existing = !created;
```

Everything downstream (`is_stream` at `:493`, the `reconcile_stream_mode` call at `:560`) is
unchanged — both already run *after* the ensure. Fix the `use` to import
`ensure_table_witnessed` (keep `ensure_table` imported only if still used elsewhere in the
file; an unused import is a clippy failure).

`src/control-plane/postgres/src/iceberg_landing.rs`:

- In `land_parquet` (`:853-862`), the pre-transaction probe keeps computing `existing_stream`
  (the branch decision needs it) but **stops** computing `pre_existing`:

```rust
    // Read-only probe (no snapshot): is this table ALREADY a declared stream table?
    // That decides which write path we take. Deliberately does NOT witness
    // `pre_existing` for the conversion guard — this probe runs on a separate pooled
    // connection before the transaction, so it cannot see a concurrent creator, and
    // `ensure_table` inside the tx resolves a lost create race to the winner's row.
    // The honest witness comes from `ensure_table_witnessed` there.
    let existing_stream = {
        let mut conn = pool.acquire().await.map_err(backend)?;
        match live_table_id(&mut conn, &table.schema, &table.name).await? {
            Some(tid) => crate::stream::pg_stream_bucket_count(&mut *conn, tid).await?,
            None => None,
        }
    };
```

- Drop the now-dead `pre_existing` argument from the `land_parquet_stream` call (`:878`) and
  from its signature (`:926`), and remove the stale mention in its doc comment (`:916`).
- Inside `land_parquet_stream`'s attempt loop (`:963`), take the witness:

```rust
        let at = next_snapshot(&mut tx, None).await?;
        // The ensure's witness is the conversion guard's `pre_existing` (see
        // `ensure_table_witnessed`): `created == false` means this table is somebody
        // else's — either long-live, or created by a writer that just won the race we
        // lost inside the ensure.
        let (tid, created) =
            ensure_table_witnessed(&mut tx, &table.schema, &table.name, at).await?;
        let pre_existing = !created;
```

Note this sits **inside the retry loop**, so a retried attempt re-witnesses — which is
correct: on attempt 2 our own attempt-1 row is rolled back, so a fresh ensure genuinely
re-decides.

- [ ] **Step 6: Run the race test — it must PASS**

Run: `buck2 test --console none //src/control-plane/postgres:stream-declare-vs-concurrent-create`
Expected: `Tests finished: Pass 1. Fail 0.`

- [ ] **Step 7: Prove the witness change broke no landing behavior**

The witness is consumed by every declaring write, so sweep the landing/stream/inline suites:

Run: `buck2 test --console none //src/control-plane/postgres/...`
Expected: `Fail 0`.

Then the services that land through these paths:

Run: `buck2 test --console none //src/services/ingest/... //src/services/query-api/...`
Expected: `Fail 0`. (If the cloud disk cap bites, scope to the stream/land targets and say so
in the report.)

- [ ] **Step 8: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/control-plane/postgres/src/iceberg_mirror.rs \
        src/control-plane/postgres/src/iceberg_landing.rs \
        src/control-plane/postgres/src/iceberg_inline.rs \
        src/control-plane/postgres/tests/stream_declare_vs_concurrent_create.rs \
        src/control-plane/postgres/BUCK
git commit -m "fix(landing): witness pre_existing from ensure_table, not a pre-tx probe"
```

---

### Task 5: Documentation — the trait's unguarded declares, the registers, the capability

Three doc-level closures. The first is a *fix*, not a note: the spec's second "out of scope"
item asks whether `StreamTables::declare_stream` / `declare_cdc` should be routed through
`reconcile_stream_mode`. **Verify before writing:** `grep -rn "\.declare_stream(\|\.declare_cdc(" src/`
shows every caller is a test or a testkit contract — there is **no production caller**. So the
answer is not a design decision and needs no issue: document them as the test/setup surface
they are.

**Files:**
- Modify: `src/control-plane/core/src/stream.rs:123-144` (the `StreamTables` trait doc)
- Modify: `docs/ISSUES.md` (remove the closed item)
- Modify: `docs/system-capabilities/stream.md` (record the landed guard)

**Interfaces:**
- Consumes: nothing. Produces: nothing. Pure documentation.

- [ ] **Step 1: Document the trait's declares as an unreconciled setup surface**

`src/control-plane/core/src/stream.rs`, on the `StreamTables` trait:

```rust
/// Direct registry writes for `stream.stream_table`. **Setup/test surface only.**
///
/// These bypass `reconcile_stream_mode` (the postgres adapter's declare seam,
/// which every production declare — HTTP `?mode=stream`/`?mode=cdc` and
/// `land`/`land_cdc` — goes through): no batch→stream conversion guard, no
/// bucket-count reconciliation, no kind check, and — for `declare_cdc` — no
/// changelog table registration. They run standalone on the pool (autocommit),
/// so a declare made through them is committed independently of any write.
/// Production code declares by landing with a stream mode; these exist so tests
/// and the testkit contracts can put a table into a declared state without
/// landing data.
#[async_trait]
pub trait StreamTables {
```

- [ ] **Step 2: Close the register item**

Remove the `iss-stream-first-declare-race-kind` entry from `docs/ISSUES.md` (registers carry
**open work only**; the closing PR removes the entry). If it is the last item under
`## ontology`, remove the now-empty heading too.

Then validate: `bash tools/docs.sh validate`
Expected: no errors.

- [ ] **Step 3: Record the landed capability**

Add to `docs/system-capabilities/stream.md`, in the declaration/reconciliation section, in the
prose voice that file already uses — the first-declare arm's post-race re-read now compares
kind as well as bucket count and rejects a concurrent cross-kind declare with a distinct
`Validation`; the CDC changelog writes happen only after that check passes; and the
batch→stream conversion guard's `pre_existing` witness now comes from `ensure_table` itself
(`ensure_table_witnessed`), so a stream declare that loses the mirror-row create race sees the
winner's table as pre-existing instead of converting it.

- [ ] **Step 4: Lint and commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/control-plane/core/src/stream.rs docs/ISSUES.md docs/system-capabilities/stream.md
git commit -m "docs(stream): close iss-stream-first-declare-race-kind; mark the trait declares test-only"
```

---

## Final gate (before the PR)

- [ ] **Whole-suite sweep:** `buck2 test --console none //src/...` → `Fail 0`.
      (In a disk-capped cloud session, scope to `//src/control-plane/...`,
      `//src/services/ingest/...`, `//src/services/query-api/...` and say so.)
- [ ] **`.sqlx` untouched:** `git status --short src/control-plane/postgres/.sqlx` prints
      nothing. The whole change added no SQL.
- [ ] **Metric gate (a FIX step, not a report):** run `loom-complexity diff` and
      `loom-duplication diff` against the **merge-base**, not the committed register. If
      `reconcile_stream_mode` got worse on any axis (cc / cognitive / MI / SLOC) — and it is
      already a long function, so the first-declare arm growing is the risk — **fix it in this
      PR**: the obvious seam is extracting the `(Some(n), None)` arm into a private
      `first_declare(conn, tid, n, decl, table, at) -> Result<i32>` in `stream.rs`. If the two
      race test files duplicate the `pg_stat_activity` barrier ≥ 20 lines, hoist it — check
      `//src/testing:seed` first, and put it there if it fits.
- [ ] **Claim refresh:** if the arc has run long, `bash tools/docs.sh claim iss-stream-first-declare-race-kind`.
- [ ] **Lease check before pushing:** `git ls-remote origin work/iss-stream-first-declare-race-kind`
      — the remote tip must be an ancestor of local HEAD. If not, STOP and surface the collision.
