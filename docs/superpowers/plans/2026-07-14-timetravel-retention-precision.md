# Time-travel retention guard — precision over proxy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop 410-ing a below-horizon time-travel read whose rows were never end-capped, without ever serving an incomplete read — by making GC record what it destroyed and testing the real condition instead of the `at < H` proxy.

**Architecture:** GC gains a durable per-incarnation reclaim watermark (`iceberg_mirror.table.reclaimed_through`), written inside its existing reclaim transaction. The read guard keeps `at >= H` as its fast path, and below `H` calls one new `Catalog` method, `snapshot_intact`, that answers a two-clause conjunction: nothing visible at `at` has *already* been destroyed (the watermark), **and** nothing visible at `at` is *eligible* to be destroyed (`begin <= at < end <= H`) on the surviving rows. Clause 2 is what keeps the verdict independent of whether GC has run, and what closes the race with a concurrent GC.

**Tech Stack:** Rust, sqlx (compile-time `query!` + runtime `AssertSqlSafe` for the dynamic `inline_<tid>` relation), Postgres, buck2, axum.

## Global Constraints

Copied verbatim from the spec and CLAUDE.md. Every task's requirements implicitly include this section.

- **Do NOT build a "quiet-table exemption."** The spec's `## The register entry's framing is wrong` section is binding: "no writes since S" is both too strong (an append end-caps nothing, so old reads stay complete) and **unsound** (a governed delete-all → `overwrite_truncate` end-caps everything and writes *no* new row, so a "quiet since S" query over surviving rows says "quiet" and we would serve **0 rows** instead of 410). The truncate-trap test (Task 3) exists to forbid re-introducing it.
- **Schema (`iceberg_mirror.column`) rows are OUT of scope for clause 2** — data files ∪ inline rows only. (Confirmed by the human on the spec's open question. Column end-caps destroy no data; GC reclaims `column` rows only at full dropped-incarnation reclaim, which deletes the `table` row and degrades the read to 404 anyway.)
- **Migration number is `0045`**, NOT the `0044` the spec guessed — `0044_dataset_view.sql` has since landed. Verify with `ls src/control-plane/postgres/migrations/ | tail -1` before creating the file.
- **Tests are `rust_test`/`loom_fixture_test` integration targets only.** Never an inline `#[cfg(test)] mod tests` — buck2 builds but never runs those, and the `no-inline-tests` prek hook fails the commit. Every new fixture test must use `loom_fixture_test` (not bare `rust_test`) or it runs without the Postgres fixture env and fails to boot.
- **After ANY SQL change** in `src/control-plane/postgres`: run `tools/sqlx-prepare.sh` and commit the resulting `.sqlx/` change. Freshness is enforced by the `//src/control-plane/postgres:sqlx-cache-check` test in the normal sweep.
- **Clippy is pedantic + restriction** on lib/bin code. No `unwrap`/`expect`/`panic`/indexing-slicing in production code (tests are exempt via the `loom_rust_test` wrapper). Silence a lint locally with `#[expect(lint, reason = "...")]` — a bare `#[allow]` needs a `reason` too.
- **Build/test commands:** `buck2 build -v0 --console none //src/...` and `buck2 test --console none //src/...`. Never pipe a superconsole `buck2 test` through `tail`/`head` — it stalls and leaves zombies.
- **Metric gate (Task 4) is a FIX step, not a reporting step.** Measured against the merge-base, not the committed register.

## Inherited assumptions (state once, do not re-litigate)

Both are pre-existing and NOT introduced by this work. Task 4 documents them; no task fixes them.

1. **Snapshot-id order vs `snapshot_time` order.** `H = max(snapshot_id) WHERE snapshot_time < cutoff` assumes ids track times. `snapshot_time` defaults to `now()` (transaction *start*) while the id comes from `nextval` at an arbitrary point in the transaction, so ids and times can invert under concurrent long writers. GC and the guard share the derivation (`iceberg_mirror::horizon_before`), so they never disagree with *each other*. This fix starts reasoning about per-row `end_snapshot` vs `H` and inherits the assumption.
2. **`H` advances during a read.** A row with `end` just above `H` at guard time can become reclaim-eligible milliseconds later as wall-clock advances. This is **not new**: today's `at >= H` fast path has exactly the same exposure (a row visible at `at` with `end > at >= H` becomes eligible once `H` advances past `end`). The retention window is a window; a read that straddles its boundary is racing GC either way. Do not claim this fix closes it, and do not try to close it here.

---

## File Structure

**Task 1 — GC records what it destroyed (postgres only):**
- Create: `src/control-plane/postgres/migrations/0045_iceberg_reclaimed_through.sql`
- Modify: `src/control-plane/postgres/src/iceberg_gc.rs` (`Victims`, `victim_data_files`, `delete_end_capped_inline_rows`, `reclaim_live`, `reclaim_dropped`, + new `bump_reclaimed_through`)
- Test: `src/control-plane/postgres/tests/iceberg_gc.rs` (2 new tests)

**Task 2 — the precise predicate behind one Catalog method:**
- Modify: `src/control-plane/core/src/catalog.rs` (trait method — no default body)
- Modify: `src/control-plane/postgres/src/iceberg_catalog.rs` (the real impl: 2 clauses, 3 queries)
- Modify: `src/control-plane/memory/src/catalog.rs` (the fake's impl)
- Modify: `src/control-plane/testkit/src/lib.rs` (contract, runs on both backends)

**Task 3 — rewire the guard + the tests that pin it:**
- Modify: `src/services/query-api/src/handler.rs` (`ensure_within_retention`)
- Create: `src/control-plane/postgres/tests/timetravel_intact.rs` + its `loom_fixture_test` target in `src/control-plane/postgres/BUCK` (the truncate trap and the end-cap cases live here — this is the layer with `land`/`overwrite`/`gc_table` in reach)
- Modify: `src/services/query-api/tests/as_of_guards_e2e.rs` (rewrite the false narrative; two assertions flip to 200) + its BUCK deps

**Task 4 — docs, registers, metric gates:**
- Modify: `docs/system-capabilities/query-api.md`, `docs/ISSUES.md`

---

### Task 1: GC records what it destroyed (`reclaimed_through`)

**Files:**
- Create: `src/control-plane/postgres/migrations/0045_iceberg_reclaimed_through.sql`
- Modify: `src/control-plane/postgres/src/iceberg_gc.rs`
- Test: `src/control-plane/postgres/tests/iceberg_gc.rs`

**Interfaces:**
- Consumes: `gc_table(catalog: &SqlCatalog, pool: &PgPool, table: &TableRef, retention: Duration) -> Result<GcSummary>` (`iceberg_gc.rs:100`); the existing private `victim_data_files` / `delete_data_files` / `delete_end_capped_inline_rows` / `reclaim_live` / `reclaim_dropped`.
- Produces: column `iceberg_mirror.table.reclaimed_through bigint not null default 0`, maintained by GC inside its reclaim transaction. **`GcSummary` is deliberately NOT changed** (adding a field would break the 10 existing `assert_eq!(summary, GcSummary {...})` call sites for no benefit — the watermark is observable by querying the column).

**Why the watermark and the deletes must share one transaction:** a watermark that commits without its deletes 410s a still-complete read (annoying but safe); deletes that commit without their watermark serve an **incomplete** read (data loss, silent). Both helpers below take `&mut sqlx::PgConnection` and are called from inside `gc_locked`'s existing `tx` (opened `iceberg_gc.rs:153`, committed `:170`), so this is structural, not a convention.

**Why `victim_data_files` must yield the watermark rather than a later query recomputing it:** its own doc (`iceberg_gc.rs:349-372`) warns that re-evaluating the victim predicate after `data_file_column_stat` rows are gone silently inverts it (the scalar subquery goes NULL and matches nothing). The victim set is materialized exactly once; the max `end_snapshot` has to ride along with it.

- [ ] **Step 1: Write the failing tests**

Append both to `src/control-plane/postgres/tests/iceberg_gc.rs`. They reuse the file's existing local helpers (`columns`, `ipc_body`, `batch`, `lineage`, `age_snapshot`, `age_all_snapshots`, `live_tid`, `SEVEN_DAYS`) and its existing imports — add only what is missing.

```rust
/// Read the live incarnation's reclaim watermark straight from the mirror.
async fn reclaimed_through(pool: &sqlx::PgPool, tid: i64) -> i64 {
    sqlx::query_scalar::<_, i64>(
        "select reclaimed_through from iceberg_mirror.table where table_id = $1",
    )
    .bind(tid)
    .fetch_one(pool)
    .await
    .expect("reclaimed_through")
}

/// GC records the highest `end_snapshot` it destroyed, in the same commit as the
/// deletes. Mirrors `gc_reclaims_aged_data_files_and_keeps_in_window`'s arrangement:
/// A lands at s1 and is end-capped at s2 by the first overwrite; B lands at s2 and is
/// end-capped at s3. Ageing ONLY s2 makes H = s2, so exactly A (end = s2) is reclaimed
/// -> the watermark is s2. B (end = s3 > H) is untouched, so the watermark does not
/// jump to s3.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_records_the_reclaim_watermark() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let t = TableRef {
        schema: "wh".into(),
        name: "t".into(),
    };

    let (schema, batches) = ipc_body(10);
    land(
        &pool,
        &catalog,
        &t,
        &columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "t"),
        None,
    )
    .await
    .expect("land");

    let tid = live_tid(&pool, "wh", "t").await;
    assert_eq!(
        reclaimed_through(&pool, tid).await,
        0,
        "a freshly landed table has reclaimed nothing"
    );

    let s2 = overwrite_parquet_snapshot(
        &pool,
        &catalog,
        &t,
        &columns(),
        vec![batch(4)],
        Some(&lineage(RunId(uuid::Uuid::new_v4()), "wh", "t")),
        &[],
    )
    .await
    .expect("ow s2");
    let _s3 = overwrite_parquet_snapshot(
        &pool,
        &catalog,
        &t,
        &columns(),
        vec![batch(2)],
        Some(&lineage(RunId(uuid::Uuid::new_v4()), "wh", "t")),
        &[],
    )
    .await
    .expect("ow s3");

    age_snapshot(&pool, s2.0).await; // H = s2

    let summary = gc_table(&catalog, &pool, &t, SEVEN_DAYS).await.expect("gc");
    assert_eq!(summary.data_file_rows, 1, "exactly A reclaimed");
    assert_eq!(
        reclaimed_through(&pool, tid).await,
        s2.0,
        "watermark == the end_snapshot of the reclaimed file A, not the tip"
    );
}

/// The watermark never regresses. A second GC run that reclaims nothing must leave it
/// alone (the `greatest(...)` in `bump_reclaimed_through`, and the no-op on an empty
/// victim set). The read guard's soundness argument leans on monotonicity.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reclaim_watermark_is_monotone() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let t = TableRef {
        schema: "wh".into(),
        name: "t".into(),
    };

    let (schema, batches) = ipc_body(10);
    land(
        &pool,
        &catalog,
        &t,
        &columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "t"),
        None,
    )
    .await
    .expect("land");
    let tid = live_tid(&pool, "wh", "t").await;

    let s2 = overwrite_parquet_snapshot(
        &pool,
        &catalog,
        &t,
        &columns(),
        vec![batch(4)],
        Some(&lineage(RunId(uuid::Uuid::new_v4()), "wh", "t")),
        &[],
    )
    .await
    .expect("ow s2");

    age_all_snapshots(&pool).await;
    gc_table(&catalog, &pool, &t, SEVEN_DAYS).await.expect("gc 1");
    let after_first = reclaimed_through(&pool, tid).await;
    assert_eq!(after_first, s2.0, "first run records A's end_snapshot");

    // Second run: nothing left to reclaim (the only live file is end-capped by nobody).
    let summary = gc_table(&catalog, &pool, &t, SEVEN_DAYS).await.expect("gc 2");
    assert_eq!(summary.data_file_rows, 0, "second run reclaims nothing");
    assert_eq!(
        reclaimed_through(&pool, tid).await,
        after_first,
        "a no-op run must not regress (or advance) the watermark"
    );
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `buck2 test --console none //src/control-plane/postgres:iceberg-gc`
Expected: FAIL — the `reclaimed_through` column does not exist, so `reclaimed_through()` errors with `column "reclaimed_through" does not exist` (SQLSTATE 42703). That is the right reason to fail.

- [ ] **Step 3: Create the migration**

Create `src/control-plane/postgres/migrations/0045_iceberg_reclaimed_through.sql`:

```sql
-- The per-incarnation reclaim watermark: the highest `end_snapshot` GC has physically
-- destroyed for this `table_id`.
--
-- Why it has to exist: reclaimed-ness was recorded NOWHERE. `gc_locked` returned a
-- `GcSummary` to its RPC caller and wrote no audit row, so any predicate computed from
-- SURVIVING mirror rows is blind to what GC already destroyed. That blindness is what
-- forced the read guard to use the `at < H` proxy (410 every below-horizon read, even
-- one whose rows were never end-capped).
--
-- What it proves: every reclaimed row `r` had `end(r) <= reclaimed_through`, and `r` was
-- visible at snapshot `S` iff `S < end(r)`. So `S >= reclaimed_through` proves no
-- reclaimed row of this incarnation was ever visible at `S` — the read is complete on
-- the already-destroyed axis. (The still-destroyable axis is clause 2 of the guard,
-- which needs no new state.)
--
-- Keyed on `table_id`, so it is per-INCARNATION for free: a dropped-and-recreated
-- `(schema, name)` does not inherit the dead incarnation's watermark.
alter table iceberg_mirror.table
    add column reclaimed_through bigint not null default 0;

-- Backfill EXISTING tables to the current snapshot tip, not to 0.
--
-- GC may already have run on this database, and what it destroyed is exactly what is not
-- recorded. Defaulting a pre-existing table to 0 would assert "nothing of mine has been
-- reclaimed" — a claim we cannot make — and the new precise guard would then SERVE a read
-- the old `at < H` guard refused. Assuming the worst (anything at or below the tip may
-- already be gone) keeps this migration monotone in safety: no read that 410s today can
-- start returning rows because of it.
--
-- Tables created after this migration start at 0 (nothing has been reclaimed, provably)
-- and get the full precision benefit immediately. Existing tables regain it as soon as
-- they take a snapshot above the tip recorded here.
update iceberg_mirror.table
   set reclaimed_through = coalesce((select max(snapshot_id) from iceberg_mirror.snapshot), 0);
```

- [ ] **Step 4: Teach the victim set to carry its watermark**

In `src/control-plane/postgres/src/iceberg_gc.rs`, extend `Victims` (currently at `:344-347`) and `victim_data_files` (`:373-400`):

```rust
struct Victims {
    ids: Vec<i64>,
    paths: Vec<String>,
    /// The highest `end_snapshot` among these victims — the reclaim watermark this batch
    /// contributes. `None` when the set is empty. It rides along with the materialized
    /// victim set BY NECESSITY, not convenience: re-deriving it after the deletes would
    /// re-evaluate the predicate below against a table whose `data_file_column_stat` rows
    /// are already gone, and that predicate silently INVERTS when they are (the scalar
    /// subquery goes NULL, so the `< $3` guard matches nothing). See this fn's doc.
    max_end: Option<i64>,
}
```

```rust
async fn victim_data_files(
    conn: &mut sqlx::PgConnection,
    tid: i64,
    h: i64,
    guard: Option<i64>,
) -> Result<Victims> {
    let rows = sqlx::query!(
        "select df.data_file_id, df.path, df.end_snapshot as \"end_snapshot!\" \
         from iceberg_mirror.data_file df \
         where df.table_id = $1 and df.end_snapshot is not null and df.end_snapshot <= $2 \
           and ($3::bigint is null or ( \
                 select cs.max_value from iceberg_mirror.data_file_column_stat cs \
                 where cs.data_file_id = df.data_file_id \
                   and cs.column_name = 'loom_offset')::bigint < $3::bigint)",
        tid,
        h,
        guard,
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(backend)?;
    let mut ids = Vec::with_capacity(rows.len());
    let mut paths = Vec::with_capacity(rows.len());
    let mut max_end: Option<i64> = None;
    for row in rows {
        ids.push(row.data_file_id);
        paths.push(row.path);
        max_end = max_end.max(Some(row.end_snapshot));
    }
    Ok(Victims {
        ids,
        paths,
        max_end,
    })
}
```

(`end_snapshot` is `nullable` in the schema but the `is not null` predicate makes it total here, hence the `"end_snapshot!"` non-null override — the same idiom `dropped_table_ids` uses at `iceberg_mirror.rs:346`.)

- [ ] **Step 5: Make the inline delete report its watermark too**

Change `delete_end_capped_inline_rows` (`:440-490`) to `RETURNING end_snapshot` and fold the max. Only the return type and the last block change; the `guard` construction above it is untouched.

```rust
/// Delete the live incarnation's end-capped inline rows under the per-bucket MV floor.
/// Returns `(rows_deleted, max_end_snapshot_deleted)` — the second is this tier's
/// contribution to the reclaim watermark.
async fn delete_end_capped_inline_rows(
    conn: &mut sqlx::PgConnection,
    tid: i64,
    h: i64,
    floor: Option<&MvFloor>,
    has_inline: bool,
) -> Result<(u64, Option<i64>)> {
    if !has_inline {
        return Ok((0, None));
    }
    let inline = inline_table_name(tid);
    let guard = match floor {
        None => String::new(),
        Some(f) => {
            let clauses: Vec<String> = f
                .per_bucket
                .iter()
                .filter(|(_, offset)| **offset > 0)
                .map(|(bucket, offset)| {
                    format!("(loom_bucket = {bucket} and loom_offset < {offset})")
                })
                .collect();
            if clauses.is_empty() {
                " and false".to_owned()
            } else {
                format!(" and ({})", clauses.join(" or "))
            }
        }
    };
    // RETURNING (so `fetch_all`, not `execute`): the deleted rows' `end_snapshot`s are the
    // watermark this tier contributes, and after the delete they are unrecoverable.
    let rows = sqlx::query(AssertSqlSafe(format!(
        "delete from {inline} where end_snapshot is not null and end_snapshot <= $1{guard} \
         returning end_snapshot"
    )))
    .bind(h)
    .fetch_all(&mut *conn)
    .await
    .map_err(backend)?;
    let mut max_end: Option<i64> = None;
    for row in &rows {
        let e: i64 = row.try_get("end_snapshot").map_err(backend)?;
        max_end = max_end.max(Some(e));
    }
    Ok((u64::try_from(rows.len()).unwrap_or(0), max_end))
}
```

Add `Row` to the sqlx import at `iceberg_gc.rs:70` so `try_get` resolves:

```rust
use sqlx::{AssertSqlSafe, PgPool, Row};
```

- [ ] **Step 6: Add the watermark write, and call it from both reclaim arms**

Add this helper next to the other private helpers in `iceberg_gc.rs` (e.g. directly after `delete_data_files`):

```rust
/// Advance an incarnation's reclaim watermark to the highest `end_snapshot` this run
/// destroyed for it.
///
/// `greatest(...)` rather than a bare assignment, and a no-op on an empty victim set, so
/// the watermark is MONOTONE: a later run that reclaims an older straggler (or reclaims
/// nothing) can never lower it. `Catalog::snapshot_intact`'s soundness leans on that — a
/// verdict must never flip from "incomplete" back to "complete" behind an already-refused
/// read.
///
/// MUST run inside the caller's GC transaction, in the same commit as the deletes it
/// describes. A watermark committed without its deletes 410s a read that is still
/// complete (safe, merely conservative); deletes committed without their watermark SERVE
/// an incomplete read (silent under-read). Both callers are already inside `gc_locked`'s
/// `tx`.
async fn bump_reclaimed_through(
    conn: &mut sqlx::PgConnection,
    tid: i64,
    max_end: Option<i64>,
) -> Result<()> {
    let Some(e) = max_end else { return Ok(()) };
    sqlx::query!(
        "update iceberg_mirror.table \
         set reclaimed_through = greatest(reclaimed_through, $2) where table_id = $1",
        tid,
        e,
    )
    .execute(&mut *conn)
    .await
    .map_err(backend)?;
    Ok(())
}
```

In `reclaim_live` (`:230-257`), fold both tiers' watermarks and write once. Only the three lines after `victim_data_files` change:

```rust
    let victims = victim_data_files(tx, tid, h, file_guard).await?;
    let files = delete_data_files(tx, &victims.ids).await?;
    let (inline, inline_max_end) =
        delete_end_capped_inline_rows(tx, tid, h, floor, has_inline).await?;
    // The watermark this run establishes for the live incarnation: the highest
    // `end_snapshot` destroyed across BOTH reclaimable tiers.
    bump_reclaimed_through(tx, tid, victims.max_end.max(inline_max_end)).await?;
    let held_by_mv_floor = candidates.saturating_sub(files.saturating_add(inline));
```

In `reclaim_dropped` (`:270-313`), bump per incarnation, right after its deletes and BEFORE the full-reclaim block:

```rust
    for inc in dropped {
        let victims = victim_data_files(tx, inc.table_id, h, None).await?;
        let files = delete_data_files(tx, &victims.ids).await?;
        // A dropped incarnation is still time-travellable until it is FULLY reclaimed, and
        // its aged-out data files are reclaimed unguarded on every run — so a read inside
        // it needs the same watermark evidence a live one does. (Once the incarnation is
        // fully reclaimed below, its `table` row goes with it and the read 404s instead;
        // the write is simply superseded, never wrong.)
        bump_reclaimed_through(tx, inc.table_id, victims.max_end).await?;
        paths.extend(victims.paths);
        file_rows += files;
        if inc.drop_snapshot <= h {
```

- [ ] **Step 7: Refresh the sqlx cache**

Run: `tools/sqlx-prepare.sh`
Expected: `.sqlx/` gains/updates query files for the changed `victim_data_files` and the new `bump_reclaimed_through`. `git status` should show changes only under `src/control-plane/postgres/.sqlx/`.

- [ ] **Step 8: Run the tests to verify they pass**

Run: `buck2 test --console none //src/control-plane/postgres:iceberg-gc //src/control-plane/postgres:sqlx-cache-check`
Expected: PASS — all 12 tests (10 pre-existing + 2 new), and the cache check green.

- [ ] **Step 9: Commit**

```bash
git add src/control-plane/postgres/migrations/0045_iceberg_reclaimed_through.sql \
        src/control-plane/postgres/src/iceberg_gc.rs \
        src/control-plane/postgres/tests/iceberg_gc.rs \
        src/control-plane/postgres/.sqlx
git commit -m "feat(gc): record a per-incarnation reclaim watermark in the same commit as the deletes"
```

---

### Task 2: `Catalog::snapshot_intact` — the precise predicate

**Files:**
- Modify: `src/control-plane/core/src/catalog.rs` (trait, after `snapshot_horizon` at `:151-155`)
- Modify: `src/control-plane/postgres/src/iceberg_catalog.rs` (impl, after `snapshot_horizon` at `:407-412`)
- Modify: `src/control-plane/memory/src/catalog.rs` (impl, after `snapshot_horizon` at `:130`)
- Modify: `src/control-plane/testkit/src/lib.rs` (contract, inside `catalog_contract`, after the `snapshot_horizon` block at `:699-727`)

**Interfaces:**
- Consumes: `reclaimed_through` (Task 1); `IcebergCatalog::resolve_table(&self, table: &TableRef, at: SnapshotId) -> Result<i64>` (`iceberg_catalog.rs:83`); `fetch_view(pool: &PgPool, table: &TableRef) -> Result<Option<ViewDef>>` (`iceberg_catalog.rs:25`); `control_plane_postgres::iceberg_inline::inline_table_exists(conn: &mut PgConnection, table_id: i64) -> Result<bool>` (`iceberg_inline.rs:64`, already `pub(crate)`) and `inline_table_name(table_id: i64) -> String` (`:42`); the memory fake's `Versioned<T> { begin: i64, end: Option<i64>, val: T }` (`memory/src/lib.rs:49`) and `CatalogState.files: HashMap<TableRef, Vec<Versioned<FileRef>>>`.
- Produces: `async fn snapshot_intact(&self, table: &TableRef, at: SnapshotId, horizon: SnapshotId) -> Result<bool>` on the `Catalog` trait. **Consumed by Task 3.**

**There are exactly TWO impls of `control_plane_core::Catalog`** — `MemoryControlPlane` and `IcebergCatalog`. (`impl Catalog for SqlCatalog` / `CommitExtrasCatalog` / `TxCommitCatalog` are `iceberg::Catalog`, a different trait — leave them alone.) Before starting, confirm with `grep -rn "impl Catalog for" --include=*.rs src/` and check each hit's `use` block.

**Give the trait method NO default body.** A default returning `Ok(true)` would silently serve incomplete reads on any impl that forgot to override it. A missing impl must be a compile error.

- [ ] **Step 1: Write the failing contract test**

In `src/control-plane/testkit/src/lib.rs`, at the **very end** of `catalog_contract` — NOT next to the `snapshot_horizon` assertions. The block calls `seeder.drop_table(&t)`, and everything after the `snapshot_horizon` assertions (the `schema(&t, cur.id)` checks, the missing-table `NotFound` checks, and a `list_tables` assertion that `t` is **live**) would break on both backends if the table were already dropped. `catalog_contract` does not drop the table itself today — the drop lives in the separate `catalog_delete_contract` — so this block introduces the only drop, and it must come last.

```rust
    // snapshot_intact(at, horizon): the precise retention predicate. `s0` and `s1` are the
    // two seeded batches; seeding APPENDS, so nothing is end-capped and every read is
    // complete — this is exactly the case the old `at < H` proxy got wrong.
    assert!(
        catalog
            .snapshot_intact(&t, s0.id, s1.id)
            .await
            .unwrap(),
        "append-only: no row visible at s0 is end-capped at all, so s0 is intact even \
         though s0 < horizon (this is the whole point of the fix)"
    );
    assert!(
        catalog.snapshot_intact(&t, s1.id, s1.id).await.unwrap(),
        "at == horizon is trivially intact"
    );

    // Now end-cap: dropping the table end-caps every file at D. A read at s0 is then
    // exposed the moment D falls at or below the horizon.
    let d = seeder.drop_table(&t).await;
    assert!(
        !catalog.snapshot_intact(&t, s0.id, d).await.unwrap(),
        "the drop end-capped s0's files at D; with horizon == D they are reclaim-eligible, \
         so s0 is NOT intact"
    );
    assert!(
        catalog.snapshot_intact(&t, s0.id, s1.id).await.unwrap(),
        "same end-cap, but horizon == s1 < D: the files are end-capped ABOVE the horizon, \
         so nothing may be reclaimed and s0 is still intact"
    );
```

Both backends really do end-cap the files at `D` on drop, so the last two assertions bite on each: the fake's `drop_table_catalog` (`memory/src/lib.rs:190-205`) sets `end = Some(D)` on every open file and column, and the pg seeder routes through `SqlCatalog::drop_table` → `iceberg_mirror::mark_dropped`, whose data-file leg is `end_cap_live_data_files` (`iceberg_mirror.rs:415-427`).

- [ ] **Step 2: Run it to verify it fails**

Run: `buck2 test --console none //src/control-plane/memory:catalog`
Expected: FAIL to **compile** — `no method named 'snapshot_intact' found for reference '&C'`. That is the right failure: the trait method does not exist yet.

- [ ] **Step 3: Add the trait method**

In `src/control-plane/core/src/catalog.rs`, directly after `snapshot_horizon` (`:155`):

```rust
    /// Whether every row visible at `at` is still readable — and cannot be destroyed out
    /// from under the read — given the retention horizon `horizon`.
    ///
    /// This is the PRECISE form of the retention guard, for reads BELOW the horizon. At or
    /// above it, completeness is already provable with no query at all (a reclaimable row
    /// has `end <= horizon <= at`, so it was never visible at `at`), and callers should
    /// keep that fast path.
    ///
    /// Returns `false` iff either:
    ///
    /// 1. some row visible at `at` has ALREADY been reclaimed — the incarnation's
    ///    `reclaimed_through` watermark is above `at`; or
    /// 2. some SURVIVING row visible at `at` is ELIGIBLE for reclaim
    ///    (`begin <= at < end <= horizon`) and so could be destroyed at any moment,
    ///    including mid-read.
    ///
    /// Clause 2 is what makes the verdict independent of whether GC has actually run, and
    /// what closes the race with a concurrent GC. The conjunction is monotone: a GC that
    /// reclaims such a row bumps the watermark above `at` in the SAME commit, so a verdict
    /// can never flip from "incomplete" back to "complete".
    ///
    /// **Implementors: evaluate clause 2 BEFORE clause 1, and order the two reads in time.**
    /// The clauses are not commutative under concurrency. Checking the (cheaper) watermark
    /// first admits the exact under-read this guard exists to prevent: the watermark read
    /// returns "nothing reclaimed", GC then commits — destroying a row visible at `at` and
    /// bumping the watermark together — and the later evidence read finds nothing eligible
    /// *because it has already been destroyed*. Reading the evidence first and the watermark
    /// last makes the pair a complete detector: whichever side of the evidence read GC lands
    /// on, one of the two clauses observes it.
    ///
    /// Both clauses are per-INCARNATION (the `table_id` live at `at`) and cover the data
    /// file and inline tiers — the two tiers GC reclaims. Schema (`column`) rows are
    /// deliberately out of scope: an end-capped column destroys no data, and GC reclaims
    /// `column` rows only at full dropped-incarnation reclaim, which deletes the `table`
    /// row and degrades the read to `NotFound` anyway.
    ///
    /// A "quiet table" proxy (`no writes since at`) is NOT a valid implementation of this:
    /// a governed delete-all end-caps every row and writes none, so it looks quiet from
    /// the surviving rows while serving zero rows. See
    /// `docs/superpowers/specs/2026-07-14-timetravel-retention-precision-design.md`.
    ///
    /// `NotFound` if the table is not live at `at`.
    async fn snapshot_intact(
        &self,
        table: &TableRef,
        at: SnapshotId,
        horizon: SnapshotId,
    ) -> Result<bool>;
```

- [ ] **Step 4: Implement it in the memory fake**

In `src/control-plane/memory/src/catalog.rs`, after `snapshot_horizon` (`:130`):

```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn snapshot_intact(
        &self,
        table: &TableRef,
        at: SnapshotId,
        horizon: SnapshotId,
    ) -> Result<bool> {
        let cat = self.catalog.lock();
        let (table, _) = cat.resolve_view(table);
        let table = &table;
        let live = cat.tables.get(table).is_some_and(|t| t.live_at(at.0));
        if !live {
            return Err(ControlPlaneError::NotFound(format!(
                "{}.{} @ {}",
                table.schema, table.name, at.0
            )));
        }
        // Clause 1 is vacuous in the fake: it models no GC, so nothing is ever physically
        // reclaimed and its watermark is permanently 0 (`at >= 0` always holds). That is
        // faithful, not a stub — there is nothing to be blind to.
        //
        // Clause 2 is the whole verdict: no file visible at `at` may be eligible for
        // reclaim (`begin <= at < end <= horizon`). The fake has no inline tier, so data
        // files are the only tier there is.
        let eligible = cat
            .files
            .get(table)
            .into_iter()
            .flatten()
            .any(|f| f.begin <= at.0 && f.end.is_some_and(|e| e > at.0 && e <= horizon.0));
        Ok(!eligible)
    }
```

- [ ] **Step 5: Implement it in the postgres adapter**

In `src/control-plane/postgres/src/iceberg_catalog.rs`, after `snapshot_horizon` (`:412`). The view-delegation preamble is copied verbatim from `files` (`:462-469`) — do not invent a variant.

```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn snapshot_intact(
        &self,
        table: &TableRef,
        at: SnapshotId,
        horizon: SnapshotId,
    ) -> Result<bool> {
        let resolved; // borrow gymnastics: delegate view -> base
        let (table, _projection) = match fetch_view(&self.pool, table).await? {
            Some(v) => {
                resolved = v.base;
                (&resolved, v.columns)
            }
            None => (table, None),
        };
        // Per-INCARNATION: the table_id live at `at`, which for a dropped-but-unreclaimed
        // incarnation is that incarnation, not the recreated one. `NotFound` here is the
        // fully-reclaimed case, and it is the one place the evidence self-destructs safely
        // (the read degrades to 404, which is honest).
        let tid = self.resolve_table(table, at).await?;

        // ORDER IS LOAD-BEARING: clause 2 (the surviving-row evidence) is read FIRST, and
        // the clause-1 watermark LAST, on one connection so the reads are strictly ordered
        // in time. Do not "simplify" by checking the cheap watermark first.
        //
        // Each statement runs on its own snapshot (loom sets no isolation level, so this is
        // READ COMMITTED), and GC commits its deletes and its watermark bump ATOMICALLY.
        // Reading the watermark first is therefore unsound: it could return 0, GC could then
        // commit (destroying a row visible at `at` AND bumping the watermark past `at`), and
        // the later evidence read would find nothing eligible — because it was already
        // destroyed — and we would serve an incomplete read. That is precisely the silent
        // under-read this whole guard exists to prevent.
        //
        // Evidence-then-watermark is a complete detector, because the watermark is monotone
        // and GC's two effects land together:
        //   - GC commits BEFORE the evidence read -> the watermark read (later still) sees
        //     the bump -> clause 1 fires.
        //   - GC commits AFTER the evidence read -> the row was still there, end-capped at
        //     or below the horizon, when we looked -> clause 2 fires.
        // There is no third case. (A `REPEATABLE READ` transaction would also close it, at
        // the cost of an isolation change; ordering is cheaper and needs no new machinery.)
        let mut conn = self.pool.acquire().await.map_err(backend)?;

        // Clause 2, data-file tier: is anything visible at `at` ELIGIBLE for reclaim?
        // `begin <= at` (visible) and `at < end <= horizon` (end-capped above the read but
        // at or below the horizon, i.e. GC may take it at any moment).
        let file_eligible = sqlx::query_scalar!(
            "select exists(select 1 from iceberg_mirror.data_file \
             where table_id = $1 and begin_snapshot <= $2 \
               and end_snapshot is not null and end_snapshot > $2 and end_snapshot <= $3) \
             as \"eligible!\"",
            tid,
            at.0,
            horizon.0,
        )
        .fetch_one(&mut *conn)
        .await
        .map_err(backend)?;
        if file_eligible {
            return Ok(false);
        }

        // Clause 2, inline tier. `inline_<tid>` is a DYNAMIC relation, so this one cannot
        // be a compile-time `query!` — probe for it, then splice the (trusted, i64-derived)
        // identifier and bind the values, exactly as `iceberg_gc` does.
        if crate::iceberg_inline::inline_table_exists(&mut conn, tid).await? {
            let inline = crate::iceberg_inline::inline_table_name(tid);
            let inline_eligible: bool = sqlx::query_scalar(AssertSqlSafe(format!(
                "select exists(select 1 from {inline} \
                 where begin_snapshot <= $1 \
                   and end_snapshot is not null and end_snapshot > $1 and end_snapshot <= $2)"
            )))
            .bind(at.0)
            .bind(horizon.0)
            .fetch_one(&mut *conn)
            .await
            .map_err(backend)?;
            if inline_eligible {
                return Ok(false);
            }
        }

        // Clause 1, read LAST (see the ordering note above): has anything visible at `at`
        // ALREADY been destroyed? Every reclaimed row had `end <= reclaimed_through`, and
        // was visible at `at` iff `at < end`. So `at >= watermark` proves none of them was.
        let watermark = sqlx::query_scalar!(
            "select reclaimed_through as \"reclaimed_through!\" \
             from iceberg_mirror.table where table_id = $1",
            tid,
        )
        .fetch_one(&mut *conn)
        .await
        .map_err(backend)?;
        Ok(at.0 >= watermark)
    }
```

Ensure `AssertSqlSafe` is in `iceberg_catalog.rs`'s sqlx import (add it if the file does not already import it: `use sqlx::{AssertSqlSafe, PgPool};` — check the existing import line first and extend, don't replace).

- [ ] **Step 6: Refresh the sqlx cache and run the contract on both backends**

Run: `tools/sqlx-prepare.sh`
Then: `buck2 test --console none //src/control-plane/memory:catalog //src/control-plane/postgres:iceberg_catalog //src/control-plane/postgres:sqlx-cache-check`
Expected: PASS on both backends — the contract now asserts the append-only case is intact and the end-capped case is not.

- [ ] **Step 7: Commit**

```bash
git add src/control-plane/core/src/catalog.rs \
        src/control-plane/memory/src/catalog.rs \
        src/control-plane/postgres/src/iceberg_catalog.rs \
        src/control-plane/testkit/src/lib.rs \
        src/control-plane/postgres/.sqlx
git commit -m "feat(catalog): snapshot_intact — the two-clause retention predicate, certified on both backends"
```

---

### Task 3: Rewire the guard, and pin the trap

**Files:**
- Modify: `src/services/query-api/src/handler.rs:452-490` (`ensure_within_retention`)
- Create: `src/control-plane/postgres/tests/timetravel_intact.rs` + a `loom_fixture_test` target in `src/control-plane/postgres/BUCK`
- Modify: `src/services/query-api/tests/as_of_guards_e2e.rs` (+ its BUCK deps)

**Interfaces:**
- Consumes: `Catalog::snapshot_intact` (Task 2); `QueryError::AsOfGone(String)` → 410 (`http.rs:1012`); `QueryError::ControlPlane(_)` → 500.
- Produces: no new public surface. `ensure_within_retention` keeps its exact signature — both call sites (`handler.rs:448`, `http.rs:362`) are untouched.

**Where the trap lives, and why.** The truncate case is a *catalog-level* claim (`snapshot_intact` must say "not intact" for a table whose rows were end-capped and reclaimed with nothing written in their place). Pinning it in `postgres/tests/timetravel_intact.rs` puts it where `land` / `overwrite_parquet_snapshot` / `gc_table` are all directly in reach, and — more importantly — puts it directly on the function a future "quiet-table" refactor would rewrite. An HTTP-level 410 test cannot fail if someone reintroduces the proxy *and* the HTTP layer happens to mask it. Both layers get a test; the trap is the catalog one.

- [ ] **Step 1: Write the failing catalog-level tests**

Create `src/control-plane/postgres/tests/timetravel_intact.rs`:

```rust
//! `Catalog::snapshot_intact` against real end-caps, real GC, and the real truncate path
//! (`iss-timetravel-quiet-table-overconservative`).
//!
//! The guard this backs replaced a proxy (`at < H` => 410) that was over-conservative in
//! one direction. The tests below pin BOTH directions, because the obvious alternative fix
//! — a "quiet table" exemption ("nothing was written since `at`, so the read is fine") — is
//! not merely imprecise, it is UNSOUND, and `truncate_is_not_quiet` is the test that says
//! so. A governed delete-all end-caps every row and writes NO new row, so a query over the
//! SURVIVING mirror rows reports "quiet since S" and the proxy would serve zero rows where
//! the data used to be: a loud 410 traded for silent data loss. Do not reintroduce it.

use std::time::Duration;

// NOTE: `ColumnSpec` lives in `control_plane_core` (`core/src/snapshot.rs:9`), NOT in
// `iceberg_mirror` — mirror the import block of `tests/iceberg_gc.rs:16-18`.
use control_plane_core::{
    Catalog, ColumnSpec, DatasetId, EventType, LineageEvent, RunId, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_gc::gc_table;
use control_plane_postgres::iceberg_landing::{InlineLimits, land, overwrite_parquet_snapshot};
use loom_test_seed::local_sql_catalog;

const SEVEN_DAYS: Duration = Duration::from_secs(7 * 24 * 3600);

// `columns`, `ipc_body`, `batch`, `lineage`, `age_snapshot`, `age_all_snapshots` are
// copied from `tests/iceberg_gc.rs`. If a shared fixture-test seed library exists by the
// time you read this (`//src/testing:seed`), prefer it — see the duplication gate in
// Task 4, which will flag these if they exceed the 20-line cross-file threshold.

/// Nothing was ever end-capped: an append-only table below the horizon is INTACT.
/// This is the acceptance case — the read the old proxy refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn append_only_below_horizon_is_intact() {
    // land s1 (10 rows), append s2 (10 rows) via a second `land`.
    // age_all_snapshots -> horizon H = s2 (the tip).
    // assert catalog.snapshot_intact(&t, s1, H).await.unwrap() == true
    //   -> s1's files are LIVE (end_snapshot is null): not eligible, never reclaimed.
    // assert files_with_stats(&t, s1) still returns s1's file (the read really is complete).
    todo!("write this body against the arrangement above")
}

/// An end-capped-and-reclaimed read is NOT intact — clause 1 (the watermark).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reclaimed_below_horizon_is_not_intact() {
    // land s1, overwrite at s2 (end-caps s1's file at s2), age_all, gc_table.
    // assert reclaimed_through == s2, and snapshot_intact(&t, s1, H) == false.
    todo!("write this body against the arrangement above")
}

/// Eligible but NOT yet reclaimed is ALSO not intact — clause 2. The verdict must not
/// depend on GC having run: the row can be destroyed mid-read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eligible_but_ungced_is_not_intact() {
    // land s1, overwrite at s2, age_all_snapshots, and DO NOT run gc.
    // assert reclaimed_through == 0 (clause 1 passes) but snapshot_intact(&t, s1, H) == false.
    todo!("write this body against the arrangement above")
}

/// THE TRAP. A governed delete-all (`overwrite_parquet_snapshot` with a zero-row batch ->
/// `overwrite_truncate`) end-caps every live row and writes NOTHING. The surviving mirror
/// state is a live `table` row, its `column` rows, and zero data files — so any
/// "was this table written since S?" proxy reports QUIET and would serve an empty result
/// set for a snapshot that had rows. It must be 410 (not intact), via the watermark.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn truncate_is_not_quiet() {
    // land s1 (10 rows), then overwrite_parquet_snapshot(..., vec![batch(0)], ...) -> s2.
    //   (`overwrite_with_cap` routes all-zero-row batches to `overwrite_truncate`.)
    // assert files_with_stats(&t, s2) is EMPTY -- nothing was written in their place.
    // age_all_snapshots; gc_table.
    // assert snapshot_intact(&t, s1, H) == false   <-- the trap
    // and assert it is false for the RIGHT reason: reclaimed_through == s2 (clause 1),
    // since after GC there is no surviving end-capped row for clause 2 to find.
    todo!("write this body against the arrangement above")
}

/// A dropped-but-unreclaimed incarnation is still time-travellable, and its verdict is
/// per-INCARNATION. Then, once it is FULLY reclaimed, the `table` row goes with it and the
/// read degrades to 404 (`NotFound`) — not 410, not 200. This is the one case where the
/// evidence self-destructs safely.
///
/// Note this is a genuine BEHAVIOR CHANGE, not just new coverage: a read inside a dropped
/// incarnation whose drop snapshot `D` is ABOVE the horizon used to 410 (it is below `H`)
/// and now correctly serves — its files are end-capped at `D > H`, so nothing may be
/// reclaimed. The spec's Testing section requires this case; it is not optional.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_incarnation_serves_then_404s_once_fully_reclaimed() {
    // land s1 (10 rows). Then drop the table via the vendored catalog (SqlCatalog::drop_table
    // -> mark_dropped -> end_cap_live_data_files), giving drop snapshot D.
    //
    // Leg 1 -- dropped, D ABOVE the horizon: with H = s1 (age ONLY s1), s1's files are
    //   end-capped at D > H, so nothing is eligible:
    //     assert snapshot_intact(&t, s1, H = s1) == true
    //
    // Leg 2 -- dropped, D AT/BELOW the horizon: age_all_snapshots so H = D:
    //     assert snapshot_intact(&t, s1, H = D) == false   (clause 2: end == D <= H)
    //
    // Leg 3 -- FULLY reclaimed: gc_table (with D <= H, `reclaim_dropped` deletes the
    //   data_file/column/table rows outright):
    //     assert matches!(catalog.snapshot_intact(&t, s1, H).await,
    //                     Err(ControlPlaneError::NotFound(_)))
    //   because `resolve_table(&t, s1)` no longer finds any incarnation. The HTTP layer
    //   never reaches the guard in this state -- `Catalog::snapshot` already returns None
    //   -> `AsOfNotFound` -> 404 -- which is exactly the intended degradation.
    todo!("write this body against the arrangement above")
}
```

**The five `todo!()`s above are the ONLY placeholders in this plan, and they are deliberate**: each body is a ~20-line mechanical transcription of `tests/iceberg_gc.rs::gc_reclaims_aged_data_files_and_keeps_in_window` (the canonical land → overwrite → age → gc → assert arrangement) against the arrangement spelled out in its own comment. Write them from that template. `todo!()` is a clippy-denied macro in production code but tests are exempt — still, none may survive Step 3.

- [ ] **Step 2: Wire the BUCK target and run to verify it fails**

Add to `src/control-plane/postgres/BUCK`, mirroring the `iceberg-gc` target (`:617-637`):

```python
loom_fixture_test(
    name = "timetravel-intact",
    crate = "timetravel_intact",
    srcs = ["tests/timetravel_intact.rs"],
    crate_root = "tests/timetravel_intact.rs",
    edition = "2024",
    deps = [
        ":postgres",
        "//src/control-plane/core:core",
        "//src/testing:seed",
        "//third-party:arrow-array",
        "//third-party:arrow-schema",
        "//third-party:serde_json",
        "//third-party:sqlx",
        "//third-party:tempfile",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```

`time` and `serde_json` are NOT optional: the copied `lineage()` helper builds its payload with `serde_json::json!` (`iceberg_gc.rs:67`) and uses `OffsetDateTime::now_utc()` (`:64`), and `age_snapshot`/`age_all_snapshots` use `time::Duration::days(365)` (`:78`, `:89`). `//third-party:iceberg` and `arrow-ipc` are NOT needed.

Run: `buck2 test --console none //src/control-plane/postgres:timetravel-intact`
Expected: FAIL — `not yet implemented` panics from the `todo!()`s.

- [ ] **Step 3: Write the four test bodies**

Transcribe each from `tests/iceberg_gc.rs::gc_reclaims_aged_data_files_and_keeps_in_window`, following each test's comment. Run again:

Run: `buck2 test --console none //src/control-plane/postgres:timetravel-intact`
Expected: PASS, 4/4. If `append_only_below_horizon_is_intact` fails, `snapshot_intact` is still over-conservative; if `truncate_is_not_quiet` fails, the watermark is not being written by the truncate path — check that `overwrite_truncate` end-caps the data files with `end_snapshot = s2` and that GC reclaims them.

- [ ] **Step 4: Rewire the guard**

Replace `ensure_within_retention` in `src/services/query-api/src/handler.rs:452-490` entirely (signature unchanged — both callers keep working):

```rust
/// Reject a resolved as-of snapshot whose data may already be — or may at any moment be —
/// physically reclaimed.
///
/// Two tiers, cheapest first:
///
/// 1. `at >= H` (where `H = max(snapshot_id) WHERE snapshot_time < now() - gc_retention`,
///    the exact horizon `iceberg_gc` reclaims under — shared derivation, so guard and
///    reclaimer cannot drift): provably complete with no further query, because a
///    reclaimable row has `end <= H <= at` and so was never visible at `at`. The
///    overwhelmingly common case; unchanged.
/// 2. Below `H`, ask the catalog the REAL question (`Catalog::snapshot_intact`): has
///    anything visible at `at` already been reclaimed, or is anything visible at `at`
///    eligible to be? Only if neither holds is the read served.
///
/// Tier 2 is the fix for `iss-timetravel-quiet-table-overconservative`. The old guard
/// stopped at tier 1 and 410'd everything below `H` — including an append-only table,
/// which end-caps nothing and whose old snapshots are therefore complete FOREVER.
///
/// It is emphatically NOT a "quiet table" exemption ("nothing written since `at`"). That
/// proxy is unsound in the other direction: a governed delete-all end-caps every row and
/// writes none, so the table looks quiet while the read would return zero rows. See
/// `snapshot_intact`'s contract and `postgres/tests/timetravel_intact.rs::truncate_is_not_quiet`.
pub(crate) async fn ensure_within_retention(
    catalog: &(dyn control_plane_core::Catalog + Send + Sync),
    gc_retention: std::time::Duration,
    table: &control_plane_core::TableRef,
    at: control_plane_core::SnapshotId,
) -> Result<(), QueryError> {
    let cutoff =
        time::OffsetDateTime::now_utc() - time::Duration::seconds(gc_retention.as_secs() as i64);
    let horizon = catalog
        .snapshot_horizon(cutoff)
        .await
        .map_err(QueryError::ControlPlane)?;
    let Some(h) = horizon else { return Ok(()) };
    if at >= h {
        return Ok(());
    }
    if catalog
        .snapshot_intact(table, at, h)
        .await
        .map_err(QueryError::ControlPlane)?
    {
        return Ok(());
    }
    Err(QueryError::AsOfGone(format!(
        "{}.{} snapshot {} is older than the GC retention horizon ({}) and its data has \
         been — or may at any moment be — reclaimed; pick a snapshot >= {} or widen \
         LOOM_GC_RETENTION_SECS",
        table.schema, table.name, at.0, h.0, h.0
    )))
}
```

Note the `ControlPlaneError::NotFound` case maps to 500, not 404 — and that is correct: both callers have *already* validated the table is live at `at` (`Catalog::snapshot` in `resolve_read_snapshot:427` and `resolve_dataset_snapshot`), so a `NotFound` here is a genuine anomaly (a table dropped and fully reclaimed between two queries), not a caller error.

- [ ] **Step 5: Rewrite the e2e narrative — two assertions flip to 200**

`src/services/query-api/tests/as_of_guards_e2e.rs`'s module doc and its `NOTE on the removed "quiet-table exemption" branch` are now **factually wrong about their own fixture**, and were before this change: the comment claims S1 is "a genuinely stale read of a table that has since been **rewritten**", but `setup` calls `seed_arrays` twice, and `seed_arrays` → `append_batches` is an **APPEND**. Nothing is end-capped. `main.thing` at S1 is complete and always will be.

Rewrite the module doc to state the real semantics, delete the whole stale `NOTE on the removed "quiet-table exemption"` block (it describes a limitation that no longer exists), and flip the two assertions:

- Assertion 1 (`?as_of_snapshot={s1}`, ZERO retention): `StatusCode::GONE` → **`StatusCode::OK`**, and assert `ids(&body) == vec![1, 2]` (S1's rows, not the live set).
- Assertion 2 (`?as_of={s1_time}`, ZERO retention): `StatusCode::GONE` → **`StatusCode::OK`** + the same ids.
- Assertion 5 (dataset `?as_of_snapshot={s1}`, ZERO retention): `StatusCode::GONE` → **`StatusCode::OK`**, and assert `body["snapshot_id"].as_i64() == Some(s1)`.
- Assertions 3, 4, 6 are unchanged (they already assert 200).

Then ADD a genuine 410 case to the same test — the file must still cover the refusal, or the rewrite silently deletes the guard's only e2e coverage:

```rust
    // 7. A genuinely end-capped read still 410s. Overwrite `main.thing` (this REPLACES the
    //    live set, end-capping S1's and S2's files at S3) and read at S1 under ZERO
    //    retention: S1's rows are now eligible for reclaim, so the read is refused. This is
    //    the case the file previously only CLAIMED to cover — `seed_arrays` appends, so
    //    assertion 1 above was never testing a rewritten table at all.
    let cols = [
        ColumnSpec { name: "id".into(), ty: "long".into(), nullable: false },
        ColumnSpec { name: "name".into(), ty: "string".into(), nullable: true },
    ];
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let replacement = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int64Array::from(vec![9_i64])),
            Arc::new(StringArray::from(vec!["z"])),
        ],
    )
    .expect("replacement batch");

    overwrite_parquet_snapshot(
        &pool,
        &writer.sql_catalog().await,
        &thing,
        &cols,
        vec![replacement],
        None,
        &[],
    )
    .await
    .expect("overwrite");

    let (status, body) = get_with_retention(
        cp.clone(),
        eng.clone(),
        &format!("/objects/Thing?as_of_snapshot={s1}"),
        "alice",
        std::time::Duration::ZERO,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::GONE,
        "as_of_snapshot=S1 after a REAL rewrite (ZERO): {body}"
    );
```

Imports this adds to the test file: `std::sync::Arc` (already there), `arrow_array::{Int64Array, RecordBatch, StringArray}`, `arrow_schema::{DataType, Field, Schema as ArrowSchema}`, `control_plane_core::ColumnSpec` (extend the existing `control_plane_core::{...}` line — `ColumnSpec` is in **core**, not `iceberg_mirror`), and `control_plane_postgres::iceberg_landing::overwrite_parquet_snapshot`.

`setup` must also return the `pool` it already builds (add it to the tuple). The BUCK target gains `//third-party:arrow-array` and `//third-party:arrow-schema`.

- [ ] **Step 6: Run the full affected suite**

Run: `buck2 test --console none //src/services/query-api/... //src/control-plane/...`
Expected: PASS. `as_of_guards_e2e` now proves the append-only read is served; `timetravel_intact` proves the end-capped, the eligible-but-un-GC'd, and the truncate reads are all refused.

- [ ] **Step 7: Commit**

```bash
git add src/services/query-api/src/handler.rs \
        src/services/query-api/tests/as_of_guards_e2e.rs \
        src/services/query-api/BUCK \
        src/control-plane/postgres/tests/timetravel_intact.rs \
        src/control-plane/postgres/BUCK
git commit -m "fix(query-api): test the real retention condition, not the at<H proxy — close iss-timetravel-quiet-table-overconservative"
```

---

### Task 4: Gates, docs, registers

**Files:**
- Modify: `docs/system-capabilities/query-api.md` (lines ~60, ~64, ~105 describe today's conservative behavior and name the issue id)
- Modify: `docs/ISSUES.md` (remove the closed entry)

- [ ] **Step 1: Run the full sweep**

Run: `buck2 test --console none //src/...`
Expected: PASS, except the known `src/ui/e2e:login` container gap (`libnspr4.so`) — this branch touches zero `src/ui/` files. Record the actual pass/fail counts; do not round them off.

- [ ] **Step 2: Run BOTH metric gates against the MERGE-BASE**

```bash
git merge-base HEAD main    # measure the touched functions HERE, and again at HEAD
```

Run: `/loom-complexity diff` and `/loom-duplication diff`.

**These are FIX steps.** If the branch worsened any touched hotspot on any axis (cc, cognitive, MI, SLOC), or introduced any cross-file duplication pair >= 20 lines, **fix it in this PR** — not in a follow-up, not in the PR body. Two seams are pre-identified as likely findings:

- **`gc_locked` / `reclaim_live` complexity.** The watermark write was deliberately pushed DOWN into `reclaim_live` / `reclaim_dropped` (which already hold the tx) rather than up into `gc_locked`, precisely so `gc_locked` gains zero branches. Verify that held.
- **Fixture-helper duplication** between the new `postgres/tests/timetravel_intact.rs` and the existing `postgres/tests/iceberg_gc.rs` (`columns`, `ipc_body`, `batch`, `lineage`, `age_snapshot`, `age_all_snapshots`, plus the 5-line `PgFixture` preamble). This is the *likely* finding, and "it's just test seeding" is **not** an acceptable answer — check `//src/testing:seed` and extract a shared helper (the `e2e-support` pattern) if the pair clears 20 lines. `iceberg_gc.rs` has no shared `setup()` today and repeats the preamble 10 times; if the gate points there, that extraction is the fix, and it should have existed already.

Put the before/after numbers in the PR body.

- [ ] **Step 3: Update the OpenAPI 410 text**

Two `#[utoipa::path]` blocks describe the 410 as *"Selector resolves to a snapshot older than the GC retention horizon (data may be reclaimed)"* — `http.rs:325` (`/datasets/{schema}/{table}`) and `http.rs:565` (`/objects/{type_name}`). After this change that is **no longer the trigger**: a snapshot older than the horizon is served when its rows are intact. Reword both to the real condition, e.g. *"The snapshot's data has been — or may at any moment be — reclaimed (below the retention horizon AND end-capped)"*. Check the handler doc comment near `http.rs:76` too.

No drift-guard test asserts these strings (`query-api/tests/openapi.rs`, `runtime/tests/openapi_fragments.rs` were both checked), so this is a correctness-of-documentation fix, not a build break — which is exactly why it is easy to forget.

- [ ] **Step 4: Update the capability docs**

`docs/system-capabilities/query-api.md` documents the conservative guard and names `iss-timetravel-quiet-table-overconservative`. **Grep for the id** rather than trusting line numbers (it appears at least twice, including a "known gaps" bullet; surrounding prose also describes the old semantics). Rewrite those passages to describe the shipped two-clause guard. State plainly, without hedging:

- `at >= H` is still the fast path (no extra query).
- Below `H`, the guard tests the real condition; an append-only table's old snapshots now serve.
- The truncate case still 410s, and *why* the obvious alternative (a quiet-table exemption) is unsound — a future reader must not "simplify" this back.
- **The engine has NO parallel guard.** `EngineTicket::AsOfSql` → `do_get_as_of_sql` (`engine/src/flight.rs:102-117`) runs with no retention check; query-api is the sole enforcement point. That is fine while the Flight surface is internal-only, and it becomes a real gap when the external SQL wire lands. Say this explicitly rather than let symmetry be assumed.
- Both inherited assumptions from this plan's header (snapshot-id/time inversion; `H` advancing mid-read), as caveats — noting the second is not new.
- The clause **ordering** requirement (evidence read before watermark read) is load-bearing for soundness under a concurrent GC, not an implementation detail. A future reader who "optimizes" by checking the cheap watermark first reintroduces a silent under-read. Say so where the guard is described.

- [ ] **Step 5: Close the register item**

Invoke the `loom-docs-update` skill. It should remove the `iss-timetravel-quiet-table-overconservative` entry from `docs/ISSUES.md` (registers carry open work only) and fold the landed capability into `docs/system-capabilities/`.

**File nothing you could have fixed.** If you found a defect while building this, fix it here — see the checkout skill's "FIX them, do not FILE them". A new ISSUES entry is justified only for a design decision a human must make, or a fix in a different subsystem needing its own spec; say which, in the entry.

**One known collapse, in reach, fix it here:** `iceberg_gc.rs:430` defines a private `inline_table_exists` that duplicates the `pub(crate)` `iceberg_inline::inline_table_exists` (`:64`). Task 2 makes the catalog use the `iceberg_inline` one, which leaves the GC-local copy redundant. Delete it and point `reclaim_live` at the shared helper. (There is a third `to_regclass` probe, `iceberg_inline.rs:1014`'s spliced-literal `inline_relation_exists` — collapse it too if it is a drop-in; do not contort the call sites to force it.)

- [ ] **Step 6: Run the hooks, then commit**

Run: `buck2 run //tools:prek -- run --all-files`
The hooks (rustfmt, clippy, end-of-file-fixer, trailing-whitespace, docs-validate, reindeer-check) rewrite files in place. Commit whatever they change — a markdown file with a stray trailing newline fails CI `lint` even on a green build.

```bash
git add -A
git commit -m "docs(query-api): record the precise retention guard; close iss-timetravel-quiet-table-overconservative"
```

- [ ] **Step 7: Open the PR**

Head branch MUST be `work/iss-timetravel-quiet-table-overconservative` (this is what binds the claim to the PR). Before pushing, lease-check:

```bash
git ls-remote origin work/iss-timetravel-quiet-table-overconservative
```

Verify the remote tip is an ancestor of your local branch. If it is not — someone else's commits are on the branch — STOP and surface the collision rather than force-pushing over live work.

The PR body must name the id + the closing, and carry the before/after metric numbers from Step 2.

---

## Self-Review

**Spec coverage.** Every section maps to a task:

| Spec requirement | Task |
|---|---|
| Durable reclaim watermark (migration + GC write inside the tx) | 1 |
| Guard becomes a conjunction (clause 1 watermark + clause 2 eligibility) | 2 (predicate), 3 (guard) |
| `at >= H` stays the cheap fast path | 3, Step 4 |
| Per-incarnation, not per `(schema, name)` | 2, Step 5 (`resolve_table(table, at)` → `tid`) |
| Open question: schema rows excluded | Global Constraints (human-confirmed) |
| New `Catalog` method + memory impl + testkit contract | 2 |
| Inline tier needs `to_regclass` + `AssertSqlSafe` | 2, Step 5 |
| Acceptance 1: append-only below H → 200 | 3 (`append_only_below_horizon_is_intact`, e2e assertion 1) |
| Acceptance 2: end-capped+reclaimed → 410; truncate pinned | 3 (`reclaimed_below_horizon_is_not_intact`, `truncate_is_not_quiet`) |
| Acceptance 3: eligible-but-un-GC'd → 410 | 3 (`eligible_but_ungced_is_not_intact`) |
| Watermark monotonicity | 1 (`reclaim_watermark_is_monotone`) |
| Dropped incarnation → 404 after full reclaim | 3 (`dropped_incarnation_serves_then_404s_once_fully_reclaimed` — a real test, not just the `resolve_table` → `NotFound` implementation detail) |
| Rewrite `as_of_guards_e2e.rs`'s false prose | 3, Step 5 |
| GC-side fixture updates | 1, Step 1 |
| Docs: `system-capabilities/query-api.md` | 4, Step 3 |
| Engine has no parallel guard — say so | 4, Step 3 |
| Known assumption (id/time inversion) | Plan header |
| Acceptance 4: existing suites green | 4, Step 1 |

**One place the SPEC is incomplete, corrected here.** The spec argues the two-clause conjunction is monotone and concludes "a verdict can never flip from 'complete' to 'incomplete' behind an already-served read." That is true of the *design* but not of any *implementation* of it: the clauses are read as separate statements under READ COMMITTED, and if the watermark is read FIRST, a GC that commits between the two reads destroys a row visible at `at` and hides it from both clauses — the watermark read was too early to see the bump, the evidence read too late to see the row. The plan therefore mandates **evidence first, watermark last, ordered in time** (Task 2, Step 5), and states the requirement in the trait contract so an implementor cannot reproduce the bug. This was caught in plan review, before any code.

**Deviations from the spec, and why** (all deliberate, all called out at the point of use):

1. **Migration is `0045`, not `0044`** — `0044_dataset_view.sql` landed after the spec was written.
2. **The migration BACKFILLS existing tables to the snapshot tip** rather than leaving them at the `default 0` the spec implies. The spec's design is right that reclaimed-ness is recorded nowhere — which means for a table that *already* exists when this migration runs, `0` is a claim we cannot substantiate, and the new guard would then serve a read the old one refused. Backfilling to the tip makes the change monotone in safety.
3. **The truncate trap is pinned at the catalog level** (`postgres/tests/timetravel_intact.rs`), not only via HTTP: the proxy a future session might reintroduce would be rewritten *in `snapshot_intact`*, so that is where the test must bite. The e2e keeps an end-cap 410 case too.
4. **`GcSummary` gains no field.** The spec does not ask for one; adding it would break 10 existing exact-match assertions for no benefit.

**Placeholder scan.** The four `todo!()`s in Task 3 Step 1 are the only ones, are labeled as deliberate scaffolding with the exact arrangement spelled out per test, and Step 3 requires all four gone before the task can pass. No "add appropriate error handling", no "similar to Task N", no "TBD".

**Type consistency.** `snapshot_intact(&self, table: &TableRef, at: SnapshotId, horizon: SnapshotId) -> Result<bool>` is spelled identically in the trait (2/Step 3), the memory impl (2/Step 4), the postgres impl (2/Step 5), the contract (2/Step 1), and the guard call site (3/Step 4). `bump_reclaimed_through(conn: &mut sqlx::PgConnection, tid: i64, max_end: Option<i64>) -> Result<()>` matches its two call sites. `Victims.max_end: Option<i64>` matches `victims.max_end.max(inline_max_end)`, and `delete_end_capped_inline_rows` returns `(u64, Option<i64>)` destructured as `(inline, inline_max_end)`.
