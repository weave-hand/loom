# Iceberg GC — dropped-table reclaim Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend `gc_table(schema, name)` so it reclaims the aged-out dead bytes of every **dropped** incarnation of `(schema, name)` — its end-capped `data_file` Parquet, and (once the drop snapshot ages past the retention horizon) its physical `inline_<tid>` table and `table`/`column` mirror rows — in addition to today's live-table reclaim.

**Architecture:** Refactor slice-1's per-table reclaim in `iceberg_gc::gc_locked` into small `table_id`-parameterized helpers (behavior-preserving), then add a dropped-incarnation loop that reuses those helpers for the data-file/Parquet reclaim and adds a metadata-drop leg gated on the drop snapshot `D ≤ H`. A new `iceberg_mirror::dropped_table_ids` resolves the dropped `table_id`s (end-capped `table` rows for the name; the live row is excluded by the `end_snapshot is null` invariant). No new endpoint, RPC, or public `gc_table` signature change — the existing `POST /maintenance/gc/{schema}/{table}` and `EngineControl::GcTable` cover it.

**Tech Stack:** Rust, buck2, sqlx (compile-time `query!` + committed `.sqlx` cache; runtime `AssertSqlSafe` for the dynamic `inline_<tid>` identifier), Postgres (transactional DDL), fixture-backed `loom_fixture_test` against the Iceberg backend.

## Global Constraints

- **Tests are `rust_test` integration targets only — never inline `#[cfg(test)]`.** All new tests go in the existing `src/control-plane/postgres/tests/iceberg_gc.rs`, wired by the existing `loom_fixture_test(name = "iceberg-gc", ...)` target (`src/control-plane/postgres/BUCK:405`). No BUCK edit is needed — that target already carries `iceberg`, `sqlx`, `time`, `tempfile`, `uuid`, `serde_json` deps.
- **Fixture tests must stay `loom_fixture_test`** (already satisfied — `iceberg-gc` is one). They boot `initdb`/`postgres` and route the test command local; the build stays on RE.
- **Compile-time `query!` needs a fresh `.sqlx` cache.** Any new `sqlx::query!`/`query_scalar!` in `control-plane/postgres` requires regenerating `src/control-plane/postgres/.sqlx/` via `tools/sqlx-prepare.sh` and committing it. The `//src/control-plane/postgres:sqlx-cache-check` test (part of `buck2 test //src/...`) fails on a stale/missing cache. `iss-sqlx-prepare-serde-dep` is **fixed**, so the tool works. The dynamic `inline_<tid>` identifier must use runtime `sqlx::query(AssertSqlSafe(...))` (no cache entry) — it cannot be a `query!` (table name is not a bind param).
- **Time-travel safety invariant (from slice 1, preserved):** a `data_file` row is deleted only when `end_snapshot IS NOT NULL AND end_snapshot <= H`, where `H = max(snapshot_id) WHERE snapshot_time < now() - retention`. Live rows (`end_snapshot IS NULL`) are never touched. The metadata drop (inline table + `column`/`table` rows) for a dropped incarnation is additionally gated on its **drop snapshot** `D = table.end_snapshot <= H` — proven invisible to every in-window time-travel read.
- **Commit-then-delete (from slice 1, preserved):** mirror-row deletes commit first; Parquet objects delete after commit. A failed object delete is logged (`tracing::warn!`) and left as a deferred orphan — never re-raised, never a dangling mirror→file reference.
- **Clippy is strict (pedantic + restriction).** Production code must not trip `unwrap_used`/`expect_used`/`indexing_slicing`/`panic`/etc. Use `?` + `.map_err(backend)`. Test code is exempt from the panic-safety lints via `loom_fixture_test`.
- **`gc_table`'s public signature and `GcSummary`'s fields are UNCHANGED.** Dropped-incarnation data-file/Parquet reclaims accumulate into the existing `data_file_rows` / `objects_deleted` counters; the metadata drop is logged via `tracing`. Not changing `GcSummary` keeps the wire (`GcTableResponse`, 3 counters) and the existing struct-literal test assertions compiling unchanged — this is what makes "live behaviour unchanged" true by construction.

---

## File Structure

- **Modify** `src/control-plane/postgres/src/iceberg_gc.rs` — refactor `gc_locked` into `table_id`-parameterized helpers; add the dropped-incarnation reclaim loop; fix the stale module doc comment. This is the bulk of the change.
- **Modify** `src/control-plane/postgres/src/iceberg_mirror.rs` — add `dropped_table_ids(conn, ns, name)` next to `live_table_id`.
- **Modify** `src/control-plane/postgres/tests/iceberg_gc.rs` — add a shared `age_all_snapshots` helper + the drop imports, and 4 new dropped-reclaim tests.
- **Regenerate** `src/control-plane/postgres/.sqlx/` — via `tools/sqlx-prepare.sh` (adds the 3 new static-query cache files).
- **Modify** `docs/ROADMAP.md` / `docs/FUTURE.md` — close `road-iceberg-gc-dropped-table`, record `fut-iceberg-gc-orphan-sweep` (via `loom-docs-update`, in the PR).

---

## Task 1: Behavior-preserving refactor of `gc_locked`

Extract slice-1's per-table reclaim into `table_id`-parameterized helpers and resolve the horizon `H` **before** the live-table lookup, so the same helpers serve both the live incarnation and (Task 2) the dropped incarnations. Pure refactor — no new SQL, no `.sqlx` change, no behavior change. The existing `iceberg-gc` tests are the regression gate.

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_gc.rs` (rewrite `gc_locked`, add private helpers)
- Test (regression only): `src/control-plane/postgres/tests/iceberg_gc.rs` (existing tests, unchanged)

**Interfaces:**
- Consumes: `crate::iceberg_flush::lock_key`, `crate::iceberg_inline::inline_table_name`, `crate::iceberg_mirror::live_table_id`, `crate::iceberg_sql_catalog::SqlCatalog`, `crate::backend`, `control_plane_core::{Result, TableRef}`, `sqlx::{AssertSqlSafe, PgPool, PgConnection}`, `time::OffsetDateTime`.
- Produces (private to `iceberg_gc.rs`, used by Task 2):
  - `async fn reclaimable_paths(pool: &PgPool, tid: i64, h: i64) -> Result<Vec<String>>` — Parquet paths of `tid`'s data files with `end_snapshot <= h`.
  - `async fn delete_data_files(conn: &mut PgConnection, tid: i64, h: i64) -> Result<u64>` — deletes `tid`'s `data_file_column_stat` (child) then `data_file` rows with `end_snapshot <= h`; returns `data_file` rows deleted.
  - `async fn delete_end_capped_inline_rows(conn: &mut PgConnection, tid: i64, h: i64) -> Result<u64>` — deletes end-capped rows (`end_snapshot <= h`) from `inline_<tid>` if the table exists; returns rows deleted.

- [ ] **Step 1: Rewrite `gc_locked` and add the three helpers.**

Replace the body of `gc_locked` (currently `src/control-plane/postgres/src/iceberg_gc.rs:88-199`) and add the helpers below it. The new `gc_locked` resolves `H` first, then the live `tid`, collects paths, deletes rows in one transaction, then deletes Parquet post-commit — identical outcomes to slice 1, just factored:

```rust
async fn gc_locked(
    catalog: &SqlCatalog,
    pool: &PgPool,
    table: &TableRef,
    retention: Duration,
) -> Result<GcSummary> {
    // 1. Horizon H = youngest snapshot fully aged out of the window. Applies to
    //    every incarnation (live and, in the dropped loop, each dropped one).
    //    `now()` is taken in Rust; sub-second precision is irrelevant at GC scale.
    //    `max()` over zero matching rows yields NULL → None → a clean no-op.
    let cutoff = OffsetDateTime::now_utc() - time::Duration::seconds(retention.as_secs() as i64);
    let horizon: Option<i64> = sqlx::query_scalar!(
        "select max(snapshot_id) from iceberg_mirror.snapshot where snapshot_time < $1",
        cutoff,
    )
    .fetch_one(pool)
    .await
    .map_err(backend)?;
    let Some(h) = horizon else {
        return Ok(GcSummary::default());
    };

    // 2. Resolve the (maybe) live incarnation of (schema, name).
    let mut conn = pool.acquire().await.map_err(backend)?;
    let live = live_table_id(&mut conn, &table.schema, &table.name).await?;
    drop(conn);

    // 3. Collect the reclaimable Parquet paths (before deleting the rows that name
    //    them). Live incarnation only in this slice; Task 2 adds the dropped ones.
    let mut paths: Vec<String> = Vec::new();
    if let Some(tid) = live {
        paths.extend(reclaimable_paths(pool, tid, h).await?);
    }

    // 4. One transaction: delete the reclaimable mirror rows.
    let mut tx = pool.begin().await.map_err(backend)?;
    let mut data_file_rows = 0u64;
    let mut inline_rows = 0u64;
    if let Some(tid) = live {
        data_file_rows += delete_data_files(&mut tx, tid, h).await?;
        inline_rows += delete_end_capped_inline_rows(&mut tx, tid, h).await?;
    }
    tx.commit().await.map_err(backend)?;

    // 5. Commit-then-delete: reclaim the Parquet objects. A failed delete is logged
    //    and left as an orphan (never re-raised into a hard error).
    let mut objects_deleted = 0u64;
    for path in &paths {
        match catalog.delete_file(path).await {
            Ok(()) => objects_deleted += 1,
            Err(e) => tracing::warn!(
                error = %e,
                path = %path,
                "gc: failed to delete Parquet object; leaving as orphan"
            ),
        }
    }

    Ok(GcSummary {
        data_file_rows,
        inline_rows,
        objects_deleted,
    })
}

/// Parquet paths of `tid`'s data files reclaimable at horizon `h`
/// (`end_snapshot IS NOT NULL AND end_snapshot <= h`).
async fn reclaimable_paths(pool: &PgPool, tid: i64, h: i64) -> Result<Vec<String>> {
    sqlx::query_scalar!(
        "select path from iceberg_mirror.data_file \
         where table_id = $1 and end_snapshot is not null and end_snapshot <= $2",
        tid,
        h,
    )
    .fetch_all(pool)
    .await
    .map_err(backend)
}

/// Delete `tid`'s reclaimable data files (stats child first, then the rows).
/// Returns the number of `data_file` rows deleted.
async fn delete_data_files(conn: &mut sqlx::PgConnection, tid: i64, h: i64) -> Result<u64> {
    sqlx::query!(
        "delete from iceberg_mirror.data_file_column_stat \
         where data_file_id in ( \
             select data_file_id from iceberg_mirror.data_file \
             where table_id = $1 and end_snapshot is not null and end_snapshot <= $2)",
        tid,
        h,
    )
    .execute(&mut *conn)
    .await
    .map_err(backend)?;
    let rows = sqlx::query!(
        "delete from iceberg_mirror.data_file \
         where table_id = $1 and end_snapshot is not null and end_snapshot <= $2",
        tid,
        h,
    )
    .execute(&mut *conn)
    .await
    .map_err(backend)?
    .rows_affected();
    Ok(rows)
}

/// Delete end-capped inline rows (`end_snapshot <= h`) from `inline_<tid>`, if the
/// physical table exists. Returns the number of rows deleted.
async fn delete_end_capped_inline_rows(
    conn: &mut sqlx::PgConnection,
    tid: i64,
    h: i64,
) -> Result<u64> {
    let inline = inline_table_name(tid);
    let exists: Option<String> = sqlx::query_scalar(AssertSqlSafe("select to_regclass($1)::text"))
        .bind(&inline)
        .fetch_one(&mut *conn)
        .await
        .map_err(backend)?;
    if exists.is_none() {
        return Ok(0);
    }
    let rows = sqlx::query(AssertSqlSafe(format!(
        "delete from {inline} where end_snapshot is not null and end_snapshot <= $1"
    )))
    .bind(h)
    .execute(&mut *conn)
    .await
    .map_err(backend)?
    .rows_affected();
    Ok(rows)
}
```

Add `use sqlx::PgConnection;` is **not** needed — the helpers spell `sqlx::PgConnection` inline (matching the crate style). Keep the existing `use` block; `live_table_id` is already imported.

- [ ] **Step 2: Build the crate.**

Run: `buck2 build //src/control-plane/postgres:postgres 2>&1 | tail -15`
Expected: `BUILD SUCCEEDED`. (No `.sqlx` regen — every `query!` string is byte-identical to slice 1, so the committed cache still matches.)

- [ ] **Step 3: Run the existing GC suite (regression gate).**

Run: `buck2 test //src/control-plane/postgres:iceberg-gc > /tmp/gc1.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/gc1.log`
Expected: all 5 existing tests PASS (`delete_file_removes_object_and_is_idempotent`, `gc_reclaims_aged_data_files_and_keeps_in_window`, `gc_reclaims_aged_inline_rows`, `gc_is_a_noop_when_nothing_aged_out`, `gc_serializes_with_concurrent_flush`). Behavior is unchanged.

- [ ] **Step 4: Commit.**

```bash
git add src/control-plane/postgres/src/iceberg_gc.rs
git commit -m "refactor(iceberg-gc): factor gc_locked into per-table_id reclaim helpers"
```

---

## Task 2: Dropped-incarnation reclaim

Add `dropped_table_ids` and the dropped-reclaim loop. Reuse Task 1's helpers for the data-file/Parquet reclaim; add a metadata-drop leg (physical `DROP TABLE inline_<tid>` + `column`/`table` row delete) gated on the drop snapshot `D <= H`. TDD: the new tests fail first (slice-1 `gc_table` no-ops on dropped tables), then pass.

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_mirror.rs` (add `dropped_table_ids`)
- Modify: `src/control-plane/postgres/src/iceberg_gc.rs` (dropped loop + 3 metadata helpers + doc-comment fix)
- Test: `src/control-plane/postgres/tests/iceberg_gc.rs` (add helper + 4 tests)
- Regenerate: `src/control-plane/postgres/.sqlx/`

**Interfaces:**
- Consumes: Task 1's `reclaimable_paths` / `delete_data_files`; `crate::iceberg_inline::inline_table_name`; `sqlx::{AssertSqlSafe, PgConnection}`.
- Produces:
  - `iceberg_mirror::DroppedIncarnation { pub table_id: i64, pub drop_snapshot: i64 }` (derive `Debug, Clone, Copy, PartialEq, Eq`).
  - `async fn iceberg_mirror::dropped_table_ids(conn: &mut PgConnection, ns: &str, name: &str) -> Result<Vec<DroppedIncarnation>>`.
  - Private in `iceberg_gc.rs`: `async fn drop_inline_table(conn: &mut PgConnection, tid: i64) -> Result<()>`, `async fn delete_column_rows(conn: &mut PgConnection, tid: i64) -> Result<()>`, `async fn delete_table_row(conn: &mut PgConnection, tid: i64) -> Result<()>`.

- [ ] **Step 1: Add the test helper + drop imports to `tests/iceberg_gc.rs`.**

At the top of the file, extend the `iceberg` import (currently `use iceberg::CatalogBuilder;` — line 28 — alongside `use iceberg::io::LocalFsStorageFactory;`) to add the drop machinery:

```rust
use iceberg::io::LocalFsStorageFactory;
use iceberg::{Catalog as _, CatalogBuilder, NamespaceIdent, TableIdent};
```

Add a helper next to `age_snapshot` (after line 112):

```rust
/// Backdate EVERY snapshot so the whole history looks aged out (H = max snapshot id).
async fn age_all_snapshots(pool: &sqlx::PgPool) {
    let old = OffsetDateTime::now_utc() - time::Duration::days(365);
    sqlx::query("update iceberg_mirror.snapshot set snapshot_time = $1")
        .bind(old)
        .execute(pool)
        .await
        .expect("age all snapshots");
}

/// The currently-live `table_id` for `(ns, name)` (fixture-side, before a drop).
/// `iceberg_mirror.table` is spelled unquoted to match the crate's own SQL (the
/// keyword parses fine after the schema qualifier — the committed `.sqlx` cache
/// proves real Postgres accepts it).
async fn live_tid(pool: &sqlx::PgPool, ns: &str, name: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(
        "select table_id from iceberg_mirror.table \
         where table_namespace = $1 and table_name = $2 and end_snapshot is null",
    )
    .bind(ns)
    .bind(name)
    .fetch_one(pool)
    .await
    .expect("live tid")
}

/// Count of `table`/`column`/`data_file` mirror rows for a specific `table_id`
/// (used to assert a dropped incarnation's metadata is fully gone). Returns
/// (table_rows, column_rows, data_file_rows).
async fn mirror_row_counts(pool: &sqlx::PgPool, tid: i64) -> (i64, i64, i64) {
    let t: i64 = sqlx::query_scalar("select count(*) from iceberg_mirror.table where table_id = $1")
        .bind(tid).fetch_one(pool).await.expect("t count");
    let c: i64 = sqlx::query_scalar("select count(*) from iceberg_mirror.column where table_id = $1")
        .bind(tid).fetch_one(pool).await.expect("c count");
    let d: i64 = sqlx::query_scalar("select count(*) from iceberg_mirror.data_file where table_id = $1")
        .bind(tid).fetch_one(pool).await.expect("d count");
    (t, c, d)
}

/// True if the physical `iceberg_mirror.inline_<tid>` table still exists.
async fn inline_table_exists(pool: &sqlx::PgPool, tid: i64) -> bool {
    let name = format!("iceberg_mirror.inline_{tid}");
    let reg: Option<String> = sqlx::query_scalar("select to_regclass($1)::text")
        .bind(&name).fetch_one(pool).await.expect("to_regclass");
    reg.is_some()
}
```

Note: `iceberg_mirror.table` is a reserved word, so the raw-SQL helpers quote it as `"table"`.

- [ ] **Step 2: Write the four failing tests.**

Append to `tests/iceberg_gc.rs`:

```rust
/// Dropped table reclaimed: land a file + inline rows, drop, age the whole history,
/// gc → data-file Parquet deleted, inline_<tid> dropped, table/column/data_file rows
/// gone; the object store no longer holds the file.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_reclaims_a_dropped_table() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let ice = IcebergCatalog::new(pool.clone());
    let t = TableRef { schema: "wh".into(), name: "gone".into() };
    let run = RunId(uuid::Uuid::new_v4());

    let s1 = land(&pool, &catalog, &t, &columns(), &ipc_body(10), 0, i64::MAX,
        lineage(run, "wh", "gone")).await.expect("land");
    let a_path = local_path(&ice.files_with_stats(&t, s1).await.expect("files@s1")[0].path);
    inline_append(&pool, &t, &columns(), &batch(3), lineage(run, "wh", "gone"), None)
        .await.expect("inline_append");
    let tid = live_tid(&pool, "wh", "gone").await;
    assert!(inline_table_exists(&pool, tid).await, "inline table exists before drop");

    let ident = TableIdent::new(NamespaceIdent::new("wh".into()), "gone".into());
    catalog.drop_table(&ident).await.expect("drop");
    age_all_snapshots(&pool).await; // drop snapshot D <= H → full reclaim

    let summary = gc_table(&catalog, &pool, &t, SEVEN_DAYS).await.expect("gc");
    assert!(summary.data_file_rows >= 1 && summary.objects_deleted >= 1,
        "dropped data file reclaimed (got {summary:?})");
    assert!(!a_path.exists(), "dropped table's Parquet deleted");
    assert!(!inline_table_exists(&pool, tid).await, "inline_<tid> dropped");
    assert_eq!(mirror_row_counts(&pool, tid).await, (0, 0, 0),
        "table/column/data_file mirror rows removed");
}

/// Within-window drop preserved: dropping then gc-ing BEFORE the drop snapshot ages
/// out deletes nothing (a time-travel read as-of before the drop still resolves); a
/// later gc after aging completes the reclaim.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_preserves_a_within_window_dropped_table() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let ice = IcebergCatalog::new(pool.clone());
    let t = TableRef { schema: "wh".into(), name: "recent".into() };

    let s1 = land(&pool, &catalog, &t, &columns(), &ipc_body(10), 0, i64::MAX,
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "recent")).await.expect("land");
    let a_path = local_path(&ice.files_with_stats(&t, s1).await.expect("files@s1")[0].path);
    let tid = live_tid(&pool, "wh", "recent").await;

    let ident = TableIdent::new(NamespaceIdent::new("wh".into()), "recent".into());
    catalog.drop_table(&ident).await.expect("drop"); // drop snapshot s2 (recent)
    age_snapshot(&pool, s1.0).await; // H = s1; drop snapshot s2 > H

    let summary = gc_table(&catalog, &pool, &t, SEVEN_DAYS).await.expect("gc");
    assert_eq!(summary, GcSummary::default(), "within-window drop reclaims nothing");
    assert!(a_path.exists(), "Parquet retained while drop is in-window");
    assert_eq!(mirror_row_counts(&pool, tid).await.0, 1,
        "dropped incarnation's table row retained while in-window");

    // A later gc, after the drop snapshot ages out, completes the reclaim.
    age_all_snapshots(&pool).await;
    gc_table(&catalog, &pool, &t, SEVEN_DAYS).await.expect("gc2");
    assert!(!a_path.exists(), "Parquet reclaimed once aged out");
    assert_eq!(mirror_row_counts(&pool, tid).await, (0, 0, 0), "metadata reclaimed once aged out");
}

/// Drop/recreate isolation: create (s,t), drop it, recreate (s,t) with a new table_id,
/// land into the live one, age the whole history, gc → the DROPPED incarnation's bytes
/// are reclaimed while the LIVE incarnation's current files + metadata are untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_isolates_dropped_from_recreated_incarnation() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let ice = IcebergCatalog::new(pool.clone());
    let t = TableRef { schema: "wh".into(), name: "reused".into() };

    // Incarnation 1: land, capture its file + tid, drop.
    let s1 = land(&pool, &catalog, &t, &columns(), &ipc_body(10), 0, i64::MAX,
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "reused")).await.expect("land1");
    let p1 = local_path(&ice.files_with_stats(&t, s1).await.expect("files@s1")[0].path);
    let tid1 = live_tid(&pool, "wh", "reused").await;
    let ident = TableIdent::new(NamespaceIdent::new("wh".into()), "reused".into());
    catalog.drop_table(&ident).await.expect("drop1");

    // Incarnation 2 (live): re-land under the same name → new table_id.
    let s2 = land(&pool, &catalog, &t, &columns(), &ipc_body(5), 0, i64::MAX,
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "reused")).await.expect("land2");
    let p2 = local_path(&ice.files_with_stats(&t, s2).await.expect("files@s2")[0].path);
    let tid2 = live_tid(&pool, "wh", "reused").await;
    assert_ne!(tid1, tid2, "recreate allocates a fresh table_id");

    age_all_snapshots(&pool).await;
    gc_table(&catalog, &pool, &t, SEVEN_DAYS).await.expect("gc");

    // Dropped incarnation reclaimed; live incarnation untouched.
    assert!(!p1.exists(), "dropped incarnation's Parquet reclaimed");
    assert_eq!(mirror_row_counts(&pool, tid1).await, (0, 0, 0), "dropped metadata gone");
    assert!(p2.exists(), "live incarnation's current Parquet retained");
    assert_eq!(mirror_row_counts(&pool, tid2).await.0, 1, "live table row retained");
    let cur = ice.current_snapshot(&t).await.expect("current");
    let rows: i64 = ice.files_with_stats(&t, cur.id).await.expect("files")
        .iter().map(|f| f.record_count).sum();
    assert_eq!(rows, 5, "live incarnation reads back intact");
}

/// Idempotent: a second gc on a fully-reclaimed dropped name is a clean no-op.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_on_fully_reclaimed_dropped_name_is_a_noop() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let t = TableRef { schema: "wh".into(), name: "twice".into() };

    land(&pool, &catalog, &t, &columns(), &ipc_body(10), 0, i64::MAX,
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "twice")).await.expect("land");
    let ident = TableIdent::new(NamespaceIdent::new("wh".into()), "twice".into());
    catalog.drop_table(&ident).await.expect("drop");
    age_all_snapshots(&pool).await;

    let first = gc_table(&catalog, &pool, &t, SEVEN_DAYS).await.expect("gc1");
    assert!(first.data_file_rows >= 1, "first gc reclaims the dropped table");
    let second = gc_table(&catalog, &pool, &t, SEVEN_DAYS).await.expect("gc2");
    assert_eq!(second, GcSummary::default(), "second gc is a clean no-op");
}
```

- [ ] **Step 3: Build + run to confirm the new tests FAIL.**

Run: `buck2 test //src/control-plane/postgres:iceberg-gc > /tmp/gc2.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/gc2.log`
Expected: the 4 new tests FAIL (slice-1 `gc_table` no-ops on dropped names: `gc_reclaims_a_dropped_table` fails at `!a_path.exists()`/`(0,0,0)`; the isolation + idempotent tests fail similarly). The 5 existing tests still PASS. (`gc_preserves_a_within_window_dropped_table` may already pass — it asserts a no-op — which is fine.) If the crate fails to build, fix the test code before proceeding.

- [ ] **Step 4: Add `dropped_table_ids` to `iceberg_mirror.rs`.**

Insert after `live_table_id` (after `src/control-plane/postgres/src/iceberg_mirror.rs:187`):

```rust
/// One dropped incarnation of a `(namespace, name)`: its `table_id` and the snapshot
/// at which it was dropped (`table.end_snapshot`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DroppedIncarnation {
    pub table_id: i64,
    pub drop_snapshot: i64,
}

/// Every DROPPED incarnation of `(ns, name)` — the end-capped `iceberg_mirror.table`
/// rows for the name. The currently-live row (if any) is excluded by construction:
/// a live row has `end_snapshot IS NULL`, and this selects `end_snapshot IS NOT NULL`.
/// A `(ns, name)` maps to several rows across a drop/recreate history; GC reclaims the
/// dead incarnations by iterating these ids under the same horizon `H`.
pub async fn dropped_table_ids(
    conn: &mut PgConnection,
    ns: &str,
    name: &str,
) -> Result<Vec<DroppedIncarnation>> {
    let rows = sqlx::query!(
        "select table_id as \"table_id!\", end_snapshot as \"drop_snapshot!\" \
         from iceberg_mirror.table \
         where table_namespace = $1 and table_name = $2 and end_snapshot is not null",
        ns,
        name,
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(backend)?;
    Ok(rows
        .into_iter()
        .map(|r| DroppedIncarnation {
            table_id: r.table_id,
            drop_snapshot: r.drop_snapshot,
        })
        .collect())
}
```

- [ ] **Step 5: Add the dropped-reclaim loop + metadata helpers to `iceberg_gc.rs`.**

In the `use` block, extend the mirror import:

```rust
use crate::iceberg_mirror::{dropped_table_ids, live_table_id};
```

In `gc_locked`, after resolving `live` (Step 1's Task-1 code, the `let live = ...; drop(conn);` block — reuse the same `conn` before dropping it), also resolve the dropped incarnations:

```rust
    // 2. Resolve the (maybe) live incarnation AND every dropped incarnation.
    let mut conn = pool.acquire().await.map_err(backend)?;
    let live = live_table_id(&mut conn, &table.schema, &table.name).await?;
    let dropped = dropped_table_ids(&mut conn, &table.schema, &table.name).await?;
    drop(conn);
```

In step 3 (path collection), also collect dropped-incarnation paths:

```rust
    let mut paths: Vec<String> = Vec::new();
    if let Some(tid) = live {
        paths.extend(reclaimable_paths(pool, tid, h).await?);
    }
    for inc in &dropped {
        paths.extend(reclaimable_paths(pool, inc.table_id, h).await?);
    }
```

In step 4 (the transaction), after the live-incarnation deletes, add the dropped loop:

```rust
    // Dropped incarnations: always reclaim aged-out data files; drop the physical
    // inline table + metadata rows only once the DROP snapshot itself ages past H
    // (D <= h), so no in-window time-travel read as-of before the drop can reach it.
    for inc in &dropped {
        data_file_rows += delete_data_files(&mut tx, inc.table_id, h).await?;
        if inc.drop_snapshot <= h {
            drop_inline_table(&mut tx, inc.table_id).await?;
            delete_column_rows(&mut tx, inc.table_id).await?;
            delete_table_row(&mut tx, inc.table_id).await?;
            tracing::info!(
                table_id = inc.table_id,
                "gc: fully reclaimed dropped incarnation (dropped inline table + metadata rows)"
            );
        }
    }
```

Add the three metadata helpers below the Task-1 helpers:

```rust
/// Physically drop the per-incarnation inline table `inline_<tid>` (idempotent).
/// Only called once a dropped incarnation is fully reclaimed, so it can never gain a
/// live row again. Postgres DDL is transactional, so this rolls back with the tx.
async fn drop_inline_table(conn: &mut sqlx::PgConnection, tid: i64) -> Result<()> {
    let inline = inline_table_name(tid);
    sqlx::query(AssertSqlSafe(format!("drop table if exists {inline}")))
        .execute(&mut *conn)
        .await
        .map_err(backend)?;
    Ok(())
}

/// Delete every `iceberg_mirror.column` row for `tid` (only after its data files are
/// gone — the FK from `column` to `table` is satisfied because we delete `table` last).
async fn delete_column_rows(conn: &mut sqlx::PgConnection, tid: i64) -> Result<()> {
    sqlx::query!("delete from iceberg_mirror.column where table_id = $1", tid)
        .execute(&mut *conn)
        .await
        .map_err(backend)?;
    Ok(())
}

/// Delete the `iceberg_mirror.table` row for `tid`. Its `column`/`data_file` children
/// must already be gone (delete order: data_file → column → table).
async fn delete_table_row(conn: &mut sqlx::PgConnection, tid: i64) -> Result<()> {
    sqlx::query!("delete from iceberg_mirror.table where table_id = $1", tid)
        .execute(&mut *conn)
        .await
        .map_err(backend)?;
    Ok(())
}
```

- [ ] **Step 6: Fix the stale module doc comment.**

The `## SQL strategy` paragraph (`src/control-plane/postgres/src/iceberg_gc.rs:32-39`) claims the module uses runtime queries; the code uses compile-time `query!` for static SQL. Replace that paragraph with:

```rust
//! ## SQL strategy
//! Static SQL uses the compile-time `sqlx::query!`/`query_scalar!` macros, verified
//! against the committed `.sqlx` cache (regenerated by `tools/sqlx-prepare.sh`). Only
//! the dynamic `inline_<table_id>` identifier — a table name, not a bind parameter —
//! uses a runtime `sqlx::query(AssertSqlSafe(...))`. Every query is exercised against a
//! real schema by the fixture tests in `tests/iceberg_gc.rs`.
```

- [ ] **Step 7: Regenerate the `.sqlx` cache.**

Run: `tools/sqlx-prepare.sh 2>&1 | tail -20`
Expected: it boots the pinned postgres, applies migrations, runs `cargo sqlx prepare`, and writes/updates `src/control-plane/postgres/.sqlx/`. Confirm three new query files appear (the `dropped_table_ids` select, the `column` delete, the `table` delete):
`git status --short src/control-plane/postgres/.sqlx/` should show the new/changed cache files.

- [ ] **Step 8: Build + run the full GC suite → all PASS.**

Run: `buck2 test //src/control-plane/postgres:iceberg-gc > /tmp/gc3.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/gc3.log`
Expected: all 9 tests PASS (5 existing + 4 new).

- [ ] **Step 9: Commit.**

```bash
git add src/control-plane/postgres/src/iceberg_gc.rs \
        src/control-plane/postgres/src/iceberg_mirror.rs \
        src/control-plane/postgres/tests/iceberg_gc.rs \
        src/control-plane/postgres/.sqlx/
git commit -m "feat(iceberg-gc): reclaim dropped-table incarnations (data files + inline table + metadata)"
```

---

## Task 3: Full verification, clippy, and register update

**Files:**
- Modify: `docs/ROADMAP.md`, `docs/FUTURE.md` (via `loom-docs-update`)

- [ ] **Step 1: `.sqlx` freshness check.**

Run: `buck2 test //src/control-plane/postgres:sqlx-cache-check > /tmp/sqlx.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/sqlx.log`
Expected: PASS (the committed cache matches the live schema for every `query!`, including the 3 new ones).

- [ ] **Step 2: Full test sweep of the crate + dependents.**

Run: `buck2 test //src/control-plane/... > /tmp/cp.log 2>&1; grep -E "Tests finished|FAIL" /tmp/cp.log`
Expected: `Tests finished: … 0 fail`. (Confirms `dropped_table_ids`/the mirror change didn't disturb other mirror consumers; the delete-contract + live-tables tests that exercise `mark_dropped` still pass.)

- [ ] **Step 3: Clippy on the touched crate.**

Run: `buck2 build '//src/control-plane/postgres:postgres[clippy.txt]' 2>&1 | tail -5` and confirm the emitted `clippy.txt` is empty (no warnings). If pedantic/restriction fires, fix with `?`/`.map_err`/`#[expect(..., reason = "...")]` — never `unwrap`/`indexing`.

- [ ] **Step 4: Update the documentation registers (in this PR).**

Use the `loom-docs-update` skill to: flip `road-iceberg-gc-dropped-table` to `- [x]` / `status:done` / `pr:#<n>` in `docs/ROADMAP.md`; ensure `fut-iceberg-gc-orphan-sweep` is recorded as `status:deferred` in `docs/FUTURE.md` (the spec's out-of-scope orphan sweep). Then `bash tools/docs.sh validate`.

- [ ] **Step 5: Commit the register update.**

```bash
git add docs/ROADMAP.md docs/FUTURE.md
git commit -m "docs(registers): close road-iceberg-gc-dropped-table"
```

---

## Self-Review notes (author)

- **Spec coverage:** dropped-`table_id` resolution → `dropped_table_ids` (Task 2 Step 4). Dropped-incarnation reclaim loop → Task 2 Step 5. Physical `DROP TABLE inline_<tid>` + `table`/`column` delete gated on full reclaim → `drop_inline_table`/`delete_column_rows`/`delete_table_row`, gated on `inc.drop_snapshot <= h`. Reuse of horizon/lock/`delete_file`/RPC (no new surface) → `gc_locked`/`gc_table` unchanged in signature; horizon+lock reused; helpers reuse existing SQL. Tests 1–5 of the spec → `gc_reclaims_a_dropped_table`, `gc_preserves_a_within_window_dropped_table`, `gc_isolates_dropped_from_recreated_incarnation`, existing suite (test 4 = "live unchanged"), `gc_on_fully_reclaimed_dropped_name_is_a_noop`.
- **Full-reclaim gate is the drop snapshot `D <= H`, not "no data_file rows remain":** the latter mis-fires for inline-only or pre-drop-overwritten incarnations (metadata dropped while an in-window read as-of before the drop should still resolve). Gating on `table.end_snapshot` is the precise, time-travel-safe condition and matches the spec's parenthetical intent.
- **Type consistency:** `DroppedIncarnation { table_id: i64, drop_snapshot: i64 }` used verbatim in `dropped_table_ids` and the loop; helper signatures take `&mut sqlx::PgConnection` and callers pass `&mut tx`; `h: i64`, `tid: i64` throughout.
- **No placeholders:** every code step shows full code; every run step shows the command + expected result.
