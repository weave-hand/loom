# Engine-wire Compaction Job Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make compaction a queue-driven job in the engine-wire model: an operator HTTP endpoint enqueues a `compact_table` job; a zero-pool worker streams the small files over Arrow Flight, rewrites them coalesced to object store, and commits the swap over a new `EngineControl::CompactTable` RPC — the engine commits to Iceberg, the worker computes, time travel preserved.

**Architecture:** Two parts in one slice. **Part A** builds the missing Iceberg subset-expire commit primitive (today `IcebergTx::compact_files` is an error stub; the engine-wire stack is Iceberg-backed, mirror-only). **Part B** wires the job: `CompactJob` type, the operator enqueue endpoint, two new gRPC RPCs (`ListFiles` for metadata, `CompactTable` for the commit), a postgres-free object-store config crate the zero-pool worker can depend on, and the worker dispatch branch (ListFiles → pick small → Flight read → coalesce → CompactTable).

**Tech Stack:** Rust (edition 2024), buck2, tonic 0.14 + prost (gRPC over UDS), arrow / arrow-flight / parquet 58, DataFusion (`datafusion_io::write_dataset`), Iceberg (mirror projection in `iceberg_mirror.*`), sqlx 0.9 compile-time queries, axum (ingest HTTP), `loom_fixture_test` (hermetic Postgres + object store).

## Global Constraints

- **Tests are `rust_test` integration targets only** — never inline `#[cfg(test)]`/`#[test]`. The `no-inline-tests` prek hook fails otherwise. Put each test in a sibling `tests/<name>.rs` wired as its own target in the crate's `BUCK`.
- **Fixture-backed tests (hermetic Postgres/DuckDB/object store) MUST use `loom_fixture_test`** (`load("//src/control-plane/postgres:defs.bzl", "loom_fixture_test")`), not a bare `rust_test`, or they route to RE and fail as root.
- **New Postgres SQL uses compile-time `sqlx::query!`/`query_scalar!`.** After changing any SQL, run `tools/sqlx-prepare.sh` and commit the `src/control-plane/postgres/.sqlx/` change. The `//src/control-plane/postgres:sqlx-cache-check` test enforces freshness in the normal sweep.
- **`//src/services/worker:worker-bin` MUST stay postgres-free.** Guard: `buck2 uquery "deps(//src/services/worker:worker-bin)" | grep -i "control-plane/postgres"` must be empty. The worker may depend only on postgres-free crates (`control-plane/core`, `control-plane/worker`, `engine-wire`, `services/transform`, `services/datafusion-io`, the new `services/store-config`, and third-party).
- **One arrow major (58) everywhere.** No `arrow-*-57`/`parquet-57`. `arrow-flight` is `//third-party:arrow-flight` (58.3.0), tonic 0.14.
- **No new third-party crates** are required (serde, arrow, object_store, tonic, prost are all already vendored). Do **not** run `reindeer update`/`cargo generate-lockfile` — no manifest changes are needed, so the `duckdb 1.10503.1` pin is untouched.
- **Conventional Commits** for every commit message (the `commit-msg` hook enforces it). Frequent commits — one per task minimum.
- **Markdown lint:** any `.md` you touch ends with exactly one trailing newline, no trailing whitespace.
- **Acceptance gate:** `buck2 build //src/...` and `buck2 test //src/...` green; the existing direct-call `transform::compact_table` primitive and the flush vertical unchanged.

**Don't pipe `buck2 test`/`buck2 bxl` through `tail`/`head`** — redirect to a file and grep: `buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`.

## Key facts (ground truth from exploration)

- **Layer 1 (Arrow Flight data plane) already exists** (shipped in `road-engine-wire-flight`). Reuse, do not rebuild:
  - `engine_wire::flight::FlightTicket { schema: String, name: String, files: Vec<String> }` with `encode()`/`decode()` (JSON).
  - `engine_wire::flight::FlightTableClient::connect(socket) -> Result<Self>` and `fetch(&self, ticket: FlightTicket) -> Result<Vec<RecordBatch>>` (schema is carried by the stream; derive from `batches[0].schema()`).
  - Engine `FlightDataService::do_get` already serves the file-ticket branch via `read_files_as_batches(&catalog, &table, &req.files)`; `engine/src/main.rs` already serves `FlightServiceServer` alongside `EngineControlServer` on the same UDS.
- **The engine-wire stack is Iceberg-backed and mirror-only.** loom-governed reads (incl. the Flight file-ticket read) resolve through `iceberg_mirror.*`. `register_files` projects into the mirror; there is **no** real Iceberg `fast_append` on this path (the accepted `iss-iceberg-inline-visibility` gap). Therefore the compaction commit is a Postgres-only mirror mutation.
- **Mirror paths are ABSOLUTE** (`file://…` / `s3://…`). Flush/landing register `df.file_path().to_string()` with `path_is_relative: false`. The worker must therefore register the coalesced files with **absolute** paths, and pass the small files' absolute paths (as returned by `ListFiles`) in the expire list.
- `IcebergTx::compact_files` is currently: `Err(ControlPlaneError::Backend("IcebergTx::compact_files is unsupported (deferred, fut-iceberg-gc)".into()))` at `src/control-plane/postgres/src/iceberg_control_plane.rs`.
- `service_runtime` (crate `service_runtime`, dir `src/services/runtime/`) **depends on `//src/control-plane/postgres`** — the worker cannot depend on it. Its object-store config (`ObjectStoreConfig`, `ObjectStoreBackend`, `S3Backend`, `ObjectStoreConfig::parse`, `local_store`, `build_serving_object_store`) is itself postgres-free and must be extracted into a new `store-config` crate.
- `service_runtime::control_plane(pool: PgPool, lock_timeout: Duration) -> PgControlPlane` (impls `ControlPlane`; `cp.queue()` returns `&dyn Queue` with `enqueue`).

## File Structure

**Part A (Iceberg primitive) — `src/control-plane/postgres/`:**
- Modify `src/iceberg_mirror.rs`: add `end_cap_files_by_path` (conflict-guarded subset end-cap).
- Modify `src/iceberg_landing.rs`: add `WriteMode::Compact { expire_paths }`; handle it in `register_files` (subset end-cap + project, skip schema reconcile).
- Create `src/iceberg_compact.rs`: `compact_table(pool, table, expire, write) -> Result<Option<SnapshotId>>` (the engine entry).
- Modify `src/lib.rs`: `pub mod iceberg_compact;` + re-export.
- Modify `src/iceberg_control_plane.rs`: un-stub `IcebergTx::compact_files` (stage + commit via the same machinery).
- Tests: `tests/iceberg_compact.rs`, `tests/iceberg_tx_compact.rs`.

**Part B (job/wire):**
- `src/control-plane/core/`: create `src/compact_job.rs` (`COMPACT_JOB_KIND`, `CompactJob`); modify `src/snapshot.rs` (add serde derives); modify `src/lib.rs` (exports). Test `tests/compact_job.rs`.
- `src/services/engine-wire/`: modify `proto/engine_control.proto` (+`ListFiles`, +`CompactTable`); modify `src/client.rs` (client methods); modify `src/convert.rs` if present. Test `tests/compact_rpc.rs`.
- `src/services/engine/`: modify `src/service.rs` (handlers). Test extends `tests/wire.rs` (or new `tests/compact_wire.rs`).
- `src/services/ingest/`: modify `src/http.rs` (AppState + endpoint), `src/main.rs` (build cp). Test `tests/compact_endpoint.rs`.
- `src/services/store-config/` (NEW crate): `BUCK`, `src/lib.rs`. Modify `src/services/runtime/` to depend on + re-export it. Test `tests/parse.rs`.
- `src/services/worker/`: create `src/compact.rs`; modify `src/lib.rs`, `src/main.rs`, `BUCK`. Test `tests/compact_e2e.rs`.

---

## Task A1: Iceberg subset-expire commit machinery + engine entry function

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_mirror.rs`
- Modify: `src/control-plane/postgres/src/iceberg_landing.rs` (`WriteMode` enum ~lines 399-408; `register_files` ~lines 421-439)
- Create: `src/control-plane/postgres/src/iceberg_compact.rs`
- Modify: `src/control-plane/postgres/src/lib.rs`
- Test: `src/control-plane/postgres/tests/iceberg_compact.rs` (new `loom_fixture_test`)
- Regenerate: `src/control-plane/postgres/.sqlx/`

**Interfaces:**
- Produces:
  - `iceberg_mirror::end_cap_files_by_path(conn: &mut PgConnection, table_id: i64, paths: &[String], at: SnapshotId) -> Result<()>` — end-caps the named live files; returns `ControlPlaneError::Conflict` if any path is not currently live.
  - `iceberg_landing::WriteMode::Compact { expire_paths: Vec<String> }` — new variant.
  - `iceberg_compact::compact_table(pool: &PgPool, table: &TableRef, expire: &[String], write: &[DataFile]) -> Result<Option<SnapshotId>>` — reads current snapshot (`NotFound` ⇒ `Ok(None)`), allocates one snapshot, subset-expires `expire`, projects `write`, commits. Used by the engine `CompactTable` handler (Task B3) and the `IcebergTx` seam (Task A2).
- Consumes (existing): `iceberg_mirror::{next_snapshot, ensure_table, project_files, stamp_schema_version, projected_files? }`, `iceberg_catalog::IcebergCatalog::{new, current_snapshot}`, `crate::backend`, `control_plane_core::{DataFile, SnapshotId, TableRef, ControlPlaneError, Result}`.

- [ ] **Step 1: Write the failing test** — `src/control-plane/postgres/tests/iceberg_compact.rs`

Mirror `tests/iceberg_overwrite.rs` for fixture/seed setup — copy its `use`-imports and the test-local helpers (`make_catalog`, `columns`, `batch`, `ipc_body`, `lineage`) verbatim from that file's top, adjusting only the test bodies. Note `land` is **not** a test-local helper: it is a crate export `control_plane_postgres::iceberg_landing::land` that `iceberg_overwrite.rs` imports — bring it in via the same `use`. Then:

```rust
//! Iceberg subset-expire compaction commit (`iceberg_compact::compact_table`): expire
//! a subset of live files + register coalesced files at one snapshot, preserving time
//! travel; conflict on a non-live expire path; NotFound -> None.

// ... (copy use-imports + make_catalog/land/columns/batch/ipc_body/lineage from tests/iceberg_overwrite.rs)

use control_plane_core::{ControlPlaneError, DataFile, FileFormat, TableRef};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_compact::compact_table;

fn syn_file(path: &str, rows: i64) -> DataFile {
    DataFile {
        path: path.into(),
        path_is_relative: false,
        file_format: FileFormat::Parquet,
        record_count: rows,
        file_size_bytes: rows * 16,
        column_stats: vec![],
        parquet_footer_size: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compact_expires_subset_and_preserves_time_travel() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let ice = IcebergCatalog::new(pool.clone());
    let t = TableRef { schema: "wh".into(), name: "t".into() };

    // Land three real files (a:10, b:5, c:2) so the mirror has three live rows.
    let s1 = land(&pool, &catalog, &t, &columns(), &ipc_body(10), 0, i64::MAX,
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "t")).await.expect("a");
    land(&pool, &catalog, &t, &columns(), &ipc_body(5), 0, i64::MAX,
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "t")).await.expect("b");
    let before = land(&pool, &catalog, &t, &columns(), &ipc_body(2), 0, i64::MAX,
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "t")).await.expect("c");

    let live = ice.files_with_stats(&t, before).await.expect("live");
    assert_eq!(live.len(), 3);
    // Expire the two smallest (b,c by record_count); coalesce into one synthetic file.
    let mut by_rows = live.clone();
    by_rows.sort_by_key(|f| f.record_count);
    let expire: Vec<String> = by_rows[..2].iter().map(|f| f.path.clone()).collect();
    let coalesced_rows: i64 = by_rows[..2].iter().map(|f| f.record_count).sum();
    let new = vec![syn_file(&format!("{}/wh/t/compact-1/part-0.parquet", wh.path().display()), coalesced_rows)];

    let snap = compact_table(&pool, &t, &expire, &new).await.expect("compact").expect("snapshot");
    assert!(snap.0 > before.0, "compaction advances the snapshot");

    // Current: the untouched large file + the coalesced file; row total preserved.
    let now = ice.files_with_stats(&t, snap).await.expect("now");
    assert_eq!(now.len(), 2, "one untouched + one coalesced");
    let total: i64 = now.iter().map(|f| f.record_count).sum();
    assert_eq!(total, 17, "10 + (5+2) preserved");

    // Time travel: the prior snapshot still lists all three originals.
    let back = ice.files_with_stats(&t, before).await.expect("back");
    assert_eq!(back.len(), 3, "prior snapshot retains the three originals");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compact_conflicts_on_non_live_expire_path() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let t = TableRef { schema: "wh".into(), name: "t".into() };
    land(&pool, &catalog, &t, &columns(), &ipc_body(3), 0, i64::MAX,
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "t")).await.expect("a");

    let err = compact_table(&pool, &t, &["does/not/exist.parquet".into()],
        &[syn_file(&format!("{}/x.parquet", wh.path().display()), 3)]).await.unwrap_err();
    assert!(matches!(err, ControlPlaneError::Conflict(_)), "non-live expire path conflicts, got {err:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compact_missing_table_is_none() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let t = TableRef { schema: "wh".into(), name: "ghost".into() };
    let out = compact_table(&pool, &t, &[], &[]).await.expect("ok");
    assert!(out.is_none(), "no snapshot for a table that was never written");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_concurrent_compactions_race_exactly_one_commits() {
    // The spec's real conflict case: two compactions target the SAME live small-file
    // set; exactly one commits, the other gets Conflict (then a re-run converges).
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let ice = IcebergCatalog::new(pool.clone());
    let t = TableRef { schema: "wh".into(), name: "t".into() };

    land(&pool, &catalog, &t, &columns(), &ipc_body(3), 0, i64::MAX,
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "t")).await.expect("a");
    let before = land(&pool, &catalog, &t, &columns(), &ipc_body(2), 0, i64::MAX,
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "t")).await.expect("b");
    let live = ice.files_with_stats(&t, before).await.expect("live");
    let expire: Vec<String> = live.iter().map(|f| f.path.clone()).collect();
    let total: i64 = live.iter().map(|f| f.record_count).sum();

    // Two compactions of the SAME expire set, each adding a distinct coalesced file.
    let mk = |p: &str| vec![syn_file(&format!("{}/wh/t/{p}/part-0.parquet", wh.path().display()), total)];
    let (p1, p2) = (pool.clone(), pool.clone());
    let (e1, e2) = (expire.clone(), expire.clone());
    let (t1, t2) = (t.clone(), t.clone());
    let (f1, f2) = (mk("c1"), mk("c2"));
    let (r1, r2) = tokio::join!(
        async move { compact_table(&p1, &t1, &e1, &f1).await },
        async move { compact_table(&p2, &t2, &e2, &f2).await },
    );
    let oks = [&r1, &r2].iter().filter(|r| matches!(r, Ok(Some(_)))).count();
    let conflicts = [&r1, &r2].iter()
        .filter(|r| matches!(r, Err(ControlPlaneError::Conflict(_)))).count();
    assert_eq!(oks, 1, "exactly one compaction commits: {r1:?} / {r2:?}");
    assert_eq!(conflicts, 1, "the loser gets Conflict: {r1:?} / {r2:?}");

    // The winner's coalesced file is the sole live file; row set preserved.
    let head = ice.current_snapshot(&t).await.expect("head").id;
    let now = ice.files_with_stats(&t, head).await.expect("now");
    assert_eq!(now.len(), 1, "exactly one coalesced file is live after the race");
    assert_eq!(now.iter().map(|f| f.record_count).sum::<i64>(), total);

    // A re-run of the loser now converges to a clean no-op (only one live file left,
    // and its path is not in the stale expire set -> nothing to expire -> Conflict-free
    // path is moot; the worker would compute a fresh small set and no-op). Assert a
    // fresh compaction with the CURRENT live set is a no-op-equivalent single commit.
    // (The worker's <2-small-files no-op is covered in the worker e2e; here we only
    // assert the race left the table consistent.)
}
```

Note for Step 3 implementation: `stamp_schema_version` (`iceberg_mirror.rs`) must stamp the new snapshot's schema version by carrying forward the table's live schema version, NOT by depending on a just-run `reconcile_and_project`. Confirm this when wiring the Compact arm; if it requires a projected column set, call `reconcile_and_project(conn, tid, at, &[])` first (a no-op for an unchanged schema) before `stamp_schema_version`.

Wire the target in `src/control-plane/postgres/BUCK` (mirror the existing `iceberg-overwrite` `loom_fixture_test` target — copy its `deps`, set `name="iceberg-compact"`, `crate="iceberg_compact"`, `srcs=["tests/iceberg_compact.rs"]`, `crate_root="tests/iceberg_compact.rs"`).

- [ ] **Step 2: Run the test to verify it fails**

```
buck2 test //src/control-plane/postgres:iceberg-compact > /tmp/t.log 2>&1; grep -E "error\[|cannot find|Tests finished|FAIL" /tmp/t.log
```
Expected: build failure — `iceberg_compact` module / `compact_table` does not exist yet.

- [ ] **Step 3: Add `end_cap_files_by_path` to `iceberg_mirror.rs`**

Place next to `end_cap_live_data_files`. Use a compile-time `query!` with `RETURNING` and verify the matched count equals the (de-duplicated) request, else `Conflict` — mirroring DuckLake `compact_files`' conflict guard.

```rust
/// End-cap (set `end_snapshot = at`) the specific live `iceberg_mirror.data_file`
/// rows named by `paths` for `table_id` — the subset-expire leg of compaction
/// (`Tx::compact_files`' Iceberg twin). Prior snapshots still time-travel (the rows
/// keep `begin_snapshot < at`). Returns `ControlPlaneError::Conflict` if any path is
/// not currently live (a concurrent compaction already superseded it), so a raced
/// compaction fails rather than silently dropping files. `paths` is de-duplicated
/// before the count check.
pub async fn end_cap_files_by_path(
    conn: &mut PgConnection,
    table_id: i64,
    paths: &[String],
    at: SnapshotId,
) -> Result<()> {
    if paths.is_empty() {
        return Ok(());
    }
    let mut unique: Vec<String> = paths.to_vec();
    unique.sort();
    unique.dedup();
    let capped = sqlx::query_scalar!(
        "update iceberg_mirror.data_file set end_snapshot = $2 \
         where table_id = $1 and path = any($3) and end_snapshot is null \
         returning path",
        table_id,
        at.0,
        &unique[..],
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(backend)?;
    if capped.len() != unique.len() {
        return Err(ControlPlaneError::Conflict(format!(
            "compact: {} of {} expire paths were not live (raced compaction)",
            unique.len() - capped.len(),
            unique.len()
        )));
    }
    Ok(())
}
```

Confirm the imports at the top of `iceberg_mirror.rs` include `control_plane_core::ControlPlaneError` (add it to the existing `use control_plane_core::{...}` if absent).

- [ ] **Step 4: Add the `WriteMode::Compact` arm to `register_files` in `iceberg_landing.rs`**

Extend the `WriteMode` enum:

```rust
#[derive(Clone)]
pub enum WriteMode {
    Append,
    Overwrite,
    /// Expire the specific live files named by `expire_paths`, then add `files`, at
    /// the new snapshot. Schema-invariant (compaction never changes columns), so it
    /// skips *column* reconciliation (`reconcile_and_project`) — callers pass `&[]`
    /// for `columns` — but still stamps the snapshot's schema version
    /// (`stamp_schema_version`), since `current_snapshot` reads a `schema_version`
    /// for every snapshot row.
    Compact { expire_paths: Vec<String> },
}
```

(`WriteMode` was `#[derive(Clone, Copy)]`; `Compact` holds a `Vec`, so drop `Copy` and make it `Clone`. Fix the two existing `*mode` copies in `iceberg_control_plane.rs::commit` to `mode.clone()` — Task A2 touches that file anyway; if A2 runs after, do the `.clone()` fix here to keep the build green.)

In `register_files`, replace the single `if let WriteMode::Overwrite` block with a match that keeps Append/Overwrite behavior and adds Compact:

```rust
let tid = ensure_table(conn, &table.schema, &table.name, at).await?;
match &mode {
    WriteMode::Append => {}
    WriteMode::Overwrite => {
        end_cap_live_data_files(conn, tid, at).await?;
    }
    WriteMode::Compact { expire_paths } => {
        // Subset-expire the named files; project the new ones below. Schema is
        // unchanged, so skip reconcile_and_project (it would require `columns`).
        crate::iceberg_mirror::end_cap_files_by_path(conn, tid, expire_paths, at).await?;
        project_files(conn, tid, at, &projected_files(files)?).await?;
        stamp_schema_version(conn, tid, at).await?;
        return Ok(());
    }
}
reconcile_and_project(conn, tid, at, &projected_columns(columns)?).await?;
project_files(conn, tid, at, &projected_files(files)?).await?;
stamp_schema_version(conn, tid, at).await?;
Ok(())
```

(Import `end_cap_files_by_path` or call fully-qualified as shown.)

- [ ] **Step 5: Create `iceberg_compact.rs` (engine entry function)**

```rust
//! Iceberg subset-expire compaction commit — the engine-callable entry the
//! `EngineControl::CompactTable` RPC and `IcebergTx::compact_files` share. Mirrors
//! `iceberg_flush::flush_table`'s shape (read current snapshot, one Postgres tx,
//! mirror-only), but expires a *subset* of live files instead of end-capping all of
//! them, and registers the caller's already-written coalesced Parquet. No lineage
//! (physical reorganization). Time travel preserved: expired rows keep
//! `begin_snapshot < at`.

use control_plane_core::{ControlPlaneError, DataFile, Result, SnapshotId, TableRef};
use sqlx::PgPool;

use crate::backend;
use crate::iceberg_catalog::IcebergCatalog;
use crate::iceberg_landing::{WriteMode, register_files};
use crate::iceberg_mirror::next_snapshot;

/// Compact `table`: at one new snapshot, expire the live files named by `expire`
/// (their absolute mirror paths) and register `write` (already written to object
/// store, absolute paths). Returns the new snapshot id, or `Ok(None)` if the table
/// was never written (no current snapshot). `Conflict` if any `expire` path is no
/// longer live (raced compaction).
pub async fn compact_table(
    pool: &PgPool,
    table: &TableRef,
    expire: &[String],
    write: &[DataFile],
) -> Result<Option<SnapshotId>> {
    let ice = IcebergCatalog::new(pool.clone());
    match ice.current_snapshot(table).await {
        Ok(_) => {}
        Err(ControlPlaneError::NotFound(_)) => return Ok(None),
        Err(e) => return Err(e),
    };

    let mut tx = pool.begin().await.map_err(backend)?;
    let at = next_snapshot(&mut tx, None).await?;
    // columns unused for Compact (schema-invariant) — pass &[].
    register_files(
        &mut tx,
        table,
        &[],
        write,
        WriteMode::Compact { expire_paths: expire.to_vec() },
        at,
    )
    .await?;
    tx.commit().await.map_err(backend)?;
    Ok(Some(at))
}
```

Add to `src/control-plane/postgres/src/lib.rs`: `pub mod iceberg_compact;` (next to `pub mod iceberg_flush;`).

- [ ] **Step 6: Regenerate the sqlx cache**

```
bash tools/sqlx-prepare.sh > /tmp/sqlx.log 2>&1; tail -5 /tmp/sqlx.log
git add src/control-plane/postgres/.sqlx
```

- [ ] **Step 7: Run the test to verify it passes**

```
buck2 test //src/control-plane/postgres:iceberg-compact //src/control-plane/postgres:sqlx-cache-check > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```
Expected: PASS (3 tests + cache-check green).

- [ ] **Step 8: Commit**

```
git add src/control-plane/postgres
git commit -m "feat(iceberg): subset-expire compaction commit primitive (iceberg_compact::compact_table)"
```

---

## Task A2: Un-stub `IcebergTx::compact_files` (polymorphic seam)

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_control_plane.rs`
- Test: `src/control-plane/postgres/tests/iceberg_tx_compact.rs` (new `loom_fixture_test`)

**Interfaces:**
- Consumes: `iceberg_landing::{WriteMode, register_files}`, `iceberg_mirror::next_snapshot` (already in scope in this file).
- Produces: a working `IcebergTx::compact_files` (replaces the error stub), so `cp.begin() → compact_files → commit` works on the Iceberg adapter.

**Interface note (the IcebergTx commit):** `IcebergTx` has `staged_creates: Vec<(TableRef, Vec<ColumnSpec>)>` and `staged_files: Vec<(TableRef, Vec<DataFile>, WriteMode)>`. `commit` iterates `staged_files` calling `register_files(&mut tx, table, cols, files, mode, at)` and looks `cols` up from `staged_creates` (erroring if absent). For `Compact` mode, `register_files` ignores `columns`, so a compaction with no preceding `create_table` must not hit that error path — stage compactions in a **separate** vector that the commit registers with `&[]` columns.

- [ ] **Step 1: Write the failing test** — `tests/iceberg_tx_compact.rs`

Copy fixture/seed helpers from `tests/iceberg_overwrite.rs`. The test drives the polymorphic `ControlPlane` seam (build an `IcebergControlPlane` exactly as the existing Iceberg-adapter tests do — search `tests/iceberg_overwrite.rs` / `make_catalog` usage; if an Iceberg adapter test already constructs `IcebergControlPlane::new(pg, catalog)`, mirror it):

```rust
//! IcebergTx::compact_files over the polymorphic ControlPlane seam: stage + commit
//! expires a subset and adds coalesced files at one snapshot, time travel preserved.

use control_plane_core::{ControlPlane, DataFile, FileFormat, TableRef};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_control_plane::IcebergControlPlane;

// ... copy make_catalog/land/columns/ipc_body/lineage + PgFixture imports ...

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn iceberg_tx_compact_files_swaps_subset() {
    let fx = PgFixture::start();
    let (pgcp, db) = fx.fresh_db().await;            // pgcp: PgControlPlane
    let wh = tempfile::tempdir().expect("wh");
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let ice_cat = IcebergCatalog::new(pool.clone());
    let t = TableRef { schema: "wh".into(), name: "t".into() };

    land(&pool, &catalog, &t, &columns(), &ipc_body(10), 0, i64::MAX,
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "t")).await.expect("a");
    let before = land(&pool, &catalog, &t, &columns(), &ipc_body(2), 0, i64::MAX,
        lineage(RunId(uuid::Uuid::new_v4()), "wh", "t")).await.expect("b");
    let live = ice_cat.files_with_stats(&t, before).await.expect("live");
    let small = live.iter().min_by_key(|f| f.record_count).unwrap();
    let expire = vec![small.path.clone()];
    let new = vec![DataFile {
        path: format!("{}/wh/t/c/part-0.parquet", wh.path().display()),
        path_is_relative: false, file_format: FileFormat::Parquet,
        record_count: small.record_count, file_size_bytes: 99,
        column_stats: vec![], parquet_footer_size: None,
    }];

    let cp = IcebergControlPlane::new(pgcp, catalog);
    let mut tx = cp.begin().await.expect("begin");
    tx.compact_files(&t, &expire, &new).await.expect("stage");
    let snap = tx.commit().await.expect("commit").expect("snapshot");

    let now = ice_cat.files_with_stats(&t, snap).await.expect("now");
    assert_eq!(now.len(), 2, "untouched + coalesced");
    let back = ice_cat.files_with_stats(&t, before).await.expect("back");
    assert_eq!(back.len(), 2, "prior snapshot intact");
}
```

Wire the `loom_fixture_test` target `iceberg-tx-compact` in `BUCK` (copy `iceberg-compact`'s deps).

- [ ] **Step 2: Run to verify it fails**

```
buck2 test //src/control-plane/postgres:iceberg-tx-compact > /tmp/t.log 2>&1; grep -E "unsupported|assertion|Tests finished|FAIL" /tmp/t.log
```
Expected: FAIL — `compact_files` returns the `unsupported` Backend error.

- [ ] **Step 3: Add `staged_compacts` to `IcebergTx` and implement `compact_files` + commit**

In `iceberg_control_plane.rs`:
- Add field: `staged_compacts: Vec<(TableRef, Vec<String>, Vec<DataFile>)>,` to `struct IcebergTx`.
- In `begin`, initialize `staged_compacts: Vec::new(),`.
- Replace the `compact_files` stub body:

```rust
async fn compact_files(
    &mut self,
    table: &TableRef,
    expire: &[String],
    write: &[DataFile],
) -> Result<()> {
    self.staged_compacts
        .push((table.clone(), expire.to_vec(), write.to_vec()));
    Ok(())
}
```

- In `commit`, destructure `staged_compacts` alongside the others, include it in the early `is_empty()` short-circuit (`staged_creates.is_empty() && staged_files.is_empty() && staged_compacts.is_empty()`), and after the `staged_files` loop register the compactions (columns unused → `&[]`):

```rust
for (table, expire, write) in &staged_compacts {
    register_files(
        &mut tx,
        table,
        &[],
        write,
        WriteMode::Compact { expire_paths: expire.clone() },
        at,
    )
    .await?;
}
```

- Fix the existing `*mode` in the `staged_files` loop to `mode.clone()` (WriteMode lost `Copy` in A1).

- [ ] **Step 4: Run to verify it passes**

```
buck2 test //src/control-plane/postgres:iceberg-tx-compact > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```
Expected: PASS.

- [ ] **Step 5: Commit**

```
git add src/control-plane/postgres
git commit -m "feat(iceberg): implement IcebergTx::compact_files via the subset-expire machinery"
```

---

## Task B1: `CompactJob` core type + serde on catalog file types

**Files:**
- Create: `src/control-plane/core/src/compact_job.rs`
- Modify: `src/control-plane/core/src/snapshot.rs` (add serde derives to `DataFile`, `ColumnStat`, `StatValue`, `FileFormat`)
- Modify: `src/control-plane/core/src/lib.rs` (module + re-exports)
- Test: `src/control-plane/core/tests/compact_job.rs` (`rust_test`)

**Interfaces:**
- Produces:
  - `control_plane_core::COMPACT_JOB_KIND: &str = "compact_table"`.
  - `control_plane_core::CompactJob { pub schema: String, pub name: String }` (serde).
  - `serde::{Serialize, Deserialize}` on `DataFile`, `ColumnStat`, `StatValue`, `FileFormat` (so `DataFile` crosses the gRPC wire as JSON in Task B2/B3).

- [ ] **Step 1: Write the failing test** — `tests/compact_job.rs`

```rust
use control_plane_core::{
    ColumnStat, CompactJob, DataFile, FileFormat, StatValue, COMPACT_JOB_KIND,
};

#[test]
fn compact_job_round_trips_json() {
    assert_eq!(COMPACT_JOB_KIND, "compact_table");
    let j = CompactJob { schema: "main".into(), name: "orders".into() };
    let v = serde_json::to_value(&j).unwrap();
    let back: CompactJob = serde_json::from_value(v).unwrap();
    assert_eq!(back.schema, "main");
    assert_eq!(back.name, "orders");
}

#[test]
fn data_file_round_trips_json() {
    let f = DataFile {
        path: "s3://b/main/orders/c/part-0.parquet".into(),
        path_is_relative: false,
        file_format: FileFormat::Parquet,
        record_count: 7,
        file_size_bytes: 1234,
        column_stats: vec![ColumnStat {
            column_name: "id".into(),
            null_count: 0,
            column_size_bytes: 64,
            min: Some(StatValue::I64(1)),
            max: Some(StatValue::I64(7)),
        }],
        parquet_footer_size: Some(40),
    };
    let s = serde_json::to_string(&f).unwrap();
    let back: DataFile = serde_json::from_str(&s).unwrap();
    assert_eq!(back, f);
}
```

Wire a `rust_test` target `compact-job` in `src/control-plane/core/BUCK` (mirror an existing core test target, e.g. `page`; deps `[":core", "//third-party:serde_json"]`).

- [ ] **Step 2: Run to verify it fails**

```
buck2 test //src/control-plane/core:compact-job > /tmp/t.log 2>&1; grep -E "cannot find|error\[|Tests finished|FAIL" /tmp/t.log
```
Expected: FAIL — `CompactJob`/`COMPACT_JOB_KIND` missing; `DataFile` not `Serialize`.

- [ ] **Step 3: Create `compact_job.rs`**

```rust
//! The compact-table job contract, shared by the producer (the operator ingest
//! endpoint) and the consumer (the zero-pool worker). Lives in core so a zero-pool
//! worker can read it without the postgres adapter. Mirrors `flush.rs`.

/// The queue `kind` for an operator-triggered compaction job.
pub const COMPACT_JOB_KIND: &str = "compact_table";

/// The payload of a `compact_table` job: which table to compact.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct CompactJob {
    pub schema: String,
    pub name: String,
}
```

- [ ] **Step 4: Add serde derives in `snapshot.rs`**

Add `serde::Serialize, serde::Deserialize` to the existing derive lists of `StatValue` (enum), `FileFormat` (enum), `ColumnStat`, and `DataFile`. Example:

```rust
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum StatValue { Bool(bool), I32(i32), I64(i64), F32(f32), F64(f64), Str(String) }
```
…and likewise the other three (keep their existing derives, append the two serde ones). `serde` is already a dependency of `control-plane/core` (used by `flush.rs`); no BUCK/dep change.

- [ ] **Step 5: Export from `lib.rs`**

Add `pub mod compact_job;` and `pub use compact_job::{CompactJob, COMPACT_JOB_KIND};` next to the flush exports.

- [ ] **Step 6: Run to verify it passes + clippy clean**

```
buck2 test //src/control-plane/core:compact-job > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
buck2 build '//src/control-plane/core:core[clippy.txt]'
```
Expected: PASS; empty clippy output.

- [ ] **Step 7: Commit**

```
git add src/control-plane/core
git commit -m "feat(core): CompactJob/COMPACT_JOB_KIND + serde derives on DataFile for the wire"
```

---

## Task B2: `ListFiles` + `CompactTable` gRPC + engine-wire client methods

**Files:**
- Modify: `src/services/engine-wire/proto/engine_control.proto`
- Modify: `src/services/engine-wire/src/client.rs`
- Test: `src/services/engine-wire/tests/compact_rpc.rs` (new `rust_test`)

**Interfaces:**
- Produces (proto, package `loom.engine.v1`):
  - `rpc ListFiles(ListFilesRequest) returns (ListFilesResponse);`
  - `rpc CompactTable(CompactTableRequest) returns (CompactTableResponse);`
  - `message FileMeta { string path = 1; int64 record_count = 2; int64 file_size_bytes = 3; }`
  - `message ListFilesRequest { string schema = 1; string name = 2; }`
  - `message ListFilesResponse { repeated FileMeta files = 1; }`
  - `message CompactTableRequest { string schema = 1; string name = 2; repeated string expire = 3; repeated string write_json = 4; }`
  - `message CompactTableResponse { optional int64 snapshot_id = 1; }`
- Produces (client, on `GrpcQueueClient`):
  - `list_files(&self, schema: String, name: String) -> Result<Vec<control_plane_core::FileRef>>`
  - `compact_table(&self, schema: String, name: String, expire: Vec<String>, write: &[control_plane_core::DataFile]) -> Result<Option<i64>>` (serializes each `DataFile` via `serde_json::to_string`).
- Consumes: existing `GrpcQueueClient.inner: EngineControlClient<Channel>`, `crate::client::be` error mapper, `crate::pb`.

**Design note:** `DataFile` carries typed `StatValue` min/max — modeling that in protobuf is heavy, so `write_json` carries each `DataFile` as a JSON string (consistent with `FlightTicket`/job-payload JSON conventions). `FileRef` (3 scalar fields) maps to a proper `FileMeta` message.

- [ ] **Step 1: Write the failing test** — `tests/compact_rpc.rs`

```rust
//! The new compact RPC messages encode/serialize as expected (no server needed).
use control_plane_core::{ColumnStat, DataFile, FileFormat, StatValue};

#[test]
fn data_file_json_round_trips_for_wire() {
    // The client puts each DataFile in write_json as a JSON string; the engine reads
    // it back. Guard that contract here (the engine handler depends on it).
    let f = DataFile {
        path: "file:///wh/main/t/c/part-0.parquet".into(),
        path_is_relative: false,
        file_format: FileFormat::Parquet,
        record_count: 3,
        file_size_bytes: 100,
        column_stats: vec![ColumnStat {
            column_name: "id".into(), null_count: 0, column_size_bytes: 8,
            min: Some(StatValue::I64(1)), max: Some(StatValue::I64(3)),
        }],
        parquet_footer_size: Some(20),
    };
    let s = serde_json::to_string(&f).unwrap();
    let back: DataFile = serde_json::from_str(&s).unwrap();
    assert_eq!(back, f);
}

#[test]
fn compact_request_message_constructs() {
    // Proof the generated pb types exist with the expected fields.
    let req = engine_wire::pb::CompactTableRequest {
        schema: "main".into(), name: "t".into(),
        expire: vec!["file:///a.parquet".into()],
        write_json: vec!["{}".into()],
    };
    assert_eq!(req.expire.len(), 1);
    let lf = engine_wire::pb::ListFilesResponse {
        files: vec![engine_wire::pb::FileMeta { path: "p".into(), record_count: 1, file_size_bytes: 2 }],
    };
    assert_eq!(lf.files[0].record_count, 1);
}
```

Wire target `compact-rpc` in `engine-wire/BUCK` (mirror the `convert`/`flight-ticket` test targets; deps `[":engine-wire", "//src/control-plane/core:core", "//third-party:serde_json"]`). Confirm `engine_wire::pb` is `pub` (the existing `flight.rs`/`client.rs` reference `crate::pb`; expose `pub mod pb;` in `lib.rs` if not already — check, the `convert` test implies access).

- [ ] **Step 2: Run to verify it fails**

```
buck2 test //src/services/engine-wire:compact-rpc > /tmp/t.log 2>&1; grep -E "cannot find|error\[|Tests finished|FAIL" /tmp/t.log
```
Expected: FAIL — `pb::CompactTableRequest` / `pb::FileMeta` don't exist.

- [ ] **Step 3: Extend the proto**

Add to the `EngineControl` service block:
```protobuf
  rpc ListFiles   (ListFilesRequest)   returns (ListFilesResponse);
  rpc CompactTable(CompactTableRequest) returns (CompactTableResponse);
```
And the messages (after `FlushTableResponse`):
```protobuf
message FileMeta { string path = 1; int64 record_count = 2; int64 file_size_bytes = 3; }
message ListFilesRequest  { string schema = 1; string name = 2; }
message ListFilesResponse { repeated FileMeta files = 1; }
message CompactTableRequest  { string schema = 1; string name = 2; repeated string expire = 3; repeated string write_json = 4; }
message CompactTableResponse { optional int64 snapshot_id = 1; }
```
The `:pb-gen` genrule regenerates on build (no manual codegen run needed).

- [ ] **Step 4: Add client methods to `GrpcQueueClient` in `client.rs`**

Mirror `flush_table`'s shape exactly:

```rust
/// List a table's live files (path + counts) for worker-side small-file selection.
pub async fn list_files(
    &self,
    schema: String,
    name: String,
) -> Result<Vec<control_plane_core::FileRef>> {
    let resp = self.inner.clone()
        .list_files(pb::ListFilesRequest { schema, name })
        .await.map_err(be)?.into_inner();
    Ok(resp.files.into_iter().map(|f| control_plane_core::FileRef {
        path: f.path,
        record_count: f.record_count,
        file_size_bytes: f.file_size_bytes,
    }).collect())
}

/// Commit a compaction swap: expire `expire` (absolute paths) + register `write`
/// (already written). Returns the new snapshot id (or `None` if the table was
/// never written). Each `DataFile` is sent as a JSON string in `write_json`.
pub async fn compact_table(
    &self,
    schema: String,
    name: String,
    expire: Vec<String>,
    write: &[control_plane_core::DataFile],
) -> Result<Option<i64>> {
    let write_json = write.iter()
        .map(|f| serde_json::to_string(f).map_err(|e| be(e)))
        .collect::<Result<Vec<_>>>()?;
    let resp = self.inner.clone()
        .compact_table(pb::CompactTableRequest { schema, name, expire, write_json })
        .await.map_err(be)?.into_inner();
    Ok(resp.snapshot_id)
}
```

(`serde_json` is already an engine-wire dep.)

- [ ] **Step 5: Run to verify it passes**

```
buck2 test //src/services/engine-wire:compact-rpc //src/services/engine-wire:convert > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```
Expected: PASS.

- [ ] **Step 6: Commit**

```
git add src/services/engine-wire
git commit -m "feat(engine-wire): ListFiles + CompactTable RPCs and GrpcQueueClient methods"
```

---

## Task B3: Engine-side `ListFiles` + `CompactTable` handlers

**Files:**
- Modify: `src/services/engine/src/service.rs`
- Modify: `src/services/engine/BUCK` (add `//src/control-plane/core` and `//third-party:serde_json` to `:engine` deps if absent)
- Test: `src/services/engine/tests/compact_wire.rs` (new `loom_fixture_test`)

**Interfaces:**
- Consumes: `EngineControlService { cp: PgControlPlane, catalog: SqlCatalog, pool: PgPool }`; `IcebergCatalog::{new, current_snapshot, files_with_stats}`; `control_plane_postgres::iceberg_compact::compact_table`; `control_plane_core::{DataFile, TableRef}`.
- Produces: the two server-side RPC methods, completing `impl EngineControl for EngineControlService`.

- [ ] **Step 1: Write the failing test** — `tests/compact_wire.rs`

Model on `engine/tests/wire.rs` (copy its server-spawn + fixture seeding). Seed an Iceberg table with ≥2 files (use the same `land`/append helper `wire.rs`/`flight_roundtrip.rs` uses), then drive the two RPCs through `GrpcQueueClient`:

```rust
//! Engine ListFiles + CompactTable over the wire: list returns live files; compact
//! expires a subset and commits a new snapshot.
// ... copy spawn_server / make_catalog / seed helpers from engine/tests/wire.rs ...

#[tokio::test(flavor = "multi_thread")]
async fn list_files_then_compact_over_the_wire() {
    // 1. fresh pg + engine on UDS; warehouse tempdir.
    // 2. land two small files (a:3, b:2) into Iceberg table ("main","t").
    // 3. client.list_files -> 2 entries with sizes.
    let client = GrpcQueueClient::connect(&sock).await.unwrap();
    let files = client.list_files("main".into(), "t".into()).await.unwrap();
    assert_eq!(files.len(), 2);

    // 4. compact: expire both, register one synthetic coalesced DataFile (absolute path).
    let expire: Vec<String> = files.iter().map(|f| f.path.clone()).collect();
    let coalesced = vec![control_plane_core::DataFile {
        path: format!("{}/main/t/c/part-0.parquet", wh_abs),
        path_is_relative: false,
        file_format: control_plane_core::FileFormat::Parquet,
        record_count: 5, file_size_bytes: 200, column_stats: vec![], parquet_footer_size: None,
    }];
    let snap = client.compact_table("main".into(), "t".into(), expire, &coalesced).await.unwrap();
    assert!(snap.is_some(), "compaction returns a snapshot");

    // 5. list again -> exactly the coalesced file.
    let after = client.list_files("main".into(), "t".into()).await.unwrap();
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].record_count, 5);
}
```

Wire `loom_fixture_test` target `compact-wire` in `engine/BUCK` (copy `:wire` deps).

- [ ] **Step 2: Run to verify it fails**

```
buck2 test //src/services/engine:compact-wire > /tmp/t.log 2>&1; grep -E "Unimplemented|error\[|Tests finished|FAIL" /tmp/t.log
```
Expected: FAIL — RPCs unimplemented on the server (tonic returns `Unimplemented`, or build error if the generated trait now requires them).

- [ ] **Step 3: Implement the handlers in `service.rs`**

Add to `impl pb::engine_control_server::EngineControl for EngineControlService`:

```rust
async fn list_files(
    &self,
    req: Request<pb::ListFilesRequest>,
) -> std::result::Result<Response<pb::ListFilesResponse>, Status> {
    let r = req.into_inner();
    let table = TableRef { schema: r.schema, name: r.name };
    let ice = control_plane_postgres::iceberg_catalog::IcebergCatalog::new(self.pool.clone());
    let files = match ice.current_snapshot(&table).await {
        Ok(snap) => ice.files_with_stats(&table, snap.id).await.map_err(status)?,
        Err(control_plane_core::ControlPlaneError::NotFound(_)) => Vec::new(),
        Err(e) => return Err(status(e)),
    };
    Ok(Response::new(pb::ListFilesResponse {
        files: files.into_iter().map(|f| pb::FileMeta {
            path: f.path, record_count: f.record_count, file_size_bytes: f.file_size_bytes,
        }).collect(),
    }))
}

async fn compact_table(
    &self,
    req: Request<pb::CompactTableRequest>,
) -> std::result::Result<Response<pb::CompactTableResponse>, Status> {
    let r = req.into_inner();
    let table = TableRef { schema: r.schema, name: r.name };
    let write: Vec<control_plane_core::DataFile> = r.write_json.iter()
        .map(|s| serde_json::from_str(s)
            .map_err(|e| Status::invalid_argument(format!("bad write DataFile json: {e}"))))
        .collect::<std::result::Result<_, _>>()?;
    let snap = control_plane_postgres::iceberg_compact::compact_table(
        &self.pool, &table, &r.expire, &write,
    ).await.map_err(status)?;
    Ok(Response::new(pb::CompactTableResponse { snapshot_id: snap.map(|s| s.0) }))
}
```

`status()` maps `NotFound → not_found`, else `internal`. Extend it to surface `Conflict` so the worker can distinguish it (still retryable — the worker treats any `CompactTable` error as `Retry`, but a precise code aids observability):

```rust
fn status(e: control_plane_core::ControlPlaneError) -> Status {
    use control_plane_core::ControlPlaneError::*;
    match e {
        NotFound(m) => Status::not_found(m.to_string()),
        Conflict(m) => Status::aborted(m.to_string()),
        other => Status::internal(other.to_string()),
    }
}
```

`files_with_stats` returns a type with `.path/.record_count/.file_size_bytes` (`FileWithStats`); confirm the field names against `iceberg_catalog.rs` and adjust if it returns `FileRef` instead.

- [ ] **Step 4: Run to verify it passes**

```
buck2 test //src/services/engine:compact-wire //src/services/engine:wire > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```
Expected: PASS (and the existing `:wire` flush test still green).

- [ ] **Step 5: Commit**

```
git add src/services/engine
git commit -m "feat(engine): serve ListFiles + CompactTable RPCs (Iceberg subset-expire commit)"
```

---

## Task B4: Operator enqueue endpoint `POST /tables/{schema}/{table}/compact`

**Files:**
- Modify: `src/services/ingest/src/http.rs` (`AppState`, `router`, new `compact` handler)
- Modify: `src/services/ingest/src/main.rs` (build a `ControlPlane` handle, pass to `AppState`)
- Modify: `src/services/ingest/BUCK` (ensure `:ingest` lib deps include `//src/control-plane/core`; `:ingest-bin` already has postgres + runtime)
- Test: `src/services/ingest/tests/compact_endpoint.rs` (new `loom_fixture_test`)

**Interfaces:**
- Produces: `POST /tables/:schema/:table/compact` → `202 ACCEPTED` + JSON `{ "job_id": "<uuid>" }`; enqueues `NewJob { kind: COMPACT_JOB_KIND, payload: CompactJob, run_at: None, priority: 0 }`.
- `AppState` gains `pub cp: Arc<dyn ControlPlane>`.

- [ ] **Step 1: Write the failing test** — `tests/compact_endpoint.rs`

Drive the router in-process with `tower::oneshot` (mirror an existing ingest http test). Use a real `PgControlPlane` from the fixture as `cp`, a stub materializer (the endpoint doesn't touch it):

```rust
//! POST /tables/{schema}/{table}/compact enqueues a compact_table job and returns its id.
use std::sync::Arc;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use control_plane_core::{COMPACT_JOB_KIND, ControlPlane, Queue};
use ingest::http::{AppState, router};
use tower::ServiceExt;

#[tokio::test(flavor = "multi_thread")]
async fn compact_endpoint_enqueues_job() {
    let fx = PgFixture::start();
    let (cp, _db) = fx.fresh_db().await;          // cp: PgControlPlane
    let cp = Arc::new(cp);
    let state = AppState {
        materializer: /* stub LandingMaterializer that panics if called */ stub_materializer(),
        cp: cp.clone(),
    };
    let app = router(state);
    let resp = app.oneshot(
        Request::builder().method("POST").uri("/tables/main/orders/compact")
            .body(Body::empty()).unwrap()
    ).await.unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    // The job is now dequeueable.
    let job = cp.dequeue(&[COMPACT_JOB_KIND.to_string()], "test").await.unwrap();
    let job = job.expect("a compact_table job was enqueued");
    assert_eq!(job.kind, COMPACT_JOB_KIND);
    let payload: serde_json::Value = job.payload;
    assert_eq!(payload["schema"], "main");
    assert_eq!(payload["name"], "orders");
}
```

Provide a minimal `stub_materializer()` returning an `Arc<dyn LandingMaterializer>` whose `land` `unreachable!()`s (the compact route never calls it). Wire `loom_fixture_test` target `compact-endpoint` (deps mirror an existing ingest fixture test + `//third-party:tower`, `//third-party:axum`).

- [ ] **Step 2: Run to verify it fails**

```
buck2 test //src/services/ingest:compact-endpoint > /tmp/t.log 2>&1; grep -E "no field|cannot find|404|Tests finished|FAIL" /tmp/t.log
```
Expected: FAIL — `AppState` has no `cp`; route missing (404).

- [ ] **Step 3: Extend `AppState`, `router`, and add the handler in `http.rs`**

```rust
use std::sync::Arc;
use control_plane_core::{COMPACT_JOB_KIND, CompactJob, ControlPlane, NewJob, Queue};

#[derive(Clone)]
pub struct AppState {
    pub materializer: Arc<dyn LandingMaterializer>,
    pub cp: Arc<dyn ControlPlane>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/datasets/:schema/:table", post(land))
        .route("/tables/:schema/:table/compact", post(compact))
        .with_state(state)
}

/// Operator action: enqueue a compaction job for `{schema}.{table}`. Returns the
/// JobId; a zero-pool worker performs the compaction asynchronously.
async fn compact(
    State(st): State<AppState>,
    Path((schema, table)): Path<(String, String)>,
) -> Response {
    let payload = match serde_json::to_value(CompactJob { schema, name: table }) {
        Ok(v) => v,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    };
    let job = NewJob { kind: COMPACT_JOB_KIND.to_string(), payload, run_at: None, priority: 0 };
    match st.cp.queue().enqueue(job).await {
        Ok(id) => (StatusCode::ACCEPTED, Json(serde_json::json!({ "job_id": id.0.to_string() }))).into_response(),
        // Opaque on backend faults (governance-fronted service).
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    }
}
```

- [ ] **Step 4: Build the `cp` in `main.rs` and pass it to `AppState`**

`pool` is consumed by both landing branches, so build the `cp` from `pool.clone()` **before** the `match backend`, and pass it into `AppState`:

```rust
let cp: std::sync::Arc<dyn control_plane_core::ControlPlane> =
    std::sync::Arc::new(service_runtime::control_plane(pool.clone(), cfg.lock_timeout));
// ... existing materializer match (uses `pool`) ...
let app = router(AppState { materializer, cp });
```

(`PgControlPlane: ControlPlane`, so the `Arc<dyn ControlPlane>` coercion is direct. `control_plane` takes `pool` by value — pass `pool.clone()`; the DuckLake branch also clones `pool` into its own `cp` already, and the Iceberg branch moves `pool` into the materializer, so order the `cp` build first.)

Add `//src/control-plane/core:core` to the `:ingest` lib `deps` in `BUCK` if not already present (needed for `ControlPlane`, `NewJob`, `CompactJob`, `COMPACT_JOB_KIND`, `Queue`).

- [ ] **Step 5: Run to verify it passes**

```
buck2 test //src/services/ingest:compact-endpoint > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
buck2 build //src/services/ingest:ingest-bin
```
Expected: PASS; binary builds.

- [ ] **Step 6: Commit**

```
git add src/services/ingest
git commit -m "feat(ingest): operator POST /tables/{schema}/{table}/compact enqueues a compact job"
```

---

## Task B5: Extract a postgres-free `store-config` crate

**Files:**
- Create: `src/services/store-config/BUCK`, `src/services/store-config/src/lib.rs`
- Modify: `src/services/runtime/src/lib.rs` (depend on + re-export `store_config`; delete the moved definitions)
- Modify: `src/services/runtime/BUCK` (add `//src/services/store-config` dep)
- Test: `src/services/store-config/tests/config.rs` (new `rust_test`)

**Interfaces:**
- Produces crate `store_config` (postgres-free) exporting: `ObjectStoreConfig`, `ObjectStoreBackend`, `S3Backend`, `StoreConfigError` (the parse error), the **real existing** `ObjectStoreConfig::parse(vars: &HashMap<String,String>, data_path: &Path) -> Result<ObjectStoreConfig, StoreConfigError>` (made `pub` during the move — it keeps its 2-arg shape; `data_path` is the warehouse fallback when `LOOM_WAREHOUSE_URI` is unset), a NEW `ObjectStoreConfig::parse_from_env(vars: &HashMap<String,String>) -> Result<ObjectStoreConfig, StoreConfigError>` for postgres-free callers (the worker) that **requires** `LOOM_WAREHOUSE_URI` (errors if unset — no `data_path` fallback), `local_store(&Path) -> Result<LocalFileSystem, …>`, `build_serving_object_store(&ObjectStoreConfig) -> Result<Option<(String, Arc<dyn ObjectStore>)>, …>`, and a NEW `build_write_store(&ObjectStoreConfig) -> Result<WriteStore, …>` where `WriteStore { pub store: Arc<dyn ObjectStore>, pub root_url: String }`.
- **Do NOT change `Config::from_map`'s existing call** `ObjectStoreConfig::parse(vars, &data_path)` in `service_runtime` — it keeps using the 2-arg `parse` (now re-exported). Only the worker uses `parse_from_env`.
- `service_runtime` keeps its public API by `pub use store_config::{…}` re-exports, so `engine/main.rs`, `ingest/main.rs`, etc. compile unchanged.

**Note on what NOT to move:** Only move the object-store-config items that are postgres-free. `build_storage_factory` (uses `control_plane_postgres::…::S3StorageFactory`) stays in `service_runtime`. `Config`/`DbConfig`/`build_pool`/`control_plane`/`serve`/`init_tracing` stay in `service_runtime`. `Config` keeps an `object_store: ObjectStoreConfig` field — it now references the re-exported type.

**`build_write_store` semantics** (new; the worker needs a writable store + a way to form absolute paths):
- Local: `WriteStore { store: Arc::new(local_store(data_path)?), root_url: warehouse_uri.clone() }` where `data_path` is parsed from `warehouse_uri` (strip a leading `file://`). `root_url` is the `file://…` warehouse URI.
- S3: `WriteStore { store, root_url: format!("s3://{bucket}") }` reusing `build_serving_object_store`'s builder.
- The worker forms a coalesced file's absolute path as `format!("{root_url}/{schema}/{table}/{relpath}")` where `relpath` is `write_dataset`'s table-relative path (e.g. `"{run_id}/part-0.parquet"`).

- [ ] **Step 1: Write the failing test** — `tests/config.rs`

```rust
use std::collections::HashMap;
use store_config::{build_write_store, ObjectStoreBackend, ObjectStoreConfig};

fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

#[test]
fn parse_from_env_local_and_builds_write_store() {
    let cfg = ObjectStoreConfig::parse_from_env(&env(&[("LOOM_WAREHOUSE_URI", "file:///tmp/wh")])).unwrap();
    assert!(matches!(cfg.backend, ObjectStoreBackend::Local));
    let ws = build_write_store(&cfg).unwrap();
    assert_eq!(ws.root_url, "file:///tmp/wh");
}

#[test]
fn parse_from_env_s3_and_builds_write_store() {
    let cfg = ObjectStoreConfig::parse_from_env(&env(&[
        ("LOOM_WAREHOUSE_URI", "s3://bucket/wh"),
        ("AWS_REGION", "us-east-1"),
        ("AWS_ACCESS_KEY_ID", "k"),
        ("AWS_SECRET_ACCESS_KEY", "s"),
    ])).unwrap();
    assert!(matches!(cfg.backend, ObjectStoreBackend::S3(_)));
    let ws = build_write_store(&cfg).unwrap();
    assert_eq!(ws.root_url, "s3://bucket");
}

#[test]
fn parse_from_env_requires_warehouse_uri() {
    // postgres-free callers have no data_path fallback: missing LOOM_WAREHOUSE_URI errors.
    assert!(ObjectStoreConfig::parse_from_env(&env(&[])).is_err());
}
```

Wire `rust_test` target `config` in `store-config/BUCK`.

- [ ] **Step 2: Run to verify it fails**

```
buck2 build //src/services/store-config:store-config > /tmp/t.log 2>&1; grep -E "not found|error\[|Build ID" /tmp/t.log
```
Expected: FAIL — crate doesn't exist yet.

- [ ] **Step 3: Create the `store-config` crate**

`src/services/store-config/BUCK`:
```python
load("@prelude//rust:cargo_package.bzl", "cargo")

cargo.rust_library(
    name = "store-config",
    crate = "store_config",
    srcs = glob(["src/**/*.rs"]),
    crate_root = "src/lib.rs",
    edition = "2024",
    deps = [
        "//third-party:object_store",
        "//third-party:thiserror",
    ],
    visibility = ["PUBLIC"],
)

rust_test(
    name = "config",
    crate = "config",
    srcs = ["tests/config.rs"],
    crate_root = "tests/config.rs",
    edition = "2024",
    deps = [":store-config"],
)
```

`src/services/store-config/src/lib.rs`: move the postgres-free items out of `src/services/runtime/src/lib.rs` verbatim — `ObjectStoreConfig`, `ObjectStoreBackend`, `S3Backend`, the parse `ConfigError` (rename to avoid clashing with runtime's broader `ConfigError`, use `StoreConfigError`), `ObjectStoreConfig::parse` (keep its real 2-arg signature `parse(vars: &HashMap<String,String>, data_path: &Path)` but make it `pub`), `local_store`, `build_serving_object_store` (and its `ServingStore`/`RuntimeError::Store` → make a local error). Add a postgres-free env entry point that requires the warehouse URI (the worker has no `data_path`):

```rust
impl ObjectStoreConfig {
    /// Parse from env without a `data_path` fallback: `LOOM_WAREHOUSE_URI` is
    /// required (postgres-free callers like the zero-pool worker have no data dir).
    pub fn parse_from_env(vars: &HashMap<String, String>) -> Result<Self, StoreConfigError> {
        let uri = vars.get("LOOM_WAREHOUSE_URI")
            .ok_or_else(|| StoreConfigError::Missing("LOOM_WAREHOUSE_URI".into()))?;
        // Reuse the real parse with the warehouse path as its own data_path: when
        // LOOM_WAREHOUSE_URI is present, `parse` ignores the data_path fallback.
        let stripped = uri.strip_prefix("file://").unwrap_or(uri);
        Self::parse(vars, std::path::Path::new(stripped))
    }
}
```

Add a `StoreConfigError::Missing(String)` variant. Then add `WriteStore` + `build_write_store`:

```rust
use std::path::Path;
use std::sync::Arc;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;

/// A writable object store plus the absolute URL root under which its keys live, so
/// callers can form absolute data-file paths (`{root_url}/{schema}/{table}/{rel}`).
pub struct WriteStore {
    pub store: Arc<dyn ObjectStore>,
    pub root_url: String,
}

pub fn build_write_store(cfg: &ObjectStoreConfig) -> Result<WriteStore, StoreConfigError> {
    match &cfg.backend {
        ObjectStoreBackend::Local => {
            // warehouse_uri is "file://<abs>"; LocalFileSystem roots at the abs path.
            let path = cfg.warehouse_uri.strip_prefix("file://").unwrap_or(&cfg.warehouse_uri);
            Ok(WriteStore {
                store: Arc::new(local_store(Path::new(path))?),
                root_url: cfg.warehouse_uri.clone(),
            })
        }
        ObjectStoreBackend::S3(s) => {
            let (bucket, store) = build_serving_object_store(cfg)?
                .expect("S3 backend yields a serving store");
            Ok(WriteStore { store, root_url: format!("s3://{bucket}") })
        }
    }
}
```

- [ ] **Step 4: Re-export from `service_runtime` and delete the moved code**

In `src/services/runtime/src/lib.rs`: delete the moved definitions and add `pub use store_config::{ObjectStoreConfig, ObjectStoreBackend, S3Backend, local_store, build_serving_object_store, WriteStore, build_write_store};` (and the `ServingStore` type if other code referenced it). Keep `Config`'s `object_store: ObjectStoreConfig` field pointing at the re-export. Add `//src/services/store-config:store-config` to `runtime/BUCK` deps.

- [ ] **Step 5: Verify nothing regressed (engine/ingest still build) + new test passes**

```
buck2 build //src/services/runtime:runtime //src/services/engine:engine-bin //src/services/ingest:ingest-bin > /tmp/b.log 2>&1; grep -E "error\[|BUILD SUCCEEDED|Build ID" /tmp/b.log
buck2 test //src/services/store-config:config > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```
Expected: builds succeed; test passes.

- [ ] **Step 6: Commit**

```
git add src/services/store-config src/services/runtime
git commit -m "refactor(runtime): extract postgres-free store-config crate (+ build_write_store)"
```

---

## Task B6: Worker compaction handler + dispatch + end-to-end proof

**Files:**
- Create: `src/services/worker/src/compact.rs`
- Modify: `src/services/worker/src/lib.rs` (`pub mod compact;`)
- Modify: `src/services/worker/src/main.rs` (kinds list + dispatch + build compaction context)
- Modify: `src/services/worker/BUCK` (`:worker` lib + `:worker-bin` deps; new `loom_fixture_test`)
- Test: `src/services/worker/tests/compact_e2e.rs` (new `loom_fixture_test`)

**Interfaces:**
- Consumes: `engine_wire::client::GrpcQueueClient::{list_files, compact_table}`, `engine_wire::flight::{FlightTicket, FlightTableClient}`, `transform::small_files`, `transform::CompactConfig`/`datafusion_io::{WriteConfig, write_dataset}`, `store_config::{ObjectStoreConfig, build_write_store, WriteStore}`, `control_plane_core::{CompactJob, COMPACT_JOB_KIND, DataFile, FileFormat, FileRef, Job, JobFailure, RetryPolicy}`.
- Produces: `worker::compact::handle_compact(ctx: &CompactCtx, job: Job) -> std::result::Result<(), JobFailure>` and `CompactCtx { control: GrpcQueueClient, flight: FlightTableClient, write: Arc<WriteStore>, threshold_bytes: i64 }`.

**Worker flow (handle_compact):**
1. Parse `job.payload` → `CompactJob { schema, name }` (bad payload → `Abandon`).
2. `let live = ctx.control.list_files(schema, name).await` → `Vec<FileRef>` (RPC error → `RetryPolicy::Retry { delay }`).
3. `let small = transform::small_files(&live, ctx.threshold_bytes)` → `Vec<&FileRef>`. If `small.len() < 2` → `Ok(())` (no-op Complete — matches the primitive's `Ok(None)`).
4. `let small_paths: Vec<String> = small.iter().map(|f| f.path.clone()).collect()` (absolute, from the mirror).
5. `let batches = ctx.flight.fetch(FlightTicket { schema, name, files: small_paths.clone() }).await` (error → `Retry`). If `batches.is_empty()` → `Ok(())` (no rows; nothing to coalesce).
6. `let schema_ref = batches[0].schema();`
7. `let run_id = uuid::Uuid::new_v4().to_string(); let dir_prefix = format!("{schema}/{name}/{run_id}");`
8. `let written = write_dataset(ctx.write.store.clone(), &dir_prefix, schema_ref, &batches, &WriteConfig::default()).await` (error → `Retry`).
9. Build absolute `DataFile`s: for each `WrittenFile w`, `path = format!("{}/{}/{}/{}", ctx.write.root_url, schema, name, w.path)` (since `w.path` is table-relative `"{run_id}/part-N.parquet"`), `path_is_relative: false`, `file_format: Parquet`, counts/size/stats from `w`, `parquet_footer_size: Some(w.footer_size)`.
10. `ctx.control.compact_table(schema, name, small_paths, &new_files).await` (error → `Retry`; this includes the `Aborted`/`Conflict` raced case, which retries and converges to a clean no-op on the next run).
11. `Ok(())`.

**Dispatch (main.rs):** add `COMPACT_JOB_KIND` to the `kinds` slice and branch in the handler closure on `job.kind`:
```rust
worker.run(&[FLUSH_JOB_KIND.to_string(), COMPACT_JOB_KIND.to_string()], shutdown, move |job| {
    let flush = flush.clone();
    let cctx = cctx.clone();   // Arc<CompactCtx>-ish; clone the GrpcQueueClient/FlightTableClient (both Clone) + Arc<WriteStore>
    async move {
        match job.kind.as_str() {
            k if k == FLUSH_JOB_KIND => worker::handler::handle_flush(flush, job).await,
            k if k == COMPACT_JOB_KIND => worker::compact::handle_compact(&cctx, job).await,
            other => Err(JobFailure { error: format!("unknown job kind: {other}"), policy: RetryPolicy::Abandon }),
        }
    }
}).await?;
```
Build `cctx` in main: `let env: std::collections::HashMap<String,String> = std::env::vars().collect(); let store_cfg = store_config::ObjectStoreConfig::parse_from_env(&env)?; let write = std::sync::Arc::new(store_config::build_write_store(&store_cfg)?); let flight = FlightTableClient::connect(&socket).await?; let threshold_bytes = std::env::var("LOOM_COMPACT_THRESHOLD_BYTES").ok().and_then(|v| v.parse().ok()).unwrap_or(128 * 1024 * 1024);` then `CompactCtx { control: client.clone(), flight, write, threshold_bytes }`.

- [ ] **Step 1: Write the failing e2e test** — `tests/compact_e2e.rs`

Model on `worker/tests/flight_roundtrip.rs` (server spawn) + `worker/tests/e2e.rs` (enqueue/dequeue/handle/complete) + `transform/tests/compact_e2e.rs` (assertions). Use the Iceberg backend (engine serves EngineControl+Flight; seed via the same `land`/append the flight-roundtrip test uses):

```rust
//! Engine-wire compaction e2e (Iceberg): land several small files, run the worker's
//! compact handler over the wire, assert the small files coalesce, the row set is
//! preserved, and a prior snapshot still time-travels. Plus a no-op (<2 small files).
// ... copy spawn_server / make_catalog / seed helpers ...

#[tokio::test(flavor = "multi_thread")]
async fn worker_compacts_small_files_over_the_wire() {
    // 1. fresh pg + engine (EngineControl + Flight) on UDS; warehouse tempdir whose
    //    path matches the worker's LOOM_WAREHOUSE_URI=file://<wh>.
    // 2. land three small files (1 row each) into Iceberg table ("main","acc").
    // 3. Build CompactCtx { control: GrpcQueueClient::connect(sock), flight: FlightTableClient::connect(sock),
    //    write: build_write_store(parse(env with LOOM_WAREHOUSE_URI=file://wh)), threshold: 10 MiB }.
    // 4. handle_compact(&ctx, compact_job("main","acc")).await.expect("compact");
    let ice = IcebergCatalog::new(pool.clone());
    let head = ice.current_snapshot(&acc).await.unwrap().id;
    let after = ice.files_with_stats(&acc, head).await.unwrap();
    assert_eq!(after.len(), 1, "three small files coalesced into one");
    let total: i64 = after.iter().map(|f| f.record_count).sum();
    assert_eq!(total, 3, "row set preserved");
    // 4b. CRITICAL: read the coalesced file's rows back over Flight to prove the
    //     worker's registered absolute path ({root_url}/{schema}/{table}/{rel}) is
    //     actually resolvable by the engine's Iceberg FileIO (catches a path-scheme
    //     or layout mismatch that a count-only assert would miss).
    let coalesced_path = after[0].path.clone();
    let flight = FlightTableClient::connect(&sock).await.unwrap();
    let batches = flight.fetch(FlightTicket {
        schema: acc.schema.clone(), name: acc.name.clone(), files: vec![coalesced_path],
    }).await.expect("coalesced file is Flight-readable");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3, "coalesced file streams back the full row set");
    // 5. time travel: prior snapshot still lists the three originals.
    // 6. second run is a no-op (1 file left): handle_compact again -> Ok, snapshot unchanged.
}
```

Add a `loom_fixture_test` target `compact-e2e` in `worker/BUCK` (deps mirror `:flight-roundtrip` + `//src/services/transform`, `//src/services/datafusion-io`, `//src/services/store-config`, `//third-party:object_store`).

- [ ] **Step 2: Run to verify it fails**

```
buck2 test //src/services/worker:compact-e2e > /tmp/t.log 2>&1; grep -E "cannot find|error\[|Tests finished|FAIL" /tmp/t.log
```
Expected: FAIL — `worker::compact` doesn't exist.

- [ ] **Step 3: Implement `src/services/worker/src/compact.rs`** (per the flow above)

```rust
//! The worker's compaction job handler: list a table's live files over the wire,
//! pick the small ones, stream them via Flight, rewrite coalesced to object store,
//! and commit the swap over CompactTable. Zero Postgres — the engine owns it.
use std::sync::Arc;
use std::time::Duration;

use control_plane_core::{CompactJob, DataFile, FileFormat, Job, JobFailure, RetryPolicy};
use datafusion_io::{WriteConfig, write_dataset};
use engine_wire::client::GrpcQueueClient;
use engine_wire::flight::{FlightTableClient, FlightTicket};
use store_config::WriteStore;
use transform::small_files;

#[derive(Clone)]
pub struct CompactCtx {
    pub control: GrpcQueueClient,
    pub flight: FlightTableClient,
    pub write: Arc<WriteStore>,
    pub threshold_bytes: i64,
}

fn retry(attempts: i32, error: String) -> JobFailure {
    let secs = 1u64.checked_shl(attempts.clamp(0, 6) as u32).unwrap_or(64).min(60);
    JobFailure { error, policy: RetryPolicy::Retry { delay: Duration::from_secs(secs) } }
}

pub async fn handle_compact(ctx: &CompactCtx, job: Job) -> std::result::Result<(), JobFailure> {
    let attempts = job.attempts;
    let CompactJob { schema, name } = serde_json::from_value(job.payload)
        .map_err(|e| JobFailure { error: format!("bad compact payload: {e}"), policy: RetryPolicy::Abandon })?;

    let live = ctx.control.list_files(schema.clone(), name.clone()).await
        .map_err(|e| retry(attempts, format!("list_files: {e}")))?;
    let small = small_files(&live, ctx.threshold_bytes);
    if small.len() < 2 {
        return Ok(()); // no-op: nothing worth coalescing (converges).
    }
    let small_paths: Vec<String> = small.iter().map(|f| f.path.clone()).collect();

    let batches = ctx.flight
        .fetch(FlightTicket { schema: schema.clone(), name: name.clone(), files: small_paths.clone() })
        .await.map_err(|e| retry(attempts, format!("flight fetch: {e}")))?;
    if batches.is_empty() {
        return Ok(());
    }
    let arrow_schema = batches[0].schema();
    let run_id = uuid::Uuid::new_v4().to_string();
    let dir_prefix = format!("{schema}/{name}/{run_id}");
    let written = write_dataset(ctx.write.store.clone(), &dir_prefix, arrow_schema, &batches, &WriteConfig::default())
        .await.map_err(|e| retry(attempts, format!("write_dataset: {e}")))?;

    let new_files: Vec<DataFile> = written.into_iter().map(|w| DataFile {
        path: format!("{}/{}/{}/{}", ctx.write.root_url, schema, name, w.path),
        path_is_relative: false,
        file_format: FileFormat::Parquet,
        record_count: w.record_count,
        file_size_bytes: w.file_size_bytes,
        column_stats: w.column_stats,
        parquet_footer_size: Some(w.footer_size),
    }).collect();

    ctx.control.compact_table(schema, name, small_paths, &new_files).await
        .map_err(|e| retry(attempts, format!("compact_table: {e}")))?;
    Ok(())
}
```

Add `pub mod compact;` to `worker/src/lib.rs`.

- [ ] **Step 4: Wire dispatch in `main.rs`** (per the Dispatch section above), and update `worker/BUCK`:
  - `:worker` lib deps: add `//src/services/transform`, `//src/services/datafusion-io`, `//src/services/store-config`, `//third-party:object_store`, `//third-party:arrow`.
  - `:worker-bin` deps: add the same (it builds `CompactCtx` + parses config) — but NOT postgres.

- [ ] **Step 5: Run the e2e + the postgres-free guard**

```
buck2 test //src/services/worker:compact-e2e //src/services/worker:e2e //src/services/worker:flight-roundtrip > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
buck2 uquery "deps(//src/services/worker:worker-bin)" 2>/dev/null | grep -i "control-plane/postgres" | head
```
Expected: tests PASS (existing flush e2e + flight-roundtrip still green); the `uquery | grep` prints **nothing** (postgres-free preserved).

- [ ] **Step 6: Commit**

```
git add src/services/worker
git commit -m "feat(worker): compact_table job dispatch — ListFiles + Flight read + CompactTable commit"
```

---

## Task B7: Full-suite verification + register update

**Files:**
- Modify: `docs/ROADMAP.md` (close `road-compaction-job`)
- Possibly: `docs/FUTURE.md` (note any newly-deferred follow-ons surfaced)

- [ ] **Step 1: Full build + test sweep**

```
buck2 build //src/... > /tmp/b.log 2>&1; grep -E "error|BUILD SUCCEEDED|Build ID" /tmp/b.log
buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```
Expected: build succeeds; all tests pass (incl. `sqlx-cache-check`). The duckdb-downgrade footgun lands failures in untouched crates — a green full sweep is the gate, not a per-crate build.

- [ ] **Step 2: Clippy + prek hooks**

```
bash tools/clippy-all.sh > /tmp/clippy.log 2>&1; tail -5 /tmp/clippy.log
buck2 run //tools:prek -- run --all-files > /tmp/prek.log 2>&1; tail -20 /tmp/prek.log
git add -A   # commit any in-place hook fixes
```

- [ ] **Step 3: Close the register item** — run `loom-docs-update` (or edit directly): in `docs/ROADMAP.md`, flip `road-compaction-job` `- [ ]`→`- [x]`, set `status:done`, add `pr:#<N>` once the PR exists. Note in `docs/FUTURE.md` the deferred follow-ons the spec lists (watermark/incremental compaction output; automatic threshold triggering; orphaned-Parquet GC under `fut-iceberg-gc`) if not already tracked.

- [ ] **Step 4: Final commit**

```
git add docs
git commit -m "docs(registers): close road-compaction-job (engine-wire compaction job shipped)"
```

---

## Self-Review

**Spec coverage:**
- Layer 1 (Flight data plane): already shipped — reused (`FlightTableClient.fetch`, file-ticket `do_get`), explicitly not rebuilt. ✓ (acceptance #4)
- `COMPACT_JOB_KIND` / `CompactJob`: Task B1. ✓
- Operator endpoint `POST /tables/{schema}/{table}/compact`: Task B4. ✓ (acceptance #1, spec testing "Operator endpoint")
- Worker dispatch (resolve small set, stream Flight, coalesce, commit): Task B6. ✓ (acceptance #1, #2)
- `EngineControl::CompactTable` commit RPC: Tasks B2/B3. ✓ (acceptance #2)
- Iceberg commit primitive (the spec's `Tx::compact_files`, an unimplemented stub for Iceberg): Tasks A1/A2 — the in-scope addition the "all in one slice" decision approved. ✓
- No-op (<2 small files): Task B6 handler + e2e. ✓ (spec testing "No-op")
- Conflict/retry (raced compaction → Conflict → bounded Retry → clean no-op): A1 conflict guard + B3 `Status::aborted` + B6 retry; e2e. ✓ (acceptance #3, spec testing "Conflict/retry")
- No lineage on compaction: `iceberg_compact::compact_table` emits none. ✓
- Existing direct-call `compact_table` primitive + flush vertical unchanged: not modified; full sweep gate. ✓ (acceptance #5)
- Metadata-over-the-wire for worker small-file selection: `ListFiles` RPC (Tasks B2/B3) — the spec's "catalog metadata fetched over the wire". ✓
- Worker holds no Postgres: postgres-free `store-config` crate (B5) + dep-closure guard (B6 Step 5). ✓ (acceptance #2)

**Deviations from the spec, surfaced deliberately:**
- The spec says the engine "stages `Tx::compact_files`." For the Iceberg engine-wire backend that primitive did not exist, so Part A implements it. The engine's `CompactTable` handler calls the free function `iceberg_compact::compact_table` (mirroring how `FlushTable` calls `iceberg_flush::flush_table`) rather than constructing an `IcebergControlPlane` Tx — both share the same `register_files(WriteMode::Compact)` machinery; `IcebergTx::compact_files` is also un-stubbed (A2) so the polymorphic seam is honored.
- New coalesced files are registered with **absolute** paths (`path_is_relative: false`), matching how the Iceberg mirror stores all other files — not the DuckLake primitive's relative-path convention.

**Placeholder scan:** every code step shows the code; every command shows the expected signal. Test bodies that copy fixture helpers from a named existing file (`iceberg_overwrite.rs`, `flight_roundtrip.rs`, `wire.rs`) cite the exact source to mirror — these are concrete patterns, not placeholders.

**Type consistency:** `compact_table(pool, table, expire: &[String], write: &[DataFile]) -> Result<Option<SnapshotId>>` is used identically by Task A1 (definition), A2 path (shared machinery), and B3 (engine handler). `GrpcQueueClient::{list_files -> Vec<FileRef>, compact_table -> Option<i64>}` defined in B2 and consumed in B6. `WriteStore { store, root_url }` defined in B5 and consumed in B6. `CompactJob { schema, name }` / `COMPACT_JOB_KIND` defined in B1, consumed in B4 and B6. `WriteMode::Compact { expire_paths }` defined in A1, consumed in A1/A2.
