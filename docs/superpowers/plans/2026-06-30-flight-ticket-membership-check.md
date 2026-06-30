# Flight `do_get` Ticket Membership Check Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the engine's Arrow Flight `do_get` reject a file-ticket whose named paths are not in the target table's live-snapshot file set, with `Status::invalid_argument`.

**Architecture:** Add a pure, unit-testable helper `all_in_live_set(live, requested) -> Result<(), ()>` to `engine::flight`. In `do_get`'s file-ticket branch, before reading any bytes, fetch the table's live file set via the already-held `serving_catalog` (`current_snapshot` + `files_with_stats`), build a `HashSet<String>` of paths, and run the helper. An unknown path (or unknown table) maps to `Status::invalid_argument` with a non-leaky message. `read_files_as_batches` stays an unchanged pure byte reader — authorization lives at the wire boundary.

**Tech Stack:** Rust 2024, tonic/arrow-flight, `control_plane_postgres::iceberg_catalog::IcebergCatalog`, buck2 `rust_test` / `loom_fixture_test` targets.

## Global Constraints

- **Tests are `rust_test` integration targets only — NOT inline `#[cfg(test)]` modules.** Each test file is its own target in `src/services/engine/BUCK`. The `no-inline-tests` prek hook fails on any `#[test]`/`#[tokio::test]` inside `src/**`.
- **Fixture-backed tests must use `loom_fixture_test`** (already loaded in `src/services/engine/BUCK` via `load("//src/control-plane/postgres:defs.bzl", "loom_fixture_test")`), not a bare `rust_test`, or they route to RE and fail as root. The pure-logic unit test uses a plain `rust_test`.
- **Clippy is strict** (pedantic + restriction enforced on production code): no `unwrap`/`expect`/`panic`/indexing in `src/**`. The new helper must avoid those. Test code is exempt from the panic-safety lints via the wrappers.
- **Run the suite** with `buck2 test //src/...`. Don't pipe `buck2 test` through `tail`/`head`; redirect to a file and grep it: `buck2 test //src/services/engine/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`.
- **Conventional Commits** are enforced on commit messages (commit-msg hook). Use `fix(iceberg): ...` / `test(iceberg): ...` style.
- **Markdown lint:** any `.md` file must end with exactly one trailing newline and no trailing whitespace.

---

### Task 1: Pure `all_in_live_set` helper + its unit test

**Files:**
- Modify: `src/services/engine/src/flight.rs` (add `use std::collections::HashSet;` near the existing `use` block, and add the `pub fn all_in_live_set`)
- Create: `src/services/engine/tests/flight_membership_helper.rs`
- Modify: `src/services/engine/BUCK` (add a `rust_test` target for the new test file)

**Interfaces:**
- Produces: `engine::flight::all_in_live_set(live: &std::collections::HashSet<String>, requested: &[String]) -> Result<(), ()>` — `Ok(())` iff every element of `requested` is in `live`; `Err(())` on the first path not in `live`. Pure, no I/O. An empty `requested` is `Ok(())` (vacuous).

- [ ] **Step 1: Write the failing test**

Create `src/services/engine/tests/flight_membership_helper.rs`:

```rust
//! Unit test for `engine::flight::all_in_live_set`: the pure membership check
//! the Flight `do_get` file-ticket branch uses to reject paths outside a
//! table's live snapshot. No fixture / no wire — pure logic.

use std::collections::HashSet;

use engine::flight::all_in_live_set;

fn live(paths: &[&str]) -> HashSet<String> {
    paths.iter().map(|p| (*p).to_string()).collect()
}

#[test]
fn subset_passes() {
    let live = live(&["file://a/1.parquet", "file://a/2.parquet"]);
    let req = vec!["file://a/1.parquet".to_string()];
    assert!(all_in_live_set(&live, &req).is_ok());
}

#[test]
fn full_set_passes() {
    let live = live(&["file://a/1.parquet", "file://a/2.parquet"]);
    let req = vec![
        "file://a/1.parquet".to_string(),
        "file://a/2.parquet".to_string(),
    ];
    assert!(all_in_live_set(&live, &req).is_ok());
}

#[test]
fn empty_requested_passes() {
    let live = live(&["file://a/1.parquet"]);
    assert!(all_in_live_set(&live, &[]).is_ok());
}

#[test]
fn disjoint_path_fails() {
    let live = live(&["file://a/1.parquet"]);
    let req = vec!["file://b/9.parquet".to_string()];
    assert!(all_in_live_set(&live, &req).is_err());
}

#[test]
fn superset_fails() {
    let live = live(&["file://a/1.parquet"]);
    let req = vec![
        "file://a/1.parquet".to_string(),
        "file://b/9.parquet".to_string(),
    ];
    assert!(all_in_live_set(&live, &req).is_err());
}
```

- [ ] **Step 2: Wire the BUCK target, run the test, verify it FAILS to build**

Add to `src/services/engine/BUCK` (a plain `rust_test`, pure logic — runs on RE, no fixture):

```python
rust_test(
    name = "flight-membership-helper",
    crate = "flight_membership_helper",
    srcs = ["tests/flight_membership_helper.rs"],
    crate_root = "tests/flight_membership_helper.rs",
    deps = [
        ":engine",
    ],
)
```

Run: `buck2 test //src/services/engine:flight-membership-helper > /tmp/t1.log 2>&1; grep -E "error\[|cannot find|Tests finished|FAIL" /tmp/t1.log`
Expected: FAIL — `cannot find function `all_in_live_set` in module `engine::flight`` (the helper does not exist yet).

- [ ] **Step 3: Implement the helper**

In `src/services/engine/src/flight.rs`, add to the imports near the top (after the existing `use std::sync::Arc;`):

```rust
use std::collections::HashSet;
```

Add this free function (place it just above the `FlightDataService` struct, or just below the `FlightTicketReq` impl at the bottom — keep it module-level and `pub`):

```rust
/// Pure membership check for the Flight file-ticket guard: `Ok(())` iff every
/// `requested` path is present in `live` (the table's live-snapshot file set).
/// `Err(())` on the first path that is not live. No I/O — the catalog round-trip
/// that builds `live` stays in `do_get`. An empty `requested` passes vacuously.
///
/// Both `live` and `requested` are mirror path strings in the **same encoding**
/// (absolute, as stored by the iceberg mirror), so an exact-string `HashSet`
/// compare is correct — no path normalization is needed.
#[must_use = "the membership result must be mapped to a Status"]
pub fn all_in_live_set(live: &HashSet<String>, requested: &[String]) -> Result<(), ()> {
    if requested.iter().all(|p| live.contains(p)) {
        Ok(())
    } else {
        Err(())
    }
}
```

- [ ] **Step 4: Run the test, verify it PASSES**

Run: `buck2 test //src/services/engine:flight-membership-helper > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t1.log`
Expected: PASS — `Tests finished: Pass 5. Fail 0.`

- [ ] **Step 5: Commit**

```bash
git add src/services/engine/src/flight.rs src/services/engine/tests/flight_membership_helper.rs src/services/engine/BUCK
git commit -m "feat(iceberg): add pure all_in_live_set helper for Flight ticket guard"
```

---

### Task 2: Enforce the guard in `do_get`'s file-ticket branch

**Files:**
- Modify: `src/services/engine/src/flight.rs` (the file-ticket branch of `do_get`, ~lines 135-145, plus the `control_plane_core::Catalog` import for `current_snapshot`)

**Interfaces:**
- Consumes: `engine::flight::all_in_live_set` (Task 1); `self.serving_catalog: IcebergCatalog` with `current_snapshot(&TableRef) -> Result<Snapshot>` (the `control_plane_core::Catalog` trait method) and the inherent `files_with_stats(&TableRef, SnapshotId) -> Result<Vec<FileWithStats>>` (each `FileWithStats` has `path: String`).
- Produces: a `do_get` file-ticket branch that returns `Status::invalid_argument` for any ticket path outside the table's live snapshot (and for an unknown table), before any bytes are read.

- [ ] **Step 1: Add the `Catalog` trait import**

`current_snapshot` is a trait method on `control_plane_core::Catalog`, so the trait must be in scope. In `src/services/engine/src/flight.rs`, change:

```rust
use control_plane_core::TableRef;
```

to:

```rust
use control_plane_core::{Catalog, TableRef};
```

- [ ] **Step 2: Insert the guard in the file-ticket branch**

In `do_get`, the current file-ticket branch reads:

```rust
        // File-ticket data plane (existing): a JSON `FlightTicket` naming data files.
        let req = FlightTicketReq::decode(ticket)?;
        let table = TableRef {
            schema: req.schema,
            name: req.name,
        };
        // The schema is discarded here on purpose: FlightDataEncoderBuilder
        // derives it from the batches below, so we don't pass it explicitly.
        let (_schema, batches) = read_files_as_batches(&self.catalog, &table, &req.files)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
```

Replace it with (insert the guard block between `let table = ...` and the `read_files_as_batches` call):

```rust
        // File-ticket data plane (existing): a JSON `FlightTicket` naming data files.
        let req = FlightTicketReq::decode(ticket)?;
        let table = TableRef {
            schema: req.schema,
            name: req.name,
        };

        // Defense-in-depth: every ticket-named path must belong to the table's
        // live snapshot. The mirror stores absolute `file://` paths, so an
        // unchecked ticket could otherwise name another table's file (or any path
        // FileIO can resolve). Cross-check against the live file set before reading
        // any bytes. An unknown table is itself a bad ticket (no-leak: we never
        // reveal existence beyond "rejected").
        let snap = self
            .serving_catalog
            .current_snapshot(&table)
            .await
            .map_err(|_| Status::invalid_argument("flight ticket names an unknown table"))?;
        let live: HashSet<String> = self
            .serving_catalog
            .files_with_stats(&table, snap.id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .into_iter()
            .map(|f| f.path)
            .collect();
        all_in_live_set(&live, &req.files).map_err(|()| {
            // Do not echo the offending path — that would confirm what paths exist.
            Status::invalid_argument("flight ticket names a file outside the table's live snapshot")
        })?;

        // The schema is discarded here on purpose: FlightDataEncoderBuilder
        // derives it from the batches below, so we don't pass it explicitly.
        let (_schema, batches) = read_files_as_batches(&self.catalog, &table, &req.files)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
```

- [ ] **Step 3: Verify the engine library still builds + clippy clean**

Run: `buck2 build //src/services/engine:engine '//src/services/engine:engine[clippy.txt]' > /tmp/t2.log 2>&1; grep -E "error|BUILD SUCCEEDED|Failed" /tmp/t2.log; cat buck-out/*/gen/src/services/engine/__engine__/clippy.txt 2>/dev/null || true`
Expected: builds clean; the `[clippy.txt]` sub-target is empty (no lint findings). (If the cat path differs, the key signal is the build SUCCEEDED with no `error`.)

- [ ] **Step 4: Commit**

```bash
git add src/services/engine/src/flight.rs
git commit -m "fix(iceberg): reject Flight tickets naming files outside the live snapshot"
```

---

### Task 3: Live-engine integration test (positive + negative)

**Files:**
- Create: `src/services/engine/tests/flight_ticket_membership.rs`
- Modify: `src/services/engine/BUCK` (add a `loom_fixture_test` target)

**Interfaces:**
- Consumes: the public test harness pieces already used by `tests/flight_sql.rs` — `engine::flight::FlightDataService`, `control_plane_postgres::fixture::{IcebergWriter, PgFixture}`, `control_plane_postgres::iceberg_catalog::IcebergCatalog`, `engine_wire::flight::{FlightTicket, FlightTableClient}`, and the `SqlCatalog` builder. `IcebergWriter::seed(ns, name, &[(name,type,nullable)], &[rows_per_batch]) -> Vec<i64>` lands real Parquet files. `IcebergCatalog::current_snapshot` (trait `Catalog`) + `files_with_stats` capture a table's live file paths.
- Produces: a regression test that fails on `main` for the negative (cross-table) case and passes the positive (own-files) case.

- [ ] **Step 1: Write the test file**

Create `src/services/engine/tests/flight_ticket_membership.rs`:

```rust
//! Engine-level Flight file-ticket membership guard: boot a `FlightDataService`
//! over a UDS, land two tables A and B, and drive `do_get` with `FlightTicket`s:
//!   * positive — A's ticket naming A's live files streams A's rows;
//!   * negative — A's ticket naming B's file path is rejected (no bytes), the
//!     case that fails on `main` today.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, Int64Array};
use arrow_flight::flight_service_server::FlightServiceServer;
use control_plane_core::{Catalog, TableRef};
use control_plane_postgres::fixture::{IcebergWriter, PgFixture};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use engine::flight::FlightDataService;
use engine_wire::flight::{FlightTableClient, FlightTicket};
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;

async fn make_catalog(dsn: String, warehouse: &str) -> SqlCatalog {
    let mut props = std::collections::HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), dsn);
    props.insert(
        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
        format!("file://{warehouse}"),
    );
    SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("loom", props)
        .await
        .expect("catalog")
}

async fn spawn_flight(fx: &PgFixture, db: &str, warehouse: &str) -> (tempfile::TempDir, String) {
    let sock_dir = tempfile::tempdir().expect("socket dir");
    let sock_path = sock_dir.path().join("engine.sock");
    let sock_str = sock_path.to_string_lossy().to_string();

    let pool = fx.pool_for(db).await;
    let file_catalog = make_catalog(fx.pg_dsn(db), warehouse).await;
    let svc = FlightDataService {
        catalog: file_catalog,
        serving_catalog: IcebergCatalog::new(pool.clone()),
        serving_store: None,
        pool,
    };

    let listener = tokio::net::UnixListener::bind(&sock_path).expect("bind uds");
    let incoming = UnixListenerStream::new(listener);
    tokio::spawn(async move {
        drop(
            Server::builder()
                .add_service(FlightServiceServer::new(svc))
                .serve_with_incoming(incoming)
                .await,
        );
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    (sock_dir, sock_str)
}

/// Capture a table's live-snapshot data-file paths from the mirror.
async fn live_files(pool: &sqlx::PgPool, table: &TableRef) -> Vec<String> {
    let cat = IcebergCatalog::new(pool.clone());
    let snap = cat.current_snapshot(table).await.expect("current_snapshot");
    cat.files_with_stats(table, snap.id)
        .await
        .expect("files_with_stats")
        .into_iter()
        .map(|f| f.path)
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejects_ticket_naming_files_outside_live_snapshot() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let wh = tempfile::tempdir().expect("warehouse");

    let cols = vec![("id".to_string(), "long".to_string(), false)];
    let writer = IcebergWriter::new(pool.clone(), dsn);
    // Table A: one Parquet file with 3 rows (ids 0,1,2).
    writer.seed("main", "a", &cols, &[3]).await;
    // Table B: one Parquet file with 2 rows.
    writer.seed("main", "b", &cols, &[2]).await;

    let table_a = TableRef { schema: "main".into(), name: "a".into() };
    let table_b = TableRef { schema: "main".into(), name: "b".into() };
    let a_files = live_files(&pool, &table_a).await;
    let b_files = live_files(&pool, &table_b).await;
    assert!(!a_files.is_empty(), "table A must have at least one live file");
    assert!(!b_files.is_empty(), "table B must have at least one live file");

    let (_sock_dir, sock) = spawn_flight(&fx, &db, &wh.path().display().to_string()).await;
    let client = FlightTableClient::connect(&sock).await.expect("connect");

    // Positive: A's ticket naming A's own live files streams A's rows.
    let ok = client
        .fetch(FlightTicket {
            schema: "main".into(),
            name: "a".into(),
            files: a_files.clone(),
        })
        .await
        .expect("positive fetch must succeed");
    let mut ids = Vec::new();
    for b in &ok {
        let col = b.column(0).as_any().downcast_ref::<Int64Array>().expect("i64");
        for i in 0..col.len() {
            ids.push(col.value(i));
        }
    }
    ids.sort_unstable();
    assert_eq!(ids, vec![0, 1, 2], "positive ticket streams A's rows");

    // Negative: A's ticket naming B's file path is rejected (the case that fails on main).
    let err = client
        .fetch(FlightTicket {
            schema: "main".into(),
            name: "a".into(),
            files: b_files.clone(),
        })
        .await;
    let msg = format!("{}", err.expect_err("negative ticket must be rejected"));
    assert!(
        msg.contains("outside the table's live snapshot"),
        "rejection must come from the membership guard, got: {msg}"
    );

    // Negative: a bogus path is likewise rejected.
    let err2 = client
        .fetch(FlightTicket {
            schema: "main".into(),
            name: "a".into(),
            files: vec!["file:///nope/0.parquet".to_string()],
        })
        .await;
    let msg2 = format!("{}", err2.expect_err("bogus ticket must be rejected"));
    assert!(
        msg2.contains("outside the table's live snapshot"),
        "bogus ticket must hit the membership guard, got: {msg2}"
    );
}
```

- [ ] **Step 2: Wire the BUCK target**

Add to `src/services/engine/BUCK` (a `loom_fixture_test` — boots hermetic Postgres + a local warehouse; mirror the `flight-sql` target's deps and add `engine-serving` is NOT needed, but add `sqlx` for the `PgPool` type and `arrow-array` for the row decode):

```python
loom_fixture_test(
    name = "flight-ticket-membership",
    crate = "flight_ticket_membership",
    srcs = ["tests/flight_ticket_membership.rs"],
    crate_root = "tests/flight_ticket_membership.rs",
    deps = [
        ":engine",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//src/services/engine-wire:engine-wire",
        "//third-party:arrow-array",
        "//third-party:arrow-flight",
        "//third-party:iceberg",
        "//third-party:sqlx",
        "//third-party:tempfile",
        "//third-party:tokio",
        "//third-party:tokio-stream",
        "//third-party:tonic",
    ],
)
```

- [ ] **Step 3: Run the integration test, verify it PASSES**

Run: `buck2 test //src/services/engine:flight-ticket-membership > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL|error\[|panicked" /tmp/t3.log`
Expected: PASS — `Tests finished: Pass 1. Fail 0.`

If it fails to compile on an unused dep (e.g. `uuid`), drop the offending dep from the BUCK target; if it fails on a missing dep, add it. The deps list mirrors `flight-sql` plus `sqlx`/`arrow-array`.

- [ ] **Step 4: Commit**

```bash
git add src/services/engine/tests/flight_ticket_membership.rs src/services/engine/BUCK
git commit -m "test(iceberg): e2e guard rejects Flight ticket naming non-live files"
```

---

### Task 4: Close the register item + full-suite verification

**Files:**
- Modify: `docs/ISSUES.md` (close `iss-flight-ticket-path-unchecked`)

- [ ] **Step 1: Run the engine test suite to confirm no regression**

Run: `buck2 test //src/services/engine/... > /tmp/all.log 2>&1; grep -E "Tests finished|FAIL" /tmp/all.log`
Expected: all engine tests pass (`Fail 0`).

- [ ] **Step 2: Update the register via loom-docs-update**

Invoke the `loom-docs-update` skill (or edit directly): mark `iss-flight-ticket-path-unchecked` in `docs/ISSUES.md` `- [ ]`→`- [x]`, set `status:open`→`status:fixed`, and set `pr:-`→`pr:#<N>` once the PR number is known (the PR step can backfill this). Run `bash tools/docs.sh validate` and confirm it passes.

- [ ] **Step 3: Commit**

```bash
git add docs/ISSUES.md
git commit -m "docs(iceberg): close iss-flight-ticket-path-unchecked"
```

---

## Notes for the implementer

- The two negative assertions check the error **message** (not the gRPC code) because `FlightTableClient::fetch` flattens `tonic::Status` to a string via `client::be`. The guard's message string is unique to this code path, so a message-substring match reliably distinguishes the membership rejection from any other error (e.g. an internal read error).
- Empty `req.files` stays valid by design (`all_in_live_set` returns `Ok(())` vacuously) — it yields the table schema and zero batches, unchanged from today. No test is required for it, but it is covered by the `empty_requested_passes` unit case.
- Out of scope (do **not** touch): the k-NN ticket branch, the Flight SQL branch, `read_files_as_batches`, the `FlightTicket` wire shape, and the compaction caller — all per the spec's Scope section.
