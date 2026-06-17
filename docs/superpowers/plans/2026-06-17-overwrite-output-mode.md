# Overwrite Output Mode Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a selectable transform output mode — `Append` (default, today's behavior) or `Overwrite` (replace the output table's contents) — backed by a new DuckLake-faithful `Tx::replace_files` primitive.

**Architecture:** `replace_files` expires a table's currently-live `ducklake_data_file` rows at the new snapshot (`end_snapshot = new_id`; older snapshots still time-travel to them), resets table-level stats, and writes the new files via the existing `write_data_file` path. `run_transform` branches `append_files` vs `replace_files` on a new `OutputMode`; the wire payloads gain an optional `output_mode` (default `Append`).

**Tech Stack:** Rust, buck2, async-trait, sqlx compile-time macros (postgres), DuckLake catalog, DataFusion, DuckDB serving.

**Design spec:** `docs/superpowers/specs/2026-06-17-overwrite-output-mode-design.md`

**Build/test reminders (loom-specific):**
- Tests are integration `rust_test`/`loom_fixture_test` targets ONLY — never inline `#[cfg(test)]`/`#[test]` in `src/**` (the `no-inline-tests` prek hook fails the build).
- Run a target: `buck2 test //src/<path>:<target> > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL|error\[" /tmp/t.log`. NEVER pipe `buck2 test` through `tail`/`head` (it stalls). NEVER run two `buck2` invocations concurrently.
- After ANY change to postgres SQL, regenerate the `.sqlx` cache: `./tools/sqlx-prepare.sh`, then commit the `.sqlx` change (the `sqlx-cache-check` test enforces freshness).
- Commit trailer (every commit): `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`. Conventional-commit subject required.

---

## File Structure

| File | Responsibility | Change |
|------|----------------|--------|
| `src/control-plane/core/src/transaction.rs` | the `Tx` trait | add `replace_files` method |
| `src/control-plane/memory/src/transaction.rs` | in-memory `Tx` | `staged_replacements` + `replace_files` + commit replace loop |
| `src/control-plane/postgres/src/transaction.rs` | postgres `Tx` | `staged_replacements` + `replace_files` + commit trigger |
| `src/control-plane/postgres/src/snapshot.rs` (+ `.sqlx`) | DuckLake writer | `commit_snapshot` replace loop (expire + reset + write) |
| `src/control-plane/testkit/src/lib.rs` | adapter contracts | `snapshot_replace_contract` |
| `src/control-plane/memory/tests/snapshot.rs` | memory contract caller | call the replace contract |
| `src/control-plane/postgres/tests/snapshot_conformance.rs` | pg contract caller | call the replace contract |
| `src/control-plane/postgres/tests/snapshot_replace.rs` | NEW — focused pg replace test | create |
| `src/control-plane/postgres/BUCK` | pg test wiring | add `snapshot-replace` target |
| `src/services/transform/src/run.rs` | transform primitive | `OutputMode` + append/replace branch |
| `src/services/transform/src/lib.rs` | transform exports | export `OutputMode` |
| `src/services/transform/tests/output_mode.rs` | NEW — serde unit test | create |
| `src/services/transform/src/typed.rs` | typed transform | thread `output_mode` param |
| `src/services/transform/src/handler.rs` | wire payloads | `output_mode` on both payloads |
| `src/services/transform/tests/overwrite_e2e.rs` | NEW — fixture e2e | create |
| `src/services/transform/BUCK` | transform test wiring | add `output-mode`, `overwrite-e2e` targets |

---

## Task 1: `Tx::replace_files` primitive (core + memory + postgres + testkit)

Adding an abstract trait method requires every implementor to satisfy it in one change to keep the build green; the testkit contract and a focused postgres test are the tests. This task is atomic across core/memory/postgres/testkit + the committed sqlx cache.

**Files:** core `transaction.rs`; memory `transaction.rs`; postgres `transaction.rs` + `snapshot.rs` (+ `.sqlx`); testkit `lib.rs`; memory & postgres snapshot test callers; new `snapshot_replace.rs` + BUCK.

- [ ] **Step 1: Write the failing testkit contract**

In `src/control-plane/testkit/src/lib.rs`, add a new contract function immediately AFTER `snapshot_commit_contract` (after its closing brace). It exercises append→replace and asserts the current snapshot lists only the replacement while the prior snapshot still time-travels to the old file:

```rust
/// Contract for `Tx::replace_files` (overwrite). Append a file, then replace it: the
/// current snapshot lists ONLY the replacement (stats reflect the new file, not the
/// sum), and the prior snapshot still time-travels to the original file. `cp` must be
/// freshly empty (postgres: a DuckLake catalog must be bootstrapped first).
pub async fn snapshot_replace_contract<C>(cp: &C)
where
    C: control_plane_core::ControlPlane + control_plane_core::Catalog,
{
    use control_plane_core::{ColumnSpec, DataFile, FileFormat, PageReq, TableRef};
    let t = TableRef {
        schema: "main".into(),
        name: "replace_me".into(),
    };
    let file = |path: &str, rows: i64| DataFile {
        path: path.into(),
        path_is_relative: true,
        file_format: FileFormat::Parquet,
        record_count: rows,
        file_size_bytes: rows * 16,
        column_stats: vec![],
        parquet_footer_size: Some(10),
    };

    // create + append a.parquet (3 rows).
    let mut tx = cp.begin().await.unwrap();
    tx.create_table(
        &t,
        &[ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        }],
    )
    .await
    .unwrap();
    tx.append_files(&t, &[file("a.parquet", 3)]).await.unwrap();
    let s1 = tx.commit().await.unwrap().expect("append yields a snapshot");

    let at1 = cp.files(&t, s1, PageReq::unbounded()).await.unwrap();
    assert_eq!(at1.len(), 1, "one file live after append");
    assert_eq!(at1.items[0].path, "a.parquet");

    // replace with b.parquet (5 rows).
    let mut tx = cp.begin().await.unwrap();
    tx.replace_files(&t, &[file("b.parquet", 5)]).await.unwrap();
    let s2 = tx.commit().await.unwrap().expect("replace yields a snapshot");

    // current snapshot lists ONLY the replacement.
    let cur = cp.current_snapshot(&t).await.unwrap();
    assert_eq!(cur.id, s2, "replace advanced the current snapshot");
    let at2 = cp.files(&t, s2, PageReq::unbounded()).await.unwrap();
    assert_eq!(at2.len(), 1, "replace leaves exactly the new file live");
    assert_eq!(at2.items[0].path, "b.parquet");
    assert_eq!(at2.items[0].record_count, 5, "stats reflect the new file only");

    // time travel: the prior snapshot still lists the original file.
    let back = cp.files(&t, s1, PageReq::unbounded()).await.unwrap();
    assert_eq!(back.len(), 1, "prior snapshot retains its file (time travel)");
    assert_eq!(back.items[0].path, "a.parquet");
}
```

- [ ] **Step 2: Wire the contract into both adapter test callers**

In `src/control-plane/memory/tests/snapshot.rs`, add a second test:

```rust
#[tokio::test]
async fn snapshot_replace_contract() {
    let cp = MemoryControlPlane::new(std::time::Duration::from_millis(300));
    control_plane_testkit::snapshot_replace_contract(&cp).await;
}
```

In `src/control-plane/postgres/tests/snapshot_conformance.rs`, add a second test:

```rust
#[tokio::test]
async fn snapshot_replace_contract() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    DuckLakeWriter::new(fx.socket_path(), &db).bootstrap().await;
    control_plane_testkit::snapshot_replace_contract(&cp).await;
}
```

- [ ] **Step 3: Add the trait method**

In `src/control-plane/core/src/transaction.rs`, in the `Tx` trait, add immediately after `append_files` (line 50):

```rust
    /// Replace the table's live data files with `files` in this snapshot: the
    /// currently-live files are expired at the new snapshot (still visible at older
    /// snapshots — time travel preserved), and `files` become the table's live
    /// contents. Staged; applied at commit.
    async fn replace_files(&mut self, table: &TableRef, files: &[DataFile]) -> Result<()>;
```

- [ ] **Step 4: Memory impl**

In `src/control-plane/memory/src/transaction.rs`:

(a) Add a field to `MemoryTx` (after `staged_files`, line 23):

```rust
    pub(crate) staged_replacements: Vec<(TableRef, Vec<DataFile>)>,
```

(b) Add the `replace_files` method (after `append_files`, line 141):

```rust
    async fn replace_files(&mut self, table: &TableRef, files: &[DataFile]) -> Result<()> {
        self.staged_replacements.push((table.clone(), files.to_vec()));
        Ok(())
    }
```

(c) In `commit`, include replacements in the catalog-op trigger. Change the `has_catalog_ops` line (line 48) to:

```rust
        let has_catalog_ops = !self.staged_tables.is_empty()
            || !self.staged_files.is_empty()
            || !self.staged_replacements.is_empty();
```

(d) In `commit`, after the staged-file-append loop (the `for (table, files) in self.staged_files { ... }` block ends at line 109), add the replacement loop INSIDE the same `cat` lock scope (before the closing brace at line 110):

```rust
            // Apply staged file replacements: expire the table's live files at a new
            // snapshot, then add the replacements live at that snapshot.
            for (table, files) in self.staged_replacements {
                use control_plane_core::FileRef;
                let key = (table.schema.clone(), table.name.clone());
                let s = cat.new_snapshot();
                last_snapshot = Some(s);
                if let Some(existing) = cat.files.get_mut(&key) {
                    for f in existing.iter_mut() {
                        if f.end.is_none() {
                            f.end = Some(s);
                        }
                    }
                }
                for file in files {
                    cat.files.entry(key.clone()).or_default().push(Versioned {
                        begin: s,
                        end: None,
                        val: FileRef {
                            path: file.path,
                            record_count: file.record_count,
                            file_size_bytes: file.file_size_bytes,
                        },
                    });
                }
            }
```

(e) Find where `MemoryTx` is constructed (search the crate: `grep -rn "MemoryTx {" src/control-plane/memory/src`) and add `staged_replacements: Vec::new(),` to the struct literal, alongside `staged_files: Vec::new(),`.

- [ ] **Step 5: Postgres staging**

In `src/control-plane/postgres/src/transaction.rs`:

(a) Add a field to `PgTx` (after `staged_files`, line 14):

```rust
    pub(crate) staged_replacements: Vec<(TableRef, Vec<DataFile>)>,
```

(b) Change the commit trigger (line 21) to include replacements, and pass them to `commit_snapshot`:

```rust
        if !self.staged_tables.is_empty()
            || !self.staged_files.is_empty()
            || !self.staged_replacements.is_empty()
        {
            let id = crate::snapshot::commit_snapshot(
                &mut self.tx,
                &self.staged_tables,
                &self.staged_files,
                &self.staged_replacements,
            )
            .await?;
            self.tx.commit().await.map_err(backend)?;
            return Ok(Some(id));
        }
```

(c) Add the `replace_files` method (after `append_files`, line 58):

```rust
    async fn replace_files(&mut self, table: &TableRef, files: &[DataFile]) -> Result<()> {
        self.staged_replacements.push((table.clone(), files.to_vec()));
        Ok(())
    }
```

(d) Find where `PgTx` is constructed (`grep -rn "PgTx {" src/control-plane/postgres/src`) and add `staged_replacements: Vec::new(),` to the struct literal.

- [ ] **Step 6: Postgres `commit_snapshot` replace loop**

In `src/control-plane/postgres/src/snapshot.rs`:

(a) Change the `commit_snapshot` signature to accept replacements (add the parameter after `staged_files`):

```rust
pub(crate) async fn commit_snapshot(
    tx: &mut Transaction<'_, Postgres>,
    staged_tables: &[(TableRef, Vec<ColumnSpec>)],
    staged_files: &[(TableRef, Vec<DataFile>)],
    staged_replacements: &[(TableRef, Vec<DataFile>)],
) -> Result<SnapshotId> {
```

Also update the `#[tracing::instrument(...)]` attribute above it: add `staged_replacements` to `skip(...)` and add `replacements = staged_replacements.len()` to `fields(...)`.

(b) Add the replace loop immediately AFTER the append loop (after the `for (table, files) in staged_files { ... }` block that ends at line 95), and BEFORE the `ducklake_snapshot` insert (line 99):

```rust
    // (3b) replace loop: for each replacement, expire the table's currently-live data
    //      files at the new snapshot (time travel preserved), reset table-level stats so
    //      they recompute from the new files (next_row_id is NOT reset — row-ids must stay
    //      unique across snapshots), then write the new files via the same path as append.
    for (table, files) in staged_replacements {
        let table_id = resolve_table_id(tx, table).await?;
        sqlx::query!(
            "update ducklake_data_file set end_snapshot = $1 \
             where table_id = $2 and end_snapshot is null",
            new_snapshot_id,
            table_id,
        )
        .execute(&mut **tx)
        .await
        .map_err(backend)?;
        sqlx::query!(
            "update ducklake_table_stats set record_count = 0, file_size_bytes = 0 \
             where table_id = $1",
            table_id,
        )
        .execute(&mut **tx)
        .await
        .map_err(backend)?;
        sqlx::query!(
            "delete from ducklake_table_column_stats where table_id = $1",
            table_id,
        )
        .execute(&mut **tx)
        .await
        .map_err(backend)?;
        for file in files {
            let data_file_id = next_file_id;
            next_file_id += 1;
            write_data_file(tx, table_id, new_snapshot_id, data_file_id, file).await?;
        }
    }
```

(c) Add the replacement change segments. After the existing `staged_files` segment loop (the `for (table, files) in staged_files { ... segments.push("inserted_into_table:...") }` block, lines 115-121), add:

```rust
    for (table, files) in staged_replacements {
        let table_id = resolve_table_id(tx, table).await?;
        segments.push(format!("deleted_from_table:{table_id}"));
        if !files.is_empty() {
            segments.push(format!("inserted_into_table:{table_id}"));
        }
    }
```

- [ ] **Step 7: Regenerate the sqlx cache**

Run: `./tools/sqlx-prepare.sh > /tmp/sqlx1.log 2>&1; grep -nE "error|query data written|Finished|panic" /tmp/sqlx1.log`
Expected: success; new `.sqlx/query-*.json` files for the three new queries (expire UPDATE, stats-reset UPDATE, column-stats DELETE). Boots a hermetic postgres+duckdb; takes a couple minutes.

- [ ] **Step 8: Run the contracts + focused test (after Step 9 adds it)**

(Defer running until the focused pg test is added in Step 9 so one buck2 run covers all.)

- [ ] **Step 9: Add the focused postgres replace test**

Create `src/control-plane/postgres/tests/snapshot_replace.rs`:

```rust
use control_plane_core::{
    Catalog, ColumnStat, ControlPlane, DataFile, FileFormat, PageReq, StatValue, TableRef,
};
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};

fn data_file(path: &str, rows: i64) -> DataFile {
    DataFile {
        path: path.into(),
        path_is_relative: true,
        file_format: FileFormat::Parquet,
        record_count: rows,
        file_size_bytes: rows * 16,
        column_stats: vec![ColumnStat {
            column_name: "id".into(),
            null_count: 0,
            column_size_bytes: rows * 8,
            min: Some(StatValue::I64(0)),
            max: Some(StatValue::I64(rows - 1)),
        }],
        parquet_footer_size: Some(120),
    }
}

// Append a data file, then replace_files: at the new snapshot only the replacement is
// live and table stats reflect it alone; at the prior snapshot the original file is
// still live (time travel via end_snapshot).
#[tokio::test]
async fn replace_files_expires_old_and_preserves_time_travel() {
    let fixture = PgFixture::start();
    let (cp, db) = fixture.fresh_db().await;
    let writer = DuckLakeWriter::new(fixture.socket_path(), &db);
    writer
        .seed("main", "t", &[("id".into(), "BIGINT".into(), false)], &[])
        .await;

    let t = TableRef {
        schema: "main".into(),
        name: "t".into(),
    };

    // append a.parquet (10 rows).
    let mut tx = cp.begin().await.unwrap();
    tx.append_files(&t, &[data_file("a.parquet", 10)])
        .await
        .unwrap();
    let s1 = tx.commit().await.unwrap().expect("append snapshot");

    // replace with b.parquet (4 rows).
    let mut tx = cp.begin().await.unwrap();
    tx.replace_files(&t, &[data_file("b.parquet", 4)])
        .await
        .unwrap();
    let s2 = tx.commit().await.unwrap().expect("replace snapshot");

    // current snapshot: only b.parquet, 4 rows.
    let cur = cp.current_snapshot(&t).await.unwrap();
    assert_eq!(cur.id, s2);
    let now = cp.files(&t, s2, PageReq::unbounded()).await.unwrap();
    assert_eq!(now.len(), 1, "only the replacement is live");
    assert_eq!(now.items[0].path, "b.parquet");
    assert_eq!(now.items[0].record_count, 4);

    // prior snapshot: a.parquet still live (time travel).
    let before = cp.files(&t, s1, PageReq::unbounded()).await.unwrap();
    assert_eq!(before.len(), 1, "prior snapshot retains the original file");
    assert_eq!(before.items[0].path, "a.parquet");
    assert_eq!(before.items[0].record_count, 10);
}
```

Add the BUCK target in `src/control-plane/postgres/BUCK` (mirror `snapshot-append`, lines 230-237):

```python
loom_fixture_test(
    name = "snapshot-replace",
    crate = "snapshot_replace",
    srcs = ["tests/snapshot_replace.rs"],
    crate_root = "tests/snapshot_replace.rs",
    duckdb = True,
    deps = [":postgres", "//src/control-plane/core:core", "//third-party:tokio"],
)
```

- [ ] **Step 10: Run all Task-1 tests**

Run: `buck2 test //src/control-plane/memory:snapshot //src/control-plane/postgres:snapshot-conformance //src/control-plane/postgres:snapshot-replace //src/control-plane/postgres:sqlx-cache-check > /tmp/t1.log 2>&1; grep -nE "Tests finished|FAIL|error\[" /tmp/t1.log`
Expected: ALL PASS — memory `snapshot` (both contracts), postgres `snapshot-conformance` (both contracts), `snapshot-replace`, and `sqlx-cache-check` green.

- [ ] **Step 11: Commit**

```bash
git add src/control-plane/core/src/transaction.rs src/control-plane/memory/src/transaction.rs src/control-plane/postgres/src/transaction.rs src/control-plane/postgres/src/snapshot.rs src/control-plane/postgres/.sqlx src/control-plane/testkit/src/lib.rs src/control-plane/memory/tests/snapshot.rs src/control-plane/postgres/tests/snapshot_conformance.rs src/control-plane/postgres/tests/snapshot_replace.rs src/control-plane/postgres/BUCK
git commit -m "feat(control-plane): Tx::replace_files overwrite primitive (pg + memory)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: `OutputMode` + `run_transform` append/replace branch

Add the mode enum and branch the commit between `append_files` and `replace_files`. Keep existing construction sites compiling by defaulting them to `Append` (Task 3 wires the real value from the payload).

**Files:** `transform/src/run.rs`, `transform/src/lib.rs`, `transform/src/handler.rs`, `transform/src/typed.rs`, new `transform/tests/output_mode.rs`, `transform/BUCK`.

- [ ] **Step 1: Write the failing serde unit test**

Create `src/services/transform/tests/output_mode.rs`:

```rust
//! `OutputMode` wire contract: lowercase serde, defaults to Append, rejects unknown.

use transform::OutputMode;

#[test]
fn default_is_append() {
    assert_eq!(OutputMode::default(), OutputMode::Append);
}

#[test]
fn deserializes_lowercase_variants() {
    assert_eq!(
        serde_json::from_str::<OutputMode>("\"append\"").unwrap(),
        OutputMode::Append
    );
    assert_eq!(
        serde_json::from_str::<OutputMode>("\"overwrite\"").unwrap(),
        OutputMode::Overwrite
    );
}

#[test]
fn rejects_unknown_value() {
    assert!(serde_json::from_str::<OutputMode>("\"merge\"").is_err());
}

#[test]
fn absent_field_defaults_to_append() {
    #[derive(serde::Deserialize)]
    struct P {
        #[serde(default)]
        output_mode: OutputMode,
    }
    let p: P = serde_json::from_str("{}").unwrap();
    assert_eq!(p.output_mode, OutputMode::Append);
}
```

- [ ] **Step 2: Wire the test target**

In `src/services/transform/BUCK`, add (mirror the `conform` target, lines 42-52):

```python
rust_test(
    name = "output-mode",
    crate = "output_mode",
    srcs = ["tests/output_mode.rs"],
    crate_root = "tests/output_mode.rs",
    edition = "2024",
    deps = [
        ":transform",
        "//third-party:serde",
        "//third-party:serde_json",
    ],
)
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `buck2 test //src/services/transform:output-mode > /tmp/t2.log 2>&1; grep -nE "Tests finished|FAIL|error\[|unresolved" /tmp/t2.log`
Expected: FAIL — `OutputMode` not found in `transform`.

- [ ] **Step 4: Add `OutputMode` + the `TransformRequest` field**

In `src/services/transform/src/run.rs`:

(a) Add near the top, after the imports (before `TransformInput`, around line 17):

```rust
/// Where a transform's result lands relative to the output table's existing contents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OutputMode {
    /// Add the result's files to the table (today's behavior).
    #[default]
    Append,
    /// Replace the table's live contents with the result (older snapshots time-travel).
    Overwrite,
}
```

(b) Add the field to `TransformRequest` (after `conform`, before `lineage`, around line 33):

```rust
    /// Append (default) adds the result to the table; Overwrite replaces its live
    /// contents (expiring the prior files at the new snapshot).
    pub output_mode: OutputMode,
```

(c) Branch the write in `run_transform` step 6. Replace the single `tx.append_files(...)` line (line 155) with:

```rust
    match req.output_mode {
        OutputMode::Append => tx.append_files(req.output, &data_files).await?,
        OutputMode::Overwrite => tx.replace_files(req.output, &data_files).await?,
    }
```

- [ ] **Step 5: Export `OutputMode`**

In `src/services/transform/src/lib.rs`, change the `run` re-export line to include `OutputMode`:

```rust
pub use run::{OutputMode, TransformError, TransformInput, TransformRequest, run_transform};
```

- [ ] **Step 6: Keep existing construction sites compiling (default Append)**

`TransformRequest` is constructed in two places; both must set `output_mode` or the build breaks.

`handler.rs` and `typed.rs` are INSIDE the `transform` crate, so the type is referred to as `crate::run::OutputMode` (NOT `transform::OutputMode`, which does not resolve from within the crate).

In `src/services/transform/src/handler.rs`, the `TransformRequest { inputs, output, sql, conform: None, lineage }` literal (around line 82-88) becomes:

```rust
        TransformRequest {
            inputs: &inputs,
            output: &output,
            sql: &payload.sql,
            conform: None,
            output_mode: crate::run::OutputMode::Append,
            lineage,
        },
```

In `src/services/transform/src/typed.rs`, the `TransformRequest { inputs: &specs, output: &out_type.table, sql, conform: Some(&out_type.properties), lineage }` literal (around line 80-87) gains:

```rust
            output_mode: crate::run::OutputMode::Append,
```

(Task 3 replaces these `Append` literals with the real value.)

- [ ] **Step 7: Run the unit test + build the crate**

Run: `buck2 test //src/services/transform:output-mode > /tmp/t2b.log 2>&1; grep -nE "Tests finished|FAIL|error\[" /tmp/t2b.log`
Expected: PASS (4 tests). The crate compiles with the construction-site updates.

- [ ] **Step 8: Commit**

```bash
git add src/services/transform/src/run.rs src/services/transform/src/lib.rs src/services/transform/src/handler.rs src/services/transform/src/typed.rs src/services/transform/tests/output_mode.rs src/services/transform/BUCK
git commit -m "feat(transform): OutputMode + append/replace branch in run_transform

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Wire `output_mode` through the payloads + the overwrite e2e

Thread the mode from the wire payloads (physical + typed) into `run_transform`, and prove the full path with a fixture e2e: overwrite replaces the output table's rows (DuckDB read-back), a second overwrite replaces again, and time travel to the pre-overwrite snapshot still returns the old rows.

**Files:** `transform/src/handler.rs`, `transform/src/typed.rs`, new `transform/tests/overwrite_e2e.rs`, `transform/BUCK`.

- [ ] **Step 1: Write the failing e2e**

Create `src/services/transform/tests/overwrite_e2e.rs`:

```rust
//! Overwrite output mode e2e: a transform with output_mode=overwrite replaces the output
//! table's contents (DuckDB read-back sees only the new result), a second overwrite
//! replaces again, and a read at the pre-overwrite snapshot still time-travels to the old
//! rows. Drives the real queue -> worker -> transform_handler path.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{
    Catalog, ControlPlane, DatasetRef, EventType, LineageEvent, NewJob, PageReq, Queue, RunId,
    TableRef,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use control_plane_worker::Worker;
use ingest::{MaterializeRequest, materialize};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use time::OffsetDateTime;
use tokio_util::sync::CancellationToken;
use transform::transform_handler;
use uuid::Uuid;

fn tref(s: &str, n: &str) -> TableRef {
    TableRef {
        schema: s.into(),
        name: n.into(),
    }
}

async fn land(
    cp: &PgControlPlane,
    store: &Arc<dyn ObjectStore>,
    table: &TableRef,
    schema: Arc<Schema>,
    batch: RecordBatch,
) {
    let lineage = LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetRef::from(table)],
        payload: serde_json::json!({}),
    };
    materialize(
        cp,
        store.clone(),
        MaterializeRequest {
            table,
            schema,
            batches: &[batch],
            file_prefix: "run-1",
            gate: None,
            lineage,
        },
    )
    .await
    .unwrap();
}

/// Run the worker once over the `transform` queue until the job drains.
async fn drain_transforms(cp: &PgControlPlane, store: &Arc<dyn ObjectStore>) {
    let store_h = store.clone();
    let token = CancellationToken::new();
    let t = token.clone();
    let worker = Worker::new(cp.clone(), "overwrite-test", Duration::from_millis(300))
        .with_poll_interval(Duration::from_millis(50));
    let cp_h: Arc<dyn ControlPlane> = Arc::new(cp.clone());
    let handle = tokio::spawn(async move {
        worker
            .run(&["transform".to_string()], t, move |job| {
                let cp = cp_h.clone();
                let store = store_h.clone();
                async move { transform_handler(cp.as_ref(), store, job).await }
            })
            .await
    });
    tokio::time::sleep(Duration::from_millis(800)).await;
    token.cancel();
    handle.await.unwrap().unwrap();
}

/// Enqueue an overwrite transform that selects all rows of `src` into `out`.
async fn enqueue_overwrite(cp: &PgControlPlane, src: &str, out: &str) {
    cp.enqueue(NewJob {
        kind: "transform".into(),
        payload: serde_json::json!({
            "inputs": [{ "schema": "main", "name": src }],
            "output": { "schema": "main", "name": out },
            "sql": format!("SELECT id, label FROM {src}"),
            "output_mode": "overwrite"
        }),
        run_at: None,
        priority: 0,
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn overwrite_replaces_contents_and_preserves_time_travel() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(writer.data_path()).unwrap());

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, true),
    ]));

    // Two source tables: src_a (2 rows), src_b (1 row, different labels).
    let src_a = tref("main", "src_a");
    land(
        &cp,
        &store,
        &src_a,
        schema.clone(),
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("a1"), Some("a2")])),
            ],
        )
        .unwrap(),
    )
    .await;
    let src_b = tref("main", "src_b");
    land(
        &cp,
        &store,
        &src_b,
        schema.clone(),
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![9])),
                Arc::new(StringArray::from(vec![Some("b9")])),
            ],
        )
        .unwrap(),
    )
    .await;

    // First overwrite: out := src_a (2 rows). (Output table is new -> create + replace.)
    enqueue_overwrite(&cp, "src_a", "out").await;
    drain_transforms(&cp, &store).await;
    let out = tref("main", "out");
    let snap_after_a = cp.current_snapshot(&out).await.unwrap().id;
    let count_a = writer.query_scalar("SELECT count(*) FROM lake.main.out;").await;
    assert_eq!(count_a, "2", "first overwrite wrote src_a's rows");
    let labels_a = writer
        .query_scalar("SELECT string_agg(label, ',' ORDER BY id) FROM lake.main.out;")
        .await;
    assert_eq!(labels_a, "a1,a2");

    // Second overwrite: out := src_b (1 row). Replaces, not appends.
    enqueue_overwrite(&cp, "src_b", "out").await;
    drain_transforms(&cp, &store).await;
    let count_b = writer.query_scalar("SELECT count(*) FROM lake.main.out;").await;
    assert_eq!(count_b, "1", "second overwrite REPLACED (not appended) -> 1 row");
    let labels_b = writer
        .query_scalar("SELECT string_agg(label, ',' ORDER BY id) FROM lake.main.out;")
        .await;
    assert_eq!(labels_b, "b9", "only src_b's row remains live");

    // Time travel: the snapshot after the first overwrite still has src_a's 2 rows. The
    // catalog lists exactly the files live at that snapshot (the b-file is excluded).
    let files_then = cp
        .files(&out, snap_after_a, PageReq::unbounded())
        .await
        .unwrap();
    let rows_then: i64 = files_then.items.iter().map(|f| f.record_count).sum();
    assert_eq!(rows_then, 2, "prior snapshot still sees src_a's 2 rows (time travel)");
}
```

- [ ] **Step 2: Wire the test target**

In `src/services/transform/BUCK`, add (mirror `transform-e2e`, lines 54-74):

```python
loom_fixture_test(
    name = "overwrite-e2e",
    crate = "overwrite_e2e",
    srcs = ["tests/overwrite_e2e.rs"],
    crate_root = "tests/overwrite_e2e.rs",
    duckdb = True,
    deps = [
        ":transform",
        "//src/services/ingest:ingest",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//src/control-plane/worker:worker",
        "//third-party:arrow",
        "//third-party:object_store",
        "//third-party:serde_json",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:tokio-util",
        "//third-party:uuid",
    ],
)
```

- [ ] **Step 3: Run the e2e to verify it fails**

Run: `buck2 test //src/services/transform:overwrite-e2e > /tmp/t3.log 2>&1; grep -nE "Tests finished|FAIL|error\[|2 row|REPLACED" /tmp/t3.log`
Expected: FAIL — the payload's `output_mode` is ignored today (handler hardcodes `Append`), so the second overwrite APPENDS and the count is 3 (or the assertion that it equals 1 fails).

- [ ] **Step 4: Parse `output_mode` in the physical payload**

In `src/services/transform/src/handler.rs`:

(a) Add the field to `TransformPayload` (the struct around lines 33-38):

```rust
#[derive(Deserialize)]
struct TransformPayload {
    inputs: Vec<TableSpec>,
    output: TableSpec,
    sql: String,
    #[serde(default)]
    output_mode: crate::run::OutputMode,
}
```

(b) Pass it into the `TransformRequest` (replace the `output_mode: crate::run::OutputMode::Append,` line added in Task 2):

```rust
            output_mode: payload.output_mode,
```

- [ ] **Step 5: Thread `output_mode` through the typed path**

In `src/services/transform/src/typed.rs`:

(a) Add an `output_mode` parameter to `run_typed_transform` (after `sql`):

```rust
pub async fn run_typed_transform(
    cp: &dyn ControlPlane,
    store: Arc<dyn ObjectStore>,
    run_id: &str,
    inputs: &[TypeName],
    output: &TypeName,
    sql: &str,
    output_mode: crate::run::OutputMode,
) -> Result<SnapshotId, TypedTransformError> {
```

(b) Pass it into the `TransformRequest` (replace the `output_mode: crate::run::OutputMode::Append,` line added in Task 2):

```rust
            output_mode,
```

(c) In `src/services/transform/src/handler.rs`, the `TypedTransformPayload` struct (lines 99-104) gains the field:

```rust
#[derive(Deserialize)]
struct TypedTransformPayload {
    inputs: Vec<String>,
    output: String,
    sql: String,
    #[serde(default)]
    output_mode: crate::run::OutputMode,
}
```

(d) Update the `run_typed_transform(...)` call in `typed_transform_handler` (around line 126) to pass the mode:

```rust
    let res =
        run_typed_transform(cp, store, &run_id, &inputs, &output, &payload.sql, payload.output_mode)
            .await;
```

- [ ] **Step 6: Run the e2e + the typed regression**

Run: `buck2 test //src/services/transform:overwrite-e2e //src/services/transform:typed-transform-e2e //src/services/transform:transform-e2e > /tmp/t3b.log 2>&1; grep -nE "Tests finished|FAIL|error\[" /tmp/t3b.log`
Expected: ALL PASS — `overwrite-e2e` green; the existing `typed-transform-e2e` and `transform-e2e` still green (they omit `output_mode` → default Append → unchanged behavior).

- [ ] **Step 7: Commit**

```bash
git add src/services/transform/src/handler.rs src/services/transform/src/typed.rs src/services/transform/tests/overwrite_e2e.rs src/services/transform/BUCK
git commit -m "feat(transform): wire output_mode through transform payloads

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Docs — roadmap + FUTURE.md

- [ ] **Step 1: Update the roadmap**

In `docs/superpowers/specs/2026-06-06-loom-roadmap.md`, in the transform pillar's `*Later:*` line (around line 232-233, currently `programmatic (registered-plan) transforms; overwrite/incremental output (with compaction); ...`), and the "Where we are" `Candidate next slices:` sentence: mark overwrite as delivered. Read the surrounding text first and match its style. Concretely:

- In the `*Later:*` bullet, change `overwrite/incremental output (with compaction)` to `compaction of small files; watermark/incremental output` (overwrite is no longer "later").
- After the last delivered transform paragraph in "Where we are", add a paragraph:

```markdown
**Overwrite output mode** (`2026-06-17-overwrite-output-mode-design.md`) is now delivered:
a transform can set `output_mode = overwrite` to replace its output table's live contents
(vs the default append), backed by a new DuckLake-faithful `Tx::replace_files` primitive
that expires the prior files at the new snapshot — older snapshots still time-travel to
them — and resets table stats. Compaction (reusing `replace_files`) and watermark-tracked
incremental output remain deferred.
```

- In the `Candidate next slices:` sentence, replace `overwrite/incremental output` with `compaction (reuses replace_files), watermark/incremental output`.

- [ ] **Step 2: Update FUTURE.md**

In `docs/FUTURE.md`, read the transform follow-ups section. Mark overwrite delivered (with the spec path) and note the deferred compaction + watermark-incremental follow-ups, matching the file's existing bullet style:

```markdown
- **Overwrite output mode** (transform) — DELIVERED
  (`docs/superpowers/specs/2026-06-17-overwrite-output-mode-design.md`): `output_mode =
  overwrite` replaces a transform's output table via the new `Tx::replace_files` primitive
  (expire prior files at the new snapshot + reset stats; time travel preserved). Deferred
  follow-ups: **compaction** of small Parquet files (reuses `replace_files`) and
  **watermark/incremental** output (stateful append-delta).
```

(If an existing "overwrite/incremental" pending item is present, replace it; otherwise add under the transform follow-ups. Match the file's heading/bullet conventions.)

- [ ] **Step 3: Lint the docs**

Run: `buck2 run //tools:prek -- run --files docs/superpowers/specs/2026-06-06-loom-roadmap.md docs/FUTURE.md > /tmp/lint.log 2>&1; grep -nE "Failed|Passed" /tmp/lint.log`
Expected: `trim trailing whitespace` + `fix end of files` Pass. If a hook rewrites a file, re-`git add`.

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/specs/2026-06-06-loom-roadmap.md docs/FUTURE.md
git commit -m "docs(transform): overwrite output mode delivered

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Final verification (after all tasks)

- [ ] **Whole control-plane + transform suites green:**

Run: `buck2 test //src/control-plane/... //src/services/transform/... > /tmp/final.log 2>&1; grep -nE "Tests finished|FAIL|error\[" /tmp/final.log`
Expected: all PASS — incl. both snapshot contracts (memory + pg), `snapshot-replace`, `sqlx-cache-check`, `output-mode`, `overwrite-e2e`, and the unchanged `transform-e2e` / `typed-transform-e2e`.

- [ ] **Clippy clean:**

Run: `./tools/clippy-all.sh > /tmp/clippy.log 2>&1; grep -nE "error|warning" /tmp/clippy.log`
Expected: no errors/warnings.

---

## Self-Review notes (author)

- **Spec coverage:** `Tx::replace_files` primitive across core+memory+postgres (Task 1) ✓; expire-live + reset-stats + write-new + change segments, with `next_row_id` preserved (Task 1, Step 6) ✓; testkit contract + focused pg test incl. time travel (Task 1) ✓; `OutputMode` + append/replace branch (Task 2) ✓; wire `output_mode` on both payloads + typed param, default Append (Task 3) ✓; overwrite e2e incl. second-overwrite-replaces + time travel + Append regression (Task 3) ✓; docs (Task 4) ✓.
- **Type consistency:** `OutputMode::{Append, Overwrite}` (`#[serde(rename_all="lowercase")]`, `#[default] Append`); `replace_files(&mut self, &TableRef, &[DataFile])`; `commit_snapshot(tx, staged_tables, staged_files, staged_replacements)`; `run_typed_transform(.., sql, output_mode)` — used identically across tasks.
- **Build atomicity:** the trait method + both adapter impls + testkit land in one commit (Task 1); the `TransformRequest.output_mode` field is added with both construction sites defaulted to `Append` in the same commit (Task 2); Task 3 only swaps the defaulted literals for the payload value. No intermediate red build.
- **No placeholders:** all code is verbatim except the two doc edits (Task 4), which must match the live file text and are explicitly marked "read first / match style". Two construction-site `grep`s (MemoryTx/PgTx literals, Task 1 Steps 4e/5d) are explicit because the struct-literal location is the one detail not pre-quoted.
