# Selective Compaction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Coalesce a table's small Parquet files into fewer size-targeted ones (leaving large files untouched), via a new partial-supersede control-plane primitive (`Tx::compact_files`) and a `compact_table` service function.

**Architecture:** `compact_files` expires a *named subset* of a table's live data files at a new snapshot and writes coalesced replacements, adjusting table stats by delta (vs `replace_files`, which expires all files and zeroes stats). `compact_table` reads only the sub-threshold files with DataFusion, rewrites them through `write_dataset`, and commits the swap through `compact_files`. No lineage edge — compaction is physical reorganization; full time-travel is preserved.

**Tech Stack:** Rust, buck2, sqlx compile-time macros (postgres adapter), DataFusion + `datafusion-io`, DuckLake catalog over Postgres, hermetic Postgres/DuckDB fixture tests.

**Design:** `docs/superpowers/specs/2026-06-17-compaction-design.md`

**Build/test notes (read before running anything):**
- Never run two `buck2` commands concurrently.
- Never pipe `buck2 test` through `tail`/`head` — redirect and grep:
  `buck2 test //target > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL|error\[" /tmp/t.log`
- Tests are integration `rust_test`/`loom_fixture_test` targets only — no inline `#[test]` in `src/**` (the `no-inline-tests` hook fails the build).
- After changing any postgres SQL, run `./tools/sqlx-prepare.sh` and commit the `.sqlx` change.
- Commit trailer: `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`

---

## Task 1: `Tx::compact_files` primitive (core + memory + postgres)

**Files:**
- Modify: `src/control-plane/core/src/transaction.rs` (add trait method)
- Modify: `src/control-plane/memory/src/transaction.rs` (field, method, commit loop)
- Modify: `src/control-plane/memory/src/lib.rs:209` (constructor)
- Modify: `src/control-plane/postgres/src/transaction.rs` (field, method, commit trigger)
- Modify: `src/control-plane/postgres/src/lib.rs:94` (constructor)
- Modify: `src/control-plane/postgres/src/snapshot.rs` (commit_snapshot compaction loop + signature)
- Modify: `src/control-plane/testkit/src/lib.rs` (add `snapshot_compact_contract`)
- Modify: `src/control-plane/memory/tests/snapshot.rs` (caller)
- Modify: `src/control-plane/postgres/tests/snapshot_conformance.rs` (caller)
- Refresh: `src/control-plane/postgres/.sqlx/` (via `tools/sqlx-prepare.sh`)

- [ ] **Step 1: Add the trait method to core**

In `src/control-plane/core/src/transaction.rs`, add after the `replace_files` method (line 55, inside `trait Tx`):

```rust
    /// Compact a subset of the table's live data files: the files named by `expire`
    /// (their table-relative paths, exactly as stored) are superseded at the new
    /// snapshot (still visible at older snapshots — time travel preserved) and `write`
    /// becomes live in their place. Unlike `replace_files`, the table's OTHER live files
    /// are untouched and table stats are adjusted by delta. Staged; applied at commit.
    /// Returns `ControlPlaneError::Conflict` at commit if any named file is not live
    /// (e.g. a concurrent compaction already superseded it).
    async fn compact_files(
        &mut self,
        table: &TableRef,
        expire: &[String],
        write: &[DataFile],
    ) -> Result<()>;
```

- [ ] **Step 2: Add the staging field + method + constructor to the memory adapter**

In `src/control-plane/memory/src/transaction.rs`, add the field to `MemoryTx` (after `staged_replacements`, line 24):

```rust
    pub(crate) staged_compactions: Vec<(TableRef, Vec<String>, Vec<DataFile>)>,
```

Add the method at the end of `impl Tx for MemoryTx` (after `replace_files`, line 177):

```rust
    async fn compact_files(
        &mut self,
        table: &TableRef,
        expire: &[String],
        write: &[DataFile],
    ) -> Result<()> {
        self.staged_compactions
            .push((table.clone(), expire.to_vec(), write.to_vec()));
        Ok(())
    }
```

In `src/control-plane/memory/src/lib.rs`, add to the `MemoryTx { ... }` constructor after line 209 (`staged_replacements: Vec::new(),`):

```rust
            staged_compactions: Vec::new(),
```

- [ ] **Step 3: Add the staging field + method + constructor to the postgres adapter**

In `src/control-plane/postgres/src/transaction.rs`, add the field to `PgTx` (after `staged_replacements`, line 15):

```rust
    pub(crate) staged_compactions: Vec<(TableRef, Vec<String>, Vec<DataFile>)>,
```

Add the method at the end of `impl Tx for PgTx` (after `replace_files`, line 69):

```rust
    async fn compact_files(
        &mut self,
        table: &TableRef,
        expire: &[String],
        write: &[DataFile],
    ) -> Result<()> {
        self.staged_compactions
            .push((table.clone(), expire.to_vec(), write.to_vec()));
        Ok(())
    }
```

Update the commit trigger in `commit` (lines 22-32): change the condition and the `commit_snapshot` call:

```rust
        if !self.staged_tables.is_empty()
            || !self.staged_files.is_empty()
            || !self.staged_replacements.is_empty()
            || !self.staged_compactions.is_empty()
        {
            let id = crate::snapshot::commit_snapshot(
                &mut self.tx,
                &self.staged_tables,
                &self.staged_files,
                &self.staged_replacements,
                &self.staged_compactions,
            )
            .await?;
```

In `src/control-plane/postgres/src/lib.rs`, add to the `PgTx { ... }` constructor after line 94 (`staged_replacements: Vec::new(),`):

```rust
            staged_compactions: Vec::new(),
```

- [ ] **Step 4: Extend `commit_snapshot` to accept compactions (signature + tracing, no loop yet)**

In `src/control-plane/postgres/src/snapshot.rs`, update the `#[tracing::instrument(...)]` attribute and the `commit_snapshot` signature (lines 36-46) to add the new parameter:

```rust
#[tracing::instrument(
    skip(tx, staged_tables, staged_files, staged_replacements, staged_compactions),
    fields(tables = staged_tables.len(), files = staged_files.len(), replacements = staged_replacements.len(), compactions = staged_compactions.len()),
    level = "debug"
)]
pub(crate) async fn commit_snapshot(
    tx: &mut Transaction<'_, Postgres>,
    staged_tables: &[(TableRef, Vec<ColumnSpec>)],
    staged_files: &[(TableRef, Vec<DataFile>)],
    staged_replacements: &[(TableRef, Vec<DataFile>)],
    staged_compactions: &[(TableRef, Vec<String>, Vec<DataFile>)],
) -> Result<SnapshotId> {
```

- [ ] **Step 5: Build to confirm everything still compiles (no behavior change yet)**

Run: `buck2 build //src/control-plane/... > /tmp/b.log 2>&1; grep -nE "BUILD SUCCEEDED|FAIL|error\[" /tmp/b.log`
Expected: BUILD SUCCEEDED (the trait method exists on both adapters; staged compactions are collected but not yet applied at commit).

- [ ] **Step 6: Write the contract in testkit (the failing test)**

In `src/control-plane/testkit/src/lib.rs`, add at the end of the file (after `snapshot_replace_contract`):

```rust
/// Contract for `Tx::compact_files` (selective compaction). Append three files, then
/// compact two of them into one: the current snapshot lists the untouched file plus the
/// coalesced one (the two compacted files expired), the total record_count is preserved,
/// and the prior snapshot still time-travels to all three originals. `cp` must be freshly
/// empty (postgres: a DuckLake catalog must be bootstrapped first).
pub async fn snapshot_compact_contract<C>(cp: &C)
where
    C: control_plane_core::ControlPlane + control_plane_core::Catalog,
{
    use control_plane_core::{ColumnSpec, DataFile, FileFormat, PageReq, TableRef};
    let t = TableRef {
        schema: "main".into(),
        name: "compact_me".into(),
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

    // create + append a(3) + b(5) + c(2) as three files.
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
    tx.append_files(
        &t,
        &[file("a.parquet", 3), file("b.parquet", 5), file("c.parquet", 2)],
    )
    .await
    .unwrap();
    let s1 = tx
        .commit()
        .await
        .unwrap()
        .expect("append yields a snapshot");

    let at1 = cp.files(&t, s1, PageReq::unbounded()).await.unwrap();
    assert_eq!(at1.len(), 3, "three files live after append");
    let total1: i64 = at1.items.iter().map(|f| f.record_count).sum();
    assert_eq!(total1, 10);

    // compact a + b into d(8); c untouched.
    let mut tx = cp.begin().await.unwrap();
    tx.compact_files(
        &t,
        &["a.parquet".into(), "b.parquet".into()],
        &[file("d.parquet", 8)],
    )
    .await
    .unwrap();
    let s2 = tx
        .commit()
        .await
        .unwrap()
        .expect("compact yields a snapshot");

    // current snapshot lists c + d only; record_count preserved.
    let cur = cp.current_snapshot(&t).await.unwrap();
    assert_eq!(cur.id, s2, "compact advanced the current snapshot");
    let at2 = cp.files(&t, s2, PageReq::unbounded()).await.unwrap();
    let mut paths: Vec<&str> = at2.items.iter().map(|f| f.path.as_str()).collect();
    paths.sort();
    assert_eq!(
        paths,
        vec!["c.parquet", "d.parquet"],
        "a,b expired; d added; c untouched"
    );
    let total2: i64 = at2.items.iter().map(|f| f.record_count).sum();
    assert_eq!(total2, 10, "compaction preserves the row set");

    // time travel: the prior snapshot still lists all three originals.
    let back = cp.files(&t, s1, PageReq::unbounded()).await.unwrap();
    let mut bpaths: Vec<&str> = back.items.iter().map(|f| f.path.as_str()).collect();
    bpaths.sort();
    assert_eq!(bpaths, vec!["a.parquet", "b.parquet", "c.parquet"]);
}
```

- [ ] **Step 7: Wire the memory caller and run it to verify it FAILS**

In `src/control-plane/memory/tests/snapshot.rs`, add:

```rust
#[tokio::test]
async fn snapshot_compact_contract() {
    let cp = MemoryControlPlane::new(std::time::Duration::from_millis(300));
    control_plane_testkit::snapshot_compact_contract(&cp).await;
}
```

Run: `buck2 test //src/control-plane/memory:snapshot > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL|error\[|panicked" /tmp/t.log`
Expected: FAIL — `snapshot_compact_contract` panics (the compaction is staged but never applied at commit, so the files are not expired and `d.parquet` is not added; the `at2` paths assertion fails). The other contracts still pass.

- [ ] **Step 8: Implement the memory commit loop**

In `src/control-plane/memory/src/transaction.rs`, in `commit`, update the `has_catalog_ops` check (lines 49-51) to include compactions:

```rust
        let has_catalog_ops = !self.staged_tables.is_empty()
            || !self.staged_files.is_empty()
            || !self.staged_replacements.is_empty()
            || !self.staged_compactions.is_empty();
```

Then add the compaction loop immediately after the existing replacement loop (after line 139, inside the `cat` lock block, before its closing `}`):

```rust
            // Apply staged file compactions: expire the NAMED live files at a new
            // snapshot and add the coalesced replacements. Unlike a replacement, the
            // table's other live files are untouched. Validate BEFORE mutating (the
            // memory commit applies directly to shared state, so a mid-apply error must
            // not leave partial changes): a named path that is not live -> Conflict (a
            // concurrent compaction superseded it), matching the postgres guard.
            for (table, expire, files) in self.staged_compactions {
                use control_plane_core::FileRef;
                let key = (table.schema.clone(), table.name.clone());
                let expire_set: std::collections::HashSet<&str> =
                    expire.iter().map(|p| p.as_str()).collect();
                let live_matched = cat
                    .files
                    .get(&key)
                    .map(|fs| {
                        fs.iter()
                            .filter(|f| {
                                f.end.is_none() && expire_set.contains(f.val.path.as_str())
                            })
                            .count()
                    })
                    .unwrap_or(0);
                if live_matched != expire.len() {
                    return Err(control_plane_core::ControlPlaneError::Conflict(format!(
                        "compact_files: {} of {} expire targets live for {}.{}",
                        live_matched,
                        expire.len(),
                        table.schema,
                        table.name
                    )));
                }
                let s = cat.new_snapshot();
                last_snapshot = Some(s);
                if let Some(existing) = cat.files.get_mut(&key) {
                    for f in existing.iter_mut() {
                        if f.end.is_none() && expire_set.contains(f.val.path.as_str()) {
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

- [ ] **Step 9: Run the memory contract to verify it PASSES**

Run: `buck2 test //src/control-plane/memory:snapshot > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL|error\[|panicked" /tmp/t.log`
Expected: PASS — all three contracts (`snapshot_commit_contract`, `snapshot_replace_contract`, `snapshot_compact_contract`) pass.

- [ ] **Step 10: Implement the postgres commit_snapshot compaction loop**

In `src/control-plane/postgres/src/snapshot.rs`, add the compaction loop immediately after the replace loop (after line 133, before the `// (4) the single new snapshot row` comment):

```rust
    // (3c) compaction loop: supersede a SUBSET of the table's live files (named by
    //      relative path) and write the coalesced replacements. Unlike the replace loop,
    //      the table's other live files stay and table stats are adjusted by delta — the
    //      expired files' record_count/file_size are subtracted, then write_data_file
    //      re-adds the new files. next_row_id is NOT decremented (row-ids are monotonic).
    //      The expire UPDATE must affect exactly `expire.len()` rows; a shortfall means a
    //      concurrent compaction already superseded a target -> Conflict, whole tx rolls
    //      back (commit_snapshot holds the per-database advisory lock, so the loser
    //      observes the winner's committed end_snapshot).
    for (table, expire, files) in staged_compactions {
        let table_id = resolve_table_id(tx, table).await?;
        let expired = sqlx::query!(
            "update ducklake_data_file set end_snapshot = $1 \
             where table_id = $2 and end_snapshot is null and path = any($3) \
             returning record_count as \"record_count!\", file_size_bytes as \"file_size_bytes!\"",
            new_snapshot_id,
            table_id,
            &expire[..],
        )
        .fetch_all(&mut **tx)
        .await
        .map_err(backend)?;
        if expired.len() != expire.len() {
            return Err(ControlPlaneError::Conflict(format!(
                "compact_files: expired {} of {} files for {}.{} (concurrent supersession)",
                expired.len(),
                expire.len(),
                table.schema,
                table.name
            )));
        }
        let expired_records: i64 = expired.iter().map(|r| r.record_count).sum();
        let expired_bytes: i64 = expired.iter().map(|r| r.file_size_bytes).sum();
        sqlx::query!(
            "update ducklake_table_stats \
             set record_count = record_count - $2, file_size_bytes = file_size_bytes - $3 \
             where table_id = $1",
            table_id,
            expired_records,
            expired_bytes,
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

Then add the snapshot-changes segments for compactions, immediately after the replacement-segments loop (after line 166, before `let changes_made = segments.join(",");`):

```rust
    for (table, expire, files) in staged_compactions {
        let table_id = resolve_table_id(tx, table).await?;
        if !expire.is_empty() {
            segments.push(format!("deleted_from_table:{table_id}"));
        }
        if !files.is_empty() {
            segments.push(format!("inserted_into_table:{table_id}"));
        }
    }
```

- [ ] **Step 11: Refresh the sqlx cache**

Run: `./tools/sqlx-prepare.sh > /tmp/sqlx.log 2>&1; grep -nE "error|prepared|finished|Failed" /tmp/sqlx.log`
Expected: completes; `git status src/control-plane/postgres/.sqlx/` shows two new `query-*.json` files (the expire UPDATE...RETURNING and the table_stats delta UPDATE).

- [ ] **Step 12: Wire the postgres conformance caller**

In `src/control-plane/postgres/tests/snapshot_conformance.rs`, add:

```rust
#[tokio::test]
async fn snapshot_compact_contract() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    DuckLakeWriter::new(fx.socket_path(), &db).bootstrap().await;
    control_plane_testkit::snapshot_compact_contract(&cp).await;
}
```

- [ ] **Step 13: Run the postgres conformance suite to verify it PASSES**

Run: `buck2 test //src/control-plane/postgres:snapshot-conformance > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL|error\[|panicked" /tmp/t.log`
Expected: PASS — `snapshot_compact_contract` passes against real Postgres + a bootstrapped DuckLake catalog.

- [ ] **Step 14: Run the full control-plane suite + clippy**

Run: `buck2 test //src/control-plane/... > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: all pass.
Run: `tools/clippy-all.sh > /tmp/c.log 2>&1; grep -nE "warning|error" /tmp/c.log || echo CLEAN`
Expected: CLEAN.

- [ ] **Step 15: Commit**

```bash
git add src/control-plane/ docs/superpowers/plans/2026-06-17-compaction.md
git commit -m "feat(control-plane): Tx::compact_files partial-supersede primitive (pg + memory)

Compact a NAMED subset of a table's live data files: expire them at the new
snapshot and write coalesced replacements, adjusting table stats by delta
(vs replace_files, which expires all files and zeroes stats). next_row_id is
preserved. A shortfall in the expire set -> Conflict (concurrent supersession).
Contract run on both adapters.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: `compact_table` service function + selection unit + e2e

**Files:**
- Create: `src/services/transform/src/compact.rs`
- Modify: `src/services/transform/src/lib.rs` (module + re-exports)
- Create: `src/services/transform/tests/compact_unit.rs`
- Create: `src/services/transform/tests/compact_e2e.rs`
- Modify: `src/services/transform/BUCK` (two new test targets)

- [ ] **Step 1: Write the selection unit test (the failing test)**

Create `src/services/transform/tests/compact_unit.rs`:

```rust
//! Pure selection logic for compaction: which live files fall below the size threshold.

use control_plane_core::FileRef;
use transform::small_files;

fn f(path: &str, size: i64) -> FileRef {
    FileRef {
        path: path.into(),
        record_count: 1,
        file_size_bytes: size,
    }
}

#[test]
fn small_files_selects_sub_threshold() {
    let files = vec![f("a", 10), f("b", 200), f("c", 50)];
    let small = small_files(&files, 100);
    let paths: Vec<&str> = small.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(paths, vec!["a", "c"], "only sub-threshold files selected");
}

#[test]
fn small_files_empty_when_all_large() {
    let files = vec![f("a", 200), f("b", 300)];
    assert!(small_files(&files, 100).is_empty());
}
```

- [ ] **Step 2: Create the `compact` module**

Create `src/services/transform/src/compact.rs`:

```rust
//! Selective (size-threshold) compaction: coalesce a table's small Parquet files into
//! fewer size-targeted ones, leaving already-large files in place. Reads ONLY the small
//! files with DataFusion, rewrites them via `write_dataset`, and commits the swap through
//! the `compact_files` partial-supersede primitive. Physical reorganization only — the row
//! set and full time-travel history are preserved; no lineage edge is emitted.

use std::sync::Arc;

use control_plane_core::{ControlPlane, DataFile, FileRef, SnapshotId, TableRef};
use datafusion::execution::context::SessionContext;
use datafusion_io::{WriteConfig, scan_table, write_dataset};
use object_store::ObjectStore;

/// Tunables for a compaction run.
pub struct CompactConfig {
    /// Live files strictly smaller than this (in bytes) are compaction candidates.
    pub small_file_threshold_bytes: i64,
    /// Output sizing for the coalesced files.
    pub write: WriteConfig,
}

#[derive(Debug, thiserror::Error)]
pub enum CompactError {
    #[error("sql/datafusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),
    #[error(transparent)]
    Scan(#[from] datafusion_io::ScanError),
    #[error(transparent)]
    Write(#[from] datafusion_io::WriteError),
    #[error(transparent)]
    ControlPlane(#[from] control_plane_core::ControlPlaneError),
    #[error("compact commit produced no snapshot id")]
    NoSnapshot,
}

/// Select the sub-threshold files among `files`. Pure — no I/O — so it is unit-testable
/// without a catalog.
pub fn small_files(files: &[FileRef], threshold_bytes: i64) -> Vec<&FileRef> {
    files
        .iter()
        .filter(|f| f.file_size_bytes < threshold_bytes)
        .collect()
}

/// Compact `table`'s small files. Returns the new snapshot id, or `Ok(None)` when there
/// are fewer than two small files (nothing worth coalescing — a no-op that also makes
/// re-running compaction converge). `run_id` is a caller-unique output-file prefix.
pub async fn compact_table(
    cp: &dyn ControlPlane,
    store: Arc<dyn ObjectStore>,
    run_id: &str,
    table: &TableRef,
    cfg: &CompactConfig,
) -> Result<Option<SnapshotId>, CompactError> {
    // 1. Current snapshot's live files.
    let snapshot = cp.catalog().current_snapshot(table).await?;
    let files = cp
        .catalog()
        .files(
            table,
            snapshot.id,
            control_plane_core::PageReq::unbounded(),
        )
        .await?;

    // 2. Select the sub-threshold files; bail out unless at least two can be coalesced.
    let small = small_files(&files.items, cfg.small_file_threshold_bytes);
    if small.len() < 2 {
        return Ok(None);
    }
    let small_refs: Vec<FileRef> = small.iter().map(|f| (*f).clone()).collect();
    let expire_paths: Vec<String> = small.iter().map(|f| f.path.clone()).collect();

    // 3. Read ONLY the small files. SELECT * + collect fully materializes their rows
    //    BEFORE the transaction opens, so reading and then expiring the same files in
    //    one commit is safe (no read-after-expire).
    let ctx = SessionContext::new();
    scan_table(&ctx, store.clone(), &table.name, table, &small_refs).await?;
    let df = ctx
        .sql(&format!("SELECT * FROM \"{}\"", table.name))
        .await?;
    let schema: Arc<arrow::datatypes::Schema> = Arc::new(df.schema().as_arrow().clone());
    let batches = df.collect().await?;

    // 4. Write the coalesced, size-targeted files.
    let dir_prefix = format!("{}/{}/{}", table.schema, table.name, run_id);
    let written = write_dataset(store, &dir_prefix, schema, &batches, &cfg.write).await?;
    let new_files: Vec<DataFile> = written
        .into_iter()
        .map(|f| DataFile {
            path: f.path,
            path_is_relative: true,
            file_format: control_plane_core::FileFormat::Parquet,
            record_count: f.record_count,
            file_size_bytes: f.file_size_bytes,
            column_stats: f.column_stats,
            parquet_footer_size: Some(f.footer_size),
        })
        .collect();

    // 5. One Tx: swap the small files for the coalesced ones. No lineage (physical reorg).
    let mut tx = cp.begin().await?;
    tx.compact_files(table, &expire_paths, &new_files).await?;
    let snap = tx.commit().await?.ok_or(CompactError::NoSnapshot)?;
    Ok(Some(snap))
}
```

- [ ] **Step 3: Wire the module + re-exports**

In `src/services/transform/src/lib.rs`, add the module declaration after `pub mod conform;`:

```rust
pub mod compact;
```

And add a re-export line after the existing `pub use` lines:

```rust
pub use compact::{CompactConfig, CompactError, compact_table, small_files};
pub use datafusion_io::WriteConfig;
```

- [ ] **Step 4: Add the unit-test BUCK target and run it**

In `src/services/transform/BUCK`, add after the `output-mode` `rust_test` block:

```python
rust_test(
    name = "compact-unit",
    crate = "compact_unit",
    srcs = ["tests/compact_unit.rs"],
    crate_root = "tests/compact_unit.rs",
    edition = "2024",
    deps = [
        ":transform",
        "//src/control-plane/core:core",
    ],
)
```

Run: `buck2 test //src/services/transform:compact-unit > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: PASS — both `small_files` cases pass.

- [ ] **Step 5: Write the e2e test**

Create `src/services/transform/tests/compact_e2e.rs`:

```rust
//! Compaction e2e: land three small files into one table, compact_table coalesces them
//! into a single file (DuckDB read-back sees the same rows), the pre-compaction snapshot
//! still time-travels to the three originals, and a second compaction is a no-op (one
//! file left -> fewer than two small files -> None).

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{
    Catalog, DatasetRef, EventType, LineageEvent, PageReq, RunId, TableRef,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use ingest::{MaterializeRequest, materialize};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use time::OffsetDateTime;
use transform::{CompactConfig, WriteConfig, compact_table};
use uuid::Uuid;

fn tref(s: &str, n: &str) -> TableRef {
    TableRef {
        schema: s.into(),
        name: n.into(),
    }
}

/// Land one batch into `table` under a distinct file prefix (one append -> one data file).
async fn land(
    cp: &PgControlPlane,
    store: &Arc<dyn ObjectStore>,
    table: &TableRef,
    schema: Arc<Schema>,
    batch: RecordBatch,
    prefix: &str,
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
            file_prefix: prefix,
            gate: None,
            lineage,
        },
    )
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn compact_coalesces_small_files_and_preserves_time_travel() {
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

    let acc = tref("main", "acc");
    let row = |id: i64, label: &str| {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![id])),
                Arc::new(StringArray::from(vec![Some(label.to_string())])),
            ],
        )
        .unwrap()
    };

    // Three appends -> three small data files.
    land(&cp, &store, &acc, schema.clone(), row(1, "a"), "run-1").await;
    land(&cp, &store, &acc, schema.clone(), row(2, "b"), "run-2").await;
    land(&cp, &store, &acc, schema.clone(), row(3, "c"), "run-3").await;

    let before = cp.current_snapshot(&acc).await.unwrap().id;
    let files_before = cp.files(&acc, before, PageReq::unbounded()).await.unwrap();
    assert_eq!(files_before.len(), 3, "three small files before compaction");

    // Compact: 10 MiB threshold (all three qualify), default 128 MiB output target -> 1 file.
    let cfg = CompactConfig {
        small_file_threshold_bytes: 10 * 1024 * 1024,
        write: WriteConfig::default(),
    };
    let snap = compact_table(&cp, store.clone(), "compact-1", &acc, &cfg)
        .await
        .unwrap()
        .expect("compaction produced a snapshot");

    let files_after = cp.files(&acc, snap, PageReq::unbounded()).await.unwrap();
    assert_eq!(files_after.len(), 1, "three small files coalesced into one");

    // DuckDB read-back: the row set is unchanged.
    let count = writer
        .query_scalar("SELECT count(*) FROM lake.main.acc;")
        .await;
    assert_eq!(count, "3", "compaction preserves the row set");
    let labels = writer
        .query_scalar("SELECT string_agg(label, ',' ORDER BY id) FROM lake.main.acc;")
        .await;
    assert_eq!(labels, "a,b,c", "values intact after rewrite");

    // Time travel: the pre-compaction snapshot still lists the three originals.
    let files_then = cp.files(&acc, before, PageReq::unbounded()).await.unwrap();
    assert_eq!(
        files_then.len(),
        3,
        "prior snapshot retains the original three files (time travel)"
    );

    // No-op: one coalesced file remains (< 2 small files) -> None, no new snapshot.
    let again = compact_table(&cp, store.clone(), "compact-2", &acc, &cfg)
        .await
        .unwrap();
    assert!(again.is_none(), "fewer than two small files -> no-op");
    let head = cp.current_snapshot(&acc).await.unwrap().id;
    assert_eq!(head, snap, "no-op created no new snapshot");
}
```

- [ ] **Step 6: Add the e2e BUCK target**

In `src/services/transform/BUCK`, add after the `overwrite-e2e` `loom_fixture_test` block:

```python
loom_fixture_test(
    name = "compact-e2e",
    crate = "compact_e2e",
    srcs = ["tests/compact_e2e.rs"],
    crate_root = "tests/compact_e2e.rs",
    duckdb = True,
    deps = [
        ":transform",
        "//src/services/ingest:ingest",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:arrow",
        "//third-party:object_store",
        "//third-party:serde_json",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```

- [ ] **Step 7: Run the e2e to verify it PASSES**

Run: `buck2 test //src/services/transform:compact-e2e > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL|error\[|panicked" /tmp/t.log`
Expected: PASS — coalesces to one file, DuckDB count is 3, time-travel sees three, second call is a no-op.

- [ ] **Step 8: Run the full transform suite + clippy**

Run: `buck2 test //src/services/transform/... > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: all pass.
Run: `tools/clippy-all.sh > /tmp/c.log 2>&1; grep -nE "warning|error" /tmp/c.log || echo CLEAN`
Expected: CLEAN.

- [ ] **Step 9: Commit**

```bash
git add src/services/transform/
git commit -m "feat(transform): compact_table selective compaction service fn

Coalesce a table's sub-threshold Parquet files into fewer size-targeted ones,
leaving large files in place. Reads only the small files with DataFusion,
rewrites via write_dataset, and swaps them through Tx::compact_files. No-op
below two small files (convergent). Library primitive only; proven by a
selection unit test and a DuckDB read-back e2e.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Docs

**Files:**
- Modify: `docs/superpowers/specs/2026-06-06-loom-roadmap.md`
- Modify: `docs/FUTURE.md`

- [ ] **Step 1: Mark compaction delivered in the roadmap**

In `docs/superpowers/specs/2026-06-06-loom-roadmap.md`, in the Transform workers "Later" bullet (around line 233-235), remove "compaction of small files (reuses `replace_files`)" from the deferred list. In the "Where we are" section, add a paragraph after the inverse-direction-hops paragraph (around line 306) recording the delivery:

```markdown
**Selective compaction** (`2026-06-17-compaction-design.md`) is now delivered: a
table's sub-threshold Parquet files can be coalesced into fewer size-targeted ones
(large files left in place) via a new partial-supersede control-plane primitive
(`Tx::compact_files`) and a `compact_table` service function. The primitive expires a
named subset of live files at the new snapshot and writes coalesced replacements,
adjusting table stats by delta (vs `replace_files`, which expires all files); older
snapshots still time-travel to the originals. A concurrent-compaction race is rejected
by an exact expire-count assertion. Library primitive only — a queue job and operator
endpoint remain deferred, as does watermark-tracked incremental output.
```

- [ ] **Step 2: Update FUTURE.md**

In `docs/FUTURE.md`, update the file-supersession / compaction bullet to record that selective compaction is now delivered via `Tx::compact_files` + `compact_table`, that compacted files get fresh row-ids (sound today because loom has no merge-on-read delete vectors — to revisit if row-level deletes land), and that the remaining deferred pieces are a queue-driven compaction job / operator endpoint and watermark-tracked incremental output.

(Read `docs/FUTURE.md` first to match the existing bullet's wording and placement; edit the single relevant bullet in place rather than appending.)

- [ ] **Step 3: Run the markdown lint hooks and commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p.log 2>&1; grep -nE "Failed|Passed|error" /tmp/p.log | tail -20`
Expected: all hooks pass (fix any trailing whitespace / EOF newline the hooks flag, then re-run).

```bash
git add docs/
git commit -m "docs(transform): mark selective compaction delivered

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Final verification

- [ ] **Run the whole first-party suite**

Run: `buck2 test //src/... > /tmp/all.log 2>&1; grep -nE "Tests finished|FAIL|error\[" /tmp/all.log`
Expected: all pass, zero failures.

- [ ] **Confirm clippy is clean across all first-party Rust**

Run: `tools/clippy-all.sh > /tmp/c.log 2>&1; grep -nE "warning|error" /tmp/c.log || echo CLEAN`
Expected: CLEAN.
