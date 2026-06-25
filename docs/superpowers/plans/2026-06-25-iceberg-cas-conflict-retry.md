# Iceberg CAS-conflict retry/backoff Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a bounded, backed-off retry around the Iceberg `fast_append` commit so a writer whose pointer compare-and-swap (CAS) is lost to a concurrent committer reloads the table, re-stages the same already-written Parquet, and re-commits — instead of failing.

**Architecture:** A single private helper `commit_append_with_retry` in `src/control-plane/postgres/src/iceberg_writer.rs` owns the reload + re-stage + commit loop. The two real-Parquet append entry points (`append_batches` and `append_batches_with_extras`) write Parquet once, then route their commit section through the helper. The vendored catalog (`SqlCatalog`, `do_update_table`, `CommitExtrasCatalog`) is untouched — its CAS-conflict detection already returns `ErrorKind::CatalogCommitConflicts` flagged `with_retryable(true)`; this slice only adds the consumer that respects it.

**Tech Stack:** Rust 2024, the `iceberg` crate (`Transaction`/`fast_append`/`Catalog`/`TableIdent`/`ErrorKind`), `tokio::time::sleep` (the postgres crate's `tokio` already carries the `time` feature), buck2 + `loom_fixture_test` (hermetic Postgres).

## Global Constraints

- **No new dependencies.** Backoff is inline `tokio::time::sleep`; jitter is derived from a hash of the `TableIdent` XOR the attempt number — no `rand`/`backoff` crate. The repo keeps its dependency closure tight.
- **No public-surface change.** `append_batches`/`append_batches_with_extras`/`append_batches_with_lineage` keep their existing signatures and return types. No new public error variants. On retry exhaustion the original `CatalogCommitConflicts` error propagates unchanged (still `with_retryable(true)`).
- **Untouched:** `do_update_table`, the `SqlCatalog` `Catalog` impl, and the `CommitExtrasCatalog` decorator. Only `iceberg_writer.rs` (helper + two call-site rewrites) and the roundtrip test file change.
- **Retry constants:** `COMMIT_MAX_RETRIES = 5`, `COMMIT_BACKOFF_BASE = 5ms`, `COMMIT_BACKOFF_CAP = 200ms`.
- **Tests are `rust_test`/`loom_fixture_test` integration targets only** — never inline `#[cfg(test)]`. The fixture tests boot hermetic Postgres and must use the `loom_fixture_test` macro (already the case for `iceberg-write-roundtrip`).
- **Scope:** `append_batches*` only (ingest landing + flush both route here). `inline_append` is mirror-only with no pointer CAS and is out of scope by construction. The overwrite path ([[road-iceberg-overwrite-mode]]) wiring is a deferred follow-up owned by that slice.

---

### Task 1: The `commit_append_with_retry` helper

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_writer.rs` (add imports, constants, helper fn)

**Interfaces:**
- Consumes: `iceberg::{Catalog, Result, TableIdent}`, `iceberg::ErrorKind`, `iceberg::spec::DataFile`, `iceberg::table::Table`, `iceberg::transaction::{ApplyTransactionAction, Transaction}` (all but `ErrorKind` already imported in the file).
- Produces:
  ```rust
  async fn commit_append_with_retry(
      catalog: &dyn Catalog,
      ident: &TableIdent,
      table: Table,              // the already-loaded table for attempt 0
      data_files: Vec<DataFile>,
  ) -> Result<()>
  ```
  Task 2 (the two append call sites) consumes this. Note `table` is taken **by value** (the loop rebinds it on reload) and `data_files` by value (cloned per attempt). It returns `()` — the callers already built their `WrittenFile` summaries from `data_files` before calling.

- [ ] **Step 1: Add the `ErrorKind` import**

In the `use iceberg::{...}` line (currently `use iceberg::{Catalog, Namespace, NamespaceIdent, Result, TableCommit, TableCreation, TableIdent};`), add `ErrorKind`:

```rust
use iceberg::{
    Catalog, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation, TableIdent,
};
```

Add a `Duration` import near the top imports (after `use std::collections::HashMap;`):

```rust
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::time::Duration;
```

(`std::hash` is used for the jitter term; `Duration` for the backoff math.)

- [ ] **Step 2: Add the retry constants**

Place these module-level constants just below the imports, above `WrittenFile`:

```rust
/// Bound on commit retries after a lost pointer CAS. A conflict at the cap
/// propagates the original (still retryable-flagged) error so a higher layer
/// (e.g. the queue worker's RetryPolicy) can re-drive it.
const COMMIT_MAX_RETRIES: u32 = 5;
/// Base delay for exponential backoff between commit attempts.
const COMMIT_BACKOFF_BASE: Duration = Duration::from_millis(5);
/// Cap on the exponential backoff delay.
const COMMIT_BACKOFF_CAP: Duration = Duration::from_millis(200);
```

- [ ] **Step 3: Write the helper**

Add this private fn (place it after `append_batches`, before `CommitExtrasCatalog`, or anywhere module-private — keep it adjacent to the append fns):

```rust
/// Commit a `fast_append` of `data_files`, retrying on a lost pointer CAS.
///
/// A conflict means this writer's staged metadata was built against a now-stale
/// `metadata_location`, so a bare CAS replay would re-conflict forever. On
/// `CatalogCommitConflicts` we instead reload the table (picking up the winning
/// writer's new parent snapshot), re-stage the **same** already-written
/// `data_files` against it, and re-commit, with bounded exponential backoff plus
/// a per-writer jitter term so colliding writers de-synchronize. The data files
/// are written once (UUID-prefixed paths) and reused across attempts, so
/// re-adding them is correct and collision-free. Attempt 0 reuses the
/// already-loaded `table`, so the reload round-trip is paid only on the retry
/// path. Any non-conflict error, or a conflict past `COMMIT_MAX_RETRIES`,
/// propagates unchanged.
async fn commit_append_with_retry(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    mut table: Table,
    data_files: Vec<DataFile>,
) -> Result<()> {
    let mut attempt: u32 = 0;
    loop {
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(data_files.clone());
        let tx = action.apply(tx)?;
        match tx.commit(catalog).await {
            Ok(_) => return Ok(()),
            Err(e)
                if e.kind() == ErrorKind::CatalogCommitConflicts
                    && attempt < COMMIT_MAX_RETRIES =>
            {
                tokio::time::sleep(commit_backoff(ident, attempt)).await;
                table = catalog.load_table(ident).await?;
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Backoff delay for a given attempt: `min(CAP, BASE * 2^attempt)` plus a small
/// jitter derived from the table identity hashed with the attempt, so one
/// writer's attempt N and N+1 do not land on the same delay. No `rand` crate —
/// the jitter is a deterministic hash, which is enough to break ties. Note the
/// jitter is the *same* across writers contending on one table (they share the
/// `TableIdent`), so de-synchronization across writers comes mainly from the
/// exponential growth and the natural spread in Postgres commit latency rather
/// than from the jitter; if high-N contention ever proves under-spread, mix a
/// per-writer entropy source into the hash here (a tracked deferred refinement,
/// not needed at the calibrated N=8).
fn commit_backoff(ident: &TableIdent, attempt: u32) -> Duration {
    let exp = COMMIT_BACKOFF_BASE
        .saturating_mul(1u32 << attempt.min(16))
        .min(COMMIT_BACKOFF_CAP);
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    ident.hash(&mut hasher);
    attempt.hash(&mut hasher);
    // Jitter in [0, BASE): keep it bounded so it never dominates the delay.
    let jitter_ms = hasher.finish() % (COMMIT_BACKOFF_BASE.as_millis() as u64).max(1);
    exp + Duration::from_millis(jitter_ms)
}
```

Notes for the implementer:
- **Confirmed against the pinned iceberg rev `148afc50`:** `TableIdent` derives `Hash` (`#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]` in `crates/iceberg/src/catalog/mod.rs`), so `ident.hash(&mut hasher)` compiles directly.
- `1u32 << attempt.min(16)` guards the shift from overflowing `u32`; `saturating_mul` then `.min(CAP)` clamps the result. Since `attempt <= COMMIT_MAX_RETRIES (5)` at the call site, the cap dominates well before the shift could overflow, but the guard keeps `commit_backoff` total over all inputs.

- [ ] **Step 4: Build the library to verify it compiles**

Run: `buck2 build //src/control-plane/postgres:postgres > /tmp/cas-build.log 2>&1; grep -E "BUILD SUCCEEDED|BUILD FAILED|error\[|error:" /tmp/cas-build.log`
Expected: `BUILD SUCCEEDED`. The helper is dead-code at this point (no caller yet) — Rust does **not** warn on unused private async fns that are about to be used, but if a `dead_code` warning appears it will clear once Task 2 wires the callers. (`TableIdent: Hash`, `Table: Clone`, and `Table::identifier()` are all confirmed against the pin, so no fallback is needed.)

- [ ] **Step 5: Commit**

```bash
git add src/control-plane/postgres/src/iceberg_writer.rs
git commit -m "feat(iceberg): add commit_append_with_retry helper"
```

---

### Task 2: Route the two append paths through the helper

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_writer.rs` (rewrite the commit sections of `append_batches` and `append_batches_with_extras`)

**Interfaces:**
- Consumes: `commit_append_with_retry` from Task 1.
- Produces: no signature change. `append_batches`/`append_batches_with_extras`/`append_batches_with_lineage` keep their existing public signatures.

- [ ] **Step 1: Rewrite `append_batches`'s commit section**

Current tail (after building `summaries`):

```rust
    let tx = Transaction::new(table);
    let action = tx.fast_append().add_data_files(data_files);
    let tx = action.apply(tx)?;
    tx.commit(catalog).await?;
    Ok(summaries)
```

Replace with a call to the helper. `append_batches` already holds `table: &Table` and `catalog: &dyn Catalog`; the helper needs an owned `Table` and the `TableIdent`:

```rust
    commit_append_with_retry(catalog, table.identifier(), table.clone(), data_files).await?;
    Ok(summaries)
```

`Table` derives `Clone` and `Table::identifier(&self) -> &TableIdent` is confirmed against the pinned iceberg rev (see API facts in Self-Review), so `table.clone()` and `table.identifier()` are valid.

- [ ] **Step 2: Rewrite `append_batches_with_extras`'s commit section**

Current tail:

```rust
    let wrapper = CommitExtrasCatalog {
        inner: catalog,
        lineage,
        end_cap,
        overwrite,
    };
    let tx = Transaction::new(table);
    let action = tx.fast_append().add_data_files(data_files);
    let tx = action.apply(tx)?;
    tx.commit(&wrapper).await?;
    Ok(summaries)
```

The `CommitExtrasCatalog` wrapper holds only borrows (`inner`, `lineage`, `end_cap`) and is cheap to rebuild, but `end_cap: Option<InlineEndCap<'_>>` is moved into the struct, so it cannot be rebuilt per attempt without re-borrowing. The wrapper itself is stable across attempts (its fields do not depend on the reloaded table) — so build it **once** and pass `&wrapper` as the `&dyn Catalog`. The helper's reload calls go through `wrapper.load_table`, which delegates to `inner.load_table` (pure delegation), so the extras still re-present per attempt via `wrapper.update_table` and persist only on the winning commit:

```rust
    let wrapper = CommitExtrasCatalog {
        inner: catalog,
        lineage,
        end_cap,
        overwrite,
    };
    commit_append_with_retry(&wrapper, table.identifier(), table.clone(), data_files).await?;
    Ok(summaries)
```

This matches the promise the `append_batches_with_lineage` doc comment already makes ("re-presented on each commit-retry attempt and only persists on the winning, committed attempt").

- [ ] **Step 3: Build the library**

Run: `buck2 build //src/control-plane/postgres:postgres > /tmp/cas-build2.log 2>&1; grep -E "BUILD SUCCEEDED|BUILD FAILED|error\[|error:|warning:" /tmp/cas-build2.log`
Expected: `BUILD SUCCEEDED`, no `dead_code` warning (the helper now has callers).

- [ ] **Step 4: Run clippy on the library**

Run: `buck2 build '//src/control-plane/postgres:postgres[clippy.txt]' > /tmp/cas-clippy.log 2>&1; cat $(buck2 build '//src/control-plane/postgres:postgres[clippy.txt]' --show-simple-output 2>/dev/null | head -1) 2>/dev/null; grep -E "warning|error" /tmp/cas-clippy.log || echo "clippy clean"`
Expected: empty `clippy.txt` (clean). Fix any lint (e.g. needless clone) before committing.

- [ ] **Step 5: Run the existing roundtrip tests (regression guard)**

Run: `buck2 test //src/control-plane/postgres:iceberg-write-roundtrip > /tmp/cas-rt.log 2>&1; grep -E "Tests finished|PASS|FAIL" /tmp/cas-rt.log`
Expected: all 3 existing tests pass (`append_round_trips_through_the_mirror`, `drop_unappended_table_succeeds_without_orphan_snapshot`, `concurrent_appends_keep_the_mirror_consistent`). The `N=4` concurrent test must stay green — the retry must not regress the no/low-contention path.

- [ ] **Step 6: Commit**

```bash
git add src/control-plane/postgres/src/iceberg_writer.rs
git commit -m "feat(iceberg): route append commits through CAS-conflict retry"
```

---

### Task 3: Extract a shared concurrent-append test harness

**Files:**
- Modify: `src/control-plane/postgres/tests/iceberg_write_roundtrip.rs` (parameterize the concurrent body over `N`)

**Interfaces:**
- Consumes: existing `make_catalog`, `create_t`, `batch` helpers in the test file.
- Produces: an `async fn concurrent_appends_consistent(n: i64)` helper that both the `N=4` and the new `N=8` test call. No copy-paste of the spawn/append/assert body.

- [ ] **Step 1: Add the parameterized harness helper**

Add this `async fn` to the test file (e.g. just above `concurrent_appends_keep_the_mirror_consistent`). It is the body of the existing test, lifted verbatim with `const N` replaced by the `n` parameter and the assertion messages kept:

```rust
/// Spawn `n` writers that each append one file to the same table concurrently,
/// then assert the mirror is consistent: exactly `n` snapshots, no orphan
/// snapshot (every snapshot carries its files), and exactly `n` data files
/// (none dropped, none duplicated). With the CAS-conflict retry in place this
/// holds even when writers collide on the pointer CAS.
async fn concurrent_appends_consistent(n: i64) {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let whs = wh.path().display().to_string();
    let setup = make_catalog(fx.pg_dsn(&db), &whs).await;
    create_t(&setup, &whs).await;
    let cs = setup
        .load_table(&TableIdent::new(
            NamespaceIdent::new("wh".into()),
            "t".into(),
        ))
        .await
        .expect("load")
        .metadata()
        .current_schema()
        .clone();

    let mut handles = Vec::new();
    for k in 0..n {
        let dsn = fx.pg_dsn(&db);
        let whs = whs.clone();
        let cs = cs.clone();
        handles.push(tokio::spawn(async move {
            let catalog = make_catalog(dsn, &whs).await;
            let table = catalog
                .load_table(&TableIdent::new(
                    NamespaceIdent::new("wh".into()),
                    "t".into(),
                ))
                .await
                .expect("load");
            append_batches(&catalog, &table, vec![batch(&cs, vec![k * 10, k * 10 + 1])])
                .await
                .expect("append");
        }));
    }
    for h in handles {
        h.await.expect("join");
    }

    let pool: PgPool = fx.pool_for(&db).await;
    // Exactly n snapshots, each carrying at least one data file (no orphan snapshot
    // rows from a rolled-back CAS attempt), and all n files present (none dropped).
    let snap_count: i64 = sqlx::query_scalar("select count(*) from iceberg_mirror.snapshot")
        .fetch_one(&pool)
        .await
        .expect("count snapshots");
    assert_eq!(snap_count, n, "one snapshot per successful append");
    let orphans: i64 = sqlx::query_scalar(
        "select count(*) from iceberg_mirror.snapshot s \
         where not exists (select 1 from iceberg_mirror.data_file f where f.begin_snapshot = s.snapshot_id)",
    )
    .fetch_one(&pool)
    .await
    .expect("orphan check");
    assert_eq!(orphans, 0, "no snapshot without its files");
    let files: i64 = sqlx::query_scalar("select count(*) from iceberg_mirror.data_file")
        .fetch_one(&pool)
        .await
        .expect("count files");
    assert_eq!(files, n, "all appended files present, none duplicated");
}
```

- [ ] **Step 2: Reduce the existing `N=4` test to a one-line call**

Replace the entire body of `concurrent_appends_keep_the_mirror_consistent` (lines ~151-215) with a delegation, keeping its `#[tokio::test]` attribute and `worker_threads = 4`:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_appends_keep_the_mirror_consistent() {
    // Baseline contention level; kept green before the retry existed.
    concurrent_appends_consistent(4).await;
}
```

- [ ] **Step 3: Add the `N=8` contention test**

Add directly below it:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_appends_tolerate_contention() {
    // N=8 went red without the CAS-conflict retry (lost pointer CAS, no re-drive)
    // and the bump was reverted. Green here is the proof the retry re-commits a
    // lost CAS. No minimum-retry-count assertion: contention is nondeterministic,
    // so the invariant set (n snapshots, no orphans, n files) is the robust proof.
    concurrent_appends_consistent(8).await;
}
```

Note: keep `worker_threads = 4` (not 8) — the existing baseline test ran 4 writers on 4 worker threads; with 8 spawned tasks on 4 threads the writers still interleave and contend (the conflict is on the Postgres CAS, not on CPU parallelism), which is sufficient. Using a thread count below the task count actually *increases* the chance of interleaving at the await points.

- [ ] **Step 4: Verify the old test still has no unused-import / unused-`const` fallout**

The old body declared `const N: i64 = 4;` — it is gone now, so no `unused` warning. Confirm the imports (`HashMap`, `Arc`, etc.) are all still used by the remaining helpers (they are — `make_catalog`/`create_t`/`batch` use them).

- [ ] **Step 5: Run the full roundtrip suite (both concurrent tests)**

Run: `buck2 test //src/control-plane/postgres:iceberg-write-roundtrip > /tmp/cas-rt8.log 2>&1; grep -E "Tests finished|PASS|FAIL|concurrent" /tmp/cas-rt8.log`
Expected: all 4 tests pass — `append_round_trips_through_the_mirror`, `drop_unappended_table_succeeds_without_orphan_snapshot`, `concurrent_appends_keep_the_mirror_consistent` (N=4), `concurrent_appends_tolerate_contention` (N=8). The N=8 test going green is the headline proof.

Run it a few times to shake out nondeterminism (contention is racy):
`for i in 1 2 3; do buck2 test //src/control-plane/postgres:iceberg-write-roundtrip > /tmp/cas-rt8-$i.log 2>&1; grep -E "Tests finished|FAIL" /tmp/cas-rt8-$i.log; done`
Expected: green every time. If N=8 flakes, the retry cap/backoff may be too low for the fixture's contention; revisit `COMMIT_MAX_RETRIES`/`COMMIT_BACKOFF_CAP` (but the spec's values are calibrated — investigate before changing).

- [ ] **Step 6: Commit**

```bash
git add src/control-plane/postgres/tests/iceberg_write_roundtrip.rs
git commit -m "test(iceberg): N=8 concurrent-append contention test over shared harness"
```

---

### Task 4: Close the register item and full-suite verification

**Files:**
- Modify: `docs/ROADMAP.md` (close `road-iceberg-cas-conflict-retry`)

**Interfaces:**
- Consumes: nothing (docs + verification only).
- Produces: the closed register entry that the PR references.

- [ ] **Step 1: Run the full postgres crate test sweep**

Because `reindeer`/native-dep regressions surface in crates the diff never touched, run the whole control-plane/postgres test set, not just the one target:

Run: `buck2 test //src/control-plane/postgres/... > /tmp/cas-pg-all.log 2>&1; grep -E "Tests finished|FAIL" /tmp/cas-pg-all.log`
Expected: `Tests finished: ... 0 failed`. In particular `sqlx-cache-check` must stay green (no SQL changed, so it will — but confirm).

- [ ] **Step 2: Run clippy across all first-party Rust**

Run: `./tools/clippy-all.sh > /tmp/cas-clippy-all.log 2>&1; grep -E "warning|error|clean|FAIL" /tmp/cas-clippy-all.log | tail -20`
Expected: clean. Fix anything attributable to this diff.

- [ ] **Step 3: Close the register item via loom-docs-update**

The item line in `docs/ROADMAP.md` is:

```
- [ ] **Multi-writer CAS-conflict retry/backoff** `{#road-iceberg-cas-conflict-retry area:iceberg status:planned from:2026-06-22-iceberg-tx-objectstore-scope-design pr:- spec:2026-06-23-iceberg-cas-conflict-retry-design}`
```

Change `- [ ]` → `- [x]`, `status:planned` → `status:done`, and `pr:-` → `pr:#<N>` once the PR number is known (do this in the same branch; the PR can be updated after creation if the number is not yet known). Then run `bash tools/docs.sh validate` to confirm the grammar still parses. Prefer driving this edit through the `loom-docs-update` skill so the prose register note is recorded consistently.

- [ ] **Step 4: Run the docs validator and markdown-lint hooks**

Run: `bash tools/docs.sh validate > /tmp/cas-docs.log 2>&1; cat /tmp/cas-docs.log`
Expected: validation passes.

Run: `buck2 run //tools:prek -- run --all-files > /tmp/cas-prek.log 2>&1; grep -E "Passed|Failed|failed" /tmp/cas-prek.log`
Expected: all hooks pass (esp. `end-of-file-fixer`, `trim trailing whitespace`, `rustfmt`, `clippy`). Commit any in-place fixes the hooks make.

- [ ] **Step 5: Commit and push**

```bash
git add docs/ROADMAP.md docs/superpowers/plans/2026-06-25-iceberg-cas-conflict-retry.md
git commit -m "docs(iceberg): close road-iceberg-cas-conflict-retry; add plan"
git push -u origin work/road-iceberg-cas-conflict-retry
```

---

## Self-Review

**1. Spec coverage:**
- *Helper `commit_append_with_retry` (reload + re-stage + commit loop, backoff+jitter)* → Task 1. ✓
- *`append_batches` and `append_batches_with_extras` route their commit through the helper; `CommitExtrasCatalog` rebuilt/stable so extras re-present per attempt* → Task 2. ✓
- *Attempt 0 reuses the already-loaded table; reload only on retry* → Task 1 helper (`mut table`, reload in the conflict arm). ✓
- *Constants `COMMIT_MAX_RETRIES=5`, `BASE=5ms`, `CAP=200ms`* → Task 1 Step 2. ✓
- *No new dependency; jitter from TableIdent⊕attempt hash; `tokio::time::sleep`* → Task 1 Step 3 `commit_backoff`. ✓
- *Inline appends out of scope; overwrite path deferred* → Global Constraints; helper is only wired to `append_batches*`. ✓
- *Error contract unchanged; conflict at cap propagates retryable-flagged* → Task 1 helper `Err(e) => return Err(e)` arm (original error, untouched). ✓
- *Keep `concurrent_appends_keep_the_mirror_consistent` at N=4; add sibling `concurrent_appends_tolerate_contention` at N=8 over a shared harness; same invariants; no minimum-retry assertion* → Task 3. ✓
- *Both tests stay `loom_fixture_test` under `iceberg-write-roundtrip`* → Task 3 (same file/target; no BUCK change needed). ✓

**2. Placeholder scan:** No TBD/TODO/"add error handling"/"similar to Task N". All code blocks are complete and the test body is shown verbatim. ✓

**3. Type consistency:** `commit_append_with_retry(catalog: &dyn Catalog, ident: &TableIdent, table: Table, data_files: Vec<DataFile>) -> Result<()>` is defined in Task 1 and called identically in Task 2 (passing `table.identifier()`, `table.clone()`). `commit_backoff(ident: &TableIdent, attempt: u32) -> Duration` is defined and called in Task 1 only. `concurrent_appends_consistent(n: i64)` defined in Task 3 Step 1, called with `4` and `8` in Steps 2–3. ✓

**API facts confirmed against the pinned iceberg rev `148afc50`:**
- `TableIdent` derives `Hash` (`crates/iceberg/src/catalog/mod.rs`) — `ident.hash(&mut hasher)` is valid.
- `Table` derives `Clone` and exposes `pub fn identifier(&self) -> &TableIdent` (`crates/iceberg/src/table.rs`) — `table.clone()` and `table.identifier()` are valid.
