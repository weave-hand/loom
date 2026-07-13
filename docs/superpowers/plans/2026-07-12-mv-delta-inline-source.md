# MV Delta over Inline-Only Sources Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix `iss-mv-delta-inline-source-unflushed`: `mv_delta_scan` fails at `catalog.load_table` for a source table that has only ever been inline-appended (every fresh micro-batch MV output), so a downstream MV chained onto an upstream MV's output cannot run until the upstream flushes.

**Architecture:** Make `mv_delta_locked` skip the file tier when the mirror reports zero live data files — mirroring the `#421` `build_serving_provider` fix (`serving.rs:168`, `serving.rs:211`) and `consolidate_locked`'s shadow read (`consolidate.rs:420`), both of which already register a file tier only when it is non-empty. Schema for the resulting empty delta comes from the **mirror** via the existing `framed_schema(&user_cols)` helper, not from the Iceberg SQL catalog. `read_files_as_batches` itself is untouched (no signature change); it is simply not called when there are no files. The workaround in `worker/tests/stream_mv_join_triggers.rs` (an explicit `flush_table` before the downstream MV reads the upstream's output) is deleted and becomes the acceptance test.

**Tech Stack:** Rust 2024, DataFusion (in-process `SessionContext` union), sqlx (no new SQL), buck2 `loom_fixture_test` with the hermetic Postgres fixture + a `tempfile` local warehouse.

**Spec:** `docs/superpowers/specs/2026-07-12-mv-delta-inline-source-design.md`
**Issue:** `iss-mv-delta-inline-source-unflushed` (`docs/ISSUES.md`, `## transform`)

## Global Constraints

Carried from the spec + `CLAUDE.md`; every task implicitly includes these:

- **Tests are `rust_test` / `loom_fixture_test` integration targets only — never inline `#[cfg(test)]`.** The new engine-serving test boots Postgres, so it MUST be a `loom_fixture_test` (`src/control-plane/postgres/defs.bzl`), not a bare `rust_test`, or the fixture env (PG binaries, `libxml2`, MinIO, boot-slot dir) is missing and the fixture cannot boot. The `no-inline-tests` prek hook fails the build on any `#[test]` under `src/**`.
- **No `.sqlx` impact.** This change adds no `query!`/`query_scalar!` SQL — it reuses `IcebergCatalog::files_with_stats` / `inline_live_batch_full` / `schema`, all already cached. Do **not** run `tools/sqlx-prepare.sh`. If the implementation drifts into new compile-time SQL, stop and reconsider (cloud sessions cannot regenerate the cache — `initdb` refuses to run as root).
- **Sources with at least one flushed file must take the existing path byte-identically** (spec, *Non-regression*). The new branch fires only when `files_with_stats(...)` is empty.
- **Clippy is strict (pedantic + restriction)** on production code: no `unwrap`/`expect`/`indexing_slicing`/`panic`/`unreachable`/`todo`. The new code uses only `Vec<&str>`, `is_empty()`, `if let Some(..)`, and `format!` — nothing that trips them. Test code is exempted from the panic-safety lints by the `loom_fixture_test` wrapper (`src/loom_test.bzl`).
- **Run prek before every commit:** `buck2 run //tools:prek -- run --all-files`. `git add` new files FIRST — prek skips untracked files (rustfmt would otherwise land as a follow-up fixup commit).
- **Build/test commands:** build `buck2 build -v0 --console none <targets>`; test `buck2 test --console none <targets>`. Never pipe a superconsole `buck2 test` through `tail`/`head`. A full local `buck2 test //src/...` needs `-j 8` (the hermetic-Postgres boot-slot pool is 8; unlimited parallelism causes non-deterministic 120s timeouts). In a cloud session build with `-M none` and scope tests to the touched targets.
- **Conventional Commits** on every commit message (the `conventional-commit` prek hook runs at commit-msg stage).

---

## Ground truth (read before starting — the spec has two path errors)

Verified against the tree at `b11ef18d`:

1. **`read_files_as_batches` lives in the control-plane crate, not engine-serving:** `src/control-plane/postgres/src/iceberg_read.rs:25`, re-exported as `control_plane_postgres::read_files_as_batches` (`postgres/src/lib.rs:42`). There is no `src/services/engine-serving/src/iceberg_read.rs`.
2. `read_files_as_batches`'s current signature (unchanged by this plan):

```rust
pub async fn read_files_as_batches(
    catalog: &SqlCatalog,
    table: &TableRef,
    files: &[String],
) -> Result<(SchemaRef, Vec<RecordBatch>)>
```

   Its second statement is the defect's root: `let tbl = catalog.load_table(&ident).await.map_err(backend)?;` (`iceberg_read.rs:32`) — called **before** the (possibly empty) `files` loop, purely so the Arrow schema can be derived from `tbl.metadata().current_schema()` (`iceberg_read.rs:38-40`). `SqlCatalog::load_table` returns `no_such_table_err(identifier)` when the vendored `iceberg_tables` catalog has no row (`postgres/src/iceberg_sql_catalog/catalog.rs:822-825`).
3. The only writer of that catalog row is `ensure_iceberg_table` (`iceberg_landing.rs:628`), reached **only** from `append_parquet_snapshot` (`iceberg_landing.rs:267`), the CDC changelog pre-create (`iceberg_landing.rs:177`, `iceberg_flush.rs:258`), and the direct-write paths (`iceberg_landing.rs:565`, `:944`, `iceberg_control_plane.rs:155`). The inline branch of `land_cdc` (`iceberg_landing.rs:181-195` → `inline_append_decl`) never calls it, and neither does `inline_append_mv` (`iceberg_inline.rs:826`, which delegates to `inline_append_decl`). So an inline-only table genuinely has no loadable Iceberg metadata — the spec's claim holds.
4. The mirror, by contrast, IS populated by the inline path — which is why `mv_delta_locked`'s `ice.current_snapshot(table)` and `ice.schema(table, current.id)` already succeed for such a table, and only the `read_files_as_batches` leg fails. That asymmetry is exactly what makes this a bug and not a design gap.
5. **The `docs/ISSUES.md` entry's "fix shape" sentence is stale** — it says "teach `read_files_as_batches` to serve the inline tier (files ∪ inline)". The spec explicitly rules that out ("Explicitly not in scope: making `read_files_as_batches` itself mirror-schema-authoritative"). Follow the spec; the ISSUES entry is deleted on close anyway (Task 4).

**Every call site of `read_files_as_batches`** (grep, first-party, excluding `buck-out`) — **none needs a signature change**, and none is edited by this plan except the one inside `mv_delta_locked` (moved under an `if`):

| Call site | Table always has an Iceberg catalog row? |
|---|---|
| `src/services/engine-serving/src/mv_delta.rs:131` | **NO** — the bug. Fixed in Task 2. |
| `src/services/engine-serving/src/consolidate.rs:219` | CDC/identity base; see the honesty note in Self-Review. Out of scope per spec. |
| `src/services/engine-serving/src/consolidate.rs:422` | Already guarded by `if has_files` — the pattern this fix copies. |
| `src/control-plane/postgres/src/vector_index.rs:441` | Cold tier — only called with file paths from the mirror; index build implies files. |
| `src/services/engine/src/flight.rs:306` | `TableTicket` file read — the caller supplies `req.files` from a mirror listing. |
| tests: `postgres/tests/{iceberg_read,vector_landing,stream_cdc_dual_flush}.rs`, `query-api/tests/stream_{cdc_consolidate,merge_firstrow,merge_versioned}.rs`, `engine-serving/tests/cow_consolidate.rs` | All seed Parquet first. |

**Does the inline tier need a Postgres pool that the file read lacks?** No. `mv_delta_locked` already holds `pool: &PgPool` and constructs `let ice = IcebergCatalog::new(pool.clone());` (`mv_delta.rs:100`); the inline tier is read through `ice.inline_live_batch_full(table, current.id)` (`mv_delta.rs:140-143`), which acquires its own connection from that pool. It is **already fetched today**, before the fix. Nothing new is plumbed.

**Snapshot consistency.** Both tiers are already pinned to one snapshot: `current` from `ice.current_snapshot(table)` (`mv_delta.rs:101`) feeds both `files_with_stats(table, current.id)` and `inline_live_batch_full(table, current.id)`, and the whole read runs under the per-table advisory lock `lock_table(pool, table)` taken in `mv_delta_scan` (`mv_delta.rs:87`) — the same lock flush/GC/consolidate take, so a concurrent flush cannot move rows between the tiers mid-read. The fix does not touch any of that: it only decides whether to *call* `read_files_as_batches` for the same already-snapshotted `paths`, and the empty-tier schema is derived from `ice.schema(table, current.id)` (`mv_delta.rs:112-123`) — the mirror at the SAME snapshot. No new consistency surface.

---

## File Structure

**Create:**
- `src/services/engine-serving/tests/mv_delta_inline_source.rs` — the fixture test for the three spec cases (inline-only full delta; file+inline union after a flush; empty delta when the watermark has consumed everything).

**Modify (production):**
- `src/services/engine-serving/src/mv_delta.rs` — `mv_delta_locked` (`:93-201`): early-return when neither tier exists; register the file tier only when `paths` is non-empty; build the union SQL from the registered tiers. **Only production file changed.**

**Modify (tests/build):**
- `src/services/engine-serving/BUCK` — new `loom_fixture_test` target `mv-delta-inline-source` (mirror `serving-empty-table` at the file's tail, which already deps `//src/testing:seed` + `//third-party:tempfile`).
- `src/services/worker/tests/stream_mv_join_triggers.rs` — delete the `flush_table` workaround (`:594-612`) and its now-unused import (`:40`); assert the source is genuinely file-less at that point.
- `docs/ISSUES.md` — remove the closed item (Task 4).
- `docs/system-capabilities/` — record the landed behavior (Task 4).

---

## Task 1: Failing fixture test for `mv_delta_scan` over an inline-only source

**Files:**
- Create: `src/services/engine-serving/tests/mv_delta_inline_source.rs`
- Modify: `src/services/engine-serving/BUCK` (append a `loom_fixture_test` after the `serving-empty-table` target at the end of the file)

**Interfaces:**
- Consumes: `engine_serving::mv_delta_scan(cp: &PgControlPlane, catalog: &SqlCatalog, pool: &PgPool, table: &TableRef, mv: &str) -> Result<(SchemaRef, Vec<RecordBatch>), EngineServingError>` (re-exported at `engine_serving::mv_delta_scan`, `lib.rs:23`); `control_plane_postgres::fixture::PgFixture` (`fresh_db` → `(PgControlPlane, String)`, `pool_for`, `pg_dsn`); `loom_test_seed::local_sql_catalog(dsn: String, warehouse: &str) -> SqlCatalog`; `control_plane_postgres::iceberg_landing::{land, InlineLimits}` (`land(pool, catalog, table, columns, schema, batches, limits, lineage, stream_buckets) -> Result<SnapshotId>`); `control_plane_postgres::iceberg_flush::flush_table(catalog, pool, table, run_id) -> Result<Option<SnapshotId>>`; `control_plane_postgres::iceberg_mirror::live_table_id(&mut conn, schema, name) -> Result<Option<i64>>`; `control_plane_core::{MvWatermarks, WatermarkAdvance{bucket, from, to}}`.
- Produces: the three `#[tokio::test]` fns Task 2 must turn green — `inline_only_source_reads_full_delta_without_flush`, `flushed_then_inline_source_unions_both_tiers`, `inline_only_source_with_consumed_watermark_reads_empty_delta` — and the buck target `//src/services/engine-serving:mv-delta-inline-source`.

**Facts the test leans on (verified):**
- `land(..., stream_buckets = Some(2))` with `InlineLimits { inline_byte_limit: usize::MAX, flush_byte_threshold: i64::MAX }` takes the inline branch (`iceberg_landing.rs:181`: `if bytes <= limits.inline_byte_limit`) and declares the table a LOG stream (`StreamDecl::Log(2)`), so `cp.stream_meta(tid)` returns `StreamKind::Log` — the kind `mv_delta_scan` requires (`mv_delta.rs:68-78`). It writes **no** Parquet and **no** Iceberg catalog row. That is precisely the "fresh MV output" shape (`commit_micro_batch` → `inline_append_mv`).
- The framed physical schema of a declared log table is user columns **then** `loom_change_kind` (string, non-null), `loom_bucket` (integer, nullable), `loom_offset` (long, nullable) — `framing_column_specs()` (`iceberg_landing.rs:659-677`).
- A `(mv, source_table_id, bucket)` with no `stream.mv_watermark` row reads as offset 0 (`mv_delta.rs:179-184`), so a fresh `mv` name sees the full delta with no setup.

- [ ] **Step 1: Write the failing test**

Create `src/services/engine-serving/tests/mv_delta_inline_source.rs`:

```rust
//! `mv_delta_scan` over a source table that has ONLY ever been inline-appended —
//! the shape of every fresh micro-batch MV output (`commit_micro_batch` ->
//! `inline_append_mv`), which never creates an Iceberg SQL-catalog row
//! (`ensure_iceberg_table` runs only on the Parquet-write path). Before the fix,
//! the file leg (`read_files_as_batches` -> `catalog.load_table`) errored even
//! with an EMPTY file list, so a downstream MV chained onto a fresh upstream MV
//! output could not run until the upstream flushed
//! (`iss-mv-delta-inline-source-unflushed`).
//!
//! Cases: (1) inline-only source -> the full framed delta; (2) flush, then more
//! inline rows -> the file ∪ inline union still correct (the transition case);
//! (3) inline-only source whose watermark has consumed everything -> an EMPTY
//! delta over the mirror-derived framed schema, not an error.
//!
//! loom_fixture_test (Postgres + a local `tempfile` warehouse), harness mirrored
//! from `serving_empty_table.rs` (PgFixture + `local_sql_catalog`) and
//! `postgres/tests/iceberg_flush.rs` (land/flush seeding).
//! Spec: docs/superpowers/specs/2026-07-12-mv-delta-inline-source-design.md

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Int32Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use control_plane_core::{
    ColumnSpec, DatasetId, EventType, LineageEvent, MvWatermarks, RunId, TableRef,
    WatermarkAdvance,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_flush::flush_table;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use control_plane_postgres::iceberg_mirror::live_table_id;
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use engine_serving::mv_delta_scan;
use loom_test_seed::local_sql_catalog;
use sqlx::PgPool;

/// The MV key every case reads under. A key with no `stream.mv_watermark` rows
/// reads every bucket at offset 0 (the documented "absent reads as 0" contract).
const MV: &str = "mv:test";

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef {
        schema: schema.into(),
        name: name.into(),
    }
}

/// The `(id long, val long)` logical (framing-free) schema of the source.
fn columns() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        },
        ColumnSpec {
            name: "val".into(),
            ty: "long".into(),
            nullable: false,
        },
    ]
}

fn arrow_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
    ]))
}

fn batch(ids: &[i64], vals: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        arrow_schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(Int64Array::from(vals.to_vec())),
        ],
    )
    .expect("batch")
}

fn lineage(table: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(table).dataset_ref()],
        payload: serde_json::json!({ "source": "mv-delta-inline-source-test" }),
    }
}

/// Land `rows` INLINE ONLY into `table`, declaring it a 2-bucket LOG stream:
/// `inline_byte_limit: usize::MAX` forces `land`'s inline branch, so no Parquet
/// file and no Iceberg SQL-catalog row is ever created; `flush_byte_threshold:
/// i64::MAX` disarms the byte-trigger auto-flush.
async fn land_inline(pool: &PgPool, catalog: &SqlCatalog, table: &TableRef, ids: &[i64], vals: &[i64]) {
    land(
        pool,
        catalog,
        table,
        &columns(),
        arrow_schema(),
        vec![batch(ids, vals)],
        InlineLimits {
            inline_byte_limit: usize::MAX,
            flush_byte_threshold: i64::MAX,
        },
        lineage(table),
        Some(2),
    )
    .await
    .expect("inline land (log stream declare)");
}

/// The `id` column (column 0 of the framed projection), sorted.
fn ids(batches: &[RecordBatch]) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id is Int64");
        for i in 0..b.num_rows() {
            out.push(arr.value(i));
        }
    }
    out.sort_unstable();
    out
}

/// The `(loom_bucket, loom_offset)` framing pairs, in the order the scan returned
/// them (the scan's `order by loom_bucket, loom_offset`).
fn framing(batches: &[RecordBatch]) -> Vec<(i32, i64)> {
    let mut out = Vec::new();
    for b in batches {
        let bucket = b
            .column_by_name("loom_bucket")
            .expect("loom_bucket present")
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("loom_bucket is Int32");
        let offset = b
            .column_by_name("loom_offset")
            .expect("loom_offset present")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("loom_offset is Int64");
        for i in 0..b.num_rows() {
            out.push((bucket.value(i), offset.value(i)));
        }
    }
    out
}

/// Field names of the framed delta schema.
fn names(schema: &SchemaRef) -> Vec<String> {
    schema.fields().iter().map(|f| f.name().clone()).collect()
}

/// CAS-advance `MV`'s watermark past every `(bucket, offset)` in `batches` — the
/// same `advance_mv_watermark` a micro-batch commit issues. After this the MV has
/// "consumed" that delta.
async fn consume(cp: &PgControlPlane, tid: i64, batches: &[RecordBatch]) {
    let mut maxes: BTreeMap<i32, i64> = BTreeMap::new();
    for (bucket, offset) in framing(batches) {
        let e = maxes.entry(bucket).or_insert(offset);
        *e = (*e).max(offset);
    }
    let current = cp.mv_watermarks(MV, tid).await.expect("mv_watermarks");
    let advances: Vec<WatermarkAdvance> = maxes
        .iter()
        .map(|(&bucket, &max_offset)| WatermarkAdvance {
            bucket,
            from: current.get(&bucket).copied().unwrap_or(0),
            to: max_offset + 1,
        })
        .collect();
    cp.advance_mv_watermark(MV, tid, &advances)
        .await
        .expect("advance_mv_watermark");
}

/// Resolve the live mirror table id of `table`.
async fn tid_of(pool: &PgPool, table: &TableRef) -> i64 {
    let mut conn = pool.acquire().await.expect("acquire");
    live_table_id(&mut conn, &table.schema, &table.name)
        .await
        .expect("live_table_id")
        .expect("table is live")
}

/// Case 1 (the defect): a source that has NEVER been flushed — no Parquet files,
/// no Iceberg catalog row — must still yield its full framed delta. Before the
/// fix this errored inside `read_files_as_batches`'s unconditional
/// `catalog.load_table`, even though the file list was empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_only_source_reads_full_delta_without_flush() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let table = tref("s", "mv_out");

    land_inline(&pool, &catalog, &table, &[1, 2, 3], &[10, 20, 30]).await;

    let (schema, batches) = mv_delta_scan(&cp, &catalog, &pool, &table, MV)
        .await
        .expect("an inline-only source must read as a delta without a flush");

    assert_eq!(ids(&batches), vec![1, 2, 3], "the whole inline tail is the delta");
    assert_eq!(
        names(&schema),
        vec!["id", "val", "loom_change_kind", "loom_bucket", "loom_offset"],
        "framed schema: user columns then the three reserved framing columns"
    );
    let pairs = framing(&batches);
    let mut sorted = pairs.clone();
    sorted.sort_unstable();
    assert_eq!(pairs, sorted, "rows come back (loom_bucket, loom_offset)-ordered");
}

/// Case 2 (the transition): flush the inline-only source, then inline-append more
/// rows. The delta is now the UNION of the file tier and the still-live inline
/// tail — the pre-fix path for the file leg, unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flushed_then_inline_source_unions_both_tiers() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let table = tref("s", "mv_out");

    land_inline(&pool, &catalog, &table, &[1, 2], &[10, 20]).await;
    flush_table(&catalog, &pool, &table, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush")
        .expect("the inline rows were flushed to Parquet");
    land_inline(&pool, &catalog, &table, &[3, 4], &[30, 40]).await;

    let (_schema, batches) = mv_delta_scan(&cp, &catalog, &pool, &table, MV)
        .await
        .expect("file ∪ inline delta");
    assert_eq!(
        ids(&batches),
        vec![1, 2, 3, 4],
        "flushed rows (file tier) UNION the un-flushed tail (inline tier)"
    );
}

/// Case 3 (the empty delta): an inline-only source whose watermark has already
/// consumed every event. No files, nothing live above the watermark — an EMPTY
/// delta over the mirror-derived framed schema, never an error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_only_source_with_consumed_watermark_reads_empty_delta() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let table = tref("s", "mv_out");

    land_inline(&pool, &catalog, &table, &[1, 2, 3], &[10, 20, 30]).await;
    let tid = tid_of(&pool, &table).await;

    let (_schema, first) = mv_delta_scan(&cp, &catalog, &pool, &table, MV)
        .await
        .expect("first delta");
    consume(&cp, tid, &first).await;

    let (schema, batches) = mv_delta_scan(&cp, &catalog, &pool, &table, MV)
        .await
        .expect("a fully-consumed inline-only source reads EMPTY, not an error");
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        0,
        "nothing at or beyond the watermark"
    );
    assert_eq!(
        names(&schema),
        vec!["id", "val", "loom_change_kind", "loom_bucket", "loom_offset"],
        "an empty delta still carries the mirror-derived framed schema"
    );
}
```

Then append the target to `src/services/engine-serving/BUCK` (after the `serving-empty-table` target, which is the last one in the file):

```python
loom_fixture_test(
    name = "mv-delta-inline-source",
    crate = "mv_delta_inline_source",
    srcs = ["tests/mv_delta_inline_source.rs"],
    crate_root = "tests/mv_delta_inline_source.rs",
    deps = [
        ":engine-serving",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//src/testing:seed",
        "//third-party:arrow",
        "//third-party:serde_json",
        "//third-party:sqlx",
        "//third-party:tempfile",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```

(`loom_fixture_test` is already loaded at the top of that BUCK: `load("//src/control-plane/postgres:defs.bzl", "loom_fixture_test")`. It sets `edition` itself — do not add one, matching the neighbouring fixture targets.)

- [ ] **Step 2: Run the test to verify it fails**

```bash
buck2 test --console none //src/services/engine-serving:mv-delta-inline-source
```

Expected: `Tests finished: Pass 1. Fail 2.` — `inline_only_source_reads_full_delta_without_flush` and `inline_only_source_with_consumed_watermark_reads_empty_delta` fail with a panic from their `.expect(...)`, carrying an `engine serving: ...` error whose text names the missing Iceberg table (`load_table`'s `NoSuchTableExist` for `s.mv_out`). `flushed_then_inline_source_unions_both_tiers` PASSES already — it is the non-regression control (its source has files, so `load_table` resolves).

If instead all three pass, STOP: the seeding is wrong (most likely `land` took the Parquet branch, creating the catalog row). Check that `inline_byte_limit: usize::MAX` is set and that no `flush_table` runs before the case-1 scan.

- [ ] **Step 3: Commit the failing test**

```bash
git add src/services/engine-serving/tests/mv_delta_inline_source.rs src/services/engine-serving/BUCK
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "test(engine-serving): pin mv_delta_scan over an inline-only source"
```

---

## Task 2: Skip the file tier when the mirror reports no live files

**Files:**
- Modify: `src/services/engine-serving/src/mv_delta.rs:125-173` (inside `mv_delta_locked`)

**Interfaces:**
- Consumes: `IcebergCatalog::files_with_stats(table, snap) -> Result<Vec<FileWithStats>>`, `IcebergCatalog::inline_live_batch_full(table, snap) -> Result<Option<(i64, Vec<i64>, RecordBatch)>>`, `datafusion_io::register_batches(&SessionContext, name, SchemaRef, Vec<RecordBatch>)`, and the file-local `framed_schema(user_cols: &[ColumnSpec]) -> Result<SchemaRef, EngineServingError>` (`mv_delta.rs:206`) — all already imported in this file. **No signature changes anywhere.**
- Produces: `mv_delta_locked` returning `Ok((framed_schema(&user_cols)?, vec![]))` when neither tier exists, and never calling `read_files_as_batches` when `paths.is_empty()`.

**Why this shape (and not the alternatives):** it is the same mirror-authoritative pattern as the closed `iss-serving-empty-table-not-found` (#421): `build_serving_provider` derives its schema from `arrow_schema_from_mirror(&table_schema.columns)` (`serving.rs:132`), sets `file_provider = None` when `files_with_stats.is_empty()` (`serving.rs:168-175`), and returns a zero-row `empty_provider(&schema)` when both tiers are absent (`serving.rs:211`, `:228`). That fix never calls `load_table` at all — this task reuses the same seam (skip-empty file tier + mirror-derived schema) instead of inventing a parallel one. `consolidate_locked`'s shadow read already does the identical `if has_files { read_files_as_batches(...) }` guard (`consolidate.rs:420-426`) — copy that.

- [ ] **Step 1: Replace the tier-read + registration block**

In `src/services/engine-serving/src/mv_delta.rs`, replace lines 125-173 (from the `// The table's physical framed rows` comment through the `union_sql` binding) with:

```rust
    // The table's physical framed rows: live Parquet files ...
    let files = ice
        .files_with_stats(table, current.id)
        .await
        .map_err(to_serving)?;
    let paths: Vec<String> = files.into_iter().map(|f| f.path).collect();

    // ... UNION any still-live inline tail (un-flushed events), so a delta read
    // that runs without a preceding flush still sees the whole tail. `_full`
    // keeps every framed row (a log table never emits `-U`, so this is exactly
    // "every live inline row" — the `_full` variant is used only for parity with
    // `consolidate_locked`'s read shape).
    let inline = ice
        .inline_live_batch_full(table, current.id)
        .await
        .map_err(to_serving)?;

    // Neither tier: an empty delta over the MIRROR-derived framed schema. Returning
    // here (rather than falling through) is what keeps `read_files_as_batches` — and
    // therefore `catalog.load_table` — off the path for a table that has ONLY ever
    // been inline-appended: such a table has no vendored `iceberg_tables` row (only
    // the Parquet-write path's `ensure_iceberg_table` creates one), so `load_table`
    // would error even for an EMPTY file list. Same mirror-authoritative shape as
    // `build_serving_provider`'s zero-row provider (`serving.rs:211`, the #421
    // `iss-serving-empty-table-not-found` fix).
    if paths.is_empty() && inline.is_none() {
        return Ok((framed_schema(&user_cols)?, vec![]));
    }

    // Register ONLY the tiers that exist. Skipping the file tier when the mirror
    // reports no live files is the other half of the same fix — an MV output is
    // inline-only until its first flush (`commit_micro_batch` -> `inline_append_mv`),
    // and a downstream MV must be able to read it as a source immediately
    // (`iss-mv-delta-inline-source-unflushed`). A source WITH files takes the
    // unchanged path. Mirrors `consolidate_locked`'s shadow read
    // (`consolidate.rs:420`).
    let df_ctx = SessionContext::new();
    let mut tiers: Vec<&str> = Vec::new();
    if !paths.is_empty() {
        let (file_schema, file_batches) = read_files_as_batches(catalog, table, &paths)
            .await
            .map_err(to_serving)?;
        register_batches(&df_ctx, "mv_delta_files", file_schema, file_batches)
            .map_err(to_serving)?;
        tiers.push("mv_delta_files");
    }
    if let Some((_, _, inline_batch)) = &inline {
        register_batches(
            &df_ctx,
            "mv_delta_inline",
            inline_batch.schema(),
            vec![inline_batch.clone()],
        )
        .map_err(to_serving)?;
        tiers.push("mv_delta_inline");
    }

    let col_list = user_cols
        .iter()
        .map(|c| quote_ident(&c.name))
        .collect::<Vec<_>>()
        .join(", ");
    // One `select ... from <tier>` per registered tier, UNION ALL'd. With both tiers
    // present this is byte-identical to the previous two-arm `has_inline` format!.
    let union_sql = tiers
        .iter()
        .map(|t| {
            format!("select {col_list}, loom_change_kind, loom_bucket, loom_offset from {t}")
        })
        .collect::<Vec<_>>()
        .join(" union all ");
```

Everything below (the per-bucket watermark predicate at `:175-189`, the `ctx.sql` + `collect`, and the `batches.first()` / `framed_schema` schema fallback at `:194-199`) is UNCHANGED. `read_files_as_batches` keeps its current signature and its `use control_plane_postgres::read_files_as_batches;` import (`mv_delta.rs:26`) — it is still called, just not when the file list is empty.

- [ ] **Step 2: Update the module doc comment**

`mv_delta.rs:1-9`'s header says the read mirrors `consolidate_locked`'s shape "`files_with_stats` -> `read_files_as_batches` + `inline_live_batch_full`". Adjust line 5-6 to record the skip:

```rust
//! `schema` -> `user_cols` + `files_with_stats` -> `read_files_as_batches` (only
//! when the mirror reports live files — an inline-only source has no Iceberg
//! catalog row to load) + `inline_live_batch_full` read — but folds NOTHING: the
```

Also extend `mv_delta_scan`'s doc (`mv_delta.rs:40-48`) with one sentence: `A source that has only ever been inline-appended (every fresh micro-batch MV output) reads fine — the file tier is skipped, not loaded.`

- [ ] **Step 3: Run the test to verify it passes**

```bash
buck2 test --console none //src/services/engine-serving:mv-delta-inline-source
```

Expected: `Tests finished: Pass 3. Fail 0.`

- [ ] **Step 4: Lint + non-regression on the touched crate**

```bash
buck2 build -v0 --console none //src/services/engine-serving/...
buck2 build --console none --show-simple-output '//src/services/engine-serving:engine-serving[clippy.txt]'
```

Read the printed path and `cat` it in a separate step — an EMPTY file means clippy-clean (pedantic + restriction are on).

```bash
buck2 test --console none //src/services/engine-serving/...
```

Expected: every engine-serving test target passes (notably `mv-enrich-scan`, `merge-on-read`, `cow-consolidate`, `serving-empty-table` — the file-backed paths must be untouched).

- [ ] **Step 5: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "fix(engine-serving): mv_delta over an inline-only source — skip the empty file tier

mv_delta_locked no longer calls read_files_as_batches (and therefore
catalog.load_table) when the mirror reports no live data files, and returns a
mirror-derived framed schema with zero batches when neither tier exists. A table
that has only ever been inline-appended — every fresh micro-batch MV output —
has no vendored iceberg_tables row, so the old unconditional load_table failed
even for an empty file list. Same mirror-authoritative shape as
build_serving_provider's zero-row provider (#421).

Part of iss-mv-delta-inline-source-unflushed."
```

---

## Task 3: Delete the `flush_table` workaround — the chained-MV acceptance test

**Files:**
- Modify: `src/services/worker/tests/stream_mv_join_triggers.rs:40` (import), `:590-612` (the workaround), `:1-25` (header comment)

**Interfaces:**
- Consumes: the Task 2 fix; `control_plane_core::Catalog` (for `current_snapshot`) and `control_plane_postgres::iceberg_catalog::IcebergCatalog::files_with_stats` — added as imports here.
- Produces: nothing downstream; this is the acceptance test for the whole plan (spec *Acceptance* 1 & 2).

**Current state (verified):** `flush_table` is imported at `:40` (`use control_plane_postgres::iceberg_flush::flush_table;`) and called exactly once, at `:605`, under a 12-line comment (`:594-604`) that describes this defect verbatim ("an MV output that has ONLY ever been inline-appended (as every `commit_micro_batch` write is) has no such row yet"). Removing the call makes the import unused → a build error, so both must go.

- [ ] **Step 1: Delete the workaround and assert the source is genuinely file-less**

Replace `stream_mv_join_triggers.rs:592-612` (the `// Flush enriched_orders …` comment block plus the `flush_table(...)` call and its `.expect(...)`) with:

```rust
    // NO FLUSH. `s.enriched_orders` has only ever been inline-appended (every
    // `commit_micro_batch` write goes through `inline_append_mv`), so it has no
    // Iceberg SQL-catalog row — and `mv_delta_scan` reads it as the downstream
    // MV's SOURCE anyway: the file tier is skipped when the mirror reports no
    // live files (`iss-mv-delta-inline-source-unflushed`). Pin that precondition,
    // so a future auto-flush cannot silently turn this back into the
    // already-file-backed case and hide a regression.
    let ice = IcebergCatalog::new(pool.clone());
    let dst_current = ice
        .current_snapshot(&dst)
        .await
        .expect("the join MV's output has a mirror snapshot");
    assert!(
        ice.files_with_stats(&dst, dst_current.id)
            .await
            .expect("files_with_stats")
            .is_empty(),
        "Case 3 precondition: the join MV's output is INLINE-ONLY (zero Parquet \
         files) when the downstream MV reads it as a delta source"
    );
```

Then:
- delete the import at `:40` (`use control_plane_postgres::iceberg_flush::flush_table;`);
- add `use control_plane_postgres::iceberg_catalog::IcebergCatalog;` alongside the other `control_plane_postgres` imports;
- add `Catalog` to the `control_plane_core::{...}` import list at `:32-37` (`current_snapshot` is a `Catalog` trait method — `iceberg_catalog.rs:274-276`; `files_with_stats` is inherent).

`pool` is already in scope in this test fn (it is the `PgPool` the fixture handed out and that `land`/`flush_table` were called with). `uuid` stays used elsewhere (`lin()`), so its import stays.

- [ ] **Step 2: Update the file header**

`stream_mv_join_triggers.rs:12-14` describes case 3. Append to that bullet: `— with NO flush of the upstream output first: the downstream MV reads an inline-only source (iss-mv-delta-inline-source-unflushed).`

- [ ] **Step 3: Run the acceptance test**

```bash
buck2 test --console none //src/services/worker:stream-mv-join-triggers
```

Expected: `Tests finished: Pass N. Fail 0.` (all four cases in the file, including `enrich_edge_cycle_rejected_at_define_time`). If Case 3 fails at `handle_stream_mv` with an `engine serving:`/`load_table` message, Task 2 is not actually on the engine path being exercised — the worker test spawns a real engine over UDS (`loom_test_flight::spawn_engine_uds`), so rebuild rather than assuming a stale artifact.

- [ ] **Step 4: Full worker + engine sweep**

```bash
buck2 test --console none //src/services/worker/... //src/services/engine/... //src/services/engine-serving/...
```

Expected: all pass. `stream-mv-e2e` (which flushes between micro-batches deliberately) is the file-tier non-regression signal.

- [ ] **Step 5: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "test(worker): drop the flush_table workaround from the chained-MV e2e

Case 3 now runs the downstream MV against an INLINE-ONLY upstream output (and
asserts zero Parquet files at that snapshot), which is the acceptance test for
iss-mv-delta-inline-source-unflushed."
```

---

## Task 4: Close the register item + record the capability

**Files:**
- Modify: `docs/ISSUES.md` (delete the `iss-mv-delta-inline-source-unflushed` entry, lines 30-31 under `## transform`)
- Modify: `docs/system-capabilities/` — the transform/stream capability page (find it: `ls docs/system-capabilities/` and pick the page covering micro-batch MVs; read its README for the house style first)

**Interfaces:** none (docs only).

- [ ] **Step 1: Run the docs-update skill**

Invoke the `loom-docs-update` skill. It closes the resolved register item and records any new deferrals, staged alongside the work. The item to close is `iss-mv-delta-inline-source-unflushed`; registers carry OPEN work only, so the entry is **removed**, not checked off.

- [ ] **Step 2: Record the landed behavior in system-capabilities**

Add one sentence to the micro-batch-MV / stream section: a micro-batch MV's output is readable as another MV's delta source immediately, with no flush — `mv_delta_scan` skips the file tier when the mirror reports no live data files and derives the framed schema from the mirror.

- [ ] **Step 3: Validate the registers**

```bash
bash tools/docs.sh validate
```

Expected: no errors (grammar/ids/vocab/links, and every `spec:` slug resolving on disk).

- [ ] **Step 4: Full suite + commit**

```bash
buck2 test --console none //src/... -j 8
```

(Local: `-j 8` — the hermetic-Postgres fixture has 8 boot slots and an unthrottled full suite throws non-deterministic 120s timeouts. Cloud: build with `-M none` and scope to the touched targets instead; `buck2 clean` between heavy phases.)

Expected: `Tests finished: Pass N. Fail 0.`

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "docs: close iss-mv-delta-inline-source-unflushed"
```

Then finish the branch with the `superpowers:finishing-a-development-branch` skill (push + open a PR — never a local merge).

---

## Self-Review

**1. Spec coverage**

| Spec requirement | Task |
|---|---|
| Design 1 — `mv_delta_locked`: when `paths.is_empty()`, skip `read_files_as_batches` entirely | Task 2, Step 1 (`if !paths.is_empty() { … }` + the both-tiers-absent early return) |
| Design 2 — derive the union/predicate schema from the mirror via `framed_schema(&user_cols)`, not from the loaded Iceberg table; leave the non-empty-files leg untouched | Task 2, Step 1. Note the *precise* shape: when files exist the leg is byte-identical (still `read_files_as_batches`'s `load_table` schema); when they do not, the only schema consumer left is the zero-row return + the existing `batches.first()`-is-`None` fallback, both of which already used `framed_schema`. No new schema plumbing was needed — the union SQL selects by NAME from whichever tiers registered. |
| Design 3 — delete the `flush_table` workaround + comment from `stream_mv_join_triggers.rs` | Task 3 |
| Not in scope — refactoring `read_files_as_batches` itself | Honored: no signature change, no call-site change (the call-site table above enumerates all 6 production + 7 test callers). |
| Testing — chained MV without flush | Task 3 (`//src/services/worker:stream-mv-join-triggers`, Case 3) |
| Testing — inline-only source unit + the flush transition | Task 1 (`inline_only_source_reads_full_delta_without_flush`, `flushed_then_inline_source_unions_both_tiers`) |
| Testing — empty source | Task 1 (`inline_only_source_with_consumed_watermark_reads_empty_delta`) |
| Non-regression — flushed sources byte-identical; no migration/new SQL | Task 2 Step 4 (crate sweep) + Task 3 Step 4 (`stream-mv-e2e`); Global Constraints (no `.sqlx`) |
| Acceptance 1-3 | Task 3 (1, 2) and Task 4 Step 4 (3) |
| Global constraints (clippy, prek, `rust_test`-only, sqlx) | Global Constraints section; each task's commit step |

**2. Placeholder scan** — no TBD/TODO/"handle edge cases"/"similar to Task N". Every code step carries the literal code; every command carries its expected output.

**3. Type consistency** — `mv_delta_scan(&PgControlPlane, &SqlCatalog, &PgPool, &TableRef, &str) -> Result<(SchemaRef, Vec<RecordBatch>), EngineServingError>` is used identically in Task 1's three tests and unchanged in Task 2. `framed_schema(&[ColumnSpec]) -> Result<SchemaRef, EngineServingError>` is the existing private helper (`mv_delta.rs:206`) — same name in the early return and the untouched `batches.first()` fallback. `register_batches(&SessionContext, &str, SchemaRef, Vec<RecordBatch>)` is called exactly as today. `WatermarkAdvance { bucket: i32, from: i64, to: i64 }` matches `core/src/stream.rs:159-163`. `InlineLimits { inline_byte_limit: usize, flush_byte_threshold: i64 }` matches `iceberg_landing.rs:56-62` (hence `usize::MAX` / `i64::MAX`). `flush_table(&SqlCatalog, &PgPool, &TableRef, RunId) -> Result<Option<SnapshotId>>` matches `iceberg_flush.rs:40-45`.

**4. Failure-mode honesty**

- **`consolidate_locked` has the SAME latent bug and this plan does not fix it.** `consolidate.rs:219` calls `read_files_as_batches(catalog, table, &paths)` unconditionally (its *shadow* sibling at `:422` is already guarded by `if has_files`). A CDC/identity base that has only ever been inline-appended would fail identically at `load_table`. It is not reachable through the MV path this issue is about, the spec scopes it out, and no test exercises it — but an implementer who "helpfully" extends the fix there is outside the spec and outside this plan's test coverage. Preferred handling: leave it, and (during Task 4) file a new ISSUES item pointing at `consolidate.rs:219` rather than widening this PR.
- **The `docs/ISSUES.md` entry's stated fix shape ("teach `read_files_as_batches` to serve the inline tier") is not what gets built** — the spec supersedes it. Anyone reconciling the register against the diff should expect that mismatch.
- **Task 1's case 2 passes before the fix.** That is intentional (it is the byte-identical-file-path control), but it means the RED signal in Task 1 Step 2 is `Pass 1. Fail 2.`, not a clean total failure. Do not "fix" the harness to make case 2 fail.
- **The new test's inline-only seeding depends on `land`'s inline branch being taken** (`bytes <= inline_byte_limit`). If a future change moves the branch or arms an auto-flush regardless of `flush_byte_threshold`, these tests would silently start exercising the file path. Task 3's explicit `files_with_stats(...).is_empty()` assertion is the durable guard for the e2e; the unit tests rely on `land`'s documented threshold semantics.
- **Bucket assignment is not asserted** — only that rows come back `(loom_bucket, loom_offset)`-ordered and that the id set is complete. Hash-bucketing is another slice's contract; pinning it here would couple this test to it.
