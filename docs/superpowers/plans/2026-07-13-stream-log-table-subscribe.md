# Log-table (non-CDC) subscribe Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make an append-only (log) stream table subscribable through `GET /objects/{type}/changes`, exactly like a CDC table, by reading the base table's own offset-framed rows (files ∪ inline) as the tail feed.

**Architecture:** A log table carries the same `(loom_change_kind, loom_bucket, loom_offset)` framing as CDC but has no `__changelog` sibling — the base rows *are* the log. Three seams change, all engine/control-plane side (no wire, no client, no handler-contract change): (1) the positions probe stops refusing log tables; (2) the feed scan gains a LOG arm that reads the base table directly (mirroring `mv_delta_scan`'s files ∪ inline union) instead of the changelog ∪ inline union, dispatched by `StreamMeta.kind`; (3) the inline-append NOTIFY fires for log tables so `await_changelog` wakes promptly. The kind dispatch lives inside `changelog_feed_scan` (engine-serving), the single chokepoint every serving path (in-process and the already-shipped production wire, PR #426) funnels through.

**Tech Stack:** Rust, buck2, DataFusion, Arrow, Iceberg mirror catalog, Postgres (sqlx compile-time macros), Axum, tonic/Flight.

## Global Constraints

- **Strict clippy (pedantic + restriction)** on production lib/bin code — no `unwrap`/`expect`/`panic`/`indexing_slicing`/`todo` etc. Use `#[expect(lint, reason = "...")]` locally; every allow needs a reason. Test code is exempt from the panic-safety lints via the `loom_rust_test`/`loom_fixture_test` wrappers.
- **Tests are `rust_test` / `loom_fixture_test` integration targets only** — never inline `#[cfg(test)] mod tests`. Each new test file is its own target in the crate's `BUCK`. New **fixture** tests MUST use `loom_fixture_test` (not bare `rust_test`) or they boot without the Postgres/MinIO fixture env.
- **`buck2 run //tools:prek -- run --all-files` before every commit** (rustfmt, clippy, file hygiene, reindeer-in-sync). Markdown/text files must end in exactly one newline with no trailing whitespace.
- **New SQL → `tools/sqlx-prepare.sh` + commit the `.sqlx` change.** This plan adds NO new SQL literal (it reuses `pg_peek_offset`, `pg_stream_meta`, `live_table_id`, `pg_notify_changelog`, all already prepared), so **no `.sqlx` regen is expected** — but if a step introduces a `query!`/`query_scalar!` with new SQL text, regen and commit.
- **Build/test commands use `--console none`** (and `-v0` for builds): `buck2 build -v0 --console none //...`, `buck2 test --console none //...`. Keep each a bare `buck2 …` invocation (no leading `cd`/`eval`, no `; grep`) so the permission allow-rules match.
- **Non-regression is a hard requirement:** CDC feeds must stay byte-identical. The LOG behavior is a new dispatch arm, not a change to the CDC path. `mv_delta_scan` is untouched. Run the **full** CDC subscribe suite green before finishing.

---

## File Structure

**Modified — control plane (`src/control-plane/`):**
- `postgres/src/stream.rs` — add pub `stream_meta_for(pool, table)` (TableRef→StreamMeta resolver, the cross-crate handle the feed dispatch needs); relax `changelog_positions_latest` to accept `Log`; doc-comment touch-ups.
- `postgres/src/iceberg_inline.rs` — fire the subscribe NOTIFY for `Log` appends too (line ~683), so `await_changelog` wakes on a log append.

**Modified — engine-serving (`src/services/engine-serving/`):**
- `src/feed.rs` — the core change: extract the shared union→govern→read tail (`union_govern_read`) from `changelog_feed_scan_at`; add `build_base_file_tier`; add `log_feed_scan_at`; make `changelog_feed_scan` resolve `StreamMeta.kind` and dispatch LOG→`log_feed_scan_at` / CDC→existing pair-pinned path.
- `src/lib.rs` — export `log_feed_scan_at`.

**Modified — query-api (`src/services/query-api/`):**
- `src/http.rs` — the 400 message "not backed by a declared CDC table" → "…declared stream table" (log tables are now subscribable).
- `tests/e2e_support.rs` — add `seed_log_stream` (lands offset-framed rows into a declared log table) and `define_log_type` (an ontology type over a log table); reused by the router tests.

**Modified — test seed (`src/testing/`):**
- `seed.rs` — add pure `id_val_columns()` + `id_val_batch(ids, vals)` Arrow builders (the `(id: Long, val: Long)` shape the log tests land), so the three new tests don't each re-hand-roll a batch.

**Created — tests:**
- `postgres/tests/stream_log_positions.rs` — the relaxed positions probe + `stream_meta_for`.
- `postgres/tests/stream_log_notify.rs` — `await_changelog` wakes on a log append.
- `query-api/tests/stream_log_subscribe_scan.rs` — the `changelog_feed_scan` LOG arm (files ∪ inline, ordered, resumable, across a flush, governance masking).
- `query-api/tests/stream_log_subscribe_e2e.rs` — the full governed NDJSON feed through the router for a log type; non-stream type still 400s.
- `query-api/tests/stream_log_subscribe_wire_e2e.rs` — the same over the production gRPC wire (proves acceptance #1's production-wire clause with no wire change).

**BUCK wiring:** `src/testing/BUCK` (seed builders are in the existing `:seed` target — no target change, just new fns), `src/control-plane/postgres/BUCK` (two new `loom_fixture_test` targets), `src/services/query-api/BUCK` (three new `loom_fixture_test` targets).

---

### Task 1: Positions probe accepts log tables + `stream_meta_for` resolver

**Files:**
- Modify: `src/control-plane/postgres/src/stream.rs` (`changelog_positions_latest` ~638-659; add `stream_meta_for`)
- Modify: `src/testing/seed.rs` (add `id_val_columns` / `id_val_batch`)
- Modify: `src/testing/BUCK` (add `//third-party:arrow-array` etc. only if missing — the `:seed` target already deps arrow-array/arrow-schema)
- Create: `src/control-plane/postgres/tests/stream_log_positions.rs`
- Modify: `src/control-plane/postgres/BUCK` (new `loom_fixture_test`)

**Interfaces:**
- Consumes: `land` (`control_plane_postgres::iceberg_landing::land`), `local_sql_catalog` (`loom_test_seed`), `IcebergCatalog`, `PgFixture`, `LineageEvent`/`EventType`/`DatasetId`/`RunId`.
- Produces:
  - `pub async fn stream_meta_for(pool: &sqlx::PgPool, table: &control_plane_core::TableRef) -> control_plane_core::Result<Option<control_plane_core::StreamMeta>>` (in `control_plane_postgres::stream`) — the `TableRef`-keyed sibling of `pg_stream_meta`; `None` when no live mirror row or no stream declaration.
  - `pub fn id_val_columns() -> Vec<control_plane_core::ColumnSpec>` and `pub fn id_val_batch(ids: &[i64], vals: &[i64]) -> (arrow::datatypes::SchemaRef, Vec<arrow::array::RecordBatch>)` (in `loom_test_seed`) — the `(id: Long, val: Long)` seed shape.
  - `changelog_positions_latest` now returns `Some(BTreeMap<bucket, next>)` for a declared **log** table (unchanged shape).

- [ ] **Step 1: Add the `(id, val)` batch builders to loom_test_seed.**

In `src/testing/seed.rs`, add (mirror the existing `vec4_columns`/`vec4_batches` style; `ColumnSpec`, `Field`, `DataType`, `Int64Array`, `RecordBatch`, `Schema`, `SchemaRef`, `Arc` are already imported or add the imports):

```rust
/// The `(id: Long, val: Long)` column specs the log-stream tests land.
#[must_use]
pub fn id_val_columns() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec { name: "id".into(), ty: "long".into(), nullable: false },
        ColumnSpec { name: "val".into(), ty: "long".into(), nullable: false },
    ]
}

/// An `(id, val)` Arrow batch for `land` seeding — one row per index of the
/// two equal-length slices.
#[must_use]
pub fn id_val_batch(ids: &[i64], vals: &[i64]) -> (SchemaRef, Vec<RecordBatch>) {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(Int64Array::from(vals.to_vec())),
        ],
    )
    .expect("id_val_batch");
    (schema, vec![batch])
}
```

If `Int64Array`/`Field`/`DataType`/`Schema` are not yet imported in `seed.rs`, add `use arrow_array::Int64Array;` and `use arrow_schema::{DataType, Field, Schema, SchemaRef};` (match the crate's existing arrow import style — check the top of `seed.rs`).

- [ ] **Step 2: Write the failing probe test.**

Create `src/control-plane/postgres/tests/stream_log_positions.rs`:

```rust
//! road-stream-log-table-subscribe seam 1: the positions probe stops refusing
//! log tables. `changelog_positions_latest` returns per-bucket next offsets for
//! a declared LOG table; `None` for an undeclared table. loom_fixture_test.

use control_plane_core::{DatasetId, EventType, LineageEvent, RunId, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::land;
use control_plane_postgres::stream::{changelog_positions_latest, stream_meta_for};
use loom_test_seed::{hot_limits, id_val_batch, id_val_columns, local_sql_catalog};

fn lineage(table: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(table).dataset_ref()],
        payload: serde_json::json!({ "source": "stream-log-positions-test" }),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn positions_probe_answers_for_a_log_table() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("warehouse");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;

    let table = TableRef { schema: "s".into(), name: "events".into() };
    // 3 rows into a 2-bucket log stream (row_index % 2 → buckets 0,1,0).
    // `hot_limits()` = inline path (the offset-framed inline append), which is
    // what a log producer uses and what the subscribe feed's inline tier reads.
    let (schema, batches) = id_val_batch(&[1, 2, 3], &[10, 20, 30]);
    land(
        &pool,
        &catalog,
        &table,
        &id_val_columns(),
        schema,
        batches,
        hot_limits(),
        lineage(&table),
        Some(2),
    )
    .await
    .expect("land log rows");

    let kind = stream_meta_for(&pool, &table)
        .await
        .expect("meta")
        .expect("declared")
        .kind;
    assert_eq!(kind, control_plane_core::StreamKind::Log);

    let positions = changelog_positions_latest(&pool, &table)
        .await
        .expect("probe")
        .expect("a declared log table is subscribable");
    // 2 buckets present; total next offsets == 3 rows landed.
    assert_eq!(positions.len(), 2, "one entry per bucket: {positions:?}");
    assert_eq!(
        positions.values().sum::<i64>(),
        3,
        "per-bucket next offsets sum to the 3 landed rows: {positions:?}"
    );

    // An undeclared table (no mirror row) is not subscribable.
    let missing = TableRef { schema: "s".into(), name: "nope".into() };
    assert!(
        changelog_positions_latest(&pool, &missing)
            .await
            .expect("probe missing")
            .is_none(),
        "undeclared table probes to None"
    );
    assert!(stream_meta_for(&pool, &missing).await.expect("meta missing").is_none());
}
```

- [ ] **Step 3: Wire the test target and run it to verify it FAILS to build/compile** (`stream_meta_for` does not exist yet).

Add to `src/control-plane/postgres/BUCK` (mirror an existing `loom_fixture_test`, e.g. the `snapshot_pair` target; keep deps minimal):

```python
loom_fixture_test(
    name = "stream-log-positions",
    crate = "stream_log_positions",
    srcs = ["tests/stream_log_positions.rs"],
    crate_root = "tests/stream_log_positions.rs",
    edition = "2024",
    deps = [
        ":postgres",
        "//src/control-plane/core:core",
        "//src/testing:seed",
        "//third-party:serde_json",
        "//third-party:tempfile",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```

Run: `buck2 build -v0 --console none //src/control-plane/postgres:stream-log-positions`
Expected: FAIL — `stream_meta_for` (and possibly `id_val_batch`) unresolved.

- [ ] **Step 4: Add `stream_meta_for` and relax the probe.**

In `src/control-plane/postgres/src/stream.rs`, add near `pg_stream_meta` / `changelog_positions_latest`:

```rust
/// The stream declaration (kind + bucket count) for a table addressed by
/// `TableRef` — the `TableRef`-keyed sibling of the `table_id`-keyed
/// [`pg_stream_meta`]. `None` when the table has no live mirror row or no
/// stream declaration. Used by the engine-serving feed dispatch (a different
/// crate) to key the CDC-vs-log feed arm off `StreamMeta.kind`.
pub async fn stream_meta_for(
    pool: &sqlx::PgPool,
    table: &TableRef,
) -> Result<Option<control_plane_core::StreamMeta>> {
    let mut conn = pool.acquire().await.map_err(backend)?;
    let Some(tid) =
        crate::iceberg_mirror::live_table_id(&mut conn, &table.schema, &table.name).await?
    else {
        return Ok(None);
    };
    pg_stream_meta(&mut *conn, tid).await
}
```

Relax the `changelog_positions_latest` kind guard (was CDC-only) to accept both stream kinds:

```rust
    if !matches!(
        meta.kind,
        control_plane_core::StreamKind::Cdc | control_plane_core::StreamKind::Log
    ) {
        return Ok(None);
    }
```

Update the `changelog_positions_latest` doc comment: replace "for a declared CDC table" with "for a declared **stream** table (CDC or log)".

- [ ] **Step 5: Run the test to verify it PASSES.**

Run: `buck2 test --console none //src/control-plane/postgres:stream-log-positions`
Expected: `Tests finished: Pass 1. Fail 0`.

- [ ] **Step 6: Confirm no `.sqlx` drift and clippy is clean.**

Run: `buck2 test --console none //src/control-plane/postgres:sqlx-cache-check` → Pass (no new SQL was added; the cache is still fresh).
Run: `buck2 build --console none '//src/control-plane/postgres:postgres[clippy.txt]'` and `cat` the printed path — expect empty.
Run: `buck2 build --console none '//src/testing:seed[clippy.txt]'` — expect empty.

- [ ] **Step 7: prek + commit.**

```bash
buck2 run //tools:prek -- run --all-files
git add src/control-plane/postgres/src/stream.rs src/testing/seed.rs \
        src/control-plane/postgres/tests/stream_log_positions.rs \
        src/control-plane/postgres/BUCK
git commit -m "feat(stream): positions probe answers for log tables + stream_meta_for resolver"
```

---

### Task 2: Log-table appends fire the subscribe NOTIFY

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_inline.rs` (the NOTIFY guard ~683; doc touch-ups on `pg_notify_changelog`/`await_changelog` in `stream.rs`)
- Create: `src/control-plane/postgres/tests/stream_log_notify.rs`
- Modify: `src/control-plane/postgres/BUCK` (new `loom_fixture_test`)

**Interfaces:**
- Consumes: `await_changelog` (`control_plane_postgres::stream`), `land`, `id_val_batch`/`id_val_columns`, `local_sql_catalog`.
- Produces: no new public API — behavioral change only (a log append now `pg_notify`s `loom_changelog:{tid}`).

- [ ] **Step 1: Write the failing await-wakeup test.**

Create `src/control-plane/postgres/tests/stream_log_notify.rs`:

```rust
//! road-stream-log-table-subscribe seam 3: a log-table inline append fires the
//! subscribe NOTIFY, so a blocked `await_changelog` wakes promptly (not only on
//! the poll-fallback timeout). loom_fixture_test.

use std::time::{Duration, Instant};

use control_plane_core::{DatasetId, EventType, LineageEvent, RunId, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::land;
use control_plane_postgres::stream::await_changelog;
use loom_test_seed::{hot_limits, id_val_batch, id_val_columns, local_sql_catalog};

fn lineage(table: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(table).dataset_ref()],
        payload: serde_json::json!({ "source": "stream-log-notify-test" }),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn await_wakes_on_a_log_append() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("warehouse");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let table = TableRef { schema: "s".into(), name: "events".into() };

    // Declare + seed one row so the mirror table exists (await resolves the tid).
    // `hot_limits()` = the INLINE append path — the only path that fires the
    // NOTIFY (the direct-Parquet bulk path does not; see the plan's scope note).
    let (schema, batches) = id_val_batch(&[1], &[10]);
    land(
        &pool, &catalog, &table, &id_val_columns(), schema, batches,
        hot_limits(),
        lineage(&table), Some(2),
    )
    .await
    .expect("seed one row");

    // Block on await with a generous timeout, then append after a short delay.
    // The NOTIFY must wake it well before the timeout.
    let appender = {
        let pool = pool.clone();
        let catalog = catalog.clone();
        let table = table.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let (schema, batches) = id_val_batch(&[2], &[20]);
            land(
                &pool, &catalog, &table, &id_val_columns(), schema, batches,
                hot_limits(),
                lineage(&table), Some(2),
            )
            .await
            .expect("append triggers NOTIFY");
        })
    };

    let started = Instant::now();
    await_changelog(&pool, &table, Duration::from_secs(10))
        .await
        .expect("await returns");
    let waited = started.elapsed();
    appender.await.expect("appender joined");

    // Woken by the NOTIFY (~200ms), NOT the 10s poll-fallback timeout.
    assert!(
        waited < Duration::from_secs(2),
        "await woke on the log-append NOTIFY, not the timeout: waited {waited:?}"
    );
}
```

- [ ] **Step 2: Wire the target and run to verify it FAILS** (today a log append does not notify, so `await` blocks until it times out — but our timeout is 10s and the assert is `< 2s`, so the test fails by exceeding 2s).

Add to `src/control-plane/postgres/BUCK`:

```python
loom_fixture_test(
    name = "stream-log-notify",
    crate = "stream_log_notify",
    srcs = ["tests/stream_log_notify.rs"],
    crate_root = "tests/stream_log_notify.rs",
    edition = "2024",
    deps = [
        ":postgres",
        "//src/control-plane/core:core",
        "//src/testing:seed",
        "//third-party:serde_json",
        "//third-party:tempfile",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```

Run: `buck2 test --console none //src/control-plane/postgres:stream-log-notify`
Expected: FAIL — the assertion `waited < 2s` trips (await only returns after the 10s timeout).

- [ ] **Step 3: Fire the NOTIFY for log appends.**

In `src/control-plane/postgres/src/iceberg_inline.rs`, at the subscribe-wakeup guard (~line 683), broaden the condition from CDC-only to any stream kind and update the comment:

```rust
        // Subscribe wakeup (road-stream-subscribe / road-stream-log-table-subscribe):
        // one fire-and-forget notify per committed STREAM write batch (CDC or log),
        // buffered until this tx commits. Batch (non-stream) tables never notify.
        if matches!(
            &meta,
            Some(m) if matches!(
                m.kind,
                control_plane_core::StreamKind::Cdc | control_plane_core::StreamKind::Log
            )
        ) {
            crate::stream::pg_notify_changelog(&mut *conn, tid).await?;
        }
```

Update the two doc comments in `src/control-plane/postgres/src/stream.rs`:
- `pg_notify_changelog`: "Fire the changelog wakeup for a **stream (CDC or log)** base table's inline write."
- `await_changelog`: "Block until a **stream** inline write commits against `table`…".

> **Scope note (matches the spec's await bullet, which is about *inline* appends):** this fix fires the NOTIFY on the **inline** append path only (`inline_append_decl`, the offset-framed path a log producer and the CDC writer use). The separate **direct-to-Parquet bulk** landing path (`land_parquet_stream`, taken when a batch exceeds `inline_byte_limit`) fires no `pg_notify_changelog` for *either* CDC or Log today — a pre-existing gap, out of scope here. A blocked subscriber still catches those writes via the 1 s poll-fallback; only the sub-second wakeup is missed. If the final review wants it tracked, add a `fut-stream-bulk-append-notify` FUTURE item via `loom-docs-update` rather than widening this slice.

- [ ] **Step 4: Run the test to verify it PASSES.**

Run: `buck2 test --console none //src/control-plane/postgres:stream-log-notify`
Expected: `Tests finished: Pass 1. Fail 0`.

- [ ] **Step 5: CDC-notify non-regression.** Confirm the existing CDC subscribe suite still passes (the guard still fires for CDC):

Run: `buck2 test --console none //src/services/query-api:stream-subscribe-e2e //src/services/query-api:stream-feed-torn-read`
Expected: all Pass.

- [ ] **Step 6: clippy + prek + commit.**

Run: `buck2 build --console none '//src/control-plane/postgres:postgres[clippy.txt]'` and `cat` the path — empty.

```bash
buck2 run //tools:prek -- run --all-files
git add src/control-plane/postgres/src/iceberg_inline.rs src/control-plane/postgres/src/stream.rs \
        src/control-plane/postgres/tests/stream_log_notify.rs src/control-plane/postgres/BUCK
git commit -m "feat(stream): log-table appends fire the subscribe NOTIFY"
```

---

### Task 3: The log-table feed scan + kind dispatch (the core change)

**Files:**
- Modify: `src/services/engine-serving/src/feed.rs` (extract `union_govern_read`; add `build_base_file_tier`, `log_feed_scan_at`; dispatch in `changelog_feed_scan`)
- Modify: `src/services/engine-serving/src/lib.rs` (export `log_feed_scan_at`)
- Modify: `src/services/query-api/tests/e2e_support.rs` (add `seed_log_stream`)
- Create: `src/services/query-api/tests/stream_log_subscribe_scan.rs`
- Modify: `src/services/query-api/BUCK` (new `loom_fixture_test`)

**Interfaces:**
- Consumes: `stream_meta_for` (Task 1), `IcebergCatalog::{schema, files_with_stats, inline_live_batch_full, current_snapshot, pool}`, `with_feed_framing_fields`/`build_inline_tier`/`union_select_columns`/`union_tiers`/`build_resume_predicate`/`decode_page`/`register_object_stores`/`GovernedTableProvider` (all already in `feed.rs`), `Snapshot`/`SnapshotId`/`StreamKind`/`ControlPlaneError`.
- Produces:
  - `pub async fn log_feed_scan_at(catalog: &IcebergCatalog, base: &TableRef, serving_store: Option<&ServingStore>, positions: &BTreeMap<i32,i64>, limit: usize, policy: &TablePolicy, base_pin: &Snapshot) -> Result<ChangeFeedPage, EngineServingError>` (in `engine_serving::feed`, re-exported from `engine_serving`).
  - `changelog_feed_scan` unchanged signature; now dispatches by kind.
  - `pub async fn seed_log_stream(pool: &sqlx::PgPool, catalog: &SqlCatalog, table: &TableRef, ids: &[i64], vals: &[i64], buckets: i32)` (in `e2e_support`) — lands `(id,val)` rows into a declared log stream table.

- [ ] **Step 1: Add the `seed_log_stream` helper to e2e_support.**

In `src/services/query-api/tests/e2e_support.rs` (it already imports `land`, `InlineLimits`, and `loom_test_seed`; add `id_val_batch`, `id_val_columns` to the `loom_test_seed` use and `SqlCatalog` if not present):

```rust
/// Land `(id, val)` rows into `table` as a declared `buckets`-bucket LOG stream
/// via the offset-framed **inline** append path (`hot_limits()`), so the rows
/// populate the feed's inline tier and a later `flush_table` genuinely moves
/// them to files (with `cold_limits()`/`inline_byte_limit: 0` they would land
/// straight to Parquet and the inline tier would always be empty). The
/// log-subscribe analogue of the CDC `seed_widget_*` helpers.
pub async fn seed_log_stream(
    pool: &sqlx::PgPool,
    catalog: &control_plane_postgres::iceberg_sql_catalog::SqlCatalog,
    table: &TableRef,
    ids: &[i64],
    vals: &[i64],
    buckets: i32,
) {
    let (schema, batches) = loom_test_seed::id_val_batch(ids, vals);
    let lineage = LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(table).dataset_ref()],
        payload: serde_json::json!({ "source": "seed_log_stream" }),
    };
    land(
        pool,
        catalog,
        table,
        &loom_test_seed::id_val_columns(),
        schema,
        batches,
        loom_test_seed::hot_limits(),
        lineage,
        Some(buckets),
    )
    .await
    .expect("seed_log_stream: land");
}
```

Ensure the names it uses (`LineageEvent`, `RunId`, `EventType`, `DatasetId`, `OffsetDateTime`, `TableRef`) are already imported at the top of `e2e_support.rs` — add any missing to the existing `control_plane_core::{…}` / `use time::OffsetDateTime;` imports. Confirm `SqlCatalog`'s path is `control_plane_postgres::iceberg_sql_catalog::SqlCatalog` (grep it if unsure).

- [ ] **Step 2: Write the failing scan test.**

Create `src/services/query-api/tests/stream_log_subscribe_scan.rs`:

```rust
//! road-stream-log-table-subscribe seam 2: `changelog_feed_scan`'s LOG arm —
//! the governed, (bucket, offset)-ordered disjoint union of the base table's
//! own files ∪ live inline tail, resumable by per-bucket positions, every event
//! a stored '+I'. Mirrors `stream_subscribe_scan.rs` (the CDC scan test).
//! loom_fixture_test (Postgres + LocalFsStorage warehouse).

use std::collections::BTreeMap;

use control_plane_core::{ChangeEvent, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_flush::flush_table;
use e2e_support::seed_log_stream;
use engine_serving::TablePolicy;
use engine_serving::feed::changelog_feed_scan;
use loom_test_seed::local_sql_catalog;

fn keys(evs: &[ChangeEvent]) -> Vec<(i32, i64, String)> {
    evs.iter().map(|e| (e.bucket, e.offset, e.change_kind.clone())).collect()
}

fn assert_ordered_gapless(evs: &[ChangeEvent], start: &BTreeMap<i32, i64>) {
    let k = keys(evs);
    let mut sorted = k.clone();
    sorted.sort();
    assert_eq!(k, sorted, "events ordered by (bucket, offset): {k:?}");
    let mut cursor = start.clone();
    for e in evs {
        let at = cursor.get(&e.bucket).copied().unwrap_or(0);
        assert_eq!(e.offset, at, "gapless per-bucket offsets: {k:?}");
        cursor.insert(e.bucket, at + 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn log_feed_scan_unions_files_and_inline_ordered_and_resumable() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("warehouse");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let ice = IcebergCatalog::new(pool.clone());
    let table = TableRef { schema: "s".into(), name: "events".into() };
    let open = TablePolicy::default();

    // 3 rows into a 2-bucket log stream, all inline (buckets: 0,1,0).
    seed_log_stream(&pool, &catalog, &table, &[1, 2, 3], &[10, 20, 30], 2).await;

    let earliest: BTreeMap<i32, i64> = BTreeMap::from([(0, 0), (1, 0)]);

    // 1. All-inline scan: 3 events, ordered, every kind '+I', no loom_* in fields.
    let page1 = changelog_feed_scan(&ice, &table, None, &earliest, 100, &open)
        .await
        .expect("scan inline");
    assert_eq!(page1.events.len(), 3, "3 inline events: {:?}", keys(&page1.events));
    assert_ordered_gapless(&page1.events, &earliest);
    assert!(
        page1.events.iter().all(|e| e.change_kind == "+I"),
        "log events are all '+I': {:?}",
        keys(&page1.events)
    );
    for e in &page1.events {
        assert!(
            e.fields.keys().all(|k| !k.starts_with("loom_")),
            "no framing key in fields: {:?}",
            e.fields
        );
        assert!(e.fields.contains_key("id") && e.fields.contains_key("val"), "user cols present");
    }

    // 2. Flush: same 3 events, now from the base files tier. Identical keys, no dup.
    flush_table(&catalog, &pool, &table, control_plane_core::RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush");
    let page2 = changelog_feed_scan(&ice, &table, None, &earliest, 100, &open)
        .await
        .expect("scan files");
    assert_eq!(keys(&page2.events), keys(&page1.events), "files == inline, no dup/gap");

    // 3. Two more rows post-flush: the scan spans the flush boundary (files ∪ inline).
    seed_log_stream(&pool, &catalog, &table, &[4, 5], &[40, 50], 2).await;
    let page3 = changelog_feed_scan(&ice, &table, None, &earliest, 100, &open)
        .await
        .expect("scan union");
    assert_eq!(page3.events.len(), 5, "files ∪ inline: {:?}", keys(&page3.events));
    assert_ordered_gapless(&page3.events, &earliest);

    // 4. Resume: limit 3, then from `next` — exact concatenation.
    let head = changelog_feed_scan(&ice, &table, None, &earliest, 3, &open)
        .await
        .expect("head");
    assert_eq!(head.events.len(), 3);
    let tail = changelog_feed_scan(&ice, &table, None, &head.next, 100, &open)
        .await
        .expect("tail");
    let mut joined = keys(&head.events);
    joined.extend(keys(&tail.events));
    assert_eq!(joined, keys(&page3.events), "resume is gapless and dup-free");

    // 5. Governance: masking `val` reads '***' on every event; framing survives.
    let masked = TablePolicy {
        row_filters: vec![],
        denied: std::collections::HashSet::new(),
        masked: std::collections::HashSet::from(["val".to_string()]),
    };
    let page5 = changelog_feed_scan(&ice, &table, None, &earliest, 100, &masked)
        .await
        .expect("scan masked");
    assert_eq!(page5.events.len(), 5, "masking does not drop events");
    assert!(
        page5.events.iter().all(|e| e.fields.get("val") == Some(&serde_json::json!("***"))),
        "masked column is '***' on every event"
    );
}
```

- [ ] **Step 3: Wire the target and run to verify it FAILS** (the LOG arm is not implemented — today `changelog_feed_scan` takes the CDC pair-pin path, tries `changelog_table_ref(base)`, and there is no changelog table, so it returns zero events → `assert_eq!(page1.events.len(), 3)` fails).

Add to `src/services/query-api/BUCK`:

```python
loom_fixture_test(
    name = "stream-log-subscribe-scan",
    crate = "stream_log_subscribe_scan",
    srcs = ["tests/stream_log_subscribe_scan.rs"],
    crate_root = "tests/stream_log_subscribe_scan.rs",
    edition = "2024",
    deps = [
        ":query-api",
        ":e2e-support",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//src/services/engine-serving:engine-serving",
        "//src/testing:seed",
        "//third-party:serde_json",
        "//third-party:tempfile",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```

Run: `buck2 test --console none //src/services/query-api:stream-log-subscribe-scan`
Expected: FAIL — 0 events returned (CDC path finds no changelog table for a log table).

- [ ] **Step 4: Extract the shared feed tail `union_govern_read`.**

In `src/services/engine-serving/src/feed.rs`, add `StreamKind` and `stream_meta_for` to the imports:

```rust
use control_plane_core::{
    Catalog, ChangeEvent, ChangeFeedPage, ControlPlaneError, Snapshot, SnapshotId, StreamKind,
    TableRef,
};
use control_plane_postgres::stream::stream_meta_for;
```

(`SnapshotId` may already resolve via `control_plane_core::SnapshotId` used inline in `build_inline_tier` — add it to the import list so the new helper can name it directly.)

Add the shared tail helper (this is the exact sequence currently inside `changelog_feed_scan_at` from `base_cols` onward — moving it verbatim keeps CDC byte-identical):

```rust
/// The shared tail of every feed scan, given both tiers already built: project
/// to the common `[user…, loom_change_kind, loom_bucket, loom_offset]` order,
/// disjoint UNION ALL, wrap in governance BEFORE the ordered read, then apply
/// the per-bucket resume predicate, order by `(loom_bucket, loom_offset)`,
/// limit, and decode. Identical for CDC and LOG feeds — only the file tier
/// (changelog files vs the base's own files) differs upstream.
async fn union_govern_read(
    ctx: &SessionContext,
    catalog: &IcebergCatalog,
    base: &TableRef,
    base_pin_id: SnapshotId,
    file_provider: Option<IcebergMirrorTableProvider>,
    inline_provider: Option<MemTable>,
    positions: &BTreeMap<i32, i64>,
    limit: usize,
    policy: &TablePolicy,
) -> Result<ChangeFeedPage, EngineServingError> {
    let empty = || ChangeFeedPage { events: vec![], next: positions.clone() };
    let base_cols = catalog.schema(base, base_pin_id).await.map_err(to_serving)?.columns;
    let select = union_select_columns(&base_cols);
    let Some(unioned) = union_tiers(ctx, file_provider, inline_provider, &select)? else {
        return Ok(empty());
    };
    let governed = GovernedTableProvider::new(unioned.into_view(), policy.clone())?;
    let df = ctx.read_table(Arc::new(governed)).map_err(to_serving)?;
    let Some(pred) = build_resume_predicate(positions) else {
        return Ok(empty());
    };
    let batches = df
        .filter(pred)
        .map_err(to_serving)?
        .sort(vec![
            col("loom_bucket").sort(true, false),
            col("loom_offset").sort(true, false),
        ])
        .map_err(to_serving)?
        .limit(0, Some(limit))
        .map_err(to_serving)?
        .collect()
        .await
        .map_err(to_serving)?;
    decode_page(&batches, positions)
}
```

- [ ] **Step 5: Rewrite `changelog_feed_scan_at`'s body to call the shared tail** (behavior unchanged — same tiers, same tail):

```rust
pub async fn changelog_feed_scan_at(
    catalog: &IcebergCatalog,
    base: &TableRef,
    serving_store: Option<&ServingStore>,
    positions: &BTreeMap<i32, i64>,
    limit: usize,
    policy: &TablePolicy,
    pins: &FeedPins,
) -> Result<ChangeFeedPage, EngineServingError> {
    if positions.is_empty() || limit == 0 {
        return Ok(ChangeFeedPage { events: vec![], next: positions.clone() });
    }
    let ctx = SessionContext::new();
    register_object_stores(&ctx, serving_store)?;
    // Tier 1: the changelog Iceberg files, AT the pinned changelog snapshot.
    let file_provider = build_file_tier(catalog, base, pins.clog.as_ref()).await?;
    // Tier 2: the base's inline tail, AT the pinned base snapshot.
    let inline_provider = build_inline_tier(catalog, base, pins.base.id).await?;
    union_govern_read(
        &ctx, catalog, base, pins.base.id, file_provider, inline_provider, positions, limit, policy,
    )
    .await
}
```

Keep the existing doc comment on `changelog_feed_scan_at`.

- [ ] **Step 6: Add `build_base_file_tier` and `log_feed_scan_at`.**

```rust
/// Tier 1 of a LOG feed union: the BASE table's own Iceberg files, AT the pinned
/// base snapshot. A log table has no changelog sibling — the events ARE the base
/// rows — so this reads `base` where [`build_file_tier`] reads its changelog.
async fn build_base_file_tier(
    catalog: &IcebergCatalog,
    base: &TableRef,
    base_pin: &Snapshot,
) -> Result<Option<IcebergMirrorTableProvider>, EngineServingError> {
    let cols = catalog.schema(base, base_pin.id).await.map_err(to_serving)?.columns;
    let framed = with_feed_framing_fields(&arrow_schema_from_mirror(&cols)?);
    let files = catalog.files_with_stats(base, base_pin.id).await.map_err(to_serving)?;
    if files.is_empty() {
        Ok(None)
    } else {
        Ok(Some(IcebergMirrorTableProvider::try_new_with_schema(files, framed)))
    }
}

/// The LOG-table analogue of [`changelog_feed_scan_at`]: the base table IS the
/// ordered log (no changelog sibling), so tier 1 reads the base's own files and
/// a SINGLE base snapshot pins both tiers (`inline_live_batch_full` is already an
/// as-of read against it — see [`build_inline_tier`]). `change_kind` is the
/// stored constant `'+I'`. Public so a test can pin the read deterministically.
pub async fn log_feed_scan_at(
    catalog: &IcebergCatalog,
    base: &TableRef,
    serving_store: Option<&ServingStore>,
    positions: &BTreeMap<i32, i64>,
    limit: usize,
    policy: &TablePolicy,
    base_pin: &Snapshot,
) -> Result<ChangeFeedPage, EngineServingError> {
    if positions.is_empty() || limit == 0 {
        return Ok(ChangeFeedPage { events: vec![], next: positions.clone() });
    }
    let ctx = SessionContext::new();
    register_object_stores(&ctx, serving_store)?;
    let file_provider = build_base_file_tier(catalog, base, base_pin).await?;
    let inline_provider = build_inline_tier(catalog, base, base_pin.id).await?;
    union_govern_read(
        &ctx, catalog, base, base_pin.id, file_provider, inline_provider, positions, limit, policy,
    )
    .await
}
```

- [ ] **Step 7: Make `changelog_feed_scan` dispatch by kind.**

Replace the body of `changelog_feed_scan` so it resolves `StreamMeta.kind` first and routes LOG to the single-pin path, CDC (and anything else the handler let through) to the existing pinned-pair path:

```rust
pub async fn changelog_feed_scan(
    catalog: &IcebergCatalog,
    base: &TableRef,
    serving_store: Option<&ServingStore>,
    positions: &BTreeMap<i32, i64>,
    limit: usize,
    policy: &TablePolicy,
) -> Result<ChangeFeedPage, EngineServingError> {
    let empty = || ChangeFeedPage { events: vec![], next: positions.clone() };
    if positions.is_empty() || limit == 0 {
        return Ok(empty());
    }

    // Kind dispatch (road-stream-log-table-subscribe): a LOG table has no
    // changelog sibling — its base rows ARE the feed — so it pins ONE snapshot
    // and reads base files ∪ inline. A CDC table keeps the two-tier pinned-pair
    // union. The handler only reaches here for a declared stream table; a
    // non-Log kind falls through to the CDC path unchanged.
    let kind = stream_meta_for(&catalog.pool, base)
        .await
        .map_err(to_serving)?
        .map(|m| m.kind);
    if kind == Some(StreamKind::Log) {
        let base_pin = match catalog.current_snapshot(base).await {
            Ok(s) => s,
            // Declared but never written: nothing to read. The RPC fast-path
            // normally short-circuits this, but stay total.
            Err(ControlPlaneError::NotFound(_)) => return Ok(empty()),
            Err(e) => return Err(to_serving(e)),
        };
        return log_feed_scan_at(
            catalog, base, serving_store, positions, limit, policy, &base_pin,
        )
        .await;
    }

    // CDC (and default): pin the base+changelog pair together, then read. (Body
    // unchanged from before this slice — see the torn-read note.)
    let clog = changelog_table_ref(base);
    let (base_pin, clog_pin) = catalog
        .current_snapshots_pair(base, &clog)
        .await
        .map_err(to_serving)?;
    let Some(base_snap) = base_pin else {
        return Err(to_serving(ControlPlaneError::NotFound(format!(
            "{}.{}",
            base.schema, base.name
        ))));
    };
    let pins = FeedPins { base: base_snap, clog: clog_pin };
    changelog_feed_scan_at(catalog, base, serving_store, positions, limit, policy, &pins).await
}
```

Keep the existing rich doc comment on `changelog_feed_scan` (append a sentence noting the LOG arm if helpful, but do not delete the CDC/torn-read explanation).

- [ ] **Step 8: Export `log_feed_scan_at`.**

In `src/services/engine-serving/src/lib.rs`, extend the feed re-export:

```rust
pub use feed::{FeedPins, changelog_feed_scan, changelog_feed_scan_at, log_feed_scan_at};
```

- [ ] **Step 9: Run the scan test to verify it PASSES.**

Run: `buck2 test --console none //src/services/query-api:stream-log-subscribe-scan`
Expected: `Tests finished: Pass 1. Fail 0`.

- [ ] **Step 10: CDC scan non-regression + clippy.**

Run: `buck2 test --console none //src/services/query-api:stream-subscribe-scan //src/services/query-api:stream-feed-torn-read` → Pass (CDC scan byte-identical).
Run: `buck2 build --console none '//src/services/engine-serving:engine-serving[clippy.txt]'` and `cat` the path — empty.

- [ ] **Step 11: prek + commit.**

```bash
buck2 run //tools:prek -- run --all-files
git add src/services/engine-serving/src/feed.rs src/services/engine-serving/src/lib.rs \
        src/services/query-api/tests/e2e_support.rs \
        src/services/query-api/tests/stream_log_subscribe_scan.rs src/services/query-api/BUCK
git commit -m "feat(stream): log-table feed scan (base files ∪ inline) with kind dispatch"
```

---

### Task 4: In-process router e2e + handler message

**Files:**
- Modify: `src/services/query-api/src/http.rs` (the 400 message ~653)
- Modify: `src/services/query-api/tests/e2e_support.rs` (add `define_log_type`)
- Create: `src/services/query-api/tests/stream_log_subscribe_e2e.rs`
- Modify: `src/services/query-api/BUCK` (new `loom_fixture_test`)

**Interfaces:**
- Consumes: `seed_log_stream` (Task 3), `get_ndjson`, `subject_with_role`/`grant_read` (or the subscribe suite's grant helper — grep `stream_subscribe_e2e.rs` / `stream_subscribe_gov_e2e.rs` for the exact reader-grant helper name), `InProcessServingEngine`, `IcebergCatalog`, `local_sql_catalog`.
- Produces:
  - `pub async fn define_log_type(cp: &PgControlPlane, type_name: &str, table: &TableRef) -> TypeName` (in `e2e_support`) — defines `type_name(id Long, val Long)` mapped to `table`, no actions (a log feed is positional, read-only).

- [ ] **Step 1: Add `define_log_type` to e2e_support.**

```rust
/// Define an ontology type `type_name(id Long, val Long)` over a LOG stream
/// `table`, with NO actions — a log feed is positional and read-only, so the
/// changes endpoint needs only the type→table mapping (unlike `define_widget`,
/// which also defines the CDC mutation actions).
pub async fn define_log_type(
    cp: &PgControlPlane,
    type_name: &str,
    table: &TableRef,
) -> TypeName {
    let name = TypeName(type_name.into());
    cp.ontology()
        .define_type(
            ObjectType::build(type_name, (table.schema.as_str(), table.name.as_str()))
                .prop_req("id", "Long")
                .prop("val", "Long")
                .done(),
        )
        .await
        .expect("define_log_type");
    name
}
```

(Confirm `ObjectType`/`TypeName` import paths already present in `e2e_support.rs`; `define_widget` uses them, so they are.)

- [ ] **Step 2: Write the failing e2e test.**

Create `src/services/query-api/tests/stream_log_subscribe_e2e.rs`. Model the harness/reader-grant on `stream_subscribe_e2e.rs` (Task-3 investigation confirmed `get_ndjson(cp_arc, read_eng, uri, role, max_lines, timeout)`), but seed via `seed_log_stream` and read as a granted reader:

```rust
//! GET /objects/Event/changes e2e (road-stream-log-table-subscribe): the full
//! governed NDJSON feed for an append-only LOG type through the real router —
//! every event a '+I', ordered gapless, resumable, across a flush boundary, and
//! a non-stream type still 400s. loom_fixture_test.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;
use control_plane_core::TableRef;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_flush::flush_table;
use e2e_support::{
    InProcessServingEngine, define_log_type, get_ndjson, grant_read, seed_log_stream,
    subject_with_role,
};
use loom_test_seed::local_sql_catalog;

fn key(line: &serde_json::Value) -> (i64, i64, String) {
    (
        line["bucket"].as_i64().expect("bucket"),
        line["offset"].as_i64().expect("offset"),
        line["change_kind"].as_str().expect("change_kind").to_string(),
    )
}
fn keys(lines: &[serde_json::Value]) -> Vec<(i64, i64, String)> {
    lines.iter().map(key).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_log_stream_across_a_flush_boundary() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("warehouse");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let table = TableRef { schema: "s".into(), name: "events".into() };

    // 3 inline + flush + 2 more = 5 events, all '+I'.
    seed_log_stream(&pool, &catalog, &table, &[1, 2, 3], &[10, 20, 30], 2).await;
    flush_table(&catalog, &pool, &table, control_plane_core::RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush");
    seed_log_stream(&pool, &catalog, &table, &[4, 5], &[40, 50], 2).await;

    let _ty = define_log_type(&cp, "Event", &table).await;
    // Grant the `reader` subject Read on `Event` — WITHOUT this the coarse gate
    // (`resolve_governed`) denies with 403 before the subscribe probe runs.
    // `get_ndjson(..., "reader", ...)` mints a session token for subject "reader".
    let (_subj, role) = subject_with_role(&cp, "reader").await;
    grant_read(&cp, &role, "Event").await;
    let cp_arc = Arc::new(cp.clone());
    let read_eng: Arc<dyn query_api::serving::ServingEngine> =
        Arc::new(InProcessServingEngine::new(IcebergCatalog::new(pool.clone())));

    let (status, lines) = get_ndjson(
        cp_arc.clone(),
        read_eng.clone(),
        "/objects/Event/changes?max_events=5",
        "reader", // the granted role name used by get_ndjson (match the grant helper)
        6,
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{lines:?}");
    assert_eq!(lines.len(), 5, "the log stream ended on its own at 5 events: {lines:?}");
    assert!(lines.iter().all(|l| l["change_kind"] == "+I"), "all '+I': {lines:?}");
    // No loom_* keys leak; user fields present.
    for l in &lines {
        assert!(l["fields"].as_object().expect("fields").keys().all(|k| !k.starts_with("loom_")));
    }
    // Ordered gapless per bucket from 0.
    let mut cursor: BTreeMap<i64, i64> = BTreeMap::new();
    for (b, o, _) in keys(&lines) {
        let at = cursor.get(&b).copied().unwrap_or(0);
        assert_eq!(o, at, "gapless offsets");
        cursor.insert(b, at + 1);
    }

    // Resume: head(2) then tail(3) from the returned cursor == full read.
    let (_s, head) = get_ndjson(cp_arc.clone(), read_eng.clone(),
        "/objects/Event/changes?max_events=2", "reader", 2, Duration::from_secs(10)).await;
    assert_eq!(head.len(), 2);
    let resume = head[1]["cursor"].as_str().expect("cursor").to_string();
    let uri = format!("/objects/Event/changes?cursor={resume}&max_events=3");
    let (_s, tail) = get_ndjson(cp_arc.clone(), read_eng.clone(), &uri, "reader", 3, Duration::from_secs(10)).await;
    let mut joined = keys(&head);
    joined.extend(keys(&tail));
    assert_eq!(joined, keys(&lines), "resume gapless + dup-free");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_stream_type_is_not_subscribable() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    // A type over a table with NO stream declaration (here: no mirror row at all,
    // so `stream_meta_for`/the positions probe resolve to `None`). The subject is
    // GRANTED Read so the coarse gate passes and we exercise the probe's None →
    // 400 path (not the 403 deny path).
    let table = TableRef { schema: "s".into(), name: "plain".into() };
    let _ty = define_log_type(&cp, "Plain", &table).await;
    let (_subj, role) = subject_with_role(&cp, "reader").await;
    grant_read(&cp, &role, "Plain").await;
    let cp_arc = Arc::new(cp.clone());
    let read_eng: Arc<dyn query_api::serving::ServingEngine> =
        Arc::new(InProcessServingEngine::new(IcebergCatalog::new(pool.clone())));
    let (status, _lines) = get_ndjson(
        cp_arc, read_eng, "/objects/Plain/changes?max_events=1", "reader", 1, Duration::from_secs(5),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "non-stream type is not subscribable");
}
```

> Implementer notes (resolve during TDD): (a) There is no single `grant_reader` helper — the reader grant is the two-call pattern `subject_with_role(&cp, "reader")` → `grant_read(&cp, &role, "<Type>")` (`e2e_support.rs:248-266`), and the subject string passed to `get_ndjson` must be `"reader"` (the CDC subscribe tests read as the *writer* subject via `grant_writer`, so they are not a copy source for the reader grant). Both Task 4 tests need the grant or they hit the 403 coarse-deny gate before the 400 probe. (b) The `non_stream_type_is_not_subscribable` test asserts 400 on a granted type whose table has no stream declaration (here: no mirror row → `changelog_latest` `None` → 400).

- [ ] **Step 3: Wire the target and run to verify it FAILS** (`define_log_type` unresolved, and/or the message assertion).

Add to `src/services/query-api/BUCK`:

```python
loom_fixture_test(
    name = "stream-log-subscribe-e2e",
    crate = "stream_log_subscribe_e2e",
    srcs = ["tests/stream_log_subscribe_e2e.rs"],
    crate_root = "tests/stream_log_subscribe_e2e.rs",
    edition = "2024",
    deps = [
        ":query-api",
        ":e2e-support",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//src/testing:seed",
        "//third-party:axum",
        "//third-party:serde_json",
        "//third-party:tempfile",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```

Run: `buck2 test --console none //src/services/query-api:stream-log-subscribe-e2e`
Expected: FAIL (build error on `define_log_type` first; after Step 4, the message assertion).

- [ ] **Step 4: Update the handler 400 message.**

In `src/services/query-api/src/http.rs` (~line 653), change the `Ok(None)` branch body:

```rust
        Ok(None) => {
            return (
                StatusCode::BAD_REQUEST,
                "type is not backed by a declared stream table",
            )
                .into_response();
        }
```

- [ ] **Step 5: Run the e2e test to verify it PASSES.**

Run: `buck2 test --console none //src/services/query-api:stream-log-subscribe-e2e`
Expected: `Tests finished: Pass 2. Fail 0`.

- [ ] **Step 6: CDC e2e non-regression + clippy.**

Run: `buck2 test --console none //src/services/query-api:stream-subscribe-e2e //src/services/query-api:stream-subscribe-gov-e2e` → Pass.
Run: `buck2 build --console none '//src/services/query-api:query-api[clippy.txt]'` and `cat` the path — empty.

- [ ] **Step 7: prek + commit.**

```bash
buck2 run //tools:prek -- run --all-files
git add src/services/query-api/src/http.rs src/services/query-api/tests/e2e_support.rs \
        src/services/query-api/tests/stream_log_subscribe_e2e.rs src/services/query-api/BUCK
git commit -m "feat(stream): log-table subscribe over the in-process router + declared-stream 400 message"
```

---

### Task 5: Production-wire e2e (acceptance #1's production-wire clause)

**Files:**
- Create: `src/services/query-api/tests/stream_log_subscribe_wire_e2e.rs`
- Modify: `src/services/query-api/BUCK` (new `loom_fixture_test`)

**Interfaces:**
- Consumes: whatever `stream_subscribe_wire_e2e.rs` uses to stand up the engine over the gRPC wire and drive the router against the **wire** serving engine (grep that file for its spawn helper — e.g. `spawn_engine_uds` / an `EngineControlClient`-backed `ServingEngine`), plus `seed_log_stream` / `define_log_type`.
- Produces: no API — a coverage test proving the kind-agnostic `ChangelogFeed` RPC (shipped in #426) serves log tables with no wire change.

- [ ] **Step 1: Write the failing wire e2e test.**

Create `src/services/query-api/tests/stream_log_subscribe_wire_e2e.rs`, mirroring `stream_subscribe_wire_e2e.rs` exactly but: seed via `seed_log_stream` (declared LOG), define the type via `define_log_type`, grant the reading subject Read via `subject_with_role(&cp, "reader")` + `grant_read(&cp, &role, "Event")` (the copy source reads as the *writer* subject — a log type has no write actions, so use a reader grant instead), and drive `/objects/Event/changes` over the **wire** serving engine. Assert the same 5-event ordered `+I` stream and gapless resume the in-process e2e asserts. (Copy the wire-engine spawn + wire `ServingEngine` construction verbatim from `stream_subscribe_wire_e2e.rs`; only the seed, the type, and the grant differ.)

> The whole point of this test: the `ChangelogFeed` unary `EngineControl` RPC is kind-agnostic (it forwards to `changelog_feed_scan`, which now dispatches to the LOG arm engine-side), so a log table serves over the production wire with zero client/handler/proto change.

- [ ] **Step 2: Wire the target and run to verify it FAILS** (test file references not yet complete / expected event assertions).

Add to `src/services/query-api/BUCK` (mirror the `stream-subscribe-wire-e2e` deps, adding `//src/testing:seed`):

```python
loom_fixture_test(
    name = "stream-log-subscribe-wire-e2e",
    crate = "stream_log_subscribe_wire_e2e",
    srcs = ["tests/stream_log_subscribe_wire_e2e.rs"],
    crate_root = "tests/stream_log_subscribe_wire_e2e.rs",
    edition = "2024",
    deps = [
        ":query-api",
        ":e2e-support",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//src/testing:flight",
        "//src/testing:seed",
        "//third-party:axum",
        "//third-party:serde_json",
        "//third-party:tempfile",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```

Run: `buck2 test --console none //src/services/query-api:stream-log-subscribe-wire-e2e`
Expected: FAIL until the test body is complete.

- [ ] **Step 3: Complete the test body** (copy the wire harness from `stream_subscribe_wire_e2e.rs`) and run to verify it PASSES.

Run: `buck2 test --console none //src/services/query-api:stream-log-subscribe-wire-e2e`
Expected: `Tests finished: Pass N. Fail 0`.

- [ ] **Step 4: prek + commit.**

```bash
buck2 run //tools:prek -- run --all-files
git add src/services/query-api/tests/stream_log_subscribe_wire_e2e.rs src/services/query-api/BUCK
git commit -m "test(stream): log-table subscribe over the production gRPC wire"
```

---

### Task 6: Register close + full-suite verification

**Files:**
- Modify: `docs/ROADMAP.md` (remove `road-stream-log-table-subscribe`)
- Modify: `docs/system-capabilities/stream.md` (fold in the landed capability)

- [ ] **Step 1: Full stream-suite green (non-regression backstop).**

Run: `buck2 test --console none //src/services/query-api/... //src/control-plane/postgres/... //src/services/engine-serving/...`
Expected: all Pass. (If disk-constrained in a cloud session, scope to the stream/subscribe targets + the two new postgres targets instead of the whole tree, and build with `-M none`.)

- [ ] **Step 2: Close the register item via `loom-docs-update`.**

Invoke the `loom-docs-update` skill (or edit directly): remove the `road-stream-log-table-subscribe` entry from `docs/ROADMAP.md`, and document the landed capability in `docs/system-capabilities/stream.md` (log tables are subscribable via `GET /objects/{type}/changes`; the feed reads base files ∪ inline via the LOG arm of `changelog_feed_scan`, dispatched by `StreamMeta.kind`; the append NOTIFY now fires for log tables). Name the id + PR number in the PR body per the register convention (registers carry open work only).

- [ ] **Step 3: Metric gate (final-review requirement).**

Run `loom-complexity diff` and `loom-duplication diff` (changed files only, print, no commit). Report any NEW hotspot over census thresholds (cc > 15, cognitive > 15, MI < 20, SLOC > 100) or NEW cross-file duplication ≥ 20 lines. Expected: the `union_govern_read` extraction should REDUCE feed.rs duplication rather than add it; the three new test files share seed helpers rather than copy them. Fix or explicitly justify each finding in the PR description.

- [ ] **Step 4: prek + commit the register close.**

```bash
buck2 run //tools:prek -- run --all-files
git add docs/ROADMAP.md docs/system-capabilities/stream.md
git commit -m "docs(stream): close road-stream-log-table-subscribe"
```

---

## Self-Review

**1. Spec coverage** (`2026-07-12-stream-log-table-subscribe-design.md`):
- Seam 1 (positions probe relaxed to Log) → Task 1. ✓
- Seam 2 (feed scan reads base files ∪ inline for Log) → Task 3. ✓
- Seam 3 (kind-agnostic dispatch engine-side; no wire/handler change) → Task 3 (`changelog_feed_scan` dispatch) + Task 5 (wire proof). ✓
- Await-NOTIFY gap ("confirm whether log-table inline appends NOTIFY; if not, the append path gains the notify") → confirmed NOT firing (iceberg_inline.rs:683 was CDC-only); fixed in Task 2 for the **inline** append path (which is what the spec's await bullet covers). The direct-Parquet bulk path notifies for neither kind (pre-existing gap, out of scope — see Task 2's scope note). ✓
- Testing → Log tail e2e (Task 3 scan + Task 4 router + Task 5 wire); positions probe (Task 1); governance masking (Task 3 step 2 case 5); await wakeup (Task 2); CDC regression (steps in Tasks 2/3/4 + Task 6 full suite). ✓
- Non-regression (CDC byte-identical, mv_delta untouched, new SQL→sqlx) → `union_govern_read` is a verbatim extraction; CDC path only gains a preceding kind lookup; mv_delta.rs is not touched; no new SQL literal added. ✓
- Acceptance #1 (in-process AND production wire) → Tasks 4 + 5. #2 (CDC unchanged) → regression steps. #3 (suites green) → Task 6. ✓

**2. Placeholder scan:** The two spots left for the implementer to resolve during TDD are named explicitly (the reader-grant helper name in Task 4, and the wire-harness copy in Task 5) — both are "copy the exact pattern from this named sibling file," not open-ended TODOs. All production code is shown in full.

**3. Type consistency:** `stream_meta_for(&PgPool, &TableRef) -> Result<Option<StreamMeta>>` is produced in Task 1 and consumed in Task 3 with the same signature. `log_feed_scan_at`'s parameter order matches `changelog_feed_scan_at` plus a trailing `base_pin: &Snapshot`. `union_govern_read` takes `base_pin_id: SnapshotId` (the `.id` of the pinned `Snapshot`), consistent across both callers. `id_val_batch`/`id_val_columns`/`seed_log_stream`/`define_log_type` names are used identically wherever referenced. The `TablePolicy` masked-set field (`masked: HashSet<String>`) matches `stream_subscribe_scan.rs`'s usage.
