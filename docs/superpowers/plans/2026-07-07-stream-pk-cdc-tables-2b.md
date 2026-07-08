# Stream engine — PK/CDC tables, Plan 2b (dual tables + LastRow compaction) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn a declared CDC table into the spec's **two** Iceberg tables — an append-only **changelog** (every event incl. `−U`) alongside the current-state **base** (`+I/+U/−D` only) — with a dual-write flush that advances both snapshots in one Postgres transaction, and a distinct `stream_consolidate` worker job that folds the base by **LastRow per identity** and re-arms the byte-trigger flush. Non-CDC (batch, slice-1 log, cow-inline-shadow) paths stay byte-identical.

**Architecture:** At `mode=cdc` declaration, create a second Iceberg table `<schema>.<name>__changelog` (physical schema = user columns + framing) plus its `iceberg_mirror` row, and store its `table_id` in the `stream.stream_table.changelog_table_id` column Plan 2a already added (inert until now). `flush_locked` gains a CDC branch: it captures the **full** inline set (incl. `−U`), writes the base delta subset (`+I/+U/−D`) and the full changelog set to two Parquet files **before** opening a transaction, then commits both mirror snapshots on **one** caller-provided tx via the `append_batches_on_tx` seam (Plan 1b's long-tx bulk-stream path), so a changelog event is durable iff its base delta is. Because the dual-write flush is delta-aware, the Plan 2a `has_shadow` flush-suppression is **lifted for CDC tables** (it stays for non-CDC cow-inline-shadow tables). Compaction is a new engine-side operation `consolidate_stream` (triggered by a thin zero-pool worker job over a new `EngineControl` RPC): it reads the base's physical framed rows, folds by LastRow (greatest `loom_offset` per identity within its bucket wins; a `−D`/tombstone drops the key), rewrites a fresh consolidated base **preserving framing**, and clears `has_shadow`. The changelog is never touched by compaction.

**Tech Stack:** Rust, buck2, sqlx compile-time queries (postgres control plane), Arrow 58 / Iceberg (single arrow major), tonic/protobuf (engine wire + `EngineControl`), DataFusion (fold), MinIO/LocalFs warehouse.

## Global Constraints

- **Tests are `rust_test` / `loom_fixture_test` integration targets only — never inline `#[cfg(test)]`.** New fixture tests MUST use the `loom_fixture_test` macro (`src/control-plane/postgres/defs.bzl`), not a bare `rust_test`, or the fixture env (PG binaries, MinIO, boot-slot dir) is missing and the test cannot boot. Wire each new test as its own target in the crate's `BUCK`, mirroring an existing sibling (e.g. `//src/control-plane/postgres:stream-cdc-emission`, `//src/services/query-api:stream-cdc-e2e`). The `no-inline-tests` prek hook fails the build if a `src/**.rs` file outside a `tests/` dir contains a `#[test]`/`#[tokio::test]`.
- **Build:** `buck2 build -v0 --console none //src/...` (silent on success; a failure still prints `BUILD FAILED` + the error). **Test:** `buck2 test --console none //src/...` (prints only the `Tests finished: Pass N. Fail 0` summary). On a root cloud host the buck2 shim injects `--unstable-allow-all-tests-on-re` automatically; locally, run the full fixture sweep with `-j 8` to avoid starving the 8 postgres boot-slots.
- **After changing any `query!`/`query_scalar!` SQL** (the new `changelog_table_id` read/write on `stream.stream_table`), run `tools/sqlx-prepare.sh` and commit the `.sqlx/` change; the `//src/control-plane/postgres:sqlx-cache-check` test enforces freshness. If `initdb` refuses to run (root host) and you cannot regenerate the cache, write the query as a runtime `sqlx::query(AssertSqlSafe(...))` against `stream`/`iceberg_mirror` (the `fixture.rs` precedent) rather than a compile-time macro — but prefer the macro + prepare.
- **Run prek before every commit:** `buck2 run //tools:prek -- run --all-files` (rustfmt is a separate hook that implementers routinely miss; clippy-clean ≠ lint-clean). Markdown files must end with exactly one trailing newline and no trailing whitespace or the `lint` job fails.
- **Clippy is strict (pedantic + restriction):** no `unwrap`/`expect`/`indexing_slicing`/`panic`/`todo`/`get_unwrap` in production lib/bin code. Use `.get()`/`.ok_or_else()`/`?`; `#[expect(lint, reason = "...")]` for a justified local exception (bare `#[allow]` trips `allow_attributes_without_reason`). Test code is exempted from the panic-safety lints via `loom_rust_test`/`loom_fixture_test`.
- **Non-CDC paths must stay byte-identical.** Batch tables, slice-1 log tables, and cow-inline-shadow (non-stream identity) tables must behave exactly as before this plan. Every new branch is gated on the registry recording `kind='cdc'`; the changelog table, the dual-write, the `has_shadow` lift, and the framing-preserving overwrite all no-op off the CDC path. When in doubt, verify a non-CDC fixture test (e.g. `//src/services/ingest:landing-materialize`, `//src/services/query-api:update-delete-e2e`) is unchanged.
- **Framing stays hidden from logical reads.** `loom_change_kind`/`loom_bucket`/`loom_offset`/`loom_tombstone` are reserved (`is_reserved`, `iceberg_catalog.rs:17`). The changelog table's physical schema carries framing; no logical read (`GET /objects`, `GET /datasets`, link traversal) may expose it. There is **no** user-facing changelog read in this slice (that is Slice 3, `road-stream-subscribe`).
- **Object-store I/O before `begin()`** (`iss-iceberg-tx-objectstore`) — except the deliberate long-tx bulk-stream exception `append_batches_on_tx` already documents (`iceberg_writer.rs:400`), which this plan reuses for the dual-write. Do not read object storage inside a short commit tx.
- **`changelog_table_id` has no FK** (migration 0037 declared it a bare `bigint`) — it is a soft pointer to an `iceberg_mirror.table` row. Set it once at declaration; treat NULL as "not a CDC table / not yet declared."

### Design decisions refining the spec (read before starting)

The spec (`docs/superpowers/specs/2026-07-07-stream-pk-cdc-tables-design.md`, §4–6) is the approved design; these are the implementation-level choices this plan locks in where the spec left latitude. They were surfaced to the user with the plan.

1. **Fold precedence is `loom_offset`, not `begin_snapshot`.** Flushed base file rows carry no per-row `begin_snapshot` (the merge-on-read file tier synthesizes precedence `0` — `build_merge_view`, `serving.rs:238`). But every event for one identity shares a bucket with a **gapless, monotonically increasing `loom_offset`** (Plan 2a). So the LastRow winner per identity is the row with the **greatest `loom_offset`** within its `loom_bucket`; if that row is a tombstone (`loom_tombstone=true`, i.e. a `−D`), the identity is dropped. This is why the base retains framing.
2. **The Plan 2a `has_shadow` flush-suppression is lifted for CDC tables.** In 2a, `has_shadow` suppresses the byte-trigger flush because the single-table flush could not correctly drain deltas. 2b's flush is delta-aware (it partitions by `loom_change_kind`), so for a `kind='cdc'` table the flush proceeds even with `has_shadow` set. The suppression stays for non-CDC cow-inline-shadow tables. `has_shadow` for CDC now means "unconsolidated deltas exist"; `consolidate_stream` clears it.
3. **Consolidation is engine-side, triggered by a thin worker job.** The worker is zero-pool (engine owns Postgres); reading base rows, folding, the framing-preserving overwrite, and clearing `has_shadow` all need the catalog + pool and must be one atomic engine operation. So `stream_consolidate` (worker job) decodes `{schema,name}` and calls a new `EngineControl::ConsolidateStream` RPC; the engine does the work.
4. **The changelog Iceberg table is created eagerly at declaration** (Iceberg metadata via `land_cdc`, mirror row + pointer via `reconcile_stream_mode`), so the declaration e2e can assert it exists immediately, matching spec §4.

---

### Task 1: Registry — `changelog_table_id` on `StreamMeta`, read/set, changelog `TableRef` helper

**Files:**
- Modify: `src/control-plane/core/src/stream.rs` (`StreamMeta` gains `changelog_table_id`)
- Modify: `src/control-plane/memory/src/stream.rs` (fake: construct + a `set_changelog_table_id`)
- Modify: `src/control-plane/postgres/src/stream.rs` (`pg_stream_meta` selects it; new `pg_set_changelog_table_id`; `StreamTables::set_changelog_table_id`)
- Modify: `src/control-plane/core/src/stream.rs` (trait gains `set_changelog_table_id`)
- Modify: `src/control-plane/postgres/.sqlx/` (regenerate via `tools/sqlx-prepare.sh`)
- Test: `src/control-plane/testkit/src/lib.rs` (extend the `StreamTables` contract) — runs on both adapters via existing targets.

**Interfaces:**
- Consumes: `StreamMeta { bucket_count, kind, bucket_key }` (`core/src/stream.rs:19`), `declare_cdc` (`core/src/stream.rs:50`), `pg_stream_meta` (`postgres/src/stream.rs:272`), `pg_declare_cdc` (`postgres/src/stream.rs:248`).
- Produces (later tasks rely on these EXACT names/types):
  - `StreamMeta` gains `pub changelog_table_id: Option<i64>` (same derives).
  - trait `async fn set_changelog_table_id(&self, table_id: i64, changelog_table_id: i64) -> Result<()>` on `StreamTables`.
  - `pub(crate) async fn pg_set_changelog_table_id<'e, E: sqlx::PgExecutor<'e>>(ex: E, table_id: i64, changelog_table_id: i64) -> Result<()>` (`postgres/src/stream.rs`).
  - `pub(crate) fn changelog_table_ref(base: &TableRef) -> TableRef` in `src/control-plane/postgres/src/iceberg_landing.rs` — `{ schema: base.schema.clone(), name: format!("{}__changelog", base.name) }`.

- [ ] **Step 1: Extend `StreamMeta` with the changelog pointer**

In `src/control-plane/core/src/stream.rs`, add the field to `StreamMeta`:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamMeta {
    pub bucket_count: i32,
    pub kind: StreamKind,
    pub bucket_key: Option<String>,
    /// The durable changelog table's `iceberg_mirror` `table_id` for a CDC table
    /// (slice 2b); `None` for a log table or a CDC table not yet given its
    /// changelog pointer. Soft pointer — no FK.
    pub changelog_table_id: Option<i64>,
}
```

Add the trait method after `declare_cdc` in the `StreamTables` trait:

```rust
/// Point a CDC table's registry row at its durable changelog table's mirror
/// `table_id`. Idempotent overwrite; only meaningful for a `kind='cdc'` row.
async fn set_changelog_table_id(&self, table_id: i64, changelog_table_id: i64) -> Result<()>;
```

- [ ] **Step 2: Update the memory fake**

In `src/control-plane/memory/src/stream.rs`, every `StreamMeta { ... }` literal must set the new field. In `declare_cdc` and `declare_stream`, add `changelog_table_id: None`. Add the method:

```rust
async fn set_changelog_table_id(&self, table_id: i64, changelog_table_id: i64) -> Result<()> {
    if let Some(m) = self.stream_tables.lock().get_mut(&table_id) {
        m.changelog_table_id = Some(changelog_table_id);
    }
    Ok(())
}
```

- [ ] **Step 3: Update the postgres `pg_stream_meta` read + add the setter**

In `src/control-plane/postgres/src/stream.rs`, extend `pg_stream_meta`'s SELECT to include `changelog_table_id` and map it into `StreamMeta`. Add:

```rust
pub(crate) async fn pg_set_changelog_table_id<'e, E: sqlx::PgExecutor<'e>>(
    ex: E,
    table_id: i64,
    changelog_table_id: i64,
) -> Result<()> {
    sqlx::query!(
        "update stream.stream_table set changelog_table_id = $2 where table_id = $1",
        table_id,
        changelog_table_id,
    )
    .execute(ex)
    .await
    .map_err(backend)?;
    Ok(())
}
```

And the trait impl on `PgControlPlane`:

```rust
async fn set_changelog_table_id(&self, table_id: i64, changelog_table_id: i64) -> Result<()> {
    pg_set_changelog_table_id(self.pool(), table_id, changelog_table_id).await
}
```

- [ ] **Step 4: Add the `changelog_table_ref` helper**

In `src/control-plane/postgres/src/iceberg_landing.rs`, near `augment_with_framing`:

```rust
/// The durable changelog table's `TableRef` for a CDC base table: same schema,
/// name suffixed `__changelog` (slice 2b, spec §4).
pub(crate) fn changelog_table_ref(base: &TableRef) -> TableRef {
    TableRef {
        schema: base.schema.clone(),
        name: format!("{}__changelog", base.name),
    }
}
```

- [ ] **Step 5: Regenerate the sqlx cache**

Run: `tools/sqlx-prepare.sh` (boots pinned postgres, applies migrations, `cargo sqlx prepare`). Commit the `.sqlx/` change. If on a root host where `initdb` refuses, fall back to `sqlx::query(AssertSqlSafe(...))` for the two changed statements (see Global Constraints).

- [ ] **Step 6: Extend the `StreamTables` testkit contract**

In `src/control-plane/testkit/src/lib.rs`, in the existing CDC contract, after `declare_cdc` assert `stream_meta(tid).changelog_table_id == None`, then call `set_changelog_table_id(tid, 4242)` and assert it round-trips to `Some(4242)`; assert a `declare_stream` (log) table keeps `changelog_table_id == None`.

```rust
// (inside the existing declare_cdc contract fn, after the kind/bucket asserts)
let m = cp.stream_meta(tid).await.expect("meta").expect("row");
assert_eq!(m.changelog_table_id, None, "changelog pointer null until set");
cp.set_changelog_table_id(tid, 4242).await.expect("set changelog id");
let m = cp.stream_meta(tid).await.expect("meta").expect("row");
assert_eq!(m.changelog_table_id, Some(4242), "changelog pointer round-trips");
```

- [ ] **Step 7: Build + test both adapters**

Run: `buck2 test --console none //src/control-plane/...`
Expected: `Pass N. Fail 0` (memory + postgres contract targets both green).

- [ ] **Step 8: Commit**

```bash
git add src/control-plane/core src/control-plane/memory src/control-plane/postgres src/control-plane/testkit
git commit -m "feat(stream): registry changelog_table_id pointer on StreamMeta"
```

---

### Task 2: Create the changelog Iceberg table + mirror row + pointer at CDC declaration

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_landing.rs` (`land_cdc`: create changelog Iceberg table before routing)
- Modify: `src/control-plane/postgres/src/stream.rs` (`reconcile_stream_mode`: create changelog mirror row + set pointer, in the CDC first-declare arm)
- Modify: `src/control-plane/postgres/src/iceberg_inline.rs` + `iceberg_landing.rs` if `reconcile_stream_mode`'s signature gains `at`/`catalog` threading (see Step 2)
- Test: `src/services/query-api/tests/stream_cdc_declare.rs` (new `loom_fixture_test`) + its `BUCK` target

**Interfaces:**
- Consumes: `ensure_iceberg_table(catalog, table, columns, include_framing)` (`iceberg_landing.rs:582`), `ensure_table(conn, ns, name, at)` (`iceberg_mirror.rs:91`), `next_snapshot(conn, None)` (`iceberg_mirror`), `reconcile_stream_mode(conn, tid, decl, pre_existing, table)` (`stream.rs:46`), `pg_set_changelog_table_id`/`changelog_table_ref` (Task 1), `augment_with_framing` (`iceberg_landing.rs:640`).
- Produces: after a `mode=cdc` declaration, `<schema>.<name>__changelog` exists as an Iceberg table (physical schema = user cols + framing) with its own `iceberg_mirror.table` row, and `stream.stream_table.changelog_table_id` points at that row's `table_id`.

- [ ] **Step 1: Create the changelog Iceberg table in `land_cdc` (object-store phase, before any tx)**

`land_cdc` (`iceberg_landing.rs:142`) holds the `catalog`; both its routes (`inline_append_decl`, `land_parquet`) need the changelog table's Iceberg metadata to pre-exist. After `combine_stream_decl` and `align_to_columns`, when the decl is `Cdc`, create it (idempotent — `ensure_iceberg_table` early-exits if present):

```rust
let decl = combine_stream_decl(stream_buckets, cdc)?;
let (schema, batches) = align_to_columns(&schema, batches, columns)?;

// A CDC declaration also owns a durable changelog Iceberg table (spec §4). Its
// object-store metadata is created HERE (before any commit tx), so both landing
// routes can assume it exists; its mirror row + registry pointer are set inside
// the write tx by `reconcile_stream_mode`. Idempotent.
if matches!(decl, StreamDecl::Cdc { .. }) {
    let clog = changelog_table_ref(table);
    ensure_iceberg_table(catalog, &clog, columns, true).await?;
}
```

- [ ] **Step 2: Create the changelog mirror row + set the pointer in `reconcile_stream_mode`**

`reconcile_stream_mode` (`stream.rs:46`) runs on the write tx with `tid` in hand, but has no `catalog` (fine — the Iceberg table was made in Step 1) and no `at`. Thread a snapshot in: change its signature to take `at: SnapshotId` (both callers — `inline_append_decl` at `iceberg_inline.rs` and `land_parquet_stream` at `iceberg_landing.rs:906` — already hold the write's snapshot; pass it). In the CDC first-declare arm (the `(Some(n), None)` → `StreamDecl::Cdc` branch, currently `stream.rs:108`), after `pg_declare_cdc`:

```rust
StreamDecl::Cdc { bucket_key, .. } => {
    pg_declare_cdc(&mut *conn, tid, n, bucket_key).await?;
    // Register the changelog table's mirror row (its Iceberg metadata was
    // created by land_cdc before this tx) and point the registry at it. Its
    // columns are projected on first flush append; an empty mirror row is a
    // valid never-written table (no user-facing changelog read in this slice).
    let clog = crate::iceberg_landing::changelog_table_ref(table);
    let clog_at = crate::iceberg_mirror::next_snapshot(&mut *conn, None).await?;
    let clog_tid =
        crate::iceberg_mirror::ensure_table(&mut *conn, &clog.schema, &clog.name, clog_at).await?;
    pg_set_changelog_table_id(&mut *conn, tid, clog_tid).await?;
}
```

Note: `ensure_table` opens a `SAVEPOINT` (Plan 2a's create-race fix) — it is already running inside the caller's explicit write tx here, so the savepoint is legal.

- [ ] **Step 3: Update `reconcile_stream_mode`'s two call sites for the new `at` param**

In `inline_append_decl` (`iceberg_inline.rs`) and `land_parquet_stream` (`iceberg_landing.rs:906`), pass the already-allocated write snapshot as the new `at` argument. Grep for `reconcile_stream_mode(` to find both; each has the snapshot in scope (the same value passed to `ensure_table` for the base). If a call site does not yet hold it, allocate with `next_snapshot(&mut *conn, None)` immediately before.

- [ ] **Step 4: Write the declaration test (failing first)**

Create `src/services/query-api/tests/stream_cdc_declare.rs` (`loom_fixture_test`). Seed a CDC declaration through the same path `stream_cdc_e2e.rs` uses (`define_widget` + a `mode=cdc` land, or drive `land_cdc` directly with a `CdcDecl`), then assert:
- `stream_meta(base_tid).changelog_table_id == Some(clog_tid)` and `clog_tid != base_tid`;
- the changelog mirror row exists: `resolve_table(&changelog_table_ref(&base), at)` succeeds;
- the changelog's physical columns include framing: `physical_columns(clog_tid, at)` contains `loom_change_kind`, `loom_bucket`, `loom_offset`;
- a `mode=cdc` land against a type with **no identity** → `400` (already covered in 2a; re-assert here only if convenient);
- non-CDC land creates **no** `__changelog` table (`resolve_table` on the suffix errors / `changelog_table_id` is `None`).

- [ ] **Step 5: Wire the BUCK target, run it**

Add a `loom_fixture_test` target `stream-cdc-declare` in `src/services/query-api/BUCK` mirroring `stream-cdc-e2e` (same deps incl. `:e2e-support`).
Run: `buck2 test --console none //src/services/query-api:stream-cdc-declare`
Expected: `Pass 1. Fail 0`.

- [ ] **Step 6: Regression — non-CDC + 2a CDC paths unchanged**

Run: `buck2 test --console none //src/services/ingest:landing-materialize //src/control-plane/postgres:stream-cdc-emission //src/control-plane/postgres:stream-cdc-bucket`
Expected: all green (declaration change is CDC-gated; base landing untouched).

- [ ] **Step 7: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A && git commit -m "feat(stream): create changelog Iceberg table + registry pointer at CDC declaration"
```

---

### Task 3: Full inline read (including `−U`) for the changelog

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_inline.rs` (add `inline_live_batch_full`)
- Test: `src/control-plane/postgres/tests/stream_inline_full_read.rs` (new `loom_fixture_test`) + its `BUCK` target

**Interfaces:**
- Consumes: `inline_live_batch(table, at) -> Result<Option<(i64, Vec<i64>, RecordBatch)>>` (`iceberg_inline.rs:1322`), which filters `loom_change_kind <> '-U'`; `mvcc_live_pred`, `inline_table_name`, the col-list builder it already uses.
- Produces: `pub(crate) async fn inline_live_batch_full(&self, table: &TableRef, at: SnapshotId) -> Result<Option<(i64, Vec<i64>, RecordBatch)>>` — identical to `inline_live_batch` but WITHOUT the `−U` exclusion, so the batch carries every live inline row incl. `−U` before-images. Task 4 uses it for the changelog append.

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/postgres/tests/stream_inline_full_read.rs` (`loom_fixture_test`). Declare a `buckets=1` CDC table, insert id=1, then `write_inline_delta` an update (emits `−U`/`+U`). Assert:
- `inline_live_batch` (existing) returns rows whose `loom_change_kind` set is `{+I? , +U}` and contains **no** `-U`;
- `inline_live_batch_full` (new) returns a superset that **does** contain a `-U` row.

Read the framing back by re-querying `inline_<tid>` for `loom_change_kind` on the returned `row_ids`, mirroring `stream_cdc_emission.rs::rows_by_offset`.

- [ ] **Step 2: Run it, verify it fails to compile (method absent)**

Run: `buck2 test --console none //src/control-plane/postgres:stream-inline-full-read`
Expected: build failure — `inline_live_batch_full` not found. (Add the BUCK target first so the failure is "method missing," not "target missing.")

- [ ] **Step 3: Implement `inline_live_batch_full`**

In `iceberg_inline.rs`, factor the shared body of `inline_live_batch` into a private helper taking an `exclude_minus_u: bool`, or copy the function and drop the `and (loom_change_kind is null or loom_change_kind <> '-U')` clause. Keep `inline_live_batch` byte-identical (it still excludes `−U`). The full variant's query:

```rust
let sql = format!(
    "select loom_row_id, {col_list} from {} where {} order by loom_row_id",
    inline_table_name(tid),
    mvcc_live_pred(at.0),
);
```

(No `−U` predicate.) Everything else — the `Option` early-return when the inline table is absent, the Arrow decode, the `(tid, row_ids, batch)` shape — matches `inline_live_batch`.

- [ ] **Step 4: Run the test to green**

Run: `buck2 test --console none //src/control-plane/postgres:stream-inline-full-read`
Expected: `Pass 1. Fail 0`.

- [ ] **Step 5: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A && git commit -m "feat(stream): inline_live_batch_full — full inline read including -U for the changelog"
```

---

### Task 4: Dual-write flush (base `+I/+U/−D` + changelog all-events, one tx) and lift `has_shadow` for CDC

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_flush.rs` (`flush_locked`: CDC dual-write branch)
- Modify: `src/control-plane/postgres/src/iceberg_inline.rs` (byte-trigger enqueue guard: don't suppress CDC — `iceberg_inline.rs:684`)
- Test: `src/control-plane/postgres/tests/stream_cdc_dual_flush.rs` (new `loom_fixture_test`) + its `BUCK` target

**Interfaces:**
- Consumes: `flush_locked(catalog, pool, table, run_id) -> Result<Option<SnapshotId>>` (`iceberg_flush.rs:57`); `inline_live_batch` (base subset, excludes `−U`) + `inline_live_batch_full` (Task 3, changelog set); `append_batches_on_tx(catalog, &Table, batches, extras, &mut tx) -> Result<Vec<WrittenFile>>` (`iceberg_writer.rs:403`, the caller-tx single-attempt seam); `SqlCatalog::load_table` / `ensure_iceberg_table`; `InlineEndCap`, `CommitExtras` (`iceberg_writer.rs`); `pg_stream_meta` (kind check); `COMMIT_MAX_RETRIES`/`commit_backoff` (retry, as `land_parquet` uses).
- Produces: for a `kind='cdc'` table, one flush advances TWO mirror snapshots atomically — the base table gains the `+I/+U/−D` rows, the changelog table gains ALL rows incl. `−U`. Inline rows are end-capped once. Non-CDC flush stays byte-identical (single `append_parquet_snapshot`).

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/postgres/tests/stream_cdc_dual_flush.rs` (`loom_fixture_test`). Declare a `buckets=2` CDC table (mirroring `stream_cdc_emission.rs`), insert id=1, update id=1 (qty 100→200), then invoke the flush primitive (`flush_locked` via its public entry, or the `handle_flush` seam if that's how the fixture drives it — mirror how an existing flush fixture test triggers a flush). Assert:
- **Base table** (`inline_mirror`/data files of `base_tid`) after flush holds `+I`(200 after-image ordering aside), `+U`, `−D`? — specifically the change-kind multiset EXCLUDES `−U`;
- **Changelog table** (`changelog_table_id`'s mirror, read via `read_files_as_batches` or `physical_columns`+data-file scan) holds ALL events INCLUDING the `−U` before-image (qty=100);
- Both advanced: the base and changelog mirror `current_snapshot` both moved; kill-between-snapshots leaves neither (assert via a single-tx property — if the test harness can't fault-inject, assert both snapshots share the commit by checking they reference the same lineage `run_id`/event, and add a comment that the atomicity is structural via `append_batches_on_tx` on one tx);
- offsets stay gapless per bucket across the flush boundary.

- [ ] **Step 2: Run it, verify it fails**

Run: `buck2 test --console none //src/control-plane/postgres:stream-cdc-dual-flush` (add the BUCK target first).
Expected: FAIL — flush still single-writes (no changelog rows).

- [ ] **Step 3: Lift the `has_shadow` flush-suppression for CDC**

In `iceberg_inline.rs:684` (the byte-trigger enqueue), the guard is `... && !has_shadow(&mut *conn, tid).await?`. Change it so a CDC table is NOT suppressed (its delta-aware flush is safe). Read the kind once:

```rust
let is_cdc = crate::stream::pg_stream_meta(&mut *conn, tid)
    .await?
    .is_some_and(|m| m.kind == control_plane_core::StreamKind::Cdc);
if st.live_bytes >= st.effective && !st.enqueued && (is_cdc || !has_shadow(&mut *conn, tid).await?) {
    // ... enqueue flush_table, arm trigger ...
}
```

(Non-CDC cow-inline-shadow tables keep the suppression — the `!has_shadow` branch still gates them.)

- [ ] **Step 4: Implement the CDC dual-write branch in `flush_locked`**

In `iceberg_flush.rs`, after computing `current` and the stream/kind status, branch on `kind='cdc'` (via `pg_stream_meta(pool, base_tid)`). For CDC:
1. `let Some((tid, all_ids, all_batch)) = ice.inline_live_batch_full(table, current.id).await? else { reset trigger; return Ok(None) };` — the changelog set.
2. Derive the base subset by filtering `all_batch` to rows whose `loom_change_kind <> '-U'` (compute a boolean mask over the batch's `loom_change_kind` column; `arrow` `filter::filter_record_batch`). Keep `all_batch` for the changelog.
3. Load both Iceberg tables: base via the existing path (`ensure_iceberg_table` + `load_table`), changelog via `load_table(changelog_table_ref(table))`.
4. Write BOTH Parquets and commit on ONE tx, mirroring `land_parquet`'s retry loop:

```rust
// Retry loop (single-attempt append_batches_on_tx; a lost CAS rolls back the
// whole tx so BOTH tables' pointers move together or not at all).
let mut attempt = 0;
let snap = loop {
    let mut tx = pool.begin().await.map_err(backend)?;
    // Base: +I/+U/-D subset, framing included, end-cap the inline rows here.
    let base_res = append_batches_on_tx(
        catalog, &base_table, vec![base_batch.clone()],
        CommitExtras { lineage: Some(&lineage), end_cap: Some(end_cap), jobs: &rebuild_jobs, ..CommitExtras::default() },
        &mut tx,
    ).await;
    // Changelog: ALL events incl -U, framing included, append-only (no end-cap).
    let clog_res = match base_res {
        Ok(_) => append_batches_on_tx(
            catalog, &clog_table, vec![all_batch.clone()],
            CommitExtras { lineage: Some(&lineage), ..CommitExtras::default() },
            &mut tx,
        ).await,
        Err(e) => Err(e),
    };
    match clog_res {
        Ok(_) => { tx.commit().await.map_err(backend)?; break current_after(...); }
        Err(e) if is_retryable_conflict(&e) && attempt < COMMIT_MAX_RETRIES => {
            drop(tx.rollback().await); attempt += 1; commit_backoff(attempt).await; continue;
        }
        Err(e) => { drop(tx.rollback().await); return Err(e); }
    }
};
```

Adapt the exact `CommitExtras` fields and the `end_cap`/`rebuild_jobs` construction from the current `flush_locked` (`iceberg_flush.rs:112-148`). **The end-cap must be applied exactly once** (on the base append) — it marks the inline rows consumed for both tables. Match `land_parquet`'s retry/`is_retryable_conflict`/`commit_backoff` helpers precisely (grep `commit_backoff` / `COMMIT_MAX_RETRIES`).

5. For a NON-CDC table, keep the current single `append_parquet_snapshot` path verbatim (byte-identical).

- [ ] **Step 5: Run the test to green**

Run: `buck2 test --console none //src/control-plane/postgres:stream-cdc-dual-flush`
Expected: `Pass 1. Fail 0`.

- [ ] **Step 6: Regression — flush + non-CDC unchanged**

Run: `buck2 test --console none //src/control-plane/postgres:... //src/services/ingest:...` (the full postgres + ingest fixture sweep, `-j 8` locally). Expected: green; confirm existing flush tests and `landing-materialize` unchanged.

- [ ] **Step 7: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A && git commit -m "feat(stream): dual-write flush (base + changelog, one tx); lift has_shadow suppression for CDC"
```

---

### Task 5: Framing-preserving whole-table overwrite

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_landing.rs` (`overwrite_parquet_snapshot`: derive `include_framing`)
- Test: `src/control-plane/postgres/tests/stream_overwrite_framing.rs` (new `loom_fixture_test`) + its `BUCK` target

**Interfaces:**
- Consumes: `overwrite_parquet_snapshot(pool, catalog, table, columns, batches, lineage) -> Result<SnapshotId>` (`iceberg_landing.rs:1080`), which today hardcodes `include_framing=false` (comment: "future scope — Plan 1b Task 5"); `append_parquet_snapshot(..., include_framing)` (`iceberg_landing.rs:239`); `pg_stream_meta` (kind/stream check).
- Produces: `overwrite_parquet_snapshot` preserves framing when overwriting a declared stream table — a CDC base overwrite keeps `loom_change_kind`/`loom_bucket`/`loom_offset`. Batch-table overwrite stays `include_framing=false` (byte-identical). Task 6's consolidation relies on this.

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/postgres/tests/stream_overwrite_framing.rs` (`loom_fixture_test`). Declare a `buckets=1` CDC table, insert + flush so the base holds framed file rows, then call `overwrite_parquet_snapshot` with the framed batch (user cols + framing). Assert the resulting base's `physical_columns(base_tid, at_after)` still contains `loom_change_kind`/`loom_bucket`/`loom_offset` (today it would not). Add a second assertion that a NON-stream (batch) table overwrite has NO framing columns (unchanged behaviour).

- [ ] **Step 2: Run it, verify it fails**

Run: `buck2 test --console none //src/control-plane/postgres:stream-overwrite-framing` (add BUCK target first).
Expected: FAIL — framing dropped.

- [ ] **Step 3: Derive `include_framing` in `overwrite_parquet_snapshot`**

Replace the hardcoded `false` (`iceberg_landing.rs:1107`) with a value derived from the registry: the table keeps framing iff it is a declared stream table.

```rust
// A declared stream table's physical schema carries framing; an overwrite must
// preserve it (else later flushes see a schema divergence). Batch tables stay
// framing-free — byte-identical to before.
let include_framing = {
    let tid = resolve_table(pool, table).await?; // mirror the resolve used elsewhere in this fn
    crate::stream::pg_stream_bucket_count(pool, tid).await?.is_some()
};
append_parquet_snapshot(pool, catalog, table, columns, batches, CommitExtras {
    lineage,
    overwrite: true,
    jobs: &rebuild_jobs,
    data_trigger_tables: std::slice::from_ref(table),
    ..CommitExtras::default()
}, include_framing).await
```

Use whatever table-id resolution the function already performs (it computes `rebuild_jobs` for the table, so a `tid` is at hand — reuse it rather than re-resolving). Passing framed `batches` (user + framing columns) with `include_framing=true` makes `augment_with_framing(columns, true)` rebuild the physical column list to match the batch layout.

- [ ] **Step 4: Run to green**

Run: `buck2 test --console none //src/control-plane/postgres:stream-overwrite-framing`
Expected: `Pass 1. Fail 0`.

- [ ] **Step 5: Regression — non-stream overwrite (action UPDATE/DELETE) unchanged**

Run: `buck2 test --console none //src/services/query-api:update-delete-e2e //src/services/query-api:cow-inline-shadow-gov-e2e`
Expected: green — those tables are non-stream, so `include_framing` stays `false`.

- [ ] **Step 6: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A && git commit -m "feat(stream): framing-preserving overwrite for declared stream tables"
```

---

### Task 6: Engine-side `consolidate_stream` — LastRow fold + framing overwrite + clear `has_shadow`

**Files:**
- Modify: engine proto (`src/services/engine-wire/proto/*.proto` — add `ConsolidateStream` to the `EngineControl` service)
- Modify: `src/services/engine-wire/src/client.rs` (client method `consolidate_stream`)
- Modify: engine control-service impl (`src/services/engine/src/control.rs` or wherever `CommitTransform` is served) — `consolidate_stream` handler
- Modify: `src/services/engine-serving/src/action_writer.rs` (or a new `consolidate.rs`) — the engine-side fold + overwrite
- Modify: `src/control-plane/postgres/src/iceberg_inline.rs` (add `clear_has_shadow`)
- Test: `src/services/query-api/tests/stream_cdc_consolidate.rs` (new `loom_fixture_test`) + its `BUCK` target

**Interfaces:**
- Consumes: `read_files_as_batches(catalog, table, files) -> Result<(SchemaRef, Vec<RecordBatch>)>` (`iceberg_read.rs:25`, returns framed physical rows); `physical_columns(tid, at)` (`iceberg_catalog.rs:146`, framed column list); `overwrite_parquet_snapshot` (Task 5, now framing-preserving); `list_files`/live-data-file listing (as `handle_compact` uses); `has_shadow`/`set_has_shadow` (`iceberg_inline.rs:736`); `changelog_table_ref`; a `SessionContext` (DataFusion, as `worker/src/transform.rs:228` builds).
- Produces: `pub(crate) async fn clear_has_shadow(conn: &mut PgConnection, tid: i64) -> Result<()>` (`DELETE FROM iceberg_mirror.shadow_flag WHERE table_id=$1`); an engine RPC `ConsolidateStream { schema, name }` that folds the base and clears the flag; a wire client `consolidate_stream(schema, name) -> Result<i64>` (returns the new base snapshot id, or `0` if nothing to do).

- [ ] **Step 1: Add `clear_has_shadow`**

In `iceberg_inline.rs`, next to `set_has_shadow` (`736`):

```rust
/// Clear `tid`'s inline-shadow flag after consolidation, re-arming the
/// byte-trigger flush. Idempotent.
pub(crate) async fn clear_has_shadow(conn: &mut sqlx::PgConnection, tid: i64) -> Result<()> {
    sqlx::query(AssertSqlSafe(
        "delete from iceberg_mirror.shadow_flag where table_id = $1",
    ))
    .bind(tid)
    .execute(&mut *conn)
    .await
    .map_err(backend)?;
    Ok(())
}
```

- [ ] **Step 2: Implement the engine-side fold (`consolidate_stream`)**

In `engine-serving` (new `consolidate.rs`, wired into the control handler), given `(schema, name)`:
1. Resolve `base_tid`; read `StreamMeta` — if not `kind='cdc'`, return `Ok(0)` (no-op; consolidation only applies to CDC).
2. List the base's live data files (as `handle_compact` does) and read them framed: `read_files_as_batches(catalog, &base, &files)`. Also read the live inline framed rows if any remain (`inline_live_batch_full`) so an un-flushed tail folds in too — union them.
3. Fold with DataFusion (`SessionContext`, mirroring `transform.rs`): register the union batch, run

```sql
select <user_cols>, loom_change_kind, loom_bucket, loom_offset
from (
  select *, row_number() over (
    partition by "<identity>" order by loom_offset desc
  ) as _rn
  from base_input
) t
where _rn = 1 and (loom_tombstone is not true) and loom_change_kind <> '-D'
```

(greatest `loom_offset` per identity wins; drop tombstoned/`−D` winners). Collect the folded framed batches.
4. Overwrite the base with the folded framed batches via `overwrite_parquet_snapshot` (Task 5 preserves framing). This end-caps the prior base files and inline rows and projects the folded rows as the new live base — all in the overwrite's commit tx.
5. In the SAME control flow, after the overwrite commits, `clear_has_shadow(conn, base_tid)`.
6. **Do NOT touch the changelog table** (spec §6 — it is the durable log; its retention rides `gc_table`).
7. Return the new base snapshot id.

Keep the fold SQL identity-quoted (injection-safe, as the existing derived-property SQL does).

- [ ] **Step 3: Add the `EngineControl::ConsolidateStream` RPC + client**

In the engine proto, add `rpc ConsolidateStream(ConsolidateStreamRequest) returns (ConsolidateStreamResponse)` with `{ string schema; string name; }` → `{ int64 snapshot_id; }`. Regenerate (the build's tonic codegen). Add the engine service handler delegating to Step 2, and the wire client:

```rust
pub async fn consolidate_stream(&self, schema: String, name: String) -> Result<i64> {
    let resp = self.inner.clone()
        .consolidate_stream(pb::ConsolidateStreamRequest { schema, name })
        .await.map_err(be)?.into_inner();
    Ok(resp.snapshot_id)
}
```

- [ ] **Step 4: Write the consolidation test (failing first)**

Create `src/services/query-api/tests/stream_cdc_consolidate.rs` (`loom_fixture_test`, spawning the engine writer like `stream_cdc_e2e.rs`). On a CDC table: insert id=1, update id=1 (v→v'), insert id=2, delete id=2 — flush so deltas land in the base — then call `consolidate_stream("main","widget")`. Assert:
- the base now holds exactly ONE row for id=1 (the latest, greatest `loom_offset`), id=2 absent (deleted);
- framing columns still present on the consolidated base (`physical_columns` contains `loom_offset`);
- `has_shadow(base_tid)` is now false;
- the **changelog table is unchanged** — every historical event (incl. `−U`) still present, same row count as before consolidation;
- a `GET /objects/Widget` current-state read is IDENTICAL before and after consolidation (id=1 only, correct values, no `loom_*` leak).

- [ ] **Step 5: Wire the target, run to green**

Add `stream-cdc-consolidate` to `src/services/query-api/BUCK`.
Run: `buck2 test --console none //src/services/query-api:stream-cdc-consolidate`
Expected: `Pass 1. Fail 0`.

- [ ] **Step 6: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A && git commit -m "feat(stream): engine-side consolidate_stream — LastRow fold, framing-preserving base rewrite, clear has_shadow"
```

---

### Task 7: `stream_consolidate` worker job — thin trigger + registration

**Files:**
- Create: `src/control-plane/core/src/stream_consolidate_job.rs` (`STREAM_CONSOLIDATE_JOB_KIND` + `StreamConsolidateJob`)
- Modify: `src/control-plane/core/src/lib.rs` (re-export)
- Create: `src/services/worker/src/consolidate.rs` (`handle_stream_consolidate`)
- Modify: `src/services/worker/src/main.rs` (register kind + dispatch arm) and `worker/src/lib.rs`/module wiring
- Test: `src/services/worker/tests/stream_consolidate_job.rs` (new `loom_fixture_test`) + its `BUCK` target

**Interfaces:**
- Consumes: `Job`/`NewJob` (`core/src/queue.rs:16`); the worker dispatch match (`worker/src/main.rs:95`); the engine-wire `consolidate_stream` client (Task 6); the worker's existing `GrpcQueueClient`/engine client context (as `CompactCtx` holds).
- Produces: `pub const STREAM_CONSOLIDATE_JOB_KIND: &str = "stream_consolidate";` and `pub struct StreamConsolidateJob { pub schema: String, pub name: String }` (serde); a `handle_stream_consolidate(ctx, job)` that decodes the payload and calls `consolidate_stream`.

- [ ] **Step 1: Define the job kind + payload**

`src/control-plane/core/src/stream_consolidate_job.rs`:

```rust
pub const STREAM_CONSOLIDATE_JOB_KIND: &str = "stream_consolidate";

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct StreamConsolidateJob {
    pub schema: String,
    pub name: String,
}
```

Re-export both from `core/src/lib.rs` (mirror `FlushJob`/`FLUSH_JOB_KIND`).

- [ ] **Step 2: Implement the thin handler**

`src/services/worker/src/consolidate.rs` — mirror `handle_compact`'s shape (decode payload → call engine), but the work is one RPC:

```rust
pub async fn handle_stream_consolidate(
    ctx: &ConsolidateCtx,
    job: Job,
) -> std::result::Result<(), JobFailure> {
    let StreamConsolidateJob { schema, name } = serde_json::from_value(job.payload)
        .map_err(|e| JobFailure::abandon(format!("bad stream_consolidate payload: {e}")))?;
    ctx.engine
        .consolidate_stream(schema, name)
        .await
        .map_err(|e| JobFailure::retry(format!("consolidate_stream: {e}")))?;
    Ok(())
}
```

Define `ConsolidateCtx { pub engine: <engine-wire control client> }` (or reuse an existing worker ctx that already holds the control client — check `CompactCtx`/`tctx`; prefer reusing).

- [ ] **Step 3: Register the kind + dispatch arm**

In `worker/src/main.rs`, add `STREAM_CONSOLIDATE_JOB_KIND.to_string()` to the `worker.run(&[...])` registration array (`main.rs:81`), and a match arm (`main.rs:95`):

```rust
k if k == STREAM_CONSOLIDATE_JOB_KIND => handle_stream_consolidate(&scctx, job).await,
```

Construct `scctx` alongside the other ctxs.

- [ ] **Step 4: Write the worker test**

Create `src/services/worker/tests/stream_consolidate_job.rs` (`loom_fixture_test`). Enqueue a `StreamConsolidateJob` for a seeded CDC table with unconsolidated deltas, run the worker's dispatch (mirror how an existing worker fixture test drives `handle_flush`/`handle_compact`), and assert the base folded + `has_shadow` cleared (the same end-state Task 6 asserts, reached through the job path). If the worker fixture cannot spawn an engine, assert the handler decodes and issues the RPC against a stub, and rely on Task 6's e2e for the engine-side behaviour — document which.

- [ ] **Step 5: Wire the target, run to green**

Add `stream-consolidate-job` to `src/services/worker/BUCK`.
Run: `buck2 test --console none //src/services/worker:stream-consolidate-job`
Expected: `Pass 1. Fail 0`.

- [ ] **Step 6: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A && git commit -m "feat(stream): stream_consolidate worker job — thin trigger over ConsolidateStream RPC"
```

---

### Task 8: Consolidation enqueue trigger (delta-count threshold + `LOOM_*` knob)

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_inline.rs` (`write_inline_delta`: accrue + enqueue `stream_consolidate` on threshold)
- Modify: `src/services/ingest/src/config.rs` (add a `consolidate_delta_threshold` knob to `RoutingTuning` or a sibling; `LOOM_CONSOLIDATE_DELTA_THRESHOLD`)
- Modify: wherever the inline write path receives its tuning, thread the threshold to `write_inline_delta` (mirror `flush_byte_threshold` threading)
- Test: `src/control-plane/postgres/tests/stream_cdc_consolidate_trigger.rs` (new `loom_fixture_test`) + its `BUCK` target

**Interfaces:**
- Consumes: the byte-trigger enqueue pattern (`iceberg_inline.rs:684` — `bump_inline_trigger`/`arm_inline_trigger`/`pg_insert`); `NewJob`; `STREAM_CONSOLIDATE_JOB_KIND` (Task 7); `RoutingTuning`/`overlay_opt` (`ingest/src/config.rs:33`).
- Produces: on a CDC table, after N accumulated deltas (default e.g. 128; `LOOM_CONSOLIDATE_DELTA_THRESHOLD`), one `stream_consolidate` job is enqueued atomically with the delta write; armed once until the job runs (which clears `has_shadow` → re-arms).

- [ ] **Step 1: Add the config knob**

In `ingest/src/config.rs`, add `pub consolidate_delta_threshold: i64` to the routing tuning struct with a `#[serde(default)]`-friendly default (Default impl → e.g. `128`), and in `overlay_env` add `overlay_opt(vars, "LOOM_CONSOLIDATE_DELTA_THRESHOLD", &mut self.consolidate_delta_threshold)?;`. Add a `validate()` range check (`>= 1`).

- [ ] **Step 2: Write the failing test**

Create `src/control-plane/postgres/tests/stream_cdc_consolidate_trigger.rs` (`loom_fixture_test`). Declare a CDC table with a small threshold (pass it into `write_inline_delta`), emit that many deltas, and assert exactly one `stream_consolidate` job exists in the queue (`select count(*) from queue... where kind='stream_consolidate'`), and that a second run below the next threshold does NOT enqueue a second (armed-once).

- [ ] **Step 3: Implement the enqueue**

In `write_inline_delta` (`iceberg_inline.rs`, where the CDC branch emits `−U/+U/−D` and sets `has_shadow`), after the delta rows are written, accrue a per-table delta count and enqueue on threshold — mirroring the byte-trigger block (`684`) but counting deltas, not bytes, and using a separate trigger row so it doesn't clash with the byte trigger:

```rust
if let Some(threshold) = consolidate_threshold {
    let st = bump_consolidate_trigger(&mut *tx, tid, emitted_rows, threshold).await?;
    if st.count >= threshold && !st.enqueued {
        let job = NewJob {
            kind: control_plane_core::STREAM_CONSOLIDATE_JOB_KIND.to_string(),
            payload: serde_json::json!({ "schema": table.schema, "name": table.name }),
            run_at: None,
            priority: 0,
        };
        crate::queue::pg_insert(&mut *tx, &job).await?;
        arm_consolidate_trigger(&mut *tx, tid).await?;
    }
}
```

Add `bump_consolidate_trigger`/`arm_consolidate_trigger` mirroring `bump_inline_trigger`/`arm_inline_trigger` (a small trigger-state row keyed by `tid`; a new column or a sibling table). The trigger is disarmed when `consolidate_stream` runs — either clear it in the engine handler alongside `clear_has_shadow` (Task 6), or key the re-arm off `has_shadow` being cleared. Thread `consolidate_threshold: Option<i64>` from the tuning into `write_inline_delta` the way `flush_threshold` is threaded into `inline_append`.

- [ ] **Step 4: Run to green**

Run: `buck2 test --console none //src/control-plane/postgres:stream-cdc-consolidate-trigger`
Expected: `Pass 1. Fail 0`.

- [ ] **Step 5: Regenerate sqlx if the trigger uses a compile-time query**

If `bump_consolidate_trigger` uses `query!`, run `tools/sqlx-prepare.sh` and commit `.sqlx/`.

- [ ] **Step 6: prek + full sweep + commit**

```bash
buck2 run //tools:prek -- run --all-files
buck2 test --console none //src/...   # full sweep, -j 8 locally
git add -A && git commit -m "feat(stream): enqueue stream_consolidate on a delta-count threshold (LOOM_CONSOLIDATE_DELTA_THRESHOLD)"
```

---

## Post-plan: registers + docs

After all tasks land (before/with the PR), run the **loom-docs-update** skill:
- Close `road-stream-pk-tables` (slice 2 fully shipped: 2a emission + 2b dual tables + compaction) — remove from `docs/ROADMAP.md`, record the shipped capability in `docs/system-capabilities/`.
- Re-check `iss-stream-log-vs-cdc-declare` (still open — unrelated to 2b).
- Note any residual (e.g. changelog GC retention rides `gc_table`; no user-facing changelog read until Slice 3 `road-stream-subscribe`).
- If the fold-precedence-via-`loom_offset` or `has_shadow`-lift decisions revealed a spec gap, add a one-line spec correction.

## Self-review notes

- **Spec coverage:** §4 dual tables → Tasks 1,2; §5 dual-write flush → Task 4; §6 LastRow compaction → Tasks 5,6,7,8; §7 reads unchanged (framing hidden) → enforced by Global Constraints + regression steps. §2's `−U` exclusion (3 read sites) is already shipped in 2a — this plan adds the changelog as the one place `−U` is retained (Task 3/4).
- **Type consistency:** `changelog_table_id: Option<i64>`, `changelog_table_ref`, `inline_live_batch_full`, `clear_has_shadow`, `consolidate_stream`, `STREAM_CONSOLIDATE_JOB_KIND`/`StreamConsolidateJob` are used with identical signatures across the tasks that produce and consume them.
- **Riskiest tasks:** Task 4 (dual-write retry/atomicity on one tx) and Task 6 (engine-side fold + framing overwrite + RPC). Both have dedicated `loom_fixture_test`s; the final whole-branch review should scrutinize the atomicity (both snapshots move together) and the framing round-trip (consolidated base still flushable).
- **Non-CDC byte-identical:** every branch is CDC-gated; Tasks 2,4,5 each carry an explicit non-CDC regression step.
