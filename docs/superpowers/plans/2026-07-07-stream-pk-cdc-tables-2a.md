# Stream engine — PK/CDC tables, Plan 2a (CDC emission) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a declared PK/CDC stream table emit the full `+I/−U/+U/−D` change sequence on the governed mutation path, with per-key (hash-on-identity) bucketing and gapless per-bucket offsets — events persisting in the existing single Iceberg table (the dual-table split + compaction is Plan 2b).

**Architecture:** Extend the `stream.stream_table` registry with a `kind` (`log`|`cdc`) and a `bucket_key` (the identity column), so every write path derives CDC bucketing from one `tid` lookup. Bucketing becomes `hash(identity) % bucket_count` for CDC tables (a key's whole history stays in one bucket). The `run_mutate` path already holds the prior merged row (`action.rs:1128`); thread it as an additive optional **before-image** through the `write_delta` RPC so postgres `write_inline_delta` can emit the `−U` before-image (update) and the `−D` with-image (delete). The `−U` row lives in the inline tier but is excluded from every current-state read.

**Tech Stack:** Rust, buck2, sqlx compile-time queries (postgres control plane), Arrow 58 / Iceberg, tonic/protobuf (engine wire), DataFusion (engine serving).

## Global Constraints

- **Tests are `rust_test` / `loom_fixture_test` integration targets only — never inline `#[cfg(test)]`.** New fixture tests MUST use the `loom_fixture_test` macro (`src/control-plane/postgres/defs.bzl`), not a bare `rust_test`, or the fixture env is missing and the test cannot boot. Wire each new test as its own target in the crate's `BUCK`, mirroring an existing sibling.
- **Build:** `buck2 build -v0 --console none //src/...` (silent on success). **Test:** `buck2 test --console none //src/...` (prints only the `Tests finished` summary). On a root cloud host, the buck2 shim injects `--unstable-allow-all-tests-on-re` automatically.
- **After changing any `query!`/`query_scalar!` SQL** (new columns on `stream.stream_table`), run `tools/sqlx-prepare.sh` and commit the `.sqlx/` change; the `//src/control-plane/postgres:sqlx-cache-check` test enforces freshness. If `initdb` refuses to run (root host) and you cannot regenerate the cache, write the new query as a runtime `sqlx::query(AssertSqlSafe(...))` against `iceberg_mirror`/`stream` (the `fixture.rs` precedent) rather than a compile-time macro — but prefer the macro + prepare.
- **Run prek before every commit:** `buck2 run //tools:prek -- run --all-files` (rustfmt is a separate hook; clippy-clean ≠ lint-clean).
- **Clippy is strict (pedantic + restriction):** no `unwrap`/`expect`/`indexing_slicing`/`panic`/`todo` in production lib/bin code. Use `.get()`/`.ok_or_else()`; `#[expect(lint, reason = "...")]` for local exceptions.
- **Non-CDC paths must stay byte-identical.** Batch tables, slice-1 log tables, and non-stream identity (cow-inline-shadow) tables must behave exactly as before this plan. Every emission/bucketing branch is gated on the registry recording `kind='cdc'`; the before-image param is additive and ignored off the CDC path.
- **Framing stays hidden.** `loom_change_kind`/`loom_bucket`/`loom_offset` are reserved (`is_reserved`, `iceberg_catalog.rs:17`); no logical read may expose them. This plan adds no new logical columns.
- **`changelog_table_id`** is added to the registry in Task 1 but stays NULL and unused in 2a — it is 2b's pointer. Do not create a changelog table in 2a.

---

### Task 1: Registry `kind` + `bucket_key` + `changelog_table_id`; `StreamTables` CDC methods

**Files:**
- Create: `src/control-plane/postgres/migrations/0037_stream_table_cdc_columns.sql`
- Modify: `src/control-plane/core/src/stream.rs` (trait + `StreamKind`/`StreamMeta`)
- Modify: `src/control-plane/core/src/lib.rs` (re-export new types)
- Modify: `src/control-plane/memory/src/stream.rs` (fake)
- Modify: `src/control-plane/memory/src/lib.rs:` the `stream_tables` field type
- Modify: `src/control-plane/postgres/src/stream.rs` (adapter + `pg_declare_cdc`/`pg_stream_meta`)
- Modify: `src/control-plane/postgres/.sqlx/` (regenerate)
- Test: `src/control-plane/testkit/src/lib.rs` (extend the `StreamTables` contract) + its `BUCK` target

**Interfaces:**
- Consumes: existing `StreamTables { declare_stream, stream_bucket_count }` (`core/src/stream.rs:22`), `pg_declare_stream`/`pg_stream_bucket_count` (`postgres/src/stream.rs:161,179`).
- Produces (later tasks rely on these EXACT names/types):
  - `pub enum StreamKind { Log, Cdc }` (core), `Serialize`/`Deserialize`/`Clone`/`Copy`/`PartialEq`/`Eq`/`Debug`.
  - `pub struct StreamMeta { pub bucket_count: i32, pub kind: StreamKind, pub bucket_key: Option<String> }` (core), same derives.
  - trait methods `async fn declare_cdc(&self, table_id: i64, bucket_count: i32, bucket_key: &str) -> Result<()>` and `async fn stream_meta(&self, table_id: i64) -> Result<Option<StreamMeta>>`.
  - `pub(crate) async fn pg_declare_cdc<'e, E: sqlx::PgExecutor<'e>>(ex: E, table_id: i64, bucket_count: i32, bucket_key: &str) -> Result<()>` and `pub(crate) async fn pg_stream_meta<'e, E: sqlx::PgExecutor<'e>>(ex: E, table_id: i64) -> Result<Option<StreamMeta>>` (`postgres/src/stream.rs`).

- [ ] **Step 1: Write the migration**

Create `src/control-plane/postgres/migrations/0037_stream_table_cdc_columns.sql`:

```sql
-- PK/CDC stream tables (stream engine slice 2). `kind` discriminates an
-- append-only log table (slice 1) from a PK/CDC table; `bucket_key` names the
-- identity column a CDC table buckets on (hash(bucket_key) % bucket_count);
-- `changelog_table_id` points at the durable changelog Iceberg table (slice 2b;
-- NULL until then).
alter table stream.stream_table
    add column kind text not null default 'log'
        check (kind in ('log', 'cdc')),
    add column bucket_key text,
    add column changelog_table_id bigint;

-- A CDC table must name its bucket key; a log table must not.
alter table stream.stream_table
    add constraint cdc_requires_bucket_key
        check ((kind = 'cdc') = (bucket_key is not null));
```

- [ ] **Step 2: Add `StreamKind`/`StreamMeta` and the trait methods (core)**

In `src/control-plane/core/src/stream.rs`, above the `StreamTables` trait, add the types and extend the trait:

```rust
use serde::{Deserialize, Serialize};

/// The flavor of a declared stream table. `Log` = append-only (slice 1);
/// `Cdc` = PK table emitting +I/−U/+U/−D on mutation (slice 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamKind {
    Log,
    Cdc,
}

/// A declared stream table's metadata: its fixed bucket count, its kind, and —
/// for a CDC table — the identity column it buckets on (`hash(bucket_key) %
/// bucket_count`). `bucket_key` is `None` for a log table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamMeta {
    pub bucket_count: i32,
    pub kind: StreamKind,
    pub bucket_key: Option<String>,
}
```

Add to the `StreamTables` trait (keep the existing two methods):

```rust
    /// Declare table_id as a PK/CDC table with bucket_count buckets keyed on
    /// `bucket_key` (the identity column). Idempotent, first-wins on all fields.
    async fn declare_cdc(&self, table_id: i64, bucket_count: i32, bucket_key: &str)
        -> Result<()>;
    /// Full stream metadata for table_id if it is a declared stream table, else None.
    async fn stream_meta(&self, table_id: i64) -> Result<Option<StreamMeta>>;
```

Re-export from `src/control-plane/core/src/lib.rs` wherever `StreamTables`/`BucketOffsets` are re-exported (add `StreamKind, StreamMeta`).

- [ ] **Step 3: Run it to make sure it fails (contract test compiles-then-fails)**

Write the failing contract extension first (Step 4), then: `buck2 test --console none //src/control-plane/testkit/...` — expect a **build** failure (`declare_cdc`/`stream_meta` not found on the fake/adapter). That is the red state.

- [ ] **Step 4: Extend the testkit contract**

In `src/control-plane/testkit/src/lib.rs`, in the existing `StreamTables` contract fn (near line 5029), add after the log-table assertions:

```rust
    // CDC declaration: kind='cdc', bucket_key recorded, idempotent first-wins.
    cp.declare_cdc(2, 4, "id").await.expect("declare cdc");
    let meta = cp.stream_meta(2).await.expect("meta").expect("declared");
    assert_eq!(meta.bucket_count, 4);
    assert_eq!(meta.kind, control_plane_core::StreamKind::Cdc);
    assert_eq!(meta.bucket_key.as_deref(), Some("id"));
    cp.declare_cdc(2, 8, "other").await.expect("idempotent redeclare no-ops");
    let meta2 = cp.stream_meta(2).await.expect("meta").expect("declared");
    assert_eq!(meta2.bucket_count, 4, "first declaration's fields stand");
    assert_eq!(meta2.bucket_key.as_deref(), Some("id"));
    // A log table reports kind=Log with no bucket_key.
    let log_meta = cp.stream_meta(1).await.expect("meta").expect("declared");
    assert_eq!(log_meta.kind, control_plane_core::StreamKind::Log);
    assert_eq!(log_meta.bucket_key, None);
    // Unknown table → None.
    assert!(cp.stream_meta(999).await.expect("meta").is_none());
```

(Table `1` is already declared as a log table earlier in this contract via `declare_stream(1, 4)`.)

- [ ] **Step 5: Implement the memory fake**

In `src/control-plane/memory/src/lib.rs`, change the `stream_tables` field from `Mutex<HashMap<i64, i32>>` to `Mutex<HashMap<i64, control_plane_core::StreamMeta>>` (import `StreamMeta`).

In `src/control-plane/memory/src/stream.rs`, rewrite the `StreamTables` impl:

```rust
use control_plane_core::{
    BucketOffsets, ControlPlaneError, Result, StreamKind, StreamMeta, StreamTables,
};

#[async_trait]
impl StreamTables for MemoryControlPlane {
    #[tracing::instrument(skip(self), level = "debug")]
    async fn declare_stream(&self, table_id: i64, bucket_count: i32) -> Result<()> {
        self.stream_tables.lock().entry(table_id).or_insert(StreamMeta {
            bucket_count,
            kind: StreamKind::Log,
            bucket_key: None,
        });
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn declare_cdc(
        &self,
        table_id: i64,
        bucket_count: i32,
        bucket_key: &str,
    ) -> Result<()> {
        self.stream_tables.lock().entry(table_id).or_insert(StreamMeta {
            bucket_count,
            kind: StreamKind::Cdc,
            bucket_key: Some(bucket_key.to_string()),
        });
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn stream_bucket_count(&self, table_id: i64) -> Result<Option<i32>> {
        Ok(self.stream_tables.lock().get(&table_id).map(|m| m.bucket_count))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn stream_meta(&self, table_id: i64) -> Result<Option<StreamMeta>> {
        Ok(self.stream_tables.lock().get(&table_id).cloned())
    }
}
```

- [ ] **Step 6: Implement the postgres adapter**

In `src/control-plane/postgres/src/stream.rs`, add the free functions and extend the impl. Add `pg_declare_cdc`:

```rust
/// Declare a PK/CDC table (idempotent, first-wins on all fields).
pub(crate) async fn pg_declare_cdc<'e, E: sqlx::PgExecutor<'e>>(
    ex: E,
    table_id: i64,
    bucket_count: i32,
    bucket_key: &str,
) -> Result<()> {
    sqlx::query!(
        "insert into stream.stream_table (table_id, bucket_count, kind, bucket_key) \
         values ($1, $2, 'cdc', $3) on conflict (table_id) do nothing",
        table_id,
        bucket_count,
        bucket_key,
    )
    .execute(ex)
    .await
    .map_err(backend)?;
    Ok(())
}

/// Full stream metadata for table_id if declared, else None.
pub(crate) async fn pg_stream_meta<'e, E: sqlx::PgExecutor<'e>>(
    ex: E,
    table_id: i64,
) -> Result<Option<StreamMeta>> {
    let row = sqlx::query!(
        "select bucket_count, kind, bucket_key from stream.stream_table where table_id = $1",
        table_id,
    )
    .fetch_optional(ex)
    .await
    .map_err(backend)?;
    Ok(row.map(|r| {
        let kind = if r.kind == "cdc" { StreamKind::Cdc } else { StreamKind::Log };
        StreamMeta { bucket_count: r.bucket_count, kind, bucket_key: r.bucket_key }
    }))
}
```

Import `StreamKind, StreamMeta` at the top. Extend the `StreamTables for PgControlPlane` impl with `declare_cdc` (→ `pg_declare_cdc(self.pool(), ...)`) and `stream_meta` (→ `pg_stream_meta(self.pool(), ...)`).

- [ ] **Step 7: Regenerate the sqlx cache**

Run `tools/sqlx-prepare.sh` and stage `src/control-plane/postgres/.sqlx/`. (If `initdb` refuses to run as root, rewrite `pg_declare_cdc`/`pg_stream_meta` as `sqlx::query(AssertSqlSafe(...))` runtime queries reading typed columns via `try_get`, per the Global Constraints fallback.)

- [ ] **Step 8: Run the contract + adapters green**

Run: `buck2 test --console none //src/control-plane/testkit/... //src/control-plane/memory/... //src/control-plane/postgres:sqlx-cache-check`
Expected: `Tests finished: Pass N. Fail 0`.

- [ ] **Step 9: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/control-plane migrations
git commit -m "feat(stream): add kind/bucket_key/changelog_table_id to stream registry + declare_cdc/stream_meta"
```

---

### Task 2: `cdc_bucket` helper + CDC-aware `inline_append` bucketing

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_inline.rs` (add `cdc_bucket`; CDC branch in `inline_append`)
- Test: `src/control-plane/postgres/tests/stream_cdc_bucket.rs` (new `loom_fixture_test`) + `BUCK` target

**Interfaces:**
- Consumes: `advisory_key_for_id`-style hashing (`iceberg_inline.rs:778`), `Cell` (`iceberg_inline.rs:154`), `pg_stream_meta`/`StreamMeta`/`StreamKind` (Task 1), `pg_allocate_offset` (`stream.rs:103`), `reconcile_stream_mode` (`stream.rs:24`), `inline_append`'s stream branch (`iceberg_inline.rs:526-596`).
- Produces: `fn cdc_bucket(id: &Cell, bucket_count: i32) -> Result<i32>` returning a bucket in `0..bucket_count`.

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/postgres/tests/stream_cdc_bucket.rs`. Declare a CDC table (via `declare_cdc`), append rows for two distinct identities across two separate appends, and assert each identity's rows all carry the SAME `loom_bucket` and that bucket is `< bucket_count`, with gapless per-bucket offsets. (Mirror the setup in the existing `stream_flush_persist.rs` / `stream_reserved_schema.rs` fixture tests: land a small batch, then read `loom_bucket`/`loom_offset` from `inline_<tid>` directly.)

```rust
// Pseudocode shape — follow the existing stream fixture test's boot/land helpers.
// 1. declare_cdc(tid, 4, "id"); land two rows id=1,id=2 (append A), then id=1,id=3 (append B).
// 2. SELECT "id", loom_bucket, loom_offset FROM inline_<tid> ORDER BY loom_offset.
// 3. assert all rows with id=1 share one bucket; id=2/id=3 buckets are in 0..4;
//    per-bucket loom_offset is gapless from 0.
```

- [ ] **Step 2: Run it to verify it fails**

Run: `buck2 test --console none //src/control-plane/postgres:stream-cdc-bucket`
Expected: FAIL — id=1's two rows land in DIFFERENT buckets (today's `row_index % bc`), not the same.

- [ ] **Step 3: Add the `cdc_bucket` helper**

In `src/control-plane/postgres/src/iceberg_inline.rs`, near `advisory_key_for_id`:

```rust
/// The bucket for one identity of a CDC table: `stable_hash(id) % bucket_count`,
/// so a key's whole change history (+I/−U/+U/−D) stays in one bucket (LastRow
/// merge and per-key ordering depend on it). Reuses `advisory_key_for_id`'s
/// deterministic, `rand`-free hash family; the modulus is taken on the unsigned
/// value so the result is always in `0..bucket_count`.
fn cdc_bucket(id: &Cell, bucket_count: i32) -> Result<i32> {
    if bucket_count < 1 {
        return Err(ControlPlaneError::Validation(format!(
            "cdc bucket_count must be >= 1, got {bucket_count}"
        )));
    }
    // advisory_key_for_id returns an i64 already tagged-per-variant; take it as u64
    // and mod by bucket_count. `as u64` reinterprets the bits (no sign bias), and
    // `% bucket_count` (bucket_count >= 1) yields 0..bucket_count.
    let h = advisory_key_for_id(0, id) as u64;
    let bc = u64::try_from(bucket_count).map_err(|e| {
        ControlPlaneError::Backend(format!("invalid bucket_count {bucket_count}: {e}").into())
    })?;
    let b = (h % bc) as i32;
    Ok(b)
}
```

(Note `advisory_key_for_id(0, id)` — the `tid` argument only salts the advisory-lock keyspace; passing a fixed `0` gives a stable per-identity bucket independent of table. Buckets need only be stable and well-distributed, which the identity hash gives.)

- [ ] **Step 4: Route the CDC bucketing in `inline_append`**

In `inline_append` (`iceberg_inline.rs`), the stream branch is currently `if let Some(bc) = effective { ... row % bc ... }` (lines 526-596). Replace the per-row bucket computation with a kind-aware one. Fetch the stream meta once (before the loop):

```rust
    if let Some(bc) = effective {
        let bc_usize = usize::try_from(bc).map_err(|e| {
            ControlPlaneError::Backend(format!("invalid stream bucket count {bc}: {e}").into())
        })?;
        // CDC tables bucket by hash(identity); log tables by row_index % bc.
        let meta = crate::stream::pg_stream_meta(&mut *conn, tid).await?;
        let cdc_key: Option<String> = match &meta {
            Some(m) if m.kind == control_plane_core::StreamKind::Cdc => m.bucket_key.clone(),
            _ => None,
        };
        // Compute each row's bucket up front (so offset runs can be reserved per bucket).
        let mut row_bucket = vec![0usize; batch.num_rows()];
        for row in 0..batch.num_rows() {
            let b = match &cdc_key {
                Some(key) => {
                    let idx = columns.iter().position(|c| &c.name == key).ok_or_else(|| {
                        ControlPlaneError::Backend(
                            format!("cdc bucket_key `{key}` not in appended columns").into(),
                        )
                    })?;
                    let spec = columns.get(idx).ok_or_else(|| {
                        ControlPlaneError::Backend("bucket_key column index out of range".into())
                    })?;
                    let cell = cell_from_arrow(batch, idx, row, &spec.ty)?;
                    usize::try_from(cdc_bucket(&cell, bc)?).map_err(|e| {
                        ControlPlaneError::Backend(format!("bucket overflow: {e}").into())
                    })?
                }
                None => row % bc_usize,
            };
            let slot = row_bucket.get_mut(row).ok_or_else(|| {
                ControlPlaneError::Backend("row index out of range".into())
            })?;
            *slot = b;
        }
        // ... reserve counts/cursor per bucket using row_bucket[row] instead of `row % bc_usize`,
        //     and in the insert loop read the bucket from row_bucket[row]. (Replace the two
        //     `let b = row % bc_usize;` sites with `let b = *row_bucket.get(row)...;`.)
```

Keep the rest of the reserve/cursor/insert machinery (lines 536-596) unchanged except that `b` now comes from `row_bucket[row]` at both the counting loop and the insert loop.

- [ ] **Step 5: Run the test green**

Run: `buck2 test --console none //src/control-plane/postgres:stream-cdc-bucket`
Expected: PASS. Also run `//src/control-plane/postgres:stream-flush-persist` and `:stream-reserved-schema` to confirm log-table bucketing is unchanged.

- [ ] **Step 6: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/control-plane/postgres
git commit -m "feat(stream): hash-on-identity bucketing for CDC tables in inline_append"
```

---

### Task 3: Exclude `−U` rows from every current-state read

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_inline.rs` (`inline_live_batch` ~1116, `read_max_version` ~759)
- Test: `src/control-plane/postgres/tests/stream_cdc_minus_u_hidden.rs` (new `loom_fixture_test`) + `BUCK`

**Interfaces:**
- Consumes: `inline_live_batch` (`iceberg_inline.rs:1116`), `read_max_version` (`iceberg_inline.rs:739`), `mvcc_live_pred` (`iceberg_inline.rs:56`).
- Produces: both live reads exclude physical rows where `loom_change_kind = '-U'`.

- [ ] **Step 1: Write the failing test**

Create `stream_cdc_minus_u_hidden.rs`: on a CDC table with one live row for `id=1`, manually `INSERT` a `loom_change_kind='-U'` row for `id=1` into `inline_<tid>` (a before-image), then assert `inline_live_batch` / a merged `GET`-style read still returns exactly ONE row for `id=1` (the `+U`/`+I` current row), never the `−U` image, and that `read_max_version` is unchanged by the `−U` row.

- [ ] **Step 2: Run it to verify it fails**

Run: `buck2 test --console none //src/control-plane/postgres:stream-cdc-minus-u-hidden`
Expected: FAIL — the `−U` row currently appears as a live row / competes in the merge.

- [ ] **Step 3: Add the exclusion predicate**

In `read_max_version` (`iceberg_inline.rs:759`), change the SELECT's predicate to also exclude `−U`:

```rust
    let sql = format!(
        "select coalesce(max(begin_snapshot), 0) as v from {} \
         where \"{}\" = $1 and end_snapshot is null \
           and (loom_change_kind is null or loom_change_kind <> '-U')",
        inline_table_name(tid),
        id_column.replace('"', "\"\""),
    );
```

In `inline_live_batch` (`iceberg_inline.rs:1116`), find the SELECT over `inline_<tid>` that applies the MVCC live predicate and append the same clause: `and (loom_change_kind is null or loom_change_kind <> '-U')`. (The `loom_change_kind is null` disjunct keeps batch/non-CDC rows — which may predate the column's default — live.)

- [ ] **Step 4: Run the test green**

Run: `buck2 test --console none //src/control-plane/postgres:stream-cdc-minus-u-hidden`
Expected: PASS. Also run `//src/control-plane/postgres:cow-inline-shadow`-style existing merge tests (grep the crate's `BUCK` for the inline-delta/merge test targets) to confirm `+U`/`−D` merge behavior is unchanged.

- [ ] **Step 5: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/control-plane/postgres
git commit -m "feat(stream): exclude -U before-image rows from current-state reads"
```

---

### Task 4: Thread an optional before-image through the `write_delta` RPC (plumbing; postgres ignores it)

**Files:**
- Modify: the engine protobuf (`WriteDeltaRequest`) — find via `grep -rn "WriteDeltaRequest" src/services/*/proto src/services/engine-wire` (the `.proto` under the engine-wire cell)
- Modify: `src/services/query-api/src/serving.rs:383` (`ActionEngine::write_delta` trait sig)
- Modify: `src/services/query-api/src/engine_action_client.rs:174` (encode before-image)
- Modify: `src/services/engine-wire/src/client.rs:319` (forward)
- Modify: `src/services/engine-serving/src/action_writer.rs` (`write_delta` — forward the before batch)
- Modify: `src/services/engine/src/service.rs:445` (decode before-image)
- Modify: `src/control-plane/postgres/src/iceberg_inline.rs` (`write_inline_delta:903` — accept `before: Option<&RecordBatch>`, ignore it for now)
- Modify: `src/services/query-api/src/action.rs:1154` (`run_mutate` passes `before`)
- Also update the in-process `ActionEngine` stubs / other `write_delta` impls (`serving.rs:383` default, `engine/src/service.rs:445`) and any callers surfaced by the compiler.
- Test: existing mutation e2e must stay green (non-regression) — no new test in this task.

**Interfaces:**
- Produces (later tasks consume):
  - `pub struct BeforeImage<'a> { pub columns: &'a [String], pub values: &'a [SqlValue], pub logical_types: &'a [String] }` in `serving.rs` (re-exported as needed). `None` off the CDC update/delete path.
  - `ActionEngine::write_delta(&self, table, id_column, tombstone, columns, values, logical_types, before: Option<BeforeImage<'_>>, event, expected_version)`.
  - proto `WriteDeltaRequest` gains `bytes before_ipc = N;` and `string before_columns_json = N+1;` (empty ⇒ absent).
  - `iceberg_inline::write_inline_delta(pool, table, columns, id_column, tombstone, batch, before: Option<(&[ColumnSpec], &RecordBatch)>, lineage, expected_version)` — the before-image carries its OWN `ColumnSpec`s, positionally aligned with its batch (do NOT reuse `full_cols` for the before-image row: `full_cols`' order need not match the before-batch's column order, which would bind cells to the wrong columns).

- [ ] **Step 1: Add the proto fields**

In the engine-wire `.proto`, add to `WriteDeltaRequest` two new fields (next free tags): `bytes before_ipc` and `string before_columns_json`. Rebuild to regenerate the Rust types (`buck2 build -v0 --console none` the engine-wire target). An empty `before_ipc` means "no before-image" (the non-CDC and insert cases).

- [ ] **Step 2: Extend the `ActionEngine::write_delta` trait**

In `serving.rs`, define `BeforeImage` (above the trait) and add the `before: Option<BeforeImage<'_>>` parameter to the trait method (before `event`). Update the default impl signature (it still errors). Keep the `#[allow(clippy::too_many_arguments, reason = ...)]`.

- [ ] **Step 3: Encode the before-image in the wire client**

In `engine_action_client.rs:174`, after building the main `ipc`/`columns_json`, build the before-image pair when `before` is `Some`:

```rust
        let (before_ipc, before_columns_json) = match before {
            Some(b) => {
                let (_schema, batch, specs) =
                    build_object_batch(b.columns, b.values, b.logical_types)?;
                (encode_ipc_stream(&batch)?, serde_json::to_string(&specs).map_err(to_serving)?)
            }
            None => (Vec::new(), String::new()),
        };
```

Pass `before_ipc`, `before_columns_json` into the `self.ctl.write_delta(...)` wire call (add the two args to the wire client method in `engine-wire/src/client.rs` and forward into the generated request struct).

- [ ] **Step 4: Forward through engine-serving + decode in the engine service**

In `engine-serving/src/action_writer.rs` `write_delta`, add a `before_ipc: &[u8]` and `before_columns_json: &str` parameter pair; when non-empty, decode the columns (`serde_json::from_str::<Vec<ColumnSpec>>`) and the batch (`datafusion_io::decode_ipc`, taking the single batch), and pass `Some((&before_columns, &before_batch))` to `iceberg_inline::write_inline_delta` (else `None`).

In `engine/src/service.rs:445`, decode `r.before_ipc`/`r.before_columns_json` (empty ⇒ `None`) and pass them to `self.writer.write_delta(...)`.

- [ ] **Step 5: Accept (and ignore) `before` in `write_inline_delta`**

In `iceberg_inline.rs:903`, add `before: Option<(&[ColumnSpec], &RecordBatch)>` to `write_inline_delta` (before `lineage`). Do NOT use it yet — Task 5 adds the CDC logic. Update the `#[allow(clippy::too_many_arguments)]` reason if needed. Thread `None`/`Some` from `action_writer.rs`.

- [ ] **Step 6: Pass the before-image from `run_mutate`**

In `action.rs:1154`, both `write_delta` calls gain a `before` arg. For BOTH update and delete, pass the full current image:

```rust
        let before = Some(crate::serving::BeforeImage {
            columns: &columns,        // full ordered property set (line 1055)
            values: &existing,        // the merged prior row (line 1128)
            logical_types: &logical,  // line 1056
        });
```

Pass `before.clone()`/`before` into each `write_delta(...)` call (insert the arg before `event.clone()`). (`existing` is the `Vec<SqlValue>` current image; `columns`/`logical` are the full property lists already in scope.)

- [ ] **Step 7: Build everything + run the existing mutation e2e green**

Run: `buck2 build -v0 --console none //src/...` then `buck2 test --console none //src/services/query-api:cow-inline-shadow-gov-e2e` (and any other update/delete action e2e targets — grep the query-api `BUCK` for `mutate`/`cow`/`delete`).
Expected: PASS — non-CDC update/delete unchanged; the before-image rides the wire but postgres ignores it.

- [ ] **Step 8: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/services src/control-plane/postgres
git commit -m "feat(stream): thread optional before-image through the write_delta RPC (unused)"
```

---

### Task 5: Emit `−U/+U/−D` with bucketing + offsets in `write_inline_delta` (CDC path)

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_inline.rs` (`write_inline_delta:903-1010`)
- Test: `src/control-plane/postgres/tests/stream_cdc_emission.rs` (new `loom_fixture_test`) + `BUCK`

**Interfaces:**
- Consumes: `pg_stream_meta`/`StreamMeta`/`StreamKind` (Task 1), `cdc_bucket` (Task 2), `pg_allocate_offset` (`stream.rs:103`), `extract_id_cell`/`cell_from_arrow`/`bind_cell` (`iceberg_inline.rs`), `before: Option<&RecordBatch>` (Task 4).
- Produces: on a `kind='cdc'` table, an update writes `(−U before-image, +U after-image)` at consecutive offsets in `hash(id)%bc`; a delete writes `−D` with the before-image at one offset; all `loom_bucket`/`loom_offset` stamped.

- [ ] **Step 1: Write the failing test**

Create `stream_cdc_emission.rs`: declare a CDC table (`declare_cdc(tid, 2, "id")`), land an initial row `id=1`, then drive an update and a delete of `id=1` through `write_inline_delta` (pass the before-image batch). Assert, by reading `inline_<tid>` directly ordered by `loom_offset` within `id=1`'s bucket:
- after the update: a `-U` row (before-image values) and a `+U` row (after-image values) exist, with consecutive `loom_offset` (`-U` first), same `loom_bucket = cdc_bucket(id=1, 2)`;
- after the delete: a `-D` row carrying the FULL prior image (not NULLs) with `loom_tombstone=true`, at the next offset;
- a current-state merged read returns the updated row (then, post-delete, no row) and never surfaces the `-U` row.

- [ ] **Step 2: Run it to verify it fails**

Run: `buck2 test --console none //src/control-plane/postgres:stream-cdc-emission`
Expected: FAIL — today no `-U` row is written, offsets are NULL, and the tombstone carries NULLs.

- [ ] **Step 3: Implement the CDC emission branch**

In `write_inline_delta` (`iceberg_inline.rs`), after the CAS passes and `at` is allocated (line 955), branch on the registry meta. Add before the existing `if tombstone { ... } else { ... }`:

```rust
    let meta = crate::stream::pg_stream_meta(&mut *tx, tid).await?;
    let cdc = matches!(&meta, Some(m) if m.kind == control_plane_core::StreamKind::Cdc);

    if cdc {
        let m = meta.as_ref().ok_or_else(|| {
            ControlPlaneError::Backend("cdc meta vanished after check".into())
        })?;
        let bucket = cdc_bucket(&id_cell, m.bucket_count)?;
        // The before-image carries its OWN columns (positionally aligned with its
        // batch) — never `full_cols`, whose order need not match.
        let (before_cols, before_batch) = before.ok_or_else(|| {
            ControlPlaneError::Backend("cdc mutation requires a before-image".into())
        })?;
        if tombstone {
            // Delete → one -D carrying the FULL prior image, so the changelog event is
            // complete. loom_tombstone=true still hides the base row in merge-on-read.
            let off = crate::stream::pg_allocate_offset(&mut *tx, tid, bucket, 1).await?;
            write_cdc_row(&mut tx, tid, at, "-D", true, bucket, off, before_cols, before_batch).await?;
        } else {
            // Update → adjacent (-U before-image, +U after-image), -U first.
            // -U uses the before-image (cols+batch); +U uses the caller's after-image.
            let first = crate::stream::pg_allocate_offset(&mut *tx, tid, bucket, 2).await?;
            write_cdc_row(&mut tx, tid, at, "-U", false, bucket, first, before_cols, before_batch).await?;
            write_cdc_row(&mut tx, tid, at, "+U", false, bucket, first + 1, columns, batch).await?;
        }
    } else if tombstone {
        // ... EXISTING non-CDC tombstone insert (unchanged) ...
    } else {
        // ... EXISTING non-CDC version insert (unchanged) ...
    }
```

Add a private helper `write_cdc_row` that inserts one framed inline row (begin_snapshot, loom_tombstone, loom_change_kind, loom_bucket, loom_offset, + the data columns from `cols`/`batch` row 0):

```rust
#[allow(clippy::too_many_arguments, reason = "one framed inline-row insert; a struct would obscure the two call sites")]
async fn write_cdc_row(
    tx: &mut sqlx::PgConnection,
    tid: i64,
    at: SnapshotId,
    change_kind: &str,       // '-U' | '+U' | '-D'
    tombstone: bool,
    bucket: i32,
    offset: i64,
    cols: &[ColumnSpec],
    batch: &RecordBatch,
) -> Result<()> {
    let col_list = cols
        .iter()
        .map(|c| format!("\"{}\"", c.name.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ");
    // $1 begin_snapshot, $2 tombstone, $3 change_kind, $4 bucket, $5 offset, then data from $6.
    let placeholders = (0..cols.len()).map(|i| format!("${}", i + 6)).collect::<Vec<_>>().join(", ");
    let sql = format!(
        "insert into {} (begin_snapshot, loom_tombstone, loom_change_kind, loom_bucket, loom_offset, {col_list}) \
         values ($1, $2, $3, $4, $5, {placeholders})",
        inline_table_name(tid),
    );
    let cells = cols
        .iter()
        .enumerate()
        .map(|(c, spec)| cell_from_arrow(batch, c, 0, &spec.ty))
        .collect::<Result<Vec<_>>>()?;
    let mut q = sqlx::query(AssertSqlSafe(sql))
        .bind(at.0)
        .bind(tombstone)
        .bind(change_kind)
        .bind(bucket)
        .bind(offset);
    for cell in &cells {
        q = bind_cell(q, cell);
    }
    q.execute(&mut *tx).await.map_err(backend)?;
    Ok(())
}
```

Note: the `-D` row uses `full_cols` + the `before` batch so it carries every column (the full prior image); the `+U` row uses the caller's `columns`/`batch` (after-image); the `-U` row uses `full_cols` + `before`. Ensure `before`'s batch schema names match `full_cols` (the before-image is the full property set from `run_mutate`).

- [ ] **Step 4: Run the test green**

Run: `buck2 test --console none //src/control-plane/postgres:stream-cdc-emission`
Expected: PASS.

- [ ] **Step 5: Run the non-CDC regression set**

Run: `buck2 test --console none //src/services/query-api:cow-inline-shadow-gov-e2e //src/control-plane/postgres:stream-flush-persist`
Expected: PASS (non-CDC update/delete and log-table flush unchanged).

- [ ] **Step 6: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/control-plane/postgres
git commit -m "feat(stream): emit -U/+U/-D CDC events with hash bucketing and offsets"
```

---

### Task 6: `mode=cdc` declaration on the model/bind surface

**Files:**
- Modify: `src/services/ingest/src/http.rs` (`land_model` ~239-320; `ModelParams` ~201; thread the declaration)
- Modify: `src/services/ingest/src/landing.rs` (`LandRequest` — carry a CDC declaration) and `src/control-plane/postgres/src/iceberg_landing.rs` (`land`/`reconcile_stream_mode` call — CDC-aware) as needed to thread `kind`+`bucket_key`.
- Test: `src/services/ingest/tests/model_cdc_declare.rs` (new `loom_fixture_test`) + `BUCK`

**Interfaces:**
- Consumes: `declare_cdc`/`pg_declare_cdc` (Task 1), `reconcile_stream_mode` (`stream.rs:24`), the model land path (`land_model` → `land`), `ObjectType.identity`.
- Produces: `POST /models/{type}?mode=cdc&buckets=N` declares a `kind='cdc'` `stream_table` row with `bucket_key` = the type's identity, in the table-creation transaction; `400` when the type has no identity, when `mode=cdc` targets an existing log/batch table, or on a bucket-count conflict.

- [ ] **Step 1: Write the failing test**

Create `model_cdc_declare.rs` (`loom_fixture_test`, ingest fixture):
- `POST /models/{type}?mode=cdc&buckets=2` for a type WITH an identity → 2xx; assert `stream_meta(tid)` is `Some { kind: Cdc, bucket_count: 2, bucket_key: Some("<identity>") }`.
- Same call for a type with NO identity → `400`.
- `mode=cdc` against a table already landed as batch (or `mode=stream` log) → `400`.

- [ ] **Step 2: Run it to verify it fails**

Run: `buck2 test --console none //src/services/ingest:model-cdc-declare`
Expected: FAIL — `land_model` currently never declares stream intent (`http.rs:357`).

- [ ] **Step 3: Parse `mode=cdc` and enforce the identity requirement**

In `http.rs`, add `mode`/`buckets` to `ModelParams` (mirror the dataset `StreamParams` at line 214). In `land_model`, after the `ObjectType` is resolved/inferred and before the land call:

```rust
    // `?mode=cdc&buckets=N` declares this type's table as a PK/CDC stream table on
    // first creation. Requires a declared identity (the bucket key); immutable after.
    let cdc_decl = if q.mode.as_deref() == Some("cdc") {
        let n = q.buckets.unwrap_or(1);
        if n < 1 {
            return Err(ApiError::BadRequest(Cow::Borrowed("buckets must be >= 1")));
        }
        let identity = object_type.identity.clone().ok_or_else(|| {
            ApiError::BadRequest(Cow::Borrowed(
                "mode=cdc requires the type to declare an identity property",
            ))
        })?;
        Some((n, identity))
    } else {
        None
    };
```

- [ ] **Step 4: Thread the CDC declaration into the land transaction**

Extend `LandRequest` (`landing.rs`) and `iceberg_landing::land` to accept an optional CDC declaration `Option<CdcDecl { buckets: i32, bucket_key: String }>` alongside the existing `stream_buckets`. In the land path, where `reconcile_stream_mode` runs (`iceberg_inline.rs:515` / `iceberg_landing.rs:829`), when a CDC declaration is present call `pg_declare_cdc(&mut *conn, tid, buckets, &bucket_key)` on a brand-new table (mirroring `reconcile_stream_mode`'s `(Some(n), None)` fresh-declare arm, including the `pre_existing` batch→stream rejection and the post-declare bucket-count re-read/mismatch guard). A `mode=cdc` against a `pre_existing` table → `Validation` (400); a bucket mismatch vs an existing stream row → `Conflict` (400). Reuse the existing rejection helpers so the CDC and log declares cannot diverge.

(Implementation note: the cleanest shape is to widen `reconcile_stream_mode`'s `stream_buckets: Option<i32>` into an enum `StreamDecl { None, Log(i32), Cdc { buckets: i32, bucket_key: String } }` threaded from `LandRequest`; the `Cdc` arm calls `pg_declare_cdc` where the `Log` arm calls `pg_declare_stream`. Keep both arms' rejection logic identical.)

- [ ] **Step 5: Run the test green**

Run: `buck2 test --console none //src/services/ingest:model-cdc-declare`
Expected: PASS. Also run the slice-1 declaration e2e (`grep BUCK` for the `mode=stream` declaration test) to confirm log-table declaration is unchanged.

- [ ] **Step 6: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/services/ingest src/control-plane/postgres
git commit -m "feat(stream): mode=cdc declaration on POST /models/{type} (requires identity)"
```

---

### Task 7: End-to-end CDC lifecycle (governed insert/update/delete)

**Files:**
- Test: `src/services/query-api/tests/stream_cdc_e2e.rs` (new `loom_fixture_test`, `use e2e_support::{...}`) + `BUCK` (`deps += ":e2e-support"`)

**Interfaces:**
- Consumes: the whole 2a stack (declaration via `mode=cdc`, governed insert/update/delete actions, `stream_meta`), the `e2e-support` seed/router helpers.

- [ ] **Step 1: Write the end-to-end test**

Create `stream_cdc_e2e.rs`: declare a CDC type (`mode=cdc&buckets=2`), then via the governed action router:
1. insert `id=1` → assert a `+I` inline row with `loom_bucket = hash(1)%2`, `loom_offset=0` in that bucket;
2. update `id=1` → assert a `(−U,+U)` pair at the next two offsets in the same bucket;
3. delete `id=1` → assert a `−D` (full prior image, `loom_tombstone=true`) at the next offset;
4. a `GET /objects/{type}` read at each stage returns the correct current state and NEVER exposes any `loom_*` column or a `−U` row;
5. all four events for `id=1` share one bucket with gapless offsets `0..4`.

(Read the inline framing by querying `inline_<tid>` directly through the fixture's pool, as the postgres-crate stream tests do; assert the read-side invisibility through the `GET` router path.)

- [ ] **Step 2: Run it**

Run: `buck2 test --console none //src/services/query-api:stream-cdc-e2e`
Expected: PASS (all prior tasks landed).

- [ ] **Step 3: Full regression sweep**

Run: `buck2 test --console none //src/...` (locally add `-j 8` to avoid starving the 8 postgres boot slots).
Expected: `Tests finished: Pass N. Fail 0`.

- [ ] **Step 4: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/services/query-api
git commit -m "test(stream): end-to-end CDC insert/update/delete lifecycle"
```

---

## Notes for Plan 2b (not built here)

2b creates the changelog Iceberg table at declaration (fills `changelog_table_id`), dual-writes flush (base + changelog atomically via the `TxCommitCatalog` seam from slice 1b), and adds the `stream_consolidate` LastRow compaction worker job. It will be planned against 2a's landed interfaces (`stream_meta`, `write_cdc_row`, the CDC emission branch).
