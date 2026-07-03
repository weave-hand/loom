# Scalable COW slice 1 — inline-shadow merge-on-read + CAS guard — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a governed UPDATE/DELETE cost **O(change)** — write one tiny inline delta (a row-version or a tombstone) per mutation and merge-on-read, instead of rewriting the whole table's Parquet, with a per-identity compare-and-swap guarding the read→commit window.

**Architecture:** The inline MVCC tier (`iceberg_mirror.inline_<tid>`) gains a `loom_tombstone` column and now carries three row kinds keyed by the type's **identity**: append (new object), version (an UPDATE — new row, same identity, later `begin_snapshot`, full values), tombstone (a DELETE — `loom_tombstone=true`). The engine read path (`build_serving_provider`) resolves the identity column from the ontology (same Postgres pool) and replaces the additive `UNION ALL` with an **identity-dedup merge-on-read** (max `begin_snapshot` wins per identity, file rows rank below any inline delta, a tombstone winner hides the id) — running **below** the governed layer so ACL is unaffected. `run_mutate` (query-api) stops calling `overwrite_table` for identity types and instead captures a per-identity version token, resolves+governs the single object, and commits **one inline delta** through two new `EngineControl` gRPC methods (`current_inline_version`, `write_delta`); `write_delta` holds a per-identity advisory lock and aborts (retryable `Conflict`) if a newer version for that identity appeared. The byte-trigger flush is suppressed for tables that have taken a mutation (`has_shadow` marker) to avoid duplicating the shadowed file row.

**Tech Stack:** Rust, buck2, DataFusion 54, Apache Iceberg (git pin), Postgres (sqlx — inline SQL is runtime `AssertSqlSafe`, fixed-table SQL is compile-time `query!` with a committed `.sqlx` cache), tonic/prost gRPC (`EngineControl`), Arrow Flight (reads), `loom_fixture_test` (hermetic Postgres e2e).

## Global Constraints

- **Tests are `rust_test`/`loom_fixture_test` targets only — never inline `#[cfg(test)]`.** Any fixture-backed test (boots hermetic Postgres) MUST be wired with the `loom_fixture_test` macro, not a bare `rust_test`, or it routes to remote execution and fails as root. (`prek` `no-inline-tests` hook enforces no `#[test]` in `src/**`.)
- **Clippy is strict** (`pedantic` + `restriction` groups enforced on prod code): no `unwrap`/`expect`/`panic`/`todo`/`indexing_slicing`/`dbg`; carry source errors (no `map_err_ignore`). Use `#[expect(lint, reason = "…")]` locally when unavoidable. Test code is exempted from panic-safety lints via the `loom_fixture_test`/`loom_rust_test` wrapper.
- **Do NOT change `overwrite_parquet_snapshot` / `overwrite_table` behavior** — transform's replace path still uses it. The whole-table COW primitive stays; only the *mutation* path (`run_mutate`) stops calling it for identity types.
- **`begin_snapshot` is a monotonic Postgres sequence** (`iceberg_mirror.snapshot_seq`, values ≥ 1), allocated by `next_snapshot(conn, None)` (`iceberg_mirror.rs:46-65`). File-tier rows have no per-row `begin_snapshot`; they are given synthetic precedence `0` at merge time, so any inline delta (`begin_snapshot ≥ 1`) shadows a file row for the same identity.
- **MVCC visibility predicate is unchanged:** `begin_snapshot <= q AND (end_snapshot IS NULL OR end_snapshot > q)`. Inline versions/tombstones are **never end-capped** in slice 1 (consolidation is slice 2); time-travel stays correct because the dedup runs over the already-visible set.
- **Cloud/build discipline:** build with `buck2 build -M none //src/...` scoped to touched targets; never a bare whole-tree build/test (ENOSPC). Do NOT pipe `buck2 test`/`bxl` through `tail`/`head` — redirect to a file and grep. `buck2 clean` between heavy phases.
- **This environment cannot regenerate the `.sqlx` cache.** `tools/sqlx-prepare.sh` fails here (`initdb` refuses to run as root; the `loom_fixture_test` harness has a workaround, the standalone script does not). Therefore **all new SQL in this plan uses runtime `sqlx::query(AssertSqlSafe(...))` / `query_scalar(AssertSqlSafe(...))`** (needs no cache), and **no new `query!`/`query_scalar!` is added against any fixed table** — so the committed `src/control-plane/postgres/.sqlx/` stays byte-identical and `//src/control-plane/postgres:sqlx-cache-check` passes unchanged. New tables/columns are added via migration (applied fresh by the fixture harness + CI) but accessed only through `AssertSqlSafe`. Each such site carries a `// AssertSqlSafe: <static SQL>; sqlx regen unavailable in this env (initdb-as-root)` comment. (Existing `query!` calls are left untouched.)
- **`loom_fixture_test` targets run locally in this session** (verified: `//src/control-plane/postgres:iceberg-inline` passes as root), so TDD via fixture tests works here and in CI.

---

## File Structure

- `src/control-plane/postgres/src/iceberg_inline.rs` — add `loom_tombstone` to the inline DDL + an idempotent ensure-column ALTER; new `current_inline_version()` and `write_inline_delta()` (per-identity advisory-lock CAS delta write); flush-enqueue suppression.
- `src/control-plane/postgres/migrations/0016_inline_shadow.sql` — new: `has_shadow` column on `iceberg_mirror.inline_trigger`.
- `src/control-plane/postgres/src/iceberg_mirror.rs` — `set_has_shadow()` / `has_shadow()` accessors (compile-time `query!` on `inline_trigger`).
- `src/control-plane/postgres/src/ontology.rs` (or a new small module) — `identity_for_table(pool, &TableRef) -> Option<String>` reverse lookup (compile-time `query!` on `ontology.object_type`).
- `src/control-plane/postgres/src/iceberg_flush.rs` — early-return in `flush_locked` when `has_shadow`.
- `src/services/engine-serving/src/serving.rs` — identity-aware merge-on-read in `build_serving_provider`; extend `build_inline_provider` to expose `begin_snapshot`+`loom_tombstone` for the merge.
- `src/services/engine-serving/src/action_writer.rs` — `current_inline_version()` + `write_delta()` executor methods on `IcebergActionWriter`.
- `src/services/engine-wire/proto/engine_control.proto` — 2 new RPCs + messages.
- `src/services/engine-wire/src/client.rs` — 2 new `GrpcQueueClient` methods.
- `src/services/engine/src/service.rs` — 2 new `EngineControlService` handlers.
- `src/services/query-api/src/serving.rs` — `ActionEngine` trait: 2 new methods; `ServingError::Conflict` variant.
- `src/services/query-api/src/engine_action_client.rs` — `EngineActionClient` impls of the 2 methods (map gRPC `Aborted` → `ServingError::Conflict`).
- `src/services/query-api/src/action.rs` — `run_mutate` rewrite (O(change) + bounded retry); new `select_object_sql()`.
- `src/services/query-api/tests/*.rs` + `src/services/query-api/BUCK` — new `loom_fixture_test` targets for the 7 spec cases + flush-suppression.
- `src/control-plane/postgres/tests/*.rs` + `src/control-plane/postgres/BUCK` — postgres-level fixture tests for the delta write + CAS + merge helpers.
- `docs/ROADMAP.md` / `docs/FUTURE.md` — close `road-cow-inline-shadow`, note reconciliations (via `loom-docs-update`).

## Reconciliation notes (spec vs. current code — read before starting)

The spec's "Current state" file:line refs are slightly stale. Ground truth (verified):
- The whole-table COW read-modify-overwrite lives in **`query-api::action.rs::run_mutate` (l.698-829)**, NOT `engine-serving::action_writer::write_object`. `action_writer::write_object` (l.57) is the *insert* wrapper; the overwrite is `action_writer::overwrite_table` (l.83).
- The additive union to replace is at **`engine-serving/src/serving.rs:126-140`** (`DataFrame::union` at `:133`), inside `build_serving_provider` (l.71-143).
- **All UPDATE/DELETE targets already require an `identity`** (`check_mutate_conformance` rejects identity-less mutations, `action.rs:259-315`). So the O(change) path is *the* mutation path; the spec's "identity-less keep whole-table COW" is unreachable for mutations. **Spec test 7 is reconciled to: a mutation on an identity-less type is rejected at conformance (`Misconfigured` → 500), not routed to whole-table COW.** Keep `run_mutate`'s identity branch defensively (it never triggers) and test the actual conformance rejection.

---

## Task 1: Add `loom_tombstone` to the inline tier

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_inline.rs` (`inline_ddl` at ~246-263; `inline_append` DDL step at ~321-325)
- Test: `src/control-plane/postgres/tests/inline_tombstone.rs` (new)
- Modify: `src/control-plane/postgres/BUCK` (new `loom_fixture_test` target)

**Interfaces:**
- Produces: `ensure_inline_schema(conn, tid, columns) -> Result<()>` — creates `inline_<tid>` if absent (DDL now includes `loom_tombstone boolean not null default false`) AND runs `alter table … add column if not exists loom_tombstone boolean not null default false` (idempotent for pre-existing tables). Replaces the bare `create table if not exists` at `inline_append:321-325`.

- [ ] **Step 1: Write the failing test** (`src/control-plane/postgres/tests/inline_tombstone.rs`)

```rust
//! Verifies the inline tier carries a `loom_tombstone` column after an append.
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::{iceberg_inline, iceberg_landing};
use control_plane_core::{ColumnSpec, TableRef, LineageEvent, EventType};
// (mirror the imports/seed of an existing inline test, e.g. tests that call inline_append)

#[tokio::test]
async fn inline_table_has_tombstone_column() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let table = TableRef { schema: "s".into(), name: "t".into() };
    let columns = vec![ColumnSpec { name: "id".into(), ty: "Long".into(), nullable: false }];
    // append one row through the normal inline path (mirror an existing inline_append test's batch build)
    // … build a one-row RecordBatch with id=1 …
    // iceberg_inline::inline_append(&pool, &table, &columns, &batch, lineage, None).await.unwrap();

    // Assert the physical column exists and defaults false.
    let tid = /* resolve table_id via the same helper existing tests use */ ;
    let exists: bool = sqlx::query_scalar(sqlx::types::... /* AssertSqlSafe */)
        // select exists(select 1 from information_schema.columns
        //   where table_schema='iceberg_mirror' and table_name = 'inline_<tid>'
        //     and column_name='loom_tombstone')
        .fetch_one(&pool).await.unwrap();
    assert!(exists, "inline table must carry loom_tombstone");
}
```

Use an existing inline test in `src/control-plane/postgres/tests/` as the template for the batch build, `table_id` resolution, and imports (grep for `inline_append(` in that dir). Keep the assertion query as `information_schema.columns` existence.

- [ ] **Step 2: Run test to verify it fails**

Run: `buck2 test //src/control-plane/postgres:inline-tombstone > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error" /tmp/t.log`
Expected: FAIL — `loom_tombstone` column absent (or target missing → add it in Step 3's BUCK edit first).

- [ ] **Step 3: Implement `ensure_inline_schema` and update `inline_ddl`**

In `iceberg_inline.rs`, change `inline_ddl` (~256-262) to include the column in the fixed prelude:

```rust
Ok(format!(
    "create table if not exists {} (\
       loom_row_id bigserial primary key, \
       begin_snapshot bigint not null, \
       end_snapshot bigint, \
       loom_tombstone boolean not null default false{cols})",
    inline_table_name(table_id),
))
```

Add an idempotent ensure helper next to it:

```rust
/// Create the inline table if absent and guarantee the `loom_tombstone` column
/// exists (pre-existing tables created before slice 1 lack it).
async fn ensure_inline_schema(
    conn: &mut sqlx::PgConnection,
    tid: i64,
    columns: &[ColumnSpec],
) -> Result<()> {
    sqlx::query(AssertSqlSafe(inline_ddl(tid, columns)?))
        .execute(&mut *conn)
        .await
        .map_err(backend)?;
    let alter = format!(
        "alter table {} add column if not exists loom_tombstone boolean not null default false",
        inline_table_name(tid),
    );
    sqlx::query(AssertSqlSafe(alter))
        .execute(&mut *conn)
        .await
        .map_err(backend)?;
    Ok(())
}
```

Replace the DDL step in `inline_append` (~321-325) to call `ensure_inline_schema(&mut *conn, tid, columns).await?;`.

- [ ] **Step 4: Add the BUCK target and run the test**

In `src/control-plane/postgres/BUCK`, add (mirror an existing `loom_fixture_test` target in that file, e.g. the inline tests):

```python
loom_fixture_test(
    name = "inline-tombstone",
    crate = "inline_tombstone",
    srcs = ["tests/inline_tombstone.rs"],
    crate_root = "tests/inline_tombstone.rs",
    deps = [":postgres", "//src/control-plane/core:core", "//third-party:arrow-array", "//third-party:sqlx", "//third-party:tokio"],
)
```

Run: `buck2 test //src/control-plane/postgres:inline-tombstone > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error" /tmp/t.log`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/control-plane/postgres/src/iceberg_inline.rs src/control-plane/postgres/tests/inline_tombstone.rs src/control-plane/postgres/BUCK
git commit -m "feat(cow): add loom_tombstone column to the inline tier"
```

---

## Task 2: `identity_for_table` ontology reverse lookup

**Files:**
- Modify: `src/control-plane/postgres/src/ontology.rs` (add the function; export it from the crate `lib.rs` if needed)
- Test: `src/control-plane/postgres/tests/identity_for_table.rs` (new) + BUCK target
- **No `.sqlx` regen** (uses `AssertSqlSafe`; see Global Constraints).

**Interfaces:**
- Produces: `pub async fn identity_for_table(pool: &PgPool, table: &TableRef) -> Result<Option<String>>` — returns the `identity` column name for the object type whose `table` matches `(schema, name)`, or `None` if the type has no identity / no such type.

- [ ] **Step 1: Write the failing test** (`tests/identity_for_table.rs`)

```rust
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::ontology::identity_for_table;
use control_plane_core::{TableRef, /* ontology define API used by existing ontology tests */};

#[tokio::test]
async fn resolves_identity_from_ontology() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    // define a type Widget(table s.t) with identity "id" using the same ontology define
    // API an existing ontology test uses (grep tests/ for `define_type`/`ObjectType::build`).
    // … define Widget with identity("id"), table = TableRef{schema:"s", name:"t"} …
    let got = identity_for_table(&pool, &TableRef { schema: "s".into(), name: "t".into() }).await.unwrap();
    assert_eq!(got.as_deref(), Some("id"));
    let none = identity_for_table(&pool, &TableRef { schema: "s".into(), name: "nope".into() }).await.unwrap();
    assert_eq!(none, None);
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `buck2 test //src/control-plane/postgres:identity-for-table > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error" /tmp/t.log`
Expected: FAIL — function undefined.

- [ ] **Step 3: Implement the lookup**

In `ontology.rs`, using the schema of `ontology.object_type` (confirm the exact columns in `0014_object_identity.sql` / `PgOntology::get_type` at `ontology.rs:158/212` — it may store the table as `table_schema`/`table_name` or a single `table` text; adjust the WHERE accordingly). Use runtime `AssertSqlSafe` (no `.sqlx` regen possible here — see Global Constraints):

```rust
/// Reverse-lookup: the identity column name for the object type stored at `table`,
/// or None if it has no declared identity / does not exist. Used by the engine
/// serving read to make merge-on-read identity-aware without a wire round-trip.
// AssertSqlSafe: static query against ontology.object_type; sqlx regen unavailable
// in this env (initdb-as-root). Convert to query! when regenerating locally.
pub async fn identity_for_table(pool: &PgPool, table: &TableRef) -> Result<Option<String>> {
    let row: Option<Option<String>> = sqlx::query_scalar(AssertSqlSafe(
        "select identity from ontology.object_type \
         where table_schema = $1 and table_name = $2",
    ))
    .bind(&table.schema)
    .bind(&table.name)
    .fetch_optional(pool)
    .await
    .map_err(backend)?;
    // `identity` column is itself nullable → flatten Option<Option<String>>.
    Ok(row.flatten())
}
```

`AssertSqlSafe` is already imported/used in this crate (e.g. `iceberg_inline.rs`); import it the same way. Verify the runtime scalar type matches (`identity` is a nullable `text` → `Option<String>`; `fetch_optional` wraps the row → `Option<Option<String>>`).

- [ ] **Step 4: Add BUCK target and run**

Add the BUCK target (mirror Task 1's shape, deps `[":postgres", "//src/control-plane/core:core", "//third-party:sqlx", "//third-party:tokio"]`).

Run: `buck2 test //src/control-plane/postgres:identity-for-table > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error" /tmp/t.log`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/control-plane/postgres/src/ontology.rs src/control-plane/postgres/tests/identity_for_table.rs src/control-plane/postgres/BUCK
git commit -m "feat(cow): identity_for_table ontology reverse lookup"
```

---

## Task 3: Identity-aware merge-on-read in the engine read path

**Files:**
- Modify: `src/services/engine-serving/src/serving.rs` (`build_serving_provider` l.71-143; `build_inline_provider` l.181-230)
- Test: `src/services/engine-serving/tests/merge_on_read.rs` (new) + `src/services/engine-serving/BUCK` target

**Interfaces:**
- Consumes: `identity_for_table(pool, table)` (Task 2); `loom_tombstone` column (Task 1).
- Produces: `build_serving_provider` now returns an **identity-deduped** provider when the type has an identity; identity-less types keep the additive union. The returned provider's schema is exactly the mirror data schema (`arrow_schema_from_mirror`) — unchanged for callers/governed layer.

**Merge semantics (definition of correctness):** for a type with identity column `<id>`, the served rows are equivalent to:

```sql
SELECT <data_cols> FROM (
  SELECT <data_cols>, _loom_prec, _loom_tomb,
         ROW_NUMBER() OVER (PARTITION BY <id> ORDER BY _loom_prec DESC) AS _loom_rn
  FROM (
    SELECT <data_cols>, 0            AS _loom_prec, false          AS _loom_tomb FROM <file_tier>
    UNION ALL
    SELECT <data_cols>, begin_snapshot AS _loom_prec, loom_tombstone AS _loom_tomb FROM <inline_tier_visible>
  ) u
) ranked
WHERE _loom_rn = 1 AND _loom_tomb = false
```

`<inline_tier_visible>` already has the MVCC base predicate applied (`build_inline_provider`'s `base` filter). `<data_cols>` are the mirror columns in `arrow_schema_from_mirror` order.

- [ ] **Step 1: Write the failing test** (`tests/merge_on_read.rs`)

Seed the two tiers directly via Postgres + a file, then read through `engine_serving::execute_query`. Mirror `update_delete_tiers_e2e.rs` / `e2e_support.rs` seeding for how to stand up a file-resident row and how to run `execute_query`. Three cases:

```rust
#[tokio::test]
async fn inline_version_shadows_file_row() {
    // define Widget(id identity, qty) via ontology; seed a FILE-resident row {id:1, qty:1};
    // insert an inline VERSION {id:1, qty:9, begin_snapshot > file, loom_tombstone:false};
    // read `SELECT id, qty FROM s.t` via execute_query;
    // assert exactly one row for id=1 with qty=9 (version shadows file).
}
#[tokio::test]
async fn tombstone_hides_file_row() {
    // seed file row {id:1}; insert inline TOMBSTONE {id:1, loom_tombstone:true};
    // read → id=1 absent.
}
#[tokio::test]
async fn identity_less_type_unions_additively() {
    // a type with NO identity: two rows survive (no dedup) — additive union unchanged.
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `buck2 test //src/services/engine-serving:merge-on-read > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error" /tmp/t.log`
Expected: FAIL — current additive union returns both file+inline rows (2 rows) instead of the shadowed 1.

- [ ] **Step 3: Extend `build_inline_provider` to expose precedence + tombstone (merge mode)**

Give `build_inline_provider` an `identity: Option<&str>` parameter. When `identity.is_some()`, build the `PgTableProvider` over a schema that **also** includes `begin_snapshot` (aliased `_loom_prec`, i64 non-null) and `loom_tombstone` (aliased `_loom_tomb`, bool non-null), so the provider's SELECT projects them (the `PgTableProvider` selects its schema columns; see `provider.rs:73-113`). When `identity.is_none()`, keep today's data-only schema.

Concretely: extend the arrow schema handed to `PgTableProvider::new` with two extra fields and the `logical_types` vec with the two extra logical types; keep the same `base` MVCC filter. (The physical column names are `begin_snapshot` / `loom_tombstone`; expose them under the merge-only alias names by selecting `begin_snapshot as _loom_prec, loom_tombstone as _loom_tomb` — if `PgTableProvider` cannot alias, name the schema fields `begin_snapshot`/`loom_tombstone` and reference those names in the merge SQL instead.)

- [ ] **Step 4: Implement the merge in `build_serving_provider`**

At the top of `build_serving_provider`, resolve identity:

```rust
let identity = control_plane_postgres::ontology::identity_for_table(catalog.pool(), table)
    .await
    .map_err(to_serving)?;
```

(If `IcebergCatalog` exposes the pool via a field not a method, use that; add a `pub fn pool(&self) -> &PgPool` accessor to `IcebergCatalog` if none exists — small, in `control-plane/postgres`.)

Replace the `match (file_provider, inline_provider)` combine block (l.126-140):
- **`identity.is_none()`** → keep the existing additive union / single-provider logic verbatim.
- **`identity = Some(id)`** → register the file provider (with synthesized `_loom_prec=0`, `_loom_tomb=false`) and the merge-mode inline provider under temporary unique names in `ctx`, then run the merge SQL (the "definition of correctness" above) via `ctx.sql(&merge_sql).await` and return `.into_view()`. Build `<data_cols>` from `schema` (the mirror arrow schema), quoting identifiers. When only one tier exists, still apply the dedup+tombstone filter over that single tier (a lone tombstone must still hide its id; a lone inline set must still dedup versions).

Verify the returned view's schema equals `schema` (data columns only) — the governed layer and callers depend on it (`serving.rs:120-125`). Adjust the final projection/casts if DataFusion widens types (e.g. cast `_loom_prec` literal to match).

> Implementer note: express the merge as SQL over registered providers (mirrors the existing `ROW_NUMBER() OVER (PARTITION BY identity)` dedup in `query-api/src/sql.rs:662-688`). If you instead use the DataFrame builder (`with_column`/`window`/`distinct_on`), verify it compiles against DataFusion 54 and produces the identical result; the SQL form is the reference.

- [ ] **Step 5: Run the test to verify it passes**

Run: `buck2 test //src/services/engine-serving:merge-on-read > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error" /tmp/t.log`
Expected: PASS (all three cases).

- [ ] **Step 6: Commit**

```bash
git add src/services/engine-serving/src/serving.rs src/services/engine-serving/tests/merge_on_read.rs src/services/engine-serving/BUCK src/control-plane/postgres/src/iceberg_catalog.rs
git commit -m "feat(cow): identity-aware merge-on-read in build_serving_provider"
```

---

## Task 4: `write_inline_delta` + `current_inline_version` (per-identity CAS)

**Files:**
- Create: `src/control-plane/postgres/migrations/0016_shadow_flag.sql` (the shadow marker table — defined here because `write_inline_delta` sets it; Task 5 only reads it)
- Modify: `src/control-plane/postgres/src/iceberg_inline.rs`
- Test: `src/control-plane/postgres/tests/inline_delta_cas.rs` (new) + BUCK target
- **No `.sqlx` regen** (standalone table + `AssertSqlSafe`).

**Interfaces:**
- Consumes: `ensure_inline_schema` (Task 1), `next_snapshot` (`iceberg_mirror.rs:46`), `bind_cell`/`Cell` (`iceberg_inline.rs:139-243`), `pg_emit`.
- Produces:
  - `pub async fn current_inline_version(pool: &PgPool, table: &TableRef, id_column: &str, id_value: &Cell) -> Result<i64>` — `coalesce(max(begin_snapshot),0)` over live inline rows for the id (0 if none / no inline table).
  - `pub async fn write_inline_delta(pool: &PgPool, table: &TableRef, columns: &[ColumnSpec], id_column: &str, id_value: &Cell, tombstone: bool, batch: Option<&RecordBatch>, lineage: LineageEvent, expected_version: i64) -> Result<SnapshotId>` — one-tx delta write with per-identity advisory-lock CAS; `Err(ControlPlaneError::Conflict(_))` if a newer version for the id exists.
  - `set_has_shadow(conn, tid) -> Result<()>`, `has_shadow(conn, tid) -> Result<bool>` (used by Task 5's flush guards).

- [ ] **Step 0: Add the `shadow_flag` marker table + accessors**

Create `src/control-plane/postgres/migrations/0016_shadow_flag.sql`:

```sql
-- Slice-1 scalable COW: a table listed here has taken a mutation (carries inline
-- shadow deltas) so the byte-trigger flush is suppressed until slice-2
-- consolidation (flushing a version/tombstone would duplicate/resurrect a file row).
create table iceberg_mirror.shadow_flag (
    table_id bigint primary key references iceberg_mirror.table(table_id)
);
```

(Confirm the FK target `iceberg_mirror.table(table_id)` per `0012_iceberg_mirror.sql`; drop `references` if `table_id` may not yet exist there at write time.)

Add to `iceberg_inline.rs` (`AssertSqlSafe`, no `.sqlx` regen — see Global Constraints):

```rust
// AssertSqlSafe: static queries against a standalone table; sqlx regen unavailable
// in this env (initdb-as-root).
pub async fn set_has_shadow(conn: &mut sqlx::PgConnection, tid: i64) -> Result<()> {
    sqlx::query(AssertSqlSafe(
        "insert into iceberg_mirror.shadow_flag (table_id) values ($1) on conflict do nothing",
    )).bind(tid).execute(&mut *conn).await.map_err(backend)?;
    Ok(())
}
pub async fn has_shadow(conn: &mut sqlx::PgConnection, tid: i64) -> Result<bool> {
    let v: bool = sqlx::query_scalar(AssertSqlSafe(
        "select exists(select 1 from iceberg_mirror.shadow_flag where table_id = $1)",
    )).bind(tid).fetch_one(&mut *conn).await.map_err(backend)?;
    Ok(v)
}
```

- [ ] **Step 1: Write the failing test** (`tests/inline_delta_cas.rs`)

```rust
#[tokio::test]
async fn delta_write_and_cas_conflict() {
    // seed inline table via ensure_inline_schema + one append {id:1, qty:1} at v0.
    // v0 = current_inline_version(pool, table, "id", &Cell::Int(1)).await.unwrap();
    // write a VERSION {id:1, qty:9} with expected_version = v0 → Ok(v1), v1 > v0.
    // write again with the STALE expected_version = v0 → Err(Conflict) (a newer version exists).
    // write a delta for a DIFFERENT id=2 with its own expected_version → Ok (no conflict).
}
#[tokio::test]
async fn tombstone_delta_marks_deleted() {
    // append {id:1}; write_inline_delta(tombstone=true, batch=None, expected_version=v0) → Ok.
    // assert a live inline row for id=1 with loom_tombstone=true exists.
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `buck2 test //src/control-plane/postgres:inline-delta-cas > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error" /tmp/t.log`
Expected: FAIL — functions undefined.

- [ ] **Step 3: Implement `current_inline_version`**

```rust
pub async fn current_inline_version(
    pool: &PgPool,
    table: &TableRef,
    id_column: &str,
    id_value: &Cell,
) -> Result<i64> {
    let mut conn = pool.acquire().await.map_err(backend)?;
    let Some(tid) = live_table_id(&mut conn, table).await? else { return Ok(0) };
    // AssertSqlSafe: dynamic inline_<tid> name; id_column is a quoted mirror identifier.
    let sql = format!(
        "select coalesce(max(begin_snapshot), 0) from {} where \"{}\" = $1 and end_snapshot is null",
        inline_table_name(tid),
        id_column.replace('"', "\"\""),
    );
    let q = bind_cell(sqlx::query_scalar(AssertSqlSafe(sql)), id_value);
    let v: i64 = q.fetch_one(&mut *conn).await.map_err(backend)?;
    Ok(v)
}
```

(Use the same `live_table_id` helper `build_inline_provider` uses to resolve `tid`; if the inline table does not exist yet, return 0. `bind_cell` currently binds onto `sqlx::query`; add/confirm a `query_scalar` binding variant or bind then `fetch_scalar` — mirror an existing scalar bind.)

- [ ] **Step 4: Implement `write_inline_delta` (advisory-lock CAS)**

Mirror `inline_append` (`iceberg_inline.rs:275-377`) — one `pool.begin()` tx:

```rust
pub async fn write_inline_delta(
    pool: &PgPool,
    table: &TableRef,
    columns: &[ColumnSpec],
    id_column: &str,
    id_value: &Cell,
    tombstone: bool,
    batch: Option<&RecordBatch>,
    lineage: LineageEvent,
    expected_version: i64,
) -> Result<SnapshotId> {
    let mut tx = pool.begin().await.map_err(backend)?;
    let tid = /* ensure table_id: next_snapshot bookkeeping requires the mirror `table` row;
                 mirror inline_append's ensure_table/project_columns preamble */;
    ensure_inline_schema(&mut tx, tid, columns).await?;

    // Per-identity serialization: advisory xact lock keyed by (tid, id hash).
    // Mirrors iceberg_flush.rs:43's pg_advisory_xact_lock(lock_key(...)).
    // AssertSqlSafe: static; sqlx regen unavailable in this env (initdb-as-root).
    let key = advisory_key_for_id(tid, id_value);
    sqlx::query(AssertSqlSafe("select pg_advisory_xact_lock($1)"))
        .bind(key).execute(&mut *tx).await.map_err(backend)?;

    // CAS: current live max version for id must still equal expected_version.
    let cur_sql = format!(
        "select coalesce(max(begin_snapshot),0) from {} where \"{}\" = $1 and end_snapshot is null",
        inline_table_name(tid), id_column.replace('"', "\"\""),
    );
    let cur: i64 = bind_cell(sqlx::query_scalar(AssertSqlSafe(cur_sql)), id_value)
        .fetch_one(&mut *tx).await.map_err(backend)?;
    if cur != expected_version {
        return Err(ControlPlaneError::Conflict(format!(
            "cow: identity version advanced {expected_version} -> {cur} (concurrent mutation)"
        )));
    }

    let at = next_snapshot(&mut tx, None).await?;

    // Insert one delta row.
    if tombstone {
        let sql = format!(
            "insert into {} (begin_snapshot, loom_tombstone) values ($1, true)",
            inline_table_name(tid),
        );
        sqlx::query(AssertSqlSafe(sql)).bind(at.0).execute(&mut *tx).await.map_err(backend)?;
    } else {
        let batch = batch.ok_or_else(|| ControlPlaneError::Backend("version delta requires a row".into()))?;
        // mirror inline_append's INSERT (l.328-352): build col_list + placeholders,
        // insert into inline_<tid> (begin_snapshot, loom_tombstone, <cols>) values ($1, false, …),
        // binding at.0 then each cell_from_arrow(batch, c, 0, ty).
    }

    set_has_shadow(&mut tx, tid).await?;   // Step 0 helper
    pg_emit(&mut *tx, &lineage).await?;
    tx.commit().await.map_err(backend)?;
    Ok(at)
}
```

Add `fn advisory_key_for_id(tid: i64, id: &Cell) -> i64` — a deterministic hash of `(tid, id)` into an `i64` (mirror the deterministic hashing used by `commit_backoff` / `lock_key`; no `rand`). Reuse `next_snapshot`'s `&mut tx` overload (it accepts `&mut PgConnection`).

`ControlPlaneError::Conflict` already exists (`end_cap_files_by_path` raises it). Confirm the variant and constructor.

- [ ] **Step 5: Add BUCK target and run**

Run: `buck2 test //src/control-plane/postgres:inline-delta-cas //src/control-plane/postgres:sqlx-cache-check > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error" /tmp/t.log`
Expected: PASS — including `sqlx-cache-check` (proves the new migration/table left the committed `.sqlx` cache valid).

- [ ] **Step 6: Commit**

```bash
git add src/control-plane/postgres/migrations/0016_shadow_flag.sql src/control-plane/postgres/src/iceberg_inline.rs src/control-plane/postgres/tests/inline_delta_cas.rs src/control-plane/postgres/BUCK
git commit -m "feat(cow): write_inline_delta + current_inline_version with per-identity CAS"
```

---

## Task 5: Flush suppression for shadow-bearing tables

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_inline.rs` (suppress trigger-enqueue when shadowed)
- Modify: `src/control-plane/postgres/src/iceberg_flush.rs` (`flush_locked` early-return when shadowed)
- Test: `src/control-plane/postgres/tests/flush_suppression.rs` (new) + BUCK target

**Interfaces:**
- Consumes: `has_shadow(conn, tid)` / `set_has_shadow` (Task 4, Step 0); `write_inline_delta` (Task 4).

The `shadow_flag` table + `set_has_shadow`/`has_shadow` helpers already exist (Task 4). This task only wires the two suppression guards + a test.

- [ ] **Step 1: Write the failing test** (`tests/flush_suppression.rs`)

```rust
#[tokio::test]
async fn flush_is_suppressed_after_a_mutation() {
    // append {id:1, qty:1} (file or inline); write_inline_delta version {id:1, qty:9};
    // assert has_shadow(tid) == true;
    // call iceberg_flush::flush_table(...) → returns Ok(None) (suppressed, no corruption);
    // read back via execute_query → still one row id=1 qty=9 (no duplicate/no resurrection).
}
#[tokio::test]
async fn append_only_table_still_flushes() {
    // append rows to a table with NO mutation; has_shadow==false; flush_table drains normally.
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `buck2 test //src/control-plane/postgres:flush-suppression > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error" /tmp/t.log`
Expected: FAIL.

- [ ] **Step 3: Wire suppression**

- `write_inline_delta` (Task 4) already calls `set_has_shadow(&mut tx, tid)`.
- In `inline_append`'s flush-trigger block (`iceberg_inline.rs:357-370`): before enqueuing the flush job, `if has_shadow(&mut *conn, tid).await? { /* skip enqueue + skip arm */ }`.
- In `iceberg_flush.rs::flush_locked` (l.56, before reading `inline_live_batch`): `if has_shadow(conn, tid).await? { reset_inline_trigger(conn, tid).await?; return Ok(None); }` — mirror the existing `None`-branch self-heal (l.73-80).

- [ ] **Step 4: Run the test to verify it passes**

Run: `buck2 test //src/control-plane/postgres:flush-suppression > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error" /tmp/t.log`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/control-plane/postgres/src/iceberg_inline.rs src/control-plane/postgres/src/iceberg_flush.rs src/control-plane/postgres/tests/flush_suppression.rs src/control-plane/postgres/BUCK
git commit -m "feat(cow): suppress flush for shadow-bearing tables"
```

---

## Task 6: `EngineControl` gRPC — `current_inline_version` + `write_delta`

**Files:**
- Modify: `src/services/engine-wire/proto/engine_control.proto`
- Modify: `src/services/engine-wire/src/client.rs` (`GrpcQueueClient`)
- Modify: `src/services/engine/src/service.rs` (`EngineControlService`)
- Modify: `src/services/engine-serving/src/action_writer.rs` (`IcebergActionWriter`)
- Test: covered by the query-api e2e in Tasks 8-9 (wire has no standalone fixture). Add a compile-smoke by building the crates.

**Interfaces:**
- Produces (on `IcebergActionWriter`):
  - `async fn current_inline_version(&self, table, id_column: &str, id_value: &SqlValue) -> Result<i64, EngineServingError>`
  - `async fn write_delta(&self, table, id_column: &str, id_value: &SqlValue, tombstone: bool, ipc: &[u8], columns: &[ColumnSpec], event: LineageEvent, expected_version: i64) -> Result<SnapshotId, EngineServingError>` (maps `ControlPlaneError::Conflict` → `EngineServingError::Conflict`).

- [ ] **Step 1: Extend the proto**

Mirror `WriteObjectRequest`/`OverwriteTableRequest` (`engine_control.proto:66-82`):

```proto
message CurrentInlineVersionRequest {
  string schema = 1;
  string name = 2;
  string id_column = 3;
  string id_value_json = 4;   // SqlValue serialized
}
message CurrentInlineVersionResponse { int64 version = 1; }

message WriteDeltaRequest {
  string schema = 1;
  string name = 2;
  string id_column = 3;
  string id_value_json = 4;
  bool   tombstone = 5;
  bytes  ipc = 6;             // one-row Arrow IPC (empty when tombstone)
  string columns_json = 7;    // Vec<ColumnSpec>
  string lineage_json = 8;    // LineageWire
  int64  expected_version = 9;
}
message WriteDeltaResponse { int64 snapshot_id = 1; }
```

Add to the `service EngineControl` block:

```proto
  rpc CurrentInlineVersion (CurrentInlineVersionRequest) returns (CurrentInlineVersionResponse);
  rpc WriteDelta (WriteDeltaRequest) returns (WriteDeltaResponse);
```

**The prost/tonic types are generated at build time** by the `:engine-control-pb` genrule (`src/services/engine-wire/BUCK:19-23`, `include!(concat!(env!("ENGINE_PB"), "/loom.engine.v1.rs"))` in `lib.rs:10-11`) — **nothing to check in or regenerate manually**; editing `proto/engine_control.proto` and rebuilding regenerates `engine_wire::pb` automatically. The new messages appear as `engine_wire::pb::CurrentInlineVersionRequest` etc.; the new client/server stubs appear on the generated `EngineControl` service traits.

- [ ] **Step 2: `IcebergActionWriter` methods** (`action_writer.rs`)

```rust
pub async fn current_inline_version(&self, table: &TableRef, id_column: &str, id_value: &SqlValue)
    -> Result<i64, EngineServingError>
{
    let cell = cell_from_sqlvalue(id_value)?;  // SqlValue -> Cell (add small helper)
    iceberg_inline::current_inline_version(&self.pool, table, id_column, &cell)
        .await.map_err(|e| EngineServingError::Engine(e.to_string()))
}

pub async fn write_delta(&self, table: &TableRef, id_column: &str, id_value: &SqlValue,
    tombstone: bool, ipc: &[u8], columns: &[ColumnSpec], event: LineageEvent, expected_version: i64)
    -> Result<SnapshotId, EngineServingError>
{
    let cell = cell_from_sqlvalue(id_value)?;
    let batch = if tombstone { None } else { Some(decode_ipc(ipc)?.into_iter().next()
        .ok_or_else(|| EngineServingError::Engine("empty version batch".into()))?) };
    iceberg_inline::write_inline_delta(&self.pool, table, columns, id_column, &cell, tombstone,
        batch.as_ref(), event, expected_version)
        .await
        .map_err(|e| match e {
            ControlPlaneError::Conflict(m) => EngineServingError::Conflict(m),
            other => EngineServingError::Engine(other.to_string()),
        })
}
```

Add `EngineServingError::Conflict(String)` variant. Add `cell_from_sqlvalue` (match `SqlValue` variants → `Cell`; mirror the reverse `column_array`/`cell_from_arrow` typing).

- [ ] **Step 3: `EngineControlService` handlers** (`engine/src/service.rs`, mirror `write_object` l.234-258)

Deserialize `id_value_json` → `SqlValue`, `columns_json` → `Vec<ColumnSpec>`, `lineage_json` → `LineageEvent`; call the writer; map `EngineServingError::Conflict` → `Status::aborted(msg)`, other → `Status::internal`. Return the snapshot id / version.

- [ ] **Step 4: `GrpcQueueClient` methods** (`engine-wire/src/client.rs`, mirror `write_object` l.183-231)

Build the request, call the generated stub, return the response field. Surface `tonic::Code::Aborted` distinctly (do not collapse to a generic error) so the query-api client can map it to `Conflict`.

- [ ] **Step 5: Build the affected crates**

Run: `buck2 build -M none //src/services/engine-wire/... //src/services/engine/... //src/services/engine-serving/... > /tmp/b.log 2>&1; tail -5 /tmp/b.log`
Expected: build succeeds.

- [ ] **Step 6: Commit**

```bash
git add src/services/engine-wire/proto/engine_control.proto src/services/engine-wire/src/client.rs src/services/engine/src/service.rs src/services/engine-serving/src/action_writer.rs
# (pb is build-time generated — nothing to add)
git commit -m "feat(cow): EngineControl current_inline_version + write_delta RPCs"
```

---

## Task 7: `ActionEngine` trait + `EngineActionClient` (query-api side)

**Files:**
- Modify: `src/services/query-api/src/serving.rs` (`ActionEngine` trait; `ServingError::Conflict`)
- Modify: `src/services/query-api/src/engine_action_client.rs` (`EngineActionClient` impls)
- Test: exercised by Tasks 8-9; build-smoke here.

**Interfaces:**
- Produces (on `ActionEngine`):
  - `async fn current_inline_version(&self, table: &TableRef, id_column: &str, id_value: &SqlValue) -> Result<i64, ServingError>`
  - `async fn write_delta(&self, table: &TableRef, id_column: &str, id_value: &SqlValue, tombstone: bool, columns: &[String], values: &[SqlValue], logical_types: &[String], event: LineageEvent, expected_version: i64) -> Result<SnapshotId, ServingError>` (builds the one-row IPC batch client-side for versions, like `write_object`; empty for tombstone).
  - `ServingError::Conflict(String)` variant.

- [ ] **Step 1: Add trait methods + `ServingError::Conflict`** (`serving.rs:287-309`)

Add the two methods to the `ActionEngine` trait. Add `Conflict(String)` to `ServingError`.

- [ ] **Step 2: Implement on `EngineActionClient`** (`engine_action_client.rs:45-109`, mirror `write_object`/`overwrite_table`)

`current_inline_version`: serialize `id_value` → JSON, call `GrpcQueueClient::current_inline_version`, return `version`.
`write_delta`: for a version, build the one-row Arrow batch + IPC (reuse `build_object_batch` for a single row) and serialize `Vec<ColumnSpec>`; for a tombstone, empty ipc + columns still serialized (needed for `ensure_inline_schema`); serialize the `LineageEvent`; call `GrpcQueueClient::write_delta`. Map a `tonic::Code::Aborted` status → `ServingError::Conflict`.

Add impls for the in-process test engine too (`InProcessServingEngine`/the test `ActionEngine` in `e2e_support.rs`) so e2e tests call the same seam without the wire — delegate directly to `IcebergActionWriter::current_inline_version`/`write_delta`. (Check `e2e_support.rs` for how the test `ActionEngine` is built; extend it.)

- [ ] **Step 3: Build query-api**

Run: `buck2 build -M none //src/services/query-api/... > /tmp/b.log 2>&1; tail -5 /tmp/b.log`
Expected: build succeeds (trait fully implemented by all impls).

- [ ] **Step 4: Commit**

```bash
git add src/services/query-api/src/serving.rs src/services/query-api/src/engine_action_client.rs
git commit -m "feat(cow): ActionEngine current_inline_version + write_delta with Conflict mapping"
```

---

## Task 8: `run_mutate` O(change) rewrite + bounded retry — e2e tests 1-3

**Files:**
- Modify: `src/services/query-api/src/action.rs` (`run_mutate` l.698-829; add `select_object_sql`)
- Test: `src/services/query-api/tests/cow_inline_shadow_e2e.rs` (new) + BUCK target
- Modify: `src/services/query-api/tests/e2e_support.rs` if a shared helper is needed

**Interfaces:**
- Consumes: `ActionEngine::current_inline_version`/`write_delta` (Task 7); merge-on-read (Task 3); `enforce_mutate_policy` (unchanged, `action.rs:652-690`).
- Produces: `run_mutate` writes one inline delta per mutation (no whole-table overwrite for identity types), with a bounded retry on `Conflict`.

- [ ] **Step 1: Write the failing e2e tests** (`tests/cow_inline_shadow_e2e.rs`)

Use the `update_delete_tiers_e2e.rs` template (`define_widget`, `grant_writer`, `spawn_engine_writer`, `read_widget`, `run_action`):

```rust
#[tokio::test]
async fn update_shadows_file_row_without_rewrite() {
    // spawn_engine_writer with inline_byte_limit=0 (force FILE tier), flush_threshold=i64::MAX.
    // createWidget {id:1, qty:1} → file-resident. record the data_file set.
    // updateWidget {id:1, qty:9}.
    // read_widget(1) → qty == 9 (inline version shadows file).
    // assert the data_file set is UNCHANGED (no Parquet rewrite) and exactly one inline row was added.
}
#[tokio::test]
async fn delete_via_tombstone_hides_row() {
    // seed file row {id:1} and {id:2}; deleteWidget {id:1}.
    // read_widget(1) → None; read_widget(2) → present. data_file set unchanged.
}
#[tokio::test]
async fn latest_version_wins() {
    // seed {id:1, qty:1}; updateWidget qty:5; updateWidget qty:9.
    // read_widget(1) → qty == 9 (max begin_snapshot).
}
```

Add helpers to `e2e_support.rs` if needed to read the `iceberg_mirror.data_file` set for a table and count live inline rows.

- [ ] **Step 2: Run to verify it fails**

Run: `buck2 test //src/services/query-api:cow-inline-shadow-e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error" /tmp/t.log`
Expected: FAIL — current `run_mutate` rewrites the whole table (data_file set changes; assertion on "unchanged file set" fails), and after Task 3 the read may already reflect merge but the write is still overwrite.

- [ ] **Step 3: Add `select_object_sql` and rewrite `run_mutate`**

Add a targeted read helper next to `select_all_sql` (`action.rs:573-589`):

```rust
/// Targeted single-object read: `SELECT "c1",… FROM "schema"."table" WHERE "id" = $1`.
fn select_object_sql(target: &ObjectType, id_column: &str) -> String {
    let cols = target.properties.iter()
        .map(|p| format!("\"{}\"", p.name.replace('"', "\"\"")))
        .collect::<Vec<_>>().join(", ");
    format!(
        "SELECT {cols} FROM \"{}\".\"{}\" WHERE \"{}\" = $1",
        target.table.schema.replace('"', "\"\""),
        target.table.name.replace('"', "\"\""),
        id_column.replace('"', "\"\""),
    )
}
```

Rewrite `run_mutate` (keep `ensure_cow_supported` + identity resolution + conformance verbatim). Replace the full-table read + `overwrite_table` (l.730-824) with a bounded retry loop:

```rust
const COW_MAX_RETRIES: u32 = 5;
let idprop = target.identity.clone().ok_or(ActionError::Misconfigured(/* … */))?;
let id_idx = /* index of idprop in columns */;
let mut attempt = 0u32;
loop {
    // 1. capture the per-identity version token BEFORE the row read (provably safe ordering).
    let v0 = deps.action_engine
        .current_inline_version(&target.table, &idprop, &id_value)
        .await
        .map_err(ActionError::from)?;

    // 2. targeted merged read of the current live object.
    let live = deps.serving
        .fetch_rows(&select_object_sql(target, &idprop), &[id_value.clone()])
        .await
        .map_err(ActionError::from)?;
    let Some(existing) = live.rows.first() else { return Err(ActionError::NotFound); };

    // 3. build set_pairs + new_row (UPDATE) / None (DELETE) — unchanged logic (l.743-760/795-809).
    // 4. governance + constraints — enforce_mutate_policy(...) + constraint validation VERBATIM (l.764-793).
    // 5. commit ONE inline delta (version for UPDATE, tombstone for DELETE) with CAS on v0.
    let res = if is_update {
        deps.action_engine.write_delta(&target.table, &idprop, &id_value, /*tombstone=*/false,
            &columns, &new_row_values, &logical, event.clone(), v0).await
    } else {
        deps.action_engine.write_delta(&target.table, &idprop, &id_value, /*tombstone=*/true,
            &columns, &[], &logical, event.clone(), v0).await
    };
    match res {
        Ok(_) => break,
        Err(ServingError::Conflict(_)) if attempt < COW_MAX_RETRIES => {
            attempt += 1;
            // small backoff; deterministic jitter (no rand) — mirror commit_backoff.
            tokio::time::sleep(cow_backoff(attempt)).await;
            continue;
        }
        Err(e) => return Err(ActionError::from(e)),
    }
}
// 6. return affected object (UPDATE: new row; DELETE: existing row) + RunId — unchanged shape.
```

Notes:
- `id_value` is the typed identity param (already resolved at l.711-721). Clone per retry as needed.
- Map `ServingError::Conflict` past the retry cap to a `ServingError`/500 (a genuinely contended object) via `ActionError::from`.
- Add `ActionError::from(ServingError)` handling for the new `Conflict` variant (non-retryable path → backend/500).
- `add cow_backoff(attempt) -> Duration` — capped exponential + deterministic jitter (mirror `iceberg_writer.rs:124-137`).
- **Do not touch** the `overwrite_table` path; leave `ActionEngine::overwrite_table` in place (unused by `run_mutate` now, still used by transform/tests).

- [ ] **Step 4: Run the e2e tests**

Run: `buck2 test //src/services/query-api:cow-inline-shadow-e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error" /tmp/t.log`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/services/query-api/src/action.rs src/services/query-api/tests/cow_inline_shadow_e2e.rs src/services/query-api/tests/e2e_support.rs src/services/query-api/BUCK
git commit -m "feat(cow): run_mutate O(change) inline-delta write with per-identity CAS retry"
```

---

## Task 9: e2e tests 4-7 + flush-suppression e2e

**Files:**
- Test: `src/services/query-api/tests/cow_inline_shadow_gov_e2e.rs` (new) + BUCK target (governance + time-travel + CAS + identity-less)
- Test: extend `cow_inline_shadow_e2e.rs` or a new file for flush-suppression e2e

**Interfaces:** consumes everything above; no new production code expected (if a test reveals a gap, fix minimally and note it).

- [ ] **Step 1: Write the tests**

```rust
#[tokio::test]
async fn time_travel_sees_pre_mutation_value() {
    // seed file {id:1, qty:1} at snapshot s0; updateWidget qty:9 at s1.
    // a read as-of s0 (via ice.inline_live_batch / a versioned serving read — mirror
    // update_delete_tiers_e2e time-travel assertions at :77/:169) still returns qty=1.
}
#[tokio::test]
async fn concurrent_updates_cas_no_lost_update() {
    // seed {id:1, name:"x", qty:1}. Two updates that PATCH DIFFERENT columns from the same base:
    //   A: updateWidget {id:1, name:"y"}   B: updateWidget {id:1, qty:9}
    // Drive them so one hits the CAS conflict and retries (serialize deterministically:
    //   run A to completion, then B — B's current_inline_version(before its read) is v0 from
    //   before A, forcing the Conflict+retry path; OR use tokio::join! and assert final state).
    // Final read → {name:"y", qty:9} (both applied, no lost update).
    // A concurrent update to a DIFFERENT id never conflicts (write id=2 between → still Ok).
}
#[tokio::test]
async fn governance_denies_and_never_leaks() {
    // column-denied updateWidget → 403 WriteDenied, nothing written (read unchanged);
    // row-filter-denied updateWidget (old row outside; new row outside) → 403;
    // row-filter-denied deleteWidget → 403;
    // a masked/denied column never appears in the merged read output.
    // (mirror update_delete_governance_e2e.rs assertions.)
}
#[tokio::test]
async fn identity_less_type_mutation_is_misconfigured() {
    // define a type WITHOUT identity + an Update action (if definable) → run → Misconfigured/500
    // (conformance rejects; reconciles spec test 7 to the actual behavior).
}
#[tokio::test]
async fn flush_suppressed_after_mutation_no_corruption() {
    // spawn_engine_writer with a LOW flush_threshold; seed file {id:1,qty:1}; updateWidget qty:9;
    // append more inserts to try to trip the byte trigger; assert read stays {id:1,qty:9}
    // (no duplicate file row, no resurrected delete) and has_shadow suppressed the flush.
}
```

- [ ] **Step 2: Run to verify (write tests first, expect the untested paths to surface any gaps)**

Run: `buck2 test //src/services/query-api:cow-inline-shadow-gov-e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error" /tmp/t.log`
Expected: initially some FAIL if a governance/time-travel gap exists; fix minimally in the relevant production file (re-run its unit test), then all PASS.

- [ ] **Step 3: Make them pass** — fix any surfaced gaps (e.g. ensure `enforce_mutate_policy` runs on the targeted read's row exactly as before; ensure the CAS retry path re-governs on the fresh row). Keep production changes minimal and covered.

- [ ] **Step 4: Commit**

```bash
git add src/services/query-api/tests/cow_inline_shadow_gov_e2e.rs src/services/query-api/BUCK
git commit -m "test(cow): time-travel, CAS concurrency, governance, identity-less, flush-suppression e2e"
```

---

## Task 10: Full sweep, docs registers, and green CI

**Files:**
- Modify: `docs/ROADMAP.md`, `docs/FUTURE.md` (via `loom-docs-update`)

- [ ] **Step 1: Full affected build + test**

```bash
buck2 build -M none //src/control-plane/postgres/... //src/services/engine-serving/... //src/services/engine/... //src/services/engine-wire/... //src/services/query-api/... > /tmp/b.log 2>&1; tail -8 /tmp/b.log
buck2 test //src/control-plane/postgres/... //src/services/engine-serving/... //src/services/query-api/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```
Expected: build clean; all tests pass. `buck2 clean` between phases if disk pressure appears.

- [ ] **Step 2: Lint (clippy + prek hooks)**

```bash
./tools/clippy-all.sh > /tmp/c.log 2>&1; tail -20 /tmp/c.log
buck2 run //tools:prek -- run --all-files > /tmp/p.log 2>&1; tail -20 /tmp/p.log
```
Expected: no clippy findings on production code; prek clean (commit any hook-applied fixes). Confirm `//src/control-plane/postgres:sqlx-cache-check` passes (part of the postgres test sweep) — proves the `.sqlx` cache is fresh.

- [ ] **Step 3: Close the register item**

Use `loom-docs-update`: flip `road-cow-inline-shadow` `- [ ]`→`- [x]`, set `status:done`, add `pr:#<N>` once the PR exists. Record reconciliations in FUTURE if relevant (`fut-cow-cas-guard` already `promoted`; add a note that slice-1 shipped merge-on-read + per-identity CAS). Note the "identity-less mutation is a conformance rejection, not whole-table COW" reconciliation.

- [ ] **Step 4: Commit docs + push**

```bash
git add docs/ROADMAP.md docs/FUTURE.md
git commit -m "docs(cow): close road-cow-inline-shadow (slice 1 shipped)"
git push -u origin work/road-cow-inline-shadow
```

- [ ] **Step 5: Open the PR** (head `work/road-cow-inline-shadow`, base `main`) and drive CI green (`build-test`/`affected` + `lint`). Fix any CI-only failures (markdown trailing-whitespace/EOF on the plan/docs; a stale `.sqlx`; a fixture routed to RE) and re-push until green.

---

## Self-Review

**Spec coverage:**
- `loom_tombstone` column + append/version/tombstone model → Task 1 (column), Task 4 (version/tombstone writes). ✓
- Identity-aware merge-on-read (replaces additive union; identity-less unchanged; below governed layer) → Task 3. ✓
- O(change) mutation write replacing `overwrite_parquet_snapshot` for identity types, preserving PATCH + per-row ACL → Task 8 (`run_mutate`), ACL `enforce_mutate_policy` kept verbatim. ✓
- Per-identity CAS with bounded retry → Task 4 (advisory-lock CAS + `Conflict`), Task 8 (retry loop). ✓
- Suppress automatic flush for shadow-delta tables → Task 5. ✓
- Testing cases 1-7 → Tasks 8-9 (case 7 reconciled to conformance rejection). ✓
- MVCC/time-travel unchanged → Global Constraints + Task 9 time-travel test. ✓
- Out-of-scope (consolidation, identity-change, file-granular, vector, identity-less COW) → not touched; `overwrite_table` preserved. ✓

**Placeholder scan:** SQL, proto, and `run_mutate` structure are concrete. Where the DataFusion 54 merge API and the exact `inline_trigger`/`object_type` column names are environment-verified, the plan gives the reference SQL + the exact file:line to confirm against — the implementer verifies, not invents. No "TODO/TBD/add error handling".

**Type consistency:** `current_inline_version`/`write_inline_delta` (postgres) ↔ `IcebergActionWriter::current_inline_version`/`write_delta` (engine) ↔ `ActionEngine::current_inline_version`/`write_delta` (query-api) — names and the `expected_version: i64` / `tombstone: bool` / `Conflict` mapping thread consistently. `SqlValue`↔`Cell` bridged by `cell_from_sqlvalue`. `ServingError::Conflict` ↔ `EngineServingError::Conflict` ↔ `ControlPlaneError::Conflict` ↔ gRPC `Code::Aborted` mapping is consistent end-to-end.
