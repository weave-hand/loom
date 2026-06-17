# Iceberg Adapter — Read Path (Slice 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps
> use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a second `core::Catalog` read adapter backed by Apache Iceberg, serving from a
loom-owned Postgres mirror, validated by the existing backend-agnostic `catalog_contract` /
`catalog_delete_contract` — coexisting with DuckLake, which stays green.

**Architecture:** loom **vendors** the Apache `iceberg-catalog-sql` SQL-catalog module (ported
sqlx 0.8 → loom's sqlx 0.9) so it owns `update_table` — that owns the canonical `table →
metadata.json` pointer in JDBC tables in loom's Postgres and writes spec-compliant metadata to
a `file://` warehouse. loom also owns an `iceberg_mirror.*` schema — a read-optimized
projection of snapshots / files / schema — and a `core::Catalog` impl that serves entirely from
it (pure SQL, the same shape as `postgres/src/catalog.rs`). The mirror is a **rebuildable
cache**; **loom owns the catalog-global monotonic `SnapshotId` space and MVCC
(`begin/end_snapshot`)** in the mirror, independent of Iceberg's per-table snapshot ids (which
the mirror records for traceability only). At read time the adapter touches neither `iceberg`
nor object store; `iceberg` + the vendored catalog are used only in the test seeder.

**Tech Stack:** Rust, buck2, sqlx 0.9 (runtime queries in the vendored catalog; compile-time
`query!` + committed `.sqlx` for the loom-side mirror reads), `iceberg` 0.9.1, `strum`,
`object_store::local::LocalFileSystem` (tests), hermetic Postgres fixture (`loom_fixture_test`).

**Design spec:** `docs/superpowers/specs/2026-06-16-iceberg-adapter-read-path-design.md`.

---

## Orientation for the implementer (read before Task 2)

You are adding files to the `//src/control-plane/postgres` crate. The **DuckLake adapter is
your template** for the loom-side files — read these first and mirror their shape:

- `src/control-plane/postgres/src/catalog.rs` — the `impl Catalog for PgControlPlane` you
  parallel as `impl Catalog for IcebergCatalog` over `iceberg_mirror.*`. Note the
  `sqlx::query!` style with `as "col!"` non-null annotations, the `backend` error mapping,
  `NotFound` on empty, and the `resolve_table` helper with the MVCC predicate
  `begin_snapshot <= $at AND (end_snapshot IS NULL OR end_snapshot > $at)`.
- `src/control-plane/postgres/src/ducklake_type.rs` — `logical_from_ducklake`; you write the
  Iceberg twin `logical_from_iceberg`.
- `src/control-plane/postgres/src/fixture.rs` — `PgFixture`, `fresh_db()`, and the
  `DuckLakeWriter` seeder. Your `IcebergWriter` is a sibling.
- `src/control-plane/postgres/tests/catalog.rs` — `PgSeeder` + `postgres_passes_catalog_contract`.
  Your `tests/iceberg_catalog.rs` is the Iceberg twin.
- `src/control-plane/testkit/src/lib.rs` — the `CatalogSeed` trait (`seed` / `drop_table`),
  `SeedSpec` / `SeedColumn` / `SeededSnapshot`, and `catalog_contract` /
  `catalog_delete_contract`. **Do not modify testkit** — it is already backend-agnostic and your
  adapter must satisfy it as-is.
- `src/control-plane/postgres/migrations/0001_queue.sql` — migration style (you add `0012_…`).
- `src/control-plane/postgres/defs.bzl` — the `loom_fixture_test` macro; `BUCK` — the existing
  `loom_fixture_test(name="catalog", … duckdb=True …)` target you parallel.
- `CLAUDE.md` §"Compile-time SQL" and `tools/sqlx-prepare.sh` — how the `.sqlx` cache is
  generated and the `sqlx-cache-check` test that enforces it.

**Two hard rules from prior loom work (do not skip):**
1. **Format before committing.** `buck2 run //tools:rustfmt -- --edition 2024 <files>` *before*
   `git add`. The prek rustfmt hook is check-only and silently aborts `--amend`. After any
   commit run `git rev-parse --short HEAD` and confirm the SHA changed.
2. **Stage by explicit path, never `git add -A`.** `docs/TO_BE_PLANNED.md` is unrelated scratch —
   never stage it. `git status` before every commit.

**iceberg-rust 0.9.1 API:** writer/transaction/catalog signatures below are written against the
published 0.9.1 API; where a call is marked `// VERIFY`, confirm the exact name/signature
against `https://docs.rs/iceberg/0.9.1` (or the vendored source) before relying on it. The
loom-side code (Tasks 3–6, 8) carries no such uncertainty.

---

## Task 1 — Dependencies (DONE: commit `da1f76e`)

Already landed; recorded here for context. Do not redo.

- `iceberg = "0.9"` + `strum` added to `src/control-plane/postgres/Cargo.toml`;
  `iceberg-catalog-sql` deliberately **not** a dependency (it pins sqlx 0.8 and owns its own
  connection — can't share a transaction; loom vendors it instead in Task 2). Single sqlx 0.9.
- 7 buildscript fixups added (`anyhow`, `erased-serde`, `fastnum`, `portable-atomic`,
  `prettyplease`, `typeid`, `typetag`).
- Deleted the stale `third-party/fixups/brotli/fixups.toml` (its v3 alloc-no-stdlib remap died
  when the lock unified on v2). `//third-party:iceberg` and `//third-party:brotli-8` build on RE.

If you re-run `./tools/buckify.sh`, expect a clean `third-party/BUCK` diff (the `reindeer-check`
hook enforces it).

---

## Task 2: Vendor + port the SQL catalog (sqlx 0.8 → 0.9)

**Files:**
- Create: `src/control-plane/postgres/src/iceberg_sql_catalog/mod.rs` (from upstream `lib.rs`)
- Create: `src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs` (the ~996 impl lines)
- Create: `src/control-plane/postgres/src/iceberg_sql_catalog/error.rs` (from upstream `error.rs`)
- Modify: `src/control-plane/postgres/src/lib.rs` (declare the module)
- Modify: `src/control-plane/postgres/BUCK` (add `//third-party:iceberg`, `//third-party:strum`,
  and any sqlx/async-trait deps the module needs to the `rust_library`'s `deps`)

This vendors the Apache `iceberg-catalog-sql` 0.9.1 catalog into loom, ported to sqlx 0.9 over
Postgres. loom owns it so a later slice can fold the mirror upsert into `update_table`'s
transaction. **No standalone unit test** — Task 8's contract exercises it end to end through the
seeder.

- [ ] **Step 1: Fetch the upstream source at the pinned tag**

```bash
mkdir -p src/control-plane/postgres/src/iceberg_sql_catalog
for f in catalog.rs error.rs lib.rs; do
  gh api "repos/apache/iceberg-rust/contents/crates/catalog/sql/src/$f?ref=v0.9.1" \
    --jq '.content' | base64 -d > "src/control-plane/postgres/src/iceberg_sql_catalog/$f"
done
mv src/control-plane/postgres/src/iceberg_sql_catalog/lib.rs \
   src/control-plane/postgres/src/iceberg_sql_catalog/mod.rs
```
Keep the Apache license header at the top of each file (it's an Apache-2.0 vendored copy).

- [ ] **Step 2: Strip the upstream test module**

`catalog.rs` is 2453 lines; lines ~997–2453 are `#[cfg(test)] mod tests`. Delete that module
entirely (loom's `catalog_contract` replaces it, and inline `#[cfg(test)]` violates loom's
no-inline-tests rule). After deletion the file is ~996 impl lines.

- [ ] **Step 3: Port sqlx 0.8 `any` → sqlx 0.9 `postgres`**

The upstream import block is:
```rust
use sqlx::any::{AnyPoolOptions, AnyQueryResult, AnyRow, install_default_drivers};
use sqlx::{Any, AnyPool, Row, Transaction};
```
Replace with concrete Postgres types so the catalog shares loom's sqlx 0.9 Postgres stack:
```rust
use sqlx::postgres::{PgPoolOptions, PgQueryResult, PgRow};
use sqlx::{PgPool, Postgres, Row, Transaction};
```
Then mechanically port the ~30 sqlx touch-points (the upstream has ~15 `sqlx::` refs / ~24
`.bind`/`.fetch`/`.execute` sites):
- `AnyPool` → `PgPool`; `AnyPoolOptions` → `PgPoolOptions`; `AnyRow` → `PgRow`;
  `AnyQueryResult` → `PgQueryResult`; `Transaction<'_, Any>` → `Transaction<'_, Postgres>`.
- **Remove every `install_default_drivers()` call** (sqlx 0.9 Postgres needs no driver install).
- **Drop the `SqlBindStyle` enum and its branching.** Postgres is always `$1..$N`, so render
  placeholders directly as `$N`. Where the upstream builds SQL with a configurable placeholder,
  hardcode the Postgres form. (Grep for `SqlBindStyle`, `bind_style`, and any `?`-vs-`$`
  placeholder helper; the `SQL_CATALOG_PROP_BIND_STYLE` constant can stay defined but unused, or
  be removed.)
- `sqlx::query(&sql)` / `.bind(..)` / `.fetch_all`/`.fetch_optional`/`.execute` keep the same
  shape; only the row/pool types change. `row.try_get::<T, _>(col)` is unchanged.
- The two `CREATE TABLE IF NOT EXISTS` statements (`iceberg_tables`,
  `iceberg_namespace_properties`) are standard SQL — keep as-is (Postgres accepts the
  `VARCHAR(n)` DDL). These run at catalog init, **not** via loom's migrator.

Keep loom's edits to the file **minimal** — this is a faithful port, not a redesign. The
slice-2 transaction-coupling change to `update_table` is out of scope here.

- [ ] **Step 4: `error.rs` + module wiring**

`error.rs` maps sqlx errors to `iceberg::Error` — port its sqlx error type references the same
way (`sqlx::Error` is version-agnostic in shape; adjust any `Any`-specific variant). In
`src/control-plane/postgres/src/lib.rs` add `pub mod iceberg_sql_catalog;`. In `mod.rs`, keep
`pub use catalog::*;` and the `SqlCatalog` / builder exports; drop the upstream rustdoc example
that references the published crate name.

- [ ] **Step 5: Build the crate (offline; the vendored catalog is runtime sqlx, no `.sqlx` needed)**

The vendored catalog uses runtime `sqlx::query(&sql)` (not the compile-time `query!` macro), so
it needs no `.sqlx` entries. Build:
```bash
buck2 build //src/control-plane/postgres:postgres > /tmp/b.log 2>&1; tail -6 /tmp/b.log
```
Expected: BUILD SUCCEEDED. Iterate on the sqlx type-swaps until it compiles. If a type or method
moved between sqlx 0.8 and 0.9, fix locally (the surface is small).

- [ ] **Step 6: Commit**

```bash
buck2 run //tools:rustfmt -- --edition 2024 src/control-plane/postgres/src/iceberg_sql_catalog/mod.rs \
  src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs \
  src/control-plane/postgres/src/iceberg_sql_catalog/error.rs src/control-plane/postgres/src/lib.rs
git add src/control-plane/postgres/src/iceberg_sql_catalog src/control-plane/postgres/src/lib.rs src/control-plane/postgres/BUCK
git status
git commit -m "feat(iceberg): vendor the SQL catalog, ported sqlx 0.8(any) -> 0.9(postgres)"
git rev-parse --short HEAD
```

---

## Task 3: The `iceberg_mirror.*` mirror schema migration

**Files:**
- Create: `src/control-plane/postgres/migrations/0012_iceberg_mirror.sql`

The mirror parallels the slice of DuckLake's catalog the read path needs, with **loom-owned**
catalog-global snapshot ids and MVCC. Keyed by `(table_namespace, table_name)`, versioned by
`begin_snapshot` / `end_snapshot` (nullable; `NULL` = still live). A single global counter
issues monotonic snapshot ids across all tables.

> **Scope note:** `core::Catalog` returns `FileRef { path, record_count, file_size_bytes }` —
> **no column stats**. So slice 1 does not project, store, or decode per-column stats; there is
> no `iceberg_mirror.column_stat` table here. Stats are write-side metadata
> (`DataFile.column_stats`) and land in slice 2.

- [ ] **Step 1: Write the migration**

Create `src/control-plane/postgres/migrations/0012_iceberg_mirror.sql`:

```sql
create schema if not exists iceberg_mirror;

-- Catalog-global monotonic snapshot ids (loom's authority, independent of Iceberg's
-- per-table snapshot ids). One row per loom snapshot.
create table iceberg_mirror.snapshot (
    snapshot_id         bigint      primary key,
    snapshot_time       timestamptz not null default now(),
    schema_version      bigint      not null default 0,
    iceberg_snapshot_id bigint
);

-- Table existence, MVCC-versioned.
create table iceberg_mirror.table (
    table_id         bigserial primary key,
    table_namespace  text      not null,
    table_name       text      not null,
    begin_snapshot   bigint    not null,
    end_snapshot     bigint
);
create index iceberg_table_lookup_idx
    on iceberg_mirror.table (table_namespace, table_name, begin_snapshot);

-- Column schema, MVCC-versioned. `column_type` holds the Iceberg primitive type name
-- (e.g. "long","string"); the adapter maps it to a loom logical type on read.
create table iceberg_mirror.column (
    table_id        bigint  not null references iceberg_mirror.table(table_id),
    column_order    bigint  not null,
    column_name     text    not null,
    column_type     text    not null,
    nulls_allowed   boolean not null,
    begin_snapshot  bigint  not null,
    end_snapshot    bigint
);
create index iceberg_column_live_idx
    on iceberg_mirror.column (table_id, begin_snapshot);

-- Data files, MVCC-versioned.
create table iceberg_mirror.data_file (
    data_file_id    bigserial primary key,
    table_id        bigint  not null references iceberg_mirror.table(table_id),
    path            text    not null,
    file_format     text    not null,
    record_count    bigint  not null,
    file_size_bytes bigint  not null,
    begin_snapshot  bigint  not null,
    end_snapshot    bigint
);
create index iceberg_data_file_live_idx
    on iceberg_mirror.data_file (table_id, begin_snapshot);
```

- [ ] **Step 2: Verify the migrations target builds**

```bash
buck2 build //src/control-plane/postgres:migrations > /tmp/mig.log 2>&1; tail -3 /tmp/mig.log
```
Expected: BUILD SUCCEEDED. (Task 8's contract boots `fresh_db()` and will fail loudly if the SQL
is malformed.)

- [ ] **Step 3: Commit**

```bash
git add src/control-plane/postgres/migrations/0012_iceberg_mirror.sql
git status
git commit -m "feat(iceberg): iceberg_mirror.* schema — loom-owned MVCC projection of Iceberg metadata"
git rev-parse --short HEAD
```

---

## Task 4: `iceberg_type.rs` — logical type decode (pure logic, TDD)

**Files:**
- Create: `src/control-plane/postgres/src/iceberg_type.rs`
- Modify: `src/control-plane/postgres/src/lib.rs` (declare `pub mod iceberg_type;`)
- Test: `src/control-plane/postgres/tests/iceberg_type.rs`
- Modify: `src/control-plane/postgres/BUCK` (a plain `rust_test`; pure logic → RE)

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/postgres/tests/iceberg_type.rs`:

```rust
use control_plane_core::BaseType;
use control_plane_postgres::iceberg_type::logical_from_iceberg;

#[test]
fn logical_from_iceberg_maps_the_closed_vocabulary() {
    assert_eq!(logical_from_iceberg("int"), Some(BaseType::Integer));
    assert_eq!(logical_from_iceberg("long"), Some(BaseType::Long));
    assert_eq!(logical_from_iceberg("double"), Some(BaseType::Double));
    assert_eq!(logical_from_iceberg("boolean"), Some(BaseType::Boolean));
    assert_eq!(logical_from_iceberg("string"), Some(BaseType::String));
    assert_eq!(logical_from_iceberg("date"), Some(BaseType::Date));
    assert_eq!(logical_from_iceberg("timestamp"), Some(BaseType::Timestamp));
    assert_eq!(logical_from_iceberg("timestamptz"), Some(BaseType::Timestamp));
}

#[test]
fn logical_from_iceberg_rejects_unknown() {
    assert_eq!(logical_from_iceberg("fixed[16]"), None);
    assert_eq!(logical_from_iceberg("decimal(9,2)"), None);
    assert_eq!(logical_from_iceberg(""), None);
}
```

- [ ] **Step 2: Run it to confirm it fails to compile (module absent)**

```bash
buck2 test //src/control-plane/postgres:iceberg_type > /tmp/t.log 2>&1; grep -E "FAIL|error\[|cannot find" /tmp/t.log | head
```
Expected: compile error — module/function not found. (If buck2 says the target is unknown, add
Step 4's target first, then re-run.)

- [ ] **Step 3: Implement `iceberg_type.rs`**

```rust
//! Iceberg physical type names → loom logical types for the `iceberg_mirror` read path. The
//! closed vocabulary is authoritative: an Iceberg type loom has no logical name for maps to
//! `None` (the adapter surfaces it as an explicit error rather than leaking a raw Iceberg type
//! string into `core`). Mirror of `ducklake_type::logical_from_ducklake`.

use control_plane_core::BaseType;

/// Iceberg primitive type name → loom logical base type (read path, `schema()`).
/// `None` for a type loom has no logical name for (decimal, fixed, uuid, binary, …).
pub fn logical_from_iceberg(physical: &str) -> Option<BaseType> {
    match physical.trim().to_ascii_lowercase().as_str() {
        "int" => Some(BaseType::Integer),
        "long" => Some(BaseType::Long),
        "double" => Some(BaseType::Double),
        "boolean" => Some(BaseType::Boolean),
        "string" => Some(BaseType::String),
        "date" => Some(BaseType::Date),
        "timestamp" | "timestamptz" => Some(BaseType::Timestamp),
        _ => None,
    }
}
```
Add `pub mod iceberg_type;` to `lib.rs` (must be `pub` for the integration test to import it,
mirroring `ducklake_type`).

- [ ] **Step 4: Add the test target to BUCK**

```python
rust_test(
    name = "iceberg_type",
    crate = "iceberg_type",
    srcs = ["tests/iceberg_type.rs"],
    edition = "2024",
    deps = [":postgres", "//src/control-plane/core:core"],
)
```
(Match the exact core-crate alias used elsewhere in this BUCK.)

- [ ] **Step 5: Run — expect PASS**

```bash
buck2 test //src/control-plane/postgres:iceberg_type > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```

- [ ] **Step 6: Commit**

```bash
buck2 run //tools:rustfmt -- --edition 2024 src/control-plane/postgres/src/iceberg_type.rs src/control-plane/postgres/src/lib.rs src/control-plane/postgres/tests/iceberg_type.rs
git add src/control-plane/postgres/src/iceberg_type.rs src/control-plane/postgres/src/lib.rs src/control-plane/postgres/tests/iceberg_type.rs src/control-plane/postgres/BUCK
git status
git commit -m "feat(iceberg): logical_from_iceberg type decode (pure logic)"
git rev-parse --short HEAD
```

---

## Task 5: `iceberg_catalog.rs` — `impl Catalog` over the mirror

**Files:**
- Create: `src/control-plane/postgres/src/iceberg_catalog.rs`
- Modify: `src/control-plane/postgres/src/lib.rs` (declare module + export the handle)
- Modify: `src/control-plane/postgres/.sqlx/` (regenerated cache — committed)

A near-mechanical parallel of `catalog.rs` querying `iceberg_mirror.*`. Define a small
`IcebergCatalog { pool: PgPool }` handle (the read adapter is independent of `PgControlPlane`).

- [ ] **Step 1: Handle + module + `resolve_table`**

```rust
use async_trait::async_trait;
use control_plane_core::{
    BaseType, Catalog, ColumnDef, ControlPlaneError, FileRef, Page, PageReq, Result, Snapshot,
    SnapshotId, TableRef, TableSchema,
};
use sqlx::PgPool;

use crate::backend;
use crate::iceberg_type::logical_from_iceberg;

/// Read adapter serving `core::Catalog` from the loom-owned `iceberg_mirror.*` projection.
#[derive(Clone)]
pub struct IcebergCatalog {
    pub pool: PgPool,
}

impl IcebergCatalog {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    async fn resolve_table(&self, table: &TableRef, at: SnapshotId) -> Result<i64> {
        sqlx::query_scalar!(
            "select table_id as \"table_id!\" from iceberg_mirror.table \
             where table_namespace = $1 and table_name = $2 \
               and begin_snapshot <= $3 and (end_snapshot is null or end_snapshot > $3)",
            table.schema,
            table.name,
            at.0,
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?
        .ok_or_else(|| {
            ControlPlaneError::NotFound(format!("{}.{} @ {}", table.schema, table.name, at.0))
        })
    }
}
```

- [ ] **Step 2: The four `Catalog` methods**

These mirror `catalog.rs` exactly; the `exists(...)` snapshot subquery filters by table liveness
so `current_snapshot`/`snapshots` are drop-aware.

```rust
#[async_trait]
impl Catalog for IcebergCatalog {
    #[tracing::instrument(skip(self), level = "debug")]
    async fn current_snapshot(&self, table: &TableRef) -> Result<Snapshot> {
        let row = sqlx::query!(
            "select sn.snapshot_id as \"snapshot_id!\", sn.snapshot_time as \"snapshot_time!\", sn.schema_version as \"schema_version!\" \
             from iceberg_mirror.snapshot sn \
             where exists ( \
                 select 1 from iceberg_mirror.table t \
                 where t.table_namespace = $1 and t.table_name = $2 \
                   and t.begin_snapshot <= sn.snapshot_id and (t.end_snapshot is null or t.end_snapshot > sn.snapshot_id)) \
             order by sn.snapshot_id desc limit 1",
            table.schema, table.name,
        )
        .fetch_optional(&self.pool).await.map_err(backend)?
        .ok_or_else(|| ControlPlaneError::NotFound(format!("{}.{}", table.schema, table.name)))?;
        Ok(Snapshot { id: SnapshotId(row.snapshot_id), time: row.snapshot_time, schema_version: row.schema_version })
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn snapshots(&self, table: &TableRef, _page: PageReq) -> Result<Page<Snapshot>> {
        let rows = sqlx::query!(
            "select sn.snapshot_id as \"snapshot_id!\", sn.snapshot_time as \"snapshot_time!\", sn.schema_version as \"schema_version!\" \
             from iceberg_mirror.snapshot sn \
             where exists ( \
                 select 1 from iceberg_mirror.table t \
                 where t.table_namespace = $1 and t.table_name = $2 \
                   and t.begin_snapshot <= sn.snapshot_id and (t.end_snapshot is null or t.end_snapshot > sn.snapshot_id)) \
             order by sn.snapshot_id",
            table.schema, table.name,
        )
        .fetch_all(&self.pool).await.map_err(backend)?;
        if rows.is_empty() {
            return Err(ControlPlaneError::NotFound(format!("{}.{}", table.schema, table.name)));
        }
        Ok(Page::from_full(rows.into_iter().map(|r| Snapshot {
            id: SnapshotId(r.snapshot_id), time: r.snapshot_time, schema_version: r.schema_version,
        }).collect()))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn files(&self, table: &TableRef, at: SnapshotId, _page: PageReq) -> Result<Page<FileRef>> {
        let tid = self.resolve_table(table, at).await?;
        let rows = sqlx::query!(
            "select path as \"path!\", record_count as \"record_count!\", file_size_bytes as \"file_size_bytes!\" \
             from iceberg_mirror.data_file \
             where table_id = $1 and begin_snapshot <= $2 and (end_snapshot is null or end_snapshot > $2) \
             order by data_file_id",
            tid, at.0,
        )
        .fetch_all(&self.pool).await.map_err(backend)?;
        Ok(Page::from_full(rows.into_iter().map(|r| FileRef {
            path: r.path, record_count: r.record_count, file_size_bytes: r.file_size_bytes,
        }).collect()))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn schema(&self, table: &TableRef, at: SnapshotId) -> Result<TableSchema> {
        let tid = self.resolve_table(table, at).await?;
        let rows = sqlx::query!(
            "select column_order as \"column_order!\", column_name as \"column_name!\", column_type as \"column_type!\", nulls_allowed as \"nulls_allowed!\" \
             from iceberg_mirror.column \
             where table_id = $1 and begin_snapshot <= $2 and (end_snapshot is null or end_snapshot > $2) \
             order by column_order",
            tid, at.0,
        )
        .fetch_all(&self.pool).await.map_err(backend)?;
        let columns = rows.into_iter().map(|r| {
            let ty = logical_from_iceberg(&r.column_type)
                .map(BaseType::canonical_name)
                .ok_or_else(|| ControlPlaneError::Backend(Box::<dyn std::error::Error + Send + Sync>::from(
                    format!("catalog column type {:?} has no loom logical type", r.column_type))))?;
            Ok(ColumnDef { order: r.column_order, name: r.column_name, ty: ty.to_string(), nullable: r.nulls_allowed })
        }).collect::<Result<Vec<_>>>()?;
        Ok(TableSchema { columns })
    }
}
```
Add `pub mod iceberg_catalog;` to `lib.rs`.

- [ ] **Step 3: Regenerate the `.sqlx` cache**

```bash
./tools/sqlx-prepare.sh > /tmp/sqlx.log 2>&1; tail -5 /tmp/sqlx.log
```
Expected: new `.sqlx/query-*.json` for the Iceberg-mirror queries. Commit them.

- [ ] **Step 4: Build**

```bash
buck2 build //src/control-plane/postgres:postgres > /tmp/b.log 2>&1; tail -3 /tmp/b.log
```
Expected: BUILD SUCCEEDED.

- [ ] **Step 5: Commit**

```bash
buck2 run //tools:rustfmt -- --edition 2024 src/control-plane/postgres/src/iceberg_catalog.rs src/control-plane/postgres/src/lib.rs
git add src/control-plane/postgres/src/iceberg_catalog.rs src/control-plane/postgres/src/lib.rs src/control-plane/postgres/.sqlx
git status
git commit -m "feat(iceberg): IcebergCatalog impl core::Catalog over the iceberg_mirror projection"
git rev-parse --short HEAD
```

---

## Task 6: `iceberg_mirror.rs` — the projection (snapshot alloc + MVCC writes)

**Files:**
- Create: `src/control-plane/postgres/src/iceberg_mirror.rs`
- Modify: `src/control-plane/postgres/src/lib.rs` (declare module)
- Modify: `src/control-plane/postgres/.sqlx/` (regenerated — INSERT/UPDATEs added)

The real, shared-with-slice-2 component: it owns loom's snapshot allocation + MVCC bookkeeping
and writes the `iceberg_mirror.*` rows. The seeder (Task 7) builds the neutral `Projected*`
structs from the `iceberg` writer output and calls these. No standalone unit test; proven by the
contract.

- [ ] **Step 1: Snapshot allocator + table upsert**

```rust
//! Projection of canonical Iceberg table metadata into the loom-owned `iceberg_mirror.*`
//! schema. loom allocates a catalog-global monotonic `SnapshotId` per appended batch and records
//! MVCC `begin/end_snapshot`. Shared with the slice-2 write path.

use control_plane_core::{Result, SnapshotId};
use sqlx::PgConnection;

use crate::backend;

/// A neutral view of one committed Iceberg data file the projection writes.
pub struct ProjectedFile {
    pub path: String,
    pub file_format: String, // "parquet"
    pub record_count: i64,
    pub file_size_bytes: i64,
}

/// A neutral column definition (name, Iceberg primitive type name, nullability), in order.
pub struct ProjectedColumn {
    pub order: i64,
    pub name: String,
    pub iceberg_type: String,
    pub nullable: bool,
}

/// Allocate the next catalog-global snapshot id and insert its `iceberg_mirror.snapshot` row.
pub async fn next_snapshot(conn: &mut PgConnection, iceberg_snapshot_id: Option<i64>) -> Result<SnapshotId> {
    let id = sqlx::query_scalar!(
        "select coalesce(max(snapshot_id), 0) + 1 as \"next!\" from iceberg_mirror.snapshot"
    ).fetch_one(&mut *conn).await.map_err(backend)?;
    sqlx::query!(
        "insert into iceberg_mirror.snapshot (snapshot_id, iceberg_snapshot_id) values ($1, $2)",
        id, iceberg_snapshot_id,
    ).execute(&mut *conn).await.map_err(backend)?;
    Ok(SnapshotId(id))
}

/// Ensure a live `iceberg_mirror.table` row exists for `(ns, name)`, returning its `table_id`.
pub async fn ensure_table(conn: &mut PgConnection, ns: &str, name: &str, at: SnapshotId) -> Result<i64> {
    if let Some(tid) = sqlx::query_scalar!(
        "select table_id as \"id!\" from iceberg_mirror.table \
         where table_namespace = $1 and table_name = $2 and end_snapshot is null",
        ns, name,
    ).fetch_optional(&mut *conn).await.map_err(backend)? {
        return Ok(tid);
    }
    let tid = sqlx::query_scalar!(
        "insert into iceberg_mirror.table (table_namespace, table_name, begin_snapshot) \
         values ($1, $2, $3) returning table_id as \"id!\"",
        ns, name, at.0,
    ).fetch_one(&mut *conn).await.map_err(backend)?;
    Ok(tid)
}
```

- [ ] **Step 2: Column + data-file projection, and `mark_dropped`**

```rust
/// Write the column rows for loom snapshot `at`.
pub async fn project_columns(conn: &mut PgConnection, table_id: i64, at: SnapshotId, columns: &[ProjectedColumn]) -> Result<()> {
    for c in columns {
        sqlx::query!(
            "insert into iceberg_mirror.column \
             (table_id, column_order, column_name, column_type, nulls_allowed, begin_snapshot) \
             values ($1, $2, $3, $4, $5, $6)",
            table_id, c.order, c.name, c.iceberg_type, c.nullable, at.0,
        ).execute(&mut *conn).await.map_err(backend)?;
    }
    Ok(())
}

/// Write the data-file rows for loom snapshot `at`.
pub async fn project_files(conn: &mut PgConnection, table_id: i64, at: SnapshotId, files: &[ProjectedFile]) -> Result<()> {
    for f in files {
        sqlx::query!(
            "insert into iceberg_mirror.data_file \
             (table_id, path, file_format, record_count, file_size_bytes, begin_snapshot) \
             values ($1, $2, $3, $4, $5, $6)",
            table_id, f.path, f.file_format, f.record_count, f.file_size_bytes, at.0,
        ).execute(&mut *conn).await.map_err(backend)?;
    }
    Ok(())
}

/// Mark a table (and its live columns/files) dropped at `at` — sets `end_snapshot = at` on every
/// currently-live row. Drives `CatalogSeed::drop_table` and the MVCC `end`-bound the delete
/// contract exercises.
pub async fn mark_dropped(conn: &mut PgConnection, ns: &str, name: &str, at: SnapshotId) -> Result<()> {
    let tid = sqlx::query_scalar!(
        "update iceberg_mirror.table set end_snapshot = $3 \
         where table_namespace = $1 and table_name = $2 and end_snapshot is null \
         returning table_id as \"id!\"",
        ns, name, at.0,
    ).fetch_one(&mut *conn).await.map_err(backend)?;
    sqlx::query!("update iceberg_mirror.column set end_snapshot = $2 where table_id = $1 and end_snapshot is null", tid, at.0)
        .execute(&mut *conn).await.map_err(backend)?;
    sqlx::query!("update iceberg_mirror.data_file set end_snapshot = $2 where table_id = $1 and end_snapshot is null", tid, at.0)
        .execute(&mut *conn).await.map_err(backend)?;
    Ok(())
}
```

- [ ] **Step 3: Declare module, regenerate `.sqlx`, build**

Add `pub mod iceberg_mirror;` to `lib.rs`. Then:
```bash
./tools/sqlx-prepare.sh > /tmp/sqlx.log 2>&1; tail -5 /tmp/sqlx.log
buck2 build //src/control-plane/postgres:postgres > /tmp/b.log 2>&1; tail -3 /tmp/b.log
```

- [ ] **Step 4: Commit**

```bash
buck2 run //tools:rustfmt -- --edition 2024 src/control-plane/postgres/src/iceberg_mirror.rs src/control-plane/postgres/src/lib.rs
git add src/control-plane/postgres/src/iceberg_mirror.rs src/control-plane/postgres/src/lib.rs src/control-plane/postgres/.sqlx
git status
git commit -m "feat(iceberg): iceberg_mirror projection — snapshot alloc + MVCC table/column/file writes"
git rev-parse --short HEAD
```

---

## Task 7: `IcebergWriter` seeder — drive the vendored catalog + iceberg, fill the mirror

**Files:**
- Modify: `src/control-plane/postgres/src/fixture.rs` (add `IcebergWriter`)
- Possibly modify: `src/control-plane/postgres/src/lib.rs` / `PgFixture` (expose a `PgPool` +
  a `pg_dsn()` the catalog and a fresh pool can use)

The only place `iceberg` + the vendored catalog are *called*. It builds loom's vendored
`SqlCatalog` over the hermetic Postgres + a `file://` warehouse, creates a namespace+table,
appends each row-batch as an Iceberg snapshot, derives `Projected*` from the writer output, and
projects them into the mirror under a fresh loom snapshot.

- [ ] **Step 1: VERIFY the iceberg 0.9.1 writer/transaction API**

Confirm against `https://docs.rs/iceberg/0.9.1`: `Catalog::{create_namespace, create_table,
load_table}` (+ `TableCreation`, `Schema`/`NestedField`/`PrimitiveType`); the writer chain
(`ParquetWriterBuilder::new(WriterProperties::default(), schema)` →
`RollingFileWriterBuilder::new_with_default_file_size(parquet_builder, file_io, location_gen,
name_gen)` → `DataFileWriterBuilder::new(rolling)` → `.build(None).await` → `.write(batch).await`
→ `.close().await -> Vec<DataFile>`); `DefaultLocationGenerator::new(table.metadata().clone())`,
`DefaultFileNameGenerator::new(name, None, DataFileFormat::Parquet)`; and
`Transaction::new(&table).fast_append()…add_data_files(Vec<DataFile>)…commit(&catalog) -> Table`.
Note `Table::file_io()` and `Table::metadata().current_snapshot_id()`.

- [ ] **Step 2: Implement `IcebergWriter` + the seed loop**

Sketch (adapt to the verified API + the vendored catalog's constructor):

```rust
use std::collections::HashMap;
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};          // VERIFY paths
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};              // VERIFY paths
use crate::iceberg_sql_catalog::SqlCatalog;                                 // loom's vendored catalog
use crate::iceberg_mirror::{ProjectedColumn, ProjectedFile, ensure_table, mark_dropped, next_snapshot, project_columns, project_files};

/// Test-only seeder: writes real canonical Iceberg tables (via the vendored catalog + iceberg)
/// into the hermetic Postgres + a temp file:// warehouse, then projects them into iceberg_mirror.*.
pub struct IcebergWriter {
    pool: PgPool,                 // the fixture's pool (for the mirror projection)
    pg_dsn: String,               // catalog URI for the vendored SqlCatalog
    warehouse: tempfile::TempDir,
}

impl IcebergWriter {
    pub fn new(pool: PgPool, pg_dsn: String) -> Self {
        Self { pool, pg_dsn, warehouse: tempfile::tempdir().unwrap() }
    }

    async fn catalog(&self) -> SqlCatalog {
        // VERIFY: the vendored catalog's constructor/builder signature after the sqlx-0.9 port.
        // It takes the Postgres DSN + a file:// warehouse + a FileIO; it create-table-if-not-exists
        // its JDBC tables on first use.
        let warehouse = format!("file://{}", self.warehouse.path().display());
        SqlCatalog::connect("loom", &self.pg_dsn, &warehouse).await.expect("build vendored SqlCatalog")
    }

    fn iceberg_type(logical: &str) -> Type {
        match logical {
            "long" => Type::Primitive(PrimitiveType::Long),
            "integer" => Type::Primitive(PrimitiveType::Int),
            "double" => Type::Primitive(PrimitiveType::Double),
            "boolean" => Type::Primitive(PrimitiveType::Boolean),
            "string" => Type::Primitive(PrimitiveType::String),
            "date" => Type::Primitive(PrimitiveType::Date),
            "timestamp" => Type::Primitive(PrimitiveType::Timestamp),
            other => panic!("seed: unmapped logical type {other:?}"),
        }
    }

    /// Create the table if absent and append each batch as its own loom snapshot.
    /// `columns`: (name, loom-logical-type, nullable). Returns the per-batch loom snapshot ids.
    pub async fn seed(&self, ns: &str, name: &str, columns: &[(String, String, bool)], batches: &[usize]) -> Vec<i64> {
        let catalog = self.catalog().await;
        // create namespace (ignore AlreadyExists) + table from `columns`              // VERIFY API
        let mut snapshots = Vec::new();
        for (batch_idx, &rows) in batches.iter().enumerate() {
            let table = catalog.load_table(/* TableIdent for ns.name */).await.expect("load"); // VERIFY
            let data_files = write_batch_parquet(&table, columns, rows).await;          // writer chain // VERIFY
            let table = iceberg::transaction::Transaction::new(&table)                  // VERIFY
                .fast_append().add_data_files(data_files.clone()) /* VERIFY */
                .commit(&catalog).await.expect("commit");
            let ice_snap = table.metadata().current_snapshot_id();                      // VERIFY accessor (Option<i64>)
            let mut tx = self.pool.begin().await.unwrap();
            let at = next_snapshot(&mut tx, ice_snap).await.unwrap();
            let tid = ensure_table(&mut tx, ns, name, at).await.unwrap();
            if batch_idx == 0 {
                project_columns(&mut tx, tid, at, &projected_columns(columns)).await.unwrap();
            }
            project_files(&mut tx, tid, at, &projected_files(&data_files)).await.unwrap();
            tx.commit().await.unwrap();
            snapshots.push(at.0);
        }
        snapshots
    }

    pub async fn drop_table(&self, ns: &str, name: &str) -> i64 {
        let mut tx = self.pool.begin().await.unwrap();
        let at = next_snapshot(&mut tx, None).await.unwrap();
        mark_dropped(&mut tx, ns, name, at).await.unwrap();
        tx.commit().await.unwrap();
        at.0
    }
}
```

Helper notes for the implementer:
- `write_batch_parquet`: build a `RecordBatch` of `rows` rows matching the contract's
  `(id long, name string)` schema (`id` = `0..rows`, `name` = `format!("r{i}")`), then run the
  verified writer chain to get `Vec<DataFile>`. Use the arrow version `iceberg` re-exports.
- `projected_columns(columns)`: map each `(name, logical, nullable)` to a `ProjectedColumn` with
  `iceberg_type` = the Iceberg primitive *name string* (`"long"`, `"string"`, …) matching what
  `logical_from_iceberg` decodes — i.e. the lowercase Iceberg primitive name, not the loom
  logical name (they coincide for `long`/`string`/`double`/`boolean`/`date`; map `integer`→`int`).
- `projected_files(&data_files)`: from each committed `DataFile` take `file_path`,
  `record_count`, `file_size_in_bytes`; `file_format` = `"parquet"`. (No stats — slice 1.)

- [ ] **Step 3: Build (no test wiring yet)**

```bash
buck2 build //src/control-plane/postgres:postgres > /tmp/b.log 2>&1; tail -6 /tmp/b.log
```
Iterate on `// VERIFY` calls until it compiles. `tempfile` is already a dep.

- [ ] **Step 4: Commit**

```bash
buck2 run //tools:rustfmt -- --edition 2024 src/control-plane/postgres/src/fixture.rs src/control-plane/postgres/src/lib.rs
git add src/control-plane/postgres/src/fixture.rs src/control-plane/postgres/src/lib.rs
git status
git commit -m "feat(iceberg): IcebergWriter test seeder — vendored catalog + iceberg writes + mirror projection"
git rev-parse --short HEAD
```

---

## Task 8: The contract test — `iceberg_passes_catalog_contract`

**Files:**
- Create: `src/control-plane/postgres/tests/iceberg_catalog.rs`
- Modify: `src/control-plane/postgres/BUCK` (new `loom_fixture_test` target — **no `duckdb`**)

- [ ] **Step 1: Write the test (the Iceberg twin of `tests/catalog.rs`)**

```rust
use async_trait::async_trait;
use control_plane_core::{SnapshotId, TableRef};
use control_plane_postgres::fixture::{IcebergWriter, PgFixture};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_testkit::{
    CatalogSeed, SeedSpec, SeededSnapshot, catalog_contract, catalog_delete_contract,
};

struct IcebergSeeder { writer: IcebergWriter }

#[async_trait]
impl CatalogSeed for IcebergSeeder {
    async fn seed(&self, spec: SeedSpec) -> Vec<SeededSnapshot> {
        let cols: Vec<(String, String, bool)> = spec.columns.into_iter()
            .map(|c| (c.name, c.ty, c.nullable)).collect(); // seeder maps logical -> Iceberg internally
        self.writer.seed(&spec.table.schema, &spec.table.name, &cols, &spec.row_batches).await
            .into_iter().map(|s| SeededSnapshot { snapshot: SnapshotId(s), files_added: 1 }).collect()
    }
    async fn drop_table(&self, table: &TableRef) -> SnapshotId {
        SnapshotId(self.writer.drop_table(&table.schema, &table.name).await)
    }
}

#[tokio::test]
async fn iceberg_passes_catalog_contract() {
    let fixture = PgFixture::start();
    let (cp, _db) = fixture.fresh_db().await;
    let catalog = IcebergCatalog::new(cp.pool.clone());          // VERIFY: expose the pool
    let seeder = IcebergSeeder { writer: IcebergWriter::new(cp.pool.clone(), fixture.pg_dsn()) }; // VERIFY: add pg_dsn()
    catalog_contract(&catalog, &seeder).await;
}

#[tokio::test]
async fn iceberg_passes_catalog_delete_contract() {
    let fixture = PgFixture::start();
    let (cp, _db) = fixture.fresh_db().await;
    let catalog = IcebergCatalog::new(cp.pool.clone());
    let seeder = IcebergSeeder { writer: IcebergWriter::new(cp.pool.clone(), fixture.pg_dsn()) };
    catalog_delete_contract(&catalog, &seeder).await;
}
```

> If `PgControlPlane` doesn't expose `pool`, add a `pub` accessor or have the fixture return the
> `PgPool`. Add `PgFixture::pg_dsn()` returning the `postgres://…?host=<socket>` DSN both the
> vendored catalog and a fresh `PgPool` accept — derive it from the socket-path/db-name the
> fixture already holds.

- [ ] **Step 2: Add the BUCK target (no `duckdb=True` — the seeder needs no DuckDB CLI)**

```python
loom_fixture_test(
    name = "iceberg_catalog",
    crate = "iceberg_catalog",
    srcs = ["tests/iceberg_catalog.rs"],
    crate_root = "tests/iceberg_catalog.rs",
    deps = [":postgres", "//src/control-plane/core:core", "//src/control-plane/testkit:testkit",
            "//third-party:async-trait", "//third-party:tokio"],
)
```
(Match the exact alias names the `catalog` target uses.)

- [ ] **Step 3: Run the contract — the headline assertion**

```bash
buck2 test //src/control-plane/postgres:iceberg_catalog > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|panicked" /tmp/t.log
```
Expected: both pass — the Iceberg adapter satisfies the same `Catalog` contract DuckLake does.
Debug against `/tmp/t.log`. Common first failures: snapshot-id monotonicity across batches, the
`(id long, name string)` Arrow batch not matching the created Iceberg schema, or the vendored
catalog's JDBC-table init racing `fresh_db()`.

- [ ] **Step 4: Commit**

```bash
buck2 run //tools:rustfmt -- --edition 2024 src/control-plane/postgres/tests/iceberg_catalog.rs
git add src/control-plane/postgres/tests/iceberg_catalog.rs src/control-plane/postgres/BUCK
git status
git commit -m "test(iceberg): IcebergCatalog passes catalog_contract + catalog_delete_contract"
git rev-parse --short HEAD
```

---

## Task 9: Full-suite verification

- [ ] **Step 1: Full build + test (local + RE)**

```bash
buck2 test //src/... > /tmp/all.log 2>&1; grep -E "Tests finished|FAIL" /tmp/all.log
```
Expected: all pass — the untouched DuckLake `catalog`/`catalog_delete` tests (coexistence proof),
the new Iceberg ones, and `sqlx-cache-check` (re-validates every committed `.sqlx`, now including
the Iceberg-mirror queries).

- [ ] **Step 2: clippy clean**

```bash
./tools/clippy-all.sh > /tmp/clippy.log 2>&1; tail -5 /tmp/clippy.log
```
Expected: no findings. (The vendored catalog is Apache code; if clippy flags style in it, prefer
`#[allow(...)]` at the module to keep it a faithful copy rather than rewriting.)

- [ ] **Step 3: prek all hooks**

```bash
buck2 run //tools:prek -- run --all-files > /tmp/prek.log 2>&1; grep -iE "failed|passed" /tmp/prek.log | tail -20
```
Expected: all Passed (rustfmt, clippy, file checks, reindeer-in-sync, no-inline-tests — confirm
the vendored catalog's tests were stripped). Commit anything the hooks rewrite.

- [ ] **Step 4: Confirm branch state, then finish**

```bash
git log --oneline main..HEAD
git rev-parse --abbrev-ref HEAD   # feat/iceberg-adapter-read-path
```
Then use **superpowers:finishing-a-development-branch** to open the PR.

---

## Notes carried forward to slices 2 & 3 (not built here)

- **Slice 2 (write path)** reuses `iceberg_mirror`'s `next_snapshot` / `ensure_table` /
  `project_columns` / `project_files` and folds the mirror upsert **into the vendored catalog's
  `update_table` transaction** — atomic pointer+mirror (the rebuildable cache is the backstop).
  Adds per-column stats (the `column_stat` table + `StatValue` decode) since `DataFile.column_stats`
  round-trips there. The ingest materializer can then target Iceberg.
- **Slice 3 (inline writes)** adds an `ActionEngine` impl for DuckLake-parity inline small writes.
- **Live-from-`Table` projection** (manifest scan via `Table::file_io()` rather than the writer's
  `Vec<DataFile>`) is the slice-2 path; slice 1 projects from the writer output the seeder holds.
