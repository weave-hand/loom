# Stream Merge Engines Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Generalize the two CDC current-state fold sites (`consolidate_stream` compaction and `build_merge_view` merge-on-read) from a hardcoded LastRow policy into a per-table, declared merge engine supporting **LastRow** (default, byte-identical), **FirstRow**, and **Versioned**.

**Architecture:** A per-CDC-table `merge_engine` column on `stream.stream_table` plus a per-type `version` property on the ontology select the precedence expression used at both fold sites. A new `MergeEngine` enum is the single source of truth; both fold sites render it to their respective `ORDER BY`/window expressions. The durable changelog is engine-agnostic and untouched.

**Tech Stack:** Rust, sqlx (compile-time `query!`), DataFusion, Iceberg, buck2 `loom_fixture_test`/`rust_test`, Postgres migrations.

## Global Constraints

Carried verbatim from the spec (`docs/superpowers/specs/2026-07-08-stream-merge-engines-design.md`); every task implicitly includes these:

- **Tests are `rust_test` / `loom_fixture_test` integration targets only — never inline `#[cfg(test)]`.** New fixture tests MUST use `loom_fixture_test` (`src/control-plane/postgres/defs.bzl`), not a bare `rust_test`, or the fixture env (PG binaries, MinIO, boot-slot dir) is missing. The `no-inline-tests` prek hook fails the build on any inline `#[test]`.
- **After changing any `query!`/`query_scalar!` SQL** (the new `version` column on `ontology.object_type`, the new `merge_engine` column on `stream.stream_table`), run `tools/sqlx-prepare.sh` and commit the `.sqlx/` change; `sqlx-cache-check` enforces freshness. **Cloud/automated sessions cannot run `sqlx-prepare.sh`** (it boots `initdb`/`postgres`, which refuse root) — runtime `AssertSqlSafe` queries are used where sqlx regen is unavailable (mirror `identity_for_table`).
- **Run prek before every commit:** `buck2 run //tools:prek -- run --all-files` (rustfmt is a separate hook; clippy-clean ≠ lint-clean). Markdown files end with exactly one trailing newline and no trailing whitespace.
- **Clippy is strict (pedantic + restriction):** no `unwrap`/`expect`/`indexing_slicing`/`panic`/`todo`/`map_err_ignore` in production lib/bin code. Use `#[expect(lint, reason = "...")]` for a justified local exception. Test code is exempted from the panic-safety lints via `loom_rust_test`/`loom_fixture_test`.
- **Non-CDC / LastRow paths must stay byte-identical.** Every new fold branch is gated on the engine value; the `Snapshot` precedence and the LastRow default are the unchanged baselines. Existing fixture tests (`stream_cdc_consolidate`, `stream_cdc_e2e`, `update_delete_e2e`, `merge_on_read`) must stay green and unchanged.
- **No framing leak.** The version column is a user column already in the logical schema; it is never a `loom_*` reserved column. Merge-on-read projects back to exactly the mirror data schema (the `_loom_*` helpers are dropped). `consolidate`'s output projection is unchanged.
- **Build/test commands** (from CLAUDE.md): build `buck2 build -v0 --console none //src/...`; test `buck2 test --console none //src/...` (cloud: add `-M none` to builds, scope tests, `buck2 clean` between heavy phases; full suite locally needs `-j 8` to avoid starving the 8 PG boot-slots).

### Declaration-validation placement (spec-aligned — revised after plan review)

All declaration validation — the **engine-choice immutability check** (redeclare with a different engine ⇒ `Conflict`) AND the **type-shape validation** (`merge_engine=versioned` requires a declared `version` property of an orderable type) — lives in `reconcile_stream_mode` (Task 4), exactly as the spec specifies. This is the only seam that is both (a) on every declarer's path (the HTTP `/models/{type}?mode=cdc` caller AND any direct `land_cdc`/`declare_cdc` caller) and (b) **reachable for a Versioned table**: the HTTP path infers types with `version: None` (there is no `?version=` param and `infer_object_type` never sets one), so a Versioned CDC table is only ever created by first defining a type with a `version` property via the control plane, then declaring CDC — which bottoms out in `land_cdc` → `reconcile_stream_mode`, NOT in the ingest handler. Validating in the ingest handler (an earlier draft's choice) would have made Versioned both unreachable via HTTP *and* unvalidated for direct callers (the plan reviewer's G7 finding). `reconcile_stream_mode` resolves the version column live via `version_for_table` (Task 2; **made executor-based** so it is callable from the `&mut PgConnection` reconcile holds) and its logical type via one join `query_scalar!`, inside the write tx — a rare, first-declare-only cost. The ingest handler keeps only the `?merge_engine=` query-param parse (unknown token ⇒ 400). (A future `?version=` HTTP param that lets an inferred type declare Versioned is a deferred follow-up; neither spec nor plan adds it.)

---

## File Structure

**Create:**
- `src/control-plane/postgres/migrations/0039_object_type_version.sql` — `ontology.object_type.version text`.
- `src/control-plane/postgres/migrations/0040_stream_table_merge_engine.sql` — `stream.stream_table.merge_engine text`.
- `src/control-plane/core/tests/version_property.rs` — `MergeEngine` + `.version()` builder unit tests.
- `src/control-plane/postgres/tests/version_for_table.rs` — `version_for_table` reverse-lookup.
- `src/services/ingest/tests/stream_merge_declare.rs` — `?merge_engine=` declaration validation (400s + Conflict).
- `src/services/query-api/tests/stream_merge_firstrow.rs` — FirstRow fold e2e.
- `src/services/query-api/tests/stream_merge_versioned.rs` — Versioned fold e2e.

**Modify (production):**
- `src/control-plane/core/src/stream.rs` — `MergeEngine` enum; `StreamMeta.merge_engine`; `StreamTables::declare_cdc` widened.
- `src/control-plane/core/src/ontology.rs` — `ObjectType.version` field + `.version()` builder.
- `src/control-plane/core/src/logical_type.rs` — `BaseType::is_version_orderable`.
- `src/control-plane/core/src/lib.rs` — re-export `MergeEngine`.
- `src/control-plane/memory/src/stream.rs` — memory `declare_cdc` carries the engine; `declare_stream` defaults LastRow.
- `src/control-plane/postgres/src/ontology.rs` — `version_for_table`; `define_type`/`get_type` read/write `version`.
- `src/control-plane/postgres/src/stream.rs` — `pg_declare_cdc`/`pg_stream_meta`/`declare_cdc` carry engine; `reconcile_stream_mode` engine immutability; `StreamDecl::Cdc` gains `merge_engine`.
- `src/control-plane/postgres/src/iceberg_landing.rs` — `CdcDecl.merge_engine`; `combine_stream_decl` threads it.
- `src/control-plane/testkit/src/lib.rs` — `ontology_contract` + `stream_tables_contract` version/engine assertions; the `declare_cdc` call sites widened.
- `src/services/ingest/src/http.rs` — `ModelQuery.merge_engine`; parse + validation; `CdcDecl` built with it; utoipa param.
- `src/services/engine-serving/src/consolidate.rs` — per-engine fold `ORDER BY`.
- `src/services/engine-serving/src/serving.rs` — `Precedence::Offset { engine, version_col }`; per-engine window ordering; `stream_meta_for_table`.

**Modify (mechanical, compiler-guided):** ~200 literal `ObjectType { … }` construction sites (add `version: None,`) and ~15 `declare_cdc(…)` call sites (add `MergeEngine::LastRow`) — Task 1 and Task 3 respectively.

---

## Task 1: Core types — `MergeEngine` enum, `ObjectType.version`, re-exports, and the literal-construction sweep

**Files:**
- Modify: `src/control-plane/core/src/stream.rs`
- Modify: `src/control-plane/core/src/ontology.rs:39-83` (struct + builder) and `:142-145` (builder method)
- Modify: `src/control-plane/core/src/lib.rs:63-72` (re-exports)
- Modify: ~200 literal `ObjectType { … }` sites (compiler-guided)
- Test: `src/control-plane/core/tests/version_property.rs` (new) + `src/control-plane/core/BUCK` (new `rust_test`)

**Interfaces:**
- Consumes: nothing (foundation).
- Produces (later tasks rely on these EXACT names/types):
  - `pub enum MergeEngine { LastRow, FirstRow, Versioned }` in `control_plane_core::stream`, re-exported as `control_plane_core::MergeEngine`, with `as_str(self) -> &'static str` and `impl FromStr` (loud `Validation` error on unknown token, mirroring `ActionKind`).
  - `ObjectType.version: Option<String>` (after `identity`), defaulting `None` in `ObjectType::build`; builder method `pub fn version(mut self, prop: impl Into<String>) -> Self`.

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/core/tests/version_property.rs`:

```rust
//! Unit tests for the MergeEngine enum and the ObjectType.version builder field.
//! External rust_test (no inline #[cfg(test)]) — see CLAUDE.md.

use control_plane_core::{MergeEngine, ObjectType};
use std::str::FromStr;

#[test]
fn merge_engine_round_trips_through_wire_token() {
    for engine in [MergeEngine::LastRow, MergeEngine::FirstRow, MergeEngine::Versioned] {
        let parsed = MergeEngine::from_str(engine.as_str()).expect("known token parses");
        assert_eq!(parsed, engine, "token round-trips");
    }
}

#[test]
fn merge_engine_unknown_token_is_a_loud_error() {
    assert!(MergeEngine::from_str("nonsense").is_err(), "unknown token rejected");
}

#[test]
fn version_builder_field_round_trips() {
    let ty = ObjectType::build("Widget", ("main", "widget"))
        .prop_req("id", "Long")
        .prop_req("seq", "Long")
        .identity("id")
        .version("seq")
        .done();
    assert_eq!(ty.identity.as_deref(), Some("id"));
    assert_eq!(ty.version.as_deref(), Some("seq"));
    // A type with no version property reads None.
    let no_version = ObjectType::build("Other", ("main", "other"))
        .prop_req("id", "Long")
        .identity("id")
        .done();
    assert!(no_version.version.is_none());
}
```

- [ ] **Step 2: Wire the test target**

In `src/control-plane/core/BUCK`, mirror an existing `rust_test` (e.g. the `identity` target) adding a `version-property` target whose `srcs = ["tests/version_property.rs"]` and `deps` include `":core"`.

- [ ] **Step 3: Run the test to verify it fails**

Run: `buck2 test --console none //src/control-plane/core:version-property`
Expected: FAIL — `MergeEngine` not found / `version` field/method missing.

- [ ] **Step 4: Add the `MergeEngine` enum**

In `src/control-plane/core/src/stream.rs`, after the `StreamKind` enum (after line 17), add:

```rust
/// The replace-class merge policy for a CDC current-state base: which row wins
/// per identity when both fold sites (`consolidate_stream` compaction and
/// `build_merge_view` merge-on-read) collapse multiple physical rows. A `-D`
/// winner drops the identity under ALL engines. The durable changelog is
/// engine-agnostic — engines govern only current-state. See
/// `docs/superpowers/specs/2026-07-08-stream-merge-engines-design.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MergeEngine {
    /// Greatest `loom_offset` per identity wins (the default; byte-identical to
    /// the pre-engine fold).
    LastRow,
    /// Smallest `loom_offset` wins — "first write wins"; later events for that
    /// identity are ignored for current-state (they still land in the changelog).
    FirstRow,
    /// A user-declared domain `version` column sets precedence (highest version
    /// wins; `loom_offset` tie-breaks). Handles out-of-order arrival.
    Versioned,
}

impl MergeEngine {
    /// The persisted `stream.stream_table.merge_engine` wire token.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            MergeEngine::LastRow => "last_row",
            MergeEngine::FirstRow => "first_row",
            MergeEngine::Versioned => "versioned",
        }
    }
}

impl std::str::FromStr for MergeEngine {
    type Err = crate::error::ControlPlaneError;

    /// Parse the persisted token. Unknown tokens are a loud error (a corrupt
    /// row / a bad `?merge_engine=` query param), never a silent default.
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "last_row" => Ok(MergeEngine::LastRow),
            "first_row" => Ok(MergeEngine::FirstRow),
            "versioned" => Ok(MergeEngine::Versioned),
            other => Err(crate::error::ControlPlaneError::Validation(format!(
                "unknown merge engine '{other}'"
            ))),
        }
    }
}
```

- [ ] **Step 5: Add `ObjectType.version` + builder**

In `src/control-plane/core/src/ontology.rs`, add the field to the `ObjectType` struct (after `identity`, line 49):

```rust
    /// The property that is this type's monotonic version/sequence column, used
    /// as precedence by the `Versioned` CDC merge engine. `None` = no version
    /// column (back-compatible). At most one version property per type.
    pub version: Option<String>,
```

In `ObjectType::build` (around line 71-82), add `version: None,` to the literal `ObjectType { … }`.

In `ObjectTypeBuilder`, after the `identity` method (after line 145), add:

```rust
    /// Declare `prop` as the type's version/sequence column (used by the
    /// `Versioned` merge engine). Like `identity`, NOT validated here or at
    /// define time: a dangling name surfaces only when the versioned engine
    /// reads it (the declaration surface validates orderability, not existence).
    pub fn version(mut self, prop: impl Into<String>) -> Self {
        self.inner.version = Some(prop.into());
        self
    }
```

- [ ] **Step 6: Re-export `MergeEngine`**

In `src/control-plane/core/src/lib.rs:72`, change:

```rust
pub use stream::{BucketOffsets, StreamKind, StreamMeta, StreamTables};
```
to
```rust
pub use stream::{BucketOffsets, MergeEngine, StreamKind, StreamMeta, StreamTables};
```

- [ ] **Step 7: Fix every literal `ObjectType { … }` construction (compiler-guided)**

Adding the `version` field breaks every literal construction (~200 sites across ~89 files — the struct definition, test seeders, and adapter internals; the 37 `ObjectType::build(…)` builder sites are unaffected). Loop:

```bash
buck2 build -v0 --console none //src/...
```

For each compile error `missing field 'version'`, add `version: None,` to that literal. (The handful of sites that should carry a real version are added in later tasks; here every site is `None`.) This is mechanical — the compiler enumerates every site. Do NOT grep-and-sed blindly; read each site to place the field next to `identity`.

- [ ] **Step 8: Run the new test + build the whole tree**

Run: `buck2 test --console none //src/control-plane/core:version-property`
Expected: PASS.

Run: `buck2 build -v0 --console none //src/...`
Expected: silent success (exit 0) — every `ObjectType` site now compiles.

- [ ] **Step 9: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(stream): add MergeEngine enum + ObjectType.version property

The replace-class merge policy for a CDC current-state base (LastRow /
FirstRow / Versioned) and the per-type version/sequence column that feeds
the Versioned engine. No behavior change yet — the field defaults None and
the enum is unused; both fold sites still hardcode LastRow."
```

---

## Task 2: `version` column on `ontology.object_type` + `version_for_table` + testkit contract

**Files:**
- Create: `src/control-plane/postgres/migrations/0039_object_type_version.sql`
- Modify: `src/control-plane/postgres/src/ontology.rs:34-46` (`define_type` insert), `:195-253` (`get_type`), `:621-633` (add `version_for_table` next to `identity_for_table`)
- Modify: `src/control-plane/testkit/src/lib.rs` — `ontology_contract` version round-trip assertions
- Regenerate + commit: `src/control-plane/postgres/.sqlx/` (via `tools/sqlx-prepare.sh`)
- Test: `src/control-plane/postgres/tests/version_for_table.rs` (new) + its `loom_fixture_test` target

**Interfaces:**
- Consumes: `ObjectType.version` (Task 1).
- Produces: `pub async fn version_for_table<'e, E: sqlx::PgExecutor<'e>>(ex: E, table: &TableRef) -> Result<Option<String>>` (`AssertSqlSafe`; mirrors `identity_for_table`'s logic but is **executor-generic** so both the fold sites with a `&PgPool` AND `reconcile_stream_mode` with a `&mut PgConnection` can call it) — consumed by the Versioned fold sites (Tasks 5, 6) and the Versioned declaration validation (Task 4).

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/postgres/tests/version_for_table.rs`, mirroring `src/control-plane/postgres/tests/identity_for_table.rs`:

```rust
//! `version_for_table` reverse-lookup: the version/sequence column name for the
//! object type stored at `table`. Mirrors `identity_for_table`.
//! loom_fixture_test (Postgres).

use control_plane_core::{ObjectType, TableRef, TypeName};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::ontology::version_for_table;

async fn define(cp: &PgControlPlane, name: &str, ty: ObjectType) {
    cp.ontology().define_type(ty).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn version_for_table_round_trips_and_defaults_none() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let with_version = ObjectType::build("Widget", ("main", "widget"))
        .prop_req("id", "Long")
        .prop_req("seq", "Long")
        .identity("id")
        .version("seq")
        .done();
    cp.ontology().define_type(with_version).await.unwrap();

    let table = TableRef { schema: "main".into(), name: "widget".into() };
    let got = version_for_table(&pool, &table).await.expect("lookup");
    assert_eq!(got.as_deref(), Some("seq"), "declared version column resolves");

    let without = ObjectType::build("Other", ("main", "other"))
        .prop_req("id", "Long")
        .identity("id")
        .done();
    cp.ontology().define_type(without).await.unwrap();
    let none = version_for_table(
        &pool,
        &TableRef { schema: "main".into(), name: "other".into() },
    )
    .await
    .expect("lookup");
    assert!(none.is_none(), "a type with no version property reads None");

    // A table with no bound type also reads None (no row).
    let absent = version_for_table(
        &pool,
        &TableRef { schema: "main".into(), name: "nope".into() },
    )
    .await
    .expect("lookup");
    assert!(absent.is_none(), "an unbound table reads None");
}
```

Add a `loom_fixture_test` target `version-for-table` to `src/control-plane/postgres/BUCK` mirroring the `identity-for-table` target (deps include `:postgres`, `//src/control-plane/core:core`, `//third-party:tokio`).

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test --console none //src/control-plane/postgres:version-for-table`
Expected: FAIL — `version_for_table` not found.

- [ ] **Step 3: Add the migration**

Create `src/control-plane/postgres/migrations/0039_object_type_version.sql`:

```sql
-- The version/sequence column for a type, used as precedence by the Versioned
-- CDC merge engine (road-stream-merge-engines). NULL = no version column
-- (the default; LastRow/FirstRow engines ignore it). Mirrors `identity`.
alter table ontology.object_type
    add column version text;
```

- [ ] **Step 4: Read/write the column in the postgres adapter**

In `src/control-plane/postgres/src/ontology.rs`:

`define_type` insert (lines 34-46) — add `version` to the column list + values + conflict update:

```rust
        sqlx::query!(
            "insert into ontology.object_type (name, table_schema, table_name, identity, version) \
             values ($1, $2, $3, $4, $5) \
             on conflict (name) do update set table_schema = excluded.table_schema, \
                 table_name = excluded.table_name, identity = excluded.identity, \
                 version = excluded.version",
            ty.name.0,
            ty.table.schema,
            ty.table.name,
            ty.identity,
            ty.version,
        )
```

`get_type` select (line 197) — add `version`:

```rust
        let row = sqlx::query!(
            "select table_schema, table_name, identity, version from ontology.object_type where name = $1",
            name.0,
        )
```

and the `ObjectType { … }` construction at the end of `get_type` (line 243-252) — add `version: row.version,`.

Add `version_for_table` next to `identity_for_table` (after line 633):

```rust
/// Reverse-lookup: the version/sequence column name for the object type stored
/// at `table`, or None if it has no declared version / does not exist. Used by
/// the Versioned merge engine's fold sites to read the precedence column live
/// (mirroring `identity_for_table` for the identity column), and by
/// `reconcile_stream_mode`'s Versioned declaration validation. Executor-generic
/// (not `&PgPool`-specific like `identity_for_table`) so the reconcile path can
/// call it with its `&mut PgConnection`.
// AssertSqlSafe: static query against ontology.object_type; sqlx regen
// unavailable in this env (initdb-as-root). Convert to query! when regenerating
// locally.
pub async fn version_for_table<'e, E: sqlx::PgExecutor<'e>>(
    ex: E,
    table: &TableRef,
) -> Result<Option<String>> {
    let row: Option<Option<String>> = sqlx::query_scalar(AssertSqlSafe(
        "select version from ontology.object_type \
         where table_schema = $1 and table_name = $2",
    ))
    .bind(&table.schema)
    .bind(&table.name)
    .fetch_optional(ex)
    .await
    .map_err(backend)?;
    Ok(row.flatten())
}
```

- [ ] **Step 5: Regenerate the `.sqlx` cache**

Run: `tools/sqlx-prepare.sh` (boots the pinned postgres, applies migrations, runs `cargo sqlx prepare`). Commit the resulting `.sqlx/` changes in Step 8. (This is a local/non-cloud step — if running in a cloud/automated session that cannot boot postgres, surface that to the user; the `AssertSqlSafe` `version_for_table` needs no cache entry, but the two changed `query!` macros do.)

- [ ] **Step 6: Extend the testkit `ontology_contract`**

In `src/control-plane/testkit/src/lib.rs`, in `ontology_contract` (near the identity round-trip assertions around line 683-694), after the customer/order identity assertions, add:

```rust
    // A declared version property round-trips through define_type/get_type.
    let versioned = o.get_type(&tn("Customer")).await.unwrap();
    assert_eq!(
        versioned.version.as_deref(),
        Some("id"),
        "declared version persists through define_type/get_type",
    );
```

…and seed the `Customer` type (around line 632) with `.version("id")` on its builder (Customer already has `identity: Some("id")` — add `.version("id")` to the builder, or `version: Some("id".into())` to the literal). Confirm the `Order` type (no version) still reads `version: None`:

```rust
    assert!(
        o.get_type(&tn("Order")).await.unwrap().version.is_none(),
        "an undeclared version stays None"
    );
```

(The memory fake needs no change — it stores the whole `ObjectType` struct, so it round-trips `version` for free now that Task 1 added the field.)

- [ ] **Step 7: Run the tests**

Run: `buck2 test --console none //src/control-plane/postgres:version-for-table //src/control-plane/postgres:ontology //src/control-plane/memory:ontology`
Expected: PASS (the testkit `ontology_contract` runs against both adapters).

- [ ] **Step 8: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(ontology): persist ObjectType.version + version_for_table lookup

Add the version/sequence column to ontology.object_type (migration 0039)
and a reverse-lookup mirroring identity_for_table. Both adapters round-trip
it (memory via the struct; postgres via the new column). Testkit contract
asserts the round-trip."
```

---

## Task 3: `merge_engine` on the stream registry — `StreamMeta`, `StreamTables::declare_cdc`, postgres column, testkit contract

**Files:**
- Create: `src/control-plane/postgres/migrations/0040_stream_table_merge_engine.sql`
- Modify: `src/control-plane/core/src/stream.rs` — `StreamMeta.merge_engine`; `StreamTables::declare_cdc` widened
- Modify: `src/control-plane/memory/src/stream.rs:38-64` — memory `declare_cdc`/`declare_stream` carry/default the engine
- Modify: `src/control-plane/postgres/src/stream.rs:270-315` (`pg_declare_cdc`, `pg_stream_meta`), `:336-361` (`declare_cdc` impl)
- Modify: `src/control-plane/testkit/src/lib.rs` — `stream_tables_contract` engine assertions; ~2 internal `declare_cdc` calls widened
- Regenerate + commit: `.sqlx/`
- Test: the testkit `stream_tables_contract` (existing targets) + the existing CDC fixture tests still pass after the signature widening.

**Interfaces:**
- Consumes: `MergeEngine` (Task 1).
- Produces: `StreamMeta.merge_engine: MergeEngine`; `StreamTables::declare_cdc(&self, table_id, bucket_count, bucket_key, merge_engine)`. `pg_stream_meta` selects `merge_engine`.

- [ ] **Step 1: Add `merge_engine` to `StreamMeta` + widen the trait**

In `src/control-plane/core/src/stream.rs`, add the field to `StreamMeta` (after `changelog_table_id`, line 30):

```rust
    /// The replace-class merge engine governing this CDC table's current-state
    /// fold (compaction + merge-on-read). Default `LastRow`. Log tables carry
    /// `LastRow` too (unused — only CDC tables fold).
    pub merge_engine: MergeEngine,
```

Widen `StreamTables::declare_cdc` (line 54):

```rust
    /// Declare table_id as a PK/CDC table with bucket_count buckets keyed on
    /// `bucket_key` (the identity column), folded by `merge_engine`. Idempotent,
    /// first-wins on all fields.
    async fn declare_cdc(
        &self,
        table_id: i64,
        bucket_count: i32,
        bucket_key: &str,
        merge_engine: MergeEngine,
    ) -> Result<()>;
```

- [ ] **Step 2: Update the memory adapter**

In `src/control-plane/memory/src/stream.rs`:

`declare_stream` (line 42-48) — add `merge_engine: MergeEngine::LastRow` to the `StreamMeta { … }`.
`declare_cdc` signature + body (line 53-64) — add the `merge_engine: MergeEngine` param and set it in the `StreamMeta { … }`.

- [ ] **Step 3: Add the migration**

Create `src/control-plane/postgres/migrations/0040_stream_table_merge_engine.sql`:

```sql
-- The replace-class merge engine for a CDC table's current-state fold
-- (road-stream-merge-engines). Default 'last_row' (byte-identical to the
-- pre-engine fold). Immutable after declaration (reconcile_stream_mode rejects
-- a redeclare with a different engine).
alter table stream.stream_table
    add column merge_engine text not null default 'last_row'
        check (merge_engine in ('last_row', 'first_row', 'versioned'));
```

- [ ] **Step 4: Read/write the column in the postgres adapter**

In `src/control-plane/postgres/src/stream.rs`:

`pg_declare_cdc` (line 276-287) — add `merge_engine`:

```rust
pub(crate) async fn pg_declare_cdc<'e, E: sqlx::PgExecutor<'e>>(
    ex: E,
    table_id: i64,
    bucket_count: i32,
    bucket_key: &str,
    merge_engine: control_plane_core::MergeEngine,
) -> Result<()> {
    sqlx::query!(
        "insert into stream.stream_table (table_id, bucket_count, kind, bucket_key, merge_engine) \
         values ($1, $2, 'cdc', $3, $4) on conflict (table_id) do nothing",
        table_id,
        bucket_count,
        bucket_key,
        merge_engine.as_str(),
    )
    .execute(ex)
    .await
    .map_err(backend)?;
    Ok(())
}
```

`pg_stream_meta` (line 294-315) — select `merge_engine` and parse it:

```rust
    let row = sqlx::query!(
        "select bucket_count, kind, bucket_key, changelog_table_id, merge_engine \
         from stream.stream_table where table_id = $1",
        table_id,
    )
    .fetch_optional(ex)
    .await
    .map_err(backend)?;
    Ok(row.map(|r| {
        let kind = if r.kind == "cdc" { StreamKind::Cdc } else { StreamKind::Log };
        let merge_engine = r.merge_engine.parse().unwrap_or(control_plane_core::MergeEngine::LastRow);
        StreamMeta {
            bucket_count: r.bucket_count,
            kind,
            bucket_key: r.bucket_key,
            changelog_table_id: r.changelog_table_id,
            merge_engine,
        }
    }))
```

(Note: `.unwrap_or(LastRow)` here is a corrupt-row backstop only — the CHECK constraint guarantees a valid token. Clippy allows `unwrap_or`; this is not `unwrap_used`. If clippy flags the `parse()` fallibility style, use `match r.merge_engine.as_str() { … }` or `.parse().unwrap_or_else(|_| LastRow)` — either is fine.)

The `StreamTables for PgControlPlane` `declare_cdc` impl (line 348-350):

```rust
    async fn declare_cdc(
        &self,
        table_id: i64,
        bucket_count: i32,
        bucket_key: &str,
        merge_engine: control_plane_core::MergeEngine,
    ) -> Result<()> {
        pg_declare_cdc(self.pool(), table_id, bucket_count, bucket_key, merge_engine).await
    }
```

- [ ] **Step 5: Regenerate the `.sqlx` cache**

Run: `tools/sqlx-prepare.sh`; the two changed `query!` macros (and the existing `pg_stream_bucket_count`/etc. unaffected) produce new/updated `.sqlx/query-*.json`. Commit in Step 8.

- [ ] **Step 6: Widen the existing `declare_cdc(…)` callers + testkit contract**

The compiler will flag every `declare_cdc(tid, n, "id")` call site (~15 across `src/control-plane/postgres/tests/*`, `src/control-plane/testkit/src/lib.rs`, `src/services/{query-api,engine-serving,worker}/tests/*`). For each, append `, control_plane_core::MergeEngine::LastRow` (or `MergeEngine::LastRow` where imported). These are existing CDC tables that keep the default engine.

In `src/control-plane/testkit/src/lib.rs` `stream_tables_contract` (around line 5423-5456), after the existing `declare_cdc(2, 4, "id")` assertions, add engine round-trip + default assertions:

```rust
    // merge_engine round-trips and defaults to LastRow.
    assert_eq!(
        meta.merge_engine,
        control_plane_core::MergeEngine::LastRow,
        "default merge engine is LastRow"
    );
```

…and add a `declare_cdc` with a non-default engine on a fresh table id (e.g. table 3) asserting it round-trips:

```rust
    cp.declare_cdc(3, 2, "id", control_plane_core::MergeEngine::FirstRow)
        .await
        .expect("declare cdc first_row");
    let me = cp.stream_meta(3).await.expect("meta").expect("declared");
    assert_eq!(me.merge_engine, control_plane_core::MergeEngine::FirstRow);
    // idempotent redeclare keeps the first engine (first-wins).
    cp.declare_cdc(3, 2, "id", control_plane_core::MergeEngine::Versioned)
        .await
        .expect("idempotent redeclare no-ops");
    assert_eq!(
        cp.stream_meta(3).await.expect("meta").expect("declared").merge_engine,
        control_plane_core::MergeEngine::FirstRow,
        "first declaration's engine stands"
    );
```

(The `declare_stream` path sets `LastRow` for log tables — the existing `log_meta` assertion is unaffected; optionally assert `log_meta.merge_engine == LastRow`.)

- [ ] **Step 7: Run the stream + CDC test suite**

Run: `buck2 test --console none //src/control-plane/postgres:stream-tables //src/control-plane/memory:stream-tables //src/control-plane/postgres:stream-cdc-bucket //src/control-plane/postgres:stream-cdc-emission //src/control-plane/postgres:stream-cdc-dual-flush`
Expected: PASS (the testkit `stream_tables_contract` runs against both adapters; the existing CDC tests pass with the widened signature).

- [ ] **Step 8: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(stream): per-table merge_engine on the stream registry

Add merge_engine to stream.stream_table (migration 0040, default last_row)
and thread it through StreamMeta + StreamTables::declare_cdc + both adapter
impls. The fold sites still hardcode LastRow behavior (Task 5/6 generalize
them); this task only persists and round-trips the choice. Testkit contract
asserts default + round-trip + first-wins."
```

---

## Task 4: Declaration surface — thread `?merge_engine=` through `CdcDecl`/`StreamDecl`/`reconcile`, with validation

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_landing.rs:44-48` (`CdcDecl`), `:69-87` (`combine_stream_decl`), `:142-189` (`land_cdc` carries it)
- Modify: `src/control-plane/postgres/src/stream.rs:51-174` (`reconcile_stream_mode` — `StreamDecl::Cdc` gains `merge_engine`; records it; engine immutability; **Versioned version-property + orderability validation**)
- Modify: `src/services/ingest/src/http.rs:207-212` (`ModelQuery`), `:336-355` (parse `?merge_engine=` + build `CdcDecl`), `:248-268` (utoipa param)
- Modify: `src/control-plane/core/src/logical_type.rs` — add `BaseType::is_version_orderable`
- Modify: `src/control-plane/core/tests/version_property.rs` — add an `is_version_orderable` assertion
- Test: `src/control-plane/postgres/tests/stream_merge_declare.rs` (new — drives `land_cdc` directly, mirroring `stream_cdc_declare.rs`) + its `loom_fixture_test` target; openapi golden update.

**Interfaces:**
- Consumes: `MergeEngine` (T1), `ObjectType.version` (T1), `declare_cdc` engine param (T3), `version_for_table` (T2, executor-based).
- Produces: `CdcDecl { buckets, bucket_key, merge_engine }`; `StreamDecl::Cdc { buckets, bucket_key, merge_engine }`; `?merge_engine=` on `POST /models/{type}`; Versioned declaration validation (version-property-required + orderable) in `reconcile_stream_mode`.

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/postgres/tests/stream_merge_declare.rs`, driving `land_cdc` directly — the primitive the HTTP `/models/{type}?mode=cdc` path bottoms out on — so the validation is exercised at the `reconcile_stream_mode` seam that enforces it for every declarer (HTTP and direct). Mirror `src/control-plane/postgres/tests/stream_cdc_declare.rs`'s `PgFixture` / `local_sql_catalog` / `land_cdc` harness and its `columns` / `batch` / `lineage` / `always_inline` helpers exactly. Each case first defines an ontology type bound to the target `TableRef` (so `version_for_table` resolves) via `cp.ontology().define_type(ObjectType::build(name, (schema, table))…done())`, `ensure_table`s the mirror row, then calls `land_cdc`.

```rust
//! merge_engine declaration validation through land_cdc -> reconcile_stream_mode
//! (the seam every declarer passes through). loom_fixture_test (Postgres).
//!   - versioned against a type with NO version property      -> Err(Validation)
//!   - versioned against a type whose version col is non-orderable -> Err(Validation)
//!   - versioned against a type with an orderable version col -> Ok, meta reports Versioned
//!   - first_row / last_row (default) accepted with no version property
//!   - redeclare an existing CDC table with a different engine -> Err(Conflict)
use control_plane_core::{ControlPlaneError, MergeEngine, ObjectType, TableRef};
use control_plane_postgres::iceberg_landing::CdcDecl;
// …mirror stream_cdc_declare.rs's remaining imports + columns/batch/lineage/always_inline…
```

Cases (one assertion block each, sharing the harness):

1. **Versioned, no version property ⇒ `Err(Validation)`**: define type `("s","w_noversion")` with `.identity("id")` and NO `.version(...)`. `land_cdc(…, Some(CdcDecl { buckets: 2, bucket_key: "id".into(), merge_engine: MergeEngine::Versioned }))` ⇒ `assert!(matches!(res, Err(ControlPlaneError::Validation(_))))`.
2. **Versioned, non-orderable version property ⇒ `Err(Validation)`**: define a type with `.prop("name","String").version("name")`. Same `Versioned` land_cdc ⇒ `Err(Validation)`.
3. **Versioned, orderable version property ⇒ `Ok`**: define a type with `.prop("seq","Long").version("seq")`. `land_cdc(…, Versioned)` ⇒ `Ok(_)`; then `cp.stream_meta(tid).await?.unwrap().merge_engine == MergeEngine::Versioned`.
4. **FirstRow / LastRow (default) ⇒ `Ok` with no version property**: against the case-1 no-version type, `land_cdc(…, FirstRow)` and `land_cdc(…, LastRow)` ⇒ `Ok`; `stream_meta` reports each engine.
5. **Redeclare with a different engine ⇒ `Err(Conflict)`**: after case-3's successful Versioned declare on `("s","w_versioned")`, a second `land_cdc` on the SAME table with `merge_engine: MergeEngine::LastRow` ⇒ `assert!(matches!(res, Err(ControlPlaneError::Conflict(_))))` (engine immutable).

Add a `loom_fixture_test` target `stream-merge-declare` to `src/control-plane/postgres/BUCK` mirroring `stream-cdc-declare`.

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test --console none //src/control-plane/postgres:stream-merge-declare`
Expected: FAIL — `MergeEngine`/`CdcDecl.merge_engine` not yet threaded (compile error) until Steps 3–6 land.

- [ ] **Step 3: Add `BaseType::is_version_orderable`**

In `src/control-plane/core/src/logical_type.rs`, after `is_ordered` (line 119), add:

```rust
    /// A version/sequence column must be a totally-ordered, monotonic-friendly
    /// type — `Integer`, `Long`, or `Timestamp` — for the `Versioned` merge engine
    /// to order current-state by it. Narrower than `is_ordered` (excludes Double,
    /// String, Date, Vector): a version is an integer or timestamp sequence, not
    /// arbitrary ordered data. See `road-stream-merge-engines`.
    #[must_use]
    pub fn is_version_orderable(self) -> bool {
        matches!(self, BaseType::Integer | BaseType::Long | BaseType::Timestamp)
    }
```

Add to `src/control-plane/core/tests/version_property.rs`:

```rust
#[test]
fn version_orderable_types_are_integer_long_timestamp() {
    use control_plane_core::BaseType;
    for ty in [BaseType::Integer, BaseType::Long, BaseType::Timestamp] {
        assert!(ty.is_version_orderable(), "{ty:?} should be version-orderable");
    }
    for ty in [
        BaseType::Double,
        BaseType::Boolean,
        BaseType::String,
        BaseType::Date,
        BaseType::Vector(4),
    ] {
        assert!(!ty.is_version_orderable(), "{ty:?} should NOT be version-orderable");
    }
}
```

- [ ] **Step 4: Thread `merge_engine` through `CdcDecl` / `StreamDecl` / `combine_stream_decl`**

In `src/control-plane/postgres/src/iceberg_landing.rs`:

`CdcDecl` (line 44-48):

```rust
#[derive(Clone, Debug)]
pub struct CdcDecl {
    pub buckets: i32,
    pub bucket_key: String,
    pub merge_engine: control_plane_core::MergeEngine,
}
```

`combine_stream_decl` (line 75-84) — destructure + carry `merge_engine`:

```rust
        (
            None,
            Some(CdcDecl {
                buckets,
                bucket_key,
                merge_engine,
            }),
        ) => Ok(StreamDecl::Cdc {
            buckets,
            bucket_key,
            merge_engine,
        }),
```

- [ ] **Step 5: `StreamDecl::Cdc` gains `merge_engine`; `reconcile_stream_mode` records it + enforces immutability**

In `src/control-plane/postgres/src/stream.rs`:

`StreamDecl::Cdc` (line 24):

```rust
    Cdc { buckets: i32, bucket_key: String, merge_engine: control_plane_core::MergeEngine },
```

Update the two existing `StreamDecl::Cdc { .. }` matches (lines 61, 93) — they use `..` so they still compile, but the `pg_declare_cdc` call (line 115) now passes the engine:

```rust
                StreamDecl::Cdc { bucket_key, merge_engine, .. } => {
                    pg_declare_cdc(&mut *conn, tid, n, bucket_key, merge_engine).await?;
```

Engine immutability — in the `(Some(_), Some(m))` arm (around line 87-105), after the existing kind-mismatch check, add an engine-mismatch check (a redeclare with a different engine ⇒ `Conflict`, mirroring the bucket-count mismatch):

```rust
            if matches!(
                decl,
                StreamDecl::Cdc { merge_engine, .. }
                    if existing_meta.as_ref().map(|m| m.merge_engine) != Some(*merge_engine)
            ) {
                let existing_engine = existing_meta
                    .as_ref()
                    .map(|m| m.merge_engine.as_str())
                    .unwrap_or("?");
                let requested_engine = match decl {
                    StreamDecl::Cdc { merge_engine, .. } => merge_engine.as_str(),
                    _ => existing_engine,
                };
                return Err(ControlPlaneError::Conflict(format!(
                    "stream merge_engine mismatch for {}.{}: requested {requested_engine}, table has {existing_engine}",
                    table.schema, table.name
                )));
            }
```

(Place this check so it only fires for a `Cdc` redeclare; a `Log` redeclare is unaffected. Keep it inside the `matches!(decl, StreamDecl::Cdc { .. }) && ...` block that already gates the kind check.)

**Versioned version-property + orderability validation.** Add this near the top of `reconcile_stream_mode`, after the requested-bucket-count validation (after line 74) and before the `existing_meta` lookup — so it gates both the first-declare and redeclare arms. It is the only seam every declarer passes through, so a Versioned table can never be created without a usable version column:

```rust
    // merge_engine=versioned requires the bound type to declare a version
    // property of an orderable type (integer/long/timestamp). Validated HERE —
    // on every declarer's path (HTTP `/models/{type}?mode=cdc` and direct
    // land_cdc), and reachable for Versioned (which is only ever created via a
    // control-plane-defined versioned type + land_cdc, since the HTTP path
    // infers types with version: None). Runs before existing/new branching so it
    // gates both first-declare and redeclare.
    if let StreamDecl::Cdc {
        merge_engine: control_plane_core::MergeEngine::Versioned,
        ..
    } = decl
    {
        let version_col = crate::ontology::version_for_table(&mut *conn, table).await?;
        let Some(vcol) = version_col else {
            return Err(ControlPlaneError::Validation(format!(
                "merge_engine=versioned requires {}.{} to declare a version property",
                table.schema, table.name
            )));
        };
        // The version property's logical type (join object_type -> property).
        let ty: Option<String> = sqlx::query_scalar!(
            "select p.ty from ontology.property p \
             join ontology.object_type o on o.name = p.type_name \
             where o.table_schema = $1 and o.table_name = $2 and p.name = $3",
            table.schema,
            table.name,
            vcol,
        )
        .fetch_optional(&mut *conn)
        .await
        .map_err(backend)?;
        let orderable = ty
            .as_deref()
            .and_then(control_plane_core::resolve_logical)
            .is_some_and(control_plane_core::BaseType::is_version_orderable);
        if !orderable {
            return Err(ControlPlaneError::Validation(format!(
                "merge_engine=versioned requires an integer/long/timestamp version column; \
                 {}.{} version column '{vcol}' is {:?}",
                table.schema, table.name, ty
            )));
        }
    }
```

This adds one compile-time `query_scalar!` (the join) → run `tools/sqlx-prepare.sh` (Step 8) so `.sqlx` carries it. `crate::ontology::version_for_table` resolves because it is `pub` in the postgres crate and now executor-based (Task 2), so `&mut *conn` satisfies `PgExecutor`.

- [ ] **Step 6: Parse `?merge_engine=` in the ingest handler (validation stays in reconcile)**

In `src/services/ingest/src/http.rs`:

`ModelQuery` (line 207-212) — add the param:

```rust
#[derive(Deserialize)]
pub(crate) struct ModelQuery {
    identity: Option<String>,
    mode: Option<String>,
    buckets: Option<i32>,
    merge_engine: Option<String>,
}
```

In `land_model`, replace the `cdc_decl` block (line 336-355) with engine parsing only. The version-property/orderability validation is deliberately NOT here — it lives in `reconcile_stream_mode` (Step 5) so it applies to every declarer and is reachable for Versioned (see the declaration-validation placement note):

```rust
    // `?mode=cdc&buckets=N&merge_engine=<engine>` declares this type's table as a
    // PK/CDC stream table on first creation. Requires a declared identity (the
    // bucket key); merge_engine=versioned additionally requires a declared
    // orderable version property — validated in `reconcile_stream_mode`.
    let cdc_decl = if q.mode.as_deref() == Some("cdc") {
        let n = q.buckets.unwrap_or(1);
        if n < 1 {
            return Err(ApiError::BadRequest(Cow::Borrowed("buckets must be >= 1")));
        }
        let identity = otype
            .identity
            .clone()
            .ok_or(ApiError::BadRequest(Cow::Borrowed(
                "mode=cdc requires the type to declare an identity property",
            )))?;
        let engine = match q.merge_engine.as_deref() {
            None => control_plane_core::MergeEngine::LastRow,
            Some(tok) => control_plane_core::MergeEngine::from_str(tok).map_err(|_| {
                ApiError::BadRequest(Cow::Owned(format!(
                    "unknown merge_engine '{tok}' (expected last_row|first_row|versioned)"
                )))
            })?,
        };
        Some(CdcDecl {
            buckets: n,
            bucket_key: identity,
            merge_engine: engine,
        })
    } else {
        None
    };
```

Add `use std::str::FromStr;` at the top of `http.rs` if not already imported.

Update the utoipa `params(...)` for `land_model` (line 248-253) — add:

```rust
        ("merge_engine" = Option<String>, Query, description = "CDC merge engine for `mode=cdc`: `last_row` (default) | `first_row` | `versioned` (requires a declared integer/long/timestamp version property on the type)"),
```

(The HTTP `?merge_engine=versioned` path infers types with `version: None`, so it 400s in `reconcile_stream_mode` until/unless a future `?version=` param is added. Versioned is exercised over HTTP only by pre-defining a versioned type out-of-band; the Step 1 `land_cdc` test covers the validation directly.)

- [ ] **Step 7: Update the existing `CdcDecl { … }` construction in `stream_cdc_declare.rs`**

`src/services/query-api/tests/stream_cdc_declare.rs:105` constructs `CdcDecl { buckets, bucket_key }` — add `merge_engine: control_plane_core::MergeEngine::LastRow`. (This is the only test-file `CdcDecl { … }` literal besides the new `stream_merge_declare.rs`; the production `CdcDecl { … }` literals in `iceberg_landing.rs` and `ingest/http.rs` are updated in Steps 4 and 6.)

- [ ] **Step 8: Regenerate `.sqlx`, run the tests, update the openapi golden**

Step 5 added a new compile-time `query_scalar!` (the version-property join) in `reconcile_stream_mode`. Regenerate the cache (local/non-cloud step — boots the pinned postgres):

Run: `tools/sqlx-prepare.sh` and commit the new `.sqlx/query-*.json` entry.

Then run: `buck2 test --console none //src/control-plane/postgres:stream-merge-declare`
Expected: PASS. Also build the widened-`CdcDecl` callers to confirm they compile: `buck2 build -v0 --console none //src/services/query-api:stream-cdc-declare //src/services/ingest:ingest`.

The new utoipa param changes the generated OpenAPI document. Run the openapi generation test and update its golden/snapshot:

Run: `buck2 test --console none //src/services/ingest:openapi` (and/or the query-api openapi test if it aggregates ingest's paths).
If FAIL on a snapshot diff, update the committed golden the test points at (the test output names the file) and re-run until green.

- [ ] **Step 9: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(stream): ?merge_engine= declaration + versioned validation

Thread the merge engine from POST /models/{type}?merge_engine= through
CdcDecl/StreamDecl into reconcile_stream_mode (which now enforces engine
immutability). merge_engine=versioned requires a declared integer/long/
timestamp version property (validated at the declaration surface, alongside
the existing identity-required check); unknown tokens 400. Default last_row
keeps existing declares byte-identical."
```

---

## Task 5: Generalize `consolidate_stream`'s fold per engine

**Files:**
- Modify: `src/services/engine-serving/src/consolidate.rs:57-100` (`consolidate_stream` — read engine + version col), `:102-221` (`consolidate_locked` — per-engine ORDER BY)
- Test: non-regression — existing `stream_cdc_consolidate` e2e + `consolidate_lock` stay green (LastRow default byte-identical). The Versioned consolidate-fold is asserted by Task 7's `stream_merge_versioned` e2e.

**Interfaces:**
- Consumes: `StreamMeta.merge_engine` (T3), `version_for_table` (T2).
- Produces: a `consolidate_stream` that folds by the declared engine.

- [ ] **Step 1: Read the engine + version column in `consolidate_stream`**

In `src/services/engine-serving/src/consolidate.rs`, after the `identity` is resolved (line 78-83) and before the lock is taken, resolve the version column for a Versioned engine:

```rust
    let version_col = if matches!(meta.merge_engine, control_plane_core::MergeEngine::Versioned) {
        Some(
            control_plane_postgres::ontology::version_for_table(pool, table)
                .await
                .map_err(to_serving)?
                .ok_or_else(|| {
                    EngineServingError::Engine(format!(
                        "cdc table {}.{} (tid {tid}) is merge_engine=versioned but its type has no version column",
                        table.schema, table.name
                    ))
                })?,
        )
    } else {
        None
    };
```

Pass the engine + version column into `consolidate_locked` (change its signature + the call at line 97):

```rust
    let result = consolidate_locked(pool, catalog, table, tid, &identity, meta.merge_engine, version_col.as_deref()).await;
```

```rust
async fn consolidate_locked(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    tid: i64,
    identity: &str,
    engine: control_plane_core::MergeEngine,
    version_col: Option<&str>,
) -> Result<i64, EngineServingError> {
```

- [ ] **Step 2: Generalize the fold `ORDER BY`**

In `consolidate_locked`, replace the `fold_sql` (line 190-197) with a per-engine `order_clause`:

```rust
    // Per-engine winner ordering (winner = ROW_NUMBER rank 1). LastRow is the
    // unchanged default (byte-identical). Versioned orders by the quoted domain
    // version column desc, tie-broken by loom_offset desc (highest version wins;
    // last-write-within-version wins). FirstRow takes the smallest offset.
    let order_clause = match engine {
        control_plane_core::MergeEngine::LastRow => "loom_offset desc".to_string(),
        control_plane_core::MergeEngine::FirstRow => "loom_offset asc".to_string(),
        control_plane_core::MergeEngine::Versioned => {
            let vcol = version_col.unwrap_or_else(|| {
                // Unreachable: consolidate_stream resolves version_col for Versioned
                // before locking. Defense in depth.
                "loom_offset"
            });
            format!("{}, loom_offset desc", quote_ident(vcol))
        }
    };
    // Greatest-precedence per identity wins; a winner tombstoned by `-D` is
    // dropped (delete is uniform across all engines).
    let fold_sql = format!(
        "select {col_list}, loom_change_kind, loom_bucket, loom_offset from ( \
             select *, row_number() over ( \
                 partition by {id_quoted} order by {order_clause} \
             ) as _rn \
             from ({union_sql}) base_input \
         ) t where _rn = 1 and loom_change_kind <> '-D'"
    );
```

The `where _rn = 1 and loom_change_kind <> '-D'` predicate, the `col_list`, `id_quoted`, `union_sql`, the `overwrite_parquet_snapshot` rewrite, `clear_has_shadow`, and `clear_consolidate_trigger` are all unchanged.

- [ ] **Step 3: Non-regression — existing CDC suite stays green**

Run: `buck2 test --console none //src/services/query-api:stream-cdc-consolidate //src/services/engine-serving:consolidate-lock //src/services/worker:stream-consolidate-job`
Expected: PASS — LastRow default folds byte-identically (the generated SQL for `LastRow` is the exact `order by loom_offset desc` string in use today).

- [ ] **Step 4: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(stream): consolidate_stream folds by the declared merge engine

The compaction fold's ORDER BY is now per-engine: LastRow (loom_offset desc,
byte-identical default), FirstRow (loom_offset asc), Versioned (version col
desc, loom_offset desc tie-break). The -D-winner-drop predicate and the
framing-preserving overwrite are unchanged. The Versioned fold's correctness
across consolidate cycles is asserted by the Task 7 e2e."
```

---

## Task 6: Generalize `build_merge_view` / `build_serving_provider` per engine

**Files:**
- Modify: `src/services/engine-serving/src/serving.rs:74-228` (`build_serving_provider` — fetch meta+engine+version col, build `Precedence::Offset`), `:245-382` (`Precedence` + `build_merge_view` — per-engine precedence expr + window ordering)
- Modify: `src/control-plane/postgres/src/ontology.rs:635-653` — replace `is_cdc_table` with `stream_meta_for_table` (single lookup yielding kind + engine)
- Test: non-regression — existing `merge_on_read`, `stream_cdc_read_mid_window`, `stream_cdc_e2e`, `object_identity_dedup_e2e` stay green.

**Interfaces:**
- Consumes: `StreamMeta.merge_engine` (T3), `version_for_table` (T2).
- Produces: `Precedence::Offset { engine: MergeEngine, version_col: Option<String> }`; `pub(crate) async fn stream_meta_for_table(pool, table) -> Result<Option<StreamMeta>>`.

- [ ] **Step 1: Add `stream_meta_for_table`, replace `is_cdc_table`**

In `src/control-plane/postgres/src/ontology.rs`, replace `is_cdc_table` (line 635-653) with a helper returning the full meta (one lookup yields both kind and engine):

```rust
/// The declared stream metadata for `table`'s live incarnation, or `None` when
/// the table is not a declared stream table (no live incarnation, no inline
/// storage provisioned, or a plain batch table). Used by the engine serving read
/// to route a CDC table's fold through `loom_offset` precedence with the table's
/// declared merge engine — see `engine_serving::serving::build_merge_view`.
pub async fn stream_meta_for_table(
    pool: &PgPool,
    table: &TableRef,
) -> Result<Option<control_plane_core::StreamMeta>> {
    let mut conn = pool.acquire().await.map_err(backend)?;
    let Some(tid) =
        crate::iceberg_mirror::live_table_id(&mut conn, &table.schema, &table.name).await?
    else {
        return Ok(None);
    };
    crate::stream::pg_stream_meta(&mut *conn, tid).await
}
```

(Remove the old `is_cdc_table` fn — its only caller, `build_serving_provider`, is rewritten below to derive `is_cdc` from this meta. No other code or doc-comment references `is_cdc_table`: the `merge_on_read.rs:40` doc comment names `identity_for_table`, which stays.)

- [ ] **Step 2: Generalize `Precedence`**

In `src/services/engine-serving/src/serving.rs`, change the `Precedence` enum (line 245-261) — `Offset` now carries the engine + version column; drop `Copy` (it holds `Option<String>`), keep `Clone`:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
enum Precedence {
    /// Non-CDC identity tables (unchanged): file tier synthesizes precedence `0`
    /// + `false` tombstone; inline tier uses `begin_snapshot` / `loom_tombstone`.
    Snapshot,
    /// CDC tables: both tiers carry real `loom_offset`/`loom_change_kind` framing.
    /// `engine` selects the winner ordering; `version_col` is set only for
    /// `Versioned` (the quoted domain version column the window orders by).
    Offset {
        engine: control_plane_core::MergeEngine,
        version_col: Option<String>,
    },
}
```

- [ ] **Step 3: Build the `Offset` precedence in `build_serving_provider`**

Replace the `is_cdc` block (line 130-141) and the two `Precedence::Offset` construction sites (line 210, 217-222) to fetch meta once and build the engine-aware precedence. Replace lines 130-141:

```rust
    // For an identity-bearing table, resolve CDC-ness AND the declared merge
    // engine in one lookup. A `kind='cdc'` base's flushed file tier can carry
    // MULTIPLE physical rows (and `-D` tombstones) per identity, so its fold uses
    // `loom_offset` precedence (`Precedence::Offset`) with the table's engine.
    let cdc_meta = if identity.is_some() {
        control_plane_postgres::ontology::stream_meta_for_table(&catalog.pool, table)
            .await
            .map_err(to_serving)?
            .filter(|m| m.kind == control_plane_core::StreamKind::Cdc)
    } else {
        None
    };
    let is_cdc = cdc_meta.is_some();
```

Then in the `Some(id)` match arms, build the precedence from `cdc_meta`. Replace the `(Some(f), None)` CDC arm (line 209-211) and the `(file_opt, Some(i))` arm's precedence (line 216-223):

```rust
            (Some(f), None) if !is_cdc => Arc::new(f),
            (Some(f), None) => build_merge_view(
                ctx,
                &schema,
                id,
                Some(f),
                None,
                offset_precedence(&catalog.pool, cdc_meta.as_ref(), table).await?,
            )?,
            (None, None) => return Ok(None),
            (file_opt, Some(i)) => {
                let precedence = if is_cdc {
                    offset_precedence(&catalog.pool, cdc_meta.as_ref(), table).await?
                } else {
                    Precedence::Snapshot
                };
                build_merge_view(ctx, &schema, id, file_opt, Some(i), precedence)?
            }
```

Add a small async helper just above `build_merge_view` that builds the `Offset` precedence (fetching the version column live for the Versioned engine — mirroring how `build_merge_view` already reads the identity column live via `identity_for_table`). It takes the pool so it can resolve the version column; thread `&catalog.pool` from `build_serving_provider`:

```rust
/// Build the CDC `Precedence::Offset` from the table's stream meta, fetching the
/// version column live for the Versioned engine.
async fn offset_precedence(
    pool: &sqlx::PgPool,
    meta: Option<&control_plane_core::StreamMeta>,
    table: &TableRef,
) -> Result<Precedence, EngineServingError> {
    let engine = meta
        .map(|m| m.merge_engine)
        .unwrap_or(control_plane_core::MergeEngine::LastRow);
    let version_col = if matches!(engine, control_plane_core::MergeEngine::Versioned) {
        Some(
            control_plane_postgres::ontology::version_for_table(pool, table)
                .await
                .map_err(to_serving)?
                .ok_or_else(|| {
                    EngineServingError::Engine(format!(
                        "cdc table {}.{} is merge_engine=versioned but has no version column",
                        table.schema, table.name
                    ))
                })?,
        )
    } else {
        None
    };
    Ok(Precedence::Offset { engine, version_col })
}
```

Update the two call sites in the match arms above to pass the pool: `offset_precedence(&catalog.pool, cdc_meta.as_ref(), table).await?`.

- [ ] **Step 4: Generalize `build_merge_view`'s precedence expr + window ordering**

`Precedence` is no longer `Copy` (it holds `Option<String>`), so the two `match precedence` sites must borrow. The `_loom_prec` projection and the window's `ORDER BY` direction/keys both depend on the engine. Replace the precedence-derivation block (line 319-326: the `let (file_prec, file_tomb)` / `let (inline_prec, inline_tomb)` matches) AND the `ranked` window (line 358-364) with this single consolidated block. (The `tier_select`, `file_df`/`inline_df`, `unioned`, and the final `.window/.filter/.select` tail are unchanged — only the precedence expr, the tombstone exprs, and the window order keys change.)

```rust
    // The precedence expr projected as `_loom_prec` — the column the per-identity
    // window orders by. Snapshot synthesizes a literal 0 (the file tier); LastRow
    // and FirstRow order by loom_offset; Versioned orders by the quoted domain
    // version column (a user column, case-preserving via Column::new_unqualified).
    let prec_expr: Expr = match &precedence {
        Precedence::Snapshot => lit(0_i64),
        Precedence::Offset { engine, version_col } => match engine {
            control_plane_core::MergeEngine::Versioned => {
                let vcol = version_col.as_deref().ok_or_else(|| {
                    EngineServingError::Engine(
                        "Versioned precedence requires a version column".into(),
                    )
                })?;
                Expr::Column(Column::new_unqualified(vcol))
            }
            control_plane_core::MergeEngine::LastRow
            | control_plane_core::MergeEngine::FirstRow => cref("loom_offset"),
        },
    };

    // Project each tier to [<data_cols>, _loom_prec, _loom_tomb]. Both tiers use
    // the SAME prec_expr (cloned) and the SAME tombstone mapping; only Snapshot
    // differs (file tier synthesizes <0, false>; inline uses begin_snapshot /
    // loom_tombstone). Offset's tombstone is `loom_change_kind = '-D'` (delete is
    // uniform across all engines) for BOTH tiers — unchanged from today.
    let (file_prec, file_tomb) = match &precedence {
        Precedence::Snapshot => (lit(0_i64), lit(false)),
        Precedence::Offset { .. } => {
            (prec_expr.clone(), cref("loom_change_kind").eq(lit("-D")))
        }
    };
    let (inline_prec, inline_tomb) = match &precedence {
        Precedence::Snapshot => (cref("begin_snapshot"), cref("loom_tombstone")),
        Precedence::Offset { .. } => {
            (prec_expr.clone(), cref("loom_change_kind").eq(lit("-D")))
        }
    };
    let file_df = match file {
        Some(f) => Some(
            ctx.read_table(Arc::new(f))
                .map_err(to_serving)?
                .select(tier_select(file_prec, file_tomb))
                .map_err(to_serving)?,
        ),
        None => None,
    };
    let inline_df = match inline {
        Some(i) => Some(
            ctx.read_table(Arc::new(i))
                .map_err(to_serving)?
                .select(tier_select(inline_prec, inline_tomb))
                .map_err(to_serving)?,
        ),
        None => None,
    };
    let unioned = match (file_df, inline_df) {
        (Some(f), Some(i)) => f.union(i).map_err(to_serving)?,
        (Some(f), None) => f,
        (None, Some(i)) => i,
        (None, None) => {
            return Err(EngineServingError::Engine(
                "build_merge_view: neither a file nor an inline tier".into(),
            ));
        }
    };

    // Per-identity window: rank 1 is the winner. Direction + keys come from the
    // engine: LastRow/Versioned order _loom_prec DESC (greatest precedence wins);
    // FirstRow orders ASC (smallest offset wins). Versioned adds a loom_offset
    // DESC tie-break (last-write-within-version wins). Snapshot is unchanged.
    let order_keys: Vec<Expr> = match &precedence {
        Precedence::Snapshot => vec![cref("_loom_prec").sort(false, false)],
        Precedence::Offset { engine, .. } => match engine {
            control_plane_core::MergeEngine::LastRow
            | control_plane_core::MergeEngine::Versioned => {
                let mut keys = vec![cref("_loom_prec").sort(false, false)];
                if matches!(engine, control_plane_core::MergeEngine::Versioned) {
                    keys.push(cref("loom_offset").sort(false, false));
                }
                keys
            }
            control_plane_core::MergeEngine::FirstRow => {
                vec![cref("_loom_prec").sort(true, false)]
            }
        },
    };
    let ranked = row_number()
        .partition_by(vec![cref(identity)])
        .order_by(order_keys)
        .build()
        .map_err(to_serving)?
        .alias("_loom_rn");
    // Keep the winner per identity, hide tombstoned winners, then project back to
    // the mirror data schema — UNCHANGED (the version column is a user column
    // already in data_cols; the _loom_* helpers are dropped).
    let merged = unioned
        .window(vec![ranked])
        .map_err(to_serving)?
        .filter(cref("_loom_rn").eq(lit(1_u64)))
        .map_err(to_serving)?
        .filter(cref("_loom_tomb").eq(lit(false)))
        .map_err(to_serving)?
        .select(
            data_cols
                .iter()
                .map(|n| cref(n.as_str()))
                .collect::<Vec<_>>(),
        )
        .map_err(to_serving)?;
    Ok(merged.into_view())
```

(`tier_select`, `cref`, `data_cols`, `row_number`, `lit`, and the `Column`/`Expr` imports are already in scope at the top of `build_merge_view` today — no new imports. For `Offset { engine: LastRow, .. }` the rendered SQL is identical to today: `prec_expr = loom_offset`, `order_keys = [_loom_prec desc]`, tombstone `(loom_change_kind = '-D')` — byte-identical merge-on-read.)

The `filter(_loom_rn = 1).filter(_loom_tomb = false).select(data_cols)` tail (line 367-380) is unchanged — version column is a user column already in `data_cols`, and the `_loom_*` helpers are dropped.

- [ ] **Step 5: Non-regression — the merge-on-read suite stays green**

Run: `buck2 test --console none //src/services/engine-serving:merge-on-read //src/services/query-api:stream-cdc-read-mid-window //src/services/query-api:stream-cdc-e2e //src/services/query-api:object-identity-dedup-e2e //src/services/query-api:update-delete-e2e`
Expected: PASS — `Snapshot` precedence and the `LastRow` default render the exact same SQL/window as before. (For `Offset { engine: LastRow, .. }`, `prec_expr = loom_offset`, `order_keys = [_loom_prec desc]` — identical to today's `cref("_loom_prec").sort(false,false)` over `_loom_prec = loom_offset`.)

- [ ] **Step 6: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(stream): merge-on-read folds by the declared merge engine

Generalize Precedence::Offset to carry the merge engine + version column;
build_merge_view derives the per-identity window's precedence expr and ORDER
BY direction/keys from it (LastRow desc, FirstRow asc, Versioned version-col
desc with loom_offset tie-break). build_serving_provider fetches kind+engine
in one lookup (stream_meta_for_table replaces is_cdc_table). Snapshot
precedence and the LastRow default are byte-identical."
```

---

## Task 7: FirstRow + Versioned fold e2e tests

**Files:**
- Create: `src/services/query-api/tests/stream_merge_firstrow.rs`
- Create: `src/services/query-api/tests/stream_merge_versioned.rs`
- Modify: `src/services/query-api/tests/e2e_support.rs` — add `define_versioned_widget` (a VWidget variant with a `seq` version column + `createVWidget`/`bumpVWidget`/`deleteVWidget` actions)
- Modify: `src/services/query-api/BUCK` — two new `loom_fixture_test` targets
- Test: the two new e2e files.

**Interfaces:**
- Consumes: the engine-aware declaration (T4), the engine-aware folds (T5, T6), the e2e helpers (`define_widget`, `grant_writer`, `spawn_engine_writer`, `connect_gov_client`, `get`), and the action runner `query_api::action::{run_action, ActionDeps}` (imported from the `query-api` crate, NOT `e2e_support` — see `stream_cdc_consolidate.rs:33`).

- [ ] **Step 1: Add `define_versioned_widget` to e2e_support**

In `src/services/query-api/tests/e2e_support.rs`, next to `define_widget` (after line 911), add a variant whose type declares a `seq` (Long) version property, plus an update action that writes `seq` so a test can emit multiple versions of one identity:

```rust
/// A `VersionedWidget` type keyed on `id` with a `seq` (Long) version column —
/// for the Versioned merge-engine e2e. `createVWidget` (id+qty+seq) and
/// `bumpVWidget` (id+seq) let a test emit multiple versions of one identity.
pub async fn define_versioned_widget(cp: &PgControlPlane) -> TypeName {
    let ty = TypeName("VWidget".into());
    cp.ontology()
        .define_type(
            ObjectType::build("VWidget", ("main", "vwidget"))
                .prop_req("id", "Long")
                .prop("qty", "Long")
                .prop_req("seq", "Long")
                .identity("id")
                .version("seq")
                .done(),
        )
        .await
        .unwrap();
    cp.ontology()
        .define_action(
            ActionDef::build("createVWidget", "VWidget", ActionKind::Insert)
                .param_req("id", "Long")
                .param("qty", "Long")
                .param_req("seq", "Long")
                .done(),
        )
        .await
        .unwrap();
    cp.ontology()
        .define_action(
            ActionDef::build("bumpVWidget", "VWidget", ActionKind::Update)
                .param_req("id", "Long")
                .param_req("seq", "Long")
                .done(),
        )
        .await
        .unwrap();
    cp.ontology()
        .define_action(
            ActionDef::build("deleteVWidget", "VWidget", ActionKind::Delete)
                .param_req("id", "Long")
                .done(),
        )
        .await
        .unwrap();
    ty
}
```

- [ ] **Step 2: Write the FirstRow e2e**

Create `src/services/query-api/tests/stream_merge_firstrow.rs`, mirroring `stream_cdc_consolidate.rs`'s fixture/spawn/action/get shape, but declare the CDC table with `MergeEngine::FirstRow`:

```rust
//! FirstRow merge-engine e2e: "first write wins". insert id=1, update id=1,
//! delete id=1, insert id=2. Current-state reads show id=1 at its FIRST value
//! (the update and the delete are ignored for current-state — they are not the
//! earliest event) and id=2 present. Consolidate folds the base to the same
//! first-row winners; the changelog holds every event (engine-agnostic).
//! loom_fixture_test (Postgres + LocalFsStorage warehouse).
```

Setup (mirror `stream_cdc_consolidate.rs:99-123`): `ensure_table` → `cp.declare_cdc(tid, 2, "id", control_plane_core::MergeEngine::FirstRow)` → `define_widget` → `grant_writer` → spawn engine writer + serving.

Actions: `createWidget(id=1,name=a,qty=1)`, `updateWidget(id=1,qty=9)`, `deleteWidget(id=1)`, `createWidget(id=2,name=b,qty=2)`.

Assertions:
- `GET /objects/Widget` (before flush) ⇒ id=1 has `qty=1` (first write wins; the qty=9 update and the delete are ignored), id=2 present. Use the e2e_support object reader.
- Flush, then `consolidate_stream` via the gov client (mirror lines 190-223).
- `table_rows(&catalog, &pool, &table)` after consolidate ⇒ exactly the first-row winners: id=1's `+I` (qty=1) and id=2's `+I` (qty=2). (Use the same `table_rows`/`decode_rows` helpers — copy them into this test file, as `stream_cdc_consolidate.rs` does.)
- The changelog (`widget__changelog`) holds every event (4 events: +I,+U,-D for id=1, +I for id=2 — plus the `-U` before-image for the update = 5 events; assert count ≥ 4 and that it is UNCHANGED by consolidate).
- `GET /objects/Widget` after consolidate is identical to the pre-flush read.

- [ ] **Step 3: Write the Versioned e2e**

Create `src/services/query-api/tests/stream_merge_versioned.rs`. Declare with `MergeEngine::Versioned` + the `VWidget` type (which has the `seq` version column). Import the action runner from where it actually lives — `use query_api::action::{ActionDeps, run_action};` (NOT `e2e_support`).

```rust
//! Versioned merge-engine e2e. (1) Highest domain version wins regardless of
//! arrival order: id=1 emitted with versions 7, 3, 5 in that offset order — the
//! highest version arrives FIRST, so LastRow would pick v=5; Versioned must pick
//! v=7. (2) Correctness across consolidate cycles: after folding to v=7, a late
//! v=4 event still loses. (3) Delete-wins: id=2 (+I seq=1, then -D carrying
//! seq=1) is DROPPED — the -D wins the version tie by offset and the identity
//! does not resurrect. The changelog holds every event (engine-agnostic).
//! loom_fixture_test (Postgres + LocalFsStorage warehouse).
```

Setup: `ensure_table` → `cp.declare_cdc(tid, 2, "id", control_plane_core::MergeEngine::Versioned)` → `define_versioned_widget` → `grant_writer` → spawn.

Actions (drive via `run_action`, `await`ed sequentially so CDC offsets are allocated in call/arrival order):
1. `createVWidget(id=1, qty=1, seq=7)` — version 7 (lowest offset).
2. `bumpVWidget(id=1, seq=3)` — version 3.
3. `bumpVWidget(id=1, seq=5)` — version 5 (highest offset, but NOT the highest version).

> **Determinism note (not a fallback):** `run_action` calls are `await`ed one after another, and CDC offsets are allocated inside each call's commit in arrival order, so id=1's three events land at strictly-increasing offsets carrying versions 7, 3, 5 — exactly the ordering that distinguishes Versioned (picks v=7) from LastRow (would pick v=5). The governed Update path's CAS reads each prior committed state, which sequential awaits guarantee. If a future change ever makes CAS defer/serialize such that offset order diverges from call order, insert a `gov.flush_table(...)` between successive same-id Updates so each observes the prior committed state — do NOT reach for private inline-write helpers; the public action path is the contract under test.

Assertions:
- `GET /objects/VWidget` ⇒ id=1's `seq` = 7 (highest version wins; NOT 5 which has the greatest offset — this is what distinguishes Versioned from LastRow). Assert `qty` corresponds to the v=7 row.
- Flush + `consolidate_stream`. `table_rows` of the base ⇒ one row for id=1 with `seq=7`.
- Emit a late event `bumpVWidget(id=1, seq=4)` (lower version than the folded winner). `GET /objects/VWidget` ⇒ still `seq=7` (the late low-version event loses even after the base was folded to v=7 — the folded winner's version is preserved and dominates — the Versioned correctness-across-consolidate-cycles invariant).
- **Delete-wins:** `createVWidget(id=2, qty=2, seq=1)` then `deleteVWidget(id=2)`. The `-D` carries id=2's prior image (`seq=1`), tying the `+I`'s version; the tie-break (offset desc) makes the `-D` the rank-1 winner, and the `<> '-D'` filter drops it. `GET /objects/VWidget` ⇒ id=2 ABSENT (no resurrection) — a Versioned table whose highest-precedence winner is a `-D` drops the identity.
- The changelog (`vwidget__changelog`) holds every emitted event (engine-agnostic) and is unchanged by consolidate.

- [ ] **Step 4: Wire both targets**

In `src/services/query-api/BUCK`, add two `loom_fixture_test` targets `stream-merge-firstrow` and `stream-merge-versioned`, mirroring `stream-cdc-consolidate`'s deps (`:query-api`, `:e2e-support`, `//src/control-plane/core:core`, `//src/control-plane/postgres:postgres`, `//third-party:arrow-array`, `//third-party:axum`, `//third-party:serde_json`, `//third-party:tokio`, `//third-party:uuid`).

- [ ] **Step 5: Run the new tests**

Run: `buck2 test --console none //src/services/query-api:stream-merge-firstrow //src/services/query-api:stream-merge-versioned`
Expected: PASS. (If the Versioned offset/version ordering ever diverges from call order due to a future CAS change, add the inter-Update `flush_table` noted in Step 3 — do not switch to private write helpers.)

- [ ] **Step 6: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "test(stream): FirstRow + Versioned merge-engine e2e

FirstRow: first write wins (id=1 stays at its first value; the update and
delete are ignored for current-state; id=2 present). Versioned: highest domain
version wins regardless of arrival order (v=7 beats the later v=5), and a late
low-version event still loses after a consolidate (correctness across cycles).
Both assert the changelog is engine-agnostic and untouched by consolidate."
```

---

## Task 8: Documentation — system-capabilities + close the register item

**Files:**
- Modify: `docs/system-capabilities/stream.md` — document the merge engines; update Known gaps.
- Modify: `docs/ROADMAP.md` — remove the `road-stream-merge-engines` entry (closed).
- (No code change; this is the `loom-docs-update` step bound into the finishing PR.)

- [ ] **Step 1: Update `docs/system-capabilities/stream.md`**

Under a new **Merge engines** subsection (after *Compaction: `consolidate_stream`*), document: the per-CDC-table `merge_engine` (`last_row` default / `first_row` / `versioned`), the per-type `version` property, the declaration surface (`?merge_engine=`), the precedence table (ORDER BY per engine), the byte-identical-default guarantee, and the engine-agnostic changelog. Reference the two fold sites by file.

In **Known gaps**, remove the `#fut-stream-merge-engines` bullet (the replace-class engines are now built) and replace with a pointer to the deferred aggregate-class engines `#fut-stream-merge-aggregate` (already filed). Bump the `_As of <commit>._` line to the landing commit.

- [ ] **Step 2: Close the register item via `loom-docs-update`**

Run the `loom-docs-update` skill (or edit directly, matching its grammar): remove the `road-stream-merge-engines` entry from `docs/ROADMAP.md`; if `fut-stream-merge-aggregate` is not already in `docs/FUTURE.md`, add it (the aggregate-class remainder the spec carved out). Validate: `bash tools/docs.sh validate docs/ROADMAP.md docs/FUTURE.md`.

- [ ] **Step 3: Commit**

```bash
buck2 run //tools:prek -- run --all-files   # markdown trailing-newline/whitespace hooks
git add -A
git commit -m "docs(stream): document replace-class merge engines; close road-stream-merge-engines

LastRow/FirstRow/Versioned replace-class merge engines are built; document
them in the stream capability. Remove the closed ROADMAP item and file the
deferred aggregate-class remainder (fut-stream-merge-aggregate)."
```

---

## Self-Review (run after writing the plan; revised after plan-review G1–G7)

**1. Spec coverage.** Every spec section maps to a task:
- Ontology `version` property + builder + reverse-lookup + both adapters + testkit ⇒ Task 1 (field/builder) + Task 2 (column/lookup/contract). ✓
- Stream registry `merge_engine` + `StreamMeta` + trait + `declare_cdc` + `pg_stream_meta` ⇒ Task 3. ✓
- Declaration surface `?merge_engine=` + validation (version-required, orderable, immutable) ⇒ Task 4 (validation in `reconcile_stream_mode`, spec-aligned). ✓
- `MergeEngine` enum + precedence semantics table ⇒ Task 1 (enum) + Tasks 5/6 (rendered at both fold sites). ✓
- `consolidate_stream` per-engine ORDER BY ⇒ Task 5. ✓
- `build_merge_view` `Precedence::Offset { engine, version_col }` ⇒ Task 6. ✓
- Byte-identical/non-regression guarantees ⇒ asserted by the non-regression runs in Tasks 5/6. ✓
- Testing (version contract, declaration validation, FirstRow e2e, Versioned e2e, **delete-wins**, LastRow regression) ⇒ Tasks 2, 4, 7. **Delete-wins** is covered by Task 7 Step 3's id=2 case (a `-D` that wins the version tie by offset and is dropped — no resurrection). ✓
- Interfaces (Consumes/Produces) ⇒ each task's Interfaces block names the exact symbols later tasks rely on. ✓

**2. Placeholder scan.** No `TBD`/`TODO`/`add appropriate`/`similar to`; the earlier `stream_meta_for_table_pool_unused()` placeholder was removed (Task 6 Step 3 now passes `&catalog.pool` directly). Task 6 Step 4's `build_merge_view` rewrite is one consolidated code block (no fragment assembly). ✓

**3. Type consistency.** `MergeEngine` (Task 1) is consumed identically in Tasks 3/4/5/6. `ObjectType.version: Option<String>` (Task 1) matches `version_for_table -> Option<String>` (Task 2, executor-based) and `Precedence::Offset { version_col: Option<String> }` (Task 6). `StreamMeta.merge_engine` (Task 3) matches both fold sites (Tasks 5/6). `declare_cdc(.., merge_engine)` (Task 3) matches all ~15 widened callers. `CdcDecl.merge_engine` / `StreamDecl::Cdc { merge_engine }` (Task 4) match `combine_stream_decl`. ✓

**4. Plan-review gaps closed (G1–G7).** G1 delete-wins ⇒ Task 7 Step 3 id=2 case. G2 stale postgres test path/target ⇒ Task 4 Steps 1/7/8 corrected. G3 fragment-assembled `build_merge_view` ⇒ Task 6 Step 4 one consolidated block. G4 bogus private-fn fallback ⇒ Task 7 Step 3 replaced with a determinism note (sequential `await`ed `run_action`; inter-Update `flush_table` if ever needed). G5 `run_action` source ⇒ Task 7 Interfaces corrected. G6 no-op `merge_on_read.rs` instruction ⇒ Task 6 Step 1 corrected. G7 validation reachability ⇒ flipped the deviation: version-property + orderability validation now lives in `reconcile_stream_mode` (Task 4 Step 5), `version_for_table` made executor-based (Task 2). ✓
