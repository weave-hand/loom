# Stream engine — Merge engines (replace-class: LastRow / FirstRow / Versioned) Design

> **Status:** design (direction). This spec makes `road-stream-merge-engines`
> build-ready. A separate work agent writes the implementation plan from it and
> builds it. **Promotes** `fut-stream-merge-engines` (carved down to its
> replace-class engines); the aggregate-class remainder is re-filed as
> [[fut-stream-merge-aggregate]].

## Goal

Generalize the two places a CDC current-state base is folded per-identity —
compaction (`consolidate_stream`) and merge-on-read (`build_merge_view`) — from a
hardcoded **LastRow** policy into a per-table, declared **merge engine**. Land
the three *replace-class* engines (pick one row per identity by precedence):

- **LastRow** (default, byte-identical to today): greatest `loom_offset` wins.
- **FirstRow**: smallest `loom_offset` wins ("first write wins"; later events for
  that identity are ignored for current-state).
- **Versioned**: a user-declared domain **version column** sets precedence
  (highest version wins; `loom_offset` tie-breaks). Handles out-of-order arrival.

A delete (`-D`) is the winning event ⇒ the identity is dropped, uniformly across
all three engines (unchanged from LastRow today). The durable **changelog is
engine-agnostic** — it stays append-only with every event; merge engines govern
only the current-state base.

The *aggregate-class* engines (Aggregation, PartialUpdate — combine rows rather
than pick one) are explicitly **deferred**: they need per-column merge-policy on
the ontology model and a `GROUP BY` fold path distinct from `ROW_NUMBER`. They
are [[fut-stream-merge-aggregate]].

## Context — what ships today (slice 2b)

A declared CDC table is two Iceberg tables: an append-only **changelog** (every
event incl. `-U`) and a current-state **base** (`+I/+U/-D` only). Every event
carries framing: `loom_change_kind` (`+I`/`-U`/`+U`/`-D`), `loom_bucket`,
`loom_offset` (gapless, monotonic per bucket per identity). Current-state reads
fold multiple physical rows per identity down to one by **LastRow** in exactly
two places:

1. **`consolidate_stream`** (`src/services/engine-serving/src/consolidate.rs`,
   `consolidate_locked`, ~line 190) — the compaction fold. DataFusion SQL:
   ```sql
   select {cols}, loom_change_kind, loom_bucket, loom_offset from (
     select *, row_number() over (
       partition by "{identity}" order by loom_offset desc
     ) as _rn
     from ({file_rows} union all {inline_tail}) base_input
   ) t where _rn = 1 and loom_change_kind <> '-D'
   ```
   then `overwrite_parquet_snapshot` (framing-preserving) rewrites the base,
   `clear_has_shadow`, `clear_consolidate_trigger`.

2. **`build_merge_view`** (`src/services/engine-serving/src/serving.rs`,
   ~line 288) — merge-on-read. A `Precedence` enum maps each tier to
   `[_loom_prec, _loom_tomb]`, UNION-ALLs file + inline tiers, then
   `ROW_NUMBER() OVER (PARTITION BY identity ORDER BY _loom_prec DESC)` rank 1
   wins and a tombstoned winner is dropped. `Precedence::Offset` (CDC) maps
   `_loom_prec = loom_offset`, `_loom_tomb = (loom_change_kind = '-D')`;
   `Precedence::Snapshot` (non-CDC identity tables) maps
   `_loom_prec = begin_snapshot`, `_loom_tomb = loom_tombstone`.

Both implement the *same* semantics: pick the winner per identity by a
precedence expression, drop a deleted winner. A merge engine parameterizes that
precedence expression (column + direction). Both sites read the table's engine
at fold time; `build_serving_provider` already gates CDC vs. non-CDC via
`ontology::is_cdc_table` and reads the identity via `identity_for_table`.

## Architecture

**The merge engine is a per-CDC-table property; the version column is a
per-type property.** They live on their natural surfaces and are cross-validated
at CDC declaration.

### Ontology: a `version` type property (mirrors `identity`)

`ObjectType` (`src/control-plane/core/src/ontology.rs`) today has
`identity: Option<String>` — the property name that is the type's primary key,
stored in `ontology.object_type.identity`, declared via builder
`.identity("id")`, reverse-looked-up by `identity_for_table(pool, table)`.

Add a parallel single property:

- `ObjectType.version: Option<String>` — the property name that is the type's
  monotonic version/sequence column. `None` = no version column (back-compatible).
  At most one version property per type.
- A new `ontology.object_type.version text` column (migration after the current
  head). Default `NULL`.
- Builder method `.version(prop: impl Into<String>) -> Self`, mirroring
  `.identity(...)`. Like identity, a dangling name (a `version` that names no
  declared property) surfaces only when used, not at literal construction.
- Reverse-lookup `version_for_table(pool, table) -> Result<Option<String>>` in
  `src/control-plane/postgres/src/ontology.rs`, mirroring `identity_for_table`
  (nullable column ⇒ flatten `Option<Option<String>>`).

The version property is an ordinary user column — it is already in the logical
schema and already projected; it is **never** a `loom_*` reserved column. Both
adapters (postgres + memory fake) carry it; the testkit `Ontology` contract
asserts it round-trips and that at most one is allowed.

### Stream registry: `merge_engine`

`stream.stream_table` (migration after `0037_stream_table_cdc_columns.sql`) gains:

```sql
alter table stream.stream_table
    add column merge_engine text not null default 'last_row'
        check (merge_engine in ('last_row', 'first_row', 'versioned'));
```

`StreamMeta` (`core/src/stream.rs`) gains `merge_engine: MergeEngine`. The
`StreamTables` trait and `pg_stream_meta` read it; `declare_cdc` carries it
(see Declaration). Default `last_row`. **Immutable after declaration** — a
redeclare with a different engine is a `Conflict`, mirroring the existing
bucket-count immutability rule in `reconcile_stream_mode`.

### Declaration surface & validation

The engine is chosen at CDC bind time: `?merge_engine=<engine>` on
`POST /models/{type}` (the CDC bind surface), alongside `?mode=cdc&buckets=N`.
Default `last_row` (so a bare `?mode=cdc&buckets=N` is unchanged).

Declaration-time validation (in `reconcile_stream_mode`, same place
`bucket_key`/`kind` are validated), returning `Validation` (`400`) on failure:

- `merge_engine=versioned` ⇒ the bound type has a declared `version` property
  (`version_for_table(table).is_some()`), else `400` — the same shape as "a CDC
  table requires an identity property". The version column must be orderable
  (an integer, bigint, or timestamp logical type); a non-orderable declared
  version property ⇒ `400`.
- `first_row` / `last_row` ⇒ no version property required (one may still be
  declared on the type; it is simply unused).
- A redeclare of an existing CDC table with a *different* engine ⇒ `Conflict`
  (engine is immutable, like `bucket_count`).

The version column name is **not** copied onto `stream.stream_table` — the fold
sites read it live via `version_for_table` when the engine is `versioned` (the
same pattern `build_merge_view` already uses for the identity column via
`identity_for_table`). The registry stores only the engine choice.

## Precedence semantics

A core enum is the single source of truth for what an engine means for ordering:

```rust
/// The replace-class merge policy for a CDC current-state base. Both fold sites
/// (compaction + merge-on-read) render this to their respective precedence
/// expression. See `docs/.../2026-07-08-stream-merge-engines-design.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MergeEngine {
    LastRow,
    FirstRow,
    Versioned,
}
```

The precedence each engine dictates, applied at **both** fold sites, partitioned
by identity (winner = `ROW_NUMBER` rank 1):

| Engine | ORDER BY (winner = rank 1) | Needs version col |
| --- | --- | --- |
| `LastRow` (default) | `loom_offset DESC` | no |
| `FirstRow` | `loom_offset ASC` | no |
| `Versioned` | `"<version_col>" DESC, loom_offset DESC` | yes |

- **Versioned tie-break.** Equal version values resolve to the greater
  `loom_offset` (last-write-within-version wins). Versioned does **not** require
  versions to arrive in offset order — that is the whole point vs. LastRow: a
  late event carrying a higher domain version wins. The version column is quoted
  (injection-safe), matching `quote_ident` in `consolidate.rs` / the derived-SQL
  precedent.
- **Delete is uniform.** The winning event is `-D` (equivalently tombstoned) ⇒
  the identity is dropped from current-state, for **all three** engines.
  `consolidate.rs` keeps `loom_change_kind <> '-D'`; `build_merge_view` keeps
  `_loom_tomb = (loom_change_kind = '-D')`. For FirstRow this is mostly moot in
  practice (a delete is usually a later, higher-offset event, so it cannot be the
  earliest), but the rule is stated uniformly so no engine can resurrect a
  deleted key.
- **FirstRow semantics.** "First write wins": the first event for a key fixes
  its current-state value; later `+U`/`-D` events for that key are ignored for
  current-state (they still land in the durable changelog). Standard
  dedup-on-insert merge-engine behavior.
- **Changelog is engine-agnostic.** Merge engines govern only the current-state
  base (read + consolidate). The changelog stays append-only with every event; a
  Versioned/FirstRow table's changelog is identical to a LastRow table's.

### Correctness invariant across consolidate cycles

`consolidate_stream` physically rewrites the base to the folded set. After a
consolidate, a Versioned base holds the highest-version row per identity. A
subsequent *late* event with a *lower* version than the already-folded winner
must still lose on the next read — and it does, because the folded winner's
version is preserved in the base and the late event's version is lower. So
Versioned stays correct across consolidate cycles. FirstRow is trivially stable
(a late event can never displace the earliest). The implementation must not
"compact away" the version column or the precedence data the next fold needs —
the folded base retains framing (`loom_offset`) and the version column is a user
column already retained.

## The two fold sites

Both sites read the engine (`stream_meta(tid).merge_engine`) and, for
`Versioned`, the version column (`version_for_table`).

### `consolidate_stream` (`consolidate.rs`)

The fold SQL's `order by loom_offset desc` becomes a per-engine clause, drawn
from the engine + (for Versioned) the quoted version column. The `where _rn = 1
and loom_change_kind <> '-D'` predicate is unchanged (delete is uniform). The
read (files ∪ inline tail), the framing-preserving `overwrite_parquet_snapshot`,
`clear_has_shadow`, and `clear_consolidate_trigger` are unchanged. A non-CDC
table still no-ops (returns snapshot id `0`); a CDC table with the default
`last_row` engine folds byte-identically to today.

### `build_merge_view` (`serving.rs`)

Generalize `Precedence` so the CDC arm carries the engine:

```rust
enum Precedence {
    Snapshot,                  // non-CDC identity tables — unchanged
    Offset { engine: MergeEngine, version_col: Option<String> },
}
```

`build_serving_provider` builds the `Offset` arm from `stream_meta` (engine) and
— only for `Versioned` — `version_for_table`; `is_cdc_table` still routes CDC vs.
non-CDC. The precedence `Expr` and the window's `ORDER BY` direction are derived
from the engine:

- The `_loom_prec` expression: for `LastRow`/`FirstRow`, `loom_offset`; for
  `Versioned`, `"<version_col>"` (so the window orders by version). The sort
  direction: `DESC` for `LastRow`/`Versioned`, `ASC` for `FirstRow`.
- For `Versioned`'s version-tie-break by offset, the window's `ORDER BY` becomes
  `_loom_prec DESC, loom_offset DESC`. (For `LastRow`/`FirstRow` the single-key
  order is unchanged in shape; `loom_offset` is unique per identity per bucket, so
  no tie-break is needed.)
- `_loom_tomb = (loom_change_kind = '-D')` is unchanged (delete is uniform).

The final projection back to the mirror data schema is unchanged — the version
column is a user column already in `schema`, and the `_loom_*` helper columns are
dropped as today. No framing leak.

## Byte-identical & non-regression guarantees

- **Default = LastRow = today, byte-identical.** A table with `merge_engine =
  'last_row'` (the column `DEFAULT`, and the value a bare `?mode=cdc&buckets=N`
  produces) folds with the exact `loom_offset DESC` expressions in use today.
  `consolidate.rs` LastRow SQL and `Precedence::Offset { engine: LastRow, .. }`
  render to the same precedence. Non-CDC tables (batch, log, non-stream identity)
  are entirely untouched: `consolidate_stream` still no-ops off the `kind='cdc'`
  gate; `build_merge_view` still uses `Precedence::Snapshot`.
- **No framing leak.** The version column is a user column already in the logical
  schema; it is never a `loom_*` reserved column. Merge-on-read projects back to
  exactly the mirror data schema (the `_loom_*` helpers are dropped, unchanged).
- **Non-CDC / non-versioned paths stay byte-identical.** Every new branch is
  gated on the engine value; the `Snapshot` precedence and the LastRow default
  are the unchanged baselines. Existing fixture tests (e.g.
  `//src/services/query-api:stream-cdc-consolidate`,
  `//src/services/query-api:stream-cdc-e2e`,
  `//src/services/query-api:update-delete-e2e`) must stay green and unchanged.

## Testing

Tests are `rust_test` / `loom_fixture_test` integration targets only — never
inline `#[cfg(test)]`. New fixture tests use the `loom_fixture_test` macro and
wire their own target in the crate's `BUCK`, mirroring an existing sibling.

- **Ontology version property** (`//src/control-plane/...:ontology`, testkit
  contract + both adapters): `.version("seq")` round-trips through store → load;
  `version_for_table(table)` reverse-lookups it; at most one version property; a
  type with no version property reads `None`.
- **Declaration validation** (`//src/services/query-api:stream-merge-declare`,
  new `loom_fixture_test`):
  - `?merge_engine=versioned` on a type with no declared `version` property ⇒
    `400 Validation`.
  - `?merge_engine=versioned` where the version property is a non-orderable type
    ⇒ `400`.
  - `?merge_engine=first_row` and the default (no param) ⇒ accepted with no
    version property required.
  - Redeclare an existing CDC table with a different engine ⇒ `Conflict`.
- **FirstRow e2e** (`//src/services/query-api:stream-merge-firstrow`, new
  `loom_fixture_test`): insert id=1 (v=10), update id=1 (v=20), delete id=1,
  insert id=2. Assert current-state read shows id=1=v=10 (first write wins; the
  update and the delete are ignored for current-state because they are not the
  earliest event) and id=2 present. Assert the changelog holds every event
  (engine-agnostic).
- **Versioned e2e** (`//src/services/query-api:stream-merge-versioned`, new
  `loom_fixture_test`): emit id=1 events out of offset order but with explicit
  versions (e.g. v=5, then v=3, then v=7). Assert current-state shows v=7
  (highest version wins regardless of arrival order). Trigger a consolidate and
  assert the folded base holds v=7; emit a late event with v=4 and assert it
  still loses on the next read (correctness invariant across consolidate cycles).
- **Delete-wins** (folded into the FirstRow/Versioned e2e or its own target): a
  Versioned/FirstRow table whose highest-precedence winner is a `-D` drops the
  identity (no resurrection).
- **LastRow regression**: the existing CDC e2e (`stream_cdc_consolidate`,
  `stream_cdc_e2e`) is byte-identical after the change (engine defaults to
  `last_row`). These existing targets must pass unchanged.

## Global constraints (loom-specific, carry into the plan)

- **Tests are `rust_test` / `loom_fixture_test` integration targets only.** New
  fixture tests MUST use `loom_fixture_test` (`src/control-plane/postgres/defs.bzl`),
  not a bare `rust_test`, or the fixture env (PG binaries, MinIO, boot-slot dir)
  is missing. The `no-inline-tests` prek hook fails the build on any inline
  `#[test]`.
- **After changing any `query!`/`query_scalar!` SQL** (the new `version` column on
  `ontology.object_type`, the new `merge_engine` column on `stream.stream_table`),
  run `tools/sqlx-prepare.sh` and commit `.sqlx/`; `sqlx-cache-check` enforces
  freshness.
- **Run prek before every commit:** `buck2 run //tools:prek -- run --all-files`
  (rustfmt is a separate hook; clippy-clean ≠ lint-clean). Markdown files end
  with exactly one trailing newline and no trailing whitespace.
- **Clippy is strict (pedantic + restriction):** no `unwrap`/`expect`/
  `indexing_slicing`/`panic`/`todo` in production lib/bin code. `#[expect(lint,
  reason = "...")]` for a justified local exception. Test code is exempted from
  the panic-safety lints via `loom_rust_test`/`loom_fixture_test`.
- **Non-CDC / LastRow paths must stay byte-identical** (see above). Framing stays
  hidden from logical reads; the version column is a user column, never reserved.

## Non-goals (deferred)

- **Aggregate-class merge engines** — Aggregation (sum/max/min/count/last-value
  per column) and PartialUpdate (last-non-null field merge). These *combine* rows
  per identity rather than pick one, need per-column merge-policy on the ontology
  model, and use a `GROUP BY` fold path distinct from `ROW_NUMBER`. Filed as
  [[fut-stream-merge-aggregate]].
- Changing the durable changelog. It is engine-agnostic and untouched.
- A user-facing changelog read / subscribe feed (that is slice 3,
  `road-stream-subscribe`).
- Multiple version properties per type, or composite version keys. One version
  property, mirroring the single `identity`.
- Re-declaring/changing the engine post-declaration (immutable by design).

## Interfaces (names the plan consumes)

- Consumes: `ObjectType { identity }` + `.identity()` (`core/src/ontology.rs`),
  `identity_for_table` (`postgres/src/ontology.rs:621`), `StreamMeta`
  (`core/src/stream.rs`), `declare_cdc` / `reconcile_stream_mode`
  (`postgres/src/stream.rs`), `consolidate_stream`/`consolidate_locked`
  (`engine-serving/src/consolidate.rs`), `Precedence`/`build_merge_view`/
  `build_serving_provider` (`engine-serving/src/serving.rs`), `is_cdc_table`
  (`postgres/src/ontology.rs:614`), `overwrite_parquet_snapshot`
  (`postgres/src/iceberg_landing.rs`), `quote_ident` (`consolidate.rs`).
- Produces (later plan tasks rely on these EXACT names/types):
  - `ObjectType.version: Option<String>` + `fn version(prop) -> Self` builder.
  - `ontology.object_type.version` column (migration) +
    `version_for_table(pool, table) -> Result<Option<String>>`.
  - `enum MergeEngine { LastRow, FirstRow, Versioned }` in `control_plane_core`,
    re-exported; `StreamMeta.merge_engine: MergeEngine`.
  - `stream.stream_table.merge_engine` column (migration) + `StreamTables`/trait
    + `declare_cdc` carrying the engine; `pg_stream_meta` selecting it.
  - `Precedence::Offset { engine: MergeEngine, version_col: Option<String> }`,
    with the precedence `Expr` + sort direction derived from the engine in
    `build_merge_view`; the matching per-engine `ORDER BY` in `consolidate.rs`.
