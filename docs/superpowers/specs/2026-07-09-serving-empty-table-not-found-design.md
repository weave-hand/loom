# Serving: a live-but-empty table must read as zero rows, not `table not found` — Design

> **Status:** design (direction). This spec makes `iss-serving-empty-table-not-found`
> build-ready. A separate work agent implements it from the matching plan
> (`docs/superpowers/plans/2026-07-09-serving-empty-table-not-found.md`).

## Goal

A table that is **live in the mirror but holds no data** — no live inline rows
and no cold Parquet files — must register in the serving `SessionContext` and
answer every read as **zero rows with the table's authoritative mirror schema**,
instead of being absent from the serving catalog and answering
`table '…' not found`. A table that is genuinely **not live** (never declared,
dropped, or — for an as-of read — not yet live at the pinned snapshot) must
keep answering not-found exactly as today. Once the serving path holds this
invariant, the transform worker's compensating `Validation`-means-empty guard
is dropped.

## Context — defect mechanics

`build_serving_provider` (`src/services/engine-serving/src/serving.rs:74`)
builds the combined file ∪ inline provider for a table at a snapshot. When a
live table has neither tier, both match arms bail with no provider:

- identity-less: `(None, None) => return Ok(None)` (`serving.rs:201`);
- identity-bearing: `(None, None) => return Ok(None)` (`serving.rs:218`).

(The register entry cites the pre-refactor line `serving.rs:176`; the two arms
above are the current locations. Likewise the register's
"`logical_arrow_schema` machinery at `serving.rs:409`" is today
`arrow_schema_from_mirror` at `serving.rs:620`, already invoked at
`serving.rs:122` — the schema needed for the fix is **already in scope** at
both arms.)

`Ok(None)` makes every registration caller skip the table:

- `register_iceberg_table` (`serving.rs:493-496`) returns without registering,
  so `execute_query_stream` (`serving.rs:846`, which registers every
  `live_tables()` entry at `serving.rs:853-854`) plans SQL against a catalog
  that lacks the table;
- `execute_governed_sql_stream` (`src/services/engine-serving/src/governed.rs:297-301`)
  `continue`s past it for the governed SQL surface.

A `SELECT *` then fails DataFusion **planning** → `EngineServingError::Plan`
(`serving.rs:856`) → Flight `invalid_argument`
(`src/services/engine/src/flight.rs:44`) → `ControlPlaneError::Validation`
(`src/services/engine-wire/src/client.rs:57`) → a client-fault error for what
is actually a well-formed read of an existing table. Observed effects:

- `GET /datasets/{schema}/{table}/preview` of the empty `employee_floor` output
  failed `table 'datafusion.public.employee_floor' not found` in the
  `2026-07-07-transform-inline-input-read-design` investigation (handler
  `dataset_preview`, `src/services/query-api/src/http.rs:373`).
- The transform worker must special-case it: `run_wire_transform`
  (`src/services/worker/src/transform.rs:273-285`) treats
  `Err(ControlPlaneError::Validation(_))` on an input read as "exists but
  empty" (`transform.rs:278`) **because** `list_files` already proved the table
  exists — a guard that conflates a real client fault with emptiness and exists
  only to paper over this serving gap (see the comment block at
  `transform.rs:247-255`).

## Architecture — the fix

### One helper, two arm replacements

Add a private helper in `serving.rs`:

```rust
/// A zero-row provider presenting the mirror's authoritative `schema`. Used for
/// a live table with no data in either tier, so the table still registers (and
/// reads as empty-with-schema) instead of being absent from the serving catalog.
fn empty_provider(schema: &SchemaRef) -> Result<Arc<dyn TableProvider>, EngineServingError> {
    // One EMPTY partition, not zero partitions — `MemTable::try_new` rejects an
    // empty partition list (same note as `datafusion_io::scan::register_batches`,
    // scan.rs:131-134).
    let mem = MemTable::try_new(schema.clone(), vec![Vec::new()]).map_err(to_serving)?;
    Ok(Arc::new(mem))
}
```

and replace both `(None, None) => return Ok(None)` arms with
`(None, None) => empty_provider(&schema)?`. `schema` is the mirror-derived
arrow schema already built at `serving.rs:122` via `arrow_schema_from_mirror`
(`serving.rs:620`) — the same authoritative schema every non-empty tier
presents, so an empty table's schema (names, `DataType`s, nullability) is
byte-identical to what the table will present once rows land.

Notes on the two arms:

- **Identity-less** (`:201`): the empty `MemTable` replaces the union — trivial.
- **Identity-bearing / CDC** (`:218`): with zero physical rows there is nothing
  to fold, so the empty `MemTable` over the plain **data** schema is exactly
  what `build_merge_view` would project back to. No `loom_*` framing columns
  appear (the CDC `file_schema` augmentation at `:153-157` applies only to the
  file tier, which does not exist here) — no framing leak.

`build_serving_provider`'s signature is unchanged
(`Result<Option<Arc<dyn TableProvider>>>`). After the fix `Ok(None)` has
exactly one meaning: **the table is not live at the requested snapshot** (the
as-of `NotFound` arm, `serving.rs:109-115`). Update the function's doc comment
(`serving.rs:69-73`, "…or `None` when the table has no live data") accordingly.

### The not-found boundary, drawn precisely

"Live" comes from the mirror catalog: a row in `iceberg_mirror.table` with
`end_snapshot is null` — exactly the set `IcebergCatalog::live_tables()`
returns (`src/control-plane/postgres/src/iceberg_catalog.rs:184-200`). Every
live table has a current snapshot (a mirror `table` row carries a
`begin_snapshot`, so `current_snapshot`'s lookup at `iceberg_catalog.rs:206-227`
cannot `NotFound` for a live table — the fixture comment at
`worker/tests/transform_e2e.rs:148-151` documents the same invariant from the
write side).

| Table state | Before | After |
| --- | --- | --- |
| Live, has data (either tier) | registered, rows | unchanged |
| Live, zero data (no inline, no files) | **not registered → `table not found`** | **registered, zero rows + mirror schema** |
| Never in the mirror / dropped (`end_snapshot` set) | not registered → `table not found` (`Plan` → 400/`Validation`) | unchanged |
| As-of: not yet live at the pinned snapshot | `Ok(None)` → not registered → not found | unchanged |
| As-of: live at the pinned snapshot, zero data at it | not registered → not found | registered, zero rows (correct: it existed, empty) |

No caller legitimately relies on live-but-empty ⇒ not-found:

- query-api's 404s for unknown **types/links** (`query_error_http.rs`
  `not_found_variants`) resolve at the ontology layer before any SQL — untouched.
- The only code that *depended* on the old behavior is the worker guard this
  spec deletes.
- The governed path registers the empty provider wrapped in
  `GovernedTableProvider` like any other live table (`governed.rs:302-304`).
  This discloses no new existence information: every live table is already
  registered in the governed context regardless of the subject's policy;
  emptiness now behaves uniformly. Row filters / masks over zero rows are
  vacuous (zero rows in ⇒ zero rows out).

### Worker: drop the `Validation`-means-empty guard

With the engine registering empty tables, `ctx.sql.execute(select_all_sql(table))`
on a live-but-empty input returns `Ok` with zero batches, which the existing
zero-batch arm already handles (`transform.rs:286-292`: `batches.first()` is
`None` → `register_empty_table` with the declared schema via
`logical_arrow_schema`, `datafusion-io/src/infer.rs:52`). So:

- Delete the `Err(ControlPlaneError::Validation(_)) => Vec::new()` arm
  (`transform.rs:278`) — the read result collapses to `Ok(batches)` /
  `Err(e) ⇒ Retry`.
- Rewrite the step-2/3 comment block (`transform.rs:247-255`) — `ListFiles`
  remains the existence + declared-schema oracle; the "Validation means empty"
  sentence goes away.
- Semantics improvement: a *genuine* `Validation` on an input read (e.g. the
  input dropped between `list_files` and the read) now retries loudly instead
  of silently registering an empty relation.

The zero-batch → `register_empty_table` arm **stays**: a Flight-decoded empty
result can legitimately arrive as zero `RecordBatch`es, and the declared-schema
registration is still the right recovery for that shape.

## Non-regression

- Non-empty tables: byte-identical — the fix touches only the `(None, None)`
  arms, which are unreachable when either tier exists.
- The as-of skip (`serving.rs:109-115`) is untouched; `serving_as_of` tests
  stay green.
- Error classes: `Plan`/`invalid_argument`/`Validation` mapping unchanged;
  unknown tables still produce exactly the same not-found planning error.
- Worker: `transform_e2e.rs`'s `empty_input_counts_zero` (`:474`) and
  `empty_input_select_star_commits_empty_output` (`:642`) — which today
  exercise the `Validation`-means-empty arm — must stay green through both the
  serving change (they start taking the `Ok`-empty path) and the guard
  deletion. `inline_only_input_is_read` (`:286`), `unknown_input_abandons`
  (`:385`) and the rest of the worker suite stay green.

## Testing

Tests are `rust_test` / `loom_fixture_test` integration targets only — never
inline `#[cfg(test)]`. The new fixture test wires its own `loom_fixture_test`
target in `src/services/engine-serving/BUCK`, mirroring the `serving_as_of`
sibling.

New `src/services/engine-serving/tests/serving_empty_table.rs` (harness:
`IcebergControlPlane` `begin_table`/`create_table`/`append_files(&[])`/`commit`
— the live-zero-file fixture proven in `worker/tests/transform_e2e.rs:145-154`):

- **Empty live table serves zero rows with the mirror schema** —
  `build_serving_provider` returns `Some`; scanning it yields 0 rows; the
  provider's schema matches the declared columns (names/types/nullability).
- **`SELECT *` over an empty live table returns an empty result** — via
  `execute_query` (the exact path previews and worker input reads consume).
- **Unknown table still plan-errors** — `execute_query` against an undeclared
  table stays `Err(EngineServingError::Plan(_))` (pins the boundary).
- **Identity-bearing empty table** — bind an ontology type with an identity to
  the empty table; still `Some` + zero rows (covers the `:218` arm).
- **As-of not-live stays `None`** — `build_serving_provider(.., at:
  Some(SnapshotId(0)))` (a snapshot before the table existed) returns `Ok(None)`.
- **Governed read of an empty table** — `execute_governed_sql_stream` with a
  policy (and with none) returns zero rows, not an error.

Worker (existing tests as evidence, no new file): after dropping the guard, the
whole `//src/services/worker:transform-e2e` target — in particular the two
empty-input tests — stays green.

## Global constraints (loom-specific, carry into the plan)

- **Tests are `rust_test` / `loom_fixture_test` integration targets only.** New
  fixture tests MUST use `loom_fixture_test`
  (`src/control-plane/postgres/defs.bzl`), not a bare `rust_test`; wire the
  BUCK target mirroring a named sibling. The `no-inline-tests` prek hook fails
  the build on any inline `#[test]`.
- **No `.sqlx` change expected** — the fix adds no SQL. If any `query!` SQL is
  touched, run `tools/sqlx-prepare.sh` and commit `.sqlx/`.
- **Run prek before every commit:** `buck2 run //tools:prek -- run --all-files`
  (rustfmt is a separate hook; clippy-clean ≠ lint-clean). Markdown files end
  with exactly one trailing newline, no trailing whitespace.
- **Clippy is strict (pedantic + restriction):** no `unwrap`/`expect`/
  `indexing_slicing`/`panic`/`todo` in production lib/bin code; the
  `empty_provider` sketch above is already `?`-based. Test code is exempted
  from the panic-safety lints via `loom_rust_test`/`loom_fixture_test`.
- **No framing leak:** the empty provider presents the mirror **data** schema
  only — never `loom_*` reserved columns.
- **Build/test commands** (CLAUDE.md): build
  `buck2 build -v0 --console none //src/...`; test
  `buck2 test --console none <targets>` (cloud: `-M none` on builds, scope
  tests; full local suite needs `-j 8`).

## Non-goals (deferred / out of scope)

- **Vector `/search`** — does not route through `build_serving_provider` (its
  callers are `register_iceberg_table` and `execute_governed_sql_stream` only);
  the `iss-search-vector` nullability question is unrelated and untouched.
- **The external SQL wire** — deliberately deferred (CLAUDE.md); this changes
  only the internal serving registration.
- **Worker zero-batch handling** — the `register_empty_table` declared-schema
  arm stays; unifying the engine-side `arrow_schema_from_mirror` with
  datafusion-io's `logical_arrow_schema` is a separate dedup idea, not taken
  here.
- **Registering empty tables lazily / schema-only providers with pushdown** —
  an empty `MemTable` is O(1); no optimization needed.

## Interfaces

- Consumes:
  - `build_serving_provider` (`src/services/engine-serving/src/serving.rs:74`),
    its two `(None, None)` arms (`serving.rs:201`, `:218`), and the in-scope
    `schema` from `arrow_schema_from_mirror` (`serving.rs:122`, defined `:620`).
  - `datafusion::datasource::MemTable` (empty-partition precedent:
    `src/services/datafusion-io/src/scan.rs:131-134`).
  - `to_serving` (`serving.rs:65`); `register_iceberg_table` (`serving.rs:486`);
    `execute_query`/`execute_query_stream` (`serving.rs:828`/`:846`);
    `execute_governed_sql_stream`
    (`src/services/engine-serving/src/governed.rs:289`).
  - `IcebergCatalog::live_tables`
    (`src/control-plane/postgres/src/iceberg_catalog.rs:184`).
  - Worker guard site: `src/services/worker/src/transform.rs:273-285` (arm at
    `:278`; comment block `:247-255`); zero-batch arm `:286-292`;
    `register_empty_table` (`src/services/datafusion-io/src/scan.rs:115`).
  - Test fixtures: `IcebergControlPlane`
    (`src/control-plane/postgres/src/iceberg_control_plane.rs:35`, `begin_table`
    `:88`), the empty-table recipe (`worker/tests/transform_e2e.rs:145-154`),
    `local_sql_catalog` (`//src/testing:seed`), the `serving_as_of`
    `loom_fixture_test` BUCK target (the sibling to mirror).
- Produces (the plan relies on these EXACT names):
  - `fn empty_provider(schema: &SchemaRef) -> Result<Arc<dyn TableProvider>,
    EngineServingError>` (private, `serving.rs`), and both `(None, None)` arms
    rewritten to `empty_provider(&schema)?`.
  - `build_serving_provider` doc contract: `Ok(None)` ⇔ not live at the
    requested snapshot.
  - `run_wire_transform` without the `Err(ControlPlaneError::Validation(_))`
    arm (`worker/src/transform.rs`).
  - New test target `//src/services/engine-serving:serving-empty-table`
    (`tests/serving_empty_table.rs`).
