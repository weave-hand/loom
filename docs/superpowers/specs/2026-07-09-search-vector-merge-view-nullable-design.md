# `/search` vector nullability 500 — hot-delta tombstones + merge-view inline-tier nullability — Design

> **Status:** design (direction). This spec makes `iss-search-vector-merge-view-nullable`
> build-ready. A separate work agent writes/executes the implementation plan
> (`docs/superpowers/plans/2026-07-09-search-vector-merge-view-nullable.md`) from it.

## Goal

Make `/search` (and every serving-path read) over an identity-bearing
`vector(N)` type survive a live inline delta — including a DELETE tombstone —
without a 500, and un-ignore the two blocked acceptance cases in
`src/services/query-api/tests/vector_search_cold_suppression_e2e.rs`
(`cold_hits_suppressed_with_no_row_filter`,
`cold_hit_suppressed_with_row_filter_regression`).

## Context — the registered defect, and the corrected diagnosis

The register entry attributes the 500 to `build_merge_view`'s UNION projecting
back to the non-nullable mirror schema, failing **only on the first** execution
("process-global DataFusion schema-inference nondeterminism"). This spec was
grounded by **reproducing the failure in-session** (running the two `#[ignore]`'d
e2e cases with temporary instrumentation, since reverted) and the diagnosis is
**corrected**: there are two concrete, fully **deterministic** defects, and no
nondeterminism anywhere.

### Verified failure message and validation site

```text
Invalid argument error: Column 'embedding' is declared as non-nullable but contains null values
```

This is arrow's `RecordBatch::try_new` nullability validation
(arrow-array 58.3.0, `src/record_batch.rs:350`): constructing a batch whose
schema declares a field non-nullable while its array carries nulls is an
`InvalidArgumentError`. Both defects are loom constructing exactly such a batch;
neither is inside DataFusion's planner.

### Defect A (the actual `/search` 500) — `inline_delta_batch` includes tombstone rows

`vector_search`'s hot path (`engine-serving/src/vector_search.rs:124-135`) reads
the inline delta via `inline_delta_batch`
(`src/control-plane/postgres/src/vector_index.rs:287`). Its SQL
(`vector_index.rs:357-365`) selects `(identity, vector)` for every inline row
born after the index's covered snapshot and MVCC-live at Q
(`mvcc_live_pred`, `iceberg_inline.rs:56`) — with **no tombstone or `-U`
exclusion**. A DELETE writes a tombstone inline row carrying **only the
identity** (`write_inline_delta`, `iceberg_inline.rs:1036`; tombstone insert at
`:1168-1171` sets `loom_tombstone=true`, `loom_change_kind='-D'`, all other data
columns NULL). The vector column is therefore NULL for that row, while
`inline_delta_batch`'s output schema hardcodes the vector field non-nullable
(`vector_index.rs:378-385`) — `RecordBatch::try_new` fails, the error propagates
as `EngineServingError::Engine` → `/search` 500.

Deterministic: **every** `/search` over the type fails once a live tombstone
inline row exists in the hot window — verified 3/3 runs pre-fix, 3/3 green
post-fix with the exclusion predicate applied experimentally.

### The "first query fails, subsequent succeed" observation, explained

There is no first-query effect and nothing process-global. In the e2e,
`cold_hit_suppressed_with_row_filter_regression` (UPDATE only — a full-row
inline delta, vector non-null) **passes**, and
`cold_hits_suppressed_with_no_row_filter` fails only on its **second** probe —
the one issued **after the tombstone write**. Same-process test ordering plus
the identical error text from Defect B (below) on ad-hoc full-column debug
queries is what made this look order-dependent. Instrumented runs pinpoint the
construction site (`inline_delta_batch`) and show the merge view itself executes
fine for the identity-only post-filter query.

### Defect B (latent, the register's actual "merge view nullable" concern) — the inline tier's declared schema lies about tombstone rows

Verified in-session with a probe: after the same tombstone write,
`GET /objects/Docs` (a full-column read over the merged view) **500s
deterministically** with the identical message. Mechanism:

- `build_inline_provider` (`engine-serving/src/serving.rs:541`) presents the
  mirror data schema — `embedding` non-nullable (`required: true`) — plus the
  four framing columns (`serving.rs:591-605`).
- The merge view legitimately **needs** tombstone rows from the inline scan
  (their identity + `_loom_tomb` hide the file row), and those rows physically
  carry NULL in every non-identity data column.
- When the query's projection includes such a column,
  `PgTableProvider::fetch_batch` builds the scan batch under the non-nullable
  projected schema (`engine-serving/src/provider.rs:146`) → the same arrow
  validation error, **before** the merge fold ever filters the tombstone out.

The identity-only survivor post-filter (`query-api/src/handler.rs:733` projects
just the identity; DataFusion's projection pruning drops the vector column from
the inline scan) is why `/search`'s own merge-view read does NOT hit Defect B —
confirmed by instrumentation. Any read that materializes a non-nullable
non-identity column does. Existing UPDATE/DELETE e2es never caught this because
their fixtures (e.g. `Widget` — `e2e_support.rs:880-892`) declare all
non-identity properties optional (nullable); `Docs.embedding` is the tree's
first **required** non-identity column combined with a tombstone.

## Architecture — the fix

Two independent fixes, one per defect. Both are data/schema honesty fixes; no
DataFusion machinery changes.

### Fix A — exclude non-scoreable rows from the hot delta (closes the `/search` 500)

In `inline_delta_batch`'s SQL (`vector_index.rs:357-365`), extend the WHERE with:

```sql
and not loom_tombstone
and (loom_change_kind is null or loom_change_kind <> '-U')
and {vec_quoted} is not null
```

- `not loom_tombstone` — a tombstoned identity contributes no hot vector. Its
  stale **cold** hit is then suppressed by the survivor post-filter, exactly the
  contract `2026-07-07-search-cold-suppression-design.md` established. Covers
  both the non-CDC tombstone and a CDC `-D` (both set `loom_tombstone=true`).
- `<> '-U'` — CDC before-images are audit rows, never current-state; mirrors the
  merge-view base predicate (`iceberg_inline.rs:868-872`, `serving.rs:570-574`).
- `{vec_quoted} is not null` — defense-in-depth: a row without a vector cannot
  be scored (`score_inline_batch` would compute a garbage distance from an
  empty slice); this also keeps the output schema's non-nullable vector field
  truthful, so `RecordBatch::try_new` can never fail here again.

Every inline table carries `loom_tombstone`/`loom_change_kind` (both `NOT NULL`
with defaults — `inline_ddl`, `iceberg_inline.rs:277-297`, plus the
`add column if not exists` backfills at `:376-381`), so the predicate is valid
against every existing inline relation. The output schema
(`vector_index.rs:378-384`) stays byte-identical — the seam contract
(`postgres/tests/vector_index_inline_delta.rs`, `int_identity_delta_batch_shape`)
pins it.

### Fix B — the inline tier declares what tombstone rows actually contain (closes the latent full-column 500)

In `build_inline_provider`'s merge-mode branch (`serving.rs:591-605`, taken when
`identity.is_some()`), present every **non-identity** data column as
**nullable** (`Field::with_nullable(true)`); the identity column keeps its
mirror nullability (a tombstone always carries the identity —
`extract_id_cell`), and the four framing fields are unchanged. The identity-less
branch (`schema.clone()`) is untouched — identity-less types cannot be
inline-shadowed, so their inline rows never carry framing NULLs.

Downstream consequences, all verified against the actual code:

- `PgTableProvider::fetch_batch` (`provider.rs:146`) now constructs scan batches
  under a truthful schema — no validation error, tombstone rows flow into the
  fold and are dropped by the `_loom_tomb = false` filter as designed.
- The logical UNION widens per input: DataFusion 54 derives union field
  nullability as *any input nullable*
  (`Union::derive_schema_from_inputs_by_position`,
  datafusion-expr `logical_plan/plan.rs:3082`, nullable-any at `:3130`). The
  window/filter/final projection are pass-through, so the merged view's output
  schema keeps exact mirror **names, types, and order**, with non-identity
  columns widened to nullable **when an inline tier is present**. This is
  precisely the register's sanctioned fix shape ("widen to nullable"), and the
  same defensive widening the identity-less union already documents
  (`serving.rs:181-184`).
- **Coercion back to non-nullable was considered and rejected**: the failure
  site is the inline tier *scan*, upstream of any projection — a provider must
  declare what its batches can contain, and DataFusion has no
  "assert-non-null" projection to legally restore the mirror flag mid-plan.
  Presenting a non-nullable schema over data that transiently holds NULLs is the
  bug, not a fix.
- `build_merge_view`'s doc comment (`serving.rs:269-293`) currently claims "the
  view's schema equals `schema` exactly". Revise it: names/types/order stay
  exact; non-identity **nullability** follows the union (widened iff an inline
  tier exists). No caller asserts nullability equality (verified: the governed
  wrapper projects/filters over whatever provider schema it wraps; query-api's
  `batches_to_rows`/JSON render are value-driven; the Flight wire encodes the
  served schema as-is).
- The file-only arms (`serving.rs:209-217`) keep the mirror-exact schema — a
  type's declared nullability only widens while it has live inline rows. That
  per-snapshot variation is benign for reads (only the declared flag moves,
  never values).

## Non-regression

- **Merge-view behavior for non-vector types stays byte-identical in values**,
  and byte-identical in schema for every existing e2e fixture: `Widget`
  (`name`/`qty` optional), `customer`/`orders`/`line_items` — their non-identity
  columns are already nullable, so Fix B's widening is a no-op for them. Pinning
  tests: `//src/services/query-api:cow-inline-shadow-e2e` (UPDATE shadow +
  DELETE tombstone reads), `:datafusion-inline-union`, `:merge-on-read` /
  object-read graph suites, and the CDC folds
  (`:stream-merge-firstrow-e2e`, `:stream-merge-versioned-e2e`,
  `:stream-cdc-e2e`).
- **The hot-delta seam contract stays byte-identical** for live row-versions:
  `//src/control-plane/postgres:vector-index-inline-delta`
  (`int_identity_delta_batch_shape` — field names/types/nullability, MVCC
  window) pins Fix A's unchanged output shape.
- **`/search` cold/hot merge for non-tombstone data is unchanged**: the 11-case
  `//src/services/query-api:vector-search-e2e` suite pins it.
- `inline_delta_batch`'s other callers: none — it is called only from
  `vector_search` (verified by grep).

## Testing

Tests are `rust_test` / `loom_fixture_test` integration targets only — never
inline `#[cfg(test)]`. New fixture tests use `loom_fixture_test`
(`src/control-plane/postgres/defs.bzl`) with BUCK wiring mirroring a named
sibling.

- **Fix A seam test** — extend
  `src/control-plane/postgres/tests/vector_index_inline_delta.rs` (target
  `vector-index-inline-delta`): after the existing cold+hot seed, write a
  tombstone inline delta (the `write_inline_delta(..., tombstone=true,
  id_batch)` pattern from `vector_search_cold_suppression_e2e.rs`), then
  `inline_delta_batch` over the widened window. Pre-fix: `Err` (the arrow
  validation). Post-fix: `Ok(Some)` containing only the scoreable row-version
  ids, tombstoned identity absent, vector column `null_count() == 0`.
- **Fix B e2e** — new
  `src/services/query-api/tests/vector_objects_read_tombstone_e2e.rs`
  (`loom_fixture_test`, BUCK mirrors `vector-search-cold-suppression-e2e`):
  `seed_vector_type`, inline UPDATE on id=1 + tombstone DELETE on id=2, then
  `GET /objects/Docs` (full-column read → materializes `embedding`). Pre-fix:
  500. Post-fix: 200; ids `[1, 3, 4]`; id=1 serves the **updated** embedding
  (the merge winner's values, proving the fold still works over the widened
  schema).
- **Acceptance** — delete both `#[ignore]` attributes in
  `vector_search_cold_suppression_e2e.rs` (`:108-112`, `:207-209`); both cases
  must pass, repeatedly (they were verified green 3/3 in-session with Fix A
  applied experimentally).
- **Non-regression sweep** — the pinning targets named above, then the full
  `buck2 test //src/...`.

## Global constraints (loom-specific, carry into the plan)

- **Tests are `rust_test` / `loom_fixture_test` integration targets only.** New
  fixture tests MUST use `loom_fixture_test`
  (`src/control-plane/postgres/defs.bzl`), not a bare `rust_test`; the
  `no-inline-tests` prek hook fails the build on any inline `#[test]`.
- **No compile-time sqlx SQL changes.** Both fixes touch runtime
  `AssertSqlSafe` SQL (`inline_delta_batch`) and arrow schema construction —
  no `query!`/`query_scalar!` is modified, so **no `.sqlx` regeneration** is
  needed (and none must sneak in).
- **Run prek before every commit:** `buck2 run //tools:prek -- run --all-files`
  (rustfmt is a separate hook; clippy-clean ≠ lint-clean). Stage new files
  first — prek skips untracked files. Markdown ends with exactly one trailing
  newline, no trailing whitespace.
- **Clippy is strict (pedantic + restriction):** no `unwrap`/`expect`/
  `indexing_slicing`/`panic`/`todo`/`map_err_ignore` in production lib/bin
  code; `#[expect(lint, reason = "...")]` for justified local exceptions. Test
  code is exempted from the panic-safety lints via
  `loom_rust_test`/`loom_fixture_test`.
- **Build/test commands:** build `buck2 build -v0 --console none //src/...`;
  test `buck2 test --console none <targets>`. Full suite locally needs `-j 8`
  (PG boot-slot starvation); cloud sessions add `-M none` to builds and scope
  tests.
- **Framing stays hidden from logical reads:** the merged view still projects
  exactly the mirror data columns; `loom_*`/`_loom_*` helpers never leak.

## Non-goals (deferred / out of scope)

- **Durable cold-entry removal** (rebuilding the Puffin index at compaction so
  tombstoned/superseded vectors leave the cold tier) — stays with
  `fut-cow-inline-shadow` slice-2 compaction; this fix keeps query-time
  suppression.
- **CDC hot-scoring semantics** — `inline_delta_batch` still scores every live
  `+I`/`+U` image in the window (CDC changelog rows are never end-capped); a
  per-identity latest-image fold for CDC vector tables is a separate concern,
  not regressed here.
- **`loom_change_kind` framing-field audit** — the inline provider declares it
  non-nullable (`serving.rs:595`), which is truthful (`NOT NULL DEFAULT '+I'`
  in `inline_ddl`); no change.
- **Nullable-`vector(N)` scoring policy** — rows with a NULL vector are simply
  unscoreable and excluded; no attempt to surface them differently.
- **Mirror-exact nullability for merged views with live inline rows** — the
  widened (nullable) schema is the honest contract; restoring the mirror flag
  would require an unsafe assertion DataFusion does not offer.

## Interfaces (names the plan consumes)

- **Consumes (unchanged, load-bearing):**
  - `inline_delta_batch` (`src/control-plane/postgres/src/vector_index.rs:287`;
    SQL `:357-365`; output schema + `try_new` `:378-385`).
  - `mvcc_live_pred` (`src/control-plane/postgres/src/iceberg_inline.rs:56`),
    `inline_ddl` framing columns (`iceberg_inline.rs:277-297`),
    `write_inline_delta` (`:1036`, tombstone insert `:1168-1171`),
    `column_array` (`:1310`, NULL-vector decode `:1375-1407`).
  - `build_serving_provider` / `build_inline_provider` / `build_merge_view`
    (`src/services/engine-serving/src/serving.rs:74` / `:541` (merge-mode
    schema `:591-605`) / `:325` (doc `:269-293`, final projection `:464-478`)).
  - `PgTableProvider::{schema, scan, fetch_batch}`
    (`src/services/engine-serving/src/provider.rs:119-207`, validation at
    `:146`).
  - `vector_search` hot path (`src/services/engine-serving/src/vector_search.rs:124-135`),
    `score_inline_batch` (`:146`).
  - The survivor post-filter (`src/services/query-api/src/handler.rs:695-760`,
    identity-only projection `:733`).
  - Arrow validation: arrow-array 58.3.0 `record_batch.rs:350`. Union
    nullability: datafusion-expr 54.0.0 `logical_plan/plan.rs:3082`/`:3130`.
  - Test plumbing: `e2e_support::{seed_vector_type, post_search, get, ids_i64,
    grant_read, grant_read_filtered, subject_with_role}`
    (`src/services/query-api/tests/e2e_support.rs`); the
    `vector_index_inline_delta.rs` `seed`/`ipc_long` harness; BUCK siblings
    `vector-search-cold-suppression-e2e` (`src/services/query-api/BUCK:297`)
    and `vector-index-inline-delta`
    (`src/control-plane/postgres/BUCK:1281`).
- **Produces (later plan tasks rely on these EXACT shapes):**
  - `inline_delta_batch`'s WHERE gains
    `and not loom_tombstone and (loom_change_kind is null or loom_change_kind
    <> '-U') and {vec_quoted} is not null`; signature, output schema, and
    callers unchanged.
  - `build_inline_provider`'s merge-mode branch builds non-identity data
    `Field`s with `.with_nullable(true)`; identity + framing fields unchanged;
    identity-less branch unchanged.
  - Revised `build_merge_view` doc comment (schema-equality claim scoped to
    names/types/order).
  - New test: `src/services/query-api/tests/vector_objects_read_tombstone_e2e.rs`
    + BUCK target `vector-objects-read-tombstone-e2e`.
  - Extended test: `tombstoned_and_before_image_rows_are_excluded` case in
    `src/control-plane/postgres/tests/vector_index_inline_delta.rs`.
  - Both `#[ignore]` attributes removed from
    `vector_search_cold_suppression_e2e.rs`.
