# `/search` Vector Nullability 500 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix `iss-search-vector-merge-view-nullable`: `/search` over an identity-bearing `vector(N)` type 500s once a DELETE tombstone inline row exists, and full-column reads over the merged view 500 the same way. Un-ignore the two blocked e2e cases in `vector_search_cold_suppression_e2e.rs`.

**Architecture** (spec: `docs/superpowers/specs/2026-07-09-search-vector-merge-view-nullable-design.md`). Both failures are deterministic arrow `RecordBatch::try_new` nullability violations — loom declares a field non-nullable over an array that carries a NULL. Two independent fixes:

- **Fix A** — `inline_delta_batch`'s hot-delta SQL (`vector_index.rs`) EXCLUDES tombstone / `-U` / NULL-vector rows. A tombstoned identity is unscoreable and must not be indexed or hot-scored at all; its stale cold hit is suppressed by query-api's survivor post-filter. The output schema stays byte-identical (its non-nullable vector field becomes truthful).
- **Fix B** — a **two-part, blast-radius-zero** change in `engine-serving/src/serving.rs`:
  - **B1 (widen the inline tier's INTERNAL schema):** `build_inline_provider`'s merge-mode branch declares non-identity data columns **nullable**. This is the only place NULLs physically exist (a non-CDC tombstone inline row is id-only), and it is what `PgTableProvider::fetch_batch` validates against.
  - **B2 (restore the SERVED schema):** `build_merge_view`'s **final projection** re-declares every mirror-required data column **non-nullable**, via a tiny identity scalar UDF (`loom_not_null`). This is legal because the projection sits **above** the `_loom_tomb = false` filter, which drops exactly the NULL-carrying rows. Net effect: the merged view's served Arrow schema is **byte-identical to the mirror** — names, types, order **and nullability** — so nothing downstream of serving observes any change.

  **Why B2 is mandatory (the blast radius):** the worker infers a transform/MV's output columns FROM the served Arrow schema (`worker/src/transform.rs:302-303`, `worker/src/stream_mv.rs:165-167` → `datafusion_io::infer_columns`, `infer.rs:66-79`, which sets `nullable: f.is_nullable()`). Widening the served schema would flow into `check_conformance` (`control-plane/core/src/conform.rs:56` — `if p.required && col.nullable` ⇒ `NullabilityViolation` ⇒ a typed transform over a source with ANY live inline row **aborts**) and into `classify_schema_change` (`iceberg_schema_evolution.rs:64` — `if l.nullable != n.nullable` ⇒ `ColumnNullabilityChanged` ⇒ an MV/transform re-run onto an existing output table **fails**; `mv_enrich.rs` reads the dimension side through `build_serving_provider`, so MVs are in scope). B2 keeps that schema constant, so none of it can fire.

**Tech Stack:** Rust, arrow 58 / DataFusion 54, sqlx runtime `AssertSqlSafe` (no compile-time SQL touched), buck2 `loom_fixture_test`.

## Verified facts this plan rests on (re-verify if the tree moves)

1. **The ONLY inline rows with NULL data columns are non-CDC tombstones.** `write_inline_delta` (`iceberg_inline.rs:1217`) has three emit arms:
   - CDC (`if cdc`): `-D` carries the **full prior image**, `-U`/`+U` carry full before/after images (`write_cdc_row(... before_cols, before_batch)` / `(... columns, batch)`).
   - non-CDC tombstone (`else if tombstone`, ~`:1345-1360`): `insert into inline_<tid> (begin_snapshot, loom_tombstone, loom_change_kind, "<id>") values ($1, true, '-D', $2)` — **"data NULL"** (the code's own comment).
   - non-CDC version (`else`, ~`:1364-1392`): inserts **every** column in `columns`. Its sole production caller (`action.rs:1196-1212`) passes the **full post-PATCH row** (`&columns`, `row`), so a live UPDATE shadow is never partial.
2. **Those exact rows are what the merge fold drops.** `build_merge_view` (`serving.rs:351`) filters `_loom_tomb = false` (`:495`) *before* the final projection (`:497-503`). For `Precedence::Snapshot`, `_loom_tomb = loom_tombstone` (`:423`) — i.e. exactly the NULL-carrying rows. ⇒ **No NULL in a mirror-required column can reach the final projection.**
3. **DataFusion 54 union widens, projections do not narrow.** `Union::derive_schema_from_inputs_by_position` computes `let nullable = fields.iter().any(|field| field.is_nullable());` (datafusion-expr 54.0.0 `src/logical_plan/plan.rs:3130`). `Expr::Column`'s field is copied from the input DFSchema, so a plain projection cannot restore non-nullability — hence B2's UDF.
4. **A scalar UDF's declared nullability IS honored, logically and physically.** `ScalarUDFImpl::return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef>` (datafusion-expr 54.0.0 `src/udf.rs:677`; the default returns `Field::new(self.name(), return_type, true)`), and physically `ScalarFunctionExpr::nullable(&self, _input_schema) -> Result<bool>` returns `Ok(self.return_field.is_nullable())` (datafusion-physical-expr 54.0.0 `src/scalar_function.rs:232-234`). So a UDF that reports a **non-nullable** return field makes both the logical `Projection` schema and the physical `ProjectionExec` schema non-nullable.
5. **Anchors (current):** `build_serving_provider` `serving.rs:84`; `build_merge_view` `:351` (final projection `:497-503`); `build_inline_provider` `:567` (merge-mode field block `:617-628`); the merge-view doc block `:295-319` — note it is currently **mis-attached**, sitting immediately above `offset_precedence` (`:322`) rather than above `build_merge_view` (`:351`); `PgTableProvider::fetch_batch` validation `provider.rs:146`; `inline_delta_batch` `vector_index.rs:287` (SQL `:354-365`, output schema + `try_new` `:377-386`); `column_array` `iceberg_inline.rs:1513`. BUCK: `src/control-plane/postgres/BUCK:1451` (`vector-index-inline-delta`), `src/services/query-api/BUCK:356` (`vector-search-cold-suppression-e2e`), `src/services/engine-serving/BUCK:75` (`merge-on-read`), `src/services/worker/BUCK:353` (`typed-transform-e2e`). `engine-serving`'s lib srcs are `glob(["src/**/*.rs"])` (`BUCK:7`), so a new `src/*.rs` module needs **no** BUCK edit — only a `mod` line in `src/lib.rs`.

## Global Constraints

Carried from the spec; every task implicitly includes these:

- **Tests are `rust_test` / `loom_fixture_test` integration targets only — never inline `#[cfg(test)]`.** New fixture tests MUST use `loom_fixture_test` (`src/control-plane/postgres/defs.bzl`); the `no-inline-tests` prek hook fails on any inline `#[test]` under `src/`.
- **No `.sqlx` regeneration:** both fixes touch runtime `AssertSqlSafe` SQL and arrow schema construction only. If a task finds itself editing a `query!`/`query_scalar!`, stop — that is outside this plan's scope.
- **Run prek before every commit:** `buck2 run //tools:prek -- run --all-files`. `git add` new files FIRST (prek skips untracked files). Markdown: exactly one trailing newline, no trailing whitespace.
- **Clippy is strict (pedantic + restriction):** no `unwrap`/`expect`/`indexing_slicing`/`panic`/`todo`/`map_err_ignore` in production lib/bin code; `#[expect(lint, reason = "...")]` for justified exceptions. Test code is exempt from the panic-safety lints via `loom_rust_test` / `loom_fixture_test`.
- **The served schema is a frozen contract.** After Fix B, `build_serving_provider(...).schema()` for an identity-bearing table with live inline rows MUST equal `arrow_schema_from_mirror(&cols)` **including nullability**. Task 3 pins this; do not relax it.
- **Build/test commands:** build `buck2 build -v0 --console none //src/...`; test `buck2 test --console none <targets>`. Full local suite: `buck2 test --console none -j 8 //src/...` (PG boot-slot starvation at higher `-j`). Cloud sessions: `-M none` on builds, scope tests to touched targets.

---

## File Structure

**Create:**
- `src/services/engine-serving/src/not_null.rs` — the `loom_not_null` identity scalar UDF (Fix B2).
- `src/services/engine-serving/tests/merge_view_schema.rs` — the served-schema (blast-radius) pin.
- `src/services/query-api/tests/vector_objects_read_tombstone_e2e.rs` — Fix B e2e (full-column merged read with a live tombstone).

**Modify (production):**
- `src/control-plane/postgres/src/vector_index.rs` — hot-delta SQL exclusion predicate (`:354-365`) + doc comment (above `:287`).
- `src/services/engine-serving/src/serving.rs` — `build_inline_provider` merge-mode nullability widening (`:617-628`); `build_merge_view` final projection nullability restore (`:497-503`); the mis-attached doc block (`:295-319`).
- `src/services/engine-serving/src/lib.rs` — `mod not_null;`.

**Modify (tests/BUCK):**
- `src/control-plane/postgres/tests/vector_index_inline_delta.rs` — tombstone-exclusion case (target already wired, `postgres/BUCK:1451`).
- `src/services/engine-serving/BUCK` — new `merge-view-schema` `loom_fixture_test` target.
- `src/services/query-api/BUCK` — new `vector-objects-read-tombstone-e2e` target (mirrors `BUCK:356`).
- `src/services/worker/tests/typed_transform_e2e.rs` — blast-radius non-regression case (target `typed-transform-e2e` already wired, `worker/BUCK:353`; it already deps `//src/control-plane/postgres:postgres`, so `iceberg_inline` is importable with no BUCK edit).
- `src/services/query-api/tests/vector_search_cold_suppression_e2e.rs` — delete both `#[ignore]` attributes (Task 5 ONLY).

---

## Task 1: Fix A — exclude tombstone / `-U` / vector-less rows from the hot delta

**Files:**
- Modify: `src/control-plane/postgres/tests/vector_index_inline_delta.rs` (failing test first)
- Modify: `src/control-plane/postgres/src/vector_index.rs`

**Interfaces:**
- Consumes: the file's existing `seed` / `ipc_long` / `columns` / `lineage_evt` harness; `iceberg_inline::{current_inline_version, write_inline_delta}`.
- Produces: `inline_delta_batch`'s WHERE additionally requires `not loom_tombstone`, `loom_change_kind <> '-U'` (null-tolerant), and a non-NULL vector. **Signature, output schema, and the single caller (`engine-serving/src/vector_search.rs:129`) unchanged.**

- [ ] **Step 1: Write the failing test**

Append to `src/control-plane/postgres/tests/vector_index_inline_delta.rs` (add `use control_plane_postgres::iceberg_inline;` to the existing import block).

> **`write_inline_delta` takes ELEVEN arguments** — `(pool, table, columns, id_column, tombstone, batch, before, lineage, expected_version, consolidate_threshold, jobs: &[NewJob])`. The trailing `&[]` is **not optional**; copy the real call at `src/services/query-api/tests/vector_search_cold_suppression_e2e.rs:173-184` if in doubt.

```rust
/// A one-column id-only batch — the tombstone's carried batch (also the CAS
/// lookup key), mirroring vector_search_cold_suppression_e2e.rs::id_batch.
fn id_batch(id: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![id]))]).expect("id_batch")
}

/// RED pre-fix: a live tombstone inline row in the delta window carries a NULL
/// vector, and inline_delta_batch declares its vector field non-nullable
/// (vector_index.rs:377-386), so arrow's RecordBatch::try_new validation fails
/// ("Column 'embedding' is declared as non-nullable but contains null values")
/// -> every /search over the type 500s (iss-search-vector-merge-view-nullable,
/// Defect A). Desired: tombstoned, -U, and vector-less rows are EXCLUDED from
/// the hot delta — they are unscoreable; suppressing their stale cold hits is
/// the survivor post-filter's job (2026-07-07-search-cold-suppression-design).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tombstoned_rows_are_excluded_from_hot_delta() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let table = TableRef {
        schema: "wh".into(),
        name: "tdocs".into(),
    };
    let cold: &[(i64, [f32; 4])] = &[(1, [1.0, 0.0, 0.0, 0.0]), (2, [0.0, 1.0, 0.0, 0.0])];
    let hot: &[(i64, [f32; 4])] = &[(5, [0.9, 0.1, 0.0, 0.0])];
    let (pool, s_cold, _s_hot) = seed(
        fx,
        &db,
        &table,
        "TDocs",
        "long",
        "Long",
        (ipc_long(cold), ipc_long(hot)),
    )
    .await;

    // Tombstone id=2: an id-only inline delta row — every other data column
    // (the vector) is NULL in inline_<tid>.
    let cols = columns("long");
    let v0 = iceberg_inline::current_inline_version(&pool, &table, &cols, "id", &id_batch(2))
        .await
        .expect("current version");
    let s_del = iceberg_inline::write_inline_delta(
        &pool,
        &table,
        &cols,
        "id",
        true, // tombstone
        &id_batch(2),
        None,
        lineage_evt(&table),
        v0,
        None,
        &[], // jobs — the 11th param
    )
    .await
    .expect("tombstone delta");

    // The window (s_cold, s_del] holds the id=5 row-version AND the id=2
    // tombstone. Pre-fix this call is Err (arrow nullability validation).
    let batch = inline_delta_batch(&pool, &table, s_cold, s_del.0)
        .await
        .expect("hot delta must not fail on a tombstone in the window")
        .expect("Some: the id=5 row-version is still in the window");
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 ids");
    let got: Vec<i64> = (0..batch.num_rows()).map(|i| ids.value(i)).collect();
    assert_eq!(
        got,
        vec![5],
        "only the scoreable row-version; tombstoned id=2 excluded"
    );
    assert_eq!(
        batch.column(1).null_count(),
        0,
        "the hot delta never carries a NULL vector"
    );
}
```

If `seed`/`columns`/`lineage_evt` have different arities than shown, mirror the file's existing `int_identity_delta_batch_shape` case verbatim — the point of the test is the tombstone write + `inline_delta_batch` assertions, not the seed shape.

- [ ] **Step 2: Run the test to verify it fails**

```
buck2 test --console none //src/control-plane/postgres:vector-index-inline-delta
```

Expected: FAIL — `tombstoned_rows_are_excluded_from_hot_delta` panics at "hot delta must not fail on a tombstone in the window" with a Backend-wrapped `Invalid argument error: Column 'embedding' is declared as non-nullable but contains null values`. The pre-existing cases stay green.

- [ ] **Step 3: Implement the exclusion predicate**

In `src/control-plane/postgres/src/vector_index.rs`, replace the delta SQL block (`:354-365`):

```rust
    // Runtime query: select only the identity + vector columns with the delta
    // MVCC predicate. Tombstones (`loom_tombstone` — set by BOTH a non-CDC
    // delete and a CDC `-D`), CDC `-U` before-images (audit rows, mirroring the
    // inline provider's base predicate, serving.rs:596-600), and rows without a
    // vector are EXCLUDED: none is scoreable, and a tombstone's NULL vector
    // would fail the non-nullable output schema's RecordBatch validation (the
    // /search 500 of iss-search-vector-merge-view-nullable). A tombstoned
    // identity's stale COLD hit is suppressed by query-api's survivor
    // post-filter, not here.
    let id_quoted = quote_ident(&identity_col);
    let vec_quoted = quote_ident(&vector_col);
    let rows = sqlx::query(AssertSqlSafe(format!(
        "select {id_quoted}, {vec_quoted} \
         from {} \
         where begin_snapshot > {born_after} \
           and {} \
           and not loom_tombstone \
           and (loom_change_kind is null or loom_change_kind <> '-U') \
           and {vec_quoted} is not null \
         order by loom_row_id",
        inline_table_name(tid),
        mvcc_live_pred(at),
    )))
```

Every inline table carries `loom_tombstone` / `loom_change_kind` `NOT NULL` with defaults (`inline_ddl`, `iceberg_inline.rs:277-297`, plus the `add column if not exists` backfills), so the predicate is valid against every existing relation. The output schema at `:377-386` stays **byte-identical** — the `is not null` clause is exactly what makes its non-nullable vector field truthful.

Also update `inline_delta_batch`'s doc comment (above `:287`) to say the returned rows are the **scoreable** delta: live row-versions born in the window, excluding tombstones, `-U` before-images, and vector-less rows.

- [ ] **Step 4: Run the seam test + the hot-path pinning suite**

```
buck2 test --console none //src/control-plane/postgres:vector-index-inline-delta //src/services/query-api:vector-search-e2e
```

Expected: PASS — the new case green; `int_identity_delta_batch_shape` (the byte-identical output-schema contract) and the 11 `vector_search_e2e` cases untouched.

- [ ] **Step 5: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "fix(vector): exclude tombstone/-U/vector-less rows from the hot delta

inline_delta_batch selected every MVCC-live inline row in the window, so a
DELETE's id-only tombstone row put a NULL vector under the batch's
non-nullable vector field — arrow's RecordBatch::try_new validation failed and
every /search over the type 500'd (iss-search-vector-merge-view-nullable,
Defect A; deterministic, not the register's suspected first-query
nondeterminism). Unscoreable rows are now excluded in SQL; the survivor
post-filter keeps suppressing their stale cold hits."
```

---

## Task 2: Fix B2 — the `loom_not_null` UDF (nullability restore primitive)

Built **before** B1 so the widening in Task 3 can be paired with the restore in the same commit — the served schema never widens, not even transiently on a bisect.

**Files:**
- Create: `src/services/engine-serving/src/not_null.rs`
- Modify: `src/services/engine-serving/src/lib.rs` (add `mod not_null;`)
- Create: `src/services/engine-serving/tests/not_null_udf.rs` + a `rust_test` target in `src/services/engine-serving/BUCK`

**Interfaces:**
- Produces: `pub(crate) fn not_null(expr: Expr) -> Expr` — wraps `expr` in an identity `ScalarUDF` whose **return field is declared non-nullable**, so `Projection`/`ProjectionExec` above a NULL-free filter can restore the mirror's declared nullability. Runtime behavior is a pass-through; if a NULL ever *does* reach it, `RecordBatch::try_new` inside `ProjectionExec` fails loudly (the same class of error as today's bug — never silent corruption).

- [ ] **Step 1: Write the failing unit test**

Create `src/services/engine-serving/tests/not_null_udf.rs` — a pure-logic `rust_test` (no fixture). It must prove BOTH halves: the declared field is non-nullable, and the values pass through unchanged.

```rust
//! `loom_not_null`: the identity scalar UDF that re-declares its argument's
//! field as NON-nullable. Used by `build_merge_view`'s final projection to
//! restore the mirror's declared nullability above the `_loom_tomb = false`
//! filter, so the merged view's served schema stays byte-identical to the
//! mirror (iss-search-vector-merge-view-nullable — the transform/MV blast
//! radius the widening would otherwise cause).

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;

/// A projection through `loom_not_null` over a NULLABLE input column yields a
/// NON-nullable output field, with values untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declares_non_nullable_and_passes_values_through() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))])
        .expect("batch");
    let ctx = SessionContext::new();
    let df = ctx.read_batch(batch).expect("read_batch");

    // The public seam under test. `engine_serving::not_null` is `pub(crate)`;
    // export a `#[doc(hidden)] pub` test seam (or make the module `pub`) so this
    // integration test can reach it — loom has no inline unit tests.
    let out = df
        .select(vec![engine_serving::not_null::not_null(
            datafusion::prelude::col("v"),
        )
        .alias("v")])
        .expect("select");

    assert!(
        !out.schema().field_with_unqualified_name("v").expect("v").is_nullable(),
        "loom_not_null re-declares the field NON-nullable at the logical level"
    );
    let batches = out.collect().await.expect("collect");
    let got: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            let a = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64");
            (0..b.num_rows()).map(|i| a.value(i)).collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(got, vec![1, 2, 3], "values pass through unchanged");
    assert!(
        !batches
            .first()
            .expect("one batch")
            .schema()
            .field(0)
            .is_nullable(),
        "and NON-nullable at the physical/batch level (ProjectionExec)"
    );
}
```

Wire the target in `src/services/engine-serving/BUCK` (mirror `pg-scan-sql`, `BUCK:31-42`, plus `//third-party:tokio`):

```python
rust_test(
    name = "not-null-udf",
    crate = "not_null_udf",
    srcs = ["tests/not_null_udf.rs"],
    crate_root = "tests/not_null_udf.rs",
    edition = "2024",
    deps = [
        ":engine-serving",
        "//third-party:arrow",
        "//third-party:datafusion",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 2: Run it — RED (does not compile: `not_null` does not exist)**

```
buck2 test --console none //src/services/engine-serving:not-null-udf
```

- [ ] **Step 3: Implement the UDF**

Create `src/services/engine-serving/src/not_null.rs`. The exact DataFusion 54 API (verified against the pinned crates):
- `ScalarUDFImpl::return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef>` — `ReturnFieldArgs { arg_fields: &[FieldRef], scalar_arguments: &[Option<&ScalarValue>] }` (datafusion-expr 54.0.0 `src/udf.rs:444-454`, `:677`).
- `ScalarUDFImpl::invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue>` — `ScalarFunctionArgs { args: Vec<ColumnarValue>, arg_fields, number_rows, return_field, config_options }` (`src/udf.rs:413-426`).
- Physical honoring: `ScalarFunctionExpr::nullable()` returns `self.return_field.is_nullable()` (datafusion-physical-expr 54.0.0 `src/scalar_function.rs:232-234`).

```rust
//! `loom_not_null` — an IDENTITY scalar UDF that re-declares its argument's field
//! as NON-nullable.
//!
//! Why this exists: `build_merge_view` unions the file tier with the inline tier,
//! and the inline tier MUST declare its non-identity data columns nullable (a
//! non-CDC DELETE's inline row is id-only, so those columns are physically NULL —
//! `iceberg_inline::write_inline_delta`'s tombstone arm). DataFusion's union
//! widens nullability per position (`nullable = any input nullable`,
//! datafusion-expr 54 `logical_plan/plan.rs:3130`) and a plain column projection
//! copies the input field verbatim — so without this the MERGED view's served
//! schema would inherit the widening.
//!
//! That widening is NOT cosmetic: the worker infers a transform/MV's output
//! columns from the served Arrow schema (`worker::transform` / `worker::stream_mv`
//! -> `datafusion_io::infer_columns`, which sets `nullable: f.is_nullable()`), and
//! `check_conformance` rejects a nullable column for a REQUIRED property while
//! `classify_schema_change` rejects any nullability change on a re-run. A benign
//! inline UPDATE shadow on a source table would start failing green transforms.
//!
//! The restore is SOUND because the final projection sits ABOVE the
//! `_loom_tomb = false` filter, which drops exactly the tombstone rows — the only
//! inline rows that carry NULL data columns (CDC `-D`/`-U`/`+U` all carry full
//! images). If a NULL ever did reach here, `ProjectionExec`'s
//! `RecordBatch::try_new` fails loudly rather than corrupting silently.

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

/// The UDF's SQL-visible name. Namespaced so it can never collide with a user
/// function; it is only ever inserted programmatically (never parsed from SQL).
const NAME: &str = "loom_not_null";

#[derive(Debug)]
struct NotNull {
    signature: Signature,
}

impl Default for NotNull {
    fn default() -> Self {
        Self {
            // Any single argument, any type; pure pass-through.
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for NotNull {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        NAME
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types {
            [t] => Ok(t.clone()),
            _ => exec_err!("{NAME} takes exactly one argument"),
        }
    }
    /// THE POINT: identical data type, nullability forced to `false`.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        match args.arg_fields {
            [f] => Ok(Arc::new(Field::new(NAME, f.data_type().clone(), false))),
            _ => exec_err!("{NAME} takes exactly one argument"),
        }
    }
    /// Pure identity — the argument is returned verbatim.
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let mut it = args.args.into_iter();
        match (it.next(), it.next()) {
            (Some(v), None) => Ok(v),
            _ => exec_err!("{NAME} takes exactly one argument"),
        }
    }
}

/// Wrap `expr` so its projected field is declared NON-nullable. Caller must
/// guarantee the expression cannot yield NULL at this point in the plan (see the
/// module doc); the alias is the caller's job.
pub fn not_null(expr: Expr) -> Expr {
    ScalarUDF::from(NotNull::default()).call(vec![expr])
}
```

Add `pub mod not_null;` to `src/services/engine-serving/src/lib.rs` (alongside `pub mod provider;`). Keeping it `pub` (not `pub(crate)`) is what lets the `not-null-udf` integration test reach it — loom has no inline unit tests, so every unit is exercised from a sibling `tests/` crate.

Clippy notes: no `unwrap`/`expect`/`panic`/indexing — every arity branch is an `exec_err!`. `Default` is implemented explicitly (not derived) because `Signature` has no `Default`.

- [ ] **Step 4: Run it — GREEN**

```
buck2 test --console none //src/services/engine-serving:not-null-udf
```

Expected: `Pass 1. Fail 0`. If the logical assertion passes but the **physical** one fails, DataFusion is deriving the ProjectionExec field from the physical expr's input schema instead of `return_field` — STOP and report; the whole of Fix B2 rests on `scalar_function.rs:232-234`.

- [ ] **Step 5: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(serving): loom_not_null — an identity UDF that re-declares a field non-nullable

DataFusion's union widens nullability per position and a plain column projection
copies the input field verbatim, so there is no way to restore a mirror's
declared non-nullability above a filter that provably removes the NULLs. A scalar
UDF can: its return_field is honored both logically (Expr::to_field) and
physically (ScalarFunctionExpr::nullable). Groundwork for the merge-view fix."
```

---

## Task 3: Fix B1 + B2 applied — inline tier widens internally, merged view serves the mirror schema

**Files:**
- Create: `src/services/engine-serving/tests/merge_view_schema.rs` (failing test first) + BUCK target
- Modify: `src/services/engine-serving/src/serving.rs`

**Interfaces:**
- Consumes: `not_null` (Task 2); `Field::with_nullable` (arrow 58); `iceberg_inline::{current_inline_version, write_inline_delta}`; `IcebergCatalog`; `arrow_schema_from_mirror`.
- Produces:
  - `build_inline_provider` (`:617-628`): in merge mode, every **non-identity** data field is `.with_nullable(true)`. Identity keeps its mirror nullability (a tombstone always carries the identity — `extract_id_cell`); the four framing fields are unchanged; the identity-less branch (`schema.clone()`) is byte-identical.
  - `build_merge_view` (`:497-503`): the final projection wraps every data column whose **mirror** field is non-nullable in `not_null(...)`, aliased back to the column name. ⇒ `build_serving_provider(...).schema()` **equals** `arrow_schema_from_mirror(&cols)` exactly, in every tier combination.

- [ ] **Step 1: Write the failing schema pin (this is the blast-radius guard)**

Create `src/services/engine-serving/tests/merge_view_schema.rs`:

```rust
//! The merged view's SERVED schema is byte-identical to the mirror's — names,
//! types, order AND nullability — even with a live inline tombstone
//! (iss-search-vector-merge-view-nullable).
//!
//! This is not cosmetic. The worker infers a transform/MV's output columns from
//! the served Arrow schema (`worker::transform` / `worker::stream_mv` ->
//! `datafusion_io::infer_columns`, which copies `f.is_nullable()`); a widened
//! flag makes `check_conformance` reject a REQUIRED property
//! (`core::conform` -> NullabilityViolation) and `classify_schema_change` reject
//! an MV re-run (ColumnNullabilityChanged). The inline TIER must declare its
//! non-identity columns nullable (a tombstone row is id-only, so they really are
//! NULL there) — but the merged OUTPUT, which sits above the `_loom_tomb = false`
//! filter, must not. loom_fixture_test (Postgres).

// imports: PgFixture, IcebergCatalog, iceberg_inline, engine_serving::{
//   build_serving_provider, serving::arrow_schema_from_mirror (make it reachable)
// }, datafusion SessionContext, control_plane_core::{Catalog, ...}
// Seed with //src/testing:seed (`land` + a vector/identity type) exactly as
// `engine-serving/tests/merge-on-read` (BUCK:75) does — mirror that file's
// fixture helpers rather than inventing new ones.

/// The pin: with an identity type carrying a REQUIRED non-identity column, a
/// live inline UPDATE shadow AND a live inline tombstone, the provider's schema
/// still equals the mirror's — field for field, nullability included.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn merged_view_schema_equals_mirror_with_live_tombstone() {
    // 1. seed: identity table with a REQUIRED non-identity column + cold rows.
    // 2. write_inline_delta(..., tombstone = false, full_row_batch, .., &[])  // UPDATE shadow
    // 3. write_inline_delta(..., tombstone = true,  id_only_batch,  .., &[])  // DELETE tombstone
    // 4. let mirror = arrow_schema_from_mirror(&catalog.schema(&table, snap).await?.columns)?;
    //    let provider = build_serving_provider(&ctx, &catalog, &table, None, None).await?.unwrap();
    // 5. assert_eq!(provider.schema().fields(), mirror.fields(),
    //        "merged view serves the mirror schema EXACTLY (nullability included)");
    //    (assert_eq on `&Fields` compares name + data_type + nullability + metadata.)
    // 6. And prove the read still works over that schema: register the provider,
    //    `SELECT * FROM t`, collect, assert the tombstoned id is absent and the
    //    UPDATE shadow's value wins.
}
```

Wire a `loom_fixture_test` target `merge-view-schema` in `src/services/engine-serving/BUCK` by copying the `merge-on-read` target (`BUCK:75-90`) verbatim and changing `name`/`crate`/`srcs`/`crate_root`.

> If `arrow_schema_from_mirror` is `pub(crate)` (`serving.rs:646`), promote it to `pub` and re-export it from `lib.rs` — the pin needs the mirror schema built the same way the provider builds it, and re-deriving it in the test would weaken the assertion.

- [ ] **Step 2: Run it — RED**

```
buck2 test --console none //src/services/engine-serving:merge-view-schema
```

Expected: FAIL. Pre-fix, step 6's `SELECT *` 500s at `PgTableProvider::fetch_batch` (`provider.rs:146`): `Column '<required col>' is declared as non-nullable but contains null values`. (The schema assertion in step 5 passes pre-fix — that's the point: the pin must go RED only on the *read*, and must stay GREEN through the fix. Assert BOTH so a naive widening cannot land.)

- [ ] **Step 3: Widen the inline tier's internal schema (B1)**

In `src/services/engine-serving/src/serving.rs`, replace the merge-mode field block (`:617-628`):

```rust
    let (provider_schema, logical_types) = if let Some(id) = identity {
        // Merge mode. Two adjustments over the raw mirror schema:
        //
        //   1. Non-identity data columns are declared NULLABLE. This tier's rows
        //      include a non-CDC DELETE's inline row, which is id-only — every
        //      other data column is physically NULL (`write_inline_delta`'s
        //      tombstone arm) — and the merge fold NEEDS that row (identity +
        //      loom_tombstone is what hides the file row). Declaring the mirror's
        //      non-nullable schema over it makes `PgTableProvider::fetch_batch`
        //      fail arrow's RecordBatch validation BEFORE the fold can drop the
        //      tombstone (iss-search-vector-merge-view-nullable, Defect B).
        //      The identity keeps its mirror nullability — a tombstone always
        //      carries it (`extract_id_cell`).
        //
        //      This widening is INTERNAL to the tier. `build_merge_view`'s final
        //      projection — which sits above the `_loom_tomb = false` filter that
        //      drops exactly these rows — restores the mirror's declared
        //      nullability via `not_null`, so the SERVED schema is unchanged. That
        //      matters: the worker infers transform/MV output columns from the
        //      served schema (`datafusion_io::infer_columns`), and a widened flag
        //      would break `check_conformance` / `classify_schema_change`.
        //
        //   2. BOTH precedence pairs are appended (physical names) so the dedup can
        //      rank rows and hide tombstoned identities under either `Precedence`.
        //      Unused columns for a given mode are never selected (DataFusion
        //      projects only what the fold references), so this is a no-op cost for
        //      the mode not in play.
        let mut fields: Vec<Field> = schema
            .fields()
            .iter()
            .map(|f| {
                let f = f.as_ref().clone();
                if f.name() == id {
                    f
                } else {
                    f.with_nullable(true)
                }
            })
            .collect();
        fields.push(Field::new("begin_snapshot", DataType::Int64, false));
        fields.push(Field::new("loom_tombstone", DataType::Boolean, false));
        fields.push(Field::new("loom_change_kind", DataType::Utf8, false));
        fields.push(Field::new("loom_offset", DataType::Int64, true));
        let mut lts = logical_types;
        lts.push(control_plane_core::BaseType::Long);
        lts.push(control_plane_core::BaseType::Boolean);
        lts.push(control_plane_core::BaseType::String);
        lts.push(control_plane_core::BaseType::Long);
        (Arc::new(Schema::new(fields)) as SchemaRef, lts)
    } else {
        (schema.clone(), logical_types)
    };
```

Note the `if identity.is_some()` becomes `if let Some(id) = identity` so the identity column can be compared by name. Extend `build_inline_provider`'s doc comment (`:550-566`) with the nullability rule.

- [ ] **Step 4: Restore the mirror's nullability on the merged view's final projection (B2)**

In `build_merge_view` (`serving.rs`), the final projection (`:497-503`) currently reads:

```rust
        .select(
            data_cols
                .iter()
                .map(|n| cref(n.as_str()))
                .collect::<Vec<_>>(),
        )
```

Replace it with a nullability-restoring projection. `schema` is the mirror schema, already in scope:

```rust
    // Final projection back to EXACTLY the mirror data schema. `_loom_tomb = false`
    // (above) has already dropped every tombstone row — the ONLY rows whose
    // non-identity data columns are physically NULL (a non-CDC DELETE's inline row
    // is id-only; CDC's -D/-U/+U all carry full images). But the inline tier had to
    // DECLARE those columns nullable to scan them, and DataFusion's union widens
    // nullability per position (`nullable = any input nullable`), which a plain
    // column projection copies verbatim.
    //
    // So: re-declare every column the MIRROR says is required as non-nullable,
    // through `not_null` (an identity UDF whose return_field is non-nullable —
    // honored by both `Expr::to_field` and `ScalarFunctionExpr::nullable`). The
    // merged view's served schema is then byte-identical to the mirror's, which is
    // load-bearing: the worker infers transform/MV output columns from it
    // (`datafusion_io::infer_columns` copies `is_nullable()`), and a widened flag
    // would make `check_conformance` reject a REQUIRED property and
    // `classify_schema_change` reject an MV re-run.
    //
    // If a NULL ever did survive the filter, `ProjectionExec`'s RecordBatch::try_new
    // fails loudly — never a silent wrong answer.
    let final_projection: Vec<Expr> = schema
        .fields()
        .iter()
        .map(|f| {
            let c = cref(f.name().as_str());
            if f.is_nullable() {
                c
            } else {
                crate::not_null::not_null(c).alias(f.name())
            }
        })
        .collect();
    let merged = unioned
        .window(vec![ranked])
        .map_err(to_serving)?
        .filter(cref("_loom_rn").eq(lit(1_u64)))
        .map_err(to_serving)?
        .filter(cref("_loom_tomb").eq(lit(false)))
        .map_err(to_serving)?
        .select(final_projection)
        .map_err(to_serving)?;
    Ok(merged.into_view())
```

`data_cols` stays as the tier-projection helper; only the FINAL select changes. (`schema.fields()` and `data_cols` are the same names in the same order — `data_cols` is derived from `schema.fields()` at `:372`.)

- [ ] **Step 5: Fix the mis-attached / stale doc block**

`serving.rs:295-319` is a doc block that describes `build_merge_view` but currently sits immediately above `offset_precedence` (`:322`) — it was orphaned by an earlier edit. **Move it to sit above `fn build_merge_view` (`:351`)**, and replace its last paragraph (`:314-319`, the "schema equals `schema` exactly" claim) with:

```rust
/// A `ROW_NUMBER()` window (not `DISTINCT ON`) is used deliberately: the window is a
/// pass-through over the data columns, so they keep their mirror `DataType` end to
/// end (union coerces identical schemas to themselves; window / filter / final
/// projection are pass-through). Thus the final projection needs no casts.
///
/// Nullability takes one extra step. The inline tier must DECLARE its non-identity
/// data columns nullable (a non-CDC DELETE's inline row is id-only, so they really
/// are NULL there — see `build_inline_provider`), and DataFusion's union widens
/// nullability per position. The final projection therefore re-declares every
/// mirror-REQUIRED column non-nullable via `not_null` — legal because it sits above
/// the `_loom_tomb = false` filter, which drops exactly the NULL-carrying rows. Net:
/// the view's schema equals `schema` EXACTLY — names, types, order and nullability —
/// which the governed layer, the Flight wire, and (critically) the worker's
/// `infer_columns` -> `check_conformance` / `classify_schema_change` path require.
/// (`DISTINCT ON` would additionally widen the identity column to nullable.)
```

Also leave `offset_precedence`'s own one-line doc (`/// Build the CDC Precedence::Offset ...`) where it is.

- [ ] **Step 6: Run the pin + every merge-view suite**

```
buck2 test --console none //src/services/engine-serving:merge-view-schema //src/services/engine-serving:merge-on-read //src/services/engine-serving:vector-merge //src/services/engine-serving:vector-search //src/services/engine-serving:governed-sql //src/services/engine-serving:cow-consolidate
```

then

```
buck2 test --console none //src/services/query-api:cow-inline-shadow-e2e //src/services/query-api:stream-merge-firstrow-e2e //src/services/query-api:stream-merge-versioned-e2e //src/services/query-api:stream-cdc-e2e
```

Expected: PASS across the board. If a target name does not resolve, list the real ones with `grep -n 'name = "' <BUCK>` and run the actual sibling — **do not skip a suite.**

- [ ] **Step 7: prek + commit (B1 and B2 land TOGETHER)**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "fix(serving): inline tier declares NULLs, merged view still serves the mirror schema

A non-CDC DELETE's inline row is id-only — every non-identity data column is
physically NULL — and the merge fold needs that row to hide the file row.
Declaring the mirror's non-nullable schema over it made
PgTableProvider::fetch_batch fail arrow's RecordBatch validation on any
full-column read of the merged view (500), before the _loom_tomb filter could
drop the tombstone (iss-search-vector-merge-view-nullable, Defect B).

The inline TIER now declares its non-identity columns nullable — and the merged
view's final projection, which sits above the _loom_tomb = false filter that
drops exactly those rows, restores the mirror's declared nullability via
loom_not_null. So the SERVED schema is byte-identical to the mirror, nullability
included. That is deliberate, not incidental: the worker infers transform/MV
output columns from the served schema (datafusion_io::infer_columns), and a
widened flag would make check_conformance reject a required property and
classify_schema_change reject an MV re-run — turning green transforms red the
moment a source acquired a benign inline UPDATE shadow."
```

---

## Task 4: Blast-radius non-regression — a typed transform over a source with a live inline row

The Task 3 pin asserts the invariant at the schema seam. This asserts it end-to-end at the seam that would actually have broken.

**Files:**
- Modify: `src/services/worker/tests/typed_transform_e2e.rs` (target `typed-transform-e2e`, `worker/BUCK:353` — it already deps `//src/control-plane/postgres:postgres`, so `iceberg_inline` needs **no** BUCK edit)
- Modify: `src/services/query-api/tests/vector_objects_read_tombstone_e2e.rs` — no; see Task 5.

- [ ] **Step 1: Add the case**

Add a fourth `#[tokio::test]` to `typed_transform_e2e.rs`, reusing the file's `tref` / `prop` / `customer_columns` / `customer_body` / `seed_lineage` / `make_typed_job` / `build_ctx` / `read_i64s` helpers verbatim:

```rust
/// Blast-radius non-regression (iss-search-vector-merge-view-nullable): a typed
/// transform whose SOURCE table has a LIVE INLINE ROW (a benign UPDATE shadow —
/// no tombstone) must still conform and commit. The worker infers the output
/// columns from the SERVED Arrow schema (`infer_columns`, which copies
/// `is_nullable()`), and `check_conformance` rejects a nullable column for a
/// REQUIRED property. Widening the merged view's served nullability — the naive
/// form of the Defect-B fix — would abort this transform. The real fix restores
/// the mirror's nullability in the merge view's final projection, so this stays
/// green.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_transform_conforms_over_a_source_with_a_live_inline_row() {
    // 1. Seed exactly as `typed_transform_commits_with_type_named_lineage` does,
    //    but ensure BOTH the input type's and the output type's non-identity
    //    properties are `prop(name, ty, /* required */ true)` and the input type
    //    declares an `identity` (that is what routes the source through
    //    build_merge_view at all — an identity-less type never merges).
    // 2. Write ONE inline UPDATE shadow on the source table:
    //        let v = iceberg_inline::current_inline_version(&pool, &src, &cols, "id", &id_batch(1)).await?;
    //        iceberg_inline::write_inline_delta(
    //            &pool, &src, &cols, "id", false, &full_row_batch(1, ..), None,
    //            seed_lineage(&src), v, None, &[],   // 11 args — the trailing &[] is required
    //        ).await?;
    // 3. Run the SAME typed transform job through `handle_typed_transform`.
    // 4. assert it SUCCEEDS (no `JobFailure::abandon("output does not conform ...")`)
    //    and the output table holds the shadow's value for id=1 (the merge winner),
    //    proving both the conformance gate AND the fold.
}
```

- [ ] **Step 2: Run it**

```
buck2 test --console none //src/services/worker:typed-transform-e2e
```

Expected: PASS (all 4 cases). **Sanity check the test actually bites:** temporarily revert Task 3 Step 4 (the `not_null` restore, keeping the B1 widening) and re-run — this case MUST fail with `output does not conform to the declared type (1 violation(s)): [NullabilityViolation { property: ... }]`. Restore Step 4 and confirm green again. Record both observations in the PR description; do not commit the temporary revert.

- [ ] **Step 3: MV non-regression sweep**

```
buck2 test --console none //src/services/worker:stream-mv-e2e //src/services/worker:stream-mv-join-e2e //src/services/worker:transform-e2e
```

(`stream_mv_join_e2e.rs` already writes an inline delta at `:537`, so it exercises the dimension-side `build_serving_provider` with a live inline row — it is the existing MV guard for `classify_schema_change`.)

- [ ] **Step 4: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "test(worker): typed transform stays green over a source with a live inline row

Pins the blast radius the merge-view nullability fix had to avoid: infer_columns
copies is_nullable() from the SERVED schema, so a widened merged-view schema
would make check_conformance reject every REQUIRED property on a source that
merely has an inline UPDATE shadow. Verified RED without the final-projection
not_null restore, GREEN with it."
```

---

## Task 5: Fix B e2e + un-ignore the two acceptance cases

**Files:**
- Create: `src/services/query-api/tests/vector_objects_read_tombstone_e2e.rs` + BUCK target
- Modify: `src/services/query-api/tests/vector_search_cold_suppression_e2e.rs` (delete both `#[ignore]`s)

- [ ] **Step 1: Write the full-column merged-read e2e**

Create `src/services/query-api/tests/vector_objects_read_tombstone_e2e.rs`. **Assertion shapes are load-bearing — the JSON render is NOT what you'd guess:**
- `id` is a `Long` ⇒ `SqlValue::Int` ⇒ rendered as a **numeric string**. `o["id"] == json!(1)` NEVER matches; it must be `json!("1")` (this is exactly why `e2e_support::ids_i64` parses `o["id"].as_str()`, `e2e_support.rs:485-494`).
- A `vector(4)` cell has **no** `SqlValue` variant: `arrow_to_sqlvalue` (`serving_datafusion.rs:78-81`) falls through to `ArrayFormatter` ⇒ `SqlValue::Text("[0.9, 0.1, 0.0, 0.0]")` ⇒ rendered as a JSON **string**. `one["embedding"].as_array()` panics. Assert on the string.

```rust
//! Full-column governed read over the merged view with a live tombstone inline
//! row (iss-search-vector-merge-view-nullable, Defect B). A non-CDC DELETE's
//! inline row carries only the identity — every other data column is physically
//! NULL — and the merge fold NEEDS that row (identity + loom_tombstone hide the
//! file row). The inline tier's DECLARED schema must admit those NULLs, or the
//! scan batch fails arrow's non-nullable validation inside
//! `PgTableProvider::fetch_batch` and the read 500s before the fold ever drops
//! the tombstone. `/search` never trips this (its survivor post-filter projects
//! only the identity, so the vector column is pruned from the inline scan); any
//! read that materializes a REQUIRED non-identity column does.
//!
//! Writes the inline deltas directly via iceberg_inline (the proven pattern from
//! vector_search_cold_suppression_e2e.rs). loom_fixture_test (Postgres).

use std::sync::Arc;

use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{ColumnSpec, DatasetId, EventType, LineageEvent, RunId, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_inline;
use e2e_support::{get, grant_read, ids_i64, seed_vector_type, subject_with_role};

use axum::http::StatusCode;

// `docs_table`, `docs_columns`, `vec_batch`, `id_batch`, `lineage` — copy verbatim
// from vector_search_cold_suppression_e2e.rs:38-98 (same fixture, same shapes).

/// GET /objects/Docs with a live inline UPDATE (id=1) and a live inline
/// tombstone (id=2): 200, survivors [1, 3, 4], and id=1 serves the UPDATED
/// embedding — the merge winner's values flow through the widened inline tier and
/// out through the mirror-nullability-restoring final projection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn objects_read_with_live_tombstone_serves_survivors() {
    let fx = PgFixture::shared();
    let (_init, db) = fx.fresh_db().await;
    let (cp, serving, _writer) = seed_vector_type(fx, &db).await;
    let (_subj, role) = subject_with_role(&cp, "reader").await;
    grant_read(&cp, &role, "Docs").await;
    let cp = Arc::new(cp);

    let pool = fx.pool_for(&db).await;
    let table = docs_table();
    let columns = docs_columns();

    // Inline UPDATE on id=1: a full-row shadow with a NEW embedding.
    let v1 = iceberg_inline::current_inline_version(&pool, &table, &columns, "id", &id_batch(1))
        .await
        .unwrap();
    iceberg_inline::write_inline_delta(
        &pool,
        &table,
        &columns,
        "id",
        false,
        &vec_batch(1, [0.9, 0.1, 0.0, 0.0]),
        None,
        lineage(RunId(uuid::Uuid::new_v4()), &table),
        v1,
        None,
        &[], // jobs — the 11th param
    )
    .await
    .unwrap();

    // Inline DELETE on id=2: an id-only tombstone (embedding NULL in inline_<tid>).
    let v2 = iceberg_inline::current_inline_version(&pool, &table, &columns, "id", &id_batch(2))
        .await
        .unwrap();
    iceberg_inline::write_inline_delta(
        &pool,
        &table,
        &columns,
        "id",
        true,
        &id_batch(2),
        None,
        lineage(RunId(uuid::Uuid::new_v4()), &table),
        v2,
        None,
        &[], // jobs — the 11th param
    )
    .await
    .unwrap();

    // Full-column read: the projection materializes `embedding` from BOTH tiers,
    // including the tombstone row's NULL. Pre-fix: 500.
    let (status, body) = get(cp.clone(), serving.clone(), "/objects/Docs", "reader").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "full-column merged read succeeds with a live tombstone: {body}"
    );
    assert_eq!(
        ids_i64(&body),
        vec![1, 3, 4],
        "tombstoned id=2 hidden; survivors served: {body}"
    );

    // The merge winner's VALUES are intact. `id` is a Long -> NumericString, and a
    // vector(4) has no SqlValue variant -> ArrayFormatter -> SqlValue::Text, i.e. a
    // JSON *string*. (arrow_to_sqlvalue, serving_datafusion.rs:78-81.)
    let objects = body["objects"].as_array().expect("objects array");
    let one = objects
        .iter()
        .find(|o| o["id"] == serde_json::json!("1"))
        .unwrap_or_else(|| panic!("id=1 present: {body}"));
    let emb = one["embedding"].as_str().unwrap_or_else(|| {
        panic!("a vector(4) renders as a JSON string (ArrayFormatter): {body}")
    });
    assert!(
        emb.starts_with("[0.9") && emb.contains("0.1"),
        "id=1 serves the UPDATED embedding [0.9, 0.1, 0.0, 0.0], got {emb}: {body}"
    );
}
```

(The `starts_with` / `contains` form is deliberate — `ArrayFormatter`'s exact float spacing is not a contract worth pinning. If you want the exact string, print it once and pin it, but keep the assertion message.)

Wire the BUCK target in `src/services/query-api/BUCK` by copying `vector-search-cold-suppression-e2e` (`BUCK:355-373`) verbatim and changing `name`/`crate`/`srcs`/`crate_root` — the dep list is identical.

- [ ] **Step 2: Run it — expect GREEN (Fix B already landed in Task 3)**

```
buck2 test --console none //src/services/query-api:vector-objects-read-tombstone-e2e
```

If it FAILS with a 500, Task 3 is incomplete — do not paper over it here.

To confirm the test actually reproduces the defect, `git stash` the `serving.rs` change, re-run (expect 500 / `Column 'embedding' is declared as non-nullable but contains null values`), then `git stash pop`.

- [ ] **Step 3: Delete both `#[ignore]` attributes**

In `src/services/query-api/tests/vector_search_cold_suppression_e2e.rs`, remove the `#[ignore = "blocked by iss-search-vector-merge-view-nullable: ..."]` attribute from:
- `cold_hits_suppressed_with_no_row_filter` (attribute at `:108-112`, fn at `:113`)
- `cold_hit_suppressed_with_row_filter_regression` (attribute at `:207-209`, fn at `:210`)

Leave the `#[tokio::test(...)]` attributes and everything else in the file untouched.

- [ ] **Step 4: Run the acceptance cases — three times, to pin determinism**

```bash
buck2 test --console none //src/services/query-api:vector-search-cold-suppression-e2e
buck2 test --console none //src/services/query-api:vector-search-cold-suppression-e2e
buck2 test --console none //src/services/query-api:vector-search-cold-suppression-e2e
```

Expected: `Tests finished: Pass 2. Fail 0` three times. (The pre-fix failure was deterministic — the register's "first query fails, subsequent succeed" was a mis-diagnosis — so three green runs pin that no order-dependence was introduced either.)

- [ ] **Step 5: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "test(search): full-column tombstone read e2e + un-ignore the cold-suppression cases

Both cases were blocked on iss-search-vector-merge-view-nullable; with the
hot-delta tombstone exclusion (Fix A) and the inline-tier widening + merged-view
nullability restore (Fix B) they pass deterministically (verified 3x)."
```

---

## Task 6: Final verification + register close

- [ ] **Step 1: Whole-tree build + full suite**

```bash
buck2 build -v0 --console none //src/...
buck2 test --console none -j 8 //src/...
```

Expected: silent build; `Tests finished: Pass N. Fail 0`. (Local runs need `-j 8` — the PG fixture has 8 boot slots. In a cloud session, build with `-M none` and scope tests to the btd-affected targets instead of the whole tree.)

- [ ] **Step 2: Clippy + lint sweep**

```bash
./tools/clippy-all.sh
buck2 run //tools:prek -- run --all-files
```

Expected: clean. Commit anything the hooks fixed.

- [ ] **Step 3: Close the register item**

Run the **loom-docs-update** skill: close `iss-search-vector-merge-view-nullable` in `docs/ISSUES.md` (remove the entry in this branch's closing commit/PR) and document the landed behavior under `docs/system-capabilities/`. The record of what shipped: (1) the hot delta excludes unscoreable rows; (2) the inline tier declares its non-identity columns nullable while the merged view's final projection restores the mirror's nullability — **the served schema never changes**, which is what keeps typed transforms and MVs green.

If triage judges either of these worth tracking, record as new FUTURE items; otherwise leave as spec/doc prose:
- **CDC hot-scoring semantics** — `inline_delta_batch` still scores every live `+I`/`+U` image in the window (spec Non-goals).
- **`fut-`: a first-class non-null assertion.** `loom_not_null` is loom's own; if DataFusion ever ships one (or a provider-level schema-override node), swap to it.

- [ ] **Step 4: Finish the branch**

Use the **finishing-a-development-branch** skill: push and open a PR (never a local merge), title `fix(serving): vector nullability 500 on hot delta + merged view (iss-search-vector-merge-view-nullable)`. Poll CI via the commit-status endpoint + BuildBuddy MCP (NOT `gh pr checks`).

---

## Self-Review

### The blast-radius question, and which option was chosen

**Chosen: option (a) — widen ONLY the inline tier's internal provider schema; the merged/served output schema's nullability is unchanged.** No `infer_columns` change, no `check_conformance` change, no `classify_schema_change` change, and no currently-green transform or MV can turn red.

Option (a) is **not** free, though — and the gate's framing of it ("IF tombstones/NULLs cannot reach the final projection, the merged view's OUTPUT can legitimately keep the mirror's non-nullable declaration") is half right and half wrong, in a way that matters:

- **Right (verified):** NULLs genuinely **cannot** reach the final projection.
  - The only inline rows with NULL data columns are **non-CDC tombstones**. `write_inline_delta` (`iceberg_inline.rs:1217`) has exactly three emit arms: the CDC arm writes `-D` with the **full prior image** and `-U`/`+U` with full before/after images (`write_cdc_row(…, before_cols, before_batch)` / `(…, columns, batch)`); the non-CDC tombstone arm literally comments **"data NULL"** and inserts `(begin_snapshot, loom_tombstone, loom_change_kind, "<id>") values ($1, true, '-D', $2)`; the non-CDC version arm inserts **every** column in `columns`, and its only production caller (`action.rs:1196-1212`) passes the **full post-PATCH row**.
  - `build_merge_view` filters `.filter(cref("_loom_tomb").eq(lit(false)))` (`serving.rs:495`) **before** the final `.select(...)` (`:497-503`), and for `Precedence::Snapshot` `_loom_tomb` **is** `loom_tombstone` (`:423`). So the filter drops precisely the NULL-carrying rows.
- **Wrong (verified):** DataFusion does **not** narrow the declared nullability back on its own. `Union::derive_schema_from_inputs_by_position` sets `let nullable = fields.iter().any(|field| field.is_nullable());` (datafusion-expr 54.0.0 `logical_plan/plan.rs:3130` — nullable-**any**), and a `Filter` never narrows nullability, and an `Expr::Column` projection copies the input DFSchema's field verbatim. **So the widening WOULD propagate to the served schema if nothing restored it.** That is exactly the trap the previous plan fell into.

Hence option (a) requires an explicit restore, which this plan adds: `loom_not_null`, a 40-line identity `ScalarUDF` whose `return_field_from_args` reports a **non-nullable** field. That is honored at both levels — logically (`ScalarUDFImpl::return_field_from_args`, datafusion-expr 54 `udf.rs:677`; the default impl is what forces `true`, so overriding it is the sanctioned mechanism — DataFusion's own `coalesce` narrows this way) and physically (`ScalarFunctionExpr::nullable(&self, _) -> Ok(self.return_field.is_nullable())`, datafusion-physical-expr 54 `scalar_function.rs:232-234`). Applied only to columns the **mirror** declares required, on the projection that sits above the tombstone filter.

Option (b) was rejected: the widening is **not** semantically required at the output, because the data there provably has no NULLs. Declaring it nullable would be a *pessimistic lie* with a large, silent cost (typed transforms aborting on `NullabilityViolation`; MV re-runs failing `ColumnNullabilityChanged`) triggered by something as benign as a single inline UPDATE shadow on a source table. Paying for that with a fleet of worker non-regression tests documenting the new failure mode would be accepting a regression, not fixing a bug.

Defect A takes the narrow route the gate predicted: **exclude** tombstoned/`-U`/vector-less rows from the index-build/hot-delta query rather than widen the field. A tombstoned row is unscoreable and must not be indexed; the survivor post-filter already suppresses its stale cold hit (`2026-07-07-search-cold-suppression-design`). `inline_delta_batch`'s output schema stays byte-identical, and its non-nullable vector field becomes *truthful* instead of merely *asserted*.

### Residual risks (called out for the implementer)

1. **DataFusion optimizer moving the final projection below the tombstone filter.** Logical projection-pushdown (`optimize_projections`) prunes columns; it does not push a projection *expression* below a filter, and the physical projection-pushdown rule cannot swap a projection past a filter whose predicate (`_loom_tomb`) is not in the projection's output. If it somehow did, the failure mode is **loud** — `RecordBatch::try_new` inside `ProjectionExec` raises the same `declared as non-nullable but contains null values` error — never a wrong answer. Task 3 Step 6 and Task 5 Step 2 would catch it immediately.
2. **The `merge-view-schema` pin (Task 3 Step 1) is the contract.** It must assert `provider.schema().fields() == mirror.fields()` (nullability included) **and** that the read succeeds. Asserting only the read would let a naive widening land.
3. **The Task 4 bite-check is mandatory, not optional.** Reverting the `not_null` restore must make the typed-transform case fail with `NullabilityViolation`. If it does not, the blast-radius theory is wrong somewhere and the whole design of Fix B should be re-derived before landing.
4. **`Signature::any(1, Volatility::Immutable)`** — if DataFusion 54's coercion rejects `List<Float32>` under `any`, switch to `Signature::user_defined(Volatility::Immutable)` with a `coerce_types` that returns the input types unchanged. The `not-null-udf` test (Task 2) is what surfaces this, and it runs before any of the serving changes.
