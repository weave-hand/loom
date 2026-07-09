# `/search` Vector Nullability 500 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix `iss-search-vector-merge-view-nullable`: `/search` over an identity-bearing `vector(N)` type 500s once a DELETE tombstone inline row exists, and full-column reads over the merged view 500 the same way. Un-ignore the two blocked e2e cases in `vector_search_cold_suppression_e2e.rs`.

**Architecture** (spec: `docs/superpowers/specs/2026-07-09-search-vector-merge-view-nullable-design.md` — the diagnosis there is **corrected** from the register prose; both failures are deterministic `RecordBatch::try_new` nullability violations, no DataFusion nondeterminism exists): **Fix A** — `inline_delta_batch`'s hot-delta SQL excludes tombstoned / `-U` / vector-less rows (they are unscoreable; the survivor post-filter suppresses their cold hits). **Fix B** — `build_inline_provider`'s merge-mode schema declares non-identity data columns nullable (tombstone rows physically carry NULL there and the merge fold needs those rows); the union widens to match, so the merged view keeps exact mirror names/types/order with non-identity nullability widened iff an inline tier exists.

**Tech Stack:** Rust, arrow 58 / DataFusion 54, sqlx runtime `AssertSqlSafe` (no compile-time SQL touched), buck2 `loom_fixture_test`.

## Global Constraints

Carried from the spec; every task implicitly includes these:

- **Tests are `rust_test` / `loom_fixture_test` integration targets only — never inline `#[cfg(test)]`.** New fixture tests MUST use `loom_fixture_test` (`src/control-plane/postgres/defs.bzl`) with BUCK wiring mirroring a named sibling; the `no-inline-tests` prek hook fails on any inline `#[test]`.
- **No `.sqlx` regeneration:** both fixes touch runtime `AssertSqlSafe` SQL and arrow schema construction only. If a task finds itself editing a `query!`/`query_scalar!`, stop — that is outside this plan's scope.
- **Run prek before every commit:** `buck2 run //tools:prek -- run --all-files`. `git add` new files FIRST (prek skips untracked files). Markdown: exactly one trailing newline, no trailing whitespace.
- **Clippy is strict (pedantic + restriction):** no `unwrap`/`expect`/`indexing_slicing`/`panic`/`todo`/`map_err_ignore` in production lib/bin code; `#[expect(lint, reason = "...")]` for justified exceptions. Test code is exempt from panic-safety lints via `loom_fixture_test`.
- **Byte-identical baselines:** the hot-delta output schema and the merged-view values for existing fixtures must not move. Pinning targets (run in Task 4): `//src/control-plane/postgres:vector-index-inline-delta`, `//src/services/query-api:vector-search-e2e` (11 cases), `:cow-inline-shadow-e2e`, `:stream-merge-firstrow-e2e`, `:stream-merge-versioned-e2e`, `:stream-cdc-e2e`.
- **Build/test commands:** build `buck2 build -v0 --console none //src/...`; test `buck2 test --console none <targets>`. Full local suite: `buck2 test --console none -j 8 //src/...` (PG boot-slot starvation at higher `-j`). Cloud sessions: `-M none` on builds, scope tests to touched targets.

---

## File Structure

**Create:**
- `src/services/query-api/tests/vector_objects_read_tombstone_e2e.rs` — Fix B e2e (full-column merged read with a live tombstone).

**Modify (production):**
- `src/control-plane/postgres/src/vector_index.rs:354-365` — hot-delta SQL exclusion predicate + doc comment.
- `src/services/engine-serving/src/serving.rs:527-605` — `build_inline_provider` merge-mode nullability widening; `:269-293` — `build_merge_view` doc-comment schema claim revised.

**Modify (tests/BUCK):**
- `src/control-plane/postgres/tests/vector_index_inline_delta.rs` — new tombstone-exclusion case (target `vector-index-inline-delta` already wired, `src/control-plane/postgres/BUCK:1281`).
- `src/services/query-api/BUCK` — new `vector-objects-read-tombstone-e2e` target mirroring `vector-search-cold-suppression-e2e` (`BUCK:297`).
- `src/services/query-api/tests/vector_search_cold_suppression_e2e.rs:108-112,207-209` — delete both `#[ignore]` attributes (Task 3 ONLY — not before Fix A lands).

---

## Task 1: Fix A — exclude tombstone / `-U` / vector-less rows from the hot delta

**Files:**
- Modify: `src/control-plane/postgres/tests/vector_index_inline_delta.rs` (failing test first)
- Modify: `src/control-plane/postgres/src/vector_index.rs:354-365`

**Interfaces:**
- Consumes: the file's existing `seed`/`ipc_long`/`columns`/`lineage_evt` harness; `iceberg_inline::{current_inline_version, write_inline_delta}` (the proven tombstone-write pattern from `vector_search_cold_suppression_e2e.rs:166-185`).
- Produces: `inline_delta_batch` whose WHERE additionally requires `not loom_tombstone`, `loom_change_kind <> '-U'` (null-tolerant), and a non-NULL vector. Signature, output schema, and the single caller (`engine-serving/src/vector_search.rs:129`) unchanged.

- [ ] **Step 1: Write the failing test**

Append to `src/control-plane/postgres/tests/vector_index_inline_delta.rs` (imports: add `use control_plane_postgres::iceberg_inline;` to the existing block):

```rust
/// A one-column id-only batch — the tombstone's carried batch (the CAS lookup
/// key), mirroring vector_search_cold_suppression_e2e.rs::id_batch.
fn id_batch(id: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![id]))]).expect("id_batch")
}

/// RED pre-fix: a live tombstone inline row in the delta window carries a NULL
/// vector, and inline_delta_batch's non-nullable output schema fails arrow's
/// RecordBatch::try_new validation ("Column 'embedding' is declared as
/// non-nullable but contains null values") -> every /search over the type 500s
/// (iss-search-vector-merge-view-nullable, Defect A). Desired: tombstoned, -U,
/// and vector-less rows are EXCLUDED from the hot delta — they are unscoreable;
/// suppressing their stale cold hits is the survivor post-filter's job
/// (2026-07-07-search-cold-suppression-design).
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
    // (the vector) is NULL in inline_<tid>. Same write pattern as
    // vector_search_cold_suppression_e2e.rs.
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

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test --console none //src/control-plane/postgres:vector-index-inline-delta`
Expected: FAIL — `tombstoned_rows_are_excluded_from_hot_delta` panics at "hot delta must not fail on a tombstone in the window" with the Backend-wrapped `Invalid argument error: Column 'embedding' is declared as non-nullable but contains null values`. The three pre-existing cases stay green.

- [ ] **Step 3: Implement the exclusion predicate**

In `src/control-plane/postgres/src/vector_index.rs`, replace the delta SQL (`:354-365`):

```rust
    // Runtime query: select only the identity + vector columns with the delta
    // MVCC predicate. Tombstones (`loom_tombstone` — both a non-CDC delete and
    // a CDC `-D` set it), CDC `-U` before-images (audit rows, mirroring the
    // merge-view base predicate), and rows without a vector are EXCLUDED: none
    // is scoreable, and a tombstone's NULL vector would fail the non-nullable
    // output schema's RecordBatch validation (the /search 500 of
    // iss-search-vector-merge-view-nullable). A tombstoned identity's stale
    // cold hit is suppressed by query-api's survivor post-filter, not here.
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

(Every inline table carries `loom_tombstone`/`loom_change_kind` `NOT NULL` with defaults — `inline_ddl`, `iceberg_inline.rs:277-297`, plus the `add column if not exists` backfills — so the predicate is valid against every existing relation. The output schema at `:378-384` stays byte-identical; the `is not null` clause is what keeps its non-nullable vector field truthful.)

Also update `inline_delta_batch`'s doc comment (above `:287`) to say the returned rows are the *scoreable* delta: live row-versions born in the window, excluding tombstones, `-U` before-images, and vector-less rows.

- [ ] **Step 4: Run the seam test + the hot-path pinning suite**

Run: `buck2 test --console none //src/control-plane/postgres:vector-index-inline-delta //src/services/query-api:vector-search-e2e`
Expected: PASS — the new case green; `int_identity_delta_batch_shape` (the byte-identical contract) and the 11 `vector_search_e2e` cases untouched.

- [ ] **Step 5: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "fix(vector): exclude tombstone/-U/vector-less rows from the hot delta

inline_delta_batch selected every MVCC-live inline row in the window, so a
DELETE's id-only tombstone row put a NULL vector under the batch's
non-nullable vector field — arrow's RecordBatch::try_new validation failed
and every /search over the type 500'd (iss-search-vector-merge-view-nullable,
Defect A; deterministic, not the register's suspected first-query
nondeterminism). Unscoreable rows are now excluded in SQL; the survivor
post-filter keeps suppressing their stale cold hits."
```

---

## Task 2: Fix B — merge-mode inline provider declares non-identity columns nullable

**Files:**
- Create: `src/services/query-api/tests/vector_objects_read_tombstone_e2e.rs` (failing test first)
- Modify: `src/services/query-api/BUCK` (new target)
- Modify: `src/services/engine-serving/src/serving.rs:527-605` and `:269-293`

**Interfaces:**
- Consumes: `e2e_support::{seed_vector_type, get, ids_i64, grant_read, subject_with_role}`; the tombstone-write pattern from Task 1; `Field::with_nullable` (arrow 58).
- Produces: `build_inline_provider` merge-mode schema with non-identity data columns nullable (identity + the four framing fields unchanged); the merged view's output schema = exact mirror names/types/order, non-identity nullability widened iff an inline tier exists (union nullable-any: datafusion-expr 54 `logical_plan/plan.rs:3130`).

- [ ] **Step 1: Write the failing e2e**

Create `src/services/query-api/tests/vector_objects_read_tombstone_e2e.rs`:

```rust
//! Full-column governed read over the merged view with a live tombstone inline
//! row (iss-search-vector-merge-view-nullable, Defect B). A DELETE's inline row
//! carries only the identity — every other data column is physically NULL —
//! and the merge fold NEEDS that row (identity + _loom_tomb hide the file
//! row). The inline tier's declared schema must admit those NULLs, or the scan
//! batch fails arrow's non-nullable validation inside
//! PgTableProvider::fetch_batch and the read 500s before the fold ever drops
//! the tombstone. /search never trips this (its survivor post-filter projects
//! only the identity, so the vector column is pruned from the inline scan);
//! any read that materializes a REQUIRED non-identity column does.
//!
//! Writes the inline deltas directly via iceberg_inline (the proven pattern
//! from vector_search_cold_suppression_e2e.rs). loom_fixture_test (Postgres).

use std::sync::Arc;

use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{ColumnSpec, DatasetId, EventType, LineageEvent, RunId, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_inline;
use e2e_support::{get, grant_read, ids_i64, seed_vector_type, subject_with_role};

use axum::http::StatusCode;

/// The `wh.docs` table `seed_vector_type` lands into.
fn docs_table() -> TableRef {
    TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    }
}

/// The `Docs(id long, embedding vector(4))` inline column set — `embedding` is
/// REQUIRED (non-nullable in the mirror), the precondition for this defect.
fn docs_columns() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        },
        ColumnSpec {
            name: "embedding".into(),
            ty: "vector(4)".into(),
            nullable: false,
        },
    ]
}

/// A one-row `{id, embedding}` batch (Int64 `id` + `List<Float32>` `embedding`).
fn vec_batch(id: i64, e: [f32; 4]) -> RecordBatch {
    let item = Arc::new(Field::new("item", DataType::Float32, false));
    let mut lb = ListBuilder::new(Float32Builder::new()).with_field(item.clone());
    lb.values().append_slice(&e);
    lb.append(true);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("embedding", DataType::List(item), false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![id])), Arc::new(lb.finish())],
    )
    .expect("vec_batch")
}

/// A one-cell id-only batch — the tombstone's carried batch.
fn id_batch(id: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![id]))]).expect("id_batch")
}

fn lineage(run: RunId, table: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(table).dataset_ref()],
        payload: serde_json::json!({ "source": "e2e" }),
    }
}

/// GET /objects/Docs with a live inline UPDATE (id=1) and a live inline
/// tombstone (id=2): 200, survivors [1, 3, 4], and id=1 serves the UPDATED
/// embedding — the merge winner's values flow through the widened schema.
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
    )
    .await
    .unwrap();

    // Full-column read: the projection materializes `embedding` from BOTH
    // tiers, including the tombstone row's NULL. Pre-fix: 500.
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

    // The merge winner's VALUES are intact over the widened schema: id=1
    // serves the updated embedding (f32 [0.9, 0.1, 0.0, 0.0], compared with
    // an f32->f64 tolerance).
    let objects = body["objects"].as_array().expect("objects array");
    let one = objects
        .iter()
        .find(|o| o["id"] == serde_json::json!(1))
        .expect("id=1 present");
    let emb: Vec<f64> = one["embedding"]
        .as_array()
        .expect("embedding array")
        .iter()
        .map(|v| v.as_f64().expect("f64 element"))
        .collect();
    let want = [0.9_f64, 0.1, 0.0, 0.0];
    assert_eq!(emb.len(), want.len(), "4-dim embedding");
    for (got, want) in emb.iter().zip(want.iter()) {
        assert!(
            (got - want).abs() < 1e-3,
            "id=1 serves the UPDATED embedding, got {emb:?}"
        );
    }
}
```

- [ ] **Step 2: Wire the BUCK target**

In `src/services/query-api/BUCK`, after the `vector-search-cold-suppression-e2e` target (`:297-315`), add a sibling mirroring it exactly (same deps list — `:query-api`, `:e2e-support`, `//src/control-plane/core:core`, `//src/control-plane/postgres:postgres`, `//third-party:arrow-array`, `//third-party:arrow-schema`, `//third-party:axum`, `//third-party:serde_json`, `//third-party:time`, `//third-party:tokio`, `//third-party:uuid`):

```python
loom_fixture_test(
    name = "vector-objects-read-tombstone-e2e",
    crate = "vector_objects_read_tombstone_e2e",
    srcs = ["tests/vector_objects_read_tombstone_e2e.rs"],
    crate_root = "tests/vector_objects_read_tombstone_e2e.rs",
    deps = [
        ":query-api",
        ":e2e-support",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:arrow-array",
        "//third-party:arrow-schema",
        "//third-party:axum",
        "//third-party:serde_json",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `buck2 test --console none //src/services/query-api:vector-objects-read-tombstone-e2e`
Expected: FAIL — status 500 (the `internal error` body): `PgTableProvider::fetch_batch` (`provider.rs:146`) rejects the inline scan batch (`Column 'embedding' is declared as non-nullable but contains null values`). If `ids_i64`'s exact shape differs from the `/objects` response, fix the test's extraction (it must fail on the STATUS assertion, not on parsing).

- [ ] **Step 4: Widen the merge-mode inline provider schema**

In `src/services/engine-serving/src/serving.rs`, `build_inline_provider` (`:591-605`), replace the merge-mode field construction:

```rust
    let (provider_schema, logical_types) = if identity.is_some() {
        // Merge mode. Two adjustments over the raw mirror schema:
        //   1. Non-identity data columns are declared NULLABLE. Tombstone rows
        //      physically carry NULL in every non-identity column (a DELETE's
        //      inline row is id-only) and the merge fold NEEDS those rows (the
        //      identity + _loom_tomb hide the file row) — a non-nullable
        //      declaration makes the scan batch fail arrow's RecordBatch
        //      validation before the fold can drop the tombstone
        //      (iss-search-vector-merge-view-nullable). The union widens to
        //      match (nullability = any input nullable), so the merged view
        //      keeps exact mirror names/types/order with non-identity
        //      nullability widened. The identity keeps its mirror nullability —
        //      a tombstone always carries it (extract_id_cell).
        //   2. BOTH precedence pairs are appended (physical names) so the dedup
        //      can rank rows and hide tombstoned identities under either
        //      `Precedence`. Unused columns for a given mode are never selected
        //      (DataFusion projects only what the fold references), so this is
        //      a no-op cost for the mode not in play.
        let mut fields: Vec<Field> = schema
            .fields()
            .iter()
            .map(|f| {
                let f = f.as_ref().clone();
                if identity == Some(f.name().as_str()) {
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

(Keep the existing doc comment on `build_inline_provider` (`:524-540`) and extend it with the nullability rule; the identity-less branch is byte-identical.)

- [ ] **Step 5: Revise the `build_merge_view` schema-equality doc claim**

In `serving.rs:288-293`, the comment currently reads "…the view's schema equals `schema` exactly — which the governed layer and callers require. (`DISTINCT ON` would widen the identity column to nullable.)". Replace that sentence block with:

```rust
/// A `ROW_NUMBER()` window (not `DISTINCT ON`) is used deliberately: the window is a
/// pass-through over the data columns, so they keep their mirror `DataType` end to
/// end (union coerces identical schemas to themselves; window / filter / final
/// projection are pass-through). Thus the final projection needs no casts and the
/// view's schema keeps EXACTLY the mirror column names, types, and order.
/// Nullability is the one deliberate deviation: when an inline tier is present, its
/// non-identity data columns are declared nullable (tombstone rows physically hold
/// NULL there — see `build_inline_provider`), and the union widens the merged
/// schema to match. Values are unaffected: tombstone winners are filtered out, and
/// surviving row-versions carry full rows. (`DISTINCT ON` would additionally widen
/// the identity column to nullable, which this shape avoids.)
```

- [ ] **Step 6: Run the e2e + the merge-view pinning suites**

Run: `buck2 test --console none //src/services/query-api:vector-objects-read-tombstone-e2e //src/services/query-api:cow-inline-shadow-e2e //src/services/query-api:datafusion-inline-union //src/services/query-api:stream-merge-firstrow-e2e //src/services/query-api:stream-merge-versioned-e2e //src/services/query-api:stream-cdc-e2e`
Expected: PASS — the new e2e green (200, `[1, 3, 4]`, updated embedding); every pinning suite unchanged (their fixtures' non-identity columns are already nullable, so the widening is a schema no-op there; CDC folds are value-asserted). If a BUCK target name above does not resolve, find the actual name with `grep -n 'name = "' src/services/query-api/BUCK` and run the real sibling — do not skip the suite.

- [ ] **Step 7: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "fix(serving): merge-mode inline tier declares non-identity columns nullable

A DELETE's inline row is id-only — every non-identity data column is
physically NULL — and the merge fold needs that row to hide the file row.
Presenting the mirror's non-nullable schema over it made
PgTableProvider::fetch_batch fail arrow's RecordBatch validation on any
full-column read of the merged view (500), before the _loom_tomb filter
could drop the tombstone (iss-search-vector-merge-view-nullable, Defect B).
The inline tier now declares non-identity columns nullable; the union widens
to match, keeping exact mirror names/types/order. Identity, framing fields,
and the identity-less path are byte-identical."
```

---

## Task 3: Un-ignore the two blocked acceptance cases

**Files:**
- Modify: `src/services/query-api/tests/vector_search_cold_suppression_e2e.rs:107-112` and `:206-209`

- [ ] **Step 1: Delete both `#[ignore]` attributes**

Remove the `#[ignore = "blocked by iss-search-vector-merge-view-nullable: …"]` attribute from `cold_hits_suppressed_with_no_row_filter` (`:108-112`) and from `cold_hit_suppressed_with_row_filter_regression` (`:207-209`), leaving the `#[tokio::test(...)]` attributes intact. No other edits to the file.

- [ ] **Step 2: Run the acceptance cases — repeatedly, to pin determinism**

```bash
buck2 test --console none //src/services/query-api:vector-search-cold-suppression-e2e
buck2 test --console none //src/services/query-api:vector-search-cold-suppression-e2e
buck2 test --console none //src/services/query-api:vector-search-cold-suppression-e2e
```

Expected: `Tests finished: Pass 2. Fail 0` three times. (The pre-fix failure was deterministic — the register's "first query fails, subsequent succeed" was a mis-diagnosis — so three green runs pin that no order-dependence was introduced either.)

- [ ] **Step 3: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "test(search): un-ignore the cold-suppression e2e cases

Both cases were blocked on iss-search-vector-merge-view-nullable; with the
hot-delta tombstone exclusion (Fix A) and the merge-mode inline-tier
nullability widening (Fix B) they pass deterministically (verified 3x)."
```

---

## Task 4: Final verification + register close

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

Run the **loom-docs-update** skill: close `iss-search-vector-merge-view-nullable` in `docs/ISSUES.md` (remove the entry in this branch's closing commit/PR), documenting the landed behavior per `docs/system-capabilities/`. The spec's corrected diagnosis (deterministic arrow validation in `inline_delta_batch` + the merge-mode inline tier; no DataFusion nondeterminism) is the record of what actually shipped. If the skill's triage judges the CDC hot-scoring note (spec Non-goals — every live `+I`/`+U` image in the window is scored) worth tracking, record it as a new FUTURE item; otherwise leave it as spec prose.

- [ ] **Step 4: Finish the branch**

Use the **finishing-a-development-branch** skill: push and open a PR (never a local merge), title `fix(serving): vector nullability 500 on hot delta + merged view (iss-search-vector-merge-view-nullable)`. Poll CI via the commit-status endpoint + BuildBuddy MCP (NOT `gh pr checks`).
