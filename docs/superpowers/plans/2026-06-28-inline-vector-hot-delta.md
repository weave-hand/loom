# Inline (hot-tier) vector storage + real k-NN hot delta — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Teach loom's inline (hot) tier to store and reconstruct a `vector(N)` column as a Postgres `real[]`, so the already-built hot/cold k-NN merge seam returns real hot-delta rows.

**Architecture:** The query path (`vector_search` → `inline_delta_batch` + `merge_topk`) and the cold index already exist from PR #207; they only ever saw an empty hot delta because the inline tier rejected `vector(N)` columns. This plan adds a `vector(N)` arm to the inline tier's per-type write/read match arms (storing native f32 as `real[]`), switches the hot-delta reader off its dead jsonb path, and lands the deferred Acceptance 4 test (a row landed inline between `S` and `Q`, merged exactly once).

**Tech Stack:** Rust, sqlx 0.9 (Postgres, runtime `AssertSqlSafe` queries — no compile-time macros here), Arrow (`arrow_array`/`arrow_schema`), buck2, hermetic Postgres fixtures (`loom_fixture_test`).

## Global Constraints

- **Tests are `rust_test` integration targets only — NEVER inline `#[cfg(test)]` modules.** Put tests in `tests/<name>.rs` wired as a `rust_test`/`loom_fixture_test` in the crate's `BUCK`. The `no-inline-tests` prek hook fails on any `#[test]` under `src/**`.
- **Postgres fixture tests must use the `loom_fixture_test` macro** (`src/control-plane/postgres/defs.bzl`), not a bare `rust_test`, or they route to remote execution and fail as root.
- **No `.sqlx` cache changes in this slice.** Every inline-path query is runtime `sqlx::query(AssertSqlSafe(...))` (the inline table name is dynamic), so no compile-time `query!`/`query_scalar!` is added and `tools/sqlx-prepare.sh` need not run.
- **Don't pipe `buck2 test` through `tail`/`head`** — redirect to a file and grep it: `buck2 test … > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`.
- **Strict clippy is enforced** (pedantic + restriction). Run `buck2 build '//<target>[clippy.txt]'` — empty output means clean. Remove imports that become unused.
- **Branch:** all work lands on `work/inline-vector-hot-delta` (stacked on PR #207). Commit per task.
- **Storage encoding is `real[]`** (native f32) — decided in the spec; not jsonb.

---

## File Structure

- `src/control-plane/postgres/src/iceberg_type.rs` — add `vector(N)` → `real[]` to `pg_type_for`; add the shared `mirror_column_type` helper (de-dups the cold path's vector special-case).
- `src/control-plane/postgres/src/iceberg_landing.rs` — cold path uses `mirror_column_type` instead of an inline `match`.
- `src/control-plane/postgres/src/iceberg_inline.rs` — `Cell::Vec` variant + `vector(N)` arms in `cell_from_arrow` / `bind_cell` / `arrow_field` / `column_array`; inline projection uses `mirror_column_type`.
- `src/control-plane/postgres/src/vector_index.rs` — `inline_delta_batch` decodes the vector column from `real[]` instead of jsonb.
- `src/control-plane/postgres/tests/iceberg_inline_types.rs` — unit tests for the new type maps (pure logic).
- `src/control-plane/postgres/tests/iceberg_inline_vector.rs` — **new** fixture test: inline vector round-trip.
- `src/control-plane/postgres/BUCK` — wire the new `iceberg-inline-vector` fixture target.
- `src/services/engine-serving/tests/vector_search.rs` — Acceptance 4: cold∪hot merge (Cosine + L2).
- `docs/FUTURE.md` — close `fut-inline-vector-hot-delta` at the end.

---

### Task 1: Type mapping — `pg_type_for` vector arm + shared `mirror_column_type`

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_type.rs`
- Modify: `src/control-plane/postgres/src/iceberg_landing.rs:478-503`
- Modify: `src/control-plane/postgres/src/iceberg_inline.rs:34` and `:191`
- Test: `src/control-plane/postgres/tests/iceberg_inline_types.rs`

**Interfaces:**
- Produces: `pub fn mirror_column_type(logical: &str) -> Option<String>` in `iceberg_type` — `vector(N)` → `"vector(N)"`, every other type → its Iceberg primitive name, `None` if unmapped.
- Produces: `pg_type_for("vector(N)") == Some("real[]")`.

- [ ] **Step 1: Write the failing unit tests** in `tests/iceberg_inline_types.rs`. Change the import line and append a test:

```rust
use control_plane_postgres::iceberg_type::{iceberg_physical_type, mirror_column_type, pg_type_for};
```

```rust
#[test]
fn vector_maps_to_real_array_and_keeps_dimension() {
    // Inline Postgres storage type: a native f32 array, dimension-independent.
    assert_eq!(pg_type_for("vector(4)"), Some("real[]"));
    assert_eq!(pg_type_for("vector(1536)"), Some("real[]"));

    // The mirror `column_type` text preserves the parameterized form for BOTH
    // write paths (the dimension lives here and is decoded by logical_from_iceberg).
    assert_eq!(mirror_column_type("vector(4)").as_deref(), Some("vector(4)"));
    assert_eq!(mirror_column_type("vector(1536)").as_deref(), Some("vector(1536)"));
    assert_eq!(mirror_column_type("long").as_deref(), Some("long"));
    assert_eq!(mirror_column_type("timestamp").as_deref(), Some("timestamp"));
    assert_eq!(mirror_column_type("decimal"), None);
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test //src/control-plane/postgres:iceberg-inline-types > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[|unresolved" /tmp/t.log`
Expected: BUILD fails — `mirror_column_type` is unresolved and `pg_type_for("vector(4)")` would be `None`.

- [ ] **Step 3: Add the `vector(N)` arm to `pg_type_for`** in `iceberg_type.rs`. Insert the guard arm immediately before `_ => None`:

```rust
        "timestamp" => Some("timestamp"),
        // A vector(N) column stores as a Postgres real[] — native f32, exact and
        // compact. The dimension N is carried by the mirror column_type text, not
        // the Postgres type, so this arm is dimension-independent.
        v if v.starts_with("vector(") => Some("real[]"),
        _ => None,
```

- [ ] **Step 4: Add the `mirror_column_type` helper** to `iceberg_type.rs` (after `iceberg_physical_type`):

```rust
/// loom logical type -> the `iceberg_mirror.column.column_type` string written by
/// BOTH the cold (Parquet) and inline write paths. A `vector(N)` column keeps its
/// parameterized form verbatim — the dimension lives in this text and is decoded
/// back by `logical_from_iceberg` — while every other type maps to its Iceberg
/// primitive name. `None` if loom has no Iceberg mapping for the type.
pub fn mirror_column_type(logical: &str) -> Option<String> {
    match control_plane_core::resolve_logical(logical) {
        Some(control_plane_core::BaseType::Vector(n)) => Some(format!("vector({n})")),
        _ => iceberg_physical_type(logical).map(str::to_string),
    }
}
```

- [ ] **Step 5: Use the helper on the cold path.** In `iceberg_landing.rs`, replace the `match`-on-`resolve_logical` block in `projected_columns` (lines 485-494) with:

```rust
            let iceberg_type = crate::iceberg_type::mirror_column_type(&c.ty).ok_or_else(|| {
                ControlPlaneError::Backend(
                    format!("register: no iceberg type for {:?}", c.ty).into(),
                )
            })?;
```

If `iceberg_physical_type` is now unused in `iceberg_landing.rs`, remove it from that file's `use` (the next clippy/build step will flag it if so).

- [ ] **Step 6: Use the helper on the inline path.** In `iceberg_inline.rs`, change the import at line 34 from:

```rust
use crate::iceberg_type::{iceberg_physical_type, pg_type_for};
```
to:
```rust
use crate::iceberg_type::{mirror_column_type, pg_type_for};
```
and change the `iceberg_type` projection in `inline_append` (line 191) from `iceberg_physical_type(&c.ty)...to_string()` to:

```rust
                iceberg_type: mirror_column_type(&c.ty).ok_or_else(|| {
                    ControlPlaneError::Backend(
                        format!("inline: no iceberg type for {:?}", c.ty).into(),
                    )
                })?,
```

(Note: `mirror_column_type` already returns an owned `String`, so the trailing `.to_string()` is gone.)

- [ ] **Step 7: Run the tests + clippy to verify they pass and nothing regressed**

Run: `buck2 test //src/control-plane/postgres:iceberg-inline-types //src/control-plane/postgres:vector-landing > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: `Tests finished: Pass … Fail 0`.
Run: `for c in '//src/control-plane/postgres:postgres'; do buck2 build "$c[clippy.txt]" --show-output 2>/dev/null | awk '{print $2}' | xargs wc -c; done`
Expected: `0` bytes (clippy clean).

- [ ] **Step 8: Commit**

```bash
git add src/control-plane/postgres/src/iceberg_type.rs src/control-plane/postgres/src/iceberg_landing.rs src/control-plane/postgres/src/iceberg_inline.rs src/control-plane/postgres/tests/iceberg_inline_types.rs
git commit -m "feat(postgres): vector(N) -> real[] inline type map + shared mirror_column_type"
```

---

### Task 2: Inline vector cell encode/decode + round-trip fixture test

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_inline.rs` (`Cell` enum ~line 79; `cell_from_arrow` ~line 90; `bind_cell` ~line 126; `arrow_field` ~line 277; `column_array` ~line 296)
- Create: `src/control-plane/postgres/tests/iceberg_inline_vector.rs`
- Modify: `src/control-plane/postgres/BUCK` (new `iceberg-inline-vector` target)

**Interfaces:**
- Consumes: `mirror_column_type` (Task 1) so inline projection of a vector column succeeds.
- Consumes: `pub async fn inline_append(pool: &PgPool, table: &TableRef, columns: &[ColumnSpec], batch: &RecordBatch, lineage: LineageEvent, flush_threshold: Option<i64>) -> Result<SnapshotId>` and `IcebergCatalog::inline_live_batch(&self, table: &TableRef, at: SnapshotId) -> Result<Option<(i64, Vec<i64>, RecordBatch)>>`.
- Produces: the inline tier reconstructs a `vector(N)` column as an Arrow `List<Float32>` (item field `"item"`, non-null child) bit-exact.

- [ ] **Step 1: Write the failing fixture test** `tests/iceberg_inline_vector.rs`:

```rust
//! Inline (hot-tier) round-trip for a vector(N) column: write via inline_append,
//! read back via inline_live_batch, assert bit-exact f32 reconstruction.
//! loom_fixture_test (Postgres; no DuckDB).

use std::sync::Arc;

use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_array::{Float32Array, Int64Array, ListArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    ColumnSpec, DatasetId, EventType, LineageEvent, RunId, SnapshotId, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_inline::inline_append;

fn columns() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec { name: "id".into(), ty: "long".into(), nullable: false },
        ColumnSpec { name: "embedding".into(), ty: "vector(4)".into(), nullable: false },
    ]
}

fn batch(rows: &[(i64, [f32; 4])]) -> RecordBatch {
    let item = Arc::new(Field::new("item", DataType::Float32, false));
    let mut lb = ListBuilder::new(Float32Builder::new()).with_field(item.clone());
    let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
    for (_, e) in rows {
        lb.values().append_slice(e);
        lb.append(true);
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("embedding", DataType::List(item), false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(ids)), Arc::new(lb.finish())],
    )
    .expect("batch")
}

fn lineage(run: RunId, table: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(table).dataset_ref()],
        payload: serde_json::json!({ "source": "test" }),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_vector_round_trips_bit_exact() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let table = TableRef { schema: "wh".into(), name: "docs".into() };
    let rows: &[(i64, [f32; 4])] =
        &[(1, [0.1, 0.2, 0.3, 0.4]), (2, [-1.5, 0.0, 3.25, 9.0])];

    let snap = inline_append(
        &pool,
        &table,
        &columns(),
        &batch(rows),
        lineage(RunId(uuid::Uuid::new_v4()), &table),
        None,
    )
    .await
    .expect("inline_append vector rows");

    let cat = IcebergCatalog::new(pool.clone());
    let (_tid, ids, out) = cat
        .inline_live_batch(&table, snap)
        .await
        .expect("inline_live_batch")
        .expect("live rows exist");

    assert_eq!(ids.len(), 2, "two live inline rows");

    let id_col = out.column(0).as_any().downcast_ref::<Int64Array>().expect("id Int64");
    assert_eq!((0..2).map(|i| id_col.value(i)).collect::<Vec<_>>(), vec![1, 2]);

    let v = out.column(1).as_any().downcast_ref::<ListArray>().expect("embedding List");
    for (i, (_, expect)) in rows.iter().enumerate() {
        let row = v.value(i);
        let f = row.as_any().downcast_ref::<Float32Array>().expect("child Float32");
        assert_eq!(f.values(), expect, "row {i} vector bit-exact");
    }
}
```

- [ ] **Step 2: Wire the BUCK target.** In `src/control-plane/postgres/BUCK`, after the `iceberg-inline` target, add:

```python
loom_fixture_test(
    name = "iceberg-inline-vector",
    crate = "iceberg_inline_vector",
    srcs = ["tests/iceberg_inline_vector.rs"],
    crate_root = "tests/iceberg_inline_vector.rs",
    deps = [
        ":postgres",
        "//src/control-plane/core:core",
        "//third-party:arrow-array",
        "//third-party:arrow-schema",
        "//third-party:serde_json",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `buck2 test //src/control-plane/postgres:iceberg-inline-vector > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|inline: unsupported|no iceberg type|error" /tmp/t.log`
Expected: FAIL — `inline_append` errors with `inline: unsupported column type "vector(4)"` from `cell_from_arrow` (column projection now succeeds via Task 1, but the cell encode has no vector arm yet).

- [ ] **Step 4: Add the `Cell::Vec` variant** in `iceberg_inline.rs` (inside `enum Cell`, after `Ts`):

```rust
    /// A dense f32 vector cell, bound as Postgres `real[]` / decoded from it.
    /// `None` is SQL NULL.
    Vec(Option<Vec<f32>>),
```

- [ ] **Step 5: Add the `cell_from_arrow` vector arm.** First extend the arrow_array import (line 15-18) to include `Float32Array` and `ListArray`:

```rust
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array,
    Int64Array, ListArray, RecordBatch, StringArray, TimestampMicrosecondArray,
};
```

Then add the arm immediately before `other =>` in `cell_from_arrow`:

```rust
        v if v.starts_with("vector(") => Cell::Vec((!null).then(|| {
            let list = dc!(ListArray);
            let elems = list.value(row);
            let f32s = elems
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("inline vector child is Float32");
            f32s.values().to_vec()
        })),
```

- [ ] **Step 6: Add the `bind_cell` vector arm** (in the `match cell` block):

```rust
        Cell::Vec(v) => q.bind(v.clone()),
```

- [ ] **Step 7: Add the `arrow_field` vector arm** immediately before `other =>`:

```rust
        v if v.starts_with("vector(") => {
            DataType::List(Arc::new(Field::new("item", DataType::Float32, false)))
        }
```

- [ ] **Step 8: Add the `column_array` vector arm** immediately before `other =>`:

```rust
        v if v.starts_with("vector(") => {
            use arrow_array::builder::{Float32Builder, ListBuilder};
            let item = Arc::new(Field::new("item", DataType::Float32, false));
            let mut b = ListBuilder::new(Float32Builder::new()).with_field(item);
            for v in get!(Vec<f32>) {
                match v {
                    Some(xs) => {
                        b.values().append_slice(&xs);
                        b.append(true);
                    }
                    None => b.append(false),
                }
            }
            Arc::new(b.finish())
        }
```

- [ ] **Step 9: Run the test + clippy to verify they pass**

Run: `buck2 test //src/control-plane/postgres:iceberg-inline-vector > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: `Tests finished: Pass 1. Fail 0`.
Run: `buck2 build '//src/control-plane/postgres:postgres[clippy.txt]' --show-output 2>/dev/null | awk '{print $2}' | xargs wc -c`
Expected: `0`.

- [ ] **Step 10: Commit**

```bash
git add src/control-plane/postgres/src/iceberg_inline.rs src/control-plane/postgres/tests/iceberg_inline_vector.rs src/control-plane/postgres/BUCK
git commit -m "feat(postgres): inline-tier vector(N) cell encode/decode as real[] + round-trip test"
```

---

### Task 3: Hot-delta reader `real[]` switch + Acceptance 4 (cold∪hot merge)

**Files:**
- Modify: `src/control-plane/postgres/src/vector_index.rs:311-327` (`inline_delta_batch` vector decode)
- Modify: `src/services/engine-serving/tests/vector_search.rs` (header comment + two new tests)

**Interfaces:**
- Consumes: Task 2's inline vector write (so `land(..., inline_byte_limit = usize::MAX, ...)` lands a vector row inline).
- Consumes: `pub async fn inline_delta_batch(pool, table, born_after: i64, at: i64) -> Result<Option<RecordBatch>>` (unchanged signature; vector decode switches jsonb → `real[]`).
- Consumes: `engine_serving::vector_search(catalog, pool, table, column, query: &[f32], k) -> Result<RecordBatch>` and the test helpers already in `vector_search.rs` (`columns`, `ipc_body`, `land`, `seed_and_build`, `ids`, `distances`, `lineage_evt`).

- [ ] **Step 1: Write the failing Acceptance 4 tests.** In `src/services/engine-serving/tests/vector_search.rs`, update the header doc-comment (lines 1-5) to:

```rust
//! k-NN over the cold Puffin index merged with the hot inline delta. Cold-only
//! (knn_cold_exact_*), no-index error, AND the cold∪hot merge (knn_cold_hot_merge_*):
//! a vector row landed inline AFTER the index's covered snapshot S is found in the
//! hot delta and merged exactly once. Cosine and L2 both verified.
```

Then append two tests:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn knn_cold_hot_merge_cosine() {
    let fx = PgFixture::start();
    let (_cp_init, db) = fx.fresh_db().await;
    let table = TableRef { schema: "wh".into(), name: "docs".into() };

    // Cold rows 1-4 + index built at S (covered_snapshot = S).
    let (catalog, pool, _cp, _wh) = seed_and_build(&fx, &db, Metric::Cosine).await;

    // Land row 5 INLINE (born after S): the unique nearest to the query, living
    // only in the hot delta. inline_byte_limit = usize::MAX forces the inline path.
    let run = RunId(uuid::Uuid::new_v4());
    let inline: &[(i64, [f32; 4])] = &[(5, [0.95, 0.05, 0.0, 0.0])];
    land(
        &pool, &catalog, &table, &columns(), &ipc_body(inline),
        usize::MAX, i64::MAX, lineage_evt(run, &table),
    )
    .await
    .expect("land inline row 5");

    // Query close to [1,0,0,0]; row 5 is strictly nearer than the cold row 1.
    let batch = engine_serving::vector_search(
        &catalog, &pool, &table, "embedding", &[0.9_f32, 0.1, 0.0, 0.0], 2,
    )
    .await
    .expect("vector_search cold+hot cosine");

    assert_eq!(batch.num_rows(), 2, "k=2");
    let id_vec = ids(&batch);
    assert_eq!(id_vec[0], 5, "hot inline row is the nearest (no miss)");
    assert_eq!(id_vec[1], 1, "cold row 1 is second (merge spans both tiers)");
    assert_eq!(id_vec.iter().filter(|&&x| x == 5).count(), 1, "inline row counted once");
    let dists = distances(&batch);
    assert!(dists[0] <= dists[1], "distances ascending");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn knn_cold_hot_merge_l2() {
    let fx = PgFixture::start();
    let (_cp_init, db) = fx.fresh_db().await;
    let table = TableRef { schema: "wh".into(), name: "docs".into() };

    let (catalog, pool, _cp, _wh) = seed_and_build(&fx, &db, Metric::L2).await;

    let run = RunId(uuid::Uuid::new_v4());
    let inline: &[(i64, [f32; 4])] = &[(5, [0.95, 0.05, 0.0, 0.0])];
    land(
        &pool, &catalog, &table, &columns(), &ipc_body(inline),
        usize::MAX, i64::MAX, lineage_evt(run, &table),
    )
    .await
    .expect("land inline row 5");

    // L2 nearest to [0.9,0.1,0,0]: row 5 (||·||²=0.005) beats cold row 1 (0.02).
    let batch = engine_serving::vector_search(
        &catalog, &pool, &table, "embedding", &[0.9_f32, 0.1, 0.0, 0.0], 2,
    )
    .await
    .expect("vector_search cold+hot l2");

    assert_eq!(batch.num_rows(), 2, "k=2");
    let id_vec = ids(&batch);
    assert_eq!(id_vec[0], 5, "hot inline row is the nearest (no miss)");
    assert_eq!(id_vec[1], 1, "cold row 1 is second");
    assert_eq!(id_vec.iter().filter(|&&x| x == 5).count(), 1, "inline row counted once");
    let dists = distances(&batch);
    assert!(dists[0] <= dists[1], "distances ascending");
}
```

- [ ] **Step 2: Run the new tests to verify they fail**

Run: `buck2 test //src/services/engine-serving:vector-search > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|not a JSON array|error returned from database" /tmp/t.log`
Expected: the two `knn_cold_hot_merge_*` tests FAIL — `inline_delta_batch` reads the new `real[]` column with the jsonb decoder and errors (`inline vector column is not a JSON array`, or a sqlx decode error).

- [ ] **Step 3: Switch the `inline_delta_batch` vector decode to `real[]`.** In `vector_index.rs`, replace the jsonb block (the `let json_val …` through the `.collect::<Result<Vec<_>>>()?;`, lines ~311-327) with:

```rust
        // Vector: stored as a Postgres real[] (native f32) in the inline table.
        let floats: Vec<f32> = r.try_get(1).map_err(backend)?;
```

Leave the surrounding lines (`let id_val: i64 = r.try_get(0)...`, and `vec_builder.values().append_slice(&floats); vec_builder.append(true);`) unchanged.

- [ ] **Step 4: Run the full vector-search + reader tests to verify they pass**

Run: `buck2 test //src/services/engine-serving:vector-search //src/control-plane/postgres:vector-index-build //src/control-plane/postgres:vector-index-mirror > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: `Tests finished: Pass … Fail 0` (the two cold-only tests + the two merge tests + the postgres vector tests all pass).
Run: `buck2 build '//src/control-plane/postgres:postgres[clippy.txt]' '//src/services/engine-serving:engine-serving[clippy.txt]' --show-output 2>/dev/null | awk '{print $2}' | xargs wc -c`
Expected: `0` for each (clippy clean — confirm `serde_json` is still used elsewhere in `vector_index.rs`; it is, in the lineage `json!` payload, so its import stays).

- [ ] **Step 5: Commit**

```bash
git add src/control-plane/postgres/src/vector_index.rs src/services/engine-serving/tests/vector_search.rs
git commit -m "feat(postgres): inline_delta_batch reads vectors from real[]; Acceptance 4 cold-hot merge"
```

---

### Task 4: Full regression sweep + close the register item

**Files:**
- Modify: `docs/FUTURE.md` (close `fut-inline-vector-hot-delta`)

- [ ] **Step 1: Run the whole first-party test suite (Acceptance 6)**

Run: `buck2 test //src/... > /tmp/sweep.log 2>&1; grep -E "Tests finished|FAIL" /tmp/sweep.log`
Expected: `Tests finished: Pass … Fail 0. … Build failure 0`. (If anything fails, fix before continuing — do not edit docs over a red sweep.)

- [ ] **Step 2: Close the FUTURE item.** In `docs/FUTURE.md`, change the `fut-inline-vector-hot-delta` line's checkbox to `[x]`, set `status:promoted` and `pr:#<this PR>` (fill the real PR number after opening it), and replace the body's "Follow-up:" sentence with a one-line resolution noting inline vectors now store as `real[]` and the hot delta + Acceptance 4 are live. Keep the cross-links.

- [ ] **Step 3: Validate the registers + lint**

Run: `bash tools/docs.sh validate 2>&1 | tail -3`
Expected: `docs.sh validate: OK`.
Run: `buck2 run //tools:prek -- run --files docs/FUTURE.md > /tmp/prek.log 2>&1; grep -E "Passed|Failed" /tmp/prek.log`
Expected: all `Passed`.

- [ ] **Step 4: Commit**

```bash
git add docs/FUTURE.md
git commit -m "docs(future): close fut-inline-vector-hot-delta (inline vectors + hot delta)"
```

- [ ] **Step 5: Push and open the stacked PR** (targets `work/road-puffin-vector-index` while #207 is open, or `main` once #207 merges). The pre-push hook runs the full build/test; allow time for it.

```bash
git push -u origin work/inline-vector-hot-delta
```

Then open the PR with `gh pr create` (base = `work/road-puffin-vector-index`), and backfill the real PR number into `docs/FUTURE.md` (Task 4 Step 2) if it was left as a placeholder.

---

## Self-Review

**Spec coverage:**
- Storage encoding `real[]` → Task 1 (`pg_type_for`) + Task 2 (cell encode/decode). ✓
- Shared `mirror_column_type` de-dup → Task 1 (cold + inline both call it). ✓
- Inline write/read (`cell_from_arrow`/`bind_cell`/`arrow_field`/`column_array`) → Task 2. ✓
- Hot-delta reader switch → Task 3 Step 3. ✓
- Acceptance 4 (cold∪hot, Cosine + L2, no double-count, no miss, row born after S) → Task 3 Steps 1-4. ✓
- Inline round-trip test (Testing 1) → Task 2. ✓
- Regression / Acceptance 6 (`buck2 test //src/...` green) → Task 4 Step 1. ✓
- Register close → Task 4 Step 2. ✓
- Out-of-scope items (order-bug, non-i64 inline identity, pgvector/ANN) → not touched; the merge seam, engine path, and `.sqlx` cache are untouched. ✓

**Placeholder scan:** No "TBD"/"handle edge cases"/"similar to" — every code step shows the code. The only deferred value is the real PR number in Task 4, explicitly backfilled in Task 4 Step 5.

**Type consistency:** `mirror_column_type(&str) -> Option<String>` used identically in Tasks 1/2. `Cell::Vec(Option<Vec<f32>>)` defined in Task 2 Step 4, bound in Step 6, produced by Step 5. `inline_append(... flush_threshold: Option<i64>)` and `inline_live_batch(... at: SnapshotId)` match the verified signatures. The `"item"`/`Float32`/non-null `List` field shape is identical across `arrow_field`, `column_array`, the round-trip test, and `inline_delta_batch`.
