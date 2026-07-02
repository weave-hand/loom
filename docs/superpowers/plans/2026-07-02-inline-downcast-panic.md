# iss-inline-downcast-panic Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A decoded IPC batch whose arrow type mismatches the declared logical
column is rejected with `ControlPlaneError::Validation` instead of panicking
the write path.

**Architecture:** `iceberg_inline.rs::cell_from_arrow`'s `dc!` macro expands to
`.expect("inline arrow downcast")` — a production panic on caller-shaped data
that slips past the `clippy::expect_used` gate because it lives in a
`macro_rules!` expansion. Make the macro fallible (`ok_or_else` + `?`) and
hoist each downcast out of the `(!null).then(|| …)` closures (a `?` cannot
cross a closure boundary). Deliberate strictness change: for the **scalar** arms, a null cell
in a mistyped column previously never ran the downcast and silently succeeded;
after the hoist it errors too. (The vector arm keeps its `if null` short-circuit
before the downcast — unchanged.) Scope excludes reclassifying the existing
`Backend` arms (vector child, unsupported type) — that's
`road-cp-adapter-hygiene`.

**Tech Stack:** Rust, buck2, `loom_fixture_test`.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-07-02-pillar-idioms-audit-design.md`, Wave 0 defect 2 (`iss-inline-downcast-panic`).
- Tests are separate `rust_test` targets, never inline `#[cfg(test)]`.
- `Validation`, not `Backend`: the mismatch is caller-shaped data (spec's classification).
- Conventional-commit message format.

---

### Task 1: Fallible downcast in cell_from_arrow

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_inline.rs:122-170` (`cell_from_arrow`), `:210-217` (the `inline_append` CONTRACT doc comment)
- Modify: `src/control-plane/postgres/BUCK:600-614` (the `iceberg-inline` target's `deps`)
- Test: `src/control-plane/postgres/tests/iceberg_inline.rs` (target `//src/control-plane/postgres:iceberg-inline`)

**Interfaces:**
- Consumes: `pub async fn inline_append(pool, table, columns, batch, lineage, flush_threshold) -> Result<SnapshotId>` (`control_plane_postgres::iceberg_inline`, already public); `ControlPlaneError::Validation(String)`.
- Produces: no API change — `cell_from_arrow` stays `fn(...) -> Result<Cell>`; only the failure mode of a mistyped column changes (panic → `Err(Validation)`).

- [ ] **Step 1: Write the failing test**

Append to `src/control-plane/postgres/tests/iceberg_inline.rs`:

```rust
/// A wire batch whose arrow type mismatches the declared logical column must be
/// rejected with Validation — NOT panic the write path. Guards
/// iss-inline-downcast-panic (the dc! macro's .expect on caller-shaped data).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_append_rejects_mistyped_batch_without_panicking() {
    use control_plane_core::{ColumnSpec, ControlPlaneError, EventType, LineageEvent, RunId, TableRef};
    use std::sync::Arc;

    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    // Declared logical schema says "long" (Int64), but the batch carries Int32.
    // Nothing in inline_append inspects the batch's arrow types before the
    // per-row cell loop, so the mismatch reaches cell_from_arrow.
    let columns = vec![ColumnSpec {
        name: "id".to_string(),
        ty: "long".to_string(),
        nullable: false,
    }];
    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int32,
        false,
    )]));
    let batch = arrow_array::RecordBatch::try_new(
        schema,
        vec![Arc::new(arrow_array::Int32Array::from(vec![1, 2]))],
    )
    .expect("test batch");
    let lineage = LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![],
        payload: serde_json::json!({ "source": "test" }),
    };

    let err = control_plane_postgres::iceberg_inline::inline_append(
        &pool,
        &TableRef {
            schema: "sales".to_string(),
            name: "orders".to_string(),
        },
        &columns,
        &batch,
        lineage,
        None,
    )
    .await
    .expect_err("mistyped batch must be rejected, not landed");
    assert!(
        matches!(err, ControlPlaneError::Validation(_)),
        "expected Validation, got: {err:?}"
    );
}
```

Add the missing deps to the `iceberg-inline` target in
`src/control-plane/postgres/BUCK` (it currently has parquet/postgres/core/
bytes/sqlx/tokio/uuid):

```python
        "//third-party:arrow-array",
        "//third-party:arrow-schema",
        "//third-party:serde_json",
        "//third-party:time",
```

- [ ] **Step 2: Run test to verify it fails**

Run: `buck2 test //src/control-plane/postgres:iceberg-inline > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL|panic" /tmp/t1.log`
Expected: FAIL — the test *panics* with `inline arrow downcast` (the defect), so `expect_err` is never reached.

- [ ] **Step 3: Make the macro fallible and hoist downcasts out of closures**

In `src/control-plane/postgres/src/iceberg_inline.rs`, replace `cell_from_arrow`'s
macro and match arms (lines 123-170):

```rust
/// Pull cell `(col, row)` out of an arrow batch, typed per the logical column.
/// A batch whose arrow type mismatches the declared logical column is rejected
/// with `Validation` (caller-shaped wire data), never a panic.
fn cell_from_arrow(batch: &RecordBatch, col: usize, row: usize, logical: &str) -> Result<Cell> {
    let a = batch.column(col);
    let null = a.is_null(row);
    macro_rules! dc {
        ($ty:ty) => {
            a.as_any().downcast_ref::<$ty>().ok_or_else(|| {
                ControlPlaneError::Validation(format!(
                    "inline: column {col} is not the declared {logical} (expected {})",
                    stringify!($ty)
                ))
            })?
        };
    }
    Ok(match logical {
        "integer" => {
            let arr = dc!(Int32Array);
            Cell::I32((!null).then(|| arr.value(row)))
        }
        "long" => {
            let arr = dc!(Int64Array);
            Cell::I64((!null).then(|| arr.value(row)))
        }
        "double" => {
            let arr = dc!(Float64Array);
            Cell::F64((!null).then(|| arr.value(row)))
        }
        "boolean" => {
            let arr = dc!(BooleanArray);
            Cell::Bool((!null).then(|| arr.value(row)))
        }
        "string" => {
            let arr = dc!(StringArray);
            Cell::Str((!null).then(|| arr.value(row).to_string()))
        }
        "date" => {
            let arr = dc!(Date32Array);
            Cell::Date((!null).then(|| {
                time::macros::date!(1970 - 01 - 01)
                    + time::Duration::days(arr.value(row) as i64)
            }))
        }
        "timestamp" => {
            let arr = dc!(TimestampMicrosecondArray);
            Cell::Ts((!null).then(|| {
                let micros = arr.value(row);
                let odt =
                    time::OffsetDateTime::from_unix_timestamp_nanos(micros as i128 * 1_000)
                        .unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
                time::PrimitiveDateTime::new(odt.date(), odt.time())
            }))
        }
        v if v.starts_with("vector(") => {
            if null {
                Cell::Vec(None)
            } else {
                let list = dc!(ListArray);
                let elems = list.value(row);
                let f32s = elems
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| {
                        ControlPlaneError::Backend("inline vector child is not Float32".into())
                    })?;
                Cell::Vec(Some(f32s.values().to_vec()))
            }
        }
        other => {
            return Err(ControlPlaneError::Backend(
                format!("inline: unsupported column type {other:?}").into(),
            ));
        }
    })
}
```

Preserve any existing `#[expect(...)]` attributes on the fn (e.g. the
`cast_possible_wrap`/`cast_precision_loss` family) exactly as they are — only
the body shown changes. If the `date`/`timestamp` casts carried per-expression
`#[expect]`s, keep them attached to the same expressions.

Also update the CONTRACT paragraph on `inline_append` (line ~215) — replace the
sentence "Both come from the landing's schema, so they agree by construction."
with:

```rust
/// come from the landing's schema, so they agree by construction; a batch that
/// violates the contract (arrow type != declared logical type) is rejected with
/// `Validation`, never a panic.
```

- [ ] **Step 4: Run the inline test suites**

Run: `buck2 test //src/control-plane/postgres:iceberg-inline //src/control-plane/postgres:iceberg-inline-vector //src/control-plane/postgres:iceberg-landing //src/control-plane/postgres:iceberg-flush > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t2.log`
Expected: PASS (all pre-existing inline/landing/flush behavior unchanged for well-typed batches).

- [ ] **Step 5: Clippy on the crate**

Run: `buck2 build '//src/control-plane/postgres:postgres[clippy.txt]' > /tmp/c1.log 2>&1` then check the clippy.txt artifact is empty.
Expected: clean (the `.expect` is gone, so no new waivers needed).

- [ ] **Step 6: Commit**

```bash
git add src/control-plane/postgres/src/iceberg_inline.rs src/control-plane/postgres/tests/iceberg_inline.rs src/control-plane/postgres/BUCK docs/superpowers/plans/2026-07-02-inline-downcast-panic.md
git commit -m "fix(iceberg): reject mistyped inline batches instead of panicking

cell_from_arrow's dc! macro expanded to .expect(), a production panic on
the inline write path reachable by any IPC batch whose arrow type
mismatches the declared logical column (align_to_columns matches by name
only) — and invisible to the clippy expect_used gate because it lives in
a macro_rules! body. The macro is now fallible (Validation, naming the
column and expected type) with downcasts hoisted out of the null-guard
closures; a mistyped scalar column now errors even when its cells are
null (fail-loud beats silent accept).

Closes iss-inline-downcast-panic."
```

(Register close commits separately with the PR number at PR time.)
