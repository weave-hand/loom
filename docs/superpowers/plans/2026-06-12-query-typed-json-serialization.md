# Query-Path Typed JSON Serialization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the governed read path return typed objects on the wire, each value rendered by its property's logical type (the `core` vocabulary), with `Long` → JSON string for >2⁵³ precision safety.

**Architecture:** `control-plane-core` gains a pure `JsonRepr` classification (`Long` → `NumericString`, `Date` → `IsoDate`, …). `query-api`'s `SqlValue` stops collapsing `Double`/`Date`/`Timestamp` to debug strings; `read_object` returns `ObjectRows` carrying each projected column's logical type; a pure `render` boundary emits a `{ "objects": [...] }` typed-object envelope.

**Tech Stack:** Rust 2024, buck2, `serde_json`, the `time` crate (ISO-8601 formatting via the already-enabled `formatting`/`macros` features), embedded `duckdb` 1.10503.1.

**Design:** `docs/superpowers/specs/2026-06-12-query-typed-json-serialization-design.md`

---

## Context for the implementer (read before starting)

- **Tests are integration `rust_test`/`loom_fixture_test` targets only** — never inline `#[cfg(test)]` (a prek hook fails the build otherwise). Fixture tests that boot postgres/duckdb MUST use `loom_fixture_test(... duckdb = True)` loaded from `//src/control-plane/postgres:defs.bzl`, never a bare `rust_test`.
- **Run tests** with `buck2 test //src/...` (redirect to a file and grep — never pipe to `tail`): `buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`. To run one target: `buck2 test //src/services/query-api:render > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`.
- **rustfmt is CHECK-ONLY.** Before committing any `.rs`, run `buck2 run //tools:rustfmt -- <files you touched>` and apply the result, or the commit/CI loops.
- **clippy:** `tools/clippy-all.sh` must be clean.
- **Commits:** Conventional Commits, body ending exactly `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`. Do NOT use `--no-verify`. Do NOT switch branches (you are on `feat/query-typed-json-serialization`; confirm with `git branch --show-current` before and after).
- **The `time` version pin is load-bearing:** use `=0.3.47` everywhere. `time 0.3.48` adds a trait impl that conflicts with `sqlx-core 0.9`'s blanket `From<T> for Json<T>` under Rust 2024 orphan rules and breaks the postgres build.
- **The arrow buck alias is `//third-party:arrow`** (not `arrow-58`, which has restricted visibility).
- `serde_json` is already a `core` dep and a `query-api` dep — no new wiring for it.

---

## Task 1: `core` — `JsonRepr` classification + resolver

**Files:**
- Modify: `src/control-plane/core/src/logical_type.rs`
- Modify: `src/control-plane/core/src/lib.rs:22` (re-export)
- Test: `src/control-plane/core/tests/logical_type.rs` (extend; target `//src/control-plane/core:logical-type` already exists, no BUCK change)

This is pure logic, no new deps. `JsonRepr` is a classification enum, NOT a `serde_json` value — `core` stays JSON-free.

- [ ] **Step 1: Write the failing tests** — append to `src/control-plane/core/tests/logical_type.rs`:

```rust
use control_plane_core::{JsonRepr, json_repr_of};

#[test]
fn base_types_classify_to_json_repr() {
    assert_eq!(BaseType::Integer.json_repr(), JsonRepr::Number);
    assert_eq!(BaseType::Double.json_repr(), JsonRepr::Number);
    assert_eq!(BaseType::Long.json_repr(), JsonRepr::NumericString);
    assert_eq!(BaseType::Boolean.json_repr(), JsonRepr::Bool);
    assert_eq!(BaseType::String.json_repr(), JsonRepr::PlainString);
    assert_eq!(BaseType::Date.json_repr(), JsonRepr::IsoDate);
    assert_eq!(BaseType::Timestamp.json_repr(), JsonRepr::IsoTimestamp);
}

#[test]
fn json_repr_of_resolves_names_and_aliases() {
    assert_eq!(json_repr_of("Long"), Ok(JsonRepr::NumericString));
    assert_eq!(json_repr_of("integer"), Ok(JsonRepr::Number));
    // semantic alias -> base -> repr, case-insensitively
    assert_eq!(json_repr_of("EmailAddress"), Ok(JsonRepr::PlainString));
    assert_eq!(json_repr_of("  timestamp "), Ok(JsonRepr::IsoTimestamp));
}

#[test]
fn json_repr_of_errors_on_unknown_type() {
    assert_eq!(json_repr_of("Money"), Err(UnknownLogicalType("Money".into())));
}
```

- [ ] **Step 2: Run, verify it fails to compile** — `buck2 test //src/control-plane/core:logical-type > /tmp/t.log 2>&1; grep -E "error\[|Tests finished|FAIL" /tmp/t.log`. Expected: compile error (`JsonRepr`/`json_repr`/`json_repr_of` not found).

- [ ] **Step 3: Implement in `src/control-plane/core/src/logical_type.rs`.** Update the module doc note (lines 7–9) to say the wire-encoding axis now lives here, and add the API:

Replace the doc lines 7–9:
```rust
//! NOTE: this vocabulary also classifies how each type renders on the JSON wire
//! (see `JsonRepr` / `json_repr_of`): Date/Timestamp -> ISO-8601 strings, Long ->
//! JSON string to keep int64 precision past 2^53. That is a classification only;
//! the actual serde_json construction lives at the query-api boundary where the
//! scalar values are, so core stays JSON-free.
```

Add after the `BaseType` enum / `impl BaseType` block:
```rust
/// How a logical type renders on the JSON wire. A classification only — the actual
/// `serde_json` construction happens where the scalar values live (query-api), so
/// `core` needs no JSON dependency.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JsonRepr {
    /// Integer, Double -> JSON number.
    Number,
    /// Long -> JSON string (int64 exceeds JSON's 2^53 safe-integer range).
    NumericString,
    /// Boolean -> JSON bool.
    Bool,
    /// String (+ aliases) -> JSON string.
    PlainString,
    /// Date -> ISO-8601 date string (YYYY-MM-DD).
    IsoDate,
    /// Timestamp -> ISO-8601 datetime string (YYYY-MM-DDThh:mm:ss).
    IsoTimestamp,
}
```

Add a method inside the existing `impl BaseType { ... }` block (next to `physical_affinity`):
```rust
    /// How a value of this base type renders on the JSON wire.
    pub fn json_repr(self) -> JsonRepr {
        match self {
            BaseType::Integer | BaseType::Double => JsonRepr::Number,
            BaseType::Long => JsonRepr::NumericString,
            BaseType::Boolean => JsonRepr::Bool,
            BaseType::String => JsonRepr::PlainString,
            BaseType::Date => JsonRepr::IsoDate,
            BaseType::Timestamp => JsonRepr::IsoTimestamp,
        }
    }
```

Add a free function after `satisfies`:
```rust
/// The JSON wire rendering for a logical type name (base or alias, case-insensitively).
/// `Err(UnknownLogicalType)` if loom does not recognize the type — callers fall back
/// to a best-effort natural rendering rather than failing a permitted read.
pub fn json_repr_of(logical_ty: &str) -> Result<JsonRepr, UnknownLogicalType> {
    resolve_logical(logical_ty)
        .map(BaseType::json_repr)
        .ok_or_else(|| UnknownLogicalType(logical_ty.trim().to_string()))
}
```

- [ ] **Step 4: Re-export** — in `src/control-plane/core/src/lib.rs:22` extend the `logical_type` re-export:
```rust
pub use logical_type::{
    BaseType, JsonRepr, UnknownLogicalType, json_repr_of, resolve_logical, satisfies,
};
```

- [ ] **Step 5: Run tests** — `buck2 test //src/control-plane/core:logical-type > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`. Expected: PASS (all logical-type tests).

- [ ] **Step 6: rustfmt + clippy** — `buck2 run //tools:rustfmt -- src/control-plane/core/src/logical_type.rs src/control-plane/core/src/lib.rs src/control-plane/core/tests/logical_type.rs` and apply; then `tools/clippy-all.sh` clean.

- [ ] **Step 7: Commit**
```bash
git add src/control-plane/core/src/logical_type.rs src/control-plane/core/src/lib.rs src/control-plane/core/tests/logical_type.rs
git commit -m "feat(core): JsonRepr classification + json_repr_of (logical->JSON axis)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: `query-api` serving — `SqlValue` fidelity (Double/Date/Timestamp)

**Files:**
- Modify: `src/services/query-api/Cargo.toml` (add `time`)
- Modify: `src/services/query-api/BUCK:9-18` (lib `deps`: add `//third-party:time`) and add a new fixture-test target
- Modify: `src/services/query-api/src/serving.rs`
- Test: `src/services/query-api/tests/serving_types.rs` (new, fixture)
- Generated: `Cargo.lock`, possibly `third-party/BUCK`

Today `from_duck` (serving.rs:215) collapses every non-int/text/bool DuckDB `Value` to `Text(format!("{other:?}"))`, destroying `Double`/`Date`/`Timestamp` fidelity. This task fixes that. The duckdb `Value` variants (confirmed against duckdb 1.10503.1): `Double(f64)`, `Float(f32)`, `Date32(i32)` (days since 1970-01-01), `Timestamp(TimeUnit, i64)` where `TimeUnit ∈ {Second, Millisecond, Microsecond, Nanosecond}`.

- [ ] **Step 1: Add the `time` dependency** — in `src/services/query-api/Cargo.toml` under `[dependencies]`:
```toml
# Pinned =0.3.47: time 0.3.48 adds a trait impl that conflicts with sqlx-core 0.9's
# blanket From<T> for Json<T> under Rust 2024 orphan rules, breaking the postgres
# build (control-plane-postgres is a transitive dep). `formatting`/`macros` features
# are already enabled workspace-wide; they render Date/Timestamp as ISO-8601.
time = { version = "=0.3.47", features = ["formatting", "macros"] }
```

- [ ] **Step 2: Refresh the lockfile + regenerate buck rules** — adding a manifest dep requires it or the `reindeer-check` hook fails:
```bash
buck2 run //tools:reindeer -- update
./tools/buckify.sh
git diff --stat Cargo.lock third-party/BUCK
```
Expected: `Cargo.lock` changes (query-api now depends on `time`); `third-party/BUCK` likely unchanged (the `time` features are already union-enabled). If `third-party/BUCK` did change, that is fine — commit it in Step 9.

- [ ] **Step 3: Add `//third-party:time` to the query-api library deps** — in `src/services/query-api/BUCK`, the `rust_library(name = "query-api", ...)` `deps` list (after `"//third-party:serde_json",`):
```python
        "//third-party:time",
```

- [ ] **Step 4: Write the failing fixture test** — `src/services/query-api/tests/serving_types.rs`. This proves real DuckDB temporal/float values survive `from_duck` as faithful typed `SqlValue`s (no data path needed — `EmbeddedDuckDb` runs data-free SELECTs after ATTACH):

```rust
//! Serving-layer type fidelity: real DuckDB DATE/TIMESTAMP/DOUBLE values come back
//! as faithful typed SqlValues, not Text(debug). The JSON rendering of these values
//! is covered by the pure render-matrix unit test; this proves the DuckDB -> SqlValue
//! decode against the real engine.

use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use query_api::serving::{EmbeddedDuckDb, ServingEngine, SqlValue};

#[tokio::test(flavor = "multi_thread")]
async fn duckdb_temporal_and_float_decode_faithfully() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;

    let eng = EmbeddedDuckDb::attach(fx.socket_path(), &db, writer.data_path())
        .await
        .unwrap();
    let rows = eng
        .fetch_rows(
            "SELECT DATE '2026-06-12' AS d, \
             TIMESTAMP '2026-06-12 14:09:42' AS ts, \
             CAST(3.5 AS DOUBLE) AS amt",
            &[],
        )
        .await
        .unwrap();

    assert_eq!(rows.columns, vec!["d", "ts", "amt"]);
    let r = &rows.rows[0];
    assert_eq!(
        r[0],
        SqlValue::Date(time::macros::date!(2026 - 06 - 12)),
        "DATE decodes to SqlValue::Date"
    );
    assert_eq!(
        r[1],
        SqlValue::Timestamp(time::macros::datetime!(2026 - 06 - 12 14:09:42)),
        "TIMESTAMP decodes to SqlValue::Timestamp"
    );
    assert_eq!(r[2], SqlValue::Double(3.5), "DOUBLE decodes to SqlValue::Double");
}
```

Wire the target in `src/services/query-api/BUCK` (after the `serving-engine` target):
```python
loom_fixture_test(
    name = "serving-types",
    crate = "serving_types",
    srcs = ["tests/serving_types.rs"],
    crate_root = "tests/serving_types.rs",
    duckdb = True,
    deps = [
        ":query-api",
        "//src/control-plane/postgres:postgres",
        "//third-party:time",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 5: Run, verify it fails** — `buck2 test //src/services/query-api:serving-types > /tmp/t.log 2>&1; grep -E "assert|Tests finished|FAIL|panicked" /tmp/t.log`. Expected: FAIL — `r[0]`/`r[1]` are `SqlValue::Text("Date32(...)")` / `Text("Timestamp(...)")` (the current lossy behavior); `amt` is `Text("Double(3.5)")`.

- [ ] **Step 6: Extend `SqlValue` and fix the conversions** in `src/services/query-api/src/serving.rs`.

Extend the enum (serving.rs:9-15):
```rust
#[derive(Clone, Debug, PartialEq)]
pub enum SqlValue {
    Text(String),
    Int(i64),
    Bool(bool),
    Double(f64),
    Date(time::Date),
    Timestamp(time::PrimitiveDateTime),
    Null,
}
```

Replace `from_duck` (serving.rs:215-227):
```rust
fn from_duck(v: duckdb::types::Value) -> SqlValue {
    use duckdb::types::Value;
    match v {
        Value::Null => SqlValue::Null,
        Value::Boolean(b) => SqlValue::Bool(b),
        Value::TinyInt(i) => SqlValue::Int(i as i64),
        Value::SmallInt(i) => SqlValue::Int(i as i64),
        Value::Int(i) => SqlValue::Int(i as i64),
        Value::BigInt(i) => SqlValue::Int(i),
        Value::Float(f) => SqlValue::Double(f as f64),
        Value::Double(f) => SqlValue::Double(f),
        Value::Date32(days) => SqlValue::Date(date_from_epoch_days(days)),
        Value::Timestamp(unit, n) => SqlValue::Timestamp(timestamp_from_unit(unit, n)),
        Value::Text(s) => SqlValue::Text(s),
        // Decimal, Time64, HugeInt, lists/structs, etc. are not yet first-class; keep
        // the defensive debug fallback so an unmapped variant never panics a read.
        other => SqlValue::Text(format!("{other:?}")),
    }
}

/// Days since the Unix epoch -> a calendar date.
fn date_from_epoch_days(days: i32) -> time::Date {
    time::macros::date!(1970 - 01 - 01) + time::Duration::days(days as i64)
}

/// A DuckDB timestamp (unit + count since epoch) -> a wall-clock datetime.
fn timestamp_from_unit(unit: duckdb::types::TimeUnit, n: i64) -> time::PrimitiveDateTime {
    use duckdb::types::TimeUnit;
    let nanos: i128 = match unit {
        TimeUnit::Second => n as i128 * 1_000_000_000,
        TimeUnit::Millisecond => n as i128 * 1_000_000,
        TimeUnit::Microsecond => n as i128 * 1_000,
        TimeUnit::Nanosecond => n as i128,
    };
    let odt = time::OffsetDateTime::from_unix_timestamp_nanos(nanos)
        .unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
    time::PrimitiveDateTime::new(odt.date(), odt.time())
}
```

Add ISO-8601 formatting helpers (these are reused by `render.rs` in Task 4 — make them `pub(crate)`). Put near the top of serving.rs after the imports:
```rust
/// ISO-8601 date `YYYY-MM-DD`. Shared by the JSON renderer and the Quack literal path.
/// `let`-bind the macro output rather than annotating its type — the format-item type
/// name varies across `time` patch versions, but inference always works.
pub(crate) fn iso_date(d: &time::Date) -> String {
    let fmt = time::macros::format_description!("[year]-[month]-[day]");
    d.format(&fmt).unwrap_or_else(|_| d.to_string())
}

/// ISO-8601 datetime `YYYY-MM-DDThh:mm:ss` (no subseconds, no offset — bare timestamp).
pub(crate) fn iso_timestamp(ts: &time::PrimitiveDateTime) -> String {
    let fmt = time::macros::format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]");
    ts.format(&fmt).unwrap_or_else(|_| ts.to_string())
}
```

Make `to_duck` total (serving.rs:205-213) — add arms for the new variants (used only when such values are bound as params; not exercised this slice but required for totality):
```rust
fn to_duck(v: &SqlValue) -> duckdb::types::Value {
    use duckdb::types::{TimeUnit, Value};
    match v {
        SqlValue::Text(s) => Value::Text(s.clone()),
        SqlValue::Int(i) => Value::BigInt(*i),
        SqlValue::Bool(b) => Value::Boolean(*b),
        SqlValue::Double(f) => Value::Double(*f),
        SqlValue::Date(d) => {
            Value::Date32((*d - time::macros::date!(1970 - 01 - 01)).whole_days() as i32)
        }
        SqlValue::Timestamp(ts) => {
            let micros = (ts.assume_utc() - time::OffsetDateTime::UNIX_EPOCH)
                .whole_microseconds() as i64;
            Value::Timestamp(TimeUnit::Microsecond, micros)
        }
        SqlValue::Null => Value::Null,
    }
}
```

Make `render_literal` total (serving.rs:174-181) — add arms (Quack inline-params path):
```rust
fn render_literal(v: &SqlValue) -> String {
    match v {
        SqlValue::Int(n) => n.to_string(),
        SqlValue::Double(f) => f.to_string(),
        SqlValue::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        SqlValue::Null => "NULL".to_string(),
        SqlValue::Text(s) => format!("'{}'", sql_escape(s)),
        SqlValue::Date(d) => format!("DATE '{}'", iso_date(d)),
        SqlValue::Timestamp(ts) => format!("TIMESTAMP '{}'", iso_timestamp(ts)),
    }
}
```

- [ ] **Step 7: Run the fixture test** — `buck2 test //src/services/query-api:serving-types > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|panicked" /tmp/t.log`. Expected: PASS.

- [ ] **Step 8: rustfmt + clippy** — `buck2 run //tools:rustfmt -- src/services/query-api/src/serving.rs src/services/query-api/tests/serving_types.rs` and apply; `tools/clippy-all.sh` clean.

- [ ] **Step 9: Commit**
```bash
git add src/services/query-api/Cargo.toml src/services/query-api/BUCK src/services/query-api/src/serving.rs src/services/query-api/tests/serving_types.rs Cargo.lock third-party/BUCK
git commit -m "feat(query-api): faithful Double/Date/Timestamp in SqlValue (stop Text-debug collapse)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: `read_object` returns `ObjectRows` (rows + per-column logical type)

**Files:**
- Modify: `src/services/query-api/src/handler.rs`
- Modify: `src/services/query-api/tests/governed_read.rs` (only the read result type; field access is unchanged)
- Test: `src/services/query-api/tests/governed_read.rs` (existing target `//src/services/query-api:governed-read`)

The projected columns (after ACL deny/mask) are computed inside `read_object`, so that is where each surviving column's logical type must be captured. `ObjectRows` deliberately keeps `columns: Vec<String>` and `rows: Vec<Vec<SqlValue>>` (field-identical to today's `serving::Rows`) and ADDS `logical_types: Vec<String>` aligned to `columns` — so the governed-read oracle's existing `.columns` / `.rows` assertions keep compiling unchanged.

- [ ] **Step 1: Define `ObjectRows` and change the return type** in `src/services/query-api/src/handler.rs`.

Add near the top (after the `use` block):
```rust
/// A governed read result: rows plus, for each projected column, the ontology
/// property's logical type — the input the wire renderer needs to type each value.
/// `columns`, `logical_types`, and every row's cells are positionally aligned.
pub struct ObjectRows {
    pub columns: Vec<String>,
    pub logical_types: Vec<String>,
    pub rows: Vec<Vec<SqlValue>>,
}
```

Change the signature (handler.rs:47-51) `-> Result<Rows, QueryError>` to `-> Result<ObjectRows, QueryError>`.

Replace the final line (handler.rs:126) `Ok(deps.serving.fetch_rows(&sql, &params).await?)` with:
```rust
    let served = deps.serving.fetch_rows(&sql, &params).await?;
    // Logical type per projected column, in `allowed` order — which is the SELECT
    // order compile_select emits, hence the order of `served.rows`' cells. A column
    // with no matching property (cannot happen post-projection) maps to "" -> the
    // renderer's natural fallback.
    let logical_types: Vec<String> = allowed
        .iter()
        .map(|name| {
            object_type
                .properties
                .iter()
                .find(|p| &p.name == name)
                .map(|p| p.ty.clone())
                .unwrap_or_default()
        })
        .collect();
    Ok(ObjectRows {
        columns: allowed,
        logical_types,
        rows: served.rows,
    })
```

`allowed` is a `Vec<String>` built earlier (handler.rs:95-100) and only borrowed afterward (the eq-filter check and `compile_select(&allowed, ...)`), so moving it into `ObjectRows.columns` at the end is fine. Remove the now-unused `Rows` import from the `use crate::serving::...` line if clippy flags it (keep `SqlValue`, `ServingEngine`).

- [ ] **Step 2: Update `governed_read.rs` (field access unchanged).** The test binds `let rows = read_object(...)`; the type is inferred, and `rows.columns` / `rows.rows` still exist on `ObjectRows`. The only required change: the import on `governed_read.rs:13` does not name `Rows`, so nothing changes there. Verify by compiling. Add ONE strengthening assertion after the existing `assert_eq!(rows.columns, ...)` at governed_read.rs:188 to lock the new field:
```rust
    // logical types travel with the projection, aligned to columns.
    assert_eq!(rows.logical_types, vec!["Long".to_string(), "String".to_string()]);
```

- [ ] **Step 3: Run the governed-read oracle** — `buck2 test //src/services/query-api:governed-read > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|panicked" /tmp/t.log`. Expected: PASS (all ACL/projection/masking assertions still hold; the new `logical_types` assertion passes).

- [ ] **Step 4: Confirm the http layer still builds** — `http.rs` calls `read_object` then `rows_to_json(&rows)`. `rows_to_json` takes `&Rows`; it now receives `&ObjectRows`, so the lib will NOT compile yet. This is expected — Task 4 rewrites `http.rs`. To keep this task's commit green on its own, temporarily adapt the `http::get_object` call site minimally: the build must pass. Do this by having Task 3 leave `http.rs` compiling against `ObjectRows` via a stopgap: change `rows_to_json` to read `ObjectRows` fields (`rows.columns`, `rows.rows`) — it already only uses those two fields, so the only edit is its parameter type `&Rows` -> `&ObjectRows` and dropping the `Rows` import if unused. The positional `{columns, rows}` envelope stays until Task 4 replaces it.

In `src/services/query-api/src/http.rs`: change `fn rows_to_json(rows: &Rows)` to `fn rows_to_json(rows: &crate::handler::ObjectRows)`, and adjust the `use crate::serving::{...}` to drop `Rows` if it becomes unused (keep `ServingEngine`, `SqlValue`).

- [ ] **Step 5: Full build + test sweep** — `buck2 build //src/... > /tmp/b.log 2>&1; grep -E "error|BUILD SUCCEEDED|Failed" /tmp/b.log` then `buck2 test //src/services/query-api/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`. Expected: builds; `http-smoke` still passes (envelope unchanged this task), `governed-read` passes.

- [ ] **Step 6: rustfmt + clippy** — `buck2 run //tools:rustfmt -- src/services/query-api/src/handler.rs src/services/query-api/src/http.rs src/services/query-api/tests/governed_read.rs` and apply; `tools/clippy-all.sh` clean.

- [ ] **Step 7: Commit**
```bash
git add src/services/query-api/src/handler.rs src/services/query-api/src/http.rs src/services/query-api/tests/governed_read.rs
git commit -m "feat(query-api): read_object returns ObjectRows (rows + per-column logical type)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 4: typed-object wire rendering + HTTP wiring + strengthened e2e

**Files:**
- Create: `src/services/query-api/src/render.rs`
- Modify: `src/services/query-api/src/lib.rs:11-14` (add `pub mod render;`)
- Modify: `src/services/query-api/src/http.rs` (call `objects_to_json`, delete `rows_to_json`)
- Test: `src/services/query-api/tests/render.rs` (new, pure `rust_test`) + BUCK target
- Modify: `src/services/query-api/tests/http_smoke.rs` (assert the typed-object envelope)
- Modify: `src/services/query-api/tests/bind_read_e2e.rs` (add a Double column + assert rendered JSON)

The renderer is pure (no I/O): `(logical_type, &SqlValue) -> serde_json::Value`, with a single shared natural-rendering fallback used whenever the declared repr can't be honored (unknown type, or repr/value mismatch). `Null` is always JSON `null`.

- [ ] **Step 1: Write the failing render-matrix unit test** — `src/services/query-api/tests/render.rs`:

```rust
//! The logical-type -> JSON rendering matrix (pure). Proves Long renders as a STRING
//! (the >2^53 precision rule), temporal types as ISO-8601, the natural fallback for
//! unknown/mismatched types, and the { "objects": [...] } envelope shape.

use query_api::handler::ObjectRows;
use query_api::render::objects_to_json;
use query_api::serving::SqlValue;
use serde_json::json;

fn one(logical_ty: &str, cell: SqlValue) -> serde_json::Value {
    let rows = ObjectRows {
        columns: vec!["c".into()],
        logical_types: vec![logical_ty.into()],
        rows: vec![vec![cell]],
    };
    objects_to_json(&rows)["objects"][0]["c"].clone()
}

#[test]
fn long_renders_as_string_preserving_precision_past_2_53() {
    // 2^53 + 1 = 9007199254740993, NOT representable as an exact f64/JSON number.
    assert_eq!(one("Long", SqlValue::Int(9_007_199_254_740_993)), json!("9007199254740993"));
}

#[test]
fn scalar_types_render_per_vocabulary() {
    assert_eq!(one("Integer", SqlValue::Int(42)), json!(42));
    assert_eq!(one("Double", SqlValue::Double(3.5)), json!(3.5));
    assert_eq!(one("Boolean", SqlValue::Bool(true)), json!(true));
    assert_eq!(one("String", SqlValue::Text("hi".into())), json!("hi"));
    assert_eq!(one("EmailAddress", SqlValue::Text("a@b.com".into())), json!("a@b.com"));
}

#[test]
fn temporal_types_render_iso_8601() {
    assert_eq!(
        one("Date", SqlValue::Date(time::macros::date!(2026 - 06 - 12))),
        json!("2026-06-12")
    );
    assert_eq!(
        one(
            "Timestamp",
            SqlValue::Timestamp(time::macros::datetime!(2026 - 06 - 12 14:09:42))
        ),
        json!("2026-06-12T14:09:42")
    );
}

#[test]
fn null_is_json_null_regardless_of_type() {
    assert_eq!(one("Long", SqlValue::Null), serde_json::Value::Null);
    assert_eq!(one("Date", SqlValue::Null), serde_json::Value::Null);
}

#[test]
fn unknown_type_falls_back_to_natural_rendering() {
    // "Money" is not in the vocabulary -> render the cell by its own variant.
    assert_eq!(one("Money", SqlValue::Double(1.25)), json!(1.25));
    assert_eq!(one("Money", SqlValue::Text("x".into())), json!("x"));
}

#[test]
fn declared_value_mismatch_falls_back_to_natural_rendering() {
    // Declared Date but the cell is an Int (e.g. table evolved post-bind): don't
    // fail the read; render the int naturally.
    assert_eq!(one("Date", SqlValue::Int(7)), json!(7));
}

#[test]
fn objects_to_json_builds_keyed_objects_in_column_order() {
    let rows = ObjectRows {
        columns: vec!["id".into(), "email".into()],
        logical_types: vec!["Long".into(), "EmailAddress".into()],
        rows: vec![
            vec![SqlValue::Int(1), SqlValue::Text("a@x".into())],
            vec![SqlValue::Int(2), SqlValue::Text("b@x".into())],
        ],
    };
    assert_eq!(
        objects_to_json(&rows),
        json!({ "objects": [
            { "id": "1", "email": "a@x" },
            { "id": "2", "email": "b@x" },
        ] })
    );
}
```

Wire the target in `src/services/query-api/BUCK` (a pure `rust_test`, no fixture):
```python
rust_test(
    name = "render",
    crate = "render",
    srcs = ["tests/render.rs"],
    crate_root = "tests/render.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//third-party:serde_json",
        "//third-party:time",
    ],
)
```

- [ ] **Step 2: Run, verify it fails to compile** — `buck2 test //src/services/query-api:render > /tmp/t.log 2>&1; grep -E "error\[|Tests finished|FAIL" /tmp/t.log`. Expected: compile error (`query_api::render` / `objects_to_json` not found).

- [ ] **Step 3: Implement `src/services/query-api/src/render.rs`:**
```rust
//! Typed-object wire rendering: turn a governed `ObjectRows` into the JSON the query
//! front door serves, with each value shaped by its property's logical type (the core
//! vocabulary). Pure — no I/O. A `Null` cell is always JSON `null`; an unknown logical
//! type or a declared/value mismatch falls back to the cell's natural rendering rather
//! than failing a permitted read.

use control_plane_core::{JsonRepr, json_repr_of};
use serde_json::{Value, json};

use crate::handler::ObjectRows;
use crate::serving::{SqlValue, iso_date, iso_timestamp};

/// `{ "objects": [ { property: typed_value, ... }, ... ] }`. Keys are the projected
/// column names in `columns` order; values are rendered per the aligned logical type.
pub fn objects_to_json(rows: &ObjectRows) -> Value {
    let objects: Vec<Value> = rows
        .rows
        .iter()
        .map(|row| {
            let mut obj = serde_json::Map::with_capacity(rows.columns.len());
            for (i, col) in rows.columns.iter().enumerate() {
                let logical_ty = rows.logical_types.get(i).map(String::as_str).unwrap_or("");
                obj.insert(col.clone(), render_cell(logical_ty, &row[i]));
            }
            Value::Object(obj)
        })
        .collect();
    json!({ "objects": objects })
}

/// Render one cell as JSON, driven by its declared logical type.
fn render_cell(logical_ty: &str, cell: &SqlValue) -> Value {
    if matches!(cell, SqlValue::Null) {
        return Value::Null;
    }
    match json_repr_of(logical_ty) {
        Ok(repr) => render_typed(repr, cell),
        Err(_) => natural(cell), // unknown logical type
    }
}

/// Render per the declared repr; on a repr/value mismatch, fall back to natural.
fn render_typed(repr: JsonRepr, cell: &SqlValue) -> Value {
    match (repr, cell) {
        (JsonRepr::Number, SqlValue::Int(i)) => json!(i),
        (JsonRepr::Number, SqlValue::Double(f)) => json!(f),
        (JsonRepr::NumericString, SqlValue::Int(i)) => json!(i.to_string()),
        (JsonRepr::Bool, SqlValue::Bool(b)) => json!(b),
        (JsonRepr::PlainString, SqlValue::Text(s)) => json!(s),
        (JsonRepr::IsoDate, SqlValue::Date(d)) => json!(iso_date(d)),
        (JsonRepr::IsoTimestamp, SqlValue::Timestamp(ts)) => json!(iso_timestamp(ts)),
        _ => natural(cell),
    }
}

/// Best-effort rendering by the cell's own variant — the shared fallback for unknown
/// types and declared/value mismatches.
fn natural(cell: &SqlValue) -> Value {
    match cell {
        SqlValue::Null => Value::Null,
        SqlValue::Int(i) => json!(i),
        SqlValue::Double(f) => json!(f),
        SqlValue::Bool(b) => json!(b),
        SqlValue::Text(s) => json!(s),
        SqlValue::Date(d) => json!(iso_date(d)),
        SqlValue::Timestamp(ts) => json!(iso_timestamp(ts)),
    }
}
```

Add the module in `src/services/query-api/src/lib.rs` (after `pub mod handler;`):
```rust
pub mod render;
```

- [ ] **Step 4: Run the render matrix** — `buck2 test //src/services/query-api:render > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|panicked" /tmp/t.log`. Expected: PASS.

- [ ] **Step 5: Wire the HTTP layer to the typed envelope.** In `src/services/query-api/src/http.rs`, replace the `Ok(rows) => Json(rows_to_json(&rows)).into_response(),` arm (http.rs:65) with a direct call to the renderer:
```rust
        Ok(rows) => Json(crate::render::objects_to_json(&rows)).into_response(),
```
Delete the now-dead `fn rows_to_json` (http.rs:77-92) and drop unused imports (`serde_json::json` if now unused; keep `SqlValue` only if still referenced — after deleting `rows_to_json` the `eq_filters` mapping still uses `SqlValue::Text`, so keep `SqlValue`; `Rows` is no longer needed).

- [ ] **Step 6: Update `http_smoke.rs` to assert the typed-object envelope.** The stub serving returns `SqlValue::Int(1)` for property `id` typed `Long`, so it now renders as the string `"1"` inside an object. Replace the two assertions (http_smoke.rs:149-150):
```rust
    assert_eq!(json["objects"][0]["id"], "1"); // Long -> JSON string
```
Update the file's top doc comment (http_smoke.rs:1) to "... and ObjectRows serialize to a typed-object JSON envelope." `StubServing` returns `serving::Rows` (the serving result type) — that is unchanged, so the stub stays as-is.

- [ ] **Step 7: Strengthen the e2e** in `src/services/query-api/tests/bind_read_e2e.rs`.

(a) Imports — add `Float64Array` to the arrow import (bind_read_e2e.rs:7) and `objects_to_json`:
```rust
use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
```
Add after the existing `query_api::...` uses:
```rust
use query_api::render::objects_to_json;
use serde_json::json;
```

(b) Widen the landed schema (bind_read_e2e.rs:34-45) to include a `Double` column (the materializer's `infer` supports `Float64` -> `double`; it does NOT support Date/Timestamp, so those stay out of the materialize path and are covered by the render unit + serving-types fixture tests):
```rust
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("email", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("a@x"), Some("b@x")])),
            Arc::new(Float64Array::from(vec![Some(1.5), Some(2.5)])),
        ],
    )
    .unwrap();
```

(c) Add the `amount` property to the bound `Customer` type (after the `email` property, bind_read_e2e.rs:85-89):
```rust
                PropertyDef {
                    name: "amount".into(),
                    ty: "Double".into(),
                    required: false,
                },
```

(d) Replace the tail assertions (bind_read_e2e.rs:132-138) with the structural checks PLUS the typed-JSON proof:
```rust
    assert_eq!(
        rows.columns,
        vec!["id".to_string(), "email".to_string(), "amount".to_string()]
    );
    assert_eq!(rows.rows.len(), 2, "both landed rows are retrievable");

    // The typed wire contract end-to-end: id (Long) renders as a STRING, amount
    // (Double) as a number, through the real materialize -> bind -> read path.
    let body = objects_to_json(&rows);
    let mut objs: Vec<serde_json::Value> = body["objects"].as_array().unwrap().clone();
    objs.sort_by_key(|o| o["id"].as_str().unwrap().to_string());
    assert_eq!(
        objs,
        vec![
            json!({ "id": "1", "email": "a@x", "amount": 1.5 }),
            json!({ "id": "2", "email": "b@x", "amount": 2.5 }),
        ],
        "Long id serializes as a string; Double amount as a number"
    );
```

(e) `bind-read-e2e` already deps on `//third-party:serde_json` and `//third-party:time` (BUCK lines 105/106) — no BUCK change needed.

- [ ] **Step 8: Full sweep** — `buck2 build //src/... > /tmp/b.log 2>&1; grep -E "error|Failed|SUCCEEDED" /tmp/b.log` then `buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`. Expected: build succeeds; all tests pass (render, http-smoke, governed-read, serving-types, bind-read-e2e, and the unchanged rest).

- [ ] **Step 9: rustfmt + clippy + prek** — `buck2 run //tools:rustfmt -- src/services/query-api/src/render.rs src/services/query-api/src/lib.rs src/services/query-api/src/http.rs src/services/query-api/tests/http_smoke.rs src/services/query-api/tests/bind_read_e2e.rs src/services/query-api/tests/render.rs` and apply; `tools/clippy-all.sh` clean; `buck2 run //tools:prek -- run --all-files` green.

- [ ] **Step 10: Commit**
```bash
git add src/services/query-api/src/render.rs src/services/query-api/src/lib.rs src/services/query-api/src/http.rs src/services/query-api/BUCK src/services/query-api/tests/render.rs src/services/query-api/tests/http_smoke.rs src/services/query-api/tests/bind_read_e2e.rs
git commit -m "feat(query-api): typed-object JSON wire (logical-type-driven render)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 5: roadmap update

**Files:**
- Modify: `docs/superpowers/specs/2026-06-06-loom-roadmap.md`

- [ ] **Step 1: Mark the slice delivered.** Find the ingest/query roadmap section that tracks the part-2b binding entry and add a sibling entry noting query-path typed JSON serialization is delivered (the logical-type vocabulary now drives logical↔JSON on the read path; `Long` → string; `Double`/`Date`/`Timestamp` fidelity in the serving layer). Match the surrounding entry's wording/format. Keep the file passing the markdown hooks: exactly one trailing newline, no trailing whitespace.

- [ ] **Step 2: prek on the doc** — `buck2 run //tools:prek -- run --all-files > /tmp/p.log 2>&1; grep -E "Passed|Failed" /tmp/p.log`. Expected: all Passed (the hooks fix files in place; commit whatever they change).

- [ ] **Step 3: Commit**
```bash
git add docs/superpowers/specs/2026-06-06-loom-roadmap.md
git commit -m "docs(roadmap): query-path typed JSON serialization delivered

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Self-review notes (spec coverage)

- **`JsonRepr` + `Long`→string** — Task 1 (core classification) + Task 4 (render matrix proves `9007199254740993` → `"9007199254740993"`).
- **Serving fidelity fix (`Double`/`Date`/`Timestamp`)** — Task 2, proven against real DuckDB by the `serving-types` fixture test.
- **`read_object` carries logical types** — Task 3 (`ObjectRows`), field-compatible with the governed-read oracle.
- **Pure typed-object boundary + `{ "objects": [...] }` envelope** — Task 4 (`render.rs`), wired into HTTP, replacing `{columns,rows}`.
- **Strengthened e2e** — Task 4: materialize (with a Double column) → bind → read → assert typed JSON with `id` as a string.
- **Edge cases (Null, unknown type, mismatch)** — Task 4 render matrix.
- **Out of scope** (typed input filters, schema sidecar, tz timestamps, paging) — untouched, per spec.
