# Typed Input Filters Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Coerce query-param equality filters to their column's declared ontology logical type before binding, so `?amount=100&active=true` filters on `Double`/`Boolean` columns actually match (today every filter binds as `Text` and silently matches nothing).

**Architecture:** A new pure `coerce_filter(name, logical_ty, raw) -> Result<SqlValue, FilterError>` (the string-input analog of `params.rs::parse_value`, reusing the `json_repr_of`/`JsonRepr` taxonomy). The query-input structs carry raw `String` filter values; the handler coerces each against the source type's `PropertyDef.ty` after the existing visibility check, `400`-ing (`BadFilter`) on an uncoercible value. The SQL compilers are unchanged (they still take typed `SqlValue` params).

**Tech Stack:** Rust 2024, buck2, axum, DuckDB-over-DuckLake serving, `loom_fixture_test`. Tests are `rust_test`/`loom_fixture_test` targets — never inline `#[cfg(test)]`.

**Spec:** `docs/superpowers/specs/2026-06-16-query-typed-input-filters-design.md`

**Key design choices (baked in):**
- Equality-only (comparison operators are a later slice); the flat `(col, value)` filter list shape is unchanged, only its value type goes `SqlValue` → raw `String`.
- Coercion happens in the handler (it has the ontology), not the HTTP edge.
- `Number` repr (Integer/Double) resolves try-`i64`-else-`f64` (equality-correct under DuckDB numeric coercion).
- Uncoercible value or unknown logical type → reuse `QueryError::BadFilter(col)` → `400` (no new variant).
- Visibility check (denied/masked → 400) runs BEFORE coercion, so coercion only sees columns the subject may view.

---

## File Structure

| File | Responsibility | Action |
|------|----------------|--------|
| `src/services/query-api/src/filter.rs` | `coerce_filter` + `FilterError` | Create |
| `src/services/query-api/src/lib.rs` | `pub mod filter;` | Modify |
| `src/services/query-api/tests/filter_coerce.rs` | unit tests | Create |
| `src/services/query-api/src/handler.rs` | 3 query structs' filter field → `String`; coerce in `read_object` + `read_linked_chain` | Modify |
| `src/services/query-api/src/http.rs` | forward raw strings (drop `SqlValue::Text` wrap) | Modify |
| (test sites building filters with `SqlValue::…`) | → raw strings | Modify (compiler-enumerated) |
| `src/services/query-api/tests/typed_filter_e2e.rs` | governed typed-filter e2e | Create |
| `src/services/query-api/BUCK` | `filter-coerce` + `typed-filter-e2e` targets | Modify |
| roadmap, `docs/FUTURE.md` | delivered marker + follow-ups | Modify |

---

## Task 1: `coerce_filter` module + unit tests

**Files:**
- Create: `src/services/query-api/src/filter.rs`
- Modify: `src/services/query-api/src/lib.rs`
- Create: `src/services/query-api/tests/filter_coerce.rs`
- Modify: `src/services/query-api/BUCK`

- [ ] **Step 1: Write the module**

Create `src/services/query-api/src/filter.rs`:

```rust
//! Coerce a raw query-param filter value to its column's declared ontology logical type.
//! The string-input analog of `params::parse_value` (which coerces a JSON `Value` from an
//! action body). Reuses the `json_repr_of` -> `JsonRepr` taxonomy. Pure logic, no I/O.

use control_plane_core::{JsonRepr, json_repr_of};

use crate::serving::SqlValue;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum FilterError {
    #[error("filter {0}: {1}")]
    BadValue(String, String),
}

/// Coerce `raw` (a query-param string) to `logical_ty`'s `SqlValue`. Equality-only. The
/// `Number` repr (Integer/Double) resolves to `Int` when `raw` is a clean integer, else
/// `Double` — equality-correct under DuckDB numeric coercion.
pub fn coerce_filter(name: &str, logical_ty: &str, raw: &str) -> Result<SqlValue, FilterError> {
    let bad = |m: &str| FilterError::BadValue(name.to_string(), m.to_string());
    let repr = json_repr_of(logical_ty).map_err(|_| bad("unknown logical type"))?;
    match repr {
        JsonRepr::Number => {
            if let Ok(i) = raw.parse::<i64>() {
                Ok(SqlValue::Int(i))
            } else {
                raw.parse::<f64>()
                    .map(SqlValue::Double)
                    .map_err(|_| bad("expected a number"))
            }
        }
        JsonRepr::NumericString => raw
            .parse::<i64>()
            .map(SqlValue::Int)
            .map_err(|_| bad("not an int64")),
        JsonRepr::Bool => match raw {
            "true" => Ok(SqlValue::Bool(true)),
            "false" => Ok(SqlValue::Bool(false)),
            _ => Err(bad("expected true or false")),
        },
        JsonRepr::PlainString => Ok(SqlValue::Text(raw.to_string())),
        JsonRepr::IsoDate => {
            let fmt = time::macros::format_description!("[year]-[month]-[day]");
            time::Date::parse(raw, &fmt)
                .map(SqlValue::Date)
                .map_err(|_| bad("invalid ISO date"))
        }
        JsonRepr::IsoTimestamp => {
            let fmt =
                time::macros::format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]");
            time::PrimitiveDateTime::parse(raw, &fmt)
                .map(SqlValue::Timestamp)
                .map_err(|_| bad("invalid ISO timestamp"))
        }
    }
}
```

NOTE: confirm against `params.rs` that `JsonRepr` has exactly these 6 variants (`Number`, `NumericString`, `Bool`, `PlainString`, `IsoDate`, `IsoTimestamp`), that `json_repr_of` returns `Result<JsonRepr, _>`, and that `SqlValue` has `Int(i64)`, `Double(f64)`, `Bool(bool)`, `Text(String)`, `Date(time::Date)`, `Timestamp(time::PrimitiveDateTime)`. Match the real `time` parsing exactly as `parse_value` does (same format descriptions). `time` and `thiserror` are already query-api deps (params.rs uses them).

- [ ] **Step 2: Register the module**

In `src/services/query-api/src/lib.rs`, add `pub mod filter;` (alphabetically near the other `mod`/`pub mod` lines — confirm the existing module-declaration style and match it).

- [ ] **Step 3: Write the unit tests**

Create `src/services/query-api/tests/filter_coerce.rs`:

```rust
//! Unit tests for query-param filter coercion. Pure logic.

use query_api::filter::{FilterError, coerce_filter};
use query_api::serving::SqlValue;

#[test]
fn integer_and_double_coerce_via_number_repr() {
    // Integer / Double both map to JsonRepr::Number; clean int -> Int, else Double.
    assert_eq!(coerce_filter("x", "Integer", "100").unwrap(), SqlValue::Int(100));
    assert_eq!(coerce_filter("x", "Double", "100").unwrap(), SqlValue::Int(100));
    assert_eq!(coerce_filter("x", "Double", "1.5").unwrap(), SqlValue::Double(1.5));
    assert_eq!(coerce_filter("x", "Integer", "-7").unwrap(), SqlValue::Int(-7));
}

#[test]
fn long_coerces_to_int() {
    assert_eq!(coerce_filter("id", "Long", "100").unwrap(), SqlValue::Int(100));
    assert!(matches!(coerce_filter("id", "Long", "1.5"), Err(FilterError::BadValue(_, _))));
}

#[test]
fn boolean_coerces_strictly() {
    assert_eq!(coerce_filter("a", "Boolean", "true").unwrap(), SqlValue::Bool(true));
    assert_eq!(coerce_filter("a", "Boolean", "false").unwrap(), SqlValue::Bool(false));
    assert!(matches!(coerce_filter("a", "Boolean", "maybe"), Err(FilterError::BadValue(_, _))));
    assert!(matches!(coerce_filter("a", "Boolean", "1"), Err(FilterError::BadValue(_, _))));
}

#[test]
fn string_passes_through() {
    assert_eq!(coerce_filter("s", "String", "hi").unwrap(), SqlValue::Text("hi".into()));
    // A numeric-looking string stays text for a String column.
    assert_eq!(coerce_filter("s", "String", "100").unwrap(), SqlValue::Text("100".into()));
}

#[test]
fn date_and_timestamp_coerce_from_iso() {
    let d = coerce_filter("d", "Date", "2026-06-16").unwrap();
    assert!(matches!(d, SqlValue::Date(_)));
    let ts = coerce_filter("t", "Timestamp", "2026-06-16T12:00:00").unwrap();
    assert!(matches!(ts, SqlValue::Timestamp(_)));
    assert!(matches!(coerce_filter("d", "Date", "nope"), Err(FilterError::BadValue(_, _))));
    assert!(matches!(coerce_filter("t", "Timestamp", "2026-06-16"), Err(FilterError::BadValue(_, _))));
}

#[test]
fn uncoercible_number_and_unknown_type_error() {
    assert!(matches!(coerce_filter("x", "Double", "abc"), Err(FilterError::BadValue(_, _))));
    assert!(matches!(coerce_filter("x", "Integer", "abc"), Err(FilterError::BadValue(_, _))));
    assert!(matches!(coerce_filter("x", "Nonsense", "1"), Err(FilterError::BadValue(_, _))));
}
```

NOTE: confirm the exact logical-type STRINGS the ontology uses (`"Integer"`, `"Double"`, `"Long"`, `"Boolean"`, `"String"`, `"Date"`, `"Timestamp"`) by checking how `BaseType`/`json_repr_of` parse them in `src/control-plane/core/src/logical_type.rs` and how `parse_value`'s tests (if any) or other tests spell them. If a name differs (e.g. `"Bool"` vs `"Boolean"`), use the REAL spelling.

- [ ] **Step 4: BUCK target**

Add to `src/services/query-api/BUCK` (mirror the existing pure-logic `sql-compile` `rust_test`, plus `//third-party:time` for the Date/Timestamp asserts):

```python
rust_test(
    name = "filter-coerce",
    srcs = ["tests/filter_coerce.rs"],
    crate = "filter_coerce",
    crate_root = "tests/filter_coerce.rs",
    deps = [":query-api", "//third-party:time"],
)
```

(If the test doesn't actually need `time` directly — it uses `matches!` rather than constructing `Date` values — drop `//third-party:time`. Confirm by building; remove an unused dep if clippy/build complains.)

- [ ] **Step 5: Run + lint + commit**

Run (serial): `buck2 test //src/services/query-api:filter-coerce > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL|panicked|left|right" /tmp/t1.log` → all pass.
Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' 2>&1 | tail -5` → empty.

```bash
git add src/services/query-api/src/filter.rs src/services/query-api/src/lib.rs src/services/query-api/tests/filter_coerce.rs src/services/query-api/BUCK
git commit -m "feat(query-api): coerce_filter — typed query-param filter coercion"
```

---

## Task 2: Raw-string filter representation + handler coercion

Change the three query structs' filter field to raw `String`, coerce in the handlers, forward raw strings from HTTP, and fix all compiler-flagged construction sites.

**Files:**
- Modify: `src/services/query-api/src/handler.rs`, `src/services/query-api/src/http.rs`
- Modify (compiler-enumerated): test sites building filters with `SqlValue::…`

- [ ] **Step 1: Change the query-struct filter field types**

In `src/services/query-api/src/handler.rs`:
- `ObjectQuery.eq_filters`: `Vec<(String, SqlValue)>` → `Vec<(String, String)>` (line ~31).
- `LinkQuery.source_filters`: `Vec<(String, SqlValue)>` → `Vec<(String, String)>` (line ~270).
- `ChainQuery.source_filters`: `Vec<(String, SqlValue)>` → `Vec<(String, String)>` (line ~300).

`read_linked_objects` (the wrapper) clones `q.source_filters` into `ChainQuery.source_filters` — both are now `Vec<(String, String)>`, so the clone still typechecks unchanged.

- [ ] **Step 2: Coerce in `read_object`**

Replace the eq-filter validation loop (currently ~lines 156-158):

```rust
    for (col, _) in &q.eq_filters {
        if !allowed.contains(col) || masked.contains(col) {
            return Err(QueryError::BadFilter(col.clone()));
        }
    }
```

with a visibility-check-then-coerce loop building a typed vec:

```rust
    // Visibility first (denied/masked column -> 400, no type info leak), then coerce the raw
    // filter value to the column's declared logical type.
    let mut eq_filters: Vec<(String, SqlValue)> = Vec::with_capacity(q.eq_filters.len());
    for (col, raw) in &q.eq_filters {
        if !allowed.contains(col) || masked.contains(col) {
            return Err(QueryError::BadFilter(col.clone()));
        }
        let ty = object_type
            .properties
            .iter()
            .find(|p| &p.name == col)
            .map(|p| p.ty.as_str())
            .unwrap_or("");
        let v = crate::filter::coerce_filter(col, ty, raw)
            .map_err(|_| QueryError::BadFilter(col.clone()))?;
        eq_filters.push((col.clone(), v));
    }
```

Then change the `compile_select` call (line ~224) to pass `&eq_filters` instead of `&q.eq_filters`.

(`object_type` is the source type's `ObjectType` in `read_object`; `allowed` is its visible projection — a column in `allowed` is guaranteed a property, so `find` is `Some`; the `unwrap_or("")` fallback would fail coercion → `BadFilter`, a safe net. `SqlValue` is already imported in handler.rs. Confirm the real variable names by reading the function.)

- [ ] **Step 3: Coerce in `read_linked_chain`**

Replace the source-filter validation loop (currently ~lines 384-386):

```rust
    for (col, _) in &q.source_filters {
        if !from_allowed.contains(col) || s_masked.contains(col) {
            return Err(QueryError::BadFilter(col.clone()));
        }
    }
```

with:

```rust
    let mut source_filters: Vec<(String, SqlValue)> = Vec::with_capacity(q.source_filters.len());
    for (col, raw) in &q.source_filters {
        if !from_allowed.contains(col) || s_masked.contains(col) {
            return Err(QueryError::BadFilter(col.clone()));
        }
        let ty = from_type
            .properties
            .iter()
            .find(|p| &p.name == col)
            .map(|p| p.ty.as_str())
            .unwrap_or("");
        let v = crate::filter::coerce_filter(col, ty, raw)
            .map_err(|_| QueryError::BadFilter(col.clone()))?;
        source_filters.push((col.clone(), v));
    }
```

Then change the `compile_chain` call (line ~406) to pass `&source_filters` instead of `&q.source_filters`. (`from_type` is the source `ObjectType`; `from_allowed`/`s_masked` are its visible/masked sets. Confirm names by reading the function.)

- [ ] **Step 4: Forward raw strings from HTTP**

In `src/services/query-api/src/http.rs`, drop the `SqlValue::Text(v)` wrap in all three handlers so the raw `(key, value)` strings flow through:

- `get_object`: `let eq_filters = params.into_iter().map(|(k, v)| (k, SqlValue::Text(v))).collect();` → `let eq_filters: Vec<(String, String)> = params.into_iter().collect();`
- `get_linked`: same change for `source_filters`.
- `get_linked_chain`: after `params.remove("path")` (keep that), the remainder: `let source_filters: Vec<(String, String)> = params.into_iter().collect();` (drop the `SqlValue::Text` map).

If `SqlValue` becomes unused in `http.rs` after this, remove it from the imports (clippy will flag an unused import). Confirm `SqlValue` isn't still used elsewhere in the file before removing.

- [ ] **Step 5: Fix compiler-flagged test construction sites**

Build to enumerate: `buck2 build //src/services/query-api:query-api 2>&1 | tail -15` then run the test targets' build. The struct field-type change breaks every NON-EMPTY filter construction that passes a `SqlValue`. Known sites (verify with the compiler — `buck2 test //src/services/query-api/... > /tmp/build.log 2>&1` surfaces them all as compile errors):
- `tests/governed_read.rs:211`: `("id".into(), SqlValue::Int(1))` → `("id".into(), "1".into())` (id is `Long`; coerces back to `Int(1)`).
- `tests/governed_read.rs:324`: `("secret".into(), SqlValue::Text("s1".into()))` → `("secret".into(), "s1".into())` (this test asserts a masked-column filter is rejected; the value is never coerced because the masked check fires first — behavior unchanged).
- `tests/multi_hop_traversal_e2e.rs:280,325`: `("region".into(), SqlValue::Text("CA".into()))` → `("region".into(), "CA".into())`.
- Any in `tests/link_traversal.rs` that build `LinkQuery.source_filters` with a `SqlValue` — convert each to a raw string.

DO NOT change `tests/sql_compile.rs`: its `SqlValue` filter values are passed DIRECTLY to `compile_select`/`compile_chain` (whose signatures are unchanged — still typed `SqlValue`), not into a query struct. Likewise `serving_engine.rs`, `quack_*`, `action_engine.rs` use `SqlValue` for serving/param tests, unrelated to the query-struct filters. Leave them.

- [ ] **Step 6: Build + lint + regression**

Run (serial): `buck2 build //src/services/query-api:query-api-bin 2>&1 | tail -10` (clean).
Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' 2>&1 | tail -5` (empty).
Run the existing read e2es + http-smoke (regression — text-column filters behave identically, and the `governed_read` Int/masked filter cases still pass):
`buck2 test //src/services/query-api:governed-read //src/services/query-api:link-traversal //src/services/query-api:derived-properties-e2e //src/services/query-api:multi-hop-traversal-e2e //src/services/query-api:http-smoke > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t2.log` → all `Fail 0`.

- [ ] **Step 7: Commit**

```bash
git add src/services/query-api/src/handler.rs src/services/query-api/src/http.rs src/services/query-api/tests/
git commit -m "feat(query-api): coerce query-param filters to the column's ontology type"
```

---

## Task 3: Governed typed-filter e2e

**Files:**
- Create: `src/services/query-api/tests/typed_filter_e2e.rs`
- Modify: `src/services/query-api/BUCK`

- [ ] **Step 1: Write the e2e**

Create `src/services/query-api/tests/typed_filter_e2e.rs`. **Mirror `tests/derived_properties_e2e.rs` EXACTLY** for the fixture/seeding/ACL/serving setup (it already seeds an `orders` table with a `Double` `amount` column via the ingest materializer — reuse that mechanism). READ it first and reproduce its setup verbatim; the assertions below are the contract.

Seed an `Order` table with non-text columns:
- `orders(id Long, amount Double, active Boolean)`: `(1, 10.5, true)`, `(2, 20.0, false)`, `(3, 10.5, true)`.

Ontology: type `Order` (`derived: vec![]`) over that table, properties `id: Long`, `amount: Double`, `active: Boolean`. Grant the subject `Read` on `Order`.

```rust
//! Typed input filters e2e: filter a Double and a Boolean column through read_object.
//! A Text bind would match nothing; coercion to the column's logical type makes it work.
//! An uncoercible value is a 400 (BadFilter).

// ... imports + fixture/seed/ACL setup mirrored from derived_properties_e2e.rs ...
// Produces `cp`, the `EmbeddedDuckDb` engine `eng`, seeded `orders` as above, subject `a`
// granted Read on Order.

#[tokio::test(flavor = "multi_thread")]
async fn typed_filters_match_and_reject() {
    // ---- fixture + seed (mirror derived_properties_e2e.rs): orders(id, amount, active) ----
    // ---- ontology: Order type; grant Read on Order to subject `a` ----

    let deps = QueryDeps { ontology: &cp, acl: &cp, serving: &eng };
    let ids = |rows: &query_api::handler::ObjectRows| {
        let body = objects_to_json(rows);
        let mut v: Vec<String> = body["objects"].as_array().unwrap().iter()
            .map(|o| o["id"].as_str().unwrap().to_string())
            .collect();
        v.sort();
        v
    };

    // Double filter: amount = 10.5 -> rows 1, 3.
    let r = read_object(
        &ObjectQuery { type_name: "Order".into(), eq_filters: vec![("amount".into(), "10.5".into())] },
        &Subject(a.clone()),
        &deps,
    ).await.unwrap();
    assert_eq!(ids(&r), vec!["1".to_string(), "3".to_string()]);

    // Boolean filter: active = true -> rows 1, 3 ; active = false -> row 2.
    let r_true = read_object(
        &ObjectQuery { type_name: "Order".into(), eq_filters: vec![("active".into(), "true".into())] },
        &Subject(a.clone()),
        &deps,
    ).await.unwrap();
    assert_eq!(ids(&r_true), vec!["1".to_string(), "3".to_string()]);
    let r_false = read_object(
        &ObjectQuery { type_name: "Order".into(), eq_filters: vec![("active".into(), "false".into())] },
        &Subject(a.clone()),
        &deps,
    ).await.unwrap();
    assert_eq!(ids(&r_false), vec!["2".to_string()]);

    // Uncoercible value -> BadFilter (400).
    let err = read_object(
        &ObjectQuery { type_name: "Order".into(), eq_filters: vec![("amount".into(), "abc".into())] },
        &Subject(a.clone()),
        &deps,
    ).await.unwrap_err();
    assert!(matches!(err, QueryError::BadFilter(_)), "uncoercible filter value -> BadFilter");
}
```

Import `read_object`, `ObjectQuery`, `Subject`, `QueryDeps`, `QueryError`, `ObjectRows` from `query_api::handler`; `objects_to_json` from `query_api::render`; `EmbeddedDuckDb` from `query_api::serving`; ontology/ACL types from `control_plane_core`; fixture from `control_plane_postgres::fixture`. CONFIRM that the materializer/landing path accepts a `Boolean` Arrow column (the infer path supports Bool); if seeding a Boolean column needs a specific Arrow builder, mirror how an existing test seeds typed columns (derived_properties_e2e seeds `Double`; add a `BooleanArray` the same way). Do NOT weaken the assertions (Double match, Boolean match both ways, uncoercible → BadFilter).

- [ ] **Step 2: BUCK target**

Add to `src/services/query-api/BUCK`, copying the EXACT deps/attrs of the neighboring `derived-properties-e2e` `loom_fixture_test` target:

```python
loom_fixture_test(
    name = "typed-filter-e2e",
    crate = "typed_filter_e2e",
    srcs = ["tests/typed_filter_e2e.rs"],
    crate_root = "tests/typed_filter_e2e.rs",
    duckdb = True,
    deps = [
        ":query-api",
        "//src/services/ingest:ingest",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:arrow",
        "//third-party:object_store",
        "//third-party:serde_json",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```

- [ ] **Step 3: Run + commit**

Run (serial): `buck2 test //src/services/query-api:typed-filter-e2e > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL|panicked|assertion|left|right" /tmp/t3.log` → `Pass 1. Fail 0.`
If a filter matches nothing, check: the seeded values, the coercion (Double `10.5` → `Double(10.5)`, Boolean `true` → `Bool(true)`), and that the bind reaches DuckDB typed (not Text). Do NOT weaken assertions — fix the setup, or if a real Task 1/2 bug surfaces, report it precisely.

```bash
git add src/services/query-api/tests/typed_filter_e2e.rs src/services/query-api/BUCK
git commit -m "test(query-api): typed-filter e2e — Double/Boolean match, bad value 400"
```

---

## Task 4: Docs

**Files:**
- Modify: `docs/superpowers/specs/2026-06-06-loom-roadmap.md`, `docs/FUTURE.md`

- [ ] **Step 1: Roadmap delivered marker**

In `docs/superpowers/specs/2026-06-06-loom-roadmap.md`, find the "Smaller query follow-ups" line (`typed input filters, a schema sidecar, tz timestamps`). Mark **typed input filters delivered** (matching the surrounding style): query-param equality filters now coerce to the column's ontology logical type (`json_repr_of` taxonomy), so `Long`/`Double`/`Boolean`/`Date`/`Timestamp` filters work across `read_object`, traversal, and chains; uncoercible value → 400; equality-only. Reference `docs/superpowers/specs/2026-06-16-query-typed-input-filters-design.md`. Leave schema sidecar + tz timestamps as remaining.

- [ ] **Step 2: FUTURE.md follow-ups**

In `docs/FUTURE.md` (match the existing style), add the typed-filter follow-ups, sourced from `2026-06-16-query-typed-input-filters-design.md`:
- **Comparison / set operators** (`>`, `<`, `>=`, `<=`, `in`, ranges) — this slice is equality-only.
- **Richer filter error body** — report the expected type + offending value, not just the column name (part-1 reuses `BadFilter(col)`).
- **`422` for body-bearing endpoints** — reconsider `POST /actions` `BadParams` as `422` rather than `400` (typed filters are URI params on a body-less GET, so they correctly stay `400`).
- **Unify the coercion taxonomy** — share one repr-match between `params::parse_value` (JSON `Value`) and `filter::coerce_filter` (`&str`).

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/specs/2026-06-06-loom-roadmap.md docs/FUTURE.md
git commit -m "docs: typed input filters delivered; record follow-ups"
```

---

## Final Verification

- [ ] `buck2 build //src/... 2>&1 | tail -20` — clean.
- [ ] `buck2 test //src/... > /tmp/sweep.log 2>&1; grep -E "Tests finished|FAIL" /tmp/sweep.log` — `Fail 0`, including `filter-coerce`, `typed-filter-e2e`, and all existing read tests (`governed-read`, `link-traversal`, `derived-properties-e2e`, `multi-hop-traversal-e2e`, `http-smoke`, `sql-compile`).
- [ ] `tools/clippy-all.sh 2>&1 | tail -5` — clean.
- [ ] `git status` — no `Cargo.lock`/`third-party`/`.sqlx` drift (this slice touches no SQL cache or deps).
- [ ] **Run buck2 commands serially** — never a second buck2 invocation (or a commit whose hooks run buck2) while a `buck2 test //src/...` sweep runs.
- [ ] Markdown edits (Task 4) end with exactly one trailing newline, no trailing whitespace.

Then proceed to **superpowers:finishing-a-development-branch**.
