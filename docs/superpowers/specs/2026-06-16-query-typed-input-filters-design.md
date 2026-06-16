# Design: typed input filters (Step 3, query — read path)

> **Status:** approved design (2026-06-16). A cross-cutting fix to the governed read path. Today
> every query-param equality filter binds as `SqlValue::Text` (an explicit "Slice limitation" in
> `read_object`, single-hop traversal, and multi-hop chains), so a filter against a non-text column
> silently matches **nothing** (`?amount=100` binds `Text("100")`, which never equals a `Double`
> column). This slice coerces each filter value to its column's **declared ontology logical type**
> before binding, so filters on `Long`/`Double`/`Boolean`/`Date`/`Timestamp` columns actually work.

## Goal

Make query-param equality filters bind as their declared type. `GET /objects/Order?amount=100&active=true`
coerces `amount` → `Double(100.0)` and `active` → `Bool(true)` using each column's ontology logical
type, so the filters match real rows. The coercion is the **string-input analog** of the existing
action-body coercion (`params.rs::parse_value`), reusing the same `json_repr_of` → `JsonRepr`
taxonomy. Applies uniformly to the source-side equality filters of `read_object`, single-hop
traversal (`read_linked_objects`), and multi-hop chains (`read_linked_chain`) — they share one
filter path.

## Scope

**In scope:** type-aware coercion of equality (`col = value`) filters against the column's ontology
logical type, for every read path's source filters; a `400` on an uncoercible value; full coverage
of the existing logical-type taxonomy (Integer/Double, Long, Boolean, String, Date, Timestamp).

**NOT in scope (later slices):**
- **Comparison / set operators** (`>`, `<`, `>=`, `in`, ranges) — this slice is equality-only. The
  existing `eq_filters` shape (a flat `col = value` list) is unchanged; a richer operator surface
  is a separate slice.
- **Target-side / intermediate filters** in traversals/chains — filters still bind to the source
  type only (slice C part-2).
- **A richer error body** — an uncoercible value reuses the existing `BadFilter(col)` → `400` (body
  is the column name); a "why" message (expected-type / bad-value detail) is a trivial follow-up.
- **`422` for body-bearing endpoints** — `POST /actions/{name}`'s `BadParams` validation keeps its
  current `400`; switching body validation to `422` is a separate, out-of-scope decision. These
  filters are URI query params on a body-less `GET`, so `400` is the correct, uniform code here.
- **New landable column types** — `Date`/`Timestamp` filter coercion is included for completeness
  and parity with `parse_value`, but landing such columns is still gated on the deferred "Wider
  DataFusion ↔ DuckLake type coverage" item; the coercion is harmless when no such column exists.

## Design

### 1. The coercion unit (`src/services/query-api/src/filter.rs`)

A new pure-logic module with one function, the string-input mirror of `params.rs::parse_value`:

```rust
pub fn coerce_filter(name: &str, logical_ty: &str, raw: &str) -> Result<SqlValue, FilterError>;
```

It reuses the existing `control_plane_core::{json_repr_of, JsonRepr}` taxonomy — no new type system:

| `JsonRepr` (from `json_repr_of(logical_ty)`) | logical types | coercion of `raw: &str` |
|---|---|---|
| `Number` | Integer, Double | parse `i64` if it parses cleanly → `Int`; else parse `f64` → `Double`; else error |
| `NumericString` | Long | parse `i64` → `Int`; else error |
| `Bool` | Boolean | `"true"`→`Bool(true)`, `"false"`→`Bool(false)`; else error |
| `PlainString` | String | `Text(raw.to_string())` (cannot fail) |
| `IsoDate` | Date | `time::Date::parse` (same `[year]-[month]-[day]` format as `parse_value`) → `Date`; else error |
| `IsoTimestamp` | Timestamp | `time::PrimitiveDateTime::parse` (same format as `parse_value`) → `Timestamp`; else error |

```rust
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum FilterError {
    #[error("filter {0}: {1}")]
    BadValue(String, String),
}
```

Notes:
- **`Number` ambiguity is deliberate.** `json_repr_of` maps both Integer and Double to `Number`, so
  the function can't tell them apart from the repr alone. Coercing "try `i64`, else `f64`" is
  equality-correct regardless of which the column is: DuckDB compares `100 = 100.0` as equal, so a
  `Double` column filtered with `?x=100` (→ `Int(100)`) and an `Integer` column filtered with
  `?x=1.5` (→ `Double(1.5)`, won't match an int — correct) both behave correctly. Exact
  type-pinning would need a finer type API than `json_repr_of` exposes; not worth it for equality.
- An **unknown logical type** (`json_repr_of` returns `Err`) is treated as a coercion failure
  (`FilterError::BadValue`) — it indicates a malformed `PropertyDef`, surfaced as `400` like any
  bad filter rather than a `500`, mirroring `parse_value`'s handling.
- The module deliberately **duplicates** the short repr-match from `parse_value` (the two differ in
  input shape — `&str` query param vs JSON `Value`); unifying the shared taxonomy is a later
  cleanup, not part-1.

### 2. Filter representation (raw strings to the handler)

The query-input structs change their filter field from typed `SqlValue` to **raw `String`**, so
type coercion happens in the handler (which has the ontology), not at the dumb HTTP edge:

- `ObjectQuery.eq_filters`: `Vec<(String, SqlValue)>` → `Vec<(String, String)>`
- `LinkQuery.source_filters`: `Vec<(String, SqlValue)>` → `Vec<(String, String)>`
- `ChainQuery.source_filters`: `Vec<(String, SqlValue)>` → `Vec<(String, String)>`

The HTTP layer (`http.rs`) already holds `params: HashMap<String, String>`; it now forwards
`(key, value)` string pairs directly instead of wrapping each in `SqlValue::Text(...)`. (For chains,
`path` is still `remove`d from the params before the rest become source filters — unchanged.)

`compile_select` / `compile_traversal-via-chain` / `compile_chain` are **unchanged** — they still
receive a typed `&[(String, SqlValue)]` (the *coerced* filters) and bind them as `?` params.

### 3. Handler integration

In each read path (`read_object`, `read_linked_chain`), the existing per-filter validation loop
already checks each filter column is in the source's visible projection (in `allowed`, not in
`masked`) and `400`s (`BadFilter(col)`) otherwise. Extend that loop:

1. **Visibility check first** (unchanged): column in `allowed` and not `masked`, else
   `BadFilter(col)` → `400`. (A denied/masked column never reaches coercion — no type info leak.)
2. **Then coerce:** look up the column's `PropertyDef.ty` in the source type's `properties`, call
   `coerce_filter(col, ty, raw)`. On `Err` → `BadFilter(col)` → `400`. On `Ok(v)` collect
   `(col, v)`.
3. Pass the collected `Vec<(String, SqlValue)>` to the compiler exactly as before.

The visibility-then-coercion order is a governance property: coercion (which reflects a column's
declared type) only runs for columns the subject may already see.

### 4. Error handling

- **Uncoercible value** or **unknown logical type** on the column → `QueryError::BadFilter(col)` →
  `400` (body = column name). Reuses the existing variant; no new error type, no widening.
- **Unknown / denied / masked filter column** → `BadFilter(col)` → `400`, unchanged.
- Valid filters on text columns behave exactly as today (`Text(raw)`).

### File structure

- **Create:** `src/services/query-api/src/filter.rs` (`coerce_filter` + `FilterError`); register
  `mod filter;` in `lib.rs` (and `pub` as needed for the unit test).
- **Modify:** `src/services/query-api/src/handler.rs` (the three query structs' filter field type;
  the coercion step in `read_object` and `read_linked_chain`); `src/services/query-api/src/http.rs`
  (forward raw strings, drop the `SqlValue::Text` wrap in `get_object`/`get_linked`/`get_linked_chain`).
- **Tests:** `tests/filter_coerce.rs` (new unit `rust_test`); a new `tests/typed_filter_e2e.rs`
  (`loom_fixture_test`); update existing test sites that construct `eq_filters`/`source_filters`
  with `SqlValue::Text(...)` to pass raw strings.
- **BUCK:** add the `filter-coerce` `rust_test` target (and the e2e target if a new file).
- **Docs:** roadmap follow-up note (typed input filters delivered); `docs/FUTURE.md` (comparison
  operators remain).

## Testing

- **Unit (`tests/filter_coerce.rs`, pure `rust_test`):** each repr coerces correctly —
  `Integer "100"`→`Int(100)`, `Double "1.5"`→`Double(1.5)`, `Double "100"`→`Int(100)` (equality-ok),
  `Long "100"`→`Int(100)`, `Boolean "true"/"false"`→`Bool`, `String "x"`→`Text("x")`,
  `Date "2026-06-16"`→`Date`, `Timestamp "2026-06-16T12:00:00"`→`Timestamp`; and each failure is an
  `Err` — `Number "abc"`, `Bool "maybe"`, `Long "1.5"`, bad ISO date/timestamp, unknown logical type.
- **Governed e2e (`tests/typed_filter_e2e.rs`, `loom_fixture_test`):** land a table with a non-text column (e.g. `Order(id Long,
  amount Double, active Boolean)`), define the type, then through `read_object` filter `?amount=<v>`
  and `?active=true` and assert exactly the matching rows return — proving end-to-end coercion+bind
  works where a `Text` bind would return nothing. Include a `400` assertion for an uncoercible value
  (`?amount=abc`).
- **Regression:** the existing read e2es stay green after the `SqlValue::Text` → raw-string test-site
  updates (text-column filters behave identically).

## Decisions

- Coerce to the column's ontology logical type via `json_repr_of`/`JsonRepr`, reusing the
  `parse_value` taxonomy; **equality-only**; raw-string filter representation coerced in the handler;
  uncoercible value → `400` (`BadFilter`, no new variant); `Number` repr resolved as try-`i64`-else-`f64`.

## Follow-ups (later slices)

- **Comparison / set operators** (`>`, `<`, `>=`, `<=`, `in`, ranges) — a richer filter surface
  beyond equality.
- **Target-side / intermediate filters** in traversals and chains (slice C part-2).
- **Richer filter error body** — report the expected type and the offending value, not just the
  column name.
- **`422` for body-bearing endpoints** — reconsider `POST /actions` `BadParams` (and any future
  body-validated write) as `422` rather than `400`.
- **Unify the coercion taxonomy** — share one repr-match between `parse_value` (JSON `Value`) and
  `coerce_filter` (`&str`).

## Roadmap

Lands under Step 3 → Query, the read-path follow-ups — the first of the "smaller query follow-ups"
(typed input filters, schema sidecar, tz timestamps). It makes the filters already accepted by every
read path (`read_object`, traversal, chains) actually typed, unblocking real non-text filtering and
laying the groundwork for the later comparison-operator slice.
