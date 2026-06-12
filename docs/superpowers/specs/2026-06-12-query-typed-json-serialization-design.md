# Design: query-path typed JSON serialization (Step 3, query — typed wire)

> **Status:** approved design (2026-06-12). The wire-rendering counterpart to the
> dataset→model binding (`2026-06-12-ingest-dataset-model-binding-design.md`). Binding made the
> ontology type authoritative for *physical* conformance; this slice makes the **governed read
> path render typed objects on the wire, driven by each property's logical type**. The
> logical-type vocabulary is the anchor: binding used it for logical↔physical; this slice uses
> it for logical↔JSON — the deferred axis the binding spec explicitly carved out.

## Goal

Make `read_object` return **typed objects**, not an untyped positional table. Each value renders
according to its property's declared **logical type** (the `core` vocabulary), so a client
gets `[{ "id": "1", "email": "a@b.com" }]` — values shaped by the ontology, not by whatever
scalar variant happened to survive the serving layer.

## Why this matters now

Two gaps make today's output untyped:

1. **The ontology type is never consulted for rendering.** `read_object` resolves the type for
   projection/ACL but returns `Rows { columns, rows }`; `http::rows_to_json` emits a positional
   `{"columns":[...],"rows":[[...]]}` envelope where each cell renders by its accidental
   `SqlValue` variant. A `Long` (int64) becomes a raw JSON number — **unsafe past 2⁵³**
   (≈9×10¹⁵). `id` columns and DuckLake `int64`s routinely exceed that; a silently-rounded ID
   is a correctness bug. This is exactly the precision wrinkle the binding spec flagged.
2. **The serving layer is lossy.** `serving::from_duck` collapses every non-int/text/bool
   DuckDB value — including **Double, Date, Timestamp** — to `SqlValue::Text(format!("{other:?}"))`,
   a Rust debug string. The faithful value is gone before it reaches JSON, so a `Double` can't
   render as a number nor a `Date` as ISO-8601 today.

This slice closes both: stop the serving layer from discarding type fidelity, and drive wire
rendering from each property's logical type.

## Output shape

A bound type is a **typed object**. The wire contract becomes an array of objects keyed by
property name, wrapped in a small envelope:

```json
{ "objects": [ { "id": "1", "email": "a@b.com" }, { "id": "2", "email": "c@d.com" } ] }
```

The `{ "objects": [...] }` envelope (rather than a bare top-level array) leaves room for a
paging cursor or schema sidecar later without another breaking change. Keys are the projected
property names (after ACL deny/mask), in ontology property order. The previous
`{"columns","rows"}` positional envelope is replaced.

## The logical-type → JSON rendering table

The vocabulary is the anchor. Each `BaseType` gets exactly one JSON rendering:

| `BaseType` | JSON rendering | example |
| --- | --- | --- |
| `Integer` (int32) | JSON number | `42` |
| `Long` (int64) | **JSON string** | `"9007199254740993"` |
| `Double` | JSON number | `3.14` |
| `Boolean` | JSON bool | `true` |
| `String` (+ aliases `EmailAddress`/`Url`/`PhoneNumber`) | JSON string | `"a@b.com"` |
| `Date` | ISO-8601 date string | `"2026-06-12"` |
| `Timestamp` | ISO-8601 datetime string | `"2026-06-12T14:09:42"` |
| any type, `NULL` cell | JSON `null` | `null` |

**`Long` → string is deliberate.** JSON numbers lose integer precision past 2⁵³; rendering
int64 as a string is the standard fix (protobuf-JSON, Twitter, Stripe all do it). `Integer`
(int32) stays a real number — it can't overflow. This is precisely why the binding vocabulary
keeps `Integer` and `Long` as distinct base types rather than one "int".

## Architecture & placement

Two homes, mirroring the binding slice's split.

### `control-plane-core` — the logical→JSON *policy* (pure, no new deps)

`logical_type.rs` gains the rendering classification. The `Long`-as-string decision lives here,
as data — `core` is the vocabulary authority:

```rust
/// How a logical type renders on the JSON wire.
pub enum JsonRepr {
    Number,         // Integer, Double  -> JSON number
    NumericString,  // Long             -> JSON string (>2^53 precision safety)
    Bool,           // Boolean          -> JSON bool
    PlainString,    // String + aliases -> JSON string
    IsoDate,        // Date             -> ISO-8601 date string
    IsoTimestamp,   // Timestamp        -> ISO-8601 datetime string
}

impl BaseType {
    pub fn json_repr(self) -> JsonRepr;
}

/// Resolve a logical type name (base or alias, case-insensitively) to its JSON repr.
/// `Err(UnknownLogicalType)` if loom doesn't know the type.
pub fn json_repr_of(logical_ty: &str) -> Result<JsonRepr, UnknownLogicalType>;
```

`core` stays pure: `JsonRepr` is a classification, not a `serde_json` value, so `core` needs no
JSON or `time` dependency. The mechanical construction happens where the scalar lives.

### `src/services/query-api` — the mechanical side

**(a) Stop the serving layer losing fidelity.** `SqlValue` grows the variants it currently
discards:

```rust
pub enum SqlValue {
    Text(String),
    Int(i64),
    Bool(bool),
    Double(f64),                       // NEW
    Date(time::Date),                  // NEW
    Timestamp(time::PrimitiveDateTime),// NEW
    Null,
}
```

- `from_duck` maps `Value::Double`/`Float` → `Double`, `Value::Date32` → `Date`,
  `Value::Timestamp` (and micro/nano variants) → `Timestamp`, instead of `Text(debug)`. A
  genuinely unhandled variant still falls back to `Text(debug)` (defensive, logged via the
  existing path) but the common scalar types are now faithful.
- `to_duck` and `render_literal` (the Quack inline-params path) get matching arms so `SqlValue`
  stays total across both serving engines.
- `time` is added to `query-api`'s `Cargo.toml` pinned `=0.3.47` (the documented control-plane
  pin — `time 0.3.48` conflicts with sqlx-core 0.9 under Rust 2024 orphan rules) with the
  `formatting` feature for ISO-8601; then `./tools/buckify.sh`.

**(b) Carry the logical type out of `read_object`.** The projected columns (after ACL
deny/mask) are computed inside `read_object`, so that is where each surviving column's logical
type is captured. The return type becomes rows aligned to a typed schema:

```rust
pub struct PropertyColumn { pub name: String, pub logical_ty: String }
pub struct ObjectRows { pub schema: Vec<PropertyColumn>, pub rows: Vec<Vec<SqlValue>> }
```

`schema` is built from `object_type.properties` filtered to the `allowed` projection, in
property order — the same order `compile_select` SELECTs — and paired by position with the
serving cells. `read_object` returns `ObjectRows` instead of `Rows`. (`serving::Rows` stays the
serving-engine result type; `read_object` zips its cells onto the projected schema.)

**(c) Render at the boundary (pure).** A new `render` module turns `ObjectRows` into the wire
JSON, so the governed core never touches `serde_json`:

```rust
pub fn objects_to_json(rows: &ObjectRows) -> serde_json::Value;
// { "objects": [ { name: render_cell(repr, cell), ... }, ... ] }
fn render_cell(repr: JsonRepr, cell: &SqlValue) -> serde_json::Value; // (JsonRepr, &SqlValue) -> Value
```

`http::get_object` calls `objects_to_json` in place of `rows_to_json`; the old `rows_to_json`
is removed.

### Flow

```
read_object: resolve type -> ACL -> project allowed columns -> compile SQL -> serving.fetch_rows
   │  build schema: allowed columns paired with their ObjectType property logical types
   ▼
ObjectRows { schema:[{name, logical_ty}], rows:[[SqlValue]] }
   │  objects_to_json (boundary)
   ▼  per row, per column: repr = json_repr_of(logical_ty); render_cell(repr, &cell)
{ "objects": [ { name: typed_value, ... } ] }
```

## Edge cases (render never 500s a permitted read)

- **`SqlValue::Null` → JSON `null`**, regardless of declared type.
- **Unknown logical type** on a persisted property (`json_repr_of` → `Err`): fall back to the
  cell's *natural* rendering (`Int`→number, `Double`→number, `Bool`→bool, `Text`→string,
  `Date`/`Timestamp`→ISO-8601, `Null`→null). `define_type` can still be called directly
  (pre-binding types, the memory adapter, tests), so render must tolerate an unrecognized type
  rather than fail the whole read.
- **Repr/value mismatch** (declared `Date`, cell arrives `Int` because the table evolved
  post-bind, or a `Long` repr over a `Text` cell): same natural-rendering fallback. This is the
  deferred schema-evolution / TOCTOU seam the binding spec already flagged — bind-time
  validation does not re-run at query time.
- **Masked columns:** the value is already redacted by `compile_select` (NULL/sentinel); the
  type still drives rendering, no special handling.

The "natural rendering" of a `SqlValue` is the single shared fallback used by both the
unknown-type and mismatch cases — defined once and applied whenever the declared repr can't be
honored for the concrete cell.

## Testing

**(a) `core` units** (`core/tests/logical_type.rs`, extend; pure, fast): `json_repr` for every
`BaseType` (`Long` → `NumericString`, `Integer`/`Double` → `Number`, `Boolean` → `Bool`,
`String` → `PlainString`, `Date` → `IsoDate`, `Timestamp` → `IsoTimestamp`); `json_repr_of`
resolves an alias (`EmailAddress` → `PlainString`, case-insensitively) and an unknown type →
`Err(UnknownLogicalType)`.

**(b) `query-api` render units** (new `tests/render.rs`, pure, no fixture): the full matrix,
asserting exact JSON —
- `Long` over `Int(9_007_199_254_740_993)` → `"9007199254740993"` (the >2⁵³ precision proof:
  the value is preserved exactly as a string, not a rounded number).
- `Integer` over `Int(42)` → `42`; `Double` over `Double(3.5)` → `3.5`; `Boolean` → bool;
  `String`/`EmailAddress` → string; `Date` → `"2026-06-12"`; `Timestamp` →
  `"2026-06-12T14:09:42"` (exact ISO-8601 strings).
- `Null` cell under each repr → `null`.
- Unknown logical type → natural rendering of the underlying cell; declared/value mismatch →
  natural rendering.
- `objects_to_json` over a multi-row/multi-column `ObjectRows` → the `{ "objects": [...] }`
  envelope with property-name keys in schema order.

**(c) e2e** (`query-api/tests/bind_read_e2e.rs`, adapt the existing
`landed_then_bound_dataset_is_queryable` — strengthened, not weakened): `materialize` a dataset
(seed widened to include a `Double`, a `Date`, and a `Timestamp` column alongside `id`/`email`)
→ `bind` a `Customer` type (`id` Long, `email` EmailAddress, plus the new typed properties) →
grant a Read ACL → `read_object` → `objects_to_json`. Assert the rendered JSON is
`{ "objects": [ {"id":"1", ...}, {"id":"2", ...} ] }` — `id` comes back as a **string** (Long
precision rule), and the `Double`/`Date`/`Timestamp` columns render as number/ISO-date/
ISO-datetime through the real DuckDB → `from_duck` path, proving the typed wire contract
end-to-end (not just against unit fakes).

## Verification

- `buck2 test //src/...` green; the render matrix passes; the e2e returns the typed
  `{ "objects": [...] }` JSON with `id` as a string and faithful temporal/double renderings.
- `tools/clippy-all.sh` clean; `buck2 run //tools:prek -- run --all-files` green (incl. the
  `.sqlx` freshness test — no schema change here, but the sweep runs it).

## Scope / non-goals

- **In:** `core`'s `JsonRepr` + `json_repr`/`json_repr_of`; the `SqlValue` fidelity fix
  (`Double`/`Date`/`Timestamp`); `ObjectRows`/`PropertyColumn` as `read_object`'s return; the
  pure `objects_to_json`/`render_cell` boundary with the `{ "objects": [...] }` envelope; the
  render matrix + the strengthened e2e.
- **Out (later slices):** typed *input* filters (query params still bind as `Text`); a schema
  sidecar (per-property logical type) in the response envelope; a paging cursor; timestamp-with-
  timezone rendering (this slice serves bare `timestamp`, matching binding); decimal/`Double`
  formatting policy beyond Rust's default; ongoing-conformance re-validation at query time.

## Open risks

- **DuckDB `Value` variant coverage.** `from_duck` must map the temporal/float variants the
  serving engine actually returns (`Date32`, the `Timestamp` unit variants, `Double`/`Float`).
  The e2e against the real pinned DuckDB is the backstop; the defensive `Text(debug)` fallback
  prevents a missed variant from panicking, and a missed mapping surfaces as a visibly-wrong
  string in the e2e rather than a silent corruption.
- **`time` formatting feature.** ISO-8601 rendering needs `time`'s `formatting` feature; the
  `=0.3.47` pin is reused so no new version conflict is introduced. Reindeer must pick up the
  feature — covered by `buckify` + the build.
- **Wire-contract break.** Replacing `{columns,rows}` with `{objects:[...]}` is a breaking
  change to the (pre-release, single-test) HTTP surface. Acceptable now; the only consumer is
  the e2e/test harness, updated in this slice.
