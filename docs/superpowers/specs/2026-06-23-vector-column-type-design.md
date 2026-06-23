# Vector column type (`FixedSizeList<f32, N>`) — governed, lineage-tracked

_Design spec. 2026-06-23._

## Context

The Grimoire/personal-KG consumer wants loom to hold **embeddings as governed,
lineage-tracked, reproducible data** — vector columns stored alongside typed
objects, versioned per snapshot, read back through the governed serving path.
loom is explicitly **not** an ANN engine (vector *serving* stays in the external
hot tier) and **not** an embedder (an external process generates the vectors and
writes them in). This slice adds only the missing substrate: a first-class
**vector column type** that loom can land, store, version, and serve. The
embedding-generation Transform is a deliberate follow-on (A3b), not this slice.

This is the **first parameterized, non-primitive** loom column type. Every existing
type is a bare `BaseType` (`Integer`/`Long`/`Double`/`Boolean`/`String`/`Date`/
`Timestamp`) — a simple enum with no parameter, mapped 1:1 to each backend's
primitive and to a `JsonRepr`. A vector carries a **dimension `N`** and maps to a
*list/array* physical type, which the current code base assumes never happens (e.g.
`iceberg_mirror::columns_of` literally `panic!`s on a non-primitive Iceberg type,
`src/control-plane/postgres/src/iceberg_mirror.rs:256`). So the work is threading
one parameterized type through the type system end to end.

## Scope decision

- **Element type is fixed to `f32`.** Embeddings are float32; `FixedSizeList<f32, N>`
  is the only vector shape this slice introduces. Other element types (`f64`, `i8`
  quantized) are deferred.
- **Iceberg path only.** Storage + landing + serving target the **Iceberg** backend
  (the Iceberg-default direction). DuckLake vector support is a tracked follow-on
  (`fut-vector-ducklake`).
- **Storage, not search.** loom stores/serves the vector verbatim; no distance
  functions, no ANN index, no vector-predicate pushdown (the external tier does ANN).

## Current state (the seams to extend)

- **Logical type** (`src/control-plane/core/src/logical_type.rs:16`): `enum BaseType`
  (7 primitives) + `as_str`/`parse` (string codec) + `json_repr` (`JsonRepr`
  classification). No parameter, no list.
- **Arrow ↔ loom inference** (`src/services/datafusion-io/src/infer.rs`):
  `infer_columns` maps arrow `DataType` → loom type string; `arrow_type_for` the
  inverse. Primitives only; `Float32`/`FixedSizeList` unmapped today.
- **Iceberg schema build** (`iceberg_landing::ice_schema` + `primitive_from`,
  `src/control-plane/postgres/src/iceberg_landing.rs`): loom type → iceberg
  `PrimitiveType`. `iceberg_type::{iceberg_physical_type, logical_from_iceberg}`:
  loom ↔ iceberg primitive-name codec.
- **Mirror projection** (`iceberg_mirror.rs`): `columns_of` reads the iceberg schema
  to `ProjectedColumn.iceberg_type` (panics on non-primitive); `project_columns`
  stores `column_type` (a single string); per-column stats stored per file.
- **Serving** (`src/services/query-api/src/render.rs`): renders a served row to JSON
  by each column's `JsonRepr`.

## Decision

### 1. Logical type — `BaseType::Vector(u32)`

Add a parameterized variant carrying the dimension:

```rust
pub enum BaseType { Integer, Long, Double, Boolean, String, Date, Timestamp,
                    Vector(u32) }   // f32 element fixed; u32 is the dimension N
```

`BaseType` stays `Copy` (`u32` is `Copy`). String codec form is **`vector(N)`**
(e.g. `vector(384)`): `as_str` emits it, `parse` accepts `vector(<u32>)`. The
dimension travels in the type string, so **no new mirror/catalog column is needed**
to remember `N` — it round-trips through the existing `column_type`/`ty` strings.
`json_repr(Vector) = JsonRepr::FloatArray` (new variant) → a JSON array of numbers.

Every exhaustive `match BaseType` gains a `Vector` arm — this is the bounded ripple:
`ducklake_physical_type` (returns an error/None for `Vector` in this slice — DuckLake
deferred), `iceberg_physical_type`/`logical_from_iceberg`, `json_repr`, and any
stat-typing match. Each is a small, compiler-enforced addition.

### 2. Physical storage — Iceberg `list<float>`, written as Arrow **`List<Float32>`**

> **Research-grounded (see §Research).** Iceberg has **no** fixed-size-array/vector
> type in any spec version (v1/v2/v3) — `list<float>` is the canonical, de-facto
> community choice for embeddings. Critically for our stack: **iceberg-rust 0.9 has no
> `FixedSizeList` handling**, **Parquet has no native fixed-size list** (a
> `FixedSizeList` is written as a regular variable `LIST` and reads back variable), and
> `FixedSizeList` Parquet round-trips have a documented **null-loss hazard**. So the
> column fed to the iceberg writer is Arrow **`List<Float32>`**, not `FixedSizeList` —
> the variable-list path iceberg-rust supports and round-trips cleanly (it preserves
> `PARQUET:field_id` for the list element).

Represent the vector as an Iceberg **`list<float>`** (an `f32` element). `N` is a
**loom-level constraint**, not an Iceberg/Parquet one: enforced at landing (validate
each row's list length = the declared `N`) and remembered in the mirror
`column_type = "vector(N)"` for read-back reconstruction. So:

- `ice_schema`/`primitive_from` learn to build a `Type::List(float)` field for a
  `Vector(N)` column (the one non-primitive `NestedField`). The list element is
  `required` `float`; the column's own nullability is independent of the element's
  (Iceberg tracks the two separately) — a `vector(N)` column is non-null with non-null
  elements in this slice.
- `columns_of` maps an Iceberg `list<float>` column back to `"vector(N)"` **using the
  declared `N` from the mirror column row** (the Iceberg list is length-free) — read
  the dimension from `column_type`, not the Iceberg schema. (Replaces the `panic!` at
  `iceberg_mirror.rs:256` with explicit list handling.)
- **No per-column stats** for vector columns — confirmed by the spec: Iceberg
  `lower_bounds`/`upper_bounds` single-value serialization is defined only for
  primitives, so a `list<float>` column has no bound semantics. `project_files`/the
  pruner already treat a column with no usable stat as unprunable, so a vector column
  is simply skipped in stat projection.

### 3. Landing

The wire accepts a vector column as **either** Arrow `FixedSizeList(Float32, N)` (the
natural shape an embedding producer emits) **or** `List<Float32>`; both infer to
`"vector(N)"`. `infer_columns` maps both, and `arrow_type_for("vector(N)")` returns
`List<Float32>` (the storage shape). Landing validates **every row's element count =
the declared `N`** (a width/length mismatch is a deterministic bad-input error, like a
declared-but-absent column) and **normalizes the column to `List<Float32>`** before the
iceberg write path. Per the research, `List<Float32>` is the path the iceberg-rust
writer supports and Parquet round-trips cleanly; a `FixedSizeList` would be silently
rewritten as a variable `LIST` anyway (and carries the null-loss hazard), so loom
converts up front rather than relying on that implicit coercion.

> **De-risk first task (downgraded from a true risk by the research):** write a
> `List<Float32>` vector column to real Parquet through the arrow-57 iceberg writer
> chain (`write_parquet`) and read it back value-exact, **before** the type-system
> wiring. The research makes this a confirmation, not an open question — but it stays
> the first task because it's the one place arrow/parquet-57 behavior is load-bearing.

### 4. Serving

`render.rs` serializes a `vector(N)` column as a JSON **array of numbers** from the
served `List<Float32>` column (`JsonRepr::FloatArray`). No precision loss beyond f32.
The vector is read-only data on the wire — no filtering/sorting on vector columns
(rejected with a clear error, consistent with deferred ANN).

### 5. Ontology

A model property may declare logical type `vector(N)`; the dataset→model bind gate
(`define`/conformance) accepts it and validates a landed `vector(N)` dataset column
against the declared dimension. No new ACL/lineage surface — a vector column is
governed and lineage-tracked exactly like any other column (it rides the snapshot).

## Surface

- `core`: `BaseType::Vector(u32)`; `as_str`/`parse`/`json_repr` extended; `JsonRepr::FloatArray`.
- `datafusion-io`: `infer.rs` accepts `FixedSizeList(Float32,N)`|`List<Float32>` → `"vector(N)"`; `arrow_type_for` → `List<Float32>`; a normalize-to-`List<Float32>` + per-row length-validate helper.
- `postgres`: `iceberg_type` + `ice_schema`/`primitive_from` build/decode `list<float>`;
  `iceberg_mirror::columns_of` handles the list column via the declared `N`; stats skipped.
- `query-api`: `render.rs` serializes a vector column to a JSON number array.

## Testing

`rust_test` (pure-logic where possible) + `loom_fixture_test` (hermetic Postgres +
object store):

- **Type codec (pure):** `vector(384)` round-trips `as_str`/`parse`; `json_repr` is
  `FloatArray`; a malformed `vector(x)`/`vector()` errors.
- **Inference (pure):** both `FixedSizeList(Float32, 384)` and `List<Float32>` infer to
  `"vector(384)"`; `arrow_type_for("vector(384)")` is `List<Float32>`; normalize converts
  a `FixedSizeList` input to `List<Float32>`.
- **De-risk write/read (fixture):** write a `List<Float32>` `vector(4)` column to real
  Parquet via the iceberg path and read it back as the same 4 floats (the one
  arrow/parquet-57-load-bearing check; do it first).
- **Landing + serving e2e (fixture):** land a small typed object with an `embedding:
  vector(4)` column over the Iceberg backend; read it back through the serving path and
  assert the JSON row carries the embedding as a `[f32; 4]` array; lineage/snapshot
  advanced. A width-mismatch land is rejected.
- **Stats skipped (fixture):** a table with a vector column projects no
  `data_file_column_stat` rows for it and still serves correctly (pruner unaffected).
- **Defaults unchanged:** the full suite stays green; primitive columns behave identically.

## Scope boundary

- **In:** `BaseType::Vector(u32)` + codecs/`JsonRepr`; arrow `FixedSizeList<Float32,N>`|
  `List<Float32>` inference normalized to `List<Float32>`; Iceberg `list<float>` storage
  + mirror handling (stats skipped); serving JSON-array read-back; ontology `vector(N)`
  property acceptance; the tests above.
- **Out (deferred, tracked):** the embedding-generation Transform (**A3b**, the next
  slice); DuckLake vector storage (`fut-vector-ducklake`); non-`f32` element types;
  ANN / distance functions / vector-predicate pushdown / vector indexes (stays in the
  external hot tier — explicit non-goal); filtering/sorting on vector columns.

## Acceptance criteria

1. A model can declare a `vector(N)` property; a dataset with a `FixedSizeList<f32,N>`
   column binds to it (width validated) and lands over the Iceberg backend.
2. The landed vector is stored governed + lineage-tracked (rides the snapshot) and is
   read back through the serving path as a JSON array of `N` numbers, value-exact to f32.
3. A vector column carries no per-column stats and does not disturb file pruning or any
   primitive column's behavior.
4. A wire vector whose per-row element count ≠ declared `N` is a deterministic
   bad-input rejection, not a silent store.
5. `buck2 test //src/...` is green; all primitive-column behavior and defaults unchanged.

## Research

Findings from a multi-source, adversarially-verified research pass (2026-06-23) that
grounded the representation choice. The headline: `list<float>` is canonical, and the
Arrow write-boundary type must be `List<Float32>` (not `FixedSizeList`).

- **No Iceberg vector type, any version.** The Iceberg primitive set (incl. v3's new
  `variant`/`geometry`/`geography`/`unknown`/nanosecond-timestamps) has no
  array/vector/tensor primitive; `list`/`struct`/`map` are the only nested types. A
  dense `f32` vector is modeled as `list<float>` or a `fixed(4N)` blob — `list<float>`
  is the de-facto community choice. (Iceberg spec; Iceberg-v3 overview.)
- **iceberg-rust 0.9 supports variable `List` (incl. list-of-struct) and preserves
  `PARQUET:field_id`, but has no `FixedSizeList` path.** (iceberg-rust schema-conversion
  source / commit; PR #1928 auto-assign-ids.)
- **Parquet has no native fixed-size list.** Arrow `FixedSizeList` is written as a
  variable `LIST` and reads back variable, with extra conversion overhead; a
  `FIXED_SIZE_LIST` logical type was only a May-2024 *proposal*, never shipped. (Parquet
  dev list; arrow-rs #6733.)
- **`FixedSizeList` Parquet round-trips have caused real null-loss** (`[[1,2],null,[3,4]]`
  losing the null row) until a Dremel rep/def-level rewrite — a hazard `List<Float32>`
  avoids. (Polars #16608 / #16747.)
- **Stats undefined for list columns.** `lower_bounds`/`upper_bounds` single-value
  serialization is primitive-only — confirms skipping per-column stats for vectors.
  (Iceberg spec, Appendix D.)
- **Lance is the ANN escalation path**, not an Iceberg representation: keep canonical
  data in Iceberg, export to Lance (a distinct file/table/catalog format) for indexed
  vector search. Reinforces loom-stores-not-searches. (LanceDB/DuckDBLab.)
- Minor: arrow-rs names the list element `item` (legacy) vs the spec's `element`;
  opt-in coercion exists. Irrelevant to loom's mirror-based reads; matters only for
  raw-metadata external faithfulness (already the deferred `iss-iceberg-inline-visibility`
  class). (arrow-rs #6733 / #6828.)
