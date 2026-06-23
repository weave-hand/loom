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

### 2. Physical storage — Iceberg `list<float>`

Iceberg has no fixed-size-list; represent the vector as an Iceberg **`list<float>`**
(required `float`/f32 element). `N` is a loom-level constraint, enforced at landing
(reject a `FixedSizeList` whose width ≠ the declared `N`) and remembered in the mirror
`column_type = "vector(N)"` for read-back reconstruction. So:

- `ice_schema`/`primitive_from` learn to build a `Type::List(float)` field for a
  `Vector(N)` column (the one non-primitive `NestedField`).
- `columns_of` maps an Iceberg `list<float>` column back to `"vector(N)"` **using the
  declared `N` from the mirror column row** rather than the (length-free) Iceberg list
  — i.e. read the dimension from `column_type`, not the Iceberg schema. (Replaces the
  `panic!` at `iceberg_mirror.rs:256` with explicit list handling.)
- **No per-column stats** for vector columns (no min/max/null bound is meaningful);
  `project_files`/the pruner already treat a column with no usable stat as
  unprunable, so a vector column is simply skipped in stat projection.

### 3. Landing

`infer_columns` maps arrow **`FixedSizeList(Float32, N)`** → `"vector(N)"` and
`arrow_type_for("vector(N)")` → `FixedSizeList(Float32, N)`. The model-gate/landing
path accepts a `vector(N)` column, validates the wire `FixedSizeList` width equals the
model's declared `N` (a width mismatch is a deterministic bad-input error, like a
declared-but-absent column), and the existing DataFusion/iceberg Parquet write path
writes it (Parquet represents a fixed-size list as a `LIST` logical group).

> **Risk to resolve in the plan/first task:** confirm the arrow-57 iceberg writer
> chain (`write_parquet`) accepts a `FixedSizeList<Float32>` column mapped under an
> Iceberg `list<float>` field and round-trips it. If the writer rejects fixed-size
> lists, fall back to landing the column as an arrow `List<Float32>` (variable) with
> the loom-side `N` validation still enforced. The de-risk task writes a vector to
> real Parquet and reads it back **before** the type-system wiring.

### 4. Serving

`render.rs` serializes a `vector(N)` column as a JSON **array of numbers** from the
served `FixedSizeList`/`List<Float32>` column (`JsonRepr::FloatArray`). No precision
loss beyond f32. The vector is read-only data on the wire — no filtering/sorting on
vector columns (rejected with a clear error, consistent with deferred ANN).

### 5. Ontology

A model property may declare logical type `vector(N)`; the dataset→model bind gate
(`define`/conformance) accepts it and validates a landed `vector(N)` dataset column
against the declared dimension. No new ACL/lineage surface — a vector column is
governed and lineage-tracked exactly like any other column (it rides the snapshot).

## Surface

- `core`: `BaseType::Vector(u32)`; `as_str`/`parse`/`json_repr` extended; `JsonRepr::FloatArray`.
- `datafusion-io`: `infer.rs` handles `FixedSizeList(Float32, N)` ↔ `"vector(N)"`.
- `postgres`: `iceberg_type` + `ice_schema`/`primitive_from` build/decode `list<float>`;
  `iceberg_mirror::columns_of` handles the list column via the declared `N`; stats skipped.
- `query-api`: `render.rs` serializes a vector column to a JSON number array.

## Testing

`rust_test` (pure-logic where possible) + `loom_fixture_test` (hermetic Postgres +
object store):

- **Type codec (pure):** `vector(384)` round-trips `as_str`/`parse`; `json_repr` is
  `FloatArray`; a malformed `vector(x)`/`vector()` errors.
- **Inference (pure):** `FixedSizeList(Float32, 384)` ↔ `"vector(384)"` both directions.
- **De-risk write/read (fixture):** write a `vector(4)` column to real Parquet via the
  iceberg path and read it back as the same 4 floats (resolves the §3 risk first).
- **Landing + serving e2e (fixture):** land a small typed object with an `embedding:
  vector(4)` column over the Iceberg backend; read it back through the serving path and
  assert the JSON row carries the embedding as a `[f32; 4]` array; lineage/snapshot
  advanced. A width-mismatch land is rejected.
- **Stats skipped (fixture):** a table with a vector column projects no
  `data_file_column_stat` rows for it and still serves correctly (pruner unaffected).
- **Defaults unchanged:** the full suite stays green; primitive columns behave identically.

## Scope boundary

- **In:** `BaseType::Vector(u32)` + codecs/`JsonRepr`; arrow `FixedSizeList<Float32,N>`
  inference; Iceberg `list<float>` storage + mirror handling (stats skipped); serving
  JSON-array read-back; ontology `vector(N)` property acceptance; the tests above.
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
4. A width mismatch (wire `FixedSizeList` width ≠ declared `N`) is a deterministic
   bad-input rejection, not a silent store.
5. `buck2 test //src/...` is green; all primitive-column behavior and defaults unchanged.
