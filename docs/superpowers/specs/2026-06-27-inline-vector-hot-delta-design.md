# Inline (hot-tier) vector storage + real k-NN hot delta — design

- **Date:** 2026-06-27
- **Status:** draft (brainstorm → spec)
- **Register item:** `fut-inline-vector-hot-delta` (promotes a FUTURE item from
  `2026-06-27-puffin-vector-index-design`)
- **Area:** query
- **Relates to:** [[road-puffin-vector-index]], [[road-vector-column-type]],
  [[fut-puffin-vector-index-ann]]

## Motivation

[`road-puffin-vector-index`](2026-06-27-puffin-vector-index-design.md) shipped the
hot/cold k-NN **merge seam** — `vector_search` looks up the cold Puffin `FlatIndex`,
reads a hot inline delta (rows born after the index's covered snapshot `S`), and
merges them via the pure, unit-tested `merge_topk` — but the hot delta is **always
empty** for vector tables today. The reason is narrow and mechanical: loom's inline
(hot) tier cannot store a `vector(N)` column.

The whole query path already exists and is wired:

- `vector_search` already calls `inline_delta_batch(pool, table, S, Q)` and
  `merge_topk(cold, hot, k)` (`src/services/engine-serving/src/vector_search.rs:100`).
- `inline_delta_batch` is already written to read the inline vector column and build
  an Arrow `List<Float32>` batch (`src/control-plane/postgres/src/vector_index.rs`).
- `merge_topk` is built and covered by a pure unit test.

What is missing is only the inline **write/read of a `vector(N)` cell**. Two call
sites reject vectors before any cell encoding happens:

1. `iceberg_inline::inline_append` projects mirror columns via
   `iceberg_physical_type(&c.ty)` (`iceberg_inline.rs:191`), which returns `None`
   for `vector(N)` → hard `Backend("inline: no iceberg type for vector(4)")` error.
2. `iceberg_type::pg_type_for` has no `vector(N)` arm, so even past (1) there is no
   Postgres column type for inline storage.

The cold/Parquet path already solves the column-projection half
(`iceberg_landing.rs:478-503` special-cases `BaseType::Vector(n)`); inline simply was
never taught the same. This slice closes that gap and lands the deferred
**Acceptance 4** scenario (a row flushed inline between `S` and `Q`, merged exactly
once).

This is the second-half of a deliberately seam-first slice, not a redesign.

## Goals

- Inline tier can store and reconstruct a `vector(N)` column, round-trip exact on
  `f32`, so a small vector write lands as hot inline rows (not just cold Parquet).
- The k-NN hot delta lights up unchanged: `inline_delta_batch` returns real rows for
  vector tables, and `vector_search`'s existing cold∪hot merge serves them.
- Acceptance 4 is met: an engine-side k-NN query at `Q` returns the exact global
  top-k across cold ∪ hot — including a row landed inline (born after `S`) — with no
  double-count and no miss, for both Cosine and L2.

## Non-goals (explicitly out of scope)

- **No pgvector / ANN.** Storage stays loom-native typed rows; the cold index stays
  the Puffin `FlatIndex`. Approximate/ANN indexing is tracked separately
  ([[fut-puffin-vector-index-ann]]).
- **No change to the merge seam, the engine query path, or `merge_topk`.** They are
  already built; this slice only makes the hot delta non-empty.
- **Inline-delta identity stays `long` (i64).** `inline_delta_batch` reconstructs the
  identity column as `Int64`. Inline-delta hot reads for `integer`/`string` identity
  columns are a separate narrowing, not addressed here (the build's cold-path
  `extract_rows` already handles all three; only the inline-delta reader is i64-only).
  The acceptance test uses a `long` identity.
- **The `column_order` 0-vs-1 indexing discrepancy** between the inline projection
  (`order: i`) and the cold projection (`order: i + 1`) is **not** fixed here. It is
  orthogonal to vector support (inline-only tables are internally self-consistent and
  existing inline tests pass) and unverified as a real defect; it will be investigated
  separately and filed in ISSUES if confirmed.

## Storage encoding decision: Postgres `real[]` (float4 array)

The inline vector column is stored as a Postgres `real[]` (`float4[]`), i.e. an array
of native single-precision floats — the same representation pgvector uses for the raw
vector components (native f32, not JSON, not `numeric`).

Rationale (jsonb was the alternative; both are exact, `real[]` wins on the merits):

- **Exact by construction.** `real[]` stores the f32 bit pattern verbatim — no decimal
  detour, no signed-zero/NaN caveats. (jsonb is *also* exact for finite f32, because
  Postgres jsonb stores numbers as arbitrary-precision `numeric` and `f32→f64` is a
  lossless widening that ryu round-trips — but it has to be argued, and it normalizes
  `-0.0→0.0` and cannot encode NaN/±Inf.)
- **Compact.** Embeddings are high-dimensional; `real[]` is 4 bytes/element vs jsonb's
  ~15-20 bytes/element of decimal text. For a 1536-dim vector that is ~6 KB vs ~27 KB
  per row — and the hot tier holds recent un-flushed rows in Postgres.
- **Simpler reader.** sqlx decodes `real[]` straight to `Vec<f32>`
  (`try_get::<Vec<f32>>`), vs the manual `as_array()`/`as_f64()` jsonb walk.

The only thing jsonb had going for it — the existing `inline_delta_batch` reader
already decodes jsonb — is moot: that reader is **dead code today** (it returns `None`
for every vector table because nothing writes inline vectors), so switching it to
`real[]` carries no migration risk and touches a few lines.

### Correctness: hot/cold agreement

The property that matters for the merge is that a given vector scores **identically**
whether served cold (Puffin/Parquet f32) or hot (inline). `real[]` reconstructs the
identical `f32`, the hot and cold paths feed the *same* `control_plane_core::distance`
the *same* bits, so scores cannot diverge and `merge_topk` ranks consistently. This
is exactly the consistency the cold-only slice could only assert by unit test.

## Design

All changes are concentrated in two adapter files plus one reader function; the inline
write/read path uses **runtime** `AssertSqlSafe` queries (the inline table name is
dynamic), so there are **no compile-time `query!` changes and no `.sqlx` cache
regeneration** in this slice.

### 1. Column-type mapping (`iceberg_type.rs`)

- **`pg_type_for`** — add a vector arm returning the inline Postgres storage type:

  ```rust
  v if v.starts_with("vector(") => Some("real[]"),
  ```

  (The dimension `N` is not encoded in the Postgres column type; it is carried by the
  mirror `column_type` text `vector(N)` and enforced logically, mirroring how the
  cold path treats it. Postgres `real[]` is unsized.)

- **Shared mirror-type helper (de-dup with the cold path).** Both the cold projection
  (`iceberg_landing.rs:478-503`) and the inline projection (`iceberg_inline.rs:191`)
  need "logical type → mirror `column_type` string", special-casing `vector(N)`.
  Extract one helper so the two paths cannot drift:

  ```rust
  /// loom logical type -> the `iceberg_mirror.column.column_type` string.
  /// `vector(N)` is preserved verbatim (the dimension lives in this text);
  /// every other type maps to its Iceberg primitive name. `None` if unmapped.
  pub fn mirror_column_type(logical: &str) -> Option<String> {
      match control_plane_core::resolve_logical(logical) {
          Some(control_plane_core::BaseType::Vector(n)) => Some(format!("vector({n})")),
          _ => iceberg_physical_type(logical).map(str::to_string),
      }
  }
  ```

  The cold path's inline `match` is replaced by a call to this helper; the inline path
  replaces `iceberg_physical_type(&c.ty)` with `mirror_column_type(&c.ty)`. Read-back
  is unchanged and already correct: `IcebergCatalog::schema` maps `column_type`
  through `logical_from_iceberg` (`iceberg_catalog.rs:263`), which routes `vector(...)`
  to `resolve_logical` → `BaseType::Vector(n)` → `ty = "vector(N)"`.

### 2. Inline cell encode/bind/reconstruct (`iceberg_inline.rs`)

The inline path matches per-logical-type in four places; add a `vector(N)` arm to each,
keyed on `logical.starts_with("vector(")`. The dimension is not needed for storage
(it rides the schema); the arm only moves f32 components.

- **`Cell` enum** — new owned variant:

  ```rust
  /// A dense f32 vector cell, bound as Postgres real[] / decoded from it.
  Vec(Option<Vec<f32>>),
  ```

- **`cell_from_arrow`** — vector arm: downcast the column to `ListArray`, take
  `value(row)`, downcast its child to `Float32Array`, collect `Vec<f32>`. (Same
  downcast shape `inline_delta_batch` and the build's `extract_rows` already use.)
  A NULL list row → `Cell::Vec(None)`.

- **`bind_cell`** — `Cell::Vec(v) => q.bind(v.clone())`. sqlx binds `Option<Vec<f32>>`
  to `real[]` / SQL NULL natively.

- **`arrow_field`** — vector arm returns
  `DataType::List(Arc::new(Field::new("item", DataType::Float32, false)))`,
  matching the cold/Parquet field shape and `inline_delta_batch`'s reader.

- **`column_array`** (read-back) — vector arm: `try_get::<Option<Vec<f32>>>(i)` per
  row, build a `ListBuilder::new(Float32Builder::new()).with_field(item_field)`,
  appending each row's slice (or a null list). Produces the canonical
  `List<Float32>` array.

### 3. Hot-delta reader (`vector_index.rs::inline_delta_batch`)

Switch the vector decode from jsonb to `real[]` (the only behavioral change to an
existing function):

- Replace `let json_val: serde_json::Value = r.try_get(1)?;` + the `as_array()/as_f64()`
  walk with `let floats: Vec<f32> = r.try_get::<Vec<f32>, _>(1)?;`.
- The identity decode (`try_get::<i64>(0)`) and the `Int64` + `List<Float32>` Arrow
  construction are unchanged.

No other reader changes: `vector_search` and `merge_topk` consume the batch as-is.

## Data flow (end to end, once landed)

1. A small vector write lands inline: `inline_append` projects `column_type =
   "vector(N)"`, creates `inline_<tid>` with a `real[]` column, binds each row's
   `Vec<f32>` — a mirror-only snapshot+rows+lineage commit, born after `S`.
2. A k-NN query at `Q`: `vector_search` looks up the bound index (cold top-k from the
   Puffin `FlatIndex`), then `inline_delta_batch(pool, table, S, Q)` returns the rows
   born after `S` and alive at `Q` as a `List<Float32>` batch (hot), scored with the
   same `distance`.
3. `merge_topk(cold, hot, k)` concatenates and stable-sorts ascending → exact global
   top-k. A row landed inline between `S` and `Q` appears exactly once (it is in the
   hot delta and, by construction, not yet in the cold index).

## Testing

Mirrors the existing fixture-test style (`loom_fixture_test`, hermetic Postgres,
`PgFixture`); fixture tests run local (Postgres refuses RE-as-root).

1. **Inline vector round-trip (unit/integration, postgres crate).** Land an inline
   `vector(4)` row via `inline_append`; read it back via `inline_live_batch`; assert
   the reconstructed `List<Float32>` equals the input bit-exact (including a NULL-vector
   row → null list). This proves the write/read encode/decode independently of the
   engine.

2. **Acceptance 4 (engine-serving k-NN, cold ∪ hot).** Extend the vector_search
   fixture test (currently cold-only, with a "do NOT land inline" note):
   - Land cold vector rows, `build_vector_index` at `S`.
   - Land **additional** vector rows **inline** (born after `S`) — now possible.
   - Run k-NN at `Q > S` for both **Cosine** and **L2**; assert the returned top-k is
     the exact global top-k over cold ∪ hot, with **no double-count** (the inline row
     appears once) and **no miss** (a near inline row outranks a far cold row).
   - Include the "flushed between S and Q" case: a row present only in the hot delta is
     counted exactly once.

3. **Regression.** `buck2 test //src/...` stays green; existing serving / inline-union
   behaviour and defaults unchanged (Acceptance 6). The existing cold-only
   vector_search assertions still hold (hot delta empty when no inline rows exist).

## Risks & mitigations

- **sqlx `real[]` ↔ `Vec<f32>`** — array codec is built into `sqlx-postgres` (no extra
  feature); the inline path is runtime `AssertSqlSafe`, so it is exercised directly by
  the fixture tests, not gated on the `.sqlx` cache. *Mitigation:* the round-trip test
  (Testing 1) fails loudly if the codec or types are wrong.
- **NULL vector elements vs NULL vector** — embeddings are non-null dense vectors; the
  design treats the whole cell as nullable (`Option<Vec<f32>>`) but assumes non-null
  *elements* (`Vec<f32>`, not `Vec<Option<f32>>`). A vector with null elements is out
  of scope and would surface as a decode error, not silent corruption.
- **Identity type narrowing** — see non-goals; the inline-delta reader is i64-only.
  The test uses a `long` identity; a non-i64 inline identity would error in
  `inline_delta_batch`, which is acceptable and pre-existing.

## Register updates (at completion, via loom-docs-update)

- Close `fut-inline-vector-hot-delta` (status → promoted, this PR).
- If the `column_order` discrepancy is confirmed a defect during implementation, file
  a new `iss-` item rather than expanding this slice.
