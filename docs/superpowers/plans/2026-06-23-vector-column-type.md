# Vector column type — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development / executing-plans. Steps use `- [ ]` tracking.

**Spec:** [`docs/superpowers/specs/2026-06-23-vector-column-type-design.md`](../specs/2026-06-23-vector-column-type-design.md) (`road-vector-column-type`).

**Goal:** A governed, lineage-tracked vector column — the first parameterized, non-primitive loom type. Logical `BaseType::Vector(u32)` (f32, dim N); stored as Iceberg `list<float>` written as Arrow `List<Float32>`; N stashed in the list field's `doc` (`"vector(N)"`, round-trips through metadata.json — confirmed); served as a JSON float array. Storage not search.

**Key grounded facts (from research + API exploration):**
- Iceberg `list<float>` is canonical; iceberg-rust 0.9 has **no `FixedSizeList` path**; Parquet has no fixed-size list — so the write-boundary Arrow type is **`List<Float32>`**.
- `NestedField::list_element(id, Type::Primitive(Float), required)` + `ListType::new(Arc<elem>)` + `Type::List(..)`; `NestedField::with_doc("vector(N)")` **persists in metadata.json** (the N-carrier — no mirror migration).
- arrow conversion: `list<float>` → `DataType::List(Field "element": Float32, non-null, PARQUET:field_id)`.
- `columns_of` (`iceberg_mirror.rs:243-261`) `panic!`s on non-primitive — add a `Type::List` arm that reads N from `field.doc`.
- `read_files_as_batches(catalog, table, &[String]) -> (SchemaRef, Vec<RecordBatch>)`; `append_batches(catalog, table, batches)`.

## Global Constraints
- Tests are `rust_test`/`loom_fixture_test` only; no inline `#[test]`. No new SQL expected (the mirror `column_type` string carries `vector(N)` — no migration); run `sqlx-prepare.sh`, expect zero diff.
- No local buck2 (macOS); CI is the compiler. Reason carefully; push small.
- Primitive-column behavior and all defaults unchanged.
- Markdown: one trailing newline, no trailing whitespace.

## Task 1 — De-risk: `List<Float32>` iceberg round-trip ✅ (this push)
**File:** `src/control-plane/postgres/tests/vector_roundtrip.rs` (+ BUCK target). Build an iceberg `{id: long, embedding: list<float> doc "vector(4)"}` table, write a `List<Float32>` batch via `append_batches`, read back via `read_files_as_batches`, assert floats value-exact AND the field `doc` survived. No type-system changes. **Gate: this must go green before Task 2.**
- [ ] de-risk test green on CI

## Task 2 — core `BaseType::Vector(u32)`
**File:** `src/control-plane/core/src/logical_type.rs`. Add `Vector(u32)`; `as_str` emits `vector(N)`; `parse` accepts `vector(<u32>)` (reject `vector()`/`vector(x)`); `json_repr` → new `JsonRepr::FloatArray`. Fix every exhaustive `match BaseType` (compiler-guided): `ducklake_physical_type` (unsupported/None for Vector — DuckLake deferred), `iceberg_physical_type`/`logical_from_iceberg`, any stat-typing match.
- Pure-logic test: `vector(384)` codec round-trip; `json_repr`; malformed rejects.
- [ ] core + codec + JsonRepr; pure test; crate builds

## Task 3 — inference + iceberg schema/mirror
**Files:** `datafusion-io/src/infer.rs`; `postgres/src/iceberg_landing.rs` (`ice_schema`/`primitive_from`); `postgres/src/iceberg_mirror.rs` (`columns_of`); `iceberg_type.rs`.
- `infer.rs`: accept arrow `FixedSizeList(Float32,N)` **and** `List<Float32>` → `"vector(N)"`; `arrow_type_for("vector(N)")` → `List<Float32>`; a `normalize_vectors(batch, columns)` helper that converts any `FixedSizeList` vector column to `List<Float32>` and **validates every row's element count = N** (length mismatch → a deterministic landing error).
- `ice_schema`/`primitive_from`: build a `Type::List(float)` `NestedField` with `.with_doc("vector(N)")` for a `Vector(N)` column.
- `columns_of`: `Type::List` arm → read N from `field.doc`, emit `iceberg_type: "vector(N)"`; element via `list.element_field`. (Replaces the panic.)
- `iceberg_type.rs`: `iceberg_physical_type`/`logical_from_iceberg` learn the `vector(N)` ↔ list mapping (note: these key the closed read/write vocab — both halves needed).
- Stats: vector columns skipped in stat projection (no bound semantics).
- Fixture test: land a `vector(4)` column via the Iceberg landing path; mirror records `column_type = vector(4)`; no `data_file_column_stat` rows for it; read back value-exact.
- [ ] infer normalize/validate; ice_schema doc; columns_of list arm; iceberg_type; stats skip; tests

## Task 4 — serving + ontology + e2e + close
**Files:** `query-api/src/render.rs`; ontology bind/conformance; e2e.
- `render.rs`: serialize a `vector(N)` column as a JSON number array (`JsonRepr::FloatArray`); reject filter/sort on vector columns with a clear error.
- Ontology: a model property may declare `vector(N)`; the bind/conformance gate accepts it and validates a landed `vector(N)` column's dimension.
- **e2e (fixture):** land a typed object with `embedding: vector(4)` over the Iceberg backend; read it back through serving; assert the JSON row carries `[f32; 4]`; a width-mismatch land is rejected; lineage/snapshot advanced.
- Full `buck2 test //src/...` green; close `road-vector-column-type` in ROADMAP (`loom-docs-update`).
- [ ] serving FloatArray; ontology accept; e2e; full green; register closed

## Acceptance — see spec §"Acceptance criteria" (1–5).
