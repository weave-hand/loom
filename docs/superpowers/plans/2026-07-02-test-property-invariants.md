# Property tests at the parser/compiler/decoder seams — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add three pure-logic proptest `rust_test` targets that pin the high-value
invariants at the three seams where loom has *demonstrably* had the bug class —
the SQL compiler (injection/parameterization), the vector-index binary decoder
(panic/over-allocation on garbage), and the caller-string parsers (panic on
arbitrary input).

**Architecture:** Three new integration-test files, each wired as its own
`rust_test` target that depends on `//third-party:proptest`. No production code
changes — this is additive test-only coverage. Each file follows the existing
proptest idiom in `src/control-plane/core/tests/serde_roundtrip.rs` (strategy
functions returning `impl Strategy<Value = T>`, a `proptest! { ... }` block,
`prop_assert!`/`prop_assert_eq!`). All three targets are pure-logic (no
Postgres/fixture) so they route to remote execution and are cheap.

**Tech Stack:** Rust 2024, buck2 `rust_test` (via the `loom_rust_test` wrapper in
`//src:loom_test.bzl`, which exempts test code from the panic-safety clippy
lints), proptest 1.11.0 (`//third-party:proptest`).

## Global Constraints

- **Tests are `rust_test` integration targets in `tests/<name>.rs`, never inline
  `#[cfg(test)]` modules.** The `no-inline-tests` prek hook fails on any
  `#[test]`/`#[tokio::test]` in a first-party `src/**.rs` file.
- **Each new `rust_test` target uses the `rust_test` symbol loaded from
  `//src:loom_test.bzl`** (already the first `load(...)` line in both target
  BUCK files) — NOT a bare prelude `rust_test`. This injects the
  `LOOM_TEST_LINT_ALLOWS` so `.unwrap()`/`panic`/indexing are allowed in tests.
- **No production `src/**.rs` changes.** This item is additive test coverage only.
- **proptest is already vendored** as `//third-party:proptest` (third-party/BUCK).
  No `reindeer`/`Cargo.toml`/`third-party/BUCK` regeneration is needed — you only
  add BUCK `deps` entries.
- **Do not pipe `buck2 test` through `tail`/`head`.** Redirect to a file and grep:
  `buck2 test //src/services/query-api:<target> > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/t.log`.
- **Markdown lint:** the plan file itself must end with exactly one trailing
  newline and carry no trailing whitespace (the `lint` CI job checks `.md` too).
- **buck2 test placement:** these are pure-logic (no fixture) so a bare
  `buck2 test //src/services/query-api:<t>` / `//src/control-plane/core:<t>` runs
  fine on RE; no `loom_fixture_test` and no `--local-only` needed.

---

## Seam facts (reference — read before Task 1)

### SQL compiler (`src/services/query-api/src/sql.rs`, module `query_api::sql`)

Everything is `pub`; reach it from `tests/` as `query_api::sql::…`, `query_api::filter::…`,
`query_api::serving::…`. Entry point for flat SELECTs:

```rust
pub fn compile_select(
    table: &TableRef,
    allowed_cols: &[String],
    mask_cols: &[String],
    inputs: &SelectInputs<'_>,
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError>;
```

- `SelectInputs<'a>` (has `#[derive(Default)]`):
  `{ row_filters: &'a [RowFilter], predicates: &'a [CallerPredicate], or_groups: &'a [Vec<CallerPredicate>], derived: &'a [DerivedSelect] }`.
- `CallerPredicate { column: String, op: control_plane_core::CompareOp, values: Vec<SqlValue> }`
  (`query_api::filter::CallerPredicate`).
- `SqlValue { Text(String), Int(i64), Bool(bool), Double(f64), Date(time::Date), Timestamp(time::PrimitiveDateTime), Null }`
  (`query_api::serving::SqlValue`). The Task 1 generators only ever construct
  `Text`/`Int`/`Bool`.
- `TableRef { schema: String, name: String }` (from `control_plane_core`).
- Placeholder token is the literal `?` (the only production dialect,
  `DataFusionDialect`, renders `?` and ignores the index). Every operand is
  `params.push(v.clone())` then a `?` placeholder — values are NEVER formatted
  into the SQL string.
- Identifier quoting: `col_ref` → `quote_ident` → `format!("\"{}\"", id.replace('"', "\"\""))`.
  Every column/table identifier is double-quote-wrapped (both the `CallerPredicate`
  path via `caller_predicate_sql` and the `RowFilter` path via `filter_sql`).
- Per-op operand arity in `caller_predicate_sql` (returns `Err(CompileError::MalformedFilter)`
  — never panics — on arity mismatch):
  - `IsNull` / `IsNotNull` → 0 operands (0 placeholders)
  - `Between` → exactly 2 operands
  - `Contains` / `StartsWith` / `EndsWith` → exactly 1 operand (emits `ILIKE ? ESCAPE '\'`)
  - `In` / `NotIn` → N operands (N placeholders)
  - all others (`Eq`,`Ne`,`Lt`,`Le`,`Gt`,`Ge`) → exactly 1 operand
- Compile-time constants the compiler itself inlines (whitelist when asserting
  "no verbatim value"): `'***'` (mask marker), the `ESCAPE '\'` token, the fixed
  operator/keyword tokens. None contain `?`.
- `compile_select` runs `control_plane_core::validate_row_filter` on each
  `row_filter` up front, so feeding a `RowFilter` that violates the
  CompareOp↔ScalarValue invariant returns `Err`, never a panic.
- **Caveat:** `query_api::serving::inline_params(sql, params)` is a SEPARATE
  wire-path step that deliberately un-parameterizes (substitutes `?` with escaped
  literals). Do NOT run the injection property against `inline_params` — scope it
  to `compile_select`'s `(sql, params)` output.

### Vector-index codec (`src/control-plane/core/src/vector_index/`, re-exported at crate root)

```rust
pub use vector_index::{FlatIndex, HnswIndex, IndexKind, IndexSpec, IvfFlatIndex,
    Metric, VectorIndex, VectorKey, decode, distance};

pub fn decode(bytes: &[u8]) -> Result<Box<dyn VectorIndex>>;   // routes on kind byte at offset 6
impl FlatIndex     { pub fn build(dim: u32, metric: Metric, rows: Vec<(VectorKey, Vec<f32>)>) -> Result<FlatIndex>;
                     pub fn deserialize(bytes: &[u8]) -> Result<FlatIndex>; }
impl IvfFlatIndex  { pub fn build(dim: u32, metric: Metric, rows: Vec<(VectorKey, Vec<f32>)>, nlist: Option<u32>) -> Result<IvfFlatIndex>;
                     pub fn deserialize(bytes: &[u8]) -> Result<IvfFlatIndex>; }
impl HnswIndex     { pub fn build(dim: u32, metric: Metric, rows: Vec<(VectorKey, Vec<f32>)>, m: Option<u32>, ef_construction: Option<u32>) -> Result<HnswIndex>;
                     pub fn deserialize(bytes: &[u8]) -> Result<HnswIndex>; }
```

- `VectorKey { Int(i64), Str(String) }`, `Metric { Cosine, L2 }`.
- `Result<T> = std::result::Result<T, ControlPlaneError>`; `ControlPlaneError` is
  `#[non_exhaustive]` and NOT `PartialEq` — assert `is_err()`/`is_ok()`, never
  equate error values.
- The three index structs derive only `Clone, Debug` (NO `PartialEq`, private
  fields) → compare round-trips **by serialized bytes**:
  `x.serialize() == decode(&x.serialize()).unwrap().serialize()`. Build is
  deterministic (fixed seeds) → serialization is byte-exact.
- `.serialize()` on `IvfFlatIndex`/`HnswIndex` is only the trait method →
  `use control_plane_core::VectorIndex;` must be in scope. `decode` returns a
  boxed trait object, so `.serialize()` on it always resolves.
- `build` requires every vector's length == `dim`, else returns `Err`. Empty
  `rows` is valid and round-trips for all three types.
- Over-allocation guard (`read_f32_section`): a declared section length exceeding
  the reader's `remaining()` bytes → `Err`, before any `with_capacity`. All
  primitive reads are bounds-checked, so `decode(arbitrary bytes)` returns `Err`
  gracefully rather than panicking. Flat header layout: magic+version+metric+kind
  = 7 bytes (kind at offset 6), then `dim: u32` at bytes 7..11, then
  `row_count: u32` at bytes 11..15.

### Caller-string parsers (`src/services/query-api/src/{path_parse,filter}.rs`)

All `pub`. There is no function literally named `path_parse`/`filter_coerce` —
those are module/test names. Entry points:

```rust
// query_api::path_parse
pub fn parse_path_hops(param: &str) -> Vec<Hop>;                              // infallible
pub fn parse_direction(raw: Option<&str>) -> Result<Direction, InvalidDirection>;
pub fn parse_graph_mode(path: Vec<Hop>, links: Vec<String>, tree: bool) -> Result<GraphMode, String>;
// query_api::filter
pub fn coerce_filter(name: &str, logical_ty: &str, raw: &str) -> Result<SqlValue, FilterError>;
pub fn coerce_predicate(column: &str, logical_ty: &str, raw: &str) -> Result<CallerPredicate, FilterError>;
pub fn split_or_members(raw: &str) -> Result<Vec<String>, FilterError>;
pub fn split_member(member: &str) -> Result<(&str, &str), FilterError>;
```

- `Hop { link: String, direction: Direction }` and
  `Direction { Forward, Inverse }` come from `query_api::handler`; `Hop::from(&str)`
  yields a Forward hop.
- `coerce_filter`/`coerce_predicate` take three `&str` — no ontology/`BaseType`
  object. `logical_ty` is a plain type-name string (`"Integer"`, `"Long"`,
  `"Double"`, `"Boolean"`, `"String"`, `"Date"`, `"Timestamp"`, aliases
  `"emailaddress"`/`"url"`/`"phonenumber"`, or `"vector(N)"`); an unknown name
  yields a graceful `Err`, never a panic.
- `FilterError` and `InvalidDirection` derive `Debug + PartialEq`.
- `parse_path_hops` drops empty/whitespace-only elements (split on `,`, trim,
  strip leading `~` inverse sigil) → every returned `Hop` has a non-empty `link`.

### The proptest idiom (mirror `src/control-plane/core/tests/serde_roundtrip.rs`)

```rust
use proptest::prelude::*;

fn some_strategy() -> impl Strategy<Value = T> { /* prop_oneof!/prop_map/prop_recursive */ }

proptest! {
    #[test]
    fn some_property(x in some_strategy()) {
        prop_assert!(/* invariant on x */);
    }
}
```

`proptest!` catches panics in the body and shrinks to a minimal failing input, so
a body that merely *calls* the function under test already proves "never panics";
where a stronger structural invariant is cheap, assert it too.

---

## File structure

- Create `src/services/query-api/tests/sql_compile_props.rs` — Task 1.
- Modify `src/services/query-api/BUCK` — add the `sql-compile-props` target (Task 1)
  and the `path-parse-props` target (Task 3).
- Create `src/control-plane/core/tests/vector_index_decode_props.rs` — Task 2.
- Modify `src/control-plane/core/BUCK` — add the `vector-index-decode-props` target (Task 2).
- Create `src/services/query-api/tests/path_parse_props.rs` — Task 3.
- Modify `docs/ROADMAP.md` — close the register item (Task 4).

---

### Task 1: `sql_compile_props` — SQL compiler injection/parameterization invariants

**Files:**
- Create: `src/services/query-api/tests/sql_compile_props.rs`
- Modify: `src/services/query-api/BUCK` (add `sql-compile-props` `rust_test`)

**Interfaces:**
- Consumes: `query_api::sql::{compile_select, SelectInputs}`,
  `query_api::filter::CallerPredicate`, `query_api::serving::SqlValue`,
  `control_plane_core::{TableRef, CompareOp, RowFilter, ScalarValue}`.
- Produces: nothing (leaf test target).

**Design of the generators (the key to robust, non-flaky assertions):**

- Identifiers (table schema/name, predicate columns) are generated with a leading
  private-use marker char `'\u{E001}'` followed by a safe ASCII tail
  (`[a-z0-9_]{0,6}`). This char is emitted by the compiler ONLY as the first char
  of a quoted identifier, so "every identifier is quote-wrapped" ⇔ "every
  `'\u{E001}'` in the SQL is immediately preceded by a `"`". Collision-free (the
  marker never appears in constants, operators, or values).
- Text operand values carry a *different* leading marker `'\u{E002}'` then
  arbitrary junk (may include `'`, `?`, `;`, `--`, quotes — realistic injection
  payloads). Since operands are always parameterized, the SQL must not contain
  `'\u{E002}'` → that is the injection property.
- Operand arity per op is generated correctly so `compile_select` returns `Ok`
  and the SQL-emitting happy path is exercised.

- [ ] **Step 1: Write the failing test file**

Create `src/services/query-api/tests/sql_compile_props.rs`:

```rust
//! Property tests for the query-api SQL compiler (`compile_select`). Pins the three
//! load-bearing invariants at the injection boundary: (1) every emitted `?`
//! placeholder is backed by exactly one bind param, (2) every emitted identifier is
//! quote-wrapped, (3) no caller-supplied value ever appears verbatim in the emitted
//! SQL (values are always parameterized). Generators tag identifiers with a
//! private-use marker `\u{E001}` and text operands with `\u{E002}` so the assertions
//! are collision-free. See docs/superpowers/specs/2026-07-02-pillar-idioms-audit-design.md.

use control_plane_core::{CompareOp, TableRef};
use proptest::prelude::*;
use query_api::filter::CallerPredicate;
use query_api::serving::SqlValue;
use query_api::sql::{SelectInputs, compile_select};

/// Marks the first char of every generated identifier. The compiler emits it ONLY
/// as the first char inside a quoted identifier, never in a constant/operator/value.
const ID_MARK: char = '\u{E001}';
/// Marks the first char of every generated text operand. Always parameterized, so
/// it must never surface in the emitted SQL.
const VAL_MARK: char = '\u{E002}';

/// Identifier: ID_MARK + safe ascii tail (no `?`, no `"`, no markers).
fn ident() -> impl Strategy<Value = String> {
    "[a-z0-9_]{0,6}".prop_map(|tail| format!("{ID_MARK}{tail}"))
}

/// A text operand value carrying VAL_MARK, with arbitrary (possibly SQL-dangerous)
/// suffix. Filters the two marker chars out of the suffix so the markers stay unique
/// (filter, not a regex char-class, to avoid depending on unicode-escape support).
fn marked_text() -> impl Strategy<Value = SqlValue> {
    any::<String>().prop_map(|s| {
        let suffix: String = s.chars().filter(|c| *c != ID_MARK && *c != VAL_MARK).take(16).collect();
        SqlValue::Text(format!("{VAL_MARK}{suffix}"))
    })
}

/// Non-text scalar operands (Int/Bool/Double). No marker needed — the injection
/// property is asserted only on marked text; these still count toward placeholders.
fn scalar_operand() -> impl Strategy<Value = SqlValue> {
    prop_oneof![
        marked_text(),
        any::<i64>().prop_map(SqlValue::Int),
        any::<bool>().prop_map(SqlValue::Bool),
    ]
}

/// A well-formed CallerPredicate: op paired with the exact operand arity it needs,
/// so `compile_select` returns Ok and emits SQL.
fn predicate() -> impl Strategy<Value = CallerPredicate> {
    let scalar_ops = prop_oneof![
        Just(CompareOp::Eq), Just(CompareOp::Ne), Just(CompareOp::Lt),
        Just(CompareOp::Le), Just(CompareOp::Gt), Just(CompareOp::Ge),
        Just(CompareOp::Contains), Just(CompareOp::StartsWith), Just(CompareOp::EndsWith),
    ];
    let scalar_pred = (ident(), scalar_ops, scalar_operand())
        .prop_map(|(column, op, v)| CallerPredicate { column, op, values: vec![v] });
    let null_pred = (ident(), prop_oneof![Just(CompareOp::IsNull), Just(CompareOp::IsNotNull)])
        .prop_map(|(column, op)| CallerPredicate { column, op, values: vec![] });
    let between_pred = (ident(), scalar_operand(), scalar_operand())
        .prop_map(|(column, a, b)| CallerPredicate { column, op: CompareOp::Between, values: vec![a, b] });
    let set_pred = (
        ident(),
        prop_oneof![Just(CompareOp::In), Just(CompareOp::NotIn)],
        prop::collection::vec(scalar_operand(), 1..4),
    )
        .prop_map(|(column, op, values)| CallerPredicate { column, op, values });
    prop_oneof![scalar_pred, null_pred, between_pred, set_pred]
}

fn table() -> impl Strategy<Value = TableRef> {
    (ident(), ident()).prop_map(|(schema, name)| TableRef { schema, name })
}

/// Count of `?` placeholders in the SQL. Safe because identifiers carry no `?` and
/// values are parameterized, so every `?` is a placeholder.
fn placeholder_count(sql: &str) -> usize {
    sql.matches('?').count()
}

proptest! {
    /// Property 1: one bind param per emitted `?` placeholder.
    #[test]
    fn placeholder_count_matches_param_count(
        table in table(),
        cols in prop::collection::vec(ident(), 1..4),
        preds in prop::collection::vec(predicate(), 0..5),
    ) {
        let inputs = SelectInputs { predicates: &preds, ..SelectInputs::default() };
        let (sql, params) = compile_select(&table, &cols, &[], &inputs, 100)
            .expect("well-formed predicates compile");
        prop_assert_eq!(placeholder_count(&sql), params.len());
    }

    /// Property 2: every emitted identifier is quote-wrapped — each ID_MARK in the
    /// SQL is immediately preceded by a `"`.
    #[test]
    fn every_identifier_is_quote_wrapped(
        table in table(),
        cols in prop::collection::vec(ident(), 1..4),
        preds in prop::collection::vec(predicate(), 0..5),
    ) {
        let inputs = SelectInputs { predicates: &preds, ..SelectInputs::default() };
        let (sql, _params) = compile_select(&table, &cols, &[], &inputs, 100)
            .expect("well-formed predicates compile");
        let chars: Vec<char> = sql.chars().collect();
        for (i, c) in chars.iter().enumerate() {
            if *c == ID_MARK {
                prop_assert!(i > 0 && chars[i - 1] == '"',
                    "identifier marker not preceded by a quote at {i} in: {sql}");
            }
        }
    }

    /// Property 3 (injection): no caller-supplied text value appears verbatim in the
    /// emitted SQL — the VAL_MARK never surfaces.
    #[test]
    fn caller_values_never_verbatim(
        table in table(),
        cols in prop::collection::vec(ident(), 1..4),
        preds in prop::collection::vec(predicate(), 0..5),
    ) {
        let inputs = SelectInputs { predicates: &preds, ..SelectInputs::default() };
        let (sql, _params) = compile_select(&table, &cols, &[], &inputs, 100)
            .expect("well-formed predicates compile");
        prop_assert!(!sql.contains(VAL_MARK),
            "a caller value leaked verbatim into: {sql}");
    }

    /// Robustness: arbitrary (possibly invariant-violating) RowFilter governance
    /// trees fed as row_filters never panic — compile_select validates up front and
    /// returns Ok or Err.
    #[test]
    fn arbitrary_row_filters_never_panic(f in arb_row_filter()) {
        let filters = [f];
        let inputs = SelectInputs { row_filters: &filters, ..SelectInputs::default() };
        let res = compile_select(
            &TableRef { schema: "s".into(), name: "t".into() },
            &["c".into()],
            &[],
            &inputs,
            10,
        );
        prop_assert!(res.is_ok() || res.is_err());
    }
}

/// Arbitrary RowFilter tree (may violate the CompareOp<->ScalarValue invariant on
/// purpose, to exercise compile_select's validate-before-emit path).
fn arb_row_filter() -> impl Strategy<Value = control_plane_core::RowFilter> {
    use control_plane_core::{CompareOp, RowFilter, ScalarValue};
    let op = prop_oneof![
        Just(CompareOp::Eq), Just(CompareOp::Ne), Just(CompareOp::Lt),
        Just(CompareOp::Le), Just(CompareOp::Gt), Just(CompareOp::Ge),
        Just(CompareOp::In), Just(CompareOp::NotIn),
        Just(CompareOp::IsNull), Just(CompareOp::IsNotNull),
    ];
    let value = prop_oneof![
        any::<String>().prop_map(ScalarValue::Text),
        any::<i64>().prop_map(ScalarValue::Int),
        any::<bool>().prop_map(ScalarValue::Bool),
        prop::collection::vec(any::<i64>().prop_map(ScalarValue::Int), 0..3).prop_map(ScalarValue::List),
    ];
    let leaf = (".*", op, value).prop_map(|(property, op, value)| RowFilter::Compare { property, op, value });
    leaf.prop_recursive(3, 16, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(RowFilter::And),
            prop::collection::vec(inner.clone(), 0..4).prop_map(RowFilter::Or),
            inner.prop_map(|f| RowFilter::Not(Box::new(f))),
        ]
    })
}
```

- [ ] **Step 2: Add the BUCK target**

In `src/services/query-api/BUCK`, mirror the existing `sql-compile` target (add
after it), adding the proptest dep:

```python
rust_test(
    name = "sql-compile-props",
    crate = "sql_compile_props",
    srcs = ["tests/sql_compile_props.rs"],
    crate_root = "tests/sql_compile_props.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
        "//third-party:proptest",
    ],
)
```

- [ ] **Step 3: Run the test and confirm it passes**

Run:
```bash
buck2 test //src/services/query-api:sql-compile-props > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL|PASS|error\[|panicked" /tmp/t1.log
```
Expected: `Tests finished:` with pass counts, no `FAIL`. If a property fails,
proptest prints a shrunk minimal case — treat it as a real finding (see
"Handling a failing property" below), do NOT weaken the assertion to hide it.

- [ ] **Step 4: Lint the new test target**

Run:
```bash
buck2 build '//src/services/query-api:sql-compile-props[clippy.txt]' > /tmp/c1.log 2>&1; cat /tmp/c1.log
```
Expected: builds clean (empty clippy output). The `loom_rust_test` wrapper allows
test panics; fix any real clippy finding.

- [ ] **Step 5: Commit**

```bash
git add src/services/query-api/tests/sql_compile_props.rs src/services/query-api/BUCK
git commit -m "test(query-api): property tests for the SQL compiler injection invariants"
```

---

### Task 2: `vector_index_decode_props` — codec never-panic + round-trip invariants

**Files:**
- Create: `src/control-plane/core/tests/vector_index_decode_props.rs`
- Modify: `src/control-plane/core/BUCK` (add `vector-index-decode-props` `rust_test`)

**Interfaces:**
- Consumes: `control_plane_core::{decode, FlatIndex, IvfFlatIndex, HnswIndex,
  Metric, VectorKey, VectorIndex}`.
- Produces: nothing (leaf test target).

- [ ] **Step 1: Write the failing test file**

Create `src/control-plane/core/tests/vector_index_decode_props.rs`:

```rust
//! Property tests for the vector-index binary codec. Two invariants at the seam
//! where loom has demonstrably had the bug class (the HNSW/Flat/IVF deserialize
//! -bounds fixes): (1) `decode(arbitrary bytes)` never panics or over-allocates —
//! it returns Ok or a graceful Err; (2) `decode(encode(x)) == x` round-trips
//! byte-exactly for every buildable index. See
//! docs/superpowers/specs/2026-07-02-pillar-idioms-audit-design.md.

use control_plane_core::{FlatIndex, HnswIndex, IvfFlatIndex, Metric, VectorIndex, VectorKey, decode};
use proptest::prelude::*;

fn metric() -> impl Strategy<Value = Metric> {
    prop_oneof![Just(Metric::Cosine), Just(Metric::L2)]
}

fn vector_key() -> impl Strategy<Value = VectorKey> {
    prop_oneof![
        any::<i64>().prop_map(VectorKey::Int),
        ".{0,8}".prop_map(VectorKey::Str),
    ]
}

/// Finite f32 in a bounded range — avoids NaN/inf ordering hazards in kmeans/HNSW
/// build while still round-tripping byte-exactly.
fn coord() -> impl Strategy<Value = f32> {
    (-1000.0f32..1000.0f32)
}

/// `(dim, rows)` where every vector has length == dim (build's precondition).
fn dim_and_rows() -> impl Strategy<Value = (u32, Vec<(VectorKey, Vec<f32>)>)> {
    (1usize..=8).prop_flat_map(|dim| {
        let row = (vector_key(), prop::collection::vec(coord(), dim..=dim));
        prop::collection::vec(row, 0..6).prop_map(move |rows| (dim as u32, rows))
    })
}

proptest! {
    /// Property 1a: decode of arbitrary bytes never panics; on the rare Ok, the
    /// decoded index re-serializes to bytes that decode again (idempotent).
    #[test]
    fn decode_arbitrary_bytes_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..2048)) {
        match decode(&bytes) {
            Ok(idx) => {
                let re = idx.serialize();
                prop_assert!(decode(&re).is_ok(), "re-decode of a decoded index failed");
            }
            Err(_) => {}
        }
    }

    /// Property 1b: the per-type deserializers also never panic on garbage.
    #[test]
    fn per_type_deserialize_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..2048)) {
        let _ = FlatIndex::deserialize(&bytes);
        let _ = IvfFlatIndex::deserialize(&bytes);
        let _ = HnswIndex::deserialize(&bytes);
        prop_assert!(true);
    }

    /// Property 1c (over-allocation guard): a valid Flat header whose row_count is
    /// poisoned to a huge value must be rejected (Err), not over-allocate/panic.
    #[test]
    fn flat_oversized_row_count_is_rejected(
        (dim, rows) in dim_and_rows(),
        huge in 1_000_000u32..=u32::MAX,
    ) {
        let idx = FlatIndex::build(dim, Metric::Cosine, rows).expect("build");
        let mut bytes = idx.serialize();
        // Flat layout: header (7 bytes, kind at 6) | dim: u32 @7..11 | row_count: u32 @11..15.
        prop_assume!(bytes.len() >= 15);
        bytes[11..15].copy_from_slice(&huge.to_le_bytes());
        prop_assert!(decode(&bytes).is_err(), "oversized row_count was not rejected");
    }

    /// Property 2: byte-exact round-trip for FlatIndex.
    #[test]
    fn flat_round_trips((dim, rows) in dim_and_rows(), m in metric()) {
        let idx = FlatIndex::build(dim, m, rows).expect("build");
        let bytes = idx.serialize();
        let back = decode(&bytes).expect("decode of our own bytes");
        prop_assert_eq!(back.serialize(), bytes);
    }

    /// Property 2: byte-exact round-trip for IvfFlatIndex.
    #[test]
    fn ivf_round_trips((dim, rows) in dim_and_rows(), m in metric()) {
        let idx = IvfFlatIndex::build(dim, m, rows, None).expect("build");
        let bytes = idx.serialize();
        let back = decode(&bytes).expect("decode of our own bytes");
        prop_assert_eq!(back.serialize(), bytes);
    }

    /// Property 2: byte-exact round-trip for HnswIndex.
    #[test]
    fn hnsw_round_trips((dim, rows) in dim_and_rows(), m in metric()) {
        let idx = HnswIndex::build(dim, m, rows, None, None).expect("build");
        let bytes = idx.serialize();
        let back = decode(&bytes).expect("decode of our own bytes");
        prop_assert_eq!(back.serialize(), bytes);
    }
}
```

- [ ] **Step 2: Add the BUCK target**

In `src/control-plane/core/BUCK`, mirror the existing `serde-roundtrip` target:

```python
rust_test(
    name = "vector-index-decode-props",
    crate = "vector_index_decode_props",
    srcs = ["tests/vector_index_decode_props.rs"],
    crate_root = "tests/vector_index_decode_props.rs",
    edition = "2024",
    visibility = ["PUBLIC"],
    deps = [
        ":core",
        "//third-party:proptest",
    ],
)
```

- [ ] **Step 3: Run the test and confirm it passes**

Run:
```bash
buck2 test //src/control-plane/core:vector-index-decode-props > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL|PASS|error\[|panicked" /tmp/t2.log
```
Expected: `Tests finished:` with pass counts, no `FAIL`. A shrunk failing case is
a real finding — see "Handling a failing property".

- [ ] **Step 4: Lint the new test target**

Run:
```bash
buck2 build '//src/control-plane/core:vector-index-decode-props[clippy.txt]' > /tmp/c2.log 2>&1; cat /tmp/c2.log
```
Expected: empty (clean).

- [ ] **Step 5: Commit**

```bash
git add src/control-plane/core/tests/vector_index_decode_props.rs src/control-plane/core/BUCK
git commit -m "test(core): property tests for the vector-index decoder (never-panic + round-trip)"
```

---

### Task 3: `path_parse_props` — caller-string parsers never panic

**Files:**
- Create: `src/services/query-api/tests/path_parse_props.rs`
- Modify: `src/services/query-api/BUCK` (add `path-parse-props` `rust_test`)

**Interfaces:**
- Consumes: `query_api::path_parse::{parse_path_hops, parse_direction,
  parse_graph_mode}`, `query_api::filter::{coerce_filter, coerce_predicate,
  split_or_members, split_member}`, `query_api::handler::{Hop, Direction}`.
- Produces: nothing (leaf test target).

- [ ] **Step 1: Write the failing test file**

Create `src/services/query-api/tests/path_parse_props.rs`:

```rust
//! Property tests for the query-api caller-string parsers. The invariant: arbitrary
//! caller input through the path/filter parsers never panics — every entry point
//! returns (Vec/Ok/Err) gracefully — plus a few cheap structural invariants. These
//! are the crate's designated pure-logic boundary (no ontology/fixture needed). See
//! docs/superpowers/specs/2026-07-02-pillar-idioms-audit-design.md.

use proptest::prelude::*;
use query_api::filter::{coerce_filter, coerce_predicate, split_member, split_or_members};
use query_api::handler::{Direction, Hop};
use query_api::path_parse::{parse_direction, parse_graph_mode, parse_path_hops};

/// A known logical-type name, sampled alongside garbage so both the coercion happy
/// path and the UnknownLogicalType Err path are exercised.
fn logical_ty() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("Integer".to_string()), Just("Long".to_string()), Just("Double".to_string()),
        Just("Boolean".to_string()), Just("String".to_string()), Just("Date".to_string()),
        Just("Timestamp".to_string()), Just("emailaddress".to_string()),
        Just("url".to_string()), Just("phonenumber".to_string()), Just("vector(4)".to_string()),
        ".{0,10}", // arbitrary / unknown (String)
    ]
}

/// A raw filter value: sometimes an op-prefixed form, sometimes arbitrary junk.
fn raw_value() -> impl Strategy<Value = String> {
    prop_oneof![
        ".{0,16}",
        (prop_oneof![
            Just("gt"), Just("lt"), Just("ge"), Just("le"), Just("ne"), Just("eq"),
            Just("in"), Just("nin"), Just("between"), Just("contains"),
            Just("startswith"), Just("endswith"), Just("isnull"), Just("isnotnull"),
        ], ".{0,16}").prop_map(|(op, rest)| format!("{op}:{rest}")),
    ]
}

fn hop() -> impl Strategy<Value = Hop> {
    (".{0,8}", prop_oneof![Just(Direction::Forward), Just(Direction::Inverse)])
        .prop_map(|(link, direction)| Hop { link, direction })
}

proptest! {
    /// parse_path_hops is infallible; assert it never panics and drops empty links.
    #[test]
    fn parse_path_hops_drops_empties(s in ".*") {
        let hops = parse_path_hops(&s);
        prop_assert!(hops.iter().all(|h| !h.link.is_empty()));
    }

    #[test]
    fn parse_direction_never_panics(s in proptest::option::of(".*")) {
        let res = parse_direction(s.as_deref());
        prop_assert!(res.is_ok() || res.is_err());
    }

    #[test]
    fn parse_graph_mode_never_panics(
        path in prop::collection::vec(hop(), 0..5),
        links in prop::collection::vec(".{0,8}", 0..5),
        tree in any::<bool>(),
    ) {
        let res = parse_graph_mode(path, links, tree);
        prop_assert!(res.is_ok() || res.is_err());
    }

    #[test]
    fn coerce_filter_never_panics(name in ".{0,8}", ty in logical_ty(), raw in ".{0,16}") {
        let res = coerce_filter(&name, &ty, &raw);
        prop_assert!(res.is_ok() || res.is_err());
    }

    #[test]
    fn coerce_predicate_never_panics(col in ".{0,8}", ty in logical_ty(), raw in raw_value()) {
        let res = coerce_predicate(&col, &ty, &raw);
        prop_assert!(res.is_ok() || res.is_err());
    }

    #[test]
    fn split_or_members_never_panics(s in ".*") {
        let _ = split_or_members(&s);
        prop_assert!(true);
    }

    #[test]
    fn split_member_never_panics(s in ".*") {
        let _ = split_member(&s);
        prop_assert!(true);
    }
}
```

- [ ] **Step 2: Add the BUCK target**

In `src/services/query-api/BUCK`, mirror the existing `path-parse` target:

```python
rust_test(
    name = "path-parse-props",
    crate = "path_parse_props",
    srcs = ["tests/path_parse_props.rs"],
    crate_root = "tests/path_parse_props.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//third-party:proptest",
    ],
)
```

- [ ] **Step 3: Run the test and confirm it passes**

Run:
```bash
buck2 test //src/services/query-api:path-parse-props > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL|PASS|error\[|panicked" /tmp/t3.log
```
Expected: `Tests finished:` with pass counts, no `FAIL`.

- [ ] **Step 4: Lint the new test target**

Run:
```bash
buck2 build '//src/services/query-api:path-parse-props[clippy.txt]' > /tmp/c3.log 2>&1; cat /tmp/c3.log
```
Expected: empty (clean).

- [ ] **Step 5: Commit**

```bash
git add src/services/query-api/tests/path_parse_props.rs src/services/query-api/BUCK
git commit -m "test(query-api): property tests for the caller-string parsers"
```

---

### Task 4: Close the register item

**Files:**
- Modify: `docs/ROADMAP.md` (the `road-test-property-invariants` entry)

- [ ] **Step 1: Flip the checkbox and set terminal status + PR number**

The register entry currently reads:

```markdown
- [ ] **Property tests at the parser/compiler/decoder seams** `{#road-test-property-invariants area:test status:planned from:2026-07-02-pillar-idioms-audit-design pr:- spec:2026-07-02-pillar-idioms-audit-design}`
```

Edit it to (leave `pr:-` for now — the PR number is filled in Step 3 once the PR
exists, per `loom-docs-update`):

```markdown
- [x] **Property tests at the parser/compiler/decoder seams** `{#road-test-property-invariants area:test status:done from:2026-07-02-pillar-idioms-audit-design pr:- spec:2026-07-02-pillar-idioms-audit-design}`
```

- [ ] **Step 2: Validate the registers**

Run:
```bash
bash tools/docs.sh validate > /tmp/dv.log 2>&1; cat /tmp/dv.log
```
Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add docs/ROADMAP.md
git commit -m "docs(roadmap): close road-test-property-invariants"
```

(After the PR is opened, `loom-docs-update` will amend `pr:-` → `pr:#N`.)

---

## Handling a failing property (systematic-debugging, do NOT paper over)

If any property fails, proptest prints a **shrunk minimal counterexample**. That
is a genuine signal — one of two things:

1. **A real defect at the seam** (e.g. `decode` panics on a specific byte pattern,
   or a value leaks verbatim). This is exactly what the item exists to catch.
   STOP, capture the minimal case, and report it — file/append a `docs/ISSUES.md`
   entry and surface it in the PR. Do NOT weaken the property to make it pass.
2. **A generator bug** (the test fed an input the seam legitimately rejects, and
   the assertion over-reached — e.g. asserting `Ok` where `Err` is correct, or a
   marker-collision false positive). Fix the *generator/assertion*, not the
   production code, and document why in a comment.

Distinguish the two by reading the shrunk input against the seam facts above. When
unsure, prefer reporting a potential defect over silently narrowing the generator.

---

## Verification (whole-item, run before opening the PR)

- [ ] All three test targets pass:
```bash
buck2 test //src/services/query-api:sql-compile-props //src/services/query-api:path-parse-props //src/control-plane/core:vector-index-decode-props > /tmp/all.log 2>&1; grep -E "Tests finished|FAIL" /tmp/all.log
```
- [ ] Both touched crates still build & their existing tests pass (no accidental
  breakage from the BUCK edits):
```bash
buck2 test //src/services/query-api/... //src/control-plane/core/... > /tmp/crates.log 2>&1; grep -E "Tests finished|FAIL" /tmp/crates.log
```
- [ ] Lint clean across the new targets (run each `[clippy.txt]` as in each task's
  Step 4; empty output == clean).
- [ ] `bash tools/docs.sh validate` passes.
- [ ] `git status` clean; the branch is `work/road-test-property-invariants`.
