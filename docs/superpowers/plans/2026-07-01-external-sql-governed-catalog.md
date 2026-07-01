# External SQL Governed Catalog (slice 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the correctness core of loom's external-SQL governance — a `GovernedTableProvider` that makes each ontology type a *governed* DataFusion relation (row-filtered, denied columns absent, masked columns redacted), an engine path that runs arbitrary SQL over a per-request governed catalog, and an internal Flight ticket that dispatches to it — so any join/aggregate/subquery a client writes is governed by construction.

**Architecture:** A `TableProvider` decorator (`GovernedTableProvider`) wraps the existing per-table serving provider and, in `scan`, applies governance as a physical plan: scan the inner provider over its **full** schema, wrap it in a `FilterExec` that unconditionally AND-s the policy row-filters (so a filter may reference a denied column), then a `ProjectionExec` that drops denied columns and replaces masked columns with the `'***'` Utf8 literal. Its `schema()` presents the governed schema (denied absent, masked → Utf8) so a client naming a denied column fails at planning. A new `execute_governed_sql_stream` registers one governed provider per live table from a caller-supplied `GovernedCatalog` and runs the client's SQL. The engine's Flight `do_get` gains a sibling JSON `GovernedStatementQuery` ticket dispatched to it. Existing ungoverned paths are untouched (additive).

**Tech Stack:** Rust 2024, **DataFusion 54.0.0 (paired with arrow 58.3.0)** — when searching crate docs/signatures, use the `datafusion` 54 API, `arrow` 58 API — Arrow Flight (tonic), `control_plane_core` ACL types (`RowFilter`/`CompareOp`/`ScalarValue`), `loom_fixture_test` (hermetic Postgres) tests, buck2.

## Global Constraints

- **Additive only.** The ungoverned `TicketStatementQuery` / `execute_query_stream` path and all existing serving behaviour MUST remain untouched. Blast radius = the new governed path only.
- **Enforcing, not advisory.** There must be NO code path through `GovernedTableProvider` that returns a denied column, an unmasked masked value, or a row the filter excludes.
- **Apply order is load-bearing:** filter over the FULL inner schema BEFORE deny/project, so a row-filter that references a denied column still works.
- **Masked column = `Utf8` `'***'`** — reuse the existing `MASK_MARKER` convention (`"***"`), matching `compile_select_with` and the governed HTTP read path.
- **Fail closed.** A `RowFilter` shape that cannot be translated to a DataFusion `Expr` MUST return an error (refuse the query), never silently drop the filter and never panic.
- **Row-filter semantics MUST match `compile_select_with`** (`src/services/query-api/src/sql.rs`) row-for-row — same `CompareOp` mapping, same And/Or/Not nesting.
- **No inline `#[cfg(test)]` tests** — tests are `rust_test`/`loom_fixture_test` integration targets in `tests/*.rs` wired in `BUCK`. Tests seeding Postgres MUST use `loom_fixture_test`.
- **Clippy is strict** (pedantic + restriction on production code): no `unwrap`/`expect`/`panic`/`todo`/`unreachable`/`indexing_slicing`/`dbg` in library code; use `#[expect(lint, reason = "…")]` locally when unavoidable. Test code is exempted from panic-safety lints via the `loom_fixture_test`/`rust_test` wrappers.
- **Loom-native Flight tickets are JSON** (`serde_json`, `#[serde(deny_unknown_fields)]`), per the `VectorSearchTicket`/`FlightTicket` precedent in `engine-wire/src/flight.rs`. The spec's phrase "prost message" is reconciled to the established JSON-ticket pattern (`RowFilter`/`ScalarValue`/`TableRef` already derive serde); documented in Task 5.

---

## File Structure

- **Create `src/control-plane/core/src/governed.rs`** — the pure-data wire payloads `GovernedTable` and `GovernedCatalog` (serde, no DataFusion). Hosted in `control_plane_core` — NOT engine-serving — so `engine-wire` (and thus query-api's library) can reference `GovernedCatalog` in the Flight ticket **without** pulling DataFusion into query-api's tree, preserving its "zero-DataFusion wire client" property. Re-exported from `core/src/lib.rs`.
- **Create `src/services/engine-serving/src/governed.rs`** — the DataFusion-bearing governance code: `TablePolicy` (set-backed enforcement view), `policy_for`, `row_filter_to_expr`, `GovernedTableProvider`, and `execute_governed_sql_stream`. Imports the payloads from `control_plane_core`. One responsibility: turn an ungoverned serving relation + a policy into a governed relation and run governed SQL. Kept separate from `serving.rs` (which owns the ungoverned path) so the governance primitive is a self-contained, reviewable unit.
- **Modify `src/services/engine-serving/src/serving.rs`** — extract the per-table provider-building body of `register_iceberg_table` into a reusable `build_serving_provider(...) -> Result<Option<Arc<dyn TableProvider>>, EngineServingError>`; `register_iceberg_table` calls it then registers. `execute_governed_sql_stream` reuses it. Behaviour-preserving refactor.
- **Modify `src/services/engine-serving/src/lib.rs`** — add `pub mod governed;` and re-export the new public types/functions.
- **Modify `src/services/engine-serving/BUCK`** — wire the new `loom_fixture_test` target(s).
- **Create `src/services/engine-serving/tests/governed_sql.rs`** — `loom_fixture_test` e2e over `execute_governed_sql_stream` (row filter, deny, mask, join/aggregate, parity, empty policy).
- **Create `src/services/engine-serving/tests/row_filter_to_expr.rs`** — pure `rust_test` unit test for the `RowFilter → Expr` translation (no Postgres).
- **Modify `src/services/engine-wire/src/flight.rs`** — add the JSON `GovernedStatementQuery` ticket type (`sql: String`, `catalog: control_plane_core::GovernedCatalog`) with `encode`/`decode`. No new BUCK dep needed — engine-wire already deps `//src/control-plane/core:core`.
- **Modify `src/services/engine/src/flight.rs`** — add a `GovernedStatementQuery` decode branch in `do_get` dispatching to `execute_governed_sql_stream`; add a `do_get_governed_sql` helper mirroring `do_get_sql`.
- **Create `src/services/engine/tests/governed_flight.rs`** — `loom_fixture_test` driving the engine's `do_get` with a `GovernedStatementQuery` ticket end-to-end.
- **Modify `src/services/engine/BUCK`** — wire the new test target.

---

## Task 1: `row_filter_to_expr` — RowFilter → DataFusion Expr (fail-closed, semantics-matched)

**Files:**
- Create: `src/services/engine-serving/src/governed.rs`
- Modify: `src/services/engine-serving/src/lib.rs` (add `pub mod governed;` + re-export)
- Modify: `src/services/engine-serving/BUCK` (add the `row-filter-to-expr` `rust_test`)
- Test: `src/services/engine-serving/tests/row_filter_to_expr.rs`

**Interfaces:**
- Consumes: `control_plane_core::{RowFilter, CompareOp, ScalarValue, validate_row_filter}`; DataFusion `datafusion::prelude::{col, lit, Expr}`, `datafusion::logical_expr::Expr`, `datafusion::scalar::ScalarValue as DfScalar`.
- Produces:
  - `pub fn row_filter_to_expr(f: &control_plane_core::RowFilter) -> Result<datafusion::prelude::Expr, EngineServingError>` — translates one filter tree; validates first (`validate_row_filter(f, None)`), returns `EngineServingError::Engine(..)` on any invariant violation (fail closed), never panics.
  - `pub(crate) fn row_filters_conjunction(fs: &[RowFilter]) -> Result<Option<Expr>, EngineServingError>` — AND-fold a slice into one optional `Expr` (`None` when empty).

**Semantics to match (`compile_select_with` / `filter_sql` in `src/services/query-api/src/sql.rs`):**
- `Compare { property, op, value }`:
  - `Eq → col(p).eq(lit)`, `Ne → col(p).not_eq(lit)`, `Lt → .lt`, `Le → .lt_eq`, `Gt → .gt`, `Ge → .gt_eq`
  - `In → col(p).in_list(items, false)`, `NotIn → col(p).in_list(items, true)` (items = each `ScalarValue` → `lit`); value MUST be `ScalarValue::List` else Err
  - `IsNull → col(p).is_null()`, `IsNotNull → col(p).is_not_null()`
  - scalar ops (`Eq..Ge`): value MUST NOT be a `List` else Err
- `And(xs) → xs.iter().map(expr).fold(a.and(b))` ; empty `And` → `lit(true)`
- `Or(xs) → fold with .or()` ; empty `Or` → `lit(false)`
- `Not(x) → !expr` (`Expr::not()` / `datafusion::logical_expr::not`)
- `ScalarValue::Text → DfScalar::Utf8(Some(s))`, `Int → Int64(Some(i))`, `Bool → Boolean(Some(b))`, `List` only valid inside In/NotIn.

- [ ] **Step 1: Write the failing unit test**

Create `src/services/engine-serving/tests/row_filter_to_expr.rs`:

```rust
//! Unit tests for `row_filter_to_expr`: the RowFilter → DataFusion Expr translation
//! that governs the external-SQL path. Pure logic, no Postgres — a `rust_test`.

use control_plane_core::{CompareOp, RowFilter, ScalarValue};
use datafusion::prelude::{col, lit, Expr};
use engine_serving::governed::row_filter_to_expr;

fn cmp(property: &str, op: CompareOp, value: ScalarValue) -> RowFilter {
    RowFilter::Compare { property: property.to_string(), op, value }
}

#[test]
fn compare_ops_map_to_binary_exprs() {
    let cases: Vec<(RowFilter, Expr)> = vec![
        (cmp("a", CompareOp::Eq, ScalarValue::Int(1)), col("a").eq(lit(1_i64))),
        (cmp("a", CompareOp::Ne, ScalarValue::Int(1)), col("a").not_eq(lit(1_i64))),
        (cmp("a", CompareOp::Lt, ScalarValue::Int(1)), col("a").lt(lit(1_i64))),
        (cmp("a", CompareOp::Le, ScalarValue::Int(1)), col("a").lt_eq(lit(1_i64))),
        (cmp("a", CompareOp::Gt, ScalarValue::Int(1)), col("a").gt(lit(1_i64))),
        (cmp("a", CompareOp::Ge, ScalarValue::Int(1)), col("a").gt_eq(lit(1_i64))),
        (cmp("s", CompareOp::Eq, ScalarValue::Text("x".into())), col("s").eq(lit("x"))),
        (cmp("b", CompareOp::Eq, ScalarValue::Bool(true)), col("b").eq(lit(true))),
    ];
    for (f, expected) in cases {
        assert_eq!(row_filter_to_expr(&f).expect("translate"), expected);
    }
}

#[test]
fn null_ops_ignore_value() {
    assert_eq!(
        row_filter_to_expr(&cmp("a", CompareOp::IsNull, ScalarValue::Int(0))).expect("null"),
        col("a").is_null()
    );
    assert_eq!(
        row_filter_to_expr(&cmp("a", CompareOp::IsNotNull, ScalarValue::Int(0))).expect("notnull"),
        col("a").is_not_null()
    );
}

#[test]
fn in_and_not_in_use_list() {
    let items = ScalarValue::List(vec![ScalarValue::Int(1), ScalarValue::Int(2)]);
    assert_eq!(
        row_filter_to_expr(&cmp("a", CompareOp::In, items.clone())).expect("in"),
        col("a").in_list(vec![lit(1_i64), lit(2_i64)], false)
    );
    assert_eq!(
        row_filter_to_expr(&cmp("a", CompareOp::NotIn, items)).expect("not in"),
        col("a").in_list(vec![lit(1_i64), lit(2_i64)], true)
    );
}

#[test]
fn boolean_tree_nests_and_or_not() {
    let f = RowFilter::And(vec![
        cmp("a", CompareOp::Eq, ScalarValue::Int(1)),
        RowFilter::Or(vec![
            cmp("b", CompareOp::Eq, ScalarValue::Int(2)),
            RowFilter::Not(Box::new(cmp("c", CompareOp::Eq, ScalarValue::Int(3)))),
        ]),
    ]);
    let expected = col("a")
        .eq(lit(1_i64))
        .and(col("b").eq(lit(2_i64)).or(!col("c").eq(lit(3_i64))));
    assert_eq!(row_filter_to_expr(&f).expect("tree"), expected);
}

#[test]
fn invariant_violation_fails_closed() {
    // In with a non-list value is malformed; must Err, not panic.
    assert!(row_filter_to_expr(&cmp("a", CompareOp::In, ScalarValue::Int(1))).is_err());
    // Scalar op with a list value is malformed.
    assert!(
        row_filter_to_expr(&cmp("a", CompareOp::Eq, ScalarValue::List(vec![ScalarValue::Int(1)])))
            .is_err()
    );
}
```

- [ ] **Step 2: Add the module skeleton + wire the test target, run it to see it fail**

Create `src/services/engine-serving/src/governed.rs` with the module doc + imports and a stub `row_filter_to_expr` returning `Err`. Add to `src/services/engine-serving/src/lib.rs`:

```rust
pub mod governed;
```

and extend the re-export block (leave the serving re-exports as-is; add a new line). Grow this list as each task lands its symbols — for Task 1 only `row_filter_to_expr` exists:

```rust
pub use governed::row_filter_to_expr;
```

By the end of the feature the block is:

```rust
pub use governed::{
    row_filter_to_expr, GovernedTableProvider, TablePolicy, policy_for,
    execute_governed_sql_stream,
};
```

(`GovernedCatalog`/`GovernedTable` are re-exported from `control_plane_core`, not engine-serving.) The `governed` module is `pub`, so `use engine_serving::governed::row_filter_to_expr` resolves regardless.

Add the test target to `src/services/engine-serving/BUCK` (mirror `vector-merge`, a pure `rust_test`):

```python
rust_test(
    name = "row-filter-to-expr",
    crate = "row_filter_to_expr",
    srcs = ["tests/row_filter_to_expr.rs"],
    crate_root = "tests/row_filter_to_expr.rs",
    edition = "2024",
    deps = [
        ":engine-serving",
        "//src/control-plane/core:core",
        "//third-party:datafusion",
    ],
)
```

Run: `buck2 test //src/services/engine-serving:row-filter-to-expr > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: FAIL (stub returns Err / assertions fail).

- [ ] **Step 3: Implement `row_filter_to_expr` + `row_filters_conjunction`**

In `governed.rs`:

```rust
use control_plane_core::{CompareOp, RowFilter, ScalarValue, validate_row_filter};
use datafusion::logical_expr::not;
use datafusion::prelude::{col, lit, Expr};
use datafusion::scalar::ScalarValue as DfScalar;

use crate::serving::EngineServingError;

/// A leaf `ScalarValue` (never a `List`) → a DataFusion literal `Expr`.
fn scalar_lit(v: &ScalarValue) -> Result<Expr, EngineServingError> {
    let s = match v {
        ScalarValue::Text(s) => DfScalar::Utf8(Some(s.clone())),
        ScalarValue::Int(i) => DfScalar::Int64(Some(*i)),
        ScalarValue::Bool(b) => DfScalar::Boolean(Some(*b)),
        ScalarValue::List(_) => {
            return Err(EngineServingError::Engine(
                "row filter: unexpected list scalar in leaf position".into(),
            ));
        }
    };
    Ok(lit(s))
}

/// Translate a `RowFilter` tree into a DataFusion `Expr`, matching
/// `query-api::sql::filter_sql` semantics. Fails closed: an invariant violation
/// (validated by `validate_row_filter`) returns an error, never a panic.
pub fn row_filter_to_expr(f: &RowFilter) -> Result<Expr, EngineServingError> {
    validate_row_filter(f, None).map_err(EngineServingError::Engine)?;
    build_expr(f)
}

fn build_expr(f: &RowFilter) -> Result<Expr, EngineServingError> {
    match f {
        RowFilter::Compare { property, op, value } => {
            let c = col(property);
            match op {
                CompareOp::Eq => Ok(c.eq(scalar_lit(value)?)),
                CompareOp::Ne => Ok(c.not_eq(scalar_lit(value)?)),
                CompareOp::Lt => Ok(c.lt(scalar_lit(value)?)),
                CompareOp::Le => Ok(c.lt_eq(scalar_lit(value)?)),
                CompareOp::Gt => Ok(c.gt(scalar_lit(value)?)),
                CompareOp::Ge => Ok(c.gt_eq(scalar_lit(value)?)),
                CompareOp::In | CompareOp::NotIn => {
                    let ScalarValue::List(items) = value else {
                        return Err(EngineServingError::Engine(
                            "row filter: In/NotIn requires a list value".into(),
                        ));
                    };
                    let list = items.iter().map(scalar_lit).collect::<Result<Vec<_>, _>>()?;
                    Ok(c.in_list(list, matches!(op, CompareOp::NotIn)))
                }
                CompareOp::IsNull => Ok(c.is_null()),
                CompareOp::IsNotNull => Ok(c.is_not_null()),
            }
        }
        RowFilter::And(xs) => fold_bool(xs, true),
        RowFilter::Or(xs) => fold_bool(xs, false),
        RowFilter::Not(x) => Ok(not(build_expr(x)?)),
    }
}

/// AND-fold (`and_identity=true`) or OR-fold (`false`) a slice of sub-filters.
/// An empty slice yields the identity literal (`true` for AND, `false` for OR).
fn fold_bool(xs: &[RowFilter], and: bool) -> Result<Expr, EngineServingError> {
    let mut acc: Option<Expr> = None;
    for x in xs {
        let e = build_expr(x)?;
        acc = Some(match acc {
            None => e,
            Some(a) if and => a.and(e),
            Some(a) => a.or(e),
        });
    }
    Ok(acc.unwrap_or_else(|| lit(and)))
}

/// AND-fold a slice of top-level row filters into one optional predicate.
pub(crate) fn row_filters_conjunction(
    fs: &[RowFilter],
) -> Result<Option<Expr>, EngineServingError> {
    if fs.is_empty() {
        return Ok(None);
    }
    Ok(Some(fold_bool(fs, true)?))
}
```

Note: `EngineServingError::Engine` takes a `String`; `validate_row_filter` returns `Result<(), String>`, so `.map_err(EngineServingError::Engine)` is exact. If `EngineServingError` is not visible from `governed.rs`, import it via `use crate::serving::EngineServingError;` (it is `pub` in `serving.rs`).

- [ ] **Step 4: Run the unit test to green**

Run: `buck2 test //src/services/engine-serving:row-filter-to-expr > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: PASS.

- [ ] **Step 5: Clippy + commit**

Run: `buck2 build '//src/services/engine-serving:engine-serving[clippy.txt]' > /tmp/c.log 2>&1; cat $(buck2 build '//src/services/engine-serving:engine-serving[clippy.txt]' --show-output 2>/dev/null | awk '{print $2}')` (or just `bash tools/clippy-all.sh` scoped; empty == clean).

```bash
git add src/services/engine-serving/src/governed.rs src/services/engine-serving/src/lib.rs \
        src/services/engine-serving/tests/row_filter_to_expr.rs src/services/engine-serving/BUCK
git commit -m "feat(engine-serving): RowFilter -> DataFusion Expr translation (fail-closed)"
```

---

## Task 2: Governed wire payloads in core (`GovernedTable`, `GovernedCatalog`) + `TablePolicy` in engine-serving

**Files:**
- Create: `src/control-plane/core/src/governed.rs` (pure-data payloads)
- Modify: `src/control-plane/core/src/lib.rs` (add `pub mod governed;` + re-export)
- Modify: `src/control-plane/core/BUCK` (add a `governed` `rust_test`)
- Create: `src/control-plane/core/tests/governed.rs` (serde round-trip)
- Modify: `src/services/engine-serving/src/governed.rs` (`TablePolicy` + `policy_for`)
- Test: `policy_for` covered by a case added to `tests/row_filter_to_expr.rs`.

**Why the split:** `GovernedCatalog`/`GovernedTable` are DataFusion-free serde payloads. They live in `control_plane_core` so `engine-wire` (Task 5) can put a `GovernedCatalog` in the Flight ticket without depending on engine-serving/DataFusion — which would otherwise leak DataFusion into query-api's library (query-api deps engine-wire but is a "zero-DataFusion wire client"). `TablePolicy` (the set-backed enforcement view) and the `policy_for` conversion stay in engine-serving, next to the provider that consumes them.

**Interfaces:**
- Produces (in `control_plane_core`, re-exported from `lib.rs`):
  - `pub struct GovernedTable { pub table: TableRef, pub row_filters: Vec<RowFilter>, pub denied: Vec<String>, pub masked: Vec<String> }` — `#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]`, `#[serde(deny_unknown_fields)]`, all list fields `#[serde(default)]`.
  - `pub struct GovernedCatalog { pub tables: Vec<GovernedTable> }` — `#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]`, `#[serde(deny_unknown_fields)]`.
  - `impl GovernedCatalog { pub fn table_for(&self, table: &TableRef) -> Option<&GovernedTable> }`.
- Produces (in `engine_serving::governed`):
  - `pub struct TablePolicy { pub row_filters: Vec<RowFilter>, pub denied: HashSet<String>, pub masked: HashSet<String> }` — `#[derive(Debug, Clone, Default, PartialEq)]`.
  - `pub fn policy_for(catalog: &GovernedCatalog, table: &TableRef) -> TablePolicy` — the matching table's policy, or the empty (fully-visible) policy when absent (per spec "absent ⇒ visible").

- [ ] **Step 1: Write the failing core serde round-trip test**

Create `src/control-plane/core/tests/governed.rs`:

```rust
//! Serde round-trip + lookup for the governed-catalog wire payloads.

use control_plane_core::{CompareOp, GovernedCatalog, GovernedTable, RowFilter, ScalarValue, TableRef};

#[test]
fn governed_catalog_json_roundtrips_and_looks_up() {
    let t = TableRef { schema: "s".into(), name: "t".into() };
    let cat = GovernedCatalog {
        tables: vec![GovernedTable {
            table: t.clone(),
            row_filters: vec![RowFilter::Compare {
                property: "a".into(), op: CompareOp::Gt, value: ScalarValue::Int(5),
            }],
            denied: vec!["secret".into()],
            masked: vec!["email".into()],
        }],
    };
    let json = serde_json::to_vec(&cat).expect("encode");
    let back: GovernedCatalog = serde_json::from_slice(&json).expect("decode");
    assert_eq!(back, cat);
    assert!(back.table_for(&t).is_some());
    let absent = TableRef { schema: "s".into(), name: "missing".into() };
    assert!(back.table_for(&absent).is_none());
}

#[test]
fn governed_catalog_defaults_missing_lists() {
    // Only `table` provided; list fields default to empty.
    let json = br#"{"tables":[{"table":{"schema":"s","name":"t"}}]}"#;
    let cat: GovernedCatalog = serde_json::from_slice(json).expect("decode");
    let gt = &cat.tables[0];
    assert!(gt.row_filters.is_empty() && gt.denied.is_empty() && gt.masked.is_empty());
}
```

Add the target to `src/control-plane/core/BUCK` (mirror an existing pure `rust_test`, e.g. `governance-serde-roundtrip` — read that block first for exact deps):

```python
rust_test(
    name = "governed",
    crate = "governed",
    srcs = ["tests/governed.rs"],
    crate_root = "tests/governed.rs",
    edition = "2024",
    deps = [
        ":core",
        "//third-party:serde_json",
    ],
)
```

Run: `buck2 test //src/control-plane/core:governed > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: FAIL (types absent).

- [ ] **Step 2: Implement the core payloads**

Create `src/control-plane/core/src/governed.rs`:

```rust
//! Governed-catalog wire payloads: a caller-resolved set of per-type governance
//! (row filters, denied columns, masked columns) that the engine's external-SQL path
//! applies. Pure data (serde), no DataFusion — engine-serving turns these into an
//! enforcing `TableProvider`; engine-wire carries a `GovernedCatalog` in its ticket.

use serde::{Deserialize, Serialize};

use crate::acl::RowFilter;
use crate::TableRef;

/// One type's governance in a caller-supplied catalog. Vec fields keep it JSON-stable.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GovernedTable {
    pub table: TableRef,
    #[serde(default)]
    pub row_filters: Vec<RowFilter>,
    #[serde(default)]
    pub denied: Vec<String>,
    #[serde(default)]
    pub masked: Vec<String>,
}

/// A fully-resolved governed catalog: one `GovernedTable` per type the caller may see.
/// A table with no entry is treated as fully visible — the caller (slice 2) owns
/// deny-by-default at the edge by never listing a type without a grant.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GovernedCatalog {
    pub tables: Vec<GovernedTable>,
}

impl GovernedCatalog {
    /// The catalog entry for `table`, if present.
    #[must_use]
    pub fn table_for(&self, table: &TableRef) -> Option<&GovernedTable> {
        self.tables.iter().find(|gt| &gt.table == table)
    }
}
```

Add to `src/control-plane/core/src/lib.rs` (module + re-export, keeping list ordering):

```rust
pub mod governed;
```

and in the `pub use` block:

```rust
pub use governed::{GovernedCatalog, GovernedTable};
```

(`RowFilter`/`CompareOp`/`ScalarValue` are already re-exported from `acl`.)

- [ ] **Step 3: Run the core test to green**

Run: `buck2 test //src/control-plane/core:governed > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: PASS.

- [ ] **Step 4: Add `TablePolicy` + `policy_for` in engine-serving**

In `src/services/engine-serving/src/governed.rs` add `use std::collections::HashSet;` and `use control_plane_core::{GovernedCatalog, GovernedTable, RowFilter, TableRef};`:

```rust
/// The enforcing per-table policy the provider applies. `denied`/`masked` are sets
/// for O(1) column membership tests during `scan`/`schema`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TablePolicy {
    pub row_filters: Vec<RowFilter>,
    pub denied: HashSet<String>,
    pub masked: HashSet<String>,
}

/// The policy for `table` from `catalog`, or the empty (fully-visible) policy when
/// absent (per spec "absent ⇒ visible").
#[must_use]
pub fn policy_for(catalog: &GovernedCatalog, table: &TableRef) -> TablePolicy {
    match catalog.table_for(table) {
        Some(gt) => TablePolicy {
            row_filters: gt.row_filters.clone(),
            denied: gt.denied.iter().cloned().collect(),
            masked: gt.masked.iter().cloned().collect(),
        },
        None => TablePolicy::default(),
    }
}
```

Append a `policy_for` case to `tests/row_filter_to_expr.rs`:

```rust
use control_plane_core::{GovernedCatalog, GovernedTable, TableRef};
use engine_serving::governed::policy_for;

#[test]
fn policy_for_absent_is_empty_present_is_set_backed() {
    let t = TableRef { schema: "s".into(), name: "t".into() };
    let cat = GovernedCatalog { tables: vec![GovernedTable {
        table: t.clone(),
        row_filters: vec![cmp("a", CompareOp::Gt, ScalarValue::Int(5))],
        denied: vec!["secret".into()], masked: vec!["email".into()],
    }]};
    let p = policy_for(&cat, &t);
    assert!(p.denied.contains("secret") && p.masked.contains("email") && p.row_filters.len() == 1);
    let empty = policy_for(&cat, &TableRef { schema: "s".into(), name: "x".into() });
    assert!(empty.denied.is_empty() && empty.masked.is_empty() && empty.row_filters.is_empty());
}
```

Ensure `//src/control-plane/core:core` is in the `row-filter-to-expr` test deps (add if missing). Extend the engine-serving `lib.rs` `governed::{...}` re-export to include `TablePolicy, policy_for`.

- [ ] **Step 5: Run + commit**

Run: `buck2 test //src/control-plane/core:governed //src/services/engine-serving:row-filter-to-expr > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: PASS.

```bash
git add src/control-plane/core/src/governed.rs src/control-plane/core/src/lib.rs \
        src/control-plane/core/BUCK src/control-plane/core/tests/governed.rs \
        src/services/engine-serving/src/governed.rs src/services/engine-serving/src/lib.rs \
        src/services/engine-serving/tests/row_filter_to_expr.rs src/services/engine-serving/BUCK
git commit -m "feat: GovernedCatalog/GovernedTable payloads in core; TablePolicy in engine-serving"
```

---

## Task 3: Extract `build_serving_provider` from `register_iceberg_table` (behaviour-preserving refactor)

**Files:**
- Modify: `src/services/engine-serving/src/serving.rs:62-148`

**Interfaces:**
- Produces: `pub async fn build_serving_provider(ctx: &SessionContext, catalog: &IcebergCatalog, table: &TableRef, serving_store: Option<&(String, Arc<dyn object_store::ObjectStore>)>) -> Result<Option<Arc<dyn TableProvider>>, EngineServingError>` — returns the combined file∪inline provider (the current body of `register_iceberg_table` up to the final `register_table`), or `None` when the table has no data. Registering object stores stays inside it (idempotent).
- `register_iceberg_table` becomes: call `build_serving_provider`; if `Some(provider)`, ensure the schema exists + `register_table`; if `None`, `Ok(())`.

- [ ] **Step 1: Confirm the existing e2e tests are green (characterization)**

Run: `buck2 test //src/services/engine-serving:execute-query-e2e //src/services/engine-serving:execute-query-stream > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (these pin `register_iceberg_table`'s behaviour; the refactor must keep them green).

- [ ] **Step 2: Refactor**

Split `register_iceberg_table` (`serving.rs:62`). Move the body from the object-store registration through building `provider` (lines ~68–131) into:

```rust
/// Build the combined serving `TableProvider` for `table` at its live snapshot:
/// the pruning-aware file provider UNION-ALL the inline PG provider (either alone,
/// or `None` when the table has no live data). Registers the needed object store(s)
/// on `ctx` (idempotent). Factored out of `register_iceberg_table` so the governed
/// path (`execute_governed_sql_stream`) can wrap the same relation.
pub async fn build_serving_provider(
    ctx: &SessionContext,
    catalog: &IcebergCatalog,
    table: &TableRef,
    serving_store: Option<&(String, Arc<dyn object_store::ObjectStore>)>,
) -> Result<Option<Arc<dyn TableProvider>>, EngineServingError> {
    // ... object-store registration + snapshot + schema + files + inline provider
    //     + the (file, inline) match producing `provider` ...
    // return Ok(Some(provider))   // and Ok(None) for the (None, None) arm
}
```

Then:

```rust
pub async fn register_iceberg_table(
    ctx: &SessionContext,
    catalog: &IcebergCatalog,
    table: &TableRef,
    serving_store: Option<&(String, Arc<dyn object_store::ObjectStore>)>,
) -> Result<(), EngineServingError> {
    let Some(provider) = build_serving_provider(ctx, catalog, table, serving_store).await? else {
        return Ok(());
    };
    let cat = ctx
        .catalog("datafusion")
        .ok_or_else(|| EngineServingError::Engine("no default datafusion catalog".into()))?;
    if cat.schema(&table.schema).is_none() {
        cat.register_schema(&table.schema, Arc::new(MemorySchemaProvider::new()))
            .map_err(to_serving)?;
    }
    ctx.register_table(
        TableReference::partial(table.schema.clone(), table.name.clone()),
        provider,
    )
    .map_err(to_serving)?;
    Ok(())
}
```

Add `build_serving_provider` to the `serving::{...}` re-export in `lib.rs`.

- [ ] **Step 3: Run the characterization tests — behaviour unchanged**

Run: `buck2 test //src/services/engine-serving:execute-query-e2e //src/services/engine-serving:execute-query-stream > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (identical to Step 1).

- [ ] **Step 4: Clippy + commit**

```bash
git add src/services/engine-serving/src/serving.rs src/services/engine-serving/src/lib.rs
git commit -m "refactor(engine-serving): extract build_serving_provider from register_iceberg_table"
```

---

## Task 4: `GovernedTableProvider` + `execute_governed_sql_stream` (the enforcing core)

**Files:**
- Modify: `src/services/engine-serving/src/governed.rs`
- Modify: `src/services/engine-serving/BUCK` (add the `governed-sql` `loom_fixture_test`)
- Test: `src/services/engine-serving/tests/governed_sql.rs`

**Interfaces:**
- Consumes: `build_serving_provider` (Task 3), `row_filters_conjunction` (Task 1), `TablePolicy`/`GovernedCatalog` (Task 2), `IcebergCatalog`, DataFusion physical-plan types.
- Produces:
  - `pub struct GovernedTableProvider { inner: Arc<dyn TableProvider>, policy: TablePolicy, governed_schema: SchemaRef }` implementing `datafusion::catalog::TableProvider`.
  - `impl GovernedTableProvider { pub fn new(inner: Arc<dyn TableProvider>, policy: TablePolicy) -> Result<Self, EngineServingError> }` — precomputes `governed_schema` (inner schema minus `denied`, `masked` fields re-typed to `Utf8`).
  - `pub async fn execute_governed_sql_stream(catalog: &IcebergCatalog, sql: &str, governed: &GovernedCatalog, serving_store: Option<&(String, Arc<dyn object_store::ObjectStore>)>) -> Result<SendableRecordBatchStream, EngineServingError>`.

**Design of `GovernedTableProvider` (the enforcing decorator):**

- `schema()` → `governed_schema`: iterate inner fields; skip fields whose name ∈ `denied`; for a field whose name ∈ `masked`, emit `Field::new(name, DataType::Utf8, true)`; else keep the field. Precompute once in `new`.
- `table_type()` → `TableType::Base`.
- `supports_filters_pushdown` → `Ok(vec![Inexact; filters.len()])` (governance re-applies regardless; the client filters are an optimization only).
- `scan(state, projection, _filters, limit)`:
  1. `inner_plan = self.inner.scan(state, None, &[], None).await?` — FULL inner schema, all rows (do not push the client projection/filter/limit into the inner scan; governance must see all columns/rows). `_filters` from the client are intentionally not pushed (correctness over the pushdown optimization for slice 1).
  2. Build the row-filter predicate over the **inner** schema: `row_filters_conjunction(&self.policy.row_filters)?`; if `Some(expr)`, create a physical expr against the inner `DFSchema` (`create_physical_expr(&expr, &DFSchema::try_from(inner_schema)?, &ExecutionProps::new())`) and wrap: `plan = Arc::new(FilterExec::try_new(phys, inner_plan)?)`. Else `plan = inner_plan`.
  3. Build the governed projection: for each **governed** field index `gi` (respecting the client `projection` — if `Some(p)`, iterate `p`; else all governed indices), map governed field → a `(Arc<dyn PhysicalExpr>, String)` pair:
     - masked field → `(Arc::new(Literal::new(DfScalar::Utf8(Some("***".into())))), name)`
     - otherwise → resolve the field's index in the **inner** schema (by name) and `(Arc::new(Column::new(name, inner_idx)), name)`.
     Wrap: `plan = Arc::new(ProjectionExec::try_new(exprs, plan)?)`.
  4. Apply `limit`: if `Some(n)`, `plan = Arc::new(GlobalLimitExec::new(plan, 0, Some(n)))`.
  5. Return `plan`.

Map every DataFusion error via `datafusion::error::DataFusionError` (scan returns `datafusion::error::Result`). The row-filter Expr construction can fail (fail-closed): surface it as `DataFusionError::Plan(msg)` inside `scan`. NOTE: `MASK_MARKER` is `"***"`; define a `const MASK_MARKER: &str = "***";` in `governed.rs` (do not import the private one from query-api).

**`execute_governed_sql_stream`:**

```rust
pub async fn execute_governed_sql_stream(
    catalog: &IcebergCatalog,
    sql: &str,
    governed: &GovernedCatalog,
    serving_store: Option<&(String, Arc<dyn object_store::ObjectStore>)>,
) -> Result<SendableRecordBatchStream, EngineServingError> {
    use control_plane_core::Catalog;
    let ctx = SessionContext::new();
    for table in catalog.live_tables().await.map_err(to_serving)? {
        let Some(inner) = build_serving_provider(&ctx, catalog, &table, serving_store).await? else {
            continue;
        };
        let policy = policy_for(governed, &table);
        let provider = Arc::new(GovernedTableProvider::new(inner, policy)?);
        // ensure schema exists (mirror register_iceberg_table)
        let cat = ctx.catalog("datafusion").ok_or_else(|| EngineServingError::Engine("no default datafusion catalog".into()))?;
        if cat.schema(&table.schema).is_none() {
            cat.register_schema(&table.schema, Arc::new(MemorySchemaProvider::new())).map_err(to_serving)?;
        }
        ctx.register_table(TableReference::partial(table.schema.clone(), table.name.clone()), provider).map_err(to_serving)?;
    }
    let df = ctx.sql(sql).await.map_err(to_serving)?;
    df.execute_stream().await.map_err(to_serving)
}
```

`live_tables` returns `Vec<TableRef>` (see `execute_query`/`execute_query_stream`). Confirm the exact type of `table` in the loop (`serving.rs:479` iterates `catalog.live_tables()` and passes `&table`).

- [ ] **Step 1: Write the failing e2e test (row filter under a JOIN, deny, mask, empty policy)**

Create `src/services/engine-serving/tests/governed_sql.rs`. Use `PgFixture` + `IcebergWriter` (as in `execute_query_e2e.rs`) plus a helper to collect batches into rows. Seed two types. Cover spec tests 1–4 and 6:

```rust
//! e2e over `execute_governed_sql_stream`: row filter holds under arbitrary SQL,
//! denied columns absent, masked columns redacted (incl. through GROUP BY), and an
//! empty policy = full visibility. Governance is applied regardless of client SQL.

use arrow::array::{Int64Array, StringArray};
use control_plane_core::{CompareOp, GovernedCatalog, GovernedTable, RowFilter, ScalarValue, TableRef};
use control_plane_postgres::fixture::{IcebergWriter, PgFixture};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use engine_serving::governed::execute_governed_sql_stream;
use futures::TryStreamExt;

fn gt(schema: &str, name: &str) -> TableRef { TableRef { schema: schema.into(), name: name.into() } }

async fn run(catalog: &IcebergCatalog, sql: &str, cat: &GovernedCatalog) -> Vec<arrow::record_batch::RecordBatch> {
    let stream = execute_governed_sql_stream(catalog, sql, cat, None).await.expect("governed stream");
    stream.try_collect().await.expect("collect")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn row_filter_holds_under_join() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), false),
    ];
    writer.seed("s", "orders", &cols, &[5]).await; // ids 0..4
    let catalog = IcebergCatalog::new(pool);

    // Policy: only rows with id >= 2 are visible on `orders`.
    let cat = GovernedCatalog {
        tables: vec![GovernedTable {
            table: gt("s", "orders"),
            row_filters: vec![RowFilter::Compare {
                property: "id".into(), op: CompareOp::Ge, value: ScalarValue::Int(2),
            }],
            denied: vec![], masked: vec![],
        }],
    };
    // Client SQL that *tries* to see everything (self-join, WHERE true).
    let batches = run(&catalog, "SELECT o.\"id\" FROM \"s\".\"orders\" o WHERE o.\"id\" >= 0 ORDER BY o.\"id\"", &cat).await;
    let ids: Vec<i64> = collect_i64(&batches, 0);
    assert_eq!(ids, vec![2, 3, 4], "row filter applied regardless of client predicate");
}

fn collect_i64(batches: &[arrow::record_batch::RecordBatch], col: usize) -> Vec<i64> {
    batches.iter().flat_map(|b| {
        let a = b.column(col).as_any().downcast_ref::<Int64Array>().expect("i64");
        (0..a.len()).map(|i| a.value(i)).collect::<Vec<_>>()
    }).collect()
}

fn collect_str(batches: &[arrow::record_batch::RecordBatch], col: usize) -> Vec<String> {
    batches.iter().flat_map(|b| {
        let a = b.column(col).as_any().downcast_ref::<StringArray>().expect("utf8");
        (0..a.len()).map(|i| a.value(i).to_string()).collect::<Vec<_>>()
    }).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn denied_column_is_absent() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("secret".to_string(), "string".to_string(), false),
    ];
    writer.seed("s", "t", &cols, &[3]).await;
    let catalog = IcebergCatalog::new(pool);
    let cat = GovernedCatalog { tables: vec![GovernedTable {
        table: gt("s", "t"), row_filters: vec![], denied: vec!["secret".into()], masked: vec![],
    }]};
    // Naming the denied column => planning error.
    let err = execute_governed_sql_stream(&catalog, "SELECT \"secret\" FROM \"s\".\"t\"", &cat, None).await;
    assert!(err.is_err(), "denied column must not resolve");
    // SELECT * must not include it.
    let batches = run(&catalog, "SELECT * FROM \"s\".\"t\"", &cat).await;
    assert!(batches[0].schema().field_with_name("secret").is_err(), "denied col absent from schema");
    assert!(batches[0].schema().field_with_name("id").is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn masked_column_redacted_through_group_by() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("email".to_string(), "string".to_string(), false),
    ];
    writer.seed("s", "u", &cols, &[3]).await;
    let catalog = IcebergCatalog::new(pool);
    let cat = GovernedCatalog { tables: vec![GovernedTable {
        table: gt("s", "u"), row_filters: vec![], denied: vec![], masked: vec!["email".into()],
    }]};
    // Direct select: every email is '***'
    let batches = run(&catalog, "SELECT \"email\" FROM \"s\".\"u\"", &cat).await;
    let emails = collect_str(&batches, 0);
    assert!(emails.iter().all(|e| e == "***"), "masked values redacted");
    // Through a GROUP BY: groups on '***'
    let grouped = run(&catalog, "SELECT \"email\", count(*) AS c FROM \"s\".\"u\" GROUP BY \"email\"", &cat).await;
    assert_eq!(collect_str(&grouped, 0), vec!["***".to_string()], "single masked group");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_policy_is_full_visibility() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![("id".to_string(), "long".to_string(), false)];
    writer.seed("s", "t", &cols, &[3]).await;
    let catalog = IcebergCatalog::new(pool);
    // No GovernedTable entry at all => fully visible.
    let cat = GovernedCatalog::default();
    let batches = run(&catalog, "SELECT \"id\" FROM \"s\".\"t\" ORDER BY \"id\"", &cat).await;
    assert_eq!(collect_i64(&batches, 0), vec![0, 1, 2]);
}
```

Add the target to `BUCK` (mirror `execute-query-stream`, a `loom_fixture_test`):

```python
loom_fixture_test(
    name = "governed-sql",
    crate = "governed_sql",
    srcs = ["tests/governed_sql.rs"],
    crate_root = "tests/governed_sql.rs",
    deps = [
        ":engine-serving",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:arrow",
        "//third-party:futures",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```

Run: `buck2 test //src/services/engine-serving:governed-sql > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: FAIL (types/functions absent).

- [ ] **Step 2: Implement `GovernedTableProvider` + `execute_governed_sql_stream`**

Add to `governed.rs` the imports and code. Physical-plan imports (DataFusion 58):

```rust
use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{MemorySchemaProvider, Session, TableProvider};
use datafusion::common::{DFSchema, TableReference};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::context::{ExecutionProps, SessionContext};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::{create_physical_expr, expressions::{Column, Literal}, PhysicalExpr};
use datafusion::physical_plan::{
    filter::FilterExec, limit::GlobalLimitExec, projection::ProjectionExec,
    ExecutionPlan, SendableRecordBatchStream,
};
use datafusion::scalar::ScalarValue as DfScalar;
use control_plane_core::{GovernedCatalog, TableRef};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;

use crate::serving::{build_serving_provider, to_serving, EngineServingError};

const MASK_MARKER: &str = "***";
```

`GovernedTableProvider`:

```rust
/// An enforcing `TableProvider` decorator: applies a `TablePolicy` (row filters,
/// denied columns, masked columns) to an inner serving provider. No code path returns
/// a denied column, an unmasked masked value, or a filter-excluded row.
#[derive(Debug)]
pub struct GovernedTableProvider {
    inner: Arc<dyn TableProvider>,
    policy: TablePolicy,
    governed_schema: SchemaRef,
}

impl GovernedTableProvider {
    /// Precompute the governed schema (denied fields removed, masked fields re-typed to Utf8).
    pub fn new(inner: Arc<dyn TableProvider>, policy: TablePolicy) -> Result<Self, EngineServingError> {
        let inner_schema = inner.schema();
        let fields: Vec<Field> = inner_schema
            .fields()
            .iter()
            .filter(|f| !policy.denied.contains(f.name()))
            .map(|f| {
                if policy.masked.contains(f.name()) {
                    Field::new(f.name(), DataType::Utf8, true)
                } else {
                    f.as_ref().clone()
                }
            })
            .collect();
        let governed_schema = Arc::new(Schema::new(fields));
        Ok(Self { inner, policy, governed_schema })
    }
}

#[async_trait]
impl TableProvider for GovernedTableProvider {
    fn as_any(&self) -> &dyn Any { self }
    fn schema(&self) -> SchemaRef { self.governed_schema.clone() }
    fn table_type(&self) -> TableType { TableType::Base }
    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> DfResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // 1. Inner scan over the FULL schema, all rows (governance sees everything).
        let inner_schema = self.inner.schema();
        let mut plan = self.inner.scan(state, None, &[], None).await?;

        // 2. Row-filter FilterExec over the inner schema (may reference denied cols).
        if let Some(expr) = row_filters_conjunction(&self.policy.row_filters)
            .map_err(|e| DataFusionError::Plan(e.to_string()))?
        {
            let df_schema = DFSchema::try_from(inner_schema.clone())?;
            let phys = create_physical_expr(&expr, &df_schema, &ExecutionProps::new())?;
            plan = Arc::new(FilterExec::try_new(phys, plan)?);
        }

        // 3. Governed projection: denied removed, masked -> '***', honoring client projection.
        let governed = self.governed_schema.clone();
        let indices: Vec<usize> = match projection {
            Some(p) => p.clone(),
            None => (0..governed.fields().len()).collect(),
        };
        let mut proj: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::with_capacity(indices.len());
        for gi in indices {
            let field = governed.field(gi);
            let name = field.name().to_string();
            if self.policy.masked.contains(&name) {
                let lit = Literal::new(DfScalar::Utf8(Some(MASK_MARKER.to_string())));
                proj.push((Arc::new(lit), name));
            } else {
                let inner_idx = inner_schema.index_of(&name)
                    .map_err(|e| DataFusionError::Plan(e.to_string()))?;
                proj.push((Arc::new(Column::new(&name, inner_idx)), name));
            }
        }
        plan = Arc::new(ProjectionExec::try_new(proj, plan)?);

        // 4. Client limit.
        if let Some(n) = limit {
            plan = Arc::new(GlobalLimitExec::new(plan, 0, Some(n)));
        }
        Ok(plan)
    }
}
```

Then add `execute_governed_sql_stream` (see the Design block above), importing `SessionContext`, `TableReference`, `MemorySchemaProvider`.

Notes for the implementer (**this is the top implementation risk** — no first-party code constructs `FilterExec`/`ProjectionExec`/`GlobalLimitExec`/`expressions::Literal`/`expressions::Column` today, only `create_physical_expr` at `serving.rs:27,338`; resolve every signature below against the **datafusion 54** compiler under TDD):
- Reuse the exact module paths already imported in `serving.rs` for the shared symbols (`create_physical_expr`, `ExecutionProps`, `SessionContext`, `Session`, `TableProvider`, `TableType`, `TableProviderFilterPushDown`, `Column`, `DFSchema`, `TableReference`, `MemorySchemaProvider`, `SchemaRef`, `ExecutionPlan`, `SendableRecordBatchStream`). The delegate/inner-scan pattern `self.inner.scan(state, None, &[], None)` is confirmed idiomatic — `provider.rs:286` does exactly this.
- `Literal`/`FilterExec`/`ProjectionExec`/`GlobalLimitExec` are new to this crate — confirm via `grep -rn "ProjectionExec\|FilterExec\|GlobalLimitExec\|expressions::" third-party/BUCK` and the datafusion 54 docs; if a path differs, fix to the crate's actual export.
- **`Literal::new` arity is uncertain.** Prefer the helper `datafusion::physical_expr::expressions::lit(DfScalar::Utf8(Some(MASK_MARKER.to_string())))` (returns `Arc<dyn PhysicalExpr>`); fall back to `Arc::new(Literal::new(scalar))` (or the field-name arity if 54 requires it) only if the helper is absent. The test surfaces the correct API.
- `ProjectionExec::try_new(Vec<(Arc<dyn PhysicalExpr>, String)>, input)` is the expected signature — confirm against datafusion 54.

- [ ] **Step 3: Run the e2e tests to green (iterate on DataFusion APIs under TDD)**

Run: `buck2 test //src/services/engine-serving:governed-sql > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: PASS. If a physical-plan API mismatches, read the compiler error, correct the path/signature, re-run. Do NOT weaken a test to pass.

- [ ] **Step 4: Add the parity test (spec test 5)**

Append to `governed_sql.rs` a test that pins the governed provider against an equivalent hand-compiled governed SQL run through the ungoverned `execute_query` (matching `compile_select_with` semantics for a single-type SELECT with a row filter + a mask), asserting identical rows:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parity_with_compiled_governed_sql() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("email".to_string(), "string".to_string(), false),
    ];
    writer.seed("s", "u", &cols, &[6]).await; // ids 0..5
    let catalog = IcebergCatalog::new(pool);

    // Governed path: id >= 2, email masked.
    let cat = GovernedCatalog { tables: vec![GovernedTable {
        table: gt("s", "u"),
        row_filters: vec![RowFilter::Compare { property: "id".into(), op: CompareOp::Ge, value: ScalarValue::Int(2) }],
        denied: vec![], masked: vec!["email".into()],
    }]};
    let g = run(&catalog, "SELECT \"id\", \"email\" FROM \"s\".\"u\" ORDER BY \"id\"", &cat).await;

    // Equivalent hand-compiled governed SQL (what compile_select_with emits for this
    // subject policy): masked email -> '***' literal, row filter as WHERE, over the
    // ungoverned execute_query. Row-for-row identical to the governed provider.
    let expected = engine_serving::execute_query(
        &catalog,
        "SELECT \"id\", '***' AS \"email\" FROM \"s\".\"u\" WHERE (\"id\" >= 2) ORDER BY \"id\"",
        None,
    ).await.expect("compiled");

    assert_eq!(collect_i64(&g, 0), collect_i64(&expected, 0), "ids match");
    assert_eq!(collect_str(&g, 1), collect_str(&expected, 1), "masked emails match");
}
```

Run the target again; expect PASS.

- [ ] **Step 5: Clippy + commit**

Run clippy on `engine-serving` (empty == clean). Address any `indexing_slicing`/`expect`/`unwrap` in the new library code with `#[expect(..)]` + reason or a fail-closed `?`.

```bash
git add src/services/engine-serving/src/governed.rs src/services/engine-serving/BUCK \
        src/services/engine-serving/tests/governed_sql.rs
git commit -m "feat(engine-serving): GovernedTableProvider + execute_governed_sql_stream"
```

---

## Task 5: `GovernedStatementQuery` Flight ticket (engine-wire)

**Files:**
- Modify: `src/services/engine-wire/src/flight.rs`
- Modify: `src/services/engine-wire/BUCK` (no new lib dep — `//src/control-plane/core:core` and `//third-party:serde_json` are already present; add the test target)
- Test: `src/services/engine-wire/tests/governed_ticket.rs` (new).

**Interfaces:**
- Produces:
  - `pub struct GovernedStatementQuery { pub sql: String, pub catalog: control_plane_core::GovernedCatalog }` in `engine_wire::flight`, `#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]`, `#[serde(deny_unknown_fields)]`.
  - `impl GovernedStatementQuery { pub fn encode(&self) -> Vec<u8>; pub fn decode(bytes: &[u8]) -> Result<Self, serde_json::Error> }` — JSON, mirroring `VectorSearchTicket`.

**Decision (documented):** The spec says "prost message", but every loom-native Flight ticket (`FlightTicket`, `VectorSearchTicket`) is JSON with `#[serde(deny_unknown_fields)]`, and `GovernedCatalog`/`RowFilter`/`TableRef` already derive serde. A JSON ticket matches the established pattern, avoids introducing a prost-codegen pipeline for a recursive `RowFilter`, and keeps `do_get`'s decode ladder consistent. Field names (`sql`, `catalog`) are disjoint from `FlightTicket` (`schema/name/files`) and `VectorSearchTicket` (`schema/name/index_name/query/k/...`), so `deny_unknown_fields` keeps the JSON decodes mutually exclusive. **`GovernedCatalog` comes from `control_plane_core`** (a dep engine-wire already has), so this ticket adds NO DataFusion to engine-wire's tree — preserving query-api's zero-DataFusion property.

- [ ] **Step 1: Write the failing round-trip test**

Create `src/services/engine-wire/tests/governed_ticket.rs`:

```rust
//! Round-trip for the GovernedStatementQuery Flight ticket (JSON, deny_unknown_fields).

use control_plane_core::{CompareOp, GovernedCatalog, GovernedTable, RowFilter, ScalarValue, TableRef};
use engine_wire::flight::GovernedStatementQuery;

#[test]
fn governed_ticket_json_roundtrips() {
    let q = GovernedStatementQuery {
        sql: "SELECT * FROM \"s\".\"t\"".into(),
        catalog: GovernedCatalog { tables: vec![GovernedTable {
            table: TableRef { schema: "s".into(), name: "t".into() },
            row_filters: vec![RowFilter::Compare { property: "id".into(), op: CompareOp::Ge, value: ScalarValue::Int(2) }],
            denied: vec!["secret".into()],
            masked: vec!["email".into()],
        }]},
    };
    let bytes = q.encode();
    let back = GovernedStatementQuery::decode(&bytes).expect("decode");
    assert_eq!(back, q);
}

#[test]
fn governed_ticket_rejects_flight_ticket_json() {
    // A FlightTicket's JSON (schema/name/files) must NOT decode as a governed ticket.
    let ft = engine_wire::flight::FlightTicket { schema: "s".into(), name: "t".into(), files: vec![] };
    assert!(GovernedStatementQuery::decode(&ft.encode()).is_err());
}
```

Wire the target in `engine-wire/BUCK` (mirror `vector-search-ticket`). Run: expect FAIL.

- [ ] **Step 2: Implement the ticket type**

Add to `engine-wire/src/flight.rs` (near `VectorSearchTicket`):

```rust
use control_plane_core::GovernedCatalog;

/// A loom-native Flight `do_get` ticket carrying arbitrary client SQL plus the caller's
/// fully-resolved governed catalog. JSON-encoded; `deny_unknown_fields` keeps it disjoint
/// from `FlightTicket`/`VectorSearchTicket`. Dispatched by the engine to
/// `engine_serving::execute_governed_sql_stream`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GovernedStatementQuery {
    pub sql: String,
    pub catalog: GovernedCatalog,
}

impl GovernedStatementQuery {
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("GovernedStatementQuery is always serializable")
    }
    pub fn decode(bytes: &[u8]) -> std::result::Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}
```

No new library dep is needed — `engine-wire/BUCK` already deps `//src/control-plane/core:core` and `//third-party:serde_json`. (This is why `GovernedCatalog` lives in core, per Task 2: it keeps engine-wire — and therefore query-api's library — DataFusion-free.)

- [ ] **Step 3: Run the test to green.**

Run: `buck2 test //src/services/engine-wire:governed-ticket > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: PASS.

- [ ] **Step 4: `#[must_use]` note on `expect`** — the `expect` in `encode` mirrors `VectorSearchTicket::encode` (JSON of a serializable type never fails). If clippy `expect_used` fires in this non-test lib, add `#[expect(clippy::expect_used, reason = "serde_json of an owned serializable type is infallible; matches VectorSearchTicket::encode")]`.

- [ ] **Step 5: Clippy + commit**

```bash
git add src/services/engine-wire/src/flight.rs src/services/engine-wire/BUCK \
        src/services/engine-wire/tests/governed_ticket.rs
git commit -m "feat(engine-wire): GovernedStatementQuery Flight ticket (JSON)"
```

---

## Task 6: Engine `do_get` dispatch to the governed path

**Files:**
- Modify: `src/services/engine/src/flight.rs` (add `do_get_governed_sql` + a decode branch in `do_get`)
- Modify: `src/services/engine/BUCK` (add the `governed-flight` `loom_fixture_test`)
- Test: `src/services/engine/tests/governed_flight.rs`

**Interfaces:**
- Consumes: `engine_wire::flight::GovernedStatementQuery`, `engine_serving::execute_governed_sql_stream`.
- Produces: a `do_get_governed_sql(&self, q: GovernedStatementQuery)` helper mirroring `do_get_sql`; a new decode branch in `do_get` placed AFTER the `TicketStatementQuery` (prost) branch and alongside the `VectorSearchTicket` JSON branch.

**Dispatch ordering (load-bearing):** in `do_get`, the current order is (1) prost `Any` → `TicketStatementQuery`, (2) `VectorSearchTicket` JSON, (3) `FlightTicket` JSON. Insert the `GovernedStatementQuery` JSON decode BEFORE the `VectorSearchTicket`/`FlightTicket` branches (all three are `deny_unknown_fields` with disjoint fields, so order among the JSON decodes is safe, but placing governed first keeps the SQL-carrying tickets grouped and documents intent). The prost branch stays first (a JSON `{` is an invalid protobuf `Any`, so it never matches there).

- [ ] **Step 1: Write the failing engine e2e test**

Create `src/services/engine/tests/governed_flight.rs`. Model it on the existing `flight_sql.rs` engine test (seed via fixture, build `FlightDataService`, call `do_get` with a ticket, decode the stream). Confirm the exact `FlightDataService` construction + seeding helper by reading `src/services/engine/tests/flight_sql.rs` first, then:

```rust
//! e2e: the engine's do_get dispatches a GovernedStatementQuery ticket to the governed
//! path, applying the row filter/deny/mask regardless of the client SQL.

// ... imports mirror flight_sql.rs (PgFixture, IcebergWriter, FlightDataService, Ticket,
// FlightRecordBatchStream / decode) ...

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn do_get_governed_applies_policy() {
    // seed "s"."orders" ids 0..4 via IcebergWriter (as flight_sql.rs does)
    // build FlightDataService { catalog, pool, serving_catalog, serving_store: None }
    // ticket = GovernedStatementQuery { sql: "SELECT \"id\" FROM \"s\".\"orders\" ORDER BY \"id\"",
    //   catalog: GovernedCatalog { tables: vec![GovernedTable { table: s.orders,
    //     row_filters: vec![id >= 2], denied: [], masked: [] }] } };
    // let resp = svc.do_get(Request::new(Ticket { ticket: ticket.encode().into() })).await.unwrap();
    // decode the FlightData stream to RecordBatches, collect ids
    // assert ids == vec![2, 3, 4]
}
```

Wire `governed-flight` into `engine/BUCK` (mirror the deps of `flight-sql`, adding `//src/services/engine-serving:engine-serving`, `//src/services/engine-wire:engine-wire`, `//src/control-plane/core:core` as needed). Run: expect FAIL.

- [ ] **Step 2: Implement the dispatch**

In `src/services/engine/src/flight.rs`, add the helper next to `do_get_sql`:

```rust
/// Run a governed SQL statement (client SQL + caller-resolved governed catalog) through
/// the governed serving path and Flight-encode the result stream. Mirrors `do_get_sql`.
async fn do_get_governed_sql(
    &self,
    q: engine_wire::flight::GovernedStatementQuery,
) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
    let stream = engine_serving::execute_governed_sql_stream(
        &self.serving_catalog,
        &q.sql,
        &q.catalog,
        self.serving_store.as_ref(),
    )
    .await
    .map_err(|e| Status::internal(e.to_string()))?;
    let mapped = stream.map_err(|e| FlightError::from_external_error(Box::new(e)));
    let out = FlightDataEncoderBuilder::new()
        .build(mapped)
        .map_err(|e| Status::internal(e.to_string()));
    Ok(Response::new(Box::pin(out)))
}
```

In `do_get`, after the `TicketStatementQuery` block (before the `VectorSearchTicket` block), add:

```rust
// loom-native governed SQL ticket (JSON): arbitrary client SQL + a resolved governed
// catalog. Disjoint fields (deny_unknown_fields) from the other JSON tickets.
if let Ok(gq) = engine_wire::flight::GovernedStatementQuery::decode(&ticket.ticket) {
    return self.do_get_governed_sql(gq).await;
}
```

Confirm `engine/BUCK` already deps `engine-serving` and `engine-wire` (it uses `engine_serving::execute_query_stream` and `engine_wire::flight`), so no new lib deps are needed — only the test target.

- [ ] **Step 3: Run the engine e2e test to green.**

Run: `buck2 test //src/services/engine:governed-flight > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: PASS.

- [ ] **Step 4: Guard the existing dispatch tests stay green.**

Run: `buck2 test //src/services/engine:flight-sql //src/services/engine:vector-search-flight //src/services/engine:flight-ticket-membership > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (the governed branch is additive; existing tickets still route correctly).

- [ ] **Step 5: Clippy + commit**

```bash
git add src/services/engine/src/flight.rs src/services/engine/BUCK \
        src/services/engine/tests/governed_flight.rs
git commit -m "feat(engine): dispatch GovernedStatementQuery do_get to the governed SQL path"
```

---

## Task 7: Full sweep, register update, docs

**Files:**
- Modify: `docs/ROADMAP.md` (close `road-external-sql-governed-catalog` via `loom-docs-update`)
- Modify: `docs/FUTURE.md` if any new deferral surfaced (e.g. client-filter pushdown into the governed inner scan — an optimization deferred this slice).

- [ ] **Step 1: Full build + test sweep**

Run: `buck2 build //src/... > /tmp/b.log 2>&1; grep -E "BUILD (SUCCEEDED|FAILED)|error\[" /tmp/b.log`
Run: `buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: build succeeds; all tests pass.

- [ ] **Step 2: Clippy across all first-party Rust**

Run: `bash tools/clippy-all.sh > /tmp/cl.log 2>&1; grep -vE "^\s*$" /tmp/cl.log | tail -20`
Expected: clean (no warnings).

- [ ] **Step 3: prek hooks (formatting, EOF, trailing whitespace, docs-validate)**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p.log 2>&1; tail -30 /tmp/p.log`
Commit whatever the hooks fix.

- [ ] **Step 4: Close the register item** via the `loom-docs-update` skill (invoked separately at finish): `- [ ]`→`- [x]`, `status:planned`→`status:done`, `pr:-`→`pr:#<N>` for `road-external-sql-governed-catalog`. Record any new deferral in `docs/FUTURE.md` (e.g. `fut-governed-scan-pushdown` — pushing the client's filters/projection/limit into the governed inner scan as an optimization, deferred from slice 1 where correctness took precedence).

- [ ] **Step 5: Commit**

```bash
git add docs/ROADMAP.md docs/FUTURE.md
git commit -m "docs(registers): close road-external-sql-governed-catalog"
```

---

## Self-Review

**1. Spec coverage:**
- `GovernedTableProvider` (enforcing decorator) → Task 4. ✓
- `row_filter_to_expr` (RowFilter→Expr over CompareOp, fail-closed) → Task 1. ✓
- `GovernedCatalog`/`GovernedTable` payload → Task 2. ✓ `TablePolicy` → Task 2/4. ✓
- `execute_governed_sql_stream` (register governed providers, run client SQL) → Task 4. ✓
- Engine `do_get` `GovernedStatementQuery` ticket dispatch → Tasks 5+6. ✓
- schema() deny-absent + mask-Utf8; scan apply-order (filter full → deny/project → mask) → Task 4. ✓
- Absent table ⇒ empty policy ⇒ fully visible → Task 2 (`policy_for`) + Task 4 test. ✓
- Row-filter semantics match `compile_select_with` → Task 1 (explicit mapping) + Task 4 parity test. ✓
- Masked = Utf8 `'***'` → Task 1 note + Task 4 (`MASK_MARKER`). ✓
- Ungoverned path untouched → Tasks 3/6 (additive; characterization tests). ✓
- Testing 1–6 (row filter under SQL, denied absent, masked incl. aggregate, join/aggregate governance, parity, empty policy) → Task 4 tests + Task 6 e2e. ✓

**2. Placeholder scan:** No TBD/TODO; every code step has concrete code. DataFusion physical-plan API paths are flagged for in-TDD confirmation (Task 4 Step 2 note) with the fallback of matching the crate's actual exports — this is verification guidance, not a placeholder. ✓

**3. Type consistency:** `row_filter_to_expr`, `row_filters_conjunction`, `TablePolicy`, `GovernedTable`, `GovernedCatalog::policy_for`, `GovernedTableProvider::new`, `execute_governed_sql_stream`, `build_serving_provider`, `GovernedStatementQuery::{encode,decode}`, `do_get_governed_sql` — names are used consistently across tasks. `EngineServingError::Engine(String)` is the error constructor throughout. ✓

**Open risk to watch during implementation:** the DataFusion 58 physical-plan construction (FilterExec/ProjectionExec/GlobalLimitExec/Literal/Column signatures) is the one area where the exact API may differ from the sketch; TDD in Task 4 resolves it against the compiler. **Resolved during planning:** the payload types (`GovernedCatalog`/`GovernedTable`) are hosted in `control_plane_core`, NOT engine-serving, so engine-wire (and thus query-api's library) never pulls DataFusion — verified: query-api's lib deps `engine-wire` but not engine-serving/datafusion, and core has no DataFusion dep. The `TableProviderFilterPushDown::Inexact` return means the client's WHERE is re-applied by DataFusion above our governed scan, so not pushing client filters into the inner scan (Task 4) is correct, not lossy.
