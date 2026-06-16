# Comparison / Set Operators on Filters Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Generalize caller query-param filters from equality-only to the full `CompareOp` surface (`ne/lt/le/gt/ge/in/nin/isnull/isnotnull`) across `read_object`, single-hop traversal, and multi-hop chains, via a value-prefixed `op:operand` grammar with typed operands.

**Architecture:** A new `CallerPredicate { column, op: CompareOp, values: Vec<SqlValue> }` (reusing the ACL `CompareOp` enum but keeping operands on `SqlValue` to preserve `Double`/`Date`/`Timestamp` typing) replaces the `(col, SqlValue)` equality pair. `filter::coerce_predicate` parses the operator + operands (reusing `coerce_filter` per operand) and validates arity; one `sql::caller_predicate_sql` renderer emits the SQL at the bare table or a `t_i` alias; the HTTP extractor switches to a duplicate-preserving multimap so repeated keys express ranges.

**Tech Stack:** Rust 2024, buck2, axum (HTTP), DuckDB-over-DuckLake serving engine, hermetic Postgres+DuckDB fixture tests (`loom_fixture_test`).

**Spec:** `docs/superpowers/specs/2026-06-16-query-comparison-set-operators-design.md`

**Conventions for every task:**
- Tests are integration `rust_test` / `loom_fixture_test` targets only — never inline `#[cfg(test)]`.
- **Never run two buck2 commands concurrently** (single daemon — they hang). One at a time.
- Don't pipe `buck2 test` through `tail`/`head` (stalls). Redirect + grep:
  `buck2 test //src/services/query-api:<tgt> > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
- Fixture tests (`typed-filter-e2e`, `multi-hop-traversal-e2e`, `link-traversal`) boot Postgres+DuckDB and take a couple minutes — run one at a time and wait.
- Commit at the end of each task; end commit messages with `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.
- Markdown files end with exactly one trailing newline, no trailing whitespace.

---

## File Structure

- `src/services/query-api/src/filter.rs` — **modify.** Add `CallerPredicate` + `coerce_predicate`; keep `coerce_filter` as the per-operand building block.
- `src/services/query-api/src/sql.rs` — **modify.** Add `caller_predicate_sql`; `ChainType.eq_filters` → `predicates: Vec<CallerPredicate>`; `compile_select`/`compile_chain` take `&[CallerPredicate]`.
- `src/services/query-api/src/handler.rs` — **modify.** `read_object` + `read_linked_chain` build `CallerPredicate`s via `coerce_predicate`.
- `src/services/query-api/src/http.rs` — **modify.** The three GET handlers extract `Query<Vec<(String,String)>>` (preserve duplicate keys); `get_linked_chain` partitions `path` out of the vec.
- `src/services/query-api/tests/filter_coerce.rs` — **modify.** `coerce_predicate` unit tests.
- `src/services/query-api/tests/sql_compile.rs` — **modify.** Update eq construction to `CallerPredicate`/`predicates`; add operator-render tests.
- `src/services/query-api/tests/typed_filter_e2e.rs` — **modify.** Add a NULL row; add an operators-end-to-end test.
- `src/services/query-api/tests/multi_hop_traversal_e2e.rs` — **modify.** Add an intermediate-hop comparison test; extend the denied-column test with an operator.
- `docs/superpowers/specs/2026-06-06-loom-roadmap.md`, `docs/FUTURE.md` — **modify.** Delivered marker + follow-ups.

---

## Task 1: `coerce_predicate` + `CallerPredicate` (pure, isolated)

Additive — defines the predicate type and the value-side parser without touching the compiler/handler yet. A pure `rust_test`, fast to iterate.

**Files:**
- Modify: `src/services/query-api/src/filter.rs`
- Test: `src/services/query-api/tests/filter_coerce.rs`

- [ ] **Step 1: Write the failing unit tests.**

Add to the top imports of `src/services/query-api/tests/filter_coerce.rs`:

```rust
use control_plane_core::CompareOp;
use query_api::filter::{CallerPredicate, coerce_predicate};
```

Append these tests:

```rust
#[test]
fn bare_value_is_eq() {
    let p = coerce_predicate("amount", "Double", "100").unwrap();
    assert_eq!(p.op, CompareOp::Eq);
    assert_eq!(p.values, vec![SqlValue::Int(100)]);
}

#[test]
fn scalar_ops_parse_and_coerce() {
    let p = coerce_predicate("amount", "Double", "gt:100").unwrap();
    assert_eq!(p.op, CompareOp::Gt);
    assert_eq!(p.values, vec![SqlValue::Int(100)]);
    let p2 = coerce_predicate("amount", "Double", "le:1.5").unwrap();
    assert_eq!(p2.op, CompareOp::Le);
    assert_eq!(p2.values, vec![SqlValue::Double(1.5)]);
    let p3 = coerce_predicate("status", "String", "ne:cancelled").unwrap();
    assert_eq!(p3.op, CompareOp::Ne);
    assert_eq!(p3.values, vec![SqlValue::Text("cancelled".into())]);
}

#[test]
fn in_and_nin_coerce_each_operand() {
    let p = coerce_predicate("id", "Long", "in:1,2,3").unwrap();
    assert_eq!(p.op, CompareOp::In);
    assert_eq!(
        p.values,
        vec![SqlValue::Int(1), SqlValue::Int(2), SqlValue::Int(3)]
    );
    let p2 = coerce_predicate("region", "String", "nin:NY,TX").unwrap();
    assert_eq!(p2.op, CompareOp::NotIn);
    assert_eq!(
        p2.values,
        vec![SqlValue::Text("NY".into()), SqlValue::Text("TX".into())]
    );
}

#[test]
fn null_ops_take_no_operand() {
    let n = coerce_predicate("c", "Timestamp", "isnull").unwrap();
    assert_eq!(n.op, CompareOp::IsNull);
    assert!(n.values.is_empty());
    let nn = coerce_predicate("c", "Timestamp", "isnotnull").unwrap();
    assert_eq!(nn.op, CompareOp::IsNotNull);
    assert!(nn.values.is_empty());
}

#[test]
fn eq_escape_forces_literal() {
    // `rest` is everything after the FIRST colon, so the literal `gt:foo` passes through.
    let p = coerce_predicate("name", "String", "eq:gt:foo").unwrap();
    assert_eq!(p.op, CompareOp::Eq);
    assert_eq!(p.values, vec![SqlValue::Text("gt:foo".into())]);
}

#[test]
fn arity_errors() {
    assert!(coerce_predicate("amount", "Double", "gt").is_err()); // scalar, no operand
    assert!(coerce_predicate("status", "String", "in:").is_err()); // set, empty
    assert!(coerce_predicate("c", "Timestamp", "isnull:x").is_err()); // null op given an operand
}

#[test]
fn bad_operand_is_error() {
    assert!(coerce_predicate("amount", "Double", "gt:abc").is_err());
    assert!(coerce_predicate("id", "Long", "in:1,x,3").is_err());
}
```

- [ ] **Step 2: Run the test target — expect compile failure.**

Run: `buck2 test //src/services/query-api:filter-coerce > /tmp/t.log 2>&1; grep -E "error\[|cannot find|unresolved|Tests finished|FAIL" /tmp/t.log`
Expected: unresolved import — `CallerPredicate` / `coerce_predicate` don't exist yet.

- [ ] **Step 3: Implement `CallerPredicate` + `coerce_predicate`.**

In `src/services/query-api/src/filter.rs`, add below the `FilterError` enum (after line 13) the predicate type, and below `coerce_filter` the parser. Keep `coerce_filter` exactly as-is (it stays the per-operand building block).

```rust
/// A caller filter predicate: a column, a comparison operator, and its coerced operands.
/// Reuses `control_plane_core::CompareOp` but keeps operands on `SqlValue` (which carries
/// Double/Date/Timestamp — `ScalarValue` does not), so typed filtering is not regressed.
/// Operand arity: 0 (null ops), 1 (scalar ops), or N (set ops).
#[derive(Debug, Clone, PartialEq)]
pub struct CallerPredicate {
    pub column: String,
    pub op: control_plane_core::CompareOp,
    pub values: Vec<SqlValue>,
}

/// Parse a query-param value into a typed predicate. Grammar: split at the FIRST `:` into
/// `head`/`rest`; if `head` is a known op token it is that operator (null ops take no
/// operand; scalar ops take `rest` as one operand; set ops split `rest` on `,`); otherwise
/// the whole value is an `Eq` operand. Each operand is coerced via `coerce_filter`.
pub fn coerce_predicate(
    column: &str,
    logical_ty: &str,
    raw: &str,
) -> Result<CallerPredicate, FilterError> {
    use control_plane_core::CompareOp::*;
    let bad = |m: &str| FilterError::BadValue(column.to_string(), m.to_string());
    let mk = |op, values| CallerPredicate {
        column: column.to_string(),
        op,
        values,
    };

    let (head, rest) = match raw.split_once(':') {
        Some((h, r)) => (h, Some(r)),
        None => (raw, None),
    };
    let op = match head {
        "eq" => Some(Eq),
        "ne" => Some(Ne),
        "lt" => Some(Lt),
        "le" => Some(Le),
        "gt" => Some(Gt),
        "ge" => Some(Ge),
        "in" => Some(In),
        "nin" => Some(NotIn),
        "isnull" => Some(IsNull),
        "isnotnull" => Some(IsNotNull),
        _ => None,
    };

    match op {
        // Not a recognized op token: the whole value is an Eq operand.
        None => Ok(mk(Eq, vec![coerce_filter(column, logical_ty, raw)?])),
        Some(o @ (IsNull | IsNotNull)) => {
            if matches!(rest, Some(r) if !r.is_empty()) {
                return Err(bad("isnull/isnotnull take no operand"));
            }
            Ok(mk(o, vec![]))
        }
        Some(o @ (In | NotIn)) => {
            let r = rest.ok_or_else(|| bad("in/nin require operands"))?;
            if r.is_empty() {
                return Err(bad("in/nin require at least one operand"));
            }
            let mut values = Vec::new();
            for part in r.split(',') {
                if part.is_empty() {
                    return Err(bad("empty operand in set"));
                }
                values.push(coerce_filter(column, logical_ty, part)?);
            }
            Ok(mk(o, values))
        }
        // Scalar ops (eq/ne/lt/le/gt/ge): exactly one operand = `rest`.
        Some(o) => {
            let r = rest.ok_or_else(|| bad("operator requires an operand"))?;
            Ok(mk(o, vec![coerce_filter(column, logical_ty, r)?]))
        }
    }
}
```

- [ ] **Step 4: Run the test target — expect PASS.**

Run: `buck2 test //src/services/query-api:filter-coerce > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: `Tests finished: Pass 1. Fail 0.` (all `coerce_filter` + new `coerce_predicate` tests pass).

- [ ] **Step 5: Commit.**

```bash
git add src/services/query-api/src/filter.rs src/services/query-api/tests/filter_coerce.rs
git commit -m "feat(query-api): coerce_predicate parses typed filter operators

CallerPredicate { column, op: CompareOp, values: Vec<SqlValue> } + coerce_predicate
with the value-prefixed op:operand grammar (bare = eq, eq: escape, null ops, set
ops, arity validation), reusing coerce_filter per operand.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Compiler renders predicates + handler builds them

Switch the caller-filter slot from equality pairs to `CallerPredicate` everywhere. The `compile_select`/`compile_chain` signature change + `ChainType` field rename force the handler and `sql_compile.rs` to change in the same commit (the lib builds as one unit). After this task, single-predicate operators work end-to-end through the handler.

**Files:**
- Modify: `src/services/query-api/src/sql.rs`
- Modify: `src/services/query-api/src/handler.rs`
- Test: `src/services/query-api/tests/sql_compile.rs`

- [ ] **Step 1: Add operator-render unit tests (failing).**

In `src/services/query-api/tests/sql_compile.rs`, add to the imports at the top:

```rust
use query_api::filter::CallerPredicate;
```

Add a helper near the existing `t`/`tr` helpers:

```rust
fn eqp(col: &str, val: SqlValue) -> CallerPredicate {
    CallerPredicate {
        column: col.into(),
        op: CompareOp::Eq,
        values: vec![val],
    }
}
```

Append these new tests:

```rust
#[test]
fn caller_predicate_gt_renders_with_param() {
    let preds = vec![CallerPredicate {
        column: "amount".into(),
        op: CompareOp::Gt,
        values: vec![SqlValue::Int(100)],
    }];
    let (sql, params) = compile_select(&t(), &["id".into()], &[], &[], &preds, &[], 10).unwrap();
    assert_eq!(
        sql,
        r#"SELECT "id" FROM "main"."orders" WHERE ("amount" > ?) LIMIT 10"#
    );
    assert_eq!(params, vec![SqlValue::Int(100)]);
}

#[test]
fn caller_predicate_in_expands_placeholders() {
    let preds = vec![CallerPredicate {
        column: "status".into(),
        op: CompareOp::In,
        values: vec![SqlValue::Text("open".into()), SqlValue::Text("paid".into())],
    }];
    let (sql, params) = compile_select(&t(), &["id".into()], &[], &[], &preds, &[], 10).unwrap();
    assert_eq!(
        sql,
        r#"SELECT "id" FROM "main"."orders" WHERE ("status" IN (?, ?)) LIMIT 10"#
    );
    assert_eq!(
        params,
        vec![SqlValue::Text("open".into()), SqlValue::Text("paid".into())]
    );
}

#[test]
fn caller_predicate_isnotnull_no_param() {
    let preds = vec![CallerPredicate {
        column: "closed_at".into(),
        op: CompareOp::IsNotNull,
        values: vec![],
    }];
    let (sql, params) = compile_select(&t(), &["id".into()], &[], &[], &preds, &[], 10).unwrap();
    assert_eq!(
        sql,
        r#"SELECT "id" FROM "main"."orders" WHERE ("closed_at" IS NOT NULL) LIMIT 10"#
    );
    assert!(params.is_empty());
}

#[test]
fn caller_predicate_range_two_same_column_ands() {
    let preds = vec![
        CallerPredicate {
            column: "amount".into(),
            op: CompareOp::Ge,
            values: vec![SqlValue::Int(100)],
        },
        CallerPredicate {
            column: "amount".into(),
            op: CompareOp::Le,
            values: vec![SqlValue::Int(200)],
        },
    ];
    let (sql, params) = compile_select(&t(), &["id".into()], &[], &[], &preds, &[], 10).unwrap();
    assert_eq!(
        sql,
        r#"SELECT "id" FROM "main"."orders" WHERE ("amount" >= ?) AND ("amount" <= ?) LIMIT 10"#
    );
    assert_eq!(params, vec![SqlValue::Int(100), SqlValue::Int(200)]);
}

#[test]
fn caller_predicate_binds_at_chain_alias() {
    let types = vec![
        ChainType {
            table: tr("main", "customer"),
            row_filters: vec![],
            predicates: vec![],
        },
        ChainType {
            table: tr("main", "orders"),
            row_filters: vec![],
            predicates: vec![CallerPredicate {
                column: "amount".into(),
                op: CompareOp::Gt,
                values: vec![SqlValue::Int(50)],
            }],
        },
    ];
    let hops = vec![LinkBacking::ForeignKey {
        from_column: "id".into(),
        to_column: "customer_id".into(),
    }];
    let (sql, params) = compile_chain(&types, &hops, &["id".to_string()], &[], 100).unwrap();
    assert_eq!(
        sql,
        "SELECT DISTINCT t_1.\"id\" FROM \"main\".\"orders\" t_1 \
         JOIN \"main\".\"customer\" t_0 ON t_0.\"id\" = t_1.\"customer_id\" \
         WHERE (t_1.\"amount\" > ?) LIMIT 100"
    );
    assert_eq!(params, vec![SqlValue::Int(50)]);
}
```

- [ ] **Step 2: Update existing eq-filter construction in the test to the new shape.**

In `src/services/query-api/tests/sql_compile.rs`:

In `ands_acl_filter_with_request_equality_filter`, change the `eq` vec and the `compile_select` arg:

```rust
    let preds = vec![eqp("status", SqlValue::Text("open".into()))];
    let (sql, params) = compile_select(
        &t(),
        &["id".into()],
        &[],
        std::slice::from_ref(&acl),
        &preds,
        &[],
        10,
    )
    .unwrap();
```

In `eq_filters_only_form_the_where_clause`:

```rust
    let preds = vec![eqp("status", SqlValue::Text("open".into()))];
    let (sql, params) = compile_select(&t(), &["id".into()], &[], &[], &preds, &[], 10).unwrap();
```

In `derived_jointable_sum_with_target_filter_orders_params_first`, change the `compile_select` request-equality argument from `&[("region".to_string(), SqlValue::Text("CA".into()))]` to `&[eqp("region", SqlValue::Text("CA".into()))]`.

In every `ChainType { ... }` literal in this file (in `chain_two_hop_fk_compiles_to_nested_joins`, `chain_fk_then_jointable_adds_mapping_join_for_that_hop_only`, `chain_params_source_eq_precedes_hop_row_filters_in_chain_order`, `chain_single_hop_jointable_renders_j1_mapping`, `chain_single_hop_reproduces_traversal_semantics`, `chain_eq_filter_on_final_target_binds_at_t_k`, `chain_eq_filters_bind_per_position_in_chain_order`), rename the field `eq_filters:` to `predicates:` and wrap any tuple contents in `eqp(...)`:
- `eq_filters: vec![]` → `predicates: vec![]`
- `eq_filters: vec![("region".into(), SqlValue::Text("CA".into()))]` → `predicates: vec![eqp("region", SqlValue::Text("CA".into()))]`
- `eq_filters: vec![("id".into(), SqlValue::Int(10))]` → `predicates: vec![eqp("id", SqlValue::Int(10))]`
- `eq_filters: vec![("sku".into(), SqlValue::Text("A".into()))]` → `predicates: vec![eqp("sku", SqlValue::Text("A".into()))]`

(The expected SQL/params assertions in all these existing tests are UNCHANGED — an `Eq` predicate renders identically to the old `(col = ?)`.)

- [ ] **Step 3: Run the test target to confirm it fails to compile.**

Run: `buck2 test //src/services/query-api:sql-compile > /tmp/t.log 2>&1; grep -E "error\[|no field|cannot find|Tests finished|FAIL" /tmp/t.log`
Expected: compile errors — `ChainType` has no field `predicates`, `compile_select`/`compile_chain` argument type mismatch.

- [ ] **Step 4: Add `caller_predicate_sql` and switch the compiler.**

In `src/services/query-api/src/sql.rs`:

Add the import near the top (after the existing `use crate::serving::SqlValue;`):

```rust
use crate::filter::CallerPredicate;
```

Add the renderer (place it just above `compile_select`, after `derived_aggregate_sql`):

```rust
/// Render one caller predicate at `alias` (empty = unqualified), pushing its operand
/// params in conjunct order. Scalar ops use `op_sql`; set ops expand to N placeholders;
/// null ops emit no param. The column is a trusted ontology identifier (quoted), every
/// operand a bound `?`.
fn caller_predicate_sql(p: &CallerPredicate, alias: &str, params: &mut Vec<SqlValue>) -> String {
    use control_plane_core::CompareOp::*;
    let col = col_ref(alias, &p.column);
    match p.op {
        In | NotIn => {
            let kw = if matches!(p.op, In) { "IN" } else { "NOT IN" };
            let mut placeholders = Vec::with_capacity(p.values.len());
            for v in &p.values {
                params.push(v.clone());
                placeholders.push("?");
            }
            format!("({col} {kw} ({}))", placeholders.join(", "))
        }
        IsNull => format!("({col} IS NULL)"),
        IsNotNull => format!("({col} IS NOT NULL)"),
        _ => {
            debug_assert_eq!(p.values.len(), 1, "scalar predicate must have one operand");
            params.push(p.values[0].clone());
            format!("({col} {} ?)", op_sql(p.op))
        }
    }
}
```

In `compile_select`: change the parameter `eq_filters: &[(String, SqlValue)],` to `predicates: &[CallerPredicate],`; update the doc comment's "`row_filters` and `eq_filters` are ANDed" to "`row_filters` and `predicates` are ANDed"; and replace the eq loop (currently lines ~280-283):

```rust
    for (col, val) in eq_filters {
        conjuncts.push(format!("({} = ?)", quote_ident(col)));
        params.push(val.clone());
    }
```

with:

```rust
    for p in predicates {
        conjuncts.push(caller_predicate_sql(p, "", &mut params));
    }
```

In `ChainType`: rename the field and its doc:

```rust
    /// Caller filter predicates for this position, bound at alias `t_i`. Position 0's
    /// predicates are the source filters (no special-case in the compiler).
    pub predicates: Vec<CallerPredicate>,
```

In `compile_chain`: update the doc comment's "Each type's `eq_filters` bind" to "Each type's `predicates` bind", and replace the eq loop in the WHERE builder (currently lines ~391-394):

```rust
        for (col, val) in &t.eq_filters {
            conjuncts.push(format!("({a}.{} = ?)", quote_ident(col)));
            params.push(val.clone());
        }
```

with:

```rust
        for p in &t.predicates {
            conjuncts.push(caller_predicate_sql(p, &a, &mut params));
        }
```

- [ ] **Step 5: Switch the handler to build predicates.**

In `src/services/query-api/src/handler.rs`:

In `read_object` (the eq-filter loop, currently lines ~157-171), replace it with:

```rust
    // Visibility first (denied/masked column -> 400, no type info leak), then parse the
    // raw value into a typed predicate (operator + coerced operands) for the column.
    let mut predicates: Vec<crate::filter::CallerPredicate> =
        Vec::with_capacity(q.eq_filters.len());
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
        let p = crate::filter::coerce_predicate(col, ty, raw)
            .map_err(|_| QueryError::BadFilter(col.clone()))?;
        predicates.push(p);
    }
```

Then change the `compile_select(...)` call in `read_object` to pass `&predicates` where it currently passes `&eq_filters` (the argument in the `eq_filters` position).

In `read_linked_chain`: both `crate::sql::ChainType { ... eq_filters: vec![] }` constructions become `predicates: vec![]`. Then replace the caller-filter loop (currently lines ~420-438) with:

```rust
    // Caller filters, governed per position: visibility first (denied/masked or unknown
    // column -> 400, no type-info leak), then parse the raw value into a typed predicate
    // (operator + coerced operands) bound at the position's alias `t_i`.
    for f in &q.filters {
        if f.position >= ctypes.len() {
            return Err(QueryError::BadFilter(f.column.clone()));
        }
        let meta = &metas[f.position];
        let allowed = project_allowed(&meta.otype.properties, &meta.denied);
        if !allowed.contains(&f.column) || meta.masked.contains(&f.column) {
            return Err(QueryError::BadFilter(f.column.clone()));
        }
        let ty = meta
            .otype
            .properties
            .iter()
            .find(|p| p.name == f.column)
            .map(|p| p.ty.as_str())
            .unwrap_or("");
        let p = crate::filter::coerce_predicate(&f.column, ty, &f.raw)
            .map_err(|_| QueryError::BadFilter(f.column.clone()))?;
        ctypes[f.position].predicates.push(p);
    }
```

- [ ] **Step 6: Run the compiler unit tests — expect PASS.**

Run: `buck2 test //src/services/query-api:sql-compile > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: `Tests finished: Pass 1. Fail 0.` (existing eq tests + the five new operator tests).

- [ ] **Step 7: Regression — run the read e2es (behavior unchanged for equality).**

Run one at a time:
`buck2 test //src/services/query-api:governed-read > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t1.log`
`buck2 test //src/services/query-api:typed-filter-e2e > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t2.log`
`buck2 test //src/services/query-api:link-traversal > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t3.log`
`buck2 test //src/services/query-api:multi-hop-traversal-e2e > /tmp/t4.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t4.log`
Expected: all `Fail 0` (bare equality filters render identically, so existing e2es are unaffected).

- [ ] **Step 8: Clippy — expect clean.**

Run: `./tools/clippy-all.sh > /tmp/c.log 2>&1; grep -iE "warning:|error" /tmp/c.log || echo CLEAN`
Expected: `CLEAN` for the query-api crate (no unused `eq_filters`, no dead code).

- [ ] **Step 9: Commit.**

```bash
git add src/services/query-api/src/sql.rs src/services/query-api/src/handler.rs src/services/query-api/tests/sql_compile.rs
git commit -m "feat(query-api): compiler renders caller predicates, handler builds them

ChainType.predicates + compile_select/compile_chain take &[CallerPredicate];
one caller_predicate_sql renderer (scalar via op_sql, set expands placeholders,
null emits no param) serves the bare table and t_i alias. Handlers parse each
raw filter via coerce_predicate. Single-predicate operators now work end-to-end.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: HTTP multimap extractor (enables repeated-key ranges)

Preserve duplicate query keys so `?amount=ge:100&amount=le:200` reaches the handler as two predicates.

**Files:**
- Modify: `src/services/query-api/src/http.rs`

- [ ] **Step 1: Switch the three GET extractors to a multimap.**

In `src/services/query-api/src/http.rs`:

`get_object`: change the extractor and the filter collection:

```rust
    Query(params): Query<Vec<(String, String)>>,
```

and replace `let eq_filters: Vec<(String, String)> = params.into_iter().collect();` with:

```rust
    // Repeated keys are preserved (a column may carry several predicates, e.g. a range);
    // the handler parses each value's operator and coerces it.
    let eq_filters = params;
```

`get_linked`: change the extractor:

```rust
    Query(params): Query<Vec<(String, String)>>,
```

and remove the now-redundant `let params: Vec<(String, String)> = params.into_iter().collect();` line (pass `params` straight into `resolve_chain_filters`).

`get_linked_chain`: change the extractor and the `path` extraction (a `HashMap::remove` no longer applies):

```rust
    Query(params): Query<Vec<(String, String)>>,
```

Replace the `path` block + the `let params: Vec<(String, String)> = ...` line with a single partition:

```rust
    // `path` is the comma-separated ordered chain of link names; every other pair is a
    // filter. Repeated filter keys are preserved (e.g. a range on one column).
    let mut path: Vec<String> = Vec::new();
    let mut filter_params: Vec<(String, String)> = Vec::with_capacity(params.len());
    for (k, v) in params {
        if k == "path" {
            path = v
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        } else {
            filter_params.push((k, v));
        }
    }
    let filters = match crate::chain_filter::resolve_chain_filters(&path, filter_params) {
        Ok(f) => f,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
```

Remove the now-unused `use std::collections::HashMap;` import if nothing else references it (check the file — `post_action` uses `serde_json::Value`, not `HashMap`).

- [ ] **Step 2: Build the binary and run the http smoke test — expect PASS.**

Run: `buck2 test //src/services/query-api:http-smoke > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: `Tests finished: Pass 1. Fail 0.` (the axum surface still compiles and serves; `Query<Vec<(String,String)>>` is a supported extractor).

- [ ] **Step 3: Clippy — expect clean (no unused HashMap import).**

Run: `./tools/clippy-all.sh > /tmp/c.log 2>&1; grep -iE "warning:|error" /tmp/c.log || echo CLEAN`
Expected: `CLEAN`.

- [ ] **Step 4: Commit.**

```bash
git add src/services/query-api/src/http.rs
git commit -m "feat(query-api): multimap query extractor for repeated filter keys

The three GET handlers extract Query<Vec<(String,String)>> so a column may carry
multiple predicates (e.g. ?amount=ge:100&amount=le:200 ranges); get_linked_chain
partitions `path` out of the vec.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Operators end-to-end (e2e through DuckDB)

Prove comparison/set/null operators run correctly against real DuckDB, including a range (two predicates on one column), a comparison on an intermediate chain hop, and that a denied column still rejects an operator filter.

**Files:**
- Test: `src/services/query-api/tests/typed_filter_e2e.rs`
- Test: `src/services/query-api/tests/multi_hop_traversal_e2e.rs`

- [ ] **Step 1: Add a NULL row to the typed-filter fixture.**

In `src/services/query-api/tests/typed_filter_e2e.rs`, in `setup`, extend the three arrays to add a 4th row `(4, NULL, NULL)` (this does not affect the existing `typed_filters_match_and_reject` assertions — row 4 matches none of them). Update the doc comment and the `RecordBatch`:

Change the `setup` doc comment line to:

```rust
/// Seed an Order table with NON-TEXT columns: id Long, amount Double, active Boolean.
/// Rows: (1, 10.5, true), (2, 20.0, false), (3, 10.5, true), (4, NULL, NULL). The caller
/// MUST keep the returned `DuckLakeWriter` alive (its TempDir holds the Parquet files).
```

Change the `ord_batch` arrays to:

```rust
    let ord_batch = RecordBatch::try_new(
        ord_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(Float64Array::from(vec![Some(10.5), Some(20.0), Some(10.5), None])),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
                None,
            ])),
        ],
    )
    .unwrap();
```

- [ ] **Step 2: Add the operators-end-to-end test (failing until run).**

Append to `src/services/query-api/tests/typed_filter_e2e.rs`:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn comparison_set_and_null_operators() {
    let fx = PgFixture::start();
    let (cp, eng, _writer, a) = setup(&fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &eng,
    };
    let ids = |rows: &query_api::handler::ObjectRows| {
        let body = objects_to_json(rows);
        let mut v: Vec<String> = body["objects"]
            .as_array()
            .unwrap()
            .iter()
            .map(|o| o["id"].as_str().unwrap().to_string())
            .collect();
        v.sort();
        v
    };
    let run = |filters: Vec<(String, String)>| {
        let deps = &deps;
        let a = a.clone();
        async move {
            read_object(
                &ObjectQuery {
                    type_name: "Order".into(),
                    eq_filters: filters,
                },
                &Subject(a),
                deps,
            )
            .await
        }
    };

    // gt on a Double column: amount > 15 -> only row 2 (20.0).
    let r = run(vec![("amount".into(), "gt:15".into())]).await.unwrap();
    assert_eq!(ids(&r), vec!["2".to_string()]);

    // Range (two predicates on one column): 11 <= amount <= 25 -> only row 2.
    let r = run(vec![
        ("amount".into(), "ge:11".into()),
        ("amount".into(), "le:25".into()),
    ])
    .await
    .unwrap();
    assert_eq!(ids(&r), vec!["2".to_string()]);

    // Set membership on Long id: id in (1,3) -> rows 1, 3.
    let r = run(vec![("id".into(), "in:1,3".into())]).await.unwrap();
    assert_eq!(ids(&r), vec!["1".to_string(), "3".to_string()]);

    // Null checks: amount isnull -> row 4; isnotnull -> rows 1,2,3.
    let r = run(vec![("amount".into(), "isnull".into())]).await.unwrap();
    assert_eq!(ids(&r), vec!["4".to_string()]);
    let r = run(vec![("amount".into(), "isnotnull".into())])
        .await
        .unwrap();
    assert_eq!(
        ids(&r),
        vec!["1".to_string(), "2".to_string(), "3".to_string()]
    );

    // Bad arity (gt with no operand) -> BadFilter (400).
    let err = run(vec![("amount".into(), "gt".into())]).await.unwrap_err();
    assert!(matches!(err, QueryError::BadFilter(_)));
}
```

- [ ] **Step 3: Run the typed-filter e2e — expect PASS.**

Run: `buck2 test //src/services/query-api:typed-filter-e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: `Tests finished: Pass 1. Fail 0.` (the original test, now with the NULL row, plus the new operators test).

- [ ] **Step 4: Add an intermediate-hop comparison test + extend the denied-column test.**

In `src/services/query-api/tests/multi_hop_traversal_e2e.rs`, append a new test (reuses the `setup`, `subject_with_role`, `grant_read`, `srcf`, `hopf` helpers; fixture: Customer 1=CA → orders 10 shipped, 11 pending → line_items 100,101 (order 10), 102 (order 11)):

```rust
#[tokio::test(flavor = "multi_thread")]
async fn intermediate_comparison_operator_narrows() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &eng,
    };
    let (a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "Order").await;
    grant_read(&cp, &role, "LineItem").await;

    // Intermediate Order.id is Long: id > 10 keeps order 11 (drops order 10) for Customer 1,
    // so only line_item 102 (which hangs off order 11) is reachable.
    let rows = read_linked_chain(
        &ChainQuery {
            from_type: "Customer".into(),
            path: vec!["orders".into(), "lineItems".into()],
            filters: vec![srcf("region", "CA"), hopf(1, "id", "gt:10")],
        },
        &Subject(a),
        &deps,
    )
    .await
    .unwrap();
    let body = objects_to_json(&rows);
    let ids: Vec<String> = body["objects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(ids, vec!["102".to_string()]);
}
```

Then, in the existing `bad_positioned_filters_are_rejected` test (which already denies the final-target `sku` column via policy), add — right after the existing denied-`sku` assertion — one more assertion proving an *operator* filter on the denied column is also rejected (visibility is checked before operator parsing):

```rust
    // An operator filter on the same denied column is rejected too (visibility precedes parse).
    let denied_op = read_linked_chain(
        &ChainQuery {
            from_type: "Customer".into(),
            path: vec!["orders".into(), "lineItems".into()],
            filters: vec![hopf(2, "sku", "ne:A")],
        },
        &Subject(a.clone()),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(denied_op, QueryError::BadFilter(c) if c == "sku"),
        "operator filter on denied column -> BadFilter; got {denied_op:?}"
    );
```

Note: this requires the subject binding `a` to still be usable at that point. The existing test uses `a.clone()` for the denied-column case and `a` for the out-of-range case. Place this new assertion BEFORE the out-of-range case that moves `a`, and use `a.clone()` (as shown). If the implementer finds the borrow already moved, reorder so all three `unwrap_err` calls use `a.clone()` except the last.

- [ ] **Step 5: Run the multi-hop e2e — expect PASS.**

Run: `buck2 test //src/services/query-api:multi-hop-traversal-e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: `Tests finished: Pass 1. Fail 0.`

- [ ] **Step 6: Commit.**

```bash
git add src/services/query-api/tests/typed_filter_e2e.rs src/services/query-api/tests/multi_hop_traversal_e2e.rs
git commit -m "test(query-api): e2e for comparison/set/null filter operators

Range (two predicates on one column), gt, in, isnull/isnotnull through read_object
(with a NULL fixture row); a gt comparison on an intermediate chain hop; an operator
filter on a denied column still rejects as BadFilter.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Docs — roadmap delivered marker + FUTURE.md follow-ups

**Files:**
- Modify: `docs/superpowers/specs/2026-06-06-loom-roadmap.md`
- Modify: `docs/FUTURE.md`

- [ ] **Step 1: Mark the slice delivered in the roadmap.**

In `docs/superpowers/specs/2026-06-06-loom-roadmap.md`, under the **Query / read path** bullets (after the "Part 6 — target / intermediate filters" entry added by the previous slice), add:

```markdown
  - *Part 7 — comparison / set operators on filters* ✅ DELIVERED
    (`2026-06-16-query-comparison-set-operators-design.md`). Caller filters across `read_object`,
    traversal, and chains (at every position) now express the full `CompareOp` surface — `ne`, `lt`,
    `le`, `gt`, `ge`, `in`, `nin`, `isnull`, `isnotnull` — via a value-prefixed `op:operand` grammar
    (bare value = `eq`; `eq:` escape; ranges as repeated keys). Operands stay typed (`SqlValue`,
    reusing `coerce_filter` per operand) so `Double`/`Date`/`Timestamp` filtering is preserved; one
    `caller_predicate_sql` renderer reuses the ACL `CompareOp`/`op_sql` machinery. Proven by
    `coerce_predicate` units, compiler-render units, and read/chain e2es.
```

In the **"Where we are"** section, append after the target/intermediate-filters paragraph:

```markdown
**Comparison / set operators** (`2026-06-16-query-comparison-set-operators-design.md`) complete the
filter arc: every caller filter, at every read path and chain position, now expresses the full
`CompareOp` surface (ranges via repeated keys), not just equality — real analytical filtering on the
governed read path.
```

- [ ] **Step 2: Add the FUTURE.md follow-ups.**

In `docs/FUTURE.md`, under the **"Ontology & read path (Step 3)"** section, append a new block:

```markdown
From the comparison/set-operators slice (`2026-06-16-query-comparison-set-operators-design.md`), which
gave caller filters the full `CompareOp` surface (`op:operand` grammar, ranges via repeated keys):

- **`or`-combined caller predicates.** All caller predicates are ANDed (matching the equality
  filters they generalize). A disjunction grammar (OR across caller predicates) is deferred.
- **`between:lo,hi` sugar.** Ranges are two predicates (`ge` + `le`) via repeated keys; a dedicated
  `between` operator is sugar only, deferred.
- **Literal comma inside an `in` operand.** `in:` splits on commas, so an operand containing a comma
  cannot be expressed — needs an escaping / alternate-delimiter convention.
- **Text-pattern matching** (`like`/`ilike`/`contains`). No such `CompareOp` variant exists today; a
  separate slice (and a new operator + safe rendering) would add it.
- **Predicates on derived (aggregate) properties.** Caller predicates validate against physical
  columns only; making derived properties filterable is shared with the derived-properties
  "filter/sort targets" follow-up.
```

- [ ] **Step 3: Run the markdown lint hooks; ensure clean.**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/lint.log 2>&1; grep -iE "Failed|Passed" /tmp/lint.log | tail -20`
Expected: `end-of-file-fixer` / `trim trailing whitespace` pass (or fix in place — include any fixes in the commit). Both `.md` files end with exactly one trailing newline.

- [ ] **Step 4: Commit.**

```bash
git add docs/superpowers/specs/2026-06-06-loom-roadmap.md docs/FUTURE.md
git commit -m "docs(query): comparison/set operators delivered; record follow-ups

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 5: Full query-api sweep (final regression).**

Run: `buck2 test //src/services/query-api/... > /tmp/sweep.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/sweep.log`
Expected: `Fail 0` across the whole query-api suite.

---

## Self-Review

**Spec coverage:**
- Value-prefixed `op:operand` grammar + recognition rule → Task 1 (`coerce_predicate`) + unit tests. ✓
- Full `CompareOp` parity incl. null checks → Task 1 (all tokens) + Task 2 (`caller_predicate_sql` renders set/null/scalar). ✓
- Operands stay on `SqlValue` (no `RowFilter` convergence) → `CallerPredicate.values: Vec<SqlValue>` (Task 1). ✓
- Per-operand coercion reusing `coerce_filter`; arity validation → Task 1. ✓
- Ranges / multiple predicates per column via repeated keys + multimap extractor → Task 3 (http) + Task 4 e2e (range). ✓
- Uniform across `read_object` / traversal / chains → Task 2 handler (`read_object` + `read_linked_chain`); single-hop forwards through the chain (unchanged). ✓
- Visibility-then-coerce governance unchanged; errors reuse `BadFilter` → Task 2 handler + Task 4 denied-column assertion. ✓
- Testing (coerce_predicate units, render units, e2e ranges/set/null/intermediate) → Tasks 1/2/4. ✓
- Docs (roadmap delivered, FUTURE follow-ups) → Task 5. ✓

**Placeholder scan:** No TBD/TODO; every code step shows full code and exact expected output. ✓

**Type consistency:** `CallerPredicate { column: String, op: CompareOp, values: Vec<SqlValue> }` defined once (Task 1), constructed by `eqp`/inline in tests and by `coerce_predicate` in handlers. `caller_predicate_sql(p, alias, params)` signature consistent. `compile_select(table, allowed_cols, mask_cols, row_filters, predicates, derived, limit)` (7 args) and `compile_chain(types, hops, allowed_cols, mask_cols, limit)` (5 args) consistent across tests and handler. `ChainType { table, row_filters, predicates }` consistent. ✓

**Note:** `op_sql` stays private in `sql.rs`; `caller_predicate_sql` is in the same module so it calls `op_sql` directly. The scalar arm only passes `Eq/Ne/Lt/Le/Gt/Ge` (set/null handled separately), so `op_sql`'s `_ => unreachable!` is never reached from the new renderer.
