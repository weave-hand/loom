# Filter operators (`between`, text-pattern ops, `eq_filters`→`filters`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add two additive caller-predicate operator families — a `between:lo,hi` range and case-insensitive text-pattern matching (`contains`/`startswith`/`endswith`) — to loom's `GET /objects/{type}` filter grammar, and rename the internal `ObjectQuery.eq_filters` field to `filters`.

**Architecture:** Caller predicates parse in `query-api/src/filter.rs::coerce_predicate` (URI query param → typed `CallerPredicate`) and render in `query-api/src/sql.rs::caller_predicate_sql` (predicate → SQL fragment with bound params). Both reuse the shared `control_plane_core::CompareOp`. We extend `CompareOp` with four caller-only variants (`Between`, `Contains`, `StartsWith`, `EndsWith`), parse+coerce them in `filter.rs`, render them in `sql.rs`, and — because **three** ACL/write-path matches on `CompareOp` are exhaustive (no unguarded `_`) — explicitly handle the new variants there (they are caller-predicate-only, so those sites fail closed: `validate_row_filter`/`row_filter_to_expr` return `Err`, and `write_filter::compare_cell` returns `None` = UNKNOWN → deny). Every operand stays a bound parameter; text-pattern operands are LIKE-metacharacter-escaped and `%`-wrapped at coerce time so the SQL renderer just binds them.

**Tech Stack:** Rust, buck2 (`rust_test` / `loom_fixture_test` targets), `thiserror`, `time`. Serving path is DataFusion via internal Flight SQL; SQL is compiled with `DataFusionDialect`.

## Global Constraints

- **Tests are `rust_test` integration targets only** — never inline `#[cfg(test)]` modules. Put tests in `tests/<name>.rs` wired in the crate `BUCK`. The `no-inline-tests` prek hook enforces this.
- **Injection boundary is untouched:** every operand is a bound placeholder (`dialect.placeholder(params.len())`) — never string-interpolated. Column names are trusted ontology identifiers (quoted via `col_ref`).
- **Panic-safety clippy lints are enforced** on production code: no `unwrap`, `expect`, `panic`, `todo`, `unreachable`, `indexing_slicing`, `dbg`. Use `#[expect(lint, reason = "...")]` for a justified local exception; test code is exempt via `loom_rust_test`/`loom_fixture_test`.
- **Text-pattern is case-insensitive (`ILIKE`)** with escaped, `%`-wrapped, **bound** operands and an explicit `ESCAPE '\'` clause — no raw wildcard operator in this slice.
- **`between` is exactly-two-operand sugar**, same per-type coercion/rules as `ge`/`le`.
- **The rename is internal only** — no external wire change (the GET wire carries `?amount=gt:5`, not a field literally named `eq_filters`).
- Build in the cloud with `buck2 build -M none //src/...` and **scope tests** to the touched targets — never a bare whole-tree `buck2 build/test //src/...` (ENOSPC on the ~38 GiB cap).
- Run buck2 test/bxl redirected to a file, never piped through `tail`/`head`: `buck2 test … > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`.

---

## File Structure

- `src/control-plane/core/src/acl.rs` — add 4 `CompareOp` variants; reject them in `validate_row_filter` (ACL row filters cannot use caller-only ops).
- `src/control-plane/core/tests/row_filter_validation.rs` — test that an ACL `RowFilter` using a new op is rejected.
- `src/services/engine-serving/src/governed.rs` — the exhaustive `match op` inside `build_expr` (delegated from `row_filter_to_expr`): add arms returning an error for the 4 new ops (ACL path).
- `src/services/engine-serving/tests/row_filter_to_expr.rs` — test the rejection.
- `src/services/query-api/src/write_filter.rs` — `compare_cell`'s exhaustive `match op` (write-path ACL eval): add an arm returning `None` (UNKNOWN → deny) for the 4 new ops. (`order_cell` already has an unguarded `_ => return None`, so it needs no change.)
- `src/services/query-api/src/filter.rs` — parse+coerce `between` (two operands) and text-pattern ops (string-only guard + LIKE escape/wrap); add `escape_like` helper.
- `src/services/query-api/tests/filter_coerce.rs` — parse/coerce/escape/guard tests.
- `src/services/query-api/src/sql.rs` — `caller_predicate_sql`: add `Between` and text-pattern arms **before** the scalar `_` arm.
- `src/services/query-api/tests/sql_compile.rs` — SQL-shape + bound-param + ESCAPE tests.
- `src/services/query-api/src/handler.rs`, `src/http.rs`, `src/flight_export.rs` — rename `ObjectQuery.eq_filters` → `filters` (field + read/assign sites).
- ~16 `.rs` test files across `query-api`/`transform` that construct `ObjectQuery { eq_filters: … }` — rename to `filters`.
- `src/services/query-api/tests/typed_filter_e2e.rs` — new `between` + text-pattern e2e cases.

---

## Task 1: Extend `CompareOp` and handle new variants on the ACL/write paths

Adding variants to the shared `control_plane_core::CompareOp` makes **three** matches non-exhaustive (compile errors); those matches deliberately have **no unguarded `_` arm** so a new variant forces a decision. The decision here: the four new operators are **caller-predicate-only**; ACL policy row filters cannot use them, so all three sites fail closed — `validate_row_filter` and `build_expr` (engine-serving) return `Err`, and `write_filter::compare_cell` returns `None` (UNKNOWN → row denied). This keeps `op_sql`'s `unreachable!` arm safe (a validated ACL filter can never carry a new op).

**Files:**
- Modify: `src/control-plane/core/src/acl.rs` (enum at `:60`; `validate_row_filter` at `:140-160`)
- Modify: `src/services/engine-serving/src/governed.rs` (the exhaustive `match op` inside `build_expr` at `:71-92`, reached via `row_filter_to_expr`)
- Modify: `src/services/query-api/src/write_filter.rs` (`compare_cell` `match op` at `:26-36`)
- Test: `src/control-plane/core/tests/row_filter_validation.rs`
- Test: `src/services/engine-serving/tests/row_filter_to_expr.rs`

**Interfaces:**
- Produces: `control_plane_core::CompareOp::{Between, Contains, StartsWith, EndsWith}` — four new unit variants on the existing `#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)] enum CompareOp`. Later tasks (`filter.rs`, `sql.rs`) construct and match on these.
- Produces: `validate_row_filter` returns `Err` for a `RowFilter::Compare` whose `op` is any of the four; `row_filter_to_expr` returns `Err(EngineServingError::Engine(...))` for them.

- [ ] **Step 1: Write the failing test — ACL validation rejects a new op**

Add to `src/control-plane/core/tests/row_filter_validation.rs`:

```rust
#[test]
fn caller_only_ops_are_rejected_in_acl_row_filters() {
    for op in [
        CompareOp::Between,
        CompareOp::Contains,
        CompareOp::StartsWith,
        CompareOp::EndsWith,
    ] {
        let f = RowFilter::Compare {
            property: "name".into(),
            op,
            value: ScalarValue::Text("x".into()),
        };
        let err = validate_row_filter(&f, None).unwrap_err();
        assert!(
            err.contains("not valid in an ACL row filter"),
            "op {op:?} should be rejected, got: {err}"
        );
    }
}
```

Confirm the test file already imports `CompareOp`, `RowFilter`, `ScalarValue`, `validate_row_filter` (it uses them elsewhere); add any missing to the `use` line.

- [ ] **Step 2: Run it to verify it fails to COMPILE (variants don't exist yet)**

Run: `buck2 test //src/control-plane/core:row-filter-validation > /tmp/t1.log 2>&1; grep -E "error\[|Tests finished|FAIL|no variant" /tmp/t1.log`
Expected: build error — `no variant named Between` on `CompareOp`.

- [ ] **Step 3: Add the four variants to `CompareOp`**

In `src/control-plane/core/src/acl.rs`, extend the enum (keep existing variants; append):

```rust
    /// `value` is ignored.
    IsNotNull,
    /// Caller-predicate-only: `col BETWEEN lo AND hi`. Two operands live on the
    /// query-api `CallerPredicate`, not on `ScalarValue`; rejected in ACL row filters.
    Between,
    /// Caller-predicate-only: case-insensitive `col ILIKE '%operand%'`. Rejected in ACL row filters.
    Contains,
    /// Caller-predicate-only: case-insensitive `col ILIKE 'operand%'`. Rejected in ACL row filters.
    StartsWith,
    /// Caller-predicate-only: case-insensitive `col ILIKE '%operand'`. Rejected in ACL row filters.
    EndsWith,
}
```

- [ ] **Step 4: Reject the new ops in `validate_row_filter`**

In `src/control-plane/core/src/acl.rs`, extend the inner `match op` in `validate_row_filter` (the `Compare` arm). Add a new arm **before** the `Eq | Ne | …` arm so the exhaustive match compiles:

```rust
                CompareOp::IsNull | CompareOp::IsNotNull => {}
                // Caller-predicate-only operators: not part of the ACL policy
                // grammar (Between needs two operands; text-pattern needs ILIKE).
                // Reject so op_sql's `unreachable!` arm stays unreachable.
                CompareOp::Between
                | CompareOp::Contains
                | CompareOp::StartsWith
                | CompareOp::EndsWith => {
                    return Err(format!("{op:?} is not valid in an ACL row filter"));
                }
                // Listed exhaustively (no `_`) so a future CompareOp variant is a
                // compile error here, forcing a deliberate structural-rule decision
                // rather than silently getting scalar treatment.
                CompareOp::Eq
                | CompareOp::Ne
```

- [ ] **Step 5: Run the ACL validation test to verify it passes**

Run: `buck2 test //src/control-plane/core:row-filter-validation > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t1.log`
Expected: PASS.

- [ ] **Step 6: Write the failing test — `row_filter_to_expr` rejects a new op**

Inspect `src/services/engine-serving/tests/row_filter_to_expr.rs` for its existing test shape and the public entry it calls (the function under test in `governed.rs`; mirror an existing negative test there). Add:

```rust
#[test]
fn caller_only_ops_are_rejected_when_lowering_to_expr() {
    let f = RowFilter::Compare {
        property: "name".into(),
        op: CompareOp::Contains,
        value: ScalarValue::Text("x".into()),
    };
    // <call the same lowering entry the other tests in this file use>, e.g.:
    let err = row_filter_to_expr(&f).unwrap_err();
    assert!(format!("{err}").contains("not supported in a row filter"));
}
```

Match the exact function name/visibility and error type the existing tests in this file use (read the top of the file first — it may be `governed::row_filter_to_expr` or a re-export). Add the same for `Between`/`StartsWith`/`EndsWith` if the file's style tests each.

- [ ] **Step 7: Run it to verify it fails to COMPILE**

Run: `buck2 test //src/services/engine-serving:row-filter-to-expr > /tmp/t2.log 2>&1; grep -E "error\[|non-exhaustive|Tests finished|FAIL" /tmp/t2.log`
Expected: build error — non-exhaustive `match op` in `governed.rs` (and/or the new test not compiling).

(If the exact target name differs, find it with `grep -n 'row_filter_to_expr\|name = ' src/services/engine-serving/BUCK`.)

- [ ] **Step 8: Reject the new ops in `row_filter_to_expr`**

In `src/services/engine-serving/src/governed.rs`, add an arm to the `match op` (before the closing brace, after `IsNotNull`):

```rust
                CompareOp::IsNull => Ok(c.is_null()),
                CompareOp::IsNotNull => Ok(c.is_not_null()),
                CompareOp::Between
                | CompareOp::Contains
                | CompareOp::StartsWith
                | CompareOp::EndsWith => Err(EngineServingError::Engine(format!(
                    "{op:?} is not supported in a row filter"
                ))),
```

Confirm `EngineServingError::Engine(String)` is the right constructor by reading the neighboring `In`/`NotIn` error at `governed.rs:80`; reuse whatever variant/shape that arm uses.

- [ ] **Step 9: Run the engine-serving test to verify it passes**

Run: `buck2 test //src/services/engine-serving:row-filter-to-expr > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t2.log`
Expected: PASS.

- [ ] **Step 10: Handle the new ops in the write-path `compare_cell` (fail closed to `None`)**

`compare_cell` (`write_filter.rs:24`) returns `Option<bool>` (three-valued: `None` = UNKNOWN, which its callers treat as "not `Some(true)`" → the row is denied). Its `match op` (`:26-36`) is exhaustive apart from a *guarded* `_` (`_ if matches!(cell, SqlValue::Null)`), which does not cover the new variants — so adding them breaks the build (E0004). A stored `Policy` `RowFilter` can never legitimately carry a caller-only op (`validate_row_filter` rejects it, Step 4), so the correct handling is UNKNOWN. Add an arm after `Lt | Le | Gt | Ge`:

```rust
        Lt | Le | Gt | Ge => order_cell(cell, op, operand),
        // Caller-predicate-only ops never appear in a stored Policy RowFilter
        // (validate_row_filter rejects them). Fail closed: None => UNKNOWN => not
        // Some(true) => row denied.
        Between | Contains | StartsWith | EndsWith => None,
```

(`use CompareOp::*;` at the top of `compare_cell` brings the variants into scope. `order_cell` at `:94` already has an unguarded `_ => return None`, so it needs no change.)

- [ ] **Step 11: Build all three crates to confirm no other exhaustive match broke**

Run: `buck2 build -M none //src/control-plane/core:core //src/services/engine-serving:engine-serving //src/services/query-api:query-api > /tmp/b1.log 2>&1; grep -E "error\[|BUILD SUCCEEDED|Build ID" /tmp/b1.log; echo done`
Expected: builds succeed. If a new non-exhaustive-match error (E0004) appears in a file this plan didn't list, add an arm handling the new ops there (fail closed — `Err` on a `Result` path, `None` on an `Option` path) and note it in the commit.

- [ ] **Step 12: Commit**

```bash
git add src/control-plane/core/src/acl.rs src/control-plane/core/tests/row_filter_validation.rs \
        src/services/engine-serving/src/governed.rs src/services/engine-serving/tests/row_filter_to_expr.rs \
        src/services/query-api/src/write_filter.rs
git commit -m "feat(query): add Between/Contains/StartsWith/EndsWith CompareOp variants, fail closed on ACL/write paths"
```

---

## Task 2: Parse + coerce `between:lo,hi` in `coerce_predicate`

**Files:**
- Modify: `src/services/query-api/src/filter.rs` (`coerce_predicate` at `:115-173`)
- Test: `src/services/query-api/tests/filter_coerce.rs`

**Interfaces:**
- Consumes: `CompareOp::Between` (Task 1); `split_set_operands` (existing, `filter.rs:81`); `coerce_filter` (existing).
- Produces: `coerce_predicate(col, ty, "between:11,25")` → `CallerPredicate { op: Between, values: vec![lo, hi] }` (exactly two coerced operands, in order). Wrong arity or a type-mismatched operand → `FilterError::BadValue`.

- [ ] **Step 1: Write the failing tests**

Add to `src/services/query-api/tests/filter_coerce.rs`:

```rust
#[test]
fn between_parses_two_operands() {
    let p = coerce_predicate("amount", "Integer", "between:11,25").unwrap();
    assert_eq!(p.op, CompareOp::Between);
    assert_eq!(p.values, vec![SqlValue::Int(11), SqlValue::Int(25)]);
}

#[test]
fn between_wrong_arity_is_rejected() {
    assert!(matches!(
        coerce_predicate("amount", "Integer", "between:11"),
        Err(FilterError::BadValue(_, _))
    ));
    assert!(matches!(
        coerce_predicate("amount", "Integer", "between:1,2,3"),
        Err(FilterError::BadValue(_, _))
    ));
}

#[test]
fn between_type_mismatch_is_rejected_like_ge() {
    assert!(matches!(
        coerce_predicate("amount", "Integer", "between:foo,25"),
        Err(FilterError::BadValue(_, _))
    ));
}
```

- [ ] **Step 2: Run to verify failure**

Run: `buck2 test //src/services/query-api:filter-coerce > /tmp/t3.log 2>&1; grep -E "error\[|no variant|Tests finished|FAIL" /tmp/t3.log`
Expected: FAIL (Between arm not yet handled — `between:` currently falls into the "not a recognized op" branch and is treated as an `Eq` operand, so the op assertion fails).

- [ ] **Step 3: Add the `between` token and arm**

In `filter.rs::coerce_predicate`, add the token to the `head` match:

```rust
        "isnotnull" => Some(IsNotNull),
        "between" => Some(Between),
        _ => None,
```

Then add an arm to the outer `match op` (before the scalar `Some(o)` arm):

```rust
        Some(Between) => {
            let r = rest.ok_or_else(|| bad("between requires two operands"))?;
            let parts = split_set_operands(r).map_err(bad)?;
            if parts.len() != 2 {
                return Err(bad("between requires exactly two operands (lo,hi)"));
            }
            let mut values = Vec::with_capacity(2);
            for part in parts {
                values.push(coerce_filter(column, logical_ty, &part)?);
            }
            Ok(mk(Between, values))
        }
```

Note: `use control_plane_core::CompareOp::*;` at the top of `coerce_predicate` already brings `Between` into scope.

- [ ] **Step 4: Run to verify pass**

Run: `buck2 test //src/services/query-api:filter-coerce > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t3.log`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/services/query-api/src/filter.rs src/services/query-api/tests/filter_coerce.rs
git commit -m "feat(query): parse and coerce between:lo,hi caller predicate"
```

---

## Task 3: Parse + coerce text-pattern ops (string-only guard + LIKE escape/wrap)

**Files:**
- Modify: `src/services/query-api/src/filter.rs` (add `escape_like` helper; extend `coerce_predicate`)
- Test: `src/services/query-api/tests/filter_coerce.rs`

**Interfaces:**
- Consumes: `CompareOp::{Contains, StartsWith, EndsWith}` (Task 1); `json_repr_of` + `JsonRepr` (already imported `filter.rs:5`).
- Produces: `coerce_predicate(col, "String", "contains:AC")` → `CallerPredicate { op: Contains, values: vec![SqlValue::Text("%AC%")] }` — the single operand is LIKE-escaped then `%`-wrapped per op. Non-string property → `FilterError`. `escape_like(raw: &str) -> String` prefixes each `\`, `%`, `_` with `\`.

- [ ] **Step 1: Write the failing tests**

Add to `src/services/query-api/tests/filter_coerce.rs`:

```rust
#[test]
fn contains_wraps_and_is_case_insensitive_op() {
    let p = coerce_predicate("name", "String", "contains:AC").unwrap();
    assert_eq!(p.op, CompareOp::Contains);
    assert_eq!(p.values, vec![SqlValue::Text("%AC%".into())]);
}

#[test]
fn startswith_and_endswith_anchor() {
    let s = coerce_predicate("name", "String", "startswith:AC").unwrap();
    assert_eq!(s.op, CompareOp::StartsWith);
    assert_eq!(s.values, vec![SqlValue::Text("AC%".into())]);
    let e = coerce_predicate("name", "String", "endswith:AC").unwrap();
    assert_eq!(e.op, CompareOp::EndsWith);
    assert_eq!(e.values, vec![SqlValue::Text("%AC".into())]);
}

#[test]
fn text_pattern_escapes_like_metacharacters() {
    // A literal % / _ / \ in the operand is escaped so it matches the character.
    let p = coerce_predicate("name", "String", r"contains:50%_\x").unwrap();
    assert_eq!(p.values, vec![SqlValue::Text(r"%50\%\_\\x%".into())]);
}

#[test]
fn text_pattern_on_non_string_is_rejected() {
    assert!(matches!(
        coerce_predicate("amount", "Integer", "contains:5"),
        Err(FilterError::BadValue(_, _))
    ));
}
```

- [ ] **Step 2: Run to verify failure**

Run: `buck2 test //src/services/query-api:filter-coerce > /tmp/t4.log 2>&1; grep -E "no variant|Tests finished|FAIL" /tmp/t4.log`
Expected: FAIL (tokens unrecognized → treated as `Eq`, op assertion fails).

- [ ] **Step 3: Add the `escape_like` helper**

In `filter.rs` (near `split_set_operands`):

```rust
/// Escape LIKE/ILIKE metacharacters in a text-pattern operand: each `\`, `%`, or
/// `_` is prefixed with the SQL escape char `\` so a literal metacharacter in the
/// caller's operand matches the character itself (not a wildcard). The caller wraps
/// the result with unescaped `%` sentinels for the chosen anchor. The escaped string
/// is bound as a parameter; the SQL is rendered with an explicit `ESCAPE '\'` clause.
fn escape_like(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len() + 2);
    for c in raw.chars() {
        if matches!(c, '\\' | '%' | '_') {
            out.push('\\');
        }
        out.push(c);
    }
    out
}
```

- [ ] **Step 4: Add the tokens and arm to `coerce_predicate`**

Add tokens to the `head` match:

```rust
        "between" => Some(Between),
        "contains" => Some(Contains),
        "startswith" => Some(StartsWith),
        "endswith" => Some(EndsWith),
        _ => None,
```

Add an arm to the outer `match op` (before the scalar `Some(o)` arm):

```rust
        Some(o @ (Contains | StartsWith | EndsWith)) => {
            let r = rest.ok_or_else(|| bad("text-pattern operator requires an operand"))?;
            // Carry the source error — `clippy::map_err_ignore` is enforced; a bare
            // `|_|` that drops `e` fails the lint gate (mirror `coerce_filter`'s style).
            let repr = json_repr_of(logical_ty).map_err(|e| {
                FilterError::BadValue(column.to_string(), format!("unknown logical type: {}", e.0))
            })?;
            if !matches!(repr, JsonRepr::PlainString) {
                return Err(bad("text-pattern operators apply to string properties only"));
            }
            let esc = escape_like(r);
            let pattern = match o {
                Contains => format!("%{esc}%"),
                StartsWith => format!("{esc}%"),
                EndsWith => format!("%{esc}"),
                // The outer pattern guarantees o ∈ {Contains,StartsWith,EndsWith};
                // keep the build total without a panic per the panic-safety lints.
                _ => format!("%{esc}%"),
            };
            Ok(mk(o, vec![SqlValue::Text(pattern)]))
        }
```

- [ ] **Step 5: Run to verify pass**

Run: `buck2 test //src/services/query-api:filter-coerce > /tmp/t4.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t4.log`
Expected: PASS (both Task 2 and Task 3 cases).

- [ ] **Step 6: Commit**

```bash
git add src/services/query-api/src/filter.rs src/services/query-api/tests/filter_coerce.rs
git commit -m "feat(query): parse contains/startswith/endswith with LIKE escaping (string-only)"
```

---

## Task 4: Render `between` + text-pattern in `caller_predicate_sql`

The new ops must be rendered **before** the scalar `_` arm, which calls `op_sql` (whose `_ => unreachable!` would panic on the new variants).

**Files:**
- Modify: `src/services/query-api/src/sql.rs` (`caller_predicate_sql` at `:269-303`)
- Test: `src/services/query-api/tests/sql_compile.rs`

**Interfaces:**
- Consumes: `CallerPredicate` with `op ∈ {Between, Contains, StartsWith, EndsWith}` (Tasks 2–3); `compile_select(table, allowed_cols, mask_cols, row_filters, predicates, derived, limit)` (existing entry; caller predicates are the 5th arg).
- Produces: SQL fragments `(col BETWEEN ? AND ?)` (two bound params) and `(col ILIKE ? ESCAPE '\')` (one bound param), pushed in conjunct order.

- [ ] **Step 1: Write the failing tests**

Add to `src/services/query-api/tests/sql_compile.rs` (mirror `compiles_acl_compare_leaf_as_bound_param`; caller predicates go in arg 5, row_filters `&[]`):

```rust
#[test]
fn compiles_between_as_two_bound_params() {
    let p = CallerPredicate {
        column: "amount".into(),
        op: CompareOp::Between,
        values: vec![SqlValue::Int(11), SqlValue::Int(25)],
    };
    let (sql, params) = compile_select(
        &t(),
        &["id".into()],
        &[],
        &[],
        std::slice::from_ref(&p),
        &[],
        100,
    )
    .unwrap();
    assert_eq!(
        sql,
        r#"SELECT "id" FROM "main"."orders" WHERE ("amount" BETWEEN ? AND ?) LIMIT 100"#
    );
    assert_eq!(params, vec![SqlValue::Int(11), SqlValue::Int(25)]);
}

#[test]
fn compiles_contains_as_ilike_with_escape_and_bound_param() {
    let p = CallerPredicate {
        column: "name".into(),
        op: CompareOp::Contains,
        values: vec![SqlValue::Text("%AC%".into())],
    };
    let (sql, params) = compile_select(
        &t(),
        &["id".into()],
        &[],
        &[],
        std::slice::from_ref(&p),
        &[],
        100,
    )
    .unwrap();
    assert_eq!(
        sql,
        r#"SELECT "id" FROM "main"."orders" WHERE ("name" ILIKE ? ESCAPE '\') LIMIT 100"#
    );
    assert_eq!(params, vec![SqlValue::Text("%AC%".into())]);
}
```

- [ ] **Step 2: Run to verify failure**

Run: `buck2 test //src/services/query-api:sql-compile > /tmp/t5.log 2>&1; grep -E "assertion|Tests finished|FAIL" /tmp/t5.log`
Expected: FAIL — the new ops currently hit the scalar `_` arm and call `op_sql` (which would `unreachable!`), or produce wrong SQL. (Confirm the target name with `grep -n 'sql_compile\|name = ' src/services/query-api/BUCK` → `sql-compile`.)

- [ ] **Step 3: Add the render arms**

In `sql.rs::caller_predicate_sql`, insert **before** the final `_ =>` (scalar) arm:

```rust
        Between => {
            debug_assert_eq!(p.values.len(), 2, "between predicate must have two operands");
            let base = params.len();
            for v in &p.values {
                params.push(v.clone());
            }
            let lo = dialect.placeholder(base + 1);
            let hi = dialect.placeholder(base + 2);
            format!("({col} BETWEEN {lo} AND {hi})")
        }
        Contains | StartsWith | EndsWith => {
            debug_assert_eq!(p.values.len(), 1, "text-pattern predicate must have one operand");
            let base = params.len();
            if let Some(v) = p.values.first() {
                params.push(v.clone());
            }
            format!("({col} ILIKE {} ESCAPE '\\')", dialect.placeholder(base + 1))
        }
```

(`use control_plane_core::CompareOp::*;` at the top of `caller_predicate_sql` brings the variants into scope.)

- [ ] **Step 4: Run to verify pass**

Run: `buck2 test //src/services/query-api:sql-compile > /tmp/t5.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t5.log`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/services/query-api/src/sql.rs src/services/query-api/tests/sql_compile.rs
git commit -m "feat(query): render between (BETWEEN ? AND ?) and text-pattern (ILIKE ? ESCAPE) predicates"
```

---

## Task 5: Rename `ObjectQuery.eq_filters` → `filters`

Pure mechanical rename, internal only — aligns the field name with the operator grammar it carries and with `ExportCommand.filters`. No external wire change.

**Files:**
- Modify: `src/services/query-api/src/handler.rs` (`ObjectQuery` field `:48`; read site `:263-264`)
- Modify: `src/services/query-api/src/http.rs` (`:140`, `:153`, `:165`)
- Modify: `src/services/query-api/src/flight_export.rs` (`:214`, `:255` — `eq_filters: cmd.filters…` → `filters: cmd.filters…`)
- Modify: every `.rs` test file constructing `ObjectQuery { eq_filters: … }` (see grep in Step 2).

**Interfaces:**
- Produces: `ObjectQuery { type_name, filters: Vec<(String, String)>, ids }`. The field is read at `handler.rs::compile_object_read` as `q.filters`.

- [ ] **Step 1: Rename the struct field and doc comment**

In `handler.rs`:

```rust
/// A read request: an ontology type plus optional filters on allowed columns.
pub struct ObjectQuery {
    pub type_name: String,
    /// Caller filter predicates as `(column, raw)` pairs — parsed by
    /// `filter::coerce_predicate` (eq/ne/lt/le/gt/ge/in/nin/isnull/isnotnull/between/
    /// contains/startswith/endswith). Repeated keys AND together.
    pub filters: Vec<(String, String)>,
    /// Object-set input: scope the read to these identity values (an `In` predicate on
    /// the declared identity). Empty = no scoping.
    pub ids: Vec<String>,
}
```

Update the read site at `handler.rs:263-264` (`q.eq_filters` → `q.filters`, `Vec::with_capacity(q.eq_filters.len())` → `q.filters.len()`).

- [ ] **Step 2: Rename all remaining references**

Find every `.rs` reference (exclude `docs/`):

```bash
grep -rln 'eq_filters' src/ --include='*.rs'
```

For each hit, replace the identifier `eq_filters` with `filters`. In `http.rs` this is the local `let mut eq_filters` + its `.push` + the `eq_filters,` struct-init shorthand (rename the local too, or use `filters,`). In `flight_export.rs` the two sites are `eq_filters: cmd.filters.clone()` / `eq_filters: cmd.filters` → `filters: cmd.filters.clone()` / `filters: cmd.filters`. In each test file it is an `ObjectQuery { … eq_filters: … }` struct literal → `filters:`.

Apply with (review the diff after):

```bash
grep -rl 'eq_filters' src/ --include='*.rs' | xargs sed -i 's/\beq_filters\b/filters/g'
```

Then fix the two follow-on lint traps the blanket sed creates:

1. **`redundant_field_names` (enforced style lint).** At `src/services/query-api/tests/typed_filter_e2e.rs:215` the sed rewrites `eq_filters: filters,` (where `filters` is a local from `let run = |filters: Vec<(String, String)>|` at `:208`) into `filters: filters,`. Change it to the field shorthand:

```rust
        filters,
```

2. Confirm `http.rs`'s struct-init shorthand at `:165` became `filters,` (it was `eq_filters,` with a matching local, so the sed already produced the shorthand — verify, don't double-edit).

- [ ] **Step 3: Verify no field reference to `eq_filters` remains**

The word-boundary sed intentionally leaves the substring `eq_filters` inside longer identifiers — two **test-function names** in `sql_compile.rs` (`eq_filters_only_form_the_where_clause`, `chain_eq_filters_bind_per_position_in_chain_order`) and a doc comment in `export_command.rs:29` — because `_` is a word char (`\beq_filters\b` won't match `eq_filters_…`). Those are harmless. Verify no *field* reference survives:

Run: `grep -rnE '\beq_filters\b|\.eq_filters|eq_filters *:' src/ --include='*.rs'; echo "exit=$?"`
Expected: no matches (`exit=1`). If any remain, inspect and fix by hand. (Optionally update the `export_command.rs:29` comment `eq_filters` → `filters` for tidiness — not required for the build.)

- [ ] **Step 4: Build query-api + its e2e crates and the transform crate that referenced the field**

Run: `buck2 build -M none //src/services/query-api:query-api //src/services/transform:transform > /tmp/b5.log 2>&1; grep -E "error\[|BUILD SUCCEEDED|Build ID" /tmp/b5.log; echo done`
Expected: builds succeed (the rename is internal; nothing external references the old name).

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor(query): rename ObjectQuery.eq_filters to filters"
```

---

## Task 6: End-to-end coverage for `between` and text-pattern reads

Prove, against the real engine, that `between` returns exactly what `ge`+`le` returns, text-pattern anchors match case-insensitively, and a literal `%` matches the character (escaping works end-to-end incl. the `ESCAPE '\'` clause parsing in DataFusion).

**Files:**
- Modify: `src/services/query-api/tests/typed_filter_e2e.rs` (a `loom_fixture_test`)

**Interfaces:**
- Consumes: the e2e driver already used in `typed_filter_e2e.rs` (HTTP GET `/objects/{type}?col=op:...` or the router helper). Read the file's existing tests first and mirror their seed/driver exactly — reuse `e2e_support` helpers (`tref`/`land`/`prop`/`get`) rather than re-copying setup.

- [ ] **Step 1: Read the existing e2e tests to learn the driver**

Read `src/services/query-api/tests/typed_filter_e2e.rs` end to end. Identify: how a type is seeded with rows, how a filtered read is issued (query-param string), and how returned ids/rows are asserted. Base the new tests on that exact pattern — do not invent a new harness.

- [ ] **Step 2: Write the failing e2e tests**

Add (adapt column/type/seed names to the file's fixture; the shapes below assume a numeric `amount` and a string `name` on the seeded type):

```rust
#[tokio::test]
async fn between_matches_ge_and_le() {
    // <seed rows with amount 5,11,20,25,30 using the file's existing setup>
    let via_between = read_ids("amount=between:11,25").await;
    let via_ge_le = read_ids("amount=ge:11&amount=le:25").await;
    assert_eq!(via_between, via_ge_le);
    assert_eq!(via_between, vec![/* the 11,20,25 ids */]);
}

#[tokio::test]
async fn contains_is_case_insensitive_and_anchors() {
    // <seed names: "ACME", "Tacme", "beacon">
    assert_eq!(read_ids("name=contains:ac").await, vec![/* ACME, Tacme, beacon ids */]);
    assert_eq!(read_ids("name=startswith:ac").await, vec![/* ACME id */]);
    assert_eq!(read_ids("name=endswith:me").await, vec![/* ACME, Tacme ids */]);
}

#[tokio::test]
async fn contains_literal_percent_matches_the_character() {
    // <seed names: "50% off", "50 off">
    assert_eq!(read_ids("name=contains:50%25").await, vec![/* only "50% off" */]);
    // note: %25 is the URL-encoding of a literal '%' in the query string; if the
    // file's driver takes an already-decoded param, pass "50%" directly.
}
```

`read_ids` stands for the file's existing "issue a governed read, collect ids" helper — use its real name/signature. If the driver does not URL-decode, pass the raw operand string the harness expects.

- [ ] **Step 3: Run to verify failure (or that they exercise the new path)**

Run: `buck2 test //src/services/query-api:typed-filter-e2e > /tmp/t6.log 2>&1; grep -E "assertion|panicked|Tests finished|FAIL" /tmp/t6.log`
Expected: the tests run against the built path; if the seed/driver names are off they fail on setup — fix names to match the file. (This is a fixture test — routed local automatically by `loom_fixture_test`.)

- [ ] **Step 4: Iterate until green**

Adjust seed values and expected id vectors to match the fixture. If `ILIKE ? ESCAPE '\'` fails to parse in the engine, capture the engine error from `/tmp/t6.log` and record it — the fallback (documented in Risk) is `LOWER(col) LIKE LOWER(?) ESCAPE '\'`, which would change Task 4's render arm and its `sql_compile` assertion. Do not silently switch dialang without updating both.

Run: `buck2 test //src/services/query-api:typed-filter-e2e > /tmp/t6.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t6.log`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/services/query-api/tests/typed_filter_e2e.rs
git commit -m "test(query): e2e between + text-pattern reads against the engine"
```

---

## Task 7: Full verification, clippy, and docs register close-out

**Files:**
- Modify: `docs/ROADMAP.md` (close `road-filter-operators` via `loom-docs-update`)

- [ ] **Step 1: Run every touched test target**

Run:
```bash
buck2 test //src/control-plane/core:row-filter-validation \
           //src/services/engine-serving:row-filter-to-expr \
           //src/services/query-api:filter-coerce \
           //src/services/query-api:sql-compile \
           //src/services/query-api:typed-filter-e2e > /tmp/all.log 2>&1
grep -E "Tests finished|FAIL|error\[" /tmp/all.log
```
Expected: all PASS. Also run the broader query-api/transform e2e targets that construct `ObjectQuery` (the rename touched them) — at minimum `//src/services/query-api:...` targets named in the Task 5 grep — to confirm the rename didn't break them.

- [ ] **Step 2: Clippy on the touched crates**

Run: `./tools/clippy-all.sh > /tmp/clippy.log 2>&1; grep -E "clippy|error|warning" /tmp/clippy.log | head` (or, faster, check the specific crates' `[clippy.txt]` sub-targets). Expected: clean — no new lint. Pay attention to `indexing_slicing`, `match_wildcard_for_single_variants`, and `unreachable` on the new arms.

- [ ] **Step 3: prek hooks (formatting, EOF, trailing whitespace)**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/prek.log 2>&1; grep -E "Failed|Passed|error" /tmp/prek.log`
Commit anything the hooks change.

- [ ] **Step 4: Close the register item**

Use the `loom-docs-update` skill to flip `road-filter-operators` in `docs/ROADMAP.md` from `- [ ]` to `- [x]`, set `status:done`, and add `pr:#<N>` once the PR number is known (the skill handles the exact edit). Stage with the branch.

- [ ] **Step 5: Final commit**

```bash
git add -A
git commit -m "docs(roadmap): close road-filter-operators"
```

---

## Self-Review (author checklist — completed)

**Spec coverage:**
- `between:lo,hi` → Tasks 2 (parse/coerce), 4 (render), 6 (e2e). ✓
- Text-pattern `contains`/`startswith`/`endswith` (ILIKE, escaped, wrapped, bound, `ESCAPE '\'`) → Tasks 3 (parse/coerce/escape/string-guard), 4 (render), 6 (e2e incl. literal-`%`). ✓
- String-only guard → Task 3 (`text_pattern_on_non_string_is_rejected`). ✓
- `eq_filters`→`filters` rename across handler/http/e2es → Task 5. ✓
- Injection boundary untouched (bound params) → asserted in Task 4 (`params` vectors) and Global Constraints. ✓
- New `CompareOp` variants + the forced fail-closed decisions at the **three** exhaustive matches (`validate_row_filter`, engine-serving `build_expr`, write-path `compare_cell`) → Task 1. ✓

**Placeholder scan:** No TBD/TODO. The only intentionally-parameterized spots are the e2e seed/driver names in Task 6 (the plan directs reading the file first because the exact helper names live there) and the exact `row_filter_to_expr` entry name in Task 1 Step 6 (directs reading the file). Both are "match the existing test's names," not un-specified logic.

**Type consistency:** `CompareOp::{Between,Contains,StartsWith,EndsWith}` used identically in Tasks 1/2/3/4. `CallerPredicate { column, op, values }` matches `filter.rs`/`sql.rs`. `compile_select(table, allowed_cols, mask_cols, row_filters, predicates, derived, limit)` argument order matches `sql.rs:445` and the Task 4 test call. `ObjectQuery { type_name, filters, ids }` consistent after Task 5. `escape_like(&str) -> String` defined and used in Task 3. ✓

## Risk

- **Additive and bounded** — new `CompareOp` variants + parse tokens + render arms; the AND-collection, ACL/visibility gating, and coercion are unchanged and run on the new ops exactly as on existing ones.
- **The one real safety point is LIKE-metacharacter escaping** — pinned by the literal-`%` coerce test (Task 3) and the e2e literal-`%` test (Task 6). Bound params keep injection off the table regardless.
- **`ILIKE ? ESCAPE '\'` must parse in DataFusion.** Pinned by the Task 6 e2e (real engine). If it does not, the fallback is `LOWER(col) LIKE LOWER(?) ESCAPE '\'` — a Task 4 render change with a matching `sql_compile` update; Task 6 Step 4 calls this out explicitly.
- **The rename is mechanical**; the footgun is a missed reference — the Step 3 grep (`exit=1`) and the crate builds (Task 5 Step 4) are the guard. Zero external wire impact.
