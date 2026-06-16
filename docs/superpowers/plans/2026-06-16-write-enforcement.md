# Fine-Grained Write Enforcement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the action-scoped `Write` policy load-bearing — `run_action` rejects an insert that sets a policy-denied column or produces a row that fails the policy's `RowFilter`, evaluated purely in-memory.

**Architecture:** A new pure, I/O-free evaluator module (`write_filter.rs`) in the query-api crate provides `compare_cell` (typed leaf compare → three-valued `Option<bool>`), `eval` (SQL three-valued-logic tree walk), and `check_write_policy` (the gate → `WriteVerdict`). `run_action` loads `policies_for(.., Action::Write, ..)` and maps a denial to the existing `ActionError::Forbidden`. Fail-closed: a row is inserted only if its filter is *definitely TRUE*. Type coercion matches the read side exactly (numeric `Int↔Double`, temporal `Date`/`Timestamp` vs ISO-`Text`).

**Tech Stack:** Rust 2024, buck2 (`rust_test` for pure logic, `loom_fixture_test` for the Postgres+DuckDB e2e), `control_plane_core` (`RowFilter`/`CompareOp`/`ScalarValue`/`Policy`), `time` crate for temporal parsing.

**Spec:** `docs/superpowers/specs/2026-06-16-write-enforcement-design.md`

---

## Context for the implementer

You are working in **loom**, a typed-object data platform. The query-api service has an action
handler (`src/services/query-api/src/action.rs::run_action`) that inserts one new typed object per
`POST /actions/{name}`. Today it enforces only a **coarse** `Acl::check(subject, Write, target)`
gate. Slice 1 (already merged) made `acl.policy` action-scoped, so a subject can hold an independent
**`Write` policy** carrying a `row_filter` (a boolean tree over properties) and `deny_columns`. This
plan makes that policy enforced.

Key existing types you will use (all re-exported from `control_plane_core`):

```rust
// src/control-plane/core/src/acl.rs
pub enum Action { Read, Write }
pub enum CompareOp { Eq, Ne, Lt, Le, Gt, Ge, In, NotIn, IsNull, IsNotNull }
pub enum ScalarValue { Text(String), Int(i64), Bool(bool), List(Vec<ScalarValue>) } // NO Double/temporal
pub enum RowFilter {
    Compare { property: String, op: CompareOp, value: ScalarValue },
    And(Vec<RowFilter>), Or(Vec<RowFilter>), Not(Box<RowFilter>),
}
pub struct Policy { pub target: PolicyTarget, pub row_filter: Option<RowFilter>,
                    pub deny_columns: Vec<String>, pub mask_columns: Vec<String> }
// policies_for(&self, subject, action: Action, target: &PolicyTarget, page: PageReq) -> Result<Page<Policy>>
```

The inserted row's cells are `query_api::serving::SqlValue` (a SUPERSET of `ScalarValue`):

```rust
// src/services/query-api/src/serving.rs
pub enum SqlValue { Text(String), Int(i64), Bool(bool), Double(f64),
                    Date(time::Date), Timestamp(time::PrimitiveDateTime), Null }
```

The evaluator's whole job is to compare a `SqlValue` cell against a `ScalarValue` operand,
SQL-faithfully, returning `Some(true)`/`Some(false)`/`None` (UNKNOWN). The gate treats anything other
than `Some(true)` as deny (fail-closed).

**Repo rules that will bite you:**
- Tests are integration `rust_test`/`loom_fixture_test` targets ONLY — never inline `#[cfg(test)]`
  (a prek hook fails the build otherwise). Each test file is its own BUCK target.
- Run a single target: `buck2 test //src/services/query-api:<target> > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL|error\[|error:" /tmp/t.log`. **Never pipe `buck2 test` through `tail`/`head`** — redirect to a file and grep. Never run two `buck2` invocations concurrently.
- `loom_fixture_test` targets boot a hermetic Postgres+DuckDB and run locally; pure `rust_test`
  targets run anywhere.
- Markdown: end every `.md` with exactly one trailing newline, no trailing whitespace.
- Commit trailer (every commit): `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.

---

## File Structure

- **Create** `src/services/query-api/src/write_filter.rs` — the pure evaluator + gate (one
  responsibility: decide whether a concrete inserted row is permitted by the subject's Write
  policies). No I/O.
- **Modify** `src/services/query-api/src/lib.rs` — add `pub mod write_filter;`.
- **Modify** `src/services/query-api/src/action.rs` — load the Write policy and run the gate.
- **Create** `src/services/query-api/tests/write_filter.rs` — pure `rust_test`.
- **Modify** `src/services/query-api/BUCK` — add the `write-filter` `rust_test` target.
- **Modify** `src/services/query-api/tests/action_e2e.rs` — add the enforcement e2e (the existing
  `action-e2e` target already covers this file).
- **Modify** `docs/superpowers/specs/2026-06-06-loom-roadmap.md` + `docs/FUTURE.md` — mark slice 2
  delivered.

---

### Task 1: Evaluator scaffolding + `compare_cell` (typed leaf compare)

Create the module with the leaf comparator and its private helpers. This is the bulk of the
coercion logic. `eval` and `check_write_policy` come in Tasks 2 and 3.

**Files:**
- Create: `src/services/query-api/src/write_filter.rs`
- Modify: `src/services/query-api/src/lib.rs` (after `pub mod sql;`)
- Create: `src/services/query-api/tests/write_filter.rs`
- Modify: `src/services/query-api/BUCK` (new `write-filter` target near `sql-compile`)

- [ ] **Step 1: Create the module file with only a doc comment (so the crate compiles)**

`src/services/query-api/src/write_filter.rs`:

```rust
//! Pure, I/O-free evaluation of a stored `RowFilter` against a concrete row being
//! inserted by an action, plus the fine-grained Write-policy gate. The write-side
//! analog of the read path's `RowFilter`→SQL pushdown: here we have ONE known row
//! (the action's columns+values), so we evaluate the predicate in memory.
//!
//! Truth is three-valued (`Option<bool>`): `Some(true)`/`Some(false)` are known,
//! `None` is SQL UNKNOWN (a NULL cell under a value op, a type mismatch, or a failed
//! temporal parse). The gate is fail-closed — a row is allowed only if the filter is
//! `Some(true)`, mirroring "an UNKNOWN `WHERE` row is excluded from a read". Coercion
//! matches the read side exactly: numeric `Int`↔`Double`, and ISO-string operands
//! against `Date`/`Timestamp` cells.
```

- [ ] **Step 2: Add the module declaration**

In `src/services/query-api/src/lib.rs`, immediately after the line `pub mod sql;`, add:

```rust
pub mod write_filter;
```

- [ ] **Step 3: Add the BUCK test target**

In `src/services/query-api/BUCK`, after the `sql-compile` `rust_test` block (the one with
`name = "sql-compile"`), add:

```python
rust_test(
    name = "write-filter",
    crate = "write_filter",
    srcs = ["tests/write_filter.rs"],
    crate_root = "tests/write_filter.rs",
    edition = "2024",
    deps = [":query-api", "//src/control-plane/core:core"],
)
```

- [ ] **Step 4: Write the failing test for `compare_cell`**

Create `src/services/query-api/tests/write_filter.rs`:

```rust
//! Pure tests for the write-policy evaluator: the typed leaf comparator
//! (`compare_cell`), the three-valued tree walk (`eval`), and the gate
//! (`check_write_policy`). No fixture.

use control_plane_core::{CompareOp, Policy, PolicyTarget, RowFilter, ScalarValue, TypeName};
use query_api::serving::SqlValue;
use query_api::write_filter::compare_cell;

fn date(y: i32, m: u8, d: u8) -> SqlValue {
    SqlValue::Date(time::Date::from_calendar_date(y, time::Month::try_from(m).unwrap(), d).unwrap())
}

#[test]
fn compare_cell_text_eq_and_ne() {
    let cell = SqlValue::Text("open".into());
    let op = ScalarValue::Text("open".into());
    assert_eq!(compare_cell(&cell, CompareOp::Eq, &op), Some(true));
    assert_eq!(compare_cell(&cell, CompareOp::Ne, &op), Some(false));
    let other = ScalarValue::Text("closed".into());
    assert_eq!(compare_cell(&cell, CompareOp::Eq, &other), Some(false));
}

#[test]
fn compare_cell_int_ordering() {
    let cell = SqlValue::Int(5);
    assert_eq!(compare_cell(&cell, CompareOp::Ge, &ScalarValue::Int(3)), Some(true));
    assert_eq!(compare_cell(&cell, CompareOp::Lt, &ScalarValue::Int(3)), Some(false));
    assert_eq!(compare_cell(&cell, CompareOp::Le, &ScalarValue::Int(5)), Some(true));
}

#[test]
fn compare_cell_double_vs_int_coerces_numerically() {
    let cell = SqlValue::Double(19.99);
    assert_eq!(compare_cell(&cell, CompareOp::Ge, &ScalarValue::Int(10)), Some(true));
    assert_eq!(compare_cell(&cell, CompareOp::Lt, &ScalarValue::Int(10)), Some(false));
    let whole = SqlValue::Double(10.0);
    assert_eq!(compare_cell(&whole, CompareOp::Eq, &ScalarValue::Int(10)), Some(true));
}

#[test]
fn compare_cell_date_vs_iso_text() {
    let cell = date(2026, 6, 16);
    assert_eq!(
        compare_cell(&cell, CompareOp::Lt, &ScalarValue::Text("2026-07-01".into())),
        Some(true)
    );
    assert_eq!(
        compare_cell(&cell, CompareOp::Eq, &ScalarValue::Text("2026-06-16".into())),
        Some(true)
    );
    // Unparseable operand -> UNKNOWN.
    assert_eq!(
        compare_cell(&cell, CompareOp::Eq, &ScalarValue::Text("not-a-date".into())),
        None
    );
}

#[test]
fn compare_cell_bool_ordering_is_unknown_but_eq_works() {
    let cell = SqlValue::Bool(true);
    assert_eq!(compare_cell(&cell, CompareOp::Eq, &ScalarValue::Bool(true)), Some(true));
    assert_eq!(compare_cell(&cell, CompareOp::Ne, &ScalarValue::Bool(true)), Some(false));
    // Ordering on bool is deliberately undefined.
    assert_eq!(compare_cell(&cell, CompareOp::Lt, &ScalarValue::Bool(false)), None);
}

#[test]
fn compare_cell_type_mismatch_is_unknown() {
    let cell = SqlValue::Int(5);
    assert_eq!(compare_cell(&cell, CompareOp::Eq, &ScalarValue::Text("5".into())), None);
}

#[test]
fn compare_cell_null_handling() {
    let null = SqlValue::Null;
    assert_eq!(compare_cell(&null, CompareOp::IsNull, &ScalarValue::Int(0)), Some(true));
    assert_eq!(compare_cell(&null, CompareOp::IsNotNull, &ScalarValue::Int(0)), Some(false));
    // Any value op on a NULL cell is UNKNOWN.
    assert_eq!(compare_cell(&null, CompareOp::Eq, &ScalarValue::Int(0)), None);
    // IsNull on a non-null cell is false.
    let cell = SqlValue::Int(1);
    assert_eq!(compare_cell(&cell, CompareOp::IsNull, &ScalarValue::Int(0)), Some(false));
    assert_eq!(compare_cell(&cell, CompareOp::IsNotNull, &ScalarValue::Int(0)), Some(true));
}

#[test]
fn compare_cell_in_and_not_in() {
    let cell = SqlValue::Int(2);
    let list = ScalarValue::List(vec![ScalarValue::Int(1), ScalarValue::Int(2)]);
    assert_eq!(compare_cell(&cell, CompareOp::In, &list), Some(true));
    assert_eq!(compare_cell(&cell, CompareOp::NotIn, &list), Some(false));
    let miss = SqlValue::Int(9);
    assert_eq!(compare_cell(&miss, CompareOp::In, &list), Some(false));
    assert_eq!(compare_cell(&miss, CompareOp::NotIn, &list), Some(true));
    // A non-list operand under In is UNKNOWN.
    assert_eq!(compare_cell(&cell, CompareOp::In, &ScalarValue::Int(2)), None);
}

// Silence unused-import warnings for symbols used by later tasks' tests.
#[allow(dead_code)]
fn _later_task_imports(_: Policy, _: PolicyTarget, _: RowFilter, _: TypeName) {}
```

- [ ] **Step 5: Run the test to verify it fails**

Run: `buck2 test //src/services/query-api:write-filter > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL|error\[|cannot find" /tmp/t.log`
Expected: FAIL — compile error, `cannot find function compare_cell in ... write_filter`.

- [ ] **Step 6: Implement `compare_cell` and its helpers**

Append to `src/services/query-api/src/write_filter.rs`:

```rust
use control_plane_core::{CompareOp, ScalarValue};

use crate::serving::SqlValue;

/// Compare one inserted cell against a `RowFilter` leaf operand, SQL-faithfully.
/// `Some(bool)` is a known truth value; `None` is UNKNOWN. `IsNull`/`IsNotNull`
/// inspect nullness; every other op on a `Null` cell is UNKNOWN.
pub fn compare_cell(cell: &SqlValue, op: CompareOp, operand: &ScalarValue) -> Option<bool> {
    use CompareOp::*;
    match op {
        IsNull => Some(matches!(cell, SqlValue::Null)),
        IsNotNull => Some(!matches!(cell, SqlValue::Null)),
        // All value ops are UNKNOWN on a NULL cell.
        _ if matches!(cell, SqlValue::Null) => None,
        In => in_list(cell, operand),
        NotIn => not3(in_list(cell, operand)),
        Eq => eq_cell(cell, operand),
        Ne => not3(eq_cell(cell, operand)),
        Lt | Le | Gt | Ge => order_cell(cell, op, operand),
    }
}

/// Three-valued NOT.
fn not3(v: Option<bool>) -> Option<bool> {
    v.map(|b| !b)
}

/// `cell IN (list)` as three-valued OR of `cell = elem` over the list. A non-list
/// operand is UNKNOWN.
fn in_list(cell: &SqlValue, operand: &ScalarValue) -> Option<bool> {
    let items = match operand {
        ScalarValue::List(xs) => xs,
        _ => return None,
    };
    let mut any_unknown = false;
    for elem in items {
        match eq_cell(cell, elem) {
            Some(true) => return Some(true),
            Some(false) => {}
            None => any_unknown = true,
        }
    }
    if any_unknown { None } else { Some(false) }
}

/// Equality of a cell against an operand, coercing read-faithfully. `None` on a
/// type mismatch or a failed temporal parse.
fn eq_cell(cell: &SqlValue, operand: &ScalarValue) -> Option<bool> {
    use std::cmp::Ordering;
    match (cell, operand) {
        (SqlValue::Text(a), ScalarValue::Text(b)) => Some(a == b),
        (SqlValue::Int(a), ScalarValue::Int(b)) => Some(a == b),
        // Avoid a direct float `==` (clippy::float_cmp): compare via partial_cmp.
        (SqlValue::Double(a), ScalarValue::Int(b)) => {
            Some(a.partial_cmp(&(*b as f64)) == Some(Ordering::Equal))
        }
        (SqlValue::Bool(a), ScalarValue::Bool(b)) => Some(a == b),
        (SqlValue::Date(a), ScalarValue::Text(b)) => parse_date(b).map(|d| *a == d),
        (SqlValue::Timestamp(a), ScalarValue::Text(b)) => parse_ts(b).map(|t| *a == t),
        _ => None,
    }
}

/// Ordering comparison (`Lt`/`Le`/`Gt`/`Ge`). Bool ordering and cross-type pairs are
/// undefined (`None`). `Double` coerces an `Int` operand to `f64`; `Date`/`Timestamp`
/// parse an ISO-`Text` operand.
fn order_cell(cell: &SqlValue, op: CompareOp, operand: &ScalarValue) -> Option<bool> {
    use std::cmp::Ordering;
    let ord: Ordering = match (cell, operand) {
        (SqlValue::Text(a), ScalarValue::Text(b)) => a.as_str().cmp(b.as_str()),
        (SqlValue::Int(a), ScalarValue::Int(b)) => a.cmp(b),
        (SqlValue::Double(a), ScalarValue::Int(b)) => a.partial_cmp(&(*b as f64))?,
        (SqlValue::Date(a), ScalarValue::Text(b)) => a.cmp(&parse_date(b)?),
        (SqlValue::Timestamp(a), ScalarValue::Text(b)) => a.cmp(&parse_ts(b)?),
        // Bool ordering and all cross-type pairs are undefined.
        _ => return None,
    };
    Some(match op {
        CompareOp::Lt => ord.is_lt(),
        CompareOp::Le => ord.is_le(),
        CompareOp::Gt => ord.is_gt(),
        CompareOp::Ge => ord.is_ge(),
        _ => return None,
    })
}

fn parse_date(s: &str) -> Option<time::Date> {
    let fmt = time::macros::format_description!("[year]-[month]-[day]");
    time::Date::parse(s, &fmt).ok()
}

fn parse_ts(s: &str) -> Option<time::PrimitiveDateTime> {
    let fmt = time::macros::format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]");
    time::PrimitiveDateTime::parse(s, &fmt).ok()
}
```

- [ ] **Step 7: Run the test to verify it passes**

Run: `buck2 test //src/services/query-api:write-filter > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL|error" /tmp/t.log`
Expected: PASS (`Tests finished: Pass N. Fail 0.`).

- [ ] **Step 8: Verify the crate still builds and lints clean**

Run: `buck2 build //src/services/query-api:query-api '//src/services/query-api:query-api[clippy.txt]' > /tmp/b.log 2>&1; grep -nE "error|warning|BUILD (SUCCEEDED|FAILED)" /tmp/b.log; echo "--- clippy ---"; cat buck-out/*/gen/src/services/query-api/*/clippy.txt 2>/dev/null | head`
Expected: build succeeds; `clippy.txt` empty (clean).

- [ ] **Step 9: Commit**

```bash
git add src/services/query-api/src/write_filter.rs src/services/query-api/src/lib.rs \
        src/services/query-api/BUCK src/services/query-api/tests/write_filter.rs
git commit -m "$(cat <<'EOF'
feat(query): typed leaf comparator for write-policy evaluation

compare_cell: SqlValue cell vs ScalarValue operand, three-valued, with
read-parity coercion (Int<->Double, Date/Timestamp vs ISO text).

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Three-valued tree walk `eval`

Add the `RowFilter` tree evaluator over a concrete row map, with SQL three-valued AND/OR/NOT.

**Files:**
- Modify: `src/services/query-api/src/write_filter.rs`
- Modify: `src/services/query-api/tests/write_filter.rs`

- [ ] **Step 1: Write the failing tests for `eval`**

In `src/services/query-api/tests/write_filter.rs`, update the import line for `write_filter` and add
tests. Change:

```rust
use query_api::write_filter::compare_cell;
```
to:
```rust
use query_api::write_filter::{compare_cell, eval};
use std::collections::BTreeMap;
```

Add these tests (and DELETE the `_later_task_imports` helper's `RowFilter` usage is now real — keep
the helper but drop `RowFilter` from it; see note):

```rust
fn row<'a>(pairs: &'a [(&'a str, &'a SqlValue)]) -> BTreeMap<&'a str, &'a SqlValue> {
    pairs.iter().copied().collect()
}

#[test]
fn eval_compare_leaf_uses_row_cell() {
    let name = SqlValue::Text("gadget".into());
    let r = row(&[("name", &name)]);
    let f = RowFilter::Compare {
        property: "name".into(),
        op: CompareOp::Eq,
        value: ScalarValue::Text("gadget".into()),
    };
    assert_eq!(eval(&f, &r), Some(true));
}

#[test]
fn eval_absent_property_reads_as_null() {
    let r: BTreeMap<&str, &SqlValue> = BTreeMap::new();
    // name is unset -> NULL -> Eq is UNKNOWN.
    let f = RowFilter::Compare {
        property: "name".into(),
        op: CompareOp::Eq,
        value: ScalarValue::Text("gadget".into()),
    };
    assert_eq!(eval(&f, &r), None);
    // IsNull on the unset cell is true.
    let g = RowFilter::Compare {
        property: "name".into(),
        op: CompareOp::IsNull,
        value: ScalarValue::Text("x".into()),
    };
    assert_eq!(eval(&g, &r), Some(true));
}

#[test]
fn eval_and_or_not_three_valued() {
    let n = SqlValue::Int(5);
    let r = row(&[("n", &n)]);
    let t = RowFilter::Compare { property: "n".into(), op: CompareOp::Ge, value: ScalarValue::Int(1) }; // true
    let f = RowFilter::Compare { property: "n".into(), op: CompareOp::Lt, value: ScalarValue::Int(1) }; // false
    let u = RowFilter::Compare { property: "missing".into(), op: CompareOp::Eq, value: ScalarValue::Int(1) }; // unknown

    // AND: false beats unknown.
    assert_eq!(eval(&RowFilter::And(vec![f.clone(), u.clone()]), &r), Some(false));
    // AND: unknown beats true.
    assert_eq!(eval(&RowFilter::And(vec![t.clone(), u.clone()]), &r), None);
    // AND of all-true.
    assert_eq!(eval(&RowFilter::And(vec![t.clone(), t.clone()]), &r), Some(true));
    // OR: true beats unknown.
    assert_eq!(eval(&RowFilter::Or(vec![t.clone(), u.clone()]), &r), Some(true));
    // OR: unknown beats false.
    assert_eq!(eval(&RowFilter::Or(vec![f.clone(), u.clone()]), &r), None);
    // NOT(unknown) = unknown; NOT(true) = false.
    assert_eq!(eval(&RowFilter::Not(Box::new(u.clone())), &r), None);
    assert_eq!(eval(&RowFilter::Not(Box::new(t.clone())), &r), Some(false));
}

#[test]
fn eval_empty_and_or_identities() {
    let r: BTreeMap<&str, &SqlValue> = BTreeMap::new();
    assert_eq!(eval(&RowFilter::And(vec![]), &r), Some(true));
    assert_eq!(eval(&RowFilter::Or(vec![]), &r), Some(false));
}
```

Also update the `_later_task_imports` helper (it kept `RowFilter`/`TypeName` warm). `RowFilter` is
now used by real tests; reduce the helper to the still-unused symbols:

```rust
// Silence unused-import warnings for symbols used by Task 3's tests.
#[allow(dead_code)]
fn _later_task_imports(_: Policy, _: PolicyTarget, _: TypeName) {}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `buck2 test //src/services/query-api:write-filter > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL|cannot find" /tmp/t.log`
Expected: FAIL — `cannot find function eval in ... write_filter`.

- [ ] **Step 3: Implement `eval` and the three-valued combinators**

Append to `src/services/query-api/src/write_filter.rs`. First extend the top `use` line to add the
needed imports — change:

```rust
use control_plane_core::{CompareOp, ScalarValue};
```
to:
```rust
use std::collections::BTreeMap;

use control_plane_core::{CompareOp, RowFilter, ScalarValue};
```

Then append:

```rust
/// Evaluate a `RowFilter` against a concrete inserted row (`property name → cell`),
/// with SQL three-valued logic. `Some(true)` means the row satisfies the filter. An
/// absent property reads as a NULL (unset) cell.
pub fn eval(filter: &RowFilter, row: &BTreeMap<&str, &SqlValue>) -> Option<bool> {
    match filter {
        RowFilter::Compare { property, op, value } => {
            let cell = row.get(property.as_str()).copied().unwrap_or(&SqlValue::Null);
            compare_cell(cell, *op, value)
        }
        RowFilter::Not(x) => not3(eval(x, row)),
        RowFilter::And(xs) => and3(xs.iter().map(|x| eval(x, row))),
        RowFilter::Or(xs) => or3(xs.iter().map(|x| eval(x, row))),
    }
}

/// Three-valued AND: `Some(false)` if any child is false; else `None` if any unknown;
/// else `Some(true)` (empty ⇒ true).
fn and3(it: impl Iterator<Item = Option<bool>>) -> Option<bool> {
    let mut any_unknown = false;
    for v in it {
        match v {
            Some(false) => return Some(false),
            None => any_unknown = true,
            Some(true) => {}
        }
    }
    if any_unknown { None } else { Some(true) }
}

/// Three-valued OR: `Some(true)` if any child is true; else `None` if any unknown;
/// else `Some(false)` (empty ⇒ false).
fn or3(it: impl Iterator<Item = Option<bool>>) -> Option<bool> {
    let mut any_unknown = false;
    for v in it {
        match v {
            Some(true) => return Some(true),
            None => any_unknown = true,
            Some(false) => {}
        }
    }
    if any_unknown { None } else { Some(false) }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `buck2 test //src/services/query-api:write-filter > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL|error" /tmp/t.log`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/services/query-api/src/write_filter.rs src/services/query-api/tests/write_filter.rs
git commit -m "$(cat <<'EOF'
feat(query): three-valued RowFilter tree evaluator over an inserted row

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: The gate — `check_write_policy` + `WriteVerdict`

Combine deny-column (union across policies) with the row-filter conjunction (every policy's filter
must be `Some(true)`).

**Files:**
- Modify: `src/services/query-api/src/write_filter.rs`
- Modify: `src/services/query-api/tests/write_filter.rs`

- [ ] **Step 1: Write the failing tests**

In `src/services/query-api/tests/write_filter.rs`, update the `write_filter` import to:

```rust
use query_api::write_filter::{check_write_policy, compare_cell, eval, WriteVerdict};
```

Delete the now-fully-used `_later_task_imports` helper (all of `Policy`/`PolicyTarget`/`TypeName`
are used below). Add:

```rust
fn type_policy(row_filter: Option<RowFilter>, deny: &[&str]) -> Policy {
    Policy {
        target: PolicyTarget::Type(TypeName("Widget".into())),
        row_filter,
        deny_columns: deny.iter().map(|s| s.to_string()).collect(),
        mask_columns: vec![],
    }
}

#[test]
fn gate_allows_with_no_policies() {
    let cols = vec!["id".to_string(), "name".to_string()];
    let vals = vec![SqlValue::Int(1), SqlValue::Text("x".into())];
    assert_eq!(check_write_policy(&[], &cols, &vals), WriteVerdict::Allow);
}

#[test]
fn gate_denies_a_denied_column() {
    let cols = vec!["id".to_string(), "name".to_string()];
    let vals = vec![SqlValue::Int(1), SqlValue::Text("x".into())];
    let p = type_policy(None, &["name"]);
    assert_eq!(
        check_write_policy(&[p], &cols, &vals),
        WriteVerdict::DenyColumn("name".into())
    );
}

#[test]
fn gate_deny_column_is_union_across_policies() {
    let cols = vec!["id".to_string(), "secret".to_string()];
    let vals = vec![SqlValue::Int(1), SqlValue::Text("s".into())];
    let p1 = type_policy(None, &["name"]);
    let p2 = type_policy(None, &["secret"]);
    assert_eq!(
        check_write_policy(&[p1, p2], &cols, &vals),
        WriteVerdict::DenyColumn("secret".into())
    );
}

#[test]
fn gate_allows_a_conforming_row() {
    let cols = vec!["id".to_string(), "name".to_string()];
    let vals = vec![SqlValue::Int(1), SqlValue::Text("gadget".into())];
    let f = RowFilter::Compare {
        property: "name".into(),
        op: CompareOp::Eq,
        value: ScalarValue::Text("gadget".into()),
    };
    let p = type_policy(Some(f), &[]);
    assert_eq!(check_write_policy(&[p], &cols, &vals), WriteVerdict::Allow);
}

#[test]
fn gate_denies_a_row_failing_the_filter() {
    let cols = vec!["id".to_string(), "name".to_string()];
    let vals = vec![SqlValue::Int(1), SqlValue::Text("widget".into())];
    let f = RowFilter::Compare {
        property: "name".into(),
        op: CompareOp::Eq,
        value: ScalarValue::Text("gadget".into()),
    };
    let p = type_policy(Some(f), &[]);
    assert_eq!(check_write_policy(&[p], &cols, &vals), WriteVerdict::DenyRow);
}

#[test]
fn gate_row_must_satisfy_every_policy_filter() {
    let cols = vec!["id".to_string(), "name".to_string()];
    let vals = vec![SqlValue::Int(1), SqlValue::Text("gadget".into())];
    let pass = type_policy(
        Some(RowFilter::Compare {
            property: "name".into(),
            op: CompareOp::Eq,
            value: ScalarValue::Text("gadget".into()),
        }),
        &[],
    );
    let fail = type_policy(
        Some(RowFilter::Compare {
            property: "id".into(),
            op: CompareOp::Ge,
            value: ScalarValue::Int(100),
        }),
        &[],
    );
    // One policy passes, the other fails -> deny (AND across policies).
    assert_eq!(check_write_policy(&[pass, fail], &cols, &vals), WriteVerdict::DenyRow);
}

#[test]
fn gate_none_row_filter_adds_no_constraint() {
    let cols = vec!["id".to_string()];
    let vals = vec![SqlValue::Int(1)];
    let p = type_policy(None, &[]);
    assert_eq!(check_write_policy(&[p], &cols, &vals), WriteVerdict::Allow);
}

#[test]
fn gate_fail_closed_on_unknown_row_filter() {
    // A filter on a column the action does not set -> NULL -> UNKNOWN -> deny.
    let cols = vec!["id".to_string()];
    let vals = vec![SqlValue::Int(1)];
    let f = RowFilter::Compare {
        property: "name".into(),
        op: CompareOp::Eq,
        value: ScalarValue::Text("gadget".into()),
    };
    let p = type_policy(Some(f), &[]);
    assert_eq!(check_write_policy(&[p], &cols, &vals), WriteVerdict::DenyRow);
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `buck2 test //src/services/query-api:write-filter > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL|cannot find" /tmp/t.log`
Expected: FAIL — `cannot find ... check_write_policy` / `WriteVerdict`.

- [ ] **Step 3: Implement the gate**

Extend the top `use` line in `src/services/query-api/src/write_filter.rs` to bring in `Policy` —
change:

```rust
use control_plane_core::{CompareOp, RowFilter, ScalarValue};
```
to:
```rust
use control_plane_core::{CompareOp, Policy, RowFilter, ScalarValue};
```

Append:

```rust
/// The outcome of the fine-grained Write gate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteVerdict {
    Allow,
    /// An inserted column is denied by a Write policy.
    DenyColumn(String),
    /// The inserted row fails a Write policy's `row_filter`.
    DenyRow,
}

/// Enforce the subject's fine-grained Write policies against the concrete insert.
/// Deny-column is checked first (union of every policy's `deny_columns`); then the
/// row must satisfy EVERY policy's `row_filter` — the read side's ANDed conjunction.
/// A policy with no `row_filter` adds no row constraint; no policies ⇒ `Allow`.
/// `mask_columns` is ignored (a read-render concept). `columns`/`values` are the
/// parallel inserted pairs (same length, by construction in `run_action`).
pub fn check_write_policy(
    policies: &[Policy],
    columns: &[String],
    values: &[SqlValue],
) -> WriteVerdict {
    // 1. deny-column: any inserted column in the union of deny_columns.
    for col in columns {
        if policies
            .iter()
            .any(|p| p.deny_columns.iter().any(|d| d == col))
        {
            return WriteVerdict::DenyColumn(col.clone());
        }
    }
    // 2. row-filter: build name -> &cell once; every row_filter must be Some(true).
    let row: BTreeMap<&str, &SqlValue> = columns
        .iter()
        .map(|c| c.as_str())
        .zip(values.iter())
        .collect();
    for p in policies {
        if let Some(f) = &p.row_filter
            && eval(f, &row) != Some(true)
        {
            return WriteVerdict::DenyRow;
        }
    }
    WriteVerdict::Allow
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `buck2 test //src/services/query-api:write-filter > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL|error" /tmp/t.log`
Expected: PASS (all `write-filter` tests).

- [ ] **Step 5: Verify clippy is clean**

Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/b.log 2>&1; cat buck-out/*/gen/src/services/query-api/*/clippy.txt 2>/dev/null | head; grep -nE "BUILD (SUCCEEDED|FAILED)|error" /tmp/b.log`
Expected: empty `clippy.txt`, build succeeds.

- [ ] **Step 6: Commit**

```bash
git add src/services/query-api/src/write_filter.rs src/services/query-api/tests/write_filter.rs
git commit -m "$(cat <<'EOF'
feat(query): check_write_policy gate (deny-column + row-filter conjunction)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: Wire the gate into `run_action` + the enforcement e2e

Load the Write policy and run the gate before the insert. Drive it with a real Postgres+DuckDB e2e
that proves row-filter and deny-column enforcement, the allow paths, and Read/Write independence.

**Files:**
- Modify: `src/services/query-api/src/action.rs`
- Modify: `src/services/query-api/tests/action_e2e.rs`

- [ ] **Step 1: Write the failing e2e**

In `src/services/query-api/tests/action_e2e.rs`, extend the import list. Change:

```rust
use control_plane_core::{
    Acl, Action, ActionDef, ActionName, ControlPlane, Effect, ObjectType, ParamDef, PolicyTarget,
    PropertyDef, RoleId, SubjectId, TableRef, TypeName,
};
```
to:
```rust
use control_plane_core::{
    Acl, Action, ActionDef, ActionName, CompareOp, ControlPlane, Effect, ObjectType, ParamDef,
    Policy, PolicyTarget, PropertyDef, RoleId, RowFilter, ScalarValue, SubjectId, TableRef,
    TypeName,
};
```

Append this test function at the end of the file (it reuses the same `Widget` + `createWidget`
scaffolding as the existing test, then exercises a Write policy):

```rust
#[tokio::test(flavor = "multi_thread")]
async fn write_policy_enforces_row_filter_and_deny_column() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer_fx = DuckLakeWriter::new(fx.socket_path(), &db);
    writer_fx.bootstrap().await;
    writer_fx
        .seed(
            "main",
            "widget",
            &[
                ("id".into(), "BIGINT".into(), false),
                ("name".into(), "VARCHAR".into(), true),
            ],
            &[],
        )
        .await;
    let data_path = writer_fx.data_path().to_path_buf();
    let pg_conn = format!(
        "dbname={db} host={} user=postgres",
        fx.socket_path().display()
    );

    let widget = TypeName("Widget".into());
    cp.ontology()
        .define_type(ObjectType {
            name: widget.clone(),
            table: TableRef { schema: "main".into(), name: "widget".into() },
            properties: vec![
                PropertyDef { name: "id".into(), ty: "Long".into(), required: true },
                PropertyDef { name: "name".into(), ty: "String".into(), required: false },
            ],
            derived: vec![],
        })
        .await
        .unwrap();
    cp.ontology()
        .define_action(ActionDef {
            name: ActionName("createWidget".into()),
            target: widget.clone(),
            parameters: vec![
                ParamDef { name: "id".into(), ty: "Long".into(), required: true },
                ParamDef { name: "name".into(), ty: "String".into(), required: false },
            ],
        })
        .await
        .unwrap();

    let subj = SubjectId("writer".into());
    let role = RoleId("writers".into());
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    cp.grant(&role, Action::Write, PolicyTarget::Type(widget.clone()), Effect::Allow)
        .await
        .unwrap();
    cp.grant(&role, Action::Read, PolicyTarget::Type(widget.clone()), Effect::Allow)
        .await
        .unwrap();

    let engine = EmbeddedDuckDbWriter::attach(&pg_conn, &data_path).await.unwrap();
    let deps = ActionDeps { cp: &cp, action_engine: &engine };

    let count = |wf: &DuckLakeWriter| {
        let wf = wf.clone();
        async move { wf.query_scalar("SELECT count(*) FROM lake.main.widget").await }
    };

    // --- Phase 1: a Write policy with a row filter `name = "gadget"`.
    cp.set_policy(
        &role,
        Action::Write,
        Policy {
            target: PolicyTarget::Type(widget.clone()),
            row_filter: Some(RowFilter::Compare {
                property: "name".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Text("gadget".into()),
            }),
            deny_columns: vec![],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();

    // Row that fails the filter -> Forbidden, nothing written.
    let err = run_action(
        "createWidget",
        json!({ "id": "1", "name": "widget" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, ActionError::Forbidden), "row filter denies non-gadget");
    assert_eq!(count(&writer_fx).await, "0", "denied write wrote nothing");

    // Row that satisfies the filter -> allowed.
    run_action(
        "createWidget",
        json!({ "id": "1", "name": "gadget" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("conforming write allowed");
    assert_eq!(count(&writer_fx).await, "1", "conforming write landed");

    // --- Phase 2: replace the policy with a deny-column on `name` (upsert).
    cp.set_policy(
        &role,
        Action::Write,
        Policy {
            target: PolicyTarget::Type(widget.clone()),
            row_filter: None,
            deny_columns: vec!["name".into()],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();

    // Setting the denied column -> Forbidden, nothing new written.
    let err = run_action(
        "createWidget",
        json!({ "id": "2", "name": "gadget" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, ActionError::Forbidden), "deny-column blocks setting name");
    assert_eq!(count(&writer_fx).await, "1", "deny-column write wrote nothing");

    // Not setting the denied column (name is optional) -> allowed.
    run_action(
        "createWidget",
        json!({ "id": "2" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("write that omits the denied column is allowed");
    assert_eq!(count(&writer_fx).await, "2", "write omitting denied column landed");

    // --- Phase 3: a restrictive *Read* policy must NOT gate writes (independence).
    cp.set_policy(
        &role,
        Action::Read,
        Policy {
            target: PolicyTarget::Type(widget.clone()),
            row_filter: Some(RowFilter::Compare {
                property: "id".into(),
                op: CompareOp::Lt,
                value: ScalarValue::Int(0),
            }),
            deny_columns: vec!["name".into()],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();
    // The Write policy is still the deny-column-`name` one from Phase 2; a write that
    // omits name still succeeds — the Read policy is not consulted on the write path.
    run_action(
        "createWidget",
        json!({ "id": "3" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("a restrictive Read policy does not gate the write");
    assert_eq!(count(&writer_fx).await, "3", "Read policy did not block the write");
}
```

> Note on the `count` closure: `DuckLakeWriter` is `Clone` (used the same way in the existing tests).
> If the borrow checker objects to the closure capturing `writer_fx`, inline the
> `writer_fx.query_scalar("SELECT count(*) FROM lake.main.widget").await` call at each assertion
> instead — it is behavior-identical. Do not change the assertions.

- [ ] **Step 2: Run the e2e to verify it fails**

Run: `buck2 test //src/services/query-api:action-e2e > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL|panicked|row filter denies" /tmp/t.log`
Expected: FAIL — `write_policy_enforces_row_filter_and_deny_column` panics at the first
`assert!(matches!(err, ActionError::Forbidden))` (the row currently inserts because nothing enforces
the policy: the unwrap_err is actually Ok).

- [ ] **Step 3: Wire the gate into `run_action`**

In `src/services/query-api/src/action.rs`, extend the `control_plane_core` import to add `PageReq`.
Change:

```rust
use control_plane_core::{
    Action, ActionName, ControlPlane, ControlPlaneError, DatasetRef, Decision, EventType,
    LineageEvent, PolicyTarget, RunId, SubjectId,
};
```
to:
```rust
use control_plane_core::{
    Action, ActionName, ControlPlane, ControlPlaneError, DatasetRef, Decision, EventType,
    LineageEvent, PageReq, PolicyTarget, RunId, SubjectId,
};
```

Add the gate import beside the other `crate::` uses. Change:

```rust
use crate::serving::{ActionEngine, SqlValue};
```
to:
```rust
use crate::serving::{ActionEngine, SqlValue};
use crate::write_filter::{self, WriteVerdict};
```

Then, in `run_action`, insert the gate between the param parse (step 4) and the inline insert (step
5). After this block:

```rust
    // 4. Parse + validate the typed params (ordered by the action's parameter list).
    let pairs = parse_params(&action.parameters, body)?;
    let columns: Vec<String> = pairs.iter().map(|(c, _)| c.clone()).collect();
    let values: Vec<SqlValue> = pairs.iter().map(|(_, v)| v.clone()).collect();
```

insert:

```rust
    // 4b. Fine-grained Write policy: deny-write-column + row-filter-on-insert. The
    //     subject already cleared the coarse Write gate; now enforce the row/column
    //     policy against the concrete row. Fail-closed (deny on UNKNOWN). The HTTP
    //     body stays a generic 403; the reason is logged only.
    let write_policies = deps
        .cp
        .acl()
        .policies_for(subject, Action::Write, &policy_target, PageReq::unbounded())
        .await?;
    match write_filter::check_write_policy(&write_policies.items, &columns, &values) {
        WriteVerdict::Allow => {}
        WriteVerdict::DenyColumn(col) => {
            tracing::info!(action = action_name, column = %col, "write denied: policy denies column");
            return Err(ActionError::Forbidden);
        }
        WriteVerdict::DenyRow => {
            tracing::info!(action = action_name, "write denied: row fails write policy filter");
            return Err(ActionError::Forbidden);
        }
    }
```

(`policy_target` is the `PolicyTarget::Type(action.target.clone())` already bound at step 3 for the
coarse check; it is borrowed there, not moved, so it is still available here.)

- [ ] **Step 4: Run the e2e to verify it passes**

Run: `buck2 test //src/services/query-api:action-e2e > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL|panicked" /tmp/t.log`
Expected: PASS — both the pre-existing tests and the new `write_policy_enforces_row_filter_and_deny_column`.

- [ ] **Step 5: Verify the unit suite + clippy still pass**

Run: `buck2 test //src/services/query-api:write-filter > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL" /tmp/t.log; buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/b.log 2>&1; cat buck-out/*/gen/src/services/query-api/*/clippy.txt 2>/dev/null | head; grep -nE "BUILD (SUCCEEDED|FAILED)" /tmp/b.log`
Expected: `write-filter` PASS; `clippy.txt` empty; build succeeds.

- [ ] **Step 6: Commit**

```bash
git add src/services/query-api/src/action.rs src/services/query-api/tests/action_e2e.rs
git commit -m "$(cat <<'EOF'
feat(query): enforce the fine-grained Write policy in run_action

run_action loads the subject's Write policy and rejects an insert that sets a
denied column or produces a row failing the policy's row_filter. Fail-closed;
generic 403 with a logged reason. Proven by a Postgres+DuckDB e2e (row-filter
deny/allow, deny-column deny/allow, Read/Write independence).

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: Documentation — mark slice 2 delivered

**Files:**
- Modify: `docs/superpowers/specs/2026-06-06-loom-roadmap.md:200-205`
- Modify: `docs/FUTURE.md:218-224`

- [ ] **Step 1: Update the roadmap**

In `docs/superpowers/specs/2026-06-06-loom-roadmap.md`, replace the block (currently lines 200–205):

```markdown
  - *Fine-grained write governance — part 1 (control plane)* ✅ DELIVERED
    (`2026-06-16-acl-action-scoped-policies-design.md`). `acl.policy` is now action-scoped:
    `set_policy`/`clear_policy`/`policies_for` key on `(role, action, target)`, so a role holds
    independent read and write policies (parity with the already-action-scoped `role_grant`). The
    read path is explicitly `Read`-scoped (unchanged). Part 2 (service enforcement: `run_action`
    consumes the `Write` policy — deny-write-column + row-filter-on-insert) is the next slice.
```

with:

```markdown
  - *Fine-grained write governance — part 1 (control plane)* ✅ DELIVERED
    (`2026-06-16-acl-action-scoped-policies-design.md`). `acl.policy` is now action-scoped:
    `set_policy`/`clear_policy`/`policies_for` key on `(role, action, target)`, so a role holds
    independent read and write policies (parity with the already-action-scoped `role_grant`). The
    read path is explicitly `Read`-scoped (unchanged).
  - *Fine-grained write governance — part 2 (service enforcement)* ✅ DELIVERED
    (`2026-06-16-write-enforcement-design.md`). `run_action` loads the subject's `Write` policy and
    rejects an insert that sets a denied column or produces a row failing the policy's `row_filter`
    (a new pure in-memory three-valued evaluator with full read-parity coercion). Fail-closed; a
    generic 403 with a logged reason. `mask_columns` is ignored on writes (read-render only). The
    write front door now reaches parity with the read-side ACL. Proven by a fixture e2e.
```

- [ ] **Step 2: Update FUTURE.md**

In `docs/FUTURE.md`, replace the block (currently lines 218–224):

```markdown
- **Fine-grained write governance.** Part 1 (control plane) ✅ DELIVERED
  (`2026-06-16-acl-action-scoped-policies-design.md`): `acl.policy` is action-scoped, so read and
  write policies are independent. Part 2 (service enforcement) remains: `run_action` loads the
  `Write` policy and enforces deny-write-column (reject if a param sets a denied column) +
  row-filter-on-insert (a pure in-memory `RowFilter` evaluator — the inserted row must satisfy the
  predicate). `mask_columns` on a `Write` policy is expected to be ignored (masking is read-only) —
  to be confirmed in part 2.
```

with:

```markdown
- **Fine-grained write governance.** ✅ DELIVERED (both parts). Part 1 (control plane,
  `2026-06-16-acl-action-scoped-policies-design.md`): `acl.policy` is action-scoped, so read and
  write policies are independent. Part 2 (service enforcement,
  `2026-06-16-write-enforcement-design.md`): `run_action` loads the `Write` policy and enforces
  deny-write-column + row-filter-on-insert via a pure in-memory three-valued `RowFilter` evaluator
  (`write_filter.rs`), fail-closed, with full read-parity coercion. `mask_columns` on a `Write`
  policy is **ignored** (masking is read-only) — confirmed. Open follow-up: surface a *structured*
  denial reason (which column / row-filter failure) instead of the current logs-only generic 403.
```

- [ ] **Step 3: Run the doc-lint hooks and commit**

```bash
buck2 run //tools:prek -- run --all-files > /tmp/lint.log 2>&1; grep -nE "Failed|Passed|error" /tmp/lint.log | tail -20
git add docs/superpowers/specs/2026-06-06-loom-roadmap.md docs/FUTURE.md
git commit -m "$(cat <<'EOF'
docs: fine-grained write governance part 2 delivered

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

Expected: hooks pass (or fix files in place — if they rewrite anything, re-`git add` and amend).

---

## Final verification (after all tasks)

- [ ] Run the whole query-api suite: `buck2 test //src/services/query-api/... > /tmp/all.log 2>&1; grep -nE "Tests finished|FAIL" /tmp/all.log` — expected all pass.
- [ ] Confirm no inline tests slipped in: the `no-inline-tests` prek hook is green (run in Task 5 step 3).
- [ ] Dispatch the final whole-branch code review, then use `superpowers:finishing-a-development-branch`.

---

## Self-Review

**Spec coverage:**
- `compare_cell` full-parity coercion (Int↔Double, Date/Timestamp vs ISO-Text), three-valued — Task 1. ✅
- `eval` SQL three-valued tree walk; absent property = NULL — Task 2. ✅
- `check_write_policy` + `WriteVerdict`; deny-column union; row-filter conjunction; no-policy Allow;
  mask ignored — Task 3. ✅
- Wiring into `run_action` after parse, before insert; `PolicyTarget` reuse; `Forbidden` mapping;
  logged reason — Task 4. ✅
- e2e: row-filter deny/allow, deny-column deny/allow, Read/Write independence — Task 4. ✅
- Fail-closed semantics — covered by `gate_fail_closed_on_unknown_row_filter` (Task 3) and the
  Phase-1 deny path (Task 4). ✅
- Docs (roadmap + FUTURE.md, follow-up recorded) — Task 5. ✅

**Placeholder scan:** none — every code/step block is complete.

**Type consistency:** `compare_cell(&SqlValue, CompareOp, &ScalarValue) -> Option<bool>`,
`eval(&RowFilter, &BTreeMap<&str, &SqlValue>) -> Option<bool>`,
`check_write_policy(&[Policy], &[String], &[SqlValue]) -> WriteVerdict`,
`WriteVerdict::{Allow, DenyColumn(String), DenyRow}` — used identically across tasks and tests. The
test file's `write_filter` import grows monotonically (Task 1 `compare_cell` → Task 2 adds `eval` →
Task 3 adds `check_write_policy`, `WriteVerdict`); the `_later_task_imports` shim keeps not-yet-used
symbols warm and is removed in Task 3.
