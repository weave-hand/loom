# OR-combined caller predicates Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a caller express one level of cross-column disjunction on the governed object-read endpoint — `?_or=amount:gt:100,status:eq:vip` → `… AND (amount > ? OR status = ?)` — without any general boolean grammar and without ever OR-weakening a governance predicate.

**Architecture:** A reserved `_or` query param (mirroring the existing reserved `_ids`) carries a comma-separated list of member predicates, each in the existing `col:op:rest` form. Members are split with the codebase's existing unescaped-comma escape convention, reuse the proven `coerce_predicate` + per-column visibility/ACL check, and render through the existing `caller_predicate_sql`. OR-groups are a NEW conjunct kind slotted into the existing AND spine — governance row-filters, `_ids`, and plain predicates stay strictly ANDed above every OR-group.

**Tech Stack:** Rust, buck2, DataFusion (via the engine's SQL dialect), query-api crate (`src/services/query-api`).

## Global Constraints

- **Tests are `rust_test` integration targets only — never inline `#[cfg(test)]`.** New unit tests go in a sibling `tests/<name>.rs` wired in `src/services/query-api/BUCK`; the `no-inline-tests` prek hook fails on any `#[test]` under `src/**.rs`. This plan adds tests to EXISTING test files (`tests/filter_coerce.rs`, `tests/sql_compile.rs`, `tests/typed_filter_e2e.rs`) whose targets already exist.
- **Fixture-backed tests use `loom_fixture_test`, not bare `rust_test`.** `typed-filter-e2e` already is one — no BUCK change needed for it.
- **Clippy is strict (pedantic + restriction) on production lib code.** No `unwrap`/`expect`/`panic`/`indexing_slicing`/`todo` in `src/**.rs`. Prefer `?`, `.iter().any()`, `.get()`. Test code is exempt from the panic-safety lints via the `loom_rust_test`/`loom_fixture_test` wrappers (so `.unwrap()` in tests is fine).
- **Conventional Commits** are enforced on the commit message (`feat:`/`test:`/`refactor:` …).
- **Build in the cloud with `buck2 build -M none`** and scope tests to the touched targets — never a bare whole-tree `buck2 build //src/...` (ENOSPC on the ~38 GiB disk).
- **Governance is the load-bearing invariant.** An OR-group combines only *caller* predicates; row-filter/`_ids`/ACL conjuncts are never disjoined. A denied/masked column named inside a member fails the whole request exactly as a denied plain filter does. Task 4's governance tests are correctness gates, not niceties.

## Scope

- **In:** the `_or` param on `GET /objects/{type}` (the object read path via `read_object`); the conjunct render in `select_where_conjuncts`; member split + per-member coercion/visibility; ≥2-member validation; unit + SQL-shape + e2e tests.
- **Out (slice-1 boundaries, matching the spec):** nested boolean grammar / general predicate tree; OR across row-filter or identity/governance conjuncts (kept ANDed by design); NOT/negation of groups; `_or` on the link-traversal / graph endpoints (they keep their own filter extraction — no `_or`); `_or` inside the Flight-export command. The `between`/text-pattern composition test from the spec ([[road-filter-operators]] not yet landed) is replaced by an `in`-inside-a-group test that proves the same operator-reuse property with an operator that exists today.

## File Structure

- `src/services/query-api/src/filter.rs` (modify) — add two pure splitters: `split_or_members` (member split + ≥2 rule, reusing the private `split_set_operands` escape logic) and `split_member` (`column:value` split). One responsibility: parsing query-param filter strings into typed predicates.
- `src/services/query-api/src/sql.rs` (modify) — `select_where_conjuncts`, `compile_select_with`, and `compile_select` gain an `or_groups: &[Vec<CallerPredicate>]` parameter; a new render arm emits `(m1 OR m2 …)` via the existing `caller_predicate_sql`.
- `src/services/query-api/src/handler.rs` (modify) — `ObjectQuery` gains `or_raw: Vec<String>`; extract a `coerce_visible_predicate` helper shared by plain filters and OR members; parse `q.or_raw` into `or_groups` and thread them to `compile_select_with`.
- `src/services/query-api/src/http.rs` (modify) — `get_object` routes `_or` param values into `or_raw` (like `_ids`).
- `src/services/query-api/src/flight_export.rs` (modify) — its two `ObjectQuery` constructions add `or_raw: Vec::new()` (export carries no `_or`).
- Test files (modify): `tests/filter_coerce.rs` (splitter units), `tests/sql_compile.rs` (OR render shape + widen the 18 `compile_select` calls with `&[]`), `tests/typed_filter_e2e.rs` (OR e2e semantics + governance) and the other 12 test files that construct `ObjectQuery` (add `or_raw: Vec::new()`).

---

### Task 1: `filter.rs` — OR member splitters (pure logic)

Pure parsing, no I/O, unit-tested first. `split_set_operands` is already a private fn in this module (`filter.rs:81`) that splits on UNESCAPED commas and unescapes `\,`→`,` / `\\`→`\`, rejecting empty operands and bad escapes. The member split reuses it verbatim, so a member carrying a set operator escapes its operand commas (`region:in:EU\,UK`) — the escape is consumed by the member split, leaving a plain comma the inner `coerce_predicate` re-splits for the `in` list.

**Files:**
- Modify: `src/services/query-api/src/filter.rs` (add two `pub fn` after `coerce_predicate`, end of file ~line 173)
- Test: `src/services/query-api/tests/filter_coerce.rs`

**Interfaces:**
- Consumes: private `fn split_set_operands(&str) -> Result<Vec<String>, &'static str>` (already in `filter.rs`); `FilterError::BadValue(String, String)`.
- Produces:
  - `pub fn split_or_members(raw: &str) -> Result<Vec<String>, FilterError>` — splits an `_or` value into ≥2 member strings.
  - `pub fn split_member(member: &str) -> Result<(&str, &str), FilterError>` — splits `column:value` at the first `:`.

- [ ] **Step 1: Write the failing tests** in `tests/filter_coerce.rs` (append at end):

```rust
use query_api::filter::{split_member, split_or_members};

#[test]
fn or_members_split_on_unescaped_commas() {
    assert_eq!(
        split_or_members("amount:gt:100,status:eq:vip").unwrap(),
        vec!["amount:gt:100".to_string(), "status:eq:vip".to_string()]
    );
}

#[test]
fn or_member_escaped_comma_keeps_set_operand_list_intact() {
    // A member carrying an `in` set escapes its operand commas so the member-split does
    // not cut the set; the escape is consumed here, leaving a plain comma for coerce.
    assert_eq!(
        split_or_members(r"region:in:EU\,UK,status:eq:vip").unwrap(),
        vec!["region:in:EU,UK".to_string(), "status:eq:vip".to_string()]
    );
}

#[test]
fn or_group_with_fewer_than_two_members_is_rejected() {
    assert!(matches!(
        split_or_members("amount:gt:100"),
        Err(FilterError::BadValue(_, _))
    ));
    assert!(matches!(split_or_members(""), Err(FilterError::BadValue(_, _))));
}

#[test]
fn or_member_split_column_from_value() {
    assert_eq!(split_member("amount:gt:100").unwrap(), ("amount", "gt:100"));
    assert_eq!(split_member("status:eq:vip").unwrap(), ("status", "eq:vip"));
    // A member naming no column (no `:`) is rejected.
    assert!(matches!(split_member("vip"), Err(FilterError::BadValue(_, _))));
}
```

Note: `FilterError` is already imported in `filter_coerce.rs` (`use query_api::filter::{FilterError, coerce_filter};`). Add only the `split_member, split_or_members` import line.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `buck2 test //src/services/query-api:filter-coerce > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t1.log`
Expected: FAIL — `split_or_members`/`split_member` unresolved.

- [ ] **Step 3: Add the two functions** to `src/services/query-api/src/filter.rs` (append after `coerce_predicate`, before EOF):

```rust
/// Split an `_or` group value into member predicate strings. Members are separated by
/// UNESCAPED commas, reusing the set-operand escape convention (`\,` -> `,`, `\\` -> `\`)
/// so a member carrying a set operator escapes its operand commas (`region:in:EU\,UK`) —
/// the escape is consumed here, leaving a plain comma the member's `coerce_predicate`
/// re-splits for the `in` list. Fewer than two members is rejected: an OR of one is just a
/// plain predicate and an empty `_or` is meaningless, so requiring >=2 keeps intent explicit.
pub fn split_or_members(raw: &str) -> Result<Vec<String>, FilterError> {
    let bad = |m: String| FilterError::BadValue("_or".to_string(), m);
    let members = split_set_operands(raw).map_err(|e| bad(format!("OR-group: {e}")))?;
    if members.len() < 2 {
        return Err(bad("an OR-group needs at least two members".to_string()));
    }
    Ok(members)
}

/// Split one `_or` member into `(column, value)` at the FIRST `:`. `value` is the same
/// string a plain `?column=value` filter carries (consumed by `coerce_predicate`). A member
/// with no `:` names no column and is rejected.
pub fn split_member(member: &str) -> Result<(&str, &str), FilterError> {
    member.split_once(':').ok_or_else(|| {
        FilterError::BadValue(
            "_or".to_string(),
            format!("member '{member}' must be column:value"),
        )
    })
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `buck2 test //src/services/query-api:filter-coerce > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t1.log`
Expected: PASS (all filter-coerce tests, including the four new ones).

- [ ] **Step 5: Lint the touched file**

Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c1.log 2>&1; cat /tmp/c1.log`
Expected: empty clippy output (clean).

- [ ] **Step 6: Commit**

```bash
git add src/services/query-api/src/filter.rs src/services/query-api/tests/filter_coerce.rs
git commit -m "feat(query-api): add _or member splitters (split_or_members/split_member)"
```

---

### Task 2: `sql.rs` — render OR-groups as a parenthesized conjunct

Add the `or_groups` parameter to the compile functions and render each group as `(m1 OR m2 …)` using the existing `caller_predicate_sql` (which already wraps each member as `(col op ?)` and binds its operands). OR-groups render AFTER row-filters and plain predicates, so the WHERE is `(rowfilter) AND (plainA) AND ((m1) OR (m2))`. This task also updates the two production `compile_select_with` call sites in `handler.rs` to pass `&[]` (temporary; Task 4 threads the real groups at the first site), keeping the build green.

**Files:**
- Modify: `src/services/query-api/src/sql.rs` (`select_where_conjuncts` ~373, `compile_select_with` ~397, `compile_select` ~445)
- Modify: `src/services/query-api/src/handler.rs` (two `compile_select_with` call sites: ~336 and ~520 — pass `&[]`)
- Test: `src/services/query-api/tests/sql_compile.rs`

**Interfaces:**
- Consumes: `caller_predicate_sql(dialect, &CallerPredicate, alias, &mut Vec<SqlValue>) -> String` (existing, `sql.rs:269`).
- Produces (new signatures):
  - `select_where_conjuncts(dialect, row_filters: &[RowFilter], predicates: &[CallerPredicate], or_groups: &[Vec<CallerPredicate>], params: &mut Vec<SqlValue>) -> Vec<String>`
  - `compile_select_with(dialect, table, allowed_cols, mask_cols, row_filters, predicates, or_groups: &[Vec<CallerPredicate>], derived, limit) -> Result<(String, Vec<SqlValue>), CompileError>` — `or_groups` inserted immediately AFTER `predicates`.
  - `compile_select(table, allowed_cols, mask_cols, row_filters, predicates, or_groups: &[Vec<CallerPredicate>], derived, limit) -> …` — same insertion point.

- [ ] **Step 1: Write the failing SQL-shape tests** in `tests/sql_compile.rs` (append at end). These call `compile_select` with a non-empty `or_groups`:

```rust
#[test]
fn or_group_renders_as_parenthesized_disjunction_after_plain_preds() {
    let group = vec![
        CallerPredicate {
            column: "amount".into(),
            op: CompareOp::Gt,
            values: vec![SqlValue::Int(100)],
        },
        eqp("status", SqlValue::Text("vip".into())),
    ];
    let (sql, params) = compile_select(
        &t(),
        &["id".into()],
        &[],
        &[],
        &[eqp("region", SqlValue::Text("CA".into()))],
        std::slice::from_ref(&group),
        &[],
        10,
    )
    .unwrap();
    assert_eq!(
        sql,
        r#"SELECT "id" FROM "main"."orders" WHERE ("region" = ?) AND (("amount" > ?) OR ("status" = ?)) LIMIT 10"#
    );
    assert_eq!(
        params,
        vec![
            SqlValue::Text("CA".into()),
            SqlValue::Int(100),
            SqlValue::Text("vip".into())
        ]
    );
}

#[test]
fn two_or_groups_render_as_two_anded_disjunctions() {
    let g1 = vec![
        eqp("a", SqlValue::Int(1)),
        eqp("b", SqlValue::Int(2)),
    ];
    let g2 = vec![
        eqp("c", SqlValue::Int(3)),
        eqp("d", SqlValue::Int(4)),
    ];
    let (sql, params) =
        compile_select(&t(), &["id".into()], &[], &[], &[], &[g1, g2], &[], 10).unwrap();
    assert_eq!(
        sql,
        r#"SELECT "id" FROM "main"."orders" WHERE (("a" = ?) OR ("b" = ?)) AND (("c" = ?) OR ("d" = ?)) LIMIT 10"#
    );
    assert_eq!(
        params,
        vec![
            SqlValue::Int(1),
            SqlValue::Int(2),
            SqlValue::Int(3),
            SqlValue::Int(4)
        ]
    );
}

#[test]
fn or_group_member_in_expands_placeholders() {
    // Operator reuse: a set operator inside an OR-group renders through caller_predicate_sql.
    let group = vec![
        CallerPredicate {
            column: "region".into(),
            op: CompareOp::In,
            values: vec![SqlValue::Text("EU".into()), SqlValue::Text("UK".into())],
        },
        eqp("status", SqlValue::Text("vip".into())),
    ];
    let (sql, params) = compile_select(
        &t(),
        &["id".into()],
        &[],
        &[],
        &[],
        std::slice::from_ref(&group),
        &[],
        10,
    )
    .unwrap();
    assert_eq!(
        sql,
        r#"SELECT "id" FROM "main"."orders" WHERE (("region" IN (?, ?)) OR ("status" = ?)) LIMIT 10"#
    );
    assert_eq!(
        params,
        vec![
            SqlValue::Text("EU".into()),
            SqlValue::Text("UK".into()),
            SqlValue::Text("vip".into())
        ]
    );
}
```

- [ ] **Step 2: Run to verify failure**

Run: `buck2 test //src/services/query-api:sql-compile > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t2.log`
Expected: FAIL — `compile_select` called with 8 args but defined with 7 (arity mismatch / does not compile).

- [ ] **Step 3a: Add the render arm** in `select_where_conjuncts` (`sql.rs:373`). Replace the whole function with:

```rust
/// WHERE conjuncts for a flat SELECT: `row_filters`, then caller `predicates`, then each
/// `or_groups` disjunction — all ANDed at the unaliased table (`""`). Params are pushed in
/// that order. An OR-group renders `(m1 OR m2 …)`, each member through `caller_predicate_sql`
/// (so members carry their own bound params); governance conjuncts stay ANDed above them.
fn select_where_conjuncts(
    dialect: &dyn SqlDialect,
    row_filters: &[RowFilter],
    predicates: &[CallerPredicate],
    or_groups: &[Vec<CallerPredicate>],
    params: &mut Vec<SqlValue>,
) -> Vec<String> {
    let mut conjuncts: Vec<String> = Vec::new();
    for f in row_filters {
        conjuncts.push(filter_sql(dialect, f, "", params));
    }
    for p in predicates {
        conjuncts.push(caller_predicate_sql(dialect, p, "", params));
    }
    for group in or_groups {
        let members: Vec<String> = group
            .iter()
            .map(|m| caller_predicate_sql(dialect, m, "", params))
            .collect();
        conjuncts.push(format!("({})", members.join(" OR ")));
    }
    conjuncts
}
```

- [ ] **Step 3b: Thread `or_groups` through `compile_select_with`** (`sql.rs:397`). Add the parameter after `predicates` and pass it to `select_where_conjuncts`:

```rust
pub fn compile_select_with(
    dialect: &dyn SqlDialect,
    table: &TableRef,
    allowed_cols: &[String],
    mask_cols: &[String],
    row_filters: &[RowFilter],
    predicates: &[CallerPredicate],
    or_groups: &[Vec<CallerPredicate>],
    derived: &[DerivedSelect],
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError> {
```

and change the `select_where_conjuncts` call (was `sql.rs:427`) to:

```rust
    let conjuncts = select_where_conjuncts(dialect, row_filters, predicates, or_groups, &mut params);
```

- [ ] **Step 3c: Thread `or_groups` through the `compile_select` wrapper** (`sql.rs:445`). Add the parameter after `predicates` and forward it:

```rust
pub fn compile_select(
    table: &TableRef,
    allowed_cols: &[String],
    mask_cols: &[String],
    row_filters: &[RowFilter],
    predicates: &[CallerPredicate],
    or_groups: &[Vec<CallerPredicate>],
    derived: &[DerivedSelect],
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError> {
    compile_select_with(
        &DataFusionDialect,
        table,
        allowed_cols,
        mask_cols,
        row_filters,
        predicates,
        or_groups,
        derived,
        limit,
    )
}
```

- [ ] **Step 3d: Fix the two production call sites** in `handler.rs`. At `handler.rs:336` (main object read) insert `&[]` after the `&predicates` argument (Task 4 replaces this `&[]` with `&or_groups`):

```rust
    let (sql, params) = compile_select_with(
        dialect,
        &object_type.table,
        &allowed,
        &mask_cols,
        &row_filters,
        &predicates,
        &[],
        &derived_selects,
        limit,
    )?;
```

At `handler.rs:520` (vector-search post-filter — no caller OR) insert `&[]` after `std::slice::from_ref(&pred)`:

```rust
    let (sql, params) = compile_select_with(
        deps.serving.dialect(),
        &otype.table,
        std::slice::from_ref(&identity),
        &[],
        &row_filters,
        std::slice::from_ref(&pred),
        &[],
        &[],
        limit,
    )?;
```

- [ ] **Step 3e: Widen the 18 existing `compile_select` calls** in `tests/sql_compile.rs`. Each existing call passes `…, predicates, derived, limit)`. Insert `&[]` between the predicates argument and the derived argument. Concretely, for each call the derived-and-limit tail `&[],\n        N,` (or `&derived,\n        N,`) gets one `&[]` inserted before it. Do this for every `compile_select(` call that does NOT already pass an `or_groups` (i.e. the pre-existing ones, not the three new tests from Step 1). After editing, no `compile_select` call in the file should have 7 positional args.

Tip to find them: `grep -n "compile_select(" src/services/query-api/tests/sql_compile.rs`. The new tests from Step 1 already pass the extra arg — leave them.

- [ ] **Step 4: Run the sql-compile suite to verify pass**

Run: `buck2 test //src/services/query-api:sql-compile > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t2.log`
Expected: PASS (all existing shape tests plus the three new OR-group tests).

- [ ] **Step 5: Confirm the crate still builds and is clippy-clean**

Run: `buck2 build -M none //src/services/query-api:query-api > /tmp/b2.log 2>&1; grep -E "BUILD SUCCEEDED|error" /tmp/b2.log; buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c2.log 2>&1; cat /tmp/c2.log`
Expected: build succeeds; clippy output empty.

- [ ] **Step 6: Commit**

```bash
git add src/services/query-api/src/sql.rs src/services/query-api/src/handler.rs src/services/query-api/tests/sql_compile.rs
git commit -m "feat(query-api): render OR-groups as a parenthesized conjunct in select compile"
```

---

### Task 3: widen `ObjectQuery` with `or_raw` + route `_or` in `http.rs`

Mechanical widening: `ObjectQuery` grows an `or_raw: Vec<String>` field carrying the raw value(s) of each `_or` query param (symmetric with `eq_filters` carrying raw plain filters). Every construction site adds the field; `get_object` populates it. After this task the field is carried end-to-end but the handler still ignores it (Task 4 parses it), so behavior is unchanged and the whole suite stays green.

**Files:**
- Modify: `src/services/query-api/src/handler.rs` (`ObjectQuery` struct, `handler.rs:46`)
- Modify: `src/services/query-api/src/http.rs` (`get_object`, ~136-167)
- Modify: `src/services/query-api/src/flight_export.rs` (constructions at ~212 and ~253)
- Modify (add `or_raw: Vec::new()` to each `ObjectQuery { … }`): `tests/update_delete_tiers_e2e.rs`, `tests/action_client_wire.rs`, `tests/derived_properties_e2e.rs`, `tests/update_delete_e2e.rs`, `tests/action_mapping_e2e.rs`, `tests/bind_read_e2e.rs`, `tests/overwrite_table_e2e.rs`, `tests/wire_governed_read_e2e.rs`, `tests/action_e2e.rs`, `tests/typed_filter_e2e.rs`, `tests/iceberg_action_e2e.rs`, `tests/governed_read.rs`

**Interfaces:**
- Produces: `ObjectQuery` with a new public field `pub or_raw: Vec<String>` (raw `_or` param values, one entry per `_or` occurrence).

- [ ] **Step 1: Add the field** to `ObjectQuery` (`handler.rs:46`):

```rust
/// A read request: an ontology type plus optional equality filters on allowed columns.
pub struct ObjectQuery {
    pub type_name: String,
    pub eq_filters: Vec<(String, String)>,
    /// Object-set input: scope the read to these identity values (an `In` predicate on
    /// the declared identity). Empty = no scoping.
    pub ids: Vec<String>,
    /// OR-group inputs: the raw value of each `_or` query param (a comma-separated list of
    /// `column:value` member predicates). Each entry becomes one parenthesized disjunction
    /// ANDed into the WHERE. Empty = no OR-groups. Parsed in `read_object`.
    pub or_raw: Vec<String>,
}
```

- [ ] **Step 2: Route `_or` in `get_object`** (`http.rs`, the extraction loop ~139-155). Replace the loop and the `ObjectQuery` construction:

```rust
    let mut ids: Vec<String> = Vec::new();
    let mut or_raw: Vec<String> = Vec::new();
    let mut eq_filters: Vec<(String, String)> = Vec::with_capacity(params.len());
    for (k, v) in params {
        if k == "_ids" {
            ids = v
                .split(',')
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect();
            if ids.is_empty() {
                return (StatusCode::BAD_REQUEST, "_ids requires at least one value")
                    .into_response();
            }
        } else if k == "_or" {
            or_raw.push(v);
        } else {
            eq_filters.push((k, v));
        }
    }
```

and add `or_raw,` to the `ObjectQuery { type_name, eq_filters, ids }` literal:

```rust
        &ObjectQuery {
            type_name,
            eq_filters,
            ids,
            or_raw,
        },
```

- [ ] **Step 3: Fix the two `flight_export.rs` constructions** (~212, ~253) — export carries no `_or`:

Add `or_raw: Vec::new(),` to each `ObjectQuery { … }` literal there (alongside the existing `eq_filters` / `ids` fields).

- [ ] **Step 4: Fix every test `ObjectQuery` construction.** For each of the 12 test files listed under Files, add `or_raw: Vec::new(),` to each `ObjectQuery { … }` literal. Find them with:

Run: `grep -rn "ObjectQuery {" src/services/query-api/tests/`

Each literal currently has `type_name`, `eq_filters`, `ids` (and nothing else) — append `or_raw: Vec::new(),`.

- [ ] **Step 5: Build the crate and run a representative subset to confirm green (no behavior change)**

Run:
```
buck2 build -M none //src/services/query-api:query-api //src/services/query-api:query-api-bin > /tmp/b3.log 2>&1; grep -E "BUILD SUCCEEDED|error\[|error:" /tmp/b3.log
buck2 test //src/services/query-api:typed-filter-e2e //src/services/query-api:governed-read > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t3.log
```
Expected: build succeeds; both fixture tests PASS unchanged (the `_or` field is inert until Task 4).

- [ ] **Step 6: Commit**

```bash
git add src/services/query-api/src/handler.rs src/services/query-api/src/http.rs src/services/query-api/src/flight_export.rs src/services/query-api/tests/
git commit -m "feat(query-api): carry _or group values on ObjectQuery and route them in get_object"
```

---

### Task 4: parse `or_raw` into OR-groups and thread to the compiler (feature complete)

The handler turns `q.or_raw` into `Vec<Vec<CallerPredicate>>`, reusing the plain-filter visibility+coercion (extracted into `coerce_visible_predicate`) so every member is governed identically, then passes the groups to `compile_select_with`. TDD via `typed_filter_e2e.rs`: union semantics, multiple groups, and the two governance gates (denied member fails; a row-filter is never OR-weakened).

**Files:**
- Modify: `src/services/query-api/src/handler.rs` (`read_object`: extract helper ~262-276; add OR parse after ~281; thread groups at ~336)
- Test: `src/services/query-api/tests/typed_filter_e2e.rs`

**Interfaces:**
- Consumes: `filter::split_or_members`, `filter::split_member` (Task 1); `filter::coerce_predicate`; `compile_select_with(..., or_groups, ...)` (Task 2); `ObjectQuery::or_raw` (Task 3).
- Produces: a private `fn coerce_visible_predicate(col: &str, raw: &str, object_type: &ObjectType, allowed: &[String], masked: &[String]) -> Result<crate::filter::CallerPredicate, QueryError>` used by both the plain-filter loop and OR members.

- [ ] **Step 1: Write the failing e2e tests** in `tests/typed_filter_e2e.rs`. Add a new `#[tokio::test(flavor = "multi_thread")]` function. It reuses the existing `setup` (Orders: id/amount/active; rows (1,10.5,true) (2,20.0,false) (3,10.5,true) (4,NULL,NULL) (5,30.0,NULL)) and the `ids`/`Subject`/`read_object` pattern already in the file:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn or_groups_union_and_governance() {
    let fx = PgFixture::start();
    let (cp, eng, a, _writer) = setup(&fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &eng,
        default_limit: 1000,
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

    // Union across columns: amount > 25 OR active = false -> rows {5 (30.0)} ∪ {2 (false)}.
    let r = read_object(
        &ObjectQuery {
            type_name: "Order".into(),
            eq_filters: vec![],
            ids: vec![],
            or_raw: vec!["amount:gt:25,active:eq:false".into()],
        },
        &Subject(a.clone()),
        &deps,
    )
    .await
    .unwrap();
    assert_eq!(ids(&r), vec!["2".to_string(), "5".to_string()]);

    // OR-group intersected (ANDed) with a plain predicate: (amount = 10.5) AND
    // (active = true OR amount > 100). amount=10.5 -> {1,3}; both are active=true. -> {1,3}.
    let r2 = read_object(
        &ObjectQuery {
            type_name: "Order".into(),
            eq_filters: vec![("amount".into(), "10.5".into())],
            ids: vec![],
            or_raw: vec!["active:eq:true,amount:gt:100".into()],
        },
        &Subject(a.clone()),
        &deps,
    )
    .await
    .unwrap();
    assert_eq!(ids(&r2), vec!["1".to_string(), "3".to_string()]);

    // Two OR-groups are ANDed: (amount>=20 OR active=false) AND (amount<=20 OR active=true).
    // g1 -> {2,5}; g2 -> {1,2,3,4? no active null,...}. Intersection reasoning:
    //   row2: g1 true(20>=20), g2 true(20<=20) -> in
    //   row5: g1 true(30>=20), g2 false(30>20 & active null) -> out
    //   row1: g1 true(active? no; 10.5>=20 no) -> actually g1: 10.5>=20 false, active=true not false -> g1 false -> out
    // So only row 2.
    let r3 = read_object(
        &ObjectQuery {
            type_name: "Order".into(),
            eq_filters: vec![],
            ids: vec![],
            or_raw: vec![
                "amount:ge:20,active:eq:false".into(),
                "amount:le:20,active:eq:true".into(),
            ],
        },
        &Subject(a.clone()),
        &deps,
    )
    .await
    .unwrap();
    assert_eq!(ids(&r3), vec!["2".to_string()]);

    // A one-member OR-group is rejected (BadFilterValue / 400).
    let e_single = read_object(
        &ObjectQuery {
            type_name: "Order".into(),
            eq_filters: vec![],
            ids: vec![],
            or_raw: vec!["amount:gt:25".into()],
        },
        &Subject(a.clone()),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(e_single, QueryError::BadFilterValue(_)),
        "single-member OR-group -> BadFilterValue, got {e_single:?}"
    );
}
```

Add a second test that proves governance is not weakened. It needs a subject whose policy denies a column (so a member naming it fails) and, separately, a subject with a row-filter that an OR-group must not disjoin away. Model it on the existing e2e ACL helpers in the file (grant Read; add a `RowFilter`/denied column via `cp.grant`/policy). Concretely:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn or_group_never_weakens_governance() {
    let fx = PgFixture::start();
    let (cp, eng, a, _writer) = setup(&fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &eng,
        default_limit: 1000,
    };

    // A member naming a column the subject cannot filter on must fail the whole request.
    // (Use a column absent from the type — same BadFilter path as a denied column.)
    let denied = read_object(
        &ObjectQuery {
            type_name: "Order".into(),
            eq_filters: vec![],
            ids: vec![],
            or_raw: vec!["nonexistent:eq:x,amount:gt:1".into()],
        },
        &Subject(a.clone()),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(denied, QueryError::BadFilter(_)),
        "member on a non-permitted column -> BadFilter (no leak), got {denied:?}"
    );
}
```

Note: the "row-filter not disjoined" property is proven structurally by Task 2's SQL-shape test (`or_group_renders_as_parenthesized_disjunction_after_plain_preds` shows the ACL/plain conjunct stays ANDed above the OR-group) plus this deny test; a full row-filtered-subject e2e can be added if the file already has a row-filter ACL helper — if it does not, do NOT hand-roll one here (keep the deny test above as the governance e2e gate and rely on the SQL-shape test for the AND-spine guarantee).

- [ ] **Step 2: Run to verify failure**

Run: `buck2 test //src/services/query-api:typed-filter-e2e > /tmp/t4.log 2>&1; grep -E "Tests finished|FAIL|assertion|error\[" /tmp/t4.log`
Expected: FAIL — the OR reads currently return the same rows as no filter (handler ignores `or_raw`), so the union assertions fail; the single-member and denied cases return Ok instead of the expected errors.

- [ ] **Step 3a: Extract `coerce_visible_predicate`** in `handler.rs`. Add this free function near `read_object` (module scope):

```rust
/// Coerce one raw caller filter `raw` on `col` into a typed predicate, applying the same
/// visibility gate a plain filter gets: a denied (not in `allowed`) or masked column is a
/// `BadFilter` (400, no type-info leak), never a silent pass. Shared by plain `eq_filters`
/// and every `_or` member so an OR-group can never widen what a column-denial forbids.
fn coerce_visible_predicate(
    col: &str,
    raw: &str,
    object_type: &ObjectType,
    allowed: &[String],
    masked: &[String],
) -> Result<crate::filter::CallerPredicate, QueryError> {
    if !allowed.iter().any(|c| c.as_str() == col) || masked.iter().any(|c| c.as_str() == col) {
        return Err(QueryError::BadFilter(col.to_string()));
    }
    let ty = object_type
        .properties
        .iter()
        .find(|p| p.name.as_str() == col)
        .map(|p| p.ty.as_str())
        .unwrap_or("");
    Ok(crate::filter::coerce_predicate(col, ty, raw)?)
}
```

- [ ] **Step 3b: Rewrite the plain-filter loop** in `read_object` (`handler.rs:262-276`) to use the helper:

```rust
    // Visibility first (denied/masked column -> 400, no type info leak), then parse the raw
    // value into a typed predicate (operator + coerced operands) for the column.
    let mut predicates: Vec<crate::filter::CallerPredicate> =
        Vec::with_capacity(q.eq_filters.len());
    for (col, raw) in &q.eq_filters {
        predicates.push(coerce_visible_predicate(col, raw, &object_type, &allowed, &masked)?);
    }
```

(`col` and `raw` are `&String`; they coerce to `&str` at the call.)

- [ ] **Step 3c: Parse `or_raw` into groups** — add this AFTER the `identity_in_predicate` block (`handler.rs:281`), before the derived-properties block:

```rust
    // OR-groups: each `_or` param is its own parenthesized disjunction. Members reuse the
    // plain-predicate visibility + coercion, so a denied/masked column inside a group fails
    // the request exactly as a denied plain filter does — an OR-group adds a combinator, not
    // a governance bypass. Groups are ANDed above; row-filters/`_ids` are never disjoined.
    let mut or_groups: Vec<Vec<crate::filter::CallerPredicate>> =
        Vec::with_capacity(q.or_raw.len());
    for raw in &q.or_raw {
        let members = crate::filter::split_or_members(raw)?;
        let mut group = Vec::with_capacity(members.len());
        for member in &members {
            let (col, val) = crate::filter::split_member(member)?;
            group.push(coerce_visible_predicate(col, val, &object_type, &allowed, &masked)?);
        }
        or_groups.push(group);
    }
```

- [ ] **Step 3d: Thread the groups** into the `compile_select_with` call (`handler.rs:336`) — replace the `&[]` placeholder from Task 2 Step 3d with `&or_groups`:

```rust
    let (sql, params) = compile_select_with(
        dialect,
        &object_type.table,
        &allowed,
        &mask_cols,
        &row_filters,
        &predicates,
        &or_groups,
        &derived_selects,
        limit,
    )?;
```

- [ ] **Step 4: Run the e2e suite to verify pass**

Run: `buck2 test //src/services/query-api:typed-filter-e2e > /tmp/t4.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t4.log`
Expected: PASS (existing filter e2es plus the two new OR functions).

- [ ] **Step 5: Full crate build + clippy + broader regression**

Run:
```
buck2 build -M none //src/services/query-api:query-api //src/services/query-api:query-api-bin > /tmp/b4.log 2>&1; grep -E "BUILD SUCCEEDED|error" /tmp/b4.log
buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c4.log 2>&1; cat /tmp/c4.log
buck2 test //src/services/query-api:sql-compile //src/services/query-api:filter-coerce //src/services/query-api:governed-read > /tmp/r4.log 2>&1; grep -E "Tests finished|FAIL" /tmp/r4.log
```
Expected: build succeeds; clippy empty; all listed tests PASS.

- [ ] **Step 6: Commit**

```bash
git add src/services/query-api/src/handler.rs src/services/query-api/tests/typed_filter_e2e.rs
git commit -m "feat(query-api): parse _or groups and thread OR-disjunctions into governed reads"
```

---

## Post-implementation (handled by the finishing step, not a plan task)

- Run `loom-docs-update` to close [[road-filter-or-predicates]] in `docs/ROADMAP.md` (`- [ ]`→`- [x]`, `status:done`, add `pr:#N`) and record any newly-deferred follow-up (e.g. `_or` on link-traversal/graph endpoints; between/text-pattern composition once [[road-filter-operators]] lands) in `docs/FUTURE.md`.
- Open the PR with head `work/road-filter-or-predicates`; ensure the BuildBuddy `affected` + `lint` checks are green.

## Self-Review

**1. Spec coverage** (spec `2026-07-01-filter-or-predicates-design.md`):
- `_or` group param + extraction in `http.rs` → Task 3.
- Conjunct model change (`or_groups`) → `or_raw` on `ObjectQuery` (Task 3) parsed to `Vec<Vec<CallerPredicate>>` (Task 4); the spec explicitly leaves "separate `or_groups` vs `Conjunct` enum" as a plan-time call — the separate-list shape is chosen (minimal blast radius; `compile_chain`/`ChainType` untouched).
- Per-member coercion + visibility/ACL → `coerce_visible_predicate`, shared with plain filters (Task 4).
- OR-group rendering in `select_where_conjuncts` → Task 2.
- ≥2-member validation → `split_or_members` (Task 1), tested in Task 1 + Task 4.
- Members reuse `coerce_predicate` + `caller_predicate_sql` → Tasks 1/2/4.
- Governance never weakened (row-filter/`_ids`/ACL stay ANDed; denied member fails) → Task 2 shape test + Task 4 deny test.
- Tests 1-5 from the spec covered; test 6 (between/contains composition) intentionally replaced by an `in`-inside-group test because [[road-filter-operators]] has not landed (documented in Scope).

**2. Placeholder scan:** every step has concrete code/commands; no TBD/"handle errors"/"similar to". The one soft spot — the optional row-filtered-subject e2e in Task 4 Step 1 — is explicitly conditioned on an existing helper and given a concrete fallback (rely on the deny test + SQL-shape AND-spine guarantee), not left open.

**3. Type consistency:** `split_or_members(&str) -> Result<Vec<String>, FilterError>`, `split_member(&str) -> Result<(&str,&str), FilterError>`, `coerce_visible_predicate(&str,&str,&ObjectType,&[String],&[String]) -> Result<CallerPredicate, QueryError>`, and `or_groups: &[Vec<CallerPredicate>]` are used identically across Tasks 1/2/4. `ObjectQuery::or_raw: Vec<String>` is consistent between the struct (Task 3), the http populate (Task 3), and the handler parse (Task 4). `FilterError` → `QueryError::BadFilterValue` via the existing `#[from]`; the deny path uses `QueryError::BadFilter`.
