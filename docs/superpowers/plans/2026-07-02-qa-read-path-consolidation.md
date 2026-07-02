# query-api read-path consolidation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close `road-qa-read-path-consolidation` — finish sql.rs's decomposition (re-inlined helpers substituted back, `ReachSpec`/`SelectInputs` params structs deleting all 7 `#[allow(too_many_arguments)]`), make `caller_predicate_sql` total via slice patterns (deleting 3 `#[expect(indexing_slicing)]`), collapse the three parallel graph entry points into one `GraphReadKind`-polymorphic `read_graph`, and consolidate http.rs (`ReservedParams` splitter, `respond_shaped`, one total `QueryError → Response` mapping, `AppState::deps()`) plus flight_export.rs's duplicated governed-read block.

**Architecture:** Behavior-preserving refactor of `src/services/query-api` only. Every emitted SQL string, HTTP status, and response body must be byte-identical for reachable inputs — the existing compile tests and e2e suites are the oracle. Spec: `docs/superpowers/specs/2026-07-02-pillar-idioms-audit-design.md` § road-qa-read-path-consolidation (lines 282–310). The Wave-1 governed spine (`governed.rs`: `resolve_governed`/`GovernedType`/`Projection`/`seed_predicates`/`resolve_hop`) already exists — this item builds on it, it does not re-create it.

**Tech Stack:** Rust, buck2 (`//src/services/query-api:query-api` is the lib target), axum, `loom_rust_test` targets in `src/services/query-api/BUCK`.

## Global Constraints

- **Branch:** `work/road-qa-read-path-consolidation` (already checked out; based on current main `96d2ddb`).
- **Behavior-preserving:** pure substitutions must produce **byte-identical SQL** (existing compile tests assert full SQL strings — they must pass UNCHANGED in Tasks 1–2). HTTP statuses and body strings are e2e-pinned — copy error-message literals **verbatim** (e.g. `"_ids requires at least one value"`, `"depth must be a positive integer"`, `"tree must be true or false"`, `"unknown shape: {other}"`).
- **The tail path's `final_g` fold is subtle and e2e-pinned** (`graph-tail-e2e`): the projection comes from the FINAL tail-landed type, the seed predicates are gated by the QUERIED type's `allowed()`, and `tail_types[0]` carries EMPTY row-filters. Move it verbatim; do not "simplify".
- **Tests are `rust_test` targets only** — never inline `#[cfg(test)]` modules (prek hook rejects them). New tests go in existing `tests/*.rs` files wired in `src/services/query-api/BUCK`.
- **Strict clippy:** the crate builds with pedantic+restriction. `buck2 build '//src/services/query-api:query-api[clippy.txt]'` must stay EMPTY. No `unwrap`/`expect`/indexing in production code. Deleted `#[allow]`/`#[expect]` attributes must not resurface. Any new `#[expect]` needs a `reason`.
- **Cloud discipline:** build with `buck2 build -M none <target>`; run only the scoped test targets named per task; NEVER pipe `buck2 test` through `tail`/`head` — redirect to a file and grep: `buck2 test <targets> > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/t.log`. **Foreground commands only — never run buck2 in the background.**
- **Commits:** Conventional Commits (`refactor(query): …`); commit per task.
- The lib target is `//src/services/query-api:query-api`. Test targets live in the same package (e.g. `//src/services/query-api:sql-compile`).

## File Structure

- `src/services/query-api/src/sql.rs` — Tasks 1–4 (substitutions, slice patterns, `ReachSpec`, `SelectInputs`).
- `src/services/query-api/src/handler.rs` — Tasks 3–5 (compile call sites; graph entry-point collapse).
- `src/services/query-api/src/http.rs` — Tasks 5–6 (graph responders collapse; `ReservedParams`/`respond_shaped`/total error mapping/`AppState::deps()`).
- `src/services/query-api/src/flight_export.rs` — Task 6 (`governed()` helper).
- Test sweeps: `tests/compile_graph_reach.rs`, `tests/compile_graph_tree.rs`, `tests/compile_graph_tree_exec.rs`, `tests/compile_graph_reach_union.rs`, `tests/compile_graph_reach_tail.rs`, `tests/recursive_cte_over_datafusion.rs`, `tests/graph_reach.rs`, `tests/graph_tree.rs`, `tests/graph_reach_union.rs`, `tests/graph_reach_tail.rs`, `tests/graph_path_e2e.rs`, `tests/sql_compile.rs`, `tests/sql_dialect.rs`, `tests/vector_search_filter.rs`.

---

### Task 1: sql.rs pure substitutions — call the helpers that already exist

The reach helpers (`reach_seed_where`, `reach_recursive_where`, `reach_projection_where`, `masked_col_exprs`, `validate_reach_filters`) already exist in `sql.rs` but `compile_graph_reach_union` and `recursive_reach_cte` re-inline their bodies, and `compile_chain_with`/`compile_graph_reach_tail` re-inline the masked-column projection. Substitute the helper calls. **The output SQL must be byte-identical** — every existing compile test asserts full SQL strings and must pass unchanged.

**Files:**
- Modify: `src/services/query-api/src/sql.rs`

**Interfaces:**
- Consumes: existing private helpers `reach_seed_where(dialect, seed_predicates, row_filters, &mut params) -> String`, `reach_recursive_where(dialect, path, row_filters, depth, &mut params) -> String`, `reach_projection_where(dialect, id, row_filters, &mut params) -> String`, `masked_col_exprs(dialect, allowed_cols, mask_cols, alias) -> Vec<String>`, `validate_reach_filters(row_filters, path) -> Result<(), CompileError>`.
- Produces: no signature changes. Later tasks rely on the seed/recursive blocks in `compile_graph_reach_union` and `recursive_reach_cte` being helper calls (Task 2 threads `Result` through `reach_seed_where` only once).

- [ ] **Step 1: Substitute in `compile_graph_reach_union`** (currently sql.rs:1066–1157)

Replace the body between `pub fn compile_graph_reach_union(...) {` and the final `let sql = format!(` assembly so it reads:

```rust
    validate_reach_filters(row_filters, &[])?;
    let q = |id: &str| dialect.quote_ident(id);
    let tbl = format!("{}.{}", q(&table.schema), q(&table.name));
    let id = q(identity);
    let mut params: Vec<SqlValue> = Vec::new();

    let seed_where = reach_seed_where(dialect, seed_predicates, row_filters, &mut params);

    // Edge subquery: each backing contributes one non-recursive arm that emits (from_id, to_id)
    // pairs. Arms are joined with UNION ALL (duplicates acceptable here; the outer CTE dedupes).
    // The join-table alias `j{i}` is per-arm so multiple join-table links never collide.
    let edge_arms: Vec<String> = backings
        .iter()
        .enumerate()
        .map(|(i, backing)| {
            let jt_alias = format!("j{i}");
            let joins = link_join(dialect, backing, "cur", "nxt", &tbl, &jt_alias);
            format!("SELECT cur.{id} AS from_id, nxt.{id} AS to_id FROM {tbl} cur{joins}")
        })
        .collect();
    let edges_sql = edge_arms.join(" UNION ALL ");

    // Recursive step: single join of `reach r` to the edge subquery, then to `nxt` for filter.
    // Row-filters are applied at `nxt` (the landing node). This single `reach` reference avoids
    // a "Circular reference to CTE" planner error that arises from multiple arms each referencing
    // the CTE name. The empty path renders exactly the depth bound + row-filters at `nxt`.
    let rec_where = reach_recursive_where(dialect, &[], row_filters, depth, &mut params);
    let recursive = format!(
        "SELECT e.to_id AS id, r.depth + 1 AS depth FROM reach r JOIN ({edges_sql}) e ON r.id = e.from_id JOIN {tbl} nxt ON e.to_id = nxt.{id} WHERE {rec_where}"
    );

    // Projection of `p`: visible columns (masked -> marker), reachable in >= 1 hop, governed.
    let cols = masked_col_exprs(dialect, allowed_cols, mask_cols, "p.").join(", ");
    let proj_where = reach_projection_where(dialect, &id, row_filters, &mut params);

    let limit_clause = dialect.limit_clause(limit);
```

The final `let sql = format!(...)` block is unchanged. Deleted: the hand-rolled `seed_conj`/`seed_where` loop, the hand-rolled `rec_conj` block, the inline masked-cols `.map(...)` closure, and the hand-rolled `proj_conj` block. Note the `q` closure is still needed (for `tbl`/`id`), and `validate_reach_filters(row_filters, &[])` replaces the standalone `for f in row_filters { validate_row_filter(...) }` loop — with an empty path it validates exactly the same set.

- [ ] **Step 2: Substitute in `recursive_reach_cte`** (currently sql.rs:1174–1219)

Replace the body so it reads:

```rust
    validate_reach_filters(row_filters, &[])?;
    let q = |id: &str| dialect.quote_ident(id);
    let tbl = format!("{}.{}", q(&table.schema), q(&table.name));
    let id = q(identity);

    let seed_where = reach_seed_where(dialect, seed_predicates, row_filters, params);

    let joins = link_join(dialect, backing, "cur", "nxt", &tbl, "j");

    let rec_where = reach_recursive_where(dialect, &[], row_filters, depth, params);

    Ok(format!(
        "WITH RECURSIVE reach(id, depth) AS (\
           SELECT s.{id} AS id, 0 AS depth FROM {tbl} s{seed_where} \
           UNION \
           SELECT nxt.{id} AS id, r.depth + 1 AS depth FROM reach r JOIN {tbl} cur ON cur.{id} = r.id{joins} WHERE {rec_where}\
         )"
    ))
```

(`params` here is already `&mut Vec<SqlValue>` — pass it directly, no `&mut`.)

- [ ] **Step 3: Substitute the masked-column projection in `compile_chain_with` and `compile_graph_reach_tail`**

In `compile_chain_with` (sql.rs:646), replace the inline `col_exprs` closure:

```rust
    let k = hops.len();
    let final_alias = format!("t_{k}");
    let col_exprs = masked_col_exprs(dialect, allowed_cols, mask_cols, &format!("{final_alias}."));
```

In `compile_graph_reach_tail` (sql.rs:1240), replace its identical inline `col_exprs` block:

```rust
    let k = tail_hops.len();
    let final_alias = format!("t_{k}");
    let col_exprs = masked_col_exprs(dialect, allowed_cols, mask_cols, &format!("{final_alias}."));
```

(The `q` closure in `compile_graph_reach_tail` remains — it is still used for `id` and the identity-dedup `partition`/`outer_cols`.)

- [ ] **Step 4: Build + run the pinning tests (must pass unchanged)**

```bash
buck2 build -M none //src/services/query-api:query-api
buck2 test //src/services/query-api:compile-graph-reach-union //src/services/query-api:compile-graph-reach-tail //src/services/query-api:recursive-cte-over-datafusion //src/services/query-api:sql-compile //src/services/query-api:compile-chain-pairs //src/services/query-api:sql-dialect //src/services/query-api:graph-union-e2e //src/services/query-api:graph-tail-e2e //src/services/query-api:multi-hop-traversal-e2e > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t1.log
```

Expected: build OK, all tests pass. If any SQL-string assertion fails, the substitution was NOT byte-identical — fix the substitution, never the test.

- [ ] **Step 5: Clippy + commit**

```bash
buck2 build '//src/services/query-api:query-api[clippy.txt]' && wc -c buck-out/*/gen/*/src/services/query-api/*/clippy.txt 2>/dev/null || true
git add src/services/query-api/src/sql.rs
git commit -m "refactor(query): substitute existing reach/mask helpers into union+cte+tail compilers"
```

(For the clippy check the reliable form is: `buck2 build '//src/services/query-api:query-api[clippy.txt]' --out /tmp/clippy.txt 2>/dev/null; wc -c /tmp/clippy.txt` — 0 bytes = clean.)

---

### Task 2: slice patterns over panic-as-fail-closed in `caller_predicate_sql`

The three `#[expect(clippy::indexing_slicing)]` blocks index `p.values[0]`/`p.values[1]` — a violated arity invariant panics the request thread. Make the invariant total: `let [..] = p.values.as_slice() else { return Err(...) }`, changing `caller_predicate_sql` to return `Result<String, CompileError>` and threading `?` through its callers. TDD: write the failing arity tests first.

**Files:**
- Modify: `src/services/query-api/src/sql.rs`
- Test: `src/services/query-api/tests/sql_compile.rs` (existing target `//src/services/query-api:sql-compile`)

**Interfaces:**
- Consumes: `CompileError::MalformedFilter(String)` (exists), `crate::filter::CallerPredicate { column, op, values }` (pub fields).
- Produces: `fn caller_predicate_sql(...) -> Result<String, CompileError>` (private); `fn select_where_conjuncts(...) -> Result<Vec<String>, CompileError>` (private); `fn reach_seed_where(...) -> Result<String, CompileError>` (private). Public signatures unchanged.

- [ ] **Step 1: Write the failing tests** (append to `tests/sql_compile.rs`)

```rust
#[test]
fn between_predicate_with_wrong_arity_is_malformed_not_panic() {
    let p = CallerPredicate {
        column: "amount".into(),
        op: CompareOp::Between,
        values: vec![SqlValue::Int(1)], // needs exactly two
    };
    let err = compile_select(
        &t(),
        &["id".into()],
        &[],
        &[],
        std::slice::from_ref(&p),
        &[],
        &[],
        100,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("between predicate"),
        "got: {err}"
    );
}

#[test]
fn scalar_predicate_with_no_operand_is_malformed_not_panic() {
    let p = CallerPredicate {
        column: "status".into(),
        op: CompareOp::Eq,
        values: vec![],
    };
    let err = compile_select(
        &t(),
        &["id".into()],
        &[],
        &[],
        std::slice::from_ref(&p),
        &[],
        &[],
        100,
    )
    .unwrap_err();
    assert!(err.to_string().contains("scalar predicate"), "got: {err}");
}

#[test]
fn text_pattern_predicate_with_no_operand_is_malformed_not_panic() {
    let p = CallerPredicate {
        column: "name".into(),
        op: CompareOp::Contains,
        values: vec![],
    };
    let err = compile_select(
        &t(),
        &["id".into()],
        &[],
        &[],
        std::slice::from_ref(&p),
        &[],
        &[],
        100,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("text-pattern predicate"),
        "got: {err}"
    );
}
```

- [ ] **Step 2: Run them — expect FAIL (panic or wrong result)**

```bash
buck2 test //src/services/query-api:sql-compile > /tmp/t2a.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t2a.log
```

Expected: the three new tests fail (today a missing operand panics on `p.values[0]` — under a test harness that shows as a test failure/abort).

- [ ] **Step 3: Rewrite `caller_predicate_sql` with slice patterns**

Replace the whole function (sql.rs:276–345). Delete the three `#[expect(clippy::indexing_slicing, ...)]` attributes and the three `debug_assert_eq!`s:

```rust
/// Render one caller predicate at `alias` (empty = unqualified), pushing its operand
/// params in conjunct order. Scalar ops use `op_sql`; set ops expand to N placeholders;
/// null ops emit no param. The column is a trusted ontology identifier (quoted), every
/// operand a bound placeholder. A violated operand-arity invariant (enforced upstream by
/// `filter::coerce_predicate`) is a `MalformedFilter` error — fail closed on this
/// ACL/caller-predicate injection-boundary path, never a panic or a placeholder bound to
/// a stale param.
fn caller_predicate_sql(
    dialect: &dyn SqlDialect,
    p: &CallerPredicate,
    alias: &str,
    params: &mut Vec<SqlValue>,
) -> Result<String, CompileError> {
    use control_plane_core::CompareOp::*;
    let col = col_ref(dialect, alias, &p.column);
    Ok(match p.op {
        In | NotIn => {
            let kw = if matches!(p.op, In) { "IN" } else { "NOT IN" };
            let mut placeholders = Vec::with_capacity(p.values.len());
            for v in &p.values {
                params.push(v.clone());
                placeholders.push(dialect.placeholder(params.len()));
            }
            format!("({col} {kw} ({}))", placeholders.join(", "))
        }
        IsNull => format!("({col} IS NULL)"),
        IsNotNull => format!("({col} IS NOT NULL)"),
        Between => {
            let [lo_v, hi_v] = p.values.as_slice() else {
                return Err(CompileError::MalformedFilter(
                    "between predicate must have two operands".to_string(),
                ));
            };
            params.push(lo_v.clone());
            let lo = dialect.placeholder(params.len());
            params.push(hi_v.clone());
            let hi = dialect.placeholder(params.len());
            format!("({col} BETWEEN {lo} AND {hi})")
        }
        Contains | StartsWith | EndsWith => {
            let [v] = p.values.as_slice() else {
                return Err(CompileError::MalformedFilter(
                    "text-pattern predicate must have one operand".to_string(),
                ));
            };
            params.push(v.clone());
            format!(
                "({col} ILIKE {} ESCAPE '\\')",
                dialect.placeholder(params.len())
            )
        }
        _ => {
            let [v] = p.values.as_slice() else {
                return Err(CompileError::MalformedFilter(
                    "scalar predicate must have one operand".to_string(),
                ));
            };
            params.push(v.clone());
            format!(
                "({col} {} {})",
                op_sql(p.op),
                dialect.placeholder(params.len())
            )
        }
    })
}
```

- [ ] **Step 4: Thread `Result` through the callers**

1. `select_where_conjuncts` becomes `-> Result<Vec<String>, CompileError>`; its two `caller_predicate_sql` calls get `?`; the OR-group members become `let members: Vec<String> = group.iter().map(|m| caller_predicate_sql(dialect, m, "", params)).collect::<Result<_, _>>()?;`; final `Ok(conjuncts)`. Its caller `compile_select_with` adds `?`:
   ```rust
   let conjuncts =
       select_where_conjuncts(dialect, row_filters, predicates, or_groups, &mut params)?;
   ```
2. `chain_from_where`: `conjuncts.push(caller_predicate_sql(dialect, p, &a, &mut params)?);` (already returns `Result`).
3. `reach_seed_where` becomes `-> Result<String, CompileError>`; its seed loop gets `?`; wrap the return in `Ok(...)`. Its four callers add `?`: `compile_graph_reach`, `compile_graph_tree`, `compile_graph_reach_union`, `recursive_reach_cte` (all already return `Result`).

- [ ] **Step 5: Run the tests — expect PASS (new + all pinning tests unchanged)**

```bash
buck2 build -M none //src/services/query-api:query-api
buck2 test //src/services/query-api:sql-compile //src/services/query-api:sql-dialect //src/services/query-api:vector_search_filter //src/services/query-api:compile-graph-reach //src/services/query-api:compile-graph-reach-union //src/services/query-api:compile-graph-reach-tail //src/services/query-api:compile-graph-tree //src/services/query-api:typed-filter-e2e //src/services/query-api:filter-error-http > /tmp/t2b.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t2b.log
```

- [ ] **Step 6: Clippy (empty) + commit**

```bash
git add src/services/query-api/src/sql.rs src/services/query-api/tests/sql_compile.rs
git commit -m "refactor(query): total slice-pattern arity checks in caller_predicate_sql"
```

---

### Task 3: `ReachSpec<'a>` — one params struct for the reach-family compilers

Introduce `ReachSpec<'a>` and re-sign the five reach-family functions, deleting their five `#[allow(clippy::too_many_arguments)]`. Update the two production call-site files (`handler.rs`) and the six test files. **Emitted SQL is unchanged** — this is a signature-only refactor; the existing SQL-string assertions pin it.

**Files:**
- Modify: `src/services/query-api/src/sql.rs`, `src/services/query-api/src/handler.rs`
- Modify (sweep): `tests/compile_graph_reach.rs`, `tests/compile_graph_tree.rs`, `tests/compile_graph_tree_exec.rs`, `tests/compile_graph_reach_union.rs`, `tests/compile_graph_reach_tail.rs`, `tests/recursive_cte_over_datafusion.rs`, `tests/graph_tree.rs` (only if it calls compilers directly — check with grep; handler-level calls need no change)

**Interfaces:**
- Produces (pub, in `crate::sql`):
  ```rust
  pub struct ReachSpec<'a> {
      pub table: &'a TableRef,
      pub identity: &'a str,
      pub seed_predicates: &'a [CallerPredicate],
      pub row_filters: &'a [RowFilter],
      pub allowed_cols: &'a [String],
      pub mask_cols: &'a [String],
      pub depth: u32,
  }
  pub fn compile_graph_reach(dialect: &dyn SqlDialect, spec: &ReachSpec<'_>, path: &[GraphStep], limit: u32) -> Result<(String, Vec<SqlValue>), CompileError>;
  pub fn compile_graph_tree(dialect: &dyn SqlDialect, spec: &ReachSpec<'_>, path: &[GraphStep]) -> Result<(String, Vec<SqlValue>), CompileError>;
  pub fn compile_graph_reach_union(dialect: &dyn SqlDialect, spec: &ReachSpec<'_>, backings: &[LinkBacking], limit: u32) -> Result<(String, Vec<SqlValue>), CompileError>;
  pub fn compile_graph_reach_tail(dialect: &dyn SqlDialect, spec: &ReachSpec<'_>, core_backing: &LinkBacking, tail_types: &[ChainType], tail_hops: &[LinkBacking], final_identity: Option<&str>, limit: u32) -> Result<(String, Vec<SqlValue>), CompileError>;
  ```
  (7 params on `compile_graph_reach_tail` — at the clippy threshold, allow deleted.)
- Task 5 relies on these exact signatures.

- [ ] **Step 1: Add `ReachSpec` to sql.rs** (place it just above `compile_graph_reach`)

```rust
/// The shared parameter core of the reach-family compilers ([`compile_graph_reach`],
/// [`compile_graph_tree`], [`compile_graph_reach_union`], [`compile_graph_reach_tail`],
/// and the private `recursive_reach_cte`): the queried table + its declared identity
/// (the recursion's dedup key), the caller seed predicates + start-type ACL row-filters,
/// the visible/masked projection, and the inlined depth bound. What varies per compiler —
/// the recursion structure (path steps / union backings / core+tail) and the limit —
/// stays a positional parameter. For the core+tail compiler, `row_filters` are the CORE
/// (queried-type) filters and `allowed_cols`/`mask_cols` are the FINAL tail-landed type's
/// projection — see [`compile_graph_reach_tail`].
pub struct ReachSpec<'a> {
    pub table: &'a TableRef,
    pub identity: &'a str,
    pub seed_predicates: &'a [CallerPredicate],
    pub row_filters: &'a [RowFilter],
    pub allowed_cols: &'a [String],
    pub mask_cols: &'a [String],
    pub depth: u32,
}
```

- [ ] **Step 2: Re-sign the five functions**

For each, delete the `#[allow(clippy::too_many_arguments, ...)]` attribute and destructure the spec at the top of the body so the rest of the body compiles unchanged. Pattern (apply to all five):

```rust
pub fn compile_graph_reach(
    dialect: &dyn SqlDialect,
    spec: &ReachSpec<'_>,
    path: &[GraphStep],
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError> {
    let &ReachSpec {
        table,
        identity,
        seed_predicates,
        row_filters,
        allowed_cols,
        mask_cols,
        depth,
    } = spec;
    // ...body unchanged from here...
```

- `compile_graph_tree(dialect, spec, path)` — the `limit` param never existed here; nothing else changes.
- `compile_graph_reach_union(dialect, spec, backings, limit)`.
- `compile_graph_reach_tail(dialect, spec, core_backing, tail_types, tail_hops, final_identity, limit)` — **partial destructure only**: after this task, `table`/`seed_predicates`/`row_filters`/`depth` are consumed solely by the pass-through `recursive_reach_cte(dialect, spec, core_backing, &mut params)?` call, so a full destructure leaves four unused bindings and fails the 0-byte clippy gate. Use:
  ```rust
  let &ReachSpec {
      identity,
      allowed_cols,
      mask_cols,
      ..
  } = spec;
  ```
- `recursive_reach_cte(dialect, spec, backing, params)` (private) — destructure only what it uses:
  ```rust
  fn recursive_reach_cte(
      dialect: &dyn SqlDialect,
      spec: &ReachSpec<'_>,
      backing: &LinkBacking,
      params: &mut Vec<SqlValue>,
  ) -> Result<String, CompileError> {
      let &ReachSpec {
          table,
          identity,
          seed_predicates,
          row_filters,
          depth,
          ..
      } = spec;
  ```
  Its caller in `compile_graph_reach_tail` passes `spec` straight through:
  `let cte = recursive_reach_cte(dialect, spec, core_backing, &mut params)?;`

- [ ] **Step 3: Update handler.rs call sites** (four)

`read_graph_reach`:
```rust
    let (sql, params) = crate::sql::compile_graph_reach(
        deps.serving.dialect(),
        &crate::sql::ReachSpec {
            table: &r.g.otype.table,
            identity: &r.identity,
            seed_predicates: &r.seed_predicates,
            row_filters: &r.g.row_filters,
            allowed_cols: &r.proj.columns,
            mask_cols: &r.proj.masked,
            depth: q.depth,
        },
        &r.steps,
        deps.default_limit,
    )?;
```

`read_graph_tree` — same spec construction, then `crate::sql::compile_graph_tree(deps.serving.dialect(), &spec, &r.steps)?` (bind the spec to a local `let spec = crate::sql::ReachSpec { ... };` when used inline gets unwieldy).

`read_graph_reach_union`:
```rust
    let (sql, params) = crate::sql::compile_graph_reach_union(
        deps.serving.dialect(),
        &crate::sql::ReachSpec {
            table: &g.otype.table,
            identity: &identity,
            seed_predicates: &seeds,
            row_filters: &g.row_filters,
            allowed_cols: &proj.columns,
            mask_cols: &proj.masked,
            depth: q.depth,
        },
        &backings,
        deps.default_limit,
    )?;
```

`read_graph_reach_with_tail`:
```rust
    let (sql, params) = crate::sql::compile_graph_reach_tail(
        deps.serving.dialect(),
        &crate::sql::ReachSpec {
            table: &g.otype.table,
            identity: &identity,
            seed_predicates: &seeds,
            row_filters: &g.row_filters,
            allowed_cols: &proj.columns,
            mask_cols: &proj.masked,
            depth: q.depth,
        },
        &core_backing,
        &tail_types,
        &tail_hops,
        final_g.otype.identity.as_deref(),
        deps.default_limit,
    )?;
```

- [ ] **Step 4: Sweep the test call sites**

Mechanical positional→struct conversion in `tests/compile_graph_reach.rs`, `tests/compile_graph_tree.rs`, `tests/compile_graph_tree_exec.rs`, `tests/compile_graph_reach_union.rs`, `tests/compile_graph_reach_tail.rs`, `tests/recursive_cte_over_datafusion.rs`. Each call like

```rust
compile_graph_reach(&d, &table, "id", &path, &seeds, &filters, &allowed, &masked, 3, 100)
```

becomes

```rust
compile_graph_reach(
    &d,
    &ReachSpec {
        table: &table,
        identity: "id",
        seed_predicates: &seeds,
        row_filters: &filters,
        allowed_cols: &allowed,
        mask_cols: &masked,
        depth: 3,
    },
    &path,
    100,
)
```

Add `ReachSpec` to each file's `use query_api::sql::{...}` import. **Do not touch any assertion string. Do not reflow or move trailing comments — if a call has a trailing comment on an argument line, keep the comment attached to the same value in the struct literal.** The old positional order was `(dialect, table, identity, path/backings, seed_predicates, row_filters, allowed_cols, mask_cols, depth, limit)` for reach/union (tree: no limit; tail: `(dialect, table, identity, core_backing, seed_predicates, core_row_filters, tail_types, tail_hops, allowed_cols, mask_cols, final_identity, depth, limit)`) — map each positional argument to its named field.

- [ ] **Step 5: Build, test, clippy, commit**

```bash
buck2 build -M none //src/services/query-api:query-api
buck2 test //src/services/query-api:compile-graph-reach //src/services/query-api:compile-graph-tree //src/services/query-api:compile-graph-tree-exec //src/services/query-api:compile-graph-reach-union //src/services/query-api:compile-graph-reach-tail //src/services/query-api:recursive-cte-over-datafusion //src/services/query-api:graph-reach //src/services/query-api:graph-tree //src/services/query-api:graph-reach-union //src/services/query-api:graph-reach-tail > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t3.log
git add -A src/services/query-api
git commit -m "refactor(query): ReachSpec params struct for the reach-family compilers"
```

---

### Task 4: `SelectInputs<'a>` — kill the last two sql.rs `too_many_arguments` allows

Group `compile_select_with`/`compile_select`'s four WHERE/SELECT input slices into `SelectInputs<'a>` (with `Default` so empty call sites stay terse), deleting the two remaining allows. sql.rs ends this task with ZERO `#[allow(clippy::too_many_arguments)]`.

**Files:**
- Modify: `src/services/query-api/src/sql.rs`, `src/services/query-api/src/handler.rs`
- Modify (sweep): `tests/sql_compile.rs`, `tests/sql_dialect.rs`, `tests/vector_search_filter.rs`

**Interfaces:**
- Produces (pub, in `crate::sql`):
  ```rust
  #[derive(Default)]
  pub struct SelectInputs<'a> {
      pub row_filters: &'a [RowFilter],
      pub predicates: &'a [CallerPredicate],
      pub or_groups: &'a [Vec<CallerPredicate>],
      pub derived: &'a [DerivedSelect],
  }
  pub fn compile_select_with(dialect: &dyn SqlDialect, table: &TableRef, allowed_cols: &[String], mask_cols: &[String], inputs: &SelectInputs<'_>, order_by: Option<&str>, limit: u32) -> Result<(String, Vec<SqlValue>), CompileError>;
  pub fn compile_select(table: &TableRef, allowed_cols: &[String], mask_cols: &[String], inputs: &SelectInputs<'_>, limit: u32) -> Result<(String, Vec<SqlValue>), CompileError>;
  ```

- [ ] **Step 1: Add `SelectInputs` and re-sign the two functions**

```rust
/// The WHERE/SELECT inputs of a governed flat SELECT, grouped: ACL row-filters, caller
/// predicates, OR-groups, and derived (aggregate-over-link) SELECT columns. All borrowed;
/// `Default` is the empty read so call sites name only what they bind:
/// `SelectInputs { row_filters: &g.row_filters, ..SelectInputs::default() }`.
#[derive(Default)]
pub struct SelectInputs<'a> {
    pub row_filters: &'a [RowFilter],
    pub predicates: &'a [CallerPredicate],
    pub or_groups: &'a [Vec<CallerPredicate>],
    pub derived: &'a [DerivedSelect],
}
```

Both functions delete their allow and destructure:

```rust
pub fn compile_select_with(
    dialect: &dyn SqlDialect,
    table: &TableRef,
    allowed_cols: &[String],
    mask_cols: &[String],
    inputs: &SelectInputs<'_>,
    order_by: Option<&str>,
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError> {
    let &SelectInputs {
        row_filters,
        predicates,
        or_groups,
        derived,
    } = inputs;
    // ...body unchanged...
```

`compile_select` forwards: `compile_select_with(&DataFusionDialect, table, allowed_cols, mask_cols, inputs, None, limit)`.

- [ ] **Step 2: Update handler.rs call sites** (two)

`compile_object_read_with`:
```rust
    let (sql, params) = compile_select_with(
        dialect,
        &g.otype.table,
        &proj.columns,
        &proj.masked,
        &crate::sql::SelectInputs {
            row_filters: &g.row_filters,
            predicates: &predicates,
            or_groups: &or_groups,
            derived: &derived_selects,
        },
        order_by,
        limit,
    )?;
```

`vector_search`:
```rust
    let (sql, params) = compile_select_with(
        deps.serving.dialect(),
        &g.otype.table,
        std::slice::from_ref(&identity),
        &[],
        &crate::sql::SelectInputs {
            row_filters: &g.row_filters,
            predicates: std::slice::from_ref(&pred),
            ..crate::sql::SelectInputs::default()
        },
        None,
        limit,
    )?;
```

- [ ] **Step 3: Sweep the test call sites** (`tests/sql_compile.rs` ~27, `tests/sql_dialect.rs` ~12, `tests/vector_search_filter.rs` ~3)

Old positional order: `compile_select(table, allowed_cols, mask_cols, row_filters, predicates, or_groups, derived, limit)`; `compile_select_with(dialect, table, allowed_cols, mask_cols, row_filters, predicates, or_groups, derived, order_by, limit)`. Convert positions 4–7 (row_filters, predicates, or_groups, derived) into the `SelectInputs` literal; when all four are empty (`&[], &[], &[], &[]`) use `&SelectInputs::default()`; when some are empty use struct-update (`..SelectInputs::default()`). Import `SelectInputs` in each file. Preserve assertion strings and trailing comments exactly (same caution as Task 3 Step 4). This includes the three arity tests added in Task 2 (their `&[], from_ref(&p), &[], &[]` becomes `&SelectInputs { predicates: std::slice::from_ref(&p), ..SelectInputs::default() }`).

- [ ] **Step 4: Verify zero allows remain, build, test, commit**

```bash
grep -c "too_many_arguments" src/services/query-api/src/sql.rs   # expect 0
buck2 build -M none //src/services/query-api:query-api
buck2 test //src/services/query-api:sql-compile //src/services/query-api:sql-dialect //src/services/query-api:vector_search_filter //src/services/query-api:governed-read //src/services/query-api:derived-properties-e2e //src/services/query-api:object-pagination-e2e //src/services/query-api:vector_search_e2e > /tmp/t4.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t4.log
git add -A src/services/query-api
git commit -m "refactor(query): SelectInputs params struct; sql.rs sheds its last too_many_arguments allows"
```

---

### Task 5: graph entry-point collapse — `GraphReadKind` + one `read_graph`

`read_graph_reach_union` and `read_graph_reach_with_tail` are full parallel copies of `resolve_graph`'s prologue + epilogue with different middles. Collapse to: a shared `graph_prologue`, per-mode resolvers, one mode-polymorphic `pub read_graph(GraphReadQuery)`, and ONE http.rs responder. `read_graph_reach_union`/`GraphUnionQuery` and `read_graph_reach_with_tail`/`GraphTailQuery` and `read_graph_reach` are **deleted**; their tests migrate to `read_graph`. `GraphQuery` + `read_graph_tree` stay (the tree returns a different shape).

**Error-precedence contract (e2e-pinned, preserve exactly):**
1. `Forbidden` (coarse Read deny) — before existence is revealed.
2. `UnknownType` (404) — genuine miss.
3. `NoIdentity` — before any mode validation.
4. Mode validation: PathCycle: empty path → `NotCyclicPath("")`; Union: empty links → `NotCyclicPath("")`, unknown link → `UnknownLink`, non-self link → `NotCyclicPath(name)`; CoreTail: empty tail → `BadGraphPath(msg)`, unknown core → `UnknownLink`, non-self core → `BadGraphPath(msg)`.
5. Projection `Forbidden` (no visible columns), then seed-filter errors.

**Files:**
- Modify: `src/services/query-api/src/handler.rs`, `src/services/query-api/src/http.rs`
- Modify (sweep): `tests/graph_reach.rs`, `tests/graph_reach_union.rs`, `tests/graph_reach_tail.rs`, `tests/graph_path_e2e.rs`, `tests/graph_tree.rs` (only its `resolve_graph`-shape uses, if any)

**Interfaces:**
- Consumes: `resolve_governed`, `Projection::visible`, `seed_predicates`, `resolve_hop`, `identity_in_predicate` (governed.rs); `ReachSpec` + the four compilers (Task 3 signatures).
- Produces (pub, in `crate::handler`):
  ```rust
  pub enum GraphReadKind {
      PathCycle { path: Vec<Hop> },
      UnionSelfLinks { links: Vec<String> },
      CoreTail { core_link: String, tail_links: Vec<String> },
  }
  pub struct GraphReadQuery {
      pub type_name: String,
      pub kind: GraphReadKind,
      pub depth: u32,
      pub filters: Vec<(String, String)>,
      pub ids: Vec<String>,
  }
  pub async fn read_graph(q: &GraphReadQuery, subject: &Subject, deps: &QueryDeps<'_>) -> Result<ObjectRows, QueryError>;
  ```
  `GraphQuery` (unchanged) + `read_graph_tree(q: &GraphQuery, ...)` (unchanged signature) remain. `GraphUnionQuery`, `GraphTailQuery`, `read_graph_reach`, `read_graph_reach_union`, `read_graph_reach_with_tail` are deleted.

- [ ] **Step 1: Add the shared prologue + mode types to handler.rs**

```rust
/// The recursion structure of a `/graph` object read: which of the three reachability
/// modes the request selects. The prologue (coarse Read gate, declared identity), the
/// seed scoping, and the serve/zip epilogue are shared by [`read_graph`]; the kind picks
/// the middle — what to resolve and which compiler to call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphReadKind {
    /// Repeat a cyclic link `path` (a 1-element path is the single-self-link case).
    PathCycle { path: Vec<Hop> },
    /// Repeatedly follow ANY ONE of `links` (each a self-link on the queried type).
    UnionSelfLinks { links: Vec<String> },
    /// Follow `core_link` (a `*`-suffixed self-link) transitively, then chain
    /// `tail_links` forward off the reachable set and project the final landed type.
    CoreTail {
        core_link: String,
        tail_links: Vec<String>,
    },
}

/// A bounded recursive reachability read, mode-polymorphic over [`GraphReadKind`].
/// `filters`/`ids` scope the SEED set (the recursion start) in every mode.
pub struct GraphReadQuery {
    pub type_name: String,
    pub kind: GraphReadKind,
    pub depth: u32,
    pub filters: Vec<(String, String)>,
    pub ids: Vec<String>,
}

/// The shared `/graph` prologue: coarse-Read-gate + resolve the queried type (404 on a
/// genuine miss) and require its declared identity (the recursion's dedup key). Every
/// graph mode starts here, so the NoIdentity-before-mode-validation error precedence
/// lives in exactly one place.
async fn graph_prologue(
    type_name: &str,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<(GovernedType, String), QueryError> {
    let name = TypeName(type_name.to_string());
    let g = resolve_governed(deps.ontology, deps.acl, &subject.0, &name, OnMissing::NotFound)
        .await?;
    let identity = g
        .otype
        .identity
        .clone()
        .ok_or_else(|| QueryError::NoIdentity(type_name.to_string()))?;
    Ok((g, identity))
}
```

- [ ] **Step 2: Slim `resolve_graph` onto the prologue and re-sign it to borrowed parts**

`resolve_graph` keeps producing `GraphResolved` (used by the PathCycle arm AND `read_graph_tree`), but its signature becomes borrowed parts and its first ~20 lines become a `graph_prologue` call:

```rust
/// Resolve + govern a path-cycle graph read: the shared prologue, the path-cycle walk
/// (Read on every intermediate type, its row-filters folded into the step), the visible
/// projection, and the coerced seed predicates + `?_ids=`. Shared by [`read_graph`]'s
/// path-cycle arm and [`read_graph_tree`] so cycle governance lives in one place.
async fn resolve_graph(
    type_name: &str,
    path: &[Hop],
    filters: &[(String, String)],
    ids: &[String],
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<GraphResolved, QueryError> {
    let (g, identity) = graph_prologue(type_name, subject, deps).await?;
    let name = TypeName(type_name.to_string());

    // Resolve the path-cycle: walk l1..lK from the queried type. Each landed type is
    // Read-gated and its row-filters loaded (intermediate governance). After the last
    // link the type must be the queried type again (a cycle) — else it cannot repeat.
    if path.is_empty() {
        return Err(QueryError::NotCyclicPath(String::new()));
    }
    let mut steps: Vec<crate::sql::GraphStep> = Vec::with_capacity(path.len());
    let mut current = name.clone();
    let last = path.len() - 1;
    for (i, hop) in path.iter().enumerate() {
        let (landed, backing) = resolve_hop(deps.ontology, &current, hop).await?;
        // Read on every reached type (intermediate + final), forward or inverse. A link
        // pointing at a missing type is an internal inconsistency, not a 404.
        let landed_g = resolve_governed(
            deps.ontology,
            deps.acl,
            &subject.0,
            &landed,
            OnMissing::Internal,
        )
        .await?;
        // Intermediates carry their own row-filters; the FINAL landing is the start
        // type, whose filters are rendered at `nxt` by the compiler -> pass empty here
        // (no double-render).
        steps.push(crate::sql::GraphStep {
            backing,
            next_table: landed_g.otype.table.clone(),
            next_filters: if i == last {
                Vec::new()
            } else {
                landed_g.row_filters
            },
        });
        current = landed;
    }
    if current != name {
        return Err(QueryError::NotCyclicPath(hop_path_string(path)));
    }

    // Projection: visible columns minus denied; masked applied. Empty -> Forbidden.
    let proj = Projection::visible(&g)?;

    // Seed predicates: source filters (visibility-checked + coerced) then the ?_ids= set.
    let seed = seed_predicates(&g, &proj.columns, filters, ids)?;

    Ok(GraphResolved {
        g,
        identity,
        steps,
        proj,
        seed_predicates: seed,
    })
}
```

Update `read_graph_reach`'s and `read_graph_tree`'s calls — `read_graph_tree` becomes:
```rust
    let r = resolve_graph(&q.type_name, &q.path, &q.filters, &q.ids, subject, deps).await?;
```
(`read_graph_reach` is deleted in Step 4, so only tree needs this.)

- [ ] **Step 3: Add the union + core-tail middle resolvers (verbatim moves)**

```rust
/// Resolve a union read's self-link set: every named link must be an outbound link of
/// the queried type whose `to` is the queried type (a self-link) -> else NotCyclicPath;
/// an unknown link -> UnknownLink. Dedup by name (first-seen order; a link listed twice
/// yields one arm). No per-link Read gate — every link lands on the already-gated
/// queried type.
async fn resolve_self_link_backings(
    ontology: &(dyn Ontology + Send + Sync),
    type_name: &TypeName,
    link_names: &[String],
) -> Result<Vec<control_plane_core::LinkBacking>, QueryError> {
    if link_names.is_empty() {
        return Err(QueryError::NotCyclicPath(String::new()));
    }
    let links = ontology.links(type_name, PageReq::unbounded()).await?;
    let mut backings: Vec<control_plane_core::LinkBacking> = Vec::with_capacity(link_names.len());
    let mut seen: std::collections::HashSet<&String> = std::collections::HashSet::new();
    for link_name in link_names {
        if !seen.insert(link_name) {
            continue; // duplicate -> one arm
        }
        let link = links
            .items
            .iter()
            .find(|l| &l.name == link_name)
            .ok_or_else(|| QueryError::UnknownLink(link_name.clone()))?;
        if &link.to != type_name {
            return Err(QueryError::NotCyclicPath(link_name.clone()));
        }
        backings.push(link.backing.clone());
    }
    Ok(backings)
}

/// Resolve a core+tail read's structure: the `*` core must be a self-link on the queried
/// type and the tail non-empty (else BadGraphPath); each tail hop resolves FORWARD with
/// Read on every landed type. Returns the core backing, the compiler tail chain
/// (position 0 = the queried type with EMPTY row-filters — its governance lives in the
/// recursive CTE), the tail hop backings, and the FINAL landed type's governance (the
/// projection source). The `final_g` fold is load-bearing: the projection and the
/// final-identity dedup key come from the LAST tail landing, not the queried type.
async fn resolve_core_tail(
    type_name: &TypeName,
    g: &GovernedType,
    core_link: &str,
    tail_links: &[String],
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<
    (
        control_plane_core::LinkBacking,
        Vec<crate::sql::ChainType>,
        Vec<control_plane_core::LinkBacking>,
        GovernedType,
    ),
    QueryError,
> {
    // The relational tail must be non-empty (a bare recursive core is `/graph/:link`).
    if tail_links.is_empty() {
        return Err(QueryError::BadGraphPath(format!(
            "recursive core `{core_link}*` requires a relational tail; use /graph/:link for bare reachability"
        )));
    }

    // Resolve the recursive core link: an outbound link of the queried type whose `to`
    // is the queried type itself (a self-link).
    let links = deps.ontology.links(type_name, PageReq::unbounded()).await?;
    let core = links
        .items
        .iter()
        .find(|l| l.name == core_link)
        .ok_or_else(|| QueryError::UnknownLink(core_link.to_string()))?;
    if &core.to != type_name {
        return Err(QueryError::BadGraphPath(format!(
            "recursive core `{core_link}*` must land back on `{}`",
            type_name.0
        )));
    }
    let core_backing = core.backing.clone();

    // Resolve the forward tail. Position 0 is the queried type with EMPTY row-filters —
    // its governance lives in the recursive CTE; the tail constrains it by
    // reach-membership. Each tail-landed type is Read-gated and its row-filters loaded;
    // the final landing is projected.
    let mut tail_types: Vec<crate::sql::ChainType> = vec![crate::sql::ChainType {
        table: g.otype.table.clone(),
        row_filters: vec![],
        predicates: vec![],
    }];
    let mut tail_hops: Vec<control_plane_core::LinkBacking> = Vec::with_capacity(tail_links.len());
    let mut current = type_name.clone();
    let mut final_g = g.clone();
    for link_name in tail_links {
        let (landed, backing) =
            resolve_hop(deps.ontology, &current, &Hop::from(link_name.as_str())).await?;
        let landed_g = resolve_governed(
            deps.ontology,
            deps.acl,
            &subject.0,
            &landed,
            OnMissing::Internal,
        )
        .await?;
        tail_hops.push(backing);
        tail_types.push(crate::sql::ChainType {
            table: landed_g.otype.table.clone(),
            row_filters: landed_g.row_filters.clone(),
            predicates: vec![],
        });
        final_g = landed_g;
        current = landed;
    }
    Ok((core_backing, tail_types, tail_hops, final_g))
}
```

The error-message strings above must match the deleted originals **byte-for-byte** — note the original tail-empty message interpolates the bare core link (`q.core_link` without the `*`), and the not-self message interpolates the queried type name; both are preserved via `{core_link}`/`type_name.0`.

- [ ] **Step 4: Add `read_graph`; delete the three old entry points + two query structs**

```rust
/// Serve a bounded recursive reachability read (deduped reachable objects), polymorphic
/// over the three graph modes. Governance per mode is documented on the mode resolvers;
/// all modes share the prologue (Read gate + declared identity), the seed scoping, and
/// the serve/zip epilogue.
pub async fn read_graph(
    q: &GraphReadQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<ObjectRows, QueryError> {
    let dialect = deps.serving.dialect();
    let (proj, (sql, params)) = match &q.kind {
        GraphReadKind::PathCycle { path } => {
            let r = resolve_graph(&q.type_name, path, &q.filters, &q.ids, subject, deps).await?;
            let compiled = crate::sql::compile_graph_reach(
                dialect,
                &crate::sql::ReachSpec {
                    table: &r.g.otype.table,
                    identity: &r.identity,
                    seed_predicates: &r.seed_predicates,
                    row_filters: &r.g.row_filters,
                    allowed_cols: &r.proj.columns,
                    mask_cols: &r.proj.masked,
                    depth: q.depth,
                },
                &r.steps,
                deps.default_limit,
            )?;
            (r.proj, compiled)
        }
        GraphReadKind::UnionSelfLinks { links } => {
            let (g, identity) = graph_prologue(&q.type_name, subject, deps).await?;
            let name = TypeName(q.type_name.clone());
            let backings = resolve_self_link_backings(deps.ontology, &name, links).await?;
            let proj = Projection::visible(&g)?;
            let seeds = seed_predicates(&g, &proj.columns, &q.filters, &q.ids)?;
            let compiled = crate::sql::compile_graph_reach_union(
                dialect,
                &crate::sql::ReachSpec {
                    table: &g.otype.table,
                    identity: &identity,
                    seed_predicates: &seeds,
                    row_filters: &g.row_filters,
                    allowed_cols: &proj.columns,
                    mask_cols: &proj.masked,
                    depth: q.depth,
                },
                &backings,
                deps.default_limit,
            )?;
            (proj, compiled)
        }
        GraphReadKind::CoreTail {
            core_link,
            tail_links,
        } => {
            let (g, identity) = graph_prologue(&q.type_name, subject, deps).await?;
            let name = TypeName(q.type_name.clone());
            let (core_backing, tail_types, tail_hops, final_g) =
                resolve_core_tail(&name, &g, core_link, tail_links, subject, deps).await?;
            // Projection: the FINAL tail type's visible columns. Empty -> Forbidden.
            let proj = Projection::visible(&final_g)?;
            // Seed predicates scope the recursion start (alias `s` in the CTE), governed
            // by the QUERIED type's projection.
            let source_allowed = g.allowed();
            let seeds = seed_predicates(&g, &source_allowed, &q.filters, &q.ids)?;
            let compiled = crate::sql::compile_graph_reach_tail(
                dialect,
                &crate::sql::ReachSpec {
                    table: &g.otype.table,
                    identity: &identity,
                    seed_predicates: &seeds,
                    row_filters: &g.row_filters,
                    allowed_cols: &proj.columns,
                    mask_cols: &proj.masked,
                    depth: q.depth,
                },
                &core_backing,
                &tail_types,
                &tail_hops,
                final_g.otype.identity.as_deref(),
                deps.default_limit,
            )?;
            (proj, compiled)
        }
    };
    let served = deps.serving.fetch_rows(&sql, &params).await?;
    Ok(proj.into_object_rows(served))
}
```

Delete: `read_graph_reach`, `read_graph_reach_union` + `GraphUnionQuery`, `read_graph_reach_with_tail` + `GraphTailQuery`, and their doc comments. `GraphQuery` stays (tree). Keep `read_graph_tree` working through the slimmed `resolve_graph`.

- [ ] **Step 5: Collapse the http.rs responders**

Replace `graph_respond`, `graph_union_respond`, and `graph_tail_respond` (and the latter's `#[allow(too_many_arguments)]`) with ONE:

```rust
/// Shared `/graph` object-read tail: build a `GraphReadQuery` of the given kind, run
/// `read_graph`, render, map errors via `graph_error`.
async fn graph_read_respond(
    st: &AppState,
    type_name: String,
    kind: GraphReadKind,
    depth: u32,
    filters: Vec<(String, String)>,
    ids: Vec<String>,
    subject: &Subject,
) -> axum::response::Response {
    let deps = QueryDeps {
        ontology: st.cp.ontology(),
        acl: st.cp.acl(),
        serving: st.serving.as_ref(),
        default_limit: st.default_limit,
    };
    match read_graph(
        &GraphReadQuery {
            type_name,
            kind,
            depth,
            filters,
            ids,
        },
        subject,
        &deps,
    )
    .await
    {
        Ok(rows) => Json(crate::render::objects_to_json(&rows, None)).into_response(),
        Err(e) => graph_error(e),
    }
}
```

Call sites:
- `get_graph` non-tree arm: `graph_read_respond(&st, type_name, GraphReadKind::PathCycle { path: vec![link_name.into()] }, depth, filters, ids, &subject).await`
- `get_graph_path` union arm: `graph_read_respond(&st, type_name, GraphReadKind::UnionSelfLinks { links }, depth, filters, ids, &subject).await`
- `get_graph_path` starred arm: `graph_read_respond(&st, type_name, GraphReadKind::CoreTail { core_link, tail_links }, depth, filters, ids, &subject).await`
- `get_graph_path` plain-path arm: `graph_read_respond(&st, type_name, GraphReadKind::PathCycle { path }, depth, filters, ids, &subject).await`

Update the `use crate::handler::{...}` import list (drop the deleted names, add `GraphReadKind, GraphReadQuery, read_graph`). `graph_tree_respond` is unchanged.

- [ ] **Step 6: Migrate the handler-level tests**

- `tests/graph_reach.rs` and `tests/graph_path_e2e.rs`: `read_graph_reach(&GraphQuery { type_name, path, depth, filters, ids }, ...)` becomes `read_graph(&GraphReadQuery { type_name, kind: GraphReadKind::PathCycle { path }, depth, filters, ids }, ...)`. (If `graph_path_e2e.rs` drives HTTP only, it needs no change — check first.)
- `tests/graph_reach_union.rs`: `read_graph_reach_union(&GraphUnionQuery { type_name, links, depth, filters, ids }, ...)` becomes `read_graph(&GraphReadQuery { type_name, kind: GraphReadKind::UnionSelfLinks { links }, depth, filters, ids }, ...)`.
- `tests/graph_reach_tail.rs`: `read_graph_reach_with_tail(&GraphTailQuery { type_name, core_link, tail_links, depth, filters, ids }, ...)` becomes `read_graph(&GraphReadQuery { type_name, kind: GraphReadKind::CoreTail { core_link, tail_links }, depth, filters, ids }, ...)`.
- `tests/graph_reach.rs` builds its queries through a local `graph_query(&[…]) -> GraphQuery` helper, not only inline literals — adapt the helper too (return `GraphReadQuery` wrapping `GraphReadKind::PathCycle`).
- Update each file's imports — DROP the now-unused `GraphQuery`/`GraphUnionQuery`/`GraphTailQuery` imports (a dangling import warns and fails the 0-byte clippy gate); add `GraphReadKind`, `GraphReadQuery`, `read_graph`. Assertions unchanged.

- [ ] **Step 7: Build, run the full graph suite, commit**

```bash
buck2 build -M none //src/services/query-api:query-api
buck2 test //src/services/query-api:graph-reach //src/services/query-api:graph-tree //src/services/query-api:graph-reach-union //src/services/query-api:graph-reach-tail //src/services/query-api:graph-reach-e2e //src/services/query-api:graph-path-e2e //src/services/query-api:graph-union-e2e //src/services/query-api:graph-tail-e2e //src/services/query-api:graph-tree-e2e //src/services/query-api:graph-inverse-e2e //src/services/query-api:http-smoke > /tmp/t5.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t5.log
git add -A src/services/query-api
git commit -m "refactor(query): collapse graph entry points into mode-polymorphic read_graph"
```

---

### Task 6: http.rs consolidation + flight_export governed helper

Four http.rs consolidations plus the flight_export duplicate: (a) `AppState::deps()`; (b) `ReservedParams` splitter + fallible `parse_ids`/`parse_depth`/`parse_tree`/`graph_depth`; (c) `respond_shaped` shared `/links` tail (closes the census pair `http.rs:404-430 ≈ 487-513`); (d) ONE total `query_error_response` replacing `chain_error`, `graph_error`, get_object's two inline matches, and post_search's mapping; (e) `FlightExportService::governed`.

**Accepted micro-divergences (document in commit body, do not "fix"):**
- When a request carries SEVERAL simultaneously-invalid reserved params (e.g. `?_ids=&depth=abc`), the 400 message may now be the one for a different param than before (split parses in param order and errors immediately). Status is 400 either way; no e2e sends multi-fault requests.
- The total mapping gives currently-UNREACHABLE variants a deliberate status (e.g. `BadPagination` on a non-paginated path → 400 instead of 500; `ServingError::NoIndex` on a chain path → 404). No reachable input changes.

**Files:**
- Modify: `src/services/query-api/src/http.rs`, `src/services/query-api/src/flight_export.rs`

**Interfaces:**
- Consumes: `QueryError` (all variants), `crate::serving::ServingError::{Engine, NoIndex, DimMismatch}`, `crate::handler::GovernedRead`, `ExportCommand` (Clone).
- Produces (private to http.rs): `AppState::deps(&self) -> QueryDeps<'_>`; `struct ReservedParams`; `fn query_error_response(e: QueryError, context: &'static str) -> Response`; `async fn respond_shaped(...)`. Private to flight_export.rs: `async fn governed(&self, cmd: ExportCommand, subject: SubjectId) -> Result<GovernedRead, Status>`.

- [ ] **Step 1: `AppState::deps()`** — add below the `AppState` struct and replace every hand-built `QueryDeps { ... }` literal (get_object, get_linked, get_linked_chain, `graph_read_respond`, `graph_tree_respond`, post_search):

```rust
impl AppState {
    /// The borrowed read-path dependency bundle — one construction point for the
    /// `QueryDeps` literal previously hand-built per endpoint.
    fn deps(&self) -> QueryDeps<'_> {
        QueryDeps {
            ontology: self.cp.ontology(),
            acl: self.cp.acl(),
            serving: self.serving.as_ref(),
            default_limit: self.default_limit,
        }
    }
}
```

- [ ] **Step 2: `ReservedParams` + parse helpers** — add near the top of http.rs; delete `parse_bool_flag`:

```rust
/// Parse an `_ids` value: comma-split, drop empties; an empty result is the caller
/// fault every read endpoint 400s on.
fn parse_ids(v: &str) -> Result<Vec<String>, axum::response::Response> {
    let ids: Vec<String> = v
        .split(',')
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect();
    if ids.is_empty() {
        return Err(
            (StatusCode::BAD_REQUEST, "_ids requires at least one value").into_response(),
        );
    }
    Ok(ids)
}

/// Parse a `depth` value; a non-integer is the exact 400 the endpoints returned inline.
fn parse_depth(v: &str) -> Result<u32, axum::response::Response> {
    v.parse::<u32>().map_err(|_| {
        (StatusCode::BAD_REQUEST, "depth must be a positive integer").into_response()
    })
}

/// Parse a `tree` flag: `true`/`false` (case-insensitive); anything else is a 400.
fn parse_tree(v: &str) -> Result<bool, axum::response::Response> {
    match v.to_ascii_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err((StatusCode::BAD_REQUEST, "tree must be true or false").into_response()),
    }
}

/// Default + bound the graph `depth` knob. The range check is a safety guardrail
/// (bounds recursion), deliberately not config.
fn graph_depth(depth: Option<u32>) -> Result<u32, axum::response::Response> {
    let depth = depth.unwrap_or(DEFAULT_GRAPH_DEPTH);
    if !(1..=MAX_GRAPH_DEPTH).contains(&depth) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("depth must be 1..={MAX_GRAPH_DEPTH}"),
        )
            .into_response());
    }
    Ok(depth)
}

/// The reserved (non-filter) query params of a read endpoint, split from the caller
/// filter pairs in one pass. Each endpoint names exactly the keys it reserves; an
/// unreserved key stays a filter (so e.g. `_direction` on `/objects/{type}` is still an
/// unknown filter column -> 400, exactly as before this splitter existed).
#[derive(Default)]
struct ReservedParams {
    ids: Vec<String>,
    or_raw: Vec<String>,
    limit: Option<String>,
    cursor: Option<String>,
    direction: Option<String>,
    shape: Option<String>,
    path: Option<String>,
    links: Vec<String>,
    depth: Option<u32>,
    tree: bool,
    filters: Vec<(String, String)>,
}

impl ReservedParams {
    /// Split `params` on the endpoint's `reserved` key set. Parse failures return the
    /// exact 400s the endpoints previously produced inline.
    fn split(
        params: Vec<(String, String)>,
        reserved: &[&str],
    ) -> Result<Self, axum::response::Response> {
        let mut out = Self::default();
        for (k, v) in params {
            if !reserved.contains(&k.as_str()) {
                out.filters.push((k, v));
                continue;
            }
            match k.as_str() {
                "_ids" => out.ids = parse_ids(&v)?,
                "_or" => out.or_raw.push(v),
                "limit" => out.limit = Some(v),
                "cursor" => out.cursor = Some(v),
                "_direction" => out.direction = Some(v),
                "_shape" => out.shape = Some(v),
                "path" | "_path" => out.path = Some(v),
                "links" => {
                    out.links = v
                        .split(',')
                        .filter(|s| !s.is_empty())
                        .map(String::from)
                        .collect();
                }
                "depth" => out.depth = Some(parse_depth(&v)?),
                "tree" => out.tree = parse_tree(&v)?,
                _ => out.filters.push((k, v)),
            }
        }
        Ok(out)
    }
}
```

Rewrite the five endpoint preludes on it (each keeps its exact reserved set):

- `get_object`: `let p = match ReservedParams::split(params, &["_ids", "_or", "limit", "cursor"]) { Ok(p) => p, Err(r) => return r };` then use `p.ids`, `p.or_raw`, `p.limit` (as `raw_limit`), `p.cursor` (as `raw_cursor`), `p.filters`. The `limit` parse/clamp logic stays in `get_object` unchanged (message `"limit must be a positive integer"`).
- `get_linked`: reserved `&["_direction", "_shape", "_ids"]`; then `parse_direction(p.direction.as_deref())` as today.
- `get_linked_chain`: reserved `&["_path", "_shape", "_ids"]`; `let hops = p.path.as_deref().map(parse_path_hops).unwrap_or_default();`.
- `get_graph`: reserved `&["depth", "_ids", "tree"]`; `let depth = match graph_depth(p.depth) { Ok(d) => d, Err(r) => return r };`.
- `get_graph_path`: reserved `&["path", "links", "depth", "_ids", "tree"]`; same `graph_depth`; `let path = p.path.as_deref().map(parse_path_hops).unwrap_or_default(); let links = p.links;`.

Delete the now-dead `ids_present` bookkeeping in every endpoint (the splitter already 400s an empty `_ids`).

- [ ] **Step 3: `respond_shaped`** — replace the duplicated tails of `get_linked` and `get_linked_chain`:

```rust
/// The shared tail of the two `/links` routes: resolve filter keys against the path's
/// bare link names, build the `ChainQuery`, and dispatch on `_shape`
/// (objects | association).
async fn respond_shaped(
    st: &AppState,
    from_type: String,
    path: Vec<Hop>,
    shape: Option<String>,
    filter_params: Vec<(String, String)>,
    ids: Vec<String>,
    subject: &Subject,
) -> axum::response::Response {
    // Filter keys reference bare link names; resolve against those (direction-independent).
    let names: Vec<String> = path.iter().map(|h| h.link.clone()).collect();
    let filters = match crate::chain_filter::resolve_chain_filters(&names, filter_params) {
        Ok(f) => f,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    let deps = st.deps();
    let query = ChainQuery {
        from_type,
        path,
        filters,
        ids,
    };
    match shape.as_deref() {
        None | Some("objects") => {
            respond_objects(read_linked_chain(&query, subject, &deps).await)
        }
        Some("association") => {
            respond_associations(read_associations(&query, subject, &deps).await)
        }
        Some(other) => {
            (StatusCode::BAD_REQUEST, format!("unknown shape: {other}")).into_response()
        }
    }
}
```

`get_linked` becomes: split → `parse_direction` → `respond_shaped(&st, from_type, vec![Hop { link: link_name, direction }], p.shape, p.filters, p.ids, &subject).await`. `get_linked_chain` becomes: split → parse hops → `respond_shaped(&st, from_type, hops, p.shape, p.filters, p.ids, &subject).await`.

- [ ] **Step 4: the total `query_error_response`** — add it; delete `chain_error` and `graph_error`; replace get_object's two inline `Err(...)` matches and post_search's error arms:

```rust
/// The single, TOTAL `QueryError` -> HTTP response mapping. Every variant is matched
/// deliberately — adding a `QueryError` variant is a compile error here, not a silent
/// 500. Governance denials are bodyless 403s; caller faults echo only the
/// caller-supplied name (never internal SQL/schema detail); backend faults log
/// server-side via `internal_error` with the given `context` and return an opaque 500.
fn query_error_response(e: QueryError, context: &'static str) -> axum::response::Response {
    use crate::serving::ServingError;
    match e {
        QueryError::UnknownType(t) => (StatusCode::NOT_FOUND, t).into_response(),
        QueryError::UnknownLink(l) => (StatusCode::NOT_FOUND, l).into_response(),
        QueryError::AmbiguousLink(l) => (StatusCode::BAD_REQUEST, l).into_response(),
        QueryError::Forbidden => StatusCode::FORBIDDEN.into_response(),
        QueryError::BadFilter(c) => (StatusCode::BAD_REQUEST, c).into_response(),
        QueryError::BadFilterValue(ref err) => bad_filter_value_response(err),
        QueryError::BadChain(m) | QueryError::BadGraphPath(m) | QueryError::BadPagination(m) => {
            (StatusCode::BAD_REQUEST, m).into_response()
        }
        QueryError::NoIdentity(t) => (StatusCode::BAD_REQUEST, t).into_response(),
        QueryError::NotCyclicPath(p) => (StatusCode::BAD_REQUEST, p).into_response(),
        QueryError::Serving(ServingError::NoIndex(m)) => {
            (StatusCode::NOT_FOUND, m).into_response()
        }
        QueryError::Serving(ServingError::DimMismatch(m)) => {
            (StatusCode::BAD_REQUEST, m).into_response()
        }
        e @ (QueryError::Serving(ServingError::Engine(_))
        | QueryError::ControlPlane(_)
        | QueryError::Malformed(_)) => internal_error(context, e),
    }
}
```

Replacement sites (each `Ok(...)` arm is UNCHANGED):
- get_object paginated: `Err(e) => query_error_response(e, "object read serving fault")` replaces the six `Err(...)` arms.
- get_object plain: same single arm.
- `respond_objects`/`respond_associations`: `Err(e) => query_error_response(e, "chain/association read serving fault")`.
- `graph_read_respond`/`graph_tree_respond`: `Err(e) => query_error_response(e, "graph read serving fault")`.
- post_search: `Err(e) => query_error_response(e, "vector search serving fault")` replaces its five `Err(...)` arms.

- [ ] **Step 5: flight_export.rs `governed()` helper**

```rust
impl FlightExportService {
    /// The shared governed-compile step of `get_flight_info` and `do_get`: re-derive
    /// the ACL'd SQL for THIS authenticated subject from the export command, compiled
    /// with `max_rows + 1` so an over-cap slice is detectable, never silently
    /// truncated.
    async fn governed(
        &self,
        cmd: ExportCommand,
        subject: SubjectId,
    ) -> Result<crate::handler::GovernedRead, Status> {
        compile_object_read(
            &ObjectQuery {
                type_name: cmd.type_name,
                filters: cmd.filters,
                ids: cmd.ids,
                or_raw: Vec::new(),
            },
            &Subject(subject),
            self.cp.ontology(),
            self.cp.acl(),
            &DataFusionDialect,
            self.max_rows.saturating_add(1),
            None,
            None,
        )
        .await
        .map_err(map_query_err)
    }
}
```

`get_flight_info`: `let governed = self.governed(cmd.clone(), subject).await?;` (it still needs `cmd` for the ticket). `do_get`: `let governed = self.governed(cmd, subject).await?;`. Delete both inline `compile_object_read` blocks.

- [ ] **Step 6: Build, test, commit**

```bash
buck2 build -M none //src/services/query-api:query-api
buck2 test //src/services/query-api:http-smoke //src/services/query-api:http-wire-e2e //src/services/query-api:object-set-e2e //src/services/query-api:object-pagination-e2e //src/services/query-api:governed-read //src/services/query-api:link-traversal //src/services/query-api:multi-hop-traversal-e2e //src/services/query-api:association-e2e //src/services/query-api:inverse-hops-e2e //src/services/query-api:filter-error-http //src/services/query-api:vector_search_e2e //src/services/query-api:governed-flight-export-e2e //src/services/query-api:export-command //src/services/query-api:lineage-http-e2e //src/services/query-api:graph-path-e2e //src/services/query-api:graph-union-e2e //src/services/query-api:graph-tail-e2e //src/services/query-api:graph-tree-e2e > /tmp/t6.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t6.log
git add -A src/services/query-api
git commit -m "refactor(query): ReservedParams/respond_shaped/total error mapping/AppState::deps; flight governed helper"
```

(`lineage_closure`'s param loop is deliberately NOT migrated — it ignores unknown params instead of treating them as filters, and its depth default is 1 with the cap enforced below in the capability; the spec's "5×" scraper count is the five object/graph endpoints.)

---

### Task 7: whole-crate verification, register evidence, docs close

- [ ] **Step 1: Full query-api suite + dependent builds**

```bash
buck2 test //src/services/query-api: > /tmp/t7.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t7.log
buck2 build -M none //src/... > /tmp/b7.log 2>&1; tail -3 /tmp/b7.log
```

Expected: all tests pass; whole-src build green.

- [ ] **Step 2: Clippy + prek**

```bash
buck2 build '//src/services/query-api:query-api[clippy.txt]' --out /tmp/clippy-qa.txt; wc -c /tmp/clippy-qa.txt   # 0 bytes
buck2 run //tools:prek -- run --all-files > /tmp/prek.log 2>&1; tail -5 /tmp/prek.log
```

- [ ] **Step 3: Register evidence** (duplication + complexity diff vs main)

```bash
buck2 run -v0 //tools:lucidshark-duplo -- --git --changed-only --json -m 20 --baseline "$PWD/docs/code-health/duplication-baseline.json" > /tmp/dup.json 2>/dev/null || true
buck2 run -v0 //tools:jq -- -rf .claude/skills/loom-duplication/render.jq --slurpfile baseline "$PWD/docs/code-health/duplication-baseline.json" /tmp/dup.json > /tmp/dup-census.md; cat /tmp/dup-census.md
```

Expected evidence: the census pair `http.rs:404-430 ≈ 487-513` is gone; no new production pairs. For complexity, run rust-code-analysis on the touched files and compare `get_graph_path` (was cc 25), `read_graph_reach_union`, `read_graph_reach_with_tail` (deleted), `caller_predicate_sql` against main. Record the numbers in `.superpowers/sdd/register-evidence.md` for the PR body.

- [ ] **Step 4: Close the register item** (loom-docs-update, staged on this branch — committed once the PR number is known at finish time)

In `docs/ROADMAP.md`, the item line `{#road-qa-read-path-consolidation ...}` changes `- [ ]` → `- [x]`, `status:planned` → `status:done`, `pr:-` → `pr:#<N>`. Then `bash tools/docs.sh validate`. Commit with the final push per superpowers:finishing-a-development-branch (open the PR with head `work/road-qa-read-path-consolidation`).

## Self-review notes

- **Spec coverage:** bullet 1 (sql.rs substitutions + ReachSpec) → Tasks 1, 3, 4; bullet 2 (slice patterns) → Task 2; bullet 3 (graph collapse, enum chosen, tail fold preserved verbatim) → Task 5; bullet 4 (http.rs extractors/respond_shaped/total mapping/deps()/flight governed) → Task 6. The spec's "six allows" counted at audit time — the file now has seven; Tasks 3+4 delete all seven (SelectInputs is the minimal extra grouping to finish the job, same idiom as ReachSpec).
- **Type consistency:** `ReachSpec`/`SelectInputs` field names and the `read_graph`/`GraphReadKind` signatures are used identically in Tasks 3–6. `reach_seed_where` becomes `Result` in Task 2 and is consumed with `?` from Task 1's substitution sites.
- **Ordering:** Task 1 before 2 (fewer `Result` propagation sites); 3 before 5 (read_graph uses ReachSpec); 5 before 6 (task 6 replaces `graph_error` which task 5's `graph_read_respond` still calls — task 6 renames that call site).
