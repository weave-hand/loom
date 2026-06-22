# Multi-file DuckLake LIMIT Read Guard — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A governed read of a multi-file DuckLake table returns correct, uncorrupted values under the standard pushed-down `LIMIT` on the DuckDB serving engine, by emitting a stable `ORDER BY` barrier before the `LIMIT`.

**Architecture:** Add a dialect capability `limit_needs_order_barrier()` (`true` only for `DuckDbDialect`); introduce a `DataFusionDialect` that renders identically to DuckDB but keeps the bare `LIMIT` (the DataFusion engine has no such bug); add two small `sql.rs` helpers (`order_key_cols`, `order_barrier_limit`) and apply them at all six `LIMIT` emit sites. `ORDER BY <key> LIMIT n` compiles to DuckDB's TopN operator, which reads the full scan output before selecting the top `n`, so the `LIMIT` is no longer pushed into the multi-file Parquet scan (the corruption site).

**Tech Stack:** Rust, buck2, DataFusion/DuckDB serving engines, DuckLake table format, Postgres control plane, `loom_fixture_test` integration tests.

## Global Constraints

- **Spec:** `docs/superpowers/specs/2026-06-21-multi-file-limit-guard-design.md`. Closes the loom-side portion of `iss-multi-file-limit-misread`.
- **Tests are integration targets only** — `rust_test` / `loom_fixture_test` in `BUCK`, never inline `#[cfg(test)]` (the `no-inline-tests` prek hook fails on any `#[test]`/`#[tokio::test]` in `src/**.rs`). Put each test in a sibling `tests/<name>.rs` file wired as its own target.
- **Fixture tests (real Postgres + DuckDB) MUST use `loom_fixture_test`** (`duckdb = True`), never a bare `rust_test`, or they route to remote execution and fail as root.
- **Never pipe `buck2 test` through `tail`/`head`** — redirect to a file and grep: `buck2 test //src/services/query-api/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`.
- **The order key MUST be composed only from columns the read already projects** — a masked column is never an order key (its projected value is the marker `***`, not the real value).
- **The always-present safety `LIMIT` stays** (reads remain capped). Reads become *ordered* on the DuckDB path; existing e2e read tests sort ids (`ids`/`ids_i64`), so they are order-robust — only compiler tests that assert exact SQL strings need updating.
- **The non-DuckDB (DataFusion/Iceberg) serving path is unchanged** — no barrier.
- **The write-side single-file pinning (`estimate_partitions`) stays** — out of scope here.
- Commit messages end with `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`. Conventional Commits enforced by the `conventional-commit` hook.

---

## File Structure

- **Modify `src/services/query-api/src/sql.rs`** — the SQL compiler. Add the `limit_needs_order_barrier()` trait method (defaulted `false`), the `DuckDbDialect` override (`true`), the new `DataFusionDialect` struct, and the two free helpers `order_key_cols` + `order_barrier_limit`. Apply `order_barrier_limit` at the six `LIMIT` emit sites (`compile_select_with`, `compile_chain_with`, `compile_chain_pairs`, `compile_graph_reach`, `compile_graph_reach_union`, `compile_graph_reach_tail`). No compiler function signatures change.
- **Modify `src/services/query-api/src/serving_datafusion.rs`** — override `dialect()` on `DataFusionServingEngine` to return `&crate::sql::DataFusionDialect`.
- **Modify `src/services/query-api/tests/sql_dialect.rs`** — unit tests for the new capability + `DataFusionDialect` rendering. Update any existing exact-SQL assertion that now gains an `ORDER BY`.
- **Modify `src/services/query-api/tests/sql_compile.rs`** — unit tests for the barrier at each compiler. Update existing exact-SQL assertions.
- **Create `src/services/query-api/tests/multi_file_limit_guard.rs`** — the `loom_fixture_test` reproduction → regression test (multi-file table read under `LIMIT` returns the exact source id set).
- **Modify `src/services/query-api/BUCK`** — add the `multi-file-limit-guard` `loom_fixture_test` target.
- **Modify `docs/ISSUES.md`** — close `iss-multi-file-limit-misread` (scoped to the loom-side guard).
- **Modify `docs/FUTURE.md`** — add the upstream-repro / version-bump-removal follow-on `fut-` item.

### Design reference — the six `LIMIT` emit sites (all in `sql.rs`)

| Compiler | Projection alias | Identity available? | Order key |
|---|---|---|---|
| `compile_select_with` (≈L413) | unqualified | not a param | `order_key_cols(None, allowed_cols, mask_cols)`, quoted unqualified |
| `compile_chain_with` (≈L571) | `t_{k}` | not a param | `order_key_cols(None, allowed_cols, mask_cols)`, qualified `t_{k}.` |
| `compile_chain_pairs` (≈L599) | `t_0` / `t_{k}` | `source_id`/`target_id` params | `[t_0.<source_id>, t_{k}.<target_id>]` |
| `compile_graph_reach` (≈L825) | `p` | `identity` param | `order_key_cols(Some(identity), allowed_cols, mask_cols)`, qualified `p.` |
| `compile_graph_reach_union` (≈L943) | `p` | `identity` param | `order_key_cols(Some(identity), allowed_cols, mask_cols)`, qualified `p.` |
| `compile_graph_reach_tail` (≈L1087) | `t_{k}` (tail target) | `identity` is the *core* type's, not the tail target's | `order_key_cols(None, allowed_cols, mask_cols)`, qualified `t_{k}.` |

The order key is always a subset of the projected visible columns, so it is always present in the `SELECT` list — compatible with the `SELECT DISTINCT` used by the chain/graph compilers.

---

## Task 1: Dialect order-barrier capability + `DataFusionDialect`

**Files:**
- Modify: `src/services/query-api/src/sql.rs:17-45` (trait + `DuckDbDialect`; add `DataFusionDialect` after L45)
- Modify: `src/services/query-api/src/serving_datafusion.rs:63-64` (override `dialect()`)
- Test: `src/services/query-api/tests/sql_dialect.rs`

**Interfaces:**
- Produces: `SqlDialect::limit_needs_order_barrier(&self) -> bool` (default `false`); `DuckDbDialect` returns `true`; `pub struct DataFusionDialect` impl `SqlDialect` (renders like DuckDB, barrier `false`). `DataFusionServingEngine::dialect()` returns `&DataFusionDialect`.
- Consumes: nothing from earlier tasks.

This task only adds the capability and the dialect; it does NOT yet change any emitted read SQL, so every existing test stays green.

- [ ] **Step 1: Write the failing unit test**

Add to `src/services/query-api/tests/sql_dialect.rs` (the file already `use`s `query_api::sql::{...}`; add `DataFusionDialect` to that import and `SqlDialect` if not present):

```rust
#[test]
fn duckdb_dialect_requests_order_barrier() {
    use query_api::sql::{DuckDbDialect, SqlDialect};
    assert!(DuckDbDialect.limit_needs_order_barrier());
}

#[test]
fn datafusion_dialect_keeps_bare_limit_and_renders_like_duckdb() {
    use query_api::sql::{DataFusionDialect, DuckDbDialect, SqlDialect};
    let df = DataFusionDialect;
    let duck = DuckDbDialect;
    // No barrier on the DataFusion path (it has no multi-file LIMIT bug).
    assert!(!df.limit_needs_order_barrier());
    // Identical rendering to DuckDB: the compiled SQL is valid for both engines.
    assert_eq!(df.quote_ident("a\"b"), duck.quote_ident("a\"b"));
    assert_eq!(df.placeholder(3), duck.placeholder(3));
    assert_eq!(df.limit_clause(1000), duck.limit_clause(1000));
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test //src/services/query-api:sql-dialect > /tmp/t.log 2>&1; grep -E "error\[|cannot find|Tests finished|FAIL" /tmp/t.log`
Expected: compile error — `no method named limit_needs_order_barrier` and `cannot find ... DataFusionDialect`.

- [ ] **Step 3: Add the trait method (defaulted false) and the `DuckDbDialect` override**

In `src/services/query-api/src/sql.rs`, inside `pub trait SqlDialect` (after `limit_clause`, before the closing `}` at L25), add:

```rust
    /// Whether this dialect needs a stable `ORDER BY` barrier before a pushed-down
    /// `LIMIT` to avoid the multi-file Parquet `LIMIT` corruption (an upstream
    /// DuckDB/DuckLake bug — `iss-multi-file-limit-misread`). Defaulted `false`;
    /// only `DuckDbDialect` opts in. An engine without the bug keeps the bare `LIMIT`.
    fn limit_needs_order_barrier(&self) -> bool {
        false
    }
```

In `impl SqlDialect for DuckDbDialect` (after `limit_clause`, before its closing `}` at L45), add:

```rust
    fn limit_needs_order_barrier(&self) -> bool {
        true
    }
```

- [ ] **Step 4: Add the `DataFusionDialect` struct**

In `src/services/query-api/src/sql.rs`, immediately after the `DuckDbDialect` impl block (after L45), add:

```rust
/// The DataFusion serving dialect. Renders identifiers, placeholders, and the
/// `LIMIT` clause exactly like `DuckDbDialect` (the compiled SQL is valid for both
/// engines), but does NOT request the `LIMIT` order barrier: DataFusion has no
/// multi-file `LIMIT` corruption bug, so it keeps the bare `LIMIT`
/// (`iss-multi-file-limit-misread`).
pub struct DataFusionDialect;

impl SqlDialect for DataFusionDialect {
    fn quote_ident(&self, id: &str) -> String {
        DuckDbDialect.quote_ident(id)
    }
    fn placeholder(&self, one_based: usize) -> String {
        DuckDbDialect.placeholder(one_based)
    }
    fn limit_clause(&self, limit: u32) -> String {
        DuckDbDialect.limit_clause(limit)
    }
    // limit_needs_order_barrier(): inherits the trait default (false).
}
```

- [ ] **Step 5: Override `dialect()` on the DataFusion serving engine**

In `src/services/query-api/src/serving_datafusion.rs`, replace the two comment lines at L63-64:

```rust
    // dialect(): inherit the trait default (DuckDbDialect). The compiled SQL it
    // produces is valid DataFusion SQL, so no override is needed.
```

with:

```rust
    fn dialect(&self) -> &'static dyn SqlDialect {
        // DataFusion has no multi-file `LIMIT` corruption bug, so it keeps the bare
        // `LIMIT` (no order barrier). Rendering is otherwise DuckDB-identical.
        &crate::sql::DataFusionDialect
    }
```

If `SqlDialect` is not already in scope in `serving_datafusion.rs`, the return type `&'static dyn SqlDialect` needs it imported — check the file's `use` block; `serving.rs` re-exports it via `crate::sql`, so add `use crate::sql::SqlDialect;` if the build reports it unresolved.

- [ ] **Step 6: Run the test to verify it passes**

Run: `buck2 test //src/services/query-api:sql-dialect > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS.

- [ ] **Step 7: Build the crate to confirm the `serving_datafusion.rs` override compiles**

Run: `buck2 build //src/services/query-api:query-api > /tmp/b.log 2>&1; grep -E "error|BUILD SUCCEEDED|Build ID" /tmp/b.log; echo done`
Expected: builds clean (no errors).

- [ ] **Step 8: Commit**

```bash
git add src/services/query-api/src/sql.rs src/services/query-api/src/serving_datafusion.rs src/services/query-api/tests/sql_dialect.rs
git commit -m "feat(query-api): add limit_needs_order_barrier dialect capability + DataFusionDialect

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: Order-key helpers + barrier on `compile_select_with`

**Files:**
- Modify: `src/services/query-api/src/sql.rs` (add helpers before `compile_select_with` ≈L375; change the `LIMIT` emit at ≈L413)
- Test: `src/services/query-api/tests/sql_compile.rs`, `src/services/query-api/tests/sql_dialect.rs`

**Interfaces:**
- Consumes: `SqlDialect::limit_needs_order_barrier` (Task 1).
- Produces: `fn order_key_cols(identity: Option<&str>, allowed_cols: &[String], mask_cols: &[String]) -> Vec<String>` (module-private) and `fn order_barrier_limit(dialect: &dyn SqlDialect, order_cols: &[String], limit: u32) -> String` (module-private). `compile_select_with` now emits `ORDER BY <quoted visible cols> LIMIT n` on a barrier dialect.

- [ ] **Step 1: Write the failing unit test**

Add to `src/services/query-api/tests/sql_compile.rs` (uses `query_api::sql::*` and `control_plane_core` types; mirror the existing `compile_select` test style):

```rust
#[test]
fn select_emits_order_barrier_before_limit_on_duckdb() {
    use query_api::sql::compile_select;
    use control_plane_core::TableRef;
    let table = TableRef { schema: "main".into(), name: "t".into() };
    let (sql, _params) = compile_select(
        &table,
        &["id".to_string(), "name".to_string()],
        &[],   // mask_cols
        &[],   // row_filters
        &[],   // predicates
        &[],   // derived
        1000,
    )
    .unwrap();
    // The barrier orders by the projected visible columns, before the LIMIT.
    assert!(
        sql.contains(r#"ORDER BY "id", "name" LIMIT 1000"#),
        "expected ORDER BY barrier before LIMIT, got: {sql}"
    );
}

#[test]
fn select_order_key_excludes_masked_columns() {
    use query_api::sql::compile_select;
    use control_plane_core::TableRef;
    let table = TableRef { schema: "main".into(), name: "t".into() };
    let (sql, _params) = compile_select(
        &table,
        &["id".to_string(), "secret".to_string()],
        &["secret".to_string()], // mask "secret"
        &[],
        &[],
        &[],
        1000,
    )
    .unwrap();
    // Masked column is not an order key; only the visible "id" is.
    assert!(sql.contains(r#"ORDER BY "id" LIMIT 1000"#), "got: {sql}");
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test //src/services/query-api:sql-compile > /tmp/t.log 2>&1; grep -E "panicked|expected ORDER BY|Tests finished|FAIL" /tmp/t.log`
Expected: FAIL — the emitted SQL ends in `... LIMIT 1000` with no `ORDER BY`.

- [ ] **Step 3: Add the two helper functions**

In `src/services/query-api/src/sql.rs`, immediately before `pub fn compile_select_with` (≈L375, after the `select_where_conjuncts` helper), add:

```rust
/// The order-key column NAMES for the stable `ORDER BY` barrier: the `identity`
/// alone when it is a visible (projected, unmasked) column; otherwise every visible
/// projected column. A masked column is never an order key (its projected value is
/// the `MASK_MARKER`, not the real value). Returns raw (unquoted, unqualified) names;
/// the caller qualifies + quotes them with its projection alias. An empty result
/// (everything masked) makes the caller emit a bare `LIMIT`.
fn order_key_cols(
    identity: Option<&str>,
    allowed_cols: &[String],
    mask_cols: &[String],
) -> Vec<String> {
    let visible: Vec<String> = allowed_cols
        .iter()
        .filter(|c| !mask_cols.iter().any(|m| m == *c))
        .cloned()
        .collect();
    match identity {
        Some(id) if visible.iter().any(|c| c == id) => vec![id.to_string()],
        _ => visible,
    }
}

/// The trailing clause after the WHERE. On a dialect that needs the multi-file
/// `LIMIT` order barrier (DuckDB), emit `ORDER BY <order_cols> LIMIT n` — which
/// compiles to DuckDB's TopN operator, so the `LIMIT` is NOT pushed into the
/// multi-file Parquet scan (the corruption site, `iss-multi-file-limit-misread`).
/// On any other dialect, or when there is no usable order key, a bare `LIMIT n`.
/// `order_cols` are already alias-qualified and quoted by the caller.
fn order_barrier_limit(dialect: &dyn SqlDialect, order_cols: &[String], limit: u32) -> String {
    if dialect.limit_needs_order_barrier() && !order_cols.is_empty() {
        format!(
            "ORDER BY {} {}",
            order_cols.join(", "),
            dialect.limit_clause(limit)
        )
    } else {
        dialect.limit_clause(limit)
    }
}
```

- [ ] **Step 4: Wire the barrier into `compile_select_with`**

In `compile_select_with`, find the final `LIMIT` emit (≈L413):

```rust
    sql.push_str(&format!(" {}", dialect.limit_clause(limit)));
```

Replace it with:

```rust
    let order_cols: Vec<String> = order_key_cols(None, allowed_cols, mask_cols)
        .iter()
        .map(|c| dialect.quote_ident(c))
        .collect();
    sql.push_str(&format!(" {}", order_barrier_limit(dialect, &order_cols, limit)));
```

(The object-read projection is unqualified, so the order key is quoted but not alias-qualified. Identity preference is exercised by the graph compilers in Task 4; object reads order by their visible projected columns, the documented fallback.)

- [ ] **Step 5: Run the new test to verify it passes**

Run: `buck2 test //src/services/query-api:sql-compile > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: the two new tests PASS. **Other tests in this target may now FAIL** because they assert exact `compile_select` SQL strings that gained an `ORDER BY` — that is expected and fixed in the next step.

- [ ] **Step 6: Update existing exact-SQL assertions broken by the new `ORDER BY`**

Run the target and read the failures: `buck2 test //src/services/query-api:sql-compile //src/services/query-api:sql-dialect > /tmp/t.log 2>&1; grep -E "assertion|panicked|FAIL|left:|right:" /tmp/t.log`

For every failing assertion that compares an exact `compile_select`/`compile_select_with`-produced SQL string, update the expected string to include the emitted ` ORDER BY <quoted visible cols>` immediately before ` LIMIT n` (DuckDb dialect now barriers; a `BacktickDialect`/`DataFusionDialect` case stays bare). Do not weaken set-based assertions. Re-run until green.

Run: `buck2 test //src/services/query-api:sql-compile //src/services/query-api:sql-dialect > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: both targets PASS.

- [ ] **Step 7: Commit**

```bash
git add src/services/query-api/src/sql.rs src/services/query-api/tests/sql_compile.rs src/services/query-api/tests/sql_dialect.rs
git commit -m "feat(query-api): ORDER BY barrier before LIMIT in compile_select_with

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: Barrier on `compile_chain_with` and `compile_chain_pairs`

**Files:**
- Modify: `src/services/query-api/src/sql.rs` (`compile_chain_with` ≈L571; `compile_chain_pairs` ≈L599)
- Test: `src/services/query-api/tests/sql_compile.rs`, `src/services/query-api/tests/compile_chain_pairs.rs`

**Interfaces:**
- Consumes: `order_key_cols`, `order_barrier_limit` (Task 2).
- Produces: chain and association-pair reads emit the `ORDER BY` barrier on DuckDB.

- [ ] **Step 1: Write the failing unit tests**

Add to `src/services/query-api/tests/sql_compile.rs` (reuse the existing chain test scaffolding for `types`/`hops`; the snippet below assumes a 1-hop FK chain over `t_0`→`t_1` projecting `["id","sku"]` — mirror the nearest existing `compile_chain` test for the exact `ChainType`/`LinkBacking` construction):

```rust
#[test]
fn chain_emits_order_barrier_before_limit_on_duckdb() {
    use query_api::sql::compile_chain;
    // Build a minimal 1-hop FK chain (copy the construction from an existing
    // compile_chain test in this file: two ChainType entries, one LinkBacking::ForeignKey).
    let (types, hops) = sample_one_hop_chain(); // local helper from the existing tests
    let (sql, _params) =
        compile_chain(&types, &hops, &["id".to_string(), "sku".to_string()], &[], 1000).unwrap();
    // Chain projects the final target alias t_1; order key is its visible cols.
    assert!(
        sql.contains(r#"ORDER BY t_1."id", t_1."sku" LIMIT 1000"#),
        "got: {sql}"
    );
}
```

Add to `src/services/query-api/tests/compile_chain_pairs.rs` (mirror its existing call style — `compile_chain_pairs(&DuckDbDialect, &types, &hops, source_id, target_id, limit)`):

```rust
#[test]
fn chain_pairs_orders_by_both_identity_columns_before_limit() {
    use query_api::sql::{compile_chain_pairs, DuckDbDialect};
    let (types, hops) = sample_one_hop_chain(); // local helper as used by the existing tests
    let (sql, _params) =
        compile_chain_pairs(&DuckDbDialect, &types, &hops, "cust_id", "ord_id", 1000).unwrap();
    assert!(
        sql.contains(r#"ORDER BY t_0."cust_id", t_1."ord_id" LIMIT 1000"#),
        "got: {sql}"
    );
}
```

If a `sample_one_hop_chain()` helper does not already exist in the test file, inline the `types`/`hops` construction directly (copy it verbatim from the nearest existing chain test in the same file — do not abstract).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `buck2 test //src/services/query-api:sql-compile //src/services/query-api:compile-chain-pairs > /tmp/t.log 2>&1; grep -E "panicked|got:|Tests finished|FAIL" /tmp/t.log`
Expected: FAIL — emitted SQL ends in `... LIMIT 1000` with no `ORDER BY`.

- [ ] **Step 3: Wire the barrier into `compile_chain_with`**

In `compile_chain_with`, the final emit is (≈L571):

```rust
    sql.push_str(&format!(" {}", dialect.limit_clause(limit)));
```

`final_alias` (= `format!("t_{k}")`) is already in scope. Replace the emit with:

```rust
    let order_cols: Vec<String> = order_key_cols(None, allowed_cols, mask_cols)
        .iter()
        .map(|c| format!("{final_alias}.{}", dialect.quote_ident(c)))
        .collect();
    sql.push_str(&format!(" {}", order_barrier_limit(dialect, &order_cols, limit)));
```

- [ ] **Step 4: Wire the barrier into `compile_chain_pairs`**

In `compile_chain_pairs`, the final emit is (≈L599):

```rust
    sql.push_str(&format!(" {}", dialect.limit_clause(limit)));
```

`k` (= `hops.len()`), `source_id`, `target_id` are in scope. Replace the emit with:

```rust
    let order_cols = vec![
        format!("t_0.{}", dialect.quote_ident(source_id)),
        format!("t_{k}.{}", dialect.quote_ident(target_id)),
    ];
    sql.push_str(&format!(" {}", order_barrier_limit(dialect, &order_cols, limit)));
```

(Both projected identity columns are always visible, so the pair is always a valid order key.)

- [ ] **Step 5: Run the tests; update broken exact-SQL assertions**

Run: `buck2 test //src/services/query-api:sql-compile //src/services/query-api:compile-chain-pairs > /tmp/t.log 2>&1; grep -E "assertion|left:|right:|Tests finished|FAIL" /tmp/t.log`

Update every existing exact-SQL assertion for `compile_chain`/`compile_chain_with`/`compile_chain_pairs` to include the new ` ORDER BY ...` before ` LIMIT n`. Re-run until both targets PASS.

- [ ] **Step 6: Commit**

```bash
git add src/services/query-api/src/sql.rs src/services/query-api/tests/sql_compile.rs src/services/query-api/tests/compile_chain_pairs.rs
git commit -m "feat(query-api): ORDER BY barrier before LIMIT in chain + association-pair compilers

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 4: Barrier on the three graph-reach compilers

**Files:**
- Modify: `src/services/query-api/src/sql.rs` (`compile_graph_reach` ≈L818-827; `compile_graph_reach_union` ≈L936-945; `compile_graph_reach_tail` ≈L1085-1089)
- Test: `src/services/query-api/tests/graph_reach.rs`, `src/services/query-api/tests/graph_reach_union.rs`, `src/services/query-api/tests/graph_reach_tail.rs`

**Interfaces:**
- Consumes: `order_key_cols`, `order_barrier_limit` (Task 2).
- Produces: all three reach reads emit the `ORDER BY` barrier on DuckDB; `compile_graph_reach`/`_union` prefer the (visible) `identity`, exercising the identity-present and identity-absent (masked → projected-columns) key selection the spec requires.

- [ ] **Step 1: Write the failing unit tests**

Add to `src/services/query-api/tests/graph_reach.rs` (mirror its existing `compile_graph_reach(&DuckDbDialect, table, identity, path, seed, row_filters, allowed, mask, depth, limit)` call style):

```rust
#[test]
fn graph_reach_orders_by_identity_when_visible() {
    // Reuse the existing single-self-link fixture builder in this file for `table`,
    // `path`, etc. Project ["id","label"]; identity = "id" (visible).
    let (table, path) = sample_self_link(); // local construction as in existing tests
    let (sql, _params) = query_api::sql::compile_graph_reach(
        &query_api::sql::DuckDbDialect,
        &table,
        "id",
        &path,
        &[],
        &[],
        &["id".to_string(), "label".to_string()],
        &[],
        3,
        1000,
    )
    .unwrap();
    // Identity is visible -> order key is identity alone, qualified at the projection alias `p`.
    assert!(sql.contains(r#"ORDER BY p."id" LIMIT 1000"#), "got: {sql}");
}

#[test]
fn graph_reach_orders_by_projected_cols_when_identity_masked() {
    let (table, path) = sample_self_link();
    let (sql, _params) = query_api::sql::compile_graph_reach(
        &query_api::sql::DuckDbDialect,
        &table,
        "id",
        &path,
        &[],
        &[],
        &["id".to_string(), "label".to_string()],
        &["id".to_string()], // identity masked -> falls back to visible projected cols
        3,
        1000,
    )
    .unwrap();
    assert!(sql.contains(r#"ORDER BY p."label" LIMIT 1000"#), "got: {sql}");
}
```

Add one barrier-presence assertion each to `graph_reach_union.rs` (order key `p."id"`) and `graph_reach_tail.rs` (order key qualified at the tail's final alias `t_{k}`, i.e. the projected tail-target columns — use the existing tail fixture's projection). Reuse each file's existing fixture builders; inline construction if no shared helper exists.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `buck2 test //src/services/query-api:graph-reach //src/services/query-api:graph-reach-union //src/services/query-api:graph-reach-tail > /tmp/t.log 2>&1; grep -E "panicked|got:|Tests finished|FAIL" /tmp/t.log`
Expected: FAIL — no `ORDER BY` before `LIMIT`.

(Target names follow the file names: confirm the exact target names with `grep -nE "name = \"graph-reach" src/services/query-api/BUCK`.)

- [ ] **Step 3: Wire the barrier into `compile_graph_reach`**

In `compile_graph_reach`, the `sql` is built by a single `format!` ending in ` {}` bound to `dialect.limit_clause(limit)` (≈L818-826). `cols` is `masked_col_exprs(dialect, allowed_cols, mask_cols, "p.")`; the raw `identity` param and `allowed_cols`/`mask_cols` are in scope. Just before that `format!`, add:

```rust
    let order_cols: Vec<String> = order_key_cols(Some(identity), allowed_cols, mask_cols)
        .iter()
        .map(|c| format!("p.{}", q(c)))
        .collect();
    let limit_clause = order_barrier_limit(dialect, &order_cols, limit);
```

Then change the trailing `{}` binding in the `format!` from `dialect.limit_clause(limit)` to `limit_clause`. The projection line becomes `... FROM {tbl} p WHERE {proj_where} {limit_clause}`.

- [ ] **Step 4: Wire the barrier into `compile_graph_reach_union`**

Same shape (≈L936-944): `cols` is projected at `p.`; `identity` and `allowed_cols`/`mask_cols` are in scope; `q` is the local quoting closure. Before the final `format!`, add:

```rust
    let order_cols: Vec<String> = order_key_cols(Some(identity), allowed_cols, mask_cols)
        .iter()
        .map(|c| format!("p.{}", q(c)))
        .collect();
    let limit_clause = order_barrier_limit(dialect, &order_cols, limit);
```

and bind the trailing `{}` to `limit_clause` instead of `dialect.limit_clause(limit)`.

- [ ] **Step 5: Wire the barrier into `compile_graph_reach_tail`**

The tail projects the tail-target alias `final_alias` (= `format!("t_{k}")`, ≈L1065). The `identity` param here is the *core* type's identity (not the tail target's), so pass `None` — the order key is the visible projected tail columns. Before the final `format!` (≈L1085), add:

```rust
    let order_cols: Vec<String> = order_key_cols(None, allowed_cols, mask_cols)
        .iter()
        .map(|c| format!("{final_alias}.{}", q(c)))
        .collect();
    let limit_clause = order_barrier_limit(dialect, &order_cols, limit);
```

and bind the trailing `{}` to `limit_clause` instead of `dialect.limit_clause(limit)`.

- [ ] **Step 6: Run the tests; update broken exact-SQL assertions**

Run: `buck2 test //src/services/query-api:graph-reach //src/services/query-api:graph-reach-union //src/services/query-api:graph-reach-tail > /tmp/t.log 2>&1; grep -E "assertion|left:|right:|Tests finished|FAIL" /tmp/t.log`

Update every existing exact-SQL assertion in these three files (and any in `sql_compile.rs` that compiled a graph reach) to include the ` ORDER BY ...` before ` LIMIT n`. Re-run until all three PASS.

- [ ] **Step 7: Run the full query-api unit/compiler sweep to catch any remaining string assertions**

Run: `buck2 test //src/services/query-api:sql-compile //src/services/query-api:sql-dialect //src/services/query-api:graph-reach //src/services/query-api:graph-reach-union //src/services/query-api:graph-reach-tail //src/services/query-api:compile-chain-pairs > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: all PASS.

- [ ] **Step 8: Commit**

```bash
git add src/services/query-api/src/sql.rs src/services/query-api/tests/graph_reach.rs src/services/query-api/tests/graph_reach_union.rs src/services/query-api/tests/graph_reach_tail.rs src/services/query-api/tests/sql_compile.rs
git commit -m "feat(query-api): ORDER BY barrier before LIMIT in graph-reach compilers

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 5: Multi-file serving reproduction → regression (`loom_fixture_test`)

**Files:**
- Create: `src/services/query-api/tests/multi_file_limit_guard.rs`
- Modify: `src/services/query-api/BUCK` (add the `multi-file-limit-guard` target)

**Interfaces:**
- Consumes: the guard from Tasks 1-4; the `e2e_support` helpers `land`, `get`, `ids_i64`, and the seed/setup helpers (`tref`, `prop`) — see `src/services/query-api/tests/e2e_support.rs`.
- Produces: a durable regression test proving a multi-file table read under the standard `LIMIT` returns the exact source id set.

This is the spec's *foundation* test. Step 1 confirms the guard does real work (reproduces the corruption, or documents the known non-determinism); Steps 4-5 are the durable regression.

- [ ] **Step 1: Write the regression test (asserts correctness WITH the guard)**

Create `src/services/query-api/tests/multi_file_limit_guard.rs`. Model the seed/read plumbing on an existing `e2e_support`-backed fixture test (e.g. `governed_read.rs` / `object_set_e2e.rs`). The test must:

1. Boot the fixture (`PgFixture` via `e2e_support`), seed ONE object type `widget` over a DuckLake table `main.widget` with an integer identity `id` and at least ~2000 rows with distinct, known `id` values (e.g. `1..=2000`).
2. **Force a genuinely multi-file table.** Land the rows in two appends to the SAME `TableRef` (`land(...)` twice, with disjoint `id` ranges `1..=1000` and `1001..=2000`), which produces ≥2 Parquet data files. **Assert the multi-file precondition** before reading, via the catalog:

```rust
use control_plane_core::Catalog;
let snap = cp.head_snapshot(&table).await.unwrap(); // confirm the exact Catalog API name in core
let files = cp.files(&table, snap).await.unwrap();
assert!(files.len() > 1, "test setup: expected a multi-file table, got {} file(s)", files.len());
```

   (Confirm the exact `Catalog` method names — `head_snapshot`/`current_snapshot` and `files` — against `src/control-plane/core`. If two appends do not split into ≥2 files, fall back to landing with a small `target_file_size_bytes` write config as `datafusion-io/tests/single_file_write.rs` does, or land in more appends; the goal is `files.len() > 1`.)

3. Grant the subject read on `widget`, then issue the governed object read through `EmbeddedDuckDb`:

```rust
let (status, body) = get(cp.clone(), eng.clone(), "/objects/widget", "alice").await;
assert_eq!(status, StatusCode::OK);
let got = ids_i64(&body); // sorted
```

4. Assert the returned ids are an uncorrupted subset of the source range (the read caps at `DEFAULT_LIMIT = 1000`, so assert the COUNT equals the cap AND every returned id is in `1..=2000` with no corruption — the corruption signature was a value jumping out of range, e.g. `10 -> 266`):

```rust
assert_eq!(got.len(), 1000, "read should return exactly DEFAULT_LIMIT rows");
for id in &got {
    assert!((1..=2000).contains(id), "corrupted id outside source range: {id}");
}
// No duplicates (corruption can collide values).
let mut dedup = got.clone();
dedup.dedup();
assert_eq!(dedup.len(), got.len(), "duplicate/corrupted ids in result: {got:?}");
```

- [ ] **Step 2: Add the BUCK target**

In `src/services/query-api/BUCK`, mirror the `governed-read` / `multi-hop-traversal-e2e` `loom_fixture_test` targets:

```python
loom_fixture_test(
    name = "multi-file-limit-guard",
    crate = "multi_file_limit_guard",
    srcs = ["tests/multi_file_limit_guard.rs"],
    crate_root = "tests/multi_file_limit_guard.rs",
    duckdb = True,
    deps = [
        ":query-api",
        ":e2e-support",
        "//src/services/ingest:ingest",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:arrow",
        "//third-party:object_store",
        "//third-party:serde_json",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```

(Trim/extend `deps` to exactly what the test file `use`s — copy the dep set from the closest existing `e2e-support`-backed target and drop anything unused to keep clippy/unused-deps quiet.)

- [ ] **Step 3: Confirm the guard does real work (reproduction)**

Temporarily make `DuckDbDialect::limit_needs_order_barrier()` return `false` (or comment the `order_barrier_limit` call in `compile_select_with`), then run the new test:

Run: `buck2 test //src/services/query-api:multi-file-limit-guard > /tmp/t.log 2>&1; grep -E "corrupted id|duplicate|assertion|Tests finished|FAIL" /tmp/t.log`

Record the observation in a top-of-file comment in `multi_file_limit_guard.rs`:
- If it FAILS without the guard → the corruption reproduces; note the observed corrupted value(s).
- If it PASSES without the guard → the corruption is non-deterministic in this environment (the spec's known risk). Document the manual repro steps and that the durable assertion is correctness *with* the guard.

**Then restore the guard** (revert the temporary `false`/comment).

- [ ] **Step 4: Run the test with the guard restored to verify it passes**

Run: `buck2 test //src/services/query-api:multi-file-limit-guard > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/services/query-api/tests/multi_file_limit_guard.rs src/services/query-api/BUCK
git commit -m "test(query-api): multi-file DuckLake LIMIT read returns uncorrupted ids

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 6: Close the issue + mint the upstream follow-on

**Files:**
- Modify: `docs/ISSUES.md` (close `iss-multi-file-limit-misread`)
- Modify: `docs/FUTURE.md` (add the upstream-repro / version-bump-removal follow-on)

**Interfaces:**
- Consumes: nothing (documentation only).
- Produces: register state reflecting the shipped guard. (This is the register half of loom-work-checkout step 4; `loom-docs-update` may also be run at finish — keep this task's edits consistent with that.)

- [ ] **Step 1: Close `iss-multi-file-limit-misread` in `docs/ISSUES.md`**

Change the entry's checkbox to `[x]`, set `status:fixed`, and add `pr:#<N>` (the PR number, filled at finish). Replace the tag line + prose (≈L16-17) with:

```markdown
- [x] **Multi-file DuckLake LIMIT mis-read** `{#iss-multi-file-limit-misread area:query status:fixed from:cross-cutting pr:#<N> spec:2026-06-21-multi-file-limit-guard-design}`
  Fixed (PR #<N>): governed reads on the DuckDB serving engine now emit a stable `ORDER BY <key>` barrier before the pushed-down `LIMIT` (at every compiler `LIMIT` site), which compiles to DuckDB's TopN and stops the `LIMIT` being pushed into the multi-file Parquet scan — the corruption site. The order key is the queried type's `identity` when visible, else its projected (unmasked) columns. Gated by a new `SqlDialect::limit_needs_order_barrier()` (`true` for `DuckDbDialect`); the new `DataFusionDialect` keeps the bare `LIMIT` (no such bug). A multi-file `loom_fixture_test` (`multi-file-limit-guard`) asserts the read returns uncorrupted ids. Upstream DuckDB repro + the workaround's removal are tracked in [[fut-multi-file-limit-upstream]].
```

- [ ] **Step 2: Add the follow-on item to `docs/FUTURE.md`**

Append a new item under the appropriate area heading (`query` or `devx` — match the file's existing grouping):

```markdown
- [ ] **Upstream DuckDB multi-file LIMIT fix + workaround removal** `{#fut-multi-file-limit-upstream area:query status:deferred from:multi-file-limit-guard pr:- spec:2026-06-21-multi-file-limit-guard-design}`
  loom works around an upstream DuckDB/DuckLake bug — a pushed-down `LIMIT` over a multi-file Parquet scan corrupts column values — with a serving-side `ORDER BY` barrier ([[iss-multi-file-limit-misread]]). File the upstream bug with a minimal multi-file + `LIMIT` reproduction; when loom's pinned `duckdb` crate (currently `1.10503.1`, bundled) is bumped to a fixed version, remove the barrier and its `SqlDialect::limit_needs_order_barrier()` gate. A DuckDB `SET`/`PRAGMA` disabling the offending optimization, if found, is an acceptable alternative removal path.
```

- [ ] **Step 3: Validate the registers**

Run: `bash tools/docs.sh validate > /tmp/v.log 2>&1; cat /tmp/v.log`
Expected: no errors (ids unique, vocab valid, `[[links]]` resolve, `spec:` slug resolves on disk).

- [ ] **Step 4: Commit**

```bash
git add docs/ISSUES.md docs/FUTURE.md
git commit -m "docs(issues): close iss-multi-file-limit-misread + mint fut-multi-file-limit-upstream

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

(The `pr:#<N>` number is filled in at finish, once the PR is opened — `loom-docs-update` handles this in the loom-work-checkout finishing step.)

---

## Final Verification (after all tasks)

- [ ] **Run the full query-api test sweep** (compiler units + all fixture e2e reads, to confirm no read path regressed and the order change broke nothing):

Run: `buck2 test //src/services/query-api/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: all PASS. e2e read tests sort ids (`ids`/`ids_i64`), so the new ordering does not break them; if any test asserted an unsorted order, update it to a set/sorted comparison.

- [ ] **Run clippy on the changed crate** (the prek `clippy` hook gate):

Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c.log 2>&1; cat $(buck2 build --show-output '//src/services/query-api:query-api[clippy.txt]' 2>/dev/null | awk '{print $2}') 2>/dev/null; echo "clippy done"`
Expected: empty clippy output (clean).

- [ ] **Run rustfmt** on the changed files: `buck2 run //tools:rustfmt -- --check src/services/query-api/src/sql.rs src/services/query-api/src/serving_datafusion.rs`. Reformat if it reports diffs.

---

## Self-Review

**1. Spec coverage:**
- "Goal: correct values under standard `LIMIT` on DuckDB" → Tasks 2-5 (barrier + fixture regression). ✅
- "Exposure: every `LIMIT` site, no `ORDER BY`" → barrier applied at all six sites (Tasks 2-4). ✅
- "Reproduction first (the foundation)" → Task 5 (multi-file `loom_fixture_test`, repro confirmation in Step 3, durable correctness assertion). ✅
- "The guard — stable `ORDER BY` barrier; TopN; order key = identity-when-visible else projected columns; masked column never an order key" → `order_key_cols` (Task 2). ✅
- "DuckDB-only, dialect-gated; `limit_needs_order_barrier()` true for DuckDb, false for DataFusion/Iceberg" → Task 1 (capability + `DataFusionDialect` + engine override). ✅
- "Applied at every `LIMIT` site, incl. the three graph reaches ordering the outer projection over the reachable set" → Task 4 (`p.`/`t_k.` qualified). ✅
- "Fallback (materialization barrier) if `ORDER BY` doesn't fully fix" → Task 5 Step 3 documents the reproduction outcome; if the barrier is proven insufficient, the implementer escalates to the documented fallback before closing (noted in the task).
- "What this does NOT change: safety LIMIT stays; write-side pinning stays; non-DuckDB path unchanged; reads become ordered (update order-assuming tests)" → Global Constraints + Tasks 2-4 update only exact-SQL assertions; Final Verification handles e2e. ✅
- "Testing: multi-file repro→regression; compiler unit test (ORDER BY for DuckDb, bare for non-barrier dialect; identity-present + identity-absent key selection); single_file_write tests stay green" → Tasks 1-5 (identity-present/absent in Task 4; bare-LIMIT dialect in Task 1/2; `single_file_write` untouched). ✅
- "Out of scope → follow-on `fut-` item (upstream repro + version-bump removal)" → Task 6. ✅
- "Files: sql.rs, possibly handler.rs, new fixture + compiler tests + BUCK, ISSUES.md close + follow-on" → note: handler.rs is NOT modified (identity/projection are all already in scope inside the compilers, so no order key is threaded from the handler — a simplification over the spec's "possibly modify handler.rs", which the spec explicitly hedged as conditional). ✅

**2. Placeholder scan:** No "TBD"/"handle edge cases"/"similar to Task N". The `<N>` PR-number placeholder in Task 6 is intentional (filled at finish). The `sample_one_hop_chain()`/`sample_self_link()` references are explicitly "reuse or inline the existing fixture construction" with instructions to copy verbatim from the neighboring tests — not hidden logic.

**3. Type consistency:** `order_key_cols(identity: Option<&str>, allowed_cols: &[String], mask_cols: &[String]) -> Vec<String>` and `order_barrier_limit(dialect: &dyn SqlDialect, order_cols: &[String], limit: u32) -> String` are used with consistent signatures across Tasks 2-4. `limit_needs_order_barrier(&self) -> bool` consistent in Task 1. `DataFusionDialect` (pub) referenced as `crate::sql::DataFusionDialect` in `serving_datafusion.rs` and `query_api::sql::DataFusionDialect` in tests. No signature changes to any `compile_*` function, so no call-site ripple beyond exact-SQL assertion updates.
