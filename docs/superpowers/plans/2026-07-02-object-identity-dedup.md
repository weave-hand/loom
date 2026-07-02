# Object-identity dedup for many-to-many traversal — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Dedup many-to-many traversal (`read_linked_chain`) and the graph reads on the target object's **raw identity value** — below the ACL masking layer — so distinct objects are never collapsed into one row when their identity column is masked or denied.

**Architecture:** The query-api SQL compiler (`src/services/query-api/src/sql.rs`) currently deduplicates the final-target set with `SELECT DISTINCT` over the *visible* (masked) projection. When ACL masks the target's identity column, distinct objects (different PKs, same visible columns) collapse to one row — a cardinality bug. The fix keys the dedup on the raw identity via a windowed `ROW_NUMBER() OVER (PARTITION BY t_k.<identity>)` subquery whose outer select keeps exactly today's visible projection; the raw identity is referenced only in `PARTITION BY` and never surfaces to the caller. The two single-table reachability compilers (which are already PK-unique via `p.id IN (SELECT id FROM reach)`) simply drop their now-redundant `DISTINCT`.

**Tech Stack:** Rust, buck2, DataFusion (sole serving engine; query-api ships SQL over Flight SQL). Tests are `rust_test` integration targets only (never inline `#[cfg(test)]`).

## Global Constraints

- **Tests are `rust_test` integration targets only** — one `tests/<name>.rs` file per target, wired in the crate `BUCK`. The `no-inline-tests` prek hook fails on any `#[test]` in `src/**.rs`. (verbatim from spec Testing section)
- **query-api is a zero-DataFusion wire client.** The compiler emits SQL text; DataFusion (the engine) plans it. The window form is chosen so every non-aggregate column projects verbatim (no `GROUP BY` aggregate wrapping). (spec SQL/query-shape change)
- **No new error variants.** A masked or denied identity is a normal, supported case here — the identity is an internal grouping key, never a projected result. `identity: None` falls back to today's `SELECT DISTINCT` (back-compat). (spec Error handling)
- **Dedup key is never caller-visible.** The raw identity appears only in `PARTITION BY` inside the subquery; the outer select projects only the masked/visible columns. No visibility leak. (spec Correctness §3)
- **Masking semantics unchanged.** Masked columns still render `'***'` (`MASK_MARKER`); denied columns are still omitted. Only the dedup key changes. (spec Non-goals)
- **`read_associations` is untouched.** (spec Non-goals)
- Reuse the shared e2e support library `//src/services/query-api:e2e-support` for the e2e regression; extend it only for genuinely reusable helpers. (spec Testing + CLAUDE.md)
- Run the query-api tests with `buck2 test //src/services/query-api/...`; fixture (e2e) tests route local automatically via `loom_fixture_test`. Don't pipe `buck2 test` through `tail`/`head` — redirect to a file and grep it.

---

## Orientation — the exact code being changed

All line numbers are as of the branch base; re-`grep` before editing.

**`src/services/query-api/src/sql.rs`:**
- `compile_chain_with` (`:579`) — the many-to-many chain compiler. Builds `SELECT DISTINCT {cols} FROM {from} [WHERE ...] LIMIT n`. `cols` is built inline (`:589–599`): a masked col → `'{MASK_MARKER}' AS {quote(c)}`, else `{final_alias}.{quote(c)}` where `final_alias = format!("t_{k}")`, `k = hops.len()`.
- `compile_chain` (`:1145`) — thin convenience wrapper over `compile_chain_with(&DataFusionDialect, …)` used by unit tests.
- `compile_graph_reach` (`:818`, path-cycle) and `compile_graph_reach_union` (`:898`) — single-table reachability. Both end with `SELECT DISTINCT {cols} FROM {tbl} p WHERE {proj_where} {limit}` where `proj_where` starts with `p.{id} IN (SELECT id FROM reach WHERE depth >= 1)` — so `p` rows are already PK-unique; the `DISTINCT` is redundant and is the masked-collapse hazard.
- `compile_graph_reach_tail` (`:1069`) — recursive-core + relational-tail. Ends with `{cte} SELECT DISTINCT {cols} FROM {from} WHERE {where_sql} {limit}`; `cols` built inline (`:1117–1127`), `final_alias = format!("t_{k}")`, `k = tail_hops.len()`. Its existing `identity` parameter is the **core self-link** identity (`table`'s PK), NOT the final target's — a new parameter is needed for the target identity.
- `MASK_MARKER` (`:51`) = `"***"`.

**`src/services/query-api/src/handler.rs`:**
- `read_linked_chain` (`:837`) — calls `compile_chain_with` (`:857`). Holds `target = metas.last()`, so `target.otype.identity.as_deref()` is the final-target identity.
- graph-tail handler — calls `compile_graph_reach_tail` (`:1405`). Holds `final_type` (`:1338`, `:1368`), so `final_type.identity.as_deref()` is the final-target identity.
- `ObjectType.identity: Option<String>` lives in `src/control-plane/core/src/ontology.rs:48`.

**Chosen SQL shapes** (DataFusion dialect, `quote_ident(c)` → `"c"`, `limit_clause(n)` → `LIMIT n`):

Identity `Some("ssn")`, chain to `t_2`, columns `name` (visible), `ssn` (masked), `city` (visible):
```sql
SELECT "name", "ssn", "city" FROM (SELECT t_2."name", '***' AS "ssn", t_2."city", ROW_NUMBER() OVER (PARTITION BY t_2."ssn") AS _loom_rn FROM <from> WHERE <conjuncts>) _dedup WHERE _loom_rn = 1 LIMIT 100
```
- Inner select: today's masked/visible column exprs, **verbatim**, plus the window column.
- `PARTITION BY t_k."<identity>"` references the **raw** identity at the final alias — independent of whether that column is masked/omitted in the projection.
- Outer select: each allowed column referenced by its bare quoted output name (a visible inner col `t_2."name"` has output name `name`; a masked inner col `'***' AS "ssn"` has output name `ssn`). No `ORDER BY` in the window (any representative is correct — spec Correctness §1) and none outside.
- When there are no conjuncts, omit the inner ` WHERE …` (mirrors today's `if !conjuncts.is_empty()`).

Identity `None` ⇒ **byte-identical** to today's `SELECT DISTINCT {cols} FROM {from} [WHERE …] LIMIT n`.

---

## Task 1: Thread identity into `compile_chain_with` + `compile_chain` and emit the windowed dedup

**Files:**
- Modify: `src/services/query-api/src/sql.rs` (`compile_chain_with` `:579`, `compile_chain` `:1145`)
- Modify: `src/services/query-api/tests/sql_compile.rs` (existing `compile_chain(...)` call sites at `:441, :491, :536, :575, :608, :654, :694, :797` — insert the new `None` arg; add new windowed-dedup assertions)

**Interfaces:**
- Produces:
  - `pub fn compile_chain_with(dialect: &dyn SqlDialect, types: &[ChainType], hops: &[LinkBacking], allowed_cols: &[String], mask_cols: &[String], identity: Option<&str>, limit: u32) -> Result<(String, Vec<SqlValue>), CompileError>` — new `identity` param inserted **immediately before `limit`**.
  - `pub fn compile_chain(types: &[ChainType], hops: &[LinkBacking], allowed_cols: &[String], mask_cols: &[String], identity: Option<&str>, limit: u32) -> Result<(String, Vec<SqlValue>), CompileError>` — same new param, same position; forwards to `compile_chain_with`.

- [ ] **Step 1: Write the failing tests** in `src/services/query-api/tests/sql_compile.rs`. First update the 8 existing `compile_chain(...)` call sites to pass `None` before the `limit` argument (mechanical — e.g. `compile_chain(&types, &hops, &["id".to_string()], &[], None, 100)`); the existing `SELECT DISTINCT …` assertions must stay byte-identical (that is the `identity: None` regression guard). Then add three new tests. Use the existing helpers (`tr`, `eqp`) already in the file; the two-hop customer→orders→line_items shape mirrors `chain_params_source_eq_precedes_hop_row_filters_in_chain_order`.

```rust
fn two_hop_types() -> Vec<ChainType> {
    vec![
        ChainType { table: tr("main", "customer"), row_filters: vec![], predicates: vec![] },
        ChainType { table: tr("main", "orders"), row_filters: vec![], predicates: vec![] },
        ChainType { table: tr("main", "person"), row_filters: vec![], predicates: vec![] },
    ]
}
fn two_hop_hops() -> Vec<LinkBacking> {
    vec![
        LinkBacking::ForeignKey { from_column: "id".into(), to_column: "customer_id".into() },
        LinkBacking::ForeignKey { from_column: "id".into(), to_column: "person_id".into() },
    ]
}

#[test]
fn identity_masked_dedups_on_raw_identity_via_window() {
    let (sql, params) = compile_chain(
        &two_hop_types(),
        &two_hop_hops(),
        &["name".to_string(), "ssn".to_string(), "city".to_string()],
        &["ssn".to_string()],
        Some("ssn"),
        100,
    )
    .unwrap();
    assert_eq!(
        sql,
        "SELECT \"name\", \"ssn\", \"city\" FROM (\
           SELECT t_2.\"name\", '***' AS \"ssn\", t_2.\"city\", \
           ROW_NUMBER() OVER (PARTITION BY t_2.\"ssn\") AS _loom_rn \
           FROM \"main\".\"person\" t_2 \
           JOIN \"main\".\"orders\" t_1 ON t_1.\"id\" = t_2.\"person_id\" \
           JOIN \"main\".\"customer\" t_0 ON t_0.\"id\" = t_1.\"customer_id\"\
         ) _dedup WHERE _loom_rn = 1 LIMIT 100"
    );
    assert!(params.is_empty());
}

#[test]
fn identity_visible_uses_same_windowed_shape() {
    let (sql, _params) = compile_chain(
        &two_hop_types(),
        &two_hop_hops(),
        &["ssn".to_string(), "name".to_string()],
        &[],
        Some("ssn"),
        100,
    )
    .unwrap();
    assert_eq!(
        sql,
        "SELECT \"ssn\", \"name\" FROM (\
           SELECT t_2.\"ssn\", t_2.\"name\", \
           ROW_NUMBER() OVER (PARTITION BY t_2.\"ssn\") AS _loom_rn \
           FROM \"main\".\"person\" t_2 \
           JOIN \"main\".\"orders\" t_1 ON t_1.\"id\" = t_2.\"person_id\" \
           JOIN \"main\".\"customer\" t_0 ON t_0.\"id\" = t_1.\"customer_id\"\
         ) _dedup WHERE _loom_rn = 1 LIMIT 100"
    );
}

#[test]
fn identity_none_is_byte_identical_to_distinct_fallback() {
    let (sql, _params) = compile_chain(
        &two_hop_types(),
        &two_hop_hops(),
        &["name".to_string()],
        &[],
        None,
        100,
    )
    .unwrap();
    assert_eq!(
        sql,
        "SELECT DISTINCT t_2.\"name\" FROM \"main\".\"person\" t_2 \
         JOIN \"main\".\"orders\" t_1 ON t_1.\"id\" = t_2.\"person_id\" \
         JOIN \"main\".\"customer\" t_0 ON t_0.\"id\" = t_1.\"customer_id\" \
         LIMIT 100"
    );
}
```

- [ ] **Step 2: Run tests to verify they fail** (compile error — `compile_chain` arity mismatch is expected until the signature changes).

Run: `buck2 test //src/services/query-api:sql-compile > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t1.log`
Expected: build/compile failure (arity) or assertion FAIL on the new tests.

- [ ] **Step 3: Implement.** In `compile_chain_with` (`sql.rs:579`), add `identity: Option<&str>` before `limit`. Build the column exprs into a `Vec<String>` (call it `col_exprs`) instead of the joined string, then branch:

```rust
pub fn compile_chain_with(
    dialect: &dyn SqlDialect,
    types: &[ChainType],
    hops: &[LinkBacking],
    allowed_cols: &[String],
    mask_cols: &[String],
    identity: Option<&str>,
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError> {
    let k = hops.len();
    let final_alias = format!("t_{k}");
    let col_exprs: Vec<String> = allowed_cols
        .iter()
        .map(|c| {
            if mask_cols.iter().any(|m| m == c) {
                format!("'{MASK_MARKER}' AS {}", dialect.quote_ident(c))
            } else {
                format!("{final_alias}.{}", dialect.quote_ident(c))
            }
        })
        .collect();
    let (from, conjuncts, params) = chain_from_where(dialect, types, hops)?;
    let where_sql = if conjuncts.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conjuncts.join(" AND "))
    };
    let limit_clause = dialect.limit_clause(limit);
    let sql = match identity {
        Some(id) => {
            let inner_cols = col_exprs.join(", ");
            let partition = format!("{final_alias}.{}", dialect.quote_ident(id));
            let outer_cols = allowed_cols
                .iter()
                .map(|c| dialect.quote_ident(c))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "SELECT {outer_cols} FROM (\
                   SELECT {inner_cols}, \
                   ROW_NUMBER() OVER (PARTITION BY {partition}) AS _loom_rn \
                   FROM {from}{where_sql}\
                 ) _dedup WHERE _loom_rn = 1 {limit_clause}"
            )
        }
        None => {
            let cols = col_exprs.join(", ");
            format!("SELECT DISTINCT {cols} FROM {from}{where_sql} {limit_clause}")
        }
    };
    Ok((sql, params))
}
```

Then in `compile_chain` (`sql.rs:1145`) add the same `identity: Option<&str>` param before `limit` and forward it:

```rust
pub fn compile_chain(
    types: &[ChainType],
    hops: &[LinkBacking],
    allowed_cols: &[String],
    mask_cols: &[String],
    identity: Option<&str>,
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError> {
    compile_chain_with(
        &DataFusionDialect,
        types,
        hops,
        allowed_cols,
        mask_cols,
        identity,
        limit,
    )
}
```

> Note the `None` branch composes `{from}{where_sql}` then ` {limit_clause}`: `where_sql` carries its own leading space (or is empty), so with no conjuncts this yields `… t_0 LIMIT 100` (single space before LIMIT) — byte-identical to today's `format!(" {}", dialect.limit_clause(limit))` after a WHERE-less FROM. Verify against the untouched existing assertions.

- [ ] **Step 4: Run tests to verify they pass.**

Run: `buck2 test //src/services/query-api:sql-compile > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t1.log`
Expected: PASS (all existing + 3 new).

- [ ] **Step 5: Commit.**

```bash
git add src/services/query-api/src/sql.rs src/services/query-api/tests/sql_compile.rs
git commit -m "feat(query): identity-keyed dedup for many-to-many traversal"
```

---

## Task 2: Pass the final-target identity from `read_linked_chain`

**Files:**
- Modify: `src/services/query-api/src/handler.rs` (`read_linked_chain`, the `compile_chain_with` call at `:857`)

**Interfaces:**
- Consumes: `compile_chain_with(dialect, types, hops, allowed, mask, identity, limit)` (Task 1).
- Produces: no signature change; `read_linked_chain` now passes `target.otype.identity.as_deref()`.

- [ ] **Step 1: Update the call site.** `target` (= `metas.last()`) is already in scope at `handler.rs:844`. Change the `compile_chain_with` call (`:857`) to pass the identity before `deps.default_limit`:

```rust
    let (sql, params) = compile_chain_with(
        deps.serving.dialect(),
        &ctypes,
        &hops,
        &to_allowed,
        &to_mask_cols,
        target.otype.identity.as_deref(),
        deps.default_limit,
    )?;
```

- [ ] **Step 2: Build to verify it type-checks.**

Run: `buck2 build //src/services/query-api:query-api > /tmp/t2.log 2>&1; grep -E "BUILD SUCCEEDED|error\[|error:" /tmp/t2.log`
Expected: BUILD SUCCEEDED.

- [ ] **Step 3: Commit.**

```bash
git add src/services/query-api/src/handler.rs
git commit -m "feat(query): read_linked_chain dedups on final-target identity"
```

---

## Task 3: Drop redundant `DISTINCT` in `compile_graph_reach` + `compile_graph_reach_union`

**Files:**
- Modify: `src/services/query-api/src/sql.rs` (`compile_graph_reach` final `SELECT` `:864`; `compile_graph_reach_union` final `SELECT` `:986`)
- Modify: `src/services/query-api/tests/compile_graph_reach.rs`, `src/services/query-api/tests/compile_graph_reach_union.rs`, and any other test asserting these two compilers' `SELECT DISTINCT` (search with the grep in Step 1)

**Interfaces:**
- No signature change (both already take `identity` and key on `p.{id} IN (SELECT id FROM reach …)`). Only the terminal `SELECT DISTINCT {cols}` → `SELECT {cols}`.

- [ ] **Step 1: Find every affected assertion.**

Run: `grep -rln "SELECT DISTINCT" src/services/query-api/tests/ | xargs grep -ln "reach" `
Also inspect `compile_graph_reach.rs`, `compile_graph_reach_union.rs`, `graph_reach.rs`, `recursive_cte_over_datafusion.rs` for `SELECT DISTINCT {cols} FROM {tbl} p`. Note which assert the reachability projection (they change) vs. the CTE-internal `UNION` (unchanged — the recursive CTE's own dedup stays).

- [ ] **Step 2: Update the failing assertions** in those test files. Note the assertion style is `sql.contains(...)`, **not** a full-string `assert_eq!`: `compile_graph_reach.rs:59` and `compile_graph_reach_union.rs:102` assert `sql.contains("SELECT DISTINCT") && sql.contains(r#"p."name""#)`. After the DISTINCT-drop the projection becomes `SELECT p."name" FROM … p`, so replace the `contains("SELECT DISTINCT")` predicate with an un-DISTINCT projection check, e.g.:

```rust
    assert!(
        sql.contains(r#"SELECT p."name" FROM"#) && !sql.contains("SELECT DISTINCT"),
        "projection (no DISTINCT): {sql}"
    );
```

`DISTINCT` appears nowhere else in these two compilers (the CTE dedups via `UNION`, not `SELECT DISTINCT`), so the `!sql.contains("SELECT DISTINCT")` guard is safe. Leave all params/CTE/other assertions unchanged.

- [ ] **Step 3: Run tests to verify they fail** against the current (still-`DISTINCT`) implementation.

Run: `buck2 test //src/services/query-api:compile-graph-reach //src/services/query-api:compile-graph-reach-union //src/services/query-api:graph-reach //src/services/query-api:recursive-cte-over-datafusion > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t3.log`
Expected: FAIL (expected text now lacks DISTINCT, impl still emits it). (Confirm the exact target names from `src/services/query-api/BUCK` — `grep -n "name = " src/services/query-api/BUCK`.)

- [ ] **Step 4: Implement.** In `compile_graph_reach` (`sql.rs:864`), change the final format string:

```rust
    let sql = format!(
        "WITH RECURSIVE reach(id, depth) AS (\
           SELECT s.{id} AS id, 0 AS depth FROM {tbl} s{seed_where} \
           UNION \
           SELECT nxt.{id} AS id, r.depth + 1 AS depth FROM reach r JOIN {tbl} cur ON cur.{id} = r.id{joins} WHERE {rec_where}\
         ) \
         SELECT {cols} FROM {tbl} p WHERE {proj_where} {limit_clause}"
    );
```

In `compile_graph_reach_union` (`sql.rs:986`), the same edit to its final `SELECT DISTINCT {cols} FROM {tbl} p WHERE {proj_where} {limit_clause}` → `SELECT {cols} FROM {tbl} p WHERE {proj_where} {limit_clause}`.

- [ ] **Step 5: Run tests to verify they pass.**

Run: `buck2 test //src/services/query-api:compile-graph-reach //src/services/query-api:compile-graph-reach-union //src/services/query-api:graph-reach //src/services/query-api:recursive-cte-over-datafusion > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t3.log`
Expected: PASS.

- [ ] **Step 6: Commit.**

```bash
git add src/services/query-api/src/sql.rs src/services/query-api/tests/
git commit -m "fix(query): drop redundant DISTINCT in reachability projection"
```

---

## Task 4: Windowed identity dedup in `compile_graph_reach_tail`

**Files:**
- Modify: `src/services/query-api/src/sql.rs` (`compile_graph_reach_tail` `:1069`, terminal projection `:1117–1138`)
- Modify: `src/services/query-api/src/handler.rs` (graph-tail `compile_graph_reach_tail` call `:1405`)
- Modify: `src/services/query-api/tests/compile_graph_reach_tail.rs` (existing call sites + assertions), `src/services/query-api/tests/graph_reach_tail.rs` if it asserts the tail SQL

**Interfaces:**
- Produces: `compile_graph_reach_tail` gains `final_identity: Option<&str>` inserted **immediately before `depth`** (the last two params become `final_identity, depth, limit`). `None` ⇒ today's `{cte} SELECT DISTINCT …`. The existing `identity` param (core self-link) is unchanged.
- Consumes: `final_type.identity.as_deref()` at the handler call site (`final_type` in scope at `handler.rs:1375`).

- [ ] **Step 1: Write/adjust the failing compiler tests** in `compile_graph_reach_tail.rs`. The existing tests use `sql.contains(...)` predicates (3 call sites at lines ~44, ~132, ~206; a `contains("SELECT DISTINCT")` at line ~85), **not** full-string `assert_eq!`. Insert `None` before the `depth` argument at each existing call site — that keeps their `contains("SELECT DISTINCT")` guards valid as the `final_identity: None` regression guard (the `None` branch still emits `SELECT DISTINCT`). Then add one NEW test that passes `final_identity = Some(...)` and asserts the windowed shape with an exact `assert_eq!`. Mirror the file's existing setup for `tail_types`/`tail_hops`/`cte`. The expected outer form is:

```
{cte} SELECT <outer cols> FROM (SELECT <inner masked/visible cols>, ROW_NUMBER() OVER (PARTITION BY t_k.<identity>) AS _loom_rn FROM <from> WHERE <where_sql>) _dedup WHERE _loom_rn = 1 <limit>
```

Build the exact expected string by reading the current test's `SELECT DISTINCT {cols} FROM {from} WHERE {where_sql} {limit}` expectation and transforming it into the windowed shape (same `cols` as inner exprs, same `from`/`where_sql`, outer select = bare quoted column names, partition on `t_k."<identity>"`).

- [ ] **Step 2: Run to verify fail.**

Run: `buck2 test //src/services/query-api:compile-graph-reach-tail //src/services/query-api:graph-reach-tail > /tmp/t4.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t4.log`
Expected: FAIL/compile error (arity).

- [ ] **Step 3: Implement the compiler change.** In `compile_graph_reach_tail`, add `final_identity: Option<&str>` before `depth`. Build `col_exprs` as a `Vec<String>` (the existing inline mask/visible builder), keep the `where_sql` computation, then branch on `final_identity` exactly as in `compile_chain_with` — reusing `final_alias = format!("t_{k}")` (`k = tail_hops.len()`), the CTE prefix `{cte} `, and the always-present `where_sql`:

```rust
    let col_exprs: Vec<String> = allowed_cols
        .iter()
        .map(|c| {
            if mask_cols.iter().any(|m| m == c) {
                format!("'{MASK_MARKER}' AS {}", q(c))
            } else {
                format!("{final_alias}.{}", q(c))
            }
        })
        .collect();
    // where_conj already built above; keep it:
    let where_sql = where_conj.join(" AND ");
    let limit_clause = dialect.limit_clause(limit);
    let sql = match final_identity {
        // NB: `id` here shadows the outer `let id = q(identity)` (the CORE self-link PK).
        // Safe: `where_sql` was built from the outer `id` above; this arm uses `id` only
        // for the final-target PARTITION BY. Keep the shadow scoped to the arm.
        Some(id) => {
            let inner_cols = col_exprs.join(", ");
            let partition = format!("{final_alias}.{}", q(id));
            let outer_cols = allowed_cols
                .iter()
                .map(|c| q(c))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "{cte} SELECT {outer_cols} FROM (\
                   SELECT {inner_cols}, \
                   ROW_NUMBER() OVER (PARTITION BY {partition}) AS _loom_rn \
                   FROM {from} WHERE {where_sql}\
                 ) _dedup WHERE _loom_rn = 1 {limit_clause}"
            )
        }
        None => {
            let cols = col_exprs.join(", ");
            format!("{cte} SELECT DISTINCT {cols} FROM {from} WHERE {where_sql} {limit_clause}")
        }
    };
    Ok((sql, params))
```

(The graph-tail `where_sql` is always non-empty — it always contains the `t_0.{id} IN (SELECT id FROM reach …)` conjunct — so the unconditional ` WHERE {where_sql}` is correct in both branches.)

- [ ] **Step 4: Update the handler call site** (`handler.rs:1405`) to pass `final_type.identity.as_deref()` before `q.depth`:

```rust
    let (sql, params) = crate::sql::compile_graph_reach_tail(
        deps.serving.dialect(),
        &object_type.table,
        &identity,
        &core_backing,
        &seed_predicates,
        &core_filters,
        &tail_types,
        &tail_hops,
        &allowed,
        &mask_cols,
        final_type.identity.as_deref(),
        q.depth,
        deps.default_limit,
    )?;
```

- [ ] **Step 5: Run to verify pass.**

Run: `buck2 test //src/services/query-api:compile-graph-reach-tail //src/services/query-api:graph-reach-tail //src/services/query-api:graph-tail-e2e > /tmp/t4.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t4.log`
Expected: PASS. Also `buck2 build //src/services/query-api:query-api` to confirm the handler compiles.

> **`graph-tail-e2e` is Task 4's runtime gate.** `tests/graph_tail_e2e.rs` seeds Company/City types with `identity: Some("id")`, so after this change both its tail tests execute the **new windowed graph-tail SQL against real DataFusion** (Task 4's unit tests only assert SQL text). They pass behaviorally — the `ids`/`ids_i64` helpers `sort_unstable`, normalizing the window's ORDER-BY-free row order, and same-object dedup is preserved. Its header comment (`:6`) and inline comments (`~:226–227`) attribute the old collapse to "SELECT DISTINCT" — update them to reference the windowed identity dedup (fold into this task's commit).

- [ ] **Step 6: Commit.**

```bash
git add src/services/query-api/src/sql.rs src/services/query-api/src/handler.rs src/services/query-api/tests/
git commit -m "fix(query): identity-keyed dedup in graph recursive-tail read"
```

---

## Task 5: End-to-end regression — masked/denied identity no longer collapses distinct objects

**Files:**
- Create: `src/services/query-api/tests/object_identity_dedup_e2e.rs`
- Modify: `src/services/query-api/BUCK` (add a `rust_test` target mirroring an existing e2e target, e.g. `object-set-e2e`, with `deps` including `":e2e-support"`)
- Reference (do not copy): `src/services/query-api/tests/e2e_support.rs` for `tref`/`land`/`prop`/`setup`/`subject_with_role`/`grant_read`/`ids`/`ids_i64`, and `src/services/query-api/tests/link_traversal.rs` for the many-to-many seed shape (`many_to_many_dedups_shared_targets`, `:406`).

**Interfaces:**
- Consumes the shared e2e helpers via `use e2e_support::{…}`. If a mask-grant or deny-grant helper is not already exported by `e2e_support.rs`, add the SELECT/ACL wiring inline in this test first (extend the library only if it turns out to be genuinely reusable across ≥2 test files — per CLAUDE.md and spec).

- [ ] **Step 1: Inspect the shared helpers and the ACL grant surface.** Read `e2e_support.rs` to confirm the exact signatures of `grant_read` / `subject_with_role` and whether a mask/deny grant helper exists. Read `link_traversal.rs::setup` and `many_to_many_dedups_shared_targets` for how a many-to-many (join-table) topology is seeded and how `grant_read`/policy masking are applied per column. Determine how a column is **masked** vs **denied** in a policy (search `mask_columns` / `deny` in `e2e_support.rs`, `link_traversal.rs`, and `association_e2e.rs`).

Run: `grep -n "mask_columns\|deny\|grant_read\|subject_with_role\|identity" src/services/query-api/tests/e2e_support.rs src/services/query-api/tests/link_traversal.rs src/services/query-api/tests/association_e2e.rs`

- [ ] **Step 2: Write the failing e2e test.** Seed a many-to-many traversal whose **final-target type has a declared `identity`** (e.g. `Person` with `identity = "ssn"`), and seed **two distinct target objects that share every non-identity column** (the collapse trap: two `Person` rows, same `name`/`city`, different `ssn`), each reachable from the source through the join. Assert the four cases from the spec Testing section:

```rust
// 1. Baseline (identity visible): full-Read subject sees 2 distinct rows.
// 2. Bug case (identity masked): subject whose policy masks `ssn` still gets 2 rows
//    (ssn rendered '***' in both). count == 2. (Fails pre-fix: returns 1.)
// 3. Identity denied (dropped from projection): subject with `ssn` denied gets 2 rows
//    (ssn column absent, count preserved).
// 4. Genuine duplicate collapse still holds: a single target reached via two
//    intermediate paths yields exactly 1 row (same-object dedup still merges).
```

Use `ids`/`ids_i64` (or a row-count assertion on `ObjectRows.rows.len()`) as appropriate — the masked case cannot key on `ssn` (it is `'***'`), so assert on **row count** for cases 2 and 3, and on distinct visible identity for case 1. Model the request path on how `link_traversal.rs` drives `read_linked_chain` (or the HTTP `GET /objects/{from}/links/{link}` surface used by the existing e2e tests — follow whichever the neighbouring e2e tests use).

- [ ] **Step 3: Wire the BUCK target and run to verify the bug case fails pre-fix on a clean checkout of Task 1's base** — but since Tasks 1–2 are already implemented on this branch, instead confirm the test **passes** on the fixed code and reason explicitly that case 2's assertion (`== 2`) is the one that would return `1` without the fix. (If practical, temporarily stash the `sql.rs` change and re-run to observe `1`, then restore — optional verification.)

Run: `buck2 test //src/services/query-api:object-identity-dedup-e2e > /tmp/t5.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t5.log`
Expected: PASS on the fixed code.

- [ ] **Step 4: Commit.**

```bash
git add src/services/query-api/tests/object_identity_dedup_e2e.rs src/services/query-api/BUCK
git commit -m "test(query): e2e regression for masked-identity traversal dedup"
```

---

## Task 6: Full query-api sweep + docs register update

**Files:**
- Modify: `docs/ROADMAP.md` (close `road-object-identity-dedup`) and `docs/FUTURE.md` (mark `fut-object-identity-dedup` promoted) via `loom-docs-update`.

- [ ] **Step 1: Run the full query-api test sweep** to catch any other assertion that referenced the changed compilers (e.g. `graph_reach_e2e.rs`, `graph_tail_e2e.rs`, `multi_hop_traversal_e2e.rs`, `wire_governed_read_e2e.rs`).

Run: `buck2 test //src/services/query-api/... > /tmp/qa.log 2>&1; grep -E "Tests finished|FAIL" /tmp/qa.log`
Expected: all PASS. Fix any straggler assertion (a test that hard-coded the old `SELECT DISTINCT` chain/tail text) by transforming it to the new shape, matching the Task 1/4 transforms.

- [ ] **Step 2: Clippy clean** on the touched crate.

Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/clip.log 2>&1; cat $(buck2 build --show-output '//src/services/query-api:query-api[clippy.txt]' 2>/dev/null | awk '{print $2}') 2>/dev/null; grep -E "warning|error" /tmp/clip.log || echo "clippy clean"`
Expected: empty clippy output. (Watch for `unwrap_used`/`indexing_slicing` etc. — production code only; tests are exempt.)

- [ ] **Step 3: Update the docs registers** via the `loom-docs-update` skill: flip `- [ ]`→`- [x]` on `road-object-identity-dedup`, set `status:done`, add `pr:#<n>` (fill after PR opens), and mark `fut-object-identity-dedup` `status:promoted` if not already. Run `bash tools/docs.sh validate` to confirm grammar.

- [ ] **Step 4: Commit.**

```bash
git add docs/ROADMAP.md docs/FUTURE.md
git commit -m "docs(registers): close road-object-identity-dedup"
```

---

## Self-review checklist (author, before handoff)

- **Spec coverage:** `compile_chain_with` window fix (Task 1) ✓; `read_linked_chain` threading (Task 2) ✓; `compile_graph_reach`/`_union` DISTINCT-drop (Task 3) ✓; `compile_graph_reach_tail` window fix + handler threading (Task 4, spec Open-question #1 recommendation) ✓; compiler unit tests for masked/visible/None/reach-DISTINCT (Tasks 1,3,4) ✓; e2e regression with the four spec cases (Task 5) ✓; `identity: None` back-compat fallback (Task 1/4 `None` branch, byte-identical assertions) ✓; no new error variants (nothing added) ✓; `read_associations` untouched ✓.
- **Placeholders:** none — every code step carries the actual code/SQL.
- **Type consistency:** `identity: Option<&str>` param name and position (before `limit`) consistent across `compile_chain_with`/`compile_chain`; `final_identity: Option<&str>` (before `depth`) in `compile_graph_reach_tail` deliberately distinct from its pre-existing core `identity` param; handler passes `…identity.as_deref()` in both call sites; `_loom_rn` / `_dedup` alias names identical across Tasks 1 and 4.
