# `/graph` Shortest-Path Tree — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Serve a shortest-path tree (parent pointer + BFS depth per node, rooted at the seed set) over the existing self-link (`/graph/:link`) and path-cycle (`/graph?path=…`) routes, selected by a `?tree=true` response-shaping flag.

**Architecture:** Extend the reachability `WITH RECURSIVE reach(id, depth)` CTE to carry a predecessor column (`reach(id, depth, pred)`), add a non-recursive `settled` CTE that keeps exactly one shortest-path parent per node via `ROW_NUMBER() OVER (PARTITION BY id ORDER BY depth ASC, pred ASC NULLS FIRST)`, and project the object plus `__depth`/`__parent`/`__id`. The handler resolution (Read-gate, path/cycle resolution, ACL, identity, projection, seed predicates) is factored out of `read_graph_reach` into a shared `resolve_graph` helper so a new `read_graph_tree` reuses it verbatim and only differs in the SQL compiler + result shape. A distinct `{roots, nodes}` JSON response is rendered by a new `tree_to_json`.

**Tech Stack:** Rust, buck2, DataFusion 54 (serving engine), axum, utoipa (OpenAPI), sqlx (unaffected). Tests are `rust_test`/`loom_fixture_test` integration targets only — never inline `#[test]`.

## Global Constraints

- **Spec:** `docs/superpowers/specs/2026-07-01-shortest-path-tree-design.md` — the authority for this work.
- **No inline tests.** Unit tests go in sibling `tests/<name>.rs` files wired as their own `rust_test` target in `src/services/query-api/BUCK`. The `no-inline-tests` prek hook fails on any `#[test]`/`#[tokio::test]` inside `src/**.rs`.
- **Fixture (Postgres/DataFusion-backed) e2e tests use `loom_fixture_test`**, never a bare `rust_test`, or they route to remote execution and fail as root. Pure-logic compiler/handler tests use `rust_test`.
- **Reuse `//src/services/query-api:e2e-support`** (`get`, `tref`, `prop`, `grant_read`, `subject_with_role`, `ids`/`ids_i64`, `InProcessServingEngine`). Extend it with a shared tree-node extractor rather than re-parsing per file.
- **Clippy is strict** (pedantic + restriction on production code; test code is exempted from the panic-safety lints via the `loom_rust_test`/`loom_fixture_test` wrappers). No `unwrap`/`expect`/`indexing`/`panic`/`todo` in `src/**`. Use `#[expect(lint, reason = "...")]` locally if truly needed.
- **Backward compatible:** absent `?tree=`, `/graph/:link` and `/graph?path=` return the reachable set exactly as today. The reachable-set SQL from `compile_graph_reach` must be byte-for-byte unchanged.
- **Slice 1 scope:** tree over the **single self-link** and the **repeated path-cycle** only. `?tree=true` combined with `?links=` (union) or a `*`-starred path segment (recursive-core+tail) → 400. Weighted edges, materialized full paths, target-scoped queries, and the union/tail tree are non-goals.
- **No `LIMIT` on the tree** (open question #2 resolved): the depth cap bounds the result; a `LIMIT` could drop a parent and leave a dangling pointer. Reachability keeps its `LIMIT`; the tree compiler omits it.
- **Determinism** (open question #3 resolved): tie-break is minimum predecessor **identity** (`pred ASC NULLS FIRST`), the only stable caller-independent key.
- **Typed null anchor** (open question #4): the anchor's `pred` uses `NULLIF(s.<id>, s.<id>)` so it is NULL typed as the identity column's type — union-compatible with the recursive term's `r.id` without the compiler needing to know the SQL type. The determinism e2e (Task 5) verifies the window form runs on the DataFusion serving path.
- **After any `.md` edit**, ensure exactly one trailing newline and no trailing whitespace (the `end-of-file-fixer`/`trim trailing whitespace` prek hooks police markdown). Run `buck2 run //tools:prek -- run --all-files` before pushing.

---

## File Structure

- **`src/services/query-api/src/sql.rs`** — add `compile_graph_tree` (new pure compiler) beside `compile_graph_reach`, reusing the existing `reach_seed_where`/`reach_joins`/`reach_recursive_where`/`masked_col_exprs`/`validate_reach_filters`/`filter_sql` helpers. Add three column-alias constants.
- **`src/services/query-api/src/handler.rs`** — add `ObjectTree`/`TreeNode` result types; factor `resolve_graph` (+ `GraphResolved` struct) out of `read_graph_reach`; add `read_graph_tree`.
- **`src/services/query-api/src/render.rs`** — add `tree_to_json`; make `render_cell` `pub(crate)`.
- **`src/services/query-api/src/http.rs`** — parse `?tree=` on `get_graph` and `get_graph_path`; add `graph_tree_respond`; reject tree+union / tree+starred with 400.
- **`src/services/query-api/src/openapi.rs`** — add `ObjectTreeResponse` doc schema; register it in `components(schemas(...))`.
- **`src/services/query-api/tests/compile_graph_tree.rs`** — new compiler unit test (`rust_test`).
- **`src/services/query-api/tests/graph_tree.rs`** — new handler test (`rust_test`, memory CP + stub serving).
- **`src/services/query-api/tests/graph_tree_e2e.rs`** — new e2e (`loom_fixture_test`, in-process DataFusion).
- **`src/services/query-api/tests/e2e_support.rs`** — add a `tree_nodes` extractor.
- **`src/services/query-api/BUCK`** — three new test targets.
- **`docs/ROADMAP.md` / `docs/FUTURE.md`** — closed via `loom-docs-update` at finish (Task 6).

---

## Task 1: `compile_graph_tree` SQL compiler

**Files:**
- Modify: `src/services/query-api/src/sql.rs` (add constants + `compile_graph_tree` after `compile_graph_reach`, ~line 867)
- Test: `src/services/query-api/tests/compile_graph_tree.rs` (create)
- Modify: `src/services/query-api/BUCK` (add `compile-graph-tree` target)

**Interfaces:**
- Consumes (existing, all in `sql.rs`): `SqlDialect`, `TableRef`, `GraphStep`, `CallerPredicate` (from `crate::filter`), `RowFilter`, `SqlValue`, `CompileError`, and the private helpers `reach_seed_where`, `reach_joins`, `reach_recursive_where`, `masked_col_exprs`, `validate_reach_filters`, `filter_sql`.
- Produces:
  ```rust
  pub const TREE_DEPTH_COL: &str = "__depth";
  pub const TREE_PARENT_COL: &str = "__parent";
  pub const TREE_NODE_ID_COL: &str = "__id";

  pub fn compile_graph_tree(
      dialect: &dyn SqlDialect,
      table: &TableRef,
      identity: &str,
      path: &[GraphStep],
      seed_predicates: &[CallerPredicate],
      row_filters: &[RowFilter],
      allowed_cols: &[String],
      mask_cols: &[String],
      depth: u32,
  ) -> Result<(String, Vec<SqlValue>), CompileError>;
  ```
  Param order (identical to `compile_graph_reach` minus the limit arg): seed predicates → seed row-filters (`s`) → recursive intermediate/start filters → projection row-filters (`p`).

- [ ] **Step 1: Write the failing compiler test**

Create `src/services/query-api/tests/compile_graph_tree.rs`:

```rust
//! compile_graph_tree emits a depth-bounded WITH RECURSIVE reach(id, depth, pred) query,
//! a `settled` window that keeps one shortest-path parent per node (ROW_NUMBER … WHERE
//! rn = 1), includes depth = 0 roots, projects __depth/__parent/__id, drops the LIMIT,
//! and binds seed/filter params in the same order as compile_graph_reach.

use control_plane_core::{CompareOp, LinkBacking, RowFilter, ScalarValue, TableRef};
use query_api::filter::CallerPredicate;
use query_api::serving::SqlValue;
use query_api::sql::{DataFusionDialect, GraphStep, compile_graph_tree};

fn person() -> TableRef {
    TableRef {
        schema: "main".into(),
        name: "person".into(),
    }
}

#[test]
fn fk_self_link_tree_shape() {
    // Person.knows_id -> Person.id (FK self-link).
    let backing = LinkBacking::ForeignKey {
        from_column: "knows_id".into(),
        to_column: "id".into(),
    };
    let (sql, params) = compile_graph_tree(
        &DataFusionDialect,
        &person(),
        "id",
        &[GraphStep {
            backing,
            next_table: person(),
            next_filters: vec![],
        }],
        &[], // no seed predicates
        &[], // no row-filters
        &["id".to_string(), "name".to_string()],
        &[],
        3,
    )
    .unwrap();
    assert!(params.is_empty());
    // predecessor carried through the CTE
    assert!(
        sql.contains("WITH RECURSIVE reach(id, depth, pred) AS"),
        "pred column in CTE: {sql}"
    );
    // anchor pred is a typed NULL, roots are parentless
    assert!(
        sql.contains("NULLIF(s.\"id\", s.\"id\") AS pred"),
        "typed-null anchor pred: {sql}"
    );
    // recursive term projects the expanded-from node as pred
    assert!(sql.contains("r.id AS pred"), "recursive pred = r.id: {sql}");
    assert!(sql.contains("r.depth < 3"), "depth bound inlined: {sql}");
    // settle: one parent per node, min depth then min pred
    assert!(
        sql.contains("ROW_NUMBER() OVER (PARTITION BY id ORDER BY depth ASC, pred ASC NULLS FIRST)"),
        "settle window: {sql}"
    );
    assert!(sql.contains("WHERE t.rn = 1"), "keep the settled parent: {sql}");
    // roots (depth 0) are INCLUDED — no `depth >= 1` filter as in reachability
    assert!(
        !sql.contains("depth >= 1"),
        "tree includes depth-0 roots, unlike reachability: {sql}"
    );
    // output columns
    assert!(
        sql.contains("t.depth AS __depth")
            && sql.contains("t.pred AS __parent")
            && sql.contains("p.\"id\" AS __id"),
        "depth/parent/id output columns: {sql}"
    );
    // no LIMIT on the tree (depth cap bounds it; a LIMIT could orphan a child)
    assert!(!sql.to_uppercase().contains("LIMIT"), "no LIMIT on the tree: {sql}");
    // stable node order
    assert!(
        sql.contains("ORDER BY t.depth ASC, p.\"id\" ASC"),
        "node ordering: {sql}"
    );
    // FK hop cur -> nxt (reused reach_joins)
    assert!(sql.contains("cur.\"knows_id\" = nxt.\"id\""), "fk join: {sql}");
    assert!(sql.contains("p.\"name\""), "object projection: {sql}");
}

#[test]
fn join_table_tree_with_row_filter_and_seed_param_order() {
    // Person knows Person via knows(a, b); an ACL row-filter active=true; a seed In-predicate.
    let backing = LinkBacking::JoinTable {
        table: TableRef {
            schema: "main".into(),
            name: "knows".into(),
        },
        from_key: "id".into(),
        from_column: "a".into(),
        to_column: "b".into(),
        to_key: "id".into(),
    };
    let row_filters = vec![RowFilter::Compare {
        property: "active".into(),
        op: CompareOp::Eq,
        value: ScalarValue::Bool(true),
    }];
    let seed = vec![CallerPredicate {
        column: "id".into(),
        op: CompareOp::In,
        values: vec![SqlValue::Int(5)],
    }];
    let (sql, params) = compile_graph_tree(
        &DataFusionDialect,
        &person(),
        "id",
        &[GraphStep {
            backing,
            next_table: person(),
            next_filters: vec![],
        }],
        &seed,
        &row_filters,
        &["id".to_string()],
        &[],
        2,
    )
    .unwrap();
    // join-table hop
    assert!(
        sql.contains("cur.\"id\" = j.\"a\"") && sql.contains("j.\"b\" = nxt.\"id\""),
        "jt join: {sql}"
    );
    // row-filter rendered at seed(s), expansion(nxt), projection(p)
    assert!(
        sql.contains("s.\"active\"") && sql.contains("nxt.\"active\"") && sql.contains("p.\"active\""),
        "row-filter at 3 positions: {sql}"
    );
    // params: seed In (1) + active at s, nxt, p (3) = 4, SAME ORDER as compile_graph_reach
    assert_eq!(params.len(), 4, "1 seed id + 3 row-filter renderings; got {params:?}");
    assert_eq!(params[0], SqlValue::Int(5));
    assert_eq!(params[1], SqlValue::Bool(true)); // s.active
    assert_eq!(params[2], SqlValue::Bool(true)); // nxt.active
    assert_eq!(params[3], SqlValue::Bool(true)); // p.active
}

#[test]
fn two_step_path_cycle_tree() {
    // Person --memberOf(FK team_id->id)--> Team --hasMember(FK id->team_id)--> Person cycle.
    let team = TableRef {
        schema: "main".into(),
        name: "team".into(),
    };
    let path = vec![
        GraphStep {
            backing: LinkBacking::ForeignKey {
                from_column: "team_id".into(),
                to_column: "id".into(),
            },
            next_table: team.clone(),
            next_filters: vec![RowFilter::Compare {
                property: "active".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Bool(true),
            }],
        },
        GraphStep {
            backing: LinkBacking::ForeignKey {
                from_column: "id".into(),
                to_column: "team_id".into(),
            },
            next_table: person(),
            next_filters: vec![],
        },
    ];
    let (sql, _params) = compile_graph_tree(
        &DataFusionDialect,
        &person(),
        "id",
        &path,
        &[],
        &[],
        &["id".to_string()],
        &[],
        2,
    )
    .unwrap();
    assert!(sql.contains("cur.\"team_id\" = g1.\"id\""), "step1 join: {sql}");
    assert!(sql.contains("g1.\"id\" = nxt.\"team_id\""), "step2 join: {sql}");
    assert!(sql.contains("g1.\"active\""), "intermediate filter at g1: {sql}");
}
```

- [ ] **Step 2: Add the `compile-graph-tree` BUCK target**

In `src/services/query-api/BUCK`, after the `compile-graph-reach` target (~line 770):

```python
rust_test(
    name = "compile-graph-tree",
    crate = "compile_graph_tree",
    srcs = ["tests/compile_graph_tree.rs"],
    crate_root = "tests/compile_graph_tree.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
    ],
)
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `buck2 test //src/services/query-api:compile-graph-tree > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[|cannot find" /tmp/t.log`
Expected: FAIL — `cannot find function compile_graph_tree` (not yet defined).

- [ ] **Step 4: Implement `compile_graph_tree`**

In `src/services/query-api/src/sql.rs`, add the constants near the top (beside `const MASK_MARKER`, ~line 51):

```rust
/// Output column aliases for the shortest-path-tree read: the settled BFS depth, the
/// predecessor (parent) identity value (NULL for roots), and the node's own identity value.
/// The handler splits these three trailing columns off each served row.
pub const TREE_DEPTH_COL: &str = "__depth";
pub const TREE_PARENT_COL: &str = "__parent";
pub const TREE_NODE_ID_COL: &str = "__id";
```

Then add the compiler immediately after `compile_graph_reach` (after ~line 867):

```rust
/// Compile a depth-bounded shortest-path-**tree** query over a path-cycle (a 1-step path is the
/// single-self-link case). A variant of [`compile_graph_reach`]: the recursive CTE carries the
/// predecessor node id (`reach(id, depth, pred)`) so every reach-edge records which node it was
/// reached from; a non-recursive `settled` CTE keeps exactly one row per node — minimum depth,
/// then minimum predecessor identity (`ROW_NUMBER() OVER (PARTITION BY id ORDER BY depth ASC,
/// pred ASC NULLS FIRST)` + `WHERE rn = 1`) — the shortest-path parent, deterministically. The
/// outer SELECT joins settled ids back to the table and projects the visible object columns plus
/// `__depth`/`__parent`/`__id`. Unlike reachability it **includes depth-0 roots** (a tree needs
/// its roots) and emits **no LIMIT** (the depth cap bounds the tree; a LIMIT could orphan a
/// child). Governance is threaded exactly as reachability: seed/every recursive hop/projection
/// row-filters. Param order matches [`compile_graph_reach`] (seed predicates, seed filters,
/// recursive filters, projection filters) minus the limit. The anchor's `pred` is
/// `NULLIF(s.<id>, s.<id>)` — a NULL typed as the identity column, union-compatible with the
/// recursive term's `r.id` without the compiler knowing the SQL type.
#[allow(
    clippy::too_many_arguments,
    reason = "SQL compile functions require all builder parameters"
)]
pub fn compile_graph_tree(
    dialect: &dyn SqlDialect,
    table: &TableRef,
    identity: &str,
    path: &[GraphStep],
    seed_predicates: &[CallerPredicate],
    row_filters: &[RowFilter],
    allowed_cols: &[String],
    mask_cols: &[String],
    depth: u32,
) -> Result<(String, Vec<SqlValue>), CompileError> {
    validate_reach_filters(row_filters, path)?;

    let q = |id: &str| dialect.quote_ident(id);
    let tbl = format!("{}.{}", q(&table.schema), q(&table.name));
    let id = q(identity);
    let mut params: Vec<SqlValue> = Vec::new();

    let seed_where = reach_seed_where(dialect, seed_predicates, row_filters, &mut params);
    let joins = reach_joins(dialect, path);
    let rec_where = reach_recursive_where(dialect, path, row_filters, depth, &mut params);

    // Projection of `p`: visible columns (masked -> marker), governed by the start row-filters.
    let cols = masked_col_exprs(dialect, allowed_cols, mask_cols, "p.").join(", ");
    let mut proj_conj: Vec<String> = Vec::new();
    for f in row_filters {
        proj_conj.push(filter_sql(dialect, f, "p", &mut params));
    }
    let proj_and = if proj_conj.is_empty() {
        String::new()
    } else {
        format!(" AND {}", proj_conj.join(" AND "))
    };

    let depth_col = TREE_DEPTH_COL;
    let parent_col = TREE_PARENT_COL;
    let node_id_col = TREE_NODE_ID_COL;

    let sql = format!(
        "WITH RECURSIVE reach(id, depth, pred) AS (\
           SELECT s.{id} AS id, 0 AS depth, NULLIF(s.{id}, s.{id}) AS pred FROM {tbl} s{seed_where} \
           UNION \
           SELECT nxt.{id} AS id, r.depth + 1 AS depth, r.id AS pred FROM reach r JOIN {tbl} cur ON cur.{id} = r.id{joins} WHERE {rec_where}\
         ), \
         settled AS (\
           SELECT id, depth, pred, ROW_NUMBER() OVER (PARTITION BY id ORDER BY depth ASC, pred ASC NULLS FIRST) AS rn FROM reach\
         ) \
         SELECT {cols}, t.depth AS {depth_col}, t.pred AS {parent_col}, p.{id} AS {node_id_col} \
         FROM settled t JOIN {tbl} p ON p.{id} = t.id WHERE t.rn = 1{proj_and} \
         ORDER BY t.depth ASC, p.{id} ASC"
    );
    Ok((sql, params))
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `buck2 test //src/services/query-api:compile-graph-tree > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (3 tests).

- [ ] **Step 6: Verify the reachable-set compiler is byte-unchanged (regression guard)**

Run: `buck2 test //src/services/query-api:compile-graph-reach //src/services/query-api:compile-graph-reach-union > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (no change to `compile_graph_reach`).

- [ ] **Step 7: Clippy + commit**

Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log` (empty output == clean).

```bash
git add src/services/query-api/src/sql.rs src/services/query-api/tests/compile_graph_tree.rs src/services/query-api/BUCK
git commit -m "feat(query-api): compile_graph_tree — shortest-path-tree SQL compiler"
```

---

## Task 2: Factor `resolve_graph` out of `read_graph_reach` (behavior-preserving)

**Files:**
- Modify: `src/services/query-api/src/handler.rs` (`read_graph_reach`, ~lines 985–1122)

**Interfaces:**
- Produces (private to `handler.rs`):
  ```rust
  struct GraphResolved {
      object_type: ObjectType,
      identity: String,
      steps: Vec<crate::sql::GraphStep>,
      row_filters: Vec<RowFilter>,
      allowed: Vec<String>,
      mask_cols: Vec<String>,
      denied: std::collections::HashSet<String>,
      masked: std::collections::HashSet<String>,
      seed_predicates: Vec<crate::filter::CallerPredicate>,
  }

  async fn resolve_graph(
      q: &GraphQuery,
      subject: &Subject,
      deps: &QueryDeps<'_>,
  ) -> Result<GraphResolved, QueryError>;
  ```
- `read_graph_reach` keeps its public signature and behavior unchanged.

This task has **no new test** — it is a pure refactor guarded by the existing `graph_reach` (`rust_test`) and `graph_reach_e2e` (`loom_fixture_test`) suites. A reviewer gates it on "existing graph tests stay green + `read_graph_reach` body is now the compile+shape only."

- [ ] **Step 1: Confirm the guarding tests are green before refactoring**

Run: `buck2 test //src/services/query-api:graph-reach > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS.

- [ ] **Step 2: Extract `resolve_graph`**

In `src/services/query-api/src/handler.rs`, replace the body of `read_graph_reach` (lines ~985–1122) with a new private `resolve_graph` plus a thin `read_graph_reach`. Add `GraphResolved` just above `read_graph_reach`.

Add the struct:

```rust
/// Everything the two graph reads (reachable-set and shortest-path-tree) need after resolving
/// the queried type, ACL policy, and the path-cycle: the resolved object type + declared
/// identity, the compiler `GraphStep`s (per-intermediate governance already folded in), the
/// start row-filters, the visible/masked projection, the raw denied/masked column sets (so a
/// caller can additionally require identity visibility), and the coerced seed predicates.
struct GraphResolved {
    object_type: ObjectType,
    identity: String,
    steps: Vec<crate::sql::GraphStep>,
    row_filters: Vec<RowFilter>,
    allowed: Vec<String>,
    mask_cols: Vec<String>,
    denied: std::collections::HashSet<String>,
    masked: std::collections::HashSet<String>,
    seed_predicates: Vec<crate::filter::CallerPredicate>,
}

/// Resolve + govern a graph read: Read-gate the queried type, resolve the path-cycle (Read on
/// every intermediate type, its row-filters folded into the step), require a declared identity
/// (the recursion's dedup key), project the visible columns, and coerce/visibility-check the
/// seed predicates + `?_ids=`. Shared verbatim by `read_graph_reach` (reachable set) and
/// `read_graph_tree` (shortest-path tree) so governance lives in one place.
async fn resolve_graph(
    q: &GraphQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<GraphResolved, QueryError> {
    let type_name = TypeName(q.type_name.clone());
    let target = PolicyTarget::Type(type_name.clone());

    // Read gate (deny-by-default, before existence is revealed).
    if deps.acl.check(&subject.0, Action::Read, &target).await? == Decision::Deny {
        return Err(QueryError::Forbidden);
    }
    let object_type = deps
        .ontology
        .get_type(&type_name)
        .await
        .map_err(|e| match e {
            ControlPlaneError::NotFound(_) => QueryError::UnknownType(q.type_name.clone()),
            other => QueryError::ControlPlane(other),
        })?;
    let (row_filters, denied, masked) = load_policy(deps.acl, &subject.0, &target).await?;

    // Declared identity is the recursion's dedup key.
    let identity = object_type
        .identity
        .clone()
        .ok_or_else(|| QueryError::NoIdentity(q.type_name.clone()))?;

    // Resolve the path-cycle: walk l1..lK forward from the queried type. Each landed type is
    // Read-gated and its row-filters loaded (intermediate governance). After the last link the
    // type must be the queried type again (a cycle) — else it cannot be repeated.
    if q.path.is_empty() {
        return Err(QueryError::NotCyclicPath(String::new()));
    }
    let mut steps: Vec<crate::sql::GraphStep> = Vec::with_capacity(q.path.len());
    let mut current = type_name.clone();
    let last = q.path.len() - 1;
    for (i, link_name) in q.path.iter().enumerate() {
        let links = deps.ontology.links(&current, PageReq::unbounded()).await?;
        let link = links
            .items
            .into_iter()
            .find(|l| &l.name == link_name)
            .ok_or_else(|| QueryError::UnknownLink(link_name.clone()))?;
        let landed = link.to.clone();
        let landed_target = PolicyTarget::Type(landed.clone());
        // Read on every reached type (intermediate + final).
        if deps
            .acl
            .check(&subject.0, Action::Read, &landed_target)
            .await?
            == Decision::Deny
        {
            return Err(QueryError::Forbidden);
        }
        let landed_type = deps.ontology.get_type(&landed).await?;
        let (landed_filters, _ld, _lm) = load_policy(deps.acl, &subject.0, &landed_target).await?;
        // Intermediates carry their own row-filters; the FINAL landing is the start type, whose
        // filters are rendered at `nxt` by the compiler -> pass empty here (no double-render).
        let next_filters = if i == last {
            Vec::new()
        } else {
            landed_filters
        };
        steps.push(crate::sql::GraphStep {
            backing: link.backing.clone(),
            next_table: landed_type.table.clone(),
            next_filters,
        });
        current = landed;
    }
    if current != type_name {
        return Err(QueryError::NotCyclicPath(q.path.join(",")));
    }

    // Projection: visible columns minus denied; masked applied. Empty -> Forbidden.
    let allowed = project_allowed(&object_type.properties, &denied);
    if allowed.is_empty() {
        return Err(QueryError::Forbidden);
    }
    let mask_cols: Vec<String> = allowed
        .iter()
        .filter(|c| masked.contains(*c))
        .cloned()
        .collect();

    // Seed predicates: source filters (visibility-checked + coerced) then the ?_ids= set.
    let mut seed_predicates: Vec<crate::filter::CallerPredicate> = Vec::new();
    for (col, raw) in &q.filters {
        if !allowed.contains(col) || masked.contains(col) {
            return Err(QueryError::BadFilter(col.clone()));
        }
        let ty = object_type
            .properties
            .iter()
            .find(|p| &p.name == col)
            .map(|p| p.ty.as_str())
            .unwrap_or("");
        seed_predicates.push(crate::filter::coerce_predicate(col, ty, raw)?);
    }
    if let Some(p) = identity_in_predicate(&object_type, &denied, &masked, &q.ids)? {
        seed_predicates.push(p);
    }

    Ok(GraphResolved {
        object_type,
        identity,
        steps,
        row_filters,
        allowed,
        mask_cols,
        denied,
        masked,
        seed_predicates,
    })
}
```

Then rewrite `read_graph_reach` to consume it (keep its doc comment):

```rust
pub async fn read_graph_reach(
    q: &GraphQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<ObjectRows, QueryError> {
    let r = resolve_graph(q, subject, deps).await?;

    let (sql, params) = crate::sql::compile_graph_reach(
        deps.serving.dialect(),
        &r.object_type.table,
        &r.identity,
        &r.steps,
        &r.seed_predicates,
        &r.row_filters,
        &r.allowed,
        &r.mask_cols,
        q.depth,
        deps.default_limit,
    )?;
    let served = deps.serving.fetch_rows(&sql, &params).await?;
    let logical_types: Vec<String> = r
        .allowed
        .iter()
        .map(|name| {
            r.object_type
                .properties
                .iter()
                .find(|p| &p.name == name)
                .map(|p| p.ty.clone())
                .unwrap_or_default()
        })
        .collect();
    debug_assert_eq!(
        served.columns, r.allowed,
        "serving engine returned columns out of the projected order"
    );
    Ok(ObjectRows {
        columns: r.allowed,
        logical_types,
        rows: served.rows,
    })
}
```

- [ ] **Step 3: Run the guarding tests to verify unchanged behavior**

Run: `buck2 test //src/services/query-api:graph-reach //src/services/query-api:graph-reach-e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (both unchanged).

- [ ] **Step 4: Clippy + commit**

Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log` (empty == clean).

```bash
git add src/services/query-api/src/handler.rs
git commit -m "refactor(query-api): extract resolve_graph shared by graph reads"
```

---

## Task 3: `ObjectTree`/`TreeNode` types, `read_graph_tree` handler, `tree_to_json` renderer

**Files:**
- Modify: `src/services/query-api/src/handler.rs` (add types + `read_graph_tree`)
- Modify: `src/services/query-api/src/render.rs` (add `tree_to_json`, make `render_cell` `pub(crate)`)
- Test: `src/services/query-api/tests/graph_tree.rs` (create)
- Modify: `src/services/query-api/BUCK` (add `graph-tree` target)

**Interfaces:**
- Consumes: `resolve_graph`/`GraphResolved` (Task 2), `compile_graph_tree`/`TREE_*_COL` (Task 1), `identity_is_governed` (existing), `render_cell` (existing, now `pub(crate)`).
- Produces:
  ```rust
  // handler.rs
  #[derive(Debug)]
  pub struct TreeNode {
      pub id: SqlValue,      // this node's identity value
      pub depth: i64,        // BFS depth (0 for roots)
      pub parent: SqlValue,  // predecessor identity value; SqlValue::Null for a root
      pub cells: Vec<SqlValue>, // object cells, aligned to ObjectTree.columns
  }
  #[derive(Debug)]
  pub struct ObjectTree {
      pub columns: Vec<String>,       // object projection columns (SELECT order)
      pub logical_types: Vec<String>, // aligned to columns
      pub identity_type: String,      // logical type of identity (renders id + parent)
      pub nodes: Vec<TreeNode>,       // ordered by (depth, id)
  }
  pub async fn read_graph_tree(
      q: &GraphQuery,
      subject: &Subject,
      deps: &QueryDeps<'_>,
  ) -> Result<ObjectTree, QueryError>;

  // render.rs
  pub fn tree_to_json(tree: &ObjectTree) -> serde_json::Value;
  ```

- [ ] **Step 1: Write the failing handler test**

Create `src/services/query-api/tests/graph_tree.rs`:

```rust
//! read_graph_tree on an in-memory control plane + a stub serving engine. The stub returns
//! canned rows shaped as compile_graph_tree projects: [allowed cols..., __depth, __parent,
//! __id]. Asserts governance short-circuits (NoIdentity; masked/denied identity -> Forbidden),
//! and the happy path builds a tree with a parentless root and correct parent pointers.
//! Real recursive determinism is covered by the e2e (Task 5).

use std::time::Duration;

use async_trait::async_trait;
use control_plane_core::{
    Acl, Action, Cardinality, Effect, LinkBacking, LinkDef, ObjectType, Ontology, Policy,
    PolicyTarget, PropertyDef, RoleId, SubjectId, TableRef, TypeName,
};
use control_plane_memory::MemoryControlPlane;
use query_api::handler::{GraphQuery, QueryDeps, QueryError, Subject, read_graph_tree};
use query_api::serving::{Rows, ServingEngine, ServingError, SqlValue};

/// Serving stub returning canned tree rows: [id, name, __depth, __parent, __id].
struct TreeServing {
    rows: Vec<Vec<SqlValue>>,
}

#[async_trait]
impl ServingEngine for TreeServing {
    async fn fetch_rows(
        &self,
        _sql: &str,
        _params: &[SqlValue],
    ) -> std::result::Result<Rows, ServingError> {
        Ok(Rows {
            columns: vec![
                "id".into(),
                "name".into(),
                "__depth".into(),
                "__parent".into(),
                "__id".into(),
            ],
            rows: self.rows.clone(),
        })
    }
}

fn person_type(identity: Option<String>) -> ObjectType {
    ObjectType {
        name: TypeName("Person".into()),
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
            PropertyDef {
                name: "name".into(),
                ty: "Text".into(),
                required: false,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
        ],
        derived: vec![],
        table: TableRef {
            schema: "main".into(),
            name: "person".into(),
        },
        identity,
    }
}

/// Person with a `knows` FK self-link and an analyst granted Read on Person. Returns the CP,
/// the analyst subject, and its role (so a caller can layer a mask/deny policy).
async fn seeded(person: ObjectType) -> (MemoryControlPlane, SubjectId, RoleId) {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    cp.define_type(person).await.unwrap();
    cp.define_link(LinkDef {
        name: "knows".into(),
        from: TypeName("Person".into()),
        to: TypeName("Person".into()),
        cardinality: Cardinality::Many,
        backing: LinkBacking::ForeignKey {
            from_column: "knows_id".into(),
            to_column: "id".into(),
        },
    })
    .await
    .unwrap();
    let analyst = SubjectId("analyst".into());
    let reader = RoleId("reader".into());
    cp.define_subject(&analyst).await.unwrap();
    cp.define_role(&reader).await.unwrap();
    cp.assign_role(&analyst, &reader).await.unwrap();
    cp.grant(
        &reader,
        Action::Read,
        PolicyTarget::Type(TypeName("Person".into())),
        Effect::Allow,
    )
    .await
    .unwrap();
    (cp, analyst, reader)
}

fn graph_query() -> GraphQuery {
    GraphQuery {
        type_name: "Person".into(),
        path: vec!["knows".into()],
        depth: 3,
        filters: vec![],
        ids: vec![],
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn rejects_a_type_without_identity() {
    let (cp, subj, _role) = seeded(person_type(None)).await;
    let serving = TreeServing { rows: vec![] };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &serving,
        default_limit: 1000,
    };
    let err = read_graph_tree(&graph_query(), &Subject(subj), &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, QueryError::NoIdentity(t) if t == "Person"),
        "expected NoIdentity(Person), got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn masked_identity_is_forbidden() {
    // The tree PROJECTS identity as id/parent, so a masked identity cannot be served.
    let (cp, subj, role) = seeded(person_type(Some("id".into()))).await;
    cp.set_policy(
        &role,
        Action::Read,
        Policy {
            target: PolicyTarget::Type(TypeName("Person".into())),
            row_filter: None,
            deny_columns: vec![],
            mask_columns: vec!["id".into()],
        },
    )
    .await
    .unwrap();
    let serving = TreeServing { rows: vec![] };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &serving,
        default_limit: 1000,
    };
    let err = read_graph_tree(&graph_query(), &Subject(subj), &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(err, QueryError::Forbidden),
        "masked identity -> Forbidden, got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn builds_tree_with_root_and_parent_pointers() {
    let (cp, subj, _role) = seeded(person_type(Some("id".into()))).await;
    // A linear tree 5 -> 7 -> 9: 5 is the root (parent NULL), 7's parent is 5, 9's parent is 7.
    let serving = TreeServing {
        rows: vec![
            vec![
                SqlValue::Int(5),
                SqlValue::Text("ann".into()),
                SqlValue::Int(0),
                SqlValue::Null,
                SqlValue::Int(5),
            ],
            vec![
                SqlValue::Int(7),
                SqlValue::Text("bob".into()),
                SqlValue::Int(1),
                SqlValue::Int(5),
                SqlValue::Int(7),
            ],
            vec![
                SqlValue::Int(9),
                SqlValue::Text("cal".into()),
                SqlValue::Int(2),
                SqlValue::Int(7),
                SqlValue::Int(9),
            ],
        ],
    };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &serving,
        default_limit: 1000,
    };
    let tree = read_graph_tree(&graph_query(), &Subject(subj), &deps)
        .await
        .unwrap();
    assert_eq!(tree.columns, vec!["id".to_string(), "name".to_string()]);
    assert_eq!(tree.identity_type, "Long");
    assert_eq!(tree.nodes.len(), 3);
    // root
    assert_eq!(tree.nodes[0].id, SqlValue::Int(5));
    assert_eq!(tree.nodes[0].depth, 0);
    assert_eq!(tree.nodes[0].parent, SqlValue::Null);
    assert_eq!(tree.nodes[0].cells, vec![SqlValue::Int(5), SqlValue::Text("ann".into())]);
    // child
    assert_eq!(tree.nodes[1].parent, SqlValue::Int(5));
    assert_eq!(tree.nodes[2].parent, SqlValue::Int(7));

    // Render to JSON: roots list + node objects. NOTE: identity is `Long`, which renders as a
    // numeric STRING on the wire (int64 > JSON safe-int range -> JsonRepr::NumericString), so
    // id/parent/root values are the strings "5"/"7", not the numbers 5/7. `depth` stays a
    // number (a synthetic BFS integer). `name` is unknown-type -> natural string rendering.
    let body = query_api::render::tree_to_json(&tree);
    assert_eq!(body["roots"], serde_json::json!(["5"]));
    assert_eq!(body["nodes"][0]["id"], serde_json::json!("5"));
    assert_eq!(body["nodes"][0]["parent"], serde_json::Value::Null);
    assert_eq!(body["nodes"][0]["depth"], serde_json::json!(0));
    assert_eq!(body["nodes"][0]["object"]["name"], serde_json::json!("ann"));
    assert_eq!(body["nodes"][1]["parent"], serde_json::json!("5"));
}
```

- [ ] **Step 2: Add the `graph-tree` BUCK target**

In `src/services/query-api/BUCK`, after the `graph-reach` target (~line 719):

```python
rust_test(
    name = "graph-tree",
    crate = "graph_tree",
    srcs = ["tests/graph_tree.rs"],
    crate_root = "tests/graph_tree.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
        "//src/control-plane/memory:memory",
        "//third-party:async-trait",
        "//third-party:serde_json",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `buck2 test //src/services/query-api:graph-tree > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|cannot find|error\[" /tmp/t.log`
Expected: FAIL — `read_graph_tree`/`tree_to_json`/`ObjectTree` not found.

- [ ] **Step 4: Add `ObjectTree`/`TreeNode` + `read_graph_tree` in `handler.rs`**

Add the result types near `ObjectRows` (after ~line 43):

```rust
/// One node of a shortest-path tree: its identity value, BFS depth (0 = root), predecessor
/// identity value (`SqlValue::Null` for a root), and the governed object cells aligned to
/// `ObjectTree.columns`.
#[derive(Debug)]
pub struct TreeNode {
    pub id: SqlValue,
    pub depth: i64,
    pub parent: SqlValue,
    pub cells: Vec<SqlValue>,
}

/// A governed shortest-path-tree read result: the object projection columns + their logical
/// types (positionally aligned to each node's `cells`), the identity property's logical type
/// (renders each node's `id` and `parent`), and the nodes ordered by `(depth, id)`.
#[derive(Debug)]
pub struct ObjectTree {
    pub columns: Vec<String>,
    pub logical_types: Vec<String>,
    pub identity_type: String,
    pub nodes: Vec<TreeNode>,
}
```

Add `read_graph_tree` after `read_graph_reach`:

```rust
/// Serve a bounded shortest-path-**tree** read over a path-cycle (a 1-element path is the
/// single-self-link case): from the seed set, repeat `path` up to `depth` times, and for every
/// reachable node return its shortest-path parent pointer + BFS depth, rooted at the seed set.
/// Governance is `read_graph_reach`'s exactly (Read on the queried + every intermediate type;
/// row-filters at seed/expansion/projection so no denied intermediate can be a parent), plus
/// one precondition: because the tree PROJECTS identity as `id`/`parent`, a denied or masked
/// identity cannot be served without leaking it -> `Forbidden` (undeclared identity is already
/// `NoIdentity` from `resolve_graph`). The compiler emits no LIMIT; the depth cap bounds the
/// forest so a parent is never dropped while a child is kept.
pub async fn read_graph_tree(
    q: &GraphQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<ObjectTree, QueryError> {
    let r = resolve_graph(q, subject, deps).await?;

    // The tree projects identity (id + parent). A denied/masked identity would leak -> Forbidden.
    if identity_is_governed(&r.object_type, &r.denied, &r.masked) {
        return Err(QueryError::Forbidden);
    }

    let (sql, params) = crate::sql::compile_graph_tree(
        deps.serving.dialect(),
        &r.object_type.table,
        &r.identity,
        &r.steps,
        &r.seed_predicates,
        &r.row_filters,
        &r.allowed,
        &r.mask_cols,
        q.depth,
    )?;
    let served = deps.serving.fetch_rows(&sql, &params).await?;

    let logical_types: Vec<String> = r
        .allowed
        .iter()
        .map(|name| {
            r.object_type
                .properties
                .iter()
                .find(|p| &p.name == name)
                .map(|p| p.ty.clone())
                .unwrap_or_default()
        })
        .collect();
    let identity_type = r
        .object_type
        .properties
        .iter()
        .find(|p| p.name == r.identity)
        .map(|p| p.ty.clone())
        .unwrap_or_default();

    // Each served row is [object cells..., __depth, __parent, __id] (compile_graph_tree order).
    // Pop the three trailing columns off the end; the remainder are the object cells.
    let ncols = r.allowed.len();
    let nodes: Vec<TreeNode> = served
        .rows
        .into_iter()
        .map(|mut row| {
            let node_id = row.pop().unwrap_or(SqlValue::Null); // __id
            let parent = row.pop().unwrap_or(SqlValue::Null); // __parent
            let depth = match row.pop() {
                Some(SqlValue::Int(d)) => d,
                _ => 0, // a serving-engine surprise must not panic a permitted read
            };
            row.truncate(ncols); // defensive: keep exactly the object cells
            TreeNode {
                id: node_id,
                depth,
                parent,
                cells: row,
            }
        })
        .collect();

    Ok(ObjectTree {
        columns: r.allowed,
        logical_types,
        identity_type,
        nodes,
    })
}
```

Note: `SqlValue` is already imported in `handler.rs` (`use crate::serving::{Rows, ServingEngine, SqlValue};`). `identity_is_governed` is defined in this file.

- [ ] **Step 5: Add `tree_to_json` in `render.rs` + make `render_cell` `pub(crate)`**

In `src/services/query-api/src/render.rs`:

Change the import line to include `ObjectTree`:

```rust
use crate::handler::{Associations, ObjectRows, ObjectTree};
```

Change `fn render_cell` to `pub(crate) fn render_cell` (line ~52).

Add the renderer after `objects_to_json`:

```rust
/// `{ "roots": [<id>, ...], "nodes": [ { "id", "depth", "parent", "object": {..} }, ... ] }`.
/// Roots are the parentless nodes (in node order, which is `(depth, id)`). `id`/`parent` are
/// rendered through the identity property's logical type (`parent` is JSON `null` for a root);
/// `object` is the governed typed projection, rendered exactly as `objects_to_json` does a row.
pub fn tree_to_json(tree: &ObjectTree) -> Value {
    let mut roots: Vec<Value> = Vec::new();
    let nodes: Vec<Value> = tree
        .nodes
        .iter()
        .map(|n| {
            let mut obj = serde_json::Map::with_capacity(tree.columns.len());
            for (i, col) in tree.columns.iter().enumerate() {
                let logical_ty = tree.logical_types.get(i).map(String::as_str).unwrap_or("");
                let cell = n.cells.get(i).unwrap_or(&SqlValue::Null);
                obj.insert(col.clone(), render_cell(logical_ty, cell));
            }
            let id_json = render_cell(&tree.identity_type, &n.id);
            let parent_json = render_cell(&tree.identity_type, &n.parent);
            if matches!(n.parent, SqlValue::Null) {
                roots.push(id_json.clone());
            }
            json!({
                "id": id_json,
                "depth": n.depth,
                "parent": parent_json,
                "object": Value::Object(obj),
            })
        })
        .collect();
    json!({ "roots": roots, "nodes": nodes })
}
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `buck2 test //src/services/query-api:graph-tree > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (3 tests).

- [ ] **Step 7: Clippy + commit**

Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log` (empty == clean).

```bash
git add src/services/query-api/src/handler.rs src/services/query-api/src/render.rs src/services/query-api/tests/graph_tree.rs src/services/query-api/BUCK
git commit -m "feat(query-api): read_graph_tree handler + tree_to_json renderer"
```

---

## Task 4: HTTP `?tree=` flag wiring, OpenAPI schema, and happy-path e2e

**Files:**
- Modify: `src/services/query-api/src/http.rs` (parse `?tree=` on both graph routes; add `graph_tree_respond`; reject tree+union/tree+starred)
- Modify: `src/services/query-api/src/openapi.rs` (add `ObjectTreeResponse`, register it)
- Modify: `src/services/query-api/tests/e2e_support.rs` (add `tree_nodes` extractor)
- Modify: `src/services/query-api/tests/e2e_support.rs` deps already include serde_json (no BUCK change for support lib)
- Test: `src/services/query-api/tests/graph_tree_e2e.rs` (create; happy path + reject cases)
- Modify: `src/services/query-api/BUCK` (add `graph-tree-e2e` target)

**Interfaces:**
- Consumes: `read_graph_tree` + `ObjectTree` (Task 3), `crate::render::tree_to_json`, the `get`/`grant_read`/`subject_with_role`/`tref`/`prop`/`InProcessServingEngine` e2e helpers.
- Produces (in `e2e_support.rs`):
  ```rust
  /// Extract (id_i64, depth_i64, parent) triples from a {roots, nodes} tree body, in node
  /// order. `parent` is `None` for a root (JSON null), else `Some(i64)`.
  pub fn tree_nodes(body: &serde_json::Value) -> Vec<(i64, i64, Option<i64>)>;
  /// The tree's root identity values as i64s, in response order.
  pub fn tree_roots(body: &serde_json::Value) -> Vec<i64>;
  ```
  **Wire-format note:** a `Long` identity renders on the JSON wire as a **numeric string**
  (`"5"`, not `5`) — int64 exceeds JSON's safe-integer range, so `render_cell` maps
  `BaseType::Long → JsonRepr::NumericString` (see the existing `ids_i64` helper). The
  extractors below therefore parse ids from strings; `depth` is a small synthetic BFS integer
  and stays a JSON number.
- Produces (in `http.rs`): a private `graph_tree_respond` and `?tree=` handling; no public signature change.

- [ ] **Step 1: Add the `tree_nodes` extractor to `e2e_support.rs`**

In `src/services/query-api/tests/e2e_support.rs`, beside `ids_i64` (~line 275), add:

```rust
/// Read an identity value that may render as a JSON number OR a numeric string (`Long`
/// identities render as numeric strings — see `ids_i64`). `None` for JSON `null`.
fn as_id(v: &serde_json::Value) -> Option<i64> {
    v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
}

/// Extract `(id, depth, parent)` triples from a `{ "roots": [...], "nodes": [...] }`
/// shortest-path-tree body, in node order. `parent` is `None` for a root (JSON `null`).
/// Ids parse via `as_id` (Long ids are numeric strings); `depth` is a JSON number.
pub fn tree_nodes(body: &serde_json::Value) -> Vec<(i64, i64, Option<i64>)> {
    body["nodes"]
        .as_array()
        .map(|ns| {
            ns.iter()
                .map(|n| {
                    let id = as_id(&n["id"]).expect("node id");
                    let depth = n["depth"].as_i64().expect("node depth i64");
                    let parent = as_id(&n["parent"]);
                    (id, depth, parent)
                })
                .collect()
        })
        .unwrap_or_default()
}

/// The tree's root identity values as i64s, in response order.
pub fn tree_roots(body: &serde_json::Value) -> Vec<i64> {
    body["roots"]
        .as_array()
        .map(|rs| rs.iter().filter_map(as_id).collect())
        .unwrap_or_default()
}
```

(`e2e_support.rs` carries a crate-level `#![allow(...)]` for panic-safety lints, so `expect` is fine here.)

- [ ] **Step 2: Write the failing happy-path + reject e2e**

Create `src/services/query-api/tests/graph_tree_e2e.rs`. This first commit covers the happy path over HTTP and the two reject cases; Task 5 adds determinism/reconstruction/cycle/ACL/forest.

```rust
//! Shortest-path-tree e2e: GET /objects/:type/graph/:link?tree=true and
//! /objects/:type/graph?path=...&tree=true over the real HTTP router backed by an in-process
//! Iceberg/DataFusion serving engine. This file proves the happy path (a {roots, nodes} tree
//! with parent pointers) and that tree is rejected on the union/recursive-core routes. Further
//! determinism/reconstruction/cycle/ACL/forest cases are added in Task 5.
//!
//! Graph: a single `Person` table with a `knows(a, b)` join-table SELF-link. Edge set for the
//! happy path: 1->2, 2->3 (a linear chain). Person declares identity `id`.

use std::sync::Arc;

use axum::http::StatusCode;
use control_plane_core::{
    Cardinality, LinkBacking, LinkDef, ObjectType, Ontology, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{IcebergWriter, PgFixture, SeedCol};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use e2e_support::{
    InProcessServingEngine, get, grant_read, prop, subject_with_role, tree_nodes, tree_roots, tref,
};

/// Seed `person(id, name)` with a `knows(a, b)` join-table self-link. `edges` is the (a, b)
/// edge set. Person declares identity `id`. Caller MUST keep the returned IcebergWriter alive.
async fn setup(
    fx: &PgFixture,
    ids: Vec<i64>,
    names: Vec<&str>,
    edges: &[(i64, i64)],
) -> (PgControlPlane, InProcessServingEngine, IcebergWriter) {
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let writer = IcebergWriter::new(pool.clone(), dsn);

    let person = tref("main", "person");
    writer
        .seed_arrays(
            "main",
            "person",
            &[
                ("id".to_string(), "long".to_string(), false),
                ("name".to_string(), "string".to_string(), true),
            ],
            &[SeedCol::Long(ids), SeedCol::Str(names)],
        )
        .await;

    let (a, b): (Vec<i64>, Vec<i64>) = edges.iter().copied().unzip();
    let knows = tref("main", "knows");
    writer
        .seed_arrays(
            "main",
            "knows",
            &[
                ("a".to_string(), "long".to_string(), false),
                ("b".to_string(), "long".to_string(), false),
            ],
            &[SeedCol::Long(a), SeedCol::Long(b)],
        )
        .await;

    cp.define_type(ObjectType {
        name: TypeName("Person".into()),
        properties: vec![prop("id", "Long", true), prop("name", "String", false)],
        derived: vec![],
        table: person.clone(),
        identity: Some("id".into()),
    })
    .await
    .unwrap();
    cp.define_link(LinkDef {
        name: "knows".into(),
        from: TypeName("Person".into()),
        to: TypeName("Person".into()),
        cardinality: Cardinality::Many,
        backing: LinkBacking::JoinTable {
            table: knows.clone(),
            from_key: "id".into(),
            from_column: "a".into(),
            to_column: "b".into(),
            to_key: "id".into(),
        },
    })
    .await
    .unwrap();

    let catalog = IcebergCatalog::new(pool.clone());
    let eng = InProcessServingEngine::new(catalog);
    (cp, eng, writer)
}

#[tokio::test(flavor = "multi_thread")]
async fn linear_chain_tree_has_root_and_parent_pointers() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) =
        setup(&fx, vec![1, 2, 3], vec!["ann", "bob", "cal"], &[(1, 2), (2, 3)]).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;

    // depth=3 from {1}: tree 1(root) -> 2 -> 3.
    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph/knows?depth=3&_ids=1&tree=true",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(tree_roots(&body), vec![1], "root is the seed: {body}");
    let nodes = tree_nodes(&body);
    assert_eq!(
        nodes,
        vec![(1, 0, None), (2, 1, Some(1)), (3, 2, Some(2))],
        "root + parent pointers along the chain: {body}"
    );
    // the object projection is present per node
    assert_eq!(body["nodes"][0]["object"]["name"], serde_json::json!("ann"), "{body}");
}

#[tokio::test(flavor = "multi_thread")]
async fn tree_with_invalid_value_is_400() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx, vec![1, 2], vec!["ann", "bob"], &[(1, 2)]).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;

    // ?tree=maybe is neither true nor false -> 400.
    let (status, _body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph/knows?depth=2&_ids=1&tree=maybe",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "tree=maybe -> 400");
}

#[tokio::test(flavor = "multi_thread")]
async fn without_flag_returns_the_flat_set() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) =
        setup(&fx, vec![1, 2, 3], vec!["ann", "bob", "cal"], &[(1, 2), (2, 3)]).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;

    // No ?tree= -> the reachable SET shape {objects:[...]}, unchanged.
    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph/knows?depth=3&_ids=1",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.get("objects").is_some(), "flat set shape without the flag: {body}");
    assert!(body.get("roots").is_none(), "no tree shape without the flag: {body}");
}

#[tokio::test(flavor = "multi_thread")]
async fn tree_with_links_union_is_400() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx, vec![1, 2], vec!["ann", "bob"], &[(1, 2)]).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;

    // ?links= (union) + tree -> 400 (tree over union is a deferred follow-on).
    let (status, _body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph?links=knows&depth=2&_ids=1&tree=true",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "tree + links -> 400");
}

#[tokio::test(flavor = "multi_thread")]
async fn tree_with_starred_path_is_400() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx, vec![1, 2], vec!["ann", "bob"], &[(1, 2)]).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;

    // ?path=knows* (recursive-core + tail) + tree -> 400 (deferred follow-on).
    let (status, _body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph?path=knows*&depth=2&_ids=1&tree=true",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "tree + starred path -> 400");
}
```

- [ ] **Step 3: Add the `graph-tree-e2e` BUCK target**

In `src/services/query-api/BUCK`, after the `graph-tail-e2e` target (~line 361):

```python
loom_fixture_test(
    name = "graph-tree-e2e",
    crate = "graph_tree_e2e",
    srcs = ["tests/graph_tree_e2e.rs"],
    crate_root = "tests/graph_tree_e2e.rs",
    deps = [
        ":query-api",
        ":e2e-support",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:axum",
        "//third-party:serde_json",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 4: Run the e2e to verify it fails**

Run: `buck2 test //src/services/query-api:graph-tree-e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[|cannot find" /tmp/t.log`
Expected: FAIL — `?tree=` not yet parsed (tree flag ignored → set shape has no `roots`), and `tree_nodes` not found until Step 1 lands. (After Step 1, the compile error is gone; the behavior asserts fail.)

- [ ] **Step 5: Add a shared bool-token parser + `?tree=` handling in `http.rs`**

In `src/services/query-api/src/http.rs`, add a small helper near the graph consts (~line 374):

```rust
/// Parse a `?tree=` (or similar) boolean flag token. Accepts `true`/`false` (case-insensitive);
/// any other value is a 400. Absent -> `false` (the caller defaults before calling this).
fn parse_bool_flag(v: &str) -> Result<bool, ()> {
    match v.to_ascii_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(()),
    }
}
```

In `get_graph` (single self-link), add a `tree` accumulator and parse the param. Update the loop and post-loop dispatch:

```rust
    let mut depth = DEFAULT_GRAPH_DEPTH;
    let mut ids: Vec<String> = Vec::new();
    let mut ids_present = false;
    let mut tree = false;
    let mut filters: Vec<(String, String)> = Vec::with_capacity(params.len());
    for (k, v) in params {
        match k.as_str() {
            "depth" => match v.parse::<u32>() {
                Ok(d) => depth = d,
                Err(_) => {
                    return (StatusCode::BAD_REQUEST, "depth must be a positive integer")
                        .into_response();
                }
            },
            "_ids" => {
                ids_present = true;
                ids = v
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(String::from)
                    .collect();
            }
            "tree" => match parse_bool_flag(&v) {
                Ok(t) => tree = t,
                Err(()) => {
                    return (StatusCode::BAD_REQUEST, "tree must be true or false")
                        .into_response();
                }
            },
            _ => filters.push((k, v)),
        }
    }
```

Then, after the `depth` range check (~line 425), before the final `graph_respond` call, dispatch on the flag. Find the tail of `get_graph`:

```rust
    // existing:  graph_respond(&st, type_name, vec![link_name], depth, filters, ids, &subject).await
    if tree {
        graph_tree_respond(&st, type_name, vec![link_name], depth, filters, ids, &subject).await
    } else {
        graph_respond(&st, type_name, vec![link_name], depth, filters, ids, &subject).await
    }
```

In `get_graph_path`, add a `let mut tree = false;` accumulator beside the others, and add this arm to its `match k.as_str()` param loop (identical to `get_graph`'s):

```rust
            "tree" => match parse_bool_flag(&v) {
                Ok(t) => tree = t,
                Err(()) => {
                    return (StatusCode::BAD_REQUEST, "tree must be true or false")
                        .into_response();
                }
            },
```

Then thread the flag through the mode dispatch: **tree is only valid on the path-cycle route.** After the existing validation but before dispatching:

- If `tree` and `!links.is_empty()` → 400 (`"tree view is not supported with links (union); use a single link or path"`).
- The `starred` handling: if `tree` and `!starred.is_empty()` → 400 (`"tree view is not supported for a recursive-core path (*)"`) — place this check inside the `if !starred.is_empty()` block, before `graph_tail_respond`.
- The final path-cycle dispatch selects `graph_tree_respond` when `tree`.

Concretely, edit `get_graph_path`:

```rust
    if !links.is_empty() {
        if tree {
            return (
                StatusCode::BAD_REQUEST,
                "tree view is not supported with links (union)",
            )
                .into_response();
        }
        return graph_union_respond(&st, type_name, links, depth, filters, ids, &subject).await;
    }
```

```rust
    if !starred.is_empty() {
        if tree {
            return (
                StatusCode::BAD_REQUEST,
                "tree view is not supported for a recursive-core (*) path",
            )
                .into_response();
        }
        // ... existing starred handling unchanged ...
    }
```

```rust
    // final path-cycle dispatch (replace the trailing `graph_respond(...)` call)
    if tree {
        graph_tree_respond(&st, type_name, path, depth, filters, ids, &subject).await
    } else {
        graph_respond(&st, type_name, path, depth, filters, ids, &subject).await
    }
```

Add `graph_tree_respond` beside `graph_respond` (~line 625):

```rust
/// Tree tail for `?tree=true` on the single `/graph/:link` and `?path=` path-cycle routes:
/// build a `GraphQuery`, run `read_graph_tree`, render `{roots, nodes}` via `tree_to_json`,
/// map errors via `graph_error`.
async fn graph_tree_respond(
    st: &AppState,
    type_name: String,
    path: Vec<String>,
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
    match read_graph_tree(
        &GraphQuery {
            type_name,
            path,
            depth,
            filters,
            ids,
        },
        subject,
        &deps,
    )
    .await
    {
        Ok(tree) => Json(crate::render::tree_to_json(&tree)).into_response(),
        Err(e) => graph_error(e),
    }
}
```

Add `read_graph_tree` to the `use crate::handler::{...}` import at the top of `http.rs` (line ~9-10, the `read_graph_*` group).

- [ ] **Step 6: Add the `ObjectTreeResponse` OpenAPI schema**

In `src/services/query-api/src/openapi.rs`, add after `ObjectsResponse` (~line 16):

```rust
/// Documentation shape for the `{ "roots": [...], "nodes": [...] }` shortest-path-tree
/// response served when `?tree=true` is set on the graph routes. `roots` are identity values;
/// each node carries `id`, `depth`, `parent` (null for a root) and the governed typed `object`.
#[derive(ToSchema)]
pub struct ObjectTreeResponse {
    pub roots: Vec<serde_json::Value>,
    pub nodes: Vec<serde_json::Value>,
}
```

Register it in `components(schemas(...))` (add the line beside `ObjectsResponse`):

```rust
        ObjectsResponse,
        ObjectTreeResponse,
```

- [ ] **Step 7: Run the e2e to verify it passes**

Run: `buck2 test //src/services/query-api:graph-tree-e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (4 tests). This confirms the DataFusion serving path runs the `settled` window + `NULLIF` anchor (open question #4 resolved empirically).

- [ ] **Step 8: Clippy + regression + commit**

Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log` (empty == clean).
Run: `buck2 test //src/services/query-api:graph-reach-e2e //src/services/query-api:graph-path-e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log` (set routes unchanged).
Expected: PASS.

```bash
git add src/services/query-api/src/http.rs src/services/query-api/src/openapi.rs src/services/query-api/tests/e2e_support.rs src/services/query-api/tests/graph_tree_e2e.rs src/services/query-api/BUCK
git commit -m "feat(query-api): ?tree= flag routes to the shortest-path tree over HTTP"
```

---

## Task 5: Determinism, reconstruction, cycle, ACL, and forest e2e cases

**Files:**
- Modify: `src/services/query-api/tests/graph_tree_e2e.rs` (add cases to the file created in Task 4)

**Interfaces:**
- Consumes: the `setup` helper + `tree_nodes` from Task 4. Adds `RowFilter`/`Policy`/`Action` usage for the ACL case (extend the `use control_plane_core::{...}` list and add `set_policy`).

- [ ] **Step 1: Write the determinism (two shortest paths) failing case**

Append to `graph_tree_e2e.rs`. The determinism case seeds `1->2, 1->3, 2->4, 3->4`: node 4 is depth-2 via parent 2 OR 3; the tie-break must pick the **smaller** parent (2) and hold across repeated runs.

```rust
#[tokio::test(flavor = "multi_thread")]
async fn tie_break_picks_smallest_parent_deterministically() {
    let fx = PgFixture::start();
    // Two shortest paths to 4: 1->2->4 and 1->3->4. Tie-break => parent(4) == 2 (smaller id).
    let (cp, eng, _writer) = setup(
        &fx,
        vec![1, 2, 3, 4],
        vec!["a", "b", "c", "d"],
        &[(1, 2), (1, 3), (2, 4), (3, 4)],
    )
    .await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;

    // Run several times; the settled tie-break must be scan/join-order independent.
    for run in 0..5 {
        let (status, body) = get(
            cp.clone(),
            eng.clone(),
            "/objects/Person/graph/knows?depth=3&_ids=1&tree=true",
            "alice",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "run {run}: {body}");
        let nodes = tree_nodes(&body);
        let four = nodes
            .iter()
            .find(|(id, _, _)| *id == 4)
            .copied()
            .expect("node 4 present");
        assert_eq!(four, (4, 2, Some(2)), "run {run}: 4 settles to depth 2, parent 2: {body}");
        assert_eq!(tree_roots(&body), vec![1], "run {run}: root is 1: {body}");
    }
}
```

- [ ] **Step 2: Write the reconstruction case**

```rust
#[tokio::test(flavor = "multi_thread")]
async fn parent_pointers_form_a_valid_rooted_tree() {
    let fx = PgFixture::start();
    // Linear plus a branch: 1->2->3, 2->4. Every non-root has a parent that is itself in the
    // tree, and reconstructed depth == reported depth (no dangling parent).
    let (cp, eng, _writer) = setup(
        &fx,
        vec![1, 2, 3, 4],
        vec!["a", "b", "c", "d"],
        &[(1, 2), (2, 3), (2, 4)],
    )
    .await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;

    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph/knows?depth=3&_ids=1&tree=true",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let nodes = tree_nodes(&body);
    let depth_of: std::collections::HashMap<i64, i64> =
        nodes.iter().map(|(id, d, _)| (*id, *d)).collect();
    for (id, depth, parent) in &nodes {
        match parent {
            None => assert_eq!(*depth, 0, "a root has depth 0 (id {id}): {body}"),
            Some(p) => {
                let pd = depth_of.get(p).copied().expect("parent present in tree");
                assert_eq!(*depth, pd + 1, "child depth = parent depth + 1 (id {id}): {body}");
            }
        }
    }
    assert_eq!(nodes.len(), 4, "all four nodes present: {body}");
}
```

- [ ] **Step 3: Write the cycle case**

```rust
#[tokio::test(flavor = "multi_thread")]
async fn cycle_terminates_and_seed_stays_a_root() {
    let fx = PgFixture::start();
    // 1->2->3->1 cycle. From {1}, depth 3 terminates; each node settles to its min depth; the
    // seed 1, re-reached by the cycle, stays a depth-0 parentless root.
    let (cp, eng, _writer) = setup(
        &fx,
        vec![1, 2, 3],
        vec!["a", "b", "c"],
        &[(1, 2), (2, 3), (3, 1)],
    )
    .await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;

    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph/knows?depth=3&_ids=1&tree=true",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let mut nodes = tree_nodes(&body);
    nodes.sort_by_key(|(id, _, _)| *id);
    assert_eq!(
        nodes,
        vec![(1, 0, None), (2, 1, Some(1)), (3, 2, Some(2))],
        "cycle terminates; 1 stays a depth-0 root; 2,3 settle to min depth: {body}"
    );
    assert_eq!(tree_roots(&body), vec![1], "{body}");
}
```

- [ ] **Step 4: Write the ACL case (extend imports + set_policy)**

At the top of `graph_tree_e2e.rs`, extend the core import to add `Action, CompareOp, Policy, PolicyTarget, RowFilter, ScalarValue` and `Acl`:

```rust
use control_plane_core::{
    Acl, Action, Cardinality, CompareOp, LinkBacking, LinkDef, ObjectType, Ontology, Policy,
    PolicyTarget, RowFilter, ScalarValue, TypeName,
};
```

Add an `active` column to the seed by introducing a second setup variant local to this case, or extend `setup` — to avoid churn, add a focused helper `setup_active` in this file that seeds `person(id, name, active)`. Then the case:

```rust
/// Seed person(id, name, active) + knows(a,b) self-link with the given edges + per-id active
/// flags. Person declares identity `id`. Caller keeps the IcebergWriter alive.
async fn setup_active(
    fx: &PgFixture,
    ids: Vec<i64>,
    names: Vec<&str>,
    active: Vec<bool>,
    edges: &[(i64, i64)],
) -> (PgControlPlane, InProcessServingEngine, IcebergWriter) {
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let writer = IcebergWriter::new(pool.clone(), dsn);

    let person = tref("main", "person");
    writer
        .seed_arrays(
            "main",
            "person",
            &[
                ("id".to_string(), "long".to_string(), false),
                ("name".to_string(), "string".to_string(), true),
                ("active".to_string(), "boolean".to_string(), false),
            ],
            &[SeedCol::Long(ids), SeedCol::Str(names), SeedCol::Bool(active)],
        )
        .await;

    let (a, b): (Vec<i64>, Vec<i64>) = edges.iter().copied().unzip();
    let knows = tref("main", "knows");
    writer
        .seed_arrays(
            "main",
            "knows",
            &[
                ("a".to_string(), "long".to_string(), false),
                ("b".to_string(), "long".to_string(), false),
            ],
            &[SeedCol::Long(a), SeedCol::Long(b)],
        )
        .await;

    cp.define_type(ObjectType {
        name: TypeName("Person".into()),
        properties: vec![
            prop("id", "Long", true),
            prop("name", "String", false),
            prop("active", "Boolean", true),
        ],
        derived: vec![],
        table: person.clone(),
        identity: Some("id".into()),
    })
    .await
    .unwrap();
    cp.define_link(LinkDef {
        name: "knows".into(),
        from: TypeName("Person".into()),
        to: TypeName("Person".into()),
        cardinality: Cardinality::Many,
        backing: LinkBacking::JoinTable {
            table: knows.clone(),
            from_key: "id".into(),
            from_column: "a".into(),
            to_column: "b".into(),
            to_key: "id".into(),
        },
    })
    .await
    .unwrap();

    let catalog = IcebergCatalog::new(pool.clone());
    let eng = InProcessServingEngine::new(catalog);
    (cp, eng, writer)
}

#[tokio::test(flavor = "multi_thread")]
async fn row_filter_prunes_and_no_node_reports_a_blocked_parent() {
    let fx = PgFixture::start();
    // Chain 1->2->3->4; node 2 is INACTIVE. A Read row-filter active=true removes 2 from the
    // permitted subgraph, so 2 is unreachable and everything reachable ONLY through 2 (3, 4)
    // is pruned. No surviving node may report 2 as its parent.
    let (cp, eng, _writer) = setup_active(
        &fx,
        vec![1, 2, 3, 4],
        vec!["a", "b", "c", "d"],
        vec![true, false, true, true],
        &[(1, 2), (2, 3), (3, 4)],
    )
    .await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_c, role) = subject_with_role(&cp, "carol").await;
    grant_read(&cp, &role, "Person").await;
    cp.set_policy(
        &role,
        Action::Read,
        Policy {
            target: PolicyTarget::Type(TypeName("Person".into())),
            row_filter: Some(RowFilter::Compare {
                property: "active".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Bool(true),
            }),
            deny_columns: vec![],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();

    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph/knows?depth=3&_ids=1&tree=true",
        "carol",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let nodes = tree_nodes(&body);
    // Only the root 1 survives (its sole first hop 2 is filtered out).
    assert_eq!(nodes, vec![(1, 0, None)], "reachability through inactive 2 is cut: {body}");
    // No node reports the blocked node 2 as a parent.
    assert!(
        nodes.iter().all(|(_, _, parent)| *parent != Some(2)),
        "no surviving node points at the denied intermediate 2: {body}"
    );
}
```

Note: `PgControlPlane` implements `set_policy` (an `Acl` method); the `Acl` trait import brings it into scope.

- [ ] **Step 5: Write the forest case**

```rust
#[tokio::test(flavor = "multi_thread")]
async fn two_seeds_produce_two_roots() {
    let fx = PgFixture::start();
    // Disjoint components 1->2 and 10->11. Seeding {1, 10} yields a forest with roots 1 and 10.
    let (cp, eng, _writer) = setup(
        &fx,
        vec![1, 2, 10, 11],
        vec!["a", "b", "x", "y"],
        &[(1, 2), (10, 11)],
    )
    .await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;

    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph/knows?depth=3&_ids=1,10&tree=true",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(tree_roots(&body), vec![1, 10], "two roots (forest): {body}");
    let mut nodes = tree_nodes(&body);
    nodes.sort_by_key(|(id, _, _)| *id);
    assert_eq!(
        nodes,
        vec![(1, 0, None), (2, 1, Some(1)), (10, 0, None), (11, 1, Some(10))],
        "each seed is a root of its component: {body}"
    );
}
```

- [ ] **Step 6: Run the full tree e2e suite**

Run: `buck2 test //src/services/query-api:graph-tree-e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (all cases: happy path + 2 rejects from Task 4, plus determinism, reconstruction, cycle, ACL, forest).

- [ ] **Step 7: Clippy + commit**

Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log` (empty == clean — note tests build under the `loom_fixture_test` wrapper, so panic-safety lints are exempted in the test crate).

```bash
git add src/services/query-api/tests/graph_tree_e2e.rs
git commit -m "test(query-api): shortest-path-tree e2e — determinism, cycle, ACL, forest"
```

---

## Task 6: Close the register items (docs)

**Files:**
- Modify: `docs/ROADMAP.md` (`road-shortest-path-tree` → done, add `pr:#N`)
- Modify: `docs/FUTURE.md` (`fut-shortest-path-tree` → promoted; `fut-min-depth-annotation` → the tree's per-node depth delivers it for the tree shape — mark promoted/dropped per the spec's note)

**Interfaces:** none (docs only). Use the `loom-docs-update` skill so the grammar/validation stays correct; do not hand-edit the tag blocks blind.

- [ ] **Step 1: Run `loom-docs-update`**

Invoke the `loom-docs-update` skill. Close `road-shortest-path-tree` (`- [ ]`→`- [x]`, `status:planned`→`status:done`, set `pr:#<this PR>`), and reconcile the two FUTURE items the spec names (`fut-shortest-path-tree`, `fut-min-depth-annotation`) — the spec states the tree's per-node depth delivers `fut-min-depth-annotation` for the tree shape and that item "can close alongside this slice." Record any newly-deferred follow-ons the spec's Non-goals list implies that are not already tracked (union/tail tree, weighted edges, materialized paths, target-scoped `?to=`) — most already exist as FUTURE items ([[fut-weighted-edges]], [[fut-graph-remaining]]); add only what is genuinely missing.

- [ ] **Step 2: Validate the registers**

Run: `bash tools/docs.sh validate > /tmp/d.log 2>&1; cat /tmp/d.log`
Expected: no errors.

- [ ] **Step 3: Run prek on all files (markdown lint) + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p.log 2>&1; grep -E "Failed|Passed|error" /tmp/p.log`
Expected: all hooks pass (commit anything the hooks fixed).

```bash
git add docs/ROADMAP.md docs/FUTURE.md
git commit -m "docs: close road-shortest-path-tree; reconcile min-depth annotation"
```

---

## Final verification (before opening the PR)

- [ ] **All new + adjacent query-api tests green:**

Run: `buck2 test //src/services/query-api:compile-graph-tree //src/services/query-api:graph-tree //src/services/query-api:graph-tree-e2e //src/services/query-api:graph-reach //src/services/query-api:graph-reach-e2e //src/services/query-api:graph-path-e2e //src/services/query-api:compile-graph-reach > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS across the board.

- [ ] **Clippy clean on the crate:**

Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log` (empty == clean).

- [ ] **Build the query-api targets (`-M none` in the cloud, mind the disk cap):**

Run: `buck2 build -M none //src/services/query-api/... > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|BUILD FAILED|error" /tmp/b.log`
Expected: BUILD SUCCEEDED.

- [ ] **prek all-files (rustfmt + markdown hooks) is clean:**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p.log 2>&1; tail -30 /tmp/p.log`
Expected: all hooks pass; no uncommitted hook edits.
