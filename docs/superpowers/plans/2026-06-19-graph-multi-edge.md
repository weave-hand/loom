# `/graph` part-3: multi-edge union reachability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `GET /objects/:type/graph?links=l1,…,lN&depth=D` — bounded recursive reachability where each step follows ANY ONE of a set of self-links on the queried type.

**Architecture:** A sibling compiler `compile_graph_reach_union` emits a `WITH RECURSIVE` query whose recursive term is a `UNION` of one self-hop arm per named link (the self-hop join shape is extracted into a shared `link_join` helper reused by the existing path-cycle compiler). A new handler `read_graph_reach_union` resolves+validates the self-link set (single-type governance, no intermediates) and a new `?links=` branch on the existing `/graph` route dispatches to it.

**Tech Stack:** Rust (edition 2024), buck2, axum, DuckDB-over-DuckLake serving, hermetic Postgres+DuckDB fixtures (`loom_fixture_test`).

## Global Constraints

- **Tests are integration `rust_test` / `loom_fixture_test` targets only** — never inline `#[cfg(test)] mod tests` / `#[test]` in `src/**` (the `no-inline-tests` prek hook fails the build). Each test file is its own target in `src/services/query-api/BUCK`.
- **Never run two buck2 commands concurrently; never pipe `buck2 test` through `tail`/`head`** (it stalls). Redirect to a file and grep: `buck2 test //target > /tmp/t.log 2>&1; grep -nE "Tests finished|Pass [0-9]|FAIL|error\[|panicked" /tmp/t.log`. Fixture tests take minutes — allow up to 600000ms.
- **Injection boundary (unchanged):** every caller VALUE is a bound `?` param; identifiers (tables/columns) come only from trusted ontology/ACL metadata and are double-quoted; `depth`/`limit` are inlined `u32`s.
- **Governance:** deny-by-default `Read` gate before existence is revealed; row-filters applied at the seed, every recursive landing node, and the projection.
- **Markdown:** end every `.md` file with exactly one trailing newline and no trailing whitespace (the `lint` CI job enforces this on all files).
- **Commit trailer:** end every commit message with `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`. Conventional Commits format (`feat(query): …`, `docs(query): …`).
- **No new error variant:** a non-self link reuses `QueryError::NotCyclicPath` (already mapped to 400).

---

## File Structure

- `src/services/query-api/src/sql.rs` — MODIFY: extract `link_join` helper, rewire `compile_graph_reach` to it, add `compile_graph_reach_union`.
- `src/services/query-api/tests/compile_graph_reach_union.rs` — CREATE: compiler unit tests.
- `src/services/query-api/src/handler.rs` — MODIFY: add `GraphUnionQuery` + `read_graph_reach_union`.
- `src/services/query-api/tests/graph_reach_union.rs` — CREATE: handler tests (in-memory CP + stub serving).
- `src/services/query-api/src/http.rs` — MODIFY: `?links=` branch + dispatch on the `/graph` route; extract `graph_error`; add `graph_union_respond`.
- `src/services/query-api/tests/graph_union_e2e.rs` — CREATE: DuckDB router e2e.
- `src/services/query-api/BUCK` — MODIFY: wire the three new test targets.
- `docs/FUTURE.md`, `docs/superpowers/specs/2026-06-06-loom-roadmap.md` — MODIFY: mark part-3 delivered.

---

## Task 1: Compiler — `link_join` helper + `compile_graph_reach_union`

**Files:**
- Modify: `src/services/query-api/src/sql.rs`
- Create: `src/services/query-api/tests/compile_graph_reach_union.rs`
- Modify: `src/services/query-api/BUCK`

**Interfaces:**
- Consumes: `SqlDialect`, `DuckDbDialect`, `CompileError`, `GraphStep`, private `filter_sql`/`caller_predicate_sql`/`MASK_MARKER` (all in `sql.rs`); `LinkBacking`, `RowFilter`, `TableRef`, `validate_row_filter` (from `control_plane_core`); `CallerPredicate` (`crate::filter`); `SqlValue` (`crate::serving`).
- Produces:
  ```rust
  pub fn compile_graph_reach_union(
      dialect: &dyn SqlDialect,
      table: &TableRef,
      identity: &str,
      backings: &[LinkBacking],
      seed_predicates: &[CallerPredicate],
      row_filters: &[RowFilter],
      allowed_cols: &[String],
      mask_cols: &[String],
      depth: u32,
      limit: u32,
  ) -> Result<(String, Vec<SqlValue>), CompileError>
  ```

- [ ] **Step 1: Write the failing compiler test**

Create `src/services/query-api/tests/compile_graph_reach_union.rs`:

```rust
//! compile_graph_reach_union emits a depth-bounded WITH RECURSIVE reachability query whose
//! recursive term is a UNION of one self-hop arm per self-link, governed by the queried
//! type's row-filters at the seed `s`, each arm's landing node `nxt`, and the projection `p`.

use control_plane_core::{CompareOp, LinkBacking, RowFilter, ScalarValue, TableRef};
use query_api::filter::CallerPredicate;
use query_api::serving::SqlValue;
use query_api::sql::{DuckDbDialect, compile_graph_reach_union};

fn person() -> TableRef {
    TableRef {
        schema: "main".into(),
        name: "person".into(),
    }
}

#[test]
fn two_self_links_union_with_row_filter() {
    // Person knows Person (FK knows_id -> id) UNION Person colleagues Person (join table).
    let fk = LinkBacking::ForeignKey {
        from_column: "knows_id".into(),
        to_column: "id".into(),
    };
    let jt = LinkBacking::JoinTable {
        table: TableRef {
            schema: "main".into(),
            name: "colleagues".into(),
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
    let (sql, params) = compile_graph_reach_union(
        &DuckDbDialect,
        &person(),
        "id",
        &[fk, jt],
        &[], // no seed predicates
        &row_filters,
        &["id".to_string(), "name".to_string()],
        &[],
        3,
        1000,
    )
    .unwrap();
    assert!(
        sql.contains("WITH RECURSIVE reach(id, depth) AS"),
        "got: {sql}"
    );
    assert!(sql.contains("r.depth < 3"), "depth bound inlined: {sql}");
    // seed UNION arm0 UNION arm1 => exactly two " UNION " tokens.
    assert_eq!(
        sql.matches(" UNION ").count(),
        2,
        "two recursive arms unioned: {sql}"
    );
    // FK arm joins cur.knows_id = nxt.id.
    assert!(
        sql.contains(r#"cur."knows_id" = nxt."id""#),
        "fk arm: {sql}"
    );
    // Join-table arm uses the per-arm alias j1 (arm index 1).
    assert!(
        sql.contains(r#"cur."id" = j1."a""#) && sql.contains(r#"j1."b" = nxt."id""#),
        "join-table arm with j1 alias: {sql}"
    );
    // Row-filter rendered at seed s, each arm's nxt (x2), and projection p.
    assert!(
        sql.contains(r#"s."active""#)
            && sql.contains(r#"nxt."active""#)
            && sql.contains(r#"p."active""#),
        "row-filter at s/nxt/p: {sql}"
    );
    // Param count: seed s (1) + arm0 nxt (1) + arm1 nxt (1) + projection p (1) = 4.
    assert_eq!(
        params.len(),
        4,
        "1 seed + 2 arm-nxt + 1 projection; got {params:?}"
    );
    assert_eq!(params, vec![SqlValue::Bool(true); 4]);
    // Reachable in >= 1 hop, projected distinct.
    assert!(sql.contains("depth >= 1"), "reachability bound: {sql}");
    assert!(
        sql.contains("SELECT DISTINCT") && sql.contains(r#"p."name""#),
        "projection: {sql}"
    );
}

#[test]
fn single_self_link_with_seed_predicate() {
    // One FK self-link (parent_id -> id) with an object-set seed In-predicate on the identity.
    let fk = LinkBacking::ForeignKey {
        from_column: "parent_id".into(),
        to_column: "id".into(),
    };
    let seed = vec![CallerPredicate {
        column: "id".into(),
        op: CompareOp::In,
        values: vec![SqlValue::Int(7)],
    }];
    let (sql, params) = compile_graph_reach_union(
        &DuckDbDialect,
        &person(),
        "id",
        &[fk],
        &seed,
        &[], // no row-filters
        &["id".to_string()],
        &[],
        2,
        1000,
    )
    .unwrap();
    // seed UNION arm0 => exactly one " UNION " token.
    assert_eq!(sql.matches(" UNION ").count(), 1, "single arm: {sql}");
    assert!(
        sql.contains(r#"cur."parent_id" = nxt."id""#),
        "fk arm: {sql}"
    );
    // Only the seed In value is bound (FK arm needs no join-table alias).
    assert_eq!(params.len(), 1, "seed id only; got {params:?}");
    assert_eq!(params[0], SqlValue::Int(7));
}
```

- [ ] **Step 2: Wire the test target and confirm it fails to build**

Add to `src/services/query-api/BUCK` (after the `compile-graph-reach` target, ~line 482):

```python
rust_test(
    name = "compile-graph-reach-union",
    crate = "compile_graph_reach_union",
    srcs = ["tests/compile_graph_reach_union.rs"],
    crate_root = "tests/compile_graph_reach_union.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
    ],
)
```

Run: `buck2 build //src/services/query-api:compile-graph-reach-union > /tmp/t.log 2>&1; grep -nE "error\[|cannot find|Build ID|BUILD SUCCEEDED|BUILD FAILED" /tmp/t.log`
Expected: FAILS to compile — `cannot find function ` compile_graph_reach_union` ` in `query_api::sql`.

- [ ] **Step 3: Extract the `link_join` self-hop helper**

In `src/services/query-api/src/sql.rs`, add this private helper immediately BEFORE `pub fn compile_graph_reach` (i.e. before the `#[allow(clippy::too_many_arguments)]` on line ~577):

```rust
/// Build a single link-hop JOIN fragment landing on `to_tbl`: `from_alias` -> `to_alias`.
/// The FK form is one JOIN (`from_alias.from_column = to_alias.to_column`); the join-table
/// form is two JOINs through `jt_alias`. The leading space is included so callers can
/// `push_str` it onto an accumulating FROM/JOIN string. Shared by the path-cycle and union
/// recursive-CTE compilers so the self-hop join shape lives in exactly one place.
fn link_join(
    dialect: &dyn SqlDialect,
    backing: &LinkBacking,
    from_alias: &str,
    to_alias: &str,
    to_tbl: &str,
    jt_alias: &str,
) -> String {
    let q = |id: &str| dialect.quote_ident(id);
    match backing {
        LinkBacking::ForeignKey {
            from_column,
            to_column,
        } => format!(
            " JOIN {to_tbl} {to_alias} ON {from_alias}.{} = {to_alias}.{}",
            q(from_column),
            q(to_column),
        ),
        LinkBacking::JoinTable {
            table,
            from_key,
            from_column,
            to_column,
            to_key,
        } => {
            let jtbl = format!("{}.{}", q(&table.schema), q(&table.name));
            format!(
                " JOIN {jtbl} {jt_alias} ON {from_alias}.{} = {jt_alias}.{} JOIN {to_tbl} {to_alias} ON {jt_alias}.{} = {to_alias}.{}",
                q(from_key),
                q(from_column),
                q(to_column),
                q(to_key),
            )
        }
    }
}
```

- [ ] **Step 4: Rewire `compile_graph_reach`'s join loop to use `link_join` (byte-identical)**

In `compile_graph_reach`, REPLACE the join-building loop (the `let mut joins = String::new();` block through its closing brace, currently lines ~634-678):

```rust
    let mut joins = String::new();
    for (i, step) in path.iter().enumerate() {
        let fa = from_alias(i);
        let ta = to_alias(i);
        let to_tbl = format!(
            "{}.{}",
            q(&step.next_table.schema),
            q(&step.next_table.name)
        );
        match &step.backing {
            LinkBacking::ForeignKey {
                from_column,
                to_column,
            } => {
                joins.push_str(&format!(
                    " JOIN {to_tbl} {ta} ON {fa}.{} = {ta}.{}",
                    q(from_column),
                    q(to_column),
                ));
            }
            LinkBacking::JoinTable {
                table: jt,
                from_key,
                from_column,
                to_column,
                to_key,
            } => {
                let jtbl = format!("{}.{}", q(&jt.schema), q(&jt.name));
                // For a single-step path use alias `j` (byte-identical to the prior single-link
                // form); multi-step paths index the alias `j{i+1}` to avoid collisions.
                let j = if k == 1 {
                    "j".to_string()
                } else {
                    format!("j{}", i + 1)
                };
                joins.push_str(&format!(
                    " JOIN {jtbl} {j} ON {fa}.{} = {j}.{} JOIN {to_tbl} {ta} ON {j}.{} = {ta}.{}",
                    q(from_key),
                    q(from_column),
                    q(to_column),
                    q(to_key),
                ));
            }
        }
    }
```

with:

```rust
    let mut joins = String::new();
    for (i, step) in path.iter().enumerate() {
        let fa = from_alias(i);
        let ta = to_alias(i);
        let to_tbl = format!(
            "{}.{}",
            q(&step.next_table.schema),
            q(&step.next_table.name)
        );
        // For a single-step path use join-table alias `j` (byte-identical to the prior
        // single-link form); multi-step paths index the alias `j{i+1}` to avoid collisions.
        let jt_alias = if k == 1 {
            "j".to_string()
        } else {
            format!("j{}", i + 1)
        };
        joins.push_str(&link_join(dialect, &step.backing, &fa, &ta, &to_tbl, &jt_alias));
    }
```

- [ ] **Step 5: Add `compile_graph_reach_union`**

In `src/services/query-api/src/sql.rs`, add immediately AFTER `compile_graph_reach`'s closing brace (~line 723):

```rust
/// Compile a depth-bounded recursive reachability query over a UNION of self-links: the deduped
/// set of `table` rows reachable from the seed set by repeatedly following ANY ONE of `backings`
/// (each a self-link on `table`) up to `depth` times. The recursive term is a UNION of one
/// self-hop arm per backing; every arm lands on `nxt` (= `table`) and is governed by the start
/// `row_filters` at `nxt`. Param order is SQL-emission order: seed predicates, seed row-filters
/// (`s`), each arm's row-filters (`nxt`, in `backings` order), then projection row-filters (`p`).
/// Termination by the inlined `depth` bound; `DISTINCT` + `depth >= 1` dedups/reachability. Every
/// caller value is a bound param. `backings` is non-empty (the handler enforces).
#[allow(clippy::too_many_arguments)]
pub fn compile_graph_reach_union(
    dialect: &dyn SqlDialect,
    table: &TableRef,
    identity: &str,
    backings: &[LinkBacking],
    seed_predicates: &[CallerPredicate],
    row_filters: &[RowFilter],
    allowed_cols: &[String],
    mask_cols: &[String],
    depth: u32,
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError> {
    for f in row_filters {
        validate_row_filter(f, None).map_err(CompileError::MalformedFilter)?;
    }
    let q = |id: &str| dialect.quote_ident(id);
    let tbl = format!("{}.{}", q(&table.schema), q(&table.name));
    let id = q(identity);
    let mut params: Vec<SqlValue> = Vec::new();

    // Anchor (seed) WHERE at alias `s`: caller seed predicates, then the start ACL row-filters.
    let mut seed_conj: Vec<String> = Vec::new();
    for p in seed_predicates {
        seed_conj.push(caller_predicate_sql(dialect, p, "s", &mut params));
    }
    for f in row_filters {
        seed_conj.push(filter_sql(dialect, f, "s", &mut params));
    }
    let seed_where = if seed_conj.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", seed_conj.join(" AND "))
    };

    // Recursive term: one UNION arm per self-link backing. Each arm joins `cur` to `nxt` (both
    // `table`) via the single hop, governed by the start row_filters at `nxt`. The join-table
    // alias `j{i}` is per-arm (arm index) so multiple join-table links never collide.
    let mut arms: Vec<String> = Vec::with_capacity(backings.len());
    for (i, backing) in backings.iter().enumerate() {
        let jt_alias = format!("j{i}");
        let joins = link_join(dialect, backing, "cur", "nxt", &tbl, &jt_alias);
        let mut rec_conj: Vec<String> = vec![format!("r.depth < {depth}")];
        for f in row_filters {
            rec_conj.push(filter_sql(dialect, f, "nxt", &mut params));
        }
        let rec_where = rec_conj.join(" AND ");
        arms.push(format!(
            "SELECT nxt.{id}, r.depth + 1 FROM reach r JOIN {tbl} cur ON cur.{id} = r.id{joins} WHERE {rec_where}"
        ));
    }
    let recursive = arms.join(" UNION ");

    // Projection of `p`: visible columns (masked -> marker), reachable in >= 1 hop, governed.
    let cols = allowed_cols
        .iter()
        .map(|c| {
            if mask_cols.iter().any(|m| m == c) {
                format!("'{MASK_MARKER}' AS {}", q(c))
            } else {
                format!("p.{}", q(c))
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let mut proj_conj: Vec<String> =
        vec![format!("p.{id} IN (SELECT id FROM reach WHERE depth >= 1)")];
    for f in row_filters {
        proj_conj.push(filter_sql(dialect, f, "p", &mut params));
    }
    let proj_where = proj_conj.join(" AND ");

    let sql = format!(
        "WITH RECURSIVE reach(id, depth) AS (\
           SELECT s.{id}, 0 FROM {tbl} s{seed_where} \
           UNION \
           {recursive}\
         ) \
         SELECT DISTINCT {cols} FROM {tbl} p WHERE {proj_where} {}",
        dialect.limit_clause(limit)
    );
    Ok((sql, params))
}
```

- [ ] **Step 6: Run the new compiler test + the existing graph compiler regression**

Run: `buck2 test //src/services/query-api:compile-graph-reach-union //src/services/query-api:compile-graph-reach //src/services/query-api:sql-compile > /tmp/t.log 2>&1; grep -nE "Tests finished|Pass [0-9]|Fail [0-9]|FAIL|error\[|panicked" /tmp/t.log`
Expected: all PASS — the two new union tests, plus `compile-graph-reach` (the byte-identical path-cycle regression proving the `link_join` extraction changed nothing) and `sql-compile`.

- [ ] **Step 7: Clippy + commit**

Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c.log 2>&1; grep -nE "warning|error" /tmp/c.log || echo CLEAN`
Expected: CLEAN (empty clippy output).

```bash
git add src/services/query-api/src/sql.rs src/services/query-api/tests/compile_graph_reach_union.rs src/services/query-api/BUCK
git commit -m "feat(query): compile_graph_reach_union + shared link_join helper

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Handler — `GraphUnionQuery` + `read_graph_reach_union`

**Files:**
- Modify: `src/services/query-api/src/handler.rs`
- Create: `src/services/query-api/tests/graph_reach_union.rs`
- Modify: `src/services/query-api/BUCK`

**Interfaces:**
- Consumes: `compile_graph_reach_union` (Task 1); `load_policy`, `project_allowed`, `identity_in_predicate`, `Subject`, `QueryDeps`, `ObjectRows`, `QueryError`, `DEFAULT_LIMIT` (all in `handler.rs`); `coerce_predicate`, `CallerPredicate` (`crate::filter`).
- Produces:
  ```rust
  pub struct GraphUnionQuery {
      pub type_name: String,
      pub links: Vec<String>,
      pub depth: u32,
      pub filters: Vec<(String, String)>,
      pub ids: Vec<String>,
  }
  pub async fn read_graph_reach_union(
      q: &GraphUnionQuery,
      subject: &Subject,
      deps: &QueryDeps<'_>,
  ) -> Result<ObjectRows, QueryError>
  ```

- [ ] **Step 1: Write the failing handler test**

Create `src/services/query-api/tests/graph_reach_union.rs`:

```rust
//! read_graph_reach_union on an in-memory control plane + a stub serving engine. The stub
//! returns canned object rows matching the Person projection [id, name]; the test asserts the
//! governance short-circuits (NotCyclicPath for a non-self link, UnknownLink, empty link set,
//! NoIdentity), the happy path returning the stub's reachable objects, and that duplicate link
//! names collapse to one arm. Real recursive reachability over DuckDB is the graph union e2e.

use std::time::Duration;

use async_trait::async_trait;
use control_plane_core::{
    Acl, Action, Cardinality, Effect, LinkBacking, LinkDef, ObjectType, Ontology, PolicyTarget,
    PropertyDef, RoleId, SubjectId, TableRef, TypeName,
};
use control_plane_memory::MemoryControlPlane;
use query_api::handler::{GraphUnionQuery, QueryDeps, QueryError, Subject, read_graph_reach_union};
use query_api::serving::{Rows, ServingEngine, ServingError, SqlValue};

/// A serving stub returning canned object rows in the projected order [id, name].
struct GraphServing {
    rows: Vec<Vec<SqlValue>>,
}

#[async_trait]
impl ServingEngine for GraphServing {
    async fn fetch_rows(
        &self,
        _sql: &str,
        _params: &[SqlValue],
    ) -> std::result::Result<Rows, ServingError> {
        Ok(Rows {
            columns: vec!["id".into(), "name".into()],
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
            },
            PropertyDef {
                name: "name".into(),
                ty: "Text".into(),
                required: false,
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

fn company_type() -> ObjectType {
    ObjectType {
        name: TypeName("Company".into()),
        properties: vec![PropertyDef {
            name: "id".into(),
            ty: "Long".into(),
            required: true,
        }],
        derived: vec![],
        table: TableRef {
            schema: "main".into(),
            name: "company".into(),
        },
        identity: Some("id".into()),
    }
}

/// Seed: Person with a `knows` FK self-link and a `colleagues` join-table self-link (both
/// Person -> Person), plus an `employer` FK link Person -> Company (a non-self link). An
/// analyst granted Read on Person and Company.
async fn seeded(person: ObjectType) -> (MemoryControlPlane, SubjectId) {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    cp.define_type(person).await.unwrap();
    cp.define_type(company_type()).await.unwrap();
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
    cp.define_link(LinkDef {
        name: "colleagues".into(),
        from: TypeName("Person".into()),
        to: TypeName("Person".into()),
        cardinality: Cardinality::Many,
        backing: LinkBacking::JoinTable {
            table: TableRef {
                schema: "main".into(),
                name: "colleagues".into(),
            },
            from_key: "id".into(),
            from_column: "a".into(),
            to_column: "b".into(),
            to_key: "id".into(),
        },
    })
    .await
    .unwrap();
    cp.define_link(LinkDef {
        name: "employer".into(),
        from: TypeName("Person".into()),
        to: TypeName("Company".into()),
        cardinality: Cardinality::One,
        backing: LinkBacking::ForeignKey {
            from_column: "employer_id".into(),
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
    cp.grant(
        &reader,
        Action::Read,
        PolicyTarget::Type(TypeName("Company".into())),
        Effect::Allow,
    )
    .await
    .unwrap();
    (cp, analyst)
}

fn union_query(links: &[&str]) -> GraphUnionQuery {
    GraphUnionQuery {
        type_name: "Person".into(),
        links: links.iter().map(|s| s.to_string()).collect(),
        depth: 3,
        filters: vec![],
        ids: vec![],
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn rejects_a_non_self_link() {
    let (cp, subj) = seeded(person_type(Some("id".into()))).await;
    let serving = GraphServing { rows: vec![] };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &serving,
    };
    // `employer` lands on Company, not back on Person -> not a self-link.
    let err = read_graph_reach_union(&union_query(&["knows", "employer"]), &Subject(subj), &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, QueryError::NotCyclicPath(l) if l == "employer"),
        "expected NotCyclicPath(employer), got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn rejects_an_unknown_link() {
    let (cp, subj) = seeded(person_type(Some("id".into()))).await;
    let serving = GraphServing { rows: vec![] };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &serving,
    };
    let err = read_graph_reach_union(&union_query(&["knows", "nope"]), &Subject(subj), &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, QueryError::UnknownLink(l) if l == "nope"),
        "expected UnknownLink(nope), got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn rejects_an_empty_link_set() {
    let (cp, subj) = seeded(person_type(Some("id".into()))).await;
    let serving = GraphServing { rows: vec![] };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &serving,
    };
    let err = read_graph_reach_union(&union_query(&[]), &Subject(subj), &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, QueryError::NotCyclicPath(_)),
        "expected NotCyclicPath, got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn rejects_a_type_without_identity() {
    let (cp, subj) = seeded(person_type(None)).await;
    let serving = GraphServing { rows: vec![] };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &serving,
    };
    let err = read_graph_reach_union(&union_query(&["knows"]), &Subject(subj), &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, QueryError::NoIdentity(t) if t == "Person"),
        "expected NoIdentity(Person), got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn returns_reachable_objects_for_a_self_link_union() {
    let (cp, subj) = seeded(person_type(Some("id".into()))).await;
    let serving = GraphServing {
        rows: vec![
            vec![SqlValue::Int(2), SqlValue::Text("Bob".into())],
            vec![SqlValue::Int(3), SqlValue::Text("Cara".into())],
        ],
    };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &serving,
    };
    let rows = read_graph_reach_union(&union_query(&["knows", "colleagues"]), &Subject(subj), &deps)
        .await
        .unwrap();
    assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
    assert_eq!(
        rows.logical_types,
        vec!["Long".to_string(), "Text".to_string()]
    );
    assert_eq!(
        rows.rows,
        vec![
            vec![SqlValue::Int(2), SqlValue::Text("Bob".into())],
            vec![SqlValue::Int(3), SqlValue::Text("Cara".into())],
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn duplicate_link_names_collapse() {
    // A link listed twice resolves to one arm (no error); the happy path still returns rows.
    let (cp, subj) = seeded(person_type(Some("id".into()))).await;
    let serving = GraphServing {
        rows: vec![vec![SqlValue::Int(2), SqlValue::Text("Bob".into())]],
    };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &serving,
    };
    let rows = read_graph_reach_union(&union_query(&["knows", "knows"]), &Subject(subj), &deps)
        .await
        .unwrap();
    assert_eq!(
        rows.rows,
        vec![vec![SqlValue::Int(2), SqlValue::Text("Bob".into())]]
    );
}
```

- [ ] **Step 2: Wire the test target and confirm it fails to build**

Add to `src/services/query-api/BUCK` (after the `graph-reach` target, ~line 461):

```python
rust_test(
    name = "graph-reach-union",
    crate = "graph_reach_union",
    srcs = ["tests/graph_reach_union.rs"],
    crate_root = "tests/graph_reach_union.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
        "//src/control-plane/memory:memory",
        "//third-party:async-trait",
        "//third-party:tokio",
    ],
)
```

Run: `buck2 build //src/services/query-api:graph-reach-union > /tmp/t.log 2>&1; grep -nE "error\[|cannot find|BUILD SUCCEEDED|BUILD FAILED" /tmp/t.log`
Expected: FAILS — `cannot find type ` GraphUnionQuery` ` / function `read_graph_reach_union`.

- [ ] **Step 3: Add `GraphUnionQuery` and `read_graph_reach_union`**

In `src/services/query-api/src/handler.rs`, add the struct immediately AFTER the `GraphQuery` struct (~line 446):

```rust
/// A bounded recursive reachability read over a UNION of self-links. `filters`/`ids` scope the
/// SEED set (the starting objects); the recursion follows ANY ONE of `links` (each a self-link
/// on `type_name`) up to `depth` times.
pub struct GraphUnionQuery {
    pub type_name: String,
    pub links: Vec<String>,
    pub depth: u32,
    pub filters: Vec<(String, String)>,
    pub ids: Vec<String>,
}
```

Then add the handler at the END of the file (after `read_graph_reach`'s closing brace, ~line 893):

```rust
/// Serve a bounded recursive reachability read over a UNION of self-links: from the seed set,
/// repeatedly follow ANY ONE of `links` (each a self-link on the queried type) up to `depth`
/// times, return the deduped reachable objects. Governed: Read on the queried type and the
/// queried type's row-filters at the seed/every recursive expansion/projection; declared identity
/// (dedup key). Every named link must be a self-link (its `to` is the queried type) -> else
/// NotCyclicPath; an unknown link -> UnknownLink. Because every link lands on the already-gated
/// queried type, there are no intermediate types and no per-link Read gate.
pub async fn read_graph_reach_union(
    q: &GraphUnionQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<ObjectRows, QueryError> {
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

    // Resolve the self-link set: every named link must be an outbound link of the queried type
    // whose `to` is the queried type (a self-link). Dedup by name (first-seen order; a link
    // listed twice yields one arm). No per-link Read gate — every link lands on the queried type,
    // already gated above.
    if q.links.is_empty() {
        return Err(QueryError::NotCyclicPath(String::new()));
    }
    let links = deps.ontology.links(&type_name, PageReq::unbounded()).await?;
    let mut backings: Vec<control_plane_core::LinkBacking> = Vec::with_capacity(q.links.len());
    let mut seen: std::collections::HashSet<&String> = std::collections::HashSet::new();
    for link_name in &q.links {
        if !seen.insert(link_name) {
            continue; // duplicate -> one arm
        }
        let link = links
            .items
            .iter()
            .find(|l| &l.name == link_name)
            .ok_or_else(|| QueryError::UnknownLink(link_name.clone()))?;
        if link.to != type_name {
            return Err(QueryError::NotCyclicPath(link_name.clone()));
        }
        backings.push(link.backing.clone());
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
        seed_predicates.push(
            crate::filter::coerce_predicate(col, ty, raw)
                .map_err(|_| QueryError::BadFilter(col.clone()))?,
        );
    }
    if let Some(p) = identity_in_predicate(&object_type, &denied, &masked, &q.ids)? {
        seed_predicates.push(p);
    }

    let (sql, params) = crate::sql::compile_graph_reach_union(
        deps.serving.dialect(),
        &object_type.table,
        &identity,
        &backings,
        &seed_predicates,
        &row_filters,
        &allowed,
        &mask_cols,
        q.depth,
        DEFAULT_LIMIT,
    )?;
    let served = deps.serving.fetch_rows(&sql, &params).await?;
    let logical_types: Vec<String> = allowed
        .iter()
        .map(|name| {
            object_type
                .properties
                .iter()
                .find(|p| &p.name == name)
                .map(|p| p.ty.clone())
                .unwrap_or_default()
        })
        .collect();
    debug_assert_eq!(
        served.columns, allowed,
        "serving engine returned columns out of the projected order"
    );
    Ok(ObjectRows {
        columns: allowed,
        logical_types,
        rows: served.rows,
    })
}
```

- [ ] **Step 4: Run the handler test**

Run: `buck2 test //src/services/query-api:graph-reach-union //src/services/query-api:graph-reach > /tmp/t.log 2>&1; grep -nE "Tests finished|Pass [0-9]|Fail [0-9]|FAIL|error\[|panicked" /tmp/t.log`
Expected: all PASS (6 new union handler tests + the existing `graph-reach` path tests unaffected).

- [ ] **Step 5: Clippy + commit**

Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c.log 2>&1; grep -nE "warning|error" /tmp/c.log || echo CLEAN`
Expected: CLEAN.

```bash
git add src/services/query-api/src/handler.rs src/services/query-api/tests/graph_reach_union.rs src/services/query-api/BUCK
git commit -m "feat(query): read_graph_reach_union governed multi-edge handler

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: HTTP `?links=` branch + DuckDB e2e

**Files:**
- Modify: `src/services/query-api/src/http.rs`
- Create: `src/services/query-api/tests/graph_union_e2e.rs`
- Modify: `src/services/query-api/BUCK`

**Interfaces:**
- Consumes: `read_graph_reach_union`, `GraphUnionQuery` (Task 2); existing `MAX_GRAPH_DEPTH`, `DEFAULT_GRAPH_DEPTH`, `graph_respond`, `AppState`, `router` in `http.rs`.
- Produces: `GET /objects/:type/graph?links=l1,…,lN&depth=D&_ids=…` (200 reachable objects; 400 on ambiguity/empty/depth/non-self; 404 unknown type/link; 403 forbidden). No new public symbol.

- [ ] **Step 1: Write the failing e2e**

Create `src/services/query-api/tests/graph_union_e2e.rs`:

```rust
//! Graph multi-edge union e2e: GET /objects/:type/graph?links=… over the real HTTP router
//! backed by DuckDB-over-DuckLake. Proves the union reachability shape {objects:[...]} where
//! each step follows ANY ONE of a set of self-links:
//!   - ?links=knows,colleagues unions both edge sets (reaches more than either alone),
//!   - ?links=knows alone is a strict subset (a colleagues-only node is absent),
//!   - a cycle terminates and the node set is deduped,
//!   - a Read row-filter (active=true) prunes reachability through a blocked node,
//!   - ?path=…&links=… together -> 400, empty ?links= -> 400.
//!
//! Graph: one `Person` table with a `knows` FK self-link via knows_id and a `colleagues(a, b)`
//! join-table self-link, plus a boolean `active` column. knows edges: 1->2, 2->3 (acyclic, so
//! knows alone reaches {2,3} from 1). colleagues edge: 1->4 (so the union adds the
//! colleagues-only node 4). No edge returns to 1, keeping the reachable sets free of the seed.
//! Cycle termination over the recursive CTE is covered by graph_reach_e2e (shared machinery);
//! this suite isolates the union axis. Person declares identity `id`.

use std::sync::Arc;

use arrow::array::{BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use control_plane_core::{
    Acl, Action, Cardinality, CompareOp, ControlPlane, DatasetRef, Effect, EventType, LineageEvent,
    LinkBacking, LinkDef, ObjectType, Ontology, Policy, PolicyTarget, PropertyDef, RoleId,
    RowFilter, RunId, ScalarValue, SubjectId, TableRef, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use http_body_util::BodyExt;
use ingest::{MaterializeRequest, materialize};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use query_api::http::{AppState, router};
use query_api::serving::{ActionEngine, EmbeddedDuckDb, ServingError, SqlValue};
use time::OffsetDateTime;
use tower::ServiceExt;
use uuid::Uuid;

fn tref(s: &str, n: &str) -> TableRef {
    TableRef {
        schema: s.into(),
        name: n.into(),
    }
}

fn prop(name: &str, ty: &str, required: bool) -> PropertyDef {
    PropertyDef {
        name: name.into(),
        ty: ty.into(),
        required,
    }
}

struct StubAction;

#[async_trait]
impl ActionEngine for StubAction {
    async fn insert_row(
        &self,
        _table: &TableRef,
        _columns: &[String],
        _values: &[SqlValue],
    ) -> std::result::Result<(), ServingError> {
        Ok(())
    }
}

async fn land(
    cp: &PgControlPlane,
    store: &Arc<dyn ObjectStore>,
    table: &TableRef,
    schema: Arc<Schema>,
    batch: RecordBatch,
) {
    let lineage = LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetRef::from(table)],
        payload: serde_json::json!({}),
    };
    materialize(
        cp,
        store.clone(),
        MaterializeRequest {
            table,
            schema,
            batches: &[batch],
            file_prefix: "run-1",
            gate: None,
            lineage,
        },
    )
    .await
    .unwrap();
}

/// Seed a `Person` table with a `knows_id` FK self-link (edges 1->2, 2->3) and a
/// `colleagues(a, b)` join-table self-link (edge 1->4), plus a boolean `active` column
/// (node 2 inactive). Person declares identity `id`. The caller MUST keep the returned
/// `DuckLakeWriter` alive (its TempDir holds the Parquet read).
async fn setup(fx: &PgFixture) -> (PgControlPlane, EmbeddedDuckDb, DuckLakeWriter) {
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(writer.data_path()).unwrap());

    // person(id, name, active, knows_id): the FK self-link edges 1->2, 2->3 live in knows_id.
    // node 2 (bob) is INACTIVE (governance test). node 4 has no knows_id (colleagues-only).
    let person = tref("main", "person");
    let person_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("active", DataType::Boolean, false),
        Field::new("knows_id", DataType::Int64, true),
    ]));
    let person_batch = RecordBatch::try_new(
        person_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec![
                Some("ann"),
                Some("bob"),
                Some("cal"),
                Some("dee"),
            ])),
            Arc::new(BooleanArray::from(vec![true, false, true, true])),
            // 1->2, 2->3, 3 and 4 have no outbound knows edge.
            Arc::new(Int64Array::from(vec![Some(2), Some(3), None, None])),
        ],
    )
    .unwrap();
    land(&cp, &store, &person, person_schema, person_batch).await;

    // colleagues(a, b): join-table self-link edge 1->4. knows never touches 4, so the union
    // adds 4 over knows alone. No back-edge to 1 -> the seed stays out of the reachable set.
    let colleagues = tref("main", "colleagues");
    let colleagues_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]));
    let colleagues_batch = RecordBatch::try_new(
        colleagues_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![4])),
        ],
    )
    .unwrap();
    land(&cp, &store, &colleagues, colleagues_schema, colleagues_batch).await;

    cp.define_type(ObjectType {
        name: TypeName("Person".into()),
        properties: vec![
            prop("id", "Long", true),
            prop("name", "String", false),
            prop("active", "Boolean", true),
            prop("knows_id", "Long", false),
        ],
        derived: vec![],
        table: person.clone(),
        identity: Some("id".into()),
    })
    .await
    .unwrap();

    // `knows`: FK self-link Person -> Person via knows_id.
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
    // `colleagues`: join-table self-link Person -> Person.
    cp.define_link(LinkDef {
        name: "colleagues".into(),
        from: TypeName("Person".into()),
        to: TypeName("Person".into()),
        cardinality: Cardinality::Many,
        backing: LinkBacking::JoinTable {
            table: colleagues.clone(),
            from_key: "id".into(),
            from_column: "a".into(),
            to_column: "b".into(),
            to_key: "id".into(),
        },
    })
    .await
    .unwrap();

    let eng = EmbeddedDuckDb::attach(
        &format!(
            "dbname={} host={} user=postgres",
            db,
            fx.socket_path().display()
        ),
        writer.data_path(),
    )
    .await
    .unwrap();
    (cp, eng, writer)
}

async fn subject_with_role(cp: &PgControlPlane, name: &str) -> (SubjectId, RoleId) {
    let subj = SubjectId(name.into());
    let role = RoleId(format!("{name}-role"));
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    (subj, role)
}

async fn grant_read(cp: &PgControlPlane, role: &RoleId, type_name: &str) {
    cp.grant(
        role,
        Action::Read,
        PolicyTarget::Type(TypeName(type_name.into())),
        Effect::Allow,
    )
    .await
    .unwrap();
}

async fn get(
    cp: Arc<PgControlPlane>,
    eng: Arc<EmbeddedDuckDb>,
    uri: &str,
    subject: &str,
) -> (StatusCode, serde_json::Value) {
    let app = router(AppState {
        cp: cp as Arc<dyn ControlPlane>,
        serving: eng,
        action_engine: Arc::new(StubAction),
    });
    let res = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Loom-Subject", subject)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let json = if bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
    };
    (status, json)
}

/// Collect the sorted set of `id`s from an {"objects":[...]} body. `id` is a `Long`, rendered
/// as a numeric STRING (int64 exceeds JSON's safe-integer range), so parse.
fn ids(body: &serde_json::Value) -> Vec<i64> {
    let mut out: Vec<i64> = body["objects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["id"].as_str().unwrap().parse::<i64>().unwrap())
        .collect();
    out.sort_unstable();
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn union_reaches_more_than_either_link_alone() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;

    // knows alone from {1}, depth 3: 1->2->3 => {2, 3} (4 is colleagues-only, absent).
    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph?links=knows&depth=3&_ids=1",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ids(&body), vec![2, 3], "knows alone: {body}");

    // knows UNION colleagues from {1}, depth 3: adds the colleagues edge 1<->4 => {2, 3, 4}.
    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph?links=knows,colleagues&depth=3&_ids=1",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        ids(&body),
        vec![2, 3, 4],
        "union adds the colleagues-only node 4: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn row_filter_prunes_union_reachability_through_blocked_node() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    // A Read row-filter active=true on Person makes node 2 (inactive) unreachable. From {1},
    // knows goes 1->2 (cut) so 3 (only reachable via 2) is also cut; colleagues still reaches 4.
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
        "/objects/Person/graph?links=knows,colleagues&depth=3&_ids=1",
        "carol",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        ids(&body),
        vec![4],
        "node 2 (inactive) cut prunes knows-reachability; colleagues still reaches 4: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn path_and_links_together_is_400() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;

    let (status, _body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph?path=knows&links=colleagues&depth=2&_ids=1",
        "alice",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "path and links together -> 400"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_links_is_400() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;

    // links= present but empty (all entries dropped) and no path => 400.
    let (status, _body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph?links=&depth=2&_ids=1",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "empty links -> 400");
}
```

- [ ] **Step 2: Wire the e2e target and confirm it fails to build**

Add to `src/services/query-api/BUCK` (after the `graph-path-e2e` target, ~line 274):

```python
loom_fixture_test(
    name = "graph-union-e2e",
    crate = "graph_union_e2e",
    srcs = ["tests/graph_union_e2e.rs"],
    crate_root = "tests/graph_union_e2e.rs",
    duckdb = True,
    deps = [
        ":query-api",
        "//src/services/ingest:ingest",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:arrow",
        "//third-party:async-trait",
        "//third-party:axum",
        "//third-party:http-body-util",
        "//third-party:object_store",
        "//third-party:serde_json",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:tower",
        "//third-party:uuid",
    ],
)
```

Run: `buck2 build //src/services/query-api:graph-union-e2e > /tmp/t.log 2>&1; grep -nE "error\[|cannot find|BUILD SUCCEEDED|BUILD FAILED" /tmp/t.log`
Expected: BUILD SUCCEEDED — the e2e compiles (it only uses public router/handlers; the `?links=` route does not yet exist, so the tests will FAIL at runtime, not compile-time). If it builds, proceed; the runtime failures are fixed in Step 3.

- [ ] **Step 3: Add the `?links=` branch, extract `graph_error`, add `graph_union_respond`**

In `src/services/query-api/src/http.rs`:

(a) Extend the handler import (lines 6-9) to add the two new symbols:

```rust
use crate::handler::{
    Associations, ChainQuery, GraphQuery, GraphUnionQuery, Hop, ObjectQuery, QueryDeps, QueryError,
    Subject, read_associations, read_graph_reach, read_graph_reach_union, read_linked_chain,
    read_object,
};
```

(b) REPLACE the body of `get_graph_path` (the parse loop + tail dispatch, currently lines ~337-382 — from `let mut depth = DEFAULT_GRAPH_DEPTH;` through the final `graph_respond(...).await`) with:

```rust
    let mut depth = DEFAULT_GRAPH_DEPTH;
    let mut ids: Vec<String> = Vec::new();
    let mut ids_present = false;
    let mut path: Vec<String> = Vec::new();
    let mut links: Vec<String> = Vec::new();
    let mut filters: Vec<(String, String)> = Vec::with_capacity(params.len());
    for (k, v) in params {
        match k.as_str() {
            "path" => {
                path = v
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(String::from)
                    .collect()
            }
            "links" => {
                links = v
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(String::from)
                    .collect()
            }
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
            _ => filters.push((k, v)),
        }
    }
    if ids_present && ids.is_empty() {
        return (StatusCode::BAD_REQUEST, "_ids requires at least one value").into_response();
    }
    if !(1..=MAX_GRAPH_DEPTH).contains(&depth) {
        return (
            StatusCode::BAD_REQUEST,
            format!("depth must be 1..={MAX_GRAPH_DEPTH}"),
        )
            .into_response();
    }
    // Exactly one of `path` (ordered cycle) or `links` (self-link union) selects the mode.
    if !path.is_empty() && !links.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            "specify either path or links, not both",
        )
            .into_response();
    }
    if !links.is_empty() {
        return graph_union_respond(&st, type_name, links, depth, filters, ids, subject).await;
    }
    if path.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            "path or links requires at least one link",
        )
            .into_response();
    }
    graph_respond(&st, type_name, path, depth, filters, ids, subject).await
```

(c) REPLACE `graph_respond` (currently lines ~388-425) with the `graph_error` helper, the slimmed `graph_respond`, and the new `graph_union_respond`:

```rust
/// Shared HTTP mapping for graph reachability read errors (path-cycle and union).
fn graph_error(e: QueryError) -> axum::response::Response {
    match e {
        QueryError::UnknownType(t) => (StatusCode::NOT_FOUND, t).into_response(),
        QueryError::UnknownLink(l) => (StatusCode::NOT_FOUND, l).into_response(),
        QueryError::NotCyclicPath(p) => (StatusCode::BAD_REQUEST, p).into_response(),
        QueryError::NoIdentity(t) => (StatusCode::BAD_REQUEST, t).into_response(),
        QueryError::BadFilter(c) => (StatusCode::BAD_REQUEST, c).into_response(),
        QueryError::Forbidden => StatusCode::FORBIDDEN.into_response(),
        // Opaque body for backend/serving faults (no internal detail leaked).
        _ => (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    }
}

/// Path-cycle (`?path=` / single `/graph/:link`) tail: build a `GraphQuery`, run
/// `read_graph_reach`, map via `graph_error`.
async fn graph_respond(
    st: &AppState,
    type_name: String,
    path: Vec<String>,
    depth: u32,
    filters: Vec<(String, String)>,
    ids: Vec<String>,
    subject: String,
) -> axum::response::Response {
    let deps = QueryDeps {
        ontology: st.cp.ontology(),
        acl: st.cp.acl(),
        serving: st.serving.as_ref(),
    };
    match read_graph_reach(
        &GraphQuery {
            type_name,
            path,
            depth,
            filters,
            ids,
        },
        &Subject(SubjectId(subject)),
        &deps,
    )
    .await
    {
        Ok(rows) => Json(crate::render::objects_to_json(&rows)).into_response(),
        Err(e) => graph_error(e),
    }
}

/// Union (`?links=`) tail: build a `GraphUnionQuery`, run `read_graph_reach_union`, map via
/// `graph_error`.
async fn graph_union_respond(
    st: &AppState,
    type_name: String,
    links: Vec<String>,
    depth: u32,
    filters: Vec<(String, String)>,
    ids: Vec<String>,
    subject: String,
) -> axum::response::Response {
    let deps = QueryDeps {
        ontology: st.cp.ontology(),
        acl: st.cp.acl(),
        serving: st.serving.as_ref(),
    };
    match read_graph_reach_union(
        &GraphUnionQuery {
            type_name,
            links,
            depth,
            filters,
            ids,
        },
        &Subject(SubjectId(subject)),
        &deps,
    )
    .await
    {
        Ok(rows) => Json(crate::render::objects_to_json(&rows)).into_response(),
        Err(e) => graph_error(e),
    }
}
```

- [ ] **Step 4: Run the e2e + the existing graph route e2es (regression)**

Run: `buck2 test //src/services/query-api:graph-union-e2e //src/services/query-api:graph-reach-e2e //src/services/query-api:graph-path-e2e > /tmp/t.log 2>&1; grep -nE "Tests finished|Pass [0-9]|Fail [0-9]|FAIL|error\[|panicked" /tmp/t.log`
Expected: all PASS (allow up to 600000ms — these are fixture tests). The new union e2e passes and the existing `?path=` and `/graph/:link` route e2es are unaffected by the dispatch change.

- [ ] **Step 5: Clippy + commit**

Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c.log 2>&1; grep -nE "warning|error" /tmp/c.log || echo CLEAN`
Expected: CLEAN.

```bash
git add src/services/query-api/src/http.rs src/services/query-api/tests/graph_union_e2e.rs src/services/query-api/BUCK
git commit -m "feat(query): GET /objects/:type/graph?links= multi-edge union route

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Docs — mark part-3 delivered

**Files:**
- Modify: `docs/FUTURE.md`
- Modify: `docs/superpowers/specs/2026-06-06-loom-roadmap.md`

- [ ] **Step 1: `docs/FUTURE.md` — add a delivered section, drop the union bullet**

In `docs/FUTURE.md`, immediately AFTER the "Part 2 — repeated path-cycle is DELIVERED" paragraph (ends `…forwards a 1-element path into the same machinery.`, ~line 214) and BEFORE the `Remaining /graph parts (deferred):` line, INSERT:

```markdown

  **Part 3 — multi-edge union reachability is DELIVERED**
  (`2026-06-19-graph-multi-edge-design.md`): `GET /objects/:type/graph?links=l1,…,lN&depth=N`
  follows ANY ONE of a set of self-links at each recursive step (a union over edge types),
  returning the deduped reachable objects of the queried type. The recursive CTE term is a
  `UNION` of one self-hop arm per named link (mixed FK/join-table backings), each governed by the
  queried type's row-filters at its landing node — single-type governance, since every link is a
  self-link with no intermediate types. A sibling compiler `compile_graph_reach_union` shares the
  `link_join` self-hop helper with the path-cycle compiler. A non-self link reuses
  `NotCyclicPath`; `?path=` and `?links=` are mutually exclusive on the `/graph` route.
```

Then DELETE the now-delivered deferred bullet (the two lines):

```markdown
  - **Multi-edge union reachability (`?links=`).** Unioning multiple self-links (option C) for
    reachability over a *set* of links rather than a fixed ordered path.
```

- [ ] **Step 2: `docs/superpowers/specs/2026-06-06-loom-roadmap.md` — add a delivered note, update the deferred list**

In the roadmap, immediately AFTER the part-2 paragraph (ends `…the old NotSelfLink error is unified into NotCyclicPath.`, ~line 372) and BEFORE the `Remaining /graph parts (deferred…)` sentence (line 373), INSERT a blank line and:

```markdown
**`/graph` part-3 — multi-edge union reachability** (`2026-06-19-graph-multi-edge-design.md`) is
now delivered: `GET /objects/:type/graph?links=l1,…,lN&depth=N` follows ANY ONE of a set of
self-links at each step (a union over edge types), returning the deduped reachable objects of the
queried type. The recursive CTE term is a `UNION` of one self-hop arm per named link; every arm
is governed by the queried type's row-filters at its landing node (single-type governance — no
intermediates). A sibling compiler `compile_graph_reach_union` shares the `link_join` self-hop
helper with the path-cycle compiler; `?path=` and `?links=` are mutually exclusive.
```

Then in the `Remaining /graph parts (deferred to docs/FUTURE.md):` sentence (now after the inserted paragraph), DELETE the `multi-edge union reachability (?links=), ` clause so it reads:

```markdown
Remaining `/graph` parts (deferred to `docs/FUTURE.md`): inverse links inside the path,
recursive-core + relational-tail (`path=knows*,worksAt`), min-depth annotation, shortest-path /
`/tree`, weighted edges.
```

- [ ] **Step 3: Run the markdown hooks and commit whatever they fix**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p.log 2>&1; grep -nE "Failed|Passed|Skipped" /tmp/p.log`
Expected: all hooks Passed (or the file-fixers report a fix — if so, re-stage). Confirm no trailing-whitespace / EOF failures remain.

```bash
git add docs/FUTURE.md docs/superpowers/specs/2026-06-06-loom-roadmap.md
git commit -m "docs(query): mark /graph part-3 (multi-edge union) delivered

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Final verification (whole branch)

- [ ] Run the full query-api suite: `buck2 test //src/services/query-api/... > /tmp/all.log 2>&1; grep -nE "Tests finished|Fail [0-9]|FAIL|error\[|panicked" /tmp/all.log` — expect 0 failures (allow up to 600000ms).
- [ ] Clippy across first-party Rust: `./tools/clippy-all.sh > /tmp/clip.log 2>&1; grep -nE "warning|error|clean|CLEAN" /tmp/clip.log` — expect clean.
- [ ] Confirm `git log --oneline` shows the four task commits plus the design-spec commit.
