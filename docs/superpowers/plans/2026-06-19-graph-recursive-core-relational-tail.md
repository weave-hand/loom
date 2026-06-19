# `/graph` part B — recursive-core + relational-tail — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Serve `GET /objects/:type/graph?path=knows*,worksAt,…` — a depth-bounded recursive self-link core followed by a forward relational tail to a (possibly different) projected type.

**Architecture:** A new compiler `compile_graph_reach_tail` composes part-1's single-self-link recursive CTE (extracted into a focused `recursive_reach_cte` helper) with the existing `chain_from_where` tail, gluing them by `t_0.id IN (SELECT id FROM reach WHERE depth >= 1)`. A new handler `read_graph_reach_with_tail` threads layered governance (Read gate + row-filters on the core type and every tail-reached type). HTTP `get_graph_path` detects a `*`-suffixed first segment and dispatches to part B.

**Tech Stack:** Rust, buck2, DuckDB-over-DuckLake serving, axum, sqlx control plane. Tests are `rust_test` integration targets (DuckDB e2e via `loom_fixture_test`).

## Global Constraints

- **Tests are `rust_test` integration targets only** — NO inline `#[cfg(test)]`/`#[test]` in `src/**.rs` (the `no-inline-tests` prek hook fails otherwise). Each test is a sibling `tests/<name>.rs` wired as its own target in `src/services/query-api/BUCK`.
- **Fixture-backed (Postgres/DuckDB) tests use the `loom_fixture_test` macro**, not bare `rust_test`, with `duckdb = True`.
- **The `*`-suffixed core is a single self-link** on the queried type, and **the `*` is the path prefix** (first segment). The tail is **forward-only, one or more links**. Caller `?param` filters bind to the **seed/source type only**.
- **Reachable set is depth ≥ 1** — the seed is excluded from the tail's input unless a cycle re-reaches it (identical to part-1/2/3 reachability).
- `recursive_reach_cte` is a **focused new helper, NOT a refactor of `compile_graph_reach`** — do not touch `compile_graph_reach`/`compile_graph_reach_union`; their existing tests must stay green untouched.
- Commit messages: Conventional Commits; end every commit body with `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.
- Run tests with the file-redirect pattern (never pipe `buck2 test` through `tail`/`head`): `buck2 test //src/services/query-api:<target> > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error" /tmp/t.log`.

---

### Task 1: `compile_graph_reach_tail` compiler

**Files:**
- Modify: `src/services/query-api/src/sql.rs` (add `recursive_reach_cte` + `compile_graph_reach_tail` after `compile_graph_reach_union`, which ends at line 863, before the `compile_chain` wrapper at line 868)
- Create: `src/services/query-api/tests/compile_graph_reach_tail.rs`
- Modify: `src/services/query-api/BUCK` (add the `compile-graph-reach-tail` target)

**Interfaces:**
- Consumes (existing, private in `sql.rs`): `link_join(dialect, backing, from_alias, to_alias, to_tbl, jt_alias) -> String`; `chain_from_where(dialect, &[ChainType], &[LinkBacking]) -> Result<(String, Vec<String>, Vec<SqlValue>), CompileError>`; `validate_row_filter`, `filter_sql`, `caller_predicate_sql`, `MASK_MARKER`. Types `ChainType{table, row_filters, predicates}`, `LinkBacking`, `RowFilter`, `TableRef`, `SqlValue`, `CallerPredicate`, `CompileError`, `SqlDialect`, `DuckDbDialect`.
- Produces (used by Task 2): `pub fn compile_graph_reach_tail(dialect: &dyn SqlDialect, table: &TableRef, identity: &str, core_backing: &LinkBacking, seed_predicates: &[CallerPredicate], core_row_filters: &[RowFilter], tail_types: &[ChainType], tail_hops: &[LinkBacking], allowed_cols: &[String], mask_cols: &[String], depth: u32, limit: u32) -> Result<(String, Vec<SqlValue>), CompileError>`.

- [ ] **Step 1: Write the failing test**

Create `src/services/query-api/tests/compile_graph_reach_tail.rs`:

```rust
//! compile_graph_reach_tail emits a recursive-core + relational-tail reachability query: a
//! single-self-link `WITH RECURSIVE reach(id, depth)` CTE, then a forward INNER-JOIN tail off
//! the depth>=1 reachable set, projecting the final tail type DISTINCT. The tail's source
//! position t_0 is the queried type, constrained to the reach set; the queried type's
//! row-filters live in the CTE (seed `s` + recursive `nxt`), not at t_0. Param order: seed
//! predicates, seed row-filters (s), recursive row-filters (nxt), then the tail's per-position
//! params.

use control_plane_core::{CompareOp, LinkBacking, RowFilter, ScalarValue, TableRef};
use query_api::filter::CallerPredicate;
use query_api::serving::SqlValue;
use query_api::sql::{ChainType, DuckDbDialect, compile_graph_reach_tail};

fn tref(n: &str) -> TableRef {
    TableRef {
        schema: "main".into(),
        name: n.into(),
    }
}

#[test]
fn fk_core_single_tail_shape() {
    // Person --knows(FK knows_id->id)*--> Person, then --worksAt(FK worksat_id->id)--> Company.
    let core = LinkBacking::ForeignKey {
        from_column: "knows_id".into(),
        to_column: "id".into(),
    };
    let tail_types = vec![
        ChainType {
            table: tref("person"),
            row_filters: vec![],
            predicates: vec![],
        },
        ChainType {
            table: tref("company"),
            row_filters: vec![],
            predicates: vec![],
        },
    ];
    let tail_hops = vec![LinkBacking::ForeignKey {
        from_column: "worksat_id".into(),
        to_column: "id".into(),
    }];
    let (sql, params) = compile_graph_reach_tail(
        &DuckDbDialect,
        &tref("person"),
        "id",
        &core,
        &[],
        &[],
        &tail_types,
        &tail_hops,
        &["id".to_string()],
        &[],
        3,
        1000,
    )
    .unwrap();
    assert!(
        sql.contains("WITH RECURSIVE reach(id, depth) AS"),
        "recursive core CTE: {sql}"
    );
    assert!(sql.contains("r.depth < 3"), "depth bound inlined: {sql}");
    // Core self-link join inside the CTE.
    assert!(
        sql.contains(r#"cur."knows_id" = nxt."id""#),
        "core fk join: {sql}"
    );
    // Tail FK join (chain_from_where joins the final target back down to t_0).
    assert!(
        sql.contains(r#"t_0."worksat_id" = t_1."id""#),
        "tail fk join: {sql}"
    );
    // Glue: t_0 (queried type) constrained to the depth>=1 reach set.
    assert!(
        sql.contains(r#"t_0."id" IN (SELECT id FROM reach WHERE depth >= 1)"#),
        "reach-membership glue: {sql}"
    );
    // Final tail type projected DISTINCT at alias t_1.
    assert!(
        sql.contains("SELECT DISTINCT") && sql.contains(r#"t_1."id""#),
        "distinct projection of final type: {sql}"
    );
    assert!(sql.contains("LIMIT 1000"), "limit: {sql}");
    // No filters, no seed => no bound params.
    assert!(params.is_empty(), "no params expected; got {params:?}");
}

#[test]
fn param_order_seed_core_tail() {
    // Seed In-predicate + a core row-filter (Person.active=true) + a tail row-filter
    // (Company.verified=true). Param order must be: seed pred, seed rf@s, recursive rf@nxt,
    // then tail rf@t_1.
    let core = LinkBacking::ForeignKey {
        from_column: "knows_id".into(),
        to_column: "id".into(),
    };
    let seed = vec![CallerPredicate {
        column: "id".into(),
        op: CompareOp::In,
        values: vec![SqlValue::Int(1)],
    }];
    let core_rf = vec![RowFilter::Compare {
        property: "active".into(),
        op: CompareOp::Eq,
        value: ScalarValue::Bool(true),
    }];
    let tail_types = vec![
        ChainType {
            table: tref("person"),
            row_filters: vec![],
            predicates: vec![],
        },
        ChainType {
            table: tref("company"),
            row_filters: vec![RowFilter::Compare {
                property: "verified".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Bool(true),
            }],
            predicates: vec![],
        },
    ];
    let tail_hops = vec![LinkBacking::ForeignKey {
        from_column: "worksat_id".into(),
        to_column: "id".into(),
    }];
    let (sql, params) = compile_graph_reach_tail(
        &DuckDbDialect,
        &tref("person"),
        "id",
        &core,
        &seed,
        &core_rf,
        &tail_types,
        &tail_hops,
        &["id".to_string()],
        &[],
        2,
        1000,
    )
    .unwrap();
    // Core filter rendered at the seed `s` and the recursive landing `nxt`; tail filter at t_1.
    assert!(
        sql.contains(r#"s."active""#) && sql.contains(r#"nxt."active""#),
        "core filter at s and nxt: {sql}"
    );
    assert!(sql.contains(r#"t_1."verified""#), "tail filter at t_1: {sql}");
    // Param order: seed In(1), seed active@s, recursive active@nxt, tail verified@t_1.
    assert_eq!(
        params,
        vec![
            SqlValue::Int(1),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
        ],
        "param order seed/core-s/core-nxt/tail: {params:?}"
    );
}

#[test]
fn join_table_core_and_multi_hop_tail() {
    // Join-table self-link core (colleagues), 2-hop tail worksAt(FK) then locatedIn(FK).
    let core = LinkBacking::JoinTable {
        table: tref("colleagues"),
        from_key: "id".into(),
        from_column: "a".into(),
        to_column: "b".into(),
        to_key: "id".into(),
    };
    let tail_types = vec![
        ChainType {
            table: tref("person"),
            row_filters: vec![],
            predicates: vec![],
        },
        ChainType {
            table: tref("company"),
            row_filters: vec![],
            predicates: vec![],
        },
        ChainType {
            table: tref("city"),
            row_filters: vec![],
            predicates: vec![],
        },
    ];
    let tail_hops = vec![
        LinkBacking::ForeignKey {
            from_column: "worksat_id".into(),
            to_column: "id".into(),
        },
        LinkBacking::ForeignKey {
            from_column: "city_id".into(),
            to_column: "id".into(),
        },
    ];
    let (sql, _params) = compile_graph_reach_tail(
        &DuckDbDialect,
        &tref("person"),
        "id",
        &core,
        &[],
        &[],
        &tail_types,
        &tail_hops,
        &["id".to_string(), "cname".to_string()],
        &[],
        4,
        1000,
    )
    .unwrap();
    // Join-table core uses alias `j` inside the CTE.
    assert!(
        sql.contains(r#"cur."id" = j."a""#) && sql.contains(r#"j."b" = nxt."id""#),
        "join-table core arm: {sql}"
    );
    // Two tail hops: worksAt (t_0->t_1) and locatedIn (t_1->t_2). Final projection at t_2.
    assert!(
        sql.contains(r#"t_0."worksat_id" = t_1."id""#),
        "tail hop 1: {sql}"
    );
    assert!(
        sql.contains(r#"t_1."city_id" = t_2."id""#),
        "tail hop 2: {sql}"
    );
    assert!(
        sql.contains(r#"t_2."id""#) && sql.contains(r#"t_2."cname""#),
        "final type projected at t_2: {sql}"
    );
    // Glue still references t_0.
    assert!(
        sql.contains(r#"t_0."id" IN (SELECT id FROM reach WHERE depth >= 1)"#),
        "membership glue on t_0: {sql}"
    );
}
```

- [ ] **Step 2: Add the BUCK target**

In `src/services/query-api/BUCK`, after the `compile-graph-reach-union` target (ends ~line 533), add:

```python
rust_test(
    name = "compile-graph-reach-tail",
    crate = "compile_graph_reach_tail",
    srcs = ["tests/compile_graph_reach_tail.rs"],
    crate_root = "tests/compile_graph_reach_tail.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
    ],
)
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `buck2 test //src/services/query-api:compile-graph-reach-tail > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL|error\[|cannot find" /tmp/t1.log`
Expected: FAIL — `compile_graph_reach_tail`/`ChainType` not found (or unresolved import).

- [ ] **Step 4: Implement the compiler**

In `src/services/query-api/src/sql.rs`, immediately after `compile_graph_reach_union` (after its closing `}` at line 863, before the `compile_chain` wrapper doc at line 865), add:

```rust
/// Build the single-self-link recursive reachability CTE (`WITH RECURSIVE reach(id, depth) AS
/// (…)`) used by the recursive-core + relational-tail compiler. The emitted CTE is the
/// degenerate 1-step case of [`compile_graph_reach`]'s path-cycle CTE: a seed anchor governed by
/// `seed_predicates` + `row_filters` at alias `s`, a distinct `UNION`, and a single recursive
/// step following `backing` (the self-link, via the shared [`link_join`]) with `row_filters`
/// applied at the landing node `nxt` under the inlined `r.depth < depth` bound. Seed then
/// recursive params are appended to `params` in that order. This is a focused helper, NOT a
/// refactor of `compile_graph_reach` (whose CTE generalizes over a multi-link path); the shared
/// surface is the self-hop join, which already lives in `link_join`.
#[allow(clippy::too_many_arguments)]
fn recursive_reach_cte(
    dialect: &dyn SqlDialect,
    table: &TableRef,
    identity: &str,
    backing: &LinkBacking,
    seed_predicates: &[CallerPredicate],
    row_filters: &[RowFilter],
    depth: u32,
    params: &mut Vec<SqlValue>,
) -> Result<String, CompileError> {
    for f in row_filters {
        validate_row_filter(f, None).map_err(CompileError::MalformedFilter)?;
    }
    let q = |id: &str| dialect.quote_ident(id);
    let tbl = format!("{}.{}", q(&table.schema), q(&table.name));
    let id = q(identity);

    let mut seed_conj: Vec<String> = Vec::new();
    for p in seed_predicates {
        seed_conj.push(caller_predicate_sql(dialect, p, "s", params));
    }
    for f in row_filters {
        seed_conj.push(filter_sql(dialect, f, "s", params));
    }
    let seed_where = if seed_conj.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", seed_conj.join(" AND "))
    };

    let joins = link_join(dialect, backing, "cur", "nxt", &tbl, "j");

    let mut rec_conj: Vec<String> = vec![format!("r.depth < {depth}")];
    for f in row_filters {
        rec_conj.push(filter_sql(dialect, f, "nxt", params));
    }
    let rec_where = rec_conj.join(" AND ");

    Ok(format!(
        "WITH RECURSIVE reach(id, depth) AS (\
           SELECT s.{id}, 0 FROM {tbl} s{seed_where} \
           UNION \
           SELECT nxt.{id}, r.depth + 1 FROM reach r JOIN {tbl} cur ON cur.{id} = r.id{joins} WHERE {rec_where}\
         )"
    ))
}

/// Compile a depth-bounded recursive-core + relational-tail reachability read: from the seed set,
/// follow `core_backing` (a self-link on `table`) 1..`depth` times to a reachable set, then chain
/// `tail_hops` forward off that set (`tail_types[0]` = `table`, `tail_types[k]` = the projected
/// final type) and project the final type's columns DISTINCT. The recursive core is the
/// [`recursive_reach_cte`]; the tail is the shared [`chain_from_where`]; the two are glued by
/// `t_0.{identity} IN (SELECT id FROM reach WHERE depth >= 1)` (the depth>=1 reachable set,
/// excluding the seed unless a cycle re-reaches it). `tail_types[0]` MUST carry empty row-filters:
/// the queried type's governance lives in the CTE (`core_row_filters`), so re-applying at `t_0`
/// would only duplicate params. Param order: seed predicates, seed `core_row_filters` (s),
/// recursive `core_row_filters` (nxt), then the tail's per-position params. Precondition:
/// `tail_types.len() == tail_hops.len() + 1` and `tail_hops` non-empty. Every caller value is a
/// bound param.
#[allow(clippy::too_many_arguments)]
pub fn compile_graph_reach_tail(
    dialect: &dyn SqlDialect,
    table: &TableRef,
    identity: &str,
    core_backing: &LinkBacking,
    seed_predicates: &[CallerPredicate],
    core_row_filters: &[RowFilter],
    tail_types: &[ChainType],
    tail_hops: &[LinkBacking],
    allowed_cols: &[String],
    mask_cols: &[String],
    depth: u32,
    limit: u32,
) -> Result<(String, Vec<SqlValue>), CompileError> {
    debug_assert_eq!(
        tail_types.len(),
        tail_hops.len() + 1,
        "tail types must be hops + 1"
    );
    debug_assert!(!tail_hops.is_empty(), "part-B tail must have >= 1 hop");
    let q = |id: &str| dialect.quote_ident(id);
    let id = q(identity);
    let mut params: Vec<SqlValue> = Vec::new();

    // Recursive core CTE first (the CTE is textually first, so its `?` placeholders bind first).
    let cte = recursive_reach_cte(
        dialect,
        table,
        identity,
        core_backing,
        seed_predicates,
        core_row_filters,
        depth,
        &mut params,
    )?;

    // Relational tail: t_0 = `table` (the reachable set), chained forward to the final type t_k.
    let (from, conjuncts, tail_params) = chain_from_where(dialect, tail_types, tail_hops)?;
    params.extend(tail_params);

    let k = tail_hops.len();
    let final_alias = format!("t_{k}");
    let cols = allowed_cols
        .iter()
        .map(|c| {
            if mask_cols.iter().any(|m| m == c) {
                format!("'{MASK_MARKER}' AS {}", q(c))
            } else {
                format!("{final_alias}.{}", q(c))
            }
        })
        .collect::<Vec<_>>()
        .join(", ");

    // Glue: t_0 (the queried type at the tail's source) is constrained to the depth>=1 reach set.
    let mut where_conj = vec![format!("t_0.{id} IN (SELECT id FROM reach WHERE depth >= 1)")];
    where_conj.extend(conjuncts);
    let where_sql = where_conj.join(" AND ");

    let sql = format!(
        "{cte} SELECT DISTINCT {cols} FROM {from} WHERE {where_sql} {}",
        dialect.limit_clause(limit)
    );
    Ok((sql, params))
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `buck2 test //src/services/query-api:compile-graph-reach-tail > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t1.log`
Expected: PASS (3 tests).

- [ ] **Step 6: Verify the existing graph compilers are untouched**

Run: `buck2 test //src/services/query-api:compile-graph-reach //src/services/query-api:compile-graph-reach-union > /tmp/t1b.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t1b.log`
Expected: PASS (unchanged — confirms no regression to part-1/2/3 compilers).

- [ ] **Step 7: Commit**

```bash
git add src/services/query-api/src/sql.rs src/services/query-api/tests/compile_graph_reach_tail.rs src/services/query-api/BUCK
git commit -m "$(cat <<'EOF'
feat(query): compile_graph_reach_tail recursive-core + relational-tail SQL

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: `read_graph_reach_with_tail` handler + `BadGraphPath` error

**Files:**
- Modify: `src/services/query-api/src/handler.rs` (add `BadGraphPath` variant to `QueryError` ~line 69; add `GraphTailQuery` struct + `read_graph_reach_with_tail` after `read_graph_reach_union`, which ends ~line 1035)
- Create: `src/services/query-api/tests/graph_reach_tail.rs`
- Modify: `src/services/query-api/BUCK` (add the `graph-reach-tail` target)

**Interfaces:**
- Consumes (Task 1): `crate::sql::compile_graph_reach_tail(...)`; `crate::sql::ChainType`.
- Consumes (existing in `handler.rs`): `QueryDeps`, `Subject`, `ObjectRows`, `load_policy`, `project_allowed`, `identity_in_predicate`, `DEFAULT_LIMIT`, `Decision`, `Action`, `PolicyTarget`, `TypeName`, `ControlPlaneError`, `PageReq`, `crate::filter::{coerce_predicate, CallerPredicate}`.
- Produces (used by Task 3): `pub struct GraphTailQuery { pub type_name: String, pub core_link: String, pub tail_links: Vec<String>, pub depth: u32, pub filters: Vec<(String, String)>, pub ids: Vec<String> }`; `pub async fn read_graph_reach_with_tail(q: &GraphTailQuery, subject: &Subject, deps: &QueryDeps<'_>) -> Result<ObjectRows, QueryError>`; `QueryError::BadGraphPath(String)`.

- [ ] **Step 1: Write the failing test**

Create `src/services/query-api/tests/graph_reach_tail.rs`:

```rust
//! read_graph_reach_with_tail on an in-memory control plane + a stub serving engine. The stub
//! returns canned object rows matching the Company projection [id, name]; the test asserts the
//! governance short-circuits (BadGraphPath for a non-self core link, UnknownLink for an unknown
//! core/tail link, BadGraphPath for an empty tail, NoIdentity, Forbidden when a tail type is not
//! Read-granted) and the happy path returning the stub's projected rows. Real recursion + the
//! DISTINCT tail over DuckDB is the graph tail e2e.

use std::time::Duration;

use async_trait::async_trait;
use control_plane_core::{
    Acl, Action, Cardinality, Effect, LinkBacking, LinkDef, ObjectType, Ontology, PolicyTarget,
    PropertyDef, RoleId, SubjectId, TableRef, TypeName,
};
use control_plane_memory::MemoryControlPlane;
use query_api::handler::{GraphTailQuery, QueryDeps, QueryError, Subject, read_graph_reach_with_tail};
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
            name: "company".into(),
        },
        identity: Some("id".into()),
    }
}

/// Seed: Person with a `knows` FK self-link, plus a `worksAt` FK link Person -> Company (the tail
/// target). `grant_company` toggles whether the subject is granted Read on Company.
async fn seeded(person: ObjectType, grant_company: bool) -> (MemoryControlPlane, SubjectId) {
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
        name: "worksAt".into(),
        from: TypeName("Person".into()),
        to: TypeName("Company".into()),
        cardinality: Cardinality::One,
        backing: LinkBacking::ForeignKey {
            from_column: "worksat_id".into(),
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
    if grant_company {
        cp.grant(
            &reader,
            Action::Read,
            PolicyTarget::Type(TypeName("Company".into())),
            Effect::Allow,
        )
        .await
        .unwrap();
    }
    (cp, analyst)
}

fn tail_query(core: &str, tail: &[&str]) -> GraphTailQuery {
    GraphTailQuery {
        type_name: "Person".into(),
        core_link: core.into(),
        tail_links: tail.iter().map(|s| s.to_string()).collect(),
        depth: 3,
        filters: vec![],
        ids: vec![],
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn rejects_a_non_self_core_link() {
    let (cp, subj) = seeded(person_type(Some("id".into())), true).await;
    let serving = GraphServing { rows: vec![] };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &serving,
    };
    // `worksAt` lands on Company, not back on Person -> not a self-link core.
    let err = read_graph_reach_with_tail(&tail_query("worksAt", &["knows"]), &Subject(subj), &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, QueryError::BadGraphPath(_)),
        "expected BadGraphPath for a non-self core, got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn rejects_an_unknown_core_link() {
    let (cp, subj) = seeded(person_type(Some("id".into())), true).await;
    let serving = GraphServing { rows: vec![] };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &serving,
    };
    let err = read_graph_reach_with_tail(&tail_query("nope", &["worksAt"]), &Subject(subj), &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, QueryError::UnknownLink(l) if l == "nope"),
        "expected UnknownLink(nope), got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn rejects_an_unknown_tail_link() {
    let (cp, subj) = seeded(person_type(Some("id".into())), true).await;
    let serving = GraphServing { rows: vec![] };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &serving,
    };
    let err = read_graph_reach_with_tail(&tail_query("knows", &["nope"]), &Subject(subj), &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, QueryError::UnknownLink(l) if l == "nope"),
        "expected UnknownLink(nope), got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn rejects_an_empty_tail() {
    let (cp, subj) = seeded(person_type(Some("id".into())), true).await;
    let serving = GraphServing { rows: vec![] };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &serving,
    };
    let err = read_graph_reach_with_tail(&tail_query("knows", &[]), &Subject(subj), &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, QueryError::BadGraphPath(_)),
        "expected BadGraphPath for an empty tail, got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn rejects_a_type_without_identity() {
    let (cp, subj) = seeded(person_type(None), true).await;
    let serving = GraphServing { rows: vec![] };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &serving,
    };
    let err = read_graph_reach_with_tail(&tail_query("knows", &["worksAt"]), &Subject(subj), &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, QueryError::NoIdentity(t) if t == "Person"),
        "expected NoIdentity(Person), got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn forbids_when_tail_type_not_granted() {
    // Read on Person but NOT Company -> the tail's Read gate denies.
    let (cp, subj) = seeded(person_type(Some("id".into())), false).await;
    let serving = GraphServing { rows: vec![] };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &serving,
    };
    let err = read_graph_reach_with_tail(&tail_query("knows", &["worksAt"]), &Subject(subj), &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, QueryError::Forbidden),
        "expected Forbidden (Company not Read-granted), got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn returns_projected_tail_objects() {
    let (cp, subj) = seeded(person_type(Some("id".into())), true).await;
    let serving = GraphServing {
        rows: vec![
            vec![SqlValue::Int(11), SqlValue::Text("Acme".into())],
            vec![SqlValue::Int(12), SqlValue::Text("Beta".into())],
        ],
    };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &serving,
    };
    let rows = read_graph_reach_with_tail(&tail_query("knows", &["worksAt"]), &Subject(subj), &deps)
        .await
        .unwrap();
    // Projection is the FINAL tail type (Company): columns [id, name], logical [Long, Text].
    assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
    assert_eq!(
        rows.logical_types,
        vec!["Long".to_string(), "Text".to_string()]
    );
    assert_eq!(
        rows.rows,
        vec![
            vec![SqlValue::Int(11), SqlValue::Text("Acme".into())],
            vec![SqlValue::Int(12), SqlValue::Text("Beta".into())],
        ]
    );
}
```

- [ ] **Step 2: Add the BUCK target**

In `src/services/query-api/BUCK`, after the `graph-reach-union` target (ends ~line 500), add:

```python
rust_test(
    name = "graph-reach-tail",
    crate = "graph_reach_tail",
    srcs = ["tests/graph_reach_tail.rs"],
    crate_root = "tests/graph_reach_tail.rs",
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

- [ ] **Step 3: Run the test to verify it fails**

Run: `buck2 test //src/services/query-api:graph-reach-tail > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL|error\[|cannot find" /tmp/t2.log`
Expected: FAIL — `GraphTailQuery`/`read_graph_reach_with_tail`/`BadGraphPath` not found.

- [ ] **Step 4: Add the `BadGraphPath` error variant**

In `src/services/query-api/src/handler.rs`, in the `QueryError` enum, immediately after the `NotCyclicPath` variant (line 69), add:

```rust
    /// `/graph` part-B: the `*`-suffixed recursive core is malformed — the core link is not a
    /// self-link on the queried type, or the relational tail is empty. (Structural faults — a
    /// misplaced/duplicated `*` — are rejected by the HTTP layer before the handler.)
    #[error("malformed graph path: {0}")]
    BadGraphPath(String),
```

- [ ] **Step 5: Add `GraphTailQuery` + `read_graph_reach_with_tail`**

In `src/services/query-api/src/handler.rs`, after `read_graph_reach_union` (after its closing `}` ~line 1035), add:

```rust
/// A bounded recursive-core + relational-tail reachability read. `core_link` is a `*`-suffixed
/// self-link on `type_name` followed transitively up to `depth` times (the recursive core);
/// `tail_links` is a forward chain of ordinary links continuing from the depth>=1 reachable set,
/// landing on a (possibly different) final type that is projected. `filters`/`ids` scope the SEED
/// set (the recursion start), as in part-1/2/3.
pub struct GraphTailQuery {
    pub type_name: String,
    pub core_link: String,
    pub tail_links: Vec<String>,
    pub depth: u32,
    pub filters: Vec<(String, String)>,
    pub ids: Vec<String>,
}

/// Serve a recursive-core + relational-tail read: from the seed set, follow `core_link` (a
/// self-link) 1..`depth` times to a reachable set, then chain `tail_links` forward off that set
/// and project the final type. Governed: Read on the queried type (whose row-filters govern the
/// recursive core, applied at the seed and every recursive expansion) AND every tail-reached type
/// (row-filters at each), declared identity on the queried type (the recursion's dedup key + the
/// join key from the tail back to the reachable set). The core link must be a self-link and the
/// tail non-empty, else `BadGraphPath`; an unknown core/tail link -> `UnknownLink`.
pub async fn read_graph_reach_with_tail(
    q: &GraphTailQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<ObjectRows, QueryError> {
    let type_name = TypeName(q.type_name.clone());
    let target = PolicyTarget::Type(type_name.clone());

    // Read gate on the queried (core) type (deny-by-default, before existence is revealed).
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
    let (core_filters, denied, masked) = load_policy(deps.acl, &subject.0, &target).await?;

    // Declared identity: the recursion's dedup key and the join key from the tail back to reach.
    let identity = object_type
        .identity
        .clone()
        .ok_or_else(|| QueryError::NoIdentity(q.type_name.clone()))?;

    // The relational tail must be non-empty (a bare recursive core is `/graph/:link`).
    if q.tail_links.is_empty() {
        return Err(QueryError::BadGraphPath(format!(
            "recursive core `{}*` requires a relational tail; use /graph/:link for bare reachability",
            q.core_link
        )));
    }

    // Resolve the recursive core link: an outbound link of the queried type whose `to` is the
    // queried type itself (a self-link).
    let links = deps.ontology.links(&type_name, PageReq::unbounded()).await?;
    let core = links
        .items
        .iter()
        .find(|l| l.name == q.core_link)
        .ok_or_else(|| QueryError::UnknownLink(q.core_link.clone()))?;
    if core.to != type_name {
        return Err(QueryError::BadGraphPath(format!(
            "recursive core `{}*` must land back on `{}`",
            q.core_link, q.type_name
        )));
    }
    let core_backing = core.backing.clone();

    // Resolve the forward tail. Position 0 is the queried type with EMPTY row-filters — its
    // governance lives in the recursive CTE; the tail constrains it by reach-membership. Each
    // tail-landed type is Read-gated and its row-filters loaded; the final landing is projected.
    let mut tail_types: Vec<crate::sql::ChainType> = vec![crate::sql::ChainType {
        table: object_type.table.clone(),
        row_filters: vec![],
        predicates: vec![],
    }];
    let mut tail_hops: Vec<control_plane_core::LinkBacking> =
        Vec::with_capacity(q.tail_links.len());
    let mut current = type_name.clone();
    let mut final_type = object_type.clone();
    let mut final_denied = denied.clone();
    let mut final_masked = masked.clone();
    for link_name in &q.tail_links {
        let outbound = deps.ontology.links(&current, PageReq::unbounded()).await?;
        let link = outbound
            .items
            .into_iter()
            .find(|l| &l.name == link_name)
            .ok_or_else(|| QueryError::UnknownLink(link_name.clone()))?;
        let landed = link.to.clone();
        let landed_target = PolicyTarget::Type(landed.clone());
        // Read on every reached type (the leak-free guarantee).
        if deps
            .acl
            .check(&subject.0, Action::Read, &landed_target)
            .await?
            == Decision::Deny
        {
            return Err(QueryError::Forbidden);
        }
        let landed_type = deps.ontology.get_type(&landed).await?;
        let (l_filters, l_denied, l_masked) =
            load_policy(deps.acl, &subject.0, &landed_target).await?;
        tail_hops.push(link.backing.clone());
        tail_types.push(crate::sql::ChainType {
            table: landed_type.table.clone(),
            row_filters: l_filters,
            predicates: vec![],
        });
        final_type = landed_type;
        final_denied = l_denied;
        final_masked = l_masked;
        current = landed;
    }

    // Projection: the FINAL tail type's visible columns (masked -> marker). Empty -> Forbidden.
    let allowed = project_allowed(&final_type.properties, &final_denied);
    if allowed.is_empty() {
        return Err(QueryError::Forbidden);
    }
    let mask_cols: Vec<String> = allowed
        .iter()
        .filter(|c| final_masked.contains(*c))
        .cloned()
        .collect();

    // Seed predicates scope the recursion start (alias `s` in the CTE): source filters
    // (visibility-checked + coerced against the queried type) then the ?_ids= set.
    let source_allowed = project_allowed(&object_type.properties, &denied);
    let mut seed_predicates: Vec<crate::filter::CallerPredicate> = Vec::new();
    for (col, raw) in &q.filters {
        if !source_allowed.contains(col) || masked.contains(col) {
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
        q.depth,
        DEFAULT_LIMIT,
    )?;
    let served = deps.serving.fetch_rows(&sql, &params).await?;
    let logical_types: Vec<String> = allowed
        .iter()
        .map(|name| {
            final_type
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

- [ ] **Step 6: Run the test to verify it passes**

Run: `buck2 test //src/services/query-api:graph-reach-tail > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t2.log`
Expected: PASS (7 tests).

- [ ] **Step 7: Commit**

```bash
git add src/services/query-api/src/handler.rs src/services/query-api/tests/graph_reach_tail.rs src/services/query-api/BUCK
git commit -m "$(cat <<'EOF'
feat(query): read_graph_reach_with_tail handler + BadGraphPath error

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: HTTP `*` parse + dispatch + DuckDB e2e

**Files:**
- Modify: `src/services/query-api/src/http.rs` (`get_graph_path` `*` detection + dispatch ~line 388-406; `graph_error` `BadGraphPath` arm ~line 410; add `graph_tail_respond`; extend the `handler` import)
- Create: `src/services/query-api/tests/graph_tail_e2e.rs`
- Modify: `src/services/query-api/BUCK` (add the `graph-tail-e2e` target)

**Interfaces:**
- Consumes (Task 2): `read_graph_reach_with_tail`, `GraphTailQuery`, `QueryError::BadGraphPath`.
- Consumes (existing in `http.rs`): `AppState`, `QueryDeps`, `Subject`, `SubjectId`, `graph_error`, `MAX_GRAPH_DEPTH`, `DEFAULT_GRAPH_DEPTH`, the `read_graph_reach`/`read_graph_reach_union` imports.

- [ ] **Step 1: Write the failing e2e test**

Create `src/services/query-api/tests/graph_tail_e2e.rs`:

```rust
//! Graph recursive-core + relational-tail e2e: GET /objects/:type/graph?path=knows*,worksAt over
//! the real HTTP router backed by DuckDB-over-DuckLake. Proves:
//!   - knows*,worksAt from {1} returns companies of the depth>=1 reachable people {2,3}, and
//!     EXCLUDES company 10 (person 1's own employer — the seed is not in its own reach set),
//!   - a multi-hop tail knows*,worksAt,locatedIn projects the final City type and DEDUPS (two
//!     companies sharing one city collapse to a single row via SELECT DISTINCT),
//!   - a Read row-filter (active=true) on Person prunes the recursive core (inactive person 3 is
//!     cut, dropping its company 12),
//!   - worksAt,knows* (the `*` is not the path prefix) -> 400,
//!   - knows* alone (empty relational tail) -> 400.
//!
//! Graph: person(id, name, active, knows_id, worksat_id) with a `knows` FK self-link (1->2, 2->3)
//! and a `worksAt` FK link to company(id, cname, city_id), which has a `locatedIn` FK link to
//! city(id, cname). worksAt: 1->10, 2->11, 3->12. locatedIn: 10->22, 11->20, 12->20 (companies 11
//! and 12 share city 20). Person 3 is inactive (for the row-filter test). All three types declare
//! identity `id`.

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

/// Seed person/company/city. knows: 1->2, 2->3 (FK knows_id). worksAt: 1->10, 2->11, 3->12 (FK
/// worksat_id). locatedIn: 10->22, 11->20, 12->20 (FK city_id; companies 11 & 12 share city 20).
/// Person 3 is INACTIVE. The caller MUST keep the returned `DuckLakeWriter` alive.
async fn setup(fx: &PgFixture) -> (PgControlPlane, EmbeddedDuckDb, DuckLakeWriter) {
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(writer.data_path()).unwrap());

    // person(id, name, active, knows_id, worksat_id).
    let person = tref("main", "person");
    let person_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("active", DataType::Boolean, false),
        Field::new("knows_id", DataType::Int64, true),
        Field::new("worksat_id", DataType::Int64, true),
    ]));
    let person_batch = RecordBatch::try_new(
        person_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("ann"), Some("bob"), Some("cal")])),
            // person 3 inactive (row-filter test); 1 and 2 active.
            Arc::new(BooleanArray::from(vec![true, true, false])),
            // knows: 1->2, 2->3 (3 has no outbound knows edge).
            Arc::new(Int64Array::from(vec![Some(2), Some(3), None])),
            // worksAt: 1->10, 2->11, 3->12.
            Arc::new(Int64Array::from(vec![Some(10), Some(11), Some(12)])),
        ],
    )
    .unwrap();
    land(&cp, &store, &person, person_schema, person_batch).await;

    // company(id, cname, city_id). locatedIn: 10->22, 11->20, 12->20.
    let company = tref("main", "company");
    let company_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("cname", DataType::Utf8, true),
        Field::new("city_id", DataType::Int64, true),
    ]));
    let company_batch = RecordBatch::try_new(
        company_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![10, 11, 12])),
            Arc::new(StringArray::from(vec![Some("x"), Some("acme"), Some("beta")])),
            Arc::new(Int64Array::from(vec![Some(22), Some(20), Some(20)])),
        ],
    )
    .unwrap();
    land(&cp, &store, &company, company_schema, company_batch).await;

    // city(id, cname).
    let city = tref("main", "city");
    let city_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("cname", DataType::Utf8, true),
    ]));
    let city_batch = RecordBatch::try_new(
        city_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![20, 22])),
            Arc::new(StringArray::from(vec![Some("hq"), Some("z")])),
        ],
    )
    .unwrap();
    land(&cp, &store, &city, city_schema, city_batch).await;

    cp.define_type(ObjectType {
        name: TypeName("Person".into()),
        properties: vec![
            prop("id", "Long", true),
            prop("name", "String", false),
            prop("active", "Boolean", true),
            prop("knows_id", "Long", false),
            prop("worksat_id", "Long", false),
        ],
        derived: vec![],
        table: person.clone(),
        identity: Some("id".into()),
    })
    .await
    .unwrap();
    cp.define_type(ObjectType {
        name: TypeName("Company".into()),
        properties: vec![
            prop("id", "Long", true),
            prop("cname", "String", false),
            prop("city_id", "Long", false),
        ],
        derived: vec![],
        table: company.clone(),
        identity: Some("id".into()),
    })
    .await
    .unwrap();
    cp.define_type(ObjectType {
        name: TypeName("City".into()),
        properties: vec![prop("id", "Long", true), prop("cname", "String", false)],
        derived: vec![],
        table: city.clone(),
        identity: Some("id".into()),
    })
    .await
    .unwrap();

    // knows: FK self-link Person -> Person via knows_id.
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
    // worksAt: FK link Person -> Company via worksat_id.
    cp.define_link(LinkDef {
        name: "worksAt".into(),
        from: TypeName("Person".into()),
        to: TypeName("Company".into()),
        cardinality: Cardinality::One,
        backing: LinkBacking::ForeignKey {
            from_column: "worksat_id".into(),
            to_column: "id".into(),
        },
    })
    .await
    .unwrap();
    // locatedIn: FK link Company -> City via city_id.
    cp.define_link(LinkDef {
        name: "locatedIn".into(),
        from: TypeName("Company".into()),
        to: TypeName("City".into()),
        cardinality: Cardinality::One,
        backing: LinkBacking::ForeignKey {
            from_column: "city_id".into(),
            to_column: "id".into(),
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

/// Sorted `id`s from an {"objects":[...]} body. `id` is a `Long`, rendered as a numeric STRING.
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
async fn core_then_tail_projects_companies_of_reachable_people() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;
    grant_read(&cp, &role, "Company").await;

    // knows* from {1} depth 3 => reachable people {2,3} (1 excluded: depth>=1). worksAt of {2,3}
    // = companies {11,12}. Company 10 (person 1's employer) is ABSENT — 1 is not in its own reach.
    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph?path=knows*,worksAt&depth=3&_ids=1",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        ids(&body),
        vec![11, 12],
        "companies of reachable {{2,3}}, excluding seed's company 10: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn multi_hop_tail_projects_cities_and_dedups() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;
    grant_read(&cp, &role, "Company").await;
    grant_read(&cp, &role, "City").await;

    // knows*,worksAt,locatedIn from {1}: reach {2,3} -> companies {11,12} -> cities {20,20}.
    // SELECT DISTINCT collapses the shared city 20 to a single row. City 22 (company 10's city) is
    // absent. => {20}.
    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph?path=knows*,worksAt,locatedIn&depth=3&_ids=1",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        ids(&body),
        vec![20],
        "two companies share city 20, deduped to one row; city 22 absent: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn row_filter_prunes_the_recursive_core() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    // active=true on Person cuts inactive person 3 from the recursive core. From {1}: 1->2 (active,
    // d1), 2->3 (3 inactive, cut). reach = {2}. worksAt of {2} = {11}. (Unfiltered it is {11,12}.)
    let (_c, role) = subject_with_role(&cp, "carol").await;
    grant_read(&cp, &role, "Person").await;
    grant_read(&cp, &role, "Company").await;
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
        "/objects/Person/graph?path=knows*,worksAt&depth=3&_ids=1",
        "carol",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        ids(&body),
        vec![11],
        "inactive person 3 cut from the core drops company 12: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn star_not_on_first_segment_is_400() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;
    grant_read(&cp, &role, "Company").await;

    // `*` on the second segment -> the recursive core is not the path prefix -> 400.
    let (status, _body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph?path=worksAt,knows*&depth=3&_ids=1",
        "alice",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "`*` not on the first segment -> 400"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_tail_is_400() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Person").await;

    // `knows*` alone (no tail) -> 400.
    let (status, _body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Person/graph?path=knows*&depth=3&_ids=1",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "empty relational tail -> 400");
}
```

- [ ] **Step 2: Add the BUCK target**

In `src/services/query-api/BUCK`, after the `graph-union-e2e` target (ends ~line 298), add:

```python
loom_fixture_test(
    name = "graph-tail-e2e",
    crate = "graph_tail_e2e",
    srcs = ["tests/graph_tail_e2e.rs"],
    crate_root = "tests/graph_tail_e2e.rs",
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

- [ ] **Step 3: Run the e2e to verify it fails**

Run: `buck2 test //src/services/query-api:graph-tail-e2e > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL|error\[|cannot find" /tmp/t3.log`
Expected: FAIL — the `path=knows*,…` requests currently route to part-2 (`read_graph_reach`), so `knows*` is treated as a literal link name → `UnknownLink` (404), not the part-B result. (Routing not yet added.)

- [ ] **Step 4: Add the `BadGraphPath` arm to `graph_error`**

In `src/services/query-api/src/http.rs`, in `graph_error` (line 410), add an arm after the `NotCyclicPath` arm (line 414):

```rust
        QueryError::BadGraphPath(m) => (StatusCode::BAD_REQUEST, m).into_response(),
```

- [ ] **Step 5: Extend the handler import**

In `src/services/query-api/src/http.rs`, find the `use crate::handler::{…}` import that brings in `read_graph_reach`, `read_graph_reach_union`, `GraphQuery`, `GraphUnionQuery` and add `GraphTailQuery` and `read_graph_reach_with_tail` to it. (If the imports are split across lines, add them alongside the existing graph handler imports.)

- [ ] **Step 6: Add `*` detection + dispatch in `get_graph_path`**

In `src/services/query-api/src/http.rs`, in `get_graph_path`, replace the dispatch block (the code from `// Exactly one of \`path\` …` through the final `graph_respond(...)` call, lines 388-406) with:

```rust
    // Exactly one of `path` (ordered cycle / `*` recursive-core+tail) or `links` (self-link
    // union) selects the mode.
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
    // Part B: a `*`-suffixed segment marks a recursive core followed by a relational tail. The
    // `*` MUST be on exactly one segment, and that segment MUST be the path prefix (index 0).
    let starred: Vec<usize> = path
        .iter()
        .enumerate()
        .filter(|(_, s)| s.ends_with('*'))
        .map(|(i, _)| i)
        .collect();
    if !starred.is_empty() {
        if starred.len() > 1 {
            return (
                StatusCode::BAD_REQUEST,
                "at most one path segment may be marked recursive with `*`",
            )
                .into_response();
        }
        if starred[0] != 0 {
            return (
                StatusCode::BAD_REQUEST,
                "the recursive `*` segment must be the first path segment",
            )
                .into_response();
        }
        let core_link = path[0].trim_end_matches('*').to_string();
        let tail_links: Vec<String> = path[1..].to_vec();
        return graph_tail_respond(
            &st, type_name, core_link, tail_links, depth, filters, ids, subject,
        )
        .await;
    }
    graph_respond(&st, type_name, path, depth, filters, ids, subject).await
```

- [ ] **Step 7: Add the `graph_tail_respond` helper**

In `src/services/query-api/src/http.rs`, after `graph_union_respond` (after its closing `}`), add:

```rust
/// Recursive-core + relational-tail (`?path=l0*,l1,…`) tail: build a `GraphTailQuery`, run
/// `read_graph_reach_with_tail`, map via `graph_error`.
#[allow(clippy::too_many_arguments)]
async fn graph_tail_respond(
    st: &AppState,
    type_name: String,
    core_link: String,
    tail_links: Vec<String>,
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
    match read_graph_reach_with_tail(
        &GraphTailQuery {
            type_name,
            core_link,
            tail_links,
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

- [ ] **Step 8: Run the e2e to verify it passes**

Run: `buck2 test //src/services/query-api:graph-tail-e2e > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t3.log`
Expected: PASS (5 tests).

- [ ] **Step 9: Run the full query-api graph suite (regression check)**

Run: `buck2 test //src/services/query-api:graph-tail-e2e //src/services/query-api:graph-reach-tail //src/services/query-api:compile-graph-reach-tail //src/services/query-api:graph-union-e2e //src/services/query-api:graph-reach-e2e //src/services/query-api:graph-path-e2e > /tmp/t3b.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t3b.log`
Expected: PASS (no regression to part-1/2/3 routes).

- [ ] **Step 10: Lint**

Run: `./tools/clippy-all.sh > /tmp/t3c.log 2>&1; grep -E "warning|error|clean|FAIL" /tmp/t3c.log | head`
Expected: clean (no warnings on the changed crates).

- [ ] **Step 11: Commit**

```bash
git add src/services/query-api/src/http.rs src/services/query-api/tests/graph_tail_e2e.rs src/services/query-api/BUCK
git commit -m "$(cat <<'EOF'
feat(query): GET /objects/:type/graph?path=l0*,tail recursive-core+tail route

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: Mark part B delivered in the roadmap docs

**Files:**
- Modify: `docs/FUTURE.md` (the `/graph` surface section — add a part-B DELIVERED paragraph; remove the multi-edge... wait — remove the "Recursive-core + relational-tail" bullet from the deferred list)
- Modify: `docs/superpowers/specs/2026-06-06-loom-roadmap.md` (add a part-B delivered note; drop the recursive-core+relational-tail clause from the deferred list)

**Interfaces:** none (documentation only).

- [ ] **Step 1: Update `docs/FUTURE.md`**

Open `docs/FUTURE.md`, find the `/graph` surface section. After the **Part 3 — multi-edge union reachability is DELIVERED** paragraph, add:

```markdown
**Part B — recursive-core + relational-tail is DELIVERED.** `GET
/objects/:type/graph?path=l0*,l1,…,lk&depth=N` follows a `*`-suffixed self-link
core (`l0`, the path prefix) transitively up to N hops, then chains the forward
relational tail `l1..lk` off the depth≥1 reachable set, projecting the final
tail type (possibly a different type). The recursive core reuses the single
self-link CTE; the tail reuses the relational chain compiler; the two are glued
by `t_0.id IN (SELECT id FROM reach WHERE depth >= 1)`. Governance is layered:
Read + row-filters on the queried type (in the CTE) and on every tail-reached
type. Caller `?param` filters scope the seed; the tail is forward-only. A
misplaced/duplicated `*`, a non-self core link, or an empty tail is a 400.
```

Then locate the deferred bullet that reads **Recursive-core + relational-tail (`path=knows*,worksAt`).** … and delete that entire bullet (it is now delivered).

- [ ] **Step 2: Update `docs/superpowers/specs/2026-06-06-loom-roadmap.md`**

Open `docs/superpowers/specs/2026-06-06-loom-roadmap.md`, find the `/graph` slice entries. Add a delivered note for part B alongside the part-1/2/3 notes (mirror their phrasing):

```markdown
- `/graph` part-B (recursive-core + relational-tail, `path=l0*,l1,…`): DELIVERED — a `*`-suffixed
  self-link core followed by a forward relational tail to the projected (possibly different) type.
```

Then remove the "recursive-core + relational-tail" clause from the deferred `/graph` list in that file (leave the other deferred parts — inverse links in the path, graph-aware filter addressing, min-depth annotation, shortest-path/`/tree`, weighted edges — intact).

- [ ] **Step 3: Verify the docs reference no longer lists part B as deferred**

Run: `grep -rn "recursive-core + relational-tail\|knows\*,worksAt" docs/FUTURE.md docs/superpowers/specs/2026-06-06-loom-roadmap.md`
Expected: matches appear ONLY in the new "DELIVERED" text, not under any "deferred"/"remaining" heading.

- [ ] **Step 4: Commit**

```bash
git add docs/FUTURE.md docs/superpowers/specs/2026-06-06-loom-roadmap.md
git commit -m "$(cat <<'EOF'
docs(query): mark /graph part-B (recursive-core + relational-tail) delivered

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Self-Review

**1. Spec coverage:**
- Surface (`?path=l0*,tail`, single `*`, prefix, self-link core, forward multi-hop tail, depth bounds core, `_ids` seed, seed-only `?param`) → Task 3 (HTTP parse/dispatch) + Task 2 (handler validation) + Task 1 (compiler). ✓
- SQL shape (recursive CTE + chain tail + `IN (SELECT id FROM reach WHERE depth >= 1)`, `SELECT DISTINCT` of final type) → Task 1, asserted by `compile_graph_reach_tail.rs`. ✓
- Governance (Read gate core + every tail type, row-filters in CTE + per tail position, identity on core type, depth≥1) → Task 2 handler + `graph_reach_tail.rs` (Forbidden/NoIdentity) + `graph_tail_e2e.rs` (row-filter prune, seed exclusion). ✓
- `recursive_reach_cte` is a focused helper, not a refactor of `compile_graph_reach` → Task 1 Step 6 verifies part-1/2 compilers untouched. ✓
- Error: `BadGraphPath`→400 (non-self core, empty tail in handler; `*` misplaced/duplicated in HTTP) → Task 2 (variant + handler) + Task 3 (`graph_error` arm + HTTP structural 400s). ✓
- Tests: 3 targets, e2e via `loom_fixture_test` (`duckdb=True`) → Tasks 1-3 BUCK. ✓
- Docs: part B delivered, dropped from deferred → Task 4. ✓
- Out of scope (multi-link/union cores, inverse tail, positional filters) → not implemented; HTTP rejects >1 `*`; tail is forward-only. ✓

**2. Placeholder scan:** No TBD/TODO/"similar to"/"add error handling" — every code step is verbatim. ✓

**3. Type consistency:** `compile_graph_reach_tail` signature is identical in Task 1 (Produces), Task 1 implementation, and Task 2's call site. `GraphTailQuery` fields identical in Task 2 (struct), Task 2 test, and Task 3 (`graph_tail_respond`). `ChainType{table, row_filters, predicates}` matches the existing struct (sql.rs:403). `recursive_reach_cte` appends to `&mut params` (seed then recursive) — consumed correctly by `compile_graph_reach_tail`. The membership predicate string `t_0.{id} IN (SELECT id FROM reach WHERE depth >= 1)` matches the e2e/unit assertions. ✓

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-06-19-graph-recursive-core-relational-tail.md`.
