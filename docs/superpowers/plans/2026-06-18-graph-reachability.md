# `/graph` Part-1: Bounded Recursive Reachability — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Serve `GET /objects/:type/graph/:link?depth=N` — the deduped set of objects reachable from a seed set via 1..N hops of a self-link — as a depth-bounded `WITH RECURSIVE` query, governed at every expansion.

**Architecture:** A new `compile_graph_reach` emits a `WITH RECURSIVE` CTE reusing the existing per-hop join shapes and `filter_sql`/`caller_predicate_sql`; `read_graph_reach` resolves the self-link + declared identity, builds seed predicates (source filters + `?_ids=`), and renders the type's ACL row-filters at the seed, every recursive expansion, and the final projection. A new route wires it; `objects_to_json` renders it.

**Tech Stack:** Rust, buck2, axum HTTP, DuckDB serving engine (`WITH RECURSIVE` native), hermetic Postgres/DuckDB fixture tests.

**Design:** `docs/superpowers/specs/2026-06-18-graph-reachability-design.md`

## Global Constraints

- Never run two `buck2` commands concurrently. One at a time.
- Never pipe `buck2 test` through `tail`/`head` — redirect and grep:
  `buck2 test //target > /tmp/t.log 2>&1; grep -nE "Tests finished|Pass [0-9]|FAIL|error\[|panicked" /tmp/t.log`. Fixture tests take minutes — allow up to 600000ms.
- Tests are integration `rust_test`/`loom_fixture_test` targets only — never inline `#[test]` in `src/**`.
- If the rustfmt pre-commit hook fails, run `buck2 run //tools:rustfmt -- <files>`, re-stage, re-commit.
- Depth bounds: `MAX_GRAPH_DEPTH = 10`, default `5`; `depth < 1` or `> 10` → 400 (validated at the HTTP edge).
- Every caller VALUE is a bound `?` param; identifiers come only from trusted ontology/ACL metadata and are double-quoted (the injection boundary). `depth` is a validated `u32`, safe to inline.
- Commit trailer: `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`

---

## Task 1: Compiler — `compile_graph_reach`

**Files:**
- Modify: `src/services/query-api/src/sql.rs` (add `compile_graph_reach`)
- Create: `src/services/query-api/tests/compile_graph_reach.rs` + BUCK target

**Interfaces:**
- Consumes: `filter_sql`, `caller_predicate_sql`, `MASK_MARKER`, `validate_row_filter`, `CallerPredicate`, `RowFilter`, `LinkBacking`, `TableRef`, `SqlValue` (all in/around `sql.rs`).
- Produces: `pub fn compile_graph_reach(dialect: &dyn SqlDialect, table: &TableRef, identity: &str, backing: &LinkBacking, seed_predicates: &[CallerPredicate], row_filters: &[RowFilter], allowed_cols: &[String], mask_cols: &[String], depth: u32, limit: u32) -> Result<(String, Vec<SqlValue>), CompileError>`

- [ ] **Step 1: Write the failing compiler unit test**

Create `src/services/query-api/tests/compile_graph_reach.rs`:

```rust
//! compile_graph_reach emits a depth-bounded WITH RECURSIVE reachability query over a
//! self-link, governed by the type's row-filters at seed/expansion/projection.

use control_plane_core::{CompareOp, LinkBacking, RowFilter, ScalarValue, TableRef};
use query_api::filter::CallerPredicate;
use query_api::serving::SqlValue;
use query_api::sql::{DuckDbDialect, compile_graph_reach};

fn person() -> TableRef {
    TableRef { schema: "main".into(), name: "person".into() }
}

#[test]
fn fk_self_link_recursive_reach() {
    // Person.knows_id -> Person.id (FK self-link).
    let backing = LinkBacking::ForeignKey {
        from_column: "knows_id".into(),
        to_column: "id".into(),
    };
    let (sql, params) = compile_graph_reach(
        &DuckDbDialect,
        &person(),
        "id",
        &backing,
        &[],   // no seed predicates
        &[],   // no row-filters
        &["id".to_string(), "name".to_string()],
        &[],
        3,
        1000,
    )
    .unwrap();
    assert!(params.is_empty());
    assert!(sql.contains("WITH RECURSIVE reach(id, depth) AS"), "got: {sql}");
    assert!(sql.contains("r.depth < 3"), "depth bound inlined: {sql}");
    assert!(sql.contains("UNION"), "recursive union: {sql}");
    // FK hop cur -> nxt.
    assert!(sql.contains(r#"cur."knows_id" = nxt."id""#), "fk join: {sql}");
    // reachable in >= 1 hop, projected from p.
    assert!(sql.contains("depth >= 1"), "reachability bound: {sql}");
    assert!(sql.contains("SELECT DISTINCT") && sql.contains(r#"p."name""#), "projection: {sql}");
}

#[test]
fn join_table_self_link_and_row_filter_and_seed() {
    // Person knows Person via a knows(a, b) join table; an ACL row-filter on `active`;
    // a seed In-predicate on the identity (object-set seeds).
    let backing = LinkBacking::JoinTable {
        table: TableRef { schema: "main".into(), name: "knows".into() },
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
    let (sql, params) = compile_graph_reach(
        &DuckDbDialect,
        &person(),
        "id",
        &backing,
        &seed,
        &row_filters,
        &["id".to_string()],
        &[],
        2,
        1000,
    )
    .unwrap();
    // join-table hop: cur.id = j.a  AND  j.b = nxt.id
    assert!(sql.contains(r#"cur."id" = j."a""#) && sql.contains(r#"j."b" = nxt."id""#), "jt join: {sql}");
    // the row-filter is rendered at all three positions (seed s, expansion nxt, projection p).
    assert!(sql.contains(r#"s."active""#) && sql.contains(r#"nxt."active""#) && sql.contains(r#"p."active""#), "row-filter at 3 positions: {sql}");
    // seed In-predicate bound; params: seed In (1) + active at s, nxt, p (3) = 4 bound values.
    assert_eq!(params.len(), 4, "1 seed id + 3 row-filter renderings; got {params:?}");
    assert_eq!(params[0], SqlValue::Int(5));
}
```

Add the BUCK target in `src/services/query-api/BUCK` (mirror the `compile-chain-pairs` pure-logic target):

```python
rust_test(
    name = "compile-graph-reach",
    crate = "compile_graph_reach",
    srcs = ["tests/compile_graph_reach.rs"],
    crate_root = "tests/compile_graph_reach.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
    ],
)
```

- [ ] **Step 2: Run to verify failure**

Run: `buck2 test //src/services/query-api:compile-graph-reach > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: FAIL (compile error — `compile_graph_reach` undefined).

- [ ] **Step 3: Implement `compile_graph_reach`**

In `src/services/query-api/src/sql.rs`, after `compile_chain_pairs`, add:

```rust
/// Compile a depth-bounded recursive reachability query over a SELF-link: the deduped set
/// of `table` rows reachable from the seed set (rows matching `seed_predicates` + the type's
/// `row_filters`) via 1..`depth` hops of `backing`. Governed: `row_filters` are rendered at
/// the seed (`s`), every recursive expansion (`nxt`), and the final projection (`p`), so a
/// node is reached only through permitted rows. `identity` is the dedup/visited key (the
/// CTE column `id`). Termination is guaranteed by the inlined `depth` bound regardless of
/// cycles; the outer `DISTINCT` dedups the node set. Every caller value is a bound param.
pub fn compile_graph_reach(
    dialect: &dyn SqlDialect,
    table: &TableRef,
    identity: &str,
    backing: &LinkBacking,
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

    // Anchor (seed) WHERE at alias `s`: caller seed predicates, then the ACL row-filters.
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

    // Recursive step: join the link cur -> nxt, bound depth, govern `nxt` with row-filters.
    let link_join = match backing {
        LinkBacking::ForeignKey {
            from_column,
            to_column,
        } => format!(" JOIN {tbl} nxt ON cur.{} = nxt.{}", q(from_column), q(to_column)),
        LinkBacking::JoinTable {
            table: jt,
            from_key,
            from_column,
            to_column,
            to_key,
        } => {
            let jtbl = format!("{}.{}", q(&jt.schema), q(&jt.name));
            format!(
                " JOIN {jtbl} j ON cur.{} = j.{} JOIN {tbl} nxt ON j.{} = nxt.{}",
                q(from_key),
                q(from_column),
                q(to_column),
                q(to_key),
            )
        }
    };
    let mut rec_conj: Vec<String> = vec![format!("r.depth < {depth}")];
    for f in row_filters {
        rec_conj.push(filter_sql(dialect, f, "nxt", &mut params));
    }
    let rec_where = rec_conj.join(" AND ");

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
           SELECT nxt.{id}, r.depth + 1 FROM reach r JOIN {tbl} cur ON cur.{id} = r.id{link_join} WHERE {rec_where}\
         ) \
         SELECT DISTINCT {cols} FROM {tbl} p WHERE {proj_where} {}",
        dialect.limit_clause(limit)
    );
    Ok((sql, params))
}
```

- [ ] **Step 4: Run to verify pass**

Run: `buck2 test //src/services/query-api:compile-graph-reach > /tmp/t.log 2>&1; grep -nE "Tests finished|Pass [0-9]|FAIL|panicked" /tmp/t.log`
Expected: PASS (both cases).

- [ ] **Step 5: Clippy + commit**

Run: `tools/clippy-all.sh > /tmp/c.log 2>&1; grep -nE "warning|error" /tmp/c.log || echo CLEAN`
Expected: CLEAN.

```bash
git add src/services/query-api/ docs/superpowers/plans/2026-06-18-graph-reachability.md
git commit -m "feat(query): compile_graph_reach — recursive self-link reachability SQL

A depth-bounded WITH RECURSIVE CTE over a self-link (FK + join-table backings),
governing the type's row-filters at the seed, every recursive expansion, and the
final projection; deduped, reachable in >= 1 hop. Bound params; depth inlined.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Handler — `read_graph_reach`

**Files:**
- Modify: `src/services/query-api/src/handler.rs` (`GraphQuery`, `read_graph_reach`, `QueryError::NotSelfLink`)
- Create: `src/services/query-api/tests/graph_reach.rs` (handler governance test on a stub serving engine) + BUCK target

**Interfaces:**
- Consumes: `compile_graph_reach` (Task 1), `identity_in_predicate`, `project_allowed`, `load_policy`, `coerce_predicate`, `QueryDeps`, `Subject`, `ObjectRows`.
- Produces: `pub struct GraphQuery { pub type_name: String, pub link: String, pub depth: u32, pub filters: Vec<(String, String)>, pub ids: Vec<String> }`, `pub async fn read_graph_reach(q: &GraphQuery, subject: &Subject, deps: &QueryDeps<'_>) -> Result<ObjectRows, QueryError>`, `QueryError::NotSelfLink(String)`.

- [ ] **Step 1: Write the failing handler test**

Create `src/services/query-api/tests/graph_reach.rs`. Mirror the `associations.rs` in-memory stub-serving harness (read it first: it defines a `ServingEngine` stub returning canned rows, seeds `MemoryControlPlane` with types/links/ACL grants, and calls the handler directly). The governance paths (`NotSelfLink`, `NoIdentity`) short-circuit before the serving call, so they need no real SQL; the happy path returns the stub's rows.

```rust
// (mirror associations.rs imports + a PairServing-style stub returning canned ObjectRows
//  columns; define a self-link `knows` (from=to=Person, FK) and a non-self link `employer`
//  (Person -> Company). Grant the analyst Read on Person.)

#[tokio::test(flavor = "multi_thread")]
async fn rejects_a_non_self_link() {
    // GraphQuery over `employer` (Person -> Company) -> QueryError::NotSelfLink("employer")
}

#[tokio::test(flavor = "multi_thread")]
async fn rejects_a_type_without_identity() {
    // Person defined with identity: None, self-link knows -> QueryError::NoIdentity("Person")
}

#[tokio::test(flavor = "multi_thread")]
async fn returns_reachable_objects_for_a_self_link() {
    // Person (identity "id") with a `knows` FK self-link, stub serving returns 2 canned rows;
    // read_graph_reach(depth 3) -> Ok(ObjectRows) with those rows + the projected columns.
}
```

(Use concrete assertions: `matches!(err, QueryError::NotSelfLink(l) if l == "employer")`, `matches!(err, QueryError::NoIdentity(t) if t == "Person")`, and for the happy path assert `rows.columns` and `rows.rows` equal the stub's canned output. Adapt the stub `ServingEngine` and seed helpers from `associations.rs`.)

Add the BUCK target (mirror `associations`'s `rust_test` target, copying its deps — `:query-api`, `//src/control-plane/core:core`, `//src/control-plane/memory:memory`, `//third-party:async-trait`, `//third-party:tokio`, and whatever else `associations` lists).

- [ ] **Step 2: Run to verify failure**

Run: `buck2 test //src/services/query-api:graph-reach > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: FAIL (compile error — `GraphQuery`/`read_graph_reach`/`NotSelfLink` undefined).

- [ ] **Step 3: Add `NotSelfLink` + `GraphQuery`**

In `src/services/query-api/src/handler.rs`, add the error variant to `QueryError`:

```rust
    /// `/graph` was asked to recurse a link that is not a self-link (`from != to`). Only a
    /// link whose endpoints are the same type can be followed repeatedly.
    #[error("not a self-link: {0}")]
    NotSelfLink(String),
```

Add the query type (near `ChainQuery`):

```rust
/// A bounded recursive reachability read over a self-link. `filters`/`ids` scope the SEED
/// set (the starting objects); the recursion follows `link` up to `depth` hops.
pub struct GraphQuery {
    pub type_name: String,
    pub link: String,
    pub depth: u32,
    pub filters: Vec<(String, String)>,
    pub ids: Vec<String>,
}
```

- [ ] **Step 4: Add `read_graph_reach`**

In `src/services/query-api/src/handler.rs`, add (place after `read_associations`):

```rust
/// Serve a bounded recursive reachability read over a self-link: from the seed set, follow
/// `link` up to `depth` hops, return the deduped reachable objects. Governed: Read on the
/// type, row-filters at the seed/every expansion/projection, declared identity (dedup key;
/// visibility not required since it is never projected unless it is itself a visible column).
pub async fn read_graph_reach(
    q: &GraphQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<ObjectRows, QueryError> {
    let type_name = TypeName(q.type_name.clone());
    let target = PolicyTarget::Type(type_name.clone());

    // Read gate (deny-by-default, before existence is revealed).
    if deps.acl.check(&subject.0, Action::Read, &target).await? == Decision::Deny {
        return Err(QueryError::Forbidden);
    }
    let object_type = deps.ontology.get_type(&type_name).await.map_err(|e| match e {
        ControlPlaneError::NotFound(_) => QueryError::UnknownType(q.type_name.clone()),
        other => QueryError::ControlPlane(other),
    })?;
    let (row_filters, denied, masked) = load_policy(deps.acl, &subject.0, &target).await?;

    // Declared identity is the recursion's dedup key.
    let identity = object_type
        .identity
        .clone()
        .ok_or_else(|| QueryError::NoIdentity(q.type_name.clone()))?;

    // Resolve the link from the type's outbound links; it MUST be a self-link.
    let links = deps.ontology.links(&type_name, PageReq::unbounded()).await?;
    let link = links
        .items
        .into_iter()
        .find(|l| l.name == q.link)
        .ok_or_else(|| QueryError::UnknownLink(q.link.clone()))?;
    if link.from != type_name || link.to != type_name {
        return Err(QueryError::NotSelfLink(q.link.clone()));
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
            crate::filter::coerce_predicate(col, ty, raw).map_err(|_| QueryError::BadFilter(col.clone()))?,
        );
    }
    if let Some(p) = identity_in_predicate(&object_type, &denied, &masked, &q.ids)? {
        seed_predicates.push(p);
    }

    let (sql, params) = crate::sql::compile_graph_reach(
        deps.serving.dialect(),
        &object_type.table,
        &identity,
        &link.backing,
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

Export it: in `src/services/query-api/src/lib.rs`, if the handler items are re-exported there, add `GraphQuery`/`read_graph_reach`; otherwise tests reach them via `query_api::handler::{…}` (confirm by how `associations.rs` imports `read_associations`).

- [ ] **Step 5: Run the handler test to verify pass**

Run: `buck2 test //src/services/query-api:graph-reach > /tmp/t.log 2>&1; grep -nE "Tests finished|Pass [0-9]|FAIL|panicked" /tmp/t.log`
Expected: PASS (the two rejection paths + the happy path).

- [ ] **Step 6: Full query-api suite + clippy + commit**

Run: `buck2 test //src/services/query-api/... > /tmp/t.log 2>&1; grep -nE "Tests finished|Pass [0-9]|FAIL|panicked" /tmp/t.log`
Expected: all pass.
Run: `tools/clippy-all.sh > /tmp/c.log 2>&1; grep -nE "warning|error" /tmp/c.log || echo CLEAN`
Expected: CLEAN.

```bash
git add src/services/query-api/
git commit -m "feat(query): read_graph_reach — governed self-link reachability handler

Resolves the self-link (from==to==type, else NotSelfLink) and the declared
identity (else NoIdentity), builds seed predicates from source filters + ?_ids=,
and compiles the governed recursive reachability query. Read + row-filters at the
seed/expansion/projection; reachable objects rendered like read_object.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: HTTP route + e2e

**Files:**
- Modify: `src/services/query-api/src/http.rs` (route + `get_graph` handler)
- Create: `src/services/query-api/tests/graph_reach_e2e.rs` (DuckDB e2e) + BUCK target

**Interfaces:**
- Consumes: `GraphQuery`, `read_graph_reach`, `QueryError::NotSelfLink` (Task 2); `objects_to_json`.
- Produces: route `GET /objects/:type_name/graph/:link_name`.

- [ ] **Step 1: Add the route + handler**

In `src/services/query-api/src/http.rs`, register the route in `router`:

```rust
        .route("/objects/:type_name/graph/:link_name", get(get_graph))
```

Add `read_graph_reach`, `GraphQuery` to the `use crate::handler::{…}` import, and add the handler. Constants at the top of the file (or near the handler):

```rust
const MAX_GRAPH_DEPTH: u32 = 10;
const DEFAULT_GRAPH_DEPTH: u32 = 5;

async fn get_graph(
    State(st): State<AppState>,
    Path((type_name, link_name)): Path<(String, String)>,
    Query(params): Query<Vec<(String, String)>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let subject = headers
        .get("X-Loom-Subject")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("anonymous")
        .to_string();
    // Pull `depth` and `_ids` out; the rest are seed filters.
    let mut depth = DEFAULT_GRAPH_DEPTH;
    let mut ids: Vec<String> = Vec::new();
    let mut ids_present = false;
    let mut filters: Vec<(String, String)> = Vec::with_capacity(params.len());
    for (k, v) in params {
        match k.as_str() {
            "depth" => match v.parse::<u32>() {
                Ok(d) => depth = d,
                Err(_) => return (StatusCode::BAD_REQUEST, "depth must be a positive integer").into_response(),
            },
            "_ids" => {
                ids_present = true;
                ids = v.split(',').filter(|s| !s.is_empty()).map(String::from).collect();
            }
            _ => filters.push((k, v)),
        }
    }
    if ids_present && ids.is_empty() {
        return (StatusCode::BAD_REQUEST, "_ids requires at least one value").into_response();
    }
    if depth < 1 || depth > MAX_GRAPH_DEPTH {
        return (StatusCode::BAD_REQUEST, format!("depth must be 1..={MAX_GRAPH_DEPTH}")).into_response();
    }
    let deps = QueryDeps {
        ontology: st.cp.ontology(),
        acl: st.cp.acl(),
        serving: st.serving.as_ref(),
    };
    match read_graph_reach(
        &GraphQuery { type_name, link: link_name, depth, filters, ids },
        &Subject(SubjectId(subject)),
        &deps,
    )
    .await
    {
        Ok(rows) => Json(crate::render::objects_to_json(&rows)).into_response(),
        Err(QueryError::UnknownType(t)) => (StatusCode::NOT_FOUND, t).into_response(),
        Err(QueryError::UnknownLink(l)) => (StatusCode::NOT_FOUND, l).into_response(),
        Err(QueryError::NotSelfLink(l)) => (StatusCode::BAD_REQUEST, l).into_response(),
        Err(QueryError::NoIdentity(t)) => (StatusCode::BAD_REQUEST, t).into_response(),
        Err(QueryError::BadFilter(c)) => (StatusCode::BAD_REQUEST, c).into_response(),
        Err(QueryError::Forbidden) => StatusCode::FORBIDDEN.into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    }
}
```

(Confirm `get`, `Path`, `Query`, `State`, `HeaderMap`, `StatusCode`, `Json`, `IntoResponse`, `SubjectId`, `Subject` are already imported in `http.rs` — they are, used by the existing handlers.)

- [ ] **Step 2: Build the crate**

Run: `buck2 build //src/services/query-api:query-api > /tmp/b.log 2>&1; grep -nE "BUILD SUCCEEDED|error\[" /tmp/b.log`
Expected: BUILD SUCCEEDED.

- [ ] **Step 3: Write the e2e (failing)**

Create `src/services/query-api/tests/graph_reach_e2e.rs`. Mirror the `multi_hop_traversal_e2e.rs` / `association_e2e.rs` DuckDB-router harness (PgFixture, DuckLakeWriter, AppState/router via tower `oneshot`, X-Loom-Subject, ACL grants). Seed a single `Person` table with a self-link and rows forming a small graph, e.g. a `knows` join-table edge set `1->2, 2->3, 3->1 (cycle), 2->4`, with a boolean `active` column. Define `Person` with `identity: "id"` and a `knows` self-link (`from=to=Person`). Grant Read on `Person`.

Assert (drive `GET /objects/Person/graph/knows…` via the router, parse `{"objects":[…]}`, collect the `id`s):

- `?depth=1&_ids=1` → reachable = {2} (one hop from 1).
- `?depth=2&_ids=1` → reachable = {2, 3} (and not 4 unless 1 reaches it).
- `?depth=3&_ids=1` over the cycle `1->2->3->1` → terminates, node set deduped (no infinite loop), reachable = {1, 2, 3} (1 reappears via the cycle at depth 3).
- a Read row-filter policy `active = true` on `Person` with node 2 inactive → reachability through 2 is cut (e.g. `?depth=3&_ids=1` no longer reaches 3/4 routed via 2).
- `GET /objects/Person/graph/<non-self-link>` → 400.
- `?depth=0` → 400; `?depth=99` → 400.

(Reuse the established fixture/seed/grant helpers; the key new shape is the self-link definition and the recursive-result `id` assertions. Copy the BUCK target deps verbatim from `association-e2e`.)

Add the BUCK target (mirror `association-e2e`'s `loom_fixture_test` with `duckdb = True`).

- [ ] **Step 4: Run the e2e + full query-api suite + clippy**

Run: `buck2 test //src/services/query-api:graph-reach-e2e > /tmp/t.log 2>&1; grep -nE "Tests finished|Pass [0-9]|FAIL|panicked|error\[" /tmp/t.log`
Expected: PASS.
Run: `buck2 test //src/services/query-api/... > /tmp/t2.log 2>&1; grep -nE "Tests finished|Pass [0-9]|FAIL|panicked" /tmp/t2.log`
Expected: all pass.
Run: `tools/clippy-all.sh > /tmp/c.log 2>&1; grep -nE "warning|error" /tmp/c.log || echo CLEAN`
Expected: CLEAN.

- [ ] **Step 5: Commit**

```bash
git add src/services/query-api/
git commit -m "feat(query): GET /objects/:type/graph/:link recursive reachability route

Wires read_graph_reach behind the /graph route with depth (1..=10, default 5) +
_ids seed parsing and error mapping. Proven by a DuckDB e2e: depth bounds, cycle
termination + dedup, a row-filter pruning reachable-through nodes, non-self link
-> 400, out-of-range depth -> 400.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Docs

**Files:**
- Modify: `docs/superpowers/specs/2026-06-06-loom-roadmap.md`
- Modify: `docs/FUTURE.md`

- [ ] **Step 1: Roadmap**

In `docs/superpowers/specs/2026-06-06-loom-roadmap.md`, add a "Where we are" paragraph: the first `/graph` surface is delivered — `GET /objects/:type/graph/:link?depth=N` serves bounded recursive reachability over a self-link (`WITH RECURSIVE`, governed per expansion, deduped by identity), the graph counterpart to the relational `/links` arc.

- [ ] **Step 2: FUTURE.md**

In `docs/FUTURE.md`, read it first, then record (editing the graph/reads area in place): `/graph` part-1 (single self-link bounded reachability, reachable object set) delivered; the remaining `/graph` parts — multi-link / heterogeneous paths, graph-aware filter addressing, min-depth annotation, shortest-path / `/tree`, weighted edges.

- [ ] **Step 3: Lint + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p.log 2>&1; grep -nE "Failed|Passed|error" /tmp/p.log | tail -20`
Expected: all hooks pass (fix any markdown whitespace/EOF the hooks flag, then re-run).

```bash
git add docs/
git commit -m "docs(query): mark /graph part-1 (recursive reachability) delivered

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Final verification

- [ ] **Whole first-party suite**

Run: `buck2 test //src/... > /tmp/all.log 2>&1; grep -nE "Tests finished|Pass [0-9]|FAIL|error\[" /tmp/all.log`
Expected: all pass, zero failures.

- [ ] **Clippy across all first-party Rust**

Run: `tools/clippy-all.sh > /tmp/c.log 2>&1; grep -nE "warning|error" /tmp/c.log || echo CLEAN`
Expected: CLEAN.
