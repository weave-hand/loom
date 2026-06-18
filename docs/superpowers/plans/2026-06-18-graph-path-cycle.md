# `/graph` Part-2: Repeated Path-Cycle — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Generalize `/graph` recursive reachability from a single self-link to a path that forms a cycle: `GET /objects/:type/graph?path=l1,…,lK&depth=N` follows the cyclic pattern up to N times, governed at every intermediate type.

**Architecture:** `compile_graph_reach`'s single `backing` parameter becomes a `&[GraphStep]` path; the recursive CTE step joins `cur` through the whole path to `nxt` (both the start type). `read_graph_reach` resolves the path forward, validates it returns to the start type (cycle), and governs each intermediate type (Read + row-filters). Part-1's single self-link becomes the degenerate 1-element path; `NotSelfLink` unifies into `NotCyclicPath`.

**Tech Stack:** Rust, buck2, axum HTTP, DuckDB serving engine (`WITH RECURSIVE`), hermetic Postgres/DuckDB fixture tests.

**Design:** `docs/superpowers/specs/2026-06-18-graph-path-cycle-design.md`

## Global Constraints

- Never run two `buck2` commands concurrently. One at a time.
- Never pipe `buck2 test` through `tail`/`head` — redirect and grep:
  `buck2 test //target > /tmp/t.log 2>&1; grep -nE "Tests finished|Pass [0-9]|FAIL|error\[|panicked" /tmp/t.log`. Fixture tests take minutes — allow up to 600000ms.
- Tests are integration `rust_test`/`loom_fixture_test` targets only — never inline `#[test]` in `src/**`.
- If the rustfmt pre-commit hook fails, run `buck2 run //tools:rustfmt -- <files>`, re-stage, re-commit.
- Depth bounds: `MAX_GRAPH_DEPTH = 10`, default `5` (unchanged from part-1; validated at the HTTP edge).
- Every caller VALUE is a bound `?` param; identifiers come only from trusted ontology/ACL metadata and are double-quoted; `depth`/`limit` are inlined.
- A 1-element path MUST emit byte-identical SQL to part-1's single-link form (regression).
- Commit trailer: `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`

---

## Task 1: Generalize the compiler to a path of steps

**Files:**
- Modify: `src/services/query-api/src/sql.rs` (`GraphStep` + generalize `compile_graph_reach`)
- Modify: `src/services/query-api/src/handler.rs` (adapt the single call site to a 1-element path)
- Modify: `src/services/query-api/tests/compile_graph_reach.rs` (port the 2 existing cases to the path param; add a multi-step case)

**Interfaces:**
- Produces: `pub struct GraphStep { pub backing: LinkBacking, pub next_table: TableRef, pub next_filters: Vec<RowFilter> }`; `compile_graph_reach(dialect, table, identity, path: &[GraphStep], seed_predicates, row_filters, allowed_cols, mask_cols, depth, limit) -> Result<(String, Vec<SqlValue>), CompileError>`.

- [ ] **Step 1: Update the compiler unit tests for the path param + add a multi-step case**

In `src/services/query-api/tests/compile_graph_reach.rs`, the two existing tests pass a single `backing`; change them to pass a 1-element `path` and add a 2-step case. Replace the file's two test bodies' `compile_graph_reach(... &backing, ...)` calls so the 4th argument is `&[GraphStep { backing, next_table: person(), next_filters: vec![] }]` (import `GraphStep` from `query_api::sql`). The 1-step assertions are unchanged (a 1-element path emits the same SQL). Then append a multi-step test:

```rust
#[test]
fn two_step_path_cycle_with_intermediate_filter() {
    // Person --memberOf(FK Person.team_id -> Team.id)--> Team
    //        --hasMember(FK Team.id -> Person.team_id)--> Person   (a Person->Team->Person cycle)
    // Team has an ACL row-filter `active = true` (intermediate governance); Person (start) has
    // `region = 'US'` (seed + nxt + projection).
    let team = TableRef { schema: "main".into(), name: "team".into() };
    let path = vec![
        GraphStep {
            backing: LinkBacking::ForeignKey { from_column: "team_id".into(), to_column: "id".into() },
            next_table: team.clone(),
            next_filters: vec![RowFilter::Compare {
                property: "active".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Bool(true),
            }],
        },
        GraphStep {
            backing: LinkBacking::ForeignKey { from_column: "id".into(), to_column: "team_id".into() },
            next_table: person(),
            next_filters: vec![], // final step: nxt is the start type, governed by row_filters
        },
    ];
    let start_filters = vec![RowFilter::Compare {
        property: "region".into(),
        op: CompareOp::Eq,
        value: ScalarValue::Text("US".into()),
    }];
    let (sql, params) = compile_graph_reach(
        &DuckDbDialect,
        &person(),
        "id",
        &path,
        &[],
        &start_filters,
        &["id".to_string()],
        &[],
        2,
        1000,
    )
    .unwrap();
    // Chain join cur -> g1(Team) -> nxt(Person).
    assert!(sql.contains(r#"cur."team_id" = g1."id""#), "step1 join: {sql}");
    assert!(sql.contains(r#"g1."id" = nxt."team_id""#), "step2 join: {sql}");
    // Intermediate Team filter at g1; start filter at s/nxt/p.
    assert!(sql.contains(r#"g1."active""#), "intermediate filter at g1: {sql}");
    assert!(sql.contains(r#"s."region""#) && sql.contains(r#"nxt."region""#) && sql.contains(r#"p."region""#), "start filter at s/nxt/p: {sql}");
    // Param order: seed start-filter (s) , g1 active, nxt region, p region = 4.
    assert_eq!(params.len(), 4, "got {params:?}");
    assert_eq!(params[0], SqlValue::Text("US".into())); // s.region
    assert_eq!(params[1], SqlValue::Bool(true));        // g1.active
}
```

(The existing imports already cover `LinkBacking`, `RowFilter`, `ScalarValue`, `CompareOp`, `TableRef`, `SqlValue`, `DuckDbDialect`; add `GraphStep` to the `query_api::sql` import.)

- [ ] **Step 2: Run to verify failure**

Run: `buck2 test //src/services/query-api:compile-graph-reach > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: FAIL (compile error — `GraphStep` undefined / arg type mismatch).

- [ ] **Step 3: Add `GraphStep` and generalize `compile_graph_reach`**

In `src/services/query-api/src/sql.rs`, replace the entire `compile_graph_reach` function (the `#[allow(clippy::too_many_arguments)] pub fn compile_graph_reach(...) { ... }` block) with:

```rust
/// One step of a graph path-cycle: a link's backing, the table of the type it lands on, and
/// that landed type's ACL row-filters (intermediate governance). For the FINAL step the
/// landed type is the start type; the handler passes its `next_filters` empty, since the
/// start type's `row_filters` govern the final node `nxt`.
pub struct GraphStep {
    pub backing: LinkBacking,
    pub next_table: TableRef,
    pub next_filters: Vec<RowFilter>,
}

/// Compile a depth-bounded recursive reachability query over a PATH-CYCLE: the deduped set of
/// `table` rows reachable from the seed set by repeating `path` (which starts and ends at
/// `table`) up to `depth` times. Each recursive step joins `cur` through the whole path to
/// `nxt` (both `table`), governing each intermediate landing with its `next_filters` and the
/// final node `nxt` with the start `row_filters`. A 1-step path is the single-self-link case
/// (byte-identical SQL). Termination by the inlined `depth` bound; `DISTINCT` dedups. Every
/// caller value is a bound param.
#[allow(clippy::too_many_arguments)]
pub fn compile_graph_reach(
    dialect: &dyn SqlDialect,
    table: &TableRef,
    identity: &str,
    path: &[GraphStep],
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
    for step in path {
        for f in &step.next_filters {
            validate_row_filter(f, None).map_err(CompileError::MalformedFilter)?;
        }
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

    // Recursive step: join `cur` through every path link to `nxt`. Intermediate landings are
    // aliased g1..g{K-1}; the final landing is `nxt`. A join-table step adds a per-step `j{n}`.
    let k = path.len();
    let from_alias = |i: usize| if i == 0 { "cur".to_string() } else { format!("g{i}") };
    let to_alias = |i: usize| {
        if i + 1 == k {
            "nxt".to_string()
        } else {
            format!("g{}", i + 1)
        }
    };
    let mut joins = String::new();
    for (i, step) in path.iter().enumerate() {
        let fa = from_alias(i);
        let ta = to_alias(i);
        let to_tbl = format!("{}.{}", q(&step.next_table.schema), q(&step.next_table.name));
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
                let j = format!("j{}", i + 1);
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

    // Recursive WHERE: depth bound, each intermediate's filters at its alias (path order),
    // then the start row_filters at the final node `nxt`.
    let mut rec_conj: Vec<String> = vec![format!("r.depth < {depth}")];
    for (i, step) in path.iter().enumerate() {
        let ta = to_alias(i);
        for f in &step.next_filters {
            rec_conj.push(filter_sql(dialect, f, &ta, &mut params));
        }
    }
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
           SELECT nxt.{id}, r.depth + 1 FROM reach r JOIN {tbl} cur ON cur.{id} = r.id{joins} WHERE {rec_where}\
         ) \
         SELECT DISTINCT {cols} FROM {tbl} p WHERE {proj_where} {}",
        dialect.limit_clause(limit)
    );
    Ok((sql, params))
}
```

- [ ] **Step 4: Adapt the single call site in `read_graph_reach`**

In `src/services/query-api/src/handler.rs`, the `compile_graph_reach(...)` call currently passes `&link.backing`. Change that one argument to a 1-element path (Task 2 replaces this with the full path resolution; this keeps part-1 compiling and behaviorally identical now):

```rust
        &[crate::sql::GraphStep {
            backing: link.backing.clone(),
            next_table: object_type.table.clone(),
            next_filters: Vec::new(),
        }],
```

(Replace exactly the `&link.backing,` argument line with the block above.)

- [ ] **Step 5: Run the compiler tests + the part-1 graph tests (regression)**

Run: `buck2 test //src/services/query-api:compile-graph-reach > /tmp/t.log 2>&1; grep -nE "Tests finished|Pass [0-9]|FAIL|panicked" /tmp/t.log`
Expected: PASS (the two 1-step cases + the new 2-step case).
Run: `buck2 test //src/services/query-api:graph-reach //src/services/query-api:graph-reach-e2e > /tmp/t2.log 2>&1; grep -nE "Tests finished|Pass [0-9]|FAIL|panicked" /tmp/t2.log`
Expected: PASS (part-1 handler test + e2e unchanged — the 1-element path is byte-identical SQL).

- [ ] **Step 6: Clippy + commit**

Run: `tools/clippy-all.sh > /tmp/c.log 2>&1; grep -nE "warning|error" /tmp/c.log || echo CLEAN`
Expected: CLEAN.

```bash
git add src/services/query-api/ docs/superpowers/plans/2026-06-18-graph-path-cycle.md
git commit -m "feat(query): compile_graph_reach over a path of steps (GraphStep)

Generalize the recursive reachability compiler from a single self-link backing to
an ordered path of steps: the recursive CTE joins cur through the whole path to
nxt, governing each intermediate landing with its row-filters. A 1-step path emits
byte-identical SQL to the prior single-link form.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Handler — path-cycle resolution + `NotCyclicPath`

**Files:**
- Modify: `src/services/query-api/src/handler.rs` (`GraphQuery.path`, path resolution, `NotCyclicPath` replacing `NotSelfLink`)
- Modify: `src/services/query-api/src/http.rs` (single-link route builds `path: vec![link]`; error mapping `NotSelfLink`→`NotCyclicPath`)
- Modify: `src/services/query-api/tests/graph_reach.rs` (the `NotSelfLink` assertion → `NotCyclicPath`; add a path test)

**Interfaces:**
- Consumes: `GraphStep`, `compile_graph_reach` (Task 1).
- Produces: `GraphQuery { type_name: String, path: Vec<String>, depth: u32, filters: Vec<(String, String)>, ids: Vec<String> }`; `QueryError::NotCyclicPath(String)` (replaces `NotSelfLink`); `read_graph_reach` resolves a multi-link cyclic path.

- [ ] **Step 1: Update the handler test (failing)**

In `src/services/query-api/tests/graph_reach.rs`: (a) change `GraphQuery { link: "...".into(), ... }` literals to `path: vec!["...".into()]`; (b) rename/retarget the non-self-link case assertion from `QueryError::NotSelfLink` to `QueryError::NotCyclicPath`; (c) add a multi-link case — a 2-link cyclic path (`memberOf,hasMember` over a seeded `Person->Team->Person`) resolves and returns the stub rows, and a 2-link **non-cyclic** path (`memberOf,worksAt` ending at Company) → `NotCyclicPath`. Mirror the file's existing stub-serving harness; assert `matches!(err, QueryError::NotCyclicPath(_))`.

(If the existing happy-path case asserts the stub rows, keep it but switch its `link` to `path: vec![...]`.)

- [ ] **Step 2: Run to verify failure**

Run: `buck2 test //src/services/query-api:graph-reach > /tmp/t.log 2>&1; grep -nE "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: FAIL (compile error — `GraphQuery.link`/`NotSelfLink` gone / `path` not yet a field).

- [ ] **Step 3: Change `GraphQuery` + the error variant**

In `src/services/query-api/src/handler.rs`, change the `GraphQuery` struct's `pub link: String,` field to:

```rust
    pub path: Vec<String>,
```

In `QueryError`, replace the `NotSelfLink` variant with:

```rust
    /// `/graph` was given a path that does not form a cycle (following it does not return to
    /// the queried type), so it cannot be repeated. Use relational `/links` for fixed paths.
    #[error("path is not a cycle on the queried type: {0}")]
    NotCyclicPath(String),
```

- [ ] **Step 4: Replace the single-link resolution with path-cycle resolution**

In `read_graph_reach`, replace the block from `// Resolve the link from the type's outbound links; it MUST be a self-link.` through the `if link.from != type_name ... return Err(QueryError::NotSelfLink(...)); }` (the single-link resolution) with the path walk:

```rust
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
        if deps.acl.check(&subject.0, Action::Read, &landed_target).await? == Decision::Deny {
            return Err(QueryError::Forbidden);
        }
        let landed_type = deps.ontology.get_type(&landed).await?;
        let (landed_filters, _ld, _lm) = load_policy(deps.acl, &subject.0, &landed_target).await?;
        // Intermediates carry their own row-filters; the FINAL landing is the start type, whose
        // filters are rendered at `nxt` by the compiler -> pass empty here (no double-render).
        let next_filters = if i == last { Vec::new() } else { landed_filters };
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
```

Then change the `compile_graph_reach(...)` call's path argument (the `&[crate::sql::GraphStep { ... }]` block Task 1 added) to pass the resolved `&steps`:

```rust
        &steps,
```

- [ ] **Step 5: Fix the single-link HTTP route + error mapping**

In `src/services/query-api/src/http.rs`, the `get_graph` handler (the `/graph/:link_name` route) constructs `GraphQuery { ..., link: link_name, ... }`. Change it to:

```rust
            path: vec![link_name],
```

In `get_graph`'s error match, replace the `Err(QueryError::NotSelfLink(l)) => ...` arm with:

```rust
        Err(QueryError::NotCyclicPath(p)) => (StatusCode::BAD_REQUEST, p).into_response(),
```

- [ ] **Step 6: Run the handler test + part-1 graph tests**

Run: `buck2 test //src/services/query-api:graph-reach > /tmp/t.log 2>&1; grep -nE "Tests finished|Pass [0-9]|FAIL|panicked" /tmp/t.log`
Expected: PASS.
Run: `buck2 test //src/services/query-api:graph-reach-e2e > /tmp/t2.log 2>&1; grep -nE "Tests finished|Pass [0-9]|FAIL|panicked" /tmp/t2.log`
Expected: PASS (part-1 e2e drives HTTP + asserts status 400 for a non-self link, which `NotCyclicPath` still maps to — no e2e edit needed).

- [ ] **Step 7: Full query-api suite + clippy + commit**

Run: `buck2 test //src/services/query-api/... > /tmp/t.log 2>&1; grep -nE "Tests finished|Pass [0-9]|FAIL|panicked" /tmp/t.log`
Expected: all pass.
Run: `tools/clippy-all.sh > /tmp/c.log 2>&1; grep -nE "warning|error" /tmp/c.log || echo CLEAN`
Expected: CLEAN.

```bash
git add src/services/query-api/
git commit -m "feat(query): read_graph_reach resolves a multi-link path-cycle

GraphQuery carries a path; read_graph_reach walks it forward, Read-gates and loads
row-filters for every intermediate type, and requires the path to return to the
queried type (NotCyclicPath, unifying the prior NotSelfLink). The single-link route
forwards a 1-element path.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: HTTP `?path=` route + e2e

**Files:**
- Modify: `src/services/query-api/src/http.rs` (new `/objects/:type/graph` route + `get_graph_path`; share parsing)
- Create: `src/services/query-api/tests/graph_path_e2e.rs` (DuckDB e2e) + BUCK target

**Interfaces:**
- Consumes: `GraphQuery` (with `path`), `read_graph_reach`, `QueryError::NotCyclicPath`.
- Produces: route `GET /objects/:type_name/graph` (multi-link via `?path=`).

- [ ] **Step 1: Add the `?path=` route + handler**

In `src/services/query-api/src/http.rs`, register the route in `router` (alongside the existing `/graph/:link_name`):

```rust
        .route("/objects/:type_name/graph", get(get_graph_path))
```

Add `get_graph_path`. It parses `?path=` (comma-split, empty/absent → 400) plus the same `depth`/`_ids`/filter handling as `get_graph`, then calls `read_graph_reach`. To avoid duplicating the depth/`_ids`/filter parsing + error mapping, factor the shared tail of `get_graph` into a helper and call it from both:

```rust
async fn get_graph_path(
    State(st): State<AppState>,
    Path(type_name): Path<String>,
    Query(params): Query<Vec<(String, String)>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let subject = headers
        .get("X-Loom-Subject")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("anonymous")
        .to_string();
    let mut depth = DEFAULT_GRAPH_DEPTH;
    let mut ids: Vec<String> = Vec::new();
    let mut ids_present = false;
    let mut path: Vec<String> = Vec::new();
    let mut filters: Vec<(String, String)> = Vec::with_capacity(params.len());
    for (k, v) in params {
        match k.as_str() {
            "path" => path = v.split(',').filter(|s| !s.is_empty()).map(String::from).collect(),
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
    if path.is_empty() {
        return (StatusCode::BAD_REQUEST, "path requires at least one link").into_response();
    }
    if ids_present && ids.is_empty() {
        return (StatusCode::BAD_REQUEST, "_ids requires at least one value").into_response();
    }
    if !(1..=MAX_GRAPH_DEPTH).contains(&depth) {
        return (StatusCode::BAD_REQUEST, format!("depth must be 1..={MAX_GRAPH_DEPTH}")).into_response();
    }
    graph_respond(&st, type_name, path, depth, filters, ids, subject).await
}
```

Refactor `get_graph` (the `/graph/:link` handler) to build `path: vec![link_name]` and call the same `graph_respond` helper, and extract the helper (the `QueryDeps` build + `read_graph_reach` call + the error match). Add `graph_respond`:

```rust
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
        &GraphQuery { type_name, path, depth, filters, ids },
        &Subject(SubjectId(subject)),
        &deps,
    )
    .await
    {
        Ok(rows) => Json(crate::render::objects_to_json(&rows)).into_response(),
        Err(QueryError::UnknownType(t)) => (StatusCode::NOT_FOUND, t).into_response(),
        Err(QueryError::UnknownLink(l)) => (StatusCode::NOT_FOUND, l).into_response(),
        Err(QueryError::NotCyclicPath(p)) => (StatusCode::BAD_REQUEST, p).into_response(),
        Err(QueryError::NoIdentity(t)) => (StatusCode::BAD_REQUEST, t).into_response(),
        Err(QueryError::BadFilter(c)) => (StatusCode::BAD_REQUEST, c).into_response(),
        Err(QueryError::Forbidden) => StatusCode::FORBIDDEN.into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    }
}
```

(Make `get_graph` build `let path = vec![link_name];` from its `Path((type_name, link_name))` and, after parsing depth/`_ids`/filters exactly as it does today, call `graph_respond(&st, type_name, path, depth, filters, ids, subject).await` — removing its now-duplicated inline error match.)

- [ ] **Step 2: Build the crate**

Run: `buck2 build //src/services/query-api:query-api > /tmp/b.log 2>&1; grep -nE "BUILD SUCCEEDED|error\[" /tmp/b.log`
Expected: BUILD SUCCEEDED.

- [ ] **Step 3: Write the e2e (failing)**

Create `src/services/query-api/tests/graph_path_e2e.rs`. Mirror the `graph_reach_e2e.rs` harness (PgFixture, DuckLakeWriter, AppState/router via tower `oneshot`, X-Loom-Subject, ACL grants). Seed a `Person` table and a `Team` table with a `memberOf` link (Person→Team) and a `hasMember` link (Team→Person), forming a `Person->Team->Person` cycle. Membership edges, e.g. (person→team): 1→T1, 2→T1, 3→T2, 4→T2, 5→T2 — so via shared-team membership person 1 reaches {1,2} (T1) and person 3 reaches {3,4,5} (T2). Give `Team` an `active` boolean.

Assert (drive `GET /objects/Person/graph?path=memberOf,hasMember…` via the router, collect `id`s from `{"objects":[…]}`):
- `?path=memberOf,hasMember&depth=2&_ids=1` → the transitively shared-membership Persons for person 1's team(s).
- distinct depths differ where the graph allows (use a 2-team chain so depth matters, e.g. a person on two teams bridging clusters).
- a cycle terminates + dedups (the pattern inherently revisits; assert no hang and a deduped set).
- a Read row-filter on **Team** (`active=true`, with one team inactive) prunes the Persons reachable only through that team → they disappear (intermediate governance inside the recursion).
- a non-cyclic `?path=memberOf,worksAt` (worksAt: Person→Company) → 400.
- absent/empty `?path=` → 400.

(Reuse the established fixture/seed/grant helpers; the new shape is the two-link cycle + the Team intermediate filter. Copy the BUCK target deps verbatim from `graph-reach-e2e`.)

Add the BUCK target (mirror `graph-reach-e2e`'s `loom_fixture_test` with `duckdb = True`).

- [ ] **Step 4: Run the e2e + full query-api suite + clippy**

Run: `buck2 test //src/services/query-api:graph-path-e2e > /tmp/t.log 2>&1; grep -nE "Tests finished|Pass [0-9]|FAIL|panicked|error\[" /tmp/t.log`
Expected: PASS.
Run: `buck2 test //src/services/query-api/... > /tmp/t2.log 2>&1; grep -nE "Tests finished|Pass [0-9]|FAIL|panicked" /tmp/t2.log`
Expected: all pass.
Run: `tools/clippy-all.sh > /tmp/c.log 2>&1; grep -nE "warning|error" /tmp/c.log || echo CLEAN`
Expected: CLEAN.

- [ ] **Step 5: Commit**

```bash
git add src/services/query-api/
git commit -m "feat(query): GET /objects/:type/graph?path= multi-link path-cycle route

Adds the multi-link /graph route (path-cycle) sharing the depth/_ids/filter parsing
and error mapping with the single-link route. Proven by a DuckDB e2e: shared-team
membership reachability, depth bounds, cycle termination + dedup, an intermediate
Team row-filter pruning reachable-through Persons, non-cyclic path -> 400.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Docs

**Files:**
- Modify: `docs/superpowers/specs/2026-06-06-loom-roadmap.md`
- Modify: `docs/FUTURE.md`

- [ ] **Step 1: Roadmap**

In `docs/superpowers/specs/2026-06-06-loom-roadmap.md`, add a "Where we are" paragraph: `/graph` part-2 (repeated path-cycle) is delivered — recursive reachability over a multi-link cyclic pattern (`?path=l1,…,lK`), governed at every intermediate type; part-1's single self-link is now the 1-element case.

- [ ] **Step 2: FUTURE.md**

In `docs/FUTURE.md`, read it first, then edit the graph area in place to record `/graph` part-2 (multi-link path-cycle) delivered, and the remaining `/graph` parts: inverse links inside the path, multi-edge union reachability (`?links=`), recursive-core + relational-tail (`path=knows*,worksAt`), min-depth annotation, shortest-path / `/tree`, weighted edges.

- [ ] **Step 3: Lint + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p.log 2>&1; grep -nE "Failed|Passed|error" /tmp/p.log | tail -20`
Expected: all hooks pass (fix any markdown whitespace/EOF the hooks flag, then re-run).

```bash
git add docs/
git commit -m "docs(query): mark /graph part-2 (repeated path-cycle) delivered

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
